"""工作区边界、UTF-8/容量、符号链接和工具权限。"""

import asyncio
import hashlib
import json
import os
from dataclasses import replace
from pathlib import Path
from threading import Event

import pytest

from krim_agent.agents import AgentError, AgentProfile, AgentProfileUpdate
from krim_agent.application.agents import AgentPromptRequest, AgentService
from krim_agent.config import Settings
from krim_agent.persistence.database import AgentSessionRow
from krim_agent.protocol.domain import ServiceError
from krim_agent.providers import FakeProvider, ModelText, ModelToolCall
from krim_agent.sdk import PythonAI
from krim_agent.tools import ToolContext, ToolRegistry
from krim_agent.tools.workspace import (
    ListFilesInput,
    ReadFileInput,
    workspace_tools,
)
from krim_agent.workspace import Workspace, WorkspaceBinding

pytestmark = pytest.mark.skipif(os.name != "posix", reason="需要POSIX dirfd/O_NOFOLLOW")


def make_workspace(tmp_path: Path) -> Workspace:
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "main.py").write_text("one\ntwo\nneedle\n", encoding="utf-8")
    (tmp_path / ".env").write_text("SECRET=hidden", encoding="utf-8")
    (tmp_path / "blob.bin").write_bytes(b"\x00\x01")
    return Workspace(tmp_path)


def test_workspace_read_list_search_hash_and_sensitive_boundaries(tmp_path):
    workspace = make_workspace(tmp_path)
    try:
        read = workspace.read("src/main.py", start_line=2, max_lines=2)
        assert read.content == "two\nneedle\n" and len(read.sha256) == 64
        assert [item.path for item in workspace.list("src").entries] == ["src/main.py"]
        matches = workspace.search("needle")
        assert matches.matches[0].path == "src/main.py" and matches.matches[0].line == 3
        for invalid in ["../secret", "/etc/passwd", ".env", "src/../main.py"]:
            with pytest.raises(ServiceError) as error:
                workspace.read(invalid)
            assert error.value.code in {"workspace_path", "workspace_sensitive"}
        with pytest.raises(ServiceError) as binary:
            workspace.read("blob.bin")
        assert binary.value.code == "workspace_binary"
    finally:
        workspace.close()


@pytest.mark.skipif(os.name != "posix", reason="需要Linux dirfd/O_NOFOLLOW实现")
def test_workspace_rejects_symlink(tmp_path):
    workspace = make_workspace(tmp_path)
    (tmp_path / "outside.txt").write_text("outside", encoding="utf-8")
    (tmp_path / "link.txt").symlink_to(tmp_path / "outside.txt")
    try:
        with pytest.raises(ServiceError) as link:
            workspace.read("link.txt")
        assert link.value.code in {"workspace_access", "workspace_file_type"}
    finally:
        workspace.close()


async def test_workspace_tools_use_same_registry_permission_and_bounded_output(tmp_path):
    workspace = make_workspace(tmp_path)
    registry = ToolRegistry()
    registry.register_many(*workspace_tools())
    binding = WorkspaceBinding("test", "v1", workspace, workspace.fingerprint)

    async def deny(_definition, _context):
        return "deny"

    try:
        result = await registry.invoke(
            "workspace_read", {"path": "src/main.py"}, ToolContext(registry, workspace=binding)
        )
        assert not result.is_error and result.data["sha256"]
        denied = await registry.invoke(
            "workspace_read",
            {"path": "src/main.py"},
            ToolContext(registry, policy=deny, workspace=binding),
        )
        assert denied.is_error and denied.code == "permission_denied"
    finally:
        workspace.close()


def test_workspace_rejects_large_file(tmp_path):
    (tmp_path / "large.txt").write_text("x" * 100, encoding="utf-8")
    workspace = Workspace(tmp_path, max_file_bytes=64)
    try:
        with pytest.raises(ServiceError) as error:
            workspace.read("large.txt")
        assert error.value.code == "workspace_file_limit"
    finally:
        workspace.close()


@pytest.mark.parametrize(
    "path",
    [
        "../secret",
        "/etc/passwd",
        "src/../main.py",
        "src//main.py",
        "src/./main.py",
        "C:/secret",
        "C:\\secret",
        "src/main.py\0",
        "src/main.py\n",
        "x/" * 17,
        "x" * 513,
        "密" * 172,
        ".env",
        ".ENV.local",
        ".git/config",
        ".ssh/id_rsa",
        ".aws/credentials",
        "private.PEM",
        "key.pfx",
        "node_modules/module.js",
    ],
)
def test_workspace_invalid_and_sensitive_paths_never_open_files(tmp_path, path):
    with Workspace(tmp_path) as workspace:
        with pytest.raises(ServiceError) as error:
            workspace.read(path)
        assert error.value.code in {"workspace_path", "workspace_sensitive"}


def test_non_utf8_hardlink_fifo_and_missing_file_are_rejected(tmp_path):
    (tmp_path / "invalid.txt").write_bytes(b"\xff")
    (tmp_path / "plain.txt").write_text("hello", encoding="utf-8")
    os.link(tmp_path / "plain.txt", tmp_path / "hard.txt")
    os.mkfifo(tmp_path / "pipe")
    with Workspace(tmp_path) as workspace:
        for path, code in [
            ("invalid.txt", "workspace_encoding"),
            ("hard.txt", "workspace_link"),
            ("pipe", "workspace_file_type"),
            ("missing.txt", "workspace_not_found"),
            (".", "workspace_file_type"),
        ]:
            with pytest.raises(ServiceError) as error:
                workspace.read(path)
            assert error.value.code == code
        assert {item.path for item in workspace.list().entries} == {"invalid.txt"}


def test_directory_swap_to_symlink_during_open_cannot_escape(tmp_path, monkeypatch):
    root, outside = tmp_path / "root", tmp_path / "outside"
    (root / "src").mkdir(parents=True)
    outside.mkdir()
    (root / "src" / "main.py").write_text("inside", encoding="utf-8")
    (outside / "main.py").write_text("hidden", encoding="utf-8")
    original_open = os.open
    switched = False

    def swap(path, flags, *args, **kwargs):
        nonlocal switched
        if path == "src" and kwargs.get("dir_fd") is not None and not switched:
            switched = True
            (root / "src").rename(root / "saved")
            (root / "src").symlink_to(outside, target_is_directory=True)
        return original_open(path, flags, *args, **kwargs)

    with Workspace(root) as workspace:
        monkeypatch.setattr(os, "open", swap)
        with pytest.raises(ServiceError) as error:
            workspace.read("src/main.py")
        assert switched and error.value.code == "workspace_access"


def test_file_changed_during_read_requires_new_snapshot(tmp_path, monkeypatch):
    file = tmp_path / "file.txt"
    file.write_text("old", encoding="utf-8")
    original_read = os.read
    changed = False

    def read(fd, size):
        nonlocal changed
        result = original_read(fd, size)
        if not changed:
            changed = True
            file.write_text("new content", encoding="utf-8")
        return result

    with Workspace(tmp_path) as workspace:
        monkeypatch.setattr(os, "read", read)
        with pytest.raises(ServiceError) as error:
            workspace.read("file.txt")
        assert error.value.code == "workspace_changed"


def test_search_reads_beyond_read_preview_and_reports_skipped_and_truncation(tmp_path):
    (tmp_path / "late.txt").write_text("line\n" * 1500 + "late needle\n", encoding="utf-8")
    (tmp_path / "bad.txt").write_bytes(b"\0bad")
    (tmp_path / ".env").write_text("needle", encoding="utf-8")
    with Workspace(tmp_path) as workspace:
        assert workspace.read("late.txt").truncated
        result = workspace.search("needle")
        assert [(item.path, item.line) for item in result.matches] == [("late.txt", 1501)]
        assert not result.truncated and result.files_skipped == 1
        limited = workspace.search("line", limit=1)
        assert len(limited.matches) == 1 and limited.truncated
        assert workspace.list(limit=1).truncated


def test_json_escaping_unicode_and_long_paths_keep_all_outputs_bounded(tmp_path):
    (tmp_path / "quoted.txt").write_text('"\\\t你好' * 10000, encoding="utf-8")
    for index in range(80):
        (tmp_path / f"{index}-{'x' * 180}.txt").write_text("match\n", encoding="utf-8")
    with Workspace(tmp_path) as workspace:
        file = workspace.read("quoted.txt")
        listing = workspace.list(limit=1000)
        search = workspace.search("match")
        for value in (file, listing, search):
            assert len(value.model_dump_json().encode()) <= 24000
        assert file.truncated and not listing.truncated
        assert file.sha256 == hashlib.sha256((tmp_path / "quoted.txt").read_bytes()).hexdigest()


async def test_concurrent_reads_are_independent_and_closed_workspace_rejects_new_reads(tmp_path):
    workspace = make_workspace(tmp_path)
    results = await asyncio.gather(
        *(asyncio.to_thread(workspace.read, "src/main.py") for _ in range(24))
    )
    assert len({result.sha256 for result in results}) == 1
    assert all(result.content == "one\ntwo\nneedle\n" for result in results)
    workspace.close()
    workspace.close()
    with pytest.raises(ServiceError) as error:
        workspace.read("src/main.py")
    assert error.value.code == "workspace_closed"


def config(name="reader", key="files", tools=None, mode="agent"):
    return AgentProfile(
        name=name,
        description="工作区权限",
        workspace_key=key,
        mode=mode,
        tool_names=tools
        if tools is not None
        else ["workspace_read", "workspace_list", "workspace_search"],
    )


async def setup(service, root, *, name="reader", key="files", tools=None, mode="agent"):
    service.register_workspace(key, root, version="v1")
    await service.profile_put(
        AgentProfileUpdate(expected_revision=0, config=config(name, key, tools, mode))
    )
    return await service.spawn(name)


async def test_profiles_isolate_roots_tools_and_never_expose_host_paths(runtime, tmp_path):
    roots = [tmp_path / "one", tmp_path / "two"]
    for index, root in enumerate(roots):
        root.mkdir()
        (root / "file.txt").write_text(f"workspace {index}", encoding="utf-8")
    service = AgentService(runtime, runtime.settings)
    try:
        first = await setup(service, roots[0], name="one", key="one", tools=["workspace_read"])
        second = await setup(service, roots[1], name="two", key="two")
        assert (
            await service.workspace_read(first.agent_id, ReadFileInput(path="file.txt"))
        ).content == "workspace 0"
        assert (
            await service.workspace_read(second.agent_id, ReadFileInput(path="file.txt"))
        ).content == "workspace 1"
        with pytest.raises(ServiceError) as denied:
            await service.workspace_list(first.agent_id, ListFilesInput())
        assert denied.value.code == "tool_not_found"
        public = json.dumps(
            [
                [item.model_dump(mode="json") for item in service.resource_inventory()],
                (await service.session(first.agent_id)).model_dump(mode="json"),
            ]
        )
        assert str(tmp_path) not in public
        plain = await service.spawn("default")
        assert not any(
            tool.name.startswith("workspace_")
            for tool in service.manager._handles[plain.agent_id].runtime.tools.definitions
        )
        with pytest.raises(ServiceError) as invalid:
            await service.profile_put(
                AgentProfileUpdate(expected_revision=0, config=config("invalid", key=None))
            )
        assert invalid.value.code == "agent_profile_workspace"
    finally:
        await service.close()


@pytest.mark.parametrize(
    "mode,allowed", [("agent", True), ("ask", False), ("plan", True), ("auto", True)]
)
async def test_workspace_management_respects_session_modes(runtime, tmp_path, mode, allowed):
    (tmp_path / "file.txt").write_text("data", encoding="utf-8")
    service = AgentService(runtime, runtime.settings)
    try:
        view = await setup(service, tmp_path, mode=mode)
        if allowed:
            assert (
                await service.workspace_read(view.agent_id, ReadFileInput(path="file.txt"))
            ).content == "data"
        else:
            with pytest.raises(ServiceError) as denied:
                await service.workspace_read(view.agent_id, ReadFileInput(path="file.txt"))
            assert denied.value.code == "permission_denied"
        audit = service.manager._handles[view.agent_id].runtime.tools.audit[-1]
        assert audit.name == "workspace_read" and audit.ok is allowed
        assert audit.run_id == "local"
    finally:
        await service.close()


async def test_management_reads_use_hooks_and_host_policy_without_creating_fake_runs(
    runtime, tmp_path
):
    (tmp_path / "file.txt").write_text("secret body", encoding="utf-8")
    entered = []

    async def ask(_definition, context):
        entered.append(context.origin)
        return "ask"

    runtime.tool_policy = ask
    service = AgentService(runtime, runtime.settings)
    try:
        view = await setup(service, tmp_path)
        with pytest.raises(ServiceError) as rejected:
            await service.workspace_read(view.agent_id, ReadFileInput(path="file.txt"))
        assert rejected.value.code == "approval_required" and entered == ["host"]
        assert (await service.get(view.agent_id)).turn == 0
        assert not await service.session_turns(view.agent_id)
    finally:
        await service.close()


async def test_model_workspace_tool_call_and_persisted_permission_audit(runtime, tmp_path):
    (tmp_path / "file.txt").write_text("model-visible", encoding="utf-8")

    class Reader:
        name = "workspace-reader"

        async def stream(self, messages, tools):
            results = [item for item in messages if item.role == "tool"]
            if results:
                yield ModelText(text=results[-1].content)
            else:
                assert {tool.name for tool in tools} == {"workspace_read"}
                yield ModelToolCall(name="workspace_read", arguments={"path": "file.txt"})

    runtime.provider = Reader()
    service = AgentService(runtime, runtime.settings)
    try:
        view = await setup(service, tmp_path, tools=["workspace_read"])
        result = await service.execute(view.agent_id, AgentPromptRequest(prompt="读取文件"))
        assert result.status == "completed" and "model-visible" in result.result.text
        audits = await runtime.repository.audits(result.current_run_id)
        assert len(audits) == 1 and audits[0].ok and audits[0].name == "workspace_read"
    finally:
        await service.close()


async def test_workspace_revocation_session_close_expiry_and_owner_loss(runtime, tmp_path):
    (tmp_path / "file.txt").write_text("data", encoding="utf-8")
    service = AgentService(runtime, runtime.settings)
    try:
        view = await setup(service, tmp_path)
        request = ReadFileInput(path="file.txt")
        closed = await service.close_session(view.agent_id, 0)
        assert not (await service.workspace_info(view.agent_id)).available
        with pytest.raises(ServiceError) as denied:
            await service.workspace_read(view.agent_id, request)
        assert denied.value.code == "agent_session_closed"
        await service.reopen_session(view.agent_id, closed.revision)
        async with runtime.repository.database.sessions.begin() as session:
            row = await session.get(AgentSessionRow, view.session_id)
            row.owner_id = "another-owner"
        with pytest.raises(ServiceError) as owner:
            await service.workspace_read(view.agent_id, request)
        assert owner.value.code == "agent_session_owner"
        async with runtime.repository.database.sessions.begin() as session:
            row = await session.get(AgentSessionRow, view.session_id)
            row.owner_id = service.manager.owner_id
            row.expires_at = 1
        with pytest.raises(ServiceError) as expired:
            await service.workspace_read(view.agent_id, request)
        assert expired.value.code == "agent_session_expired"
        async with runtime.repository.database.sessions.begin() as session:
            row = await session.get(AgentSessionRow, view.session_id)
            row.expires_at = None
        service.disable_workspace("files")
        with pytest.raises(ServiceError) as revoked:
            await service.workspace_read(view.agent_id, request)
        assert revoked.value.code == "workspace_unavailable"
        assert not (await service.workspace_info(view.agent_id)).available
    finally:
        await service.close()


async def test_parent_agent_cannot_expand_workspace_or_file_tools(runtime, tmp_path):
    first_root, second_root = tmp_path / "one", tmp_path / "two"
    first_root.mkdir()
    second_root.mkdir()
    service = AgentService(runtime, runtime.settings)
    try:
        parent = await setup(service, first_root, tools=["workspace_read"])
        await setup(service, second_root, name="other", key="other")
        definition = await service.definition("reader")
        child = await service.manager.spawn(replace(definition, parent_agent_id=parent.agent_id))
        assert child.runtime.workspace.fingerprint == definition.workspace.fingerprint
        different = await service.definition("other")
        with pytest.raises(AgentError) as rejected:
            await service.manager.spawn(replace(different, parent_agent_id=parent.agent_id))
        assert rejected.value.code == "agent_workspace_forbidden"
        plain = await service.spawn("default")
        with pytest.raises(AgentError):
            await service.manager.spawn(replace(definition, parent_agent_id=plain.agent_id))
        expanded = replace(
            definition, tools=runtime.tools.select(["workspace_read", "workspace_search"])
        )
        with pytest.raises(AgentError):
            await service.manager.spawn(replace(expanded, parent_agent_id=parent.agent_id))
    finally:
        await service.close()


async def test_inflight_management_cancel_and_close_wait_for_worker_thread(
    runtime, tmp_path, monkeypatch
):
    (tmp_path / "file.txt").write_text("data", encoding="utf-8")
    service = AgentService(runtime, runtime.settings)
    entered, release = Event(), Event()
    view = await setup(service, tmp_path)
    workspace = service.resources.workspace("files").workspace
    original = workspace.read

    def slow(*args, **kwargs):
        entered.set()
        assert release.wait(3)
        return original(*args, **kwargs)

    monkeypatch.setattr(workspace, "read", slow)
    operation = asyncio.create_task(
        service.workspace_read(view.agent_id, ReadFileInput(path="file.txt"))
    )
    close = None
    try:
        assert await asyncio.to_thread(entered.wait, 2)
        close = asyncio.create_task(service.close())
        await asyncio.sleep(0.02)
        assert not close.done() and not workspace.closed
        release.set()
        await asyncio.wait_for(close, 3)
        with pytest.raises(asyncio.CancelledError):
            await operation
        assert workspace.closed
    finally:
        release.set()
        await asyncio.gather(operation, *([close] if close else []), return_exceptions=True)
        await service.close()


async def test_sdk_persistent_restore_pins_root_identity_and_version(tmp_path):
    root, other = tmp_path / "files", tmp_path / "other"
    root.mkdir()
    other.mkdir()
    (root / "file.txt").write_text("first", encoding="utf-8")
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path / 'agents.sqlite'}")
    async with await PythonAI.create(
        settings=settings, provider=FakeProvider(delay=0), workspace_root=root
    ) as warm:
        agent = await warm.agent(persistent=True)
        original = await agent.read_file("file.txt")
        assert original.content == "first" and (await agent.workspace()).key == "local"
        await agent.execute("/echo saved", idempotency_key="saved")
        identifier, first_run = agent.agent_id, (await agent.view()).current_run_id
    (root / "file.txt").write_text("updated", encoding="utf-8")
    for changed_root, version in [(other, "local-v1"), (root, "v2")]:
        async with await PythonAI.create(
            settings=settings,
            provider=FakeProvider(delay=0),
            workspace_root=changed_root,
            workspace_version=version,
        ) as cold:
            assert await cold.agents().restore_persistent() == 0
            assert (await cold.agents().inventory())[0].error_code == "agent_definition_changed"
    async with await PythonAI.create(
        settings=settings, provider=FakeProvider(delay=0), workspace_root=root
    ) as cold:
        assert await cold.agents().restore_persistent() == 1
        restored = cold.agent_session(identifier)
        assert (await restored.view()).current_run_id == first_run
        assert (await restored.read_file("file.txt")).content == "updated"
        assert (await restored.search_files("updated")).matches[0].line == 1
        assert (await restored.list_files()).entries[0].path == "file.txt"


def test_platform_capability_and_root_identity_and_limits(tmp_path, monkeypatch):
    root = tmp_path / "root"
    root.mkdir()
    (root / "file.txt").write_text("one", encoding="utf-8")
    with Workspace(root) as first:
        fingerprint = first.fingerprint
    (root / "file.txt").write_text("two", encoding="utf-8")
    with Workspace(root) as unchanged:
        assert unchanged.fingerprint == fingerprint
    with Workspace(root, max_file_bytes=64) as restricted:
        assert restricted.fingerprint != fingerprint
    root.rename(tmp_path / "old-root")
    root.mkdir()
    with Workspace(root) as replacement:
        assert replacement.fingerprint != fingerprint
    monkeypatch.setattr(os, "supports_dir_fd", set())
    with pytest.raises(ServiceError) as unsupported:
        Workspace(root)
    assert unsupported.value.code == "workspace_platform"


async def test_owned_workspace_closes_once_and_borrowed_workspace_remains_host_owned(
    runtime, tmp_path
):
    class CountingWorkspace(Workspace):
        calls = 0

        def close(self):
            self.calls += 1
            super().close()

    owned, borrowed = CountingWorkspace(tmp_path), Workspace(tmp_path)
    service = AgentService(runtime, runtime.settings)
    try:
        service.register_workspace("one", owned, version="v1", owned=True)
        service.register_workspace("two", owned, version="v1", owned=True)
        binding = service.register_workspace("borrowed", borrowed, version="v1")
        with pytest.raises(ServiceError) as conflict:
            service.register_workspace("one", tmp_path, version="v2")
        assert conflict.value.code == "workspace_key_conflict"
        await service.close()
        assert owned.closed and owned.calls == 1
        assert not borrowed.closed and not binding.available
        await service.close()
        assert owned.calls == 1
    finally:
        borrowed.close()
        owned.close()
        await service.close()


async def test_management_read_admission_is_bounded_and_records_permission_hooks(runtime, tmp_path):
    (tmp_path / "file.txt").write_text("bounded", encoding="utf-8")
    release, ready = asyncio.Event(), asyncio.Event()
    calls = []

    async def gate(_definition, context):
        calls.append(context.call_id)
        if len(calls) == 64:
            ready.set()
        await release.wait()
        return None

    runtime.tools.add_global_pre(gate)
    service = AgentService(runtime, runtime.settings)
    operations = []
    try:
        view = await setup(service, tmp_path)
        for _ in range(64):
            operations.append(
                asyncio.create_task(
                    service.workspace_read(view.agent_id, ReadFileInput(path="file.txt"))
                )
            )
        await asyncio.wait_for(ready.wait(), 5)
        with pytest.raises(ServiceError) as busy:
            await service.workspace_read(view.agent_id, ReadFileInput(path="file.txt"))
        assert busy.value.code == "workspace_busy" and busy.value.status == 429
        release.set()
        files = await asyncio.gather(*operations)
        assert all(file.content == "bounded" for file in files)
        assert len(set(calls)) == 64
    finally:
        release.set()
        await asyncio.gather(*operations, return_exceptions=True)
        await service.close()
