"""移动公开事实与保留、双父核实必须符合真实目录状态。"""

import asyncio
import json
import sys
from dataclasses import replace

import httpx
import pytest
from alembic import command
from pydantic import ValidationError
from sqlalchemy import text
from test_directory_managed import setup as agent_setup
from test_directory_management_api import (
    KEY,
    configure,
    counts,
    run_cli,
)
from test_directory_management_api import (
    directory_server as directory_server,  # noqa: F401
)

import krim_agent.workspace_directory as module
from krim_agent.client import ClientError, KrimClient
from krim_agent.persistence.database import WorkspaceDirectoryAttemptRow
from krim_agent.persistence.migration import migration_config
from krim_agent.persistence.workspace_directories import WorkspaceDirectoryStore
from krim_agent.protocol.domain import ServiceError
from krim_agent.providers import ModelText, ModelToolCall
from krim_agent.workspace import Workspace
from krim_agent.workspace_directory import (
    DirectoryChangeAction,
    DirectoryChangeSpec,
    DirectoryPreviewRequest,
    DirectoryPreviewView,
    WorkspaceDirectoryWriter,
)

pytestmark = pytest.mark.skipif(not sys.platform.startswith("linux"), reason="需要Linux目录移动")


def prepare_paths(root):
    (root / "source").mkdir()
    (root / "target").mkdir()
    source = root / "source" / "empty"
    source.mkdir(mode=0o750)
    return source.stat().st_ino


def intent(fingerprint, key="move-preview", target="target/moved"):
    return DirectoryPreviewRequest(
        spec=DirectoryChangeSpec(
            path="source/empty",
            operation="move",
            target_path=target,
            expected_fingerprint=fingerprint,
        ),
        request_key=key,
    )


async def test_move_public_preview_returns_frozen_target_and_by_key(directory_server):
    url, root, app = directory_server
    prepare_paths(root)

    def scenario():
        with KrimClient(url, KEY) as client:
            agent = configure(client)
            snapshot = client.agent_directory_inspect(agent, "source/empty")
            request = intent(snapshot.fingerprint)
            proposal = client.agent_directory_preview(agent, request)
            assert proposal.operation == "move" and proposal.target_path == "target/moved"
            original = client.agent_directory_query(
                agent, kind="proposal_by_key", request_key=request.request_key
            ).proposal
            assert original == proposal
            assert client.agent_directory_preview(agent, request) == proposal

    await asyncio.to_thread(scenario)
    assert await counts(app) == (1, 0)


async def test_move_keep_checks_destination_then_cli_undo_original_inode(directory_server):
    url, root, app = directory_server
    inode = prepare_paths(root)

    def scenario():
        with KrimClient(url, KEY) as client:
            agent = configure(client)
            snapshot = client.agent_directory_inspect(agent, "source/empty")
            proposal = client.agent_directory_preview(agent, intent(snapshot.fingerprint))
            request = DirectoryChangeAction(
                proposal_id=proposal.proposal_id,
                action="apply",
                expected_revision=0,
                request_key="move-apply",
            )
            applied = client.agent_directory_change(agent, request, confirmed=True)
            assert applied.status == "done" and (root / "target" / "moved").stat().st_ino == inode
            assert client.agent_directory_change(agent, request, confirmed=True) == applied
            current = client.agent_directory_query(
                agent, kind="proposal", identifier=proposal.proposal_id
            ).proposal
            kept = client.agent_directory_change(
                agent,
                DirectoryChangeAction(
                    proposal_id=proposal.proposal_id,
                    action="keep",
                    expected_revision=current.revision,
                    request_key="move-keep",
                ),
                confirmed=True,
            )
            assert kept.status == "done", kept.receipt
            assert not kept.receipt.changed
            return agent, proposal.proposal_id

    agent, proposal = await asyncio.to_thread(scenario)
    code, stdout, stderr = await run_cli(url, "get", agent, proposal)
    assert code == 0, stderr
    current = json.loads(stdout)["proposal"]
    assert current["target_path"] == "target/moved" and current["status"] == "kept"
    code, stdout, stderr = await run_cli(
        url,
        "undo",
        agent,
        proposal,
        "--revision",
        current["revision"],
        "--request-key",
        "move-undo",
        "--confirm",
    )
    assert code == 0 and json.loads(stdout)["status"] == "done", stderr
    assert (root / "source" / "empty").stat().st_ino == inode
    assert not (root / "target" / "moved").exists()
    assert await counts(app) == (1, 3)


@pytest.mark.parametrize("parent", ["source", "target"])
def test_move_unknown_never_confirms_replaced_parent(tmp_path, monkeypatch, parent):
    prepare_paths(tmp_path)
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceDirectoryWriter(workspace)
        before = writer.capture("source/empty")
        plan = writer.prepare(intent(before.fingerprint).spec)
        original = module.rename_at

        def unknown(*args, **kwargs):
            original(*args, **kwargs)
            raise OSError("lost rename result")

        monkeypatch.setattr(module, "rename_at", unknown)
        result = writer.apply(plan)
        assert result.receipt.status == "uncertain" and result.recovery is not None
        monkeypatch.setattr(module, "rename_at", original)
        displaced = tmp_path / (parent + "-old")
        (tmp_path / parent).rename(displaced)
        (tmp_path / parent).mkdir()
        if parent == "target":
            (displaced / "moved").rename(tmp_path / "target" / "moved")
        verified = writer.verify(plan, result.recovery)
        assert verified.receipt.status == "uncertain"
        if parent == "target":
            (tmp_path / "target" / "moved").rename(displaced / "moved")
        (tmp_path / parent).rmdir()
        displaced.rename(tmp_path / parent)
        assert writer.verify(plan, result.recovery).receipt.status == "done"


@pytest.mark.parametrize("case", ["target", "source_mode", "source_parent", "target_parent"])
def test_move_rechecks_after_intent_without_overwriting_external_directory(tmp_path, case):
    inode = prepare_paths(tmp_path)
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceDirectoryWriter(workspace)
        before = writer.capture("source/empty")
        plan = writer.prepare(intent(before.fingerprint).spec)
        external_inode = None

        def compete(_reference):
            nonlocal external_inode
            if case == "target":
                (tmp_path / "target" / "moved").mkdir()
                external_inode = (tmp_path / "target" / "moved").stat().st_ino
            elif case == "source_mode":
                (tmp_path / "source" / "empty").chmod(0o700)
            else:
                parent = "source" if case == "source_parent" else "target"
                (tmp_path / parent).rename(tmp_path / (parent + "-old"))
                (tmp_path / parent).mkdir()

        result = writer.apply(plan, record_intent=compete)
        assert result.receipt.status == "rejected"
        if case == "target":
            assert (tmp_path / "target" / "moved").stat().st_ino == external_inode
        source_parent = "source-old" if case == "source_parent" else "source"
        assert (tmp_path / source_parent / "empty").stat().st_ino == inode


def test_move_syscall_never_overwrites_last_instant_target(tmp_path, monkeypatch):
    inode = prepare_paths(tmp_path)
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceDirectoryWriter(workspace)
        before = writer.capture("source/empty")
        plan = writer.prepare(intent(before.fingerprint).spec)
        original = module.rename_at
        external_inode = None

        def compete(*args, **kwargs):
            nonlocal external_inode
            (tmp_path / "target" / "moved").mkdir()
            external_inode = (tmp_path / "target" / "moved").stat().st_ino
            return original(*args, **kwargs)

        monkeypatch.setattr(module, "rename_at", compete)
        result = writer.apply(plan)
        assert result.receipt.status == "rejected" and not result.receipt.changed
        assert (tmp_path / "source" / "empty").stat().st_ino == inode
        assert (tmp_path / "target" / "moved").stat().st_ino == external_inode


@pytest.mark.parametrize(
    "case",
    ["source_recreated", "target_replaced", "target_nonempty", "source_parent", "target_parent"],
)
def test_move_keep_rejects_changed_entries_or_parents_without_writes(tmp_path, case):
    inode = prepare_paths(tmp_path)
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceDirectoryWriter(workspace)
        before = writer.capture("source/empty")
        plan = writer.prepare(intent(before.fingerprint).spec)
        applied = writer.apply(plan)
        assert applied.receipt.status == "done"
        if case == "source_recreated":
            (tmp_path / "source" / "empty").mkdir()
        elif case == "target_replaced":
            (tmp_path / "target" / "moved").rename(tmp_path / "target" / "displaced")
            (tmp_path / "target" / "moved").mkdir()
        elif case == "target_nonempty":
            (tmp_path / "target" / "moved" / "file").write_bytes(b"external")
        else:
            parent = "source" if case == "source_parent" else "target"
            (tmp_path / parent).rename(tmp_path / (parent + "-old"))
            (tmp_path / parent).mkdir()
        kept = writer.verify_keep(plan, applied)
        assert kept.receipt.status == "rejected" and not kept.receipt.changed
        target_parent = "target-old" if case == "target_parent" else "target"
        target_name = "displaced" if case == "target_replaced" else "moved"
        assert (tmp_path / target_parent / target_name).stat().st_ino == inode


def test_move_recovery_cannot_redirect_to_another_target(tmp_path, monkeypatch):
    prepare_paths(tmp_path)
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceDirectoryWriter(workspace)
        before = writer.capture("source/empty")
        plan = writer.prepare(intent(before.fingerprint).spec)
        original = module.rename_at

        def unknown(*args, **kwargs):
            original(*args, **kwargs)
            raise OSError("lost syscall result")

        monkeypatch.setattr(module, "rename_at", unknown)
        effect = writer.apply(plan)
        assert effect.recovery is not None
        with pytest.raises(ServiceError) as redirected:
            writer.verify(plan, replace(effect.recovery, target_path="target/another"))
        assert redirected.value.code == "workspace_directory_reference"


async def test_move_cli_preview_and_unknown_reconcile_preserve_original_key(
    directory_server, tmp_path, monkeypatch
):
    url, root, app = directory_server
    inode = prepare_paths(root)
    with KrimClient(url, KEY) as client:
        agent = await asyncio.to_thread(configure, client)
    code, stdout, stderr = await run_cli(url, "inspect", agent, "source/empty")
    assert code == 0, stderr
    request = intent(json.loads(stdout)["fingerprint"])
    request_file = tmp_path / "move.json"
    request_file.write_text(request.model_dump_json(), encoding="utf-8")
    code, stdout, stderr = await run_cli(url, "preview", agent, "--request", request_file)
    assert code == 0, stderr
    proposal = json.loads(stdout)
    assert proposal["target_path"] == "target/moved"
    backend = app.state.agents.manager.directory_changes
    original = module.rename_at
    calls = 0

    def unknown(*args, **kwargs):
        nonlocal calls
        calls += 1
        original(*args, **kwargs)
        raise OSError("lost syscall result")

    monkeypatch.setattr(module, "rename_at", unknown)
    action_args = (
        "apply",
        agent,
        proposal["proposal_id"],
        "--revision",
        0,
        "--request-key",
        "unknown-apply",
        "--confirm",
    )
    code, stdout, stderr = await run_cli(url, *action_args)
    assert code == 1, stderr
    unknown_result = json.loads(stdout)
    assert unknown_result["status"] == "uncertain" and calls == 1
    assert (root / "target" / "moved").stat().st_ino == inode
    code, stdout, stderr = await run_cli(url, *action_args)
    assert code == 1 and json.loads(stdout)["operation_id"] == unknown_result["operation_id"]
    assert calls == 1
    operation = unknown_result["operation_id"]
    code, stdout, stderr = await run_cli(url, "reconcile", agent, operation, "--confirm")
    assert code == 0 and json.loads(stdout)["status"] == "done", stderr
    assert calls == 1
    saved = await backend.store.get("session:" + agent, proposal["proposal_id"])
    assert saved.view.target_path == "target/moved" and saved.view.status == "applied"
    assert await counts(app) == (1, 1)


async def test_move_target_lock_refuses_lossy_downgrade(repository, tmp_path):
    topic = await repository.create_topic("move downgrade")
    scope = "topic:" + topic.id
    prepare_paths(tmp_path)
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceDirectoryWriter(workspace)
        snapshot = writer.capture("source/empty")
        prepared = writer.prepare(intent(snapshot.fingerprint).spec)
        store = WorkspaceDirectoryStore(repository.database)
        proposal, _ = await store.preview(scope, "files", workspace.fingerprint, "move", prepared)
        attempt, _ = await store.reserve(
            scope,
            DirectoryChangeAction(
                proposal_id=proposal.view.proposal_id,
                action="apply",
                expected_revision=0,
                request_key="move-action",
            ),
            workspace.fingerprint,
            prepared,
        )
        with pytest.raises(RuntimeError, match="目录移动目标锁非空"):
            async with repository.database.engine.begin() as connection:
                await connection.run_sync(
                    lambda sync: command.downgrade(
                        migration_config(sync), "0037_command_directory_sources"
                    )
                )
        async with repository.database.engine.connect() as connection:
            assert (
                await connection.execute(text("SELECT version_num FROM alembic_version"))
            ).scalar_one() == "0052_directory_checkpoints"
            assert (
                await connection.execute(
                    text(
                        "SELECT target_lock_key FROM workspace_directory_locks "
                        "WHERE operation_id=:id"
                    ),
                    {"id": attempt.view.operation_id},
                )
            ).scalar_one()
            assert not (await connection.execute(text("PRAGMA foreign_key_check"))).all()


def test_client_rejects_redirected_move_and_view_requires_valid_target():
    response = {
        "proposal_id": "proposal",
        "scope_id": "session:agent",
        "workspace_key": "files",
        "resource_fingerprint": "1" * 64,
        "path": "source/empty",
        "operation": "move",
        "target_path": "target/redirected",
        "before_fingerprint": "0" * 64,
        "mode": None,
        "revision": 0,
        "status": "proposed",
        "expires_at": 9999999999.0,
    }
    with KrimClient(
        transport=httpx.MockTransport(lambda _r: httpx.Response(200, json=response))
    ) as client:
        with pytest.raises(ClientError) as redirected:
            client.agent_directory_preview("agent", intent("0" * 64))
        assert redirected.value.code == "workspace_directory_response"
    for target in (None, "../escape", "source/empty"):
        with pytest.raises(ValidationError):
            DirectoryPreviewView.model_validate({**response, "target_path": target})


async def test_offline_model_move_loop_preserves_run_call_and_frozen_target(runtime, tmp_path):
    inode = prepare_paths(tmp_path)

    class Provider:
        name = "offline-directory-move"
        calls = 0

        async def stream(self, messages, tools):
            self.calls += 1
            assert {"directory_inspect", "directory_preview", "directory_change"} <= {
                item.name for item in tools
            }
            if self.calls == 1:
                yield ModelToolCall(
                    id="move-inspect-call",
                    name="directory_inspect",
                    arguments={"path": "source/empty"},
                )
            elif self.calls == 2:
                snapshot = json.loads(messages[-1].content)
                yield ModelToolCall(
                    id="move-preview-call",
                    name="directory_preview",
                    arguments=intent(snapshot["fingerprint"]).model_dump(mode="json"),
                )
            elif self.calls == 3:
                proposal = json.loads(messages[-1].content)
                assert proposal["target_path"] == "target/moved"
                yield ModelToolCall(
                    id="move-apply-call",
                    name="directory_change",
                    arguments={
                        "proposal_id": proposal["proposal_id"],
                        "action": "apply",
                        "expected_revision": proposal["revision"],
                        "request_key": "model-move-apply",
                    },
                )
            else:
                result = json.loads(messages[-1].content)
                assert result["status"] == "done"
                yield ModelText(text="已读取真实移动回执")

    runtime.provider = Provider()
    service, agent = await agent_setup(runtime, tmp_path)
    try:
        result = await agent.execute("移动已授权空目录", idempotency_key="model-move-turn")
        assert result.status == "completed" and result.text == "已读取真实移动回执"
        assert (tmp_path / "target" / "moved").stat().st_ino == inode
        store = service.manager.directory_changes.store
        saved = await store.operation_by_key("session:" + agent.session_id, "model-move-apply")
        assert saved is not None and saved.view.status == "done"
        async with runtime.repository.database.sessions() as database:
            row = await database.get(WorkspaceDirectoryAttemptRow, saved.view.operation_id)
            assert row.run_id == result.run_id and row.call_id == "move-apply-call"
        proposal = await store.get("session:" + agent.session_id, saved.view.proposal_id)
        assert proposal.view.target_path == "target/moved"
    finally:
        await service.close()
