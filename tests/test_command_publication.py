"""命令产物以真实私有来源生成提案，沿原workspace_change发布与撤销。"""

import asyncio
import hashlib
import shutil
import sys
from threading import Event

import pytest
from alembic import command
from sqlalchemy import delete, text, update

from krim_agent.agents import AgentProfile, AgentProfileUpdate
from krim_agent.application.agents import AgentService
from krim_agent.context_engine import ContextRunConfig
from krim_agent.execution import CommandSpec, LinuxSandboxRunner
from krim_agent.persistence.command_models import CommandFileRow
from krim_agent.persistence.command_proposals import CommandProposalRow
from krim_agent.persistence.migration import migration_config
from krim_agent.protocol.domain import ServiceError
from krim_agent.providers import ModelText, ModelToolCall
from krim_agent.sdk import (
    AgentCommandRequest,
    AgentSession,
    CommandChangesRequest,
    CommandProposalRequest,
    FileChangeSpec,
    WorkspaceChangeAction,
    WorkspaceCheckpointCreate,
)
from krim_agent.tools import ShortCircuit
from krim_agent.workspace import Workspace
from krim_agent.workspace_list import NativeBatchRequest, NativeBatchTarget
from krim_agent.workspace_writer import WorkspaceWriter

pytestmark = pytest.mark.skipif(
    not sys.platform.startswith("linux") or shutil.which("bwrap") is None,
    reason="需要真实Linux命令和文件事务",
)

NAMES = [
    "workspace_read",
    "workspace_preview",
    "workspace_change",
    "workspace_reconcile",
    "workspace_finalize",
    "command_execute",
    "command_get",
    "command_changes",
    "command_propose",
]


async def setup(
    runtime,
    root,
    *,
    policy=None,
    names=None,
    max_file_bytes=None,
    persistent=False,
    max_steps=8,
    usage_budget=None,
):
    async def allow(*_args):
        return "allow"

    runtime.tool_policy = policy or allow
    service = AgentService(runtime, runtime.settings)
    if max_file_bytes is None:
        service.register_workspace("files", root, version="1", writable=True)
    else:
        service.register_workspace(
            "files",
            Workspace(root, max_file_bytes=max_file_bytes, writable=True),
            version="1",
            writable=True,
            owned=True,
        )
    runner = LinuxSandboxRunner()
    service.register_execution("sandbox", runner, version="1", owned=True)
    await service.profile_put(
        AgentProfileUpdate(
            expected_revision=0,
            config=AgentProfile(
                name="publisher",
                description="命令成果",
                workspace_key="files",
                execution_key="sandbox",
                tool_names=names or NAMES,
                tool_timeout_seconds=30,
                max_steps=max_steps,
                usage_budget=usage_budget,
                context_config=ContextRunConfig(tokenizer="cl100k_base", reserved_output_tokens=128)
                if usage_budget is not None
                else ContextRunConfig(),
            ),
        )
    )
    view = await service.spawn("publisher", persistent=persistent)
    return service, AgentSession(service, view.agent_id)


async def execute(agent, code, paths=(), key="command"):
    return await agent.execute_command(
        AgentCommandRequest(
            request_key=key,
            input_paths=list(paths),
            command=CommandSpec(argv=["/usr/bin/python3", "-I", "-c", code]),
        )
    )


async def propose(agent, result, path, key=None, **kwargs):
    return await agent.propose_command_change(
        CommandProposalRequest(
            command_id=result.command_id, path=path, request_key=key or "preview:" + path, **kwargs
        )
    )


async def act(agent, proposal, kind="apply", key=None, **kwargs):
    current = await agent.file_change(proposal.proposal_id)
    return await agent.change_file(
        WorkspaceChangeAction(
            proposal_id=current.proposal_id,
            expected_revision=current.revision,
            action=kind,
            request_key=key or kind + ":" + proposal.proposal_id,
        ),
        **kwargs,
    )


async def test_real_changes_create_modify_delete_modes_large_text_and_undo(runtime, tmp_path):
    before = {
        "large.txt": "old\n" * 12000,
        "delete.txt": "delete",
        "mode.txt": "same",
        "same.txt": "unchanged",
    }
    for name, content in before.items():
        (tmp_path / name).write_text(content)
        (tmp_path / name).chmod(0o640)
    identities = {
        name: ((tmp_path / name).stat().st_uid, (tmp_path / name).stat().st_gid) for name in before
    }
    service, agent = await setup(runtime, tmp_path)
    try:
        result = await execute(
            agent,
            "from pathlib import Path;import os;Path('large.txt').write_text('new\\n'*60000);"
            "Path('delete.txt').unlink();os.chmod('mode.txt',0o600);"
            "Path('new.txt').write_text('created');os.chmod('new.txt',0o750)",
            before,
        )
        assert result.outcome == "completed"
        page = await agent.command_changes(
            CommandChangesRequest(command_id=result.command_id, limit=2)
        )
        next_page = await agent.command_changes(
            CommandChangesRequest(command_id=result.command_id, after=page.next_after, limit=2)
        )
        entries = page.entries + next_page.entries
        assert {item.path: item.kind for item in entries} == {
            "large.txt": "modified",
            "delete.txt": "deleted",
            "mode.txt": "modified",
            "new.txt": "added",
        }
        assert next_page.next_after is None
        all_files = await agent.command_changes(
            CommandChangesRequest(command_id=result.command_id, include_unchanged=True)
        )
        assert (
            next(item for item in all_files.entries if item.path == "same.txt").kind == "unchanged"
        )
        proposals = [await propose(agent, result, item.path) for item in entries]
        assert {name: (tmp_path / name).read_text() for name in before} == before
        assert not (tmp_path / "new.txt").exists()
        large = next(item for item in proposals if item.change.path == "large.txt")
        assert large.proposal.preview.truncated and large.proposal.preview.after_bytes == 240000
        assert len(large.model_dump_json()) < 32768
        mode = next(item for item in proposals if item.change.path == "mode.txt")
        assert mode.change.before_mode == 0o640 and mode.change.after_mode == 0o600
        assert mode.proposal.preview.diff == ""
        for item in proposals:
            applied = await act(agent, item.proposal)
            assert applied.status == "done" and applied.receipt.changed
        assert (tmp_path / "large.txt").read_text() == "new\n" * 60000
        assert not (tmp_path / "delete.txt").exists()
        assert (tmp_path / "new.txt").read_text() == "created" and (
            tmp_path / "new.txt"
        ).stat().st_mode & 0o777 == 0o750
        assert (tmp_path / "mode.txt").stat().st_mode & 0o777 == 0o600
        for item in proposals:
            if item.change.path in identities and item.change.kind != "deleted":
                status = (tmp_path / item.change.path).stat()
                assert (status.st_uid, status.st_gid) == identities[item.change.path]
            assert (await act(agent, item.proposal, "keep")).status == "done"
            assert (await act(agent, item.proposal, "undo")).status == "done"
        assert not (tmp_path / "new.txt").exists()
        for name, content in before.items():
            assert (tmp_path / name).read_text() == content and (
                tmp_path / name
            ).stat().st_mode & 0o777 == 0o640
        repeated = await propose(agent, result, "large.txt")
        assert (
            repeated.proposal.proposal_id == large.proposal.proposal_id
            and repeated.proposal.status == "undone"
        )
    finally:
        await service.close()


@pytest.mark.parametrize(
    "when", ["before_preview", "after_preview", "new_exists", "new_after_preview"]
)
async def test_external_changes_and_noninput_destinations_are_never_overwritten(
    runtime, tmp_path, when
):
    file = tmp_path / "in.txt"
    file.write_text("original")
    service, agent = await setup(runtime, tmp_path)
    try:
        result = await execute(
            agent,
            "open('in.txt','w').write('artifact');open('new.txt','w').write('new')",
            ["in.txt"],
        )
        path = "new.txt" if when.startswith("new") else "in.txt"
        target = tmp_path / path
        if when in {"before_preview", "new_exists"}:
            target.write_text("external")
            with pytest.raises(ServiceError) as rejected:
                await propose(agent, result, path)
            assert rejected.value.code in {"workspace_conflict", "execution_destination_exists"}
        else:
            proposal = await propose(agent, result, path)
            target.write_text("external")
            outcome = await act(agent, proposal.proposal)
            assert (
                outcome.status == "rejected" and outcome.receipt.error_code == "workspace_conflict"
            )
        assert target.read_text() == "external"
    finally:
        await service.close()


async def test_noop_successor_invalidates_pending_artifact_proposal(runtime, tmp_path):
    file = tmp_path / "in.txt"
    file.write_text("original")
    service, agent = await setup(runtime, tmp_path)
    try:
        result = await execute(agent, "open('in.txt','w').write('artifact')", ["in.txt"])
        proposal = await propose(agent, result, "in.txt")
        noop = await agent.preview_file_change(
            FileChangeSpec(
                path="in.txt",
                operation="write",
                expected_sha256=hashlib.sha256(b"original").hexdigest(),
                content="original",
            ),
            request_key="noop",
        )
        assert not (await act(agent, noop)).receipt.changed
        with pytest.raises(ServiceError) as conflict:
            await act(agent, proposal.proposal)
        assert conflict.value.code == "workspace_conflict" and file.read_text() == "original"
    finally:
        await service.close()


async def test_failed_results_require_opt_in_and_incomplete_outputs_never_publish(
    runtime, tmp_path
):
    service, agent = await setup(runtime, tmp_path)
    try:
        failed = await execute(agent, "open('out.txt','w').write('partial');raise SystemExit(7)")
        assert failed.outcome == "failed" and failed.artifacts_complete
        page = await agent.command_changes(CommandChangesRequest(command_id=failed.command_id))
        assert page.requires_failed_acceptance
        with pytest.raises(ServiceError) as explicit:
            await propose(agent, failed, "out.txt")
        assert explicit.value.code == "execution_failed_artifacts"
        proposal = await propose(agent, failed, "out.txt", accept_failed=True)
        assert (await act(agent, proposal.proposal)).status == "done"
        assert (tmp_path / "out.txt").read_text() == "partial"
        incomplete = await execute(
            agent,
            "import os;open('valid.txt','w').write('x');os.symlink('valid.txt','link')",
            key="incomplete",
        )
        assert not incomplete.artifacts_complete
        with pytest.raises(ServiceError) as invalid:
            await propose(agent, incomplete, "valid.txt", accept_failed=True)
        assert (
            invalid.value.code == "execution_artifacts_incomplete"
            and not (tmp_path / "valid.txt").exists()
        )
    finally:
        await service.close()


async def test_binary_artifact_is_readable_and_preserves_private_bytes_on_publish(
    runtime, tmp_path
):
    service, agent = await setup(runtime, tmp_path)
    try:
        result = await execute(agent, "open('binary.bin','wb').write(b'\\0\\xff')")
        page = await agent.command_changes(CommandChangesRequest(command_id=result.command_id))
        assert page.entries[0].path == "binary.bin"
        proposal = await propose(agent, result, "binary.bin")
        assert proposal.proposal.preview.truncated
        assert "binary" in proposal.proposal.preview.diff
        assert not (tmp_path / "binary.bin").exists()
        assert (await act(agent, proposal.proposal)).status == "done"
        assert (tmp_path / "binary.bin").read_bytes() == b"\0\xff"
    finally:
        await service.close()


@pytest.mark.parametrize("payload", [b"\0\xff\x01", b"\xff\xfe\x80"])
@pytest.mark.parametrize("action", ["binary", "text", "delete", "unchanged"])
async def test_binary_input_preserves_original_bytes_through_apply_keep_and_undo(
    runtime, tmp_path, payload, action
):
    target = tmp_path / "input.bin"
    target.write_bytes(payload)
    target.chmod(0o640)
    original = target.stat()
    code = {
        "binary": "open('input.bin','wb').write(bytes([0,128,4,5]))",
        "text": "open('input.bin','w').write('text result')",
        "delete": "import os;os.unlink('input.bin')",
        "unchanged": "print(open('input.bin','rb').read().hex())",
    }[action]
    service, agent = await setup(runtime, tmp_path)
    try:
        result = await execute(agent, code, ["input.bin"])
        assert result.outcome == "completed" and result.artifacts_complete
        assert target.read_bytes() == payload
        saved = await service.manager.commands.store.files(
            "session:" + agent.session_id, result.command_id, "input"
        )
        assert saved["input.bin"].data == payload and saved["input.bin"].mode == 0o640
        proposal = await propose(agent, result, "input.bin")
        preview = proposal.proposal.preview
        assert preview.before_sha256 == hashlib.sha256(payload).hexdigest()
        assert preview.before_bytes == len(payload)
        assert preview.truncated and "binary" in preview.diff
        applied = await act(agent, proposal.proposal)
        assert applied.status == "done" and applied.receipt.changed == (action != "unchanged")
        if action == "delete":
            assert not target.exists()
        else:
            assert (
                target.read_bytes()
                == {
                    "binary": b"\0\x80\x04\x05",
                    "text": b"text result",
                    "unchanged": payload,
                }[action]
            )
        assert (await act(agent, proposal.proposal, "keep")).status == "done"
        undone = await act(agent, proposal.proposal, "undo")
        assert undone.status == "done" and target.read_bytes() == payload
        restored = target.stat()
        assert (restored.st_mode & 0o777, restored.st_uid, restored.st_gid) == (
            0o640,
            original.st_uid,
            original.st_gid,
        )
    finally:
        await service.close()


@pytest.mark.parametrize("before_exists", [False, True])
@pytest.mark.parametrize("payload", [b"\0\xff\x01", b"\xff\xfe\x80"])
async def test_binary_artifact_keep_undo_and_text_entry_remains_strict(
    runtime, tmp_path, before_exists, payload
):
    target = tmp_path / "out.bin"
    if before_exists:
        target.write_text("original text")
    service, agent = await setup(runtime, tmp_path)
    try:
        result = await execute(
            agent,
            f"open('out.bin','wb').write(bytes.fromhex('{payload.hex()}'))",
            ["out.bin"] if before_exists else [],
        )
        proposal = await propose(agent, result, "out.bin")
        assert proposal.proposal.preview.after_sha256 == hashlib.sha256(payload).hexdigest()
        assert proposal.proposal.preview.after_bytes == len(payload)
        assert proposal.proposal.preview.truncated and "binary" in proposal.proposal.preview.diff
        operation = await act(agent, proposal.proposal)
        assert operation.status == "done" and target.read_bytes() == payload
        assert (await act(agent, proposal.proposal, "keep")).status == "done"
        with pytest.raises(ServiceError) as text_entry:
            await agent.preview_file_change(
                FileChangeSpec(
                    path="out.bin",
                    operation="write",
                    expected_sha256=hashlib.sha256(payload).hexdigest(),
                    content="unsafe text",
                ),
                request_key="text-entry",
            )
        assert text_entry.value.code in {"workspace_binary", "workspace_encoding"}
        assert target.read_bytes() == payload
        undo = await act(agent, proposal.proposal, "undo")
        assert undo.status == "done"
        assert target.read_text() == "original text" if before_exists else not target.exists()
    finally:
        await service.close()


@pytest.mark.parametrize("kind", ["collision", "capacity"])
async def test_binary_artifact_new_target_and_capacity_are_not_bypassed(runtime, tmp_path, kind):
    service, agent = await setup(
        runtime, tmp_path, max_file_bytes=64 if kind == "capacity" else None
    )
    try:
        result = await execute(agent, "open('out.bin','wb').write(bytes([0,255])*64)")
        existing = tmp_path / "out.bin"
        if kind == "collision":
            existing.write_bytes(b"\0external")
        with pytest.raises(ServiceError) as rejected:
            await propose(agent, result, "out.bin")
        assert rejected.value.code == (
            "workspace_file_limit" if kind == "capacity" else "execution_destination_exists"
        )
        assert (
            existing.read_bytes() == b"\0external" if kind == "collision" else not existing.exists()
        )
    finally:
        await service.close()


@pytest.mark.parametrize("after_commit", [False, True])
@pytest.mark.parametrize("before_data", [b"old text", b"\0original", b"\xff\xfeoriginal"])
async def test_binary_artifact_receipt_failure_reconciles_original_operation(
    runtime, tmp_path, monkeypatch, after_commit, before_data
):
    target = tmp_path / "source.txt"
    target.write_bytes(before_data)
    service, agent = await setup(runtime, tmp_path)
    settle = service.workspace_edits.settle
    fired = False

    async def fail_once(*args, **kwargs):
        nonlocal fired
        if not fired:
            fired = True
            if after_commit:
                await settle(*args, **kwargs)
            raise OSError("binary receipt lost")
        return await settle(*args, **kwargs)

    try:
        result = await execute(
            agent, "open('source.txt','wb').write(bytes([0,255,2]))", ["source.txt"]
        )
        proposal = await propose(agent, result, "source.txt")
        request = WorkspaceChangeAction(
            proposal_id=proposal.proposal.proposal_id,
            action="apply",
            expected_revision=0,
            request_key="binary-apply",
        )
        monkeypatch.setattr(service.workspace_edits, "settle", fail_once)
        operation = await agent.change_file(request)
        assert target.read_bytes() == b"\0\xff\x02"
        assert operation.status == ("done" if after_commit else "uncertain")
        assert (await agent.change_file(request)).operation_id == operation.operation_id
        reconciled = await agent.reconcile_file_change(operation.operation_id)
        assert reconciled.status == "done"
        await agent.finalize_file_change(operation.operation_id)
        assert (await act(agent, proposal.proposal, "undo")).status == "done"
        assert target.read_bytes() == before_data
    finally:
        await service.close()


@pytest.mark.parametrize(
    "decision,confirmed,code",
    [("deny", True, "permission_denied"), ("ask", False, "approval_required"), ("ask", True, None)],
)
async def test_publication_requires_original_write_permission(
    runtime, tmp_path, decision, confirmed, code
):
    async def policy(definition, _context):
        return decision if definition.name == "workspace_change" else "allow"

    service, agent = await setup(runtime, tmp_path, policy=policy)
    try:
        result = await execute(agent, "open('new.txt','w').write('new')")
        proposal = await propose(agent, result, "new.txt")
        assert not (tmp_path / "new.txt").exists()
        if code:
            with pytest.raises(ServiceError) as denied:
                await act(agent, proposal.proposal, confirmed=confirmed)
            assert denied.value.code == code and not (tmp_path / "new.txt").exists()
        else:
            assert (await act(agent, proposal.proposal, confirmed=confirmed)).status == "done"
    finally:
        await service.close()


async def test_missing_write_tool_and_cross_scope_cannot_publish(runtime, tmp_path):
    service, agent = await setup(
        runtime, tmp_path, names=[name for name in NAMES if name != "workspace_change"]
    )
    try:
        result = await execute(agent, "open('new.txt','w').write('new')")
        proposal = await propose(agent, result, "new.txt")
        with pytest.raises(ServiceError) as missing:
            await act(agent, proposal.proposal)
        assert missing.value.code == "tool_not_found"
        other = AgentSession(service, (await service.spawn("publisher")).agent_id)
        with pytest.raises(ServiceError) as hidden:
            await other.propose_command_change(
                CommandProposalRequest(
                    command_id=result.command_id, path="new.txt", request_key="cross"
                )
            )
        assert hidden.value.code == "execution_not_found" and not (tmp_path / "new.txt").exists()
    finally:
        await service.close()


async def test_proposal_response_hooks_cannot_forge_success_and_same_key_conflicts(
    runtime, tmp_path
):
    service, agent = await setup(runtime, tmp_path)
    try:
        result = await execute(agent, "open('a','w').write('a');open('b','w').write('b')")
        first = await propose(agent, result, "a", key="key")
        with pytest.raises(ServiceError) as conflict:
            await propose(agent, result, "b", key="key")
        assert conflict.value.code == "workspace_idempotency_conflict"

        async def pre(definition, _context):
            if definition.name == "command_propose":
                return ShortCircuit(first.model_dump(mode="json"))

        service._handle(agent.agent_id).runtime.tools.add_global_pre(pre)
        with pytest.raises(ServiceError) as forged:
            await propose(agent, result, "b")
        assert forged.value.code == "execution_receipt_unverified"
        assert len(await agent.file_changes()) == 1
    finally:
        await service.close()


async def test_corrupt_source_or_removed_link_blocks_file_action(runtime, tmp_path):
    service, agent = await setup(runtime, tmp_path)
    try:
        result = await execute(agent, "open('new.txt','w').write('new')")
        proposal = await propose(agent, result, "new.txt")
        async with runtime.repository.database.sessions.begin() as sql:
            await sql.execute(
                update(CommandFileRow)
                .where(
                    CommandFileRow.command_id == result.command_id,
                    CommandFileRow.kind == "artifact",
                )
                .values(content=b"corrupt")
            )
        with pytest.raises(ServiceError) as corrupt:
            await act(agent, proposal.proposal)
        assert corrupt.value.code == "execution_source_corrupt"
        async with runtime.repository.database.sessions.begin() as sql:
            await sql.execute(
                delete(CommandProposalRow).where(
                    CommandProposalRow.proposal_id == proposal.proposal.proposal_id
                )
            )
        with pytest.raises(ServiceError) as unlinked:
            await act(agent, proposal.proposal)
        assert (
            unlinked.value.code == "execution_proposal_source"
            and not (tmp_path / "new.txt").exists()
        )
    finally:
        await service.close()


async def test_same_proposal_race_and_nonempty_migration_refuses_loss(runtime, tmp_path):
    service, agent = await setup(runtime, tmp_path)
    try:
        result = await execute(agent, "open('new.txt','w').write('new')")
        first, second = await asyncio.gather(
            propose(agent, result, "new.txt"), propose(agent, result, "new.txt")
        )
        assert first.proposal.proposal_id == second.proposal.proposal_id
        assert len(await agent.file_changes()) == 1
        with pytest.raises(RuntimeError, match="命令产物提案非空"):
            async with runtime.repository.database.engine.begin() as sql:
                await sql.run_sync(
                    lambda conn: command.downgrade(
                        migration_config(conn), "0030_command_executions"
                    )
                )
        async with runtime.repository.database.engine.connect() as sql:
            assert (
                await sql.execute(text("SELECT version_num FROM alembic_version"))
            ).scalar_one() == "0057_missing_directory_rollbacks"
            assert not (await sql.execute(text("PRAGMA foreign_key_check"))).all()
    finally:
        await service.close()


async def test_native_batch_and_checkpoint_use_original_file_effects(runtime, tmp_path):
    (tmp_path / "in.txt").write_text("old")
    service, agent = await setup(runtime, tmp_path)
    try:
        result = await execute(
            agent,
            "open('in.txt','w').write('new');open('added.txt','w').write('added')",
            ["in.txt"],
        )
        proposals = [await propose(agent, result, path) for path in ["in.txt", "added.txt"]]
        for item in proposals:
            assert (await act(agent, item.proposal)).status == "done"
        page = await agent.file_change_list()
        assert len(page.items) == 2
        batch = await agent.start_file_change_batch(
            NativeBatchRequest(
                action="keep",
                snapshot_revision=page.snapshot_revision,
                request_key="batch",
                targets=[
                    NativeBatchTarget(proposal_id=item.proposal_id, expected_revision=item.revision)
                    for item in page.items
                ],
            )
        )
        batch = await agent.wait_file_change_batch(batch.batch_id, timeout_seconds=30)
        assert batch.status == "done"
        checkpoint = await agent.create_file_checkpoint(
            WorkspaceCheckpointCreate(paths=["in.txt", "added.txt"], request_key="after-command")
        )
        checkpoint = await agent.wait_file_checkpoint(checkpoint.checkpoint_id, timeout_seconds=30)
        assert checkpoint.status == "available"
        assert (tmp_path / "in.txt").read_text() == "new" and (
            tmp_path / "added.txt"
        ).read_text() == "added"
    finally:
        await service.close()


async def test_cancelled_preparation_and_revoked_workspace_do_not_publish(
    runtime, tmp_path, monkeypatch
):
    service, agent = await setup(runtime, tmp_path)
    entered, release = Event(), Event()
    original = WorkspaceWriter.prepare_artifact

    def blocked(self, *args, **kwargs):
        entered.set()
        assert release.wait(5)
        return original(self, *args, **kwargs)

    try:
        result = await execute(agent, "open('new.txt','w').write('new')")
        monkeypatch.setattr(WorkspaceWriter, "prepare_artifact", blocked)
        task = asyncio.create_task(propose(agent, result, "new.txt"))
        assert await asyncio.to_thread(entered.wait, 5)
        task.cancel()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert not await agent.file_changes() and not (tmp_path / "new.txt").exists()
        monkeypatch.setattr(WorkspaceWriter, "prepare_artifact", original)
        proposal = await propose(agent, result, "new.txt")
        service.disable_workspace("files")
        with pytest.raises(ServiceError) as revoked:
            await act(agent, proposal.proposal)
        assert revoked.value.code == "workspace_unavailable" and not (tmp_path / "new.txt").exists()
    finally:
        release.set()
        await service.close()


async def test_source_is_checked_again_inside_publication_boundary(runtime, tmp_path, monkeypatch):
    service, agent = await setup(runtime, tmp_path)
    entered, release = Event(), Event()
    original = WorkspaceWriter.apply

    def blocked(self, *args, **kwargs):
        entered.set()
        assert release.wait(5)
        return original(self, *args, **kwargs)

    try:
        result = await execute(agent, "open('new.txt','w').write('new')")
        proposal = await propose(agent, result, "new.txt")
        monkeypatch.setattr(WorkspaceWriter, "apply", blocked)
        task = asyncio.create_task(act(agent, proposal.proposal))
        assert await asyncio.to_thread(entered.wait, 5)
        async with runtime.repository.database.sessions.begin() as sql:
            await sql.execute(
                update(CommandFileRow)
                .where(
                    CommandFileRow.command_id == result.command_id,
                    CommandFileRow.kind == "artifact",
                )
                .values(content=b"changed")
            )
        release.set()
        rejected = await task
        assert (
            rejected.status == "rejected"
            and rejected.receipt.error_code == "execution_source_corrupt"
        )
        assert not (tmp_path / "new.txt").exists()
    finally:
        release.set()
        await service.close()


@pytest.mark.parametrize("after_commit", [False, True])
async def test_artifact_receipt_failure_preserves_original_effect_and_reconciles(
    runtime, tmp_path, monkeypatch, after_commit
):
    file = tmp_path / "in.txt"
    file.write_text("before")
    service, agent = await setup(runtime, tmp_path)
    original = service.workspace_edits.settle
    failed = False

    async def fault(*args, **kwargs):
        nonlocal failed
        if not failed:
            failed = True
            if after_commit:
                await original(*args, **kwargs)
            raise OSError("lost receipt")
        return await original(*args, **kwargs)

    try:
        result = await execute(agent, "open('in.txt','w').write('after')", ["in.txt"])
        proposal = await propose(agent, result, "in.txt")
        request = WorkspaceChangeAction(
            proposal_id=proposal.proposal.proposal_id,
            expected_revision=0,
            action="apply",
            request_key="apply",
        )
        monkeypatch.setattr(service.workspace_edits, "settle", fault)
        operation = await agent.change_file(request)
        assert operation.status == ("done" if after_commit else "uncertain")
        assert file.read_text() == "after"
        assert (await agent.change_file(request)).operation_id == operation.operation_id
        verified = await agent.reconcile_file_change(operation.operation_id)
        assert verified.status == "done"
        await agent.finalize_file_change(operation.operation_id)
        assert (await act(agent, proposal.proposal, "undo")).status == "done"
        assert file.read_text() == "before"
    finally:
        await service.close()


async def test_model_command_proposal_and_file_publish_form_one_audited_run(runtime, tmp_path):
    import json

    class Provider:
        name = "artifact-model"
        step = 0

        async def stream(self, messages, tools):
            self.step += 1
            if self.step == 1:
                yield ModelToolCall(
                    id="execute",
                    name="command_execute",
                    arguments=AgentCommandRequest(
                        request_key="cmd",
                        command=CommandSpec(
                            argv=[
                                "/usr/bin/python3",
                                "-c",
                                "open('new.txt','w').write('model artifact')",
                            ]
                        ),
                    ).model_dump(mode="json"),
                )
            elif self.step == 2:
                result = json.loads(messages[-1].content)
                yield ModelToolCall(
                    id="propose",
                    name="command_propose",
                    arguments=CommandProposalRequest(
                        command_id=result["command_id"], path="new.txt", request_key="proposal"
                    ).model_dump(mode="json"),
                )
            elif self.step == 3:
                result = json.loads(messages[-1].content)["proposal"]
                yield ModelToolCall(
                    id="publish",
                    name="workspace_change",
                    arguments=WorkspaceChangeAction(
                        proposal_id=result["proposal_id"],
                        expected_revision=result["revision"],
                        action="apply",
                        request_key="publish",
                    ).model_dump(mode="json"),
                )
            else:
                assert json.loads(messages[-1].content)["status"] == "done"
                yield ModelText(text="published")

    runtime.provider = Provider()
    service, agent = await setup(runtime, tmp_path)
    try:
        result = await agent.execute("generate and publish", idempotency_key="turn")
        assert result.status == "completed" and result.text == "published"
        assert (tmp_path / "new.txt").read_text() == "model artifact"
        proposal = (await agent.file_changes())[0]
        operation = (await agent.file_change_history(proposal.proposal_id))[0]
        assert operation.run_id == result.run_id and operation.call_id == "publish"
        async with runtime.repository.database.sessions() as sql:
            from sqlalchemy import select

            from krim_agent.persistence.database import ToolAuditRow

            audits = list(
                await sql.scalars(select(ToolAuditRow).where(ToolAuditRow.run_id == result.run_id))
            )
        assert {item.payload["name"] for item in audits} >= {
            "command_execute",
            "command_propose",
            "workspace_change",
        }
    finally:
        await service.close()


async def test_propose_has_its_own_permission_and_readonly_profile_cannot_acquire_it(
    runtime, tmp_path
):
    async def deny(definition, _context):
        return "deny" if definition.name == "command_propose" else "allow"

    service, agent = await setup(runtime, tmp_path, policy=deny)
    try:
        result = await execute(agent, "open('new.txt','w').write('new')")
        with pytest.raises(ServiceError) as denied:
            await propose(agent, result, "new.txt")
        assert denied.value.code == "permission_denied" and not await agent.file_changes()
        service.register_workspace("readonly", tmp_path, version="1", writable=False)
        with pytest.raises(ServiceError) as readonly:
            await service.profile_put(
                AgentProfileUpdate(
                    expected_revision=0,
                    config=AgentProfile(
                        name="readonly",
                        description="cannot publish",
                        workspace_key="readonly",
                        execution_key="sandbox",
                        tool_names=["command_propose"],
                    ),
                )
            )
        assert readonly.value.code == "workspace_read_only"
    finally:
        await service.close()


async def test_private_artifact_spec_is_rejected_by_normal_preview_store(runtime, tmp_path):
    service, agent = await setup(runtime, tmp_path)
    try:
        result = await execute(agent, "open('new.txt','w').write('new')")
        proposal = await propose(agent, result, "new.txt")
        scope = "session:" + agent.session_id
        original = await service.workspace_edits.get(scope, proposal.proposal.proposal_id)
        binding = service._handle(agent.agent_id).definition.workspace
        with pytest.raises(ServiceError) as private:
            await service.workspace_edits.preview(
                scope, binding.key, binding.fingerprint, "forged", original.prepared
            )
        assert private.value.code == "workspace_snapshot_invalid"
        assert not (tmp_path / "new.txt").exists()
    finally:
        await service.close()
