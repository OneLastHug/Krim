"""真实隔离命令/原Registry树→完整当前观察→只读来源核实；损坏拒绝。"""

import copy
import shutil
import sys
from dataclasses import replace

import pytest
from pydantic import ValidationError
from sqlalchemy import delete, select
from test_command_publication import execute, setup
from test_workspace_tree_fences import publish, tree

from krim_agent.execution.directory_publication import CommandDirectoryDependencyRequest
from krim_agent.persistence.command_directory_dependencies import CommandDirectoryDependencyStore
from krim_agent.persistence.command_models import CommandFileRow
from krim_agent.persistence.completed_tree_origins import CompletedTreeOriginVerifier
from krim_agent.persistence.database import (
    WorkspaceDirectoryAttemptRow,
    WorkspaceFileEffectRow,
    WorkspaceTreePlanRow,
)
from krim_agent.persistence.workspace_tree_plans import WorkspaceTreePlanStore
from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace_directory import ArtifactDirectorySpec, WorkspaceDirectoryWriter
from krim_agent.workspace_tree import TreePlanRequest
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshotReader
from krim_agent.workspace_writer import WorkspaceWriter

pytestmark = pytest.mark.skipif(
    not sys.platform.startswith("linux") or shutil.which("bwrap") is None,
    reason="真实Linux隔离命令/原Registry树",
)


async def complete(runtime, root):
    service, agent, scope, store, execution = await tree(runtime, root)
    outputs = [
        await publish(service, agent, scope, store, execution, position) for position in range(3)
    ]
    view = await store.finish(scope, execution.view.plan_id, execution.token)
    assert view.status == "done"
    binding = service._handle(agent.agent_id).definition.workspace
    snapshot = WorkspaceTreeSnapshotReader(binding.require()).capture(view.root_path)
    return service, agent, scope, store, view, binding, snapshot, outputs


async def test_complete_current_binary_snapshot_preserves_original_operations_and_cold_zero_replay(
    runtime, tmp_path, monkeypatch
):
    service, agent, scope, store, plan, binding, snapshot, outputs = await complete(
        runtime, tmp_path
    )
    try:
        verifier = CompletedTreeOriginVerifier(store)
        value = await verifier.verify(scope, binding, plan.plan_id, snapshot)
        assert value.snapshot is snapshot and value.snapshot.entries[-1].file.data == b"\0\xff"
        assert value.operation_ids == tuple(item["operation_id"] for item in outputs)
        assert value.view.plan_id == plan.plan_id and value.view.command_id == plan.command_id
        assert (
            value.view.source_hash == plan.source_hash
            and value.view.snapshot_revision == plan.snapshot_revision
        )
        assert value.view.tree_fingerprint == snapshot.fingerprint
        assert value.view.entry_count == 3 and value.view.byte_size == 2
        assert value.view.outcome == "completed" and not value.view.accept_failed
        with pytest.raises(ValidationError):
            value.view.tree_fingerprint = "0" * 64
        assert type(value.snapshot.entries[-1].file.data) is bytes

        def forbidden(*_args, **_kwargs):
            pytest.fail("来源核实不可重新命令/Producer/Writer/读取改变原观察")

        monkeypatch.setattr(binding.workspace, "read", forbidden)
        monkeypatch.setattr(WorkspaceTreeSnapshotReader, "capture", forbidden)
        monkeypatch.setattr(WorkspaceWriter, "apply", forbidden)
        monkeypatch.setattr(WorkspaceDirectoryWriter, "apply", forbidden)
        monkeypatch.setattr(service.workspace_trees, "execute", forbidden)
        execution = service._handle(agent.agent_id).definition.execution
        monkeypatch.setattr(execution.runner, "start", forbidden)
        assert await verifier.verify(scope, binding, plan.plan_id, snapshot) == value
        async with store.database.sessions.begin() as sql:
            assert (
                await verifier.verify_in_transaction(sql, scope, binding, plan.plan_id, snapshot)
                == value
            )
    finally:
        await service.close()


@pytest.mark.parametrize(
    "fault",
    [
        "bytes",
        "file-mode",
        "directory-mode",
        "directory-inode",
        "extra",
        "missing",
        "root-parent",
        "workspace",
        "scope",
    ],
)
async def test_current_full_tree_or_scope_changes_never_gain_command_origin(
    runtime, tmp_path, fault
):
    service, agent, scope, store, plan, binding, snapshot, outputs = await complete(
        runtime, tmp_path
    )
    try:
        if fault == "bytes":
            (tmp_path / "tree/sub/data.bin").write_bytes(b"changed")
        elif fault == "file-mode":
            (tmp_path / "tree/sub/data.bin").chmod(0o777)
        elif fault == "directory-mode":
            (tmp_path / "tree/sub").chmod(0o755)
        elif fault == "directory-inode":
            (tmp_path / "tree/sub").rename(tmp_path / "retained-sub")
            (tmp_path / "tree/sub").mkdir(mode=0o700)
            shutil.copyfile(tmp_path / "retained-sub/data.bin", tmp_path / "tree/sub/data.bin")
        elif fault == "extra":
            (tmp_path / "tree/extra.txt").write_text("extra")
        elif fault == "missing":
            (tmp_path / "tree/sub/data.bin").unlink()
        snapshot = WorkspaceTreeSnapshotReader(binding.require()).capture(plan.root_path)
        if fault == "root-parent":
            snapshot = replace(
                snapshot,
                parent_identity=(snapshot.parent_identity[0], snapshot.parent_identity[1] + 1),
            )
        elif fault == "workspace":
            snapshot = replace(snapshot, workspace_fingerprint="0" * 64)
        elif fault == "scope":
            other = await service.spawn("default")
            scope = "session:" + other.session_id
        with pytest.raises(ServiceError):
            await CompletedTreeOriginVerifier(store).verify(scope, binding, plan.plan_id, snapshot)
    finally:
        await service.close()


@pytest.mark.parametrize(
    "fault",
    [
        "artifact-bytes",
        "directory-observation",
        "missing-file-effect",
        "file-observation",
        "frozen-source",
    ],
)
async def test_source_blob_or_original_observation_damage_fails_closed(runtime, tmp_path, fault):
    service, agent, scope, store, plan, binding, snapshot, outputs = await complete(
        runtime, tmp_path
    )
    try:
        async with store.database.sessions.begin() as sql:
            if fault == "artifact-bytes":
                row = await sql.get(
                    CommandFileRow, (plan.command_id, "artifact", "tree/sub/data.bin")
                )
                row.content = b"changed"
            elif fault == "directory-observation":
                row = await sql.get(WorkspaceDirectoryAttemptRow, outputs[0]["operation_id"])
                row.observed_after = {**copy.deepcopy(row.observed_after), "inode": 0}
            elif fault == "missing-file-effect":
                await sql.execute(delete(WorkspaceFileEffectRow))
            elif fault == "file-observation":
                row = await sql.scalar(select(WorkspaceFileEffectRow))
                row.observed_after = {
                    **copy.deepcopy(row.observed_after),
                    "gid": row.observed_after["gid"] + 1,
                }
            else:
                row = await sql.get(WorkspaceTreePlanRow, plan.plan_id)
                row.source_hash = "0" * 64
        with pytest.raises(ServiceError):
            await CompletedTreeOriginVerifier(store).verify(scope, binding, plan.plan_id, snapshot)
    finally:
        await service.close()


@pytest.mark.parametrize("stage", ["planned", "partial"])
async def test_regular_existing_or_only_partially_published_tree_is_not_a_completed_origin(
    runtime, tmp_path, stage
):
    service, agent, scope, store, execution = await tree(
        runtime, tmp_path, claim=stage == "partial"
    )
    try:
        if stage == "partial":
            await publish(service, agent, scope, store, execution, 0)
            view = await store.finish(scope, execution.view.plan_id, execution.token)
            assert view.status == "partial"
        else:
            view = execution
            (tmp_path / "tree").mkdir()
        binding = service._handle(agent.agent_id).definition.workspace
        snapshot = WorkspaceTreeSnapshotReader(binding.require()).capture("tree")
        with pytest.raises(ServiceError) as incomplete:
            await CompletedTreeOriginVerifier(store).verify(scope, binding, view.plan_id, snapshot)
        assert incomplete.value.code == "workspace_tree_origin_incomplete"
    finally:
        await service.close()


async def test_file_inode_is_frozen_at_current_observation_without_inventing_old_identity(
    runtime, tmp_path
):
    service, agent, scope, store, plan, binding, snapshot, outputs = await complete(
        runtime, tmp_path
    )
    try:
        original = snapshot.entries[-1].file
        (tmp_path / "tree/sub/data.bin").rename(tmp_path / "retained.bin")
        (tmp_path / "tree/sub/data.bin").write_bytes(original.data)
        (tmp_path / "tree/sub/data.bin").chmod(original.mode)
        current = WorkspaceTreeSnapshotReader(binding.require()).capture("tree")
        assert current.entries[-1].file.identity != original.identity
        proof = await CompletedTreeOriginVerifier(store).verify(
            scope, binding, plan.plan_id, current
        )
        assert proof.snapshot == current and proof.snapshot != snapshot
        assert proof.view.tree_fingerprint == current.fingerprint
        assert proof.operation_ids == tuple(item["operation_id"] for item in outputs)
        # 这里证明当前内容/属性的原来源；完整inode条件从此观察开始，尚未持久封存。
    finally:
        await service.close()


async def test_real_failed_command_explicitly_accepted_tree_keeps_actual_outcome(runtime, tmp_path):
    names = [
        "command_execute",
        "command_get",
        "command_propose",
        "directory_source_preview",
        "directory_change",
        "workspace_change",
        "workspace_reconcile",
        "directory_reconcile",
    ]
    service, agent = await setup(runtime, tmp_path, names=names)
    try:
        command = await execute(
            agent,
            "import os,sys;os.makedirs('tree/sub');os.chmod('tree',0o750);"
            "os.chmod('tree/sub',0o700);"
            "open('tree/sub/data.bin','wb').write(bytes([0,255]));sys.exit(3)",
        )
        assert command.outcome == "failed"
        scope = "session:" + agent.session_id
        binding = service._handle(agent.agent_id).definition.workspace
        source = await CommandDirectoryDependencyStore(service.workspace_edits).plan(
            scope,
            CommandDirectoryDependencyRequest(command_id=command.command_id, accept_failed=True),
        )
        prepared = WorkspaceDirectoryWriter(binding.require()).prepare_artifact(
            ArtifactDirectorySpec(
                path="tree",
                operation="create",
                mode=0o750,
                command_id=command.command_id,
                source_hash=source.source_hash,
            )
        )
        store = WorkspaceTreePlanStore(
            service.workspace_edits, service.manager.directory_changes.store
        )
        plan, _ = await store.reserve(
            scope,
            binding,
            TreePlanRequest(
                command_id=command.command_id,
                source_hash=source.source_hash,
                root_path="tree",
                request_key="accepted-failed",
                accept_failed=True,
            ),
            prepared,
        )
        execution = await store.claim(scope, plan.plan_id)
        for position in range(plan.step_count):
            await publish(service, agent, scope, store, execution, position)
        finished = await store.finish(scope, plan.plan_id, execution.token)
        assert finished.status == "done"
        snapshot = WorkspaceTreeSnapshotReader(binding.require()).capture("tree")
        proof = await CompletedTreeOriginVerifier(store).verify(
            scope, binding, plan.plan_id, snapshot
        )
        assert proof.view.outcome == "failed" and proof.view.accept_failed
        assert proof.snapshot.entries[-1].file.data == b"\0\xff"
    finally:
        await service.close()
