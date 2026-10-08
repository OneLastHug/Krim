"""真实效果顺序、冻结保留见证、断链边界及无伪造历史迁移。"""

import hashlib
import stat
import time
from dataclasses import dataclass

import pytest
from alembic import command
from sqlalchemy import func, select, text, update
from sqlalchemy.exc import IntegrityError

from krim_agent.persistence import Database, KrimRepository
from krim_agent.persistence.database import (
    WorkspaceEditAttemptRow,
    WorkspaceEditProposalRow,
    WorkspaceFileEffectRow,
    WorkspaceFileLockRow,
)
from krim_agent.persistence.migration import migration_config
from krim_agent.persistence.workspace_edits import WorkspaceEditStore
from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace import Workspace
from krim_agent.workspace_change import (
    FileChangeReceipt,
    FileChangeSpec,
    WorkspaceChangeAction,
    WorkspaceFileState,
)
from krim_agent.workspace_writer import WorkspaceWriter


@dataclass
class Ledger:
    database: Database
    store: WorkspaceEditStore
    workspace: Workspace
    scope: str
    count: int = 0

    def state(self):
        path = self.workspace.root / "file.txt"
        if not path.exists():
            return WorkspaceFileState(sha256=None, mode=None, uid=None, gid=None)
        info = path.stat()
        return WorkspaceFileState(
            sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
            mode=stat.S_IMODE(info.st_mode),
            uid=info.st_uid,
            gid=info.st_gid,
        )

    async def reserve(self, proposal, prepared, action="apply", *, scope=None, resource=None):
        scope = scope or self.scope
        current = await self.store.get(scope, proposal.view.proposal_id)
        self.count += 1
        return (
            await self.store.reserve(
                scope,
                WorkspaceChangeAction(
                    proposal_id=current.view.proposal_id,
                    action=action,
                    expected_revision=current.view.revision,
                    request_key=f"act-{self.count}",
                ),
                resource or self.workspace.fingerprint,
                prepared,
            )
        )[0]

    async def apply(self, value, *, scope=None, resource=None, observed=True):
        scope = scope or self.scope
        resource = resource or self.workspace.fingerprint
        self.count += 1
        prepared = WorkspaceWriter(self.workspace).prepare(
            FileChangeSpec(
                path="file.txt",
                operation="delete" if value is None else "write",
                expected_sha256=self.state().sha256,
                content=value,
            )
        )
        proposal, _ = await self.store.preview(
            scope, "files", resource, f"p-{self.count}", prepared
        )
        attempted = await self.reserve(proposal, prepared, scope=scope, resource=resource)
        effect = WorkspaceWriter(self.workspace).apply(prepared)
        assert effect.receipt.status == "done"
        await self.store.settle(
            scope,
            attempted.view.operation_id,
            effect.receipt,
            effect.recovery,
            observed_after=self.state() if observed else None,
        )
        if effect.recovery is not None:
            WorkspaceWriter(self.workspace).finalize(prepared, effect.recovery)
        await self.store.mark_cleaned(scope, attempted.view.operation_id)
        return proposal, prepared, attempted

    async def undo(self, proposal, original, *, observed=True):
        prepared = WorkspaceWriter(self.workspace).prepare_restore(original)
        attempt = await self.reserve(proposal, prepared, "undo")
        effect = WorkspaceWriter(self.workspace).apply(prepared)
        assert effect.receipt.status == "done"
        await self.store.settle(
            self.scope,
            attempt.view.operation_id,
            effect.receipt,
            effect.recovery,
            observed_after=self.state() if observed else None,
        )
        if effect.recovery is not None:
            WorkspaceWriter(self.workspace).finalize(prepared, effect.recovery)
        await self.store.mark_cleaned(self.scope, attempt.view.operation_id)
        return attempt


@pytest.fixture
async def ledger(tmp_path):
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'effects.sqlite'}")
    await database.initialize()
    topic = await KrimRepository(database).create_topic("连续修改")
    root = tmp_path / "files"
    root.mkdir()
    (root / "file.txt").write_text("A", encoding="utf-8")
    workspace = Workspace(root, writable=True)
    try:
        yield Ledger(database, WorkspaceEditStore(database), workspace, "topic:" + topic.id)
    finally:
        workspace.close()
        await database.close()


def kept_receipt(prepared):
    return FileChangeReceipt(
        path=prepared.spec.path,
        status="done",
        changed=False,
        before_sha256=prepared.before_sha256,
        after_sha256=prepared.after_sha256,
    )


async def test_frozen_successor_proof_reopens_and_keeps_original_receipt(ledger):
    first, first_prepared, first_attempt = await ledger.apply("B")
    second, second_prepared, second_attempt = await ledger.apply("C")
    keep = await ledger.reserve(first, first_prepared, "keep")
    target, observed = await ledger.store.keep_target(ledger.scope, keep.view.operation_id)
    assert target == second_prepared and observed == ledger.state()
    async with ledger.database.sessions() as session:
        row = await session.get(WorkspaceEditAttemptRow, keep.view.operation_id)
        assert row.keep_witness["nodes"][0]["revision"] == keep.view.expected_revision
        assert all(
            "content" not in node and "prepared" not in node for node in row.keep_witness["nodes"]
        )
        # 见证仅保存引用；原keep的私有快照仍是原提案，不替换成后继内容。
        assert row.after_content == b"B"
    await ledger.store.settle(ledger.scope, keep.view.operation_id, kept_receipt(first_prepared))
    await ledger.apply("D")
    reopened_db = Database(str(ledger.database.engine.url))
    await reopened_db.initialize()
    reopened = WorkspaceEditStore(reopened_db)
    try:
        target, state = await reopened.keep_target(ledger.scope, keep.view.operation_id)
        assert target == second_prepared and state.sha256 == second_prepared.after_sha256
        proof = await reopened.operation_proof(ledger.scope, keep.view.operation_id)
        assert proof.kind == "successor_chain" and proof.successor_count == 1
        assert proof.status == "done" and proof.tip_proposal_id == second.view.proposal_id
        assert proof.tip_operation_id == second_attempt.view.operation_id
        assert proof.tip_after_sha256 == second_prepared.after_sha256 and proof.effect_sequence == 2
        assert (
            "observed_after" not in proof.model_dump_json()
            and "snapshot" not in proof.model_dump_json()
        )
        receipt = (await reopened.operation(ledger.scope, keep.view.operation_id)).view.receipt
        assert receipt.after_sha256 == first_prepared.after_sha256 and not receipt.changed
        assert (
            await reopened.operation_proof(ledger.scope, first_attempt.view.operation_id)
        ).kind == "not_applicable"
        other_topic = await KrimRepository(ledger.database).create_topic("其他作用域")
        with pytest.raises(ServiceError, match="修改操作") as hidden:
            await reopened.operation_proof("topic:" + other_topic.id, keep.view.operation_id)
        assert hidden.value.code == "workspace_operation_not_found"
    finally:
        await reopened_db.close()


async def test_reverse_undo_replays_events_before_filtering_current_undone(ledger):
    first, a, _ = await ledger.apply("B")
    second, b, _ = await ledger.apply("C")
    third, c, _ = await ledger.apply("D")
    await ledger.undo(third, c)
    keep = await ledger.reserve(first, a, "keep")
    target, observed = await ledger.store.keep_target(ledger.scope, keep.view.operation_id)
    assert target == b and observed == ledger.state()
    proof = await ledger.store.operation_proof(ledger.scope, keep.view.operation_id)
    assert proof.tip_proposal_id == second.view.proposal_id and proof.effect_sequence == 2


async def test_interrupted_keep_retains_frozen_target_without_claiming_file_effect(ledger):
    first, a, _ = await ledger.apply("B")
    _second, b, _ = await ledger.apply("C")
    keep = await ledger.reserve(first, a, "keep")
    async with ledger.database.sessions.begin() as session:
        await session.execute(
            update(WorkspaceEditAttemptRow)
            .where(WorkspaceEditAttemptRow.operation_id == keep.view.operation_id)
            .values(lease_expires_at=time.time() - 1)
        )
    assert await ledger.store.recover_interrupted() == 1
    recovered = await ledger.store.operation(ledger.scope, keep.view.operation_id)
    assert recovered.view.status == "uncertain" and not recovered.view.receipt.changed
    reopened_db = Database(str(ledger.database.engine.url))
    await reopened_db.initialize()
    reopened = WorkspaceEditStore(reopened_db)
    try:
        target, state = await reopened.keep_target(ledger.scope, keep.view.operation_id)
        assert target == b and state == ledger.state()
        await reopened.settle(ledger.scope, keep.view.operation_id, kept_receipt(a), resolve=True)
        proof = await reopened.operation_proof(ledger.scope, keep.view.operation_id)
        assert proof.status == "done" and proof.kind == "successor_chain"
        async with reopened_db.sessions() as session:
            assert (
                await session.scalar(select(func.count()).select_from(WorkspaceFileEffectRow)) == 2
            )
    finally:
        await reopened_db.close()


async def test_non_top_undo_breaks_chain_even_when_content_repeats(ledger):
    first, a, _ = await ledger.apply("B")
    second, b, _ = await ledger.apply("A")
    await ledger.apply("B")
    await ledger.undo(first, a)
    await ledger.apply("C")
    keep = await ledger.reserve(second, b, "keep")
    target, observed = await ledger.store.keep_target(ledger.scope, keep.view.operation_id)
    assert target == b and observed is None
    assert (
        await ledger.store.operation_proof(ledger.scope, keep.view.operation_id)
    ).kind == "exact_after"


@pytest.mark.parametrize(
    "boundary", ["scope", "resource", "content", "mode", "uid", "gid", "unobserved"]
)
async def test_each_unproven_boundary_prevents_earlier_keep_witness(ledger, boundary):
    first, prepared, _ = await ledger.apply("B")
    kwargs = {}
    if boundary == "scope":
        other = await KrimRepository(ledger.database).create_topic("其他会话")
        kwargs["scope"] = "topic:" + other.id
    elif boundary == "resource":
        kwargs["resource"] = "0" * 64
    elif boundary == "unobserved":
        kwargs["observed"] = False
    elif boundary == "content":
        (ledger.workspace.root / "file.txt").write_text("external", encoding="utf-8")
    elif boundary == "mode":
        (ledger.workspace.root / "file.txt").chmod(0o600)
    elif boundary in {"uid", "gid"}:
        # 仓储层独立验证不把相同正文误当作相同属主；不依赖测试进程有chown权限。
        async with ledger.database.sessions.begin() as session:
            effect = await session.scalar(select(WorkspaceFileEffectRow))
            effect.observed_after = {**effect.observed_after, boundary: 12345}
    await ledger.apply("C", **kwargs)
    if boundary in {"scope", "resource"}:
        # 回到原作用域/资源也不能越过别人的已完成效果，哪怕内容能接上。
        await ledger.apply("D")
    keep = await ledger.reserve(first, prepared, "keep")
    assert (await ledger.store.keep_target(ledger.scope, keep.view.operation_id))[1] is None
    assert (
        await ledger.store.operation_proof(ledger.scope, keep.view.operation_id)
    ).successor_count == 0


async def test_create_delete_recreate_tracks_existence_without_fake_uid(ledger):
    (ledger.workspace.root / "file.txt").unlink()
    first, prepared, _ = await ledger.apply("B")
    await ledger.apply(None)
    third, _, _ = await ledger.apply("C")
    keep = await ledger.reserve(first, prepared, "keep")
    proof = await ledger.store.operation_proof(ledger.scope, keep.view.operation_id)
    assert proof.kind == "successor_chain" and proof.successor_count == 2
    assert proof.tip_proposal_id == third.view.proposal_id
    assert (await ledger.store.keep_target(ledger.scope, keep.view.operation_id))[
        1
    ] == ledger.state()


async def test_noop_special_mode_then_restore_cannot_preserve_old_attributes(ledger):
    first, a, _ = await ledger.apply("B")
    (ledger.workspace.root / "file.txt").chmod(0o1644)
    # 在特殊位存在时先建立自己的连续段；前一条正确因属性断开。
    second, b, _ = await ledger.apply("B")
    third, c, _ = await ledger.apply("C")
    await ledger.undo(third, c)
    assert ledger.state().mode == 0o644
    await ledger.apply("D")
    keep = await ledger.reserve(second, b, "keep")
    assert (await ledger.store.keep_target(ledger.scope, keep.view.operation_id))[1] is None
    assert (await ledger.store.get(ledger.scope, first.view.proposal_id)).prepared == a


async def test_uncertain_resolution_records_one_effect_and_replay_never_adds(ledger):
    prepared = WorkspaceWriter(ledger.workspace).prepare(
        FileChangeSpec(
            path="file.txt", operation="write", expected_sha256=ledger.state().sha256, content="B"
        )
    )
    proposal, _ = await ledger.store.preview(
        ledger.scope, "files", ledger.workspace.fingerprint, "p", prepared
    )
    attempt = await ledger.reserve(proposal, prepared)
    effect = WorkspaceWriter(ledger.workspace).apply(prepared)
    uncertain = effect.receipt.model_copy(
        update={
            "status": "uncertain",
            "recovery_required": True,
            "error_code": "injected_uncertain",
        }
    )
    await ledger.store.settle(ledger.scope, attempt.view.operation_id, uncertain, effect.recovery)
    async with ledger.database.sessions() as session:
        assert await session.scalar(select(func.count()).select_from(WorkspaceFileEffectRow)) == 0
    settled = await ledger.store.settle(
        ledger.scope,
        attempt.view.operation_id,
        effect.receipt,
        effect.recovery,
        resolve=True,
        observed_after=ledger.state(),
    )
    await ledger.store.settle(
        ledger.scope,
        attempt.view.operation_id,
        settled.view.receipt,
        effect.recovery,
        resolve=True,
        observed_after=ledger.state(),
    )
    async with ledger.database.sessions() as session:
        rows = (await session.scalars(select(WorkspaceFileEffectRow))).all()
        assert len(rows) == 1 and rows[0].operation_id == attempt.view.operation_id
        assert (
            rows[0].effect_sequence == 1 and rows[0].observed_after == ledger.state().model_dump()
        )


async def test_effect_insert_failure_rolls_back_receipt_and_preserves_uncertain_lock(ledger):
    prepared = WorkspaceWriter(ledger.workspace).prepare(
        FileChangeSpec(
            path="file.txt", operation="write", expected_sha256=ledger.state().sha256, content="B"
        )
    )
    proposal, _ = await ledger.store.preview(
        ledger.scope, "files", ledger.workspace.fingerprint, "p", prepared
    )
    attempt = await ledger.reserve(proposal, prepared)
    effect = WorkspaceWriter(ledger.workspace).apply(prepared)
    async with ledger.database.engine.begin() as connection:
        await connection.execute(
            text(
                "CREATE TRIGGER fail_effect BEFORE INSERT ON workspace_file_effects "
                "BEGIN SELECT RAISE(ABORT, 'injected effect failure'); END"
            )
        )
    with pytest.raises(IntegrityError, match="injected effect failure"):
        await ledger.store.settle(
            ledger.scope,
            attempt.view.operation_id,
            effect.receipt,
            effect.recovery,
            observed_after=ledger.state(),
        )
    assert (
        await ledger.store.operation(ledger.scope, attempt.view.operation_id)
    ).view.status == "running"
    async with ledger.database.sessions.begin() as session:
        assert await session.scalar(select(func.count()).select_from(WorkspaceFileLockRow)) == 1
        await session.execute(text("DROP TRIGGER fail_effect"))
        await session.execute(
            update(WorkspaceEditAttemptRow).values(lease_expires_at=time.time() - 1)
        )
    assert await ledger.store.recover_interrupted() == 1
    await ledger.store.settle(
        ledger.scope,
        attempt.view.operation_id,
        effect.receipt,
        effect.recovery,
        resolve=True,
        observed_after=ledger.state(),
    )
    async with ledger.database.sessions() as session:
        assert await session.scalar(select(func.count()).select_from(WorkspaceFileEffectRow)) == 1
        assert await session.scalar(select(func.count()).select_from(WorkspaceFileLockRow)) == 0


@pytest.mark.parametrize("corrupt", ["witness", "effect", "snapshot"])
async def test_frozen_references_reject_corruption_without_rechoosing_tip(ledger, corrupt):
    first, a, _ = await ledger.apply("B")
    second, _, attempt = await ledger.apply("C")
    keep = await ledger.reserve(first, a, "keep")
    async with ledger.database.sessions.begin() as session:
        if corrupt == "witness":
            row = await session.get(WorkspaceEditAttemptRow, keep.view.operation_id)
            row.keep_witness = {**row.keep_witness, "file_key": "tampered"}
        elif corrupt == "effect":
            effect = await session.scalar(
                select(WorkspaceFileEffectRow).where(
                    WorkspaceFileEffectRow.operation_id == attempt.view.operation_id
                )
            )
            effect.observed_after = {**effect.observed_after, "mode": 0}
        else:
            row = await session.get(WorkspaceEditProposalRow, second.view.proposal_id)
            row.after_content = b"tampered"
    with pytest.raises(ServiceError):
        await ledger.store.keep_target(ledger.scope, keep.view.operation_id)
    with pytest.raises(ServiceError):
        await ledger.store.operation_proof(ledger.scope, keep.view.operation_id)


async def test_migration_keeps_old_order_unproven_and_preserves_active_foreign_keys(ledger):
    prepared = WorkspaceWriter(ledger.workspace).prepare(
        FileChangeSpec(
            path="file.txt", operation="write", expected_sha256=ledger.state().sha256, content="B"
        )
    )
    proposal, _ = await ledger.store.preview(
        ledger.scope, "files", ledger.workspace.fingerprint, "old", prepared
    )
    attempt = await ledger.reserve(proposal, prepared)
    async with ledger.database.engine.begin() as connection:
        await connection.run_sync(
            lambda sync: command.downgrade(
                migration_config(sync), "0025_workspace_publication_state"
            )
        )
        assert not (await connection.execute(text("PRAGMA foreign_key_check"))).all()
    await ledger.database.initialize()
    assert (
        await ledger.store.operation(ledger.scope, attempt.view.operation_id)
    ).view.status == "running"
    async with ledger.database.sessions() as session:
        assert await session.scalar(select(func.count()).select_from(WorkspaceFileEffectRow)) == 0
        assert await session.scalar(select(func.count()).select_from(WorkspaceFileLockRow)) == 1
    effect = WorkspaceWriter(ledger.workspace).apply(prepared)
    # 以真实旧表结构保存终态，再升级验证没有凭时间/UUID回填顺序。
    async with ledger.database.engine.begin() as connection:
        await connection.run_sync(
            lambda sync: command.downgrade(
                migration_config(sync), "0025_workspace_publication_state"
            )
        )
        await connection.execute(
            text(
                "UPDATE workspace_edit_attempts SET status='done',"
                "owner_id=NULL,lease_expires_at=NULL"
            )
        )
        await connection.execute(
            text(
                "UPDATE workspace_edit_proposals SET status='applied',"
                "revision=2,active_operation_id=NULL"
            )
        )
        await connection.execute(text("DELETE FROM workspace_file_locks"))
    WorkspaceWriter(ledger.workspace).finalize(prepared, effect.recovery)
    await ledger.database.initialize()
    await ledger.apply("C")
    keep = await ledger.reserve(proposal, prepared, "keep")
    assert (await ledger.store.keep_target(ledger.scope, keep.view.operation_id))[1] is None
    async with ledger.database.engine.connect() as connection:
        assert (
            await connection.execute(text("SELECT count(*) FROM workspace_file_effects"))
        ).scalar_one() == 1
        assert not (await connection.execute(text("PRAGMA foreign_key_check"))).all()
    with pytest.raises(RuntimeError, match="效果账本"):
        async with ledger.database.engine.begin() as connection:
            await connection.run_sync(
                lambda sync: command.downgrade(
                    migration_config(sync), "0025_workspace_publication_state"
                )
            )


async def test_nonempty_witness_alone_blocks_lossy_downgrade(ledger):
    first, a, _ = await ledger.apply("B")
    await ledger.apply("C")
    keep = await ledger.reserve(first, a, "keep")
    async with ledger.database.engine.begin() as connection:
        await connection.execute(text("DELETE FROM workspace_file_effects"))
    with pytest.raises(RuntimeError, match="保留见证"):
        async with ledger.database.engine.begin() as connection:
            await connection.run_sync(
                lambda sync: command.downgrade(
                    migration_config(sync), "0025_workspace_publication_state"
                )
            )
    async with ledger.database.sessions() as session:
        assert (await session.get(WorkspaceEditAttemptRow, keep.view.operation_id)).keep_witness


async def test_witness_reserve_uses_snapshot_quota_without_leaking_lock_or_attempt(ledger):
    first, a, _ = await ledger.apply("B")
    await ledger.apply("C")
    ledger.store.max_snapshot_bytes = 1024
    async with ledger.database.sessions() as session:
        before_count = await session.scalar(
            select(func.count()).select_from(WorkspaceEditAttemptRow)
        )
    original = await ledger.store.get(ledger.scope, first.view.proposal_id)
    with pytest.raises(ServiceError) as denied:
        await ledger.reserve(first, a, "keep")
    assert denied.value.code == "workspace_snapshot_limit"
    assert (await ledger.store.get(ledger.scope, first.view.proposal_id)).view == original.view
    async with ledger.database.sessions() as session:
        assert (
            await session.scalar(select(func.count()).select_from(WorkspaceEditAttemptRow))
            == before_count
        )
        assert await session.scalar(select(func.count()).select_from(WorkspaceFileLockRow)) == 0


async def test_persisted_witness_counts_toward_later_preview_quota(ledger):
    first, a, _ = await ledger.apply("B")
    await ledger.apply("C")
    keep = await ledger.reserve(first, a, "keep")
    await ledger.store.settle(ledger.scope, keep.view.operation_id, kept_receipt(a))
    ledger.store.max_snapshot_bytes = 1024
    with pytest.raises(ServiceError) as denied:
        await ledger.apply("D")
    assert denied.value.code == "workspace_snapshot_limit"
    assert (ledger.workspace.root / "file.txt").read_text() == "C"
