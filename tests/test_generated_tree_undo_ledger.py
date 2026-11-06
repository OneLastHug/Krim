"""真实原生成根撤销来源、通用Move门禁、实际还原版本链与零重放。"""

import ast
import asyncio
import copy
import json
import subprocess
import time
from datetime import timedelta

import pytest
from sqlalchemy import event, func, select
from test_completed_tree_origins import complete
from test_workspace_tree_move_ledger import action, publish

import krim_agent.workspace_tree_writer as physical
from krim_agent.generated_tree_undo import GeneratedTreeUndoSource
from krim_agent.persistence.completed_tree_seals import CompletedTreeSealStore
from krim_agent.persistence.database import (
    Database,
    RunRow,
    WorkspaceGeneratedTreeUndoRow,
    WorkspaceTreeMoveAttemptRow,
    WorkspaceTreeMoveLockRow,
    WorkspaceTreeMoveModelSourceRow,
    WorkspaceTreeMoveProposalRow,
    WorkspaceTreePlanRow,
    WorkspaceTreeSealRow,
)
from krim_agent.persistence.tree_move_codec import encode_move
from krim_agent.persistence.workspace_edits import WorkspaceEditStore, digest
from krim_agent.persistence.workspace_tree_moves import WorkspaceTreeMoveStore
from krim_agent.protocol.domain import ChatRequest, ServiceError
from krim_agent.workspace_tree_move import TreeMoveModelSource, TreeMovePreviewRequest
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshotReader
from krim_agent.workspace_tree_writer import WorkspaceTreeWriter


async def prepared(runtime, root, *, persistent=False):
    service, agent, scope, trees, plan, binding, snapshot, outputs = await complete(
        runtime, root, persistent=persistent
    )
    seals = CompletedTreeSealStore(trees)
    sealed, _ = await seals.seal(scope, binding, plan.plan_id, snapshot.fingerprint, snapshot)
    writer = WorkspaceTreeWriter(binding.require())
    change = writer.prepare("tree", "retained", expected_fingerprint=snapshot.fingerprint)
    request = TreeMovePreviewRequest(
        source_path="tree",
        target_path="retained",
        expected_fingerprint=snapshot.fingerprint,
        request_key="root-undo-preview",
    )
    store = WorkspaceTreeMoveStore(service.workspace_edits)
    source = GeneratedTreeUndoSource(plan.plan_id)
    return (
        service,
        agent,
        scope,
        trees,
        plan,
        binding,
        writer,
        seals,
        store,
        request,
        change,
        source,
    )


async def counts(database):
    async with database.sessions() as sql:
        return [
            await sql.scalar(select(func.count()).select_from(table))
            for table in (
                WorkspaceTreeMoveProposalRow,
                WorkspaceGeneratedTreeUndoRow,
                WorkspaceTreeMoveAttemptRow,
                WorkspaceTreeMoveLockRow,
                WorkspaceTreeMoveModelSourceRow,
            )
        ]


async def restored(store, writer, scope, binding, proposal, change):
    attempt, _ = await store.reserve(scope, action(proposal.view), binding.fingerprint, change)
    effect = await publish(store, writer, scope, attempt)
    assert effect.receipt.status == "done"
    await store.settle(scope, attempt.view.operation_id, effect)
    current = await store.proposal(scope, proposal.view.proposal_id)
    reverse = writer.prepare_undo(change, effect)
    restore, _ = await store.reserve(
        scope, action(current.view, "undo", "root-restore"), binding.fingerprint, reverse
    )
    actual = await publish(store, writer, scope, restore)
    assert actual.receipt.status == "done"
    await store.settle(scope, restore.view.operation_id, actual)
    return restore, actual


async def test_real_root_undo_keep_restore_repeat_preserves_original_plan_and_full_inodes(
    runtime, tmp_path, monkeypatch
):
    (
        service,
        agent,
        scope,
        trees,
        plan,
        binding,
        writer,
        seals,
        store,
        req,
        change,
        source,
    ) = await prepared(runtime, tmp_path)
    calls = 0
    original = physical.rename_at

    def rename(*args, **kwargs):
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(physical, "rename_at", rename)
    try:
        proposal, created = await store.preview(
            scope, binding, req, change, generated_origin=source
        )
        assert created and await counts(store.database) == [1, 1, 0, 0, 0]
        apply, _ = await store.reserve(scope, action(proposal.view), binding.fingerprint, change)
        effect = await publish(store, writer, scope, apply)
        assert effect.receipt.status == "done" and calls == 1
        await store.settle(scope, apply.view.operation_id, effect)
        assert not (tmp_path / "tree").exists()
        assert (tmp_path / "retained/sub/data.bin").read_bytes() == b"\0\xff"

        def identities(snapshot):
            return [
                (item.device, item.inode)
                for e in snapshot.entries
                if (item := e.directory or e.file)
            ]

        assert identities(change.before) == identities(effect.observed_after)
        current = await store.proposal(scope, proposal.view.proposal_id)
        keep, _ = await store.reserve(
            scope, action(current.view, "keep", "root-keep"), binding.fingerprint, change
        )
        loop = asyncio.get_running_loop()

        def authorize_keep():
            asyncio.run_coroutine_threadsafe(
                store.assert_execution(scope, keep.view.operation_id), loop
            ).result(timeout=5)

        keep_effect = await asyncio.to_thread(
            writer.verify_keep, change, effect, authorize=authorize_keep
        )
        await store.settle(scope, keep.view.operation_id, keep_effect)
        assert not (await store.operation(scope, keep.view.operation_id)).view.receipt.changed
        assert calls == 1
        current = await store.proposal(scope, proposal.view.proposal_id)
        restore, _ = await store.reserve(
            scope,
            action(current.view, "undo", "root-restore"),
            binding.fingerprint,
            writer.prepare_undo(change, effect),
        )
        actual = await publish(store, writer, scope, restore)
        await store.settle(scope, restore.view.operation_id, actual)
        assert actual.receipt.status == "done" and calls == 2
        assert (tmp_path / "tree/sub/data.bin").read_bytes() == b"\0\xff"
        assert not (tmp_path / "retained").exists()
        assert actual.observed_after != change.before
        again = writer.prepare(
            "tree", "retained", expected_fingerprint=actual.observed_after.fingerprint
        )
        again_request = req.model_copy(
            update={"request_key": "root-again", "expected_fingerprint": again.before.fingerprint}
        )
        with pytest.raises(ServiceError, match="原seal或前驱"):
            await store.preview(scope, binding, again_request, again, generated_origin=source)
        again_source = GeneratedTreeUndoSource(plan.plan_id, restore.view.operation_id)
        next_proposal, _ = await store.preview(
            scope, binding, again_request, again, generated_origin=again_source
        )
        next_apply, _ = await store.reserve(
            scope, action(next_proposal.view, key="root-again-apply"), binding.fingerprint, again
        )
        await store.settle(
            scope, next_apply.view.operation_id, await publish(store, writer, scope, next_apply)
        )
        assert calls == 3 and not (tmp_path / "tree").exists()
        assert await trees.get(scope, plan.plan_id) == plan

        def forbidden(*_args, **_kwargs):
            pytest.fail("根已撤销，原来源冷读/键重试不得观察路径或重新生成")

        monkeypatch.setattr(WorkspaceTreeSnapshotReader, "capture", forbidden)
        assert (await seals.get(scope, binding, plan.plan_id)).origin.snapshot == change.before
        assert (await store.preview(scope, binding, req, None, generated_origin=source))[1] is False
        assert (
            await store.preview(scope, binding, again_request, None, generated_origin=again_source)
        )[1] is False
        assert len(await store.history(scope, proposal.view.proposal_id)) == 3
    finally:
        await service.close()


@pytest.mark.parametrize("fault", ["missing-seal", "scope", "resource", "bytes", "initial-root"])
async def test_wrong_original_generation_or_first_version_never_accepts_root_undo(
    runtime, tmp_path, fault
):
    (
        service,
        agent,
        scope,
        trees,
        plan,
        binding,
        writer,
        seals,
        store,
        req,
        change,
        source,
    ) = await prepared(runtime, tmp_path)
    try:
        async with store.database.sessions.begin() as sql:
            seal = await sql.get(WorkspaceTreeSealRow, plan.plan_id)
            if fault == "missing-seal":
                await sql.delete(seal)
            elif fault == "scope":
                seal.scope_id = "session:other"
            elif fault == "resource":
                seal.resource_fingerprint = "0" * 64
            elif fault == "initial-root":
                row = await sql.get(WorkspaceTreePlanRow, plan.plan_id)
                frozen = copy.deepcopy(row.frozen)
                frozen["root_prepared"]["before"] = {"inode": 0}
                row.frozen = frozen
        if fault == "bytes":
            (tmp_path / "tree/sub/data.bin").write_bytes(b"changed")
            changed = writer.reader.capture("tree")
            change = writer.prepare("tree", "retained", expected_fingerprint=changed.fingerprint)
            req = req.model_copy(update={"expected_fingerprint": change.before.fingerprint})
        with pytest.raises(ServiceError):
            await store.preview(scope, binding, req, change, generated_origin=source)
        assert await counts(store.database) == [0, 0, 0, 0, 0]
        assert (tmp_path / "tree").exists() and not (tmp_path / "retained").exists()
    finally:
        await service.close()


@pytest.mark.parametrize("fault", ["missing", "marker", "proof", "scope", "version", "time"])
async def test_broken_source_rejected_through_all_generic_move_entry_points_and_final_writer(
    runtime, tmp_path, fault
):
    (
        service,
        agent,
        scope,
        trees,
        plan,
        binding,
        writer,
        seals,
        store,
        req,
        change,
        source,
    ) = await prepared(runtime, tmp_path)
    try:
        proposal, _ = await store.preview(scope, binding, req, change, generated_origin=source)
        op, _ = await store.reserve(scope, action(proposal.view), binding.fingerprint, change)
        async with store.database.sessions.begin() as sql:
            link = await sql.get(WorkspaceGeneratedTreeUndoRow, proposal.view.proposal_id)
            if fault == "missing":
                await sql.delete(link)
            elif fault == "marker":
                (
                    await sql.get(WorkspaceTreeMoveProposalRow, proposal.view.proposal_id)
                ).generated_plan_id = None
            elif fault == "proof":
                link.proof_hash = "0" * 64
            elif fault == "scope":
                link.scope_id = "session:other"
            elif fault == "version":
                link.approved_fingerprint = "0" * 64
            else:
                link.created_at += timedelta(seconds=1)
        checks = [
            store.proposal(scope, proposal.view.proposal_id),
            store.proposal_by_key(scope, req.request_key),
            store.preview(scope, binding, req, None),
            store.reserve(scope, action(proposal.view), binding.fingerprint, change),
            store.operation(scope, op.view.operation_id),
            store.renew(scope, op.view.operation_id),
            store.record_intent(scope, op.view.operation_id, change),
            store.assert_execution(scope, op.view.operation_id),
            store.history(scope, proposal.view.proposal_id),
            store.list(scope),
        ]
        for check in checks:
            with pytest.raises(ServiceError):
                await check
        result = await publish(store, writer, scope, op)
        assert result.receipt.status == "rejected" and not result.receipt.changed
        assert (tmp_path / "tree/sub/data.bin").read_bytes() == b"\0\xff"
        assert not (tmp_path / "retained").exists()
    finally:
        await service.close()


async def test_bad_private_source_and_same_key_cannot_replace_old_binding(runtime, tmp_path):
    (
        service,
        agent,
        scope,
        trees,
        plan,
        binding,
        writer,
        seals,
        store,
        req,
        change,
        source,
    ) = await prepared(runtime, tmp_path)
    try:
        proposal, _ = await store.preview(scope, binding, req, change, generated_origin=source)
        for bad in (
            object(),
            GeneratedTreeUndoSource("../tree"),
            GeneratedTreeUndoSource(plan.plan_id, "\0"),
        ):
            with pytest.raises(ServiceError) as error:
                await store.preview(scope, binding, req, None, generated_origin=bad)
            assert error.value.code == "workspace_generated_undo_source"
        for bad in (
            GeneratedTreeUndoSource("other"),
            GeneratedTreeUndoSource(plan.plan_id, "other"),
        ):
            with pytest.raises(ServiceError) as error:
                await store.preview(scope, binding, req, None, generated_origin=bad)
            assert error.value.code == "workspace_generated_undo_source_conflict"
        assert await store.proposal(scope, proposal.view.proposal_id) == proposal
        assert await counts(store.database) == [1, 1, 0, 0, 0]
    finally:
        await service.close()


@pytest.mark.parametrize("fault", ["action", "status", "plan", "cycle", "time", "version"])
async def test_repeat_undo_requires_actual_same_plan_restore_and_acyclic_earlier_chain(
    runtime, tmp_path, fault
):
    (
        service,
        agent,
        scope,
        trees,
        plan,
        binding,
        writer,
        seals,
        store,
        req,
        change,
        source,
    ) = await prepared(runtime, tmp_path)
    try:
        proposal, _ = await store.preview(scope, binding, req, change, generated_origin=source)
        restore, actual = await restored(store, writer, scope, binding, proposal, change)
        repeat = writer.prepare(
            "tree", "retained", expected_fingerprint=actual.observed_after.fingerprint
        )
        request = req.model_copy(
            update={"expected_fingerprint": repeat.before.fingerprint, "request_key": "again"}
        )
        follow = GeneratedTreeUndoSource(plan.plan_id, restore.view.operation_id)
        if fault == "version":
            (tmp_path / "tree/sub/data.bin").write_bytes(b"changed")
            changed = writer.reader.capture("tree")
            repeat = writer.prepare("tree", "retained", expected_fingerprint=changed.fingerprint)
            request = request.model_copy(update={"expected_fingerprint": repeat.before.fingerprint})
        async with store.database.sessions.begin() as sql:
            op = await sql.get(WorkspaceTreeMoveAttemptRow, restore.view.operation_id)
            if fault == "action":
                op.action = "keep"
            elif fault == "status":
                op.status = "uncertain"
            elif fault == "plan":
                follow = GeneratedTreeUndoSource("missing", restore.view.operation_id)
            elif fault == "cycle":
                (
                    await sql.get(WorkspaceGeneratedTreeUndoRow, proposal.view.proposal_id)
                ).previous_restore_operation_id = restore.view.operation_id
            elif fault == "time":
                op.updated_at += timedelta(days=1)
        with pytest.raises(ServiceError):
            await store.preview(scope, binding, request, repeat, generated_origin=follow)
        assert (await counts(store.database))[:3] == [1, 1, 2]
    finally:
        await service.close()


@pytest.mark.parametrize(
    "table", ["workspace_generated_tree_undos", "workspace_tree_move_model_sources"]
)
async def test_original_proposal_marker_and_model_origin_rollback_together(
    runtime, tmp_path, table
):
    (
        service,
        agent,
        scope,
        trees,
        plan,
        binding,
        writer,
        seals,
        store,
        req,
        change,
        source,
    ) = await prepared(runtime, tmp_path)
    try:
        run, _ = await runtime.repository.begin_run(
            service._handle(agent.agent_id).topic_id,
            ChatRequest(text="原根撤销来源", idempotency_key="root-origin"),
            agent_session_id=agent.session_id,
            agent_id=agent.agent_id,
            agent_owner_id=store.owner_id,
        )
        model = TreeMoveModelSource(run.id, "preview")

        def fail(_conn, _cursor, statement, _params, _context, _many):
            if statement.startswith("INSERT INTO " + table):
                raise RuntimeError("来源同事务失败")

        event.listen(store.database.engine.sync_engine, "before_cursor_execute", fail)
        try:
            with pytest.raises(RuntimeError, match="来源同事务失败"):
                await store.preview(
                    scope, binding, req, change, generated_origin=source, model_source=model
                )
        finally:
            event.remove(store.database.engine.sync_engine, "before_cursor_execute", fail)
        assert await counts(store.database) == [0, 0, 0, 0, 0]
        assert (
            await store.preview(
                scope, binding, req, change, generated_origin=source, model_source=model
            )
        )[1]
    finally:
        await service.close()


async def test_source_material_capacity_is_reserved_before_atomic_accept(runtime, tmp_path):
    (
        service,
        agent,
        scope,
        trees,
        plan,
        binding,
        writer,
        seals,
        store,
        req,
        change,
        source,
    ) = await prepared(runtime, tmp_path)
    try:
        metadata, payload = encode_move(change)
        total = (
            len(payload) + len(json.dumps(metadata)) + len(json.dumps(req.model_dump(mode="json")))
        )
        total += 36 + len(plan.plan_id) + len(scope) + len(binding.key) + 256
        store.max_snapshot_bytes = total - 1
        with pytest.raises(ServiceError) as error:
            await store.preview(scope, binding, req, change, generated_origin=source)
        assert error.value.code == "workspace_tree_move_snapshot_limit"
        assert await counts(store.database) == [0, 0, 0, 0, 0]
        store.max_snapshot_bytes = total
        assert (await store.preview(scope, binding, req, change, generated_origin=source))[1]
        async with store.database.sessions() as sql:
            await store._bytes(sql, 0)
            with pytest.raises(ServiceError):
                await store._bytes(sql, 1)
    finally:
        await service.close()


async def test_two_database_connections_accept_single_generated_source_and_namespace_operation(
    runtime, tmp_path
):
    (
        service,
        agent,
        scope,
        trees,
        plan,
        binding,
        writer,
        seals,
        store,
        req,
        change,
        source,
    ) = await prepared(runtime, tmp_path)
    other_db = Database(str(store.database.engine.url))
    await other_db.initialize()
    other = WorkspaceTreeMoveStore(WorkspaceEditStore(other_db, owner_id=store.owner_id))
    try:
        results = await asyncio.gather(
            store.preview(scope, binding, req, change, generated_origin=source),
            other.preview(scope, binding, req, change, generated_origin=source),
        )
        assert sum(created for _, created in results) == 1 and results[0][0] == results[1][0]
        proposal = results[0][0]
        attempts = await asyncio.gather(
            store.reserve(scope, action(proposal.view), binding.fingerprint, change),
            other.reserve(scope, action(proposal.view), binding.fingerprint, change),
        )
        assert sum(created for _, created in attempts) == 1
        op = attempts[0][0]
        assert op.view.operation_id == attempts[1][0].view.operation_id
        result = await store.settle(
            scope, op.view.operation_id, await publish(store, writer, scope, op)
        )
        assert await other.operation(scope, op.view.operation_id) == result
        assert await counts(other_db) == [1, 1, 1, 0, 0]
    finally:
        await other_db.close()
        await service.close()


@pytest.mark.parametrize("stop", [False, True])
async def test_original_run_stop_or_cancel_blocks_generated_root_namespace(runtime, tmp_path, stop):
    (
        service,
        agent,
        scope,
        trees,
        plan,
        binding,
        writer,
        seals,
        store,
        req,
        change,
        source,
    ) = await prepared(runtime, tmp_path)
    try:
        run, _ = await runtime.repository.begin_run(
            service._handle(agent.agent_id).topic_id,
            ChatRequest(text="原根撤销模型调用", idempotency_key="root-stop"),
            agent_session_id=agent.session_id,
            agent_id=agent.agent_id,
            agent_owner_id=store.owner_id,
        )
        proposal, _ = await store.preview(
            scope,
            binding,
            req,
            change,
            generated_origin=source,
            model_source=TreeMoveModelSource(run.id, "preview"),
        )
        op, _ = await store.reserve(
            scope,
            action(proposal.view),
            binding.fingerprint,
            change,
            model_source=TreeMoveModelSource(run.id, "apply"),
        )
        if stop:
            async with store.database.sessions.begin() as sql:
                (await sql.get(RunRow, run.id)).stop_requested = True
        else:
            await store.cancel(scope, proposal.view.proposal_id)
        result = await publish(store, writer, scope, op)
        assert result.receipt.status == "rejected" and not result.receipt.changed
        await store.settle(scope, op.view.operation_id, result)
        assert (tmp_path / "tree/sub/data.bin").read_bytes() == b"\0\xff"
        assert not (tmp_path / "retained").exists()
        await runtime.repository.finish(run.id, "aborted", "stopped", [])
        assert await store.proposal_by_key(scope, req.request_key) is not None
    finally:
        await service.close()


async def test_generated_unknown_same_operation_zero_namespace_replay_and_unchanged_old_move_hash(
    runtime, tmp_path, monkeypatch
):
    (
        service,
        agent,
        scope,
        trees,
        plan,
        binding,
        writer,
        seals,
        store,
        req,
        change,
        source,
    ) = await prepared(runtime, tmp_path)
    original, calls = physical.rename_at, 0
    try:
        assert store._wanted(req, binding.fingerprint) == digest(
            {"request": req.model_dump(mode="json"), "resource": binding.fingerprint}
        )
        proposal, _ = await store.preview(scope, binding, req, change, generated_origin=source)
        op, _ = await store.reserve(scope, action(proposal.view), binding.fingerprint, change)

        def lost(*args, **kwargs):
            nonlocal calls
            calls += 1
            original(*args, **kwargs)
            raise OSError("根移动成功但回程丢失")

        monkeypatch.setattr(physical, "rename_at", lost)
        result = await publish(store, writer, scope, op)
        assert result.receipt.status == "uncertain" and calls == 1
        async with store.database.sessions.begin() as sql:
            (await sql.get(WorkspaceTreeMoveAttemptRow, op.view.operation_id)).lease_expires_at = (
                time.time() - 1
            )
        pending = await store.operation(scope, op.view.operation_id)
        assert pending.view.status == "uncertain" and (await counts(store.database))[3] > 0

        def forbidden(*_args, **_kwargs):
            pytest.fail("根撤销unknown只核实同operation，不再次根移动")

        monkeypatch.setattr(physical, "rename_at", forbidden)
        actual = writer.reconcile(pending.prepared)
        with pytest.raises(ServiceError):
            await store.settle(scope, op.view.operation_id, actual)
        done = await store.settle(scope, op.view.operation_id, actual, resolving=True)
        assert done.view.status == "done" and done.view.operation_id == op.view.operation_id
        assert await counts(store.database) == [1, 1, 1, 0, 0]
        monkeypatch.setattr(WorkspaceTreeSnapshotReader, "capture", forbidden)
        assert (await store.preview(scope, binding, req, None, generated_origin=source))[1] is False
        assert (await seals.get(scope, binding, plan.plan_id)).origin.snapshot == change.before
    finally:
        await service.close()


async def test_published_0043_request_hash_and_codec_bytes_remain_compatible(runtime, tmp_path):
    (
        service,
        agent,
        scope,
        trees,
        plan,
        binding,
        writer,
        seals,
        store,
        req,
        change,
        source,
    ) = await prepared(runtime, tmp_path)
    try:
        old = await asyncio.to_thread(
            subprocess.check_output,
            ["git", "show", "1bb7cb04b19c59efed578127195e316153945dd1:src/krim_agent/persistence/workspace_tree_moves.py"],
            cwd="/workspace/Krim",
            text=True,
        )
        old_class = next(
            n
            for n in ast.parse(old).body
            if isinstance(n, ast.ClassDef) and n.name == "WorkspaceTreeMoveStore"
        )
        wanted = next(
            n for n in old_class.body if isinstance(n, ast.FunctionDef) and n.name == "_wanted"
        )
        wanted.decorator_list = []
        wanted.returns = None
        for argument in wanted.args.args:
            argument.annotation = None
        namespace = {"digest": digest}
        exec(
            compile(
                ast.fix_missing_locations(ast.Module(body=[wanted], type_ignores=[])),
                "published-0043-hash",
                "exec",
            ),
            namespace,
        )
        proposal, _ = await store.preview(scope, binding, req, change, generated_origin=source)
        async with store.database.sessions() as sql:
            row = await sql.get(WorkspaceTreeMoveProposalRow, proposal.view.proposal_id)
            assert row.request == req.model_dump(mode="json")
            assert row.request_hash == namespace["_wanted"](req, binding.fingerprint)
            assert (row.prepared, row.payload) == encode_move(change)
        from pathlib import Path

        import krim_agent.persistence.tree_move_codec as codec
        import krim_agent.tools.tree_moves as tools
        import krim_agent.workspace_tree_move as types

        for module, path in (
            (codec, "persistence/tree_move_codec.py"),
            (types, "workspace_tree_move.py"),
            (tools, "tools/tree_moves.py"),
        ):
            published = await asyncio.to_thread(
                subprocess.check_output,
                ["git", "show", "1bb7cb04b19c59efed578127195e316153945dd1:src/krim_agent/" + path],
                cwd="/workspace/Krim",
            )
            assert await asyncio.to_thread(Path(module.__file__).read_bytes) == published
    finally:
        await service.close()


async def test_accepted_repeat_rechecks_entire_predecessor_chain_at_final_namespace(
    runtime, tmp_path
):
    (
        service,
        agent,
        scope,
        trees,
        plan,
        binding,
        writer,
        seals,
        store,
        req,
        change,
        source,
    ) = await prepared(runtime, tmp_path)
    try:
        first, _ = await store.preview(scope, binding, req, change, generated_origin=source)
        restore, actual = await restored(store, writer, scope, binding, first, change)
        repeat = writer.prepare(
            "tree", "retained", expected_fingerprint=actual.observed_after.fingerprint
        )
        request = req.model_copy(
            update={
                "expected_fingerprint": repeat.before.fingerprint,
                "request_key": "repeat-final",
            }
        )
        proposal, _ = await store.preview(
            scope,
            binding,
            request,
            repeat,
            generated_origin=GeneratedTreeUndoSource(plan.plan_id, restore.view.operation_id),
        )
        op, _ = await store.reserve(
            scope, action(proposal.view, key="repeat-final-apply"), binding.fingerprint, repeat
        )
        async with store.database.sessions.begin() as sql:
            (await sql.get(WorkspaceGeneratedTreeUndoRow, first.view.proposal_id)).proof_hash = (
                "0" * 64
            )
        for check in (
            store.proposal(scope, proposal.view.proposal_id),
            store.renew(scope, op.view.operation_id),
            store.record_intent(scope, op.view.operation_id, repeat),
            store.assert_execution(scope, op.view.operation_id),
        ):
            with pytest.raises(ServiceError):
                await check
        result = await publish(store, writer, scope, op)
        assert result.receipt.status == "rejected" and not result.receipt.changed
        assert (tmp_path / "tree/sub/data.bin").read_bytes() == b"\0\xff" and not (
            tmp_path / "retained"
        ).exists()
    finally:
        await service.close()
