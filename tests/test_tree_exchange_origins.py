"""已生成完整树封存与原交换同事务、旧hash兼容、移动后原来源零重放。"""

import ast
import asyncio
import copy
import json
import subprocess
import sys
import time
import types

import pytest
from sqlalchemy import delete, event, func, select, update
from test_completed_tree_origins import complete
from test_workspace_tree_exchange_ledger import action, publish

import krim_agent.workspace_tree_exchange as physical
from krim_agent.persistence.completed_tree_seals import CompletedTreeSealStore
from krim_agent.persistence.database import (
    Database,
    RunRow,
    WorkspaceTreeExchangeAttemptRow,
    WorkspaceTreeExchangeLockRow,
    WorkspaceTreeExchangeModelSourceRow,
    WorkspaceTreeExchangeOriginRow,
    WorkspaceTreeExchangeProposalRow,
    WorkspaceTreeSealRow,
)
from krim_agent.persistence.tree_exchange_codec import encode_exchange
from krim_agent.persistence.workspace_edits import WorkspaceEditStore, digest
from krim_agent.persistence.workspace_tree_exchanges import WorkspaceTreeExchangeStore
from krim_agent.protocol.domain import ChatRequest, ServiceError
from krim_agent.workspace_tree_exchange import WorkspaceTreeExchangeWriter
from krim_agent.workspace_tree_exchange_types import (
    TreeExchangeModelSource,
    TreeExchangePreviewRequest,
)
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshotReader


async def prepared(runtime, root, *, persistent=False):
    service, agent, scope, trees, plan, binding, snapshot, outputs = await complete(
        runtime, root, persistent=persistent
    )
    seals = CompletedTreeSealStore(trees)
    sealed, _ = await seals.seal(scope, binding, plan.plan_id, snapshot.fingerprint, snapshot)
    (root / "live").mkdir()
    (root / "live/old.bin").write_bytes(b"old\xff\0")
    writer = WorkspaceTreeExchangeWriter(binding.require())
    left = writer.trees.reader.capture("live")
    change = writer.prepare(
        "live",
        "tree",
        expected_left=left.fingerprint,
        expected_right=sealed.origin.snapshot.fingerprint,
    )
    request = TreeExchangePreviewRequest(
        left_path="live",
        right_path="tree",
        expected_left=change.left.fingerprint,
        expected_right=change.right.fingerprint,
        request_key="exchange-original",
        source_plan_id=plan.plan_id,
    )
    store = WorkspaceTreeExchangeStore(service.workspace_edits)
    return service, agent, scope, trees, plan, binding, writer, seals, store, request, change


async def test_original_sealed_tree_exchange_keep_undo_cold_provenance_survives_path_exchange(
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
        request,
        change,
    ) = await prepared(runtime, tmp_path)
    try:
        proposal, created = await store.preview(scope, binding, request, change)
        assert (
            created
            and proposal.view.origin == "completed_tree"
            and proposal.view.source_plan_id == plan.plan_id
        )
        assert await store.preview(scope, binding, request, None) == (proposal, False)
        operation, _ = await store.reserve(
            scope, action(proposal.view), binding.fingerprint, change
        )
        effect = await publish(store, writer, scope, operation)
        assert effect.receipt.status == "done"
        await store.settle(scope, operation.view.operation_id, effect)
        assert (tmp_path / "live/sub/data.bin").read_bytes() == b"\0\xff"
        assert (tmp_path / "tree/old.bin").read_bytes() == b"old\xff\0"

        def forbidden(*_args, **_kwargs):
            pytest.fail("交换原生成来源冷读不得观察当前右路径或重跑生产者")

        monkeypatch.setattr(WorkspaceTreeSnapshotReader, "capture", forbidden)
        current = await store.proposal_by_key(scope, request.request_key)
        assert (
            current.view.origin == "completed_tree" and current.view.source_plan_id == plan.plan_id
        )
        async with store.database.sessions() as sql:
            material = await seals.get_material_in_transaction(
                sql,
                scope,
                plan.plan_id,
                workspace_key=binding.key,
                resource_fingerprint=binding.fingerprint,
            )
            assert material.origin.snapshot == change.right
        kept, _ = await store.reserve(
            scope, action(current.view, "keep", "keep"), binding.fingerprint, change
        )
        kept = await store.settle(scope, kept.view.operation_id, effect)
        assert not kept.view.receipt.changed
        monkeypatch.undo()
        current = await store.proposal(scope, proposal.view.proposal_id)
        reverse = writer.prepare_undo(change, effect)
        undo, _ = await store.reserve(
            scope, action(current.view, "undo", "undo"), binding.fingerprint, reverse
        )
        await store.settle(scope, undo.view.operation_id, await publish(store, writer, scope, undo))
        assert (tmp_path / "live/old.bin").read_bytes() == b"old\xff\0"
        assert (
            await store.proposal(scope, proposal.view.proposal_id)
        ).view.origin == "completed_tree"
    finally:
        await service.close()


@pytest.mark.parametrize("fault", ["missing-seal", "wrong-tree", "source-scope", "bad-seal"])
async def test_wrong_missing_or_foreign_source_never_accepts_exchange_proposal(
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
        request,
        change,
    ) = await prepared(runtime, tmp_path)
    try:
        if fault == "missing-seal":
            request = request.model_copy(update={"source_plan_id": "missing"})
        elif fault == "wrong-tree":
            request = request.model_copy(
                update={
                    "left_path": "tree",
                    "right_path": "live",
                    "expected_left": change.right.fingerprint,
                    "expected_right": change.left.fingerprint,
                }
            )
            change = writer.prepare(
                "tree",
                "live",
                expected_left=change.right.fingerprint,
                expected_right=change.left.fingerprint,
            )
        elif fault == "source-scope":
            async with store.database.sessions.begin() as sql:
                await sql.execute(update(WorkspaceTreeSealRow).values(scope_id="session:other"))
        else:
            async with store.database.sessions.begin() as sql:
                await sql.execute(update(WorkspaceTreeSealRow).values(payload=b"damaged"))
        with pytest.raises(ServiceError):
            await store.preview(scope, binding, request, change)
        async with store.database.sessions() as sql:
            assert (
                await sql.scalar(select(func.count()).select_from(WorkspaceTreeExchangeProposalRow))
                == 0
            )
            assert (
                await sql.scalar(select(func.count()).select_from(WorkspaceTreeExchangeOriginRow))
                == 0
            )
    finally:
        await service.close()


@pytest.mark.parametrize("fault", ["missing", "wrong-plan", "proof", "scope", "tree"])
async def test_origin_corruption_cold_retry_and_final_writer_qualification_fail_closed(
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
        request,
        change,
    ) = await prepared(runtime, tmp_path)
    try:
        proposal, _ = await store.preview(scope, binding, request, change)
        operation, _ = await store.reserve(
            scope, action(proposal.view), binding.fingerprint, change
        )
        async with store.database.sessions.begin() as sql:
            if fault == "missing":
                await sql.execute(delete(WorkspaceTreeExchangeOriginRow))
            elif fault == "wrong-plan":
                row = await sql.get(WorkspaceTreeExchangeProposalRow, proposal.view.proposal_id)
                altered = {**copy.deepcopy(row.request), "source_plan_id": "other"}
                row.request = altered
                row.request_hash = store._wanted(
                    TreeExchangePreviewRequest.model_validate(altered), binding.fingerprint
                )
            else:
                row = await sql.get(WorkspaceTreeExchangeOriginRow, proposal.view.proposal_id)
                if fault == "proof":
                    row.proof_hash = "0" * 64
                elif fault == "scope":
                    row.scope_id = "session:other"
                else:
                    row.tree_fingerprint = "0" * 64
        with pytest.raises(ServiceError):
            await store.proposal(scope, proposal.view.proposal_id)
        with pytest.raises(ServiceError):
            await store.preview(scope, binding, request, None)
        with pytest.raises(ServiceError):
            await store.assert_execution(scope, operation.view.operation_id)
        effect = await publish(store, writer, scope, operation)
        assert effect.receipt.status == "rejected" and not effect.receipt.changed
        assert (tmp_path / "live/old.bin").read_bytes() == b"old\xff\0"
    finally:
        await service.close()


async def test_source_link_insert_failure_rolls_back_original_proposal_and_model_binding(
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
        request,
        change,
    ) = await prepared(runtime, tmp_path)
    try:
        run, _ = await runtime.repository.begin_run(
            service._handle(agent.agent_id).topic_id,
            ChatRequest(text="原来源preview调用", idempotency_key="origin-source"),
            agent_session_id=agent.session_id,
            agent_id=agent.agent_id,
            agent_owner_id=store.owner_id,
        )
        model_source = TreeExchangeModelSource(run.id, "origin-preview")

        def fail(_conn, _cursor, statement, _params, _context, _many):
            if statement.startswith("INSERT INTO workspace_tree_exchange_origins"):
                raise RuntimeError("原生成来源关联写入失败")

        event.listen(store.database.engine.sync_engine, "before_cursor_execute", fail)
        try:
            with pytest.raises(RuntimeError):
                await store.preview(scope, binding, request, change, model_source=model_source)
        finally:
            event.remove(store.database.engine.sync_engine, "before_cursor_execute", fail)
        async with store.database.sessions() as sql:
            assert (
                await sql.scalar(select(func.count()).select_from(WorkspaceTreeExchangeProposalRow))
                == 0
            )
            assert (
                await sql.scalar(
                    select(func.count()).select_from(WorkspaceTreeExchangeModelSourceRow)
                )
                == 0
            )
        proposal, created = await store.preview(
            scope, binding, request, change, model_source=model_source
        )
        assert created and proposal.view.origin == "completed_tree"
    finally:
        await service.close()


async def test_old_0044_request_hash_and_bytes_unchanged_with_new_default_none_field(
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
        request,
        change,
    ) = await prepared(runtime, tmp_path)
    try:
        old = request.model_dump(mode="json", exclude={"source_plan_id"})
        expected = digest({"request": old, "resource": binding.fingerprint})
        plain = TreeExchangePreviewRequest.model_validate(old)
        assert store._wanted(plain, binding.fingerprint) == expected
        proposal, _ = await store.preview(scope, binding, plain, change)
        assert proposal.view.origin == "existing_trees" and proposal.view.source_plan_id is None
        async with store.database.sessions() as sql:
            row = await sql.get(WorkspaceTreeExchangeProposalRow, proposal.view.proposal_id)
            assert row.request == old and row.request_hash == expected
        # 从已发布版本加载原请求类型和原hash算法，核实旧行不因新增默认字段失效。
        source = await asyncio.to_thread(
            subprocess.check_output,
            [
                "git",
                "show",
                "5c377e40e056e8ec752a6f9712c6c60a11bfbea4:src/krim_agent/persistence/workspace_tree_exchanges.py",
            ],
            text=True,
            cwd="/workspace/Krim",
        )
        types_source = await asyncio.to_thread(
            subprocess.check_output,
            [
                "git",
                "show",
                "5c377e40e056e8ec752a6f9712c6c60a11bfbea4:src/krim_agent/workspace_tree_exchange_types.py",
            ],
            text=True,
            cwd="/workspace/Krim",
        )
        module = types.ModuleType("krim_old_exchange_request")
        sys.modules[module.__name__] = module
        try:
            exec(compile(types_source, "published-exchange-types", "exec"), module.__dict__)
            old_request = module.TreeExchangePreviewRequest.model_validate(old)
            parsed = ast.parse(source)
            store_class = next(
                node
                for node in parsed.body
                if isinstance(node, ast.ClassDef) and node.name == "WorkspaceTreeExchangeStore"
            )
            wanted = next(
                node
                for node in store_class.body
                if isinstance(node, ast.FunctionDef) and node.name == "_wanted"
            )
            wanted.decorator_list = []
            wanted.args.args[0].annotation = None
            wanted.args.args[1].annotation = None
            wanted.returns = None
            module.__dict__["digest"] = digest
            exec(
                compile(
                    ast.fix_missing_locations(ast.Module(body=[wanted], type_ignores=[])),
                    "published-exchange-hash",
                    "exec",
                ),
                module.__dict__,
            )
            assert module._wanted(old_request, binding.fingerprint) == row.request_hash
            assert old_request.model_dump(mode="json") == row.request
        finally:
            sys.modules.pop(module.__name__, None)
    finally:
        await service.close()


async def test_pure_material_history_does_not_claim_live_workspace_authority(
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
        request,
        change,
    ) = await prepared(runtime, tmp_path)
    try:
        binding.revoked.set()

        def forbidden(*_args, **_kwargs):
            pytest.fail("纯SQL原材料核实不观察路径或创建Workspace")

        monkeypatch.setattr(WorkspaceTreeSnapshotReader, "capture", forbidden)
        async with store.database.sessions() as sql:
            material = await seals.get_material_in_transaction(
                sql,
                scope,
                plan.plan_id,
                workspace_key=binding.key,
                resource_fingerprint=binding.fingerprint,
            )
            assert material.origin.snapshot == change.right
        with pytest.raises(ServiceError) as live:
            await seals.get(scope, binding, plan.plan_id)
        assert live.value.code == "workspace_unavailable"
        with pytest.raises(ServiceError):
            await store.preview(scope, binding, request, change)
        async with store.database.sessions() as sql:
            assert (
                await sql.scalar(select(func.count()).select_from(WorkspaceTreeExchangeProposalRow))
                == 0
            )
    finally:
        binding.revoked.clear()
        await service.close()


async def test_origin_bound_original_model_stop_refuses_namespace_but_keeps_cold_source(
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
        request,
        change,
    ) = await prepared(runtime, tmp_path)
    try:
        run, _ = await runtime.repository.begin_run(
            service._handle(agent.agent_id).topic_id,
            ChatRequest(text="原模型来源交换", idempotency_key="model-origin"),
            agent_session_id=agent.session_id,
            agent_id=agent.agent_id,
            agent_owner_id=store.owner_id,
        )
        proposal, _ = await store.preview(
            scope, binding, request, change, model_source=TreeExchangeModelSource(run.id, "preview")
        )
        operation, _ = await store.reserve(
            scope,
            action(proposal.view),
            binding.fingerprint,
            change,
            model_source=TreeExchangeModelSource(run.id, "apply"),
        )
        async with store.database.sessions.begin() as sql:
            (await sql.get(RunRow, run.id)).stop_requested = True
        effect = await publish(store, writer, scope, operation)
        assert effect.receipt.status == "rejected" and not effect.receipt.changed
        result = await store.settle(scope, operation.view.operation_id, effect)
        assert result.view.status == "rejected"
        await runtime.repository.finish(run.id, "aborted", "stopped", [])
        assert (
            await store.proposal(scope, proposal.view.proposal_id)
        ).view.origin == "completed_tree"
        assert (tmp_path / "live/old.bin").read_bytes() == b"old\xff\0"
    finally:
        await service.close()


async def test_bound_origin_unknown_after_effect_keeps_same_operation_locks_and_no_exchange_replay(
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
        request,
        change,
    ) = await prepared(runtime, tmp_path)
    original = physical.rename_at
    calls = 0
    try:
        proposal, _ = await store.preview(scope, binding, request, change)
        operation, _ = await store.reserve(
            scope, action(proposal.view), binding.fingerprint, change
        )

        def lost(*args, **kwargs):
            nonlocal calls
            calls += 1
            original(*args, **kwargs)
            raise OSError("原效果回程丢失")

        monkeypatch.setattr(physical, "rename_at", lost)
        unknown = await publish(store, writer, scope, operation)
        assert unknown.receipt.status == "uncertain" and calls == 1
        async with store.database.sessions.begin() as sql:
            await sql.execute(
                update(WorkspaceTreeExchangeAttemptRow)
                .where(WorkspaceTreeExchangeAttemptRow.operation_id == operation.view.operation_id)
                .values(lease_expires_at=time.time() - 1)
            )
        interrupted = await store.operation(scope, operation.view.operation_id)
        assert interrupted.view.status == "uncertain"
        async with store.database.sessions() as sql:
            assert (
                await sql.scalar(select(func.count()).select_from(WorkspaceTreeExchangeLockRow)) > 0
            )
        with pytest.raises(ServiceError):
            await store.settle(scope, operation.view.operation_id, unknown)

        def forbidden(*_args, **_kwargs):
            pytest.fail("来源核实/unknown不能再次交换或重新观察原源")

        monkeypatch.setattr(physical, "rename_at", forbidden)
        resolved = writer.reconcile(interrupted.prepared)
        result = await store.settle(scope, operation.view.operation_id, resolved, resolving=True)
        assert (
            result.view.operation_id == operation.view.operation_id and result.view.status == "done"
        )
        assert (
            await store.proposal(scope, proposal.view.proposal_id)
        ).view.origin == "completed_tree" and calls == 1
    finally:
        await service.close()


async def test_two_real_connections_bind_source_once_and_accept_same_operation(runtime, tmp_path):
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
        request,
        change,
    ) = await prepared(runtime, tmp_path)
    second_db = Database(str(store.database.engine.url))
    await second_db.initialize()
    second = WorkspaceTreeExchangeStore(WorkspaceEditStore(second_db, owner_id=store.owner_id))
    try:
        previews = await asyncio.gather(
            store.preview(scope, binding, request, change),
            second.preview(scope, binding, request, change),
        )
        assert sum(created for _, created in previews) == 1
        proposal = previews[0][0]
        assert proposal == previews[1][0] and proposal.view.source_plan_id == plan.plan_id
        accepts = await asyncio.gather(
            store.reserve(scope, action(proposal.view), binding.fingerprint, change),
            second.reserve(scope, action(proposal.view), binding.fingerprint, change),
        )
        assert sum(created for _, created in accepts) == 1
        operation = accepts[0][0]
        assert operation.view.operation_id == accepts[1][0].view.operation_id
        done = await store.settle(
            scope, operation.view.operation_id, await publish(store, writer, scope, operation)
        )
        assert await second.operation(scope, operation.view.operation_id) == done
        async with second_db.sessions() as sql:
            for table in (
                WorkspaceTreeExchangeProposalRow,
                WorkspaceTreeExchangeOriginRow,
                WorkspaceTreeExchangeAttemptRow,
            ):
                assert await sql.scalar(select(func.count()).select_from(table)) == 1
            assert (
                await sql.scalar(select(func.count()).select_from(WorkspaceTreeExchangeLockRow))
                == 0
            )
    finally:
        await second_db.close()
        await service.close()


async def test_source_link_counts_toward_original_quota_and_rolls_back_at_one_byte_over(
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
        request,
        change,
    ) = await prepared(runtime, tmp_path)
    try:
        metadata, payload = encode_exchange(change)
        proposal_bytes = (
            len(payload)
            + len(json.dumps(metadata))
            + len(json.dumps(request.model_dump(mode="json", exclude_none=True)))
        )
        sealed = await seals.get(scope, binding, plan.plan_id)
        link_bytes = (
            36
            + len(plan.plan_id)
            + len(scope)
            + len(binding.key)
            + len(binding.fingerprint)
            + len(change.right.fingerprint)
            + len(sealed.origin.view.proof_hash)
        )
        store.max_snapshot_bytes = proposal_bytes + link_bytes - 1
        with pytest.raises(ServiceError) as full:
            await store.preview(scope, binding, request, change)
        assert full.value.code == "workspace_tree_exchange_snapshot_limit"
        async with store.database.sessions() as sql:
            for table in (WorkspaceTreeExchangeProposalRow, WorkspaceTreeExchangeOriginRow):
                assert await sql.scalar(select(func.count()).select_from(table)) == 0
        store.max_snapshot_bytes += 1
        proposal, created = await store.preview(scope, binding, request, change)
        assert created and proposal.view.origin == "completed_tree"
        assert await store.preview(scope, binding, request, None) == (proposal, False)
        assert (tmp_path / "live/old.bin").read_bytes() == b"old\xff\0"
    finally:
        await service.close()


async def test_cancel_bound_original_before_writer_refuses_effect_and_preserves_source(
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
        request,
        change,
    ) = await prepared(runtime, tmp_path)
    try:
        proposal, _ = await store.preview(scope, binding, request, change)
        operation, _ = await store.reserve(
            scope, action(proposal.view), binding.fingerprint, change
        )
        await store.cancel(scope, proposal.view.proposal_id)
        effect = await publish(store, writer, scope, operation)
        assert effect.receipt.status == "rejected" and not effect.receipt.changed
        result = await store.settle(scope, operation.view.operation_id, effect)
        assert result.view.status == "rejected"
        original = await store.proposal(scope, proposal.view.proposal_id)
        assert (
            original.view.origin == "completed_tree"
            and original.view.source_plan_id == plan.plan_id
        )
        assert (tmp_path / "live/old.bin").read_bytes() == b"old\xff\0"
        assert (tmp_path / "tree/sub/data.bin").read_bytes() == b"\0\xff"
    finally:
        await service.close()
