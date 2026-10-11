"""混合历史冻结上界、只读标量聚合、原关联守卫及SDK/REST/Client/CLI。"""

import asyncio
import json
import socket
import time
from datetime import UTC, datetime
from uuid import UUID

import pytest
import uvicorn
from sqlalchemy import event, insert, update
from test_mixed_workspace_batch_fences import apply_child, reserve_child
from test_mixed_workspace_batch_public import cli, request_for
from test_mixed_workspace_batch_store import setup
from test_native_workspace_public_api import assert_public, native_api

from krim_agent.client import KrimClient
from krim_agent.persistence.database import MixedWorkspaceBatchChildRow, MixedWorkspaceBatchRow
from krim_agent.persistence.workspace_batches import _history_cursor
from krim_agent.protocol.domain import ServiceError
from krim_agent.sdk import AgentSession


async def seed(store, scope, proposal, count, *, start=1, created=None):
    """分页量仅合成cancelled/skipped元数据；真实操作由下方Writer测试覆盖。"""
    created = created or datetime(2026, 1, 1, tzinfo=UTC)
    rows = [
        {
            "batch_id": str(UUID(int=index)),
            "scope_id": scope,
            "workspace_key": "files",
            "resource_fingerprint": "a" * 64,
            "request_key": f"history-{index}",
            "request_hash": "b" * 64,
            "snapshot_revision": "c" * 64,
            "action": "apply",
            "producer_status": "cancelled",
            "owner_id": None,
            "lease_expires_at": None,
            "token": "d" * 64,
            "stop_reason": "workspace_mixed_batch_cancelled",
            "created_at": created,
            "updated_at": created,
        }
        for index in range(start, start + count)
    ]
    children = [
        {
            "batch_id": row["batch_id"],
            "position": 0,
            "source": "file",
            "proposal_id": proposal,
            "file_proposal_id": proposal,
            "directory_proposal_id": None,
            "expected_revision": 0,
            "path": "file.txt",
            "target_path": None,
            "physical_keys": ["e" * 64],
            "request_key": f"krim-mixed-batch:{row['batch_id']}:0",
            "file_operation_id": None,
            "directory_operation_id": None,
            "admission_open": False,
            "status": "skipped",
            "error_code": "workspace_mixed_batch_cancelled",
        }
        for row in rows
    ]
    async with store.database.sessions.begin() as sql:
        await sql.execute(insert(MixedWorkspaceBatchRow), rows)
        await sql.execute(insert(MixedWorkspaceBatchChildRow), children)


async def test_keyset_ties_new_insert_and_status_change_never_duplicate_old_snapshot(
    repository, tmp_path
):
    store, scope, workspace, request = await setup(repository, tmp_path)
    try:
        await seed(store, scope, request.targets[0].proposal_id, 35)
        first = await store.history(scope, limit=10)
        assert first.total == 35 and len(first.items) == 10 and first.next_cursor
        assert first.items[0].batch_id == str(UUID(int=35))
        await seed(
            store,
            scope,
            request.targets[0].proposal_id,
            2,
            start=36,
            created=datetime(2026, 1, 2, tzinfo=UTC),
        )
        async with store.database.sessions.begin() as sql:
            await sql.execute(
                update(MixedWorkspaceBatchRow)
                .where(MixedWorkspaceBatchRow.batch_id == str(UUID(int=22)))
                .values(producer_status="finished", updated_at=datetime(2026, 1, 3, tzinfo=UTC))
            )
        seen = [item.batch_id for item in first.items]
        cursor = first.next_cursor
        while cursor:
            page = await store.history(scope, limit=10, cursor=cursor)
            assert page.total == 35
            seen += [item.batch_id for item in page.items]
            cursor = page.next_cursor
        assert seen == [str(UUID(int=index)) for index in range(35, 0, -1)]
        current = await store.history(scope, limit=32)
        assert current.total == 37 and current.items[0].batch_id == str(UUID(int=37))
        assert_public(current.model_dump(mode="json"))
    finally:
        workspace.close()


async def test_history_sql_never_loads_snapshots_receipts_or_mutates_proposals(
    repository, tmp_path
):
    store, scope, workspace, request = await setup(repository, tmp_path)
    statements = []
    try:
        accepted, _ = await store.reserve(scope, "files", workspace.fingerprint, request)
        execution = await store.execution(scope, accepted.batch_id)
        await apply_child(store, scope, workspace, execution.children[0])
        await store.finish_child(scope, execution.children[1].fence, error_code="denied")
        done = await store.finish(scope, accepted.batch_id, execution.token)

        def observe(_connection, _cursor, statement, _params, _context, _many):
            statements.append(statement.lower())

        event.listen(store.database.engine.sync_engine, "before_cursor_execute", observe)
        try:
            page = await store.history(scope)
        finally:
            event.remove(store.database.engine.sync_engine, "before_cursor_execute", observe)
        summary = page.items[0]
        assert summary.status == done.status == "partial"
        assert (
            summary.item_count == 2
            and summary.done_count == 1
            and summary.updated_at == done.updated_at
        )
        assert all(
            word not in statement
            for statement in statements
            for word in ("before_content", "after_content", "prepared", "receipt", "recovery")
        )
        assert not any(
            statement.lstrip().startswith(("insert", "update", "delete"))
            for statement in statements
        )
        reads = [statement for statement in statements if statement.lstrip().startswith("select")]
        assert len(reads) <= 5
    finally:
        workspace.close()


@pytest.mark.parametrize("linked", [False, True])
async def test_expired_parent_is_projected_without_closing_or_replaying_in_history(
    repository, tmp_path, linked
):
    store, scope, workspace, request = await setup(repository, tmp_path)
    try:
        accepted, _ = await store.reserve(scope, "files", workspace.fingerprint, request)
        execution = await store.execution(scope, accepted.batch_id)
        if linked:
            await reserve_child(store, scope, workspace, execution.children[0])
        async with store.database.sessions.begin() as sql:
            await sql.execute(
                update(MixedWorkspaceBatchRow)
                .where(MixedWorkspaceBatchRow.batch_id == accepted.batch_id)
                .values(lease_expires_at=time.time() - 1)
            )
        history = await store.history(scope)
        summary = history.items[0]
        assert summary.producer_status == "interrupted"
        assert summary.status == ("uncertain" if linked else "interrupted")
        assert summary.stop_reason == "workspace_mixed_batch_lease_expired"
        async with store.database.sessions() as sql:
            row = await sql.get(MixedWorkspaceBatchRow, accepted.batch_id)
            assert row.producer_status == "running"
        details = await store.get(scope, accepted.batch_id)
        assert (
            details.status == summary.status and details.producer_status == summary.producer_status
        )
        assert not (tmp_path / "file.txt").exists() and not (tmp_path / "empty").exists()
    finally:
        workspace.close()


@pytest.mark.parametrize("limit", [0, 33, -1, True, 1.5, "2"])
async def test_store_invalid_history_limit_is_rejected(repository, tmp_path, limit):
    store, scope, workspace, _request = await setup(repository, tmp_path)
    try:
        with pytest.raises(ServiceError) as denied:
            await store.history(scope, limit=limit)
        assert denied.value.code == "workspace_mixed_batch_history_limit_invalid"
    finally:
        workspace.close()


async def test_cursor_domains_foreign_scope_missing_anchor_and_encoding_are_rejected(
    repository, tmp_path
):
    store, scope, workspace, request = await setup(repository, tmp_path)
    try:
        await seed(store, scope, request.targets[0].proposal_id, 3)
        page = await store.history(scope, limit=1)
        foreign = "topic:" + (await repository.create_topic("foreign history")).id
        assert (await store.history(foreign)).items == []
        for value, wanted_scope in (
            (page.next_cursor, foreign),
            ("bad", scope),
            (page.next_cursor.removeprefix("krim-mixed-history:"), scope),
            (page.next_cursor + "=", scope),
            (
                "krim-mixed-history:"
                + _history_cursor(
                    scope, (datetime(2026, 1, 1), "missing"), (datetime(2026, 1, 1), "missing")
                ),
                scope,
            ),
        ):
            with pytest.raises(ServiceError) as denied:
                await store.history(wanted_scope, cursor=value)
            assert denied.value.code == "workspace_mixed_batch_history_cursor_invalid"
    finally:
        workspace.close()


@pytest.mark.parametrize("source", ["file", "directory"])
async def test_history_corrupt_original_revision_reference_cannot_report_success(
    repository, tmp_path, source
):
    store, scope, workspace, request = await setup(repository, tmp_path)
    try:
        accepted, _ = await store.reserve(scope, "files", workspace.fingerprint, request)
        dispatch = (await store.execution(scope, accepted.batch_id)).children[
            0 if source == "file" else 1
        ]
        await apply_child(store, scope, workspace, dispatch)
        async with store.database.sessions.begin() as sql:
            await sql.execute(
                update(MixedWorkspaceBatchChildRow)
                .where(
                    MixedWorkspaceBatchChildRow.batch_id == accepted.batch_id,
                    MixedWorkspaceBatchChildRow.position == dispatch.fence.position,
                )
                .values(expected_revision=99)
            )
        with pytest.raises(ServiceError) as corrupt:
            await store.history(scope)
        assert corrupt.value.code == "workspace_mixed_batch_corrupt"
    finally:
        workspace.close()


async def test_history_5000_guard_does_not_silently_truncate(repository, tmp_path):
    store, scope, workspace, request = await setup(repository, tmp_path)
    try:
        await seed(store, scope, request.targets[0].proposal_id, 5001)
        with pytest.raises(ServiceError) as refused:
            await store.history(scope)
        assert refused.value.code == "workspace_mixed_batch_history_too_large"
    finally:
        workspace.close()


async def test_sdk_rest_history_reads_original_batch_and_rejects_duplicate_query(runtime, tmp_path):
    async with native_api(runtime, tmp_path) as (app, api, agent):
        request = await request_for(api, agent)
        base = f"/v1/agents/{agent}/workspace/mixed-batches"
        started = await api.post(base, params={"confirmed": "true"}, json=request)
        identifier = started.json()["batch_id"]
        done = (await api.post(f"{base}/{identifier}/wait")).json()
        response = await api.get(f"{base}/history")
        assert response.status_code == 200, response.text
        value = response.json()
        assert value["total"] == 1 and value["items"][0]["batch_id"] == identifier
        assert value["items"][0]["done_count"] == 2
        assert_public(value)
        # 原by-key仍由原路由处理，history不能遮蔽动态batch_id或by-key。
        assert (
            await api.get(f"{base}/by-key", params={"request_key": request["request_key"]})
        ).json() == done
        sdk = AgentSession(app.state.agents, agent)
        assert (await sdk.mixed_change_batch_history()).model_dump(mode="json") == value
        for params in (
            {"limit": 0},
            {"limit": 33},
            {"cursor": "bad"},
            {"offset": 0},
            [("limit", 1), ("limit", 2)],
        ):
            assert (await api.get(f"{base}/history", params=params)).status_code == 422


async def test_tcp_client_cli_history_is_metadata_only(runtime, tmp_path):
    async with native_api(runtime, tmp_path) as (_app, api, agent):
        request = await request_for(api, agent)
        base = f"/v1/agents/{agent}/workspace/mixed-batches"
        started = await api.post(base, params={"confirmed": "true"}, json=request)
        await api.post(f"{base}/{started.json()['batch_id']}/wait")
        app = _app
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        listener.listen(128)
        listener.setblocking(False)
        url = f"http://127.0.0.1:{listener.getsockname()[1]}"
        server = uvicorn.Server(uvicorn.Config(app, lifespan="off", log_level="error"))
        task = asyncio.create_task(server.serve(sockets=[listener]))
        try:
            async with asyncio.timeout(10):
                while not server.started:
                    if task.done():
                        await task
                    await asyncio.sleep(0.01)

            def read():
                with KrimClient(url) as client:
                    return client.agent_workspace_mixed_batch_history(agent, limit=1)

            expected = await asyncio.to_thread(read)
            code, output, errors = await cli(url, "mixed-batch-history", agent, "--limit", 1)
            assert code == 0, errors
            assert json.loads(output) == expected.model_dump(mode="json")
            assert expected.items[0].done_count == 2
        finally:
            server.should_exit = True
            await asyncio.wait_for(task, 15)
            listener.close()
