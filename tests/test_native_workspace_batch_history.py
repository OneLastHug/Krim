"""批次历史发现：稳定翻页、只读账本、轻量投影及公开消费入口。"""

import asyncio
import base64
import json
import socket
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
import uvicorn
from sqlalchemy import event, insert, update
from test_native_workspace_batch_store import ledger as ledger  # noqa: F401
from test_native_workspace_public_api import (
    apply,
    assert_public,
    batch_request,
    native_api,
    run_cli,
)
from test_workspace_changes import allow, setup
from test_workspace_effect_chain import apply_content

from krim_agent.api import create_app
from krim_agent.client import ClientError, KrimClient
from krim_agent.persistence import KrimRepository
from krim_agent.persistence.database import (
    NativeWorkspaceBatchChildRow,
    NativeWorkspaceBatchRow,
    WorkspaceEditAttemptRow,
)
from krim_agent.protocol.domain import ServiceError
from krim_agent.providers import FakeProvider
from krim_agent.sdk import AgentSession, NativeBatchHistoryPage, NativeBatchSummary, PythonAI
from krim_agent.workspace_change import FileChangeReceipt


async def seed_history(database, scope, count, *, start=1, created=None, proposal_id=None):
    """只为分页量与并列排序构造账本；文件动作真实性由下方真实批次测试覆盖。"""
    created = created or datetime(2026, 1, 1, tzinfo=UTC)
    rows = [
        {
            "batch_id": str(UUID(int=index)),
            "scope_id": scope,
            "resource_fingerprint": "a" * 64,
            "workspace_key": "files",
            "request_key": f"history/../?key={index}#%",
            "request_hash": "b" * 64,
            "snapshot_revision": "c" * 64,
            "token": "d" * 64,
            "action": "keep",
            "producer_status": "cancelled",
            "stop_reason": "workspace_batch_cancelled",
            "created_at": created,
            "updated_at": created,
        }
        for index in range(start, start + count)
    ]
    async with database.sessions.begin() as sql:
        await sql.execute(insert(NativeWorkspaceBatchRow), rows)
        if proposal_id is not None:
            await sql.execute(
                insert(NativeWorkspaceBatchChildRow),
                [
                    {
                        "batch_id": row["batch_id"],
                        "position": 0,
                        "proposal_id": proposal_id,
                        "expected_revision": 2,
                        "path": "file.txt",
                        "file_key": "e" * 64,
                        "apply_sequence": 1,
                        "request_key": f"krim-batch:{row['batch_id']}:0",
                        "admission_open": False,
                        "status": "skipped",
                        "error_code": "workspace_batch_cancelled",
                    }
                    for row in rows
                ],
            )
    return [row["batch_id"] for row in rows]


@pytest.mark.parametrize("count", [33, 65])
async def test_history_keyset_covers_all_ties_and_keeps_upper_bound_during_updates(ledger, count):
    proposal = await ledger.apply()
    ids = await seed_history(
        ledger.database, ledger.scope, count, proposal_id=proposal.view.proposal_id
    )
    page = await ledger.store.history(ledger.scope)
    assert page.total == count and len(page.items) == 32 and page.next_cursor
    assert page.items[0].item_count == 1 and page.items[0].done_count == 0
    seen = [item.batch_id for item in page.items]
    # 已读行和未读行状态、更新时间均可变化；另有新的创建时间更晚的批次。
    async with ledger.database.sessions.begin() as sql:
        await sql.execute(
            update(NativeWorkspaceBatchRow).values(
                producer_status="interrupted", updated_at=datetime(2026, 10, 1, tzinfo=UTC)
            )
        )
    await seed_history(
        ledger.database,
        ledger.scope,
        1,
        start=count + 1,
        created=datetime(2026, 2, 1, tzinfo=UTC),
        proposal_id=proposal.view.proposal_id,
    )
    while page.next_cursor:
        page = await ledger.store.history(ledger.scope, cursor=page.next_cursor)
        assert page.total == count and all(item.status == "interrupted" for item in page.items)
        seen.extend(item.batch_id for item in page.items)
    assert seen == list(reversed(ids)) and len(set(seen)) == count
    refreshed = await ledger.store.history(ledger.scope)
    assert refreshed.total == count + 1 and refreshed.items[0].batch_id == str(UUID(int=count + 1))


async def test_history_empty_scope_and_bounds(ledger):
    page = await ledger.store.history(ledger.scope)
    assert page.items == [] and page.total == 0 and page.next_cursor is None
    for limit in (0, 33, True, "32", 1.5):
        with pytest.raises(ServiceError) as caught:
            await ledger.store.history(ledger.scope, limit=limit)
        assert caught.value.code == "workspace_batch_history_limit_invalid"
    with pytest.raises(ServiceError) as missing:
        await ledger.store.history("topic:missing")
    assert missing.value.status == 404


async def test_history_rejects_malformed_foreign_and_noncanonical_cursors(ledger):
    await seed_history(ledger.database, ledger.scope, 3)
    page = await ledger.store.history(ledger.scope, limit=1)
    cursor = page.next_cursor
    other = await KrimRepository(ledger.database).create_topic("其他历史作用域")
    with pytest.raises(ServiceError) as foreign:
        await ledger.store.history("topic:" + other.id, cursor=cursor)
    assert foreign.value.code == "workspace_batch_history_cursor_invalid"
    value = json.loads(base64.urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4)))
    candidates = ["", "x" * 1025, "???", cursor + "=", "e30"]
    for field, invalid in (
        ("v", True),
        ("v", 2),
        ("scope", "topic:other"),
        ("upper", ["2026-01-01", str(UUID(int=3))]),
        ("before", ["2026-01-01T00:00:00+00:00", "missing"]),
        ("before", ["2026-01-01T00:00:00+00:00", str(UUID(int=0))]),
        ("before", ["2027-01-01T00:00:00+00:00", str(UUID(int=3))]),
    ):
        raw = json.dumps({**value, field: invalid}, sort_keys=True, separators=(",", ":")).encode()
        candidates.append(base64.urlsafe_b64encode(raw).decode().rstrip("="))
    # 语义相同但空格/重复字段不同的JSON不被接纳为规范服务游标。
    raw = json.dumps(value).encode()
    candidates.append(base64.urlsafe_b64encode(raw).decode().rstrip("="))
    raw = json.dumps(value, sort_keys=True, separators=(",", ":")).replace("{", '{"v":1,', 1)
    candidates.append(base64.urlsafe_b64encode(raw.encode()).decode().rstrip("="))
    for invalid in candidates:
        with pytest.raises(ServiceError) as caught:
            await ledger.store.history(ledger.scope, cursor=invalid)
        assert caught.value.code == "workspace_batch_history_cursor_invalid", invalid


async def test_history_hard_limit_is_per_scope_and_never_silently_truncates(ledger):
    await seed_history(ledger.database, ledger.scope, 5000)
    assert (await ledger.store.history(ledger.scope)).total == 5000
    other = await KrimRepository(ledger.database).create_topic("额外历史作用域")
    await seed_history(ledger.database, "topic:" + other.id, 1, start=5001)
    assert (await ledger.store.history(ledger.scope)).total == 5000
    await seed_history(ledger.database, ledger.scope, 1, start=5002)
    with pytest.raises(ServiceError) as caught:
        await ledger.store.history(ledger.scope)
    assert caught.value.code == "workspace_batch_history_too_large" and caught.value.status == 409


async def test_history_is_read_only_even_after_lease_expiry(ledger):
    proposal = await ledger.apply()
    batch, _ = await ledger.reserve(await ledger.request([proposal]))
    await ledger.expire(batch.batch_id)
    statements = []

    def capture(_conn, _cursor, statement, *_args):
        statements.append(statement)

    event.listen(ledger.database.engine.sync_engine, "before_cursor_execute", capture)
    try:
        page = await ledger.store.history(ledger.scope)
    finally:
        event.remove(ledger.database.engine.sync_engine, "before_cursor_execute", capture)
    assert page.items[0].producer_status == "running" and page.items[0].status == "running"
    assert not any(
        sql.lstrip().upper().startswith(("UPDATE", "INSERT", "DELETE")) for sql in statements
    )
    async with ledger.database.sessions() as sql:
        stored = await sql.get(NativeWorkspaceBatchRow, batch.batch_id)
        assert stored.producer_status == "running" and stored.owner_id == ledger.edits.owner_id
    # 点击详情才使用既有过期收束逻辑。
    assert (await ledger.store.get(ledger.scope, batch.batch_id)).producer_status == "interrupted"


async def test_history_cancelled_producer_keeps_unsettled_attempt_uncertain(ledger):
    proposal = await ledger.apply()
    batch, _ = await ledger.reserve(await ledger.request([proposal]))
    execution = await ledger.store.execution(ledger.scope, batch.batch_id)
    dispatch = execution.children[0]
    attempt, _ = await ledger.edits.reserve(
        ledger.scope,
        dispatch.request,
        ledger.workspace.fingerprint,
        proposal.prepared,
        batch_fence=dispatch.fence,
    )
    detail = await ledger.store.cancel(ledger.scope, batch.batch_id)
    summary = (await ledger.store.history(ledger.scope)).items[0]
    assert summary.status == detail.status == "uncertain"
    assert summary.producer_status == detail.producer_status == "cancelled"
    assert summary.item_count == 1 and summary.done_count == 0
    await ledger.edits.settle(
        ledger.scope,
        attempt.view.operation_id,
        FileChangeReceipt(
            path="file.txt",
            status="rejected",
            changed=False,
            before_sha256=proposal.prepared.before_sha256,
            after_sha256=proposal.prepared.after_sha256,
            error_code="workspace_batch_fenced",
        ),
    )
    summary = (await ledger.store.history(ledger.scope)).items[0]
    assert summary.status == "cancelled" and summary.done_count == 0


async def test_history_uses_operation_status_and_time_without_loading_private_json_or_bytes(ledger):
    first, second = await ledger.apply(), await ledger.apply("other", "other.txt")
    batch, _ = await ledger.reserve(await ledger.request([first, second]))
    execution = await ledger.store.execution(ledger.scope, batch.batch_id)
    dispatch = execution.children[0]
    attempt, _ = await ledger.edits.reserve(
        ledger.scope,
        dispatch.request,
        ledger.workspace.fingerprint,
        second.prepared,
        batch_fence=dispatch.fence,
    )
    await ledger.edits.settle(
        ledger.scope,
        attempt.view.operation_id,
        FileChangeReceipt(
            path="other.txt",
            status="done",
            changed=False,
            before_sha256=second.prepared.before_sha256,
            after_sha256=second.prepared.after_sha256,
        ),
    )
    await ledger.store.finish_child(
        ledger.scope, execution.children[1].fence, error_code="tool_denied"
    )
    await ledger.store.finish(ledger.scope, batch.batch_id, execution.token)
    detail = await ledger.store.get(ledger.scope, batch.batch_id)
    future = detail.updated_at + timedelta(seconds=10)
    # 损坏仅存在于测试副本的私有JSON；历史摘要不解析它或调用全量_view。
    async with ledger.database.sessions.begin() as sql:
        await sql.execute(
            update(WorkspaceEditAttemptRow)
            .where(WorkspaceEditAttemptRow.operation_id == attempt.view.operation_id)
            .values(receipt={"private_invalid_json_canary": True}, updated_at=future)
        )
    statements = []

    def capture(_conn, _cursor, statement, *_args):
        statements.append(statement)

    event.listen(ledger.database.engine.sync_engine, "before_cursor_execute", capture)
    try:
        page = await ledger.store.history(ledger.scope)
    finally:
        event.remove(ledger.database.engine.sync_engine, "before_cursor_execute", capture)
    summary = page.items[0]
    assert summary.status == detail.status == "partial" and summary.item_count == 2
    assert summary.done_count == 1 and summary.updated_at == future
    assert isinstance(summary, NativeBatchSummary)
    for sql in statements:
        if "workspace_edit_attempts" in sql or "native_workspace_batch" in sql:
            assert not any(
                f".{column}" in sql
                for column in (
                    "receipt",
                    "prepared",
                    "before_content",
                    "after_content",
                    "recovery",
                    "token",
                    "file_key",
                )
            )
    assert_public(page.model_dump(mode="json"))


async def test_sdk_history_survives_closed_session_and_resource_replacement(runtime, tmp_path):
    service, sdk, _ = await setup(runtime, tmp_path)
    try:
        proposal, _ = await apply_content(sdk, tmp_path / "sdk.txt", "B", "first")
        page = await sdk.file_change_list()
        from test_native_workspace_batches import request

        batch = await sdk.start_file_change_batch(request(page, "keep", "sdk-history"))
        completed = await sdk.wait_file_change_batch(batch.batch_id)
        assert completed.status == "done"
        state = await sdk.state()
        await service.close_session(sdk.agent_id, state.revision)
        await service.close()

        service = type(service)(runtime, runtime.settings)
        service.register_workspace("files", tmp_path, version="changed", writable=False)
        sdk = AgentSession(service, sdk.agent_id)
        history = await sdk.file_change_batch_history(limit=1)
        assert isinstance(history, NativeBatchHistoryPage)
        assert history.total == 1 and history.items[0].batch_id == batch.batch_id
        assert history.items[0].done_count == 1
        assert proposal.proposal_id == completed.items[0].proposal_id
    finally:
        await service.close()


@pytest.mark.parametrize("query", ["history", "list"])
async def test_default_memory_sdk_query_serializes_with_batch_heartbeat(
    query, tmp_path, monkeypatch
):
    from test_native_workspace_batches import request

    # 不传settings，真实使用PythonAI默认的共享SQLite内存连接。
    async with await PythonAI.create(provider=FakeProvider(delay=0)) as ai:
        ai.runtime.tool_policy = allow
        service = ai.agents()
        binding = service.register_workspace(
            "local", tmp_path, version="v1", default=True, writable=True
        )
        sdk = await ai.agent()
        await apply_content(sdk, tmp_path / "memory.txt", "new", "memory")
        page = await sdk.file_change_list()
        database, store = ai.runtime.repository.database, service.workspace_batch_store
        assert database.engine.url.database == ":memory:"
        scope = "session:" + (await sdk.state()).session_id
        batch, _ = await store.reserve(scope, request(page), binding.fingerprint, "local")
        execution = await store.execution(scope, batch.batch_id)
        entered, release, heartbeat_started = asyncio.Event(), asyncio.Event(), asyncio.Event()
        blocked = []

        class HandshakeLock(asyncio.Lock):
            async def acquire(self):
                if asyncio.current_task().get_name() == "native-query-heartbeat":
                    blocked.append(self.locked())
                    heartbeat_started.set()
                return await super().acquire()

        # 在真实SELECT已开启事务后暂停读方；续租显式尝试同一进程锁后才放行。
        monkeypatch.setattr(database, "write_lock", HandshakeLock())
        original_scope = store.edits._scope

        async def paused_scope(sql, selected_scope, **kwargs):
            result = await original_scope(sql, selected_scope, **kwargs)
            if asyncio.current_task().get_name() == "native-query-reader" and not entered.is_set():
                entered.set()
                await release.wait()
            return result

        monkeypatch.setattr(store.edits, "_scope", paused_scope)
        reader_sql = []

        def capture(_conn, _cursor, statement, *_args):
            if asyncio.current_task().get_name() == "native-query-reader":
                reader_sql.append(statement)

        event.listen(database.engine.sync_engine, "before_cursor_execute", capture)
        reader = asyncio.create_task(
            sdk.file_change_batch_history() if query == "history" else sdk.file_change_list(),
            name="native-query-reader",
        )
        heartbeat = None
        try:
            await asyncio.wait_for(entered.wait(), 5)
            heartbeat = asyncio.create_task(
                store.renew(batch.batch_id, execution.token), name="native-query-heartbeat"
            )
            await asyncio.wait_for(heartbeat_started.wait(), 5)
            assert blocked == [True] and not heartbeat.done()
            release.set()
            result, _ = await asyncio.wait_for(asyncio.gather(reader, heartbeat), 5)
            assert result.total == 1 and len(result.items) == 1
            assert not any(
                sql.lstrip().upper().startswith(("UPDATE", "INSERT", "DELETE"))
                for sql in reader_sql
            )
            # 续租真实写事务完成，原文件和批次仍可查询。
            detail = await sdk.file_change_batch(batch.batch_id)
            assert detail.producer_status == "running"
            assert (tmp_path / "memory.txt").read_text() == "new"
        finally:
            release.set()
            await asyncio.gather(
                reader, *([heartbeat] if heartbeat is not None else []), return_exceptions=True
            )
            event.remove(database.engine.sync_engine, "before_cursor_execute", capture)


async def test_rest_history_route_and_structured_errors_are_separate_from_detail(runtime, tmp_path):
    async with native_api(runtime, tmp_path) as (app, api, agent_id):
        await apply(api, agent_id, "first.txt")
        base = f"/v1/agents/{agent_id}/workspace"
        page = (await api.get(f"{base}/native-changes")).json()
        request = batch_request(page, key="history/../?confirmed=false#%")
        started = await api.post(f"{base}/native-batches", params={"confirmed": True}, json=request)
        batch_id = started.json()["batch_id"]
        detail = (await api.post(f"{base}/native-batches/{batch_id}/wait")).json()
        assert detail["status"] == "done"
        history = await api.get(f"{base}/native-batches/history")
        assert history.status_code == 200, history.text
        value = history.json()
        assert value["items"][0]["request_key"] == request["request_key"]
        assert value["items"][0]["done_count"] == 1
        assert "item_count" not in detail and "source" not in value["items"][0]
        assert_public(value)
        for params in ({"limit": 0}, {"limit": 33}, {"cursor": "bad"}, {"cursor": "x" * 1025}):
            response = await api.get(f"{base}/native-batches/history", params=params)
            assert response.status_code == 422 and response.json()["error"]["code"]
        session = (await api.get(f"/v1/agents/{agent_id}/session")).json()
        await api.post(
            f"/v1/agents/{agent_id}/session/close", json={"expected_revision": session["revision"]}
        )
        assert (await api.get(f"{base}/native-batches/history")).json() == value
        other = (await api.post("/v1/agents", json={"name": "default"})).json()["agent_id"]
        foreign = await api.get(f"/v1/agents/{other}/workspace/native-batches/history")
        assert foreign.json()["total"] == 0
        assert (
            await api.get("/v1/agents/missing/workspace/native-batches/history")
        ).status_code == 404


async def test_tcp_client_discovers_paged_history_and_keeps_structured_errors(runtime):
    app = create_app(runtime=runtime)
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(128)
    listener.setblocking(False)
    url = f"http://127.0.0.1:{listener.getsockname()[1]}"
    server = uvicorn.Server(uvicorn.Config(app, log_level="error"))
    task = asyncio.create_task(server.serve(sockets=[listener]))
    try:
        async with asyncio.timeout(10):
            while not server.started:
                if task.done():
                    await task
                await asyncio.sleep(0.01)
        agent = await app.state.agents.spawn("default")
        scope = "session:" + agent.session_id
        ids = await seed_history(runtime.repository.database, scope, 33)

        def flow():
            with KrimClient(url) as client:
                first = client.agent_workspace_native_batch_history(agent.agent_id)
                second = client.agent_workspace_native_batch_history(
                    agent.agent_id, cursor=first.next_cursor
                )
                assert isinstance(first, NativeBatchHistoryPage)
                assert [item.batch_id for item in first.items + second.items] == list(reversed(ids))
                assert second.next_cursor is None
                with pytest.raises(ClientError) as caught:
                    client.agent_workspace_native_batch_history(agent.agent_id, cursor="bad")
                assert caught.value.code == "workspace_batch_history_cursor_invalid"
                assert caught.value.status == 422
                return first, second

        first, second = await asyncio.to_thread(flow)
        for cursor, expected in ((None, first), (first.next_cursor, second)):
            args = ["batch-history", agent.agent_id, "--limit", "32"]
            if cursor is not None:
                args += ["--cursor", cursor]
            code, stdout, stderr = await run_cli(url, *args)
            assert code == 0, stderr
            assert json.loads(stdout) == expected.model_dump(mode="json")
    finally:
        server.should_exit = True
        await asyncio.wait_for(task, 10)
        listener.close()
