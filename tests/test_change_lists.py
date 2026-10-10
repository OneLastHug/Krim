import asyncio
import json
import sys

import httpx
import pytest
from alembic import command
from pydantic import ValidationError
from sqlalchemy import text

from krim_agent.app import LocalApp
from krim_agent.app_sdk import RemoteHostHandle
from krim_agent.application.change_lists import ChangeListService
from krim_agent.change_list import (
    ChangeInvoke,
    ChangeItemInput,
    ChangeItemResult,
    ChangeReceipt,
    ChangeSync,
)
from krim_agent.client import KrimClient
from krim_agent.persistence import ChangeListRepository, Database
from krim_agent.persistence.migration import migration_config
from krim_agent.protocol.domain import ServiceError


def items():
    return [
        ChangeItemInput(
            id="one", title="一号变更", metadata={"resource": "one", "before": "旧值", "version": 1}
        ),
        ChangeItemInput(
            id="two", title="二号变更", metadata={"resource": "two", "before": "旧值", "version": 2}
        ),
    ]


def receipt(execution, statuses=None):
    statuses = statuses or ["done"] * len(execution.items)
    return ChangeReceipt(
        **execution.model_dump(exclude={"items"}),
        results=[
            ChangeItemResult(
                item_id=item.id,
                status=status,
                error_code=None if status == "done" else "resource_conflict",
            )
            for item, status in zip(execution.items, statuses, strict=True)
        ],
    )


@pytest.mark.parametrize(
    "payload",
    [
        {"id": "../escape", "title": "标题"},
        {"id": "x\n", "title": "标题"},
        {"id": "ok", "title": "   "},
        {"id": "ok", "title": "标题", "metadata": {"number": 9007199254740992}},
        {"id": "ok", "title": "标题", "metadata": {"large": "😀" * 4096}},
        {"id": "ok", "title": "标题", "status": "undone"},
    ],
)
def test_untrusted_sync_cannot_claim_completion_or_exceed_boundaries(payload):
    with pytest.raises(ValidationError):
        ChangeItemInput.model_validate(payload)


@pytest.mark.parametrize(
    "action,item_id", [("undo", None), ("keep", None), ("undo-all", "one"), ("keep-all", "one")]
)
def test_single_and_batch_targets_are_unambiguous(action, item_id):
    with pytest.raises(ValidationError):
        ChangeInvoke(revision=1, action=action, item_id=item_id, idempotency_key="invalid")


def test_duplicate_and_aggregate_payload_limits():
    with pytest.raises(ValidationError):
        ChangeSync(revision=0, items=[items()[0], items()[0]])
    with pytest.raises(ValidationError):
        ChangeSync(
            revision=0,
            items=[
                ChangeItemInput(id=str(i), title="x", description="😀" * 2000) for i in range(5)
            ],
        )
    with pytest.raises(ValidationError):
        ChangeItemResult(item_id="one", status="done", error_code="fake")


async def test_sync_clear_revision_and_topic_boundaries_survive_reopen(repository):
    catalog = ChangeListRepository(repository.database)
    one, other = await repository.create_topic("清单"), await repository.create_topic("其他")
    assert (await catalog.get(one.id)).revision == 0
    saved = await catalog.sync(one.id, ChangeSync(revision=0, items=items()))
    assert saved.revision == 1 and all(item.status == "pending" for item in saved.items)
    assert not (await catalog.get(other.id)).items
    with pytest.raises(ServiceError) as stale:
        await catalog.sync(one.id, ChangeSync(revision=0))
    assert stale.value.code == "change_revision_conflict"
    reopened = Database(str(repository.database.engine.url))
    try:
        await reopened.initialize()
        assert await ChangeListRepository(reopened).get(one.id) == saved
    finally:
        await reopened.close()
    cleared = await catalog.sync(one.id, ChangeSync(revision=1))
    assert cleared.revision == 2 and not cleared.items
    with pytest.raises(ServiceError) as missing:
        await catalog.get("missing")
    assert missing.value.status == 404


@pytest.mark.parametrize("action", ["undo", "keep", "undo-all", "keep-all"])
async def test_local_app_actions_apply_real_files_and_preserve_bound_snapshot(
    runtime, tmp_path, action
):
    local = LocalApp(runtime)
    paths = {name: tmp_path / f"{name}.txt" for name in ("one", "two")}
    for path in paths.values():
        path.write_text("新值")
    kept = tmp_path / "kept.json"
    observed = []

    async def apply(execution):
        observed.append(execution)
        assert (
            await local.handle.change_list(execution.topic_id)
        ).active_operation_id == execution.operation_id
        for item in execution.items:
            assert item.metadata == next(
                source.metadata for source in items() if source.id == item.id
            )
            path = paths[item.metadata["resource"]]
            if execution.action.startswith("undo"):
                path.write_text(item.metadata["before"])
            else:
                kept.write_text(json.dumps([item.id for item in execution.items]))
        return receipt(execution)

    local.handle.on_change_invoke(apply)
    try:
        topic = await local.handle.create_topic("本地宿主")
        view = await local.handle.sync_changes(topic.id, ChangeSync(revision=0, items=items()))
        request = ChangeInvoke(
            revision=view.revision,
            action=action,
            item_id="one" if "-all" not in action else None,
            idempotency_key="once",
        )
        result = await local.handle.invoke_change(topic.id, request)
        assert result.status == "done" and len(observed) == 1
        targets = ["one"] if "-all" not in action else ["one", "two"]
        if action.startswith("undo"):
            assert all(paths[name].read_text() == "旧值" for name in targets)
        else:
            assert json.loads(kept.read_text()) == targets
            assert all(paths[name].read_text() == "新值" for name in targets)
        final = await local.handle.change_list(topic.id)
        assert final.revision == 3 and final.active_operation_id is None
        assert [item.status for item in final.items] == [
            ("undone" if action.startswith("undo") else "kept") if item.id in targets else "pending"
            for item in final.items
        ]
        assert await local.handle.invoke_change(topic.id, request) == result
        assert len(observed) == 1
        assert not (await local.handle.clear_changes(topic.id, final.revision)).items
    finally:
        await local.close()


async def test_concurrent_retry_shares_execution_and_busy_list_cannot_be_replaced(repository):
    topic = await repository.create_topic("并发")
    catalog = ChangeListRepository(repository.database)
    service = ChangeListService(catalog)
    await catalog.sync(topic.id, ChangeSync(revision=0, items=items()))
    entered, release = asyncio.Event(), asyncio.Event()
    calls = []

    async def apply(execution):
        calls.append(execution)
        entered.set()
        await release.wait()
        return receipt(execution)

    request = ChangeInvoke(revision=1, action="undo-all", idempotency_key="once")
    first = asyncio.create_task(service.invoke(topic.id, request, apply))
    try:
        await asyncio.wait_for(entered.wait(), 3)
        repeat = asyncio.create_task(service.invoke(topic.id, request, apply))
        current = await catalog.get(topic.id)
        assert current.revision == 2 and all(item.status == "executing" for item in current.items)
        with pytest.raises(ServiceError) as busy:
            await catalog.sync(topic.id, ChangeSync(revision=2))
        assert busy.value.code == "change_list_busy"
        with pytest.raises(ServiceError) as conflict:
            await service.invoke(topic.id, request.model_copy(update={"action": "keep-all"}), apply)
        assert conflict.value.code == "idempotency_conflict"
        with pytest.raises(ServiceError):
            await service.invoke(
                topic.id,
                ChangeInvoke(revision=2, action="keep-all", idempotency_key="different"),
                apply,
            )
        release.set()
        one, two = await asyncio.gather(first, repeat)
        assert one == two and len(calls) == 1
    finally:
        release.set()
        await asyncio.gather(first, return_exceptions=True)
        await service.close()


async def test_batch_partial_results_only_settle_actual_successes_and_keep_audit(
    repository, tmp_path
):
    topic = await repository.create_topic("部分完成")
    catalog, calls = ChangeListRepository(repository.database), []
    service = ChangeListService(catalog)
    await catalog.sync(topic.id, ChangeSync(revision=0, items=items()))
    resource = tmp_path / "one.txt"
    resource.write_text("新值")

    async def apply(execution):
        calls.append(execution)
        resource.write_text("旧值")
        return receipt(execution, ["done", "error"])

    try:
        request = ChangeInvoke(revision=1, action="undo-all", idempotency_key="partial")
        operation = await service.invoke(topic.id, request, apply)
        assert operation.status == "error" and resource.read_text() == "旧值"
        assert [item.status for item in (await catalog.get(topic.id)).items] == [
            "undone",
            "pending",
        ]
        assert await service.invoke(topic.id, request, apply) == operation
        assert len(calls) == 1
        assert await catalog.history(topic.id, operation.execution.operation_id) == [
            operation.receipt
        ]
        with pytest.raises(ServiceError):
            await catalog.operation(
                (await repository.create_topic("其他")).id, operation.execution.operation_id
            )
    finally:
        await service.close()


@pytest.mark.parametrize(
    "mode",
    [
        "exception",
        "timeout",
        "wrong_operation",
        "wrong_topic",
        "wrong_revision",
        "wrong_action",
        "missing_item",
        "extra_item",
        "invalid_output",
    ],
)
async def test_unconfirmed_callbacks_are_uncertain_and_never_replayed(repository, mode):
    topic = await repository.create_topic(mode)
    catalog = ChangeListRepository(repository.database)
    service = ChangeListService(catalog)
    await catalog.sync(topic.id, ChangeSync(revision=0, items=items()))
    calls = []

    async def apply(execution):
        calls.append(execution)
        if mode == "exception":
            raise RuntimeError("private executor detail")
        if mode == "timeout":
            await asyncio.Event().wait()
        if mode == "invalid_output":
            return object()
        value = receipt(execution).model_dump(mode="json")
        if mode == "wrong_operation":
            value["operation_id"] = "other"
        elif mode == "wrong_topic":
            value["topic_id"] = "other"
        elif mode == "wrong_revision":
            value["revision"] += 1
        elif mode == "wrong_action":
            value["action"] = "keep-all"
        elif mode == "missing_item":
            value["results"].pop()
        elif mode == "extra_item":
            value["results"].append({"item_id": "other", "status": "done"})
        return ChangeReceipt.model_validate(value)

    try:
        request = ChangeInvoke(
            revision=1, action="undo-all", idempotency_key="once", timeout_seconds=0.05
        )
        result = await service.invoke(topic.id, request, apply)
        assert result.status == "uncertain" and all(
            item.status == "uncertain" for item in (await catalog.get(topic.id)).items
        )
        assert "private executor detail" not in result.model_dump_json()
        assert await service.invoke(topic.id, request, apply) == result
        assert len(calls) == 1
        with pytest.raises(ServiceError):
            await catalog.sync(topic.id, ChangeSync(revision=3))
    finally:
        await service.close()


async def test_cancel_after_file_write_needs_verified_resolution_and_retains_history(
    repository, tmp_path
):
    topic = await repository.create_topic("取消")
    catalog = ChangeListRepository(repository.database)
    service = ChangeListService(catalog)
    await catalog.sync(topic.id, ChangeSync(revision=0, items=items()[:1]))
    resource = tmp_path / "actual.txt"
    resource.write_text("新值")
    started = asyncio.Event()

    async def apply(execution):
        resource.write_text("旧值")
        started.set()
        await asyncio.Event().wait()
        return receipt(execution)

    request = ChangeInvoke(revision=1, action="undo", item_id="one", idempotency_key="cancel")
    pending = asyncio.create_task(service.invoke(topic.id, request, apply))
    try:
        await asyncio.wait_for(started.wait(), 3)
        operation_id = (await catalog.get(topic.id)).active_operation_id
        result = await service.cancel(topic.id, operation_id)
        await asyncio.gather(pending, return_exceptions=True)
        assert result.status == "uncertain" and resource.read_text() == "旧值"
        assert result.receipt.results[0].error_code == "cancelled"
        assert await service.invoke(topic.id, request, apply) == result
        resolved = await catalog.settle(receipt(result.execution), resolve=True)
        assert (
            resolved.status == "done" and (await catalog.get(topic.id)).items[0].status == "undone"
        )
        assert len(await catalog.history(topic.id, operation_id)) == 2
        assert (await catalog.history(topic.id, operation_id))[0].results[0].status == "uncertain"
        assert await catalog.settle(receipt(result.execution), resolve=True) == resolved
        with pytest.raises(ServiceError):
            await catalog.settle(receipt(result.execution, ["error"]), resolve=True)
    finally:
        await service.close()


async def test_waiter_disconnect_does_not_restart_or_lose_live_operation(repository):
    topic = await repository.create_topic("消费者断线")
    catalog = ChangeListRepository(repository.database)
    service = ChangeListService(catalog)
    await catalog.sync(topic.id, ChangeSync(revision=0, items=items()))
    started, release = asyncio.Event(), asyncio.Event()
    calls = []

    async def apply(execution):
        calls.append(execution)
        started.set()
        await release.wait()
        return receipt(execution)

    request = ChangeInvoke(revision=1, action="keep-all", idempotency_key="once")
    waiter = asyncio.create_task(service.invoke(topic.id, request, apply))
    try:
        await asyncio.wait_for(started.wait(), 3)
        waiter.cancel()
        await asyncio.gather(waiter, return_exceptions=True)
        assert (await catalog.operations(topic.id))[0].status == "executing"
        release.set()
        result = await service.invoke(topic.id, request, apply)
        assert result.status == "done" and len(calls) == 1
    finally:
        release.set()
        await service.close()


async def test_process_crash_recovers_uncertain_without_replaying_actual_file_write(tmp_path):
    path = tmp_path / "crash.sqlite"
    resource = tmp_path / "resource.txt"
    resource.write_text("新值")
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "tests/fixtures/change_crash_writer.py",
        str(path),
        str(resource),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    output, errors = await asyncio.wait_for(process.communicate(), 10)
    assert process.returncode == 0, errors.decode()
    identifiers = json.loads(output)
    database = Database(f"sqlite+aiosqlite:///{path}")
    try:
        await database.initialize()
        catalog = ChangeListRepository(database)
        assert await catalog.recover_interrupted() == 1
        assert await catalog.recover_interrupted() == 0
        operation = await catalog.operation(identifiers["topic_id"], identifiers["operation_id"])
        assert (
            operation.status == "uncertain"
            and operation.receipt.results[0].error_code == "process_interrupted"
        )
        assert resource.read_text() == "旧值"
        resolved = await catalog.settle(receipt(operation.execution), resolve=True)
        assert resolved.status == "done"
    finally:
        await database.close()


async def test_migration_backfills_old_topic_and_rejects_loss_of_audit(tmp_path):
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'migration.sqlite'}")
    try:
        async with database.engine.begin() as connection:
            await connection.run_sync(
                lambda sync: command.upgrade(migration_config(sync), "0012_skills")
            )
            await connection.execute(
                text(
                    "INSERT INTO topics (id,title,created_at,updated_at,active_branches) "
                    "VALUES ('old','原话题','2026-10-02','2026-10-02','{}')"
                )
            )
        await database.initialize()
        catalog = ChangeListRepository(database)
        assert not (await catalog.get("old")).items
        async with database.engine.begin() as connection:
            await connection.run_sync(
                lambda sync: command.downgrade(migration_config(sync), "0012_skills")
            )
        await database.initialize()
        await catalog.sync("old", ChangeSync(revision=0, items=items()))
        with pytest.raises(RuntimeError, match="审计"):
            async with database.engine.begin() as connection:
                await connection.run_sync(
                    lambda sync: command.downgrade(migration_config(sync), "0012_skills")
                )
        assert (await catalog.get("old")).revision == 1
        async with database.engine.connect() as connection:
            assert (
                await connection.execute(text("SELECT version_num FROM alembic_version"))
            ).scalar() == "0032_oauth_states"
            assert not (await connection.execute(text("PRAGMA foreign_key_check"))).all()
    finally:
        await database.close()


async def test_api_validates_stale_and_cross_topic_inputs_and_routes(api):
    topic = (await api.post("/v1/topics", json={"title": "API"})).json()["id"]
    base = f"/v1/topics/{topic}/change-list"
    assert (await api.get(base)).json()["revision"] == 0
    saved = await api.put(
        base, json={"revision": 0, "items": [item.model_dump(mode="json") for item in items()]}
    )
    assert saved.status_code == 200 and saved.json()["revision"] == 1
    assert (await api.put(base, json={"revision": 0, "items": []})).status_code == 409
    assert (
        await api.post(
            base + "/invoke",
            json={"revision": 1, "action": "undo", "item_id": "missing", "idempotency_key": "bad"},
        )
    ).status_code == 404
    assert (
        await api.post(base + "/click", json={"revision": 0, "item_id": "one"})
    ).status_code == 409
    assert (await api.post(base + "/click", json={"revision": 1, "item_id": "one"})).json()["item"][
        "metadata"
    ]["version"] == 1
    assert (await api.post(base + "/clear", json={"revision": 1})).json()["items"] == []
    assert (await api.get("/v1/topics/missing/change-list")).status_code == 404


async def test_generic_host_requests_cannot_dispatch_unbound_change_execution(api):
    topic = (await api.post("/v1/topics", json={"title": "禁止旁路"})).json()["id"]
    sid = (await api.post("/v1/host/sessions")).json()["id"]
    await api.post(f"/v1/host/sessions/{sid}/ready")
    unbound = {
        "envelope": {
            "protocol_version": 2,
            "kind": "call",
            "id": "forged",
            "channel": "changeList/invoke",
            "data": {
                "operation_id": "invented",
                "topic_id": topic,
                "revision": 0,
                "action": "undo",
                "items": [items()[0].model_dump(mode="json")],
            },
        }
    }
    refused = await api.post(f"/v1/host/sessions/{sid}/requests", json=unbound)
    assert refused.status_code == 422 and refused.json()["error"]["code"] == "change_input_invalid"
    assert (await api.get(f"/v1/topics/{topic}/change-list/operations")).json() == []


async def test_generic_host_request_validates_and_persists_before_real_callback(server_url):
    async with httpx.AsyncClient(base_url=server_url, trust_env=False) as api:
        topic = (await api.post("/v1/topics", json={"title": "通用信道执行"})).json()["id"]
        await api.put(
            f"/v1/topics/{topic}/change-list",
            json={"revision": 0, "items": [items()[0].model_dump(mode="json")]},
        )
        sid = (await api.post("/v1/host/sessions")).json()["id"]
        await api.post(f"/v1/host/sessions/{sid}/ready")
        observed = []
        async with RemoteHostHandle(server_url, sid) as host:

            async def apply(execution):
                observed.append(execution)
                saved = (
                    await api.get(
                        f"/v1/topics/{topic}/change-list/operations/{execution.operation_id}"
                    )
                ).json()
                assert (
                    saved["status"] == "executing" and saved["execution"]["items"][0]["id"] == "one"
                )
                return receipt(execution)

            host.on_change_invoke(apply)

            async def listen():
                async for _event in host.events():
                    pass

            listener = asyncio.create_task(listen())
            try:
                payload = {
                    "envelope": {
                        "protocol_version": 2,
                        "kind": "call",
                        "id": "app-request",
                        "channel": "changeList/invoke",
                        "data": {
                            "topic_id": topic,
                            "revision": 1,
                            "action": "undo",
                            "item_id": "one",
                            "idempotency_key": "generic-once",
                        },
                    }
                }
                completed = await api.post(f"/v1/host/sessions/{sid}/requests", json=payload)
                assert completed.status_code == 200 and completed.json()["data"]["status"] == "done"
                assert (
                    await api.post(f"/v1/host/sessions/{sid}/requests", json=payload)
                ).json() == completed.json()
                assert len(observed) == 1
            finally:
                listener.cancel()
                await asyncio.gather(listener, return_exceptions=True)


async def test_real_tcp_remote_callback_and_python_client_apply_file_once(server_url, tmp_path):
    resource = tmp_path / "tcp.txt"
    resource.write_text("新值")
    seen, clicked = [], asyncio.Event()
    async with httpx.AsyncClient(base_url=server_url, trust_env=False) as api:
        topic = (await api.post("/v1/topics", json={"title": "TCP"})).json()["id"]
        sid = (await api.post("/v1/host/sessions")).json()["id"]
        await api.post(f"/v1/host/sessions/{sid}/ready")
        async with RemoteHostHandle(server_url, sid) as host:

            async def apply(execution):
                seen.append(execution)
                resource.write_text(execution.items[0].metadata["before"])
                return receipt(execution)

            async def click(event):
                assert event.item.id == "one" and event.item.metadata["before"] == "旧值"
                clicked.set()
                return "ignored"

            host.on_change_invoke(apply)
            host.on_change_click(click)

            async def listen():
                async for _event in host.events():
                    pass

            listener = asyncio.create_task(listen())
            try:

                def prepare():
                    with KrimClient(server_url) as client:
                        return client.sync_changes(topic, ChangeSync(revision=0, items=items()[:1]))

                view = await asyncio.to_thread(prepare)
                await host.click_change(topic, view.revision, "one")
                await asyncio.wait_for(clicked.wait(), 3)
                request = ChangeInvoke(
                    revision=1, action="undo", item_id="one", idempotency_key="real"
                )
                result = await host.invoke_change(topic, request)
                assert result.status == "done" and resource.read_text() == "旧值"
                assert await host.invoke_change(topic, request) == result and len(seen) == 1
                assert (
                    await host.change_operation(topic, result.execution.operation_id)
                ).status == "done"

                def verify():
                    with KrimClient(server_url) as client:
                        current = client.change_list(topic)
                        assert current.items[0].status == "undone"
                        assert (
                            client.change_operation(topic, result.execution.operation_id).status
                            == "done"
                        )
                        assert not client.clear_changes(topic, current.revision).items

                await asyncio.to_thread(verify)
            finally:
                listener.cancel()
                await asyncio.gather(listener, return_exceptions=True)


async def test_remote_ui_commands_local_relay_and_click_snapshot_match(runtime):
    from krim_agent.api import create_app

    local = LocalApp(runtime)
    app = create_app(runtime=runtime)
    try:
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
            ) as api:
                topic = await local.handle.create_topic("UI 回程")
                sid = (await api.post("/v1/host/sessions")).json()["id"]
                await api.post(f"/v1/host/sessions/{sid}/ready")
                reader = app.state.host_sessions.events(sid, "app", 0)

                async def relay(pending):
                    task = asyncio.create_task(pending)
                    frame = await asyncio.wait_for(anext(reader), 3)
                    result = await local.handle.emit(frame.data["channel"], frame.data["data"])
                    await api.post(
                        f"/v1/host/sessions/{sid}/reply",
                        json={
                            "protocol_version": 2,
                            "kind": "reply",
                            "id": frame.data["id"],
                            "channel": frame.data["channel"],
                            "data": result.model_dump(mode="json"),
                        },
                    )
                    return await task

                async with RemoteHostHandle(
                    "http://127.0.0.1", sid, transport=httpx.ASGITransport(app=app)
                ) as host:
                    saved = await relay(
                        host.sync_changes(topic.id, ChangeSync(revision=0, items=items()))
                    )
                    assert await relay(host.change_list(topic.id)) == saved
                    click = await host.click_change(topic.id, saved.revision, "one")
                    forged = {
                        "protocol_version": 2,
                        "kind": "notify",
                        "id": "forged",
                        "channel": "changeList/item/click",
                        "data": click.model_dump(mode="json"),
                    }
                    forged["data"]["item"]["metadata"] = {"source": "forged"}
                    assert (
                        await api.post(f"/v1/host/sessions/{sid}/notify", json=forged)
                    ).status_code == 409
                    assert not (await relay(host.clear_changes(topic.id, saved.revision))).items
                await reader.aclose()
    finally:
        await local.close()


async def test_local_cancel_and_resolution_sdk_uses_original_execution(runtime):
    local = LocalApp(runtime)
    started = asyncio.Event()
    calls = []

    async def apply(execution):
        calls.append(execution)
        started.set()
        await asyncio.Event().wait()
        return receipt(execution)

    local.handle.on_change_invoke(apply)
    try:
        topic = await local.handle.create_topic("取消与核实 SDK")
        await local.handle.sync_changes(topic.id, ChangeSync(revision=0, items=items()[:1]))
        request = ChangeInvoke(revision=1, action="undo", item_id="one", idempotency_key="one")
        pending = asyncio.create_task(local.handle.invoke_change(topic.id, request))
        await asyncio.wait_for(started.wait(), 3)
        identifier = (await local.handle.change_list(topic.id)).active_operation_id
        cancelled = await local.handle.cancel_change(topic.id, identifier)
        await asyncio.gather(pending, return_exceptions=True)
        assert cancelled.status == "uncertain"
        assert await local.handle.change_operation(topic.id, identifier) == cancelled
        resolved = await local.handle.resolve_change(receipt(cancelled.execution))
        assert resolved.status == "done" and len(calls) == 1
        event = asyncio.Event()
        local.handle.on_change_click(lambda _click: event.set())
        view = await local.handle.change_list(topic.id)
        await local.handle.click_change(topic.id, view.revision, "one")
        await asyncio.wait_for(event.wait(), 3)
    finally:
        await local.close()


async def test_receipt_store_failure_preserves_uncertain_when_external_write_already_happened(
    repository, tmp_path
):
    topic = await repository.create_topic("回执存储故障")
    catalog = ChangeListRepository(repository.database)
    service = ChangeListService(catalog)
    await catalog.sync(topic.id, ChangeSync(revision=0, items=items()[:1]))
    resource = tmp_path / "store-failure.txt"
    resource.write_text("新值")
    settle = catalog.settle
    faulted = False

    async def fail_once(value, *, resolve=False):
        nonlocal faulted
        if not faulted and value.results[0].status == "done":
            faulted = True
            raise RuntimeError("simulated storage fault")
        return await settle(value, resolve=resolve)

    catalog.settle = fail_once

    async def apply(execution):
        resource.write_text("旧值")
        return receipt(execution)

    try:
        operation = await service.invoke(
            topic.id,
            ChangeInvoke(revision=1, action="undo", item_id="one", idempotency_key="fault"),
            apply,
        )
        assert (
            operation.status == "uncertain"
            and operation.receipt.results[0].error_code == "receipt_store_failed"
        )
        assert (
            resource.read_text() == "旧值"
            and (await catalog.get(topic.id)).items[0].status == "uncertain"
        )
    finally:
        await service.close()


async def test_resolution_cannot_rewrite_confirmed_batch_results(repository):
    topic = await repository.create_topic("核实部分执行")
    catalog = ChangeListRepository(repository.database)
    service = ChangeListService(catalog)
    await catalog.sync(topic.id, ChangeSync(revision=0, items=items()))

    async def apply(execution):
        return receipt(execution, ["done", "uncertain"])

    try:
        operation = await service.invoke(
            topic.id, ChangeInvoke(revision=1, action="undo-all", idempotency_key="mixed"), apply
        )
        with pytest.raises(ServiceError) as invalid:
            await catalog.settle(receipt(operation.execution, ["error", "done"]), resolve=True)
        assert invalid.value.code == "change_resolution_invalid"
        assert (await catalog.settle(receipt(operation.execution), resolve=True)).status == "done"
    finally:
        await service.close()


@pytest.mark.parametrize("mode", ["missing", "raises", "timeout", "closed"])
async def test_remote_callback_failure_timeout_and_disconnect_are_persisted(server_url, mode):
    async with httpx.AsyncClient(base_url=server_url, trust_env=False) as api:
        topic = (await api.post("/v1/topics", json={"title": mode})).json()["id"]
        await api.put(
            f"/v1/topics/{topic}/change-list",
            json={"revision": 0, "items": [items()[0].model_dump(mode="json")]},
        )
        sid = (await api.post("/v1/host/sessions")).json()["id"]
        await api.post(f"/v1/host/sessions/{sid}/ready")
        started = asyncio.Event()
        async with RemoteHostHandle(server_url, sid) as host:

            async def apply(execution):
                started.set()
                if mode == "raises":
                    raise RuntimeError("private failure")
                await asyncio.Event().wait()
                return receipt(execution)

            if mode != "missing":
                host.on_change_invoke(apply)

            async def listen():
                async for _event in host.events():
                    pass

            listener = asyncio.create_task(listen())
            try:
                request = ChangeInvoke(
                    revision=1,
                    action="undo",
                    item_id="one",
                    idempotency_key="once",
                    timeout_seconds=0.15,
                )
                pending = asyncio.create_task(host.invoke_change(topic, request))
                if mode == "closed":
                    await asyncio.wait_for(started.wait(), 3)
                    await api.delete(f"/v1/host/sessions/{sid}")
                result = await pending
                assert (
                    result.status == "uncertain"
                    and "private failure" not in result.model_dump_json()
                )
                assert (await api.get(f"/v1/topics/{topic}/change-list")).json()["items"][0][
                    "status"
                ] == "uncertain"
                repeat = await api.post(
                    f"/v1/topics/{topic}/change-list/invoke", json=request.model_dump(mode="json")
                )
                assert (
                    repeat.status_code == 200
                    and repeat.json()["execution"]["operation_id"] == result.execution.operation_id
                )
            finally:
                listener.cancel()
                await asyncio.gather(listener, return_exceptions=True)
