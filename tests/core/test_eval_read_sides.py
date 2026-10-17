"""真实ASGI边界、薄CLI所有权与只读报告。"""

import argparse
import asyncio
import hashlib

import httpx
import pytest
from pydantic import SecretStr
from test_regression import selection

from krim_agent.api import create_app
from krim_agent.composition import open_application
from krim_agent.config import Settings


@pytest.mark.asyncio
async def test_authenticated_readonly_report_and_gate(tmp_path):
    settings = Settings(database=tmp_path / "api.sqlite", api_key=SecretStr("SYNTHETIC_M2_KEY"))
    async with await open_application(settings.database, settings=settings) as app:
        api = create_app(application=app)
        async with api.router.lifespan_context(api):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=api), base_url="http://localhost"
            ) as client:
                body = {"name": "api", "version": "1", "cases": [selection("sum", "/add 2 3", "5")]}
                denied = await client.post(
                    "/v1/datasets", json=body, headers={"Authorization": "Bearer WRONG_KEY"}
                )
                assert (
                    denied.status_code == 401 and denied.json()["error"]["code"] == "unauthorized"
                )
                response = await client.post(
                    "/v1/datasets", json=body, headers={"Authorization": "Bearer SYNTHETIC_M2_KEY"}
                )
                assert response.status_code == 200
                assert response.json()["name"] == "api"
                again = await client.post(
                    "/v1/datasets", json=body, headers={"Authorization": "Bearer SYNTHETIC_M2_KEY"}
                )
                assert again.json() == response.json()
                from sqlalchemy import func, select

                from krim_agent.storage.models import dataset_revisions

                async with app.store.engine.begin() as conn:
                    assert (
                        await conn.execute(select(func.count()).select_from(dataset_revisions))
                    ).scalar_one() == 1
                denied_get = await client.get(
                    "/v1/datasets/api/1", headers={"Authorization": "Bearer WRONG_KEY"}
                )
                assert denied_get.status_code == 401


@pytest.mark.asyncio
async def test_sdk_api_cli_share_eval_identity(tmp_path, monkeypatch):
    from test_evaluation import one_request

    from krim_agent import cli
    from krim_agent.contracts import ServiceError
    from krim_agent.evaluation import scorers
    from krim_agent.storage.sqlite import SQLiteStore

    path = tmp_path / "owned.sqlite"
    app = await open_application(path)
    request = await one_request(app, key="local")
    entered = asyncio.Event()
    release = asyncio.Event()
    original_score = scorers.score_rule

    async def held(*values):
        entered.set()
        await release.wait()
        return await original_score(*values)

    monkeypatch.setattr(scorers, "score_rule", held)
    real_open = cli.open_application

    async def own(selected):
        assert selected == path
        return app

    monkeypatch.setattr(cli, "open_application", own)
    args = argparse.Namespace(
        command="eval",
        operation="start",
        database=str(path),
        url=None,
        name="local",
        revision="1",
        fingerprint=request.dataset.fingerprint,
        target="target-v2",
        key="local",
        rules="status_code",
        repetitions=1,
    )
    task = asyncio.create_task(cli.execute(args))
    try:
        await asyncio.wait_for(entered.wait(), 3)
        assert not task.done() and not app.store.closed
        release.set()
        report = await asyncio.wait_for(task, 5)
        assert report["status"] == "completed" and report["complete"]
        assert app.store.closed and all(t.done() for t in app.evaluations.tasks.values())
        reader = await SQLiteStore.open(path, readonly=True)
        try:
            assert (await reader.eval_by_key("local"))["status"] == "completed"
        finally:
            await reader.close()
    finally:
        release.set()
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await app.close()
    monkeypatch.setattr(cli, "open_application", real_open)
    monkeypatch.setattr(scorers, "score_rule", original_score)
    # 独立本机控制在开库前拒绝，已有/无writer都不触发恢复。
    for writer in [False, True]:
        active = await real_open(path) if writer else None
        before = hashlib.sha256(path.read_bytes()).hexdigest()
        calls = []

        async def forbidden(*values, calls=calls, **kwargs):
            calls.append(1)
            raise AssertionError("must not open")

        with monkeypatch.context() as patch:
            patch.setattr(cli, "open_application", forbidden)
            patch.setattr(SQLiteStore, "open", forbidden)
            for operation in ["wait", "cancel"]:
                with pytest.raises(ServiceError, match="readonly"):
                    await cli.execute(
                        argparse.Namespace(
                            command="eval",
                            operation=operation,
                            database=str(path),
                            url=None,
                            id="original",
                        )
                    )
            assert calls == [] and hashlib.sha256(path.read_bytes()).hexdigest() == before
        if active is not None:
            await active.close()


@pytest.mark.asyncio
async def test_remote_cli_exit_does_not_cancel_server(tmp_path, monkeypatch):
    from test_evaluation import one_request

    from krim_agent import cli
    from krim_agent.client import AsyncRunClient

    async with await open_application(tmp_path / "remote.sqlite") as app:
        request = await one_request(app, text="/slow", key="remote")
        api = create_app(application=app)
        clients = []
        read = asyncio.Event()
        async with api.router.lifespan_context(api):
            # 用真实ASGI应用与边界替换传输，进程外TCP身份另由installed_eval核验。
            class Client(AsyncRunClient):
                def __init__(self, url, key):
                    self.http = httpx.AsyncClient(
                        transport=httpx.ASGITransport(app=api), base_url=url
                    )
                    clients.append(self)

                async def eval_get(self, eval_id):
                    result = await super().eval_get(eval_id)
                    read.set()
                    return result

            monkeypatch.setattr(cli, "AsyncRunClient", Client)
            start = argparse.Namespace(
                command="eval",
                operation="start",
                database=None,
                url="http://localhost",
                name="remote",
                revision="1",
                fingerprint=request.dataset.fingerprint,
                target="target-v2",
                key="remote",
                rules="status_code",
                repetitions=1,
            )
            view = await cli.execute(start)
            await asyncio.wait_for(
                app.service.bindings["target-v2"].loop.provider.entered.wait(), 3
            )
            assert clients[-1].http.is_closed and not app.evaluations.tasks[view["id"]].done()
            args = argparse.Namespace(
                command="eval",
                operation="wait",
                database=None,
                url="http://localhost",
                id=view["id"],
            )
            waiter = asyncio.create_task(cli.execute(args))
            await asyncio.wait_for(read.wait(), 3)
            waiter.cancel()
            await asyncio.gather(waiter, return_exceptions=True)
            assert clients[-1].http.is_closed and not app.evaluations.tasks[view["id"]].done()
            args.operation = "cancel"
            cancelled = await cli.execute(args)
            assert cancelled["id"] == view["id"] and cancelled["status"] == "aborted"
            assert app.service.bindings["target-v2"].loop.provider.active == 0


@pytest.mark.asyncio
async def test_local_cli_interrupt_waits_owned_cleanup(tmp_path, monkeypatch):
    from test_evaluation import one_request

    from krim_agent import cli
    from krim_agent.evaluation import scorers

    app = await open_application(tmp_path / "interrupt.sqlite")
    request = await one_request(app, key="interrupt")
    entered = asyncio.Event()
    cleaning = asyncio.Event()
    release = asyncio.Event()

    async def held(*values):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            cleaning.set()
            await release.wait()

    monkeypatch.setattr(scorers, "score_rule", held)

    async def own(path):
        return app

    monkeypatch.setattr(cli, "open_application", own)
    args = argparse.Namespace(
        command="eval",
        operation="start",
        database=str(app.settings.database),
        url=None,
        name="interrupt",
        revision="1",
        fingerprint=request.dataset.fingerprint,
        target="target-v2",
        key="interrupt",
        rules="status_code",
        repetitions=1,
    )
    task = asyncio.create_task(cli.execute(args))
    try:
        await asyncio.wait_for(entered.wait(), 3)
        task.cancel()
        await asyncio.wait_for(cleaning.wait(), 3)
        assert not task.done() and not app.store.closed
        task.cancel()
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        assert app.store.closed and all(t.done() for t in app.evaluations.tasks.values())
    finally:
        release.set()
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await app.close()


@pytest.mark.asyncio
async def test_revision_cursor_and_unknown_create(tmp_path):
    from test_evaluation import one_request

    from krim_agent.contracts import ServiceError

    async with await open_application(tmp_path / "cursor.sqlite") as app:
        request = await one_request(app)
        view = await app.evaluations.start(request)
        await app.evaluations.wait(view.id)
        assert (await app.evaluations.by_key(request.request_key)).id == view.id
        current = await app.evaluations.get(view.id)
        assert len((await app.evaluations.items(view.id, current.revision))["items"]) == 1
        with pytest.raises(ServiceError, match="snapshot_changed"):
            await app.evaluations.items(view.id, view.revision)
