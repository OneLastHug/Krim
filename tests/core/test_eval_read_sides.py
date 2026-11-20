"""Authenticated dataset reads, legacy report compatibility and retired execution."""

import argparse
import hashlib

import httpx
import pytest
from pydantic import SecretStr
from test_regression import selection

from krim_agent.api import create_app
from krim_agent.composition import open_application
from krim_agent.config import Settings


@pytest.mark.asyncio
async def test_authenticated_readonly_dataset_and_gate(tmp_path):
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
async def test_legacy_eval_start_is_retired_but_saved_report_remains_readable(tmp_path):
    from test_eval_storage import seed_legacy_report

    from krim_agent.contracts import ServiceError

    path = tmp_path / "legacy-history.sqlite"
    async with await open_application(path) as app:
        request, eval_id = await seed_legacy_report(app, "historical-report")
        before = await app.evaluations.report(eval_id)
        assert before["complete"] is True
        before_hash = before["report_hash"]
        with pytest.raises(ServiceError) as retired:
            await app.evaluations.start(request)
        assert retired.value.code == "execution_retired" and retired.value.status == 410
        assert app.evaluations.targets() == []
        assert (await app.evaluations.report(eval_id))["report_hash"] == before_hash

    async with await open_application(path) as reopened:
        view = await reopened.evaluations.by_key("historical-report")
        report = await reopened.evaluations.report(view.id)
        assert view.status == "completed"
        assert report["report_hash"] == before_hash
        assert report["items"][0]["scores"][0]["value"] == 1
        assert reopened.service.tasks == {}


@pytest.mark.asyncio
async def test_local_legacy_cli_wait_cancel_reject_before_open(tmp_path, monkeypatch):
    from krim_agent import cli
    from krim_agent.contracts import ServiceError
    from krim_agent.storage.sqlite import SQLiteStore

    path = tmp_path / "readonly.sqlite"
    async with await open_application(path):
        pass
    before = hashlib.sha256(path.read_bytes()).hexdigest()
    opened = []

    async def forbidden(*args, **kwargs):
        opened.append(True)
        raise AssertionError("legacy wait/cancel must not open a writer")

    monkeypatch.setattr(cli, "open_application", forbidden)
    monkeypatch.setattr(SQLiteStore, "open", forbidden)
    for operation in ("wait", "cancel"):
        with pytest.raises(ServiceError) as readonly:
            await cli.execute(
                argparse.Namespace(
                    command="eval", operation=operation, database=str(path), url=None, id="old"
                )
            )
        assert readonly.value.code == "readonly"
    assert opened == []
    assert hashlib.sha256(path.read_bytes()).hexdigest() == before
