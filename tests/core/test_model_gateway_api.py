"""HTTP settings contracts and pre-gateway database upgrades."""

import httpx
import pytest
from sqlalchemy import select

from krim_agent.api import create_app
from krim_agent.composition import open_application
from krim_agent.contracts import RunRequest
from krim_agent.storage.model_gateway_models import MODEL_GATEWAY_TABLES
from krim_agent.storage.sqlite import SCHEMA_VERSION, SQLiteStore


def configuration(**changes):
    return {
        "expected_revision": 0,
        "request_key": "gateway-save",
        "gateways": [
            {
                "gateway_id": "custom",
                "name": "Custom",
                "base_url": "https://gateway.example/v1",
                "wire_api": "responses",
                "models": [
                    {
                        "id": "custom-model",
                        "display_name": "Custom Model",
                        "reasoning_levels": {"deep": {"effort": "high"}},
                    }
                ],
            }
        ],
        "default_selection": {
            "gateway_id": "custom",
            "model_id": "custom-model",
            "reasoning_level": "deep",
        },
        **changes,
    }


async def test_settings_api_persists_without_configured_model_and_hides_secrets(tmp_path):
    path = tmp_path / "gateways.sqlite"
    async with await open_application(path) as app:
        api = create_app(application=app)
        api.state.application = app
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=api),
            base_url="http://127.0.0.1",
        ) as client:
            assert (await client.get("/v2/me")).json()["target_keys"] == []
            response = await client.get("/v2/me/model-gateways")
            assert response.status_code == 200, response.text
            assert response.json()["revision"] == 0
            response = await client.put("/v2/me/model-gateways", json=configuration())
            assert response.status_code == 200, response.text
            assert response.headers["cache-control"] == "no-store"
            assert response.json()["revision"] == 1
            assert response.json()["gateways"][0]["api_key_set"] is False
            assert (await client.get("/v2/me")).json()["target_keys"] == ["target-http"]
            replay = await client.put("/v2/me/model-gateways", json=configuration())
            assert replay.json() == response.json()
            conflict = await client.put(
                "/v2/me/model-gateways",
                json=configuration(
                    request_key="stale-save",
                ),
            )
            assert conflict.status_code == 409
    async with await open_application(path) as app:
        view = await app.model_gateways.get("local")
        assert view.revision == 1
        assert view.default_selection.model_id == "custom-model"


@pytest.mark.parametrize("legacy_version", [20, 21])
async def test_database_upgrades_gateway_tables_and_keeps_existing_facts(
    tmp_path, legacy_version,
):
    path = tmp_path / "upgrade.sqlite"
    store = await SQLiteStore.open(path)
    try:
        run, _ = await store.begin(RunRequest(text="existing", request_key="before-upgrade"), {})
        async with store.engine.begin() as conn:
            for table in reversed(MODEL_GATEWAY_TABLES):
                await conn.run_sync(table.drop)
            await conn.exec_driver_sql(f"PRAGMA user_version={legacy_version}")
    finally:
        await store.close()
    store = await SQLiteStore.open(path)
    try:
        assert store.schema_version == SCHEMA_VERSION == 22
        assert (await store.snapshot(run.id)).run.status == "queued"
        async with store.engine.begin() as conn:
            for table in MODEL_GATEWAY_TABLES:
                assert (await conn.execute(select(table))).all() == []
    finally:
        await store.close()
