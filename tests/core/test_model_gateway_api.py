"""HTTP settings contracts and pre-gateway database upgrades."""

import httpx
import pytest
from sqlalchemy import select

from krim_agent.api import create_app
from krim_agent.api import model_gateways as model_gateway_api
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


async def test_draft_discovery_endpoint_resolves_keys_and_keeps_document(tmp_path, monkeypatch):
    path = tmp_path / "draft-discovery.sqlite"
    async with await open_application(path) as app:
        api = create_app(application=app)
        api.state.application = app
        captured = []

        async def fake_discover(config, *, transport=None):
            captured.append(config)
            return [
                {
                    "id": "found-model",
                    "display_name": "Found Model",
                    "allow_images": False,
                    "max_output_tokens": None,
                    "reasoning_levels": {},
                }
            ]

        monkeypatch.setattr(model_gateway_api, "discover_models", fake_discover)

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=api),
            base_url="http://127.0.0.1",
        ) as client:
            response = await client.post(
                "/v2/me/model-gateways/models/discover",
                json={
                    "base_url": "https://draft.example.com/v1",
                    "wire_api": "chat_completions",
                    "expected_revision": 0,
                    "api_key": "sk-draft-probe",
                },
            )
            assert response.status_code == 200, response.text
            assert response.headers["cache-control"] == "no-store"
            models = response.json()["models"]
            assert [m["id"] for m in models] == ["found-model"]
            assert models[0]["display_name"] == "Found Model"
            assert captured[-1].api_key.get_secret_value() == "sk-draft-probe"
            assert captured[-1].base_url == "https://draft.example.com/v1"
            assert captured[-1].model == "discovery"

            monkeypatch.setenv("KRIM_TEST_GATEWAY_DISCOVERY", "{{SECRET_http3nv}}")
            saved = await client.put(
                "/v2/me/model-gateways",
                json=configuration(
                    gateways=[
                        {
                            "gateway_id": "custom",
                            "name": "Custom",
                            "base_url": "https://gateway.example/v1",
                            "wire_api": "responses",
                            "api_key_env": "KRIM_TEST_GATEWAY_DISCOVERY",
                            "models": [
                                {
                                    "id": "custom-model",
                                    "display_name": "Custom Model",
                                    "reasoning_levels": {"deep": {"effort": "high"}},
                                }
                            ],
                        }
                    ]
                ),
            )
            assert saved.status_code == 200, saved.text
            assert saved.json()["gateways"][0]["api_key_set"] is True

            probe = {
                "base_url": "https://gateway.example/v1",
                "wire_api": "responses",
                "gateway_id": "custom",
                "expected_revision": 1,
            }
            response = await client.post("/v2/me/model-gateways/models/discover", json=probe)
            assert response.status_code == 200, response.text
            assert captured[-1].api_key.get_secret_value() == "{{SECRET_http3nv}}"
            assert captured[-1].wire_api == "responses"

            override = dict(probe, api_key="sk-fresh")
            response = await client.post("/v2/me/model-gateways/models/discover", json=override)
            assert response.status_code == 200, response.text
            assert captured[-1].api_key.get_secret_value() == "sk-fresh"

            response = await client.post(
                "/v2/me/model-gateways/models/discover", json=dict(probe, clear_api_key=True)
            )
            assert response.status_code == 200, response.text
            assert captured[-1].api_key.get_secret_value() == ""

            conflict = await client.post(
                "/v2/me/model-gateways/models/discover", json=dict(probe, expected_revision=0)
            )
            assert conflict.status_code == 409
            assert conflict.json()["error"]["code"] == "revision_conflict"

            missing = await client.post(
                "/v2/me/model-gateways/models/discover", json=dict(probe, gateway_id="ghost")
            )
            assert missing.status_code == 404
            assert missing.json()["error"]["code"] == "not_found"

            invalid = await client.post(
                "/v2/me/model-gateways/models/discover", json=dict(probe, base_url="ftp://bad")
            )
            assert invalid.status_code == 422
            assert invalid.json()["error"]["code"] == "invalid_input"

            view = (await client.get("/v2/me/model-gateways")).json()
            assert view["revision"] == 1
            assert view["gateways"][0]["api_key_set"] is True

            legacy = await client.post("/v2/me/model-gateways/custom/models/discover")
            assert legacy.status_code == 200, legacy.text
            assert [m["id"] for m in legacy.json()["models"]] == ["found-model"]
            assert captured[-1].base_url == "https://gateway.example/v1"


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
