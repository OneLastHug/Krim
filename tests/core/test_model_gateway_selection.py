"""Switch idle-session model settings atomically, preserving running request snapshots."""

import asyncio

import pytest
from fixtures.http_provider_server import HTTPFixture

from krim_agent.composition import open_application
from krim_agent.contracts import ServiceError
from krim_agent.model_gateway.selection import SessionModelUpdate, get_selection, set_selection
from krim_agent.providers.openai_compatible import OpenAICompatibleConfig
from krim_agent.security import LOCAL
from krim_agent.sessions.types import SessionCreate, TurnRequest


def config(url):
    return {
        "expected_revision": 0,
        "request_key": "settings",
        "gateways": [
            {
                "gateway_id": "custom",
                "name": "Custom",
                "base_url": url,
                "models": [
                    {
                        "id": "custom-model",
                        "display_name": "Custom",
                        "reasoning_levels": {"deep": {"effort": "high"}},
                    }
                ],
            }
        ],
    }


def update_request(key="switch", revision=0):
    return SessionModelUpdate.model_validate(
        {
            "request_key": key,
            "expected_revision": revision,
            "model_selection": {
                "gateway_id": "custom",
                "model_id": "custom-model",
                "reasoning_level": "deep",
            },
        }
    )


async def test_switch_and_retry_and_return_to_host_model(tmp_path):
    async with HTTPFixture() as upstream:
        async with await open_application(
            tmp_path / "app.sqlite",
            http_config=OpenAICompatibleConfig(base_url=upstream.url, model="host-model"),
        ) as app:
            await app.model_gateways.update("local", config(upstream.url))
            session = await app.sessions.create(
                LOCAL, "local", SessionCreate(request_key="session")
            )
            body = update_request()
            result = await set_selection(app, LOCAL, "local", session.id, body)
            assert result["model"] == "custom-model" and result["revision"] == 1
            assert await set_selection(app, LOCAL, "local", session.id, body) == result
            assert (await get_selection(app, LOCAL, "local", session.id))[
                "reasoning_effort"
            ] == "high"
            turn = await app.sessions.submit(
                LOCAL,
                "local",
                session.id,
                TurnRequest(
                    request_key="message",
                    text="hello",
                    expected_revision=1,
                ),
            )
            try:
                async with asyncio.timeout(15):
                    completed = await app.sessions.wait(LOCAL, "local", session.id, turn.id)
            except TimeoutError:
                current = await app.sessions.turn(LOCAL, "local", session.id, turn.id)
                pytest.fail(f"turn did not settle: {current.status}/{current.code}")
            assert completed.status == "completed", completed.code
            assert upstream.requests[-1]["body"]["model"] == "custom-model"
            back = await set_selection(
                app,
                LOCAL,
                "local",
                session.id,
                SessionModelUpdate(request_key="host", expected_revision=2),
            )
            assert back["model"] == "host-model" and back["model_selection"] is None
            with pytest.raises(ServiceError) as conflict:
                await set_selection(app, LOCAL, "local", session.id, update_request("stale", 0))
            assert conflict.value.code == "revision_conflict"


async def test_switch_rejects_queued_turn_and_preserves_session(tmp_path):
    async with HTTPFixture() as upstream:
        async with await open_application(
            tmp_path / "app.sqlite",
            http_config=OpenAICompatibleConfig(base_url=upstream.url, model="host-model"),
        ) as app:
            await app.model_gateways.update("local", config(upstream.url))
            session = await app.sessions.create(
                LOCAL, "local", SessionCreate(request_key="session")
            )
            # Closing the dispatcher keeps this deterministic queued turn unexecuted.
            await app.sessions.close()
            from krim_agent.storage.sessions import submit

            turn = await submit(
                app.store,
                session.id,
                TurnRequest(request_key="queued", text="hello", expected_revision=0),
                app.settings,
            )
            assert turn.status == "queued"
            with pytest.raises(ServiceError) as busy:
                await set_selection(app, LOCAL, "local", session.id, update_request(revision=1))
            assert busy.value.code == "session_busy"
            view = await get_selection(app, LOCAL, "local", session.id)
            assert view["model"] == "host-model" and view["revision"] == 1
