"""Actual user host input continues a worker without a fabricated model invocation."""

from __future__ import annotations

import asyncio
from pathlib import Path

import httpx

from krim_agent.api.server import create_app
from krim_agent.composition import open_application
from krim_agent.config import Settings
from krim_agent.providers.types import ModelToolCall
from krim_agent.security import LOCAL
from krim_agent.sessions.types import SessionCreate, TurnRequest
from krim_agent.subagents.configuration import SubagentProfileConfiguration, SubagentsConfiguration
from krim_agent.tools.registry import ToolRegistry
from tests.agent_core.test_subagent_collaboration import Scripted, Step, settle


async def test_host_continues_completed_worker_after_root_completion(tmp_path: Path) -> None:
    database = tmp_path / "host.sqlite"
    main = Scripted(
        [
            Step(
                deltas=(
                    ModelToolCall(
                        id="spawn-worker",
                        name="spawn_agent",
                        arguments={"task": "first task", "name": "worker"},
                    ),
                )
            )
        ]
    )
    worker = Scripted(fallback="worker output")
    app = await open_application(
        database,
        settings=Settings(database=database, run_timeout=60),
        provider=main,
        registry=ToolRegistry(()),
        subagent_configuration=SubagentsConfiguration(
            enabled=True,
            profiles={"worker": SubagentProfileConfiguration(model="worker")},
            default_profile="worker",
        ),
        model_routes={"worker": worker},
    )
    try:
        session = await app.sessions.create(
            LOCAL, "local", SessionCreate(request_key="host-session")
        )
        turn = await app.sessions.submit(
            LOCAL,
            "local",
            session.id,
            TurnRequest(
                text="delegate",
                request_key="host-turn",
                expected_revision=0,
            ),
        )
        _, root = await settle(app, session.id, turn.id, limit=30)
        assert root.status == "completed"
        api = create_app(application=app)
        async with (
            api.router.lifespan_context(api),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=api),
                base_url="http://127.0.0.1",
            ) as client,
        ):
            base = f"/v2/workspaces/local/sessions/{session.id}/subagents"
            from krim_agent.api.subagents import _list_members

            await _list_members(app.store, session.id, None, 50)
            listing = await client.get(base)
            assert listing.status_code == 200, listing.text
            member = next(item for item in listing.json()["subagents"] if item["role"] == "agent")
            agent_id = member["agent_id"]
            detail = await client.get(f"{base}/{agent_id}")
            assert detail.status_code == 200, detail.text
            assert detail.json()["run"]["status"] == "completed"
            assert {event["run_id"] for event in detail.json()["events"]} == {
                detail.json()["run"]["id"]
            }
            requests_before = main.requests
            body = {"text": "second task", "wake": "auto", "request_key": "host-message"}
            sent = await client.post(f"{base}/{agent_id}/messages", json=body)
            assert sent.status_code == 202, sent.text
            assert sent.json()["accepted"] and sent.json()["started"]
            async with asyncio.timeout(20):
                while True:
                    current = await app.subagents.repository.get_agent(agent_id)
                    if current["task"]["status"] == "completed" and current["task"]["ordinal"] == 2:
                        break
                    await asyncio.sleep(0.02)
            assert main.requests == requests_before
            duplicate = await client.post(f"{base}/{agent_id}/messages", json=body)
            assert duplicate.status_code == 202, duplicate.text
            assert duplicate.json()["message_id"] == sent.json()["message_id"]
            assert not duplicate.json()["started"]
            messages = (await client.get(f"{base}/{agent_id}/messages")).json()["messages"]
            assert len(messages) == 2
            assert messages[-1]["sender_id"] is None
            assert messages[-1]["body"]["sender_kind"] == "user"
            assert messages[-1]["body"]["principal_id"] == LOCAL.principal_id
            bad = await client.post(f"{base}/{agent_id}/messages", json=body | {"text": "changed"})
            assert bad.status_code == 409, bad.text
            other = await app.sessions.create(
                LOCAL, "local", SessionCreate(request_key="other-host")
            )
            cross = await client.get(
                f"/v2/workspaces/local/sessions/{other.id}/subagents/{agent_id}"
            )
            assert cross.status_code == 404
            await app.subagents.control(LOCAL, "local", session.id, agent_id, action="close")
            closed = await client.post(
                f"{base}/{agent_id}/messages", json=body | {"request_key": "after-close"}
            )
            assert closed.status_code >= 400
    finally:
        await asyncio.wait_for(app.close(), 20)
