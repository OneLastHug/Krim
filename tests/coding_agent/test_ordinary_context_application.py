"""Real ordinary-session context application through open_application.

Non-development/non-workspace sessions run end to end on the production
RunService/SDK/SQLite stack with one deterministic scripted model: multi-turn
history freezes explicit schema-4 anchors, the low configured context capacity
forces an automatic summary pass at the model boundary, the durable compaction
ledger persists and is reused as the next turn incrementally extends coverage,
and the manual compaction HTTP request stays pending until that same boundary
fulfils it on the next turn.
"""

import asyncio
import hashlib
import re
from contextlib import asynccontextmanager

import httpx
import pytest
from pydantic import SecretStr
from sqlalchemy import select

from krim_agent.api.server import create_app
from krim_agent.composition import open_application
from krim_agent.config import Settings
from krim_agent.context.compaction import PROMPT
from krim_agent.context.types import OrdinarySessionContext
from krim_agent.contracts import Message, encoded, fingerprint, parse_json
from krim_agent.providers.types import ModelText, ModelToolCall
from krim_agent.security import AccessPolicy, Credential, Membership
from krim_agent.storage.context_models import context_requests
from krim_agent.storage.models import compactions, turns

PADDING = 2400
CAPACITY_PADS = {"orange": "x", "sum": "y", "apple": "z", "kiwi": "v"}
FACT_TURNS = (
    ("turn-1", "FACT orange=17"),
    ("turn-2", "TOOL add 2 3"),
    ("turn-3", "FACT apple=42"),
    ("turn-4", "recall everything"),
    ("turn-5", "FACT kiwi=9"),
)


class ScriptedModel:
    """Deterministic provider that recognizes the real compaction PROMPT.

    Summary requests receive the frozen history payload and answer with the
    concise FACT list it contains, carrying previous summaries forward. Task
    requests answer from the current user input, padding FACT replies so the
    configured context capacity is actually exceeded.
    """

    def __init__(self) -> None:
        self.requests: list[list[Message]] = []
        self.calls = 0
        self.closed = False

    async def stream(self, messages, tools):
        self.requests.append(list(messages))
        self.calls += 1
        first = messages[0]
        if first.role == "system" and first.content.startswith(PROMPT):
            assert tools == []
            payload = parse_json(messages[1].content)
            assert isinstance(payload, dict)
            facts: list[str] = []
            blobs = [
                payload.get("previous_summary") or "",
                encoded(payload.get("turns") or []),
            ]
            for blob in blobs:
                for fact in re.findall(r"FACT [a-z]+=\d+", blob):
                    if fact not in facts:
                        facts.append(fact)
            yield ModelText(text="; ".join(facts))
            return
        user = next(message for message in reversed(messages) if message.role == "user")
        if messages[-1].role == "tool":
            # Tool-result continuation of this run's own exchange.
            reply = "FACT sum=5; " + "y" * PADDING
        elif user.content.startswith("TOOL add"):
            yield ModelToolCall(
                id=f"add-{self.calls}", name="add", arguments={"left": 2.0, "right": 3.0}
            )
            return
        elif user.content.startswith("FACT "):
            pad = CAPACITY_PADS[user.content.split()[1].split("=")[0]]
            reply = user.content + "; " + pad * PADDING
        else:
            reply = "done: " + user.content
        yield ModelText(text=reply)

    async def close(self) -> None:
        self.closed = True


@pytest.fixture
async def application(tmp_path, request):
    provider = ScriptedModel()
    # Far below the production default window, yet above the composed session
    # tool schemas, live snapshots and the output reservation, so the padded
    # history genuinely exceeds the model boundary capacity on turn 4.
    settings = Settings(context_max_bytes=getattr(request, "param", 36864), context_max_messages=64)
    app = await open_application(
        tmp_path / "ordinary-context.sqlite", settings=settings, provider=provider
    )
    try:
        yield app, provider
    finally:
        await app.close()
        assert all(task.done() for task in app.service.tasks.values())
        assert app.sessions._dispatcher.done()
        await provider.close()
        assert provider.closed


@asynccontextmanager
async def http_client(app):
    api = create_app(application=app)
    async with (
        api.router.lifespan_context(api),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=api), base_url="http://127.0.0.1"
        ) as client,
    ):
        yield client


async def create_session(client, key, *, workspace_id="local"):
    response = await client.post(
        f"/v2/workspaces/{workspace_id}/sessions", json={"request_key": key}
    )
    assert response.status_code == 201, response.text
    return response.json()


async def run_turn(client, session_id, *, key, text, revision, workspace_id="local"):
    response = await client.post(
        f"/v2/workspaces/{workspace_id}/sessions/{session_id}/turns",
        json={"request_key": key, "text": text, "expected_revision": revision},
    )
    assert response.status_code == 202, response.text
    accepted = response.json()
    async with asyncio.timeout(30):
        while True:
            view = (
                await client.get(
                    f"/v2/workspaces/{workspace_id}/sessions/{session_id}/turns/{accepted['id']}"
                )
            ).json()
            if view["status"] not in {"queued", "running"}:
                return view
            await asyncio.sleep(0.02)


def messages_of(request):
    return encoded([message.model_dump(mode="json") for message in request])


def ledger_refs(sources):
    """The frozen anchors as the durable compaction ledger records them."""
    return [
        {
            "turn_id": item.turn_id,
            "run_id": item.run_id,
            "terminal_seq": item.last_seq,
            "transcript_fingerprint": item.fingerprint,
        }
        for item in sources
    ]


async def turn_row(store, turn_id):
    rows = await store.work_read(
        lambda conn: conn.execute(select(turns).where(turns.c.id == turn_id))
    )
    return rows.mappings().one()


async def compaction_rows(store, run_id):
    rows = await store.work_read(
        lambda conn: conn.execute(select(compactions).where(compactions.c.run_id == run_id))
    )
    return rows.mappings().all()


async def test_nonlocal_static_actor_runs_ordinary_history_without_legacy_flag(tmp_path):
    provider = ScriptedModel()
    access = AccessPolicy(
        credentials=(
            Credential(
                principal_id="alice",
                token=SecretStr("static-alice"),
                memberships=(Membership(workspace_id="alpha", role="member"),),
            ),
        )
    )
    actor = access.authenticate("Bearer static-alice")
    assert actor.principal_id == "alice" and actor.legacy is False
    app = await open_application(
        tmp_path / "static-ordinary-context.sqlite",
        settings=Settings(context_max_bytes=131072),
        provider=provider,
        access_policy=access,
    )
    try:
        async with http_client(app) as client:
            client.headers["Authorization"] = "Bearer static-alice"
            session = await create_session(client, "static-ordinary", workspace_id="alpha")
            views = []
            for revision, (key, text) in enumerate(FACT_TURNS[:2]):
                view = await run_turn(
                    client,
                    session["id"],
                    key=key,
                    text=text,
                    revision=revision,
                    workspace_id="alpha",
                )
                assert view["status"] == "completed", view
                views.append(view)

            snapshot = await app.store.snapshot(views[-1]["run_id"])
            context = snapshot.session_context
            assert isinstance(context, OrdinarySessionContext)
            assert context.frozen.owner_id == "alice"
            assert context.frozen.workspace_id == "alpha"
            assert context.frozen.authorization_generation is None
            assert [source.turn_id for source in context.frozen.sources] == [views[0]["id"]]
            assert context.prepared is not None
            assert [message["role"] for message in views[-1]["transcript"]] == [
                "user",
                "assistant",
                "tool",
                "assistant",
            ]
            assert len(provider.requests) == 3
            assert "FACT orange=17" in messages_of(provider.requests[-1])
    finally:
        await app.close()
        assert all(task.done() for task in app.service.tasks.values())
        assert app.sessions._dispatcher.done()
        await provider.close()
        assert provider.closed


async def test_ordinary_history_auto_summary_persists_and_reuses(application):
    app, provider = application
    assert "demo" in app.service.bindings
    async with http_client(app) as client:
        session = await create_session(client, "ordinary-context")
        assert session["target_key"] == "demo"
        revision = 0
        marks: dict[str, int] = {}
        turn_views: dict[str, dict] = {}
        for key, text in FACT_TURNS[:3]:
            marks[key] = len(provider.requests)
            view = await run_turn(client, session["id"], key=key, text=text, revision=revision)
            revision += 1
            assert view["status"] == "completed", view["code"]
            turn_views[key] = view

        first = turn_views["turn-1"]
        assert [item["role"] for item in first["transcript"]] == ["user", "assistant"]
        assert first["transcript"][1]["content"].startswith("FACT orange=17; ")
        assert first["transcript"][1]["content"].endswith("x" * PADDING)

        second = turn_views["turn-2"]
        assert [item["role"] for item in second["transcript"]] == [
            "user",
            "assistant",
            "tool",
            "assistant",
        ]
        assert second["transcript"][1]["tool_calls"] == [
            {
                "kind": "tool_call",
                "id": second["transcript"][2]["call_id"],
                "name": "add",
                "arguments": {"left": 2.0, "right": 3.0},
            }
        ]
        assert parse_json(second["transcript"][2]["content"]) == {"sum": 5.0}

        # Turn 3 crosses the trigger ratio but has no compactable turn beyond
        # the keep-turns window, so it still runs without a summary request.
        assert len(provider.requests[marks["turn-3"] :]) == 1
        uncompressed = messages_of(provider.requests[marks["turn-3"]])
        assert "x" * PADDING in uncompressed and "y" * PADDING in uncompressed
        run3 = await app.store.snapshot(turn_views["turn-3"]["run_id"])
        assert isinstance(run3.session_context, OrdinarySessionContext)
        assert run3.target["context"]["schema_version"] == 4
        assert run3.session_context.frozen.target_key == "demo"
        # No summary exists yet, but the prepared no-summary projection of this
        # run is durably persisted on the turn row like every ordinary run.
        assert run3.session_context.prepared is not None
        assert run3.session_context.prepared.summary_hash is None
        stored = dict(await turn_row(app.store, turn_views["turn-3"]["id"]))
        assert stored["context_snapshot"]["schema_version"] == 4
        assert stored["context_snapshot"]["frozen"]["schema_version"] == 4
        assert "messages" not in stored["context_snapshot"]["frozen"]
        assert stored["context_fingerprint"] == fingerprint(stored["context_snapshot"]["frozen"])
        assert (await compaction_rows(app.store, run3.run.id)) == []

        # Turn 4 exceeds the hard capacity: the model boundary runs a real
        # summary pass against the frozen first turn before the task request.
        marks["turn-4"] = len(provider.requests)
        view = await run_turn(
            client, session["id"], key="turn-4", text="recall everything", revision=revision
        )
        revision += 1
        assert view["status"] == "completed", view["code"]
        turn_views["turn-4"] = view
        assert len(provider.requests[marks["turn-4"] :]) == 2
        summary_request, task_request = provider.requests[marks["turn-4"] :]
        assert summary_request[0].role == "system"
        assert summary_request[0].content.startswith(PROMPT)
        assert summary_request[1].role == "user"
        payload = parse_json(summary_request[1].content)
        assert payload["previous_summary"] == ""
        assert [item["content"] for item in payload["turns"]] == [
            "FACT orange=17",
            turn_views["turn-1"]["transcript"][1]["content"],
        ]

        task = messages_of(task_request)
        assert task_request[1].content == "[不可信历史摘要] FACT orange=17"
        assert task_request[-1].content == "recall everything"
        assert "y" * PADDING in task and "z" * PADDING in task
        assert "x" * PADDING not in task

        run4 = await app.store.snapshot(view["run_id"])
        starts = [event for event in run4.events if event.kind == "model_start"]
        assert [event.data["purpose"] for event in starts] == ["summary", "task"]
        assert [event.data["step_index"] for event in starts] == [0, 1]
        summary_texts = [event for event in run4.events if event.kind == "summary_text"]
        assert [event.data["text"] for event in summary_texts] == ["FACT orange=17"]
        changes = starts[-1].data["context_manifest"]["compaction_changes"]
        assert len(changes) == 1 and changes[0]["pass_index"] == 0
        assert changes[0]["before_input_bytes"] > changes[0]["after_input_bytes"]
        assert changes[0]["output_hash"] == hashlib.sha256(b"FACT orange=17").hexdigest()

        rows = await compaction_rows(app.store, view["run_id"])
        assert len(rows) == 1
        row = rows[0]
        frozen = run4.session_context.frozen
        refs = ledger_refs(frozen.sources[:1])
        assert row["status"] == "complete"
        assert row["session_id"] == session["id"]
        assert row["pass_index"] == 0 and row["model_step"] == 0
        assert row["output"] == "FACT orange=17"
        assert row["output_hash"] == hashlib.sha256(row["output"].encode()).hexdigest()
        assert row["prompt_hash"] == fingerprint(PROMPT)
        assert row["provider_identity"] == run4.target["provider"]
        assert row["source_refs"] == refs
        assert row["source_hash"] == fingerprint(refs)
        assert row["policy"]["source"]["domain"] == "ordinary-session-history/4"

        transcript = [Message.model_validate(item) for item in view["transcript"]]
        assert [message.content for message in transcript] == [
            "recall everything",
            "done: recall everything",
        ]
        assert "FACT orange=17" not in messages_of(transcript)

        stored = dict(await turn_row(app.store, view["id"]))
        context_state = stored["context_snapshot"]
        assert context_state["schema_version"] == 4
        prepared = context_state["prepared"]
        assert prepared["summary_hash"] == hashlib.sha256(b"FACT orange=17").hexdigest()
        assert prepared["source_fingerprint"] == frozen.source_fingerprint
        assert prepared["sources"] == [item.model_dump(mode="json") for item in frozen.sources]
        assert prepared["messages"][0]["role"] == "system"
        assert prepared["messages"][0]["content"] == frozen.system_prompt

        # Turn 5 reuses the completed ledger row instead of summarizing the
        # first turn again, extends the summary over turn 2, and keeps the
        # uncovered turns verbatim.
        marks["turn-5"] = len(provider.requests)
        view = await run_turn(
            client, session["id"], key="turn-5", text="FACT kiwi=9", revision=revision
        )
        assert view["status"] == "completed", view["code"]
        assert len(provider.requests[marks["turn-5"] :]) == 2
        reuse_request, task_request = provider.requests[marks["turn-5"] :]
        assert reuse_request[0].content.startswith(PROMPT)
        payload = parse_json(reuse_request[1].content)
        assert payload["previous_summary"] == "FACT orange=17"
        # Only turn 2 is newly summarized, as one complete tool-group turn.
        assert payload["turns"][0]["content"] == "TOOL add 2 3"
        assert len(payload["turns"]) == 4
        assert task_request[1].content == "[不可信历史摘要] FACT orange=17; FACT sum=5"
        task = messages_of(task_request)
        assert "z" * PADDING in task and "done: recall everything" in task
        assert "x" * PADDING not in task and "y" * PADDING not in task
        assert "FACT apple=42" in task and "FACT kiwi=9" in task

        run5 = await app.store.snapshot(view["run_id"])
        starts = [event for event in run5.events if event.kind == "model_start"]
        assert [event.data["purpose"] for event in starts] == ["summary", "task"]
        rows = await compaction_rows(app.store, view["run_id"])
        assert len(rows) == 1
        refs = ledger_refs(run5.session_context.frozen.sources[:2])
        assert rows[0]["source_refs"] == refs
        assert rows[0]["output"] == "FACT orange=17; FACT sum=5"

        pending = (
            await client.get(f"/v2/workspaces/local/sessions/{session['id']}/context/compactions")
        ).json()
        assert pending == {"items": []}


@pytest.mark.parametrize("application", [131072], indirect=True)
async def test_manual_compaction_api_runs_only_at_the_next_model_boundary(application):
    app, provider = application
    async with http_client(app) as client:
        session = await create_session(client, "manual-compact")
        revision = 0
        for key, text in FACT_TURNS[:4]:
            view = await run_turn(client, session["id"], key=key, text=text, revision=revision)
            revision += 1
            assert view["status"] == "completed", view["code"]

        base = f"/v2/workspaces/local/sessions/{session['id']}/context/compactions"
        assert (await client.get(base)).json() == {"items": []}
        response = await client.post(
            base, json={"request_key": "manual-compact-1", "reason": "user requested"}
        )
        assert response.status_code == 202, response.text
        receipt = response.json()
        assert receipt["status"] == "pending"
        assert receipt["created"] is True and receipt["revision"] == 1
        replay = await client.post(
            base, json={"request_key": "manual-compact-1", "reason": "user requested"}
        )
        assert replay.status_code == 202
        assert replay.json() == receipt | {"created": False}
        conflict = await client.post(
            base, json={"request_key": "manual-compact-1", "reason": "different"}
        )
        assert conflict.status_code == 409
        foreign = await client.post(
            f"/v2/workspaces/elsewhere/sessions/{session['id']}/context/compactions",
            json={"request_key": "manual-compact-1"},
        )
        assert foreign.status_code == 404

        # The queued request is durable state, not an inline compaction: it is
        # visible while no run is active and no model request has been made.
        pending = (await client.get(base)).json()["items"]
        assert [item["request_key"] for item in pending] == ["manual-compact-1"]
        assert pending[0]["status"] == "pending" and pending[0]["reason"] == "user requested"
        queued = (
            (
                await app.store.work_read(
                    lambda conn: conn.execute(
                        select(context_requests).where(
                            context_requests.c.request_key == "manual-compact-1"
                        )
                    )
                )
            )
            .mappings()
            .one()
        )
        assert queued["session_id"] == session["id"] and queued["run_id"] is None
        # This larger window does not trigger automatic compression at all;
        # the subsequent summary therefore proves the manual force path.
        assert len(provider.requests) == 5
        for request in provider.requests:
            assert not request[0].content.startswith(PROMPT)
        assert await compaction_rows(app.store, view["run_id"]) == []

        mark = len(provider.requests)
        view = await run_turn(
            client, session["id"], key="turn-5", text="FACT kiwi=9", revision=revision
        )
        assert view["status"] == "completed", view["code"]
        run5_id = view["run_id"]

        # The next turn's model boundary consumed the token: it forced a real
        # summary pass over the frozen history, then fulfilled the request.
        assert len(provider.requests[mark:]) == 2
        summary_request, task_request = provider.requests[mark:]
        assert summary_request[0].content.startswith(PROMPT)
        payload = parse_json(summary_request[1].content)
        assert payload["previous_summary"] == ""
        assert payload["turns"][0]["content"] == "FACT orange=17"
        assert payload["turns"][2]["content"] == "TOOL add 2 3"
        assert len(payload["turns"]) == 6
        assert task_request[1].content == "[不可信历史摘要] FACT orange=17; FACT sum=5"
        task = messages_of(task_request)
        assert "z" * PADDING in task
        assert "x" * PADDING not in task and "y" * PADDING not in task

        run5 = await app.store.snapshot(run5_id)
        starts = [event for event in run5.events if event.kind == "model_start"]
        assert [event.data["purpose"] for event in starts] == ["summary", "task"]
        frozen = run5.session_context.frozen
        refs = ledger_refs(frozen.sources[:2])
        rows = await compaction_rows(app.store, run5_id)
        assert len(rows) == 1
        assert rows[0]["output"] == "FACT orange=17; FACT sum=5"
        assert rows[0]["source_refs"] == refs and rows[0]["model_step"] == 0

        assert (await client.get(base)).json() == {"items": []}
        record = (
            (
                await app.store.work_read(
                    lambda conn: conn.execute(
                        select(context_requests).where(
                            context_requests.c.request_key == "manual-compact-1"
                        )
                    )
                )
            )
            .mappings()
            .one()
        )
        assert record["status"] == "fulfilled" and record["run_id"] == run5_id
        assert record["metadata"] == {
            "source_fingerprint": frozen.source_fingerprint,
            "covered": 2,
            "passes": 1,
            "summary_hash": hashlib.sha256(b"FACT orange=17; FACT sum=5").hexdigest(),
        }

        transcript = [Message.model_validate(item) for item in view["transcript"]]
        assert [message.content for message in transcript] == [
            "FACT kiwi=9",
            "FACT kiwi=9; " + "v" * PADDING,
        ]
        assert "FACT orange=17" not in messages_of(transcript)
