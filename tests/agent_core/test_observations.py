"""只读观测API：授权边界、游标分页、旧数据缺失、usage去重与来源关联测试。"""

import asyncio
import hashlib
import uuid

import httpx
import pytest
from pydantic import SecretStr
from sqlalchemy import event, func, insert, select, update

from krim_agent.api.server import create_app
from krim_agent.composition import open_application
from krim_agent.contracts import RunEvent, RunRequest, RunView, fingerprint
from krim_agent.observation_views import project_detail
from krim_agent.runtime.contracts import RunSnapshot
from krim_agent.security import AccessPolicy, Credential, Membership
from krim_agent.sessions.types import SessionCreate, TurnRequest
from krim_agent.storage.models import compactions, run_events, runs, sessions, turns

PROMPT_SECRET = "PRIVATE_PROMPT_SENTINEL"
ANSWER_SECRET = "PRIVATE_ANSWER_SENTINEL"
PARAM_SECRET = "PRIVATE_PARAMETER_SENTINEL"
ERROR_SECRET = "PRIVATE_ERROR_SENTINEL"
SUMMARY_SECRET = "PRIVATE_SUMMARY_SENTINEL"


def access_policy() -> AccessPolicy:
    return AccessPolicy(
        credentials=(
            Credential(
                principal_id="alice",
                token=SecretStr("alice-obs-token"),
                generation=3,
                memberships=(Membership(workspace_id="alpha"),),
            ),
            Credential(
                principal_id="bob",
                token=SecretStr("bob-obs-token"),
                memberships=(Membership(workspace_id="alpha"),),
            ),
            Credential(
                principal_id="viewer",
                token=SecretStr("viewer-obs-token"),
                memberships=(Membership(workspace_id="alpha", role="viewer"),),
            ),
            Credential(
                principal_id="admin",
                token=SecretStr("admin-obs-token"),
                generation=5,
                memberships=(
                    Membership(workspace_id="alpha", role="admin"),
                    Membership(workspace_id="beta", role="admin"),
                ),
            ),
        )
    )


async def run_turn(app, actor_token: str, workspace: str, key: str, text: str):
    principal = app.access_policy.authenticate("Bearer " + actor_token)
    session = await app.sessions.create(principal, workspace, SessionCreate(request_key=key))
    turn = await app.sessions.submit(
        principal,
        workspace,
        session.id,
        TurnRequest(request_key=key + "-turn", text=text, expected_revision=0),
    )
    for _ in range(600):
        current = await app.sessions.turn(principal, workspace, session.id, turn.id)
        if current.status not in {"queued", "running"}:
            return principal, session, current
        await asyncio.sleep(0.02)
    raise AssertionError("turn未在预算内收敛")


async def seed_session_row(app, workspace: str, owner: str, key: str) -> str:
    session_id = str(uuid.uuid4())
    async with app.store.lock, app.store.engine.begin() as conn:
        await conn.execute(
            insert(sessions).values(
                id=session_id,
                workspace_id=workspace,
                owner_id=owner,
                create_key=key,
                request_hash="d" * 64,
                title="Safe session title",
                target_key="demo",
                target={},
                context_policy={},
                revision=0,
                status="open",
                dispatch_paused=False,
                created_at=1.0,
                closed_at=None,
            )
        )
    return session_id


async def seed_run_and_turn(
    app,
    session_id: str,
    workspace: str,
    owner: str,
    number: int,
    created_at: float,
    run_status: str = "completed",
    turn_status: str = "completed",
    *,
    with_run: bool = True,
) -> tuple[str, str]:
    run_id = f"00000000-0000-0000-0000-{number:012d}"
    turn_id = f"10000000-0000-0000-0000-{number:012d}"
    finished = None if run_status in {"queued", "running", "awaiting"} else created_at + 0.5
    async with app.store.lock, app.store.engine.begin() as conn:
        if with_run:
            await conn.execute(
                insert(runs).values(
                    id=run_id,
                    request_key=f"obs-legacy-run-{number}",
                    request_hash="a" * 64,
                    request={
                        "text": PROMPT_SECRET,
                        "request_key": f"obs-legacy-run-{number}",
                        "target": "demo",
                    },
                    target={"parameters": PARAM_SECRET},
                    status=run_status,
                    trace_id=f"{number:032x}",
                    root_span_id=f"{number:016x}",
                    created_at=created_at,
                    finished_at=finished,
                    code=ERROR_SECRET,
                    stop_requested=False,
                )
            )
        await conn.execute(
            insert(turns).values(
                id=turn_id,
                session_id=session_id,
                workspace_id=workspace,
                owner_id=owner,
                request_key=f"obs-legacy-turn-{number}",
                request_hash="b" * 64,
                text=PROMPT_SECRET,
                status=turn_status,
                ordinal=number,
                accepted_revision=0,
                run_id=run_id if with_run else None,
                context_snapshot=None,
                context_fingerprint=None,
                transcript=[{"role": "assistant", "content": ANSWER_SECRET}],
                projection_version="test",
                code=ERROR_SECRET,
                created_at=created_at,
                started_at=created_at,
                finished_at=finished,
            )
        )
    return run_id, turn_id


async def append_raw_events(app, run_id: str, rows: list[tuple[str, dict, float]]) -> None:
    async with app.store.lock, app.store.engine.begin() as conn:
        last = (
            await conn.execute(
                select(func.max(run_events.c.seq)).where(run_events.c.run_id == run_id)
            )
        ).scalar_one()
        seq = (last if last is not None else -1) + 1
        for offset, (kind, data, at) in enumerate(rows):
            await conn.execute(
                insert(run_events).values(
                    run_id=run_id,
                    seq=seq + offset,
                    event_id=f"obs-fixture-{run_id}-{seq + offset}",
                    kind=kind,
                    data=data,
                    at=at,
                )
            )


async def seed_compaction(
    app,
    run_id: str,
    session_id: str,
    pass_index: int,
    status: str,
    *,
    refs: list[dict] | None = None,
    output: str | None = None,
    step: int = 1,
    reserved: int = 4096,
    created_at: float = 5.0,
) -> None:
    refs = refs if refs is not None else []
    async with app.store.lock, app.store.engine.begin() as conn:
        await conn.execute(
            insert(compactions).values(
                run_id=run_id,
                pass_index=pass_index,
                session_id=session_id,
                status=status,
                source_refs=refs,
                source_hash=fingerprint(refs),
                policy={
                    "context": {"reserved_output_tokens": reserved},
                    "counter_version": "approx-utf8/1",
                },
                prompt_hash="p" * 64,
                provider_identity={},
                model_step=step,
                output=output,
                output_hash=hashlib.sha256(output.encode()).hexdigest() if output else None,
                created_at=created_at,
                finished_at=created_at + 1.0 if status == "complete" else None,
            )
        )


async def client_for(api, token: str | None = None):
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=api),
        base_url="http://krim.test",
        headers={"Authorization": "Bearer " + token} if token else {},
    )


MANIFEST = {
    "version": 1,
    "counter_version": "approx-utf8/1",
    "input_bytes": 100,
    "estimated_tokens": 25,
    "items": [],
}


@pytest.mark.asyncio
async def test_detail_projects_requests_tools_trace_and_source_join(tmp_path):
    app = await open_application(tmp_path / "obs-detail.sqlite", access_policy=access_policy())
    try:
        _, session, turn = await run_turn(
            app, "alice-obs-token", "alpha", "alice-detail", "/add 2 3"
        )
        assert turn.status == "completed"
        api = create_app(application=app)
        async with api.router.lifespan_context(api):
            async with await client_for(api, "alice-obs-token") as client:
                response = await client.get(
                    f"/v2/workspaces/alpha/sessions/{session.id}/turns/{turn.id}/observations"
                )
                assert response.status_code == 200, response.text
                body = response.json()
                assert body["session_id"] == session.id
                assert body["turn_id"] == turn.id
                assert body["cost"] is None and body["cost_status"] == "unpriced"
                run = body["run"]
                assert run["run_id"] == turn.run_id
                assert run["status"] == "completed"
                assert run["request_key"] == "ac-turn." + turn.id
                assert run["duration_source"] == "wall_clock"
                assert run["duration_ms"] >= 0.0
                trace = body["trace"]
                assert trace["trace_id"] == run["trace_id"]
                assert trace["complete"] is True
                assert {span["kind"] for span in trace["spans"]} == {"run", "model", "tool"}
                configuration = body["configuration"]
                assert configuration is not None
                assert configuration["key"] == "demo"
                assert configuration["provider_behavior_version"] == "demo-parser/1"
                assert configuration["model"] is None
                assert configuration["prompt_fingerprint"]
                assert configuration["limits_fingerprint"]
                assert configuration["max_output_tokens"] is None
                assert "system_prompt" not in configuration

                requests = body["requests"]
                assert len(requests) == 2
                first, second = requests
                assert first["status"] == "ok" and second["status"] == "ok"
                assert first["purpose"] is None
                assert first["tool_calls"] == 1 and second["tool_calls"] == 0
                for request in requests:
                    assert request["manifest_status"] == "present"
                    manifest = request["manifest"]
                    assert manifest["version"] == 1
                    assert manifest["counter_version"] == "approx-utf8/1"
                    assert manifest["model_context_window"] is None
                    assert not manifest["items_truncated"]
                    assert (
                        sum(item["input_bytes"] for item in manifest["items"])
                        == manifest["input_bytes"]
                    )
                    assert request["timing"]["source"] == "model_end"
                    assert request["timing"]["elapsed_ms"] > 0
                    assert request["duration_source"] == "mono"
                    assert request["duration_ms"] == request["timing"]["elapsed_ms"]
                    usage = request["usage"]
                    assert usage["usage"] is None
                    assert usage["usage_reports"] == 0
                    assert usage["usage_final"] is None
                    assert request["cost"] is None

                tools = body["tools"]
                assert len(tools) == 1
                tool = tools[0]
                assert tool["name"] == "add"
                assert tool["status"] == "ok"
                assert tool["step_index"] == first["step_index"] == 0
                assert tool["provider_call_id"]
                assert tool["executor_started"] is True
                assert tool["result_available"] is True
                assert "arguments" not in tool and "value" not in tool
                tool_result_items = [
                    item
                    for item in second["manifest"]["items"]
                    if item["category"] == "tool_result"
                ]
                assert tool_result_items
                assert tool_result_items[0]["call_id"] == tool["provider_call_id"]

                coverage = body["coverage"]
                assert coverage["passes"] == 0
                assert coverage["history_turns"] is None
                assert coverage["manifest_status"] == "present"
                usage_block = body["usage"]
                assert usage_block["totals"] is None
                assert usage_block["usage_complete"] is False
                assert usage_block["steps_with_usage"] == 0
                assert usage_block["steps_missing_usage"] == 2
                assert body["compactions"] == []
                assert PROMPT_SECRET not in response.text
        assert app.provider.calls == 2
    finally:
        await app.close()


@pytest.mark.asyncio
async def test_usage_collapses_duplicates_reads_cache_and_old_fields(tmp_path):
    app = await open_application(tmp_path / "obs-usage.sqlite", access_policy=access_policy())
    try:
        _, session, turn = await run_turn(
            app, "alice-obs-token", "alpha", "alice-usage", "/add 2 3"
        )
        run_id = turn.run_id
        assert run_id is not None
        await append_raw_events(
            app,
            run_id,
            [
                (
                    "usage",
                    {
                        "step_index": 0,
                        "usage": {"input_tokens": 10, "output_tokens": 4, "total_tokens": 14},
                        "final": False,
                    },
                    100.0,
                ),
                (
                    "usage",
                    {
                        "step_index": 0,
                        "usage": {
                            "input_tokens": 15,
                            "output_tokens": 6,
                            "total_tokens": 21,
                            "cached_input_tokens": 5,
                        },
                        "final": True,
                    },
                    101.0,
                ),
                (
                    "model_start",
                    {
                        "span_id": "c3c3c3c3c3c3c3c3",
                        "step_index": 3,
                        "purpose": "summary",
                        "context_manifest": dict(MANIFEST),
                    },
                    102.0,
                ),
                (
                    "usage",
                    {"step_index": 3, "usage": {"input_tokens": 7}, "final": True},
                    103.0,
                ),
                (
                    "model_end",
                    {
                        "span_id": "c3c3c3c3c3c3c3c3",
                        "step_index": 3,
                        "tool_calls": [],
                        "elapsed_ms": 123.5,
                        "first_content_ms": 12.5,
                        "first_text_ms": None,
                    },
                    104.0,
                ),
                (
                    "model_start",
                    {
                        "span_id": "c4c4c4c4c4c4c4c4",
                        "step_index": 4,
                        "context_manifest": {"version": 2},
                    },
                    105.0,
                ),
                (
                    "model_end",
                    {
                        "span_id": "c4c4c4c4c4c4c4c4",
                        "step_index": 4,
                        "tool_calls": [],
                        "elapsed_ms": 5.0,
                        "first_content_ms": None,
                        "first_text_ms": None,
                    },
                    106.0,
                ),
                (
                    "model_start",
                    {
                        "span_id": "c5c5c5c5c5c5c5c5",
                        "step_index": 5,
                        "context_manifest": dict(MANIFEST),
                    },
                    107.0,
                ),
            ],
        )
        api = create_app(application=app)
        async with api.router.lifespan_context(api):
            async with await client_for(api, "alice-obs-token") as client:
                response = await client.get(
                    f"/v2/workspaces/alpha/sessions/{session.id}/turns/{turn.id}/observations"
                )
                assert response.status_code == 200, response.text
                body = response.json()
                requests = {request["step_index"]: request for request in body["requests"]}
                assert set(requests) == {0, 1, 3, 4, 5}
                assert len(body["requests"]) == 5

                first = requests[0]
                assert first["usage"]["usage"]["input_tokens"] == 15
                assert first["usage"]["usage"]["cached_input_tokens"] == 5
                assert first["usage"]["usage_reports"] == 2
                assert first["usage"]["usage_final"] is True

                third = requests[3]
                assert third["purpose"] == "summary"
                assert third["manifest_status"] == "present"
                assert third["timing"]["elapsed_ms"] == 123.5
                assert third["timing"]["first_content_ms"] == 12.5
                assert third["timing"]["first_text_ms"] is None

                fourth = requests[4]
                assert fourth["manifest"] is None
                assert fourth["manifest_status"] == "invalid"
                assert fourth["timing"]["elapsed_ms"] == 5.0

                fifth = requests[5]
                assert fifth["status"] == "unknown"
                assert fifth["ended_at"] is None
                assert fifth["duration_ms"] is None
                assert fifth["duration_source"] is None
                assert fifth["manifest_status"] == "present"

                usage_block = body["usage"]
                totals = usage_block["totals"]
                assert totals["input_tokens"] == 22
                assert totals["output_tokens"] == 6
                assert totals["total_tokens"] == 21
                assert totals["cached_input_tokens"] == 5
                assert usage_block["usage_complete"] is False
                assert usage_block["steps_with_usage"] == 2
                assert usage_block["steps_missing_usage"] == 3
                assert body["coverage"]["manifest_status"] == "partial"
                assert PROMPT_SECRET not in response.text
    finally:
        await app.close()


@pytest.mark.asyncio
async def test_list_authorizes_scopes_pages_and_degrades_unavailable(tmp_path):
    app = await open_application(tmp_path / "obs-list.sqlite", access_policy=access_policy())
    try:
        _, alice_session, alice_turn = await run_turn(
            app, "alice-obs-token", "alpha", "alice-list", "/add 2 3"
        )
        _, bob_session, bob_turn = await run_turn(app, "bob-obs-token", "alpha", "bob-list", "hi")
        _, beta_session, beta_turn = await run_turn(
            app, "admin-obs-token", "beta", "beta-list", "hi"
        )
        viewer_session = await seed_session_row(app, "alpha", "viewer", "viewer-list")
        _, beta_session2, beta_turn2 = await run_turn(
            app, "admin-obs-token", "beta", "beta-list-2", "hi"
        )
        completed_run, _ = await seed_run_and_turn(
            app, viewer_session, "alpha", "viewer", 1, 50.0
        )
        awaiting_run, _ = await seed_run_and_turn(
            app,
            viewer_session,
            "alpha",
            "viewer",
            2,
            51.0,
            run_status="awaiting",
            turn_status="running",
        )
        extra_run, _ = await seed_run_and_turn(app, viewer_session, "alpha", "viewer", 3, 52.0)
        api = create_app(application=app)

        statements = []

        def capture_sql(_conn, _cursor, statement, _parameters, _context, _many):
            if statement.lstrip().upper().startswith("SELECT"):
                statements.append(statement)

        event.listen(app.store.engine.sync_engine, "before_cursor_execute", capture_sql)
        try:
            async with api.router.lifespan_context(api):
                async with await client_for(api, "alice-obs-token") as client:
                    owner = await client.get("/v2/workspaces/alpha/observations?limit=20")
                    assert owner.status_code == 200, owner.text
                    body = owner.json()
                    assert [item["session_id"] for item in body["items"]] == [alice_session.id]
                    item = body["items"][0]
                    assert item["projection_status"] == "ok"
                    assert item["model_requests"] == 2
                    assert item["tool_calls"] == 1
                    assert item["manifest_status"] == "present"
                    assert item["manifest_requests"] == 2
                    assert item["usage_complete"] is False
                    assert item["compaction_passes"] == 0
                    assert item["cost_status"] == "unpriced"
                    assert body["scope"] == {
                        "version": 1,
                        "workspace_id": "alpha",
                        "role": "member",
                        "status": None,
                        "session_id": None,
                        "limit": 20,
                        "counted": "returned_page",
                    }
                    serialized = owner.text
                    for private in (PROMPT_SECRET, ANSWER_SECRET, PARAM_SECRET, ERROR_SECRET):
                        assert private not in serialized

                    cross = await client.get("/v2/workspaces/beta/observations")
                    assert cross.status_code == 404
                    peer = await client.get(
                        "/v2/workspaces/alpha/observations?session_id=" + bob_session.id
                    )
                    assert peer.status_code == 200
                    assert peer.json()["items"] == []

                async with await client_for(api, "viewer-obs-token") as client:
                    viewer = await client.get("/v2/workspaces/alpha/observations")
                    assert viewer.status_code == 200, viewer.text
                    items = viewer.json()["items"]
                    assert {row["run_id"] for row in items} == {
                        completed_run,
                        awaiting_run,
                        extra_run,
                    }
                    statuses = {row["run_id"]: row["status"] for row in items}
                    assert statuses[awaiting_run] == "awaiting"
                    degraded = {row["run_id"]: row for row in items}
                    for run_id in (completed_run, awaiting_run, extra_run):
                        assert degraded[run_id]["projection_status"] == "unavailable"
                        assert degraded[run_id]["projection_code"] == "facts_corrupt"
                        assert degraded[run_id]["model_requests"] is None
                        assert degraded[run_id]["usage"] is None
                        assert degraded[run_id]["usage_complete"] is None
                    assert PROMPT_SECRET not in viewer.text

                async with await client_for(api, "admin-obs-token") as client:
                    admin = await client.get("/v2/workspaces/alpha/observations?status=completed")
                    assert admin.status_code == 200, admin.text
                    assert {row["run_id"] for row in admin.json()["items"]} == {
                        alice_turn.run_id,
                        bob_turn.run_id,
                        completed_run,
                        extra_run,
                    }
                    awaiting = await client.get("/v2/workspaces/alpha/observations?status=awaiting")
                    assert awaiting.status_code == 200
                    assert [row["run_id"] for row in awaiting.json()["items"]] == [awaiting_run]
                    assert admin.json()["scope"]["role"] == "admin"
                    beta = await client.get("/v2/workspaces/beta/observations")
                    beta_rows = beta.json()["items"]
                    assert {(row["session_id"], row["run_id"]) for row in beta_rows} == {
                        (beta_session2.id, beta_turn2.run_id),
                        (beta_session.id, beta_turn.run_id),
                    }
                    order = [(row["created_at"], row["run_id"]) for row in beta_rows]
                    assert order == sorted(order, reverse=True)

                async with await client_for(api, "viewer-obs-token") as client:
                    page1 = await client.get("/v2/workspaces/alpha/observations?limit=2")
                    assert page1.status_code == 200
                    page1 = page1.json()
                    assert len(page1["items"]) == 2
                    cursor = page1["next_cursor"]
                    assert cursor
                    page2 = await client.get(
                        "/v2/workspaces/alpha/observations?limit=2&cursor=" + cursor
                    )
                    assert page2.status_code == 200
                    page2 = page2.json()
                    assert len(page2["items"]) == 1
                    assert page2["next_cursor"] is None
                    combined = page1["items"] + page2["items"]
                    assert len({row["run_id"] for row in combined}) == 3
                    for bad in (
                        f"limit=1&cursor={cursor}",
                        f"limit=2&cursor={cursor}&status=completed",
                        f"limit=2&cursor={cursor}&session_id={viewer_session}",
                    ):
                        response = await client.get("/v2/workspaces/alpha/observations?" + bad)
                        assert response.status_code == 416, response.text
                    invalid = await client.get("/v2/workspaces/alpha/observations?cursor=%%%")
                    assert invalid.status_code == 416
                    bad_limit = await client.get("/v2/workspaces/alpha/observations?limit=51")
                    assert bad_limit.status_code == 422
                    bad_status = await client.get(
                        "/v2/workspaces/alpha/observations?status=unknown"
                    )
                    assert bad_status.status_code == 422
                    post = await client.post("/v2/workspaces/alpha/observations")
                    assert post.status_code == 405

                # /traces 铸造的游标在 observations 上同scope可用（beta无awaiting，可对照）。
                async with await client_for(api, "admin-obs-token") as client:
                    trace_page = await client.get("/v2/workspaces/beta/traces?limit=1")
                    assert trace_page.status_code == 200
                    trace_cursor = trace_page.json()["next_cursor"]
                    assert trace_cursor
                    reused = await client.get(
                        "/v2/workspaces/beta/observations?limit=1&cursor=" + trace_cursor
                    )
                    assert reused.status_code == 200, reused.text
                    assert reused.json()["next_cursor"] is None
                    assert len(reused.json()["items"]) == 1
        finally:
            event.remove(app.store.engine.sync_engine, "before_cursor_execute", capture_sql)
        assert app.provider.calls == 5
    finally:
        await app.close()


@pytest.mark.asyncio
async def test_detail_authorizes_ownership_and_not_terminal(tmp_path):
    app = await open_application(tmp_path / "obs-auth.sqlite", access_policy=access_policy())
    try:
        _, alice_session, alice_turn = await run_turn(
            app, "alice-obs-token", "alpha", "alice-auth", "hello"
        )
        viewer_session = await seed_session_row(app, "alpha", "viewer", "viewer-auth")
        _, runless_turn = await seed_run_and_turn(
            app,
            viewer_session,
            "alpha",
            "viewer",
            9,
            20.0,
            with_run=False,
            turn_status="cancelled",
        )
        api = create_app(application=app)
        async with api.router.lifespan_context(api):
            async with await client_for(api, "viewer-obs-token") as client:
                forbidden = await client.get(
                    f"/v2/workspaces/alpha/sessions/{alice_session.id}"
                    f"/turns/{alice_turn.id}/observations"
                )
                assert forbidden.status_code == 404
                runless = await client.get(
                    f"/v2/workspaces/alpha/sessions/{viewer_session}"
                    f"/turns/{runless_turn}/observations"
                )
                assert runless.status_code == 409
                assert runless.json()["error"]["code"] == "not_terminal"
            async with await client_for(api, "alice-obs-token") as client:
                missing_turn = await client.get(
                    f"/v2/workspaces/alpha/sessions/{alice_session.id}"
                    "/turns/20000000-0000-0000-0000-000000000000/observations"
                )
                assert missing_turn.status_code == 404
                missing_session = await client.get(
                    "/v2/workspaces/alpha/sessions/30000000-0000-0000-0000-000000000000"
                    f"/turns/{alice_turn.id}/observations"
                )
                assert missing_session.status_code == 404
        assert app.provider.calls == 1
    finally:
        await app.close()


@pytest.mark.asyncio
async def test_compactions_coverage_from_real_table_without_summary_body(tmp_path):
    app = await open_application(tmp_path / "obs-compaction.sqlite", access_policy=access_policy())
    try:
        _, session, turn = await run_turn(
            app, "alice-obs-token", "alpha", "alice-compaction", "hello"
        )
        other_session = await app.sessions.create(
            app.access_policy.authenticate("Bearer alice-obs-token"),
            "alpha",
            SessionCreate(request_key="alice-compaction-other"),
        )
        run_id = turn.run_id
        assert run_id is not None
        refs = [{"turn_id": turn.id, "terminal_seq": 4}]
        await seed_compaction(
            app,
            run_id,
            session.id,
            0,
            "complete",
            refs=refs,
            output=SUMMARY_SECRET,
            step=2,
        )
        await seed_compaction(
            app,
            run_id,
            session.id,
            1,
            "reserved",
            refs=[{"turn_id": turn.id, "terminal_seq": 4}],
            step=3,
            reserved=8192,
        )
        api = create_app(application=app)
        async with api.router.lifespan_context(api):
            async with await client_for(api, "alice-obs-token") as client:
                response = await client.get(
                    f"/v2/workspaces/alpha/sessions/{session.id}/turns/{turn.id}/observations"
                )
                assert response.status_code == 200, response.text
                body = response.json()
                passes = body["compactions"]
                assert [row["pass_index"] for row in passes] == [0, 1]
                assert passes[0]["status"] == "complete"
                assert passes[0]["summary_bytes"] == len(SUMMARY_SECRET.encode())
                assert passes[0]["output_hash"] == hashlib.sha256(
                    SUMMARY_SECRET.encode()
                ).hexdigest()
                assert passes[0]["covered_turn_ids"] == [turn.id]
                assert passes[0]["covered_turns"] == 1
                assert passes[0]["summary_step_index"] == 2
                assert passes[0]["counter_version"] == "approx-utf8/1"
                assert passes[0]["reserved_output_tokens"] == 4096
                assert passes[1]["status"] == "reserved"
                assert passes[1]["summary_bytes"] is None
                assert passes[1]["output_hash"] is None
                assert passes[1]["reserved_output_tokens"] == 8192
                coverage = body["coverage"]
                assert coverage["passes"] == 2
                assert coverage["complete_passes"] == 1
                assert coverage["covered_turns"] == 1
                assert coverage["first_covered_turn_id"] == turn.id
                assert coverage["last_covered_turn_id"] == turn.id
                assert coverage["summary_step_index"] == 2
                assert coverage["history_turns"] is None
                assert SUMMARY_SECRET not in response.text

                # 摘要hash被篡改：不输出正文也不假装有效，直接facts_corrupt。
                async with app.store.lock, app.store.engine.begin() as conn:
                    await conn.execute(
                        update(compactions)
                        .where(compactions.c.run_id == run_id, compactions.c.pass_index == 0)
                        .values(output="tampered")
                    )
                tampered = await client.get(
                    f"/v2/workspaces/alpha/sessions/{session.id}/turns/{turn.id}/observations"
                )
                assert tampered.status_code == 409
                assert tampered.json()["error"]["code"] == "facts_corrupt"
                assert "tampered" not in tampered.text

                # 还原后，session_id与授权会话不一致的行：拒绝而不是跨会话泄漏。
                async with app.store.lock, app.store.engine.begin() as conn:
                    await conn.execute(
                        update(compactions)
                        .where(compactions.c.run_id == run_id, compactions.c.pass_index == 0)
                        .values(output=SUMMARY_SECRET)
                    )
                await seed_compaction(app, run_id, other_session.id, 5, "reserved", created_at=9.0)
                mismatched = await client.get(
                    f"/v2/workspaces/alpha/sessions/{session.id}/turns/{turn.id}/observations"
                )
                assert mismatched.status_code == 409
                assert mismatched.json()["error"]["code"] == "facts_corrupt"
        assert app.provider.calls == 1
    finally:
        await app.close()


@pytest.mark.asyncio
async def test_old_data_projection_reports_missing_instead_of_fabricating():
    events = (
        RunEvent(
            run_id="r", seq=0, event_id="e0", kind="run_started", data={"span_id": "aa"}, at=10.0
        ),
        RunEvent(
            run_id="r",
            seq=1,
            event_id="e1",
            kind="model_start",
            data={"span_id": "bb", "step_index": 1},
            at=10.1,
        ),
        RunEvent(
            run_id="r",
            seq=2,
            event_id="e2",
            kind="usage",
            data={
                "step_index": 1,
                "usage": {"input_tokens": 10, "output_tokens": 4, "total_tokens": 14},
                "final": False,
            },
            at=10.2,
        ),
        RunEvent(
            run_id="r",
            seq=3,
            event_id="e3",
            kind="model_end",
            data={"span_id": "bb", "step_index": 1, "tool_calls": []},
            at=10.5,
        ),
        RunEvent(
            run_id="r",
            seq=4,
            event_id="e4",
            kind="terminal",
            data={"status": "completed", "code": None},
            at=10.6,
        ),
    )
    snapshot = RunSnapshot(
        run=RunView(
            id="r",
            request_key="k",
            status="completed",
            trace_id="t" * 32,
            root_span_id="aa",
            created_at=10.0,
            finished_at=10.6,
        ),
        request=RunRequest(text="x", request_key="k"),
        target={},
        events=events,
    )
    detail = project_detail(
        snapshot, session_id="s", turn_id="u", compactions=[], history_turns=None
    )
    request = detail.requests[0]
    assert request.manifest is None and request.manifest_status == "missing"
    assert request.timing is None
    assert request.duration_source == "wall_clock"
    assert request.duration_ms == pytest.approx(400.0)
    assert request.usage.usage is not None
    assert request.usage.usage_final is False
    usage = detail.usage
    assert usage.totals is not None
    assert usage.totals.input_tokens == 10
    assert usage.usage_complete is False
    assert usage.steps_missing_usage == 0
    assert detail.coverage.manifest_status == "missing"
    assert detail.coverage.history_turns is None
    assert detail.run.duration_source == "wall_clock"
    assert detail.tools == []
    assert detail.configuration is None
