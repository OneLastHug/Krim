"""工作区会话Trace索引的API授权、分页与最小投影测试。"""

import uuid
from types import SimpleNamespace

import httpx
import pytest
from pydantic import SecretStr
from sqlalchemy import event, insert

from krim_agent.api.server import create_app
from krim_agent.composition import open_application
from krim_agent.security import AccessPolicy, Credential, Membership
from krim_agent.sessions.types import SessionCreate
from krim_agent.storage.models import runs, sessions, turns

PROMPT_SECRET = "PRIVATE_PROMPT_SENTINEL"
ANSWER_SECRET = "PRIVATE_ANSWER_SENTINEL"
PARAM_SECRET = "PRIVATE_PARAMETER_SENTINEL"
ERROR_SECRET = "PRIVATE_ERROR_SENTINEL"


def access_policy() -> AccessPolicy:
    return AccessPolicy(
        credentials=(
            Credential(
                principal_id="alice",
                token=SecretStr("alice-trace-token"),
                generation=3,
                memberships=(Membership(workspace_id="alpha"),),
            ),
            Credential(
                principal_id="bob",
                token=SecretStr("bob-trace-token"),
                memberships=(Membership(workspace_id="alpha"),),
            ),
            Credential(
                principal_id="viewer",
                token=SecretStr("viewer-trace-token"),
                memberships=(Membership(workspace_id="alpha", role="viewer"),),
            ),
            Credential(
                principal_id="admin",
                token=SecretStr("admin-trace-token"),
                generation=5,
                memberships=(
                    Membership(workspace_id="alpha", role="admin"),
                    Membership(workspace_id="beta", role="admin"),
                ),
            ),
        )
    )


async def create_session(app, actor: str, workspace: str, key: str):
    principal = app.access_policy.authenticate("Bearer " + actor + "-trace-token")
    return await app.sessions.create(
        principal, workspace, SessionCreate(request_key=key, title="Safe session title")
    )


async def seed_viewer_owned_session(app, key: str):
    session_id = str(uuid.uuid4())
    async with app.store.lock, app.store.engine.begin() as conn:
        await conn.execute(
            insert(sessions).values(
                id=session_id,
                workspace_id="alpha",
                owner_id="viewer",
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
    return SimpleNamespace(
        id=session_id,
        workspace_id="alpha",
        owner_id="viewer",
    )


async def seed_trace(app, session, number: int, created_at: float, status: str = "completed"):
    run_id = f"00000000-0000-0000-0000-{number:012d}"
    turn_id = f"10000000-0000-0000-0000-{number:012d}"
    finished_at = None if status in {"queued", "running"} else created_at + 0.5
    async with app.store.lock, app.store.engine.begin() as conn:
        await conn.execute(
            insert(runs).values(
                id=run_id,
                request_key=f"test-index-run-{number}",
                request_hash="a" * 64,
                request={"text": PROMPT_SECRET, "private_error": ERROR_SECRET},
                target={"parameters": PARAM_SECRET},
                status=status,
                trace_id=f"{number:032x}",
                root_span_id=f"{number:016x}",
                created_at=created_at,
                finished_at=finished_at,
                code=ERROR_SECRET,
                stop_requested=False,
            )
        )
        await conn.execute(
            insert(turns).values(
                id=turn_id,
                session_id=session.id,
                workspace_id=session.workspace_id,
                owner_id=session.owner_id,
                request_key=f"test-index-turn-{number}",
                request_hash="b" * 64,
                text=PROMPT_SECRET,
                status=status,
                ordinal=number,
                accepted_revision=0,
                run_id=run_id,
                context_snapshot=None,
                context_fingerprint=None,
                transcript=[{"role": "assistant", "content": ANSWER_SECRET}],
                projection_version="test",
                code=ERROR_SECRET,
                created_at=created_at,
                started_at=created_at,
                finished_at=finished_at,
            )
        )
    return run_id, turn_id


async def client_for(api, token: str):
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=api),
        base_url="http://krim.test",
        headers={"Authorization": "Bearer " + token},
    )


@pytest.mark.asyncio
async def test_trace_index_authorizes_and_returns_only_the_index_projection(tmp_path):
    app = await open_application(tmp_path / "trace-index.sqlite", access_policy=access_policy())
    try:
        alice_session = await create_session(app, "alice", "alpha", "alice-session")
        bob_session = await create_session(app, "bob", "alpha", "bob-session")
        viewer_session = await seed_viewer_owned_session(app, "viewer-session")
        beta_session = await create_session(app, "admin", "beta", "beta-session")
        alice_run, alice_turn = await seed_trace(app, alice_session, 1, 10.0)
        bob_run, _ = await seed_trace(app, bob_session, 2, 100.0)
        viewer_run, _ = await seed_trace(app, viewer_session, 3, 20.0)
        beta_run, _ = await seed_trace(app, beta_session, 4, 30.0)

        api = create_app(application=app)
        statements = []
        methods = []

        def capture_sql(_conn, _cursor, statement, _parameters, _context, _many):
            if statement.lstrip().upper().startswith("SELECT"):
                statements.append(statement)

        async def observed(scope, receive, send):
            if scope["type"] == "http":
                methods.append(scope["method"])
            await api(scope, receive, send)

        event.listen(app.store.engine.sync_engine, "before_cursor_execute", capture_sql)
        try:
            async with api.router.lifespan_context(api):
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=observed), base_url="http://krim.test"
                ) as client:
                    owner = await client.get(
                        "/v2/workspaces/alpha/traces?limit=20",
                        headers={"Authorization": "Bearer alice-trace-token"},
                    )
                    assert owner.status_code == 200, owner.text
                    owner_body = owner.json()
                    assert len(owner_body["items"]) == 1
                    item = owner_body["items"][0]
                    assert item == {
                        "source_kind": "session_turn",
                        "run_id": alice_run,
                        "trace_id": f"{1:032x}",
                        "session_id": alice_session.id,
                        "turn_id": alice_turn,
                        "session_title": "Safe session title",
                        "target_key": "demo",
                        "status": "completed",
                        "created_at": 10.0,
                        "finished_at": 10.5,
                    }
                    assert set(item) == {
                        "source_kind",
                        "run_id",
                        "trace_id",
                        "session_id",
                        "turn_id",
                        "session_title",
                        "target_key",
                        "status",
                        "created_at",
                        "finished_at",
                    }
                    serialized = owner.text
                    for private in (PROMPT_SECRET, ANSWER_SECRET, PARAM_SECRET, ERROR_SECRET):
                        assert private not in serialized
                    assert len(statements) == 1

                    viewer = await client.get(
                        "/v2/workspaces/alpha/traces",
                        headers={"Authorization": "Bearer viewer-trace-token"},
                    )
                    assert [row["run_id"] for row in viewer.json()["items"]] == [viewer_run]
                    peer = await client.get(
                        "/v2/workspaces/alpha/traces",
                        headers={"Authorization": "Bearer bob-trace-token"},
                    )
                    assert [row["run_id"] for row in peer.json()["items"]] == [bob_run]
                    admin = await client.get(
                        "/v2/workspaces/alpha/traces",
                        headers={"Authorization": "Bearer admin-trace-token"},
                    )
                    assert {row["run_id"] for row in admin.json()["items"]} == {
                        alice_run,
                        bob_run,
                        viewer_run,
                    }
                    cross_workspace = await client.get(
                        "/v2/workspaces/beta/traces",
                        headers={"Authorization": "Bearer alice-trace-token"},
                    )
                    assert cross_workspace.status_code == 404
                    beta = await client.get(
                        "/v2/workspaces/beta/traces",
                        headers={"Authorization": "Bearer admin-trace-token"},
                    )
                    assert [row["run_id"] for row in beta.json()["items"]] == [beta_run]
                    selected = await client.get(
                        f"/v2/workspaces/alpha/traces?session_id={alice_session.id}",
                        headers={"Authorization": "Bearer alice-trace-token"},
                    )
                    assert [row["run_id"] for row in selected.json()["items"]] == [alice_run]
        finally:
            event.remove(app.store.engine.sync_engine, "before_cursor_execute", capture_sql)
        assert methods and set(methods) == {"GET"}
        assert app.provider.calls == 0
    finally:
        await app.close()


@pytest.mark.asyncio
async def test_trace_index_keyset_pages_are_stable_and_cursor_is_strictly_scoped(tmp_path):
    app = await open_application(tmp_path / "trace-cursor.sqlite", access_policy=access_policy())
    try:
        alice_session = await create_session(app, "alice", "alpha", "alice-pages")
        bob_session = await create_session(app, "bob", "alpha", "bob-pages")
        # Peer 的最新时间戳不能挤占 Alice 经授权过滤后的分页位置。
        _, bob_turn = await seed_trace(app, bob_session, 20, 200.0)
        expected = []
        expected_turns = []
        for number in (11, 12, 13):
            run_id, turn_id = await seed_trace(app, alice_session, number, 50.0)
            expected.append(run_id)
            expected_turns.append(turn_id)
        api = create_app(application=app)
        async with api.router.lifespan_context(api):
            async with await client_for(api, "alice-trace-token") as client:
                page1 = await client.get("/v2/workspaces/alpha/traces?limit=1")
                assert page1.status_code == 200, page1.text
                page1 = page1.json()
                assert page1["items"][0]["run_id"] == max(expected)
                cursor = page1["next_cursor"]
                assert cursor
                page2 = await client.get(
                    "/v2/workspaces/alpha/traces?limit=1&cursor=" + cursor
                )
                assert page2.status_code == 200, page2.text
                page2 = page2.json()
                page3 = await client.get(
                    "/v2/workspaces/alpha/traces?limit=1&cursor=" + page2["next_cursor"]
                )
                assert page3.status_code == 200, page3.text
                page3 = page3.json()
                assert [
                    page1["items"][0]["run_id"],
                    page2["items"][0]["run_id"],
                    page3["items"][0]["run_id"],
                ] == sorted(expected, reverse=True)
                assert page3["next_cursor"] is None

                exact = await client.get(
                    "/v2/workspaces/alpha/traces?session_id="
                    + alice_session.id
                    + "&turn_id="
                    + expected_turns[0]
                )
                assert exact.status_code == 200, exact.text
                assert [row["run_id"] for row in exact.json()["items"]] == [expected[0]]
                turn_only = await client.get(
                    "/v2/workspaces/alpha/traces?turn_id=" + expected_turns[0]
                )
                assert turn_only.status_code == 200, turn_only.text
                assert [row["run_id"] for row in turn_only.json()["items"]] == [expected[0]]
                mismatched_session = await client.get(
                    "/v2/workspaces/alpha/traces?session_id="
                    + alice_session.id
                    + "&turn_id="
                    + bob_turn
                )
                assert mismatched_session.status_code == 200, mismatched_session.text
                assert mismatched_session.json()["items"] == []
                peer_read = await client.get(
                    "/v2/workspaces/alpha/traces?session_id="
                    + alice_session.id
                    + "&turn_id="
                    + expected_turns[0],
                    headers={"Authorization": "Bearer bob-trace-token"},
                )
                assert peer_read.status_code == 200, peer_read.text
                assert peer_read.json()["items"] == []

                for query in (
                    f"limit=1&cursor={cursor}&status=completed",
                    f"limit=2&cursor={cursor}",
                    f"limit=1&cursor={cursor}&session_id={bob_session.id}",
                    f"limit=1&cursor={cursor}&turn_id={expected_turns[0]}",
                ):
                    response = await client.get("/v2/workspaces/alpha/traces?" + query)
                    assert response.status_code == 416, response.text
                invalid = await client.get("/v2/workspaces/alpha/traces?cursor=%%%")
                assert invalid.status_code == 416, invalid.text
                admin_page = await client.get(
                    "/v2/workspaces/alpha/traces?limit=1",
                    headers={"Authorization": "Bearer admin-trace-token"},
                )
                admin_cursor = admin_page.json()["next_cursor"]
                cross_workspace = await client.get(
                    "/v2/workspaces/beta/traces?limit=1&cursor=" + admin_cursor,
                    headers={"Authorization": "Bearer admin-trace-token"},
                )
                assert cross_workspace.status_code == 416, cross_workspace.text
                bad_status = await client.get("/v2/workspaces/alpha/traces?status=unknown")
                assert bad_status.status_code == 422
                bad_limit = await client.get("/v2/workspaces/alpha/traces?limit=51")
                assert bad_limit.status_code == 422

                from krim_agent.contracts import ServiceError
                from krim_agent.sdk import TraceSDK
                from krim_agent.security import PrincipalContext

                alice = app.access_policy.authenticate("Bearer alice-trace-token")
                changed_generation = PrincipalContext(
                    alice.principal_id,
                    alice.generation + 1,
                    alice.memberships,
                    alice.legacy,
                )
                with pytest.raises(ServiceError, match="invalid_cursor"):
                    await TraceSDK(
                        app.sessions, principal=changed_generation, workspace="alpha"
                    ).list(limit=1, cursor=cursor)
        assert app.provider.calls == 0
    finally:
        await app.close()


@pytest.mark.asyncio
async def test_trace_index_status_filter_keeps_nullable_finished_at(tmp_path):
    app = await open_application(tmp_path / "trace-status.sqlite", access_policy=access_policy())
    try:
        session = await create_session(app, "alice", "alpha", "alice-running")
        run_id, _ = await seed_trace(app, session, 30, 90.0, status="running")
        api = create_app(application=app)
        async with api.router.lifespan_context(api):
            async with await client_for(api, "alice-trace-token") as client:
                response = await client.get("/v2/workspaces/alpha/traces?status=running")
                assert response.status_code == 200, response.text
                assert response.json()["items"] == [
                    {
                        "source_kind": "session_turn",
                        "run_id": run_id,
                        "trace_id": f"{30:032x}",
                        "session_id": session.id,
                        "turn_id": f"10000000-0000-0000-0000-{30:012d}",
                        "session_title": "Safe session title",
                        "target_key": "demo",
                        "status": "running",
                        "created_at": 90.0,
                        "finished_at": None,
                    }
                ]
        assert app.provider.calls == 0
    finally:
        await app.close()


@pytest.mark.asyncio
async def test_trace_index_rejects_reads_after_session_service_close(tmp_path):
    from krim_agent.contracts import ServiceError
    from krim_agent.sdk import TraceSDK

    app = await open_application(tmp_path / "trace-closed.sqlite", access_policy=access_policy())
    try:
        alice = app.access_policy.authenticate("Bearer alice-trace-token")
        await create_session(app, "alice", "alpha", "alice-closed")
        await app.sessions.close()
        with pytest.raises(ServiceError, match="closed"):
            await TraceSDK(app.sessions, principal=alice, workspace="alpha").list()
    finally:
        await app.close()
