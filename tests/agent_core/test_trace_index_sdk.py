"""TraceSDK本地/远端只读入口与显式导出测试。"""

import httpx
import pytest
from pydantic import SecretStr
from sqlalchemy import insert

from krim_agent.api.server import create_app
from krim_agent.client import AsyncRunClient
from krim_agent.composition import open_application
from krim_agent.contracts import ServiceError
from krim_agent.security import AccessPolicy, Credential, Membership
from krim_agent.sessions.types import SessionCreate
from krim_agent.storage.models import runs, turns
from krim_agent.storage.sqlite import SQLiteStore


@pytest.mark.asyncio
async def test_trace_sdk_is_exported_and_reads_local_and_remote_without_posts(tmp_path):
    from krim_agent.sdk import TraceIndexItem, TraceIndexPage, TraceSDK

    policy = AccessPolicy(
        credentials=(
            Credential(
                principal_id="owner",
                token=SecretStr("owner-sdk-token"),
                generation=7,
                memberships=(Membership(workspace_id="sdk-workspace"),),
            ),
        )
    )
    database = tmp_path / "trace-sdk.sqlite"
    app = await open_application(database, access_policy=policy)
    remote = None
    try:
        principal = app.access_policy.authenticate("Bearer owner-sdk-token")
        session = await app.sessions.create(
            principal,
            "sdk-workspace",
            SessionCreate(request_key="sdk-session", title="SDK title"),
        )
        run_id = "20000000-0000-0000-0000-000000000001"
        turn_id = "30000000-0000-0000-0000-000000000001"
        async with app.store.lock, app.store.engine.begin() as conn:
            await conn.execute(
                insert(runs).values(
                    id=run_id,
                    request_key="sdk-index-run",
                    request_hash="c" * 64,
                    request={"text": "SDK_PRIVATE_PROMPT"},
                    target={"params": "SDK_PRIVATE_PARAMS"},
                    status="completed",
                    trace_id="d" * 32,
                    root_span_id="e" * 16,
                    created_at=80.0,
                    finished_at=81.0,
                    code="SDK_PRIVATE_ERROR",
                    stop_requested=False,
                )
            )
            await conn.execute(
                insert(turns).values(
                    id=turn_id,
                    session_id=session.id,
                    workspace_id=session.workspace_id,
                    owner_id=session.owner_id,
                    request_key="sdk-index-turn",
                    request_hash="f" * 64,
                    text="SDK_PRIVATE_PROMPT",
                    status="completed",
                    ordinal=1,
                    accepted_revision=0,
                    run_id=run_id,
                    context_snapshot=None,
                    context_fingerprint=None,
                    transcript=[{"role": "assistant", "content": "SDK_PRIVATE_ANSWER"}],
                    projection_version="test",
                    code="SDK_PRIVATE_ERROR",
                    created_at=80.0,
                    started_at=80.0,
                    finished_at=81.0,
                )
            )

        local = TraceSDK(app.sessions, principal=principal, workspace="sdk-workspace")
        page = await local.list(
            status="completed", session_id=session.id, turn_id=turn_id
        )
        assert isinstance(page, TraceIndexPage)
        assert isinstance(page.items[0], TraceIndexItem)
        assert page.items[0].run_id == run_id
        assert page.items[0].turn_id == turn_id
        assert page.items[0].status == "completed"
        assert page.next_cursor is None

        api = create_app(application=app)
        methods = []
        queries = []

        async def observed(scope, receive, send):
            if scope["type"] == "http":
                methods.append(scope["method"])
                queries.append(scope["query_string"])
            await api(scope, receive, send)

        async with api.router.lifespan_context(api):
            remote = AsyncRunClient("http://krim.test", api_key="owner-sdk-token")
            await remote.close()
            remote.http = httpx.AsyncClient(
                base_url="http://krim.test",
                transport=httpx.ASGITransport(app=observed),
                headers={"Authorization": "Bearer owner-sdk-token"},
            )
            remote_sdk = TraceSDK(remote, principal=principal, workspace="sdk-workspace")
            remote_page = await remote_sdk.list(
                limit=5,
                status="completed",
                session_id=session.id,
                turn_id=turn_id,
            )
            assert remote_page == page
        assert methods == ["GET"]
        assert b"turn_id=" + turn_id.encode() in queries[0]
        assert app.provider.calls == 0
        assert "SDK_PRIVATE_PROMPT" not in repr(page.model_dump(mode="json"))
        assert "SDK_PRIVATE_ANSWER" not in repr(remote_page.model_dump(mode="json"))

        reader = await SQLiteStore.open(database, readonly=True)
        try:
            readonly_page = await TraceSDK(
                reader, principal=principal, workspace="sdk-workspace"
            ).list(limit=5)
            assert readonly_page.items[0].run_id == run_id
        finally:
            await reader.close()
        with pytest.raises(ServiceError, match="closed"):
            await TraceSDK(reader, principal=principal, workspace="sdk-workspace").list()
    finally:
        if remote is not None:
            await remote.close()
        await app.close()


@pytest.mark.asyncio
async def test_trace_filters_reject_non_contract_python_values_without_reads(tmp_path):
    from krim_agent.sdk import TraceSDK

    app = await open_application(tmp_path / "trace-filter-types.sqlite")
    try:
        sdk = TraceSDK(app.sessions)
        invalid = (
            {"limit": True},
            {"limit": 2.5},
            {"limit": "2"},
            {"status": []},
            {"session_id": 1},
            {"turn_id": False},
        )
        for parameters in invalid:
            with pytest.raises(ServiceError, match="invalid_input"):
                await sdk.list(**parameters)
            with pytest.raises(ServiceError, match="invalid_input"):
                await app.sessions.traces(sdk.principal, sdk.workspace, **parameters)
        assert app.provider.calls == 0
    finally:
        await app.close()
