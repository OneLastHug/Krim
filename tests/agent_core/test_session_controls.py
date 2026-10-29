"""Session control argument validation and SDK routing regressions."""

import httpx
import pytest

from krim_agent.client import AsyncRunClient
from krim_agent.composition import open_application
from krim_agent.contracts import ApprovalReply, ServiceError
from krim_agent.sdk import SessionSDK
from krim_agent.security import LOCAL
from krim_agent.sessions.types import ControlRequest, SessionCreate, TurnRequest


@pytest.mark.parametrize(
    ("operation", "turn_id", "suffix"),
    [
        ("rename", None, "/rename"),
        ("resume", None, "/resume"),
        ("close", None, "/close"),
        ("cancel_turn", "turn/id", "/turns/turn%2Fid/cancel"),
    ],
)
async def test_remote_sdk_routes_legal_controls(operation, turn_id, suffix):
    observed = []

    async def respond(request):
        observed.append((request.method, request.url.raw_path.decode("ascii")))
        return httpx.Response(
            200,
            json={
                "id": "control-id",
                "session_id": "session/id",
                "request_key": "control-key",
                "kind": operation,
                "status": "completed",
                "result": {},
            },
        )

    client = AsyncRunClient("https://example.test")
    await client.http.aclose()
    client.http = httpx.AsyncClient(
        base_url="https://example.test", transport=httpx.MockTransport(respond)
    )
    try:
        result = await SessionSDK(client, workspace="work space").control(
            "session/id",
            operation,
            ControlRequest(request_key="control-key", expected_revision=0),
            turn_id=turn_id,
        )
        assert result.kind == operation and result.status == "completed"
        assert observed == [
            (
                "POST",
                "/v2/workspaces/work%20space/sessions/session%2Fid" + suffix,
            )
        ]
    finally:
        await client.close()


@pytest.mark.parametrize(
    ("operation", "turn_id"),
    [
        ("unknown", None),
        ("cancel_turn", None),
        ("cancel_turn", ""),
        ("rename", "turn-id"),
        ("rename", ""),
        ("resume", "turn-id"),
        ("resume", ""),
        ("close", "turn-id"),
        ("close", ""),
    ],
)
async def test_remote_sdk_rejects_illegal_controls_before_request(operation, turn_id):
    observed = []

    async def respond(request):
        observed.append(request)
        return httpx.Response(500)

    client = AsyncRunClient("https://example.test")
    await client.http.aclose()
    client.http = httpx.AsyncClient(
        base_url="https://example.test", transport=httpx.MockTransport(respond)
    )
    try:
        with pytest.raises(ServiceError, match="invalid_input"):
            await SessionSDK(client).control(
                "session-id",
                operation,
                ControlRequest(request_key="control-key", expected_revision=0),
                turn_id=turn_id,
            )
        assert observed == []
    finally:
        await client.close()


async def test_local_sdk_routes_legal_session_controls(tmp_path):
    async with await open_application(tmp_path / "local-controls.sqlite") as app:
        sdk = SessionSDK(app.sessions)
        session = await sdk.create(SessionCreate(request_key="session"))
        renamed = await sdk.control(
            session.id,
            "rename",
            ControlRequest(request_key="rename", expected_revision=session.revision, title="New title"),
        )
        assert renamed.kind == "rename"
        current = await sdk.get(session.id)
        assert current.title == "New title"

        resumed = await sdk.control(
            session.id,
            "resume",
            ControlRequest(request_key="resume", expected_revision=current.revision),
        )
        assert resumed.kind == "resume"
        current = await sdk.get(session.id)
        closed = await sdk.control(
            session.id,
            "close",
            ControlRequest(request_key="close", expected_revision=current.revision),
        )
        assert closed.kind == "close" and closed.result["status"] == "closed"


async def test_local_service_rejects_illegal_combinations_before_lookup(tmp_path, monkeypatch):
    approval = ApprovalReply(
        request_key="approval-reply",
        call_id="call-id",
        arguments_hash="arguments-hash",
        tool_hash="tool-hash",
        allow=True,
    )
    invalid = [
        ("unknown", None, None, None),
        ("cancel_turn", None, None, None),
        ("cancel_turn", "", None, None),
        ("rename", "turn-id", None, None),
        ("rename", "", None, None),
        ("resume", "turn-id", None, None),
        ("resume", "", None, None),
        ("close", "turn-id", None, None),
        ("close", "", None, None),
        ("rename", None, approval, "request-id"),
        ("rename", None, None, "request-id"),
        ("rename", None, None, ""),
        ("cancel_turn", "turn-id", None, "request-id"),
        ("approval", None, approval, "request-id"),
        ("approval", "", approval, "request-id"),
        ("approval", "turn-id", None, "request-id"),
        ("approval", "turn-id", approval, None),
        ("approval", "turn-id", approval, ""),
    ]
    async with await open_application(tmp_path / "invalid-controls.sqlite") as app:
        session = await app.sessions.create(LOCAL, "local", SessionCreate(request_key="session"))

        async def unexpected_lookup(*args, **kwargs):
            raise AssertionError("invalid controls must be rejected before session lookup")

        monkeypatch.setattr(app.sessions, "get", unexpected_lookup)
        body = ControlRequest(request_key="control", expected_revision=0)
        for kind, turn_id, reply, request_id in invalid:
            with pytest.raises(ServiceError, match="invalid_input"):
                await app.sessions.control(
                    LOCAL,
                    "local",
                    session.id,
                    kind,
                    body,
                    turn_id=turn_id,
                    approval=reply,
                    request_id=request_id,
                )
        assert app.sessions.controls == {}


async def test_cancel_turn_without_turn_id_does_not_cancel_session(tmp_path, wire):
    wire.hold_at.add(1)
    async with wire.application(tmp_path / "missing-turn-id.sqlite") as app:
        sdk = SessionSDK(app.sessions)
        session = await sdk.create(SessionCreate(request_key="session"))
        turn = await sdk.submit(
            session.id,
            TurnRequest(request_key="turn", text="hold", expected_revision=session.revision),
        )
        await wire.wait_requests(1)
        current = await sdk.get(session.id)
        with pytest.raises(ServiceError, match="invalid_input"):
            await sdk.control(
                session.id,
                "cancel_turn",
                ControlRequest(request_key="cancel-missing-id", expected_revision=current.revision),
            )
        assert (await sdk.turn(session.id, turn.id)).status == "running"
        assert app.sessions.controls == {}

        wire.release.set()
        assert (await sdk.wait(session.id, turn.id)).status == "completed"
