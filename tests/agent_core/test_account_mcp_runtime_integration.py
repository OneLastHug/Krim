"""账号MCP运行时接线验收：turn准入、停靠/恢复、快照过期与凭据不泄漏。

模型侧用本地wire替身（仅脚本化工具调用），MCP侧用真实回环streamable_http
服务器（FastMCP + uvicorn）驱动 AccountMCPService.build_runtime 与引擎
停靠恢复路径：批准的调用经恢复时重建的连接真实到达服务器，携带轮换前
的冻结凭据；拒绝的调用不触网；凭据值不出现在事件/结果/追踪里。
"""

from __future__ import annotations

import asyncio
import json
import socket
from pathlib import Path

import pytest
from fixtures.session_server import WireFixture
from mcp.server.fastmcp import FastMCP
from uvicorn import Config, Server

from krim_agent.account_mcp.secrets import MasterKeyring
from krim_agent.account_mcp.service import (
    ConnectionCreate,
    ConnectionUpdate,
    SessionConnection,
    SessionMCPSelection,
)
from krim_agent.accounts.service import IdentityService
from krim_agent.composition import open_application
from krim_agent.config import Settings
from krim_agent.contracts import ApprovalReply
from krim_agent.providers.openai_compatible import (
    OpenAICompatibleConfig,
    OpenAICompatibleProvider,
)
from krim_agent.sessions.types import SessionCreate, TurnRequest
from krim_agent.storage.sqlite import SQLiteStore

CREDENTIAL = "mcp-cred-marker-3f9a"
ROTATED_CREDENTIAL = "mcp-cred-rotated-7c21"


class McpWire(WireFixture):
    """脚本化模型：/docs前缀发起MCP工具调用；任何tool观察后收束。"""

    def __init__(self, tool_name: str) -> None:
        super().__init__()
        self.tool_name = tool_name

    def response(self, body, index):
        last = body["messages"][-1]
        if last["role"] == "tool":
            try:
                value = json.loads(last["content"])
            except ValueError:
                value = last["content"]
            delta: dict = {"content": "done:" + json.dumps(value, ensure_ascii=False)}
            finish = "stop"
        else:
            text = last["content"]
            if text.startswith("/docs "):
                delta = {
                    "tool_calls": [
                        {
                            "index": 0,
                            "id": f"call-{index}",
                            "type": "function",
                            "function": {
                                "name": self.tool_name,
                                "arguments": json.dumps({"query": text[6:].strip()}),
                            },
                        }
                    ]
                }
                finish = "tool_calls"
            else:
                delta = {"content": "收到：" + text}
                finish = "stop"

        def frame(delta_value, finish_value):
            return (
                "data: "
                + json.dumps(
                    {
                        "choices": [
                            {"index": 0, "delta": delta_value, "finish_reason": finish_value}
                        ]
                    },
                    ensure_ascii=False,
                )
                + "\n\n"
            )

        return (frame(delta, None) + frame({}, finish) + "data: [DONE]\n\n").encode()


class DocsServer:
    """回环streamable_http MCP服务器；记录请求头与真实工具调用。"""

    def __init__(self) -> None:
        self.calls: list[dict] = []
        self.requests: list[dict] = []
        server = self
        mcp = FastMCP("docs")

        @mcp.tool()
        async def search(query: str) -> dict:
            server.calls.append({"tool": "search", "query": query})
            return {"result": "found:" + query}

        inner = mcp.streamable_http_app()

        async def observed(scope, receive, send):
            if scope["type"] == "http":
                headers = {
                    key.decode("latin-1").lower(): value.decode("latin-1")
                    for key, value in scope["headers"]
                }
                server.requests.append(
                    {"method": scope["method"], "credential": headers.get("x-test-credential")}
                )
            await inner(scope, receive, send)

        self.endpoint = observed
        self._server: Server | None = None
        self._listener: socket.socket | None = None
        self.port = 0

    async def start(self) -> None:
        listener = socket.socket()
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", 0))
        listener.listen(16)
        listener.setblocking(False)
        self.port = listener.getsockname()[1]
        self._listener = listener
        instance = Server(Config(self.endpoint, access_log=False, log_level="error"))
        self._server = instance
        task = asyncio.create_task(instance.serve(sockets=[listener]), name="mcp-fixture")
        async with asyncio.timeout(10):
            while not instance.started:
                if task.done():
                    await task
                await asyncio.sleep(0.005)

    async def stop(self) -> None:
        listener, self._listener = self._listener, None
        if self._server is not None:
            self._server.should_exit = True
            await self._server.shutdown()
        if listener is not None:
            listener.close()


async def open_managed_app(wire: McpWire, tmp_path: Path, key_file: Path):
    """预先建立managed身份库并写入主密钥，再经open_application装配账号MCP。"""
    database = Path(tmp_path) / "mcp-runtime.sqlite"
    store = await SQLiteStore.open(database)
    identities = await IdentityService.open(store)
    boot = await identities.bootstrap("admin", request_key="bootstrap")
    await identities.close()
    await store.close()
    provider = OpenAICompatibleProvider(
        OpenAICompatibleConfig(base_url=f"http://127.0.0.1:{wire.port}/v1", model="fixture")
    )
    app = None
    try:
        app = await open_application(
            database,
            settings=Settings(
                run_timeout=15.0,
                tool_timeout=6.0,
                approval_timeout=60.0,
                personal_mcp_key_file=key_file,
            ),
            provider=provider,
            registry=wire.registry(),
        )
        return app, provider, boot
    except BaseException:
        if app is not None:
            await app.close()
        await provider.close()
        raise


async def owner_principal(app, boot):
    admin = app.identities.authenticate("Bearer " + boot.token)
    account = await app.identities.create_account(
        admin, {"display_name": "mcp-owner"}, request_key="account-owner"
    )
    issued = await app.identities.issue_key(
        admin, account.account.principal_id, {}, request_key="key-owner"
    )
    owner = app.identities.authenticate("Bearer " + issued.token)
    profile = await app.identities.profile(owner)
    workspace = next(ref.workspace_id for ref in profile.workspaces if ref.personal)
    return owner, workspace


async def connect_and_select(app, owner, workspace, session_id, url: str):
    service = app.account_mcp
    created = await service.create_connection(
        owner,
        ConnectionCreate(
            request_key="create-docs",
            name="docs",
            url=url,
            allowed_hosts=("127.0.0.1",),
            selected_tools=("search",),
            readonly_tools=("search",),
            credentials={"X-Test-Credential": CREDENTIAL},
            timeout_seconds=10.0,
        ),
    )
    await service.session_selection(
        owner,
        workspace,
        session_id,
        SessionMCPSelection(
            request_key="select-docs",
            expected_revision=0,
            connections=(SessionConnection(connection_id=created["id"], revision=1),),
        ),
    )
    return created


async def park_turn(app, owner, workspace, session_id, request_key: str, text: str):
    turn = await app.sessions.submit(
        owner,
        workspace,
        session_id,
        TurnRequest(request_key=request_key, text=text, expected_revision=0),
    )
    async with asyncio.timeout(20):
        while True:
            current = await app.sessions.turn(owner, workspace, session_id, turn.id)
            if current.run_id is None and current.status == "failed":
                # 派发即败（如准入/接线错误）：立刻显式失败，不空等超时。
                raise AssertionError(f"turn failed before park: {current.code}")
            if current.run_id:
                view = await app.service.get(current.run_id)
                if view.status == "awaiting":
                    request = next(
                        (
                            dict(event.data)
                            for event in await app.service.events(current.run_id)
                            if event.kind == "approval_request"
                        ),
                        None,
                    )
                    if request is not None:
                        return turn, current.run_id, request
                task = app.service.tasks.get(current.run_id)
                if task is not None and task.done() and view.status != "awaiting":
                    raise AssertionError(f"run ended before park: {view.status}/{view.code}")
            await asyncio.sleep(0.01)


def reply(request, allow: bool) -> ApprovalReply:
    return ApprovalReply(
        request_key="mcp-turn",
        call_id=request["call_id"],
        tool_hash=request["tool_hash"],
        arguments_hash=request["arguments_hash"],
        allow=allow,
    )


async def assert_no_credential_leak(app, run_id: str) -> None:
    facts = await app.service.result(run_id)
    trace = await app.service.trace(run_id)
    events = await app.service.events(run_id)
    for payload in (
        json.dumps(facts.model_dump(mode="json"), default=str),
        json.dumps(trace.model_dump(mode="json"), default=str),
        json.dumps([event.model_dump(mode="json") for event in events], default=str),
    ):
        assert CREDENTIAL not in payload
        assert ROTATED_CREDENTIAL not in payload


async def _run_case(tmp_path, body) -> None:
    """共用装配：wire + 回环MCP服务器 + managed应用；body写断言。"""
    wire = McpWire("docs__search")
    docs = DocsServer()
    await wire.start()
    await docs.start()
    key_file = Path(tmp_path) / "mcp-master.json"
    MasterKeyring.create(bytes(range(32))).save(key_file)
    try:
        app, provider, boot = await open_managed_app(wire, tmp_path, key_file)
        try:
            owner, workspace = await owner_principal(app, boot)
            session = await app.sessions.create(
                owner,
                workspace,
                SessionCreate(request_key="mcp-session"),
            )
            await body(app, wire, owner, workspace, session.id, docs)
        finally:
            await app.close()
            await provider.close()
    finally:
        await docs.stop()
        await wire.close()


@pytest.mark.asyncio
async def test_approved_mcp_ask_resumes_over_rebuilt_connection(tmp_path):
    """批准的MCP停靠批经恢复时重建的连接真实执行并完成；凭据不泄漏。"""

    async def body(app, wire, owner, workspace, session_id, docs):
        await connect_and_select(
            app, owner, workspace, session_id, f"http://127.0.0.1:{docs.port}/mcp"
        )
        _turn, run_id, request = await park_turn(
            app, owner, workspace, session_id, "mcp-turn", "/docs find krim"
        )
        assert app.service.tasks.get(run_id).done()  # 停靠后producer已收束
        await app.service.reply_approval(run_id, request["request_id"], reply(request, True))
        result = await app.service.wait(run_id)
        assert result.status == "completed", result.code
        facts = await app.service.result(run_id)
        assert facts.tools[0].name == "docs__search"
        assert facts.tools[0].status == "ok"
        assert facts.tools[0].executor_started is True
        assert docs.calls == [{"tool": "search", "query": "find krim"}]
        assert docs.requests[-1]["credential"] == CREDENTIAL
        assert len(wire.requests) == 2  # 停靠步一次模型请求，恢复收尾一次，无重放
        await assert_no_credential_leak(app, run_id)
        assert all(task.done() for task in app.service.tasks.values())

    await _run_case(tmp_path, body)


@pytest.mark.asyncio
async def test_denied_mcp_ask_feeds_back_without_reaching_server(tmp_path):
    """拒绝的MCP停靠批回传denied观察后完成；服务器零工具调用。"""

    async def body(app, wire, owner, workspace, session_id, docs):
        await connect_and_select(
            app, owner, workspace, session_id, f"http://127.0.0.1:{docs.port}/mcp"
        )
        _turn, run_id, request = await park_turn(
            app, owner, workspace, session_id, "mcp-turn", "/docs find deny"
        )
        await app.service.reply_approval(run_id, request["request_id"], reply(request, False))
        result = await app.service.wait(run_id)
        assert result.status == "completed", result.code
        facts = await app.service.result(run_id)
        assert facts.tools[0].status == "denied"
        assert facts.tools[0].code == "approval_denied"
        assert facts.tools[0].executor_started is False
        assert docs.calls == []
        assert all(captured["method"] in {"POST", "GET", "DELETE"} for captured in docs.requests)

    await _run_case(tmp_path, body)


@pytest.mark.asyncio
async def test_parked_turn_keeps_frozen_config_after_connection_rotation(tmp_path):
    """停靠期间轮换凭据：恢复按turn快照用旧凭据执行，不追新配置。"""

    async def body(app, wire, owner, workspace, session_id, docs):
        created = await connect_and_select(
            app, owner, workspace, session_id, f"http://127.0.0.1:{docs.port}/mcp"
        )
        turn, run_id, request = await park_turn(
            app, owner, workspace, session_id, "mcp-turn", "/docs find stale"
        )
        await app.account_mcp.update_connection(
            owner,
            created["id"],
            ConnectionUpdate(
                request_key="rotate-after-park",
                expected_revision=created["revision"],
                name="docs",
                url=f"http://127.0.0.1:{docs.port}/mcp",
                allowed_hosts=("127.0.0.1",),
                selected_tools=("search",),
                readonly_tools=("search",),
                credentials={"X-Test-Credential": ROTATED_CREDENTIAL},
                timeout_seconds=10.0,
            ),
        )
        frozen = await app.account_mcp.configurations_for_turn(turn.id)
        assert len(frozen) == 1
        assert frozen[0].headers["X-Test-Credential"].get_secret_value() == CREDENTIAL
        await app.service.reply_approval(run_id, request["request_id"], reply(request, True))
        result = await app.service.wait(run_id)
        assert result.status == "completed", result.code
        assert docs.calls == [{"tool": "search", "query": "find stale"}]
        assert docs.requests[-1]["credential"] == CREDENTIAL  # 旧凭据，非轮换值
        await assert_no_credential_leak(app, run_id)

    await _run_case(tmp_path, body)
