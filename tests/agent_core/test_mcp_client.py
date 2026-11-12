from __future__ import annotations

import asyncio
import importlib.util
import os
import socket
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest
from pydantic import SecretStr, ValidationError

import krim_agent.mcp.client as mcp_client
from krim_agent.contracts import ServiceError
from krim_agent.mcp import MCPConnection, MCPServerConfig

FIXTURE = Path(__file__).parents[1] / "fixtures" / "agent_core_mcp_server.py"


class _FakeSession:
    def __init__(self, _read, _write, *, pages=None, responses=None):
        self.pages = list(pages or [])
        self.responses = list(responses or [])
        self.calls: list[tuple[str, dict]] = []
        self.closed = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        self.closed = True

    async def initialize(self):
        return SimpleNamespace()

    async def list_tools(self, cursor=None):
        if not self.pages:
            return SimpleNamespace(tools=[], nextCursor=None)
        return self.pages.pop(0)

    async def call_tool(self, name, *, arguments):
        self.calls.append((name, arguments))
        if self.responses:
            response = self.responses.pop(0)
            if isinstance(response, BaseException):
                raise response
            return response
        return SimpleNamespace(
            isError=False,
            structuredContent={"called": name, "arguments": arguments},
            content=[],
        )


@asynccontextmanager
async def _fake_streams(_params, **_kwargs):
    yield object(), object()


@asynccontextmanager
async def _fake_http_streams(*_args, **_kwargs):
    yield object(), object(), lambda: None


def _tool(name: str, *, annotations=None):
    return SimpleNamespace(
        name=name,
        description=f"description for {name}",
        inputSchema={"type": "object", "properties": {"value": {"type": "string"}}},
        outputSchema={"type": "object"},
        annotations=annotations,
    )


def _page(*tools, cursor=None):
    return SimpleNamespace(tools=list(tools), nextCursor=cursor)


def _install_fake_sdk(monkeypatch, *, pages=None, responses=None):
    sessions = []

    class Session(_FakeSession):
        def __init__(self, read, write):
            super().__init__(read, write, pages=pages, responses=responses)
            sessions.append(self)

    monkeypatch.setattr(
        mcp_client,
        "_load_sdk",
        lambda: (
            Session,
            lambda **kwargs: SimpleNamespace(**kwargs),
            _fake_streams,
            _fake_http_streams,
        ),
    )
    return sessions


def _stdio_config(**changes):
    values = {
        "name": "fixture",
        "transport": "stdio",
        "command": sys.executable,
        "args": (str(FIXTURE),),
        "selected_tools": ("echo",),
    }
    values.update(changes)
    return MCPServerConfig(**values)


def test_config_validates_transport_security_and_digest_redacts_secret_values():
    with pytest.raises(ValidationError):
        MCPServerConfig(
            name="bad",
            transport="streamable_http",
            url="http://example.test/mcp",
            allowed_hosts=frozenset({"example.test"}),
        )
    with pytest.raises(ValidationError):
        MCPServerConfig(
            name="bad",
            transport="streamable_http",
            url="https://user:pass@example.test/mcp#fragment",
            allowed_hosts=frozenset({"example.test"}),
        )

    base = MCPServerConfig(
        name="remote",
        transport="streamable_http",
        url="https://mcp.example.test/v1",
        allowed_hosts=frozenset({"mcp.example.test"}),
        headers={"Authorization": SecretStr("secret-one")},
    )
    different_secret = MCPServerConfig(
        name="remote",
        transport="streamable_http",
        url="https://mcp.example.test/v1",
        allowed_hosts=frozenset({"mcp.example.test"}),
        headers={"Authorization": SecretStr("secret-two")},
    )
    other_endpoint = MCPServerConfig(
        name="remote",
        transport="streamable_http",
        url="https://other.example.test/v1",
        allowed_hosts=frozenset({"other.example.test"}),
    )
    assert base.configuration_digest == different_secret.configuration_digest
    assert base.configuration_digest != other_endpoint.configuration_digest
    assert "secret-one" not in base.configuration_digest

    env_one = MCPServerConfig(
        name="stdio-env",
        transport="stdio",
        command="fixture",
        env={"MCP_TOKEN": "token-one", "MODE": "one"},
    )
    env_two = MCPServerConfig(
        name="stdio-env",
        transport="stdio",
        command="fixture",
        env={"MCP_TOKEN": "token-two", "MODE": "two"},
    )
    assert env_one.configuration_digest == env_two.configuration_digest
    assert "token-one" not in env_one.configuration_digest
    assert "token-two" not in env_two.configuration_digest
    assert '"environment"' not in env_one.configuration_digest


async def test_mock_discovery_selects_only_approved_tools_and_defensively_copies_schema(
    monkeypatch,
):
    _install_fake_sdk(
        monkeypatch,
        pages=[_page(_tool("echo"), cursor="page-2"), _page(_tool("write"))],
    )
    connection = MCPConnection(_stdio_config(readonly_tools=frozenset({"echo"})))
    await asyncio.create_task(connection.connect())
    try:
        visible = connection.tools
        assert [tool.name for tool in visible] == ["echo"]
        assert visible[0].read_only is True
        visible[0].input_schema["properties"]["value"]["type"] = "integer"
        assert connection.tools[0].input_schema["properties"]["value"]["type"] == "string"
        assert await connection.call_tool("echo", {"value": "ok"}) == {
            "called": "echo",
            "arguments": {"value": "ok"},
        }
        with pytest.raises(ServiceError) as error:
            await connection.call_tool("write", {})
        assert error.value.status == 404
    finally:
        await asyncio.create_task(connection.close())
    assert connection.closed


async def test_remote_readonly_hints_do_not_grant_authority_and_text_results_join(monkeypatch):
    _install_fake_sdk(
        monkeypatch,
        pages=[
            _page(
                _tool("read", annotations={"readOnlyHint": True}),
                _tool("contradict", annotations={"readOnlyHint": False}),
            )
        ],
        responses=[
            SimpleNamespace(
                isError=False,
                structuredContent=None,
                content=[
                    SimpleNamespace(type="text", text="first"),
                    SimpleNamespace(type="text", text="second"),
                ],
            )
        ],
    )
    connection = MCPConnection(
        _stdio_config(
            selected_tools=("read", "contradict"),
            readonly_tools=frozenset({"contradict"}),
        )
    )
    await connection.connect()
    try:
        assert {tool.name: tool.read_only for tool in connection.tools} == {
            "read": False,
            "contradict": False,
        }
        assert await connection.call_tool("read", {}) == "firstsecond"
    finally:
        await connection.close()


async def test_empty_selection_discovers_but_does_not_expose_or_admit_tools(monkeypatch):
    _install_fake_sdk(monkeypatch, pages=[_page(_tool("echo"))])
    connection = MCPConnection(_stdio_config(selected_tools=()))
    await connection.connect()
    try:
        assert connection.tools == ()
        assert [tool.name for tool in connection.discovered_tools] == ["echo"]
        with pytest.raises(ServiceError):
            await connection.call_tool("echo", {})
    finally:
        await connection.close()


@pytest.mark.parametrize(
    ("pages", "selected", "expected_code"),
    [
        (
            [_page(_tool("echo"), cursor="same"), _page(_tool("other"), cursor="same")],
            ("echo",),
            "mcp_cursor_invalid",
        ),
        ([_page(_tool("echo"))], ("missing",), "mcp_tool_not_selected"),
    ],
)
async def test_mock_discovery_rejects_cursor_cycles_and_unknown_selection(
    monkeypatch, pages, selected, expected_code
):
    _install_fake_sdk(monkeypatch, pages=pages)
    connection = MCPConnection(_stdio_config(selected_tools=selected))
    with pytest.raises(ServiceError) as error:
        await connection.connect()
    assert error.value.status in {422, 502}
    assert error.value.code == expected_code
    await connection.close()


async def test_mock_call_errors_and_oversized_output_are_not_returned_as_success(monkeypatch):
    _install_fake_sdk(
        monkeypatch,
        pages=[_page(_tool("echo"))],
        responses=[
            SimpleNamespace(
                isError=True, structuredContent={"secret": "server detail"}, content=[]
            ),
            SimpleNamespace(
                isError=False,
                structuredContent={"value": "x" * (64 * 1024)},
                content=[],
            ),
        ],
    )
    connection = MCPConnection(_stdio_config())
    await connection.connect()
    try:
        with pytest.raises(ServiceError) as error:
            await connection.call_tool("echo", {})
        assert error.value.status == 502
        assert error.value.code == "mcp_tool_failed"
        assert "server detail" not in str(error.value)
        with pytest.raises(ServiceError) as oversized:
            await connection.call_tool("echo", {})
        assert oversized.value.status == 502
        assert oversized.value.code == "mcp_output_too_large"
    finally:
        await connection.close()


async def test_close_cancels_and_joins_the_in_flight_call(monkeypatch):
    started = asyncio.Event()
    finished = asyncio.Event()

    class BlockingSession(_FakeSession):
        async def list_tools(self, cursor=None):
            return _page(_tool("echo"))

        async def call_tool(self, name, *, arguments):
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                finished.set()

    @asynccontextmanager
    async def streams(_params, **_kwargs):
        yield object(), object()

    monkeypatch.setattr(
        mcp_client,
        "_load_sdk",
        lambda: (
            BlockingSession,
            lambda **kwargs: SimpleNamespace(**kwargs),
            streams,
            _fake_http_streams,
        ),
    )
    connection = MCPConnection(_stdio_config())
    await asyncio.create_task(connection.connect())
    caller = asyncio.create_task(connection.call_tool("echo", {}))
    await started.wait()
    await asyncio.create_task(connection.close())
    assert caller.cancelled()
    assert finished.is_set()
    assert connection.closed


async def test_missing_optional_sdk_returns_service_error_without_importing_it(monkeypatch):
    def unavailable():
        raise ImportError("private import detail")

    monkeypatch.setattr(mcp_client, "_load_sdk", unavailable)
    connection = MCPConnection(_stdio_config())
    with pytest.raises(ServiceError) as error:
        await connection.connect()
    assert error.value.status == 503
    assert error.value.code == "mcp_unavailable"
    assert "private import detail" not in str(error.value)


@pytest.mark.skipif(
    importlib.util.find_spec("mcp") is None, reason="optional MCP SDK is not installed"
)
async def test_real_official_sdk_stdio_fixture_connects_calls_and_closes():
    connection = MCPConnection(_stdio_config(selected_tools=("echo",)))
    await asyncio.create_task(connection.connect())
    try:
        assert [tool.name for tool in connection.tools] == ["echo"]
        result = await connection.call_tool("echo", {"text": "local stdio"})
        assert result == {"echo": "local stdio"}
    finally:
        await asyncio.create_task(connection.close())
    assert connection.closed


async def test_stdio_configuration_rejects_control_characters():
    with pytest.raises(ValidationError):
        _stdio_config(command="fixture\nmalicious")
    with pytest.raises(ValidationError):
        _stdio_config(args=("--value\x7f",))
    with pytest.raises(ValidationError):
        _stdio_config(env={"SAFE": "value\nextra"})


def test_stdio_environment_clears_unapproved_inherited_values(monkeypatch):
    from krim_agent.mcp.client import _stdio_environment

    monkeypatch.setenv("PATH", "/trusted/bin")
    monkeypatch.setenv("KRIM_AGENT_API_KEY", "private-model-key")
    result = _stdio_environment({"MCP_TOKEN": "explicit-secret"})
    assert result["PATH"] == "/trusted/bin"
    assert result["KRIM_AGENT_API_KEY"] == ""
    assert result["MCP_TOKEN"] == "explicit-secret"


@pytest.mark.skipif(
    importlib.util.find_spec("mcp") is None, reason="optional MCP SDK is not installed"
)
async def test_real_streamable_http_fixture_connects_calls_and_closes():
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    port = listener.getsockname()[1]
    listener.close()
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        str(FIXTURE),
        env={
            **os.environ,
            "KRIM_AGENT_TEST_MCP_HTTP": "1",
            "KRIM_AGENT_TEST_MCP_PORT": str(port),
        },
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    connection = MCPConnection(
        MCPServerConfig(
            name="http-fixture",
            transport="streamable_http",
            url=f"http://127.0.0.1:{port}/mcp",
            allowed_hosts=frozenset({"127.0.0.1"}),
            selected_tools=("echo",),
        )
    )
    try:
        async with asyncio.timeout(10):
            while True:
                if process.returncode is not None:
                    raise AssertionError("local MCP HTTP fixture exited before listening")
                try:
                    _reader, writer = await asyncio.open_connection("127.0.0.1", port)
                    writer.close()
                    await writer.wait_closed()
                    break
                except OSError:
                    await asyncio.sleep(0.05)
        await connection.connect()
        result = await connection.call_tool("echo", {"text": "local HTTP"})
        assert result == {"echo": "local HTTP"}
    finally:
        await connection.close()
        if process.returncode is None:
            process.terminate()
        await asyncio.wait_for(process.wait(), timeout=10)


async def test_owner_failure_releases_inflight_future_without_hanging(monkeypatch):
    started = asyncio.Event()

    class BlockingSession(_FakeSession):
        async def list_tools(self, cursor=None):
            return _page(_tool("echo"))

        async def call_tool(self, name, *, arguments):
            started.set()
            await asyncio.Event().wait()

    monkeypatch.setattr(
        mcp_client,
        "_load_sdk",
        lambda: (
            BlockingSession,
            lambda **kwargs: SimpleNamespace(**kwargs),
            _fake_streams,
            _fake_http_streams,
        ),
    )
    connection = MCPConnection(_stdio_config())
    await connection.connect()
    caller = asyncio.create_task(connection.call_tool("echo", {}))
    await started.wait()
    connection._owner_task.cancel()
    try:
        with pytest.raises(ServiceError) as error:
            await asyncio.wait_for(caller, 2)
        assert error.value.code == "mcp_connection_closed"
    finally:
        await connection.close()


async def test_credentials_do_not_enter_arguments_or_results(monkeypatch):
    secret = "private-mcp-fixture-token"
    sessions = _install_fake_sdk(
        monkeypatch,
        pages=[_page(_tool("echo"))],
        responses=[
            SimpleNamespace(
                isError=False, structuredContent={"text": "value:" + secret}, content=[]
            )
        ],
    )
    connection = MCPConnection(
        MCPServerConfig(
            name="private",
            transport="streamable_http",
            url="https://example.com/mcp",
            allowed_hosts=frozenset({"example.com"}),
            selected_tools=("echo",),
            headers={"Authorization": SecretStr("Bearer " + secret)},
        )
    )
    await connection.connect()
    try:
        with pytest.raises(ServiceError):
            await connection.call_tool("echo", {"value": secret})
        assert not sessions[0].calls
        result = await connection.call_tool("echo", {"value": "safe"})
        assert secret not in str(result) and result == {"text": "value:[redacted]"}
    finally:
        await connection.close()
