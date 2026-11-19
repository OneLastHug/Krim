"""MCPConnection资源接口测试：真实MCPConnection + fake SDK，无网络与容器。"""

from __future__ import annotations

import asyncio
import base64
import json
from collections.abc import Callable
from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import Any

import pytest
from pydantic import SecretStr

import krim_agent.mcp.client as mcp_client
from krim_agent.contracts import ServiceError
from krim_agent.mcp import MCPConnection, MCPServerConfig

# 合成密钥只用于脱敏断言，不是真实凭据。
SECRET = "private-mcp-resource-token"


class _ResourceSession:
    """Fake官方SDK会话：按属性暴露initialize/list_tools/list_resources等。"""

    def __init__(
        self,
        _read=None,
        _write=None,
        *,
        capabilities: Any | None = None,
        resource_pages: tuple[Any, ...] = (),
        template_pages: tuple[Any, ...] = (),
        tool_pages: tuple[Any, ...] = (),
        read_responses: dict[str, Any] | None = None,
        read_delay: float = 0.0,
    ):
        self.capabilities = (
            capabilities
            if capabilities is not None
            else SimpleNamespace(tools=SimpleNamespace(), resources=SimpleNamespace())
        )
        self._resource_pages = list(resource_pages)
        self._template_pages = list(template_pages)
        self._tool_pages = list(tool_pages)
        self.read_responses = dict(read_responses or {})
        self.read_delay = read_delay
        self.read_calls: list[str] = []
        self.resource_cursors: list[str | None] = []
        self.template_cursors: list[str | None] = []
        self.list_tools_cursors: list[str | None] = []
        self.hang_first_read = False
        self.first_read_started = asyncio.Event()
        self.first_read_cancelled = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False

    async def initialize(self):
        # 官方SDK返回InitializeResult；能力在其capabilities属性上。
        return SimpleNamespace(capabilities=self.capabilities)

    async def list_tools(self, cursor=None):
        self.list_tools_cursors.append(cursor)
        if not self._tool_pages:
            return SimpleNamespace(tools=[], nextCursor=None)
        return self._tool_pages.pop(0)

    async def list_resources(self, cursor=None):
        self.resource_cursors.append(cursor)
        if not self._resource_pages:
            return SimpleNamespace(resources=[], nextCursor=None)
        return self._resource_pages.pop(0)

    async def list_resource_templates(self, cursor=None):
        self.template_cursors.append(cursor)
        if not self._template_pages:
            return SimpleNamespace(resourceTemplates=[], nextCursor=None)
        return self._template_pages.pop(0)

    async def read_resource(self, uri):
        self.read_calls.append(uri)
        if self.hang_first_read and len(self.read_calls) == 1:
            self.first_read_started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                self.first_read_cancelled = True
                raise
        if self.read_delay:
            await asyncio.sleep(self.read_delay)
        block = self.read_responses.get(uri)
        if block is None:
            block = SimpleNamespace(uri=uri, mimeType="text/plain", text="ok")
        return SimpleNamespace(contents=[block])


@asynccontextmanager
async def _fake_streams(_params, **_kwargs):
    yield object(), object()


@asynccontextmanager
async def _fake_http_streams(*_args, **_kwargs):
    yield object(), object(), lambda: None


def _install_fake_sdk(
    monkeypatch: pytest.MonkeyPatch, factory: Callable[[], Any]
) -> list[Any]:
    """把_load_sdk替换为返回fake会话的工厂；返回已创建的会话列表。"""
    sessions: list[Any] = []

    def make_session(read, write):
        session = factory()
        sessions.append(session)
        return session

    monkeypatch.setattr(
        mcp_client,
        "_load_sdk",
        lambda: (
            make_session,
            lambda **kwargs: SimpleNamespace(**kwargs),
            _fake_streams,
            _fake_http_streams,
        ),
    )
    return sessions


def _stdio_config(**changes):
    values = {
        "name": "resource-fixture",
        "transport": "stdio",
        "command": "fixture-server",
        "args": (),
        "selected_tools": (),
    }
    values.update(changes)
    return MCPServerConfig(**values)


def _capability_pair(*, resources, tools=True):
    capabilities: dict[str, Any] = {}
    if tools:
        capabilities["tools"] = SimpleNamespace()
    if resources:
        capabilities["resources"] = SimpleNamespace()
    return SimpleNamespace(**capabilities)


def _volume_pages(total: int = 300, page_size: int = 8):
    """>256个工具、>32页的发现目录；返回页列表与全部工具名。"""
    pages: list[Any] = []
    names: list[str] = []
    for start in range(0, total, page_size):
        batch = []
        for index in range(start, min(start + page_size, total)):
            name = f"tool_{index:04d}"
            names.append(name)
            batch.append(
                SimpleNamespace(
                    name=name,
                    description=f"fixture tool {index}",
                    inputSchema={"type": "object", "properties": {}},
                    outputSchema=None,
                    annotations=None,
                )
            )
        pages.append(
            SimpleNamespace(
                tools=batch,
                nextCursor=f"cursor-{start + page_size}"
                if start + page_size < total
                else None,
            )
        )
    return tuple(pages), tuple(names)


async def test_list_resources_paginates_and_preserves_next_cursor(monkeypatch):
    pages = (
        SimpleNamespace(
            resources=[
                {"name": "alpha", "uri": "mem://alpha", "mimeType": "text/plain"},
                {
                    "name": "beta",
                    "uri": "mem://beta",
                    "mimeType": "application/json",
                    "size": 3,
                },
            ],
            nextCursor="page-2",
        ),
        SimpleNamespace(
            resources=[{"name": "gamma", "uri": "mem://gamma", "description": "last"}],
            nextCursor=None,
        ),
    )
    sessions = _install_fake_sdk(
        monkeypatch, lambda: _ResourceSession(resource_pages=pages)
    )
    connection = MCPConnection(_stdio_config())
    await connection.connect()
    try:
        assert connection.supports_resources is True
        first = await connection.list_resources()
        assert first == {
            "resources": [
                {"name": "alpha", "uri": "mem://alpha", "mimeType": "text/plain"},
                {
                    "name": "beta",
                    "uri": "mem://beta",
                    "mimeType": "application/json",
                    "size": 3,
                },
            ],
            "nextCursor": "page-2",
        }
        assert isinstance(json.dumps(first), str)
        second = await connection.list_resources(first["nextCursor"])
        assert second == {
            "resources": [{"name": "gamma", "uri": "mem://gamma", "description": "last"}]
        }
        assert "nextCursor" not in second
    finally:
        await connection.close()
    assert sessions[0].resource_cursors == [None, "page-2"]


async def test_list_resource_templates_paginates_with_uri_template(monkeypatch):
    pages = (
        SimpleNamespace(
            resourceTemplates=[
                {"name": "item", "uriTemplate": "mem://items/{id}", "mimeType": "text/plain"}
            ],
            nextCursor="tpl-2",
        ),
        SimpleNamespace(resourceTemplates=[], nextCursor=None),
    )
    sessions = _install_fake_sdk(
        monkeypatch, lambda: _ResourceSession(template_pages=pages)
    )
    connection = MCPConnection(_stdio_config())
    await connection.connect()
    try:
        first = await connection.list_resource_templates()
        assert first == {
            "resourceTemplates": [
                {"name": "item", "uriTemplate": "mem://items/{id}", "mimeType": "text/plain"}
            ],
            "nextCursor": "tpl-2",
        }
        second = await connection.list_resource_templates(first["nextCursor"])
        assert second == {"resourceTemplates": []}
    finally:
        await connection.close()
    assert sessions[0].template_cursors == [None, "tpl-2"]


async def test_read_resource_marks_text_and_base64_blob_distinctly(monkeypatch):
    blob = base64.b64encode(b"\x89PNG-fixture-bytes").decode()
    _install_fake_sdk(
        monkeypatch,
        lambda: _ResourceSession(
            read_responses={
                "mem://pic": SimpleNamespace(uri="mem://pic", mimeType="image/png", blob=blob)
            }
        ),
    )
    connection = MCPConnection(_stdio_config())
    await connection.connect()
    try:
        text = await connection.read_resource("mem://notes")
        assert text == {
            "contents": [
                {"uri": "mem://notes", "mimeType": "text/plain", "kind": "text", "text": "ok"}
            ]
        }
        # blob以base64数据返回且明确标注kind=blob：是数据，不冒称媒体已发。
        result = await connection.read_resource("mem://pic")
        assert result == {
            "contents": [
                {
                    "uri": "mem://pic",
                    "mimeType": "image/png",
                    "kind": "blob",
                    "blob": blob,
                }
            ]
        }
    finally:
        await connection.close()


async def test_large_resource_text_returns_completely(monkeypatch):
    body = "A" * (64 * 1024 + 123)
    _install_fake_sdk(
        monkeypatch,
        lambda: _ResourceSession(
            read_responses={
                "mem://big": SimpleNamespace(uri="mem://big", mimeType="text/plain", text=body)
            }
        ),
    )
    connection = MCPConnection(_stdio_config())
    await connection.connect()
    try:
        result = await connection.read_resource("mem://big")
        entry = result["contents"][0]
        assert entry["kind"] == "text"
        assert entry["text"] == body
        assert len(entry["text"]) > 64 * 1024
    finally:
        await connection.close()


@pytest.mark.parametrize(
    "capabilities",
    [
        _capability_pair(resources=False, tools=True),
        _capability_pair(resources=False, tools=False),
    ],
    ids=["tools-only", "resource-only"],
)
async def test_missing_resources_capability_reports_unavailable(monkeypatch, capabilities):
    _install_fake_sdk(
        monkeypatch, lambda: _ResourceSession(capabilities=capabilities)
    )
    connection = MCPConnection(_stdio_config())
    await connection.connect()
    try:
        assert connection.supports_resources is False
        for call in (
            connection.list_resources(),
            connection.list_resource_templates(),
            connection.read_resource("mem://x"),
        ):
            with pytest.raises(ServiceError) as error:
                await call
            assert error.value.code == "mcp_unavailable"
            assert error.value.status == 501
    finally:
        await connection.close()


async def test_resource_only_server_never_receives_tools_list(monkeypatch):
    sessions = _install_fake_sdk(
        monkeypatch,
        lambda: _ResourceSession(capabilities=_capability_pair(resources=True, tools=False)),
    )
    connection = MCPConnection(_stdio_config(selected_tools=()))
    await connection.connect()
    try:
        assert connection.tools == ()
        assert (await connection.list_resources())["resources"] == []
    finally:
        await connection.close()
    assert sessions[0].list_tools_cursors == []


async def test_tool_selection_on_tools_less_server_fails_before_listing(monkeypatch):
    sessions = _install_fake_sdk(
        monkeypatch,
        lambda: _ResourceSession(capabilities=_capability_pair(resources=True, tools=False)),
    )
    connection = MCPConnection(_stdio_config(selected_tools=("echo",)))
    with pytest.raises(ServiceError) as error:
        await connection.connect()
    assert error.value.code == "mcp_tool_not_selected"
    assert error.value.status == 422
    assert sessions[0].list_tools_cursors == []
    await connection.close()


async def test_cancelled_read_cancels_operation_and_keeps_connection_reusable(monkeypatch):
    sessions = _install_fake_sdk(monkeypatch, _ResourceSession)
    connection = MCPConnection(_stdio_config())
    await connection.connect()
    sessions[0].hang_first_read = True
    try:
        hung = asyncio.create_task(connection.read_resource("mem://hung"))
        await sessions[0].first_read_started.wait()
        hung.cancel()
        with pytest.raises(asyncio.CancelledError):
            await hung
        assert sessions[0].first_read_cancelled is True
        result = await connection.read_resource("mem://after")
        assert result["contents"][0]["text"] == "ok"
    finally:
        await connection.close()
    assert sessions[0].read_calls == ["mem://hung", "mem://after"]


async def test_credentials_redacted_from_text_and_decoded_blob(monkeypatch):
    leaked_blob = base64.b64encode(f"raw payload {SECRET}".encode()).decode()
    # 二进制前后缀包裹秘钥字节：解码结果非合法UTF-8，直接按字节扫描仍须命中。
    wrapped_blob = base64.b64encode(
        b"\xff\xfe\x00\xed\xa0\x80" + SECRET.encode() + b"\x80\x81\xff\x00"
    ).decode()
    benign_blob = base64.b64encode(b"benign bytes").decode()
    sessions = _install_fake_sdk(monkeypatch, _ResourceSession)
    connection = MCPConnection(
        MCPServerConfig(
            name="private-resources",
            transport="streamable_http",
            url="https://example.com/mcp",
            allowed_hosts=frozenset({"example.com"}),
            selected_tools=(),
            headers={"Authorization": SecretStr("Bearer " + SECRET)},
        )
    )
    await connection.connect()

    async def fake_read(uri):
        if uri == "mem://leak-text":
            return SimpleNamespace(
                contents=[
                    SimpleNamespace(uri=uri, mimeType="text/plain", text=f"key={SECRET}")
                ]
            )
        if uri == "mem://leak-blob":
            return SimpleNamespace(
                contents=[
                    SimpleNamespace(
                        uri=uri, mimeType="application/octet-stream", blob=leaked_blob
                    )
                ]
            )
        if uri == "mem://leak-binary":
            return SimpleNamespace(
                contents=[
                    SimpleNamespace(
                        uri=uri, mimeType="application/octet-stream", blob=wrapped_blob
                    )
                ]
            )
        return SimpleNamespace(
            contents=[
                SimpleNamespace(
                    uri=uri, mimeType="application/octet-stream", blob=benign_blob
                )
            ]
        )

    sessions[0].read_resource = fake_read  # type: ignore[method-assign]
    try:
        text = await connection.read_resource("mem://leak-text")
        assert text["contents"][0]["text"] == "key=[redacted]"
        blob_result = await connection.read_resource("mem://leak-blob")
        assert blob_result["contents"][0]["kind"] == "blob"
        assert blob_result["contents"][0]["blob"] == "[redacted]"
        binary_result = await connection.read_resource("mem://leak-binary")
        assert binary_result["contents"][0]["kind"] == "blob"
        assert binary_result["contents"][0]["blob"] == "[redacted]"
        intact = await connection.read_resource("mem://benign")
        assert intact["contents"][0]["blob"] == benign_blob
        for payload in (text, blob_result, binary_result, intact):
            encoded = json.dumps(payload)
            assert SECRET not in encoded
            assert leaked_blob not in encoded
            assert wrapped_blob not in encoded
    finally:
        await connection.close()


async def test_discovery_exceeds_previous_page_and_tool_limits(monkeypatch):
    pages, names = _volume_pages()
    assert len(names) > 256 and len(pages) > 32
    _install_fake_sdk(monkeypatch, lambda: _ResourceSession(tool_pages=pages))
    connection = MCPConnection(_stdio_config(selected_tools=()))
    await connection.connect()
    try:
        discovered = connection.discovered_tools
        assert len(discovered) == 300
        assert tuple(tool.name for tool in discovered) == names
        assert connection.tools == ()
    finally:
        await connection.close()


async def test_large_selected_tool_set_is_legal_and_fully_exposed(monkeypatch):
    pages, names = _volume_pages()
    _install_fake_sdk(monkeypatch, lambda: _ResourceSession(tool_pages=pages))
    connection = MCPConnection(_stdio_config(selected_tools=names))
    await connection.connect()
    try:
        assert len(connection.tools) == 300
        assert {tool.name for tool in connection.tools} == set(names)
    finally:
        await connection.close()


async def test_resource_timeout_and_input_validation(monkeypatch):
    _install_fake_sdk(
        monkeypatch, lambda: _ResourceSession(read_delay=0.5)
    )
    connection = MCPConnection(_stdio_config(timeout_seconds=0.05))
    await connection.connect()
    try:
        with pytest.raises(ServiceError) as timeout_error:
            await connection.read_resource("mem://slow")
        assert timeout_error.value.code == "mcp_timeout"
        assert timeout_error.value.status == 504
        with pytest.raises(ServiceError) as empty_cursor:
            await connection.list_resources("")
        assert empty_cursor.value.code == "mcp_invalid_json"
        assert empty_cursor.value.status == 422
        with pytest.raises(ServiceError) as bad_uri:
            await connection.read_resource("not a uri")
        assert bad_uri.value.code == "mcp_invalid_json"
        assert bad_uri.value.status == 422
    finally:
        await connection.close()
    assert connection.closed


@pytest.mark.parametrize(
    "remote_name",
    ["t" * 129, "bad\x01name"],
    ids=["name-129-chars", "control-char"],
)
async def test_discovery_rejects_overlong_and_control_char_tool_names(monkeypatch, remote_name):
    page = SimpleNamespace(
        tools=[
            SimpleNamespace(
                name=remote_name,
                description="fixture",
                inputSchema={"type": "object", "properties": {}},
                outputSchema=None,
                annotations=None,
            )
        ],
        nextCursor=None,
    )
    _install_fake_sdk(monkeypatch, lambda: _ResourceSession(tool_pages=(page,)))
    connection = MCPConnection(_stdio_config(selected_tools=()))
    with pytest.raises(ServiceError) as error:
        await connection.connect()
    assert error.value.code == "mcp_discovery_failed"
    assert error.value.status == 502
    await connection.close()


async def test_resource_calls_after_close_report_connection_closed(monkeypatch):
    _install_fake_sdk(monkeypatch, _ResourceSession)
    connection = MCPConnection(_stdio_config())
    await connection.connect()
    await connection.close()
    for call in (
        connection.list_resources(),
        connection.list_resource_templates(),
        connection.read_resource("mem://x"),
    ):
        with pytest.raises(ServiceError) as error:
            await call
        assert error.value.code == "mcp_connection_closed"
        assert error.value.status == 503
