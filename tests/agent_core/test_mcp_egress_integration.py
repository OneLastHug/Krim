"""证明实际MCP客户端向SDK注入受控HTTP传输，保留宿主配置兼容。"""

from contextlib import asynccontextmanager

import httpx
import pytest
from test_mcp_client import _install_fake_sdk, _stdio_config

from krim_agent.contracts import ServiceError
from krim_agent.mcp import client as client_module
from krim_agent.mcp.client import MCPConnection
from krim_agent.mcp.types import MCPServerConfig
from krim_agent.network.egress_policy import EgressPolicy
from krim_agent.network.http_transport import PinnedAsyncTransport


def config():
    return MCPServerConfig(
        name="controlled", transport="streamable_http", url="https://example.com/mcp",
        allowed_hosts=frozenset({"example.com"}), headers={"X-Fixture": "synthetic"},
    )


async def test_mcp_sdk_receives_pinned_client_and_owner_closes_it(monkeypatch):
    _install_fake_sdk(monkeypatch)
    sdk = client_module._load_sdk()
    clients = []

    @asynccontextmanager
    async def streams(url, *, http_client, terminate_on_close):
        assert url == "https://example.com/mcp"
        assert terminate_on_close
        assert isinstance(http_client._transport, PinnedAsyncTransport)
        assert http_client.headers["X-Fixture"] == "synthetic"
        assert not http_client.trust_env and not http_client.follow_redirects
        clients.append(http_client)
        yield object(), object(), lambda: None

    monkeypatch.setattr(client_module, "_load_sdk", lambda: (*sdk[:3], streams))
    connection = MCPConnection(config(), egress_policy=EgressPolicy({"example.com"}, {443}))
    await connection.connect()
    assert clients and not clients[0].is_closed
    await connection.close()
    assert clients[0].is_closed


async def test_injected_policy_cannot_be_bypassed_by_config_host(monkeypatch):
    _install_fake_sdk(monkeypatch)
    connection = MCPConnection(config(), egress_policy=EgressPolicy({"other.example"}, {443}))
    with pytest.raises(ServiceError, match="mcp_connect_failed"):
        await connection.connect()
    await connection.close()


def test_http_policy_cannot_claim_to_protect_stdio():
    with pytest.raises(ValueError):
        MCPConnection(_stdio_config(), egress_policy=EgressPolicy({"example.com"}, {443}))


async def test_existing_host_mcp_retains_normal_client(monkeypatch):
    _install_fake_sdk(monkeypatch)
    sdk = client_module._load_sdk()
    observed = []

    @asynccontextmanager
    async def streams(url, *, http_client, terminate_on_close):
        observed.append(type(http_client._transport))
        yield object(), object(), lambda: None

    monkeypatch.setattr(client_module, "_load_sdk", lambda: (*sdk[:3], streams))
    connection = MCPConnection(config())
    await connection.connect()
    await connection.close()
    assert observed == [httpx.AsyncHTTPTransport]
