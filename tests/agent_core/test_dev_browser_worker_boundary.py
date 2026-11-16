"""Static and loopback tests for the approval-gated worker boundary.

These tests never import Playwright, build an image, open Docker, or connect to
an application. They exercise only the fixed CONNECT envelope and source text.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from krim_agent.dev_browser.fixed_proxy import ProxyError, fixed_host, serve_connection

ROOT = Path(__file__).parents[2]
DEPLOY = ROOT / "deployment/development-browser"


@pytest.mark.parametrize("value", ["preview.example.net", "p-abc.preview.example.net"])
def test_fixed_host_accepts_dns_names_only(value):
    assert fixed_host(value) == value


@pytest.mark.parametrize(
    "value", ["127.0.0.1", "https://preview.example", ".example.net", "a..example.net", "a"]
)
def test_fixed_host_rejects_ip_url_and_ambiguous_names(value):
    with pytest.raises(ProxyError, match="proxy_config_invalid"):
        fixed_host(value)


@pytest.mark.asyncio
async def test_connect_proxy_requires_exact_host_and_bridges_raw_bytes(tmp_path):
    bridge_path = tmp_path / "bridge.sock"
    received = bytearray()

    async def bridge(reader, writer):
        line = await reader.readline()
        received.extend(line)
        writer.write(b"OK\n")
        await writer.drain()
        data = await reader.read(32)
        writer.write(data.upper())
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    bridge_server = await asyncio.start_unix_server(bridge, path=str(bridge_path))
    proxy_server = await asyncio.start_server(
        lambda reader, writer: serve_connection(
            reader,
            writer,
            str(bridge_path),
            "p-fixed.preview.example",
        ),
        "127.0.0.1",
        0,
    )
    try:
        port = proxy_server.sockets[0].getsockname()[1]
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(
            b"CONNECT p-fixed.preview.example:443 HTTP/1.1\r\n"
            b"Host: p-fixed.preview.example:443\r\n\r\nhello"
        )
        await writer.drain()
        assert await reader.readuntil(b"\r\n\r\n") == b"HTTP/1.1 200 Connection Established\r\n\r\n"
        assert await reader.readexactly(5) == b"HELLO"
        writer.close()
        await writer.wait_closed()
        assert bytes(received) == b"KRIM-PREVIEW/1 CONNECT p-fixed.preview.example 443\n"
    finally:
        proxy_server.close()
        bridge_server.close()
        await proxy_server.wait_closed()
        await bridge_server.wait_closed()


@pytest.mark.asyncio
async def test_connect_proxy_rejects_non_connect_or_foreign_target(tmp_path):
    bridge_path = tmp_path / "bridge.sock"
    called = False

    async def bridge(reader, writer):
        nonlocal called
        called = True
        writer.close()
        await writer.wait_closed()

    bridge_server = await asyncio.start_unix_server(bridge, path=str(bridge_path))
    proxy_server = await asyncio.start_server(
        lambda reader, writer: serve_connection(
            reader,
            writer,
            str(bridge_path),
            "p-fixed.preview.example",
        ),
        "127.0.0.1",
        0,
    )
    try:
        port = proxy_server.sockets[0].getsockname()[1]
        for payload in (
            b"GET https://p-fixed.preview.example/ HTTP/1.1\r\n"
            b"Host: p-fixed.preview.example\r\n\r\n",
            b"CONNECT other.preview.example:443 HTTP/1.1\r\n"
            b"Host: other.preview.example:443\r\n\r\n",
            b"CONNECT p-fixed.preview.example:443 HTTP/1.1\r\n"
            b"Host: p-fixed.preview.example:443\r\nCookie: secret\r\n\r\n",
        ):
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            writer.write(payload)
            await writer.drain()
            assert b"403" in await reader.read(256)
            writer.close()
            await writer.wait_closed()
        assert not called
    finally:
        proxy_server.close()
        bridge_server.close()
        await proxy_server.wait_closed()
        await bridge_server.wait_closed()


def test_worker_static_boundary_has_no_escape_hatches():
    source = (DEPLOY / "worker.py").read_text(encoding="utf-8")
    docker = (DEPLOY / "Dockerfile").read_text(encoding="utf-8")
    launcher = (ROOT / "src/krim_agent/dev_browser/launcher.py").read_text(encoding="utf-8")
    assert "chromium_sandbox=True" in source
    assert "ignore_https_errors=False" in source
    assert "--no-sandbox" not in source + docker + launcher
    assert "network=host" not in source + docker + launcher
    assert "connect_over_cdp" not in source
    assert "session.assert_dom" in source
    assert "locator(" not in source
    assert "asyncio.to_thread" not in source
    assert 'm.version("playwright")' in docker
    assert "mcr.microsoft.com/playwright/python:v1.61.0-noble@sha256:" in docker
    assert "USER 1000:1000" in docker
    assert "--network=none" in launcher
