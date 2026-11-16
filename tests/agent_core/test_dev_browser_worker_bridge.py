"""Real AF_UNIX authorization with fake upstream bytes; no web server/Docker."""

from __future__ import annotations

import asyncio
import os

import pytest

from krim_agent.dev_browser import host_bridge
from krim_agent.dev_browser.fixed_proxy import ProxyError, check_authority
from krim_agent.dev_browser.host_bridge import PreviewConnectBridge

HOST = "p-fixed.preview.example"


class Upstream:
    def __init__(self):
        self.reader = asyncio.StreamReader()
        self.closed = asyncio.Event()
        self.data = bytearray()

    def write(self, data):
        self.data.extend(data)
        self.reader.feed_data(data.upper())

    async def drain(self):
        pass

    def can_write_eof(self):
        return True

    def write_eof(self):
        self.reader.feed_data(b"tail")
        self.reader.feed_eof()

    def close(self):
        self.closed.set()
        if not self.reader.at_eof():
            self.reader.feed_eof()

    async def wait_closed(self):
        await self.closed.wait()


@pytest.fixture
def rig(tmp_path, monkeypatch):
    monkeypatch.setattr(host_bridge, "RUN_ROOT", tmp_path)
    monkeypatch.setattr(host_bridge, "CHECK_INTERVAL", 0.01)
    directory = tmp_path / "session"
    directory.mkdir(mode=0o700)
    state = {"checks": 0, "revoked": False, "targets": []}
    upstream = Upstream()

    async def authorize():
        state["checks"] += 1
        if state["revoked"]:
            raise RuntimeError("secret account detail must not leave bridge")

    async def dial(host, port):
        state["targets"].append((host, port))
        return upstream.reader, upstream

    monkeypatch.setattr(host_bridge.asyncio, "open_connection", dial)
    bridge = PreviewConnectBridge(
        str(directory / "bridge.sock"),
        HOST,
        authorize,
        expected_uid=os.getuid(),
        expected_gid=os.getgid(),
    )
    return bridge, upstream, state


@pytest.mark.asyncio
async def test_authority_probe_rechecks_original_closure_without_network(rig):
    bridge, _, state = rig
    async with bridge:
        await check_authority(str(bridge.socket_path), HOST)
        assert state["checks"] == 1 and state["targets"] == []
        state["revoked"] = True
        with pytest.raises(ProxyError, match="browser_authorization_lost"):
            await check_authority(str(bridge.socket_path), HOST)
    assert not bridge.socket_path.exists() and not bridge.tasks


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "line",
    [
        b"KRIM-PREVIEW/1 CONNECT other.preview.example 443\n",
        b"KRIM-PREVIEW/1 CONNECT p-fixed.preview.example 80\n",
        b"KRIM-PREVIEW/1 CONNECT 127.0.0.1 443\n",
        b"invalid command\n",
    ],
)
async def test_host_rejects_foreign_host_port_and_unrecognized_envelope(rig, line):
    bridge, _, state = rig
    async with bridge:
        reader, writer = await asyncio.open_unix_connection(str(bridge.socket_path))
        writer.write(line)
        await writer.drain()
        assert await reader.readexactly(3) == b"NO\n"
        writer.close()
        await writer.wait_closed()
    assert state["targets"] == []


@pytest.mark.asyncio
async def test_peer_credentials_are_mandatory_even_for_a_valid_envelope(rig):
    bridge, _, state = rig
    async with bridge:
        bridge.expected_uid += 1
        with pytest.raises(ProxyError):
            await check_authority(str(bridge.socket_path), HOST)
    assert state["targets"] == [] and state["checks"] == 0


@pytest.mark.asyncio
async def test_connect_preserves_half_close_and_never_changes_destination(rig):
    bridge, upstream, state = rig
    async with bridge:
        reader, writer = await asyncio.open_unix_connection(str(bridge.socket_path))
        writer.write(f"KRIM-PREVIEW/1 CONNECT {HOST} 443\n".encode())
        await writer.drain()
        assert await reader.readexactly(3) == b"OK\n"
        assert state["checks"] >= 2 and state["targets"] == [("127.0.0.1", 443)]
        writer.write(b"hello")
        await writer.drain()
        writer.write_eof()
        assert await asyncio.wait_for(reader.read(), 1) == b"HELLOtail"
        writer.close()
        await writer.wait_closed()
    assert upstream.closed.is_set() and not bridge.tasks


@pytest.mark.asyncio
async def test_idle_tunnel_is_closed_after_revocation(rig):
    bridge, upstream, state = rig
    async with bridge:
        reader, writer = await asyncio.open_unix_connection(str(bridge.socket_path))
        writer.write(f"KRIM-PREVIEW/1 CONNECT {HOST} 443\n".encode())
        await writer.drain()
        assert await reader.readexactly(3) == b"OK\n"
        state["revoked"] = True
        # No NO/error bytes may be injected into an established TLS stream.
        assert await asyncio.wait_for(reader.read(), 1) == b""
        await asyncio.wait_for(upstream.closed.wait(), 1)
        writer.close()
        await writer.wait_closed()
    assert bridge.tunnels == 0 and not bridge.tasks


@pytest.mark.asyncio
async def test_bridge_exit_cancels_active_tunnel_and_does_not_unlink_replaced_file(rig):
    bridge, upstream, _ = rig
    async with bridge:
        reader, writer = await asyncio.open_unix_connection(str(bridge.socket_path))
        writer.write(f"KRIM-PREVIEW/1 CONNECT {HOST} 443\n".encode())
        await writer.drain()
        assert await reader.readexactly(3) == b"OK\n"
        bridge.socket_path.unlink()
        bridge.socket_path.write_text("replacement is not our socket")
    assert await asyncio.wait_for(reader.read(), 1) == b""
    writer.close()
    await writer.wait_closed()
    assert upstream.closed.is_set() and not bridge.tasks
    assert bridge.socket_path.read_text() == "replacement is not our socket"
