"""Real pipe/child-process IPC tests, not Chromium or container acceptance."""

from __future__ import annotations

import asyncio
import os
import sys
from contextlib import suppress
from pathlib import Path

import pytest
from test_dev_browser_session import session

from krim_agent.dev_browser import BrowserError
from krim_agent.dev_browser.policy import trusted_control_origin

DEPLOY = Path(__file__).parents[2] / "deployment/development-browser"
sys.path.insert(0, str(DEPLOY))
import stdio_channel  # noqa: E402
from stdio_channel import FDChannel, IPCError  # noqa: E402
from worker import dispatch, limited_drive  # noqa: E402


@pytest.fixture
def pipes():
    incoming, write_input = os.pipe()
    read_output, outgoing = os.pipe()
    channel = FDChannel(incoming, outgoing)
    try:
        yield channel, write_input, read_output
    finally:
        channel.close()
        for fd in (incoming, write_input, read_output, outgoing):
            with suppress(OSError):
                os.close(fd)


@pytest.mark.asyncio
async def test_pipe_lines_eof_partial_and_bounded_send(pipes):
    channel, writer, output = pipes
    os.write(writer, b'{"op":"diagnostics"}\n{"op":"close"}\n')
    assert await channel.receive() == {"op": "diagnostics"}
    assert await channel.receive() == {"op": "close"}
    await channel.send({"ok": True})
    assert os.read(output, 100) == b'{"ok":true}\n'
    os.close(writer)
    assert await channel.receive() is None


@pytest.mark.asyncio
async def test_partial_eof_is_not_a_complete_command(pipes):
    channel, writer, _ = pipes
    os.write(writer, b'{"op":')
    os.close(writer)
    with pytest.raises(IPCError, match="browser_incomplete_input"):
        await channel.receive()


@pytest.mark.asyncio
@pytest.mark.parametrize("raw", [b"[]\n", b"{bad}\n", b'"string"\n', b"\xff\n"])
async def test_ipc_does_not_coerce_nonobject_or_bad_json(pipes, raw):
    channel, writer, _ = pipes
    os.write(writer, raw)
    with pytest.raises(IPCError, match="browser_invalid_input"):
        await channel.receive()


@pytest.mark.asyncio
async def test_line_and_output_limits_are_enforced_before_unbounded_growth(pipes, monkeypatch):
    monkeypatch.setattr(stdio_channel, "MAX_LINE", 64)
    channel, writer, _ = pipes
    os.write(writer, b"x" * 65)
    with pytest.raises(IPCError, match="browser_input_limit"):
        await channel.receive()
    assert len(channel.buffer) == 65
    with pytest.raises(IPCError, match="browser_output_limit"):
        await channel.send({"oversized": "x" * 100})


@pytest.mark.asyncio
@pytest.mark.parametrize("partial", [False, True])
async def test_idle_and_half_line_ttl_cancel_fd_read_and_close_session(pipes, partial):
    channel, writer, _ = pipes
    if partial:
        os.write(writer, b'{"op":')
    closed = asyncio.Event()

    class Session:
        async def close(self):
            closed.set()

    with pytest.raises(TimeoutError):
        await limited_drive(channel, Session(), seconds=0.02)
    assert closed.is_set()
    with pytest.raises(KeyError):
        asyncio.get_running_loop()._selector.get_key(channel.input_fd)


@pytest.mark.asyncio
async def test_blocked_stdout_cancellation_removes_writer_callback(pipes):
    channel, _, _ = pipes
    while True:
        try:
            os.write(channel.output_fd, b"x" * 4096)
        except BlockingIOError:
            break
    with pytest.raises(TimeoutError):
        async with asyncio.timeout(0.02):
            await channel.send({"ok": True})
    with pytest.raises(KeyError):
        asyncio.get_running_loop()._selector.get_key(channel.output_fd)


@pytest.mark.asyncio
@pytest.mark.parametrize("partial", [False, True])
async def test_asyncio_run_exits_with_open_stdin_without_executor_thread(partial):
    # Exercise a real Python child with stdin held open, without Playwright.
    root = DEPLOY.parents[1]
    code = f"""
import asyncio, sys
sys.path[:0] = [{str(DEPLOY)!r}, {str(root / "src")!r}]
from stdio_channel import FDChannel
from worker import limited_drive
class Session:
    closed = False
    async def close(self): self.closed = True
async def main():
    channel, session = FDChannel(0,1), Session()
    try:
        await limited_drive(channel, session, seconds=0.05)
    except TimeoutError:
        assert session.closed
        return 124
    finally:
        channel.close()
raise SystemExit(asyncio.run(main()))
"""
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-I",
        "-c",
        code,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        if partial:
            process.stdin.write(b'{"op":')
            await process.stdin.drain()
        assert await asyncio.wait_for(process.wait(), 5) == 124
        assert await process.stderr.read() == b""
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()
        process.stdin.close()


@pytest.mark.asyncio
async def test_ipc_delegates_action_validation_and_artifact_hash_to_adapter():
    context, _, adapter = await session()
    try:
        await dispatch(adapter, {"op": "goto", "path": "/fixture"})
        await dispatch(adapter, {"op": "fill", "selector": "#input", "value": "text"})
        artifact = await dispatch(adapter, {"op": "screenshot"})
        assert artifact["media_type"] == "image/png" and len(artifact["sha256"]) == 64
        with pytest.raises(BrowserError, match="browser_invalid_input"):
            await dispatch(adapter, {"op": "click", "selector": "#go", "url": "https://other"})
        with pytest.raises(BrowserError, match="browser_target_invalid"):
            await dispatch(adapter, {"op": "goto", "path": "https://foreign.example"})
    finally:
        await adapter.close()
    assert context.closed.is_set()


@pytest.mark.asyncio
async def test_independent_close_budget_stops_a_hung_cleanup(pipes, monkeypatch):
    import worker

    channel, _, _ = pipes
    entered, cancelled = asyncio.Event(), asyncio.Event()

    class Session:
        async def close(self):
            entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()

    monkeypatch.setattr(worker, "CLEANUP_SECONDS", 0.02)
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(limited_drive(channel, Session(), seconds=0.02), 1)
    assert entered.is_set() and cancelled.is_set()


@pytest.mark.asyncio
async def test_process_watchdog_exits_even_when_async_cleanup_never_finishes():
    root = DEPLOY.parents[1]
    code = f"""
import asyncio,sys
sys.path[:0] = [{str(DEPLOY)!r}, {str(root / "src")!r}]
from entrypoint import install_watchdog
install_watchdog(0.1)
async def main():
    while True:
        try:
            await asyncio.sleep(100)
        except asyncio.CancelledError:
            continue
asyncio.run(main())
"""
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-I",
        "-c",
        code,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        assert await asyncio.wait_for(process.wait(), 5) == 124
        assert await process.stderr.read() == b""
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()
        process.stdin.close()


def test_control_listener_port_is_a_header_only_not_an_egress_target():
    assert trusted_control_origin("https://control.example:8443") == "https://control.example:8443"
    for value in (
        "https://name:secret@control.example",
        "https://control.example/path",
        "http://control.example",
    ):
        with pytest.raises(BrowserError):
            trusted_control_origin(value)
