from __future__ import annotations

import queue
import subprocess
import sys
import threading
import time

import pytest

from krim_agent.dev_environment.preview_relay import RELAY_CODE

HEADER = __import__("struct").Struct("!BI")
COUNT = __import__("struct").Struct("!I")
MAX_PAYLOAD = 65536
WINDOW = 262144
DATA, FIN, PING, CLOSE, CREDIT = 1, 2, 3, 4, 5
READY, REMOTE_DATA, EOF, CONNECT_FAILED, REMOTE_CREDIT, DONE = 10, 11, 12, 13, 14, 15

# This is a raw TCP fixture, not an HTTP or other web server.  It runs in the
# same Linux network namespace as the relay child.
TCP_FIXTURE = r'''
import os
import socket
import sys
import time

mode = sys.argv[1]
listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
listener.bind(("127.0.0.1", 0))
listener.listen(1)
os.write(1, (str(listener.getsockname()[1]) + "\n").encode("ascii"))
if mode == "free":
    listener.close()
    raise SystemExit(0)
listener.settimeout(15.0)
try:
    conn, _ = listener.accept()
except Exception:
    listener.close()
    raise
listener.close()
try:
    if mode == "echo":
        while True:
            data = conn.recv(65536)
            if not data:
                conn.shutdown(socket.SHUT_WR)
                break
            conn.sendall(data)
    elif mode == "half":
        data = conn.recv(65536)
        if data:
            conn.sendall(b"echo:" + data)
        while conn.recv(65536):
            pass
        conn.sendall(b"tail-after-fin")
        conn.shutdown(socket.SHUT_WR)
    elif mode == "burst":
        trigger = conn.recv(65536)
        if trigger:
            payload = bytes(range(251)) * 1400
            conn.sendall(payload)
        time.sleep(15.0)
    elif mode == "hold":
        time.sleep(15.0)
finally:
    try:
        conn.close()
    except Exception:
        pass
'''


def _child_command(code: str, *args: str) -> list[str] | None:
    if sys.platform.startswith("linux"):
        return [sys.executable, "-I", "-c", code, *args]
    return None


def _frame(kind: int, payload: bytes = b"") -> bytes:
    return HEADER.pack(kind, len(payload)) + payload


def _read_exact(stream, size: int) -> bytes:
    value = bytearray()
    while len(value) < size:
        chunk = stream.read(size - len(value))
        if not chunk:
            break
        value.extend(chunk)
    return bytes(value)


class RawTCPFixture:
    def __init__(self, mode: str):
        command = _child_command(TCP_FIXTURE, mode)
        if command is None:
            pytest.skip("requires Linux process and socket semantics")
        self.process = subprocess.Popen(
            command,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=0,
        )
        assert self.process.stdout is not None
        line = self.process.stdout.readline().replace(b"\x00", b"").strip()
        if not line.isdigit():
            stderr = self.process.stderr.read(4096) if self.process.stderr else b""
            self.close()
            raise AssertionError(f"TCP fixture did not publish a port: {stderr!r}")
        self.port = int(line)

    def close(self):
        if self.process.poll() is None:
            self.process.kill()
        try:
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait()


class RelayProcess:
    def __init__(
        self,
        port: int,
        *,
        heartbeat: float = 5.0,
        max_age: float = 30.0,
        idle: float = 20.0,
        read_stdout: bool = True,
    ):
        command = _child_command(
            RELAY_CODE,
            str(port),
            "2",
            str(heartbeat),
            str(max_age),
            str(idle),
            "test-token",
        )
        if command is None:
            pytest.skip("requires Linux process and socket semantics")
        self.process = subprocess.Popen(
            command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=0,
        )
        self.frames: queue.Queue[tuple[int, bytes] | None] = queue.Queue()
        self.reader = None
        if read_stdout:
            self.reader = threading.Thread(target=self._read_frames, daemon=True)
            self.reader.start()

    def _read_frames(self):
        assert self.process.stdout is not None
        while True:
            header = _read_exact(self.process.stdout, HEADER.size)
            if not header:
                self.frames.put(None)
                return
            if len(header) != HEADER.size:
                self.frames.put(None)
                return
            kind, size = HEADER.unpack(header)
            payload = _read_exact(self.process.stdout, size)
            if len(payload) != size:
                self.frames.put(None)
                return
            self.frames.put((kind, payload))

    def send(self, kind: int, payload: bytes = b""):
        assert self.process.stdin is not None
        encoded = memoryview(_frame(kind, payload))
        while encoded:
            count = self.process.stdin.write(encoded)
            if not count:
                raise BrokenPipeError
            encoded = encoded[count:]
        self.process.stdin.flush()

    def next_frame(self, timeout: float = 3.0) -> tuple[int, bytes]:
        deadline = time.monotonic() + timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise queue.Empty
            try:
                item = self.frames.get(timeout=min(0.2, remaining))
            except queue.Empty:
                if self.process.poll() is None:
                    try:
                        self.send(PING)
                    except (BrokenPipeError, OSError):
                        pass
                continue
            assert item is not None, "relay stdout closed before a complete frame"
            return item

    def wait(self, timeout: float = 5.0) -> int:
        return self.process.wait(timeout=timeout)

    def close(self):
        if self.process.poll() is None:
            try:
                self.send(CLOSE)
            except (BrokenPipeError, OSError):
                pass
            if self.process.stdin is not None:
                self.process.stdin.close()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()

    def stderr(self) -> bytes:
        assert self.process.stderr is not None
        return self.process.stderr.read()


def _wait_ready(relay: RelayProcess):
    kind, payload = relay.next_frame()
    assert kind == READY
    assert len(payload) == COUNT.size
    assert COUNT.unpack(payload)[0] > 0


def _finish_echo(relay: RelayProcess, expected: bytes):
    received = bytearray()
    saw_eof = False
    while True:
        kind, payload = relay.next_frame(timeout=5)
        if kind == REMOTE_DATA:
            received.extend(payload)
            relay.send(CREDIT, COUNT.pack(len(payload)))
        elif kind == REMOTE_CREDIT:
            assert len(payload) == COUNT.size and COUNT.unpack(payload)[0] > 0
        elif kind == EOF:
            saw_eof = True
        elif kind == DONE:
            break
        else:
            raise AssertionError(f"unexpected relay frame {kind}")
    assert bytes(received) == expected
    assert saw_eof
    assert relay.wait() == 0


@pytest.fixture

def wsl_ready():
    if _child_command("pass") is None:
        pytest.skip("requires Linux process and socket semantics")


def test_binary_data_and_protocol_frames(wsl_ready):
    server = RawTCPFixture("echo")
    relay = RelayProcess(server.port)
    try:
        _wait_ready(relay)
        payload = bytes(range(256)) * 1024
        for offset in range(0, len(payload), MAX_PAYLOAD):
            relay.send(DATA, payload[offset:offset + MAX_PAYLOAD])
        relay.send(FIN)
        _finish_echo(relay, payload)
        assert relay.stderr() == b""
    finally:
        relay.close()
        server.close()


def test_fin_only_closes_tcp_write_side_and_keeps_read_side(wsl_ready):
    server = RawTCPFixture("half")
    relay = RelayProcess(server.port)
    try:
        _wait_ready(relay)
        relay.send(DATA, b"request")
        relay.send(FIN)
        received = bytearray()
        saw_eof = False
        while True:
            kind, payload = relay.next_frame(timeout=5)
            if kind == REMOTE_DATA:
                received.extend(payload)
                relay.send(CREDIT, COUNT.pack(len(payload)))
            elif kind == REMOTE_CREDIT:
                pass
            elif kind == EOF:
                saw_eof = True
            elif kind == DONE:
                break
            else:
                raise AssertionError(f"unexpected relay frame {kind}")
        assert bytes(received) == b"echo:requesttail-after-fin"
        assert saw_eof
        assert relay.wait() == 0
    finally:
        relay.close()
        server.close()


def test_credit_window_and_ping_under_stdout_backpressure(wsl_ready):
    server = RawTCPFixture("burst")
    relay = RelayProcess(server.port, heartbeat=0.2, read_stdout=False)
    try:
        assert relay.process.stdout is not None
        header = _read_exact(relay.process.stdout, HEADER.size)
        assert HEADER.unpack(header)[0] == READY
        assert COUNT.unpack(_read_exact(relay.process.stdout, COUNT.size))[0] > 0
        relay.send(DATA, b"burst")
        # Do not read stdout while the server offers more than WINDOW bytes.
        # The relay's DATA output must remain bounded and it must still read
        # heartbeats from stdin.
        deadline = time.monotonic() + 0.7
        while time.monotonic() < deadline:
            relay.send(PING)
            time.sleep(0.04)
        assert relay.process.poll() is None
    finally:
        relay.close()
        server.close()


def test_missing_heartbeat_closes_tcp_and_exits(wsl_ready):
    server = RawTCPFixture("hold")
    relay = RelayProcess(server.port, heartbeat=0.12, idle=5.0)
    try:
        _wait_ready(relay)
        assert relay.wait(timeout=3) != 0
    finally:
        relay.close()
        server.close()


def test_stdin_eof_closes_tcp_and_exits(wsl_ready):
    server = RawTCPFixture("hold")
    relay = RelayProcess(server.port)
    try:
        _wait_ready(relay)
        assert relay.process.stdin is not None
        relay.process.stdin.close()
        assert relay.wait(timeout=3) == 0
    finally:
        relay.close()
        server.close()


@pytest.mark.parametrize("kind", ["idle", "max_age"])
def test_idle_and_absolute_age_expire_even_with_heartbeats(wsl_ready, kind):
    server = RawTCPFixture("hold")
    if kind == "idle":
        relay = RelayProcess(server.port, heartbeat=0.1, idle=0.18, max_age=5.0)
    else:
        relay = RelayProcess(server.port, heartbeat=0.1, idle=5.0, max_age=0.22)
    try:
        _wait_ready(relay)
        deadline = time.monotonic() + 1.0
        while time.monotonic() < deadline and relay.process.poll() is None:
            relay.send(PING)
            time.sleep(0.03)
        assert relay.process.poll() is not None
        assert relay.wait() != 0
    finally:
        relay.close()
        server.close()


def test_connect_failed_is_a_normal_single_frame_exit(wsl_ready):
    server = RawTCPFixture("free")
    try:
        server.process.wait(timeout=3)
        relay = RelayProcess(server.port)
        try:
            kind, payload = relay.next_frame(timeout=3)
            assert (kind, payload) == (CONNECT_FAILED, b"")
            assert relay.wait(timeout=3) == 0
            assert relay.stderr() == b""
        finally:
            relay.close()
    finally:
        server.close()
