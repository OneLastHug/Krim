"""Trusted Linux host terminal supervisor, outside all project containers.

Only I's host process may reach this private socket. B supplies immutable runtime
attachment argv and stops whole execution units; killing a local attachment is
not container cancellation. Private receipts/logs are backend spools, not another
operation database. Local Shell mode is restricted to explicit functional tests.
"""

from __future__ import annotations

import argparse
import asyncio
import errno
import fcntl
import json
import os
import pty
import signal
import socket
import stat
import subprocess
import termios
import time
from pathlib import Path
from typing import Any

from .byte_log import ByteLog
from .wire import MAX_MESSAGE, canonical_digest, checked_id

TERMINAL = {"exited", "cancelled", "failed"}


def private_directory(path: Path) -> None:
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    info = path.lstat()
    if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid()
            or info.st_mode & 0o077):
        raise PermissionError("worker state must be private to its trusted uid")


def atomic_json(path: Path, value: dict[str, Any]) -> None:
    temporary = path.with_suffix(".tmp")
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o600)
    try:
        data = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
        with os.fdopen(fd, "wb", closefd=False) as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
    finally:
        os.close(fd)
    os.replace(temporary, path)
    directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def open_cwd(root: Path, relative: str) -> int:
    if (not relative or relative.startswith("/") or "\\" in relative or ":" in relative
            or any(ord(char) < 32 for char in relative)
            or ".." in relative.split("/")):
        raise ValueError("cwd must be project relative")
    resolved = (root / relative).resolve(strict=True)
    resolved.relative_to(root)
    # Pin the actual directory before spawning; reject a last-component swap.
    fd = os.open(resolved, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    actual = Path(os.readlink(f"/proc/self/fd/{fd}")).resolve()
    try:
        actual.relative_to(root)
    except ValueError:
        os.close(fd)
        raise ValueError("cwd escaped project") from None
    return fd


def child_setup(tty: bool, cwd_fd: int | None) -> None:
    if tty:
        fcntl.ioctl(0, termios.TIOCSCTTY, 0)
    if cwd_fd is not None:
        os.close(cwd_fd)


class Process:
    def __init__(self, manager: Supervisor, record: dict[str, Any]) -> None:
        self.manager = manager
        self.record = record
        self.key = checked_id(record["process_id"])
        self.receipt = manager.root / f"{self.key}.json"
        self.log = ByteLog(
            manager.root / f"{self.key}.log", manager.log_bytes,
            start=record.get("log_start", 0), end=record.get("log_end", 0),
        )
        self.child: subprocess.Popen[bytes] | None = None
        self.master: int | None = None
        self.output_fd: int | None = None
        self.input_fd: int | None = None
        self.pidfd: int | None = None
        self.stdin_closed = False
        self.cancel_reason: str | None = None
        self.finished = asyncio.Event()
        self.reader_done = asyncio.Event()
        self.lock = asyncio.Lock()
        self.input_lock = asyncio.Lock()
        self.tasks: set[asyncio.Task[Any]] = set()
        self.reader_task: asyncio.Task[Any] | None = None
        if record["state"] in TERMINAL:
            self.finished.set()
            self.reader_done.set()

    def task(self, coroutine: Any) -> asyncio.Task[Any]:
        task = asyncio.create_task(coroutine)
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)
        return task

    def persist(self) -> None:
        self.record["log_start"] = self.log.start
        self.record["log_end"] = self.log.end
        self.log.flush()
        atomic_json(self.receipt, self.record)

    def status(self) -> dict[str, Any]:
        return {key: self.record.get(key) for key in (
            "process_id", "state", "exit_code", "reason", "request_digest",
        )}

    def launch(self) -> None:
        request = self.record["request"]
        trusted = "attach_argv" in self.record
        if trusted:
            fd = None
            argv = self.record["attach_argv"]
            cwd = str(self.manager.root)
        else:
            fd = open_cwd(self.manager.project, request["cwd"])
            argv = ["/bin/sh", "-c", request["command"]]
            cwd = f"/proc/self/fd/{fd}"
        master: int | None = None
        slave: int | None = None
        try:
            if request["pty"]:
                master, slave = pty.openpty()
                os.set_blocking(master, False)
            self.child = subprocess.Popen(
                argv,
                stdin=slave if slave is not None else subprocess.PIPE,
                stdout=slave if slave is not None else subprocess.PIPE,
                stderr=slave if slave is not None else subprocess.STDOUT,
                cwd=cwd, pass_fds=() if fd is None else (fd,),
                start_new_session=True,
                preexec_fn=lambda: child_setup(request["pty"], fd),
                env=None if trusted else self.manager.shell_environment, bufsize=0,
            )
            self.pidfd = os.pidfd_open(self.child.pid)
            self.master = master
            master = None
            if self.master is not None:
                self.output_fd = self.input_fd = self.master
            else:
                assert self.child.stdout is not None and self.child.stdin is not None
                self.output_fd = self.child.stdout.fileno()
                self.input_fd = self.child.stdin.fileno()
                os.set_blocking(self.output_fd, False)
                os.set_blocking(self.input_fd, False)
        finally:
            if fd is not None:
                os.close(fd)
            if slave is not None:
                os.close(slave)
            if master is not None:
                os.close(master)
        self.record.update(state="running", pid=self.child.pid, started_at=time.time())
        self.persist()
        self.reader_task = self.task(self.read_output())
        self.task(self.watch())
        timeout = request["timeout_seconds"]
        if timeout is not None and not trusted:
            self.task(self.deadline(timeout))
        # Trusted attachments are not the actual container process. B owns
        # its deadline and complete-unit cancellation; killing this local
        # attachment is never reported as having stopped the execution.

    async def read_output(self) -> None:
        assert self.output_fd is not None
        fd = self.output_fd
        try:
            while True:
                try:
                    data = os.read(fd, 65536)
                except BlockingIOError:
                    await self.fd_ready(fd)
                    continue
                except OSError as error:
                    if error.errno == errno.EIO:
                        break
                    raise
                if not data:
                    break
                # Secret-enabled commands have an opaque output channel: even
                # accidental print/PTY echo must never enter durable logs or
                # model-visible pages. Authorized code may still copy its own
                # secret elsewhere; this is not arbitrary-program DLP.
                if not self.record["request"].get("authorization", {}).get("secret_refs"):
                    self.log.append(data)
        except OSError:
            self.record["reason"] = "output_read_failed"
        finally:
            self.reader_done.set()

    @staticmethod
    async def fd_ready(fd: int, *, write: bool = False) -> None:
        loop = asyncio.get_running_loop()
        ready = loop.create_future()

        def wake() -> None:
            if not ready.done():
                ready.set_result(None)

        (loop.add_writer if write else loop.add_reader)(fd, wake)
        try:
            await ready
        finally:
            (loop.remove_writer if write else loop.remove_reader)(fd)

    def signal_group(self, number: int) -> None:
        if self.child is not None and self.child.returncode is None:
            try:
                os.killpg(self.child.pid, number)
            except ProcessLookupError:
                pass

    async def reap_group(self) -> None:
        self.signal_group(signal.SIGTERM)
        await asyncio.sleep(self.manager.kill_grace)
        self.signal_group(signal.SIGKILL)

    async def watch(self) -> None:
        assert self.child is not None
        # Observe without reaping: the leader's PID remains reserved until
        # group cleanup is complete. A retained pidfd alone would not make a
        # numeric killpg safe after the process has been reaped.
        assert self.pidfd is not None
        try:
            await self.fd_ready(self.pidfd)
        finally:
            os.close(self.pidfd)
            self.pidfd = None
        # Popen has no watcher and has not reaped the child. Readable pidfd
        # signals exit, while the unreaped child reserves its numeric PID.
        async with self.lock:
            await self.reap_group()
            self.child.wait()
            try:
                await asyncio.wait_for(self.reader_done.wait(), 1.0)
            except TimeoutError:
                self.record["reason"] = "output_not_closed"
                if self.reader_task is not None:
                    self.reader_task.cancel()
                    await asyncio.gather(self.reader_task, return_exceptions=True)
            if self.cancel_reason is not None:
                self.record.update(state="cancelled", reason=self.cancel_reason)
            elif self.record["state"] == "running":
                self.record["state"] = "exited"
            self.record["exit_code"] = self.child.returncode
            self.persist()
            self.finished.set()
            if self.master is not None:
                os.close(self.master)
                self.master = None
            else:
                if self.child.stdout is not None:
                    self.child.stdout.close()
                if self.child.stdin is not None:
                    self.child.stdin.close()
            self.input_fd = self.output_fd = None

    async def deadline(self, seconds: float) -> None:
        try:
            await asyncio.wait_for(self.finished.wait(), seconds)
        except TimeoutError:
            await self.cancel("timeout")

    async def cancel(self, reason: str = "cancelled") -> dict[str, Any]:
        async with self.lock:
            if self.finished.is_set():
                return self.status()
            if self.child is None:
                self.record.update(state="unknown", reason="worker_lost")
                self.persist()
                return self.status()
            # Publish the terminal state only after group cleanup/reaping.
            # Otherwise a concurrent output checkpoint could persist a
            # cancelled receipt for a process that is still alive.
            self.cancel_reason = self.cancel_reason or reason
            await self.reap_group()
        await asyncio.wait_for(self.finished.wait(), 5.0)
        return self.status()

    async def write(self, data: str, close: bool) -> None:
        encoded = data.encode("utf-8")
        if len(encoded) > 65536:
            raise ValueError("stdin exceeds limit")
        async with self.input_lock:
            if (self.record["state"] != "running" or self.stdin_closed
                    or self.cancel_reason is not None):
                raise ValueError("stdin is closed")
            assert self.child is not None and self.input_fd is not None
            fd = self.input_fd
            if self.master is not None and close:
                if "attach_argv" in self.record:
                    # Docker makes its host-side TTY raw to forward control
                    # bytes to the container TTY. This is terminal Ctrl-D,
                    # not a POSIX half-close; remote raw-mode applications
                    # interpret it as input and must be cancelled to hang up.
                    eof_bytes = b"\x04"
                else:
                    attributes = termios.tcgetattr(self.master)
                    if not attributes[3] & termios.ICANON:
                        raise ValueError("PTY EOF requires canonical mode; cancel to hang up")
                    eof = attributes[6][termios.VEOF]
                    if eof in (b"\x00", 0):
                        raise ValueError("PTY EOF is disabled")
                    eof_bytes = bytes([eof]) if isinstance(eof, int) else eof
                encoded += eof_bytes * 2
            # Bound backpressure so a non-reading child cannot monopolize a
            # worker connection. The process lifetime is unaffected.
            async with asyncio.timeout(5):
                while encoded:
                    if self.input_fd != fd or self.finished.is_set():
                        raise ValueError("stdin is closed")
                    try:
                        written = os.write(fd, encoded)
                    except BlockingIOError:
                        await self.fd_ready(fd, write=True)
                        continue
                    encoded = encoded[written:]
            if close:
                if self.master is None:
                    assert self.child.stdin is not None
                    self.child.stdin.close()
                    self.input_fd = None
                self.stdin_closed = True


class Supervisor:
    def __init__(
        self, root: Path, project: Path, *, log_bytes: int = 1048576,
        page_bytes: int = 32768, kill_grace: float = 0.15,
        trusted_attachments: bool = False, allow_same_uid_for_tests: bool = False,
    ) -> None:
        if not 4 <= log_bytes <= 67108864 or not 4 <= page_bytes <= 65536:
            raise ValueError("invalid output limits")
        if not 0 <= kill_grace <= 2:
            raise ValueError("invalid termination grace")
        private_directory(root)
        if not allow_same_uid_for_tests and not trusted_attachments:
            raise PermissionError("project shell requires a distinct isolation boundary")
        self.root = root.resolve()
        self.project = project.resolve(strict=True)
        if (self.root == self.project or self.root.is_relative_to(self.project)
                or self.project.is_relative_to(self.root)):
            raise ValueError("worker state and project cannot contain each other")
        self.trusted_attachments = trusted_attachments
        self.log_bytes = log_bytes
        self.page_bytes = page_bytes
        self.kill_grace = kill_grace
        self.shell_environment = {
            "PATH": "/usr/local/bin:/usr/bin:/bin", "HOME": str(self.project),
            "LANG": "C.UTF-8", "TERM": "xterm-256color",
        }
        self.processes: dict[str, Process] = {}
        self.active_limit = 64
        self.receipt_limit = 4096
        self.admission_lock = asyncio.Lock()
        self.stopping = False
        self.closed_runs: set[str] = set()
        # Hold an advisory lock for the entire supervisor life. No overlapping
        # instance can reinterpret a live receipt as lost or run the same ID.
        self.lock_fd = os.open(root / "worker.lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
        try:
            fcntl.flock(self.lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            os.close(self.lock_fd)
            raise
        for path in root.glob("*.json"):
            record = json.loads(path.read_text())
            process = Process(self, record)
            if record["state"] not in TERMINAL:
                # An interrupted ring write may have overwritten bytes at the
                # old offsets. Preserve cursor monotonicity but do not present
                # uncertain bytes as a recovered log page.
                process.log.start = process.log.end
                record.update(state="unknown", exit_code=None, reason="worker_lost")
                process.persist()
            self.processes[record["process_id"]] = process

    async def dispatch(self, payload: dict[str, Any]) -> dict[str, Any]:
        action = payload["action"]
        if action == "ping":
            return {"ready": True}
        if action == "submit":
            async with self.admission_lock:
                return await self.submit(payload)
        if action in {"stop_environment", "stop_run"}:
            async with self.admission_lock:
                if action == "stop_environment":
                    self.stopping = True
                else:
                    self.closed_runs.add(canonical_digest(payload["ownership"]))
                selected = [process for process in self.processes.values() if (
                    action == "stop_environment" or (
                        process.record["request"]["lifecycle"] == "run"
                        and process.record["request"]["ownership"] == payload["ownership"]
                    )
                )]
                results = await asyncio.gather(*(process.cancel() for process in selected))
                return {"processes": results}
        process_id = payload["process_id"]
        checked_id(process_id)
        process = self.processes.get(process_id)
        if process is None:
            return {"process_id": process_id, "state": "unknown", "exit_code": None,
                    "reason": "receipt_missing", "request_digest": None}
        if payload.get("request_digest") != process.record["request_digest"]:
            raise ValueError("process binding changed")
        if action == "inspect":
            return process.status()
        if action == "output":
            # Persist offsets before exposing any cursor. After a supervisor
            # crash the damaged active ring may be discarded, but a cursor
            # already returned to a client must never move backwards.
            process.persist()
            page = process.log.read(
                payload["cursor"], self.page_bytes,
                eof=(process.record["state"] == "unknown" or (
                    process.finished.is_set() and process.reader_done.is_set()
                )),
            )
            return {"process_id": process_id, **page}
        if action == "stdin":
            await process.write(payload["data"], payload.get("close", False))
            return {"written": True}
        if action == "cancel":
            return await process.cancel()
        raise ValueError("unsupported worker action")

    async def submit(self, payload: dict[str, Any]) -> dict[str, Any]:
        process_id = payload["process_id"]
        checked_id(process_id)
        request = payload["request"]
        if self.trusted_attachments:
            # This digest was computed by I's command_digest at the trusted
            # host boundary. The stdlib daemon must not invent another C0
            # digest algorithm, nor accept an argv from model arguments.
            digest = payload["request_digest"]
            import re

            if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
                raise ValueError("invalid request digest")
            argv = payload.get("attach_argv")
            if (not isinstance(argv, list) or not argv or len(argv) > 128
                    or any(not isinstance(arg, str) or "\x00" in arg for arg in argv)):
                raise ValueError("trusted attachment argv required")
        else:
            if "attach_argv" in payload:
                raise ValueError("test worker cannot accept attachment argv")
            digest = canonical_digest(request)
            if digest != payload["request_digest"]:
                raise ValueError("request digest mismatch")
        operation_id = request.get("operation_id")
        if operation_id is not None:
            for known in self.processes.values():
                if (known.record["request"].get("operation_id") == operation_id
                        and known.record["process_id"] != process_id):
                    raise ValueError("operation already bound to a different process")
        existing = self.processes.get(process_id)
        if existing is not None:
            if (existing.record["request_digest"] != digest
                    or (self.trusted_attachments and existing.record.get("execution_id")
                        != payload.get("execution_id"))):
                raise ValueError("process binding changed")
            return existing.status()
        if self.stopping:
            raise ValueError("environment is stopping")
        if canonical_digest(request["ownership"]) in self.closed_runs:
            raise ValueError("creating run is stopped")
        if (len(self.processes) >= self.receipt_limit
                or sum(item.record["state"] in {"accepted", "running", "unknown"}
                       for item in self.processes.values()) >= self.active_limit):
            raise ValueError("worker process limit reached")
        # Backend-local defense. Host validates the full C0 DTO before this call.
        if (not isinstance(request["command"], str) or not request["command"].strip()
                or "\x00" in request["command"] or len(request["command"]) > 32768):
            raise ValueError("invalid command")
        timeout = request["timeout_seconds"]
        if timeout is not None and not 0 < timeout <= 86400:
            raise ValueError("invalid process timeout")
        if request["lifecycle"] not in {"run", "project_service"}:
            raise ValueError("invalid lifecycle")
        if request["lifecycle"] == "run" and timeout is None:
            raise ValueError("run process requires timeout")
        record = {"process_id": process_id, "request": request, "request_digest": digest,
                  "state": "accepted", "exit_code": None, "reason": None}
        if self.trusted_attachments:
            record["attach_argv"] = payload["attach_argv"]
            record["execution_id"] = payload["execution_id"]
        process = Process(self, record)
        self.processes[process_id] = process
        # Durable intent precedes spawn. Any crash in between becomes unknown.
        process.persist()
        try:
            process.launch()
        except Exception:
            # Spawn can have succeeded before the durable running receipt
            # failed. Never leave that child alive behind a failed admission.
            if process.child is not None:
                process.signal_group(signal.SIGKILL)
                process.child.wait(timeout=5)
                if process.pidfd is not None:
                    os.close(process.pidfd)
                    process.pidfd = None
                if process.master is not None:
                    os.close(process.master)
                    process.master = None
                if process.child.stdin is not None:
                    process.child.stdin.close()
                if process.child.stdout is not None:
                    process.child.stdout.close()
            record.update(state="failed", reason="spawn_failed", exit_code=None)
            process.finished.set()
            process.reader_done.set()
            process.persist()
            raise
        return process.status()

    async def close(self) -> None:
        async with self.admission_lock:
            self.stopping = True
            await asyncio.gather(*(process.cancel() for process in self.processes.values()))
        for process in self.processes.values():
            for task in tuple(process.tasks):
                task.cancel()
            await asyncio.gather(*tuple(process.tasks), return_exceptions=True)
            process.log.close()
        os.close(self.lock_fd)


async def serve(supervisor: Supervisor) -> None:
    path = supervisor.root / "control.sock"
    path.unlink(missing_ok=True)

    async def connection(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            line = await asyncio.wait_for(reader.readline(), 35)
            if len(line) > MAX_MESSAGE:
                raise ValueError("worker message too large")
            payload = json.loads(line)
            result = await supervisor.dispatch(payload)
            reply = {"ok": True, "result": result}
        except Exception as error:
            # No command, host path, output, or backend exception body crosses
            # the control boundary as a diagnostic.
            reply = {"ok": False, "error": type(error).__name__}
        try:
            encoded = json.dumps(reply, separators=(",", ":"), ensure_ascii=False).encode()
            writer.write(encoded + b"\n")
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_unix_server(connection, path=str(path), limit=MAX_MESSAGE + 1)
    os.chmod(path, 0o600)
    stopped = asyncio.Event()
    loop = asyncio.get_running_loop()
    for number in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(number, stopped.set)
    try:
        async with server:
            await stopped.wait()
    finally:
        await supervisor.close()
        path.unlink(missing_ok=True)


def rpc(root: Path, payload: dict[str, Any]) -> dict[str, Any]:
    data = json.dumps(payload, separators=(",", ":")).encode() + b"\n"
    if len(data) > MAX_MESSAGE:
        raise ValueError("worker message too large")
    with socket.socket(socket.AF_UNIX) as client:
        client.settimeout(35)
        client.connect(str(root / "control.sock"))
        client.sendall(data)
        chunks = bytearray()
        while b"\n" not in chunks:
            part = client.recv(65536)
            if not part:
                raise ConnectionError("worker reply incomplete")
            chunks.extend(part)
            if len(chunks) > MAX_MESSAGE:
                raise ValueError("worker reply too large")
        result = json.loads(chunks)
        if not isinstance(result, dict):
            raise ValueError("invalid worker reply")
        return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("serve", "rpc"))
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--project", type=Path)
    parser.add_argument("--trusted-attachments", action="store_true")
    parser.add_argument("--allow-same-uid-for-tests", action="store_true")
    args = parser.parse_args()
    if args.mode == "rpc":
        import sys

        raw = sys.stdin.buffer.read(MAX_MESSAGE + 1)
        if len(raw) > MAX_MESSAGE:
            raise ValueError("worker message too large")
        print(json.dumps(rpc(args.state, json.loads(raw))))
        return
    if args.project is None:
        parser.error("--project is required for serve")
    supervisor = Supervisor(
        args.state, args.project, trusted_attachments=args.trusted_attachments,
        allow_same_uid_for_tests=args.allow_same_uid_for_tests,
    )
    asyncio.run(serve(supervisor))


if __name__ == "__main__":
    main()
