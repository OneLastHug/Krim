"""Linux bubblewrap执行基础：隔离副本、准备后资格检查及pidfd收束。"""

from __future__ import annotations

import asyncio
import base64
import contextvars
import ctypes
import hashlib
import json
import os
import select
import selectors
import signal
import subprocess
import sys
import tempfile
import time
from collections.abc import Awaitable, Callable, Mapping
from pathlib import Path
from threading import Event, Lock
from typing import Any, Literal
from uuid import uuid4

from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace import Workspace

from .models import (
    CommandSpec,
    ExecutionArtifact,
    ExecutionResult,
    ExecutionStatus,
    SandboxFile,
    SandboxLimits,
)

Authorize = Callable[[], Awaitable[None]]
_current: contextvars.ContextVar[ExecutionHandle | None] = contextvars.ContextVar(
    "krim_command", default=None
)


def _syscall(number: int, *arguments: Any) -> int:
    libc = ctypes.CDLL(None, use_errno=True)
    result = int(libc.syscall(number, *arguments))
    if result < 0:
        number = ctypes.get_errno()
        raise OSError(number, os.strerror(number))
    return result


def _kill(pidfd: int) -> None:
    try:
        _syscall(424, pidfd, signal.SIGKILL, ctypes.c_void_p(), 0)
    except ProcessLookupError:
        pass


def _child_pidfd(process: subprocess.Popen[bytes], status_fd: int, deadline: float) -> int:
    """在可信bubblewrap的启动门内取得并核对namespace leader的稳定句柄。"""
    wire = bytearray()
    while time.monotonic() < deadline:
        if not select.select([status_fd], [], [], 0.02)[0]:
            continue
        chunk = os.read(status_fd, 4096)
        if not chunk or len(wire) + len(chunk) > 16384:
            raise ServiceError("execution_setup_failed", "隔离进程未能准备就绪", 503)
        wire.extend(chunk)
        while b"\n" in wire:
            line, _, rest = wire.partition(b"\n")
            wire = bytearray(rest)
            value = json.loads(line)
            child = value.get("child-pid")
            if child is None:
                continue
            if type(child) is not int or child <= 0:
                raise ValueError("sandbox pid")
            fd = _syscall(434, child, 0)
            try:
                status = Path(f"/proc/{child}/status").read_text()
                fdinfo = Path(f"/proc/self/fdinfo/{fd}").read_text()
                # 先取得pidfd再核对父子关系与FD身份；数字PID复用时不能误杀无关进程。
                if f"PPid:\t{process.pid}\n" not in status or f"Pid:\t{child}\n" not in fdinfo:
                    raise ValueError("sandbox identity")
                return fd
            except BaseException:
                os.close(fd)
                raise
    raise TimeoutError


def _inside() -> bool:
    handle = _current.get()
    return handle is not None and handle._task is not None and not handle._task.done()


async def _settle[T](task: asyncio.Task[T]) -> T:
    cancelled = False
    while True:
        try:
            result = await asyncio.shield(task)
            break
        except asyncio.CancelledError:
            if task.cancelled():
                raise
            cancelled = True
    if cancelled:
        raise asyncio.CancelledError
    return result


class ExecutionHandle:
    def __init__(self) -> None:
        self._execution_id = str(uuid4())
        self._cancel = Event()
        self._task: asyncio.Task[ExecutionResult] | None = None
        self._started = asyncio.Event()
        self._output_lock = Lock()
        self._stdout = bytearray()
        self._stderr = bytearray()

    @property
    def execution_id(self) -> str:
        return self._execution_id

    def cancel(self) -> None:
        """请求停止原命令；通过wait读取收束后的实际结果。"""
        self._cancel.set()

    async def wait(self) -> ExecutionResult:
        if _inside():
            raise ServiceError("execution_wait_reentrant", "执行资格回调不能等待命令", 409)
        assert self._task is not None
        return await asyncio.shield(self._task)

    async def wait_started(self) -> bool:
        if _inside():
            raise ServiceError("execution_wait_reentrant", "执行资格回调不能等待命令", 409)
        assert self._task is not None
        waiting = asyncio.create_task(self._started.wait())
        try:
            await asyncio.wait((waiting, self._task), return_when=asyncio.FIRST_COMPLETED)
            return self._started.is_set() or (self._task.done() and self._task.result().started)
        finally:
            waiting.cancel()
            await asyncio.gather(waiting, return_exceptions=True)

    def output(self) -> tuple[bytes, bytes]:
        """读取已保留的有界输出，不等待或重新执行命令。"""
        with self._output_lock:
            return bytes(self._stdout), bytes(self._stderr)

    @property
    def done(self) -> bool:
        return self._task is not None and self._task.done()


class LinuxSandboxRunner:
    """执行资源只由可信宿主构造；不接受模型指定宿主挂载或环境。"""

    def __init__(
        self,
        *,
        limits: SandboxLimits | None = None,
        max_active: int = 4,
        uid: int | None = None,
        gid: int | None = None,
        bubblewrap: str | Path = "/usr/bin/bwrap",
    ) -> None:
        if not sys.platform.startswith("linux") or os.uname().machine not in {"x86_64", "aarch64"}:
            raise ServiceError("execution_platform", "当前平台尚未验证隔离执行", 503)
        if not 1 <= max_active <= 16:
            raise ValueError("命令并发上限无效")
        self._limits = SandboxLimits.model_validate((limits or SandboxLimits()).model_dump())
        self._uid = (65534 if os.getuid() == 0 else os.getuid()) if uid is None else uid
        self._gid = (65534 if os.getuid() == 0 else os.getgid()) if gid is None else gid
        if (
            self.uid <= 0
            or self.gid <= 0
            or (os.getuid() != 0 and (self.uid, self.gid) != (os.getuid(), os.getgid()))
        ):
            raise ServiceError("execution_identity", "执行身份须为可用的非root用户和组", 422)
        try:
            self._binary = Path(bubblewrap).resolve(strict=True)
            self._version = subprocess.run(
                [str(self.binary), "--version"],
                capture_output=True,
                text=True,
                timeout=5,
                check=True,
                env={"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8"},
            ).stdout.strip()
        except (OSError, subprocess.SubprocessError, UnicodeError, RuntimeError):
            raise ServiceError("execution_backend", "bubblewrap执行后端不可用", 503) from None
        if not self.version.startswith("bubblewrap "):
            raise ServiceError("execution_backend", "执行后端不是bubblewrap", 503)
        for path in ("/usr/bin/python3", "/usr/lib", "/usr/lib64"):
            if not Path(path).exists():
                raise ServiceError("execution_runtime", "系统Python或只读运行目录不可用", 503)
        try:
            test_fd = _syscall(434, os.getpid(), 0)
            try:
                _syscall(424, test_fd, 0, ctypes.c_void_p(), 0)
            finally:
                os.close(test_fd)
        except OSError:
            raise ServiceError("execution_pidfd", "内核进程句柄能力不可用", 503) from None
        self._broker_code = Path(__file__).with_name("_broker.py").read_bytes()
        self._fingerprint = hashlib.sha256(
            (
                self.version
                + hashlib.sha256(self.binary.read_bytes()).hexdigest()
                + self.limits.model_dump_json()
                + hashlib.sha256(self._broker_code).hexdigest()
                + f":{self.uid}:{self.gid}:{max_active}:isolated-copy-v1"
            ).encode()
        ).hexdigest()
        self._max_active = max_active
        self._active: dict[str, ExecutionHandle] = {}
        self._lock = asyncio.Lock()
        self._closed = False

    @property
    def limits(self) -> SandboxLimits:
        return self._limits

    @property
    def uid(self) -> int:
        return self._uid

    @property
    def gid(self) -> int:
        return self._gid

    @property
    def binary(self) -> Path:
        return self._binary

    @property
    def version(self) -> str:
        return self._version

    @property
    def fingerprint(self) -> str:
        return self._fingerprint

    @property
    def max_active(self) -> int:
        return self._max_active

    def _arguments(self, broker_fd: int, status_fd: int, gate_fd: int) -> list[str]:
        args = [
            str(self.binary),
            "--unshare-all",
            "--unshare-user",
            "--disable-userns",
            "--assert-userns-disabled",
            "--cap-drop",
            "ALL",
            "--new-session",
            "--die-with-parent",
            "--as-pid-1",
            "--json-status-fd",
            str(status_fd),
            "--block-fd",
            str(gate_fd),
            "--ro-bind",
            "/usr/bin",
            "/usr/bin",
            "--ro-bind",
            "/usr/lib",
            "/usr/lib",
            "--ro-bind",
            "/usr/lib64",
            "/usr/lib64",
            "--symlink",
            "usr/bin",
            "/bin",
            "--symlink",
            "usr/lib",
            "/lib",
            "--symlink",
            "usr/lib64",
            "/lib64",
            "--proc",
            "/proc",
            "--remount-ro",
            "/proc",
            "--dev",
            "/dev",
            "--remount-ro",
            "/dev",
            "--size",
            str(self.limits.workspace_bytes),
            "--tmpfs",
            "/workspace",
            "--size",
            str(self.limits.temporary_bytes),
            "--tmpfs",
            "/tmp",
            "--dir",
            "/__krim",
            "--ro-bind-data",
            str(broker_fd),
            "/__krim/broker.py",
            "--remount-ro",
            "/",
            "--clearenv",
            "--setenv",
            "PATH",
            "/usr/bin:/bin",
            "--setenv",
            "LANG",
            "C.UTF-8",
            "--setenv",
            "HOME",
            "/tmp/home",
            "--setenv",
            "TMPDIR",
            "/tmp",
            "--chdir",
            "/workspace",
            "/usr/bin/python3",
            "-I",
            "-S",
            "/__krim/broker.py",
        ]
        return args

    async def start(
        self,
        request: CommandSpec,
        files: Mapping[str, SandboxFile | bytes] | None = None,
        *,
        directories: tuple[str, ...] = (),
        authorize: Authorize | None = None,
    ) -> ExecutionHandle:
        request = CommandSpec.model_validate(request.model_dump())
        frozen = []
        total = 0
        if (
            len(files or {}) > self.limits.artifact_files
            or len(directories) > self.limits.artifact_files
        ):
            raise ServiceError("execution_input_limit", "命令输入数量超过上限", 422)
        for path, raw in (files or {}).items():
            if not Workspace._parts(path):
                raise ServiceError("execution_input_path", "命令输入须指定文件相对路径", 422)
            value = raw if isinstance(raw, SandboxFile) else SandboxFile(raw)
            if (
                not isinstance(value.data, bytes)
                or type(value.mode) is not int
                or not 0 <= value.mode <= 0o777
            ):
                raise ServiceError("execution_input_file", "命令输入内容或权限无效", 422)
            total += len(value.data)
            if len(value.data) > self.limits.file_bytes or total > self.limits.artifact_bytes:
                raise ServiceError("execution_input_limit", "命令输入字节数超过上限", 422)
            frozen.append(
                {"path": path, "data": base64.b64encode(value.data).decode(), "mode": value.mode}
            )
        for path in directories:
            if not Workspace._parts(path):
                raise ServiceError("execution_input_path", "命令目录须为相对路径", 422)
        payload = (
            json.dumps(
                {
                    "argv": request.argv,
                    "cwd": request.cwd,
                    "files": frozen,
                    "directories": list(directories),
                    "limits": self.limits.model_dump(),
                },
                separators=(",", ":"),
            ).encode()
            + b"\n"
        )
        async with self._lock:
            if self._closed:
                raise ServiceError("execution_closed", "命令执行器已关闭", 503)
            if len(self._active) >= self.max_active:
                raise ServiceError("execution_busy", "命令并发已达上限", 429)
            handle = ExecutionHandle()
            self._active[handle.execution_id] = handle
            handle._task = asyncio.create_task(self._run(handle, payload, authorize))
            handle._task.add_done_callback(lambda task: self._finished(handle.execution_id, task))
            return handle

    def _finished(self, identifier: str, task: asyncio.Task[ExecutionResult]) -> None:
        self._active.pop(identifier, None)
        if not task.cancelled():
            task.exception()

    async def _run(
        self, handle: ExecutionHandle, payload: bytes, authorize: Authorize | None
    ) -> ExecutionResult:
        marker = _current.set(handle)
        loop = asyncio.get_running_loop()

        def guard(deadline: float) -> None:
            if authorize is None:
                return

            async def check() -> None:
                assert authorize is not None
                await authorize()

            future = asyncio.run_coroutine_threadsafe(check(), loop)
            try:
                while True:
                    if handle._cancel.is_set() or time.monotonic() >= deadline:
                        raise TimeoutError
                    try:
                        future.result(timeout=min(0.05, max(0.001, deadline - time.monotonic())))
                        return
                    except TimeoutError:
                        if future.done():
                            raise
            finally:
                if not future.done():
                    future.cancel()

        def mark_started() -> None:
            loop.call_soon_threadsafe(handle._started.set)

        work = asyncio.create_task(
            asyncio.to_thread(
                self._execute,
                handle,
                payload,
                guard,
                mark_started,
            )
        )
        try:
            try:
                return await asyncio.shield(work)
            except asyncio.CancelledError:
                handle.cancel()
                await _settle(work)
                raise
        finally:
            _current.reset(marker)

    def _execute(
        self,
        handle: ExecutionHandle,
        payload: bytes,
        guard: Callable[[float], None],
        mark_started: Callable[[], None],
    ) -> ExecutionResult:
        start = time.monotonic()
        deadline = start + self.limits.startup_seconds
        stdout, stderr, wire = handle._stdout, handle._stderr, bytearray()
        artifacts: list[ExecutionArtifact] = []
        paths: set[str] = set()
        total = 0
        dispatched = started = ready = finished = complete = truncated = artifact_error = False
        exit_code = None
        stopped = 0
        reason: Literal["cancelled", "timed_out"] | None = None
        failure: str | None = None
        process: subprocess.Popen[bytes] | None = None
        pidfd: int | None = None
        child_fd: int | None = None
        pipe_fds: list[int] = []
        selector = selectors.DefaultSelector()
        pending = memoryview(payload)
        try:
            if handle._cancel.is_set():
                reason = "cancelled"
                raise InterruptedError
            guard(deadline)
            if handle._cancel.is_set():
                reason = "cancelled"
                raise InterruptedError
            status_read, status_write = os.pipe()
            pipe_fds.extend((status_read, status_write))
            gate_read, gate_write = os.pipe()
            pipe_fds.extend((gate_read, gate_write))
            with tempfile.TemporaryFile() as broker_file:
                broker_file.write(self._broker_code)
                broker_file.flush()
                broker_file.seek(0)
                broker_fd = broker_file.fileno()
                options: dict[str, Any] = (
                    {"user": self.uid, "group": self.gid, "extra_groups": []}
                    if os.getuid() == 0
                    else {}
                )
                process = subprocess.Popen(
                    self._arguments(broker_fd, status_write, gate_read),
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    cwd="/",
                    env={"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8"},
                    close_fds=True,
                    pass_fds=(broker_fd, status_write, gate_read),
                    start_new_session=True,
                    **options,
                )
                # Popen尚未poll/wait，退出子进程仍为僵尸，PID不会在取得pidfd前复用。
                pidfd = _syscall(434, process.pid, 0)
            for fd in (status_write, gate_read):
                os.close(fd)
                pipe_fds.remove(fd)
            child_fd = _child_pidfd(process, status_read, deadline)
            if handle._cancel.is_set():
                reason = "cancelled"
                raise InterruptedError
            os.write(gate_write, b"1")
            os.close(gate_write)
            pipe_fds.remove(gate_write)
            assert (
                process.stdin is not None
                and process.stdout is not None
                and process.stderr is not None
            )
            read_pipes = {
                process.stdout.fileno(): process.stdout,
                process.stderr.fileno(): process.stderr,
            }
            for pipe, kind, mask in (
                (process.stdin, "input", selectors.EVENT_WRITE),
                (process.stdout, "control", selectors.EVENT_READ),
                (process.stderr, "diagnostic", selectors.EVENT_READ),
            ):
                os.set_blocking(pipe.fileno(), False)
                selector.register(pipe, mask, kind)
            selector.register(pidfd, selectors.EVENT_READ, "exit")
            process_exited = False
            while True:
                now = time.monotonic()
                if reason is None and handle._cancel.is_set():
                    reason = "cancelled"
                    _kill(child_fd)
                if reason is None and now >= deadline:
                    reason = "timed_out"
                    _kill(child_fd)
                for key, _ in selector.select(0.05):
                    if key.data == "exit":
                        process_exited = True
                        selector.unregister(key.fileobj)
                        continue
                    if key.data == "input":
                        if reason is not None:
                            selector.unregister(key.fileobj)
                            process.stdin.close()
                            continue
                        if pending:
                            written = os.write(key.fd, pending[:65536])
                            pending = pending[written:]
                        if not pending:
                            selector.unregister(key.fileobj)
                        continue
                    chunk = os.read(key.fd, 65536)
                    if not chunk:
                        selector.unregister(key.fileobj)
                        read_pipes[key.fd].close()
                        continue
                    if key.data == "diagnostic":
                        continue
                    wire.extend(chunk)
                    if len(wire) > self.limits.file_bytes * 2 + 131072:
                        raise ValueError("control limit")
                    while b"\n" in wire:
                        encoded, _, rest = wire.partition(b"\n")
                        wire = bytearray(rest)
                        value = json.loads(encoded)
                        if not isinstance(value, dict) or value.get("v") != 1:
                            raise ValueError("protocol")
                        message_kind = value.get("kind")
                        if not isinstance(message_kind, str):
                            raise ValueError("kind")
                        kind = message_kind
                        if kind == "ready":
                            if ready or value.get("uid") != self.uid or pending:
                                raise ValueError("ready")
                            ready = True
                            guard(deadline)
                            if reason or handle._cancel.is_set():
                                reason = reason or "cancelled"
                                _kill(child_fd)
                                continue
                            dispatched = True
                            pending = memoryview(b"run\n")
                            selector.register(process.stdin, selectors.EVENT_WRITE, "input")
                            deadline = time.monotonic() + self.limits.wall_seconds
                        elif kind == "started":
                            if not dispatched or started:
                                raise ValueError("started")
                            started = True
                            mark_started()
                        elif kind in {"stdout", "stderr"}:
                            if not started or exit_code is not None:
                                raise ValueError("output order")
                            data = base64.b64decode(value["data"], validate=True)
                            if len(stdout) + len(stderr) + len(data) > self.limits.output_bytes:
                                raise ValueError("output limit")
                            with handle._output_lock:
                                (stdout if kind == "stdout" else stderr).extend(data)
                        elif kind == "exit":
                            if (
                                not started
                                or exit_code is not None
                                or type(value["code"]) is not int
                            ):
                                raise ValueError("exit")
                            exit_code = value["code"]
                            stopped = value["descendants_stopped"]
                            truncated = bool(value["output_limit"])
                        elif kind == "artifact":
                            if exit_code is None or finished:
                                raise ValueError("artifact order")
                            path = value["path"]
                            try:
                                if not Workspace._parts(path):
                                    raise ValueError("path")
                            except (ServiceError, ValueError):
                                artifact_error = True
                                continue
                            data = base64.b64decode(value["data"], validate=True)
                            mode = value["mode"]
                            total += len(data)
                            if (
                                path in paths
                                or len(paths) >= self.limits.artifact_files
                                or len(data) > self.limits.file_bytes
                                or total > self.limits.artifact_bytes
                                or type(mode) is not int
                                or not 0 <= mode <= 0o777
                            ):
                                raise ValueError("artifact")
                            paths.add(path)
                            artifacts.append(ExecutionArtifact(path, data, mode))
                        elif kind == "artifact_error":
                            artifact_error = True
                        elif kind == "finished":
                            if (
                                exit_code is None
                                or finished
                                or type(value["artifacts_complete"]) is not bool
                            ):
                                raise ValueError("finished")
                            finished = True
                            complete = value["artifacts_complete"] and not artifact_error
                        elif kind == "error":
                            failure = "execution_broker_failed"
                        else:
                            raise ValueError("kind")
                if process_exited and not any(
                    key.data == "control" for key in selector.get_map().values()
                ):
                    break
            code = process.wait()
            if code != 0 or not finished or wire:
                failure = failure or "execution_incomplete"
        except InterruptedError:
            pass
        except TimeoutError:
            reason = "cancelled" if handle._cancel.is_set() else "timed_out"
        except ServiceError as error:
            failure = error.code
        except Exception:
            failure = "execution_protocol_failed"
        finally:
            if process is not None:
                if child_fd is not None:
                    # 保留外层bubblewrap回收leader，宿主为subreaper时也不留下僵尸。
                    _kill(child_fd)
                elif pidfd is not None:
                    _kill(pidfd)
                else:
                    # 直接子进程尚未wait，数字PID此时仍归本Popen所有。
                    process.kill()
                if process.stdin is not None:
                    process.stdin.close()
                process.wait()
                for cleanup_pipe in (process.stdout, process.stderr):
                    if cleanup_pipe is not None:
                        cleanup_pipe.close()
            selector.close()
            if pidfd is not None:
                os.close(pidfd)
            if child_fd is not None:
                os.close(child_fd)
            for fd in pipe_fds:
                os.close(fd)
        status: ExecutionStatus
        if reason is not None:
            status = reason
            complete = False
            failure = "execution_cancelled" if reason == "cancelled" else "execution_timeout"
        elif failure:
            status = "uncertain" if dispatched else "setup_failed"
            complete = False
        elif truncated:
            status = "output_limit"
            failure = "execution_output_limit"
        elif exit_code == 0 and complete:
            status = "completed"
        else:
            status = "failed"
            failure = "execution_artifact_invalid" if not complete else "execution_exit_nonzero"
        return ExecutionResult(
            handle.execution_id,
            status,
            dispatched,
            started,
            exit_code,
            bytes(stdout),
            bytes(stderr),
            truncated,
            tuple(artifacts),
            complete,
            stopped,
            failure or ("execution_artifact_invalid" if not complete and reason is None else None),
            time.monotonic() - start,
        )

    async def close(self) -> None:
        if _inside():
            raise ServiceError("execution_close_reentrant", "执行资格回调不能关闭执行器", 409)
        async with self._lock:
            self._closed = True
            handles = list(self._active.values())
        for handle in handles:
            handle.cancel()

        async def drain() -> None:
            await asyncio.gather(*(handle.wait() for handle in handles), return_exceptions=True)

        await _settle(asyncio.create_task(drain()))

    async def __aenter__(self) -> LinuxSandboxRunner:
        return self

    async def __aexit__(self, *_arguments: object) -> None:
        await self.close()
