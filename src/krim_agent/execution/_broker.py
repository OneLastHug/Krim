"""仅在bubblewrap内运行的标准库监督器；命令不继承控制通道。"""

from __future__ import annotations

import base64
import ctypes
import json
import os
import resource
import selectors
import signal
import stat
import subprocess
import sys
import time
from typing import Any


def emit(kind: str, **values: Any) -> None:
    sys.stdout.write(json.dumps({"v": 1, "kind": kind, **values}, separators=(",", ":")) + "\n")
    sys.stdout.flush()


def parts(path: str) -> list[str]:
    values = path.split("/")
    if (
        not path
        or len(path.encode("utf-8")) > 512
        or "\\" in path
        or ":" in path
        or any(ord(char) < 32 for char in path)
        or any(value in {"", ".", ".."} for value in values)
    ):
        raise ValueError("path")
    return values


def descendants() -> list[int]:
    own = os.getpid()
    return [
        int(name) for name in os.listdir("/proc") if name.isdecimal() and int(name) not in {1, own}
    ]


def stop_descendants() -> int:
    killed: set[int] = set()
    deadline = time.monotonic() + 2
    while True:
        # 本监督器是namespace PID 1；主命令已wait，其余被收养的后代由这里回收。
        while True:
            try:
                if os.waitpid(-1, os.WNOHANG)[0] == 0:
                    break
            except ChildProcessError:
                break
        children = descendants()
        if not children:
            return len(killed)
        for identifier in children:
            try:
                os.kill(identifier, signal.SIGKILL)
                killed.add(identifier)
            except ProcessLookupError:
                pass
        if time.monotonic() >= deadline:
            raise RuntimeError("descendants")
        time.sleep(0.01)


def constrain(limits: dict[str, Any]) -> None:
    for setting, value in (
        (resource.RLIMIT_CORE, 0),
        (resource.RLIMIT_AS, limits["memory_bytes"]),
        (resource.RLIMIT_CPU, limits["cpu_seconds"]),
        (resource.RLIMIT_FSIZE, limits["file_bytes"]),
        (resource.RLIMIT_NOFILE, limits["open_files"]),
        (resource.RLIMIT_NPROC, limits["process_count"]),
    ):
        resource.setrlimit(setting, (value, value))


def capture_artifacts(limits: dict[str, Any]) -> bool:
    total = 0
    count = 0
    entries = 0
    complete = True
    pending = [("", 0)]
    while pending:
        directory, depth = pending.pop()
        with os.scandir("/workspace/" + directory) as iterator:
            for entry in iterator:
                entries += 1
                if entries > limits["artifact_files"] * 4 or depth > 16:
                    emit("artifact_error", code="execution_artifact_limit")
                    return False
                relative = f"{directory}/{entry.name}" if directory else entry.name
                try:
                    parts(relative)
                    info = entry.stat(follow_symlinks=False)
                    if stat.S_ISDIR(info.st_mode):
                        pending.append((relative, depth + 1))
                        continue
                    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                        raise ValueError("type")
                    count += 1
                    if count > limits["artifact_files"] or info.st_size > limits["file_bytes"]:
                        raise ValueError("limit")
                    if total + info.st_size > limits["artifact_bytes"]:
                        emit("artifact_error", code="execution_artifact_limit")
                        return False
                    fd = os.open(entry.path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
                    try:
                        before = os.fstat(fd)
                        if (before.st_dev, before.st_ino, before.st_nlink) != (
                            info.st_dev,
                            info.st_ino,
                            1,
                        ):
                            raise ValueError("changed")
                        data = bytearray()
                        while len(data) <= limits["file_bytes"]:
                            piece = os.read(fd, min(65536, limits["file_bytes"] + 1 - len(data)))
                            if not piece:
                                break
                            data.extend(piece)
                        after = os.fstat(fd)
                        if len(data) != info.st_size or (
                            before.st_mtime_ns,
                            before.st_ctime_ns,
                        ) != (after.st_mtime_ns, after.st_ctime_ns):
                            raise ValueError("changed")
                    finally:
                        os.close(fd)
                    mode = stat.S_IMODE(info.st_mode)
                    if mode & ~0o777:
                        raise ValueError("mode")
                    total += len(data)
                    emit("artifact", path=relative, data=base64.b64encode(data).decode(), mode=mode)
                except (OSError, ValueError, UnicodeError):
                    complete = False
                    emit("artifact_error", code="execution_artifact_invalid")
    return complete


def main() -> None:
    # 同UID命令也不能通过/proc、ptrace或process_vm取得监督器的控制描述符。
    libc = ctypes.CDLL(None, use_errno=True)
    if os.getpid() != 1 or libc.prctl(4, 0, 0, 0, 0) != 0 or libc.prctl(38, 1, 0, 0, 0) != 0:
        raise RuntimeError("protection")
    raw = sys.stdin.buffer.readline(190000000)
    payload = json.loads(raw)
    limits = payload["limits"]
    for directory in payload["directories"]:
        os.makedirs("/workspace/" + "/".join(parts(directory)), mode=0o700, exist_ok=True)
    for entry in payload["files"]:
        path = "/workspace/" + "/".join(parts(entry["path"]))
        os.makedirs(os.path.dirname(path), mode=0o700, exist_ok=True)
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        try:
            data = memoryview(base64.b64decode(entry["data"], validate=True))
            while data:
                written = os.write(fd, data)
                if written <= 0:
                    raise OSError("write")
                data = data[written:]
            os.fchmod(fd, entry["mode"])
        finally:
            os.close(fd)
    cwd = payload["cwd"]
    if cwd != ".":
        parts(cwd)
    os.chdir("/workspace" if cwd == "." else "/workspace/" + cwd)
    os.environ["PWD"] = os.getcwd()
    os.makedirs("/tmp/home", mode=0o700, exist_ok=True)
    emit("ready", uid=os.getuid())
    if sys.stdin.buffer.readline(64) != b"run\n":
        return
    process = subprocess.Popen(
        payload["argv"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        close_fds=True,
        start_new_session=True,
        preexec_fn=lambda: constrain(limits),
    )
    emit("started")
    assert process.stdout is not None and process.stderr is not None
    pipes = {process.stdout.fileno(): process.stdout, process.stderr.fileno(): process.stderr}
    selector = selectors.DefaultSelector()
    for pipe, kind in ((process.stdout, "stdout"), (process.stderr, "stderr")):
        os.set_blocking(pipe.fileno(), False)
        selector.register(pipe, selectors.EVENT_READ, kind)
    observed = 0
    limited = False
    cleaned = False
    stopped = 0
    while selector.get_map() or process.poll() is None:
        for key, _ in selector.select(0.02):
            chunk = os.read(key.fd, 8192)
            if not chunk:
                selector.unregister(key.fileobj)
                pipes[key.fd].close()
                continue
            room = max(0, limits["output_bytes"] - observed)
            observed += len(chunk)
            if room:
                emit(key.data, data=base64.b64encode(chunk[:room]).decode())
            if observed > limits["output_bytes"] and not limited:
                limited = True
                process.kill()
        if process.poll() is not None and not cleaned:
            stopped = stop_descendants()
            cleaned = True
    selector.close()
    code = process.wait()
    emit("exit", code=code, output_limit=limited, descendants_stopped=stopped)
    complete = capture_artifacts(limits)
    emit("finished", artifacts_complete=complete)


if __name__ == "__main__":
    try:
        main()
    except BaseException:
        # 控制通道只输出固定错误，不回显输入、宿主路径或解释器异常正文。
        emit("error", code="execution_broker_failed")
        raise SystemExit(1) from None
