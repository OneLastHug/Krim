"""namespace PID1的可信stdlib监督器；自身文件是包资源，不导入应用。"""

import base64
import ctypes
import hashlib
import json
import os
import resource
import select
import stat
import sys
import unicodedata
from pathlib import Path


def valid_name(name):
    parts = name.split("/")
    if (
        len(name.encode()) > 512
        or len(parts) > 16
        or "\\" in name
        or unicodedata.normalize("NFC", name) != name
        or any(p in {"", ".", "..", ".git", ".ssh", ".aws", ".codex", ".env"} for p in parts)
        or any(ord(c) < 32 or ord(c) == 127 for c in name)
    ):
        raise ValueError("artifact_invalid")


def collect(root):
    """同一普通文件检查供broker与父端定向测试调用；拒绝后才可能open。"""
    result = {}
    total = 0
    entries = 0

    def walk(fd, prefix, depth):
        nonlocal total, entries
        if depth > 16:
            raise ValueError("artifact_limit")
        for name in sorted(os.listdir(fd)):
            path = prefix + name
            valid_name(path)
            entries += 1
            if entries > 128:
                raise ValueError("artifact_limit")
            st = os.stat(name, dir_fd=fd, follow_symlinks=False)
            if stat.S_ISDIR(st.st_mode):
                child = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
                try:
                    walk(child, path + "/", depth + 1)
                finally:
                    os.close(child)
            else:
                if not stat.S_ISREG(st.st_mode) or st.st_nlink != 1:
                    raise ValueError("artifact_invalid")
                total += st.st_size
                if st.st_size > 1048576 or total > 8388608 or len(result) >= 32:
                    raise ValueError("artifact_limit")
                child = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd)
                try:
                    now = os.fstat(child)
                    if (st.st_dev, st.st_ino, st.st_size) != (now.st_dev, now.st_ino, now.st_size):
                        raise ValueError("artifact_invalid")
                    with os.fdopen(child, "rb", closefd=False) as f:
                        data = f.read(1048577)
                    if len(data) != st.st_size:
                        raise ValueError("artifact_invalid")
                    result[path] = data
                finally:
                    os.close(child)

    fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        walk(fd, "", 0)
    finally:
        os.close(fd)
    return result


def install_filter():
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(4, 0, 0, 0, 0) != 0 or libc.prctl(38, 1, 0, 0, 0) != 0:
        raise RuntimeError("prctl")
    raw = Path("/filter.bin").read_bytes()

    class Insn(ctypes.Structure):
        _fields_ = [
            ("code", ctypes.c_ushort),
            ("jt", ctypes.c_ubyte),
            ("jf", ctypes.c_ubyte),
            ("k", ctypes.c_uint32),
        ]

    class Prog(ctypes.Structure):
        _fields_ = [("len", ctypes.c_ushort), ("filter", ctypes.POINTER(Insn))]

    instructions = (Insn * (len(raw) // 8)).from_buffer_copy(raw)
    prog = Prog(len(instructions), instructions)
    if libc.prctl(22, 2, ctypes.byref(prog), 0, 0) != 0:
        raise RuntimeError("seccomp")


def send(kind, **data):
    print(json.dumps({"type": kind, **data}, separators=(",", ":")), flush=True)


def reap():
    statuses = {}
    try:
        os.kill(-1, 9)
    except ProcessLookupError:
        pass
    while True:
        try:
            pid, status = os.waitpid(-1, 0)
            statuses[pid] = status
        except ChildProcessError:
            return statuses
        except InterruptedError:
            continue


def main():
    if os.getpid() != 1:
        raise RuntimeError("broker must be namespace PID1")
    request = json.loads(Path("/request.json").read_text())
    for name, data in collect("/input").items():
        p = Path("/work") / name
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)
    os.chdir("/work" if request["cwd"] == "." else "/work/" + request["cwd"])
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    resource.setrlimit(resource.RLIMIT_FSIZE, (1048576, 1048576))
    resource.setrlimit(resource.RLIMIT_NOFILE, (64, 64))
    resource.setrlimit(resource.RLIMIT_AS, (268435456, 268435456))
    resource.setrlimit(resource.RLIMIT_NPROC, (64, 64))
    install_filter()
    send(
        "ready",
        pid=os.getpid(),
        dumpable=ctypes.CDLL(None).prctl(3),
        status=Path("/proc/self/status").read_text(),
        mountinfo=Path("/proc/self/mountinfo").read_text(),
    )
    if sys.stdin.buffer.readline(128) != b"gate\n":
        return
    out_r, out_w = os.pipe()
    err_r, err_w = os.pipe()
    child = os.fork()
    if child == 0:
        os.dup2(os.open("/dev/null", os.O_RDONLY), 0)
        os.dup2(out_w, 1)
        os.dup2(err_w, 2)
        os.closerange(3, 1024)
        os.execve(
            "/opt/python/bin/python3.12",
            ["/opt/python/bin/python3.12", "-I", "-B", *request["argv"]],
            {"PATH": "/opt/python/bin", "LD_LIBRARY_PATH": "/opt/python/lib", "LANG": "C.UTF-8"},
        )
    os.close(out_w)
    os.close(err_w)
    streams = {out_r: "stdout", err_r: "stderr"}
    total = 0
    code = None
    rc = None
    main_status = None
    child_done = False
    try:
        while streams or not child_done:
            if not child_done:
                try:
                    pid, status = os.waitpid(child, os.WNOHANG)
                except ChildProcessError:
                    child_done = True
                    pid = 0
                except InterruptedError:
                    continue
                if pid:
                    main_status = status
                    child_done = True
                    rc = os.waitstatus_to_exitcode(status)
                    reap()
            ready, _, _ = select.select(list(streams), [], [], 0.02)
            for fd in ready:
                data = os.read(fd, 4096)
                if not data:
                    os.close(fd)
                    del streams[fd]
                    continue
                remaining = max(0, request["output"] - total)
                total += len(data)
                if remaining:
                    send(
                        "output",
                        stream=streams[fd],
                        data=base64.b64encode(data[:remaining]).decode(),
                    )
                if total > request["output"]:
                    code = "sandbox_limit"
                    statuses = reap()
                    if not child_done:
                        main_status = statuses.get(child)
                        rc = None if main_status is None else os.waitstatus_to_exitcode(main_status)
                        child_done = True
        send(
            "exit",
            rc=rc,
            code=code,
            observed_output=total,
            wait_status=main_status,
            waited_pid=child if main_status is not None else None,
        )
    finally:
        reap()
        for fd in streams:
            os.close(fd)
    send("quiescent")
    if sys.stdin.buffer.readline(128) != b"export\n":
        return
    try:
        files = collect("/work")
        for name, data in files.items():
            send("file_start", name=name, size=len(data), sha256=hashlib.sha256(data).hexdigest())
            for offset in range(0, len(data), 16384):
                send("chunk", data=base64.b64encode(data[offset : offset + 16384]).decode())
            send("file_end")
        send("export_end")
    except ValueError as error:
        send("export_error", code=str(error))


if __name__ == "__main__":
    main()
