"""rootless单机Runner：仅拥有精确进程/流/叶组，不拥有业务调度。"""

import asyncio
import base64
import fcntl
import hashlib
import json
import os
import signal
import sys
import time
import uuid
from dataclasses import asdict
from pathlib import Path

from krim_agent.contracts import ServiceError
from krim_agent.runtime.contracts import settled

from . import _broker, seccomp
from .cgroups import Leaf, recover
from .types import Profile, Receipt, Request, path_parts


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def fds():
    result = {}
    for fd in os.listdir("/proc/self/fd"):
        try:
            result[fd] = os.readlink("/proc/self/fd/" + fd)
        except FileNotFoundError:
            pass
    return result


class SandboxRunner:
    def __init__(self, profile: Profile, *, hook=None, fence=lambda: True):
        self.profile = profile
        self.hook = hook
        self.fence = fence
        self.jobs = set()
        self._resources = {}
        self._cancelled = set()
        self._close_task = None
        self.closed = False
        self.closing = False
        self._base = self._validate_base()
        self._seal = self._token()
        profile.material.mkdir(parents=True, exist_ok=True, mode=0o700)
        self._owner = (profile.material / "owner.lock").open("a+b")
        try:
            fcntl.flock(self._owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.recovered = recover(profile.cgroup_root, profile.material / "tickets")
        except BaseException:
            self._owner.close()
            raise ServiceError("sandbox_cleanup_required") from None

    def _validate_base(self):
        try:
            q = json.loads(self.profile.base.read_text())
            if (
                q["status"] != "BASE-QUALIFIED"
                or q["rootfs"]["path"] != str(self.profile.rootfs)
                or os.getuid() != q["identity"]["uid"]
                or os.geteuid() == 0
                or not str(self.profile.cgroup_root).startswith("/sys/fs/cgroup/")
                or str(self.profile.cgroup_root)
                != q["cgroup_pidfd"]["identity"]["path"].rsplit("/", 1)[0]
                or sha(self.profile.binary) != q["namespace"]["sha256"]
            ):
                raise ValueError
            e = self.profile.base.parents[2]
            root_stat = self.profile.cgroup_root.stat()
            if (root_stat.st_dev, root_stat.st_ino) != (
                q["cgroup_pidfd"]["identity"]["root_dev"],
                q["cgroup_pidfd"]["identity"]["root_inode"],
            ):
                raise ValueError
            if (
                self.profile.material.resolve() != self.profile.material
                or self.profile.evidence.resolve() != self.profile.evidence
                or not self.profile.material.is_relative_to(e / "materials/d1-os")
                or not self.profile.evidence.is_relative_to(e / "evidence/d1-os")
            ):
                raise ValueError
            return q
        except (OSError, ValueError, KeyError, TypeError):
            raise ServiceError("sandbox_unavailable") from None

    def _token(self):
        manifest_path = Path(self._base["rootfs"]["manifest"])
        if sha(manifest_path) != self._base["rootfs"]["manifest_sha256"]:
            raise ServiceError("sandbox_unavailable")
        manifest = json.loads(manifest_path.read_text())
        # 资格引用必须匹配实际独立rootfs；不因旧BASE文件名就信任被替换字节。
        for name, value in manifest["files"].items():
            p = self.profile.rootfs / name
            if p.is_symlink() or not p.is_file() or sha(p) != value["sha256"]:
                raise ServiceError("sandbox_unavailable")
        methods = tuple(
            id(getattr(getattr(self, n), "__func__", getattr(self, n)))
            for n in [
                "run",
                "close",
                "_close",
                "_execute",
                "_monitor",
                "_cleanup",
                "_read",
                "_phase",
            ]
        )
        return (
            repr(asdict(self.profile)),
            (self.profile.rootfs.stat().st_dev, self.profile.rootfs.stat().st_ino),
            sha(self.profile.binary),
            sha(self.profile.base),
            tuple(
                sha(Path(__file__).with_name(n + ".py"))
                for n in ["linux", "types", "cgroups", "seccomp", "_broker"]
            ),
            hashlib.sha256(seccomp.program()).hexdigest(),
            methods,
            id(self.fence),
            id(self.hook),
        )

    def _assert(self):
        try:
            current = self._token()
        except (ServiceError, OSError, ValueError):
            raise ServiceError("target_binding_changed") from None
        if current != self._seal or self.fence() is not True:
            raise ServiceError("target_binding_changed")

    async def _phase(self, phase, record):
        record("phase", phase=phase)
        if self.hook is not None:
            await self.hook(phase)
        self._assert()

    def _cancel(self, task):
        if not task.done() and task not in self._cancelled:
            self._cancelled.add(task)
            task.cancel()

    async def run(self, request: Request):
        if self.closing:
            raise ServiceError("closed")
        self._assert()
        request = Request(tuple(request.argv), dict(request.files), request.cwd)
        task = asyncio.create_task(self._execute(request))
        self.jobs.add(task)
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            self._cancel(task)
            try:
                await settled(task)
            except asyncio.CancelledError:
                pass
            raise
        finally:
            state = self._resources.get(task)
            if task.done() and (state is None or state["clean"]):
                self.jobs.discard(task)
                self._cancelled.discard(task)
                self._resources.pop(task, None)

    async def close(self):
        if self._close_task is None or (self._close_task.done() and not self.closed):
            self.closing = True
            self._close_task = asyncio.create_task(self._close())
        await settled(self._close_task)

    async def _close(self):
        for task in list(self.jobs):
            self._cancel(task)
        await asyncio.gather(*list(self.jobs), return_exceptions=True)
        failed = False
        for task in list(self.jobs):
            state = self._resources.get(task)
            if state is not None and not state["clean"]:
                try:
                    await self._cleanup(
                        state["proc"],
                        state["pidfd"],
                        state["leaf"],
                        state["monitor"],
                        state["record"],
                        state=state,
                    )
                except Exception:
                    failed = True
                    continue
            if task.done():
                self.jobs.discard(task)
                self._cancelled.discard(task)
                self._resources.pop(task, None)
        if failed or self.jobs or self._resources:
            raise ServiceError("sandbox_cleanup_failed")
        self._owner.close()
        self.closed = True

    async def _read(self, proc):
        raw = await proc.stdout.readline()
        if not raw:
            raise ServiceError("sandbox_setup")
        try:
            return json.loads(raw)
        except (ValueError, UnicodeError):
            raise ServiceError("sandbox_setup") from None

    async def _monitor(self, leaf):
        while True:
            c = leaf.counters()
            if int(c["cpu"].get("usage_usec", "0")) >= int(self.profile.cpu_seconds * 1000000):
                return c
            if int(c["memory"].get("oom_kill", "0")) or int(c["pids"].get("max", "0")):
                return c
            await asyncio.sleep(0.05)

    async def _cleanup(self, proc, pidfd, leaf, monitor, record, *, state=None):
        state = {} if state is None else state
        if state.get("clean"):
            return
        if monitor is not None and not state.get("monitor_joined"):
            monitor.cancel()
            await asyncio.gather(monitor, return_exceptions=True)
            state["monitor_joined"] = True
        if proc is not None and not state.get("waited"):
            if proc.returncode is None:
                if leaf is not None and leaf.path.exists() and str(proc.pid) in leaf.members():
                    (leaf.path / "cgroup.kill").write_text("1")
                    record("cleanup_kill")
                elif pidfd is not None:
                    try:
                        signal.pidfd_send_signal(pidfd, signal.SIGKILL)
                    except ProcessLookupError:
                        pass  # 同一自有进程可能已退出；仍必须communicate/wait。
            try:
                async with asyncio.timeout(10):
                    out, err = await proc.communicate()
                if proc.returncode is None:
                    raise ServiceError("sandbox_cleanup_failed")
                state["waited"] = True
                record(
                    "process_waited",
                    pid=proc.pid,
                    rc=proc.returncode,
                    remaining_stdout=len(out),
                    stderr=err.decode(errors="replace"),
                )
            except TimeoutError:
                raise ServiceError("sandbox_cleanup_failed") from None
        if pidfd is not None and not state.get("pidfd_closed"):
            os.close(pidfd)
            state["pidfd_closed"] = True
            record("pidfd_closed")
        if leaf is not None and not state.get("leaf_finished"):
            await leaf.finish()
            state["leaf_finished"] = True
        if state.get("receipt") is not None:
            state["receipt"]["cleanup"] = "clean"
            await asyncio.to_thread(
                (state["folder"] / "receipt.json").write_text, json.dumps(state["receipt"])
            )
            record("cleanup_recovered")
        state["clean"] = True
        record("cleanup_complete")

    async def _execute(self, request):
        folder = self.profile.evidence / ("job-" + uuid.uuid4().hex)
        folder.mkdir(parents=True)

        def record(kind, **data):
            with (folder / "resources.jsonl").open("a") as f:
                f.write(
                    json.dumps({"kind": kind, "monotonic_ns": time.monotonic_ns(), **data}) + "\n"
                )

        material = self.profile.material / folder.name
        material.mkdir(parents=True, mode=0o700)
        inputs = material / "input"
        inputs.mkdir()
        denied_fd = material / "pid1-fd"
        denied_fd.mkdir(mode=0o000)
        for name, data in request.files.items():
            p = inputs.joinpath(*path_parts(name))
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_bytes(data)
            p.chmod(0o444)
        broker = material / "broker.py"
        broker.write_bytes(Path(_broker.__file__).read_bytes())
        broker.chmod(0o444)
        filt = material / "filter.bin"
        filt.write_bytes(seccomp.program())
        filt.chmod(0o444)
        req = material / "request.json"
        req.write_text(
            json.dumps({"argv": request.argv, "cwd": request.cwd, "output": self.profile.output})
        )
        req.chmod(0o444)
        asset_hashes = {p: sha(p) for p in [broker, filt, req]}
        input_hashes = {
            name: hashlib.sha256(data).hexdigest() for name, data in request.files.items()
        }

        async def phase(name, record):
            await self._phase(name, record)
            if leaf is not None:
                leaf.check()
            try:
                if any(sha(p) != h for p, h in asset_hashes.items()):
                    raise ValueError
                if {
                    k: hashlib.sha256(v).hexdigest() for k, v in _broker.collect(inputs).items()
                } != input_hashes:
                    raise ValueError
            except (ValueError, OSError):
                raise ServiceError("target_binding_changed") from None

        leaf = None
        proc = None
        pidfd = None
        monitor = None
        controls = set()
        stdout = bytearray()
        stderr = bytearray()
        files = {}
        rc = None
        code = None
        output_complete = False
        gate_sent = False
        status = "unknown"
        state = {"clean": False, "folder": folder}
        self._resources[asyncio.current_task()] = state
        try:
            self._assert()
            leaf = Leaf(self.profile.cgroup_root, self.profile.material / "tickets", record)
            leaf.limits(self.profile)
            await phase("allocated", record)
            bwrap = [
                str(self.profile.binary),
                "--unshare-all",
                "--uid",
                "0",
                "--gid",
                "0",
                "--cap-drop",
                "ALL",
                "--as-pid-1",
                "--new-session",
                "--die-with-parent",
                "--tmpfs",
                "/",
                "--ro-bind",
                str(self.profile.rootfs / "opt"),
                "/opt",
                "--ro-bind",
                str(self.profile.rootfs / "lib"),
                "/lib",
                "--ro-bind",
                str(self.profile.rootfs / "lib64"),
                "/lib64",
                "--ro-bind",
                str(self.profile.rootfs / "usr"),
                "/usr",
                "--ro-bind",
                str(inputs),
                "/input",
                "--ro-bind",
                str(broker),
                "/broker.py",
                "--ro-bind",
                str(filt),
                "/filter.bin",
                "--ro-bind",
                str(req),
                "/request.json",
                "--proc",
                "/proc",
                "--ro-bind",
                str(denied_fd),
                "/proc/1/fd",
                "--remount-ro",
                "/proc",
                "--dev",
                "/dev",
                "--size",
                "4194304",
                "--tmpfs",
                "/tmp",
                "--size",
                "16777216",
                "--tmpfs",
                "/work",
                "--clearenv",
                "--setenv",
                "LD_LIBRARY_PATH",
                "/opt/python/lib",
                "--setenv",
                "PATH",
                "/opt/python/bin",
                "--remount-ro",
                "/",
                "--chdir",
                "/work",
                "--",
                "/opt/python/bin/python3.12",
                "-I",
                "-B",
                "/broker.py",
            ]
            launcher = "import os,sys;os.write(1,b'{\\\"type\\\":\\\"launcher_ready\\\"}\\n');assert os.read(0,1)==b'!';os.execve(sys.argv[1],sys.argv[1:],{'PATH':'/usr/local/bin:/usr/bin:/bin','LANG':'C.UTF-8'})"
            argv = [sys.executable, "-I", "-B", "-c", launcher, *bwrap]
            env = {
                "PATH": "/usr/local/bin:/usr/bin:/bin",
                "LD_LIBRARY_PATH": str(Path(sys.base_prefix) / "lib"),
                "LANG": "C.UTF-8",
            }
            record("command", argv=argv, cwd=str(Path.cwd()), env=env)
            spawn = asyncio.create_task(
                asyncio.create_subprocess_exec(
                    *argv,
                    stdin=asyncio.subprocess.PIPE,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    env=env,
                )
            )
            try:
                proc = await asyncio.shield(spawn)
            except asyncio.CancelledError:
                proc = await spawn
                raise
            pidfd = os.pidfd_open(proc.pid)
            record("process_created", pid=proc.pid, pidfd=pidfd)
            async with asyncio.timeout(5):
                message = await self._read(proc)
            if message["type"] != "launcher_ready":
                raise ServiceError("sandbox_setup")
            leaf.join(proc.pid)
            await phase("launcher", record)
            proc.stdin.write(b"!")
            await proc.stdin.drain()
            async with asyncio.timeout(5):
                ready = await self._read(proc)
            if ready["type"] != "ready":
                raise ServiceError("sandbox_setup")
            controls = leaf.members()
            record("broker_ready", controls=sorted(controls), message=ready)
            if (
                ready["pid"] != 1
                or ready["dumpable"] != 0
                or "NoNewPrivs:\t1" not in ready["status"]
                or "Seccomp:\t2" not in ready["status"]
            ):
                raise ServiceError("sandbox_setup")
            await phase("ready", record)
            self._assert()
            proc.stdin.write(b"gate\n")
            gate_sent = True
            record("gate_opened")
            await proc.stdin.drain()
            monitor = asyncio.create_task(self._monitor(leaf))

            async def receive():
                nonlocal rc, code
                while True:
                    message = await self._read(proc)
                    if message["type"] == "output":
                        data = base64.b64decode(message["data"], validate=True)
                        (stdout if message["stream"] == "stdout" else stderr).extend(data)
                        if len(stdout) + len(stderr) == len(data):
                            await phase("payload_output", record)
                        if len(stdout) + len(stderr) > self.profile.output:
                            raise ServiceError("sandbox_limit")
                    elif message["type"] == "exit":
                        rc = message["rc"]
                        code = message["code"]
                        record("payload_exit", message=message)
                    elif message["type"] == "quiescent":
                        return
                    else:
                        raise ServiceError("sandbox_setup")

            receive_task = asyncio.create_task(receive())
            try:
                async with asyncio.timeout(self.profile.wall):
                    done, _ = await asyncio.wait(
                        [receive_task, monitor], return_when=asyncio.FIRST_COMPLETED
                    )
                    if monitor in done:
                        record("limit", counters=monitor.result())
                        raise ServiceError("sandbox_limit")
                    await receive_task
            finally:
                if not receive_task.done():
                    receive_task.cancel()
                await asyncio.gather(receive_task, return_exceptions=True)
            if leaf.members() != controls:
                raise ServiceError("sandbox_cleanup_failed")
            await phase("quiescent", record)
            self._assert()
            proc.stdin.write(b"export\n")
            await proc.stdin.drain()
            name = None
            expected = None
            buffer = bytearray()
            async with asyncio.timeout(5):
                while True:
                    m = await self._read(proc)
                    if m["type"] == "file_start":
                        name = m["name"]
                        path_parts(name)
                        if name in files or len(files) >= 32 or not 0 <= m["size"] <= 1048576:
                            raise ServiceError("artifact_limit")
                        expected = m
                        buffer = bytearray()
                    elif m["type"] == "chunk":
                        buffer.extend(base64.b64decode(m["data"], validate=True))
                        if expected is None or len(buffer) > expected["size"]:
                            raise ServiceError("artifact_invalid")
                    elif m["type"] == "file_end":
                        if (
                            expected is None
                            or len(buffer) != expected["size"]
                            or hashlib.sha256(buffer).hexdigest() != expected["sha256"]
                        ):
                            raise ServiceError("artifact_invalid")
                        files[name] = bytes(buffer)
                        expected = None
                        if sum(map(len, files.values())) > 8388608:
                            raise ServiceError("artifact_limit")
                        await phase("export_chunk", record)
                    elif m["type"] == "export_error":
                        code = m["code"]
                        break
                    elif m["type"] == "export_end":
                        if expected is not None:
                            raise ServiceError("artifact_invalid")
                        output_complete = code is None
                        break
                    else:
                        raise ServiceError("sandbox_setup")
            await phase("export_end", record)
            counters = leaf.counters()
            record("counters", counters=counters)
            if int(counters["memory"].get("oom_kill", "0")) or int(
                counters["pids"].get("max", "0")
            ):
                code = "sandbox_limit"
                output_complete = False
        except asyncio.CancelledError:
            status = "cancelled"
            code = "cancelled"
            record("cancelled")
            raise
        except TimeoutError:
            status = "timeout"
            code = "sandbox_limit"
            record("timeout")
            if leaf is not None and leaf.path.exists():
                record("limit", counters=leaf.counters())
        except ServiceError as error:
            status = "failed"
            code = error.code
            if leaf is not None and leaf.path.exists():
                counters = leaf.counters()
                record("failure_counters", counters=counters)
                if int(counters["memory"].get("oom_kill", "0")) or int(
                    counters["pids"].get("max", "0")
                ):
                    code = "sandbox_limit"
            record("failed", code=code)
            if code in {"target_binding_changed", "sandbox_cleanup_failed"}:
                raise
        finally:
            cleanup = "unknown"
            state.update(proc=proc, pidfd=pidfd, leaf=leaf, monitor=monitor, record=record)
            try:
                await settled(self._cleanup(proc, pidfd, leaf, monitor, record, state=state))
                cleanup = "clean"
            finally:
                # 取消和丢回程也保留已观测输出；不拿bwrap退出码补造payload退出码。
                observed = (
                    True if rc is not None or stdout or stderr else None if gate_sent else False
                )
                state["receipt"] = {
                    "status": status
                    if status != "unknown"
                    else "exited"
                    if rc is not None
                    else "unknown",
                    "gate_sent": gate_sent,
                    "payload_observed": observed,
                    "exit_code": rc,
                    "stdout": base64.b64encode(stdout).decode(),
                    "stderr": base64.b64encode(stderr).decode(),
                    "files": {p: hashlib.sha256(b).hexdigest() for p, b in files.items()},
                    "code": code,
                    "output_complete": output_complete,
                    "cleanup": cleanup,
                }
                (folder / "receipt.json").write_text(json.dumps(state["receipt"]))
        result = Receipt(
            rc, bytes(stdout), bytes(stderr), files, output_complete, "clean", code, str(folder)
        )
        return result
