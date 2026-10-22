"""四个D1_OS节点；全部payload只通过真实产品Runner。"""

import ast
import asyncio
import base64
import json
import os
import signal
import socket
import sys
import time
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest
from fixtures.sandbox_cases import CASES, payload

from krim_agent.contracts import ServiceError
from krim_agent.execution import _broker
from krim_agent.execution.linux import SandboxRunner, fds
from krim_agent.execution.types import Profile, Request

E = Path(
    os.environ.get(
        "KRIM_D1_REPAIR_ROOT",
        "/artifacts/krim-agent-core-20261019/acceptance/ac-m2-vm-base-20261019T164700",
    )
)


def profile(**updates):
    base = Profile(
        Path(
            json.loads((E / "evidence/prep-base/BASE-QUALIFIED.json").read_text())["rootfs"]["path"]
        ),
        E / "evidence/prep-base/BASE-QUALIFIED.json",
        Path("/sys/fs/cgroup/system.slice/krim-task.service"),
        E / "materials/d1-os",
        Path(os.environ["KRIM_D1_EVIDENCE"]),
    )
    return replace(base, **updates)


def events(root):
    return [
        json.loads(l)
        for f in root.glob("job-*/resources.jsonl")
        for l in f.read_text().splitlines()
    ]


def assert_clean(root, baseline):
    data = events(root)
    made = [d["pid"] for d in data if d["kind"] == "process_created"]
    waited = [d["pid"] for d in data if d["kind"] == "process_waited"]
    assert made == waited
    assert sum(d["kind"] == "pidfd_closed" for d in data) == len(made)
    leaves = [d["name"] for d in data if d["kind"] == "leaf_created"]
    assert all(
        not (Path("/sys/fs/cgroup/system.slice/krim-task.service") / name).exists()
        for name in leaves
    )
    assert sum(d["kind"] == "leaf_removed" for d in data) == len(leaves)
    assert fds() == baseline


@pytest.mark.parametrize("case", CASES)
async def test_real_negative_isolation_and_limits(case):
    before = fds()
    changes = {}
    case_start = time.monotonic_ns()
    if case == "memory":
        changes["memory"] = 67108864
    if case == "wall":
        changes["wall"] = 0.5
    runner = SandboxRunner(profile(**changes))
    try:
        result = await runner.run(Request(("-c", payload(case))))
        if case == "normal":
            assert result.exit_code == 0 and result.stdout == b"5\n"
        elif case in {"cpu", "memory", "pids", "output", "wall"}:
            assert result.code == "sandbox_limit", (case, result)
            assert not result.output_complete
            if case != "output":
                assert b"ENTER" in result.stdout
            if case == "output":
                assert len(result.stdout) == 262144
            counter = [
                x
                for x in events(Path(result.evidence).parent)
                if x["monotonic_ns"] >= case_start
                and x["kind"] in {"limit", "failure_counters", "counters"}
            ]
            assert counter
            if case == "memory":
                assert any(int(x["counters"]["memory"]["oom_kill"]) > 0 for x in counter)
            if case == "pids":
                assert any(int(x["counters"]["pids"]["max"]) > 0 for x in counter)
            if case == "cpu":
                assert any(int(x["counters"]["cpu"]["usage_usec"]) >= 2000000 for x in counter)
        elif case == "i386":
            assert result.exit_code == -signal.SIGSYS and b"BEFORE_BAD_ARCH" in result.stdout
        elif case in {"fifo_export", "symlink_export", "hardlink_export", "secret_export"}:
            assert (
                result.exit_code == 0
                and result.code == "artifact_invalid"
                and not result.output_complete
            )
        elif case == "workfs":
            assert (
                result.exit_code == 0
                and b"SPACE_LIMIT 28" in result.stdout
                and result.code == "artifact_limit"
            )
        else:
            assert result.exit_code == 0 and result.code is None, (case, result)
        assert result.cleanup == "clean"
    finally:
        await runner.close()
    assert not runner.jobs and runner.closed
    assert_clean(profile().evidence, before)


CANCEL_PHASES = [
    "allocated",
    "launcher",
    "ready",
    "payload_output",
    "quiescent",
    "export_chunk",
    "export_end",
]


@pytest.mark.parametrize("phase", CANCEL_PHASES)
async def test_cancel_reaps_descendants_and_closes_owned_resources(phase):
    before = fds()
    case_start = time.monotonic_ns()
    arrived = asyncio.Event()
    release = asyncio.Event()

    async def hook(current):
        if current == phase:
            arrived.set()
            await release.wait()

    runner = SandboxRunner(profile(), hook=hook)
    code = "import os,time\nopen('out','w').write('kept')\nprint('ENTER',flush=True)\nif os.fork()==0:os.setsid();time.sleep(60)"
    task = asyncio.create_task(runner.run(Request(("-c", code))))
    try:
        await asyncio.wait_for(arrived.wait(), 10)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        await runner.close()
        await runner.close()
        assert runner.closed and not runner.jobs
        # 用同一参数的资源事件定位对应job，避免跨参数累计。
        current_jobs = [
            f.parent
            for f in profile().evidence.glob("job-*/resources.jsonl")
            if any(json.loads(l)["monotonic_ns"] >= case_start for l in f.read_text().splitlines())
        ]
        assert len(current_jobs) == 1
        raw = json.loads((current_jobs[0] / "receipt.json").read_text())
        assert raw["status"] == "cancelled" and raw["cleanup"] == "clean"
        if phase in {"allocated", "launcher", "ready"}:
            assert raw["payload_observed"] is False and raw["exit_code"] is None
        else:
            assert raw["payload_observed"] is True and b"ENTER" in base64.b64decode(raw["stdout"])
            if phase == "payload_output":
                assert raw["exit_code"] is None
        assert raw["output_complete"] == (phase == "export_end")
        gate = [
            e
            for e in events(profile().evidence)
            if e["kind"] == "gate_opened" and e["monotonic_ns"] >= case_start
        ]
        if phase in {"allocated", "launcher", "ready"}:
            assert not gate
        else:
            assert gate
    finally:
        release.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await runner.close()
    assert_clean(profile().evidence, before)
    second = SandboxRunner(profile())
    try:
        assert (await second.run(Request(("-c", "print(5)")))).stdout == b"5\n"
    finally:
        await second.close()


FENCES = [
    "allocated",
    "launcher",
    "ready",
    "quiescent",
    "export_chunk",
    "export_end",
    "missing_rootfs",
    "missing_base",
    "missing_delegation",
    "binary-ready",
    "rootfs-ready",
    "filter-ready",
    "runner-ready",
    "copy-filter-launcher",
    "copy-request-ready",
    "live-owner",
]


@pytest.mark.parametrize("phase", FENCES)
async def test_binding_fence_and_unavailable_fail_closed(phase):
    before = fds()
    case_start = time.monotonic_ns()
    permit = [True]
    if phase == "live-owner":
        first = SandboxRunner(profile())
        try:
            with pytest.raises(ServiceError) as err:
                SandboxRunner(profile())
            assert err.value.code == "sandbox_cleanup_required"
            assert (await first.run(Request(("-c", "print(5)")))).stdout == b"5\n"
        finally:
            await first.close()
        assert fds() == before
        return
    if phase.startswith("missing_"):
        key = {
            "missing_rootfs": "rootfs",
            "missing_base": "base",
            "missing_delegation": "cgroup_root",
        }[phase]
        with pytest.raises(ServiceError) as err:
            SandboxRunner(profile(**{key: E / "materials/d1-os/missing"}))
        assert err.value.code == "sandbox_unavailable"
        assert fds() == before
        return
    from krim_agent.execution import seccomp

    old_program = seccomp.program

    async def hook(current):
        if current == phase:
            permit[0] = False
        if phase.endswith("-ready") and current == "ready":
            if phase == "binary-ready":
                object.__setattr__(runner.profile, "binary", E / "materials/d1-os/not-a-binary")
            if phase == "rootfs-ready":
                object.__setattr__(runner.profile, "rootfs", E / "materials/d1-os/not-rootfs")
            if phase == "filter-ready":
                seccomp.program = lambda: b"changed"
            if phase == "runner-ready":
                runner._execute = lambda *a: None
        if (phase == "copy-filter-launcher" and current == "launcher") or (
            phase == "copy-request-ready" and current == "ready"
        ):
            folder = (
                E
                / "materials/d1-os"
                / max(profile().evidence.glob("job-*"), key=lambda p: p.stat().st_ctime_ns).name
            )
            file = folder / ("filter.bin" if phase == "copy-filter-launcher" else "request.json")
            file.chmod(0o600)
            file.write_bytes(b"changed")

    runner = SandboxRunner(profile(), hook=hook, fence=lambda: permit[0])
    try:
        with pytest.raises(ServiceError) as err:
            await runner.run(Request(("-c", "open('out','w').write('ok');print('ENTER')")))
        assert err.value.code == "target_binding_changed"
        gate = [
            e
            for e in events(profile().evidence)
            if e["kind"] == "gate_opened" and e["monotonic_ns"] >= case_start
        ]
        assert bool(gate) == (phase in {"quiescent", "export_chunk", "export_end"})
    finally:
        seccomp.program = old_program
        await runner.close()
    assert_clean(profile().evidence, before)


EXPORT_CASES = [
    "valid",
    "symlink",
    "hardlink",
    "fifo",
    "socket",
    "device",
    "count",
    "size",
    "depth",
    "secret",
    "restart-ready",
    "restart-payload_output",
]


@pytest.mark.parametrize("case", EXPORT_CASES)
async def test_quiescent_export_and_restart_cleanup(case, tmp_path):
    before = fds()
    if case.startswith("restart-"):
        worker_evidence = profile().evidence / case
        worker_evidence.mkdir()
        ready = worker_evidence / "worker-ready"
        env = {
            "PATH": "/usr/local/bin:/usr/bin:/bin",
            "LD_LIBRARY_PATH": str(Path(sys.base_prefix) / "lib"),
            "TMPDIR": str(E / "tmp"),
            "PYTHONDONTWRITEBYTECODE": "1",
        }
        proc = await asyncio.create_subprocess_exec(
            sys.executable,
            "-B",
            "tests/agent_core/fixtures/sandbox_cases.py",
            str(E),
            str(worker_evidence),
            case.removeprefix("restart-"),
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        fd = os.pidfd_open(proc.pid)
        worker_command = {
            "argv": [
                sys.executable,
                "-B",
                "tests/agent_core/fixtures/sandbox_cases.py",
                str(E),
                str(worker_evidence),
                case.removeprefix("restart-"),
            ],
            "cwd": str(Path.cwd()),
            "env": env,
            "pid": proc.pid,
            "pidfd": fd,
            "started_ns": time.monotonic_ns(),
        }
        out = err = b""
        try:
            async with asyncio.timeout(10):
                while not ready.exists():
                    if proc.returncode is not None:
                        raise AssertionError(await proc.communicate())
                    await asyncio.sleep(0.02)
            signal.pidfd_send_signal(fd, signal.SIGKILL)
            out, err = await proc.communicate()
            assert proc.returncode == -9, (out, err)
        finally:
            if proc.returncode is None:
                signal.pidfd_send_signal(fd, signal.SIGKILL)
                await proc.communicate()
            os.close(fd)
            (worker_evidence / "worker.stdout").write_bytes(out)
            (worker_evidence / "worker.stderr").write_bytes(err)
            worker_command.update(
                exit_code=proc.returncode,
                waited=True,
                pidfd_closed=True,
                finished_ns=time.monotonic_ns(),
            )
            (worker_evidence / "worker-command.json").write_text(
                json.dumps(worker_command, indent=2)
            )
        runner = SandboxRunner(profile())
        try:
            assert runner.recovered and all(
                r["exit_code"] is None and r["status"] == "unknown" and r["cleanup"] == "clean"
                for r in runner.recovered
            )
            (worker_evidence / "recovery.json").write_text(json.dumps(runner.recovered, indent=2))
            assert all(
                r["leaf_removed"] and "populated 0" in r["events_after"] for r in runner.recovered
            )
            assert (await runner.run(Request(("-c", "print(5)")))).stdout == b"5\n"
        finally:
            await runner.close()
        assert not list((E / "materials/d1-os/tickets").iterdir())
        assert fds() == before
        return
    if case == "valid":
        runner = SandboxRunner(profile())
        try:
            result = await runner.run(
                Request(
                    (
                        "-c",
                        "import os,time\nopen('out','wb').write(b'x'*40000)\nif os.fork()==0:os.setsid();time.sleep(60)",
                    )
                )
            )
            assert (
                result.exit_code == 0
                and result.files["out"] == b"x" * 40000
                and result.output_complete
            )
            log = events(Path(result.evidence).parent)
            ready = [e for e in log if e["kind"] == "broker_ready"][-1]
            assert ready["message"]["dumpable"] == 0 and ready["message"]["pid"] == 1
        finally:
            await runner.close()
        assert_clean(profile().evidence, before)
        return
    root = tmp_path / "export"
    root.mkdir()
    sock = None
    try:
        if case == "symlink":
            (root / "bad").symlink_to("/etc/passwd")
        if case == "hardlink":
            (root / "a").write_bytes(b"x")
            os.link(root / "a", root / "b")
        if case == "fifo":
            os.mkfifo(root / "fifo")
        if case == "socket":
            sock = socket.socket(socket.AF_UNIX)
            directory_fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
            try:
                # sockaddr_un有108字节限制；稳定dirfd指向真实长路径目录。
                sock.bind(f"/proc/self/fd/{directory_fd}/sock")
            finally:
                os.close(directory_fd)
        if case == "device":
            # 同一拒绝函数检查已有真实设备类型，绝不打开设备；内层mknod负例另由策略执行。
            with pytest.raises(ValueError):
                _broker.collect("/dev")
            return
        if case == "count":
            for i in range(33):
                (root / str(i)).write_bytes(b"")
        if case == "size":
            (root / "big").write_bytes(b"x" * 1048577)
        if case == "depth":
            p = root
            for i in range(17):
                p = p / str(i)
                p.mkdir()
            (p / "f").write_bytes(b"x")
        if case == "secret":
            (root / ".env").write_bytes(b"fixture only")
        with pytest.raises(ValueError):
            _broker.collect(root)
    finally:
        if sock is not None:
            sock.close()
    assert fds() == before


@pytest.mark.parametrize("seam", ["wait", "leaf"])
async def test_cleanup_failure_retains_real_owner(seam, monkeypatch):
    """真实Runner/Popen/leaf；仅在真实收束方法上放置可解除故障。"""
    before = fds()
    monkeypatch.setenv("KRIM_D1_EVIDENCE", str(profile().evidence / ("cleanup-fault-" + seam)))
    captured = {}
    held = [True]
    communicate = asyncio.subprocess.Process.communicate
    leaf_type = __import__("krim_agent.execution.cgroups", fromlist=["Leaf"]).Leaf
    leaf_init, leaf_finish = leaf_type.__init__, leaf_type.finish

    def capture_leaf(leaf, *args, **kwargs):
        leaf_init(leaf, *args, **kwargs)
        captured["leaf"] = leaf

    async def obstruct_wait(proc, *args, **kwargs):
        captured["proc"] = proc
        if held[0] and seam == "wait":
            raise TimeoutError("bounded injected wait failure on real Popen")
        return await communicate(proc, *args, **kwargs)

    async def obstruct_leaf(leaf):
        if held[0] and seam == "leaf":
            raise ServiceError("sandbox_cleanup_failed")
        return await leaf_finish(leaf)

    monkeypatch.setattr(leaf_type, "__init__", capture_leaf)
    monkeypatch.setattr(leaf_type, "finish", obstruct_leaf)
    monkeypatch.setattr(asyncio.subprocess.Process, "communicate", obstruct_wait)

    async def fail_after_effect(phase):
        if phase == "payload_output":
            raise ServiceError("sandbox_cleanup_failed")

    runner = SandboxRunner(profile(), hook=fail_after_effect)
    cleanup_observations = {"seam": seam, "emergency_cleanup": False}
    try:
        with pytest.raises(ServiceError) as error:
            await runner.run(
                Request(("-c", "import time;print('ENTER',flush=True);time.sleep(60)"))
            )
        assert error.value.code == "sandbox_cleanup_failed"
        assert runner.jobs, "Task.done must not discard unclosed real resources"
        assert not runner.closed and not runner._owner.closed
        raw = json.loads(next(profile().evidence.glob("job-*/receipt.json")).read_text())
        assert raw["cleanup"] == "unknown"
        for _ in range(2):
            with pytest.raises(ServiceError) as blocked:
                await runner.close()
            assert blocked.value.code == "sandbox_cleanup_failed"
            assert runner.jobs and not runner.closed and not runner._owner.closed
            with pytest.raises(ServiceError) as duplicate:
                SandboxRunner(profile())
            assert duplicate.value.code == "sandbox_cleanup_required"
        data = events(profile().evidence)
        assert sum(r["kind"] == "process_created" for r in data) == 1
        assert sum(r["kind"] == "gate_opened" for r in data) == 1
        held[0] = False
        await runner.close()
        await runner.close()
        assert runner.closed and not runner.jobs and runner._owner.closed
        raw = json.loads(next(profile().evidence.glob("job-*/receipt.json")).read_text())
        assert raw["cleanup"] == "clean" and raw["code"] == "sandbox_cleanup_failed"
        assert sum(r["kind"] == "process_created" for r in events(profile().evidence)) == 1
    finally:
        held[0] = False
        await runner.close()
        leaf = captured.get("leaf")
        # RED旧实现会丢owner；用同一真实句柄兜底，不能让RED遗留进程/leaf。
        if leaf is not None and leaf.path.exists():
            cleanup_observations["emergency_cleanup"] = True
            proc = captured["proc"]
            out, err = await communicate(proc)
            cleanup_observations.update(
                pid=proc.pid,
                waited_rc=proc.returncode,
                remaining_stdout=len(out),
                remaining_stderr=len(err),
            )
            made = [r for r in events(profile().evidence) if r["kind"] == "process_created"]
            closed = [r for r in events(profile().evidence) if r["kind"] == "pidfd_closed"]
            if not closed:
                os.close(made[0]["pidfd"])
                cleanup_observations["emergency_pidfd_closed"] = True
            await leaf_finish(leaf)
        cleanup_observations.update(
            leaf_absent=leaf is None or not leaf.path.exists(),
            tickets_empty=not list((E / "materials/d1-os/tickets").iterdir()),
            fd_returned=fds() == before,
        )
        (profile().evidence / ("cleanup-fault-" + seam + ".json")).write_text(
            json.dumps(cleanup_observations, indent=2)
        )
        assert cleanup_observations["leaf_absent"] and cleanup_observations["tickets_empty"]
        assert fds() == before
    second = SandboxRunner(profile())
    try:
        assert (await second.run(Request(("-c", "print(5)")))).stdout == b"5\n"
    finally:
        await second.close()
    assert fds() == before


@pytest.mark.parametrize("ending", ["natural", "killed", "unknown"])
def test_output_limit_wait_status_boundary(ending):
    """安全纯状态边界：只执行真实reap/main AST，所有OS操作是假对象，不发信号。"""
    source = Path(_broker.__file__).read_text()
    tree = ast.parse(source)
    nodes = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in {"reap", "main"}]
    child = 123
    raw_status = {"natural": 7 << 8, "killed": 9, "unknown": None}[ending]
    statuses = [(222, 0)] + ([] if raw_status is None else [(child, raw_status)])
    signals, sent = [], []

    def waitpid(pid, flags):
        if flags:
            return (0, 0)  # 两次非阻塞轮询之间的退出，由reap取得实际状态。
        if statuses:
            return statuses.pop(0)
        raise ChildProcessError

    reads = {10: [b"abcd", b""], 12: [b""]}
    fake_os = SimpleNamespace(
        getpid=lambda: 1,
        chdir=lambda p: None,
        pipe=iter([(10, 11), (12, 13)]).__next__,
        fork=lambda: child,
        close=lambda fd: None,
        WNOHANG=1,
        waitpid=waitpid,
        kill=lambda pid, sig: signals.append((pid, sig)),
        read=lambda fd, size: reads[fd].pop(0),
        waitstatus_to_exitcode=os.waitstatus_to_exitcode,
    )

    class TextPath:
        def __init__(self, path):
            self.path = path

        def read_text(self):
            if self.path == "/request.json":
                return json.dumps({"cwd": ".", "argv": ["-c", "controlled"], "output": 1})
            return "synthetic metadata only"

    class Input:
        def __init__(self):
            self.buffer = self
            self.lines = iter([b"gate\n", b"export\n"])

        def readline(self, size):
            return next(self.lines)

    env = {
        "os": fake_os,
        "Path": TextPath,
        "json": json,
        "base64": base64,
        "resource": SimpleNamespace(
            setrlimit=lambda *a: None,
            **{
                n: 0
                for n in [
                    "RLIMIT_CORE",
                    "RLIMIT_FSIZE",
                    "RLIMIT_NOFILE",
                    "RLIMIT_AS",
                    "RLIMIT_NPROC",
                ]
            },
        ),
        "install_filter": lambda: None,
        "collect": lambda root: {},
        "ctypes": SimpleNamespace(CDLL=lambda x: SimpleNamespace(prctl=lambda n: 0)),
        "sys": SimpleNamespace(stdin=Input()),
        "select": SimpleNamespace(select=lambda fds, *a: (fds[:1], [], [])),
        "send": lambda kind, **data: sent.append({"type": kind, **data}),
    }
    exec(
        compile(ast.Module(body=nodes, type_ignores=[]), "<safe-broker-state-boundary>", "exec"),
        env,
    )
    env["main"]()
    exit_message = next(m for m in sent if m["type"] == "exit")
    expected = None if raw_status is None else os.waitstatus_to_exitcode(raw_status)
    assert exit_message["rc"] == expected, "kill intent is not a payload exit status"
    assert exit_message.get("wait_status") == raw_status
    assert exit_message.get("waited_pid") == (None if raw_status is None else child)
    assert exit_message["code"] == "sandbox_limit"
    assert b"".join(base64.b64decode(m["data"]) for m in sent if m["type"] == "output") == b"a"
    assert signals and all(item == (-1, 9) for item in signals)


async def test_output_limit_real_wait_fact(monkeypatch):
    """真实隔离内输出超限；exit事实必须能由保存的wait状态重算。"""
    before = fds()
    monkeypatch.setenv("KRIM_D1_EVIDENCE", str(profile().evidence / "real-wait-fact"))
    runner = SandboxRunner(profile())
    try:
        result = await runner.run(Request(("-c", payload("output"))))
        assert result.code == "sandbox_limit" and not result.output_complete
        assert len(result.stdout) == 262144 and result.exit_code == -9
        message = next(
            r["message"] for r in events(profile().evidence) if r["kind"] == "payload_exit"
        )
        assert message.get("wait_status") == 9 and type(message.get("waited_pid")) is int
        assert message["waited_pid"] > 1
        assert os.waitstatus_to_exitcode(message["wait_status"]) == result.exit_code
    finally:
        await runner.close()
    assert_clean(profile().evidence, before)
