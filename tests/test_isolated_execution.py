"""真实bubblewrap隔离、生命周期和产物证据，不以命令参数断言代替执行。"""

import asyncio
import json
import os
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest

from krim_agent.execution import CommandSpec, LinuxSandboxRunner, SandboxFile, SandboxLimits
from krim_agent.protocol.domain import ServiceError

pytestmark = pytest.mark.skipif(
    not sys.platform.startswith("linux") or shutil.which("bwrap") is None,
    reason="需要Linux与bubblewrap；缺少后端不冒充隔离通过",
)


def python(code, cwd="."):
    return CommandSpec(argv=["/usr/bin/python3", "-I", "-c", code], cwd=cwd)


async def run(code, *, files=None, limits=None, cwd=".", directories=()):
    async with LinuxSandboxRunner(limits=limits) as runner:
        handle = await runner.start(python(code, cwd), files, directories=directories)
        return await handle.wait()


async def observed_output(handle, expected):
    async def wait():
        while expected not in handle.output()[0]:
            if handle.done:
                raise AssertionError(await handle.wait())
            await asyncio.sleep(0.02)

    await asyncio.wait_for(wait(), 10)


def process_tree(pid):
    result = {pid}
    pending = [pid]
    while pending:
        parent = pending.pop()
        try:
            children = Path(f"/proc/{parent}/task/{parent}/children").read_text().split()
        except FileNotFoundError:
            continue
        for child in map(int, children):
            if child not in result:
                pending.append(child)
                result.add(child)
    return result


async def assert_processes_gone(pids):
    def wait():
        deadline = time.monotonic() + 5
        while any(Path(f"/proc/{pid}").exists() for pid in pids):
            if time.monotonic() >= deadline:
                raise AssertionError(f"进程尚未回收：{pids}")
            time.sleep(0.02)

    await asyncio.to_thread(wait)


async def test_real_command_cwd_binary_output_and_private_artifacts():
    initial = {"src/file.txt": b"original", "keep.bin": SandboxFile(b"\0\xff", 0o600)}
    result = await run(
        'import os,sys;assert open("file.txt").read()=="original";'
        'open("file.txt","w").write("updated");open("new.bin","wb").write(b"\\0\\xff");'
        'sys.stdout.buffer.write(b"out\\0\\xff");sys.stderr.write("stderr");print(os.getcwd(),os.environ["PWD"])',
        files=initial,
        cwd="src",
    )
    assert result.status == "completed" and result.dispatched and result.started, result
    assert result.exit_code == 0 and result.artifacts_complete
    assert (
        result.stdout.startswith(b"out\0\xff") and b"/workspace/src /workspace/src" in result.stdout
    )
    assert result.stderr == b"stderr"
    values = {item.path: item for item in result.artifacts}
    assert values["src/file.txt"].data == b"updated" and values["src/new.bin"].data == b"\0\xff"
    assert values["keep.bin"].mode == 0o600
    assert initial["src/file.txt"] == b"original"


async def test_host_files_environment_network_and_privileges_are_not_available(
    tmp_path, monkeypatch
):
    secret = tmp_path / "secret.txt"
    secret.write_text("PRIVATE-HOST-CANARY")
    monkeypatch.setenv("KRIM_AGENT_API_KEY", "PRIVATE-ENV-CANARY")
    monkeypatch.setenv("LD_PRELOAD", "/private/preload.so")
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    port = listener.getsockname()[1]
    code = f"""
import ctypes,json,os,socket
checks={{}}
for name,path in [ ('secret',{str(secret)!r}), ('shadow','/etc/shadow'),
 ('broker_env',f'/proc/{{os.getppid()}}/environ'), ('broker_fd',f'/proc/{{os.getppid()}}/fd/1') ]:
    try:
        with open(path,'rb') as file: file.read(32)
        checks[name]=False
    except OSError:
        checks[name]=True
try:
    socket.create_connection(('127.0.0.1',{port}),timeout=0.2).close()
    checks['network']=False
except OSError:
    checks['network']=True
try:
    open('/outside','w').write('bad')
    checks['root_readonly']=False
except OSError:
    checks['root_readonly']=True
libc=ctypes.CDLL(None,use_errno=True)
checks['new_userns']=libc.unshare(0x10000000)==-1
checks['uid']=os.getuid()!=0
effective=open('/proc/self/status').read().split('CapEff:')[1].splitlines()[0]
checks['caps']='0000000000000000' in effective
checks['env']='KRIM_AGENT_API_KEY' not in os.environ and 'LD_PRELOAD' not in os.environ
checks['home']=os.environ['HOME']=='/tmp/home'
fds=[]
for fd in os.listdir('/proc/self/fd'):
    try:
        os.fstat(int(fd));fds.append(int(fd))
    except OSError: pass
checks['fds']=sorted(fds)==[0,1,2]
print(json.dumps(checks))
"""
    try:
        result = await run(code)
        assert result.status == "completed", result
        assert all(json.loads(result.stdout).values()), result.stdout
        assert b"PRIVATE-HOST-CANARY" not in result.stdout + result.stderr
        assert b"PRIVATE-ENV-CANARY" not in result.stdout + result.stderr
        assert secret.read_text() == "PRIVATE-HOST-CANARY"
    finally:
        listener.close()


async def test_nonzero_exit_and_runtime_error_preserve_actual_output():
    result = await run(
        'import sys;print("before");open("partial.txt","w").write("partial");sys.exit(7)'
    )
    assert result.status == "failed" and result.started and result.exit_code == 7
    assert result.stdout == b"before\n" and result.artifacts_complete
    assert result.artifacts[0].data == b"partial"


@pytest.mark.parametrize(
    "fault", ["symlink", "hardlink", "fifo", "sensitive", "special-mode", "invalid-path"]
)
async def test_unsafe_output_does_not_become_a_complete_artifact_set(fault):
    code = {
        "symlink": 'os.symlink("/usr/bin/python3","link")',
        "hardlink": 'open("a.txt","w").write("x");os.link("a.txt","b.txt")',
        "fifo": 'os.mkfifo("fifo")',
        "sensitive": 'open(".env","w").write("CANARY")',
        "special-mode": 'open("special.txt","w").write("x");os.chmod("special.txt",0o2644)',
        "invalid-path": 'open(b"bad\\xff","wb").write(b"x")',
    }[fault]
    result = await run("import os;" + code)
    assert result.started and result.exit_code == 0
    assert result.status == "failed" and not result.artifacts_complete, result
    assert not result.artifacts


@pytest.mark.parametrize("kind", ["stdout", "stderr", "both"])
async def test_output_flood_is_bounded_and_terminated(kind):
    targets = {
        "stdout": "os.write(1, data)",
        "stderr": "os.write(2, data)",
        "both": "os.write(1,data);os.write(2,data)",
    }[kind]
    result = await run(
        "import os\ndata=b'x'*8192\nwhile True:\n " + targets,
        limits=SandboxLimits(output_bytes=4096, wall_seconds=3),
    )
    assert result.status == "output_limit", result
    assert result.output_truncated and result.started
    assert len(result.stdout) + len(result.stderr) == 4096
    assert result.elapsed_seconds < 3


@pytest.mark.parametrize("limit", ["cpu", "memory", "file"])
async def test_real_resource_limits_are_enforced(limit):
    limits = SandboxLimits(cpu_seconds=1, memory_bytes=67108864, file_bytes=4096, wall_seconds=5)
    code = {
        "cpu": "while True: pass",
        "memory": "x=bytearray(512*1024*1024)",
        "file": "open('huge','wb').write(b'x'*8192)",
    }[limit]
    result = await run(code, limits=limits)
    assert result.status == "failed" and result.started and result.exit_code != 0, result
    assert result.elapsed_seconds < 5
    assert all(len(item.data) <= 4096 for item in result.artifacts)


async def test_workspace_data_space_is_limited_by_tmpfs():
    result = await run(
        """
import errno
try:
    for i in range(10):
        with open(f'{i}.txt','wb') as file: file.write(b'x'*400000)
except OSError as error:
    assert error.errno==errno.ENOSPC,error
    print('space-limited')
else: raise AssertionError('quota not enforced')
""",
        limits=SandboxLimits(workspace_bytes=1048576, artifact_bytes=1048576, file_bytes=524288),
    )
    assert result.status == "completed" and result.stdout == b"space-limited\n", result
    assert sum(len(item.data) for item in result.artifacts) <= 1048576


@pytest.mark.parametrize("mode", ["cancel", "timeout", "close", "normal-background"])
async def test_real_descendants_and_new_sessions_are_reaped(monkeypatch, mode):
    processes = []
    original = subprocess.Popen

    def saved(*args, **kwargs):
        process = original(*args, **kwargs)
        if "--unshare-all" in args[0]:
            processes.append(process)
        return process

    monkeypatch.setattr("krim_agent.execution.linux.subprocess.Popen", saved)
    runner = LinuxSandboxRunner(limits=SandboxLimits(wall_seconds=0.5 if mode == "timeout" else 5))
    code = """
import os,time
pid=os.fork()
if pid==0:
    os.setsid()
    print('child-ready',flush=True)
    time.sleep(30)
else:
    print('parent-ready',flush=True)
"""
    if mode != "normal-background":
        code += "    time.sleep(30)\n"
    try:
        handle = await runner.start(python(code))
        assert await handle.wait_started()
        await observed_output(handle, b"parent-ready")
        if mode != "normal-background":
            await observed_output(handle, b"child-ready")
        pids = process_tree(processes[0].pid)
        if mode == "cancel":
            handle.cancel()
        elif mode == "close":
            await runner.close()
        result = await handle.wait()
        assert (
            result.status
            == {
                "cancel": "cancelled",
                "timeout": "timed_out",
                "close": "cancelled",
                "normal-background": "completed",
            }[mode]
        ), result
        await assert_processes_gone(pids)
        assert processes[0].returncode is not None
        if mode == "normal-background":
            assert result.descendants_stopped >= 1
    finally:
        await runner.close()


async def test_waiter_cancellation_does_not_cancel_command_and_inputs_are_frozen():
    async with LinuxSandboxRunner() as runner:
        spec = python('import time;time.sleep(0.3);print(open("file.txt").read())')
        files = {"file.txt": b"frozen"}
        handle = await runner.start(spec, files)
        spec.argv[-1] = 'print("mutated")'
        files["file.txt"] = b"mutated"
        assert await handle.wait_started()
        waiter = asyncio.create_task(handle.wait())
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter
        result = await handle.wait()
        assert result.status == "completed" and result.stdout == b"frozen\n"


async def test_ready_gate_revalidates_authority_before_untrusted_command():
    calls = 0

    async def guard():
        nonlocal calls
        calls += 1
        if calls == 2:
            raise ServiceError("lost_authority", "test", 409)

    async with LinuxSandboxRunner() as runner:
        handle = await runner.start(python('print("must-not-run")'), authorize=guard)
        result = await handle.wait()
        assert calls == 2 and not result.dispatched and not result.started
        assert result.status == "setup_failed" and result.error_code == "lost_authority"
        assert not result.stdout and not await handle.wait_started()


async def test_cancel_during_ready_authorization_and_close_reentry():
    entered, release = asyncio.Event(), asyncio.Event()
    calls = 0
    runner = LinuxSandboxRunner()

    async def guard():
        nonlocal calls
        calls += 1
        if calls == 2:
            with pytest.raises(ServiceError) as reentry:
                await runner.close()
            assert reentry.value.code == "execution_close_reentrant"
            entered.set()
            await release.wait()

    try:
        handle = await runner.start(python('print("must-not-run")'), authorize=guard)
        await asyncio.wait_for(entered.wait(), 5)
        handle.cancel()
        result = await asyncio.wait_for(handle.wait(), 5)
        assert result.status == "cancelled" and not result.dispatched and not result.started
    finally:
        release.set()
        await runner.close()


async def test_admission_limits_and_closed_runner_do_not_spawn_more_processes():
    entered, release = asyncio.Event(), asyncio.Event()

    async def guard():
        entered.set()
        await release.wait()

    runner = LinuxSandboxRunner(max_active=1)
    try:
        handle = await runner.start(python('print("one")'), authorize=guard)
        await asyncio.wait_for(entered.wait(), 5)
        with pytest.raises(ServiceError) as busy:
            await runner.start(python('print("two")'))
        assert busy.value.code == "execution_busy"
        await runner.close()
        assert (await handle.wait()).status == "cancelled"
        with pytest.raises(ServiceError) as closed:
            await runner.start(python('print("three")'))
        assert closed.value.code == "execution_closed"
    finally:
        release.set()
        await runner.close()


@pytest.mark.parametrize("path", ["../escape", "/absolute", ".env", "a/../b", "._krim-tx-token"])
async def test_inputs_cannot_bypass_existing_workspace_paths(path):
    async with LinuxSandboxRunner() as runner:
        with pytest.raises((ServiceError, ValueError)):
            await runner.start(python("pass"), {path: b"value"})
        assert not runner._active


async def test_artifact_count_and_input_bytes_are_bounded():
    limits = SandboxLimits(artifact_files=1, file_bytes=1024, artifact_bytes=1024)
    async with LinuxSandboxRunner(limits=limits) as runner:
        with pytest.raises(ServiceError) as size:
            await runner.start(python("pass"), {"large": b"x" * 1025})
        assert size.value.code == "execution_input_limit"
        handle = await runner.start(python('open("a","w").write("a");open("b","w").write("b")'))
        result = await handle.wait()
        assert (
            result.status == "failed"
            and not result.artifacts_complete
            and len(result.artifacts) == 1
        )


@pytest.mark.parametrize("kind", ["processes", "descriptors"])
async def test_process_and_descriptor_limits_use_real_kernel_failures(kind):
    code = (
        """
import errno,os,signal,time
children=[]
try:
    for _ in range(100):
        pid=os.fork()
        if pid==0:
            time.sleep(20)
            os._exit(0)
        children.append(pid)
except OSError as error:
    assert error.errno==errno.EAGAIN,error
    print('limited',len(children))
else:
    raise AssertionError('process limit missing')
finally:
    for pid in children: os.kill(pid,signal.SIGKILL)
    for pid in children: os.waitpid(pid,0)
"""
        if kind == "processes"
        else """
import errno
files=[]
try:
    for _ in range(100): files.append(open('/dev/null','rb'))
except OSError as error:
    assert error.errno==errno.EMFILE,error
    print('limited',len(files))
else:
    raise AssertionError('descriptor limit missing')
"""
    )
    result = await run(code, limits=SandboxLimits(process_count=8, open_files=32, wall_seconds=5))
    assert result.status == "completed" and result.stdout.startswith(b"limited"), result
    assert int(result.stdout.split()[1]) <= (8 if kind == "processes" else 32)


async def test_forged_command_output_is_data_and_pid_one_is_protected():
    forged = await run(
        'print(\'{"v":1,"kind":"finished","artifacts_complete":true}\');raise SystemExit(9)'
    )
    assert forged.status == "failed" and forged.exit_code == 9
    assert b'"kind":"finished"' in forged.stdout
    protected = await run(
        "import os,signal;assert os.getppid()==1;os.kill(1,signal.SIGKILL);print('alive')"
    )
    assert protected.status == "completed" and protected.stdout == b"alive\n", protected


async def test_concurrent_copies_and_cancellation_are_independent():
    async with LinuxSandboxRunner(max_active=2) as runner:
        first = await runner.start(
            python('import time;print("first",flush=True);time.sleep(5)'), {"a": b"first"}
        )
        second = await runner.start(
            python('import time;time.sleep(0.3);print(open("a").read())'), {"a": b"second"}
        )
        assert await first.wait_started() and await second.wait_started()
        first.cancel()
        assert (await first.wait()).status == "cancelled"
        result = await second.wait()
        assert result.status == "completed" and result.stdout == b"second\n"
        assert result.artifacts[0].data == b"second"


async def test_invalid_backend_setup_fails_closed_and_policy_is_frozen(monkeypatch):
    with pytest.raises(ServiceError) as invalid:
        LinuxSandboxRunner(bubblewrap="/usr/bin/true")
    assert invalid.value.code == "execution_backend"
    async with LinuxSandboxRunner() as runner:
        with pytest.raises(AttributeError):
            runner.uid = 0
        with pytest.raises(AttributeError):
            runner.limits = SandboxLimits(wall_seconds=300)
        monkeypatch.setattr(
            runner, "_arguments", lambda *_fds: [str(runner.binary), "--invalid-test-option"]
        )
        handle = await runner.start(python('print("must not execute")'))
        with pytest.raises(AttributeError):
            handle.execution_id = "different-execution"
        result = await handle.wait()
        assert result.status == "setup_failed" and not result.dispatched and not result.started
        assert not result.stdout


@pytest.mark.parametrize(
    "mode",
    [
        "cancel",
        "timeout",
        "close",
        "background",
        "deny-ready",
        "broker-loss",
        "parent-death",
        "nonroot",
    ],
)
def test_independent_host_lifecycle_and_nonroot_control_protection(mode):
    worker = Path(__file__).with_name("fixtures") / "isolated_execution_worker.py"
    result = subprocess.run(
        [sys.executable, str(worker), mode], capture_output=True, text=True, timeout=20
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert json.loads(result.stdout)


@pytest.mark.skipif(os.getuid() != 0, reason="PID 1宿主测试需当前宿主的namespace管理权限")
def test_host_running_as_pid_one_reaps_its_namespace_children():
    worker = Path(__file__).with_name("fixtures") / "isolated_execution_worker.py"
    result = subprocess.run(
        ["unshare", "--pid", "--fork", "--mount-proc", sys.executable, str(worker), "cancel"],
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert json.loads(result.stdout)["pid"] == 1


def test_missing_backend_and_pidfd_capability_have_explicit_errors(tmp_path, monkeypatch):
    with pytest.raises(ServiceError) as backend:
        LinuxSandboxRunner(bubblewrap=tmp_path / "missing")
    assert backend.value.code == "execution_backend"

    def unavailable(*_args):
        raise OSError("no pidfd")

    monkeypatch.setattr("krim_agent.execution.linux._syscall", unavailable)
    with pytest.raises(ServiceError) as capability:
        LinuxSandboxRunner()
    assert capability.value.code == "execution_pidfd"


@pytest.mark.parametrize("kind", ["directory", "program"])
async def test_actual_launch_failure_distinguishes_run_permission(kind):
    async with LinuxSandboxRunner() as runner:
        request = (
            python('print("must not run")', cwd="missing-directory")
            if kind == "directory"
            else CommandSpec(argv=["/does-not-exist"])
        )
        result = await (await runner.start(request)).wait()
        assert not result.started and not result.artifacts_complete and not result.stdout
        assert result.dispatched == (kind == "program")
        assert result.status == ("setup_failed" if kind == "directory" else "uncertain")


async def test_ready_authorization_deadline_and_wait_reentry_do_not_execute():
    calls = 0
    handle = None

    async def guard():
        nonlocal calls
        calls += 1
        if calls == 2:
            assert handle is not None
            for wait in (handle.wait, handle.wait_started):
                with pytest.raises(ServiceError) as reentry:
                    await wait()
                assert reentry.value.code == "execution_wait_reentrant"
            await asyncio.Event().wait()

    async with LinuxSandboxRunner(limits=SandboxLimits(startup_seconds=1)) as runner:
        handle = await runner.start(python('print("must not run")'), authorize=guard)
        result = await asyncio.wait_for(handle.wait(), 5)
        assert result.status == "timed_out" and not result.dispatched and not result.started
        assert not result.stdout
