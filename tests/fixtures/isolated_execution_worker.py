"""独立进程证据：subreaper/PID 1回收、父进程骤停与普通用户控制通道。"""

import asyncio
import concurrent.futures.thread  # noqa: F401  # 在降权前载入项目解释器的惰性标准库。
import ctypes
import json
import os
import selectors
import signal
import subprocess
import sys
import time
from pathlib import Path

from krim_agent.execution import CommandSpec, LinuxSandboxRunner, SandboxLimits
from krim_agent.execution.linux import _kill, _syscall
from krim_agent.protocol.domain import ServiceError


def children(pid):
    result = set()
    for task in Path(f"/proc/{pid}/task").glob("*"):
        try:
            result.update(map(int, (task / "children").read_text().split()))
        except FileNotFoundError:
            pass
    return result


def tree(pid):
    result, pending = {pid}, [pid]
    while pending:
        try:
            found = children(pending.pop()) - result
        except FileNotFoundError:
            continue
        result.update(found)
        pending.extend(found)
    return result


def assert_reaped(pids):
    assert not children(os.getpid()), children(os.getpid())
    assert not any(Path(f"/proc/{pid}").exists() for pid in pids), pids


def cleanup_children():
    for child in children(os.getpid()):
        try:
            os.kill(child, signal.SIGKILL)
        except ProcessLookupError:
            pass
        os.waitpid(child, 0)


async def host(mode):
    assert ctypes.CDLL(None).prctl(36, 1, 0, 0, 0) == 0  # PR_SET_CHILD_SUBREAPER
    runner = LinuxSandboxRunner(limits=SandboxLimits(wall_seconds=0.6 if mode == "timeout" else 10))
    processes = []
    original = subprocess.Popen

    def record(*args, **kwargs):
        process = original(*args, **kwargs)
        if "--unshare-all" in args[0]:
            processes.append(process)
        return process

    subprocess.Popen = record
    code = """
import os,time
pid=os.fork()
if pid==0:
    os.setsid()
    print('descendant-ready',flush=True)
    time.sleep(30)
else:
    print('parent-ready',flush=True)
"""
    if mode != "background":
        code += "    time.sleep(30)\n"
    calls = 0

    async def guard():
        nonlocal calls
        calls += 1
        if calls == 2 and mode == "deny-ready":
            raise ServiceError("lost_authority", "test", 409)

    try:
        handle = await runner.start(
            CommandSpec(argv=["/usr/bin/python3", "-I", "-c", code]), authorize=guard
        )
        if mode == "deny-ready":
            assert not await handle.wait_started()
        else:
            assert await handle.wait_started()
            if mode != "background":
                async with asyncio.timeout(5):
                    while b"descendant-ready" not in handle.output()[0]:
                        assert not handle.done
                        await asyncio.sleep(0.01)
        pids = tree(processes[0].pid)
        if mode == "await-death":
            print(json.dumps({"pids": sorted(pids), "host": os.getpid()}), flush=True)
            await asyncio.Event().wait()
        elif mode == "cancel":
            handle.cancel()
        elif mode == "close":
            closing = asyncio.create_task(runner.close())
            await asyncio.sleep(0)
            closing.cancel()
            await asyncio.sleep(0)
            closing.cancel()
            try:
                await closing
            except asyncio.CancelledError:
                pass
        elif mode == "broker-loss":
            leaders = children(processes[0].pid)
            assert len(leaders) == 1, leaders
            fd = _syscall(434, next(iter(leaders)), 0)
            try:
                _kill(fd)
            finally:
                os.close(fd)
        result = await handle.wait()
        assert (
            result.status
            == {
                "cancel": "cancelled",
                "timeout": "timed_out",
                "close": "cancelled",
                "background": "completed",
                "deny-ready": "setup_failed",
                "broker-loss": "uncertain",
            }[mode]
        ), result
        await asyncio.to_thread(assert_reaped, pids)
        print(
            json.dumps({"mode": mode, "status": result.status, "pid": os.getpid(), "reaped": True})
        )
    finally:
        await runner.close()
        subprocess.Popen = original
        # 即使断言失败，也收束本测试进程的被收养者，避免污染其他资源配额测试。
        await asyncio.to_thread(cleanup_children)


def parent_death():
    assert ctypes.CDLL(None).prctl(36, 1, 0, 0, 0) == 0
    process = subprocess.Popen(
        [sys.executable, __file__, "await-death"], stdout=subprocess.PIPE, text=True
    )
    assert process.stdout is not None
    selector = selectors.DefaultSelector()
    selector.register(process.stdout, selectors.EVENT_READ)
    descriptors = []
    try:
        assert selector.select(10), "宿主未报告命令就绪"
        report = json.loads(process.stdout.readline())
        assert len(report["pids"]) >= 4, report
        descriptors = [_syscall(434, pid, 0) for pid in report["pids"]]
        process.kill()
        process.wait(timeout=5)
        deadline = time.monotonic() + 5
        while True:
            try:
                while os.waitpid(-1, os.WNOHANG)[0]:
                    pass
            except ChildProcessError:
                break
            assert time.monotonic() < deadline, "宿主死亡后隔离进程仍存活"
            time.sleep(0.02)
        assert not any(Path(f"/proc/{pid}").exists() for pid in report["pids"]), report
        print(
            json.dumps({"host_killed": True, "descendants_stopped": True, "external_reaper": True})
        )
    finally:
        process.kill()
        process.wait()
        for fd in descriptors:
            _kill(fd)
            os.close(fd)
        selector.close()
        process.stdout.close()


async def nonroot():
    runner = LinuxSandboxRunner()
    # 测试机的项目解释器位于root私有目录；先加载依赖，再降权创建所有管道/进程。
    if os.getuid() == 0:
        os.setgroups([])
        os.setgid(runner.gid)
        os.setuid(runner.uid)
    assert os.getuid() != 0
    async with runner:
        handle = await runner.start(
            CommandSpec(
                argv=[
                    "/usr/bin/python3",
                    "-I",
                    "-c",
                    """
import errno,os
assert os.getppid()==1
for path in ['/proc/1/fd/0','/proc/1/fd/1','/proc/1/fd/2','/proc/1/environ','/proc/1/mem']:
    try:
        fd=os.open(path,os.O_RDWR)
    except OSError as error:
        assert error.errno in (errno.EACCES,errno.EPERM),error
    else:
        os.close(fd)
        raise AssertionError('control channel exposed: '+path)
print('protected')
""",
                ]
            )
        )
        result = await handle.wait()
        assert result.status == "completed" and result.stdout == b"protected\n", result
        print(json.dumps({"nonroot_host": os.getuid(), "control_protected": True}))


if __name__ == "__main__":
    mode = sys.argv[1]
    if mode == "parent-death":
        parent_death()
    elif mode == "nonroot":
        asyncio.run(nonroot())
    else:
        asyncio.run(host(mode))
