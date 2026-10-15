"""真实cgroup2多进程合计限制；每个子进程均低于原rlimit。"""

import asyncio
import json
from pathlib import Path
from uuid import uuid4

import pytest

from krim_agent.execution import CommandSpec, LinuxSandboxRunner, SandboxLimits
from krim_agent.execution.cgroups import CgroupRoot
from krim_agent.execution.records import CommandSnapshot, CommandView
from krim_agent.execution.tree_models import SandboxTreeLimits
from krim_agent.protocol.domain import ServiceError
from krim_agent.tools.commands import command_tools


@pytest.fixture
def tree_root():
    path = Path("/sys/fs/cgroup") / ("krim-agent-quota-test-" + uuid4().hex)
    root = CgroupRoot.prepare(path, remove_on_close=True)
    try:
        yield root
    finally:
        root.close()
        assert not path.exists()


def limits(*, cpu=10.0, memory=134217728, processes=16):
    return SandboxLimits(
        cpu_seconds=10,
        memory_bytes=268435456,
        process_count=128,
        wall_seconds=10,
        tree=SandboxTreeLimits(cpu_seconds=cpu, memory_bytes=memory, process_count=processes),
    )


def command(code):
    return CommandSpec(argv=["/usr/bin/python3", "-c", code])


async def run(root, code, **kwargs):
    async with LinuxSandboxRunner(limits=limits(**kwargs), cgroup_root=root) as runner:
        handle = await runner.start(command(code))
        return await handle.wait()


async def test_published_legacy_limits_fingerprint_snapshots_and_eight_tool_schemas_unchanged():
    value = json.loads(
        (Path(__file__).parent / "fixtures/execution_tree_legacy_vectors.json").read_text()
    )
    async with LinuxSandboxRunner() as runner:
        assert runner.limits.model_dump_json() == value["limits_json"]
        assert runner.fingerprint == value["fingerprint"]
    assert (
        CommandSnapshot.model_validate_json(json.dumps(value["snapshot"])).model_dump(mode="json")
        == value["snapshot"]
    )
    assert (
        CommandView.model_validate_json(json.dumps(value["view"])).model_dump(mode="json")
        == value["view"]
    )
    assert [tool.schema().model_dump(mode="json") for tool in command_tools()] == value["tools"]


async def test_real_group_success_nonroot_fds_stats_and_all_children_removed(tree_root):
    result = await run(
        tree_root, "import os; print(os.getuid()); print(sorted(os.listdir('/proc/self/fd')))"
    )
    assert result.status == "completed" and result.started and result.dispatched, result
    assert result.stdout.startswith(b"65534\n"), result.stdout
    usage = result.tree_usage
    assert usage.membership_verified and usage.populated is False
    assert usage.cpu_microseconds > 0 and usage.memory_peak_bytes > 0 and usage.tasks_peak >= 3
    assert usage.oom_kills == 0 and usage.task_limit_hits == 0
    assert not list(tree_root.path.glob("krim-exec-*"))


async def test_real_two_children_memory_sum_oom_below_individual_address_limit(tree_root):
    code = """
import os,time
for index in range(2):
    if os.fork()==0:
        value=bytearray(70*1024*1024)
        for pos in range(0,len(value),4096): value[pos]=1
        time.sleep(5)
        os._exit(0)
for index in range(2): os.wait()
"""
    result = await run(tree_root, code)
    assert result.status == "failed" and result.error_code == "execution_tree_memory_limit", result
    usage = result.tree_usage
    assert result.started and usage.membership_verified and usage.populated is False
    assert usage.oom_kills > 0 and usage.tasks_peak >= 5 and usage.task_limit_hits == 0
    assert usage.memory_peak_bytes > 100 * 1024 * 1024
    assert not result.artifacts_complete


async def test_real_tree_process_limit_counts_forks_without_changing_per_uid_ceiling(tree_root):
    code = """
import os,time
for index in range(64):
    try: pid=os.fork()
    except OSError: break
    if pid==0:
        time.sleep(5)
        os._exit(0)
time.sleep(5)
"""
    result = await run(tree_root, code, processes=8)
    assert result.status == "failed" and result.error_code == "execution_tree_process_limit", result
    usage = result.tree_usage
    assert usage.membership_verified and usage.populated is False
    assert usage.task_limit_hits > 0 and usage.tasks_peak <= 8 and usage.oom_kills == 0


async def test_real_children_cpu_sum_stops_before_each_old_ten_second_limit(tree_root):
    code = """
import os,time
for index in range(2):
    if os.fork()==0:
        start=time.process_time()
        while time.process_time()-start<2: pass
        os._exit(0)
for index in range(2): os.wait()
"""
    result = await run(tree_root, code, cpu=1.0)
    assert result.status == "failed" and result.error_code == "execution_tree_cpu_limit", result
    usage = result.tree_usage
    assert usage.membership_verified and usage.populated is False
    assert 1000000 <= usage.cpu_microseconds < 2000000
    assert usage.tasks_peak >= 5 and usage.oom_kills == usage.task_limit_hits == 0


async def test_real_cancel_stops_group_and_keeps_original_host_membership(tree_root):
    before = await asyncio.to_thread(Path("/proc/self/cgroup").read_text)
    async with LinuxSandboxRunner(limits=limits(), cgroup_root=tree_root) as runner:
        handle = await runner.start(
            command(
                "import subprocess,time; subprocess.Popen(['/usr/bin/sleep','5']); time.sleep(5)"
            )
        )
        assert await handle.wait_started()
        handle.cancel()
        result = await handle.wait()
        assert result.status == "cancelled" and result.tree_usage.populated is False, result
        assert result.tree_usage.leader_reaped is True
    assert await asyncio.to_thread(Path("/proc/self/cgroup").read_text) == before
    assert not list(tree_root.path.glob("krim-exec-*"))


async def test_output_flood_with_forked_child_is_bounded_and_entire_group_reaped(tree_root):
    result = await run(
        tree_root,
        "import os\nos.fork()\nwhile True: os.write(1,b'x'*4096)",
    )
    assert result.status == "output_limit" and result.output_truncated, result
    assert len(result.stdout) + len(result.stderr) <= limits().output_bytes
    assert result.tree_usage.populated is False and result.tree_usage.leader_reaped is True
    assert result.error_code == "execution_output_limit"
    # 输出超限与文件集合完整性是独立事实；监督器仍可能完整采集已停止的副本。
    assert not list(tree_root.path.glob("krim-exec-*"))


async def test_closing_trusted_root_stops_running_group_without_claiming_command_success(tree_root):
    async with LinuxSandboxRunner(limits=limits(), cgroup_root=tree_root) as runner:
        handle = await runner.start(command("import os,time\nos.fork()\ntime.sleep(5)"))
        assert await handle.wait_started()
        await asyncio.to_thread(tree_root.close)
        result = await handle.wait()
        assert runner.closed and result.status != "completed", result
        assert result.tree_usage.populated is False and result.tree_usage.leader_reaped is True
        assert not tree_root.path.exists()


async def test_two_execution_groups_are_independent(tree_root):
    first, second = await asyncio.gather(
        run(tree_root, "import time; value=bytearray(20*1024*1024); print('a'); time.sleep(.1)"),
        run(tree_root, "import time; value=bytearray(30*1024*1024); print('b'); time.sleep(.1)"),
    )
    assert first.status == second.status == "completed", (first, second)
    assert (
        first.execution_id != second.execution_id
        and first.stdout == b"a\n"
        and second.stdout == b"b\n"
    )
    for result in (first, second):
        assert result.tree_usage.membership_verified and result.tree_usage.populated is False
        assert result.tree_usage.oom_kills == result.tree_usage.task_limit_hits == 0


def test_plain_directory_or_missing_explicit_root_is_rejected(tmp_path):
    fake = tmp_path / "krim-fake"
    fake.mkdir()
    with pytest.raises(ServiceError) as rejected:
        CgroupRoot(fake)
    assert rejected.value.code == "execution_tree_unavailable"
    with pytest.raises(ServiceError) as missing:
        LinuxSandboxRunner(limits=limits())
    assert missing.value.code == "execution_tree_configuration"


def test_group_limits_wrong_readback_cannot_pass_dispatch(tree_root):
    lease = tree_root.allocate(str(uuid4()), SandboxTreeLimits())
    try:
        (tree_root.path / lease.name / "memory.max").write_text("67108864\n")
        with pytest.raises(ServiceError) as rejected:
            lease.verify_limits()
        assert rejected.value.code == "execution_tree_unavailable"
    finally:
        assert lease.finish().populated is False
