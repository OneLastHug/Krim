"""跨进程兼容验证器的可重复门禁；普通CI不依赖历史wheel缓存。"""

import asyncio
import json
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(not sys.platform.startswith("linux"), reason="需要Linux文件事务")


async def test_agent_restore_contract_and_rejected_definition_changes_across_real_processes(
    tmp_path,
):
    script = Path(__file__).parent / "fixtures" / "workspace_effect_compatibility_check.py"

    async def run(mode):
        child = await asyncio.create_subprocess_exec(
            sys.executable,
            str(script),
            mode,
            str(tmp_path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(child.communicate(), 40)
            assert child.returncode == 0, stderr.decode()
            return json.loads(stdout)
        finally:
            if child.returncode is None:
                child.kill()
                await asyncio.wait_for(child.communicate(), 10)

    created = await run("create")
    assert created["agents"] == 2
    for mode in ("reject-tool", "reject-resource"):
        rejected = await run(mode)
        assert len(rejected["rejected"]) == 2 and rejected["provider_calls"] == 0
    restored = await run("restore")
    assert restored["restored_agents"] == 2 and restored["restore_provider_calls"] == 0
    parsed = await run("parse")
    assert parsed["parsed_attempts"] == 14 and parsed["strict_extra_rejected"]
