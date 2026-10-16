"""真正SIGKILL后新Host自然租约恢复，原SQL/同operation只读核实且零重放。"""

import asyncio
import json
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(not sys.platform.startswith("linux"), reason="Linux SIGKILL")


@pytest.mark.parametrize("point", ["before_intent", "intent", "namespace", "receipt"])
async def test_real_host_sigkill_current_registry_and_cold_same_key_no_replay(tmp_path, point):
    script = Path(__file__).parent / "fixtures/container_retention_host_process.py"
    producer = await asyncio.create_subprocess_exec(
        sys.executable,
        "-I",
        str(script),
        "produce",
        point,
        str(tmp_path),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    recovery = None
    try:
        line = await asyncio.wait_for(producer.stdout.readline(), 25)
        if not line:
            _, error = await producer.communicate()
            raise AssertionError(error.decode())
        signal = json.loads(line)
        assert signal["pid"] == producer.pid
        producer.kill()
        _, error = await asyncio.wait_for(producer.communicate(), 5)
        assert producer.returncode == -9, error.decode()
        recovery = await asyncio.create_subprocess_exec(
            sys.executable,
            "-I",
            str(script),
            "recover",
            point,
            str(tmp_path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        output, error = await asyncio.wait_for(recovery.communicate(), 25)
        assert recovery.returncode == 0, error.decode()
        proof = json.loads(output)
        metadata = json.loads((tmp_path / "original.json").read_text())
        assert proof["pid"] != signal["pid"] and proof["operation_id"] == metadata["operation_id"]
        assert proof["natural_actor_gc_lease_expiry"] and proof["production_replays"] == 0
        assert proof["binary"] == "00ff"
        print(json.dumps({"producer": signal, "recovery": proof}))
    finally:
        for process in (producer, recovery):
            if process is not None and process.returncode is None:
                process.kill()
                await process.communicate()
