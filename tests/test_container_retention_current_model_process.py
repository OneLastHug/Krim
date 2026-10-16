"""当前GC模型来源真实强杀与自然租约冷恢复；原call是明确合成输入。"""

import asyncio
import json
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(not sys.platform.startswith("linux"), reason="Linux SIGKILL")


@pytest.mark.parametrize("point", ["admitted", "namespace", "receipt"])
async def test_current_model_provenance_survives_real_process_death_without_replaying(
    tmp_path, point
):
    script = Path(__file__).parent / "fixtures/container_retention_current_model_process.py"
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
        original = json.loads(line)
        assert original["pid"] == producer.pid
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
        assert proof["pid"] != producer.pid and proof["operation_id"] == metadata["operation_id"]
        assert proof["original_model_source"] == {
            "run_id": metadata["run_id"],
            "call_id": metadata["call_id"],
        }
        assert proof["natural_actor_gc_expiry"] and proof["production_replays"] == 0
        print(json.dumps({"producer": original, "recovery": proof}))
    finally:
        for process in (producer, recovery):
            if process is not None and process.returncode is None:
                process.kill()
                await process.communicate()
