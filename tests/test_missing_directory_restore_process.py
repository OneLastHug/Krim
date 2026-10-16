"""缺失账本六个真实强杀窗口、原材料/Owner/自然租约与冷恢复零重放。"""

import asyncio
import json
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(not sys.platform.startswith("linux"), reason="Linux SIGKILL")


@pytest.mark.parametrize("point", ["absence", "ready", "stage", "intent", "namespace", "settled"])
async def test_real_process_death_preserves_missing_facts_without_cold_republication(
    tmp_path, point
):
    script = Path(__file__).parent / "fixtures/missing_directory_restore_store_process.py"
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
        line = await asyncio.wait_for(producer.stdout.readline(), 20)
        if not line:
            _, errors = await producer.communicate()
            raise AssertionError(errors.decode())
        signal = json.loads(line)
        assert signal["pid"] == producer.pid and signal["point"] == point
        producer.kill()
        _, errors = await asyncio.wait_for(producer.communicate(), 5)
        assert producer.returncode == -9, errors.decode()
        metadata = json.loads((tmp_path / "metadata.json").read_text())
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
        output, errors = await asyncio.wait_for(recovery.communicate(), 20)
        assert recovery.returncode == 0, errors.decode()
        proof = json.loads(output)
        assert proof["pid"] != signal["pid"] and proof["replays"] == 0
        assert (
            proof["plan_id"] == metadata["plan_id"]
            and proof["operation_id"] == metadata["operation_id"]
        )
        assert proof["natural_lease_expiry"] == (point not in {"ready", "settled"})
        assert proof["lease_not_applicable"] == (point in {"ready", "settled"})
        assert proof["binary_hex"] == ("00ff" if point in {"namespace", "settled"} else None)
        print(json.dumps({"producer": signal, "recovery": proof}))
    finally:
        for process in (producer, recovery):
            if process is not None and process.returncode is None:
                process.kill()
                await process.communicate()
