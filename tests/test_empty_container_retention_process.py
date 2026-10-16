"""回收命名空间调用前/后真正强杀，新进程不做旧删除或生产。"""

import asyncio
import json
import sys
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parent / "fixtures/empty_container_retention_process.py"


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="Linux SIGKILL")
@pytest.mark.parametrize("point", ["before", "after"])
async def test_real_empty_container_sigkill_keeps_target_and_only_observes_original(
    point, tmp_path
):
    producer = await asyncio.create_subprocess_exec(
        sys.executable,
        "-I",
        str(SCRIPT),
        "produce",
        point,
        str(tmp_path),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    recovery = None
    try:
        line = await asyncio.wait_for(producer.stdout.readline(), 15)
        if not line:
            _, errors = await producer.communicate()
            raise AssertionError(errors.decode())
        original = json.loads(line)
        assert original["pid"] == producer.pid and original["point"] == point
        producer.kill()
        _, errors = await asyncio.wait_for(producer.communicate(), 5)
        assert producer.returncode == -9, errors.decode()
        recovery = await asyncio.create_subprocess_exec(
            sys.executable,
            "-I",
            str(SCRIPT),
            "recover",
            point,
            str(tmp_path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        output, errors = await asyncio.wait_for(recovery.communicate(), 15)
        assert recovery.returncode == 0, errors.decode()
        proof = json.loads(output)
        assert proof["producer"] == producer.pid and proof["pid"] != producer.pid
        assert proof["status"] == ("empty" if point == "before" else "absent")
        assert proof["binary_hex"] == "00ff" and proof["intent_saved"] and proof["replays"] == 0
        print(json.dumps({"producer": original, "recovery": proof}))
    finally:
        for process in (producer, recovery):
            if process is not None and process.returncode is None:
                process.kill()
                await process.communicate()
