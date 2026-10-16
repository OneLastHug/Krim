"""产品SQL/真实rmdir四点强杀，原key/Source/目标与冷只读零重放。"""

import asyncio
import json
import sys
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parent / "fixtures/container_retention_process.py"


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="Linux SIGKILL")
@pytest.mark.parametrize("point", ["before_intent", "intent", "namespace", "receipt"])
async def test_real_sql_and_namespace_sigkill_original_key_and_readonly_recovery(point, tmp_path):
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
        line = await asyncio.wait_for(producer.stdout.readline(), 20)
        if not line:
            _, error = await producer.communicate()
            raise AssertionError(error.decode())
        original = json.loads(line)
        assert original["pid"] == producer.pid and original["point"] == point
        producer.kill()
        _, error = await asyncio.wait_for(producer.communicate(), 5)
        assert producer.returncode == -9, error.decode()
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
        output, error = await asyncio.wait_for(recovery.communicate(), 20)
        assert recovery.returncode == 0, error.decode()
        proof = json.loads(output)
        assert proof["pid"] != producer.pid and proof["producer"] == producer.pid
        assert proof["operation_id"] == original["operation_id"]
        assert (
            proof["replays"] == 0
            and proof["binary_hex"] == "00ff"
            and proof["original_source_same"]
            and proof["original_finished_same"]
        )
        assert proof["recovered"] == (0 if point == "receipt" else 1)
        assert proof["natural_retention_lease_expiry"] == (point != "receipt")
        print(json.dumps({"producer": original, "recovery": proof}))
    finally:
        for process in (producer, recovery):
            if process is not None and process.returncode is None:
                process.kill()
                await process.communicate()


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="Linux进程间SQL并发")
async def test_other_process_original_key_cold_read_does_not_readmit_or_create_duplicate(tmp_path):
    producer = await asyncio.create_subprocess_exec(
        sys.executable,
        "-I",
        str(SCRIPT),
        "produce",
        "admitted",
        str(tmp_path),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    consumer = None
    try:
        line = await asyncio.wait_for(producer.stdout.readline(), 20)
        if not line:
            _, error = await producer.communicate()
            raise AssertionError(error.decode())
        original = json.loads(line)
        consumer = await asyncio.create_subprocess_exec(
            sys.executable,
            "-I",
            str(SCRIPT),
            "join",
            "admitted",
            str(tmp_path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        output, error = await asyncio.wait_for(consumer.communicate(), 20)
        assert consumer.returncode == 0, error.decode()
        value = json.loads(output)
        assert value["pid"] != original["pid"] and not value["created"]
        assert value["operation_id"] == original["operation_id"] and value["status"] == "running"
        print(json.dumps({"producer": original, "other_process_read": value}))
    finally:
        for process in (producer, consumer):
            if process is not None and process.returncode is None:
                process.kill()
                await process.communicate()
