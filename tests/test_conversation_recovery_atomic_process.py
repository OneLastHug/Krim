"""SQL提交前/后强杀恢复，原键只读不会添第二条Host消息或重切分支。"""

import asyncio
import json
import sys
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parent / "fixtures/conversation_recovery_atomic_process.py"


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="Linux SIGKILL")
@pytest.mark.parametrize("point", ["before", "after"])
async def test_true_sql_transaction_death_keeps_old_group_and_one_recovery_message(tmp_path, point):
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
        signal = json.loads(line)
        assert signal["pid"] == producer.pid and signal["point"] == point
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
        assert proof["pid"] != producer.pid and proof["original_key_readback"]
        assert proof["recovery_id"] == signal["recovery_id"] and proof["provider_calls"] == 0
        assert proof["final_messages"] == proof["original_messages"] + 1
        assert proof["atomic_rollback_observed"] == (point == "before")
        print(json.dumps({"producer": signal, "recovery": proof}))
    finally:
        for process in (producer, recovery):
            if process is not None and process.returncode is None:
                process.kill()
                await process.communicate()
