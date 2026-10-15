"""真正进程强杀后的成对消息/事件与原文件效果、SDK/HTTP同事实。"""

import asyncio
import json
import sys
from pathlib import Path

import pytest


@pytest.mark.parametrize("point", ["tool", "assistant"])
async def test_original_message_event_sql_sigkill_cold_sdk_http_and_zero_replay(tmp_path, point):
    script = Path(__file__).parent / "fixtures/runtime_message_event_process.py"
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
        line = await asyncio.wait_for(producer.stdout.readline(), 30)
        if not line:
            _, error = await producer.communicate()
            raise AssertionError(error.decode())
        metadata = json.loads(line)
        assert metadata["pid"] == producer.pid and metadata["point"] == point
        producer.kill()
        _, error = await asyncio.wait_for(producer.communicate(), 10)
        (tmp_path / "producer-stderr.log").write_bytes(error)
        assert producer.returncode == -9
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
        output, error = await asyncio.wait_for(recovery.communicate(), 30)
        (tmp_path / "recovery-output.json").write_bytes(output)
        (tmp_path / "recovery-stderr.log").write_bytes(error)
        assert recovery.returncode == 0, error.decode()
        proof = json.loads(output)
        assert proof["run_id"] == metadata["run"]["id"] and proof["frame"] == metadata["frame"]
        assert (
            proof["message_id"] == metadata["message"]["id"] and proof["inode"] == metadata["inode"]
        )
        assert proof["same_sdk_http"] and proof["replay_calls"] == proof["provider_calls"] == 0
    finally:
        for process in (producer, recovery):
            if process is not None and process.returncode is None:
                process.kill()
                out, error = await process.communicate()
                label = "producer" if process is producer else "recovery"
                (tmp_path / (label + "-remaining-output.log")).write_bytes(out)
                (tmp_path / (label + "-remaining-error.log")).write_bytes(error)
