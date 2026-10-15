"""真实模型恢复ready/部分文件SQL后强杀，跨进程来源与原事实零重放。"""

import asyncio
import json
import sys
import time
from pathlib import Path

import pytest
from sqlalchemy import update

from krim_agent.persistence import Database
from krim_agent.persistence.database import AgentHandleRow, WorkspaceRestorePlanRow


@pytest.mark.parametrize("point", ["ready", "effect"])
async def test_actual_model_restore_sql_sigkill_cold_reads_original_source_and_zero_replay(
    tmp_path, point
):
    root = tmp_path / "files"
    root.mkdir()
    (root / "plain").write_text("target\n")
    (root / "binary").write_bytes(b"\x00\xff")
    fixture = Path(__file__).parent / "fixtures/model_native_checkpoint_restore_process.py"
    producer = await asyncio.create_subprocess_exec(
        sys.executable,
        "-I",
        str(fixture),
        "produce",
        point,
        str(tmp_path),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'state.sqlite'}")
    recovery = None
    try:
        line = await asyncio.wait_for(producer.stdout.readline(), 40)
        if not line:
            _, error = await producer.communicate()
            raise AssertionError(error.decode())
        metadata = json.loads(line)
        assert metadata["pid"] == producer.pid and metadata["point"] == point
        assert len(metadata["sources"]) == (1 if point == "ready" else 2)
        producer.kill()
        _, error = await asyncio.wait_for(producer.communicate(), 10)
        (tmp_path / "producer-stderr.log").write_bytes(error)
        assert producer.returncode == -9
        async with database.write_lock, database.sessions.begin() as sql:
            # 只受控到期当前合成测试库；不冒充生产自然等待或多worker证明。
            await sql.execute(
                update(AgentHandleRow)
                .where(AgentHandleRow.agent_id == metadata["agent_id"])
                .values(lease_expires_at=time.time() - 1)
            )
            if point == "effect":
                await sql.execute(
                    update(WorkspaceRestorePlanRow)
                    .where(WorkspaceRestorePlanRow.plan_id == metadata["plan_id"])
                    .values(lease_expires_at=time.time() - 1)
                )
        recovery = await asyncio.create_subprocess_exec(
            sys.executable,
            "-I",
            str(fixture),
            "recover",
            point,
            str(tmp_path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        output, error = await asyncio.wait_for(recovery.communicate(), 30)
        (tmp_path / "recovery-stdout.json").write_bytes(output)
        (tmp_path / "recovery-stderr.log").write_bytes(error)
        assert recovery.returncode == 0, error.decode()
        proof = json.loads(output)
        assert proof["plan"]["plan_id"] == metadata["plan_id"]
        assert proof["sources"] == metadata["sources"] and proof["files"] == metadata["files"]
        assert proof["same_sdk_http"] and proof["old_owner_fenced"]
        assert proof["replay_calls"] == proof["provider_calls"] == 0
    finally:
        for process in (producer, recovery):
            if process is not None and process.returncode is None:
                process.kill()
                out, error = await process.communicate()
                label = "producer" if process is producer else "recovery"
                (tmp_path / (label + "-remaining-stdout.log")).write_bytes(out)
                (tmp_path / (label + "-remaining-stderr.log")).write_bytes(error)
        await database.close()
