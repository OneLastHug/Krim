"""模型原Blob实际SQL提交后强杀与独立SDK/HTTP冷读，非伪造材料。"""

import asyncio
import json
import sys
import time
from pathlib import Path

from sqlalchemy import update

from krim_agent.persistence import Database
from krim_agent.persistence.database import AgentHandleRow, NativeWorkspaceCheckpointRow


async def test_actual_model_capture_sql_then_sigkill_preserves_source_blob_and_zero_replay(
    tmp_path,
):
    files = tmp_path / "files"
    files.mkdir()
    (files / "binary.bin").write_bytes(b"\0\xff")
    script = Path(__file__).parent / "fixtures/model_native_checkpoint_process.py"
    child = await asyncio.create_subprocess_exec(
        sys.executable,
        "-I",
        str(script),
        "produce",
        str(tmp_path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'state.sqlite'}")
    recovery = None
    try:
        line = await asyncio.wait_for(child.stdout.readline(), 20)
        if not line:
            _out, error = await child.communicate()
            raise AssertionError(error.decode())
        metadata = json.loads(line)
        assert metadata["pid"] == child.pid and metadata["record_committed"]
        assert metadata["provider_calls"] == 1 and metadata["view"]["status"] == "capturing"
        child.kill()
        await asyncio.wait_for(child.communicate(), 10)
        assert child.returncode == -9
        # 只对本测试库受控到期，不将其当作生产自然租约等待。
        async with database.write_lock, database.sessions.begin() as sql:
            await sql.execute(
                update(AgentHandleRow)
                .where(AgentHandleRow.agent_id == metadata["agent_id"])
                .values(lease_expires_at=time.time() - 1)
            )
            await sql.execute(
                update(NativeWorkspaceCheckpointRow)
                .where(NativeWorkspaceCheckpointRow.checkpoint_id == metadata["checkpoint_id"])
                .values(lease_expires_at=time.time() - 1)
            )
        recovery = await asyncio.create_subprocess_exec(
            sys.executable,
            "-I",
            str(script),
            "recover",
            str(tmp_path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        output, error = await asyncio.wait_for(recovery.communicate(), 20)
        assert recovery.returncode == 0, error.decode()
        proof = json.loads(output)
        assert proof["view"]["checkpoint_id"] == metadata["checkpoint_id"]
        assert proof["view"]["scope_id"] == metadata["scope"]
        assert proof["source"] == {"run_id": metadata["run_id"], "call_id": metadata["call_id"]}
        assert proof["view"]["status"] == "interrupted" and proof["counts"] == [1, 1]
        assert (
            proof["provider_calls"] == proof["capture_replays"] == 0 and proof["old_owner_fenced"]
        )
        assert (files / "binary.bin").read_bytes() == b"\0\xff"
    finally:
        for process in (child, recovery):
            if process is not None and process.returncode is None:
                process.kill()
                await process.communicate()
        await database.close()
