"""项目外wheel独立生产者SIGKILL、冷重开与原操作核实，禁止重发Writer。"""

import asyncio
import json
import sys
import time
from pathlib import Path
from tempfile import TemporaryDirectory

from sqlalchemy import update

import krim_agent
from krim_agent.persistence.database import (
    AgentHandleRow,
    Database,
    MixedWorkspaceBatchRow,
    WorkspaceDirectoryAttemptRow,
    WorkspaceEditAttemptRow,
)


async def main():
    assert "/site-packages/" in str(krim_agent.__file__)
    script = Path(__file__).with_name("mixed_batch_worker_process.py")
    results = []
    for source in ("file", "directory"):
        with TemporaryDirectory(
            prefix=f"krim-mixed-worker-wheel-{source}-", dir="/data/tmp/root"
        ) as folder:
            root = Path(folder)
            (root / "files").mkdir()
            child = await asyncio.create_subprocess_exec(
                sys.executable,
                "-I",
                str(script),
                "after-write",
                source,
                str(root),
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            database = Database(f"sqlite+aiosqlite:///{root / 'state.sqlite'}")
            try:
                line = await asyncio.wait_for(child.stdout.readline(), 35)
                assert line
                metadata = json.loads(line)
                assert "/site-packages/" in metadata["package"]
                assert metadata["pid"] == child.pid and metadata["provider_calls"] == 0
                attempt = metadata["batch"]["items"][0]["operation"]
                assert attempt["status"] == "running"
                child.kill()
                _stdout, errors = await asyncio.wait_for(child.communicate(), 10)
                assert child.returncode == -9, errors.decode()
                original = (
                    WorkspaceEditAttemptRow if source == "file" else WorkspaceDirectoryAttemptRow
                )
                async with database.sessions.begin() as sql:
                    for model, predicate in (
                        (AgentHandleRow, AgentHandleRow.agent_id == metadata["agent_id"]),
                        (
                            MixedWorkspaceBatchRow,
                            MixedWorkspaceBatchRow.batch_id == metadata["batch_id"],
                        ),
                        (original, original.operation_id == attempt["operation_id"]),
                    ):
                        await sql.execute(
                            update(model)
                            .where(predicate, model.lease_expires_at.is_not(None))
                            .values(lease_expires_at=time.time() - 1)
                        )
                resumed = await asyncio.create_subprocess_exec(
                    sys.executable,
                    "-I",
                    str(script),
                    "recover",
                    source,
                    str(root),
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                try:
                    stdout, errors = await asyncio.wait_for(resumed.communicate(), 45)
                    assert resumed.returncode == 0, errors.decode()
                    recovered = json.loads(stdout)
                    assert "/site-packages/" in recovered["package"]
                    assert recovered["replays"] == recovered["provider_calls"] == 0
                    first, second = recovered["batch"]["items"]
                    assert (
                        first["status"] == "done"
                        and first["operation"]["operation_id"] == attempt["operation_id"]
                    )
                    assert second["status"] == "skipped" and second["operation"] is None
                    results.append(
                        {
                            "source": source,
                            "sigkill": True,
                            "original_operation": True,
                            "unstarted_skipped": True,
                            "zero_replay": True,
                        }
                    )
                finally:
                    if resumed.returncode is None:
                        resumed.kill()
                        await resumed.communicate()
            finally:
                if child.returncode is None:
                    child.kill()
                    await child.communicate()
                await database.close()
    print(json.dumps({"package": krim_agent.__file__, "results": results}))


asyncio.run(main())
