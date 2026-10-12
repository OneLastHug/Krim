"""当前wheel实际模型闭环、两来源SIGKILL后原操作核实与零重放。"""

import argparse
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
    WorkspaceDirectoryAttemptRow,
    WorkspaceEditAttemptRow,
    WorkspaceTreePlanRow,
)

SCRIPT = Path(__file__).with_name("model_tree_tools_process.py")


async def run(phase, source, root):
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-I",
        str(SCRIPT),
        phase,
        source,
        str(root),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        output, errors = await asyncio.wait_for(process.communicate(), 60)
        assert process.returncode == 0, errors.decode()
        return json.loads(output)
    finally:
        if process.returncode is None:
            process.kill()
            await process.communicate()


async def main():
    assert "/site-packages/" in str(krim_agent.__file__)
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-complete", action="store_true")
    args = parser.parse_args()
    results = []
    if not args.skip_complete:
        with TemporaryDirectory(
            prefix="krim-model-tree-tools-complete-", dir="/data/tmp/root"
        ) as folder:
            root = Path(folder)
            (root / "files").mkdir()
            produced = await run("complete", "file", root)
            cold = await run("cold", "file", root)
            assert produced["provider_calls"] == 4 and cold["zero_replay"]
            assert produced["view"] == cold["view"] and produced["sources"] == cold["sources"]
            results.append({"phase": "complete-cold", "produced": produced, "cold": cold})
            print(json.dumps({"phase": "complete-cold", "verified": True}), flush=True)
    for source in ("directory", "file"):
        with TemporaryDirectory(
            prefix=f"krim-model-tree-tools-{source}-", dir="/data/tmp/root"
        ) as folder:
            root = Path(folder)
            (root / "files").mkdir()
            process = await asyncio.create_subprocess_exec(
                sys.executable,
                "-I",
                str(SCRIPT),
                "after-write",
                source,
                str(root),
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            database = Database(f"sqlite+aiosqlite:///{root / 'state.sqlite'}")
            try:
                line = await asyncio.wait_for(process.stdout.readline(), 35)
                if not line:
                    _output, errors = await process.communicate()
                    raise AssertionError(errors.decode())
                metadata = json.loads(line)
                assert metadata["pid"] == process.pid and metadata["provider_calls"] == 2
                position = 0 if source == "directory" else 2
                attempt = metadata["steps"]["items"][position]["operation"]
                assert attempt["status"] == "running"
                process.kill()
                _output, errors = await asyncio.wait_for(process.communicate(), 10)
                assert process.returncode == -9, errors.decode()
                model = (
                    WorkspaceDirectoryAttemptRow
                    if source == "directory"
                    else WorkspaceEditAttemptRow
                )
                async with database.sessions.begin() as sql:
                    for table, predicate in (
                        (AgentHandleRow, AgentHandleRow.agent_id == metadata["agent_id"]),
                        (
                            WorkspaceTreePlanRow,
                            WorkspaceTreePlanRow.plan_id == metadata["view"]["plan_id"],
                        ),
                        (model, model.operation_id == attempt["operation_id"]),
                    ):
                        await sql.execute(
                            update(table).where(predicate).values(lease_expires_at=time.time() - 1)
                        )
                recovered = await run("recover", source, root)
                assert recovered["zero_replay"] and recovered["sources"] == metadata["sources"]
                results.append(
                    {
                        "phase": source + "-sigkill",
                        "sigkill_after_original_effect": True,
                        "controlled_test_lease_expiry": True,
                        "metadata": metadata,
                        "recovered": recovered,
                    }
                )
            finally:
                if process.returncode is None:
                    process.kill()
                    await process.communicate()
                await database.close()
    print(json.dumps({"package": str(krim_agent.__file__), "schema": "0042", "results": results}))


asyncio.run(main())
