"""项目外wheel真实树发布、冷读取与两个来源SIGKILL恢复，零重放。"""

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


async def run(script, phase, source, root):
    if "--public" in sys.argv and phase == "complete":
        phase = "public-complete"
    elif "--worker" in sys.argv and phase == "complete":
        phase = "worker-complete"
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-I",
        str(script),
        phase,
        source,
        str(root),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, errors = await asyncio.wait_for(process.communicate(), 45)
        assert process.returncode == 0, errors.decode()
        result = json.loads(stdout)
        assert "/site-packages/" in result["package"]
        return result
    finally:
        if process.returncode is None:
            process.kill()
            await process.communicate()


async def main():
    assert "/site-packages/" in str(krim_agent.__file__)
    script = Path(__file__).with_name("workspace_tree_fence_process.py")
    results = []
    with TemporaryDirectory(
        prefix="krim-tree-fences-wheel-publish-", dir="/data/tmp/root"
    ) as folder:
        root = Path(folder)
        (root / "files").mkdir()
        published = await run(script, "complete", "file", root)
        assert published["view"]["status"] == "done" and published["view"]["done_count"] == 3
        cold = await run(script, "read", "file", root)
        assert (
            cold["view"] == published["view"]
            and cold["replay_calls"] == cold["provider_calls"] == 0
        )
        results.append(
            {
                "full_tree_publication": True,
                "separate_process_cold_read": True,
                "counts": cold["counts"],
                "zero_replay": True,
            }
        )
    for source in ("directory", "file"):
        with TemporaryDirectory(
            prefix=f"krim-tree-fences-wheel-{source}-", dir="/data/tmp/root"
        ) as folder:
            root = Path(folder)
            (root / "files").mkdir()
            child = await asyncio.create_subprocess_exec(
                sys.executable,
                "-I",
                str(script),
                "public-after-write"
                if "--public" in sys.argv
                else "worker-after-write"
                if "--worker" in sys.argv
                else "after-write",
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
                assert "/site-packages/" in metadata["package"] and metadata["pid"] == child.pid
                assert metadata["provider_calls"] == 0
                position = 0 if source == "directory" else 2
                original = metadata["steps"]["items"][position]["operation"]
                assert original["status"] == "running"
                child.kill()
                _, errors = await asyncio.wait_for(child.communicate(), 10)
                assert child.returncode == -9, errors.decode()
                async with database.sessions.begin() as sql:
                    model = (
                        WorkspaceEditAttemptRow
                        if source == "file"
                        else WorkspaceDirectoryAttemptRow
                    )
                    for current, predicate in (
                        (AgentHandleRow, AgentHandleRow.agent_id == metadata["agent_id"]),
                        (WorkspaceTreePlanRow, WorkspaceTreePlanRow.plan_id == metadata["plan_id"]),
                        (model, model.operation_id == original["operation_id"]),
                    ):
                        await sql.execute(
                            update(current)
                            .where(predicate)
                            .values(lease_expires_at=time.time() - 1)
                        )
                recovered = await run(script, "recover", source, root)
                assert recovered["view"]["done_count"] == position + 1
                assert recovered["replay_calls"] == recovered["provider_calls"] == 0
                results.append(
                    {
                        "source": source,
                        "sigkill_after_original_effect": True,
                        "controlled_lease_expiry": True,
                        "original_operation_reconciled": True,
                        "done_count": position + 1,
                        "zero_replay": True,
                    }
                )
            finally:
                if child.returncode is None:
                    child.kill()
                    await child.communicate()
                await database.close()
    print(
        json.dumps(
            {
                "package": krim_agent.__file__,
                "schema": "0041_workspace_tree_plans",
                "worker": "--worker" in sys.argv,
                "public": "--public" in sys.argv,
                "results": results,
            }
        )
    )


asyncio.run(main())
