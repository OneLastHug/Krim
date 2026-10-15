"""真实进程竞争只存一条；目录已移走后新进程仍按原材料零重放。"""

import asyncio
import json
import shutil
import sys
from pathlib import Path

from test_directory_checkpoint_materials import count, directory

from krim_agent.persistence.workspace_directory_checkpoints import WorkspaceDirectoryCheckpointStore
from krim_agent.persistence.workspace_edits import WorkspaceEditStore


async def test_two_actual_processes_same_material_and_new_process_cold_read_zero_replay(
    repository, tmp_path
):
    root = tmp_path / "files"
    directory(root)
    topic = await repository.create_topic("目录材料跨进程")
    scope = "topic:" + topic.id
    script = Path(__file__).parent / "fixtures/directory_checkpoint_material_process.py"
    args = [str(repository.database.engine.url), str(root), scope, "original-process"]
    children = []
    try:
        for _index in range(2):
            children.append(
                await asyncio.create_subprocess_exec(
                    sys.executable,
                    "-I",
                    str(script),
                    "save",
                    *args,
                    stdin=asyncio.subprocess.PIPE,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
            )
        ready = []
        for child in children:
            line = await asyncio.wait_for(child.stdout.readline(), 15)
            if not line:
                _out, err = await child.communicate()
                raise AssertionError(err.decode())
            ready.append(json.loads(line))
        assert [x["ready"] for x in ready] == [p.pid for p in children]
        assert ready[0]["snapshot"] == ready[1]["snapshot"]
        for child in children:
            child.stdin.write(b"save\n")
            await child.stdin.drain()
        results = []
        for child in children:
            out, err = await asyncio.wait_for(child.communicate(), 15)
            assert child.returncode == 0, err.decode()
            results.append(json.loads(out))
        assert results[0]["id"] == results[1]["id"]
        assert sorted(x["created"] for x in results) == [False, True]
        store = WorkspaceDirectoryCheckpointStore(WorkspaceEditStore(repository.database))
        assert await count(store) == 1
        shutil.rmtree(root / "tree")
        cold = await asyncio.create_subprocess_exec(
            sys.executable,
            "-I",
            str(script),
            "read",
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        children.append(cold)
        out, err = await asyncio.wait_for(cold.communicate(), 15)
        assert cold.returncode == 0, err.decode()
        queried = json.loads(out)
        assert queried["id"] == results[0]["id"]
        assert queried["snapshot"] == ready[0]["snapshot"]
        assert queried["binary_hex"] == "00ff" and queried["mode"] == 0o640
        assert queried["replay_calls"] == 0 and not (root / "tree").exists()
        print(json.dumps({"created": results, "cold": queried}))
    finally:
        for child in children:
            if child.returncode is None:
                child.kill()
                await child.communicate()
