import asyncio
import json
import selectors
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from krim_agent.persistence import Database, KrimRepository

pytestmark = pytest.mark.skipif(
    not sys.platform.startswith("linux") or shutil.which("bwrap") is None,
    reason="需要真实Linux bubblewrap后端",
)
WORKER = Path(__file__).with_name("fixtures") / "durable_command_worker.py"


async def prepare(path):
    database = Database(f"sqlite+aiosqlite:///{path}")
    try:
        await database.initialize()
        topic = await KrimRepository(database).create_topic("cross process")
        return "topic:" + topic.id
    finally:
        await database.close()


def test_two_real_processes_race_for_one_command(tmp_path):
    path = tmp_path / "race.sqlite"
    scope = asyncio.run(prepare(path))
    processes = [
        subprocess.Popen(
            [sys.executable, str(WORKER), str(path), scope, "write"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for _ in range(2)
    ]
    try:
        reports = []
        for process in processes:
            output, error = process.communicate(timeout=25)
            assert process.returncode == 0, output + error
            reports.append(json.loads(output))
        assert reports[0]["command_id"] == reports[1]["command_id"]
        assert reports[0]["stdout"] == reports[1]["stdout"]
        assert sum(item["calls"] for item in reports) == 1
        assert all(item["outcome"] == "completed" and item["binary_artifact"] for item in reports)
        assert sum(event["kind"] == "dispatched" for event in reports[0]["events"]) == 1
    finally:
        for process in processes:
            process.kill()
            process.wait()


def test_crashed_producer_is_read_by_another_process_without_replay(tmp_path):
    path = tmp_path / "crash.sqlite"
    scope = asyncio.run(prepare(path))
    process = subprocess.Popen(
        [sys.executable, str(WORKER), str(path), scope, "crash"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    selector = selectors.DefaultSelector()
    selector.register(process.stdout, selectors.EVENT_READ)
    try:
        assert selector.select(15)
        line = process.stdout.readline()
        assert line, process.stderr.read()
        started = json.loads(line)
        process.kill()
        process.wait(timeout=5)

        async def expire():
            await asyncio.sleep(1.7)

        asyncio.run(expire())
        reader = subprocess.run(
            [sys.executable, str(WORKER), str(path), scope, "read"],
            capture_output=True,
            text=True,
            timeout=20,
        )
        assert reader.returncode == 0, reader.stdout + reader.stderr
        report = json.loads(reader.stdout)
        assert report["command_id"] == started["command_id"]
        assert (
            report["calls"] == 0
            and report["phase"] == "interrupted"
            and report["outcome"] == "uncertain"
        )
        assert report["dispatched"] and report["started"] and not report["processes_reaped"]
        assert report["stdout"] == "ready\n"
        assert report["events"][-1]["kind"] == "interrupted"
    finally:
        process.kill()
        process.wait()
        selector.close()
        process.stdout.close()
        process.stderr.close()
