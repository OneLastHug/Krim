"""Crash windows use actual Linux workers, never mocked process execution."""
from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from krim_agent.dev_terminal.worker import Supervisor, atomic_json, canonical_digest, checked_id


def request():
    return {"command": "echo side-effect >> effect", "cwd": ".", "pty": False,
            "lifecycle": "run", "timeout_seconds": 10.0,
            "ownership": {"owner_id": "owner", "session_id": "session", "run_id": "run"}}


@pytest.mark.asyncio
async def test_durable_accepted_crash_window_does_not_spawn(tmp_path: Path):
    project = tmp_path / "project"
    project.mkdir()
    state = tmp_path / "state"
    state.mkdir(mode=0o700)
    command = request()
    record = {"process_id": "crash", "request": command,
              "request_digest": canonical_digest(command), "state": "accepted",
              "exit_code": None, "log_start": 0, "log_end": 0}
    atomic_json(state / f"{checked_id('crash')}.json", record)
    supervisor = Supervisor(state, project, allow_same_uid_for_tests=True)
    try:
        reply = await supervisor.dispatch({"action": "submit", **record})
        assert reply["state"] == "unknown"
        assert not (project / "effect").exists()
        stored = json.loads((state / f"{checked_id('crash')}.json").read_text())
        assert stored["state"] == "unknown"
    finally:
        await supervisor.close()


@pytest.mark.asyncio
async def test_completed_receipt_and_log_recover_without_execution(tmp_path: Path):
    project = tmp_path / "project"
    project.mkdir()
    state = tmp_path / "state"
    command = {**request(), "command": "printf retained; echo once >> effect"}
    submit = {"action": "submit", "process_id": "complete", "request": command,
              "request_digest": canonical_digest(command)}
    supervisor = Supervisor(state, project, allow_same_uid_for_tests=True)
    await supervisor.dispatch(submit)
    await asyncio.wait_for(supervisor.processes["complete"].finished.wait(), 5)
    await supervisor.close()
    recovered = Supervisor(state, project, allow_same_uid_for_tests=True)
    try:
        assert (await recovered.dispatch(submit))["state"] == "exited"
        page = await recovered.dispatch({"action": "output", "process_id": "complete",
                                         "request_digest": submit["request_digest"], "cursor": 0})
        assert page["text"] == "retained" and page["eof"]
        assert (project / "effect").read_text() == "once\n"
    finally:
        await recovered.close()


@pytest.mark.asyncio
async def test_overlapping_worker_start_rejected(tmp_path: Path):
    project = tmp_path / "project"
    project.mkdir()
    state = tmp_path / "state"
    supervisor = Supervisor(state, project, allow_same_uid_for_tests=True)
    try:
        with pytest.raises(BlockingIOError):
            Supervisor(state, project, allow_same_uid_for_tests=True)
    finally:
        await supervisor.close()
