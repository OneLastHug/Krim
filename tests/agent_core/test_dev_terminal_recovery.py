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
async def test_append_log_recovers_tail_without_replaying_unknown_process(tmp_path: Path):
    project = tmp_path / "project"
    project.mkdir()
    state = tmp_path / "state"
    state.mkdir(mode=0o700)
    command = request()
    key = checked_id("append-crash")
    record = {"process_id": "append-crash", "request": command,
              "request_digest": canonical_digest(command), "state": "running",
              "exit_code": None, "log_start": 0, "log_end": 3,
              "log_format": "append-v1", "log_capacity": None}
    atomic_json(state / f"{key}.json", record)
    output = "old中🙂 plus uncheckpointed tail".encode()
    (state / f"{key}.log").write_bytes(output)
    supervisor = Supervisor(state, project, allow_same_uid_for_tests=True)
    try:
        control = {"process_id": "append-crash", "request_digest": record["request_digest"]}
        page = await supervisor.dispatch({**control, "action": "output", "cursor": 0})
        assert page["text"].encode() == output
        assert not page["truncated"] and page["eof"]
        assert page["next_cursor"] == len(output)
        assert (await supervisor.dispatch({**control, "action": "inspect"}))["state"] == "unknown"
        assert (await supervisor.dispatch({**record, "action": "submit"}))["state"] == "unknown"
        assert not (project / "effect").exists()
    finally:
        await supervisor.close()


@pytest.mark.asyncio
async def test_fresh_operation_rejects_an_orphan_log(tmp_path: Path):
    project = tmp_path / "project"
    project.mkdir()
    state = tmp_path / "state"
    state.mkdir(mode=0o700)
    key = checked_id("orphan")
    path = state / f"{key}.log"
    path.write_bytes(b"other operation output")
    supervisor = Supervisor(state, project, allow_same_uid_for_tests=True)
    command = request()
    try:
        with pytest.raises(ValueError, match="without a receipt"):
            await supervisor.dispatch({"action": "submit", "process_id": "orphan",
                                       "request": command,
                                       "request_digest": canonical_digest(command)})
        assert path.read_bytes() == b"other operation output"
        assert not (project / "effect").exists()
    finally:
        await supervisor.close()


@pytest.mark.asyncio
async def test_invalid_log_metadata_releases_worker_lock(tmp_path: Path):
    project = tmp_path / "project"
    project.mkdir()
    state = tmp_path / "state"
    state.mkdir(mode=0o700)
    command = request()
    path = state / f"{checked_id('bad-format')}.json"
    record = {"process_id": "bad-format", "request": command,
              "request_digest": canonical_digest(command), "state": "exited",
              "exit_code": 0, "log_start": 0, "log_end": 0,
              "log_format": "unrecognized"}
    atomic_json(path, record)
    with pytest.raises(ValueError, match="format"):
        Supervisor(state, project, allow_same_uid_for_tests=True)
    path.unlink()
    recovered = Supervisor(state, project, allow_same_uid_for_tests=True)
    await recovered.close()


@pytest.mark.asyncio
async def test_completed_legacy_ring_is_not_reinterpreted_as_append(tmp_path: Path):
    from krim_agent.dev_terminal.byte_log import ByteLog

    project = tmp_path / "project"
    project.mkdir()
    state = tmp_path / "state"
    state.mkdir(mode=0o700)
    command = request()
    key = checked_id("legacy")
    log = ByteLog(state / f"{key}.log", 8)
    log.append(b"abcdefghijkl")
    log.flush()
    record = {"process_id": "legacy", "request": command,
              "request_digest": canonical_digest(command), "state": "exited",
              "exit_code": 0, "log_start": log.start, "log_end": log.end}
    log.close()
    atomic_json(state / f"{key}.json", record)
    supervisor = Supervisor(state, project, allow_same_uid_for_tests=True, log_bytes=8)
    try:
        page = await supervisor.dispatch({"action": "output", "process_id": "legacy",
                                          "request_digest": record["request_digest"], "cursor": 0})
        assert page["text"] == "efghijkl" and page["truncated"]
        assert page["next_cursor"] == 12 and page["eof"]
        stored = json.loads((state / f"{key}.json").read_text())
        assert stored["log_format"] == "ring-v1" and stored["log_capacity"] == 8
    finally:
        await supervisor.close()


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
