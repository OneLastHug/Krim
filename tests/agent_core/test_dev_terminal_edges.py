from __future__ import annotations

import asyncio
import json

import pytest

from krim_agent.dev_terminal.worker import Supervisor, canonical_digest


def submit(command, **updates):
    request = {"command": command, "cwd": ".", "pty": False, "lifecycle": "run",
               "timeout_seconds": 3, "ownership": {"run_id": "r"}, **updates}
    return {"action": "submit", "process_id": "p", "request": request,
            "request_digest": canonical_digest(request)}


def control(start, action, **updates):
    return {"action": action, "process_id": "p", "request_digest": start["request_digest"],
            **updates}


async def wait_text(worker, start, text):
    for _ in range(100):
        page = await worker.dispatch(control(start, "output", cursor=0))
        if text in page["text"]:
            return
        await asyncio.sleep(0.01)
    pytest.fail(f"missing {text}")


@pytest.mark.asyncio
async def test_partial_line_pty_eof_and_raw_mode_rejection(tmp_path):
    project = tmp_path / "project"
    project.mkdir()
    worker = Supervisor(tmp_path / "state", project, allow_same_uid_for_tests=True)
    start = submit("printf ready; cat; printf done", pty=True)
    try:
        await worker.dispatch(start)
        await wait_text(worker, start, "ready")
        await worker.dispatch(control(start, "stdin", data="no-newline", close=True))
        await asyncio.wait_for(worker.processes["p"].finished.wait(), 5)
        page = await worker.dispatch(control(start, "output", cursor=0))
        assert "no-newline" in page["text"] and "done" in page["text"]
    finally:
        await worker.close()
    raw = Supervisor(tmp_path / "raw-state", project, allow_same_uid_for_tests=True)
    start = submit("stty raw -echo; printf raw-ready; sleep 30", pty=True)
    try:
        await raw.dispatch(start)
        await wait_text(raw, start, "raw-ready")
        with pytest.raises(ValueError, match="canonical"):
            await raw.dispatch(control(start, "stdin", data="", close=True))
    finally:
        await raw.close()


@pytest.mark.asyncio
async def test_concurrent_cancel_and_submit_settle_once(tmp_path):
    project = tmp_path / "project"
    project.mkdir()
    worker = Supervisor(tmp_path / "state", project, allow_same_uid_for_tests=True)
    start = submit("sleep 30")
    try:
        await worker.dispatch(start)
        cancel = asyncio.create_task(worker.dispatch(control(start, "cancel")))
        await asyncio.sleep(0.02)
        await worker.dispatch(control(start, "output", cursor=0))
        record = json.loads(next((tmp_path / "state").glob("*.json")).read_text())
        assert record["state"] == "running"
        replies = await asyncio.gather(
            cancel,
            worker.dispatch(control(start, "cancel")),
            worker.dispatch(control(start, "cancel")),
            worker.dispatch(start),
        )
        assert replies[0]["state"] == replies[1]["state"] == "cancelled"
        assert replies[0]["exit_code"] == replies[1]["exit_code"]
        assert len(worker.processes) == 1
        assert worker.processes["p"].finished.is_set()
    finally:
        await worker.close()


@pytest.mark.asyncio
async def test_large_live_log_truncates_and_checkpoints_returned_cursor(tmp_path):
    project = tmp_path / "project"
    project.mkdir()
    state = tmp_path / "state"
    worker = Supervisor(state, project, allow_same_uid_for_tests=True, log_bytes=64, page_bytes=8)
    start = submit("printf '%0200d' 0; printf marker; sleep 1")
    try:
        await worker.dispatch(start)
        for _ in range(100):
            if worker.processes["p"].log.end >= 206:
                break
            await asyncio.sleep(0.01)
        page = await worker.dispatch(control(start, "output", cursor=0))
        assert page["truncated"]
        assert page["next_cursor"] == 150
        persisted = json.loads(next(state.glob("*.json")).read_text())
        assert persisted["log_end"] >= page["next_cursor"]
        assert next(state.glob("*.log")).stat().st_size <= 64
        await worker.dispatch(control(start, "cancel"))
    finally:
        await worker.close()


@pytest.mark.asyncio
async def test_unknown_log_eof_does_not_claim_process_exited(tmp_path):
    from krim_agent.dev_terminal.worker import atomic_json, checked_id

    project = tmp_path / "project"
    project.mkdir()
    state = tmp_path / "state"
    state.mkdir(mode=0o700)
    start = submit("echo once")
    record = {**start, "state": "running", "exit_code": None, "log_start": 0, "log_end": 8}
    atomic_json(state / f"{checked_id('p')}.json", record)
    (state / f"{checked_id('p')}.log").write_bytes(b"oldbytes")
    worker = Supervisor(state, project, allow_same_uid_for_tests=True)
    try:
        page = await worker.dispatch(control(start, "output", cursor=0))
        assert page["truncated"] and page["eof"] and page["text"] == ""
        assert page["next_cursor"] == 8
        assert (await worker.dispatch(control(start, "inspect")))["state"] == "unknown"
        assert (await worker.dispatch(start))["state"] == "unknown"
    finally:
        await worker.close()
