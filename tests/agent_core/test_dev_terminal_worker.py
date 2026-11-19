from __future__ import annotations

import asyncio
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest
import pytest_asyncio

from krim_agent.dev_terminal.worker import Supervisor, canonical_digest, rpc


@pytest_asyncio.fixture
async def worker(tmp_path: Path):
    project = tmp_path / "project"
    project.mkdir()
    (project / "nested").mkdir()
    supervisor = Supervisor(tmp_path / "state", project, allow_same_uid_for_tests=True)
    try:
        yield supervisor
    finally:
        await supervisor.close()


def payload(command: str, process_id: str = "process-1", **overrides):
    request = {
        "command": command, "cwd": ".", "pty": False,
        "lifecycle": "run", "timeout_seconds": 10.0,
        "ownership": {"owner_id": "owner", "session_id": "session", "run_id": "run"},
        **overrides,
    }
    return {"action": "submit", "process_id": process_id,
            "request": request, "request_digest": canonical_digest(request)}


def action(submit, name, **values):
    return {"action": name, "process_id": submit["process_id"],
            "request_digest": submit["request_digest"], **values}


async def done(worker, submit):
    await asyncio.wait_for(worker.processes[submit["process_id"]].finished.wait(), 5)
    return await worker.dispatch(action(submit, "inspect"))


async def output_contains(worker, submit, expected):
    for _ in range(200):
        page = await worker.dispatch(action(submit, "output", cursor=0))
        if expected in page["text"]:
            return page
        await asyncio.sleep(0.01)
    pytest.fail(f"missing output {expected!r}: {page}")


@pytest.mark.asyncio
async def test_real_shell_cwd_exit_and_stdout_stderr(worker):
    submit = payload("pwd; printf out; printf err >&2; exit 7", cwd="nested")
    await worker.dispatch(submit)
    status = await done(worker, submit)
    assert status["state"] == "exited" and status["exit_code"] == 7
    page = await worker.dispatch(action(submit, "output", cursor=0))
    assert str(worker.project / "nested") in page["text"]
    assert "outerr" in page["text"]
    assert page["eof"]


@pytest.mark.asyncio
async def test_real_process_large_output_is_preserved_and_paged(worker):
    import hashlib

    # A few MiB exceed the old default log capacity without stressing the host.
    size = 3 * 1024 * 1024 + 17
    submit = payload(f"/usr/bin/python3 -I -c 'import os; os.write(1, b\"x\" * {size})'",
                     timeout_seconds=None)
    await worker.dispatch(submit)
    assert (await done(worker, submit))["exit_code"] == 0
    cursor = 0
    digest = hashlib.sha256()
    while True:
        page = await worker.dispatch(action(submit, "output", cursor=cursor))
        assert not page["truncated"]
        digest.update(page["text"].encode())
        assert page["next_cursor"] > cursor
        cursor = page["next_cursor"]
        if page["eof"]:
            break
    assert cursor == size and digest.hexdigest() == hashlib.sha256(b"x" * size).hexdigest()
    assert worker.processes[submit["process_id"]].log.start == 0


@pytest.mark.asyncio
async def test_pipe_stdin_and_eof(worker):
    submit = payload("cat; printf eof")
    await worker.dispatch(submit)
    await worker.dispatch(action(submit, "stdin", data="hello\n", close=True))
    assert (await done(worker, submit))["exit_code"] == 0
    page = await worker.dispatch(action(submit, "output", cursor=0))
    assert page["text"] == "hello\neof"
    with pytest.raises(ValueError, match="closed"):
        await worker.dispatch(action(submit, "stdin", data="again"))


@pytest.mark.asyncio
async def test_real_pty_is_controlling_terminal_and_eof(worker):
    submit = payload("test -t 0 && test -t 1 && printf tty; cat; printf finished", pty=True)
    await worker.dispatch(submit)
    await output_contains(worker, submit, "tty")
    await worker.dispatch(action(submit, "stdin", data="hello\n"))
    await output_contains(worker, submit, "hello")
    await worker.dispatch(action(submit, "stdin", data="", close=True))
    assert (await done(worker, submit))["exit_code"] == 0
    assert "finished" in (await worker.dispatch(action(submit, "output", cursor=0)))["text"]


@pytest.mark.asyncio
async def test_real_pty_ctrl_c_interrupts_foreground_group(worker):
    submit = payload("printf ready; sleep 30", pty=True)
    await worker.dispatch(submit)
    await output_contains(worker, submit, "ready")
    await worker.dispatch(action(submit, "stdin", data="\x03"))
    status = await done(worker, submit)
    assert status["state"] == "exited"
    assert status["exit_code"] != 0


def is_running(pid):
    path = Path(f"/proc/{pid}/stat")
    try:
        return path.read_text().split(")", 1)[1].split()[0] != "Z"
    except FileNotFoundError:
        return False


@pytest.mark.asyncio
@pytest.mark.parametrize("how", ["cancel", "deadline", "shell_exit"])
async def test_process_group_cleanup_including_term_ignoring_child(worker, how):
    suffix = "wait" if how != "shell_exit" else "exit 0"
    command = f"sh -c 'trap \"\" TERM; echo $$ > child.pid; sleep 60' & {suffix}"
    submit = payload(command, timeout_seconds=0.3 if how == "deadline" else 10)
    await worker.dispatch(submit)
    for _ in range(100):
        if (worker.project / "child.pid").exists():
            break
        await asyncio.sleep(0.01)
    pid = int((worker.project / "child.pid").read_text())
    if how == "cancel":
        await worker.dispatch(action(submit, "cancel"))
    status = await done(worker, submit)
    assert not is_running(pid)
    if how != "shell_exit":
        assert status["state"] == "cancelled"
    if how == "deadline":
        assert status["reason"] == "timeout"


@pytest.mark.asyncio
async def test_duplicate_process_never_executes_again_and_drift_rejected(worker):
    submit = payload("echo once >> effect")
    first = await worker.dispatch(submit)
    await done(worker, submit)
    second = await worker.dispatch(submit)
    assert first["process_id"] == second["process_id"]
    assert (worker.project / "effect").read_text() == "once\n"
    with pytest.raises(ValueError, match="binding"):
        await worker.dispatch(payload("echo twice >> effect"))
    with pytest.raises(ValueError, match="binding"):
        await worker.dispatch({**action(submit, "output", cursor=0), "request_digest": "0" * 64})


@pytest.mark.asyncio
async def test_run_without_deadline_retains_control_and_cancel(worker):
    submit = payload("printf ready; cat; printf finished", timeout_seconds=None)
    await worker.dispatch(submit)
    await output_contains(worker, submit, "ready")
    await worker.dispatch(action(submit, "stdin", data="中🙂\n", close=True))
    assert (await done(worker, submit))["exit_code"] == 0
    assert (await worker.dispatch(action(submit, "output", cursor=0)))["text"] == (
        "ready中🙂\nfinished"
    )
    second = payload("printf cancellable; sleep 30", "no-deadline", timeout_seconds=None)
    await worker.dispatch(second)
    await output_contains(worker, second, "cancellable")
    assert (await worker.dispatch(action(second, "cancel")))["state"] == "cancelled"


@pytest.mark.asyncio
async def test_continuous_output_without_deadline_remains_cancellable(worker):
    submit = payload("while :; do printf 'continuous output\\n'; done", timeout_seconds=None)
    await worker.dispatch(submit)
    await output_contains(worker, submit, "continuous output")
    stopped = await asyncio.wait_for(worker.dispatch(action(submit, "cancel")), 5)
    assert stopped["state"] == "cancelled"
    page = await worker.dispatch(action(submit, "output", cursor=0))
    assert not page["truncated"] and page["next_cursor"] > 0


@pytest.mark.asyncio
async def test_explicit_deadline_over_one_day_is_valid(worker):
    submit = payload("printf finished", timeout_seconds=172800)
    await worker.dispatch(submit)
    assert (await done(worker, submit))["exit_code"] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("deadline_value", [0, -1, float("inf"), float("nan"), True, "60"])
async def test_deadline_validation_remains_strict(worker, deadline_value):
    with pytest.raises(ValueError, match="timeout"):
        await worker.dispatch(payload("printf invalid", timeout_seconds=deadline_value))
    assert not worker.processes


@pytest.mark.asyncio
async def test_more_than_previous_active_and_receipt_limits_without_spawn(worker, monkeypatch):
    from types import SimpleNamespace

    from krim_agent.dev_terminal.worker import Process

    # Exercise admission against old thresholds without spawning thousands of children.
    worker.processes.update({
        f"existing-{index}": SimpleNamespace(record={"state": "running", "request": {}})
        for index in range(4100)
    })
    monkeypatch.setattr(Process, "launch", lambda self: self.record.update(state="running"))
    submit = payload("printf admitted", "new-process", timeout_seconds=None)
    try:
        assert (await worker.dispatch(submit))["state"] == "running"
        assert len(worker.processes) == 4101
    finally:
        process = worker.processes["new-process"]
        process.record["state"] = "exited"
        process.finished.set()
        process.reader_done.set()
        worker.processes = {"new-process": process}


@pytest.mark.asyncio
async def test_run_stop_preserves_service_environment_stop_covers_it(worker):
    run = payload("sleep 30", "run-process")
    service = payload(
        "sleep 30", "service-process", lifecycle="project_service", timeout_seconds=None,
    )
    await worker.dispatch(run)
    await worker.dispatch(service)
    await worker.dispatch({"action": "stop_run", "ownership": run["request"]["ownership"]})
    assert (await worker.dispatch(action(run, "inspect")))["state"] == "cancelled"
    assert (await worker.dispatch(action(service, "inspect")))["state"] == "running"
    with pytest.raises(ValueError, match="run is stopped"):
        await worker.dispatch(payload("echo late-run", "late-run"))
    await worker.dispatch({"action": "stop_environment"})
    assert (await worker.dispatch(action(service, "inspect")))["state"] == "cancelled"
    with pytest.raises(ValueError, match="stopping"):
        await worker.dispatch(payload("echo late", "late"))


@pytest.mark.asyncio
async def test_reject_symlink_cwd_escape(worker, tmp_path):
    (worker.project / "escape").symlink_to(tmp_path, target_is_directory=True)
    with pytest.raises(ValueError):
        await worker.dispatch(payload("touch escaped", cwd="escape"))
    assert not (tmp_path / "escaped").exists()


def start_server(root, project):
    return subprocess.Popen(
        [sys.executable, "-m", "krim_agent.dev_terminal.worker", "serve",
         "--state", str(root), "--project", str(project), "--allow-same-uid-for-tests"],
        stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
    )


def wait_server(root, process):
    for _ in range(200):
        if process.poll() is not None:
            pytest.fail(process.stderr.read().decode())
        try:
            if rpc(root, {"action": "ping"})["ok"]:
                return
        except (OSError, ConnectionError):
            pass
        time.sleep(0.01)
    pytest.fail("worker not ready")


def test_new_client_and_new_worker_recovery_never_replays(tmp_path):
    root = tmp_path / "state"
    project = tmp_path / "project"
    project.mkdir()
    server = start_server(root, project)
    wait_server(root, server)
    submit = payload("echo once >> effect; sleep 30")
    child_pid = None
    try:
        assert rpc(root, submit)["result"]["state"] == "running"
        for _ in range(100):
            if (project / "effect").exists():
                break
            time.sleep(0.01)
        # A distinct Python client reaches the surviving supervisor/receipt.
        read = subprocess.run(
            [sys.executable, "-m", "krim_agent.dev_terminal.worker", "rpc", "--state", str(root)],
            input=json.dumps(action(submit, "inspect")), text=True, capture_output=True, check=True,
        )
        assert json.loads(read.stdout)["result"]["state"] == "running"
        receipt = json.loads(next(root.glob("*.json")).read_text())
        child_pid = receipt["pid"]
        server.kill()
        server.wait(timeout=5)
        server = start_server(root, project)
        wait_server(root, server)
        result = rpc(root, submit)["result"]
        assert result["state"] == "unknown"
        assert (project / "effect").read_text() == "once\n"
    finally:
        if child_pid is not None:
            try:
                os.killpg(child_pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        server.terminate()
        server.wait(timeout=5)
        if server.stderr:
            server.stderr.close()


def test_production_rejects_shell_same_uid(tmp_path):
    with pytest.raises(PermissionError, match="distinct"):
        Supervisor(tmp_path / "state", tmp_path)
