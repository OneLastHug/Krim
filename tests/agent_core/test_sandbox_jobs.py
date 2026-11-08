"""持久命令作业：原键不重放、失联取消、容量与检查点失败。"""

import time
import uuid

import pytest
from test_sandbox_broker import FakeQuota

from krim_agent.contracts import ServiceError
from krim_agent.sandbox import broker as broker_module
from krim_agent.sandbox import jobs as jobs_module
from krim_agent.sandbox.broker import StorageBroker
from krim_agent.sandbox.jobs import CommandJobs
from krim_agent.sandbox.quota import QuotaLimits


class Runner:
    identity = "sha256:" + "a" * 64
    executions = 0
    hold = False

    def __init__(self, image, installation_id):
        pass

    def run(self, operation, content, argv, wall, cancel, phase):
        type(self).executions += 1
        phase("creating", {})
        phase("started", {"executor_started": True})
        until = time.monotonic() + 8
        while type(self).hold and not cancel.wait(0.01):
            if time.monotonic() > until:
                raise RuntimeError("test exceeded bound")
        return {
            "executor_started": True,
            "exit_code": 143 if cancel.is_set() else 0,
            "cleanup": "clean",
            "output_complete": True,
            "stdout": "",
            "stderr": "",
            "code": "cancelled" if cancel.is_set() else None,
        }

    def stop(self, operation):
        return {"State": {"ExitCode": 143, "Running": False, "Pid": 0}}

    def remove(self, operation):
        pass


class Checkpoints:
    def __init__(self, root):
        pass

    def capture(self, environment, project, files, excluded, limits):
        return {"checkpoint_id": str(uuid.uuid4()), "pinned": False}

    def pin(self, *args, **kwargs):
        pass

    def reference(self, *args, **kwargs):
        pass

    def release_reference(self, *args, **kwargs):
        pass


@pytest.fixture
def command_jobs(tmp_path, monkeypatch):
    disk, state = tmp_path / "disk", tmp_path / "state"
    disk.mkdir(mode=0o700)
    state.mkdir(mode=0o700)
    monkeypatch.setattr(broker_module, "XfsAccounts", FakeQuota)
    monkeypatch.setattr(broker_module.os, "chown", lambda *args, **kwargs: None)
    monkeypatch.setattr(jobs_module, "RunscRunner", Runner)
    monkeypatch.setattr(jobs_module, "ShadowCheckpoints", Checkpoints)
    Runner.executions = 0
    Runner.hold = False
    broker = StorageBroker(disk, state)
    environment, project = str(uuid.uuid4()), str(uuid.uuid4())
    broker.provision_environment(environment, QuotaLimits().model_dump(mode="json"))
    broker.provision_project(environment, project)
    record = broker._environment(environment)
    record["checkpoint_number"] = 999
    broker._save("env-" + environment + ".json", record)
    jobs = CommandJobs(broker, Runner.identity)
    broker.jobs = jobs
    try:
        yield broker, jobs, environment, project
    finally:
        broker.close()


def command(project):
    return {
        "project_id": project,
        "expected_generation": 0,
        "argv": ["-c", "print(1)"],
        "wall_seconds": 1,
    }


def wait_result(jobs, environment, project, operation):
    until = time.monotonic() + 5
    while True:
        result = jobs.status(environment, project, operation, renew=True)
        if result["status"] in {"finished", "unknown"}:
            return result
        assert time.monotonic() < until
        time.sleep(0.01)


def test_original_operation_is_never_executed_twice(command_jobs):
    _, jobs, env, project = command_jobs
    operation = str(uuid.uuid4())
    jobs.submit(env, project, operation, command(project))
    result = wait_result(jobs, env, project, operation)
    assert result["status"] == "finished" and result["generation"] == 1
    assert result["receipt"]["checkpoint_id"]
    replay = jobs.submit(env, project, operation, command(project))
    assert replay["status"] == "finished" and Runner.executions == 1
    with pytest.raises(ServiceError, match="key_conflict"):
        jobs.submit(env, project, operation, {**command(project), "argv": ["-c", "different"]})


def test_permit_expiry_cancels_and_preserves_result(command_jobs):
    _, jobs, env, project = command_jobs
    Runner.hold = True
    operation = str(uuid.uuid4())
    jobs.submit(env, project, operation, command(project))
    with jobs.lock:
        jobs.active[operation]["permit"] = 0
    until = time.monotonic() + 5
    while True:
        result = jobs.status(env, project, operation)
        if result["status"] == "finished":
            break
        assert time.monotonic() < until
        time.sleep(0.02)
    assert result["receipt"]["code"] == "cancelled"
    assert result["receipt"]["cleanup"] == "clean" and result["generation"] == 1


def test_same_project_exclusion_and_explicit_cancellation(command_jobs):
    _, jobs, env, project = command_jobs
    Runner.hold = True
    operation = str(uuid.uuid4())
    jobs.submit(env, project, operation, command(project))
    with pytest.raises(ServiceError, match="workspace_busy"):
        jobs.submit(env, project, str(uuid.uuid4()), command(project))
    jobs.status(env, project, operation, cancel=True)
    assert wait_result(jobs, env, project, operation)["receipt"]["code"] == "cancelled"


def test_stale_receipt_is_only_stopped_not_reexecuted(command_jobs):
    broker, jobs, env, project = command_jobs
    operation = str(uuid.uuid4())
    broker._save(
        jobs.filename(operation),
        {
            "operation_id": operation,
            "environment_id": env,
            "project_id": project,
            "status": "started",
            "generation": 0,
        },
    )
    result = jobs.status(env, project, operation)
    assert result["status"] == "unknown"
    assert result["receipt"]["code"] == "execution_unknown"
    assert Runner.executions == 0


def test_failed_checkpoint_prevents_payload_and_releases_lease(command_jobs, monkeypatch):
    broker, jobs, env, project = command_jobs

    def unavailable(*args, **kwargs):
        raise ServiceError("sandbox_limit")

    monkeypatch.setattr(Checkpoints, "capture", unavailable)
    operation = str(uuid.uuid4())
    jobs.submit(env, project, operation, command(project))
    result = wait_result(jobs, env, project, operation)
    assert result["status"] == "finished" and result["receipt"]["executor_started"] is False
    assert result["receipt"]["code"] == "sandbox_limit"
    assert broker.leases.status(project)["status"] == "idle"
    assert Runner.executions == 0


def test_reconcile_stops_unknown_writer_and_explicitly_releases_lease(command_jobs):
    broker, jobs, env, project = command_jobs
    operation = str(uuid.uuid4())
    lease = broker.leases.claim(project, operation, deadline=time.time() + 60)
    lease.release(clean=False)
    broker._save(
        jobs.filename(operation),
        {
            "operation_id": operation,
            "environment_id": env,
            "project_id": project,
            "status": "unknown",
            "generation": 0,
        },
    )
    result = jobs.reconcile(env, project, operation)
    assert result["status"] == "finished" and result["reconciled"]
    assert result["receipt"]["code"] == "execution_unknown"
    assert result["receipt"]["output_complete"] is False
    assert broker.leases.status(project)["status"] == "idle"
    assert broker._project(env, project)["generation"] == 1
    assert jobs.reconcile(env, project, operation)["generation"] == 1
    assert Runner.executions == 0


def test_independent_projects_of_one_account_can_run_concurrently(command_jobs):
    broker, jobs, env, project = command_jobs
    other = str(uuid.uuid4())
    broker.provision_project(env, other)
    Runner.hold = True
    first, second = str(uuid.uuid4()), str(uuid.uuid4())
    jobs.submit(env, project, first, command(project))
    jobs.submit(env, other, second, command(other))
    with pytest.raises(ServiceError, match="capacity"):
        jobs.submit(env, other, str(uuid.uuid4()), command(other))
    jobs.status(env, project, first, cancel=True)
    jobs.status(env, other, second, cancel=True)
    assert wait_result(jobs, env, project, first)["status"] == "finished"
    assert wait_result(jobs, env, other, second)["status"] == "finished"


def test_reconcile_uses_durable_clean_receipt_after_container_cleanup(command_jobs, monkeypatch):
    broker, jobs, env, project = command_jobs
    operation = str(uuid.uuid4())
    jobs.submit(env, project, operation, command(project))
    wait_result(jobs, env, project, operation)

    def gone(*args):
        raise AssertionError("An already-confirmed, deleted container must not be stopped again")

    monkeypatch.setattr(jobs.runner, "stop", gone)
    result = jobs.reconcile(env, project, operation)
    assert result["reconciled"] and result["receipt"]["cleanup"] == "clean"
    assert Runner.executions == 1


def test_file_mutations_share_checkpoint_lease_and_generation(command_jobs):
    import hashlib

    broker, jobs, env, project = command_jobs
    operation = str(uuid.uuid4())
    jobs.submit(
        env,
        project,
        operation,
        {
            "project_id": project,
            "expected_generation": 0,
            "path": "note.txt",
            "expected_sha256": None,
            "content": "持久文件",
        },
        kind="write",
    )
    written = wait_result(jobs, env, project, operation)
    assert written["receipt"]["file"]["sha256"] == hashlib.sha256("持久文件".encode()).hexdigest()
    assert written["generation"] == 1 and written["receipt"]["checkpoint_id"]
    assert Runner.executions == 0
    assert broker.files.read(env, project, "note.txt")["size"] == len("持久文件".encode())
    folder = str(uuid.uuid4())
    jobs.submit(
        env,
        project,
        folder,
        {"project_id": project, "expected_generation": 1, "path": "a/b"},
        kind="mkdir",
    )
    assert wait_result(jobs, env, project, folder)["receipt"]["file"]["created"]
    remove = str(uuid.uuid4())
    jobs.submit(
        env,
        project,
        remove,
        {
            "project_id": project,
            "expected_generation": 2,
            "path": "note.txt",
            "expected_sha256": written["receipt"]["file"]["sha256"],
        },
        kind="remove",
    )
    assert wait_result(jobs, env, project, remove)["generation"] == 3
    with pytest.raises(ServiceError, match="not_found"):
        broker.files.read(env, project, "note.txt")
    assert broker.leases.status(project)["status"] == "idle"


def test_file_hash_conflict_never_overwrites_current_content(command_jobs):
    broker, jobs, env, project = command_jobs
    broker.files.write(env, project, "keep", b"existing", None)
    operation = str(uuid.uuid4())
    jobs.submit(
        env,
        project,
        operation,
        {
            "project_id": project,
            "expected_generation": 0,
            "path": "keep",
            "expected_sha256": "0" * 64,
            "content": "wrong",
        },
        kind="write",
    )
    failed = wait_result(jobs, env, project, operation)
    assert failed["receipt"]["code"] == "artifact_conflict"
    assert failed["receipt"]["cleanup"] == "clean"
    import base64

    assert base64.b64decode(broker.files.read(env, project, "keep")["data"]) == b"existing"
    assert Runner.executions == 0
