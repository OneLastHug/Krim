"""首轮真实Broker文件接线与既有Agent审批链；不冒充runsc/XFS节点验收。"""

import hashlib
import uuid

import pytest
from test_sandbox_command import approval, command_app, finished
from test_sandbox_jobs import command_jobs as command_jobs
from test_sandbox_jobs import wait_result

from krim_agent.sandbox.file_models import FilePatch, FileRename
from krim_agent.sandbox.operations import SandboxOperations


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def patch(project: str) -> dict:
    return {"project_id": project, "expected_generation": 0, "path": "note.txt",
            "expected_sha256": sha(b"hello"), "offset": 1, "delete_length": 3,
            "replacement": "i"}


def test_patch_uses_real_project_files_and_durable_job_journal(command_jobs):
    broker, jobs, env, project = command_jobs
    broker.files.write(env, project, "note.txt", b"hello", None)
    operation = str(uuid.uuid4())
    jobs.submit(env, project, operation, patch(project), kind="patch")
    row = wait_result(jobs, env, project, operation)
    assert row["status"] == "finished", row
    assert row["receipt"]["code"] is None
    assert row["edit_status"]["status"] == "applied"
    assert row["edit_plan"]["result_sha256"] == sha(b"hio")
    path = broker.environments / env / "projects" / project / "content" / "note.txt"
    assert path.read_bytes() == b"hio"
    assert broker._read(jobs.filename(operation))["edit_status"] == row["edit_status"]
    replay = jobs.submit(env, project, operation, patch(project), kind="patch")
    assert replay["generation"] == 1
    assert path.read_bytes() == b"hio"


def test_rename_uses_existing_lease_and_preserves_content(command_jobs):
    broker, jobs, env, project = command_jobs
    broker.files.write(env, project, "note.txt", b"hello", None)
    values = {"project_id": project, "expected_generation": 0, "source": "note.txt",
              "destination": "moved.txt", "expected_source_sha256": sha(b"hello")}
    operation = str(uuid.uuid4())
    jobs.submit(env, project, operation, values, kind="rename")
    row = wait_result(jobs, env, project, operation)
    assert row["status"] == "finished", row
    assert row["receipt"]["code"] is None
    assert row["edit_status"]["status"] == "applied"
    content = broker.environments / env / "projects" / project / "content"
    assert not (content / "note.txt").exists()
    assert (content / "moved.txt").read_bytes() == b"hello"
    assert broker.leases.status(project)["status"] == "idle"


def test_corrupt_file_edit_journal_does_not_become_an_untracked_write():
    from krim_agent.contracts import ServiceError
    from krim_agent.sandbox.jobs import _FileEditJournal

    journal = _FileEditJournal(None, {"edit_status": {"plan_id": "plan", "status": 1}})
    with pytest.raises(ServiceError, match="facts_corrupt"):
        journal.status("plan")


def test_retired_checkpoint_listing_does_not_recreate_the_directory(command_jobs):
    from krim_agent.contracts import ServiceError

    broker, _, env, project = command_jobs
    row = broker._environment(env)
    row["checkpoint_retired_number"] = row.pop("checkpoint_number")
    row["quota_state"] = "retired"
    broker._save("env-" + env + ".json", row)
    root = broker.environments / env / "checkpoints"
    assert not root.exists()
    result = broker.dispatch("checkpoint_list", {
        "environment_id": env, "project_id": project, "offset": 0, "limit": 20,
    })
    assert result == {"items": [], "total": 0, "offset": 0, "limit": 20}
    with pytest.raises(ServiceError, match="not_found"):
        broker.dispatch("checkpoint_cleanup_preview", {
            "environment_id": env, "project_id": project,
        })
    assert not root.exists()


def test_runtime_profile_is_forwarded_and_changes_operation_fingerprint(command_jobs, monkeypatch):
    from test_sandbox_jobs import Runner, command

    from krim_agent.sandbox import jobs as jobs_module
    from krim_agent.sandbox.jobs import CommandJobs
    from krim_agent.sandbox.runtime_profiles import LEGACY_PYTHON_PROFILE, ResolvedRuntime

    broker, old_jobs, env, project = command_jobs
    old_jobs.close()
    runtime = ResolvedRuntime(
        name="verified-python", program="python", purpose="normal_job", scope="dev_only",
        image=Runner.identity, profile=LEGACY_PYTHON_PROFILE,
    )

    class ProfileRunner(Runner):
        def __init__(self, image, installation_id, selected):
            assert selected is runtime
            self.runtime_fingerprint = selected.fingerprint

    monkeypatch.setattr(jobs_module, "RunscRunner", ProfileRunner)
    jobs = CommandJobs(broker, Runner.identity, runtime)
    broker.jobs = jobs
    operation = str(uuid.uuid4())
    jobs.submit(env, project, operation, command(project))
    assert wait_result(jobs, env, project, operation)["status"] == "finished"
    # 相同镜像但运行规格变化也不能重用旧作业绑定。
    jobs.runner.runtime_fingerprint = "f" * 64
    from krim_agent.contracts import ServiceError

    with pytest.raises(ServiceError, match="key_conflict"):
        jobs.submit(env, project, operation, command(project))


def test_cancellation_after_runner_handoff_allows_runner_cleanup(command_jobs, monkeypatch):
    _, jobs, env, project = command_jobs
    original = jobs.runner.run

    def cancel_on_entry(operation, content, argv, wall, cancel, phase):
        cancel.set()
        return original(operation, content, argv, wall, cancel, phase)

    monkeypatch.setattr(jobs.runner, "run", cancel_on_entry)
    operation = str(uuid.uuid4())
    jobs.submit(env, project, operation, {
        "project_id": project, "expected_generation": 0, "argv": ["-c", "pass"],
        "wall_seconds": 1,
    })
    row = wait_result(jobs, env, project, operation)
    assert row["status"] == "finished", row
    assert row["receipt"]["cleanup"] == "clean"
    assert row["receipt"]["code"] == "cancelled"


def test_applied_file_edit_followed_by_error_never_claims_not_started(command_jobs, monkeypatch):
    from krim_agent.contracts import ServiceError

    broker, jobs, env, project = command_jobs
    broker.files.write(env, project, "note.txt", b"hello", None)
    original = broker.files.restore_write

    def applied_then_error(*args, **kwargs):
        original(*args, **kwargs)
        raise ServiceError("artifact_conflict")

    monkeypatch.setattr(broker.files, "restore_write", applied_then_error)
    operation = str(uuid.uuid4())
    jobs.submit(env, project, operation, patch(project), kind="patch")
    row = wait_result(jobs, env, project, operation)
    assert row["status"] == "finished", row
    assert row["edit_status"]["status"] == "applied"
    assert row["receipt"]["executor_started"] is True
    assert row["receipt"]["code"] == "artifact_conflict"
    content = broker.environments / env / "projects" / project / "content" / "note.txt"
    assert content.read_bytes() == b"hio"


def test_lost_fence_after_patch_is_unknown_and_not_replayed(command_jobs, monkeypatch):
    broker, jobs, env, project = command_jobs
    broker.files.write(env, project, "note.txt", b"hello", None)
    original = broker.files.restore_write

    def changed_then_lost_fence(*args, **kwargs):
        result = original(*args, **kwargs)
        row = broker._project(env, project)
        row["generation"] += 1
        broker._save("project-" + project + ".json", row)
        return result

    monkeypatch.setattr(broker.files, "restore_write", changed_then_lost_fence)
    operation = str(uuid.uuid4())
    jobs.submit(env, project, operation, patch(project), kind="patch")
    row = wait_result(jobs, env, project, operation)
    assert row["status"] == "unknown", row
    assert row["edit_status"]["status"] == "unknown"
    path = broker.environments / env / "projects" / project / "content" / "note.txt"
    assert path.read_bytes() == b"hio"
    assert broker.leases.status(project)["status"] == "blocked"


@pytest.mark.parametrize(
    "name,model", [("sandbox_patch", FilePatch), ("sandbox_rename", FileRename)]
)
def test_edit_tools_are_registered_as_private_scoped_writes(name, model):
    registry = SandboxOperations(None).registry()
    spec = registry.get(name)
    assert spec.parameters is model
    assert spec.risk == "write" and spec.capability == "write"


@pytest.mark.parametrize("name,kind,body", [
    ("sandbox_patch", "patch", {"expected_generation": 0, "path": "note.txt",
     "expected_sha256": sha(b"hello"), "offset": 1, "delete_length": 3, "replacement": "i"}),
    ("sandbox_rename", "rename", {"expected_generation": 0, "source": "note.txt",
     "destination": "moved.txt", "expected_source_sha256": sha(b"hello")}),
])
async def test_edits_park_for_approval_and_dispatch_exact_kind(
    tmp_path, wire, monkeypatch, name, kind, body
):
    async with command_app(tmp_path, wire, monkeypatch, tool_name=name, tool_body=body) as (
        app, broker, actor, project, run, request
    ):
        assert request and not broker.starts
        await app.service.reply_approval(run, request["request_id"], approval(request))
        assert (await finished(app, run)).status == "completed"
        assert len(broker.starts) == 1
        assert broker.starts[0]["kind"] == kind
        assert broker.starts[0]["mutation"] == {"project_id": project["project_id"], **body}


async def test_rejected_patch_never_reaches_broker(tmp_path, wire, monkeypatch):
    body = patch(None)
    body.pop("project_id")
    async with command_app(
        tmp_path, wire, monkeypatch, tool_name="sandbox_patch", tool_body=body
    ) as (app, broker, _, _, run, request):
        await app.service.reply_approval(run, request["request_id"], approval(request, False))
        await finished(app, run)
        assert not broker.starts
