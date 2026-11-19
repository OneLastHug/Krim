"""Regress command admission and revocation through production control paths.

Admission uses real Broker dispatch/jobs with quota and runner substitutes.
Revocation uses real account/AgentLoop cancellation and a synthetic Broker;
these tests do not exercise Docker/runsc or XFS enforcement.
"""

import asyncio
import base64
import json
import uuid

import pytest
from sqlalchemy import select, update
from test_sandbox_command import CommandBroker, approval, command_app, finished
from test_sandbox_jobs import Runner, command, wait_result
from test_sandbox_jobs import command_jobs as command_jobs
from test_sandbox_tasks import response

from krim_agent.contracts import ServiceError
from krim_agent.sandbox import jobs as jobs_module
from krim_agent.sandbox.jobs import CommandJobs
from krim_agent.sandbox.runtime_profiles import RuntimeCatalog, RuntimeProfile, RuntimeProfileEntry
from krim_agent.sandbox.tasks import cancel_run_tasks
from krim_agent.storage.sandbox_models import sandbox_operations, sandbox_projects


class ProfileRunner(Runner):
    def __init__(self, image, installation_id, runtime=None):
        self.image = image
        self.runtime = runtime
        self.runtime_fingerprint = runtime.fingerprint if runtime is not None else None


def replace_job_runtime(broker, old_jobs, monkeypatch, image):
    old_jobs.close()
    monkeypatch.setattr(jobs_module, "RunscRunner", ProfileRunner)
    ProfileRunner.executions = 0
    ProfileRunner.hold = False
    catalog = RuntimeCatalog(
        profiles=(
            RuntimeProfileEntry(
                name="python-regression-runtime",
                profile=RuntimeProfile(program="python", interpreter="/usr/bin/python3.12"),
                availability="dev_verified",
                scope="dev_only" if image.startswith("sha256:") else "release",
                image=image,
            ),
        )
    )
    selected = CommandJobs(broker, image, catalog=catalog, allow_dev=True)
    broker.jobs = selected
    return selected


@pytest.mark.parametrize("catalog_enabled", [False, True], ids=["legacy-image", "catalog"])
def test_first_foreground_command_is_admitted_without_preseeded_records(
    command_jobs, monkeypatch, catalog_enabled
):
    broker, jobs, environment, project = command_jobs
    if catalog_enabled:
        jobs = replace_job_runtime(broker, jobs, monkeypatch, Runner.identity)
    operation = str(uuid.uuid4())
    assert broker._read(jobs.filename(operation)) is None
    assert broker._read("probe-" + operation + ".json") is None

    accepted = broker.dispatch(
        "command_start",
        {
            "environment_id": environment,
            "project_id": project,
            "operation_id": operation,
            "command": command(project),
            "runtime_image": jobs.image,
            "runtime_fingerprint": (
                jobs.catalog.resolve("python", "normal_job", allow_dev=True).fingerprint
                if catalog_enabled
                else None
            ),
        },
    )

    assert accepted["operation_id"] == operation
    result = wait_result(jobs, environment, project, operation)
    assert result["status"] == "finished"
    assert result["receipt"]["executor_started"] is True
    assert result["receipt"]["exit_code"] == 0
    assert (ProfileRunner if catalog_enabled else Runner).executions == 1


def test_background_command_accepts_repository_digest_reference(command_jobs, monkeypatch):
    broker, old_jobs, environment, project = command_jobs
    image = "docker.io/library/python@sha256:" + "b" * 64
    jobs = replace_job_runtime(broker, old_jobs, monkeypatch, image)
    runtime = jobs.catalog.resolve("python", "normal_job", allow_dev=True)
    assert image != ProfileRunner.identity
    operation = str(uuid.uuid4())

    arguments = {
        "environment_id": environment,
        "project_id": project,
        "operation_id": operation,
        "command": command(project),
        "runtime_image": image,
        "runtime_fingerprint": runtime.fingerprint,
    }
    accepted = broker.dispatch("command_start_background", arguments)

    assert accepted["operation_id"] == operation
    result = wait_result(jobs, environment, project, operation)
    assert result["receipt"]["executor_started"] is True
    assert result["receipt"]["exit_code"] == 0
    assert ProfileRunner.executions == 1

    def unavailable(_program):
        raise ServiceError("sandbox_backend_unavailable", 503)

    monkeypatch.setattr(jobs, "_runner_for", unavailable)
    assert broker.dispatch("command_start_background", arguments) == result
    assert ProfileRunner.executions == 1


def test_background_command_still_rejects_a_different_repository_digest(command_jobs, monkeypatch):
    broker, old_jobs, environment, project = command_jobs
    image = "docker.io/library/python@sha256:" + "b" * 64
    jobs = replace_job_runtime(broker, old_jobs, monkeypatch, image)
    runtime = jobs.catalog.resolve("python", "normal_job", allow_dev=True)
    operation = str(uuid.uuid4())

    with pytest.raises(ServiceError, match="target_binding_changed"):
        broker.dispatch(
            "command_start_background",
            {
                "environment_id": environment,
                "project_id": project,
                "operation_id": operation,
                "command": command(project),
                "runtime_image": "docker.io/library/python@sha256:" + "c" * 64,
                "runtime_fingerprint": runtime.fingerprint,
            },
        )
    assert ProfileRunner.executions == 0
    assert broker._read(jobs.filename(operation)) is None


async def test_approved_foreground_command_reaches_real_broker_and_returns_result(
    tmp_path, wire, monkeypatch, command_jobs
):
    broker, jobs, _environment, _project = command_jobs
    execute = jobs.runner.run
    stdout = base64.b64encode(b"approved stdout").decode()
    stderr = base64.b64encode(b"diagnostic stderr").decode()

    def run_with_output(*args, **kwargs):
        return {**execute(*args, **kwargs), "stdout": stdout, "stderr": stderr}

    monkeypatch.setattr(jobs.runner, "run", run_with_output)

    class LocalBrokerClient:
        async def call(self, action, **arguments):
            return await asyncio.to_thread(broker.dispatch, action, arguments)

    async with command_app(
        tmp_path, wire, monkeypatch, real_broker=LocalBrokerClient(), image=jobs.image
    ) as (app, _client, actor, project, run_id, request):
        # Match the fixture's synthetic checkpoint quota; do not seed a job/probe.
        environment = broker._environment(project["environment_id"])
        environment["checkpoint_number"] = 999
        broker._save("env-" + project["environment_id"] + ".json", environment)
        await app.service.reply_approval(run_id, request["request_id"], approval(request))
        result = await finished(app, run_id)

        assert result.status == "completed", result.code
        ends = [
            event.data for event in await app.service.events(run_id) if event.kind == "tool_end"
        ]
        assert ends[0]["status"] == "ok"
        assert ends[0]["value"]["executor_started"] is True
        assert ends[0]["value"]["exit_code"] == 0
        assert ends[0]["value"]["stdout"] == stdout
        assert ends[0]["value"]["stderr"] == stderr
        tool_results = [
            json.loads(message["content"])
            for model_request in wire.requests
            for message in model_request["messages"]
            if message["role"] == "tool"
        ]
        assert any(
            value.get("stdout") == stdout and value.get("stderr") == stderr
            for value in tool_results
        )
        assert (await app.sandbox.project(actor, project["project_id"]))["generation"] == 1
        assert Runner.executions == 1


@pytest.mark.parametrize("catalog_enabled", [False, True], ids=["legacy-image", "catalog"])
def test_broker_replays_active_and_finished_commands_without_reexecution(
    command_jobs, monkeypatch, catalog_enabled
):
    broker, jobs, environment, project = command_jobs
    if catalog_enabled:
        jobs = replace_job_runtime(broker, jobs, monkeypatch, Runner.identity)
    runner = ProfileRunner if catalog_enabled else Runner
    arguments = {
        "environment_id": environment,
        "project_id": project,
        "operation_id": str(uuid.uuid4()),
        "command": command(project),
        "runtime_image": jobs.image,
        "runtime_fingerprint": (
            jobs.catalog.resolve("python", "normal_job", allow_dev=True).fingerprint
            if catalog_enabled
            else None
        ),
    }
    runner.hold = True
    try:
        broker.dispatch("command_start", arguments)
        replayed = broker.dispatch("command_start", arguments)
        assert replayed["operation_id"] == arguments["operation_id"]
    finally:
        runner.hold = False
    result = wait_result(jobs, environment, project, arguments["operation_id"])
    assert result["status"] == "finished"
    assert broker.dispatch("command_start", arguments) == result
    assert runner.executions == 1


@pytest.mark.parametrize(
    "changed,code",
    [
        ("image", "target_binding_changed"),
        ("fingerprint", "target_binding_changed"),
        ("command", "key_conflict"),
        ("mode", "key_conflict"),
    ],
)
def test_broker_replay_rejects_changed_approval_binding(command_jobs, changed, code):
    broker, jobs, environment, project = command_jobs
    operation = str(uuid.uuid4())
    arguments = {
        "environment_id": environment,
        "project_id": project,
        "operation_id": operation,
        "command": command(project),
        "runtime_image": jobs.image,
        "runtime_fingerprint": None,
    }
    broker.dispatch("command_start", arguments)
    wait_result(jobs, environment, project, operation)
    action = "command_start"
    if changed == "image":
        arguments["runtime_image"] = "sha256:" + "d" * 64
    elif changed == "fingerprint":
        arguments["runtime_fingerprint"] = "e" * 64
    elif changed == "command":
        arguments["command"] = {**command(project), "argv": ["-c", "print(2)"]}
    else:
        action = "command_start_background"
    with pytest.raises(ServiceError, match=code):
        broker.dispatch(action, arguments)
    assert Runner.executions == 1


def test_catalog_command_rejects_wrong_fingerprint_before_acceptance(command_jobs, monkeypatch):
    broker, old_jobs, environment, project = command_jobs
    jobs = replace_job_runtime(broker, old_jobs, monkeypatch, Runner.identity)
    operation = str(uuid.uuid4())
    with pytest.raises(ServiceError, match="target_binding_changed"):
        broker.dispatch(
            "command_start",
            {
                "environment_id": environment,
                "project_id": project,
                "operation_id": operation,
                "command": command(project),
                "runtime_image": jobs.image,
                "runtime_fingerprint": "f" * 64,
            },
        )
    assert broker._read(jobs.filename(operation)) is None
    assert ProfileRunner.executions == 0


async def _start_background_and_wait(app, wire, monkeypatch, broker, run_id, request):
    def wait_on_original_task(body, index):
        handle = json.loads(body["messages"][-1]["content"])["operation_id"]
        return response(
            "sandbox_task_wait",
            {"operation_id": handle, "seconds": 10},
            call_id=f"task-wait-{index}",
        )

    monkeypatch.setattr(wire, "response", wait_on_original_task)
    await app.service.reply_approval(run_id, request["request_id"], approval(request))
    async with asyncio.timeout(15):
        await broker.entered.wait()


@pytest.mark.parametrize("revocation", ["disable-account", "revoke-key"])
async def test_revoking_owner_stops_the_already_accepted_background_task(
    tmp_path, wire, monkeypatch, revocation
):
    broker = CommandBroker()
    broker.waiting = True
    async with command_app(
        tmp_path,
        wire,
        monkeypatch,
        real_broker=broker,
        tool_name="sandbox_task_start",
        bind_session=True,
    ) as (app, _broker, owner, project, run_id, request):
        created = await app.identities.create_account(
            owner,
            {"display_name": "Revocation controller", "platform_role": "platform_admin"},
            request_key="create-revocation-controller",
        )
        issued = await app.identities.issue_key(
            owner, created.account.principal_id, {}, request_key="issue-controller-key"
        )
        controller = app.identities.authenticate("Bearer " + issued.token)

        await _start_background_and_wait(app, wire, monkeypatch, broker, run_id, request)
        assert len(broker.starts) == 1
        assert broker.cancelled is False
        assert (await app.service.get(run_id)).status == "running"
        await cancel_run_tasks(app.sandbox, str(uuid.uuid4()))
        assert broker.cancelled is False

        if revocation == "disable-account":
            await app.identities.update_account(
                controller,
                owner.principal_id,
                {"status": "disabled"},
                request_key="disable-task-owner",
            )
        else:
            await app.identities.revoke_key(
                controller, owner.principal_id, {}, request_key="revoke-task-owner-key"
            )

        assert (await finished(app, run_id)).status in {"aborted", "failed"}
        assert broker.cancelled is True, "Run ended without stopping its accepted Broker job"
        assert any(poll["cancel"] for poll in broker.statuses)
        assert all(
            poll["operation_id"] == broker.starts[0]["operation_id"] and not poll["renew"]
            for poll in broker.statuses
            if poll["cancel"]
        )
        assert len(broker.starts) == 1
        with pytest.raises(ServiceError, match="unauthorized"):
            await app.sandbox.project(owner, project["project_id"], write=True)
        async with app.store.lock, app.store.engine.connect() as conn:
            operation = (
                (
                    await conn.execute(
                        select(sandbox_operations).where(
                            sandbox_operations.c.operation_id == broker.starts[0]["operation_id"]
                        )
                    )
                )
                .mappings()
                .one()
            )
            saved_project = (
                (
                    await conn.execute(
                        select(sandbox_projects).where(
                            sandbox_projects.c.project_id == project["project_id"]
                        )
                    )
                )
                .mappings()
                .one()
            )
        assert operation["status"] == "completed"
        assert operation["result"]["receipt"]["cleanup"] == "clean"
        assert operation["result"]["receipt"]["code"] == "cancelled"
        assert saved_project["status"] == "ready"
        assert saved_project["lease_id"] is None
        polls = len(broker.statuses)
        await cancel_run_tasks(app.sandbox, run_id)
        assert len(broker.statuses) == polls


@pytest.mark.parametrize("revocation", ["disable-account", "revoke-key"])
@pytest.mark.parametrize("forget_producer", [False, True], ids=["done-producer", "no-producer"])
async def test_revocation_stops_parked_run_and_its_existing_background_job(
    tmp_path, wire, monkeypatch, revocation, forget_producer
):
    broker = CommandBroker()
    broker.waiting = True
    async with command_app(
        tmp_path,
        wire,
        monkeypatch,
        real_broker=broker,
        tool_name="sandbox_task_start",
        bind_session=True,
    ) as (app, _broker, owner, _project, run_id, request):
        created = await app.identities.create_account(
            owner,
            {"display_name": "Parked revocation controller", "platform_role": "platform_admin"},
            request_key="parked-controller",
        )
        issued = await app.identities.issue_key(
            owner, created.account.principal_id, {}, request_key="parked-controller-key"
        )
        controller = app.identities.authenticate("Bearer " + issued.token)

        def ask_to_cancel(body, index):
            handle = json.loads(body["messages"][-1]["content"])["operation_id"]
            return response(
                "sandbox_task_cancel",
                {"operation_id": handle, "expected_generation": 0},
                call_id=f"parked-cancel-{index}",
            )

        monkeypatch.setattr(wire, "response", ask_to_cancel)
        await app.service.reply_approval(run_id, request["request_id"], approval(request))
        assert (await app.service.wait(run_id)).status == "awaiting"
        assert app.service.tasks[run_id].done()
        second_request = (await app.service.approvals(run_id))[0]
        assert len(broker.starts) == 1 and not broker.cancelled
        if forget_producer:
            # No live task exists; persisted run ownership must remain sufficient.
            app.service.tasks.pop(run_id)
        try:
            if revocation == "disable-account":
                await app.identities.update_account(
                    controller,
                    owner.principal_id,
                    {"status": "disabled"},
                    request_key="disable-parked-owner",
                )
            else:
                await app.identities.revoke_key(
                    controller, owner.principal_id, {}, request_key="revoke-parked-owner"
                )
            saved = await app.service.get(run_id)
            assert saved.status == "aborted" and saved.code == "cancelled"
            assert broker.cancelled and len(broker.starts) == 1
            assert await app.service.approvals(run_id) == []
            with pytest.raises(ServiceError, match="approval_unavailable"):
                await app.service.reply_approval(
                    run_id, second_request["request_id"], approval(second_request)
                )
            assert all(not poll["renew"] for poll in broker.statuses)
            facts = await app.service.result(run_id)
            assert sum(event.kind == "terminal" for event in facts.events) == 1
            assert len(wire.requests) == 2
        finally:
            await app.service.cancel(run_id)


@pytest.mark.parametrize("corruption", ["environment", "session", "principal"])
async def test_host_cleanup_rejects_corrupted_job_ownership(
    tmp_path, wire, monkeypatch, corruption
):
    broker = CommandBroker()
    broker.waiting = True
    async with command_app(
        tmp_path,
        wire,
        monkeypatch,
        real_broker=broker,
        tool_name="sandbox_task_start",
        bind_session=True,
    ) as (app, _broker, owner, _project, run_id, request):
        other = await app.identities.create_account(
            owner, {"display_name": "Other job owner"}, request_key="other-job-owner"
        )
        await _start_background_and_wait(app, wire, monkeypatch, broker, run_id, request)
        operation_id = broker.starts[0]["operation_id"]
        async with app.store.lock, app.store.engine.connect() as conn:
            original = (
                (
                    await conn.execute(
                        select(sandbox_operations).where(
                            sandbox_operations.c.operation_id == operation_id
                        )
                    )
                )
                .mappings()
                .one()
            )
        changed = dict(original["result"])
        principal_id = original["principal_id"]
        if corruption == "principal":
            principal_id = other.account.principal_id
        else:
            changed[corruption + "_id"] = str(uuid.uuid4())
        try:
            async with app.store.lock, app.store.engine.begin() as conn:
                await conn.execute(
                    update(sandbox_operations)
                    .where(sandbox_operations.c.operation_id == operation_id)
                    .values(principal_id=principal_id, result=changed)
                )
            with pytest.raises(ServiceError, match="facts_corrupt"):
                await cancel_run_tasks(app.sandbox, run_id)
            assert broker.cancelled is False
            assert not any(poll["cancel"] for poll in broker.statuses)
        finally:
            async with app.store.lock, app.store.engine.begin() as conn:
                await conn.execute(
                    update(sandbox_operations)
                    .where(sandbox_operations.c.operation_id == operation_id)
                    .values(principal_id=original["principal_id"], result=original["result"])
                )
            await app.service.cancel(run_id)
        assert broker.cancelled is True
        assert len(broker.starts) == 1


@pytest.mark.parametrize("phase", ["parking", "parked"])
async def test_cancel_at_second_approval_stops_the_existing_background_job(
    tmp_path, wire, monkeypatch, phase
):
    broker = CommandBroker()
    broker.waiting = True
    async with command_app(
        tmp_path,
        wire,
        monkeypatch,
        real_broker=broker,
        tool_name="sandbox_task_start",
        bind_session=True,
    ) as (app, _broker, _owner, _project, run_id, request):
        entered, release, stopped = (asyncio.Event() for _ in range(3))
        original_awaiting = app.store.set_awaiting
        original_stop = app.store.request_stop

        async def held_awaiting(selected_run):
            parked = None
            if phase == "parked":
                parked = await original_awaiting(selected_run)
            entered.set()
            await release.wait()
            if phase == "parking":
                parked = await original_awaiting(selected_run)
            return parked

        async def observed_stop(selected_run):
            await original_stop(selected_run)
            stopped.set()

        def request_cancel_approval(body, index):
            handle = json.loads(body["messages"][-1]["content"])["operation_id"]
            return response(
                "sandbox_task_cancel",
                {"operation_id": handle, "expected_generation": 0},
                call_id=f"cancel-background-{index}",
            )

        monkeypatch.setattr(app.store, "set_awaiting", held_awaiting)
        monkeypatch.setattr(app.store, "request_stop", observed_stop)
        monkeypatch.setattr(wire, "response", request_cancel_approval)
        await app.service.reply_approval(run_id, request["request_id"], approval(request))
        caller = None
        try:
            await asyncio.wait_for(entered.wait(), 10)
            assert len(broker.starts) == 1
            assert broker.cancelled is False
            caller = asyncio.create_task(app.service.cancel(run_id))
            await asyncio.wait_for(stopped.wait(), 5)
            release.set()
            result = await asyncio.wait_for(caller, 5)
            assert result.status == "aborted" and result.code == "cancelled"
            assert broker.cancelled is True
            assert await app.service.approvals(run_id) == []
            assert len(broker.starts) == 1
            assert all(not poll["renew"] for poll in broker.statuses)
        finally:
            release.set()
            if caller is not None:
                await asyncio.gather(caller, return_exceptions=True)
