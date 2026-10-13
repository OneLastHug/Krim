"""目录树必须逐项走原Registry/source proposal/attempt，父回执才开放后代。"""

import asyncio
import json
import shutil
import sys
import time
from dataclasses import asdict, replace
from pathlib import Path
from threading import Event

import pytest
from sqlalchemy import event, update
from test_command_directory_dependency_source import counts
from test_command_publication import execute, setup

from krim_agent.execution.directory_publication import CommandDirectoryDependencyRequest
from krim_agent.persistence.command_directory_dependencies import CommandDirectoryDependencyStore
from krim_agent.persistence.database import (
    AgentSessionRow,
    Database,
    WorkspaceDirectoryAttemptRow,
    WorkspaceDirectoryProposalRow,
    WorkspaceTreePlanRow,
    WorkspaceTreeStepRow,
)
from krim_agent.persistence.workspace_directories import WorkspaceDirectoryStore
from krim_agent.persistence.workspace_edits import WorkspaceEditStore
from krim_agent.persistence.workspace_tree_plans import WorkspaceTreePlanStore
from krim_agent.protocol.domain import ServiceError
from krim_agent.tools import ShortCircuit, ToolContext
from krim_agent.workspace_directory import ArtifactDirectorySpec, WorkspaceDirectoryWriter
from krim_agent.workspace_tree import TreePlanRequest
from krim_agent.workspace_writer import WorkspaceWriter

pytestmark = pytest.mark.skipif(
    not sys.platform.startswith("linux") or shutil.which("bwrap") is None,
    reason="需要真实隔离来源和原文件/目录事务",
)


async def tree(runtime, root, *, claim=True, persistent=False, extra_names=()):
    names = [
        "command_execute",
        "command_get",
        "command_propose",
        "directory_source_preview",
        "directory_change",
        "workspace_change",
        "workspace_reconcile",
        "directory_reconcile",
        *extra_names,
    ]
    service, agent = await setup(
        runtime, root, names=names, persistent=persistent, max_steps=16 if extra_names else 8
    )
    command = await execute(
        agent,
        "import os;os.makedirs('tree/sub');os.chmod('tree',0o750);"
        "os.chmod('tree/sub',0o700);open('tree/sub/data.bin','wb').write(bytes([0,255]))",
    )
    scope = "session:" + agent.session_id
    binding = service._handle(agent.agent_id).definition.workspace
    source = await CommandDirectoryDependencyStore(service.workspace_edits).plan(
        scope, CommandDirectoryDependencyRequest(command_id=command.command_id)
    )
    prepared = WorkspaceDirectoryWriter(binding.require()).prepare_artifact(
        ArtifactDirectorySpec(
            path="tree",
            operation="create",
            mode=0o750,
            command_id=command.command_id,
            source_hash=source.source_hash,
        )
    )
    store = WorkspaceTreePlanStore(service.workspace_edits, service.manager.directory_changes.store)
    plan, _ = await store.reserve(
        scope,
        binding,
        TreePlanRequest(
            command_id=command.command_id,
            source_hash=source.source_hash,
            root_path="tree",
            request_key="tree",
        ),
        prepared,
    )
    execution = await store.claim(scope, plan.plan_id) if claim else plan
    return service, agent, scope, store, execution


async def invoke(service, agent, dispatch):
    return await service._workspace_call(
        agent.agent_id,
        dispatch.name,
        dispatch.request.model_dump(),
        confirmed=True,
        tree_fence=dispatch.fence,
    )


async def publish(service, agent, scope, store, execution, position):
    preview = await store.dispatch(scope, execution.view.plan_id, execution.token, position)
    assert preview.fence.phase == "preview"
    await invoke(service, agent, preview)
    action = await store.dispatch(scope, execution.view.plan_id, execution.token, position)
    assert action.fence.phase == "change"
    value = await invoke(service, agent, action)
    assert value["status"] == "done"
    await store.finish_step(scope, execution.view.plan_id, execution.token, position)
    return value


async def test_real_tree_registry_publishes_original_directory_and_binary_attempts_once(
    runtime, tmp_path
):
    service, agent, scope, store, execution = await tree(runtime, tmp_path)
    try:
        results = [
            await publish(service, agent, scope, store, execution, position)
            for position in range(3)
        ]
        done = await store.finish(scope, execution.view.plan_id, execution.token)
        assert done.status == "done" and done.done_count == done.step_count == 3
        assert (tmp_path / "tree/sub/data.bin").read_bytes() == b"\x00\xff"
        assert (tmp_path / "tree").stat().st_mode & 0o777 == 0o750
        assert (tmp_path / "tree/sub").stat().st_mode & 0o777 == 0o700
        assert await counts(store.database) == [2, 2, 1, 1]
        cold = await store.steps(scope, done.plan_id)
        assert [item.operation.operation_id for item in cold.items] == [
            item["operation_id"] for item in results
        ]
        assert await store.get(scope, done.plan_id) == done
        with pytest.raises(ServiceError) as closed:
            await store.dispatch(scope, done.plan_id, execution.token, 2)
        assert closed.value.code == "workspace_tree_fenced"
        assert await counts(store.database) == [2, 2, 1, 1]
    finally:
        await service.close()


async def test_parent_pending_or_proposed_never_opens_descendant_preview(runtime, tmp_path):
    service, agent, scope, store, execution = await tree(runtime, tmp_path)
    try:
        for phase in ("pending", "proposed"):
            if phase == "proposed":
                await invoke(
                    service,
                    agent,
                    await store.dispatch(scope, execution.view.plan_id, execution.token, 0),
                )
            with pytest.raises(ServiceError) as refused:
                await store.dispatch(scope, execution.view.plan_id, execution.token, 1)
            assert refused.value.code == "workspace_tree_parent_unresolved"
        assert await counts(store.database) == [1, 0, 0, 0]
        assert list(tmp_path.iterdir()) == []
    finally:
        await service.close()


async def test_cancel_after_root_receipt_preserves_root_and_closes_unstarted_descendants(
    runtime, tmp_path
):
    service, agent, scope, store, execution = await tree(runtime, tmp_path)
    try:
        root = await publish(service, agent, scope, store, execution, 0)
        value = await store.cancel(scope, execution.view.plan_id)
        assert value.producer_status == "cancelled" and value.done_count == 1
        page = await store.steps(scope, value.plan_id)
        assert page.items[0].operation.operation_id == root["operation_id"]
        assert all(item.operation is None and item.status == "skipped" for item in page.items[1:])
        assert (tmp_path / "tree").exists() and not (tmp_path / "tree/sub").exists()
        with pytest.raises(ServiceError) as closed:
            await store.dispatch(scope, value.plan_id, execution.token, 1)
        assert closed.value.code == "workspace_tree_fenced"
        assert await counts(store.database) == [1, 1, 0, 0]
    finally:
        await service.close()


async def ready(runtime, root, source):
    service, agent, scope, store, execution = await tree(runtime, root)
    position = 0 if source == "directory" else 2
    for parent in range(position):
        await publish(service, agent, scope, store, execution, parent)
    preview = await store.dispatch(scope, execution.view.plan_id, execution.token, position)
    await invoke(service, agent, preview)
    dispatch = await store.dispatch(scope, execution.view.plan_id, execution.token, position)
    return service, agent, scope, store, execution, preview, dispatch


@pytest.mark.parametrize("source", ["directory", "file"])
async def test_public_key_cannot_admit_tree_proposal_and_terminal_retries_only_read(
    runtime, tmp_path, source
):
    service, agent, scope, store, execution, preview, dispatch = await ready(
        runtime, tmp_path, source
    )
    before = await counts(store.database)
    try:
        for request in (
            dispatch.request,
            dispatch.request.model_copy(update={"request_key": "public"}),
        ):
            with pytest.raises(ServiceError) as denied:
                await service._workspace_call(
                    agent.agent_id, dispatch.name, request.model_dump(), confirmed=True
                )
            assert denied.value.code == "workspace_tree_request_reserved"
            assert denied.value.status == 403
        assert await counts(store.database) == before
        value = await invoke(service, agent, dispatch)
        assert value["status"] == "done"
        await store.finish_step(
            scope, execution.view.plan_id, execution.token, dispatch.fence.position
        )
        closed = await store.finish(scope, execution.view.plan_id, execution.token)
        assert closed.done_count == (1 if source == "directory" else 3)
        assert await invoke(service, agent, dispatch) == value
        public_read = await service._workspace_call(
            agent.agent_id, dispatch.name, dispatch.request.model_dump(), confirmed=True
        )
        assert public_read == value
        assert await invoke(service, agent, preview) is not None
        assert await counts(store.database) == (
            [1, 1, 0, 0] if source == "directory" else [2, 2, 1, 1]
        )
    finally:
        await service.close()


@pytest.mark.parametrize("phase", ["preview", "change"])
@pytest.mark.parametrize("field", ["request_key", "tree_fence", "run_id", "origin"])
async def test_hook_cannot_redirect_frozen_phase_or_context(runtime, tmp_path, phase, field):
    service, agent, scope, store, execution = await tree(runtime, tmp_path)
    try:
        dispatch = await store.dispatch(scope, execution.view.plan_id, execution.token, 0)
        if phase == "change":
            await invoke(service, agent, dispatch)
            dispatch = await store.dispatch(scope, execution.view.plan_id, execution.token, 0)
        registry = service._handle(agent.agent_id).runtime.tools

        async def redirect(_definition, context):
            if field == "request_key":
                context.arguments[field] = "hook-key"
            else:
                setattr(
                    context,
                    field,
                    {"tree_fence": None, "run_id": "foreign", "origin": "model"}[field],
                )

        registry.add_global_pre(redirect)
        with pytest.raises(ServiceError) as denied:
            await invoke(service, agent, dispatch)
        assert denied.value.code == "workspace_tree_fence_mismatch"
        assert await counts(store.database) == (
            [0, 0, 0, 0] if phase == "preview" else [1, 0, 0, 0]
        )
        assert list(tmp_path.iterdir()) == []
    finally:
        await service.close()


@pytest.mark.parametrize("source", ["directory", "file"])
async def test_hook_success_without_original_attempt_cannot_open_descendants(
    runtime, tmp_path, source
):
    service, agent, scope, store, execution, _preview, dispatch = await ready(
        runtime, tmp_path, source
    )
    before = await counts(store.database)
    try:

        async def shortcut(definition, _context):
            if definition.name == dispatch.name:
                return ShortCircuit({"status": "done", "operation_id": "invented"})
            return None

        service._handle(agent.agent_id).runtime.tools.add_global_pre(shortcut)
        with pytest.raises(ServiceError):
            await invoke(service, agent, dispatch)
        await store.finish_step(
            scope, execution.view.plan_id, execution.token, dispatch.fence.position
        )
        item = (await store.steps(scope, execution.view.plan_id)).items[dispatch.fence.position]
        assert item.status == "rejected" and item.operation is None
        assert await counts(store.database) == before
        with pytest.raises(ServiceError) as closed:
            await store.dispatch(
                scope, execution.view.plan_id, execution.token, dispatch.fence.position
            )
        assert closed.value.code == "workspace_tree_step_closed"
        if source == "directory":
            with pytest.raises(ServiceError) as parent:
                await store.dispatch(scope, execution.view.plan_id, execution.token, 1)
            assert parent.value.code == "workspace_tree_parent_unresolved"
        assert not (tmp_path / "tree/sub/data.bin").exists()
    finally:
        await service.close()


async def test_parent_replaced_after_done_is_never_adopted(runtime, tmp_path):
    service, agent, scope, store, execution = await tree(runtime, tmp_path)
    try:
        await publish(service, agent, scope, store, execution, 0)
        original_identity = (tmp_path / "tree").stat().st_ino
        (tmp_path / "tree").rename(tmp_path / "original")
        (tmp_path / "tree").mkdir(mode=0o750)
        assert (tmp_path / "tree").stat().st_ino != original_identity
        dispatch = await store.dispatch(scope, execution.view.plan_id, execution.token, 1)
        with pytest.raises(ServiceError) as denied:
            await invoke(service, agent, dispatch)
        assert denied.value.code == "workspace_tree_parent_mismatch"
        assert denied.value.status == 409
        assert await counts(store.database) == [1, 1, 0, 0]
        assert list((tmp_path / "tree").iterdir()) == []
        assert list((tmp_path / "original").iterdir()) == []
    finally:
        await service.close()


@pytest.mark.parametrize(
    "field", ["observed_after", "receipt", "request_key", "revision", "resource"]
)
async def test_damaged_parent_original_fact_cannot_open_child(runtime, tmp_path, field):
    service, agent, scope, store, execution = await tree(runtime, tmp_path)
    try:
        value = await publish(service, agent, scope, store, execution, 0)
        async with store.database.sessions.begin() as sql:
            row = await sql.get(WorkspaceDirectoryAttemptRow, value["operation_id"])
            if field == "observed_after":
                row.observed_after = None
            elif field == "receipt":
                row.receipt = {**row.receipt, "path": "foreign"}
            elif field == "request_key":
                row.request_key = "foreign"
            elif field == "revision":
                row.expected_revision = 8
            else:
                proposal = await sql.get(WorkspaceDirectoryProposalRow, value["proposal_id"])
                proposal.resource_fingerprint = "0" * 64
        with pytest.raises(ServiceError) as denied:
            await store.dispatch(scope, execution.view.plan_id, execution.token, 1)
        assert denied.value.code == "workspace_tree_corrupt"
        assert await counts(store.database) == [1, 1, 0, 0]
        assert not (tmp_path / "tree/sub").exists()
    finally:
        await service.close()


async def test_two_connections_claim_one_owner_and_closed_tree_never_revives(runtime, tmp_path):
    service, agent, scope, store, plan = await tree(runtime, tmp_path, claim=False)
    other_db = Database(str(store.database.engine.url))
    other_files = WorkspaceEditStore(other_db, owner_id=store.owner_id)
    other = WorkspaceTreePlanStore(
        other_files, WorkspaceDirectoryStore(other_db, owner_id=store.owner_id)
    )
    try:
        foreign = WorkspaceTreePlanStore(
            WorkspaceEditStore(other_db, owner_id="foreign"),
            WorkspaceDirectoryStore(other_db, owner_id="foreign"),
        )
        with pytest.raises(ServiceError) as denied:
            await foreign.claim(scope, plan.plan_id)
        assert denied.value.code == "agent_session_owner"
        values = await asyncio.gather(
            store.claim(scope, plan.plan_id),
            other.claim(scope, plan.plan_id),
            return_exceptions=True,
        )
        assert sum(not isinstance(item, Exception) for item in values) == 1
        denied = next(item for item in values if isinstance(item, ServiceError))
        assert denied.code == "workspace_tree_claimed"
        closed = await other.cancel(scope, plan.plan_id)
        assert closed.status == "cancelled"
        for current in (store, other):
            with pytest.raises(ServiceError) as denied:
                await current.claim(scope, plan.plan_id)
            assert denied.value.code == "workspace_tree_claimed"
        assert await counts(store.database) == [0, 0, 0, 0]
        assert list(tmp_path.iterdir()) == []
    finally:
        await other_db.close()
        await service.close()


@pytest.mark.parametrize("source", ["directory", "file"])
async def test_two_connections_admit_one_original_attempt(runtime, tmp_path, source):
    service, agent, scope, store, execution, _preview, dispatch = await ready(
        runtime, tmp_path, source
    )
    other_db = Database(str(store.database.engine.url))
    other_files = WorkspaceEditStore(other_db, owner_id=store.owner_id)
    other_directory = WorkspaceDirectoryStore(other_db, owner_id=store.owner_id)
    try:
        backend = store.files if source == "file" else store.directories
        other = other_files if source == "file" else other_directory
        prepared = (await backend.get(scope, dispatch.request.proposal_id)).prepared
        results = await asyncio.gather(
            *(
                current.reserve(
                    scope,
                    dispatch.request,
                    execution.view.resource_fingerprint,
                    prepared,
                    tree_fence=dispatch.fence,
                )
                for current in (backend, other)
            )
        )
        assert sum(created for _attempt, created in results) == 1
        assert len({attempt.view.operation_id for attempt, _created in results}) == 1
        assert await counts(store.database) == (
            [1, 1, 0, 0] if source == "directory" else [2, 2, 1, 1]
        )
        assert not (tmp_path / "tree/sub/data.bin").exists()
    finally:
        await other_db.close()
        await service.close()


@pytest.mark.parametrize("source", ["directory", "file"])
@pytest.mark.parametrize("phase", ["preview", "change"])
async def test_link_write_failure_rolls_back_original_ledger_and_tree_admission(
    runtime, tmp_path, source, phase
):
    service, agent, scope, store, execution = await tree(runtime, tmp_path)
    position = 0 if source == "directory" else 2
    try:
        for parent in range(position):
            await publish(service, agent, scope, store, execution, parent)
        dispatch = await store.dispatch(scope, execution.view.plan_id, execution.token, position)
        if phase == "change":
            await invoke(service, agent, dispatch)
            dispatch = await store.dispatch(
                scope, execution.view.plan_id, execution.token, position
            )
        before = await counts(store.database)

        def fail(_conn, _cursor, statement, _params, _context, _many):
            if statement.startswith("UPDATE workspace_tree_steps"):
                raise RuntimeError("test tree association write failed")

        event.listen(store.database.engine.sync_engine, "before_cursor_execute", fail)
        try:
            with pytest.raises(ServiceError):
                await invoke(service, agent, dispatch)
        finally:
            event.remove(store.database.engine.sync_engine, "before_cursor_execute", fail)
        assert await counts(store.database) == before
        item = (await store.steps(scope, execution.view.plan_id)).items[position]
        assert item.status == "pending" and item.operation is None
        assert (item.proposal_id is None) == (phase == "preview")
        async with store.database.sessions() as sql:
            step = await sql.get(WorkspaceTreeStepRow, (execution.view.plan_id, position))
            assert step.admission_open
        assert not (tmp_path / "tree/sub/data.bin").exists()
    finally:
        await service.close()


async def wait_thread(entered):
    assert await asyncio.to_thread(entered.wait, 10)


@pytest.mark.parametrize("source", ["directory", "file"])
@pytest.mark.parametrize("closed", ["cancel", "expiry", "owner"])
async def test_late_writer_after_admission_is_fenced(
    runtime, tmp_path, monkeypatch, source, closed
):
    service, agent, scope, store, execution, _preview, dispatch = await ready(
        runtime, tmp_path, source
    )
    entered, release = Event(), Event()
    writer_type = WorkspaceWriter if source == "file" else WorkspaceDirectoryWriter
    original = writer_type.apply

    def waiting(self, prepared, **kwargs):
        entered.set()
        assert release.wait(10)
        return original(self, prepared, **kwargs)

    monkeypatch.setattr(writer_type, "apply", waiting)
    task = asyncio.create_task(invoke(service, agent, dispatch))
    try:
        await wait_thread(entered)
        linked = (await store.steps(scope, execution.view.plan_id)).items[dispatch.fence.position]
        assert linked.operation is not None and linked.status == "running"
        if closed == "cancel":
            await store.cancel(scope, execution.view.plan_id)
        else:
            async with store.database.sessions.begin() as sql:
                await sql.execute(
                    update(WorkspaceTreePlanRow)
                    .where(WorkspaceTreePlanRow.plan_id == execution.view.plan_id)
                    .values(
                        **(
                            {"lease_expires_at": time.time() - 1}
                            if closed == "expiry"
                            else {"owner_id": "foreign"}
                        )
                    )
                )
        release.set()
        raw = await asyncio.wait_for(task, 20)
        assert raw["status"] in {"rejected", "uncertain"}
        assert await invoke(service, agent, dispatch) == raw
        assert not (tmp_path / "tree/sub/data.bin").exists()
        if source == "directory":
            assert not (tmp_path / "tree").exists()
        assert await counts(store.database) == (
            [1, 1, 0, 0] if source == "directory" else [2, 2, 1, 1]
        )
        if closed == "expiry":
            assert (await store.get(scope, execution.view.plan_id)).producer_status == "interrupted"
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        await service.close()


@pytest.mark.parametrize("source", ["directory", "file"])
async def test_unknown_reconcile_updates_original_fact_with_zero_replay(
    runtime, tmp_path, monkeypatch, source
):
    import krim_agent.workspace_directory as directory_module
    import krim_agent.workspace_writer as file_module

    service, agent, scope, store, execution, _preview, dispatch = await ready(
        runtime, tmp_path, source
    )
    module = file_module if source == "file" else directory_module
    original = module.os.link if source == "file" else module.rename_at
    calls = 0

    def ambiguous(*args, **kwargs):
        nonlocal calls
        calls += 1
        original(*args, **kwargs)
        raise OSError("test lost original publication receipt")

    if source == "file":
        monkeypatch.setattr(module.os, "link", ambiguous)
    else:
        monkeypatch.setattr(module, "rename_at", ambiguous)
    try:
        value = await invoke(service, agent, dispatch)
        assert value["status"] == "uncertain" and calls == 1
        await store.finish_step(
            scope, execution.view.plan_id, execution.token, dispatch.fence.position
        )
        finished = await store.finish(scope, execution.view.plan_id, execution.token)
        assert finished.status == "uncertain"
        assert await invoke(service, agent, dispatch) == value and calls == 1
        reconciled = await (
            agent.reconcile_file_change(value["operation_id"])
            if source == "file"
            else agent.reconcile_directory_change(value["operation_id"])
        )
        assert reconciled.status == "done" and calls == 1
        cold = await store.get(scope, execution.view.plan_id)
        assert cold.status == ("done" if source == "file" else "partial")
        page = await store.steps(scope, cold.plan_id)
        assert page.items[dispatch.fence.position].operation == reconciled
        assert (await invoke(service, agent, dispatch))["operation_id"] == value["operation_id"]
        assert calls == 1
    finally:
        await service.close()


async def test_tree_credentials_reject_other_tools_model_origin_and_changed_token(
    runtime, tmp_path
):
    service, agent, scope, store, execution = await tree(runtime, tmp_path)
    try:
        dispatch = await store.dispatch(scope, execution.view.plan_id, execution.token, 0)
        registry = service._handle(agent.agent_id).runtime.tools
        context = ToolContext(registry, origin="host", tree_fence=dispatch.fence)
        for name in (
            "command_execute",
            "directory_change",
            "workspace_change",
            "directory_finalize",
            "echo",
        ):
            result = await registry.invoke(name, {}, context)
            assert result.is_error and result.code == "workspace_tree_action"
        context.origin, context.run_id = "model", "original-run"
        result = await registry.invoke(dispatch.name, dispatch.request.model_dump(), context)
        assert result.is_error and result.code == "workspace_tree_model_unbound"
        for fence in (replace(dispatch.fence, token="0" * 64), replace(dispatch.fence, position=1)):
            with pytest.raises(ServiceError) as denied:
                await invoke(service, agent, replace(dispatch, fence=fence))
            assert denied.value.code == "workspace_tree_fence_mismatch"
            assert denied.value.status == 403
        assert await counts(store.database) == [0, 0, 0, 0]
        assert list(tmp_path.iterdir()) == []
    finally:
        await service.close()


@pytest.mark.parametrize("resource", ["workspace", "execution"])
async def test_original_session_resource_change_closes_tree_admission(runtime, tmp_path, resource):
    service, agent, scope, store, execution = await tree(runtime, tmp_path)
    try:
        dispatch = await store.dispatch(scope, execution.view.plan_id, execution.token, 0)
        async with store.database.sessions.begin() as sql:
            session = await sql.get(AgentSessionRow, agent.session_id)
            session.permissions = {
                **session.permissions,
                resource: {**session.permissions[resource], "fingerprint": "0" * 64},
            }
        with pytest.raises(ServiceError):
            await invoke(service, agent, dispatch)
        with pytest.raises(ServiceError):
            await store.renew(scope, execution.view.plan_id, execution.token)
        assert await counts(store.database) == [0, 0, 0, 0]
        assert list(tmp_path.iterdir()) == []
    finally:
        await service.close()


@pytest.mark.parametrize("source", ["directory", "file"])
async def test_two_processes_admit_one_tree_original_attempt(runtime, tmp_path, source):
    service, agent, scope, store, execution, _preview, dispatch = await ready(
        runtime, tmp_path, source
    )
    processes = []
    try:
        payload = json.dumps(
            {
                "database": str(store.database.engine.url),
                "scope": scope,
                "resource": execution.view.resource_fingerprint,
                "owner": store.owner_id,
                "source": source,
                "request": dispatch.request.model_dump(),
                "fence": asdict(dispatch.fence),
            }
        ).encode()
        script = Path(__file__).parent / "fixtures" / "workspace_tree_fence_worker.py"
        for _ in range(2):
            processes.append(
                await asyncio.create_subprocess_exec(
                    sys.executable,
                    str(script),
                    stdin=asyncio.subprocess.PIPE,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
            )
        results = await asyncio.wait_for(
            asyncio.gather(*(process.communicate(payload) for process in processes)), 30
        )
        assert all(process.returncode == 0 for process in processes), [
            stderr.decode() for _, stderr in results
        ]
        values = [json.loads(stdout) for stdout, _ in results]
        assert sum(value["created"] for value in values) == 1
        assert len({value["operation_id"] for value in values}) == 1
        page = await store.steps(scope, execution.view.plan_id)
        assert (
            page.items[dispatch.fence.position].operation.operation_id == values[0]["operation_id"]
        )
        assert await counts(store.database) == (
            [1, 1, 0, 0] if source == "directory" else [2, 2, 1, 1]
        )
        assert not (tmp_path / "tree/sub/data.bin").exists()
    finally:
        for process in processes:
            if process.returncode is None:
                process.kill()
        await asyncio.gather(*(process.wait() for process in processes))
        await service.close()
