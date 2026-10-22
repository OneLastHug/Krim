"""D2唯一真实权限链；夹具只观测Store/Provider/Runner，不复制它们。"""

import asyncio
import hashlib
import json
import os
import sqlite3
from contextlib import closing
from pathlib import Path

import pytest
import pytest_asyncio

from krim_agent.contracts import RunRequest, ServiceError, encoded
from krim_agent.execution.linux import fds
from krim_agent.storage.sqlite import SQLiteStore
from krim_agent.tools.builtins import builtin_registry
from krim_agent.tools.core import Invocation
from krim_agent.tools.executor import ToolContext, ToolExecutor


@pytest_asyncio.fixture
async def data_case(tmp_path, request):
    from test_session_read_sides import Connections

    root = Path(os.environ["KRIM_D2_ROOT"])
    before = fds()
    existing = asyncio.all_tasks()
    loop = asyncio.get_running_loop()
    factory = loop.get_task_factory()
    owned_tasks = []
    def created(active_loop, coro, **kwargs):
        task = factory(active_loop, coro, **kwargs) if factory else asyncio.Task(coro,
            loop=active_loop, **kwargs)
        owned_tasks.append(task)
        return task
    loop.set_task_factory(created)
    database = tmp_path / "pipeline.sqlite"
    store = await SQLiteStore.open(database)
    probe = Connections(store)
    notes = {"node": request.node.nodeid, "database": str(database), "provider_calls": 0,
             "actual_executor_entries": [], "owned": []}
    try:
        yield store, notes
    finally:
        for value in reversed(notes.pop("owned")):
            try:
                await value.close()
            except BaseException as error:
                notes.setdefault("owner_close_errors",
                    []).append({"owner": type(value).__name__, "error": type(error).__name__})
        for manual_store, run_id in notes.pop("manual_runs", []):
            if not manual_store.closed:
                await manual_store.finish(run_id, "failed", "completion_unverified")
        probes = notes.pop("store_probes", [])
        notes["additional_stores"] = [{"closed": value.closed, "pending": sorted(watch.pending),
            "unsafe_gc": watch.unsafe, "pool": watch.events} for value, watch in probes]
        assert all(value.closed and not watch.pending and watch.unsafe == 0 for value,
            watch in probes)
        await store.close()
        second = await SQLiteStore.open(database)
        await second.close()
        folder = Path(os.environ["KRIM_D2_EVIDENCE"]) / "cases"
        folder.mkdir(exist_ok=True)
        consumers = {}
        import krim_agent.composition as composition
        import krim_agent.runtime.engine as engine
        import krim_agent.runtime.service as service
        import krim_agent.tools.executor as executor
        import krim_agent.tools.pipeline as pipeline
        import krim_agent.workspace.operations as operations
        for module in [executor, pipeline, service, engine, operations, composition]:
            file = Path(module.__file__)
            assert file.is_relative_to(root / "source/src")
            consumers[module.__name__] = {"path": str(file),
                "sha256": hashlib.sha256(await asyncio.to_thread(file.read_bytes)).hexdigest()}
        loop.set_task_factory(factory)
        joining = [task for task in owned_tasks if task is not asyncio.current_task()
            and not task.done()]
        notes["joined_task_coroutines"] = [str(task.get_coro()) for task in joining]
        if joining:
            async with asyncio.timeout(5):
                await asyncio.gather(*joining)
        notes["owned_tasks_created"] = len(owned_tasks)
        notes["owned_tasks_joined"] = all(task.done()
            or task is asyncio.current_task() for task in owned_tasks)
        notes["runners"] = [{"closed": runner.closed, "jobs": len(runner.jobs),
            "resources": len(runner._resources),
            "owner_handle_closed": runner._owner.closed,
            "material": str(runner.profile.material), "evidence": str(runner.profile.evidence),
            "tickets": [
            str(
            path) for path in (
            runner.profile.material/"tickets").iterdir(
            )] if (runner.profile.material/"tickets").exists() else []}
            for runner in notes.pop("runners", [])]
        assert all(item["closed"] and item["owner_handle_closed"] and not item["jobs"]
            and not item["resources"] and not item["tickets"] for item in notes["runners"])
        notes["providers"] = [{"calls": p.calls, "active": p.active, "closed": p.closed,
            "streams_closed": p.streams_closed} for p in notes.pop("observed_providers", [])]
        assert all(p["active"] == 0 and p["closed"]
            and p["calls"] == p["streams_closed"] for p in notes["providers"])
        pending = [t.get_name() for t in asyncio.all_tasks()
                   if t is not asyncio.current_task() and t not in existing and not t.done()]
        original = hashlib.sha256(database.read_bytes()).hexdigest()
        backup = folder / (hashlib.sha256(request.node.nodeid.encode()).hexdigest()+".sqlite")
        with closing(sqlite3.connect(database.absolute().as_uri()+"?mode=ro", uri=True)) as source:
            with closing(sqlite3.connect(backup)) as destination, destination:
                source.backup(destination)
        assert hashlib.sha256(database.read_bytes()).hexdigest() == original
        result = notes | {"uid": os.getuid(), "consumers": consumers, "pool": probe.events,
            "store_closed": store.closed, "second_writer_closed": second.closed,
            "pending": sorted(probe.pending), "unsafe_gc": probe.unsafe,
            "FD_before": before, "FD_after": fds(), "FD_equal": fds() == before,
            "unexpected_tasks": pending, "database_sha256": original,
            "readonly_backup": str(backup),
            "backup_sha256": hashlib.sha256(backup.read_bytes()).hexdigest()}
        (
            folder / (
            hashlib.sha256(
            request.node.nodeid.encode()).hexdigest()+".json")).write_text(json.dumps(result,
            indent=2))
        assert not probe.pending and probe.unsafe == 0
        assert sum(
            e[
            "kind"] == "checkout" for e in probe.events) == sum(
            e["kind"] == "checkin" for e in probe.events)
        assert fds() == before and not pending


@pytest.mark.parametrize("case", ["middleware-veto", "zero-next", "middleware-mutate",
     "wrap-args", "twice-next", "concurrent-next", "post-fail", "wrap-after-fail", "normal",
     "service-self-close"])
async def test_wrap_once_permissions_and_truth_after_post_failure(data_case, case, tmp_path,
    monkeypatch):
    if case == "service-self-close":
        await self_close_owner_case(data_case, tmp_path, monkeypatch)
        return
    if case not in {"middleware-veto", "zero-next"}:
        await workspace_wrap_case(data_case, case, tmp_path, monkeypatch)
        return
    store, notes = data_case
    run, _ = await store.begin(RunRequest(text="真实权限内核", request_key="pipeline"), {})
    await store.set_running(run.id)
    registry = builtin_registry()
    original = registry.get("echo")

    async def policy(invocation):
        notes["policy_entered"] = True
        return "allow"

    async def inspect(invocation):
        return "veto" if case == "middleware-veto" else None

    async def wrap(invocation, next_once):
        return {"text": "不得伪造"}

    from dataclasses import replace

    from krim_agent.tools.registry import ToolRegistry

    async def actual(parameters):
        notes["actual_executor_entries"].append("actual echo executor")
        return await original.executor(parameters)

    trusted = replace(original, executor=actual,
                      implementation_digest=hashlib.sha256(
     await asyncio.to_thread(Path(__file__).read_bytes)).hexdigest())
    registry = ToolRegistry((trusted,))
    executor = ToolExecutor(registry, policy, middleware=(inspect,),
                            wrap=wrap if case == "zero-next" else None)
    invocation = Invocation(run.id, "internal-call", "span", 0, "echo", encoded({"text": "合成"}),
        trusted.fingerprint)

    async def approve(invocation, remaining):
        raise AssertionError("此只读节点不应请求审批")

    async def emit(kind, value):
        if kind == "tool_intent":
            notes["intent_count"] = notes.get("intent_count", 0)+1
        return await store.append(run.id, kind, value)

    result = await executor.execute(invocation, ToolContext(emit, approve, 2))
    assert result.status == "failed"
    assert result.code == "middleware_error"
    assert result.executor_started is False and result.result_available is False
    assert notes["actual_executor_entries"] == []
    await store.finish(run.id, "failed", result.code)


def os_profile(tmp_path):
    from krim_agent.execution.types import Profile
    c = Path(os.environ["KRIM_D2_ROOT"])
    base = c / "evidence/prep-base/BASE-QUALIFIED.json"
    raw = json.loads(base.read_bytes())
    return Profile(rootfs=Path(raw["rootfs"]["path"]), base=base,
        cgroup_root=Path("/sys/fs/cgroup/system.slice/krim-task.service"),
        material=c/"materials/d1-os"/(Path(os.environ["KRIM_D2_EVIDENCE"]).name+"-"+tmp_path.name),
        evidence=c/"evidence/d1-os"/(Path(os.environ["KRIM_D2_EVIDENCE"]).name+"-"+tmp_path.name))


async def application_case(data_case, tmp_path, *, cap=4, pre=None, post=None, middleware=(),
     wrap=None, wire=None):
    import importlib.util
    import sys
    name = "fixtures.http_provider_server"
    if name not in sys.modules:
        fixture_source = Path.cwd()/"tests/core/fixtures/http_provider_server.py"
        specification = importlib.util.spec_from_file_location(name, fixture_source)
        module = importlib.util.module_from_spec(specification)
        sys.modules[name] = module
        specification.loader.exec_module(module)
    HTTPFixture = sys.modules[name].HTTPFixture
    from test_session_read_sides import Connections

    from krim_agent.composition import open_application
    from krim_agent.providers.openai_compatible import (
        OpenAICompatibleConfig,
        OpenAICompatibleProvider,
    )
    from krim_agent.workspace.targets import WorkspaceConfiguration, environment
    from krim_agent.workspace.types import WorkspaceProfile

    store, notes = data_case
    await store.close()
    wire = await (wire or HTTPFixture()).__aenter__()
    class OwnedWire:
        async def close(self):
            await wire.__aexit__(None, None, None)
    notes["owned"].append(OwnedWire())
    provider = OpenAICompatibleProvider(OpenAICompatibleConfig(base_url=wire.url,
        model="fixture", max_tool_calls=cap))
    notes["owned"].append(provider)
    notes.setdefault("observed_providers", []).append(provider)
    profile = os_profile(tmp_path)
    configuration = WorkspaceConfiguration(WorkspaceProfile(max_tools_per_step=cap,
        environment=environment(profile)), profile, tmp_path/"objects")
    app = await open_application(Path(notes["database"]), provider=provider,
                                 workspace_profile=configuration, pre=pre, post=post,
        workspace_middleware=middleware, workspace_wrap=wrap)
    notes["owned"].append(app)
    notes.setdefault("runners", []).append(app.runner)
    notes.setdefault("store_probes", []).append((app.store, Connections(app.store)))
    notes["app"] = {"same_provider": app.work is not None
        and app.service.bindings[
        "target-workspace"].loop.provider is app.service.bindings[
        "target-http"].loop.provider is provider,
                    "borrowed": provider not in app.owned_providers}
    return app, wire, provider


async def manual_turn(app, notes, monkeypatch, *, age=0):
    import time
    from types import SimpleNamespace

    import krim_agent.storage.sqlite as sqlite_module
    from krim_agent.config import Settings
    from krim_agent.security import LOCAL
    from krim_agent.sessions.types import SessionCreate
    from krim_agent.storage import sessions as sql
    from krim_agent.workspace.types import Completion, ExecutionAccess, WorkspaceTurnRequest

    binding = app.service.bindings["target-workspace"]
    async with app.service._admission:
        session = await app.sessions.create(LOCAL, "local",
            SessionCreate(request_key="workspace", target_key="target-workspace"))
        access = ExecutionAccess(LOCAL, "local", session.id)
        turn = await sql.submit(app.store, session.id, WorkspaceTurnRequest(request_key="work",
            text="受控写入",
            expected_revision=0, completion=Completion()), Settings(), access=access)
        body, _ = await app.store.turn_request(turn.id)
        actual_clock = sqlite_module.time
        if age:
            monkeypatch.setattr(sqlite_module, "time",
                SimpleNamespace(time=lambda: time.time()-age))
        try:
            run, fresh = await app.store.begin_turn_run(turn.id, body,
                json.loads(binding.descriptor_json), binding.settings)
        finally:
            monkeypatch.setattr(sqlite_module, "time", actual_clock)
        assert fresh
        await app.store.set_running(run.id)
    notes.setdefault("manual_runs", []).append((app.store, run.id))
    return run, binding, access


@pytest.mark.parametrize("case", ["after-legacy-run", "run-first", "tool-first"])
async def test_workspace_approval_uses_frozen_remaining_deadlines(data_case, tmp_path,
    monkeypatch, case):
    import time
    import uuid

    from krim_agent.contracts import ApprovalReply, fingerprint

    app, wire, provider = await application_case(data_case, tmp_path)
    notes = data_case[1]
    run, binding, access = await manual_turn(app, notes, monkeypatch,
        age=170 if case == "run-first" else 40)
    frame = await app.work.prepare(run.id)
    assert frame.conditions["runtime"]["run_timeout_seconds"] == 180.0
    assert frame.conditions["runtime"]["tool_timeout_seconds"] == 90.0
    assert frame.conditions["runtime"]["approval_timeout_seconds"] == 30.0
    assert app.settings.run_timeout == 30 and app.settings.approval_timeout == 20
    registry = binding.loop.executor.registry
    spec = registry.get("workspace_write")
    args = {"expected_revision": 0, "path": "approved.txt", "expected_file_hash": None,
            "offset": 0, "delete_length": 0, "replacement": "真实获批CAS"}
    invocation = Invocation(run.id, str(uuid.uuid4()), "write-span", 0,
                             "workspace_write", encoded(args), spec.fingerprint)
    entered = asyncio.Event()
    actual_append = app.store.append
    async def appended(run_id, kind, data, **kwargs):
        result = await actual_append(run_id, kind, data, **kwargs)
        if kind == "approval_request":
            entered.set()
        return result
    monkeypatch.setattr(app.store, "append", appended)
    scope = app.work.scope(frame, binding.assert_current, asyncio.get_running_loop().time()+140)
    if case == "tool-first":
        # 单独核对真实审批的最短工具预算，不把 1 秒审批预算误作文件发布性能承诺。
        # 原 CAS 成功断言继续由 after-legacy-run/run-first 按冻结 90 秒执行。
        task = asyncio.create_task(app.service._approve(invocation, 1))
        waiter = asyncio.create_task(entered.wait())
        try:
            async with asyncio.timeout(5):
                await entered.wait()
            pending = (await app.service.approvals(run.id))[0]
            assert 0 < pending["expires_at"] - time.time() <= 1
            reply = ApprovalReply(request_key="tool-first", call_id=invocation.call_id,
                tool_hash=invocation.tool_hash, arguments_hash=fingerprint(args), allow=True)
            first = await app.service.reply_approval(run.id, pending["request_id"], reply)
            again = await app.service.reply_approval(run.id, pending["request_id"], reply)
            assert first["replayed"] is False and again["replayed"] is True
            assert await task is True
            assert (await app.artifacts.head(access)).revision == 0
            assert not app.service._pending and provider.calls == len(wire.requests) == 0
            notes["tool_first_approval"] = {
                "expires_at": pending["expires_at"], "remaining": 1, "executions": 0
            }
        finally:
            for pending_task in [task, waiter]:
                if not pending_task.done():
                    pending_task.cancel()
            await asyncio.gather(task, waiter, return_exceptions=True)
            await app.store.finish(run.id, "failed", "completion_unverified")
        return
    context = ToolContext(lambda kind, data: app.store.append(run.id, kind, data),
        app.service._approve, 90, binding.assert_current, scope)
    task = asyncio.create_task(binding.loop.executor.execute(invocation, context))
    waiter = asyncio.create_task(entered.wait())
    try:
        async with asyncio.timeout(5):
            await asyncio.wait((task, waiter), return_when=asyncio.FIRST_COMPLETED)
        if not task.done():
            requests = await app.service.approvals(run.id)
            if requests:
                pending = requests[0]
                assert 0 < pending["expires_at"]-time.time() <= 30
                limit = 10 if case == "run-first" else 30
                assert limit - 1 < pending["expires_at"]-time.time() <= limit
                reply = ApprovalReply(request_key="approve-stable", call_id=invocation.call_id,
                    tool_hash=invocation.tool_hash,
                                      arguments_hash=fingerprint(args), allow=True)
                first = await app.service.reply_approval(run.id, pending["request_id"], reply)
                again = await app.service.reply_approval(run.id, pending["request_id"], reply)
                assert first["replayed"] is False and again["replayed"] is True
        result = await task
        notes["actual_outcome"] = result.model_dump(mode="json")
        assert result.status == "ok"
        assert result.executor_started is True and result.result_available is True
        revision = await app.artifacts.head(access)
        assert revision.revision == 1
        found = await app.artifacts.by_key(access, invocation.call_id, kind="effect")
        assert found.revision_id == revision.revision_id
        chunk = await app.artifacts.chunk(access, revision.revision_id, "approved.txt")
        import base64
        assert base64.b64decode(chunk.data) == "真实获批CAS".encode()
        assert not app.service._pending
        assert provider.calls == len(wire.requests) == 0
        notes["CAS_revision"] = revision.model_dump(mode="json")
    finally:
        for pending in (task, waiter):
            if not pending.done():
                pending.cancel()
        await asyncio.gather(task, waiter, return_exceptions=True)
        await app.store.finish(run.id, "failed", "completion_unverified")


@pytest.mark.parametrize("case", ["run-expired", "tool-expired", "snapshot-expired",
     "pre-expired", "conditions-corrupt", "deny",
     "cancel", "repeat-cancel", "wrong-call", "wrong-args", "wrong-tool", "reply-conflict"])
async def test_workspace_expired_denied_cancelled_approval_zero_effect(data_case, tmp_path,
    monkeypatch, case):
    import uuid

    from sqlalchemy import func, select

    from krim_agent.contracts import ApprovalReply, fingerprint
    from krim_agent.storage.models import artifact_revisions, tool_effects

    async def pre(invocation):
        if case == "pre-expired":
            await asyncio.sleep(0.04)
        return None
    app, wire, provider = await application_case(data_case, tmp_path, pre=pre)
    notes = data_case[1]
    run, binding, access = await manual_turn(app, notes, monkeypatch,
        age=181 if case == "run-expired" else 0)
    frame = await app.work.prepare(run.id)
    args = {"expected_revision": 0, "path": "denied.txt", "expected_file_hash": None,
            "offset": 0, "delete_length": 0, "replacement": "不应写入"}
    spec = binding.loop.executor.registry.get("workspace_write")
    invocation = Invocation(run.id, str(uuid.uuid4()), "deny-span", 0, "workspace_write",
        encoded(args), spec.fingerprint)
    if case == "conditions-corrupt":
        from sqlalchemy import update

        from krim_agent.storage.models import work_turns
        conditions = json.loads(encoded(frame.conditions))
        conditions["runtime"]["approval_timeout_seconds"] = 29.0
        async with app.store.engine.begin() as conn:
            await conn.execute(update(work_turns).where(work_turns.c.run_id == run.id).values(
                conditions=conditions, effective_hash=fingerprint({"subject": frame.subject,
                "conditions": conditions})))
    requested = asyncio.Event()
    actual_append = app.store.append
    async def append(run_id, kind, value, **kwargs):
        result = await actual_append(run_id, kind, value, **kwargs)
        if kind == "approval_request":
            requested.set()
        return result
    monkeypatch.setattr(app.store, "append", append)
    scope = app.work.scope(frame, binding.assert_current,
        asyncio.get_running_loop().time()+(0 if case == "run-expired" else 180))
    timeout = 0 if case == "tool-expired" else 0.02 if case == "pre-expired" else 90
    task = None
    waiter = None
    try:
        if case == "snapshot-expired":
            entered, release = asyncio.Event(), asyncio.Event()
            original = app.store.snapshot
            async def snapshot(run_id):
                value = await original(run_id)
                entered.set()
                await release.wait()
                return value
            monkeypatch.setattr(app.store, "snapshot", snapshot)
            task = asyncio.create_task(app.service._approve(invocation, 0.02))
            async with asyncio.timeout(5):
                await entered.wait()
            await asyncio.sleep(0.04)
            release.set()
            with pytest.raises(ServiceError, match="approval_expired"):
                await task
            assert not requested.is_set()
            monkeypatch.setattr(app.store, "snapshot", original)
        else:
            context = ToolContext(lambda kind, data: app.store.append(run.id, kind, data),
                app.service._approve, timeout, binding.assert_current, scope)
            task = asyncio.create_task(binding.loop.executor.execute(invocation, context))
            waiter = asyncio.create_task(requested.wait())
            async with asyncio.timeout(5):
                await asyncio.wait((task, waiter), return_when=asyncio.FIRST_COMPLETED)
            if case in {"run-expired", "tool-expired", "pre-expired", "conditions-corrupt"}:
                result = await task
                if case == "conditions-corrupt":
                    assert result.code == "facts_corrupt"
                else:
                    assert result.code in {"run_timeout", "tool_timeout", "approval_expired"}
                assert not requested.is_set()
                assert result.executor_started is False
            else:
                assert requested.is_set() and not task.done()
                pending = (await app.service.approvals(run.id))[0]
                reply = ApprovalReply(request_key="deny", call_id=invocation.call_id,
                    arguments_hash=fingerprint(args), tool_hash=spec.fingerprint, allow=False)
                if case in {"wrong-call", "wrong-args", "wrong-tool"}:
                    field = {"wrong-call": "call_id", "wrong-args": "arguments_hash",
                        "wrong-tool": "tool_hash"}[case]
                    wrong = reply.model_copy(
                        update={field: str(uuid.uuid4()) if field == "call_id" else "f"*64})
                    with pytest.raises(ServiceError, match="approval_conflict"):
                        await app.service.reply_approval(run.id, pending["request_id"], wrong)
                    assert not task.done()
                if case in {"cancel", "repeat-cancel"}:
                    task.cancel()
                    if case == "repeat-cancel":
                        task.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await task
                else:
                    first = await app.service.reply_approval(run.id, pending["request_id"], reply)
                    again = await app.service.reply_approval(run.id, pending["request_id"], reply)
                    assert first["replayed"] is False and again["replayed"] is True
                    if case == "reply-conflict":
                        with pytest.raises(ServiceError, match="approval_conflict"):
                            await app.service.reply_approval(run.id, pending["request_id"],
                                reply.model_copy(update={"allow": True}))
                    result = await task
                    assert result.code == "approval_denied" and result.executor_started is False
        assert not app.service._pending
        async with app.store.engine.begin() as conn:
            assert await conn.scalar(select(func.count()).select_from(tool_effects)) == 0
            assert await conn.scalar(select(func.count()).select_from(artifact_revisions)) == 1
        assert (await app.artifacts.head(access)).revision == 0
        assert not list(app.runner.profile.material.glob("job-*"))
        assert provider.calls == len(wire.requests) == 0
        notes["zero_effect"] = {"ledger": 0, "published": 0, "runner_jobs": 0,
            "provider": provider.calls}
    finally:
        for value in [task, waiter]:
            if value is not None and not value.done():
                value.cancel()
        await asyncio.gather(*[value for value in [task, waiter] if value is not None],
            return_exceptions=True)
        if case == "conditions-corrupt":
            backup = tmp_path / "corrupt-conditions.sqlite"
            database = Path(notes["database"])
            def snapshot_corruption():
                before_hash = hashlib.sha256(database.read_bytes()).hexdigest()
                uri = database.absolute().as_uri()+"?mode=ro"
                with closing(sqlite3.connect(uri, uri=True)) as source:
                    with closing(sqlite3.connect(backup)) as destination, destination:
                        source.backup(destination)
                assert hashlib.sha256(database.read_bytes()).hexdigest() == before_hash
                return {
                    "path": str(backup), "sha256": hashlib.sha256(backup.read_bytes()).hexdigest()
                }
            notes["corrupt_conditions_evidence"] = await asyncio.to_thread(snapshot_corruption)
            async with app.store.engine.begin() as conn:
                await conn.execute(update(work_turns).where(work_turns.c.run_id == run.id).values(
                    conditions=frame.conditions, effective_hash=frame.effective_fingerprint))
        await app.store.finish(run.id, "failed", "completion_unverified")


def approve_fixture(app, monkeypatch):
    from krim_agent.contracts import ApprovalReply
    original = app.store.append
    async def append(run_id, kind, value, **kwargs):
        result = await original(run_id, kind, value, **kwargs)
        if kind == "approval_request":
            await app.service.reply_approval(run_id, value["request_id"], ApprovalReply(
                request_key="synthetic-approval-"+value["call_id"], call_id=value["call_id"],
                arguments_hash=value["arguments_hash"], tool_hash=value["tool_hash"], allow=True))
        return result
    monkeypatch.setattr(app.store, "append", append)


async def workspace_wrap_case(data_case, case, tmp_path, monkeypatch):
    import uuid

    from sqlalchemy import func, select
    from sqlalchemy.ext.asyncio import AsyncConnection

    from krim_agent.runtime.facts import project_facts
    from krim_agent.storage.models import tool_effects

    entered, release = asyncio.Event(), asyncio.Event()
    notes = data_case[1]
    async def middleware(invocation):
        if case == "middleware-mutate":
            object.__setattr__(invocation, "arguments_json", "{}")
        return None
    async def post(invocation, observation):
        if case == "post-fail":
            raise RuntimeError("synthetic read-only audit failure")
    async def wrap(invocation, next_once):
        if case == "wrap-args":
            return await next_once({"replacement": "不准改参"})
        first = next_once()
        try:
            if case in {"twice-next", "concurrent-next"}:
                async with asyncio.timeout(5):
                    await entered.wait()
                assert not first.done()
                if case == "twice-next":
                    next_once()
                else:
                    async def other():
                        return await next_once()
                    second = asyncio.create_task(other())
                    try:
                        await second
                    finally:
                        await asyncio.gather(second, return_exceptions=True)
            result = await first
            if case == "wrap-after-fail":
                raise RuntimeError("synthetic wrapper failure after real publish")
            return result
        finally:
            release.set()

    app, wire, provider = await application_case(data_case, tmp_path, middleware=(middleware,),
     post=post, wrap=wrap)
    run, binding, access = await manual_turn(app, notes, monkeypatch)
    approve_fixture(app, monkeypatch)
    execute = AsyncConnection.execute
    actual_CAS = []
    async def barrier(conn, statement, *args, **kwargs):
        result = await execute(conn, statement, *args, **kwargs)
        if conn.engine is app.store.engine and getattr(statement, "is_insert",
            False) and getattr(getattr(statement, "table", None), "name",
            None) == "artifact_revisions":
            actual_CAS.append("actual INSERT before commit")
            if case in {"twice-next", "concurrent-next"}:
                entered.set()
                await release.wait()
        return result
    monkeypatch.setattr(AsyncConnection, "execute", barrier)
    frame = await app.work.prepare(run.id)
    scope = app.work.scope(frame, binding.assert_current, asyncio.get_running_loop().time()+180)
    spec = binding.loop.executor.registry.get("workspace_write")
    args = {"expected_revision": 0, "path": "truth.txt", "expected_file_hash": None,
            "offset": 0, "delete_length": 0, "replacement": "不可抹除的真实写"}
    invocation = Invocation(run.id, str(uuid.uuid4()), "truth-span", 0, "workspace_write",
        encoded(args), spec.fingerprint)
    task = asyncio.create_task(binding.loop.executor.execute(invocation,
        ToolContext(lambda kind, value: app.store.append(run.id, kind, value),
        app.service._approve, 90, binding.assert_current, scope)))
    try:
        async with asyncio.timeout(10):
            result = await task
        wrote = case not in {"middleware-mutate", "wrap-args"}
        assert len(actual_CAS) == int(wrote)
        head = await app.artifacts.head(access)
        assert head.revision == int(wrote)
        assert result.executor_started is wrote and result.result_available is wrote
        assert result.status == ("ok" if case == "normal" else "failed")
        assert result.code == (
            None if case == "normal" else "post_hook_error" if case == "post-fail"
            else "wrap_reentry" if case in {"twice-next", "concurrent-next"}
            else "middleware_error"
        )
        if wrote:
            found = await app.artifacts.by_key(access, invocation.call_id, kind="effect")
            assert found.revision_id == head.revision_id
            async with app.store.engine.begin() as conn:
                effects = (await conn.execute(select(tool_effects))).mappings().all()
                assert len(
                    effects) == 1 and effects[
                    0]["receipt"]["artifact_revision_id"] == head.revision_id
            facts = project_facts(await app.store.snapshot(run.id))
            observation = next(x for x in facts.tools if x.call_id == invocation.call_id)
            assert observation.executor_started is True and observation.result_available is True
            assert observation.code == result.code
        else:
            async with app.store.engine.begin() as conn:
                assert await conn.scalar(select(func.count()).select_from(tool_effects)) == 0
        assert provider.calls == len(wire.requests) == 0
        notes["write_truth"] = {"CAS_count": len(actual_CAS),
            "head": head.model_dump(mode="json"), "outcome": result.model_dump(mode="json")}
    finally:
        release.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await app.store.finish(run.id, "failed", "completion_unverified")


def http_suite():
    import importlib.util
    import sys
    # 先加载本任务原HTTP夹具，避免不同pytest收集根的fixtures命名空间歧义。
    for name, path in [("fixtures.http_provider_server",
        "tests/core/fixtures/http_provider_server.py"),
                       ("test_http_provider", "tests/core/test_http_provider.py")]:
        if name not in sys.modules:
            spec = importlib.util.spec_from_file_location(name, Path.cwd()/path)
            module = importlib.util.module_from_spec(spec)
            sys.modules[name] = module
            spec.loader.exec_module(module)
    return sys.modules["test_http_provider"]


@pytest.mark.parametrize("case", ["reads-2", "reads-3", "reads-4", "read-write-order",
     "legacy-cap"])
async def test_read_parallel_write_serial_and_scope(data_case, tmp_path, monkeypatch, case):
    import base64
    import threading

    from krim_agent.security import LOCAL
    from krim_agent.sessions.types import SessionCreate
    from krim_agent.tools.pipeline import CURRENT
    from krim_agent.workspace.artifacts import ArtifactStore
    from krim_agent.workspace.types import (
        Completion,
        ExecutionAccess,
        InputFile,
        InputSnapshotRequest,
        WorkspaceTurnRequest,
    )
    blob_marker = {}
    original_blob = ArtifactStore._blob
    def blob(artifacts, digest, size):
        value = original_blob(artifacts, digest, size)
        if CURRENT.get() is not None and blob_marker and not blob_marker["release"].is_set():
            blob_marker["readers"].add(threading.get_ident())
            if len(blob_marker["readers"]) == blob_marker["count"]:
                blob_marker["loop"].call_soon_threadsafe(blob_marker["entered"].set)
            if not blob_marker["release"].wait(5):
                raise AssertionError("actual file reader barrier was not released")
            blob_marker["completed"].append(threading.get_ident())
        return value
    monkeypatch.setattr(ArtifactStore, "_blob", blob)
    suite = http_suite()
    wire = suite.D2Wire([])
    count = int(case[-1]) if case.startswith("reads-") else 3 if case == "read-write-order" else 2
    cap = count if case.startswith("reads-") else 4
    app, wire, provider = await application_case(data_case, tmp_path, cap=cap, wire=wire)
    notes = data_case[1]
    session = await app.sessions.create(LOCAL, "local", SessionCreate(request_key="parallel",
        target_key="target-workspace"))
    access = ExecutionAccess(LOCAL, "local", session.id)
    body = b"0123456789"
    seed = await app.artifacts.input(access, InputSnapshotRequest(request_key="seed",
        expected_head_revision=0,
        files=(InputFile(path="source.txt", data=base64.b64encode(body).decode(),
        sha256=hashlib.sha256(body).hexdigest()),)))
    calls = [suite.d2_call(i, name="workspace_read",
        arguments=json.dumps({"revision_id": seed.revision_id,
        "path": "source.txt", "offset": i, "length": 1})) for i in range(count)]
    if case == "read-write-order":
        calls.append(suite.d2_call(count, name="workspace_write",
            arguments=json.dumps({"expected_revision": 1,
            "path": "changed.txt", "expected_file_hash": None, "offset": 0, "delete_length": 0,
            "replacement": "after reads"})))
    wire.parts = [suite.frame(suite.choice({"tool_calls": calls[:1]})),
                  suite.frame(suite.choice({"tool_calls": calls[1:]})),
                  suite.frame(suite.choice(finish="tool_calls")), suite.frame("[DONE]")]
    wire.sent = [asyncio.Event() for _ in wire.parts]
    wire.gates = [asyncio.Event() for _ in wire.parts]
    wire.consumed = [asyncio.Event() for _ in wire.parts]
    # 客户端消费确认由HTTP三节点负责；此处同真实Provider/AgentLoop核实调度。
    readers = set()
    all_readers = asyncio.Event()
    read_release = threading.Event()
    read_done = []
    write_started = []
    target_started = []
    sql_released = []
    loop = asyncio.get_running_loop()
    # 测真实文件读取并发；受保护的Store元数据事务有意串行，不以此强求改锁。
    # 下面的原_blob调用已在application创建前安装并进入绑定，且始终调用原实现。
    marker = {"loop": loop, "count": count, "readers": readers, "entered": all_readers,
              "release": read_release, "completed": sql_released}
    blob_marker.update(marker)
    original_append = app.store.append
    async def append(run_id, kind, data, **kwargs):
        if kind == "tool_start":
            target_started.append(data["name"])
            if data["name"] == "workspace_write":
                assert len(read_done) == count
                write_started.append(data["call_id"])
        result = await original_append(run_id, kind, data, **kwargs)
        if kind == "tool_end" and data["name"] == "workspace_read":
            read_done.append(data["provider_call_id"])
        if kind == "approval_request":
            from krim_agent.contracts import ApprovalReply
            await app.service.reply_approval(run_id, data["request_id"],
                ApprovalReply(request_key="parallel-approval",
                call_id=data["call_id"], tool_hash=data["tool_hash"],
                arguments_hash=data["arguments_hash"], allow=True))
        return result
    monkeypatch.setattr(app.store, "append", append)
    run = None
    try:
        if case == "legacy-cap":
            from krim_agent.contracts import RunRequest
            run = await app.service.start(RunRequest(text="no legacy multi",
                target="target-http", request_key="legacy"))
        else:
            turn = await app.sessions.submit(LOCAL, "local", session.id,
                WorkspaceTurnRequest(request_key="parallel-turn",
                text="parallel", expected_revision=0, completion=Completion()))
        for index in range(3):
            async with asyncio.timeout(5):
                await wire.sent[index].wait()
            assert target_started == []
            wire.gates[index].set()
        wire.gates[3].set()
        if case == "legacy-cap":
            finished = await app.service.wait(run.id)
            assert finished.code == "tool_limit"
            assert not target_started
            assert len(wire.requests) == provider.calls == 1
        else:
            async with asyncio.timeout(5):
                await all_readers.wait()
            assert len(readers) == count and write_started == []
            read_release.set()
            finished_turn = await app.sessions.wait(LOCAL, "local", session.id, turn.id)
            run = await app.service.get(finished_turn.run_id)
            assert run.code == "completion_unverified"
            assert len(read_done) == count and len(sql_released) == count
            assert len(write_started) == int(case == "read-write-order")
            assert len(wire.requests) == provider.calls == 2
            observations = wire.bodies[1]["messages"]
            tools = [m for m in observations if m["role"] == "tool"]
            assert [m["tool_call_id"] for m in tools] == [c["id"] for c in calls]
            for index, message in enumerate(tools[:count]):
                assert base64.b64decode(
                    json.loads(message["content"])["data"]) == str(index).encode()
        assert app.service.bindings[
            "target-http"].loop.provider is app.service.bindings[
            "target-workspace"].loop.provider is provider
        assert not app.runner.jobs and not app.runner._resources
        notes["parallel"] = {"case": case, "actual_file_reader_threads": len(readers),
            "file_reads_released": len(sql_released),
            "read_ends": read_done, "write_starts": write_started, "provider_calls": provider.calls,
            "run_id": run.id if run else None, "actual_client_shared": True}
    finally:
        read_release.set()
        for gate in wire.gates:
            gate.set()
        if run is not None and run.id in app.service.tasks and not app.service.tasks[run.id].done():
            await app.service.cancel(run.id)


@pytest.mark.parametrize("phase", ["pre", "middleware", "policy", "approval", "intent",
     "file-thread", "before-cas", "ready", "after-gate", "export", "command-normal"])
async def test_effect_fence_after_every_await(data_case, tmp_path, monkeypatch, phase):
    import uuid

    from sqlalchemy import func, select
    from sqlalchemy.ext.asyncio import AsyncConnection

    from krim_agent.storage.models import artifact_revisions
    from krim_agent.tools.pipeline import CURRENT, PHASE
    from krim_agent.workspace.operations import WorkspaceOperations

    holder = {}
    notes = data_case[1]
    def change():
        app = holder["app"]
        async def replacement(invocation):
            return "allow"
        app.service.bindings["target-workspace"].loop.executor.policy = replacement
        notes["binding_changes"] = notes.get("binding_changes", 0)+1
    async def pre(invocation):
        if phase == "pre":
            change()
        return None
    async def middleware(invocation):
        if phase == "middleware":
            change()
        return None
    policy = WorkspaceOperations.policy
    async def decided(operations, invocation):
        result = await policy(operations, invocation)
        if phase == "policy":
            change()
        return result
    monkeypatch.setattr(WorkspaceOperations, "policy", decided)
    app, wire, provider = await application_case(data_case, tmp_path, pre=pre,
        middleware=(middleware,))
    holder["app"] = app
    run, binding, access = await manual_turn(app, notes, monkeypatch)
    original_append = app.store.append
    async def append(run_id, kind, value, **kwargs):
        result = await original_append(run_id, kind, value, **kwargs)
        if kind == "approval_request":
            from krim_agent.contracts import ApprovalReply
            await app.service.reply_approval(run_id, value["request_id"],
                ApprovalReply(request_key="fence-approval",
                call_id=value["call_id"], arguments_hash=value["arguments_hash"],
                tool_hash=value["tool_hash"], allow=True))
            if phase == "approval":
                change()
        if kind == "tool_intent" and phase == "intent":
            change()
        return result
    monkeypatch.setattr(app.store, "append", append)
    thread = asyncio.to_thread
    async def submitted(function, *args, **kwargs):
        if phase == "file-thread" and CURRENT.get() is not None:
            change()
        return await thread(function, *args, **kwargs)
    monkeypatch.setattr(asyncio, "to_thread", submitted)
    execute = AsyncConnection.execute
    fired = False
    async def query(conn, statement, *args, **kwargs):
        nonlocal fired
        result = await execute(conn, statement, *args, **kwargs)
        current = CURRENT.get()
        target = {"ready": "ready", "after-gate": "payload_output",
            "export": "export_end"}.get(phase)
        cas = phase == "before-cas" and getattr(statement, "is_insert",
            False) and getattr(getattr(statement, "table", None), "name",
            None) == "artifact_revisions"
        if current is not None and not fired and (cas or target is not None
            and PHASE.get() == target):
            fired = True
            change()
        return result
    monkeypatch.setattr(AsyncConnection, "execute", query)
    command = phase in {"ready", "after-gate", "export", "command-normal"}
    name = "command_run" if command else "workspace_write"
    if command:
        code = (
            "from pathlib import Path;import time;print('SYNTHETIC-E"
            "NTER',flush=True);time.sleep(0.2);Path('command.txt').w"
            "rite_text('synthetic')"
        )
        args = {"expected_revision": 0, "program": "python", "argv": ["-c", code], "cwd": "."}
    else:
        args = {"expected_revision": 0, "path": "fenced.txt", "expected_file_hash": None,
                "offset": 0, "delete_length": 0, "replacement": "synthetic fenced write"}
    spec = binding.loop.executor.registry.get(name)
    invocation = Invocation(run.id, str(uuid.uuid4()), "fence-span", 0, name, encoded(args),
        spec.fingerprint)
    frame = await app.work.prepare(run.id)
    scope = app.work.scope(frame, binding.assert_current, asyncio.get_running_loop().time()+180)
    result = await binding.loop.executor.execute(invocation, ToolContext(lambda kind,
        data: app.store.append(run.id, kind, data),
        app.service._approve, 90, binding.assert_current, scope))
    notes["outcome"] = result.model_dump(mode="json")
    normal = phase == "command-normal"
    assert result.status == ("ok" if normal else "failed")
    assert result.code == (None if normal else "target_binding_changed")
    head = await app.artifacts.head(access)
    assert head.revision == int(normal)
    async with app.store.engine.begin() as conn:
        assert await conn.scalar(
            select(func.count()).select_from(artifact_revisions)) == 1+int(normal)
    events = []
    if app.runner.profile.evidence.exists():
        import stat
        for job in app.runner.profile.evidence.iterdir():
            path = job/"resources.jsonl"
            assert stat.S_ISREG(path.lstat().st_mode)
            events.extend(json.loads(line) for line in path.read_text().splitlines())
    gate = sum(item["kind"] == "gate_opened" for item in events)
    assert gate == int(phase in {"after-gate", "export", "command-normal"})
    if command:
        assert sum(item["kind"] == "process_created" for item in events) == 1
        assert sum(item["kind"] == "process_waited" for item in events) == 1
        assert sum(item["kind"] == "pidfd_closed" for item in events) == 1
        assert any(item["kind"] == "populated_zero" for item in events)
        assert any(item["kind"] == "leaf_removed" for item in events)
    if phase in {"after-gate", "export", "command-normal"}:
        from krim_agent.storage import work
        effect = await app.store.work_read(lambda conn: work.effect(conn, access,
            invocation.call_id))
        assert effect["receipt"]["executor_started"] is True
        if normal:
            assert effect[
                "receipt"]["exit_code"] == 0 and effect["receipt"]["output_complete"] is True
            assert effect["receipt"]["cleanup"] == "clean"
    assert not app.runner.jobs and not app.runner._resources
    assert provider.calls == len(wire.requests) == 0
    notes["fence"] = {"phase": phase, "gate_opened": gate, "actual_CAS_published": int(normal),
        "private_object_files": sorted(path.name for path in app.artifacts.root.iterdir()),
        "runner_evidence": str(app.runner.profile.evidence)}
    await app.store.finish(run.id, "failed", "completion_unverified")


async def self_close_owner_case(data_case, tmp_path, monkeypatch):
    """同一 HTTP cap2 → 并行 read → post，真实捕获 self_wait 后自然收束。"""
    import base64

    from krim_agent.security import LOCAL
    from krim_agent.sessions.types import SessionCreate
    from krim_agent.workspace.types import (
        Completion,
        ExecutionAccess,
        InputFile,
        InputSnapshotRequest,
        WorkspaceTurnRequest,
    )

    holder = {}
    entered = asyncio.Event()
    attempted = False
    rejected = []
    async def post(invocation, result):
        nonlocal attempted
        if not attempted:
            attempted = True
            service = holder["app"].service
            producer = service.tasks[invocation.run_id]
            entered.set()
            with pytest.raises(ServiceError, match="self_wait") as error:
                await service.close()
            assert error.value.code == "self_wait"
            assert service._close_task is None and not service.closed
            assert producer.cancelling() == 0 and not producer.done()
            rejected.append(invocation.call_id)
    suite = http_suite()
    app, wire, provider = await application_case(data_case, tmp_path, cap=2, post=post)
    holder["app"] = app
    notes = data_case[1]
    stopped = asyncio.Event()
    original_stop = app.store.request_stop
    async def stopped_writer(run_id):
        result = await original_stop(run_id)
        stopped.set()
        return result
    monkeypatch.setattr(app.store, "request_stop", stopped_writer)
    session = await app.sessions.create(LOCAL, "local", SessionCreate(request_key="self-close",
        target_key="target-workspace"))
    access = ExecutionAccess(LOCAL, "local", session.id)
    body = b"synthetic-owned-close"
    revision = await app.artifacts.input(access, InputSnapshotRequest(request_key="seed",
        expected_head_revision=0,
        files=(InputFile(path="data.txt", data=base64.b64encode(body).decode(),
        sha256=hashlib.sha256(body).hexdigest()),)))
    def script(request):
        if request["messages"][-1]["role"] == "tool":
            return [suite.frame(suite.choice({"content": "done"})),
                suite.frame(suite.choice(finish="stop")), suite.frame("[DONE]")]
        return [suite.frame(suite.choice({"tool_calls": [suite.d2_call(i, name="workspace_read",
            arguments=json.dumps({
            "revision_id": revision.revision_id, "path": "data.txt", "offset": 0,
            "length": 1})) for i in range(2)]})),
            suite.frame(suite.choice(finish="tool_calls")), suite.frame("[DONE]")]
    wire.script = script
    request = WorkspaceTurnRequest(request_key="self-close-turn", text="parallel owned close",
        expected_revision=0, completion=Completion())
    turn = await app.sessions.submit(LOCAL, "local", session.id, request)
    try:
        async with asyncio.timeout(5):
            await entered.wait()
            finished = await app.sessions.wait(LOCAL, "local", session.id, turn.id)
        producer = app.service.tasks[finished.run_id]
        closing = app.service._close_task
        cyclic = closing is not None and not closing.done(
            ) and not producer.done() and producer.cancelling() > 0
        assert not cyclic, (
            "并行工具子Task的self-close必须被原权限边界拒绝，"
            "不能取消并等待自己的producer形成循环"
        )
        assert rejected and not stopped.is_set()
        assert producer.done() and not producer.cancelled()
        facts = await app.service.result(finished.run_id)
        assert len(facts.tools) == 2 and all(tool.status == "ok" for tool in facts.tools)
        assert all(tool.executor_started is True
            and tool.result_available is True for tool in facts.tools)
        assert (await app.service.get(finished.run_id)).code == "completion_unverified"
        replay = await app.sessions.submit(LOCAL, "local", session.id, request)
        assert replay.id == turn.id and replay.run_id == finished.run_id
        assert provider.calls == len(wire.requests) == 2
        notes["self_close_dependency"] = {"producer": id(producer), "closing": None,
            "parallel_reader_post": True, "cycle_observed": cyclic, "self_wait": rejected,
            "external_circuit_breaker_used": False,
            "product_close_completed_without_external_help": True,
            "run_id": finished.run_id,
            "tools": [tool.model_dump(mode="json") for tool in facts.tools],
            "original_key_same_turn": replay.id == turn.id, "provider_calls": provider.calls}
    finally:
        await app.close()
        notes["actual_finally_resources"] = {"store_closed": app.store.closed,
            "runner_closed": app.runner.closed, "artifact_closed": app.artifacts.closed,
            "producer_done": all(task.done() for task in app.service.tasks.values()),
            "service_close_task_cancelled": bool(app.service._close_task
            and app.service._close_task.cancelled())}
        assert all(notes["actual_finally_resources"][key] for key in ["store_closed",
            "runner_closed", "artifact_closed", "producer_done"])
        assert not notes["actual_finally_resources"]["service_close_task_cancelled"]


@pytest.mark.parametrize("site,action", [(site, action) for site in ("middleware", "wrap",
     "next") for action in ("close", "wait", "cancel")] + [("cleanup", "close")])
async def test_causal_owner_reentrancy(data_case, tmp_path, monkeypatch, site, action):
    """真实已注册 producer 的调用后代，拒绝先于关闭/stop/cancel 副作用。"""
    import base64

    from krim_agent.security import LOCAL
    from krim_agent.sessions.types import SessionCreate
    from krim_agent.workspace.operations import WorkspaceOperations
    from krim_agent.workspace.types import (
        Completion,
        ExecutionAccess,
        InputFile,
        InputSnapshotRequest,
        WorkspaceTurnRequest,
    )

    holder, seen = {}, []
    entered, release = asyncio.Event(), asyncio.Event()
    async def probe(invocation):
        app = holder["app"]
        producer = app.service.tasks[invocation.run_id]
        before = producer.cancelling()
        assert not producer.done()
        with pytest.raises(ServiceError, match="self_wait") as error:
            if action == "close":
                await app.service.close()
            elif action == "cancel":
                await app.service.cancel(invocation.run_id)
            else:
                await app.service.wait(invocation.run_id)
        assert error.value.code == "self_wait"
        assert app.service._close_task is None and not app.service.closed
        assert producer.cancelling() == before
        assert invocation.run_id not in app.service._cancelled or site == "cleanup"
        assert not holder["stops"] or site == "cleanup"
        seen.append({"site": site, "action": action, "task": id(asyncio.current_task()),
            "producer": id(producer), "cancelling": before})
    async def middleware(invocation):
        child = asyncio.create_task(probe(invocation))
        await child
    async def wrap(invocation, next_once):
        if site == "wrap":
            child = asyncio.create_task(probe(invocation))
            await child
        return await next_once()
    async def post(invocation, observation):
        if site == "cleanup" and not entered.is_set():
            entered.set()
            try:
                await release.wait()
            except asyncio.CancelledError:
                await probe(invocation)
                raise
    original_policy = WorkspaceOperations.policy
    async def policy(operations, invocation):
        if site == "next":
            await probe(invocation)
        return await original_policy(operations, invocation)
    monkeypatch.setattr(WorkspaceOperations, "policy", policy)
    app, wire, provider = await application_case(data_case, tmp_path, cap=2,
        middleware=(middleware,) if site == "middleware" else (), wrap=wrap if site in {"wrap",
        "next"} else None, post=post)
    holder.update(app=app, stops=[])
    original_stop = app.store.request_stop
    async def stop(run_id):
        holder["stops"].append(run_id)
        return await original_stop(run_id)
    monkeypatch.setattr(app.store, "request_stop", stop)
    session = await app.sessions.create(LOCAL, "local", SessionCreate(request_key="owner",
        target_key="target-workspace"))
    access = ExecutionAccess(LOCAL, "local", session.id)
    body = b"synthetic"
    revision = await app.artifacts.input(access, InputSnapshotRequest(request_key="seed",
        expected_head_revision=0, files=(InputFile(path="data.txt",
        data=base64.b64encode(body).decode(), sha256=hashlib.sha256(body).hexdigest()),)))
    suite = http_suite()
    def script(request):
        if request["messages"][-1]["role"] == "tool":
            return [suite.frame(suite.choice({"content": "done"})),
                suite.frame(suite.choice(finish="stop")), suite.frame("[DONE]")]
        return [suite.frame(suite.choice({"tool_calls": [suite.d2_call(i, name="workspace_read",
            arguments=json.dumps({"revision_id": revision.revision_id, "path": "data.txt",
            "offset": 0, "length": 1})) for i in range(2)]})),
            suite.frame(suite.choice(finish="tool_calls")), suite.frame("[DONE]")]
    wire.script = script
    request = WorkspaceTurnRequest(request_key="owner-turn", text="owner", expected_revision=0,
        completion=Completion())
    turn = await app.sessions.submit(LOCAL, "local", session.id, request)
    if site == "cleanup":
        async with asyncio.timeout(5):
            await entered.wait()
        linked = await app.sessions.turn(LOCAL, "local", session.id, turn.id)
        await app.service.cancel(linked.run_id)
        release.set()
    async with asyncio.timeout(5):
        finished = await app.sessions.wait(LOCAL, "local", session.id, turn.id)
    assert seen and all(item["task"] != item["producer"] for item in seen)
    if site == "cleanup":
        assert any(item["cancelling"] > 0 for item in seen)
    else:
        assert len(seen) == 2 and holder["stops"] == []
        assert all(
            tool.status == "ok" for tool in (await app.service.result(finished.run_id)).tools)
    replay = await app.sessions.submit(LOCAL, "local", session.id, request)
    assert replay.id == turn.id and replay.run_id == finished.run_id
    assert provider.calls == len(wire.requests) == (1 if site == "cleanup" else 2)
    data_case[1]["owner_checks"] = seen
    await app.close()


async def test_causal_owner_revocation_and_external_contexts(data_case, tmp_path, monkeypatch):
    """调用结束/producer结束的继承上下文共享撤销；公开 Scope 不授予身份。"""
    import base64
    from dataclasses import replace

    from krim_agent.runtime.service import RunService
    from krim_agent.security import LOCAL
    from krim_agent.sessions.types import SessionCreate
    from krim_agent.tools.pipeline import CURRENT
    from krim_agent.workspace.types import (
        Completion,
        ExecutionAccess,
        InputFile,
        InputSnapshotRequest,
        WorkspaceTurnRequest,
    )

    suite = http_suite()
    wire = suite.HTTPFixture(pause_requests=(2,))
    holder, children, seen, scopes = {}, [], [], []
    after_call, after_producer = asyncio.Event(), asyncio.Event()
    call_checked, producer_checked = asyncio.Event(), asyncio.Event()
    async def post(invocation, result):
        app = holder["app"]
        scopes.append(CURRENT.get())
        # 同一字符串甚至同一 Task 映射，仍不能把另一 service 当本调用 owner。
        other = RunService(app.store, app.service.loop, app.settings, {})
        other.tasks[invocation.run_id] = app.service.tasks[invocation.run_id]
        other._external()
        other.tasks.clear()
        await other.close()
        async def inherited():
            await after_call.wait()
            producer = app.service.tasks[invocation.run_id]
            assert not producer.done()
            app.service._external()
            seen.append("call-revoked-producer-active")
            call_checked.set()
            await after_producer.wait()
            assert producer.done()
            app.service._external()
            await app.service.wait(invocation.run_id)
            seen.append("producer-done-revoked")
            producer_checked.set()
        if not children:
            children.append(asyncio.create_task(inherited()))
    app, wire, provider = await application_case(data_case, tmp_path, cap=2, post=post, wire=wire)
    holder["app"] = app
    session = await app.sessions.create(LOCAL, "local", SessionCreate(request_key="contexts",
        target_key="target-workspace"))
    access = ExecutionAccess(LOCAL, "local", session.id)
    body = b"synthetic"
    revision = await app.artifacts.input(access, InputSnapshotRequest(request_key="seed",
        expected_head_revision=0, files=(InputFile(path="data.txt",
        data=base64.b64encode(body).decode(), sha256=hashlib.sha256(body).hexdigest()),)))
    def script(request):
        if request["messages"][-1]["role"] == "tool":
            return [suite.frame(suite.choice({"content": "done"})),
                suite.frame(suite.choice(finish="stop")), suite.frame("[DONE]")]
        return [suite.frame(suite.choice({"tool_calls": [suite.d2_call(i, name="workspace_read",
            arguments=json.dumps({"revision_id": revision.revision_id, "path": "data.txt",
            "offset": 0, "length": 1})) for i in range(2)]})),
            suite.frame(suite.choice(finish="tool_calls")), suite.frame("[DONE]")]
    wire.script = script
    turn = await app.sessions.submit(LOCAL, "local", session.id,
        WorkspaceTurnRequest(request_key="contexts-turn", text="contexts", expected_revision=0,
        completion=Completion()))
    try:
        async with asyncio.timeout(5):
            await wire.step_entered[1].wait()
            # 原始外部 Task；然后只复制可公开构造的数据而不复制私有 owner 记录。
            app.service._external()
            public = replace(scopes[0], invocation=replace(scopes[0].invocation, call_id="forged"))
            token = CURRENT.set(public)
            try:
                async def forged():
                    app.service._external()
                    return await app.service.get(public.invocation.run_id)
                external = asyncio.create_task(forged())
                assert (await external).id == public.invocation.run_id
            finally:
                CURRENT.reset(token)
            after_call.set()
            await call_checked.wait()
        wire.step_release[1].set()
        finished = await app.sessions.wait(LOCAL, "local", session.id, turn.id)
        after_producer.set()
        async with asyncio.timeout(5):
            await producer_checked.wait()
            await asyncio.gather(*children)
        assert seen == ["call-revoked-producer-active", "producer-done-revoked"]
        assert all(
            tool.status == "ok" for tool in (await app.service.result(finished.run_id)).tools)
        assert provider.calls == len(wire.requests) == 2
        data_case[1]["external_contexts"] = seen + ["ordinary-external", "forged-public-scope",
            "different-service-same-id"]
    finally:
        wire.step_release[1].set()
        after_call.set()
        after_producer.set()
        await asyncio.gather(*children)
        await app.close()


async def test_workspace_permissions_and_six_routes(data_case, tmp_path, monkeypatch):
    """真实 Session/Store 权限入口、错误 revision 零效果及六工具的同内核路由。"""
    import uuid

    from sqlalchemy import func, select

    from krim_agent.security import Membership, PrincipalContext
    from krim_agent.sessions.types import SessionCreate
    from krim_agent.storage.models import tool_effects
    from krim_agent.workspace.types import Completion, WorkspaceTurnRequest

    app, wire, provider = await application_case(data_case, tmp_path)
    notes = data_case[1]
    run, binding, access = await manual_turn(app, notes, monkeypatch)
    request = WorkspaceTurnRequest(request_key="denied-access", text="synthetic",
        expected_revision=0, completion=Completion())
    viewer = PrincipalContext("local", 1, (Membership(workspace_id="local", role="viewer"),))
    with pytest.raises(ServiceError, match="permission_denied"):
        await app.sessions.submit(viewer, "local", access.session_id, request)
    stranger = PrincipalContext("stranger", 1, (Membership(workspace_id="other", role="admin"),))
    with pytest.raises(ServiceError, match="not_found"):
        await app.sessions.submit(stranger, "other", access.session_id, request)
    with pytest.raises(ServiceError, match="permission_denied"):
        await app.sessions.create(viewer, "local", SessionCreate(request_key="viewer-create",
            target_key="target-workspace"))
    frame = await app.work.prepare(run.id)
    scope = app.work.scope(frame, binding.assert_current, asyncio.get_running_loop().time()+180)
    registry = binding.loop.executor.registry
    assert {s["name"] for s in registry.schemas()} == {"workspace_read",
        "workspace_search", "workspace_write", "command_run", "artifact_read", "plan_update"}
    checks = []
    for name in ["workspace_read", "artifact_read"]:
        spec = registry.get(name)
        invocation = Invocation(run.id, str(uuid.uuid4()), "bad-revision", 0, name,
            encoded({"revision_id": str(uuid.uuid4()), "path": "absent.txt"}), spec.fingerprint)
        result = await binding.loop.executor.execute(invocation, ToolContext(
            lambda kind, data: app.store.append(run.id, kind, data), app.service._approve,
            90, binding.assert_current, scope))
        assert result.code == "not_found" and result.status == "failed"
        checks.append(result.model_dump(mode="json"))
    async with app.store.engine.begin() as conn:
        assert await conn.scalar(select(func.count()).select_from(tool_effects)) == 0
    for name, args in [("workspace_search", {"revision_id": frame.input_revision.revision_id,
        "query": "synthetic"}),
                       ("plan_update", {"expected_revision": 0, "steps": [{"id": "one",
        "text": "synthetic plan", "status": "pending"}]})]:
        spec = registry.get(name)
        invocation = Invocation(run.id, str(uuid.uuid4()), "routes", 0, name, encoded(args),
            spec.fingerprint)
        result = await binding.loop.executor.execute(invocation, ToolContext(
            lambda kind, data: app.store.append(run.id, kind, data), app.service._approve,
            90, binding.assert_current, scope))
        assert result.status == "ok" and result.executor_started is True
        checks.append(result.model_dump(mode="json"))
    assert (await app.artifacts.head(access)).revision == 0
    assert not app.service._pending and not app.runner.jobs
    assert provider.calls == len(wire.requests) == 0
    notes["permissions_and_routes"] = checks
    await app.store.finish(run.id, "failed", "completion_unverified")


async def test_workspace_trusted_initialization_and_owned_cleanup(tmp_path, monkeypatch):
    """可信 cap 在开库前投影；真实 owned/borrowed 及装配失败 finally。"""
    from test_session_read_sides import Connections

    from krim_agent.composition import open_application
    from krim_agent.execution.linux import SandboxRunner
    from krim_agent.providers.openai_compatible import (
        OpenAICompatibleConfig,
        OpenAICompatibleProvider,
    )
    from krim_agent.runtime.service import RunBinding
    from krim_agent.workspace.artifacts import ArtifactStore
    from krim_agent.workspace.targets import WorkspaceConfiguration, environment
    from krim_agent.workspace.types import WorkspaceProfile

    before, tasks_before = fds(), asyncio.all_tasks()
    suite = http_suite()
    providers, stores, runners, artifacts = [], [], [], []
    original_open = SQLiteStore.open
    original_provider = OpenAICompatibleProvider.__init__
    original_runner = SandboxRunner.__init__
    original_artifacts = ArtifactStore.__init__
    async def opened(cls, path, **kwargs):
        store = await original_open(path, **kwargs)
        stores.append((store, Connections(store)))
        return store
    def initialized_provider(self, config, **kwargs):
        original_provider(self, config, **kwargs)
        providers.append(self)
    def initialized_runner(self, *args, **kwargs):
        original_runner(self, *args, **kwargs)
        runners.append(self)
    def initialized_artifacts(self, *args, **kwargs):
        original_artifacts(self, *args, **kwargs)
        artifacts.append(self)
    monkeypatch.setattr(SQLiteStore, "open", classmethod(opened))
    monkeypatch.setattr(OpenAICompatibleProvider, "__init__", initialized_provider)
    monkeypatch.setattr(SandboxRunner, "__init__", initialized_runner)
    monkeypatch.setattr(ArtifactStore, "__init__", initialized_artifacts)
    profile = os_profile(tmp_path)
    configuration = WorkspaceConfiguration(WorkspaceProfile(max_tools_per_step=2,
        environment=environment(profile)), profile, tmp_path/"objects")
    from dataclasses import asdict
    configuration_path = tmp_path / "trusted-workspace.json"
    payload = {"profile": configuration.profile.model_dump(mode="json"),
        "execution": {key: str(value) if isinstance(value, Path) else value
                      for key, value in asdict(profile).items()},
        "artifact_root": str(configuration.artifact_root)}
    def load_trusted():
        configuration_path.write_text(json.dumps(payload))
        configuration_path.chmod(0o600)
        return WorkspaceConfiguration.load(configuration_path)
    configuration = await asyncio.to_thread(load_trusted)
    assert configuration.profile.max_tools_per_step == 2
    async with suite.HTTPFixture() as wire:
        implicit = OpenAICompatibleConfig(base_url=wire.url, model="fixture")
        assert "max_tool_calls" not in implicit.model_fields_set
        app = await open_application(tmp_path/"owned.sqlite", http_config=implicit,
            workspace_profile=configuration)
        try:
            actual = app.service.bindings["target-workspace"].loop.provider
            assert actual.config.max_tool_calls == 2 and actual in app.owned_providers
            assert app.service.bindings["target-http"].loop.provider is actual
            assert (implicit.max_tool_calls == 1
                    and "max_tool_calls" not in implicit.model_fields_set)
            assert (
                app.evaluations.targets(
                )) and all(item["key"] != "target-workspace" for item in app.evaluations.targets())
        finally:
            await app.close()
        assert actual.closed and app.store.closed and app.artifacts.closed and app.runner.closed
        count = len(stores)
        bad = OpenAICompatibleConfig(base_url=wire.url, model="fixture", max_tool_calls=1)
        with pytest.raises(ServiceError, match="provider_config"):
            await open_application(tmp_path/"conflict.sqlite", http_config=bad,
                workspace_profile=configuration)
        assert len(stores) == count and not (tmp_path/"conflict.sqlite").exists()
        borrowed = OpenAICompatibleProvider(bad)
        seal = borrowed.binding_token()
        try:
            with pytest.raises(ServiceError, match="provider_config"):
                await open_application(tmp_path/"borrowed-conflict.sqlite", provider=borrowed,
                    workspace_profile=configuration)
            assert len(stores) == count and borrowed.binding_token() == seal and not borrowed.closed
        finally:
            await borrowed.close()
        freeze = RunBinding.freeze.__func__
        def fail_workspace(cls, key, loop, settings):
            if key == "target-workspace":
                raise ServiceError("workspace_unavailable")
            return freeze(cls, key, loop, settings)
        monkeypatch.setattr(RunBinding, "freeze", classmethod(fail_workspace))
        with pytest.raises(ServiceError, match="workspace_unavailable"):
            await open_application(tmp_path/"failed-owned.sqlite", http_config=implicit,
                workspace_profile=configuration)
        assert all(p.closed and p.active == 0 and p.calls == 0 for p in providers)
        borrowed = OpenAICompatibleProvider(OpenAICompatibleConfig(base_url=wire.url,
            model="fixture", max_tool_calls=2))
        try:
            with pytest.raises(ServiceError, match="workspace_unavailable"):
                await open_application(tmp_path/"failed-borrowed.sqlite", provider=borrowed,
                    workspace_profile=configuration)
            assert not borrowed.closed and borrowed.active == 0
        finally:
            await borrowed.close()
        assert not wire.requests
    assert all(store.closed and not watch.pending and watch.unsafe == 0 for store, watch in stores)
    assert all(r.closed and not r.jobs and not r._resources and r._owner.closed for r in runners)
    assert all(a.closed for a in artifacts)
    second = await SQLiteStore.open(tmp_path/"failed-owned.sqlite")
    await second.close()
    assert fds() == before
    assert not [t for t in asyncio.all_tasks() if t not in tasks_before
        and t is not asyncio.current_task() and not t.done()]
    evidence = Path(os.environ["KRIM_D2_EVIDENCE"])/"initialization.json"
    evidence.write_text(json.dumps({"owned_and_borrowed": True,
        "provider_calls": sum(p.calls for p in providers),
        "providers_closed": all(p.closed for p in providers), "stores": [{"closed": st.closed,
        "pool": watch.events, "pending": sorted(watch.pending),
        "unsafe_gc": watch.unsafe} for st, watch in stores],
        "runners_closed": all(r.closed for r in runners),
        "artifacts_closed": all(a.closed for a in artifacts), "FD_equal": fds() == before},
        indent=2))


async def test_original_producer_direct_self_wait(data_case, tmp_path):
    """原 producer 内直接 self-close 仍在原 self_wait 路径拒绝。"""
    from dataclasses import replace

    from krim_agent.composition import open_application
    from krim_agent.tools.registry import ToolRegistry

    notes = data_case[1]
    await data_case[0].close()
    holder, entered = {}, []
    original = builtin_registry().get("echo")
    async def actual(parameters):
        service = holder["app"].service
        assert asyncio.current_task() in service.tasks.values()
        entered.append(id(asyncio.current_task()))
        await service.close()
        raise AssertionError("self-close 不得返回")
    spec = replace(original, executor=actual)
    app = await open_application(Path(notes["database"]), registry=ToolRegistry((spec,)))
    holder["app"] = app
    notes["owned"].append(app)
    run = await app.service.start(RunRequest(text="/echo owner", request_key="direct-owner"))
    finished = await app.service.wait(run.id)
    assert finished.code == "self_wait" and entered
    assert app.service._close_task is None and not app.service.closed
    assert (await app.service.by_key("direct-owner")).id == run.id
    assert len((await app.service.result(run.id)).tools) == 1
    notes["direct_producer_self_wait"] = {
        "run_id": run.id, "producer": entered, "code": finished.code
    }
    await app.close()


async def repair01_rows(store):
    """本轮真实库的原事实，不从随机 UUID 猜归属。"""
    from sqlalchemy import select

    from krim_agent.storage import models

    result = {}
    async with store.engine.connect() as conn:
        for name in ["runs", "run_events", "sessions", "turns", "work_sessions",
                     "work_turns", "tool_effects"]:
            rows = (await conn.execute(select(getattr(models, name)))).mappings().all()
            result[name] = [dict(row) for row in rows]
    return result


@pytest.mark.parametrize("case", ["success", "resource-failure", "missing-config"])
async def test_repair01_recovery_is_resource_first(data_case, tmp_path, monkeypatch, case):
    """真实非终态库与自有活叶；只观测原恢复，不复制其实现。"""
    import subprocess
    import sys
    import uuid

    from test_session_read_sides import Connections

    from krim_agent.composition import open_application
    from krim_agent.contracts import fingerprint
    from krim_agent.execution.cgroups import Leaf
    from krim_agent.execution.linux import SandboxRunner
    from krim_agent.storage import work
    from krim_agent.workspace.targets import WorkspaceConfiguration

    notes = data_case[1]
    app, wire, provider = await application_case(data_case, tmp_path)
    run, binding, access = await manual_turn(app, notes, monkeypatch)
    call_id = str(uuid.uuid4())
    # 取得 revision 后再进入原 writer，避免嵌套读锁。
    revision_id = (await app.artifacts.head(access)).revision_id
    async def seed_effect(conn):
        await work.intent(conn, access, run.id, call_id=call_id, operation_key=call_id,
            args_hash=fingerprint({}), tool_hash="a"*64, kind="command",
            input_revision=revision_id)
        await work.dispatch(conn, call_id, access)
        return True
    await app.store.work_write(seed_effect, lambda: asyncio.sleep(0, result=None))
    configuration = WorkspaceConfiguration(app.work.profile, app.runner.profile, app.artifacts.root)
    await app.close()
    from krim_agent.config import Settings
    from krim_agent.storage import sessions as session_sql
    from krim_agent.workspace.types import Completion, WorkspaceTurnRequest
    seed_store = await SQLiteStore.open(Path(notes["database"]))
    notes.setdefault("store_probes", []).append((seed_store, Connections(seed_store)))
    try:
        await session_sql.submit(seed_store, access.session_id, WorkspaceTurnRequest(
            request_key="queued-after-crash", text="synthetic queued", expected_revision=1,
            completion=Completion()), Settings(), access=access)
        before_rows = await repair01_rows(seed_store)
    finally:
        await seed_store.close()
    timeline, records, opened_stores, recovered = [], [], [], []
    leaf = Leaf(configuration.execution.cgroup_root,
                configuration.execution.material/"tickets",
                lambda kind, **v: records.append({"kind": kind, **v}))
    leaf.limits(configuration.execution)
    process = await asyncio.to_thread(subprocess.Popen, [sys.executable, "-I", "-B", "-c",
        "import os;os.write(1,b'OWNED-READY\\n');os.read(0,1)"],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        env={"PATH": "/usr/local/bin:/usr/bin:/bin",
             "LD_LIBRARY_PATH": str(Path(sys.base_prefix)/"lib")})
    pidfd = os.pidfd_open(process.pid)
    ready = await asyncio.to_thread(process.stdout.readline)
    assert ready == b"OWNED-READY\n"
    leaf.join(process.pid)
    original_ticket = leaf.ticket.read_bytes()
    if case == "resource-failure":
        broken = json.loads(original_ticket)
        broken["inode"] += 1
        leaf.ticket.write_text(json.dumps(broken))
    original_open, original_recover = SQLiteStore.open, SQLiteStore.recover
    original_runner = SandboxRunner.__init__
    async def opened(cls, *args, **kwargs):
        store = await original_open(*args, **kwargs)
        opened_stores.append((store, Connections(store)))
        return store
    async def recovering(store):
        timeline.append({"event": "store.recover", "leaf_exists": leaf.path.exists(),
                         "child_alive": process.poll() is None})
        return await original_recover(store)
    def runner_init(runner, *args, **kwargs):
        timeline.append({"event": "runner.begin"})
        try:
            original_runner(runner, *args, **kwargs)
        except BaseException:
            timeline.append({"event": "runner.failed",
                "owner_closed": not hasattr(runner, "_owner") or runner._owner.closed})
            raise
        recovered.append(runner)
        timeline.append({"event": "runner.recovered", "leaf_exists": leaf.path.exists(),
                         "facts": runner.recovered})
    monkeypatch.setattr(SQLiteStore, "open", classmethod(opened))
    monkeypatch.setattr(SQLiteStore, "recover", recovering)
    monkeypatch.setattr(SandboxRunner, "__init__", runner_init)
    reopened = None
    error = None
    try:
        try:
            reopened = await open_application(Path(notes["database"]),
                **({"provider": provider, "workspace_profile": configuration}
                   if case != "missing-config" else {}))
        except ServiceError as failure:
            error = failure.code
        if reopened is not None:
            notes["owned"].append(reopened)
            if reopened.runner is not None:
                notes.setdefault("runners", []).append(reopened.runner)
            await reopened.close()
        readonly = await SQLiteStore.open(Path(notes["database"]), readonly=True)
        try:
            after_rows = await repair01_rows(readonly)
            snapshot = await readonly.snapshot(run.id)
            notes["recovery_snapshot"] = {"run_id": run.id, "status": snapshot.run.status,
                "turn_id": access.session_id, "effect_call_id": call_id}
        finally:
            await readonly.close()
        notes["recovery"] = {"case": case, "timeline": list(timeline), "error": error,
            "before": before_rows, "after": after_rows, "leaf_identity": leaf.identity,
            "process_pid": process.pid, "provider_calls": provider.calls,
            "owned_leaf_events": records}
        if case == "success":
            assert error is None
            assert [r["event"] for r in timeline][:3] == [
                "runner.begin", "runner.recovered", "store.recover"]
            assert not timeline[2]["leaf_exists"]
            assert snapshot.run.status == "interrupted"
            assert all(r["dispatch_paused"] is True for r in after_rows["sessions"])
        else:
            assert error == ("sandbox_cleanup_required" if case == "resource-failure"
                             else "workspace_unavailable")
            assert not any(r["event"] == "store.recover" for r in timeline)
            assert after_rows == before_rows
            assert snapshot.run.status == "running"
        assert provider.calls == len(wire.requests) == 0
    finally:
        # 故障原件先保留，再只解除自己票据的精确可恢复障碍；不杀未知 PID。
        notes["recovery_ticket_failure_bytes"] = (
            leaf.ticket.read_text() if leaf.ticket.exists() else None
        )
        if leaf.ticket.exists():
            leaf.ticket.write_bytes(original_ticket)
        if leaf.path.exists() or leaf.ticket.exists():
            cleanup_runner = SandboxRunner(configuration.execution)
            try:
                assert not leaf.path.exists()
                notes["recovery_cleanup"] = cleanup_runner.recovered
            finally:
                await cleanup_runner.close()
        stdout, stderr = await asyncio.to_thread(process.communicate, timeout=5)
        assert process.returncode == -9 and process.poll() is not None
        os.close(pidfd)
        notes["recovery_process"] = {"pid": process.pid, "waited_pid": process.pid,
            "returncode": process.returncode, "pidfd_opened": pidfd, "pidfd_closed": True,
            "stdout": stdout.decode(), "stderr": stderr.decode(),
            "leaf_removed": not leaf.path.exists(),
            "tickets_empty": not list(leaf.tickets.iterdir())}
        assert not leaf.path.exists() and not list(leaf.tickets.iterdir())
        notes.setdefault("store_probes", []).extend(opened_stores)
        assert all(st.closed for st, watch in opened_stores)
        # readonly 仍可读取原事实，且 writer lease 已释放；不调用 recover。
        second = await original_open(Path(notes["database"]))
        await second.close()


@pytest.mark.parametrize("phase", ["approval", "file-read"])
async def test_repair01_runner_entry_is_sealed(data_case, tmp_path, monkeypatch, phase):
    import uuid

    from krim_agent.workspace.operations import WorkspaceOperations

    holder, entries = {}, []
    original_files = WorkspaceOperations._files
    async def reject(request):
        entries.append("replacement-run")
        raise ServiceError("target_binding_changed")
    async def files(operations, *args, **kwargs):
        value = await original_files(operations, *args, **kwargs)
        if phase == "file-read":
            holder["app"].runner.run = reject
        return value
    monkeypatch.setattr(WorkspaceOperations, "_files", files)
    app, wire, provider = await application_case(data_case, tmp_path)
    holder["app"] = app
    notes = data_case[1]
    run, binding, access = await manual_turn(app, notes, monkeypatch)
    original_append = app.store.append
    async def append(run_id, kind, value, **kwargs):
        event = await original_append(run_id, kind, value, **kwargs)
        if kind == "approval_request":
            from krim_agent.contracts import ApprovalReply
            await app.service.reply_approval(run_id, value["request_id"], ApprovalReply(
                request_key="entry-seal", call_id=value["call_id"],
                arguments_hash=value["arguments_hash"], tool_hash=value["tool_hash"], allow=True))
            if phase == "approval":
                app.runner.run = reject
        return event
    monkeypatch.setattr(app.store, "append", append)
    spec = binding.loop.executor.registry.get("command_run")
    invocation = Invocation(run.id, str(uuid.uuid4()), "entry-seal", 0, "command_run",
        encoded({"expected_revision": 0, "program": "python",
                 "argv": ["-c", "print('synthetic')"]}), spec.fingerprint)
    scope = app.work.scope(await app.work.prepare(run.id), binding.assert_current,
                           asyncio.get_running_loop().time()+180)
    result = await binding.loop.executor.execute(invocation, ToolContext(
        lambda kind, data: app.store.append(run.id, kind, data), app.service._approve,
        90, binding.assert_current, scope))
    notes["runner_entry"] = {"phase": phase, "replacement_entries": entries,
                             "result": result.model_dump(mode="json"), "run_id": run.id}
    assert result.code == "target_binding_changed" and result.status == "failed"
    assert entries == []
    assert not app.runner.jobs and not app.runner._resources
    assert not list(app.runner.profile.material.glob("job-*"))
    assert (await app.artifacts.head(access)).revision == 0
    assert provider.calls == len(wire.requests) == 0
    await app.store.finish(run.id, "failed", "target_binding_changed")


@pytest.mark.parametrize("case", ["drift-insert", "normal", "lost-return", "cancel-before",
                                   "repeat-cancel-before", "cancel-after", "close-after",
                                   "rejected-file"])
async def test_repair01_command_publication_commit_fence(data_case, tmp_path, monkeypatch, case):
    import uuid

    from sqlalchemy import select
    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncTransaction

    from krim_agent.storage.models import artifact_revisions, tool_effects

    app, wire, provider = await application_case(data_case, tmp_path)
    notes = data_case[1]
    run, binding, access = await manual_turn(app, notes, monkeypatch)
    approve_fixture(app, monkeypatch)
    spec = binding.loop.executor.registry.get("command_run")
    name = ".env.production" if case == "rejected-file" else "command.txt"
    code = ("from pathlib import Path;print('SYNTHETIC-REPAIR',flush=True);Path("
            + repr(name) + ").write_text('synthetic')")
    invocation = Invocation(run.id, str(uuid.uuid4()), "command-cas", 0, "command_run",
        encoded({"expected_revision": 0, "program": "python", "argv": ["-c", code]}),
        spec.fingerprint)
    scope = app.work.scope(await app.work.prepare(run.id), binding.assert_current,
                           asyncio.get_running_loop().time()+180)
    entered, release = asyncio.Event(), asyncio.Event()
    window, issued, closers = [], [], []
    execute, exit_transaction = AsyncConnection.execute, AsyncTransaction.__aexit__
    async def query(conn, statement, *args, **kwargs):
        value = await execute(conn, statement, *args, **kwargs)
        if (
            getattr(statement, "is_insert", False)
            and getattr(getattr(statement, "table", None), "name", None) == "artifact_revisions"
        ):
            window.append("real-INSERT-returned-before-commit")
            issued.append(conn)
            if case == "drift-insert":
                async def replacement(invocation):
                    return "allow"
                binding.loop.executor.policy = replacement
            if case in {"cancel-before", "repeat-cancel-before"}:
                entered.set()
                await release.wait()
        return value
    async def committed(transaction, exc_type, exc_value, traceback):
        value = await exit_transaction(transaction, exc_type, exc_value, traceback)
        if exc_type is None and any(transaction.connection is conn for conn in issued):
            window.append("real-COMMIT-returned")
            if case == "lost-return":
                issued.clear()
                raise OSError("synthetic committed acknowledgement loss")
            if case in {"cancel-after", "close-after"}:
                entered.set()
                await release.wait()
        return value
    monkeypatch.setattr(AsyncConnection, "execute", query)
    monkeypatch.setattr(AsyncTransaction, "__aexit__", committed)
    task = asyncio.create_task(binding.loop.executor.execute(invocation, ToolContext(
        lambda kind, data: app.store.append(run.id, kind, data), app.service._approve,
        90, binding.assert_current, scope)))
    try:
        if case in {"cancel-before", "repeat-cancel-before", "cancel-after", "close-after"}:
            async with asyncio.timeout(10):
                await entered.wait()
            if case == "close-after":
                closers.append(asyncio.create_task(app.artifacts.close()))
                await asyncio.sleep(0)
            else:
                task.cancel()
                if case == "repeat-cancel-before":
                    task.cancel()
            release.set()
        result = (await asyncio.gather(task, return_exceptions=True))[0]
        await asyncio.gather(*closers)
        async with app.store.engine.connect() as conn:
            revisions = (await conn.execute(select(artifact_revisions))).mappings().all()
            effect = (await conn.execute(
                select(tool_effects).where(tool_effects.c.call_id == invocation.call_id)
            )).mappings().one()
        notes["command_publication"] = {"case": case, "window": window, "run_id": run.id,
            "call_id": invocation.call_id, "effect": dict(effect),
            "revisions": [dict(r) for r in revisions],
            "result": (result.model_dump(mode="json") if not isinstance(result, BaseException)
                       else type(result).__name__)}
        if case in {"normal", "lost-return", "cancel-after", "close-after"}:
            assert "real-COMMIT-returned" in window
        receipt = effect["receipt"]
        assert receipt["executor_started"] is True and receipt["exit_code"] == 0
        assert receipt["output_complete"] is True and receipt["cleanup"] == "clean"
        if case == "drift-insert":
            assert result.code == "target_binding_changed"
            assert "real-INSERT-returned-before-commit" in window
            assert len(revisions) == 1 and "artifact_revision_id" not in receipt
        elif case == "rejected-file":
            assert result.code == "artifact_invalid"
            assert len(revisions) == 1 and "artifact_revision_id" not in receipt
        else:
            # Store._write/settled owns commit: cancellation after INSERT joins the same commit.
            assert len(revisions) == 2 and receipt["artifact_revision_id"] == revisions[-1]["id"]
            if case in {"normal", "lost-return"}:
                assert result.status == "ok"
        readonly = await SQLiteStore.open(Path(notes["database"]), readonly=True)
        try:
            snapshot = await readonly.snapshot(run.id)
            assert any(e.kind == "workspace_receipt" and e.data["receipt"]["exit_code"] == 0
                       for e in snapshot.events)
        finally:
            await readonly.close()
        if case not in {"drift-insert", "rejected-file", "close-after"}:
            original = await app.artifacts.by_key(access, invocation.call_id, kind="effect")
            # 原 DTO/key 只核实已提交同 ID/hash，不能再次进入 Runner。
            replay = await app.artifacts.by_key(access, invocation.call_id, kind="effect")
            assert replay.revision_id == original.revision_id
            assert replay.manifest_hash == original.manifest_hash
        logs = []
        for job in app.runner.profile.evidence.iterdir():
            path = job/"resources.jsonl"
            logs.extend(json.loads(line) for line in path.read_text().splitlines())
        assert sum(r["kind"] == "gate_opened" for r in logs) == 1
        assert sum(r["kind"] == "process_created" for r in logs) == 1
        assert sum(r["kind"] == "process_waited" for r in logs) == 1
        assert sum(r["kind"] == "pidfd_closed" for r in logs) == 1
        assert any(r["kind"] == "populated_zero" for r in logs)
        assert any(r["kind"] == "leaf_removed" for r in logs)
        assert not app.runner.jobs and not app.runner._resources
        assert provider.calls == len(wire.requests) == 0
    finally:
        release.set()
        await asyncio.gather(task, *closers, return_exceptions=True)
        await app.store.finish(run.id, "failed", "completion_unverified")
