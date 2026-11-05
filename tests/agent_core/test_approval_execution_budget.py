"""端到端人工审批与执行预算分离；Provider只连接本机合成HTTP服务。"""

import asyncio
import hashlib
import math
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from krim_agent.composition import open_application
from krim_agent.config import Settings
from krim_agent.contracts import ApprovalReply, ServiceError, encoded, fingerprint
from krim_agent.providers.openai_compatible import (
    OpenAICompatibleConfig,
    OpenAICompatibleProvider,
)
from krim_agent.runtime.contracts import ExecutionBudget
from krim_agent.security import LOCAL
from krim_agent.sessions.types import SessionCreate, TurnRequest
from krim_agent.tools.builtins import builtin_registry
from krim_agent.tools.core import Invocation
from krim_agent.tools.executor import ToolContext, ToolExecutor
from krim_agent.tools.registry import ToolRegistry
from krim_agent.workspace.operations import WorkspaceOperations
from krim_agent.workspace.types import ExecutionAccess


@asynccontextmanager
async def approval_application(wire, tmp_path, *, settings, tool_delay=0.0):
    entered = asyncio.Event()
    values = []
    for spec, _policy in wire.registry()._frozen:
        if spec.name == "echo" and tool_delay:
            original = spec.executor

            async def delayed(args, original=original):
                entered.set()
                await asyncio.sleep(tool_delay)
                return await original(args)

            spec = replace(
                spec,
                executor=delayed,
                implementation_digest=hashlib.sha256(
                    f"approval-budget-slow/{tool_delay}".encode()
                ).hexdigest(),
            )
        values.append(spec)

    async def ask(_invocation):
        return "ask"

    provider = OpenAICompatibleProvider(
        OpenAICompatibleConfig(base_url=f"http://127.0.0.1:{wire.port}/v1", model="fixture")
    )
    app = None
    try:
        app = await open_application(
            Path(tmp_path) / "approval-budget.sqlite",
            settings=settings,
            provider=provider,
            registry=ToolRegistry(tuple(values)),
            policy=ask,
        )
        yield app, entered
    finally:
        if app is not None:
            await app.close()
        await provider.close()


async def start_echo_session(app, text):
    session = await app.sessions.create(
        LOCAL, "local", SessionCreate(request_key="approval-budget-session")
    )
    turn = await app.sessions.submit(
        LOCAL,
        "local",
        session.id,
        TurnRequest(text=text, request_key="approval-budget-turn", expected_revision=0),
    )
    async with asyncio.timeout(5):
        while True:
            current = await app.sessions.turn(LOCAL, "local", session.id, turn.id)
            if current.run_id:
                requests = await app.service.approvals(current.run_id)
                if requests:
                    return session, turn, current.run_id, requests[0]
                task = app.service.tasks.get(current.run_id)
                if task is not None and task.done():
                    # 停靠（ADR-0015）：任务结束与 awaiting 落库之间存在竞态，复查一次；
                    # 短审批窗口下请求可能已过期，直接认 awaiting 状态下的请求事件。
                    result = await app.service.get(current.run_id)
                    events = await app.service.events(current.run_id)
                    if result.status == "awaiting":
                        request_event = next(
                            (e for e in events if e.kind == "approval_request"), None
                        )
                        if request_event is not None:
                            return session, turn, current.run_id, request_event.data
                    raise AssertionError(
                        f"run ended before approval: {result.status}/{result.code}; "
                        f"events={[event.kind for event in events]}"
                    )
            await asyncio.sleep(0.005)


def approval_reply(request, *, allow):
    return ApprovalReply(
        request_key="approval-budget-turn",
        call_id=request["call_id"],
        tool_hash=request["tool_hash"],
        arguments_hash=request["arguments_hash"],
        allow=allow,
    )


def test_approval_window_defaults_and_explicit_short_values():
    from krim_agent.workspace.types import WorkLimits

    assert Settings().approval_timeout == 300
    assert Settings(approval_timeout=17).approval_timeout == 17
    assert WorkLimits().approval_timeout_seconds == 300
    assert WorkLimits(approval_timeout_seconds=11).approval_timeout_seconds == 11


@pytest.mark.asyncio
async def test_overlapping_approval_waits_pause_one_union_interval():
    loop = asyncio.get_running_loop()
    budget = ExecutionBudget(loop.time() + 0.06)
    timeout = asyncio.timeout_at(budget.deadline)
    budget.bind(timeout)
    first_entered, second_entered = asyncio.Event(), asyncio.Event()
    first_release, second_release = asyncio.Event(), asyncio.Event()

    async def wait_for_approval(entered, release):
        with budget.pause():
            entered.set()
            await release.wait()

    try:
        async with timeout:
            first = asyncio.create_task(wait_for_approval(first_entered, first_release))
            await first_entered.wait()
            await asyncio.sleep(0.01)
            second = asyncio.create_task(wait_for_approval(second_entered, second_release))
            await second_entered.wait()
            assert budget.paused and timeout.when() is None
            await asyncio.sleep(0.03)
            first_release.set()
            await first
            assert budget.paused
            await asyncio.sleep(0.03)
            second_release.set()
            await second
            assert not budget.paused
            assert 0.04 < budget.remaining <= 0.06
            await asyncio.sleep(0.02)
    finally:
        budget.finish()
    assert not timeout.expired()


@pytest.mark.asyncio
async def test_workspace_scope_moves_while_another_tool_keeps_its_timer():
    loop = asyncio.get_running_loop()
    deadline = loop.time() + 0.5
    budget = ExecutionBudget(deadline)
    run_timeout = asyncio.timeout_at(deadline)
    budget.bind(run_timeout)
    profile_data = {"fixture": "approval-budget"}
    access = ExecutionAccess(LOCAL, "local", "scope-session")
    frame = SimpleNamespace(
        actor_id="local", grant=access.grant(), session_id="scope-session"
    )
    operations = object.__new__(WorkspaceOperations)
    operations.profile = SimpleNamespace(model_dump=lambda mode: profile_data)
    operations._profile_hash = fingerprint(profile_data)
    operations.closed = False
    operations.principal = lambda _frame: LOCAL

    async def observe(_invocation):
        return None

    operations.observe = observe
    scope = operations.scope(frame, lambda: None, deadline, budget)
    registry = builtin_registry()
    slow_spec = registry.get("add")
    original = slow_spec.executor
    slow_entered = asyncio.Event()

    async def slow_echo(args):
        slow_entered.set()
        await asyncio.sleep(0.15)
        return await original(args)

    values = [
        replace(spec, executor=slow_echo,
                implementation_digest=hashlib.sha256(b"scope-slow-add/1").hexdigest())
        if spec.name == "add" else spec
        for spec, _policy in registry._frozen
    ]
    registry = ToolRegistry(tuple(values))

    async def policy(invocation):
        return "ask" if invocation.name == "echo" else "allow"

    async def emit(_kind, _data):
        return None

    approval_entered, approval_release = asyncio.Event(), asyncio.Event()

    async def approve(_invocation, _window):
        with budget.pause():
            approval_entered.set()
            await approval_release.wait()
        return True

    approval_spec = registry.get("echo")
    add_spec = registry.get("add")
    executor = ToolExecutor(registry, policy)
    run_id = str(uuid.uuid4())
    approval_invocation = Invocation(run_id, str(uuid.uuid4()), "approval-span", 0,
        approval_spec.name, encoded({"text": "approved"}), approval_spec.fingerprint)
    slow_invocation = Invocation(run_id, str(uuid.uuid4()), "slow-span", 0,
        add_spec.name, encoded({"left": 2.0, "right": 3.0}), add_spec.fingerprint)
    approval_task = slow_task = None
    try:
        async with run_timeout:
            approval_task = asyncio.create_task(executor.execute(
                approval_invocation,
                ToolContext(emit, approve, 0.2, scope.check, scope, 0.3),
            ))
            await asyncio.wait_for(approval_entered.wait(), 1)
            initial_deadline = scope.deadline
            slow_task = asyncio.create_task(executor.execute(
                slow_invocation,
                ToolContext(emit, approve, 0.04, scope.check, scope, 0.3),
            ))
            slow_result = await asyncio.wait_for(slow_task, 1)
            assert slow_entered.is_set()
            assert slow_result.code == "tool_timeout"
            await asyncio.sleep(0.5)
            assert scope.deadline > initial_deadline
            scope.check()
            assert run_timeout.when() is None
            approval_release.set()
            approved_result = await asyncio.wait_for(approval_task, 1)
            assert approved_result.status == "ok"
    finally:
        approval_release.set()
        for task in (approval_task, slow_task):
            if task is not None and not task.done():
                task.cancel()
        await asyncio.gather(
            *(task for task in (approval_task, slow_task) if task is not None),
            return_exceptions=True,
        )
    deadline_after_exit = scope.deadline
    budget.finish()
    assert math.isfinite(deadline_after_exit)
    assert scope.deadline == deadline_after_exit
    scope.check()
    assert not run_timeout.expired()


@pytest.mark.asyncio
async def test_workspace_terminal_handoff_keeps_finite_deadline_after_timer_exit():
    from krim_agent.tools.pipeline import _producing
    from krim_agent.workspace.verification import (
        VerificationIdentity,
        VerificationResult,
        _TerminalAttempt,
        private_handoff_for_same_owner_and_run,
        stage_result,
        terminal_commit_fence,
    )

    loop = asyncio.get_running_loop()
    deadline = loop.time() + 10
    budget = ExecutionBudget(deadline)
    timeout = asyncio.timeout_at(deadline)
    budget.bind(timeout)
    profile_data = {"fixture": "terminal-handoff"}
    access = ExecutionAccess(LOCAL, "local", "handoff-session")
    frame = SimpleNamespace(
        actor_id="local", grant=access.grant(), session_id="handoff-session"
    )
    operations = object.__new__(WorkspaceOperations)
    operations.profile = SimpleNamespace(model_dump=lambda mode: profile_data)
    operations._profile_hash = fingerprint(profile_data)
    operations.closed = False
    operations.principal = lambda _frame: LOCAL
    scope = operations.scope(frame, lambda: None, deadline, budget)
    run_id = str(uuid.uuid4())
    result = VerificationResult(
        run_id=run_id,
        turn_id="turn",
        session_id="handoff-session",
        workspace_id="local",
        actor_id="local",
        grant_hash="grant",
        request_hash="request",
        completion_hash="completion",
        conditions_hash="conditions",
        effective_fingerprint="effective",
        admission_input_revision_id="input-revision",
        command_call_id=None,
        command_args_hash=None,
        command_input_revision_id=None,
        output_revision_id=None,
        output_manifest_hash=None,
        receipt_hash=None,
        observation_hash=None,
        verifier_identity=VerificationIdentity("schema", "rules", "impl", "command", "limits"),
        state="passed",
        results=(),
        reason=None,
    )
    store = object()
    issuer = SimpleNamespace(
        store=store,
        _issued={run_id: result},
        assert_current=lambda: None,
    )
    producer = asyncio.current_task()
    assert producer is not None

    with _producing(object(), run_id, producer):
        async with timeout:
            stage_result(
                result,
                issuer=issuer,
                store=store,
                producer=producer,
                access=access,
                check=scope.check,
                run_deadline=scope.deadline,
            )
        budget.finish()
        handoff = private_handoff_for_same_owner_and_run(run_id)
        assert handoff is not None
        assert math.isfinite(handoff.run_deadline) and handoff.run_deadline == deadline
        terminal_commit_fence(
            _TerminalAttempt(store, None, run_id, handoff),
            selected_status="completed",
            selected_code=None,
        )
        assert scope.deadline == deadline
    assert not timeout.expired()


@pytest.mark.parametrize(
    ("run_timeout", "tool_timeout", "tool_delay", "wait_seconds", "expected_status", "code"),
    [
        (3.0, 1.0, 0.0, 4.2, "completed", None),
        (5.0, 1.0, 1.5, 1.2, "failed", "tool_timeout"),
    ],
    ids=["park-outlasts-run-budget", "tool-still-times-out-after-resume"],
)
async def test_real_session_park_does_not_consume_execution_budget(
    wire,
    tmp_path,
    run_timeout,
    tool_timeout,
    tool_delay,
    wait_seconds,
    expected_status,
    code,
):
    # ADR-0015：停靠不占并发名额、不挂协程、等待不消耗执行预算——
    # 等待超过 run_timeout 后批准仍能完成；恢复后的执行仍受工具超时约束。
    settings = Settings(run_timeout=run_timeout, tool_timeout=tool_timeout)
    async with approval_application(
        wire, tmp_path, settings=settings, tool_delay=tool_delay
    ) as (app, entered):
        assert app.settings.approval_timeout == 300
        _session, _turn, run_id, request = await start_echo_session(app, "/echo wait for approval")
        assert request["expires_at"] > time.time()
        await asyncio.sleep(wait_seconds)
        assert (await app.service.get(run_id)).status == "awaiting"  # 停靠持久状态
        assert run_id not in app.service._execution_budgets  # 等待期无活预算
        reply = approval_reply(request, allow=True)
        await app.service.reply_approval(run_id, request["request_id"], reply)
        result = await app.service.wait(run_id)
        assert result.status == expected_status, result.code
        assert result.code == code
        assert entered.is_set() == (tool_delay > 0)
        if expected_status == "completed":
            assert len(wire.executions) == 1
        else:
            assert not wire.executions
        assert app.service._execution_budgets == {}


@pytest.mark.asyncio
async def test_real_session_approval_replay_and_conflict_do_not_repeat_tool(wire, tmp_path):
    settings = Settings(run_timeout=3, tool_timeout=1)
    async with approval_application(wire, tmp_path, settings=settings) as (app, _entered):
        _session, _turn, run_id, request = await start_echo_session(app, "/echo replay approval")
        approved = approval_reply(request, allow=True)
        first = await app.service.reply_approval(run_id, request["request_id"], approved)
        replay = await app.service.reply_approval(run_id, request["request_id"], approved)
        assert first["replayed"] is False and replay["replayed"] is True
        with pytest.raises(ServiceError, match="approval_conflict"):
            await app.service.reply_approval(
                run_id, request["request_id"], approval_reply(request, allow=False)
            )
        result = await app.service.wait(run_id)
        assert result.status == "completed"
        assert len(wire.executions) == 1
        assert app.service._execution_budgets == {}


@pytest.mark.parametrize("action", ["reject", "expire", "late", "cancel"])
async def test_real_session_unapproved_call_has_no_execution_or_timer_leak(
    wire, tmp_path, action, monkeypatch
):
    settings = Settings(
        run_timeout=3,
        tool_timeout=1,
        approval_timeout=0.04 if action in {"expire", "late"} else 300,
    )
    async with approval_application(wire, tmp_path, settings=settings) as (app, entered):
        finish_started = finish_release = None
        if action == "late":
            finish_started, finish_release = asyncio.Event(), asyncio.Event()
            original_finish = app.store.finish

            async def hold_expired_finish(run_id, status, code=None):
                if status == "failed" and code == "approval_expired":
                    finish_started.set()
                    await finish_release.wait()
                return await original_finish(run_id, status, code)

            monkeypatch.setattr(app.store, "finish", hold_expired_finish)
        _session, _turn, run_id, request = await start_echo_session(app, "/echo do not execute")
        if action == "reject":
            await app.service.reply_approval(run_id, request["request_id"],
                                             approval_reply(request, allow=False))
            result = await app.service.wait(run_id)
            # ADR-0015：拒绝回传模型收尾——运行完成，工具从未进入执行器
            assert result.status == "completed" and result.code is None
            events = await app.service.events(run_id)
            denial = [e for e in events if e.kind == "tool_end"]
            assert denial and denial[-1].data["code"] == "approval_denied"
            assert denial[-1].data["executor_started"] is False
        elif action == "expire":
            # 过期惰性定稿：无人等待超时；答复触发整批评废
            await asyncio.sleep(0.07)
            assert (await app.service.get(run_id)).status == "awaiting"
            with pytest.raises(ServiceError, match="approval_expired"):
                await app.service.reply_approval(
                    run_id, request["request_id"], approval_reply(request, allow=True)
                )
            result = await app.service.wait(run_id)
            assert result.status == "failed" and result.code == "approval_expired"
        elif action == "late":
            assert finish_started is not None and finish_release is not None
            await asyncio.sleep(0.07)
            caller = asyncio.ensure_future(app.service.reply_approval(
                run_id, request["request_id"], approval_reply(request, allow=True)))
            async with asyncio.timeout(5):
                await finish_started.wait()
            assert app.service._execution_budgets == {}
            finish_release.set()
            with pytest.raises(ServiceError, match="approval_expired"):
                await caller
            result = await app.service.wait(run_id)
            assert result.status == "failed" and result.code == "approval_expired"
        else:
            result = await app.service.cancel(run_id)
            assert result.status == "aborted" and result.code == "cancelled"
        assert not entered.is_set()
        assert not wire.executions
        assert app.service._execution_budgets == {}
