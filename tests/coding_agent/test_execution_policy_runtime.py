"""会话执行模式热切换的运行时回归：唤醒、执行前重验、放行事件与启动闸门。

覆盖计划5.6/5.9与验收9.3的运行时部分：
- 停靠审批因manual→bypass被唤醒：独立execution_policy_allow事件，无伪approval_reply；
- 用户拒绝的调用不因bypass复活；auto不明确时维持待答；过期确认窗口不放行；
- bypass→manual后同批未启动调用在队列→执行边界按最新模式重判并停靠；
- ask准入后、派发前切bypass：跳过确认直接执行并记录模式放行事件；
- 通知订阅驱动唤醒（回调create_task，重读持久数据）；通知失败不致卡死。
真实SQLite + RunService + DevelopmentSDKLoop，仅模型输入脚本化。
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from krim_agent.accounts.service import IdentityService
from krim_agent.config import Settings
from krim_agent.context.types import ContextPolicy
from krim_agent.contracts import ApprovalReply, ServiceError, StrictModel, parse_json
from krim_agent.providers.types import ModelText, ModelToolCall
from krim_agent.runtime.sdk_loop import DevelopmentRunScope, DevelopmentSDKLoop
from krim_agent.runtime.service import RunBinding, RunService
from krim_agent.sandbox.bindings import bind_project
from krim_agent.sandbox.configuration import SandboxConfiguration
from krim_agent.sandbox.models import BindProjectBody
from krim_agent.sandbox.service import SandboxService
from krim_agent.sessions.execution_policy import (
    ExecutionPolicyChange,
    execution_policy_notifications,
)
from krim_agent.sessions.types import SessionCreate, TurnRequest
from krim_agent.storage import sessions as session_sql
from krim_agent.storage.execution_policy import (
    allow_rule_satisfied,
    clear_allow_rules,
    get_mode,
    get_policy,
    set_mode,
)
from krim_agent.tools.core import ToolSource, ToolSpec
from krim_agent.tools.executor import ToolExecutor
from krim_agent.tools.registry import ToolRegistry
from tests.coding_agent.test_development_storage import prepare


class ScriptedModel:
    """RunService路径的模型替身：按脚本产出provider层工具调用与文本。"""

    def __init__(self, script: list[list[ModelToolCall | ModelText]]) -> None:
        self.script = script
        self.index = 0
        self.histories: list[Any] = []

    async def stream(self, messages: Any, tools: Any):
        self.histories.append(messages)
        index = self.index
        self.index += 1
        assert index < len(self.script), "unexpected extra model call"
        for item in self.script[index]:
            yield item

    async def close(self) -> None:
        return None


def write_call(value: int) -> ModelToolCall:
    return ModelToolCall(id=f"wire-{value}", name="write", arguments={"value": value})


DONE = ModelText(text="done")


class WriteArgs(StrictModel):
    value: int


SCHEMA = {
    "type": "object",
    "properties": {"value": {"type": "integer"}},
    "required": ["value"],
    "additionalProperties": False,
}


async def application(
    tmp_path: Path,
    script: list[list[ModelToolCall | ModelText]],
    *,
    approval_timeout: float = 300,
    request_key: str = "policy-turn",
) -> SimpleNamespace:
    """真实存储与SDK停靠状态机；只有会话模式表示与模型输入是测试替身。"""
    store, repositories, record, seeded = await prepare(tmp_path / "policy.sqlite")
    identities = await IdentityService.open(store)
    sandbox = SandboxService(store, identities, SandboxConfiguration())
    actor = identities.resolve(seeded.ownership.owner_id)
    initial = await store.snapshot(seeded.ownership.run_id)
    workspace = initial.session_context.workspace_id
    mode = {"value": "manual"}
    counts = {"write": 0}
    hooks: dict[str, Any] = {"on_execute": None, "on_decide": None}

    async def unavailable(_args: Any) -> Any:
        raise AssertionError("external executor must be used")

    async def execute_write(_invocation: Any, args: Any) -> Any:
        counts["write"] += 1
        action = hooks["on_execute"]
        if action is not None:
            await action()
        return {"value": args.value}

    async def policy(invocation: Any) -> str:
        if invocation.name == "write":
            action = hooks["on_decide"]
            if action is not None:
                await action("write")
            return "allow" if mode["value"] == "bypass" else "ask"
        return "allow"

    settings = Settings(
        run_timeout=30,
        tool_timeout=10,
        max_request_bytes=65536,
        history_compaction=False,
        development_history_compaction=False,
        approval_timeout=approval_timeout,
    )
    executor = ToolExecutor(
        ToolRegistry((ToolSpec(
            "write", WriteArgs, unavailable, SCHEMA, "1", "b" * 64,
            risk="write", external_executor=execute_write,
            source=ToolSource("mcp", "policy-fixture", "write", "c" * 64),
        ),)),
        policy,
    )
    provider = ScriptedModel(script)

    async def scope(run_id: str, check: Any, budget: Any) -> DevelopmentRunScope:
        snapshot = await store.snapshot(run_id)
        owner = seeded.ownership.model_copy(update={
            "run_id": run_id, "session_id": snapshot.session_context.session_id,
        })
        check()
        return DevelopmentRunScope(record.environment, owner, None)

    async def close_run(_run_id: str) -> None:
        return None

    loop = DevelopmentSDKLoop(
        store, provider, executor, settings,
        resolve_scope=scope, repositories=repositories,
        cleanup_run=close_run, descriptor={"version": "policy-hotswitch/1"},
    )
    binding = RunBinding.freeze("target-development", loop, settings)
    target = parse_json(binding.descriptor_json)
    # prepare()的会话已有排队turn，不能改绑；热切换用全新绑定会话承载。
    session = await session_sql.create_session(
        store,
        workspace,
        actor.principal_id,
        SessionCreate(request_key="policy-session", target_key="target-development"),
        target,
        ContextPolicy(),
        settings,
    )
    session_id = session.id
    await bind_project(
        sandbox,
        actor,
        session_id,
        BindProjectBody(
            expected_revision=0,
            expected_session_revision=0,
            project_id=record.environment.project_id,
        ),
        request_key="bind-policy-project",
    )

    async def recheck(session: str, _invocation: Any, _spec: Any) -> str:
        """组合层注入点的最小替身：每次从权威存储重读，判定口径与policy一致。"""
        current = await store.work_read(lambda conn: get_mode(conn, session))
        mode["value"] = current
        return "allow" if current == "bypass" else "ask"

    service = RunService(
        store, loop, settings, target, {"target-development": binding},
        execution_policy_recheck=recheck,
    )
    submitted = await session_sql.submit(
        store,
        session_id,
        TurnRequest(text="run under session policy", request_key=request_key,
                    expected_revision=1),
        settings,
    )
    run = await service.start_turn(submitted.id)
    return SimpleNamespace(
        store=store,
        repositories=repositories,
        identities=identities,
        sandbox=sandbox,
        actor=actor,
        workspace=workspace,
        session_id=session_id,
        mode=mode,
        counts=counts,
        hooks=hooks,
        provider=provider,
        service=service,
        run=run,
        settings=settings,
    )


async def set_session_mode(case: SimpleNamespace, value: str) -> int:
    """走真实CAS模式API路径：GET基线→PUT提交；发布与否由调用方决定。"""
    view = await get_policy(
        case.store,
        principal=case.actor,
        workspace=case.workspace,
        session_id=case.session_id,
    )
    result = await set_mode(
        case.store,
        principal=case.actor,
        workspace=case.workspace,
        session_id=case.session_id,
        request_key=f"policy-{view.revision + 1}-{value}",
        expected_revision=view.revision,
        mode=value,
    )
    case.mode["value"] = result.mode
    return result.revision


def reply(pending: dict[str, Any], *, allow: bool) -> ApprovalReply:
    return ApprovalReply(
        request_key="policy-approval-reply",
        call_id=pending["call_id"],
        tool_hash=pending["tool_hash"],
        arguments_hash=pending["arguments_hash"],
        allow=allow,
    )


async def park_and_pending(case: SimpleNamespace) -> dict[str, Any]:
    parked = await case.service.wait(case.run.id)
    assert parked.status == "awaiting", parked.code
    assert case.counts["write"] == 0
    pending = await case.service.approvals(case.run.id)
    assert len(pending) == 1
    return pending[0]


async def pending_from_events(case: SimpleNamespace) -> dict[str, Any]:
    """直接取账本中未答复的approval_request（approvals()会滤掉已过期窗口）。"""
    snapshot = await case.store.snapshot(case.run.id)
    answered = {
        event.data["request_id"]
        for event in snapshot.events if event.kind == "approval_reply"
    }
    requests = [
        dict(event.data) for event in snapshot.events
        if event.kind == "approval_request"
        and event.data["request_id"] not in answered
    ]
    assert requests
    return requests[-1]


async def await_completed(case: SimpleNamespace, timeout_seconds: float = 10) -> None:
    """轮询至运行定稿：停靠task已完成，wait()不会等待新触发的恢复任务。"""

    async def until_done() -> None:
        while True:
            view = await case.service.get(case.run.id)
            if view.status == "completed":
                return
            assert view.status in {"queued", "running", "awaiting"}, view.code
            await asyncio.sleep(0.05)

    await asyncio.wait_for(until_done(), timeout_seconds)


def policy_events(snapshot: Any) -> list[Any]:
    return [event for event in snapshot.events if event.kind == "execution_policy_allow"]


async def close_case(case: SimpleNamespace) -> None:
    await case.service.close()
    await case.sandbox.close()
    await case.identities.close()
    await case.store.close()


async def test_mode_bypass_wakes_parked_ask_without_user_reply(tmp_path: Path) -> None:
    case = await application(tmp_path, [[write_call(1)], [DONE]])
    try:
        pending = await park_and_pending(case)
        # 模式未变：重评不产生放行，运行保持停靠。
        assert await case.service.execution_policy_changed(case.session_id) == 0
        assert (await case.service.get(case.run.id)).status == "awaiting"
        assert case.service.execution_gate(case.session_id) is case.service.execution_gate(
            case.session_id
        )

        await set_session_mode(case, "bypass")
        assert await case.service.execution_policy_changed(case.session_id) == 1
        result = await asyncio.wait_for(case.service.wait(case.run.id), 10)
        assert result.status == "completed", result.code
        assert case.counts["write"] == 1  # 只执行一次
        assert len(case.provider.histories) == 2
        snapshot = await case.store.snapshot(case.run.id)
        events = policy_events(snapshot)
        assert len(events) == 1
        assert events[0].data["request_id"] == pending["request_id"]
        assert events[0].data["reason"] == "session_mode"
        assert events[0].data["call_id"] == pending["call_id"]
        assert not [event for event in snapshot.events if event.kind == "approval_reply"]
        tool_end = next(
            event for event in snapshot.events
            if event.kind == "tool_end" and event.data.get("call_id") == pending["call_id"]
        )
        assert tool_end.data["status"] == "ok"
    finally:
        await close_case(case)


async def test_user_denied_call_is_not_revived_by_bypass(tmp_path: Path) -> None:
    case = await application(tmp_path, [[write_call(1)], [DONE]])
    try:
        pending = await park_and_pending(case)
        await case.service.reply_approval(
            case.run.id, pending["request_id"], reply(pending, allow=False)
        )
        result = await asyncio.wait_for(case.service.wait(case.run.id), 10)
        assert result.status == "completed", result.code
        assert case.counts["write"] == 0

        await set_session_mode(case, "bypass")
        assert await case.service.execution_policy_changed(case.session_id) == 0
        assert (await case.service.get(case.run.id)).status == "completed"
        snapshot = await case.store.snapshot(case.run.id)
        assert policy_events(snapshot) == []
        denied = next(
            event for event in snapshot.events
            if event.kind == "tool_end" and event.data.get("call_id") == pending["call_id"]
        )
        assert denied.data["status"] == "denied"
        assert denied.data["code"] == "approval_denied"
    finally:
        await close_case(case)


async def test_queued_batch_rejudged_when_mode_tightens_mid_batch(tmp_path: Path) -> None:
    case = await application(
        tmp_path, [[write_call(1), write_call(2)], [DONE]],
        request_key="tighten-turn",
    )
    try:
        case.mode["value"] = "bypass"

        async def set_session_mode_to_manual() -> None:
            # 模拟PUT在w1已启动、w2仍在队列时提交：bypass→manual。
            await set_session_mode(case, "manual")

        case.hooks["on_execute"] = set_session_mode_to_manual

        parked = await asyncio.wait_for(case.service.wait(case.run.id), 10)
        assert parked.status == "awaiting", parked.code
        assert case.counts["write"] == 1  # w1已启动，继续至完成
        snapshot = await case.store.snapshot(case.run.id)
        assert policy_events(snapshot) == []
        pending = await case.service.approvals(case.run.id)
        assert len(pending) == 1
        w2_call = next(
            event.data["call_id"] for event in snapshot.events
            if event.kind == "tool_start" and event.data.get("name") == "write"
            and event.data["arguments"]["value"] == 2
        )
        assert pending[0]["call_id"] == w2_call  # 未启动的w2按新模式要求确认
        started = [
            event for event in snapshot.events
            if event.kind == "tool_end" and event.data.get("status") == "ok"
        ]
        assert len(started) == 1 and started[0].data["arguments"]["value"] == 1

        await case.service.reply_approval(
            case.run.id, pending[0]["request_id"], reply(pending[0], allow=True)
        )
        result = await asyncio.wait_for(case.service.wait(case.run.id), 10)
        assert result.status == "completed", result.code
        assert case.counts["write"] == 2  # w2只执行一次
        snapshot = await case.store.snapshot(case.run.id)
        assert policy_events(snapshot) == []
    finally:
        await close_case(case)


async def test_ask_admitted_call_dispatches_allow_after_mode_flips(tmp_path: Path) -> None:
    case = await application(tmp_path, [[write_call(1)], [DONE]],
                             request_key="policy-loosen-turn")
    try:
        # 准备（admit）与派发（revalidate）各调用一次策略；第二次调用前提交
        # bypass切换，模拟manual下准入ask后、真实执行前的模式热切换。
        seen = {"write": 0}

        async def flip_on_second_decision(_name: str) -> None:
            seen["write"] += 1
            if seen["write"] == 2:
                await set_session_mode(case, "bypass")

        case.hooks["on_decide"] = flip_on_second_decision
        result = await asyncio.wait_for(case.service.wait(case.run.id), 10)
        assert result.status == "completed", result.code
        assert case.counts["write"] == 1
        snapshot = await case.store.snapshot(case.run.id)
        assert not [event for event in snapshot.events if event.kind == "approval_request"]
        assert not [event for event in snapshot.events if event.kind == "approval_reply"]
        events = policy_events(snapshot)
        assert len(events) == 1
        assert events[0].data["reason"] == "session_mode"
        assert events[0].data.get("request_id") is None  # 派发边界放行，尚未停靠
    finally:
        await close_case(case)


async def test_auto_unclear_ask_stays_parked_on_policy_change(tmp_path: Path) -> None:
    case = await application(tmp_path, [[write_call(1)], [DONE]],
                             request_key="auto-turn")
    try:
        pending = await park_and_pending(case)
        await set_session_mode(case, "auto")
        assert await case.service.execution_policy_changed(case.session_id) == 0
        assert (await case.service.get(case.run.id)).status == "awaiting"
        snapshot = await case.store.snapshot(case.run.id)
        assert policy_events(snapshot) == []
        assert case.counts["write"] == 0
        assert (await case.service.approvals(case.run.id))[0]["request_id"] == (
            pending["request_id"]
        )
    finally:
        await close_case(case)


async def test_expired_approval_window_is_not_policy_allowed(tmp_path: Path) -> None:
    case = await application(
        tmp_path, [[write_call(1)], [DONE]],
        approval_timeout=0.05, request_key="policy-expiry-turn",
    )
    try:
        parked = await case.service.wait(case.run.id)
        assert parked.status == "awaiting", parked.code
        assert case.counts["write"] == 0
        pending = await pending_from_events(case)
        await asyncio.sleep(0.15)
        await set_session_mode(case, "bypass")
        assert await case.service.execution_policy_changed(case.session_id) == 0
        assert (await case.service.get(case.run.id)).status == "awaiting"
        snapshot = await case.store.snapshot(case.run.id)
        assert policy_events(snapshot) == []
        assert case.counts["write"] == 0
        assert pending["request_id"]  # 账本事实保留，等待用户按现状批复或取消
    finally:
        await close_case(case)


async def test_notification_subscription_drives_wake(tmp_path: Path) -> None:
    case = await application(tmp_path, [[write_call(1)], [DONE]],
                             request_key="notify-turn")
    try:
        pending = await park_and_pending(case)
        revision = await set_session_mode(case, "bypass")
        # 模式API提交后的发布路径：同步回调create_task，运行时重读持久数据。
        execution_policy_notifications.publish(
            ExecutionPolicyChange(case.session_id, "bypass", revision)
        )
        # 停靠task已完成，wait()不会等新触发的恢复；轮询至定稿。
        await await_completed(case)
        assert case.counts["write"] == 1
        snapshot = await case.store.snapshot(case.run.id)
        events = policy_events(snapshot)
        assert len(events) == 1
        assert events[0].data["request_id"] == pending["request_id"]
        assert not [event for event in snapshot.events if event.kind == "approval_reply"]
    finally:
        await close_case(case)


async def test_recheck_failure_keeps_run_parked_without_fake_allow(tmp_path: Path) -> None:
    case = await application(tmp_path, [[write_call(1)], [DONE]],
                             request_key="fault-turn")
    try:
        await park_and_pending(case)

        async def failing_recheck(_session: str, _invocation: Any, _spec: Any) -> str:
            raise ServiceError("storage_unavailable", 503)

        case.service.execution_policy_recheck = failing_recheck
        await set_session_mode(case, "bypass")
        assert await case.service.execution_policy_changed(case.session_id) == 0
        assert (await case.service.get(case.run.id)).status == "awaiting"
        snapshot = await case.store.snapshot(case.run.id)
        assert policy_events(snapshot) == []
        assert case.counts["write"] == 0
    finally:
        await close_case(case)



async def test_session_scope_approval_commits_rule_once_and_replay_never_revives(
    tmp_path: Path,
) -> None:
    """session范围批复与精确规则同一事务定稿；清除后重放旧批复不复活规则。"""
    case = await application(tmp_path, [[write_call(1)], [DONE]])
    try:
        pending = await park_and_pending(case)
        session_id = case.session_id
        approval = ApprovalReply(
            request_key="policy-approval-reply",
            call_id=pending["call_id"],
            tool_hash=pending["tool_hash"],
            arguments_hash=pending["arguments_hash"],
            allow=True,
            scope="session",
        )
        first = await case.service.reply_approval(case.run.id, pending["request_id"], approval)
        assert first == {"request_id": pending["request_id"], "allow": True, "replayed": False}
        await await_completed(case)
        async with case.store.engine.begin() as conn:
            assert await allow_rule_satisfied(
                conn,
                session_id,
                tool_hash=pending["tool_hash"],
                arguments_hash=pending["arguments_hash"],
            )
        # 用户清除规则后，同request_key重放只确认原结果，不补写规则。
        async with case.store.engine.begin() as conn:
            assert await clear_allow_rules(conn, session_id) == 1
        replayed = await case.service.reply_approval(case.run.id, pending["request_id"], approval)
        assert replayed == {"request_id": pending["request_id"], "allow": True, "replayed": True}
        async with case.store.engine.begin() as conn:
            assert not await allow_rule_satisfied(
                conn,
                session_id,
                tool_hash=pending["tool_hash"],
                arguments_hash=pending["arguments_hash"],
            )
    finally:
        await close_case(case)
