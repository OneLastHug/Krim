"""子运行runtime集成：真实RunService + scripted provider 的内部准入、归属、
容量、审批、撤销与收尾；不以裸store充当功能完成。"""

from __future__ import annotations

import asyncio
import hashlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest
from pydantic import BaseModel

from krim_agent.config import Settings
from krim_agent.context.types import ChildRunContext, ContextPolicy
from krim_agent.contracts import (
    ApprovalReply,
    Message,
    RunRequest,
    ServiceError,
    parse_json,
)
from krim_agent.runtime.engine import AgentLoop
from krim_agent.runtime.recovery import BootstrapResume
from krim_agent.runtime.run_tools import open_run_tools
from krim_agent.runtime.service import RunBinding, RunService
from krim_agent.runtime.session_tools import SessionToolHost
from krim_agent.security import LOCAL
from krim_agent.sessions.types import SessionCreate, TurnRequest
from krim_agent.storage import sessions as session_sql
from krim_agent.storage.sqlite import SQLiteStore
from krim_agent.subagents.storage import SubagentStore
from krim_agent.subagents.types import AcceptSpawn, ReserveAgent, StartTask, TreeScope
from krim_agent.tools.core import Decision, Invocation, ToolSpec
from krim_agent.tools.executor import ToolExecutor
from krim_agent.tools.registry import ToolRegistry
from krim_agent.tools.session import SessionToolScope
from tests.support.fake_provider import FakeProvider

WORKSPACE = "local"
DELEGATE_KEY = "subagent:worker"
DIGEST = hashlib.sha256(b"trusted-test-fixture").hexdigest()


class AddArgs(BaseModel):
    left: float
    right: float


class NoteArgs(BaseModel):
    key: str


class GatedProvider(FakeProvider):
    """离线脚本：/hold 阻塞到放行；其余输入复用 FakeProvider。"""

    def __init__(self) -> None:
        super().__init__(delay=0)
        self.release = asyncio.Event()
        self.hold_started = asyncio.Event()

    async def stream(self, messages, tools):
        if messages[-1].content == "/hold":
            self.hold_started.set()
            await self.release.wait()
            from krim_agent.providers.types import ModelText

            yield ModelText(text="released")
            return
        async for event in super().stream(messages, tools):
            yield event


def build_registry(broken_note: bool) -> ToolRegistry:
    async def add(args):
        return {"sum": args.left + args.right}

    async def note(args):
        if broken_note:
            raise RuntimeError("note backend down")
        return {"note": "stored:" + args.key}

    return ToolRegistry((
        ToolSpec("add", AddArgs, add, {"type": "object"}, "test-1", DIGEST),
        ToolSpec("lookup_note", NoteArgs, note, {"type": "object"}, "test-1", DIGEST),
    ))


def build_policy(ask_note: bool):
    async def policy(invocation: Invocation) -> Decision:
        if ask_note and invocation.name == "lookup_note":
            return "ask"
        return "allow"

    return policy


@dataclass
class Harness:
    store: SQLiteStore
    service: RunService
    provider: GatedProvider
    session: Any
    turn: Any
    root: Any
    subagents: SubagentStore
    delegate_descriptor: dict
    captured: dict = field(default_factory=dict)

    async def spawn(self, *, text="child task", key="ac-child.1", history=None):
        tree = await self.subagents.ensure_tree(TreeScope(
            session_id=self.session.id, turn_id=self.turn.id, run_id=self.root.id,
            workspace_id=WORKSPACE, owner_id=LOCAL.principal_id, request_key="tree-1",
        ))
        lead = await self.subagents.reserve_agent(ReserveAgent(
            tree_id=tree["tree_id"], path="lead", role="root", provider="sdk_session",
            profile="coordinator", provider_version="v1", profile_version="p1",
            request_key="spawn-root",
        ))
        reserved = await self.subagents.reserve_agent(ReserveAgent(
            tree_id=tree["tree_id"], path="research", provider="sdk_session",
            profile="worker", provider_version="v1", profile_version="p1",
            parent_id=lead["agent"]["agent_id"], request_key="spawn-1",
        ))
        agent_id = reserved["agent"]["agent_id"]
        activation_id = reserved["activation"]["activation_id"]
        task_input: dict[str, Any] = {"text": text}
        if history is not None:
            task_input["history"] = history
        spawned = await self.subagents.accept_spawn(AcceptSpawn(
            agent_id=agent_id, activation_id=activation_id, input=task_input,
            parent_run_id=self.root.id,
        ))
        child = await self.service.start_delegate(StartTask(
            agent_id=agent_id, task_id=spawned["task_id"],
            activation_id=activation_id,
            request=RunRequest(text=text, request_key=key, target="target-http"),
            target=self.delegate_descriptor, owner_turn_id=self.turn.id,
        ))
        return child, spawned


async def harness(
    tmp_path: Path,
    *,
    root_text: str = "hello",
    max_active_runs: int | None = None,
    broken_note: bool = False,
    ask_note: bool = False,
    with_tools: bool = False,
    loop_cls: type[AgentLoop] = AgentLoop,
) -> Harness:
    provider = GatedProvider()
    config = Settings(
        database=tmp_path / "child.sqlite", run_timeout=30,
        max_request_bytes=262144, max_active_runs=max_active_runs,
        history_compaction=False,
    )
    store = await SQLiteStore.open(config.database)
    executor = ToolExecutor(build_registry(broken_note), build_policy(ask_note))
    loop = loop_cls(store, provider, executor, config)
    captured: dict[str, Any] = {}
    host: SessionToolHost | None = None
    if with_tools:
        # 组合层同序：services必须在freeze bindings之前注入loop（identities）。
        host = SessionToolHost(store)
        loop.session_tool_host = host

        async def factory(scope, base):
            captured["scope"] = scope
            return await open_run_tools(base, scope=scope, resolve=host.resolve)

    binding = RunBinding.freeze("target-http", loop, config)
    delegate = RunBinding.freeze(DELEGATE_KEY, loop, config)
    service = RunService(
        store, loop, config, parse_json(binding.descriptor_json),
        {"target-http": binding},
        delegates={DELEGATE_KEY: delegate},
        subagents_store=SubagentStore(store),
    )
    if with_tools:
        assert host is not None
        host.runtime = service  # composition同序：producer fence需要runtime
        service.session_tool_host = host
        service.run_tool_factory = factory

    target = parse_json(binding.descriptor_json)
    session = await session_sql.create_session(
        store, WORKSPACE, LOCAL.principal_id,
        SessionCreate(request_key="root-session", title="root", target_key="target-http"),
        target, ContextPolicy(), config,
    )
    turn = await session_sql.submit(
        store, session.id,
        TurnRequest(text=root_text, request_key="root-turn", expected_revision=0), config,
    )
    root = await service.start_turn(turn.id)
    return Harness(
        store=store, service=service, provider=provider, session=session, turn=turn,
        root=root, subagents=SubagentStore(store),
        delegate_descriptor=parse_json(delegate.descriptor_json), captured=captured,
    )


async def close_harness(h: Harness) -> None:
    await h.service.close()
    await h.store.close()


@pytest.mark.asyncio
async def test_child_completes_without_user_turn(tmp_path: Path) -> None:
    h = await harness(tmp_path)
    try:
        child, spawned = await h.spawn()
        assert child.status == "queued"
        assert child.request_key == "ac-child.1" and spawned["created"]
        result = await asyncio.wait_for(h.service.wait(child.id), 10)
        assert result.status == "completed", result.code
        facts = await h.service.result(child.id)
        assert "child task" in facts.text
        snapshot = await h.store.snapshot(child.id)
        context = snapshot.session_context
        assert isinstance(context, ChildRunContext)
        assert context.session_id == h.session.id
        assert context.turn_id == h.turn.id
        assert context.owner_id == LOCAL.principal_id
        assert context.parent_run_id == h.root.id
        assert context.messages == ()
        assert context.profile == "worker" and context.depth == 1

        async def count(conn: Any) -> int:
            from sqlalchemy import func, select

            from krim_agent.storage.models import turns

            return await conn.scalar(select(func.count()).select_from(turns))

        assert await h.store.work_read(count) == 1  # 根turn之外未建用户turn
    finally:
        await close_harness(h)


@pytest.mark.asyncio
async def test_ac_child_prefix_is_host_only(tmp_path: Path) -> None:
    h = await harness(tmp_path)
    try:
        with pytest.raises(ServiceError, match="invalid_input"):
            await h.service.start(RunRequest(
                text="forged", request_key="ac-child.forged", target="target-http",
            ))
    finally:
        await close_harness(h)


@pytest.mark.asyncio
async def test_child_capacity_shares_max_active_runs(tmp_path: Path) -> None:
    h = await harness(tmp_path, root_text="/hold", max_active_runs=1)
    try:
        await asyncio.wait_for(h.provider.hold_started.wait(), 5)
        with pytest.raises(ServiceError, match="capacity"):
            await h.spawn()
        h.provider.release.set()
        await asyncio.wait_for(h.service.wait(h.root.id), 10)
        child, _ = await h.spawn()
        result = await asyncio.wait_for(h.service.wait(child.id), 10)
        assert result.status == "completed", result.code
    finally:
        await close_harness(h)


@pytest.mark.asyncio
async def test_child_tool_failure_is_returned_not_fatal(tmp_path: Path) -> None:
    h = await harness(tmp_path, broken_note=True)
    try:
        child, _ = await h.spawn(text="/note k")
        result = await asyncio.wait_for(h.service.wait(child.id), 10)
        # 子worker普通工具错误以结果返回，可修正；不杀整棵树。
        assert result.status == "completed", result.code
        failed = await h.service.start(RunRequest(
            text="/note k", request_key="root-note", target="target-http",
        ))
        root_result = await asyncio.wait_for(h.service.wait(failed.id), 10)
        # 同一错误在单代理默认语义下仍然使运行失败（fail_on_tool_error=True）。
        assert root_result.status == "failed"
    finally:
        await close_harness(h)


@pytest.mark.asyncio
async def test_child_scope_inherits_owner_session_and_turn(tmp_path: Path) -> None:
    h = await harness(tmp_path, with_tools=True)
    try:
        child, _ = await h.spawn(text="/add 2 3")
        result = await asyncio.wait_for(h.service.wait(child.id), 10)
        assert result.status == "completed", result.code
        facts = await h.service.result(child.id)
        assert facts.tools and facts.tools[0].value == {"sum": 5.0}
        scope = h.captured.get("scope")
        assert isinstance(scope, SessionToolScope)
        assert scope.session_id == h.session.id
        assert scope.turn_id == h.turn.id
        assert scope.run_id == child.id
        assert scope.workspace_id == WORKSPACE
        assert scope.actor.principal_id == LOCAL.principal_id
    finally:
        await close_harness(h)


@pytest.mark.asyncio
async def test_child_fork_history_reaches_context(tmp_path: Path) -> None:
    h = await harness(tmp_path)
    try:
        earlier = Message(role="user", content="earlier context").model_dump(mode="json")
        child, _ = await h.spawn(history=[earlier])
        await asyncio.wait_for(h.service.wait(child.id), 10)
        snapshot = await h.store.snapshot(child.id)
        context = snapshot.session_context
        assert isinstance(context, ChildRunContext)
        assert context.messages
        assert context.messages[0].content == "earlier context"
    finally:
        await close_harness(h)


@pytest.mark.asyncio
async def test_child_approval_parks_and_resumes(tmp_path: Path) -> None:
    h = await harness(tmp_path, ask_note=True)
    try:
        child, _ = await h.spawn(text="/note k")
        parked = await asyncio.wait_for(h.service.wait(child.id), 10)
        assert parked.status == "awaiting", parked.code
        pending = await h.service.approvals(child.id)
        assert len(pending) == 1
        request = pending[0]
        reply = ApprovalReply(
            request_key="child-approval", call_id=request["call_id"],
            tool_hash=request["tool_hash"], arguments_hash=request["arguments_hash"],
            allow=True,
        )
        await h.service.reply_approval(child.id, request["request_id"], reply)
        result = await asyncio.wait_for(h.service.wait(child.id), 10)
        assert result.status == "completed", result.code
    finally:
        await close_harness(h)


@pytest.mark.asyncio
async def test_child_runs_are_owned_for_revoke_and_internal_cancel(tmp_path: Path) -> None:
    h = await harness(tmp_path, root_text="/hold")
    try:
        await asyncio.wait_for(h.provider.hold_started.wait(), 5)
        child, _ = await h.spawn(text="/hold")
        owned = await session_sql.owned_active_runs(h.store, LOCAL.principal_id)
        assert child.id in owned  # 撤销收敛经child_run映射覆盖子运行
        view = await h.service.cancel_delegate(child.id)
        assert view.status == "aborted"
        with pytest.raises(ServiceError, match="not_found"):
            # 用户根run不属于child映射：内部取消入口拒绝。
            await h.service.cancel_delegate(h.root.id)
        h.provider.release.set()
        root_result = await asyncio.wait_for(h.service.wait(h.root.id), 10)
        assert root_result.status == "completed", root_result.code
    finally:
        await close_harness(h)


class Recorder:
    def __init__(self) -> None:
        self.events: list[tuple[str, str]] = []
        self.on_stopping_hook: Any = None

    async def on_stopping(self, run_id: str) -> None:
        self.events.append(("stopping", run_id))
        if self.on_stopping_hook is not None:
            await self.on_stopping_hook(run_id)

    async def on_settled(self, run_id: str, status: str, code: str | None) -> None:
        self.events.append(("settled:" + status, run_id))

    async def on_cancelled(self, run_id: str) -> None:
        self.events.append(("cancelled", run_id))


@pytest.mark.asyncio
async def test_on_stopping_stops_descendants_before_root_settles(tmp_path: Path) -> None:
    h = await harness(tmp_path, root_text="/hold")
    try:
        await asyncio.wait_for(h.provider.hold_started.wait(), 5)
        child, _ = await h.spawn(text="/hold")
        recorder = Recorder()

        async def hook(run_id: str) -> None:
            # 仅根事件停后代；子自身on_stopping不得再递归取消。
            if run_id == h.root.id:
                await h.service.cancel_delegate(child.id)

        recorder.on_stopping_hook = hook
        h.service.lifecycle = recorder
        await h.service.cancel(h.root.id)
        root_result = await asyncio.wait_for(h.service.wait(h.root.id), 10)
        assert root_result.status == "aborted"
        child_view = await h.service.get(child.id)
        assert child_view.status == "aborted"
        assert recorder.events[0] == ("stopping", h.root.id)
        assert ("settled:aborted", h.root.id) in recorder.events
        assert any(event[1] == child.id and event[0] == "settled:aborted"
                   for event in recorder.events)
    finally:
        await close_harness(h)


@pytest.mark.asyncio
async def test_close_drains_child_runs(tmp_path: Path) -> None:
    h = await harness(tmp_path)
    try:
        child, _ = await h.spawn(text="/hold")
        await asyncio.wait_for(h.provider.hold_started.wait(), 5)
        await h.service.close()
        assert (await h.service.get(child.id)).status == "aborted"
    finally:
        await close_harness(h)


@pytest.mark.asyncio
async def test_child_budget_expires_with_exhausted_root(tmp_path: Path) -> None:
    """根预算耗尽（remaining≤0）：子deadline立即到期，不给新满预算（§8.3）。"""
    h = await harness(tmp_path, root_text="/hold")
    try:
        await asyncio.wait_for(h.provider.hold_started.wait(), 10)
        root_budget = h.service._execution_budgets.get(h.root.id)
        assert root_budget is not None
        loop = asyncio.get_running_loop()
        root_budget._deadline = loop.time() - 1  # 根剩余归零（测试注入耗尽态）
        assert root_budget.remaining == 0.0
        child, _ = await h.spawn(text="/hold")
        result = await asyncio.wait_for(h.service.wait(child.id), 10)
        assert result.status == "failed", result.code
        assert result.code == "run_timeout"
        # 对照：根预算充足时子可正常推进（其余用例已覆盖），此处释放根收束。
        h.provider.release.set()
        root_result = await asyncio.wait_for(h.service.wait(h.root.id), 10)
        assert root_result.status == "completed", root_result.code
    finally:
        await close_harness(h)


class RecordingLoop(AgentLoop):
    """拦截恢复信封交付（bootstrap sdk_loop完成后立即收束的替身）；
    fresh运行仍走真实AgentLoop（模型/工具/停靠），builder transcript有效。"""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.resumes: list[Any] = []

    async def run(
        self, run_id, approval_request, assert_target_current, *, resume=None, **kwargs
    ) -> None:
        if resume is not None:
            self.resumes.append(resume)
            return
        await super().run(
            run_id, approval_request, assert_target_current, resume=resume, **kwargs
        )


@pytest.mark.asyncio
async def test_bootstrap_resume_park_reply_resume(tmp_path: Path) -> None:
    """真实RunService停靠→批复→恢复：isolated-bootstrap/1 checkpoint解析为
    BootstrapResume并交给loop.run（不复用旧SDKResume、不伪造HostState）。"""
    h = await harness(tmp_path, ask_note=True, loop_cls=RecordingLoop)
    try:
        # RecordingLoop下根立即完成；等根收束再spawn，避开预算pop与
        # 终态落库之间的竞态窗口（生产内父工具spawn总有内存预算）。
        root_result = await asyncio.wait_for(h.service.wait(h.root.id), 10)
        assert root_result.status == "completed", root_result.code
        child, _ = await h.spawn(text="/note k")
        parked = await asyncio.wait_for(h.service.wait(child.id), 10)
        assert parked.status == "awaiting", parked.code
        pending = await h.service.approvals(child.id)
        assert len(pending) == 1
        # helper契约：停车前写isolated-bootstrap/1 checkpoint（此处以真实
        # store.write_checkpoint模拟helper落库动作）。
        checkpoint = {
            "format": "isolated-bootstrap/1",
            "run_id": child.id,
            "ownership": {"owner_id": LOCAL.principal_id, "workspace_id": WORKSPACE},
            "identity": {"agent_id": "bootstrap-agent", "generation": 1},
            "budget": {"remaining": 12.5},
            "pending_invocations": [dict(pending[0])],
        }
        from krim_agent.contracts import fingerprint

        await h.store.write_checkpoint(child.id, checkpoint, fingerprint(checkpoint))
        reply = ApprovalReply(
            request_key="bootstrap-approval", call_id=pending[0]["call_id"],
            tool_hash=pending[0]["tool_hash"], arguments_hash=pending[0]["arguments_hash"],
            allow=True,
        )
        await h.service.reply_approval(child.id, pending[0]["request_id"], reply)
        result = await asyncio.wait_for(h.service.wait(child.id), 10)
        assert result.status == "completed", result.code
        delegate_loop = h.service.delegates.get(DELEGATE_KEY).loop
        assert isinstance(delegate_loop, RecordingLoop)
        resumes = [r for r in delegate_loop.resumes if isinstance(r, BootstrapResume)]
        assert len(resumes) == 1
        resume = resumes[0]
        assert resume.ownership == {
            "owner_id": LOCAL.principal_id, "workspace_id": WORKSPACE,
        }
        assert resume.identity == {"agent_id": "bootstrap-agent", "generation": 1}
        assert resume.budget_remaining == 12.5
        assert resume.pending[0]["request_id"] == pending[0]["request_id"]
        assert resume.replies == {pending[0]["call_id"]: True}
    finally:
        await close_harness(h)


@pytest.mark.asyncio
async def test_bootstrap_resume_rejects_unknown_checkpoint(tmp_path: Path) -> None:
    """helper未写bootstrap格式checkpoint（未知format残留）→批复后恢复必须
    facts_corrupt，不存在无checkpoint的任意审批恢复。"""
    h = await harness(tmp_path, ask_note=True, loop_cls=RecordingLoop)
    try:
        # RecordingLoop下根立即完成；等根收束再spawn，避开预算pop与
        # 终态落库之间的竞态窗口（生产内父工具spawn总有内存预算）。
        root_result = await asyncio.wait_for(h.service.wait(h.root.id), 10)
        assert root_result.status == "completed", root_result.code
        child, _ = await h.spawn(text="/note k")
        parked = await asyncio.wait_for(h.service.wait(child.id), 10)
        assert parked.status == "awaiting", parked.code
        pending = await h.service.approvals(child.id)
        assert len(pending) == 1
        stale = {
            "format": "unknown-bootstrap/9",
            "run_id": child.id,
            "ownership": {},
            "identity": {},
            "budget": {"remaining": 99.0},
            "pending_invocations": [dict(pending[0])],
        }
        from krim_agent.contracts import fingerprint

        await h.store.write_checkpoint(child.id, stale, fingerprint(stale))
        reply = ApprovalReply(
            request_key="bootstrap-approval", call_id=pending[0]["call_id"],
            tool_hash=pending[0]["tool_hash"], arguments_hash=pending[0]["arguments_hash"],
            allow=True,
        )
        await h.service.reply_approval(child.id, pending[0]["request_id"], reply)
        result = await asyncio.wait_for(h.service.wait(child.id), 10)
        assert result.status == "failed", result.code
        assert result.code == "facts_corrupt"
        delegate_loop = h.service.delegates.get(DELEGATE_KEY).loop
        assert isinstance(delegate_loop, RecordingLoop)
        assert not [r for r in delegate_loop.resumes if isinstance(r, BootstrapResume)]
    finally:
        await close_harness(h)
