"""isolated_write 派生执行面 × 组合层 guard 的真实组合回归。

走 open_application 装配出的 ``service.run_tool_factory``（composition.open_tools
原始 guard），按 sdk_loop 的派生方式替换 ``runtime.executor`` 后调用派生 admit。
只窄 mock 无关 I/O 权威（``SessionToolHost.resolve``、``interactions.permits``、
``project_access.execution.session_decision``）；guard 内的 policy_spec、
child_allows 与组合准入链全部执行真实实现。
"""

from __future__ import annotations

import asyncio
import hashlib
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

import krim_agent.composition as composition_module
from krim_agent.composition import open_application
from krim_agent.contracts import (
    RunEvent,
    ServiceError,
    encoded,
    fingerprint,
    parse_json,
)
from krim_agent.development.file_tools import WriteInput
from krim_agent.development.service import ShellInput
from krim_agent.security import LOCAL
from krim_agent.subagents.isolated_tools import ISOLATED_VERSION, isolated_executor
from krim_agent.subagents.workspace import workspace_directory
from krim_agent.tools.bundles import policy_spec as bundles_policy_spec
from krim_agent.tools.core import Invocation, ToolSpec
from krim_agent.tools.executor import ToolContext, ToolExecutor
from krim_agent.tools.pipeline import Scope, _producing
from krim_agent.tools.registry import ToolRegistry
from krim_agent.tools.session import SessionToolScope

DIRECTORY = workspace_directory("iso-regression")
RUN_ID = "run-iso-compose"


def _digest(name: str) -> str:
    return hashlib.sha256(name.encode()).hexdigest()


async def _unavailable(parameters: object) -> object:
    raise ServiceError("workspace_contract_required")


async def _scoped_unavailable(invocation: object, parsed: object, context: object) -> object:
    raise ServiceError("workspace_contract_required")


async def _allow_policy(invocation: object) -> str:
    return "allow"


def _registry() -> ToolRegistry:
    specs = (
        ToolSpec(
            name="shell",
            parameters=ShellInput,
            executor=_unavailable,
            output_schema={"type": "object"},
            version="development/1",
            implementation_digest=_digest("shell"),
            risk="exec",
            scoped_executor=_scoped_unavailable,
            capability="command",
            description="shell tool",
        ),
        ToolSpec(
            name="write",
            parameters=WriteInput,
            executor=_unavailable,
            output_schema={"type": "object"},
            version="development/1",
            implementation_digest=_digest("write"),
            risk="write",
            scoped_executor=_scoped_unavailable,
            capability="write",
            description="write tool",
        ),
    )
    return ToolRegistry(specs)


async def _emit(kind: str, data: dict[str, Any]) -> RunEvent:
    return RunEvent(run_id=RUN_ID, seq=1, event_id="1", kind=kind, data=data)


async def _approve(call: object, seconds: float | None) -> bool:
    return True


def _scope() -> SessionToolScope:
    return SessionToolScope(
        actor=LOCAL,
        workspace_id="local",
        session_id="session-iso",
        turn_id="turn-iso",
        run_id=RUN_ID,
        project_id=None,
        check=lambda: None,
    )


def _context() -> ToolContext:
    frame = Scope(
        frame=object(),
        access=object(),
        operations=object(),
        check=lambda: None,
        initial_deadline=0.0,
    )
    return ToolContext(_emit, _approve, None, lambda: None, frame, None)


def _write_invocation(tool_hash: str, *, path: str = "src/x.py") -> Invocation:
    return Invocation(
        run_id=RUN_ID,
        call_id="call-1",
        span_id="span-1",
        step_index=0,
        name="write",
        arguments_json=encoded({"path": path, "content": "body"}),
        tool_hash=tool_hash,
    )


@dataclass
class Composed:
    app: Any
    run_tools: Any
    derived: ToolExecutor
    original_write: ToolSpec
    runtime: Any
    resolves: list[Invocation] = field(default_factory=list)
    permits: list[ToolSpec] = field(default_factory=list)
    decisions: list[tuple[ToolSpec, Invocation]] = field(default_factory=list)

    async def admit_write(self, *, path: str = "src/x.py", tool_hash: str | None = None) -> Any:
        # 生产中 admit 始终在 RunService 的 producer 任务内执行；_producing
        # 恢复该前提，使 executor 的 _calling 记录与真实链一致。
        task = asyncio.current_task()
        assert task is not None
        invocation = _write_invocation(
            tool_hash or self.derived.registry.get("write").fingerprint,
            path=path,
        )
        with _producing(self.runtime, RUN_ID, task):
            return await self.derived.admit(invocation, _context(), timeout_seconds=None)


async def _composed_run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    permit: bool,
) -> Composed:
    app = await open_application(
        tmp_path / "iso-compose.sqlite",
        registry=_registry(),
        policy=_allow_policy,
    )
    try:
        host = app.service.session_tool_host
        scope = _scope()
        # SessionToolHost._producer 围栏的最小运行面：RunService 在生产中
        # 提供同样的属性；resolve mock 保留 _CALL/CURRENT 身份断言。
        runtime = SimpleNamespace(closed=False, tasks={RUN_ID: None}, _cancelled=set())
        host.runtime = runtime

        async def resolve(invocation: Invocation) -> SessionToolScope:
            from krim_agent.tools.pipeline import _CALL, CURRENT

            record = _CALL.get()
            assert record is not None and record.invocation is invocation
            current = CURRENT.get()
            assert current is None or current.invocation == invocation
            from krim_agent.subagents.providers.process import is_internal_process_invocation

            assert not is_internal_process_invocation(invocation)
            resolves.append(invocation)
            return scope

        resolves: list[Invocation] = []
        monkeypatch.setattr(host, "resolve", resolve)

        async def permits(
            actual_scope: SessionToolScope, spec: ToolSpec, *, trusted_readonly_names: Any = None
        ) -> bool:
            actual_scope.check()
            permits_seen.append(spec)
            return permit

        permits_seen: list[ToolSpec] = []
        monkeypatch.setattr(app.interactions, "permits", permits)

        import krim_agent.project_access.execution as project_execution

        async def session_decision(
            store: object,
            actual_scope: SessionToolScope,
            spec: ToolSpec,
            invocation: Invocation,
            *,
            trusted_readonly_names: Any = frozenset(),
        ) -> str:
            actual_scope.check()
            decisions.append((spec, invocation))
            return "allow"

        decisions: list[tuple[ToolSpec, Invocation]] = []
        monkeypatch.setattr(project_execution, "session_decision", session_decision)

        run_tools = await app.service.run_tool_factory(scope, app.service.loop.executor)
        original_write = run_tools.executor.registry.get("write")
        # sdk_loop.run 的派生替换：isolated_executor 包住当前 executor，
        # 再整体换入 runtime.executor 并刷新 registry 指纹。
        derived = isolated_executor(run_tools.executor, DIRECTORY)
        run_tools.executor = derived
        run_tools._registry_hash = fingerprint(derived.registry.schemas())
        return Composed(
            app=app,
            run_tools=run_tools,
            derived=derived,
            original_write=original_write,
            runtime=runtime,
            resolves=resolves,
            permits=permits_seen,
            decisions=decisions,
        )
    except BaseException:
        await app.close()
        raise


async def test_derived_executor_admission_passes_the_original_guard(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    composed = await _composed_run(tmp_path, monkeypatch, permit=True)
    try:
        # 模型可见面已切到派生指纹；与原 spec 同名不同 hash。
        derived_write = composed.derived.registry.get("write")
        assert derived_write.fingerprint != composed.original_write.fingerprint
        assert derived_write.version == ISOLATED_VERSION
        assert derived_write.scoped_executor is not composed.original_write.scoped_executor
        exposed = {item["name"]: item for item in composed.run_tools.schemas()}
        assert (
            exposed["write"]["implementation_digest"]
            == (derived_write.descriptor()["implementation_digest"])
        )
        assert exposed["shell"]["version"] == ISOLATED_VERSION

        admission = await composed.admit_write()
        assert admission.decision == "allow"

        # guard 定位到原策略表中的原 spec 对象，并以原 hash 回调宿主权威。
        assert composed.permits == [composed.original_write]
        seen_spec, seen_invocation = composed.decisions[0]
        assert seen_spec is composed.original_write
        assert seen_spec.fingerprint == composed.original_write.fingerprint
        # 会话权威看到的是重定位后的调用参数，调用身份仍是原 hash。
        assert seen_invocation.tool_hash == composed.original_write.fingerprint
        assert parse_json(seen_invocation.arguments_json) == {
            "path": f"{DIRECTORY}/src/x.py",
            "content": "body",
        }
        assert composed.resolves[0].tool_hash == composed.original_write.fingerprint
        assert parse_json(composed.resolves[0].arguments_json)["path"] == (f"{DIRECTORY}/src/x.py")
    finally:
        await composed.app.close()


async def test_derived_fingerprint_fence_rejects_original_hash(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    composed = await _composed_run(tmp_path, monkeypatch, permit=True)
    try:
        with pytest.raises(ServiceError) as stale:
            await composed.admit_write(tool_hash=composed.original_write.fingerprint)
        assert stale.value.code == "tool_binding_changed"
        assert composed.decisions == []
    finally:
        await composed.app.close()


async def test_guard_denial_stays_deny_for_derived_calls(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    composed = await _composed_run(tmp_path, monkeypatch, permit=False)
    try:
        admission = await composed.admit_write()
        assert admission.decision == "deny"
        assert admission.code == "permission_denied"
        # 会话权威未被咨询：deny 由 guard 的许可检查直接给出。
        assert composed.decisions == []
        assert composed.permits == [composed.original_write]
    finally:
        await composed.app.close()


async def test_runtime_registry_lookup_reproduces_the_prefix_rejection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """修复前 guard 每次读 state["runtime"].executor.registry：派生替换后
    原 hash 在派生表中查不到，隔离调用被误判 tool_binding_changed。这里
    monkeypatch composition 的 policy_spec 名字恢复该查表行为，验证固定
    原 registry 的必要性。"""
    composed = await _composed_run(tmp_path, monkeypatch, permit=True)
    try:

        def runtime_lookup(registry: ToolRegistry, invocation: Invocation) -> ToolSpec:
            return bundles_policy_spec(composed.run_tools.executor.registry, invocation)

        monkeypatch.setattr(composition_module, "policy_spec", runtime_lookup)
        with pytest.raises(ServiceError) as rejected:
            await composed.admit_write()
        assert rejected.value.code == "tool_binding_changed"
        # 拒绝发生在宿主权威之前，未产生任何许可或会话决议。
        assert composed.permits == [] and composed.decisions == []
    finally:
        await composed.app.close()
