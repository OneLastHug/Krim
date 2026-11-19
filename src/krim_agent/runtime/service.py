"""Run 生命周期和审批；内存任务只调度，持久状态来自 RunStore。"""

from __future__ import annotations

import asyncio
import inspect
import secrets
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Any, cast

from pydantic import JsonValue
from sqlalchemy import select, update

from krim_agent.config import SESSION_FIELDS, Settings
from krim_agent.context import builder
from krim_agent.contracts import (
    TERMINAL,
    ApprovalReply,
    RunEvent,
    RunFacts,
    RunRequest,
    RunStatus,
    RunView,
    ServiceError,
    encoded,
    fingerprint,
    parse_json,
)
from krim_agent.observability import TraceView, project_trace
from krim_agent.providers.openai_compatible import OpenAICompatibleProvider
from krim_agent.source_fingerprints import source_digest
from krim_agent.storage.sandbox_models import sandbox_operations
from krim_agent.tools.core import Decision, Invocation, ToolSpec
from krim_agent.tools.executor import ToolExecutor
from krim_agent.tools.pipeline import _owns, _producing
from krim_agent.tools.registry import ToolRegistry

from . import model_step as model_step_module
from .contracts import ExecutionBudget, RunSnapshot, RunStore, settled
from .engine import AgentLoop, Parked
from .facts import project_facts
from .recovery import SDKResume, sdk_resume
from .sdk_loop import DevelopmentSDKLoop

type ExecutionPolicyRecheck = Callable[[str, Invocation, ToolSpec], Awaitable[Decision]]
"""停靠审批的模式重评接口（组合层注入）。

签名 ``(session_id, invocation, spec) -> Decision``：实现须每次从权威存储
重读项目访问、会话执行模式与本会话允许规则（即
``project_access.execution.session_decision`` 的薄封装），不得缓存模式或
信任通知。RunService 只把 ``allow`` 用作模式放行依据；``ask`` 维持停靠，
``deny`` 与异常一律不触发放行。
"""


def implementation(value: object | None) -> dict[str, JsonValue]:
    if value is None:
        return {"id": "none", "version": "1", "digest": fingerprint("none")}
    selected = value if inspect.isfunction(value) else type(value)
    path = inspect.getsourcefile(selected)
    digest = (
        source_digest(path)
        if path
        else fingerprint(getattr(selected, "__qualname__", "host"))
    )
    return {
        "id": str(getattr(selected, "__module__", "host"))
        + "."
        + str(getattr(selected, "__qualname__", "host")),
        "version": "1",
        "digest": digest,
    }


@dataclass(frozen=True)
class RunBinding:
    loop: AgentLoop
    settings: Settings
    descriptor_json: str
    identities: tuple[int | str, ...]
    tool_identities: tuple[int, ...]
    _http_token: tuple[object, ...] | None = field(default=None, repr=False)

    @staticmethod
    def runtime_identities(loop: AgentLoop) -> tuple[int | str, ...]:
        def function(value: object) -> object:
            return getattr(value, "__func__", value)

        result: tuple[int | str, ...] = tuple(
            map(
                id,
                (
                    loop.provider,
                    function(loop.provider.stream),
                    # close归Application资源所有权；宿主可包装清理观察，不改变执行绑定。
                    loop.executor.registry,
                    loop.executor.policy,
                    loop.executor.pre,
                    loop.executor.post,
                    loop.settings,
                ),
            )
        )
        if loop.executor.middleware or loop.executor.wrap is not None or loop.work is not None:
            result += tuple(map(id, (*loop.executor.middleware, loop.executor.wrap, loop.work)))
        if loop.work is not None:
            # identities是稳定服务引用：改名/轮换/成员变更不替换对象，绑定不漂移。
            result += tuple(map(id, (loop.work.profile, loop.work.artifacts, loop.work.runner,
                loop.work.access_policy, loop.work.identities)))
            if loop.work.runner is not None:
                result += (id(loop.work.runner.profile), id(loop.work.runner.fence),
                    id(loop.work.runner.hook), hash(repr(loop.work.runner.profile)),
                    id(function(loop.work.runner.run)))
            result += (loop.work.artifacts.fd, hash(repr(loop.work.artifacts.root)))
            result += tuple(id(getattr(getattr(loop.work.artifacts, name), "__func__",
                getattr(loop.work.artifacts, name))) for name in ["_blob", "_stage", "input",
                "publish", "_owned"])
            result += (id(getattr(loop.executor.execute, "__func__", loop.executor.execute)),
                       id(getattr(loop.work.execute, "__func__", loop.work.execute)))
            from krim_agent.workspace import verification

            verifier = loop._verifier
            result += (id(verification), id(verifier), id(type(verifier)))
            # 在describe/Verifier方法被调用前，直接核对实际函数及其代码对象。
            result += tuple(
                token
                for name in ("verify", "descriptor", "binding_token", "assert_current")
                for method in (function(getattr(verifier, name)),)
                for token in (id(method), id(getattr(method, "__code__", None)))
            )
            result += tuple(id(getattr(verification, name)) for name in verification._NAMES)
        from krim_agent.agent_sdk import adapter as sdk_adapter
        from krim_agent.agent_sdk import context as sdk_context
        from krim_agent.agent_sdk import host as sdk_host
        from krim_agent.agent_sdk import model as sdk_model
        from krim_agent.agent_sdk import response as sdk_response

        result += tuple(source_digest(cast(str, module.__file__)) for module in (
            sdk_adapter, sdk_context, sdk_host, sdk_model, sdk_response,
        ))
        result += tuple(map(id, (
            loop.context_service, loop.session_tool_host, loop.session_media,
        )))
        if loop.context_service is not None:
            result += (id(loop.context_service.snapshots),)
        root = Path(__file__).parents[1]
        result += tuple(source_digest(root / path) for path in (
            "storage/session_context.py", "context/serialization.py", "context/service.py",
            "runtime/session_tools.py", "runtime/session_snapshots.py", "runtime/run_tools.py",
            "runtime/session_media.py", "tools/bundles.py",
        ))
        if isinstance(loop, DevelopmentSDKLoop):
            from krim_agent.context import compaction, compaction_source
            from krim_agent.runtime import sdk_loop
            from krim_agent.storage import development_context

            result += (id(function(loop.run)), id(loop.development_store),
                       id(function(loop.development_scope)), id(function(loop.development_cleanup)),
                       id(function(loop.development_approval_projection)),
                       id(function(loop.development_media_for_run)))
            result += tuple(source_digest(cast(str, module.__file__)) for module in (
                sdk_loop, compaction, compaction_source, development_context,
            ))
            if loop.settings.development_history_compaction:
                result += tuple(map(id, (
                    development_context.DevelopmentCompactionSource.verify,
                    development_context.DevelopmentCompactionSource.verify_frozen,
                    development_context.DevelopmentCompactionSource.page,
                    development_context.DevelopmentCompactionSource.save_prepared,
                    development_context.DevelopmentCompactionSource.read_prepared,
                )))
                result += (hash(compaction.binding_token()),)
        if loop.sandbox is not None:
            result += (
                id(loop.sandbox),
                id(loop.sandbox.service),
                id(loop.sandbox.service.broker),
                id(loop.sandbox.service.identities),
                hash(loop.sandbox.service.configuration.model_dump_json(exclude_unset=True)),
                id(function(loop.sandbox.scope)),
                id(function(loop.sandbox.execute)),
            )
            from krim_agent.sandbox import bindings as sandbox_bindings
            from krim_agent.sandbox import command as sandbox_command
            from krim_agent.sandbox import execution as sandbox_execution
            from krim_agent.sandbox import file_models as sandbox_file_models
            from krim_agent.sandbox import operations as sandbox_operations
            from krim_agent.sandbox import tasks as sandbox_tasks

            result += tuple(
                source_digest(cast(str, module.__file__))
                for module in (
                    sandbox_command,
                    sandbox_execution,
                    sandbox_operations,
                    sandbox_bindings,
                    sandbox_file_models,
                    sandbox_tasks,
                )
            )
        from krim_agent import config as config_module
        from krim_agent.tools import executor as executor_module
        from krim_agent.tools import pipeline as pipeline_module
        from krim_agent.workspace import operations as operations_module
        from krim_agent.workspace import types as workspace_types

        from . import contracts as contracts_module
        from . import engine as engine_module
        from .contracts import ExecutionBudget

        result += tuple(map(id, (
            ExecutionBudget,
            ExecutionBudget.bind,
            ExecutionBudget.pause,
            ExecutionBudget.finish,
            cast(property, ExecutionBudget.deadline).fget,
            engine_module.AgentLoop.run,
            model_step_module.ModelStep.call,
            executor_module.ToolExecutor.execute,
            cast(property, pipeline_module.Scope.deadline).fget,
            operations_module.WorkspaceOperations.scope,
            RunService._drive,
            RunService._approval_request,
        )))
        result += tuple(source_digest(cast(str, module.__file__)) for module in (
            config_module,
            contracts_module,
            engine_module,
            model_step_module,
            executor_module,
            pipeline_module,
            operations_module,
            workspace_types,
        )) + (source_digest(__file__),)
        return result

    @staticmethod
    def tool_tokens(loop: AgentLoop) -> tuple[int, ...]:
        result = tuple(
            token
            for spec, _ in loop.executor.registry._frozen
            for token in (
                id(spec),
                id(spec.executor),
                id(spec.parameters),
                id(
                    getattr(
                        spec.parameters.model_validate, "__func__", spec.parameters.model_validate
                    )
                ),
            )
        )
        result += tuple(id(spec.scoped_executor) for spec,
            _ in loop.executor.registry._frozen if spec.scoped_executor is not None)
        return result + tuple(
            token
            for spec, _ in loop.executor.registry._frozen if spec.external_executor is not None
            for token in (
                id(spec.external_executor), id(getattr(spec.external_executor, "__code__", None)),
                id(spec.source), id(spec.binding_target),
            )
        )

    @staticmethod
    def describe(key: str, loop: AgentLoop, settings: Settings) -> dict[str, JsonValue]:
        provider = implementation(loop.provider)
        provider.update(
            name=provider["id"],
            behavior_version=str(getattr(loop.provider, "mode", "demo")) + "-parser/1",
            implementation_digest=provider["digest"],
            config={
                "mode": str(getattr(loop.provider, "mode", "demo")),
                "delay": getattr(loop.provider, "delay", None),
            },
        )
        if isinstance(loop.provider, OpenAICompatibleProvider):
            provider.update(behavior_version="http-sse/1", config=loop.provider.identity())
        policy_callable = (getattr(loop.executor.policy, "__func__", loop.executor.policy)
                           if loop.work is not None or loop.sandbox is not None
                           or isinstance(loop, DevelopmentSDKLoop)
                           else loop.executor.policy)
        policy = implementation(policy_callable)
        policy["allowed_names"] = (
            ["add", "echo"]
            if policy["id"]
            in {
                "krim_agent.tools.builtins.demo_policy",
                "krim_agent.evaluation.targets.controlled_policy",
            }
            else None
        )
        limits = settings.model_dump(
            mode="json",
            exclude={
                "database",
                "api_key",
                "system_prompt",
                "target_version",
                "provider",
                *SESSION_FIELDS,
            },
        )
        if isinstance(loop.provider, OpenAICompatibleProvider):
            limits["http"] = loop.provider.execution_limits()
        descriptor: dict[str, JsonValue] = {
            "schema_version": 2,
            "key": key,
            "version": settings.target_version,
            "provider": provider,
            "system_prompt": settings.system_prompt,
            "prompt_fingerprint": fingerprint(settings.system_prompt),
            "tools": cast(JsonValue, loop.executor.registry.schemas()),
            "policy": policy,
            "pre": implementation(loop.executor.pre),
            "post": implementation(loop.executor.post),
            "limits": limits,
            "limits_fingerprint": fingerprint(limits),
            "agent_loop": "ai-python/0.9.0-host-v2",
        }
        if loop.work is not None:
            descriptor["workspace_profile"] = loop.work.profile.model_dump(mode="json")
            descriptor["workspace_implementation"] = {
                "operations": implementation(loop.work), "executor": implementation(loop.executor),
                "middleware": [implementation(value) for value in loop.executor.middleware],
                "wrap": implementation(loop.executor.wrap),
            }
            assert loop._verifier is not None
            descriptor["workspace_verification"] = loop._verifier.descriptor()
        if isinstance(loop, DevelopmentSDKLoop):
            descriptor["development"] = loop.development_descriptor
            descriptor["agent_loop"] = "ai-python/0.9.0-host-v1"
            if settings.development_history_compaction:
                descriptor["development_history_compaction"] = True
        if loop.sandbox is not None:
            descriptor["sandbox"] = {"version": "sandbox/1",
                "image": loop.sandbox.service.configuration.command_image,
                "completion": "sandbox-completion/1-when-declared-otherwise-receipt-only",
                "implementation": implementation(loop.sandbox)}
        return descriptor

    @classmethod
    def freeze(cls, key: str, loop: AgentLoop, settings: Settings) -> RunBinding:
        descriptor = cls.describe(key, loop, settings)
        return cls(
            loop,
            settings,
            encoded(descriptor),
            cls.runtime_identities(loop),
            cls.tool_tokens(loop),
            loop.provider.binding_token()
            if isinstance(loop.provider, OpenAICompatibleProvider)
            else None,
        )

    def assert_current(self) -> None:
        # describe() validates and returns the current registry schemas below.
        # Keep one complete schema check per synchronous binding check.
        if self.tool_tokens(self.loop) != self.tool_identities:
            raise ServiceError("tool_binding_changed")
        if self._http_token is not None and (
            not isinstance(self.loop.provider, OpenAICompatibleProvider)
            or self.loop.provider.binding_token() != self._http_token
        ):
            raise ServiceError("target_binding_changed")
        actual = self.runtime_identities(self.loop)
        descriptor = cast(dict[str, JsonValue], parse_json(self.descriptor_json))
        if (
            actual != self.identities
            or encoded(self.describe(cast(str, descriptor["key"]), self.loop, self.loop.settings))
            != self.descriptor_json
        ):
            raise ServiceError("target_binding_changed")


@dataclass(frozen=True)
class TurnBinding:
    base: RunBinding
    context_token: tuple[object, ...]
    executor: ToolExecutor | None = None
    account_mcp_connections: tuple[Any, ...] = ()

    @property
    def loop(self) -> AgentLoop:
        return self.base.loop

    @property
    def settings(self) -> Settings:
        return self.base.settings

    def assert_current(self) -> None:
        self.base.assert_current()
        if builder.binding_token() != self.context_token:
            raise ServiceError("context_binding_changed")
        if self.executor is not None:
            self.executor.registry.assert_current()


class RunService:
    def __init__(
        self,
        store: RunStore,
        loop: AgentLoop,
        settings: Settings,
        target: dict[str, JsonValue],
        bindings: dict[str, RunBinding] | None = None,
        *,
        execution_policy_recheck: ExecutionPolicyRecheck | None = None,
    ) -> None:
        self.store, self.loop, self.settings = store, loop, settings
        self._target_json = encoded(target)
        self.execution_policy_recheck = execution_policy_recheck
        self._policy_unsubscribe: Callable[[], None] | None = None
        self.bindings = MappingProxyType(
            bindings
            if bindings is not None
            else {"demo": RunBinding.freeze("demo", loop, settings)}
        )
        self.tasks: dict[str, asyncio.Task[None]] = {}
        self._admission = asyncio.Lock()
        self._approval_lock = asyncio.Lock()
        self._input_lock = asyncio.Lock()
        self.input_gate = asyncio.Lock()
        self._execution_budgets: dict[str, ExecutionBudget] = {}
        self.account_mcp: Any | None = None
        self.interactions: Any | None = None
        self.session_tool_host: Any | None = None
        self.tool_runtime: Any | None = None
        self.run_tool_factory: Any | None = None
        self._cancelled: set[str] = set()
        self._finalizing: set[str] = set()
        self._close_task: asyncio.Task[None] | None = None
        self.closed = False
        self.slot_changed = asyncio.Event()
        self._subscribe_execution_policy()

    @staticmethod
    def execution_gate(session_id: str) -> asyncio.Lock:
        """单会话启动闸门；注册表共享，模式API提交与运行时交接同锁（ADR-0015）。

        模式API经 sessions.execution_policy.commit_mode 在闸门内提交CAS；
        运行时在queued调用派发边界取同一把闸门（见 _drive 的 startup_gate），
        executor在started标记落定后释放，不持有整段工具执行或模型等待。
        """
        from krim_agent.sessions.execution_policy import execution_gate as shared_gate

        return shared_gate(session_id)

    def _external(self) -> None:
        if asyncio.current_task() in self.tasks.values():
            raise ServiceError("self_wait")
        if _owns(self, self.tasks):
            raise ServiceError("self_wait")

    async def start(self, request: RunRequest) -> RunView:
        job = asyncio.create_task(self._admit(request))
        try:
            return await asyncio.shield(job)
        except asyncio.CancelledError:
            while not job.done():
                try:
                    await asyncio.shield(job)
                except asyncio.CancelledError:
                    pass
            if not job.cancelled() and job.exception() is None:
                try:
                    await settled(self.cancel(job.result().id))
                finally:
                    raise asyncio.CancelledError
            raise

    async def _admit(self, request: RunRequest) -> RunView:
        async with self._admission:
            return await self._admit_locked(request)

    async def start_child(self, eval_id: str, case_id: str, repetition: int) -> RunView:
        async with self._admission:
            request, digest = await self.store.child_request(eval_id, case_id, repetition)
            return await self._admit_locked(request, internal=True, expected=digest)

    async def start_turn(self, turn_id: str) -> RunView:
        async with self._admission:
            request, digest = await self.store.turn_request(turn_id)
            prior = await self.store.by_key(request.request_key)
            if prior is not None:
                snapshot = await self.store.snapshot(prior.id)
                if snapshot.session_context is None or snapshot.session_context.turn_id != turn_id:
                    raise ServiceError("facts_corrupt")
                return prior
            return await self._admit_locked(
                request, internal=True, expected=digest, turn_id=turn_id
            )

    async def _admit_locked(
        self,
        request: RunRequest,
        *,
        internal: bool = False,
        expected: str | None = None,
        turn_id: str | None = None,
    ) -> RunView:
        if self.closed:
            raise ServiceError("closed", 503)
        if not self.store.healthy:
            raise ServiceError("storage_unavailable", 503)
        if (self.settings.max_input_bytes is not None
                and len(request.text.encode()) > self.settings.max_input_bytes):
            raise ServiceError("input_limit", 422)
        existing = await self.store.by_key(request.request_key)
        if existing is not None:
            snapshot = await self.store.snapshot(existing.id)
            if (
                snapshot.request != request
                or expected is not None
                and fingerprint(snapshot.target) != expected
            ):
                raise ServiceError("key_conflict")
            return existing
        if request.request_key.startswith(("lc-eval.", "ac-turn.")) and not internal:
            raise ServiceError("invalid_input", 422)
        if request.target == "target-workspace" and turn_id is None:
            raise ServiceError("workspace_contract_required")
        binding = self.bindings.get(request.target)
        if binding is None:
            raise ServiceError("invalid_input", 422)
        binding.assert_current()
        if (binding.loop.sandbox is not None or isinstance(binding.loop, DevelopmentSDKLoop)) \
                and turn_id is None:
            raise ServiceError("workspace_contract_required")
        if expected is not None and fingerprint(parse_json(binding.descriptor_json)) != expected:
            raise ServiceError("target_binding_changed")
        if (binding.settings.max_input_bytes is not None
                and len(request.text.encode()) > binding.settings.max_input_bytes):
            raise ServiceError("input_limit", 422)
        if (
            existing is None and self.settings.max_active_runs is not None
            and sum(not t.done() for t in self.tasks.values()) >= self.settings.max_active_runs
        ):
            raise ServiceError("capacity", 429)
        descriptor = cast(dict[str, JsonValue], parse_json(binding.descriptor_json))
        executing: RunBinding | TurnBinding = binding
        provisional_connections: tuple[Any, ...] = ()
        if turn_id is None:
            run, created = await self.store.begin(request, descriptor)
        else:
            executor = None
            try:
                if self.account_mcp is not None and self.run_tool_factory is None:
                    configs = await self.account_mcp.configurations_for_turn(turn_id)
                    if configs:
                        executor, provisional_connections = await self.account_mcp.build_runtime(
                            configs, binding.loop.executor
                        )
                run, created = await self.store.begin_turn_run(
                    turn_id, request, descriptor, binding.settings
                )
            except BaseException:
                if provisional_connections:
                    await asyncio.gather(
                        *(item.close() for item in provisional_connections),
                        return_exceptions=True,
                    )
                raise
            if created:
                executing = TurnBinding(
                    binding,
                    builder.binding_token(),
                    executor=executor,
                    account_mcp_connections=provisional_connections,
                )
            elif provisional_connections:
                await asyncio.gather(
                    *(item.close() for item in provisional_connections),
                    return_exceptions=True,
                )
        if created:
            if isinstance(executing, TurnBinding):
                try:
                    executing.assert_current()
                except BaseException:
                    if executing.account_mcp_connections:
                        await asyncio.gather(
                            *(item.close() for item in executing.account_mcp_connections),
                            return_exceptions=True,
                        )
                    raise
            self.tasks[run.id] = asyncio.create_task(
                self._drive(run, executing), name="krim-run-" + run.id
            )
            self.tasks[run.id].add_done_callback(self._task_done)
            self.tasks[run.id].add_done_callback(lambda _task: self.slot_changed.set())
        if len(self.tasks) > 128:
            for run_id, task in list(self.tasks.items()):
                if task.done() and run_id != run.id:
                    self.tasks.pop(run_id)
                    if len(self.tasks) <= 128:
                        break
        return run

    @staticmethod
    def _task_done(task: asyncio.Task[None]) -> None:
        if not task.cancelled():
            task.exception()  # 读取异常，避免未消费 Task 把私有异常写入事件循环日志。

    async def _drive(
        self, run: RunView, binding: RunBinding | TurnBinding, *, resume: bool = False
    ) -> None:
        producer = self.tasks.get(run.id)
        if producer is None or producer is not asyncio.current_task():
            raise ServiceError("self_wait")
        with _producing(self, run.id, cast(asyncio.Task[None], producer)):
            status: RunStatus = "completed"
            code: str | None = None
            budget: ExecutionBudget | None = None
            parked = False
            recovery_checkpoint = None
            run_tools = None
            session_scope = None
            executing: RunBinding | TurnBinding = binding
            base = binding.base if isinstance(binding, TurnBinding) else binding
            gate_session = getattr(
                (await self.store.snapshot(run.id)).session_context, "session_id", None
            )

            def startup_gate() -> asyncio.Lock | None:
                # 非会话运行（demo/eval/workspace）没有会话执行模式，不设闸门。
                return self.execution_gate(gate_session) if gate_session is not None else None

            try:
                if resume:
                    # 停靠恢复（ADR-0015）：预算从恢复点剩余量重建，等待期不消耗
                    recovery_checkpoint = await self.store.read_checkpoint(run.id)
                    if recovery_checkpoint is None:
                        raise ServiceError("facts_corrupt")
                    data, _stored = recovery_checkpoint
                    remaining = data.get("budget_remaining")
                    deadline = (None if remaining is None
                                else asyncio.get_running_loop().time()
                                + max(0.0, float(remaining)))
                else:
                    timeout = (
                        binding.loop.work.profile.runtime.run_timeout_seconds
                        if binding.loop.work is not None else binding.settings.run_timeout
                    )
                    deadline = (None if timeout is None else
                                asyncio.get_running_loop().time() + max(
                                    0, run.created_at + timeout - time.time()))
                budget = ExecutionBudget(deadline)
                self._execution_budgets[run.id] = budget
                timeout_context = asyncio.timeout_at(deadline)
                budget.bind(timeout_context)
                async with timeout_context:
                    await self.store.set_running(run.id)
                    recovery: SDKResume | None = None
                    if resume:
                        assert recovery_checkpoint is not None
                        snapshot = await self.store.snapshot(run.id)
                        # turn 运行的 target 带会话 context 字段：按既有约定排除后比对
                        if fingerprint(parse_json(base.descriptor_json)) != fingerprint({
                            key: value
                            for key, value in snapshot.target.items()
                            if key != "context"
                        }):
                            # 停靠后环境（目标档案）变了：不静默续跑（ADR-0015 硬门）
                            raise ServiceError("target_binding_changed")
                        if snapshot.session_context is not None:
                            from krim_agent.context import builder as context_builder

                            try:
                                executing = TurnBinding(base, context_builder.binding_token())
                                if self.account_mcp is not None and self.run_tool_factory is None:
                                    configs = await self.account_mcp.configurations_for_turn(
                                        snapshot.session_context.turn_id
                                    )
                                    if configs:
                                        executor, connections = (
                                            await self.account_mcp.build_runtime(
                                                configs, base.loop.executor
                                            )
                                        )
                                        executing = TurnBinding(
                                            base,
                                            context_builder.binding_token(),
                                            executor,
                                            connections,
                                        )
                            except BaseException:
                                if (
                                    isinstance(executing, TurnBinding)
                                    and executing.account_mcp_connections
                                ):
                                    await asyncio.gather(
                                        *(
                                            item.close()
                                            for item in executing.account_mcp_connections
                                        ),
                                        return_exceptions=True,
                                    )
                                raise
                        replies = {
                            str(cast(dict[str, JsonValue], event.data["reply"])["call_id"]):
                            bool(cast(dict[str, JsonValue], event.data["reply"])["allow"])
                            for event in snapshot.events
                            if event.kind == "approval_reply"
                        }
                        for event in snapshot.events:
                            if event.kind != "execution_policy_allow":
                                continue
                            call_id = event.data.get("call_id")
                            # 用户批复优先；模式放行只是内部恢复决定，仅映射
                            # SDK hook 的allow决议，绝不写用户approval_reply。
                            if isinstance(call_id, str) and call_id:
                                replies.setdefault(call_id, True)
                        data, stored = recovery_checkpoint
                        recovery = sdk_resume(data, stored, replies)
                    if self.session_tool_host is not None:
                        current = await self.store.snapshot(run.id)
                        if current.session_context is not None:
                            session_scope = await self.session_tool_host.by_run(
                                run.id, executing.assert_current,
                            )
                            if self.run_tool_factory is not None:
                                run_tools = await self.run_tool_factory(
                                    session_scope, executing.loop.executor,
                                )
                    try:
                        await executing.loop.run(
                            run.id,
                            self._approval_request,
                            executing.assert_current,
                            execution_budget=budget,
                            resume=cast(Any, recovery),
                            executor=(
                                run_tools.executor if run_tools is not None else
                                executing.executor if isinstance(executing, TurnBinding) else None
                            ),
                            startup_gate=startup_gate,
                            **({"run_tools": run_tools, "session_scope": session_scope}
                               if run_tools is not None or session_scope is not None else {}),
                        )
                    except BaseException:
                        if isinstance(executing, TurnBinding) and executing.account_mcp_connections:
                            await asyncio.gather(
                                *(
                                    connection.close()
                                    for connection in executing.account_mcp_connections
                                ),
                                return_exceptions=True,
                            )
                        raise
            except Parked:
                parked = True  # 停靠：恢复点已由引擎写入，状态转 awaiting
            except asyncio.CancelledError:
                status, code = "aborted", "cancelled"
            except TimeoutError:
                status, code = "failed", "run_timeout"
            except ServiceError as error:
                status = "aborted" if error.code == "cancelled" else "failed"
                code = error.code
            except Exception:
                status, code = "failed", "execution_error"
            finally:
                if run_tools is not None:
                    await asyncio.gather(run_tools.close(), return_exceptions=True)
                if isinstance(binding, TurnBinding) and binding.account_mcp_connections:
                    await asyncio.gather(
                        *(connection.close() for connection in binding.account_mcp_connections),
                        return_exceptions=True,
                    )
                if (
                    isinstance(executing, TurnBinding)
                    and executing is not binding
                    and executing.account_mcp_connections
                ):
                    await asyncio.gather(
                        *(connection.close() for connection in executing.account_mcp_connections),
                        return_exceptions=True,
                    )
                if budget is not None:
                    budget.finish()
                    self._execution_budgets.pop(run.id, None)
                self._finalizing.add(run.id)
                try:
                    if parked:
                        parked = await self.store.set_awaiting(run.id)
                        if not parked:
                            status, code = "aborted", "cancelled"
                    if not parked and isinstance(base.loop, DevelopmentSDKLoop):
                        try:
                            await base.loop.development_cleanup(run.id)
                        except Exception:
                            status, code = "failed", "sandbox_cleanup_failed"
                    if not parked and base.loop.sandbox is not None:
                        from krim_agent.sandbox.tasks import cancel_run_tasks

                        try:
                            await cancel_run_tasks(base.loop.sandbox.service, run.id)
                        except ServiceError as error:
                            failure_code = error.code

                            async def mark_tasks_unknown(conn: Any) -> None:
                                rows = (
                                    await conn.execute(
                                        select(sandbox_operations).where(
                                            sandbox_operations.c.kind == "command",
                                            sandbox_operations.c.result["run_id"].as_string()
                                            == run.id,
                                        )
                                    )
                                ).mappings().all()
                                now = time.time()
                                for operation in rows:
                                    saved = operation["result"]
                                    if (
                                        not isinstance(saved, dict)
                                        or saved.get("background") is not True
                                        or saved.get("run_id") != run.id
                                    ):
                                        continue
                                    receipt = saved.get("receipt")
                                    if isinstance(receipt, dict):
                                        receipt = {
                                            **receipt,
                                            "cleanup": "unknown",
                                            "output_complete": False,
                                            "code": failure_code,
                                            "verification": "unknown",
                                        }
                                    if not isinstance(receipt, dict):
                                        receipt = {
                                            "executor_started": None,
                                            "exit_code": None,
                                            "stdout": "",
                                            "stderr": "",
                                            "cleanup": "unknown",
                                            "output_complete": False,
                                            "code": failure_code,
                                            "verification": "unknown",
                                        }
                                    await conn.execute(
                                        update(sandbox_operations)
                                        .where(
                                            sandbox_operations.c.operation_id
                                            == operation["operation_id"]
                                        )
                                        .values(
                                            status="unknown",
                                            result={**saved, "receipt": receipt},
                                            updated_at=now,
                                        )
                                    )
                            await base.loop.sandbox.service.store._write(mark_tasks_unknown)
                            if status == "completed":
                                status, code = "failed", error.code
                    if not parked:
                        await self.store.finish(run.id, status, code)
                finally:
                    self._finalizing.discard(run.id)
                    self._cancelled.discard(run.id)

    async def get(self, run_id: str) -> RunView:
        return (await self.store.snapshot(run_id)).run

    async def by_key(self, key: str) -> RunView:
        view = await self.store.by_key(key)
        if view is None:
            raise ServiceError("not_found", 404)
        return view

    async def wait(self, run_id: str) -> RunView:
        self._external()
        task = self.tasks.get(run_id)
        if task is not None:
            await asyncio.shield(task)
        return await self.get(run_id)

    async def result(self, run_id: str) -> RunFacts:
        snapshot = await self.store.snapshot(run_id)
        if snapshot.run.status not in TERMINAL:
            raise ServiceError("not_terminal")
        return project_facts(snapshot)

    async def trace(self, run_id: str) -> TraceView:
        return project_trace(await self.store.snapshot(run_id))

    async def events(self, run_id: str, from_seq: int = 0, limit: int = 64) -> list[RunEvent]:
        page = await self.store.event_page(run_id, from_seq, limit)
        return list(page.events)

    async def stream(self, run_id: str, from_seq: int = 0) -> AsyncIterator[RunEvent | None]:
        cursor = from_seq
        heartbeat = time.monotonic()
        while True:
            page = await self.store.event_page(run_id, cursor, 64)
            for record in page.events:
                cursor = record.seq + 1
                heartbeat = time.monotonic()
                yield record
            if cursor >= page.event_count and page.status in TERMINAL:
                return
            if time.monotonic() - heartbeat >= 10:
                heartbeat = time.monotonic()
                yield None
            await asyncio.sleep(0.02)

    async def cancel(self, run_id: str) -> RunView:
        self._external()
        return await settled(self._cancel(run_id))

    async def _cancel(self, run_id: str) -> RunView:
        task = self.tasks.get(run_id)
        error: ServiceError | None = None
        try:
            # Persist stop before joining: parking may commit while the owner
            # is finalizing, but cannot discard this request or resume the run.
            await self.store.request_stop(run_id)
        except ServiceError as failure:
            error = failure
        finally:
            # Approval may replace the parked producer while request_stop waits.
            task = self.tasks.get(run_id, task)
            if task is not None:
                if (
                    not task.done()
                    and run_id not in self._cancelled
                    and run_id not in self._finalizing
                ):
                    self._cancelled.add(run_id)
                    task.cancel()
                await settled(task)
        if error is not None:
            raise error
        snapshot = await self.store.snapshot(run_id)
        if snapshot.run.status == "awaiting":
            binding = self.bindings.get(snapshot.request.target)
            if binding is not None and isinstance(binding.loop, DevelopmentSDKLoop):
                await binding.loop.development_cleanup(run_id)
            if binding is not None and binding.loop.sandbox is not None:
                from krim_agent.sandbox.tasks import cancel_run_tasks

                await cancel_run_tasks(binding.loop.sandbox.service, run_id)
            return await self.store.finish(run_id, "aborted", "cancelled")
        return snapshot.run

    async def _approval_request(
        self, invocation: Invocation, budget_remaining: float | None
    ) -> dict[str, JsonValue]:
        """构造 approval_request 事件内容（ADR-0015 停靠回调，不再进程内等待）。

        溯源与 workspace WorkLimits 校验沿用 ADR-0014 语义；expires_at 由
        引擎按整批同刻原则收紧（取 min）。budget_remaining 为停靠时刻的
        执行预算剩余，随事件落库供恢复核对。
        """
        snapshot = await self.store.snapshot(invocation.run_id)
        provenance: dict[str, JsonValue] = {}
        declared = snapshot.target.get("tools")
        if isinstance(declared, list):
            for spec in declared:
                if not isinstance(spec, dict) or spec.get("name") != invocation.name:
                    continue
                if fingerprint(spec) != invocation.tool_hash:
                    raise ServiceError("target_binding_changed")
                if "risk" in spec:
                    provenance["risk"] = spec["risk"]
                if isinstance(spec.get("source"), dict):
                    provenance["source"] = spec["source"]
                break
        expires: float | None
        if snapshot.request.target == "target-workspace":
            binding = self.bindings.get("target-workspace")
            if snapshot.work is None or binding is None or binding.loop.work is None:
                raise ServiceError("workspace_contract_required")
            binding.assert_current()
            if encoded({key: value for key,
                value in snapshot.target.items() if key != "context"}) != binding.descriptor_json:
                raise ServiceError("target_binding_changed")
            frozen = cast(dict[str, JsonValue], snapshot.work.conditions["runtime"])
            expected = binding.loop.work.profile.runtime.model_dump(mode="json")
            from krim_agent.workspace.types import WorkLimits
            try:
                selected = WorkLimits.model_validate_json(
                    encoded({key: frozen.get(key) for key in expected}))
                reference = WorkLimits.model_validate_json(
                    binding.loop.work.profile.runtime.model_dump_json())
            except ValueError:
                raise ServiceError("target_binding_changed") from None
            if selected != reference:
                raise ServiceError("target_binding_changed")
            expires = time.time() + selected.approval_timeout_seconds
        else:
            binding = self.bindings.get(snapshot.request.target)
            if binding is None:
                raise ServiceError("target_binding_changed")
            timeout: float | None = binding.settings.approval_timeout
            # timeout=None 表示取消产品审批倒计时；批复与取消仍是唯一定稿路径。
            expires = None if timeout is None else time.time() + timeout
        data: dict[str, JsonValue] = {
            "request_id": secrets.token_hex(16),
            "run_id": invocation.run_id,
            "call_id": invocation.call_id,
            "step_index": invocation.step_index,
            "arguments_hash": fingerprint(parse_json(invocation.arguments_json)),
            "tool_hash": invocation.tool_hash,
            "arguments": parse_json(invocation.arguments_json),
            "name": invocation.name,
            "expires_at": expires,
            "budget_remaining": budget_remaining,
            **provenance,
        }
        selected_binding = self.bindings.get(snapshot.request.target)
        if selected_binding is not None and isinstance(selected_binding.loop, DevelopmentSDKLoop):
            projection = selected_binding.loop.development_approval_projection
            if projection is not None:
                data = await projection(invocation, data)
        return data

    def _subscribe_execution_policy(self) -> None:
        """订阅会话执行模式变化；回调只登记重评任务，不把通知当事实。"""
        try:
            from krim_agent.sessions.execution_policy import execution_policy_notifications
        except ImportError:
            return  # sessions.execution_policy 未装配：唤醒只走显式入口
        self._policy_unsubscribe = execution_policy_notifications.subscribe(
            self._on_execution_policy_change
        )

    def _on_execution_policy_change(self, change: Any) -> None:
        session_id = getattr(change, "session_id", None)
        if self.closed or not isinstance(session_id, str) or not session_id:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        task = loop.create_task(self._execution_policy_wake(session_id))
        task.add_done_callback(RunService._consume_wake_task)

    @staticmethod
    def _consume_wake_task(task: asyncio.Task[None]) -> None:
        if not task.cancelled():
            task.exception()  # 通知驱动的重评失败不传播给发布方；批复路径仍权威。

    async def _execution_policy_wake(self, session_id: str) -> None:
        try:
            await self.execution_policy_changed(session_id)
        except asyncio.CancelledError:
            raise
        except Exception:
            pass  # 唤醒失败不影响停靠事实；下一次通知、批复或取消仍可定稿。

    async def execution_policy_changed(self, session_id: str) -> int:
        """会话模式提交后的重评入口：重读持久策略并放行满足条件的停靠审批。

        通知不是事实来源。对每个未答复的审批请求按当前项目访问、会话执行
        模式与本会话允许规则重新判定：allow 记录独立 execution_policy_allow
        事件（不写 approval_reply，不伪称用户批准），等待满足后沿现有停靠
        恢复路径续跑；ask 维持待答，deny/过期窗口/身份或工具绑定不匹配一律
        不放行。运行中的调用不受影响，用户拒绝过的调用不复活。返回唤醒的
        运行数；未注入 recheck 或服务已关闭时不做任何事。
        """
        if self.closed or self.execution_policy_recheck is None:
            return 0
        resumed = 0
        async with self.input_gate, self._approval_lock:
            # 停靠运行的候选：本进程tasks + 存储中该会话绑定的awaiting运行
            # （服务重启后旧停靠运行没有task，仍可被模式切换唤醒）。
            candidates: set[str] = set(self.tasks)
            from krim_agent.storage.models import runs, turns

            async def bound_runs(conn: Any) -> list[str]:
                return [
                    row[0] for row in (await conn.execute(
                        select(runs.c.id)
                        .select_from(turns.join(runs, turns.c.run_id == runs.c.id))
                        .where(turns.c.session_id == session_id,
                               runs.c.status == "awaiting")
                    )).all()
                ]

            try:
                candidates.update(await self.store.work_read(bound_runs))
            except ServiceError:
                pass  # 存储不可用时只扫进程内候选，下次通知/批复仍可恢复
            for run_id in candidates:
                try:
                    resumed += await self._policy_wake_run(run_id, session_id)
                except ServiceError:
                    continue  # 运行同时被定稿/取消：以现有状态机为准
        return resumed

    async def reconcile_execution_policies(self) -> int:
        """全部装配完成后的批量重评：重读持久策略并恢复已满足的停靠审批。

        供composition在初始化末尾调用一次，覆盖重启前遗留的awaiting运行。
        只做模式放行的内部恢复决定（独立事件），不做用户批复，不伪记
        approval_reply；无recheck或服务已关闭时不做任何事。返回恢复数。
        """
        if self.closed or self.execution_policy_recheck is None:
            return 0
        resumed = 0
        async with self.input_gate, self._approval_lock:
            from krim_agent.storage.models import runs

            async def awaiting_runs(conn: Any) -> list[str]:
                return [
                    row[0] for row in (await conn.execute(
                        select(runs.c.id).where(runs.c.status == "awaiting")
                    )).all()
                ]

            for run_id in await self.store.work_read(awaiting_runs):
                try:
                    snapshot = await self.store.snapshot(run_id)
                except ServiceError:
                    continue
                session_id = getattr(snapshot.session_context, "session_id", None)
                if not isinstance(session_id, str) or not session_id:
                    continue  # 非会话运行没有执行模式
                try:
                    resumed += await self._policy_wake_run(run_id, session_id)
                except ServiceError:
                    continue
        return resumed

    async def _policy_wake_run(self, run_id: str, session_id: str) -> int:
        snapshot = await self.store.snapshot(run_id)
        if (snapshot.run.status != "awaiting" or snapshot.run.stop_requested
                or getattr(snapshot.session_context, "session_id", None) != session_id):
            return 0
        # 目标绑定loop持有运行的真实工具集（shell/write等在development绑定上）；
        # base loop registry通常只有普通工具，查不到会让热切换永不唤醒。
        registry = self.loop.executor.registry
        binding = self.bindings.get(snapshot.request.target)
        if binding is not None:
            registry = binding.loop.executor.registry
        for request in self._unanswered_requests(snapshot):
            if not await self._policy_allow_request(
                session_id, run_id, request, registry,
            ):
                continue
            await self.store.append(
                run_id,
                "execution_policy_allow",
                {
                    "request_id": str(request["request_id"]),
                    "call_id": str(request["call_id"]),
                    "span_id": "execution-policy-" + str(request["request_id"]),
                    "step_index": cast(int, request["step_index"]),
                    "name": str(request["name"]),
                    "tool_hash": str(request["tool_hash"]),
                    "arguments_hash": str(request["arguments_hash"]),
                    "session_id": session_id,
                    "reason": "session_mode",
                },
                event_id="execution-policy-allow:" + str(request["request_id"]),
            )
        # 先前崩溃在trigger_resume前的运行：事件已在账本，此处补恢复，
        # 不因本轮无新增放行而卡死（_trigger_resume幂等，running不动）。
        snapshot = await self.store.snapshot(run_id)
        if self._all_waits_answered(snapshot):
            return 1 if self._trigger_resume(run_id, snapshot) else 0
        return 0

    async def _policy_allow_request(
        self, session_id: str, run_id: str, request: dict[str, JsonValue],
        registry: ToolRegistry,
    ) -> bool:
        expires = request.get("expires_at")
        if isinstance(expires, (int, float)) and not isinstance(expires, bool):
            if expires <= time.time():
                return False  # 过期确认窗口不复活；仍可由用户按现状批复或取消
        assert self.execution_policy_recheck is not None
        spec = self._ledger_spec(request, registry)
        invocation = self._ledger_invocation(run_id, request)
        if spec is None or invocation is None:
            return False  # 工具身份或参数摘要无法核对：宁可继续等待，不猜着放行
        try:
            decision = await self.execution_policy_recheck(session_id, invocation, spec)
        except ServiceError:
            return False
        return decision == "allow"

    def _ledger_spec(
        self, request: dict[str, JsonValue], registry: ToolRegistry,
    ) -> ToolSpec | None:
        name, tool_hash = request.get("name"), request.get("tool_hash")
        if not isinstance(name, str) or not isinstance(tool_hash, str):
            return None
        try:
            spec = registry.get(name)
        except ServiceError:
            return None
        return spec if spec.fingerprint == tool_hash else None

    def _ledger_invocation(
        self, run_id: str, request: dict[str, JsonValue],
    ) -> Invocation | None:
        arguments = request.get("arguments")
        call_id, request_id = request.get("call_id"), request.get("request_id")
        step_index = request.get("step_index")
        if (not isinstance(arguments, dict) or not isinstance(call_id, str)
                or not isinstance(request_id, str) or type(step_index) is not int):
            return None
        if fingerprint(arguments) != request.get("arguments_hash"):
            return None
        return Invocation(
            run_id=run_id,
            call_id=call_id,
            span_id="execution-policy-" + request_id,
            step_index=step_index,
            name=str(request["name"]),
            arguments_json=encoded(arguments),
            tool_hash=str(request["tool_hash"]),
        )

    def _unanswered_requests(self, snapshot: RunSnapshot) -> list[dict[str, JsonValue]]:
        """快照里尚未答复的审批请求（不做过期判断，过期由答复路径处理）。

        execution_policy_allow 事件是模式放行的内部恢复决定：同一请求不再
        等待用户 approval_reply，也不会被重复放行（事件按request_id幂等）。
        """
        answered = {
            str(event.data["request_id"])
            for event in snapshot.events
            if event.kind == "approval_reply"
        }
        policy_allowed = {
            str(event.data["request_id"])
            for event in snapshot.events
            if event.kind == "execution_policy_allow"
            and isinstance(event.data.get("request_id"), str)
        }
        return [
            dict(event.data)
            for event in snapshot.events
            if event.kind == "approval_request"
            and str(event.data["request_id"]) not in answered
            and str(event.data["request_id"]) not in policy_allowed
        ]

    def _all_waits_answered(self, snapshot: RunSnapshot) -> bool:
        if self._unanswered_requests(snapshot):
            return False
        replies = {event.data.get("request_id") for event in snapshot.events
                   if event.kind == "user_input_reply"}
        return not any(event.kind == "user_input_request"
                       and event.data.get("request_id") not in replies
                       for event in snapshot.events)

    async def reconcile_inputs(self) -> None:
        """The session dispatcher reconciles durable answers at its admission fence."""
        if self.interactions is None or self.closed:
            return
        async with self.input_gate, self._input_lock:
            await self._reconcile_inputs()

    async def _reconcile_inputs(self) -> None:
        from krim_agent.storage.interaction_models import interaction_requests
        from krim_agent.storage.models import runs

        async def read(conn: Any) -> list[dict[str, Any]]:
            return [dict(row) for row in (await conn.execute(
                select(interaction_requests).join(
                    runs, interaction_requests.c.run_id == runs.c.id,
                ).where(runs.c.status == "awaiting")
            )).mappings().all()]

        rows = await self.store.work_read(read)
        run_ids: set[str] = set()
        for row in rows:
            if row["status"] == "pending":
                continue
            if row["status"] == "superseded":
                await self.cancel(row["run_id"])
                continue
            snapshot = await self.store.snapshot(row["run_id"])
            if snapshot.run.stop_requested or snapshot.run.status != "awaiting":
                continue
            request = next((event.data for event in snapshot.events
                            if event.kind == "user_input_request"
                            and event.data.get("request_id") == row["id"]), None)
            if request is None:
                continue
            if not any(event.kind == "user_input_reply"
                       and event.data.get("request_id") == row["id"]
                       for event in snapshot.events):
                await self.store.append(row["run_id"], "user_input_reply", {
                    "request_id": row["id"], "status": row["status"],
                }, event_id="user-input-reply:" + row["id"])
            run_ids.add(row["run_id"])
        for run_id in run_ids:
            snapshot = await self.store.snapshot(run_id)
            if self._all_waits_answered(snapshot):
                self._trigger_resume(run_id, snapshot)

    async def input_answered(self, _result: dict[str, Any]) -> None:
        # No direct resume here: new-user-message admission and reconciliation
        # share the SessionService dispatcher fence, including notification races.
        self.slot_changed.set()

    def _trigger_resume(self, run_id: str, snapshot: RunSnapshot) -> bool:
        """停靠批全部答复后启动恢复驱动；已有驱动在跑则不动（幂等）。"""
        if (snapshot.run.stop_requested or snapshot.run.status != "awaiting"
                or not self._all_waits_answered(snapshot)):
            return False
        task = self.tasks.get(run_id)
        if task is not None and not task.done():
            return False
        binding = self.bindings.get(snapshot.request.target)
        if binding is None:
            return False  # 存储的目标键已不存在：留给 recover 定稿，不猜着续跑
        resumed = asyncio.create_task(
            self._drive(snapshot.run, binding, resume=True), name="krim-run-" + run_id
        )
        self.tasks[run_id] = resumed
        resumed.add_done_callback(self._task_done)
        resumed.add_done_callback(lambda _task: self.slot_changed.set())
        return True

    async def approvals(self, run_id: str) -> list[dict[str, JsonValue]]:
        snapshot = await self.store.snapshot(run_id)
        consumed = {
            event.data["request_id"] for event in snapshot.events if event.kind == "approval_reply"
        }
        consumed |= {
            event.data["request_id"] for event in snapshot.events
            if event.kind == "execution_policy_allow"
            and isinstance(event.data.get("request_id"), str)
        }
        if snapshot.run.status != "awaiting" or snapshot.run.stop_requested:
            return []

        def unexpired(data: dict[str, JsonValue]) -> bool:
            expires = data.get("expires_at")
            if isinstance(expires, (int, float)) and not isinstance(expires, bool):
                return expires > time.time()
            return True  # expires_at=None：无产品倒计时的请求只按批复/取消定稿

        return [
            dict(event.data)
            for event in snapshot.events
            if event.kind == "approval_request"
            and event.data["request_id"] not in consumed
            and unexpired(event.data)
        ]

    async def reply_approval(
        self, run_id: str, request_id: str, reply: ApprovalReply
    ) -> dict[str, JsonValue]:
        # 保护整个提交与交付，而不只保护存储事务，避免持久决定遗留在原 future 之外。
        result = await settled(self._reply_approval(run_id, request_id, reply))
        if reply.allow and reply.scope == "session" and result.get("replayed") is False:
            snapshot = await self.store.snapshot(run_id)
            session_id = getattr(snapshot.session_context, "session_id", None)
            if isinstance(session_id, str):
                # Reevaluate other already-parked calls under the new exact rule.
                # The reply transaction/approval lock has completed at this point.
                await settled(self.execution_policy_changed(session_id))
        return result

    async def _reply_approval(
        self, run_id: str, request_id: str, reply: ApprovalReply
    ) -> dict[str, JsonValue]:
        async with self.input_gate, self._approval_lock:
            snapshot = await self.store.snapshot(run_id)
            request = next(
                (
                    event.data
                    for event in snapshot.events
                    if event.kind == "approval_request" and event.data["request_id"] == request_id
                ),
                None,
            )
            if request is None or any(
                request[key] != getattr(reply, key)
                for key in ("call_id", "arguments_hash", "tool_hash")
            ):
                raise ServiceError("approval_conflict")
            previous = next(
                (
                    event.data
                    for event in snapshot.events
                    if event.kind == "approval_reply" and event.data["request_id"] == request_id
                ),
                None,
            )
            body = reply.model_dump(mode="json")
            if previous is not None:
                raw = previous.get("reply")
                prior = dict(raw) if isinstance(raw, dict) else {}
                prior.setdefault("scope", "once")  # 旧账本无scope字段，按缺省once比对
                if prior != body:
                    raise ServiceError("approval_conflict")
                # 重放只确认原结果，不补写规则：用户清除后旧批复重放不得复活。
                if snapshot.run.status == "awaiting" and self._all_waits_answered(snapshot):
                    # 幂等重放也能补触发恢复（首次答复后恢复启动失败的场景）
                    self._trigger_resume(run_id, snapshot)
                return {"request_id": request_id, "allow": reply.allow, "replayed": True}
            if snapshot.run.status in TERMINAL or snapshot.run.stop_requested:
                raise ServiceError("approval_unavailable")
            if snapshot.run.status != "awaiting":
                # 事件已写但状态未及转停靠（进程在两者之间崩溃）：不猜着续跑
                raise ServiceError("approval_unavailable")
            expires = request.get("expires_at")
            if (isinstance(expires, (int, float)) and not isinstance(expires, bool)
                    and expires <= time.time()):
                # 过期惰性定稿：同停靠批同刻到期，过期即整批不可再批复（ADR-0015）；
                # 无产品倒计时（None）的请求没有此定稿路径。
                await self.store.finish(run_id, "failed", "approval_expired")
                raise ServiceError("approval_expired")
            from krim_agent.storage.execution_policy import (
                record_allow_rule,
                session_allow_rule_context,
            )
            from krim_agent.storage.sqlite import SQLiteStore

            store = cast(SQLiteStore, self.store)
            session_id = getattr(snapshot.session_context, "session_id", None)
            if reply.scope == "session":
                tool_name = request.get("name")
                if (
                    not isinstance(session_id, str)
                    or not session_id
                    or not isinstance(tool_name, str)
                    or not tool_name
                    or await session_allow_rule_context(store, session_id) is None
                ):
                    # 无会话或无可承接的项目上下文：整体拒绝，不落批复也不降级。
                    raise ServiceError("permission_denied", 403)
            frozen = cast(dict[str, JsonValue], {"request_id": request_id, "reply": body})
            event_id = "approval:" + request_id

            async def commit(conn: Any) -> dict[str, JsonValue]:
                # 批复事件与会话范围精确规则同一事务：规则随批复定稿，不存在
                # "批复成了、规则没落"的中间态需要重放补偿。
                await store._event(conn, run_id, "approval_reply", frozen, event_id)
                if reply.scope == "session" and isinstance(session_id, str):
                    await record_allow_rule(
                        conn,
                        session_id,
                        tool_name=str(request.get("name", "")),
                        tool_hash=str(request.get("tool_hash", "")),
                        arguments_hash=str(request.get("arguments_hash", "")),
                    )
                return {"request_id": request_id, "allow": reply.allow, "replayed": False}

            async def confirm() -> dict[str, JsonValue] | None:
                # COMMIT回程未知：按原event_id与事件体核实是否已提交。
                try:
                    verify = await store.snapshot(run_id)
                except Exception:
                    return None
                found = next(
                    (event for event in verify.events if event.event_id == event_id), None
                )
                if found is None or found.kind != "approval_reply" or found.data != frozen:
                    return None
                return {"request_id": request_id, "allow": reply.allow, "replayed": False}

            if reply.scope == "session":
                await store._eval_write(commit, confirm)
            else:
                # Preserve the generic RunStore and its append/commit recovery
                # seam for ordinary one-shot approvals, including legacy hosts.
                await self.store.append(run_id, "approval_reply", frozen, event_id=event_id)
            snapshot = await self.store.snapshot(run_id)
            if self._all_waits_answered(snapshot):  # Both approvals and user answers are required.
                self._trigger_resume(run_id, snapshot)
            return {"request_id": request_id, "allow": reply.allow, "replayed": False}

    async def close(self) -> None:
        self._external()
        if self._close_task is None:
            self._close_task = asyncio.create_task(self._close())
        await settled(self._close_task)

    async def _close(self) -> None:
        unsubscribe, self._policy_unsubscribe = self._policy_unsubscribe, None
        if unsubscribe is not None:
            unsubscribe()
        async with self._admission:
            self.closed = True
        outcomes = await asyncio.gather(
            *(self.cancel(run_id) for run_id, task in list(self.tasks.items()) if not task.done()),
            return_exceptions=True,
        )
        # 必须等全部 producer 清理完毕，Application 才能关闭共享资源和释放写端锁。
        for outcome in outcomes:
            if isinstance(outcome, BaseException):
                raise outcome
