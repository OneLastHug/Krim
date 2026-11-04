"""Run 生命周期和审批；内存任务只调度，持久状态来自 RunStore。"""

from __future__ import annotations

import asyncio
import inspect
import secrets
import time
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import cast

from pydantic import JsonValue

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
from krim_agent.tools.core import Invocation
from krim_agent.tools.pipeline import _owns, _producing

from . import model_step as model_step_module
from .contracts import ExecutionBudget, RunStore, settled
from .engine import AgentLoop
from .facts import project_facts


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
    identities: tuple[int, ...]
    tool_identities: tuple[int, ...]
    _http_token: tuple[object, ...] | None = field(default=None, repr=False)

    @staticmethod
    def runtime_identities(loop: AgentLoop) -> tuple[int, ...]:
        def function(value: object) -> object:
            return getattr(value, "__func__", value)

        result = tuple(
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
            result += tuple(map(id, (loop.work.profile, loop.work.artifacts, loop.work.runner,
                loop.work.access_policy)))
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
            ExecutionBudget.deadline.fget,
            engine_module.AgentLoop.run,
            model_step_module.ModelStep.call,
            executor_module.ToolExecutor.execute,
            pipeline_module.Scope.deadline.fget,
            operations_module.WorkspaceOperations.scope,
            RunService._drive,
            RunService._approve,
        )))
        result += tuple(source_digest(module.__file__) for module in (
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
        policy = implementation(getattr(loop.executor.policy, "__func__",
            loop.executor.policy)) if loop.work is not None else implementation(
            loop.executor.policy)
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
        self.loop.executor.registry.assert_current()
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


class RunService:
    def __init__(
        self,
        store: RunStore,
        loop: AgentLoop,
        settings: Settings,
        target: dict[str, JsonValue],
        bindings: dict[str, RunBinding] | None = None,
    ) -> None:
        self.store, self.loop, self.settings = store, loop, settings
        self._target_json = encoded(target)
        self.bindings = MappingProxyType(
            bindings
            if bindings is not None
            else {"demo": RunBinding.freeze("demo", loop, settings)}
        )
        self.tasks: dict[str, asyncio.Task[None]] = {}
        self._admission = asyncio.Lock()
        self._approval_lock = asyncio.Lock()
        self._pending: dict[str, asyncio.Future[bool]] = {}
        self._execution_budgets: dict[str, ExecutionBudget] = {}
        self._cancelled: set[str] = set()
        self._finalizing: set[str] = set()
        self._close_task: asyncio.Task[None] | None = None
        self.closed = False
        self.slot_changed = asyncio.Event()

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
        if len(request.text.encode()) > self.settings.max_input_bytes:
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
        if expected is not None and fingerprint(parse_json(binding.descriptor_json)) != expected:
            raise ServiceError("target_binding_changed")
        if len(request.text.encode()) > binding.settings.max_input_bytes:
            raise ServiceError("input_limit", 422)
        if (
            existing is None
            and sum(not t.done() for t in self.tasks.values()) >= self.settings.max_active_runs
        ):
            raise ServiceError("capacity", 429)
        descriptor = cast(dict[str, JsonValue], parse_json(binding.descriptor_json))
        executing: RunBinding | TurnBinding = binding
        if turn_id is None:
            run, created = await self.store.begin(request, descriptor)
        else:
            executing = TurnBinding(binding, builder.binding_token())
            run, created = await self.store.begin_turn_run(
                turn_id, request, descriptor, binding.settings
            )
        if created:
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

    async def _drive(self, run: RunView, binding: RunBinding | TurnBinding) -> None:
        producer = self.tasks.get(run.id)
        if producer is None or producer is not asyncio.current_task():
            raise ServiceError("self_wait")
        with _producing(self, run.id, cast(asyncio.Task[None], producer)):
            status: RunStatus = "completed"
            code: str | None = None
            budget: ExecutionBudget | None = None
            try:
                timeout = (
                    binding.loop.work.profile.runtime.run_timeout_seconds
                    if binding.loop.work is not None else binding.settings.run_timeout
                )
                deadline = asyncio.get_running_loop().time() + max(
                    0, run.created_at + timeout - time.time())
                budget = ExecutionBudget(deadline)
                self._execution_budgets[run.id] = budget
                timeout_context = asyncio.timeout_at(deadline)
                budget.bind(timeout_context)
                async with timeout_context:
                    await self.store.set_running(run.id)
                    await binding.loop.run(run.id, self._approve, binding.assert_current,
                                           execution_budget=budget)
            except asyncio.CancelledError:
                status, code = "aborted", "cancelled"
            except TimeoutError:
                status, code = "failed", "run_timeout"
            except ServiceError as error:
                status, code = "failed", error.code
            except Exception:
                status, code = "failed", "execution_error"
            finally:
                if budget is not None:
                    budget.finish()
                    self._execution_budgets.pop(run.id, None)
                self._finalizing.add(run.id)
                try:
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
        if task is None:
            return await self.get(run_id)
        error: ServiceError | None = None
        try:
            # 内存 producer 的所有权不依赖读库成功；失败也必须进入清理。
            await self.get(run_id)
            await self.store.request_stop(run_id)
        except ServiceError as failure:
            error = failure
        finally:
            if not task.done() and run_id not in self._cancelled and run_id not in self._finalizing:
                self._cancelled.add(run_id)
                task.cancel()
            await settled(task)
        if error is not None:
            raise error
        return await self.get(run_id)

    async def _approve(self, invocation: Invocation, approval_window_seconds: float) -> bool:
        snapshot = await self.store.snapshot(invocation.run_id)
        request_id = secrets.token_hex(16)
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
            now = time.time()
            expires = now + max(0, min(
                approval_window_seconds, selected.approval_timeout_seconds))
            if expires <= now:
                raise ServiceError("approval_expired")
        else:
            binding = self.bindings.get(snapshot.request.target)
            if binding is None:
                raise ServiceError("target_binding_changed")
            expires = time.time() + min(
                approval_window_seconds, binding.settings.approval_timeout)
        future: asyncio.Future[bool] = asyncio.get_running_loop().create_future()
        self._pending[request_id] = future
        try:
            await self.store.append(
                invocation.run_id,
                "approval_request",
                {
                    "request_id": request_id,
                    "run_id": invocation.run_id,
                    "call_id": invocation.call_id,
                    "arguments_hash": fingerprint(parse_json(invocation.arguments_json)),
                    "tool_hash": invocation.tool_hash,
                    "arguments": parse_json(invocation.arguments_json),
                    "name": invocation.name,
                    "expires_at": expires,
                    **provenance,
                },
            )
            try:
                budget = getattr(self, "_execution_budgets", {}).get(invocation.run_id)
                if budget is None:
                    return await asyncio.wait_for(future, max(0, expires - time.time()))
                with budget.pause():
                    return await asyncio.wait_for(future, max(0, expires - time.time()))
            except TimeoutError:
                raise ServiceError("approval_expired") from None
        finally:
            self._pending.pop(request_id, None)
            if not future.done():
                future.cancel()

    async def approvals(self, run_id: str) -> list[dict[str, JsonValue]]:
        snapshot = await self.store.snapshot(run_id)
        consumed = {
            event.data["request_id"] for event in snapshot.events if event.kind == "approval_reply"
        }
        if snapshot.run.status in TERMINAL:
            return []
        return [
            dict(event.data)
            for event in snapshot.events
            if event.kind == "approval_request"
            and event.data["request_id"] not in consumed
            and cast(float, event.data["expires_at"]) > time.time()
        ]

    async def reply_approval(
        self, run_id: str, request_id: str, reply: ApprovalReply
    ) -> dict[str, JsonValue]:
        # 保护整个提交与交付，而不只保护存储事务，避免持久决定遗留在原 future 之外。
        return await settled(self._reply_approval(run_id, request_id, reply))

    async def _reply_approval(
        self, run_id: str, request_id: str, reply: ApprovalReply
    ) -> dict[str, JsonValue]:
        async with self._approval_lock:
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
                if previous.get("reply") != body:
                    raise ServiceError("approval_conflict")
                return {"request_id": request_id, "allow": reply.allow, "replayed": True}
            if snapshot.run.status in TERMINAL:
                raise ServiceError("approval_unavailable")
            future = self._pending.get(request_id)
            if cast(float, request["expires_at"]) <= time.time() or future is None or future.done():
                raise ServiceError("approval_expired")
            await self.store.append(
                run_id,
                "approval_reply",
                {"request_id": request_id, "reply": body},
                event_id="approval:" + request_id,
            )
            if future.done() or cast(float, request["expires_at"]) <= time.time():
                raise ServiceError("approval_expired")
            future.set_result(reply.allow)
            return {"request_id": request_id, "allow": reply.allow, "replayed": False}

    async def close(self) -> None:
        self._external()
        if self._close_task is None:
            self._close_task = asyncio.create_task(self._close())
        await settled(self._close_task)

    async def _close(self) -> None:
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
