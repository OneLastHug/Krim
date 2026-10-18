"""Run 生命周期和审批；内存任务只调度，持久状态来自 RunStore。"""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import secrets
import time
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import cast

from pydantic import JsonValue

from krim_agent.config import Settings
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
from krim_agent.tools.core import Invocation

from .contracts import RunStore, settled
from .engine import AgentLoop
from .facts import project_facts


def implementation(value: object | None) -> dict[str, JsonValue]:
    if value is None:
        return {"id": "none", "version": "1", "digest": fingerprint("none")}
    selected = value if inspect.isfunction(value) else type(value)
    path = inspect.getsourcefile(selected)
    digest = (
        hashlib.sha256(Path(path).read_bytes()).hexdigest()
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

        return tuple(
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

    @staticmethod
    def tool_tokens(loop: AgentLoop) -> tuple[int, ...]:
        return tuple(
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
        policy = implementation(loop.executor.policy)
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
            exclude={"database", "api_key", "system_prompt", "target_version", "provider"},
        )
        if isinstance(loop.provider, OpenAICompatibleProvider):
            limits["http"] = loop.provider.execution_limits()
        return {
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
        self._cancelled: set[str] = set()
        self._finalizing: set[str] = set()
        self._close_task: asyncio.Task[None] | None = None
        self.closed = False

    def _external(self) -> None:
        if asyncio.current_task() in self.tasks.values():
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

    async def _admit_locked(
        self, request: RunRequest, *, internal: bool = False, expected: str | None = None
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
        if request.request_key.startswith("lc-eval.") and not internal:
            raise ServiceError("invalid_input", 422)
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
        run, created = await self.store.begin(
            request, cast(dict[str, JsonValue], parse_json(binding.descriptor_json))
        )
        if created:
            self.tasks[run.id] = asyncio.create_task(
                self._drive(run, binding), name="krim-run-" + run.id
            )
            self.tasks[run.id].add_done_callback(self._task_done)
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

    async def _drive(self, run: RunView, binding: RunBinding) -> None:
        status: RunStatus = "completed"
        code: str | None = None
        try:
            remaining = max(0, run.created_at + binding.settings.run_timeout - time.time())
            async with asyncio.timeout(remaining):
                await self.store.set_running(run.id)
                await binding.loop.run(run.id, self._approve, binding.assert_current)
        except asyncio.CancelledError:
            status, code = "aborted", "cancelled"
        except TimeoutError:
            status, code = "failed", "run_timeout"
        except ServiceError as error:
            status, code = "failed", error.code
        except Exception:
            status, code = "failed", "execution_error"
        finally:
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
        snapshot = await self.store.snapshot(run_id)
        if from_seq < 0 or from_seq > len(snapshot.events) or not 1 <= limit <= 64:
            raise ServiceError("invalid_cursor", 416)
        return list(snapshot.events[from_seq : from_seq + limit])

    async def stream(self, run_id: str, from_seq: int = 0) -> AsyncIterator[RunEvent | None]:
        cursor = from_seq
        heartbeat = time.monotonic()
        while True:
            records = await self.events(run_id, cursor)
            for record in records:
                cursor = record.seq + 1
                heartbeat = time.monotonic()
                yield record
            snapshot = await self.store.snapshot(run_id)
            if cursor >= len(snapshot.events) and snapshot.run.status in TERMINAL:
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

    async def _approve(self, invocation: Invocation, remaining_seconds: float) -> bool:
        snapshot = await self.store.snapshot(invocation.run_id)
        request_id = secrets.token_hex(16)
        expires = min(
            time.time() + remaining_seconds,
            time.time() + self.settings.approval_timeout,
            snapshot.run.created_at + self.settings.run_timeout,
        )
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
                },
            )
            try:
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
