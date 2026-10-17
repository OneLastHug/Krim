"""唯一装配点；交互与有限Eval借用同一RunService、SQLite和资源所有权。"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from pathlib import Path
from types import TracebackType
from typing import cast

from pydantic import JsonValue

from krim_agent.config import Settings
from krim_agent.evaluation.service import EvaluationService
from krim_agent.evaluation.targets import make_eval_bindings
from krim_agent.providers.fake import FakeProvider
from krim_agent.providers.types import ModelProvider
from krim_agent.runtime.contracts import settled
from krim_agent.runtime.engine import AgentLoop
from krim_agent.runtime.service import RunBinding, RunService
from krim_agent.storage.sqlite import SQLiteStore
from krim_agent.tools.builtins import builtin_registry, demo_policy
from krim_agent.tools.core import Policy, PostHook, PreHook
from krim_agent.tools.executor import ToolExecutor
from krim_agent.tools.registry import ToolRegistry


@dataclass
class Application:
    store: SQLiteStore
    service: RunService
    provider: ModelProvider
    settings: Settings
    owns_provider: bool
    owned_providers: tuple[ModelProvider, ...] = ()
    evaluations: EvaluationService | None = None
    _close_task: asyncio.Task[None] | None = field(default=None, init=False)

    async def __aenter__(self) -> Application:
        return self

    async def __aexit__(
        self,
        kind: type[BaseException] | None,
        value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.close()

    async def close(self) -> None:
        self.service._external()
        if self.evaluations is not None:
            self.evaluations._external()
        if self._close_task is None:
            self._close_task = asyncio.create_task(self._close())
        await settled(self._close_task)

    async def _close(self) -> None:
        try:
            # 先关闭两个准入门；即使Eval写失败，所有Run仍必须被真实join。
            async with self.service._admission:
                self.service.closed = True
            try:
                if self.evaluations is not None:
                    await self.evaluations.close()
            finally:
                await self.service.close()
        finally:
            try:
                outcomes = await asyncio.gather(
                    *(p.close() for p in self.owned_providers), return_exceptions=True
                )
                for outcome in outcomes:
                    if isinstance(outcome, BaseException):
                        raise outcome
            finally:
                await self.store.close()


async def open_application(
    path: Path,
    *,
    settings: Settings | None = None,
    provider: ModelProvider | None = None,
    registry: ToolRegistry | None = None,
    policy: Policy | None = demo_policy,
    pre: PreHook | None = None,
    post: PostHook | None = None,
) -> Application:
    config = settings if settings is not None else Settings(database=path)
    selected = provider if provider is not None else FakeProvider()
    tools = registry if registry is not None else builtin_registry()
    store = await SQLiteStore.open(path, max_events=config.max_events)
    try:
        await store.recover()
        executor = ToolExecutor(tools, policy, pre, post)
        loop = AgentLoop(store, selected, executor, config)
        target: dict[str, JsonValue] = {
            "version": config.target_version,
            "system_prompt": config.system_prompt,
            "tools": cast(JsonValue, tools.schemas()),
            "limits": config.model_dump(
                mode="json", exclude={"database", "api_key", "system_prompt"}
            ),
        }
        bindings = make_eval_bindings(store, config)
        bindings["demo"] = RunBinding.freeze("demo", loop, config)
        service = RunService(store, loop, config, target, bindings)
        owned = [selected] if provider is None else []
        owned.extend(b.loop.provider for key, b in bindings.items() if key != "demo")
        return Application(
            store,
            service,
            selected,
            config,
            provider is None,
            tuple({id(p): p for p in owned}.values()),
            EvaluationService(store, service),
        )
    except BaseException:
        await store.close()
        if provider is None:
            await selected.close()
        raise
