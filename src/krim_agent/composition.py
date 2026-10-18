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
from krim_agent.evaluation.targets import controlled_policy, make_eval_bindings
from krim_agent.providers.fake import FakeProvider
from krim_agent.providers.openai_compatible import OpenAICompatibleConfig, OpenAICompatibleProvider
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
    http_config: OpenAICompatibleConfig | None = None,
    pre: PreHook | None = None,
    post: PostHook | None = None,
) -> Application:
    config = settings if settings is not None else Settings(database=path)
    if http_config is not None:
        from krim_agent.providers.openai_compatible import ProviderError

        try:
            http_config = OpenAICompatibleConfig.model_validate(http_config.model_dump())
        except ValueError:
            raise ProviderError("provider_config", 422) from None
    tools = registry if registry is not None else builtin_registry()
    store = await SQLiteStore.open(path, max_events=config.max_events)
    owned: list[ModelProvider] = []
    try:
        await store.recover()
        selected = provider if provider is not None else FakeProvider()
        if provider is None:
            owned.append(selected)
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
        owned.extend(b.loop.provider for b in bindings.values())
        bindings["demo"] = RunBinding.freeze("demo", loop, config)
        if http_config is not None:
            http_provider = OpenAICompatibleProvider(
                http_config, max_connections=config.max_active_runs
            )
            owned.append(http_provider)
            bindings["target-http"] = _make_http_binding(store, config, http_provider)
        service = RunService(store, loop, config, target, bindings)
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
        try:
            outcomes = await settled(
                asyncio.gather(*(p.close() for p in owned), return_exceptions=True)
            )
        finally:
            await settled(store.close())
        for outcome in outcomes:
            if isinstance(outcome, BaseException):
                raise outcome from None
        raise


def _make_http_binding(
    store: SQLiteStore, settings: Settings, provider: OpenAICompatibleProvider
) -> RunBinding:
    config = settings.model_copy(
        update={
            "system_prompt": "使用受控工具回答任务；保持数值精度。",
            "target_version": "target-http/1",
            "max_input_bytes": min(4096, settings.max_input_bytes),
            "max_output_bytes": min(8192, settings.max_output_bytes),
            "max_request_bytes": min(32768, settings.max_request_bytes),
            "max_steps": min(4, settings.max_steps),
            "max_tools_per_step": 1,
            "max_tool_calls": 1,
            "run_timeout": min(10, settings.run_timeout),
            "tool_timeout": min(2, settings.tool_timeout),
        }
    )
    loop = AgentLoop(store, provider, ToolExecutor(builtin_registry(), controlled_policy), config)
    return RunBinding.freeze("target-http", loop, config)
