"""唯一装配点；交互与有限Eval借用同一RunService、SQLite和资源所有权。"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from copy import deepcopy
from dataclasses import dataclass, field
from pathlib import Path
from types import TracebackType
from typing import cast

from pydantic import JsonValue

from krim_agent.accounts.service import IdentityService
from krim_agent.config import SESSION_FIELDS, Settings
from krim_agent.contracts import ServiceError
from krim_agent.evaluation.adapters.krim import NeutralKrimAdapter
from krim_agent.evaluation.app import EvalApplication, open_eval
from krim_agent.evaluation.automation import EvaluationAutomation
from krim_agent.evaluation.ports import AdapterLease
from krim_agent.evaluation.product import EvaluationProduct
from krim_agent.evaluation.service import EvaluationService
from krim_agent.evaluation.targets import controlled_policy, make_eval_bindings
from krim_agent.execution.linux import SandboxRunner
from krim_agent.extensions import ExtensionService
from krim_agent.providers.fake import FakeProvider
from krim_agent.providers.openai_compatible import OpenAICompatibleConfig, OpenAICompatibleProvider
from krim_agent.providers.types import ModelProvider
from krim_agent.runtime.contracts import settled
from krim_agent.runtime.engine import AgentLoop
from krim_agent.runtime.service import RunBinding, RunService
from krim_agent.sandbox.configuration import SandboxConfiguration
from krim_agent.sandbox.operations import SandboxOperations
from krim_agent.sandbox.service import SandboxService
from krim_agent.security import LOCAL, AccessPolicy, PrincipalContext
from krim_agent.sessions.service import SessionService
from krim_agent.source_fingerprints import reset_source_fingerprints
from krim_agent.storage.sqlite import SQLiteStore
from krim_agent.tools.builtins import builtin_registry, demo_policy
from krim_agent.tools.core import Policy, PostHook, PreHook
from krim_agent.tools.executor import ToolExecutor
from krim_agent.tools.pipeline import Middleware, Wrap
from krim_agent.tools.registry import ToolRegistry
from krim_agent.tools.runtime import ToolRuntime, ToolRuntimeConfig
from krim_agent.tools.workspace import registry as workspace_registry
from krim_agent.workspace.artifacts import ArtifactStore
from krim_agent.workspace.operations import WorkspaceOperations
from krim_agent.workspace.targets import WorkspaceConfiguration


@dataclass
class Application:
    store: SQLiteStore
    service: RunService
    provider: ModelProvider
    settings: Settings
    owns_provider: bool
    owned_providers: tuple[ModelProvider, ...] = ()
    evaluations: EvaluationService | None = None
    sessions: SessionService | None = None
    access_policy: AccessPolicy | None = field(default=None, repr=False)
    _close_task: asyncio.Task[None] | None = field(default=None, init=False)
    work: WorkspaceOperations | None = None
    artifacts: ArtifactStore | None = None
    runner: SandboxRunner | None = None
    product_evaluations: EvalApplication | None = None
    evaluation_product: EvaluationProduct | None = None
    evaluation_automation: EvaluationAutomation | None = None
    tool_runtime: ToolRuntime | None = None
    identities: IdentityService | None = field(default=None, repr=False)
    sandbox: SandboxService | None = None
    extensions: ExtensionService | None = None

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
        if self.product_evaluations is not None:
            self.product_evaluations.kernel._external()
        if self._close_task is None:
            self._close_task = asyncio.create_task(self._close())
        await settled(self._close_task)

    async def _close(self) -> None:
        try:
            if self.evaluation_automation is not None:
                await self.evaluation_automation.close()
        finally:
            await self._close_runtime()

    async def _close_runtime(self) -> None:
        try:
            # 关闭运行准入；即使评测写失败，也等待所有Run实际收束。
            async with self.service._admission:
                self.service.closed = True
            try:
                try:
                    if self.product_evaluations is not None:
                        await self.product_evaluations.close()
                finally:
                    try:
                        if self.sessions is not None:
                            await self.sessions.close()
                    finally:
                        if self.evaluations is not None:
                            await self.evaluations.close()
            finally:
                await self.service.close()
        finally:
            try:
                try:
                    if self.work is not None:
                        await self.work.close()
                finally:
                    try:
                        if self.runner is not None:
                            await cast(Callable[[], Awaitable[None]], self.runner.close)()
                    finally:
                        if self.artifacts is not None:
                            await self.artifacts.close()
            finally:
                try:
                    try:
                        if self.tool_runtime is not None:
                            await self.tool_runtime.close()
                    finally:
                        outcomes = await asyncio.gather(
                            *(p.close() for p in self.owned_providers), return_exceptions=True
                        )
                        for outcome in outcomes:
                            if isinstance(outcome, BaseException):
                                raise outcome
                finally:
                    try:
                        try:
                            if self.sandbox is not None:
                                await self.sandbox.close()
                        finally:
                            if self.identities is not None:
                                await self.identities.close()
                    finally:
                        await self.store.close()


def _evaluation_database_path(database: Path, configured: Path | None) -> Path:
    # 在写端准入前固定可信启动路径，不把相对路径解析留到运行中。
    selected = (configured or database.with_name(database.stem + ".eval.sqlite")).absolute()
    if selected == database.absolute():
        raise ServiceError("invalid_input", 422)
    return selected


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
    access_policy: AccessPolicy | None = None,
    workspace_profile: WorkspaceConfiguration | None = None,
    workspace_middleware: tuple[Middleware, ...] = (),
    workspace_wrap: Wrap | None = None,
    tool_config: ToolRuntimeConfig | None = None,
) -> Application:
    from sqlalchemy import select

    from krim_agent.contracts import TERMINAL
    from krim_agent.providers.openai_compatible import ProviderError
    from krim_agent.storage.models import runs, session_controls, sessions, tool_effects, turns

    config = settings if settings is not None else Settings(database=path)
    if config.provider not in ("script", "http"):
        raise ProviderError("provider_config", 422)
    live = config.provider == "http"
    if config.http_mode not in ("bounded-demo", "agent") or (
        config.http_mode == "agent" and not live
    ):
        raise ProviderError("provider_config", 422)
    agent_http = live and config.http_mode == "agent"
    if config.evaluation_automation_enabled and not config.product_eval_enabled:
        raise ServiceError("invalid_input", 422)
    sandbox_configuration = (
        SandboxConfiguration.load(config.sandbox_config_file)
        if config.sandbox_config_file is not None
        else None
    )
    reset_source_fingerprints()
    if workspace_profile is not None and config.workspace_profile_file is not None:
        raise ServiceError("workspace_unavailable")
    workspace = workspace_profile or (
        WorkspaceConfiguration.load(config.workspace_profile_file)
        if config.workspace_profile_file is not None
        else None
    )
    if workspace is not None:
        workspace.__post_init__()
    raw_http_fields = set(http_config.model_fields_set) if http_config is not None else set()
    if isinstance(provider, OpenAICompatibleProvider):
        cap = (
            workspace.profile.max_tools_per_step
            if workspace is not None
            else (
                min(config.max_tools_per_step, provider.config.max_tool_calls) if agent_http else 1
            )
        )
        if provider.binding_token() != provider._sealed or provider.config.max_tool_calls != cap:
            raise ProviderError("provider_config", 422)
        if workspace is not None and http_config is not None:
            raise ProviderError("provider_config", 422)
    if http_config is not None:
        cap = (
            workspace.profile.max_tools_per_step
            if workspace is not None
            else (min(config.max_tools_per_step, 4) if agent_http else 1)
        )
        if "max_tool_calls" in raw_http_fields:
            if agent_http and workspace is None:
                if http_config.max_tool_calls > cap:
                    raise ProviderError("provider_config", 422)
                cap = http_config.max_tool_calls
            elif http_config.max_tool_calls != cap:
                raise ProviderError("provider_config", 422)
        if workspace is not None or agent_http:
            http_config = OpenAICompatibleConfig.model_validate(
                http_config.model_dump() | {"max_tool_calls": cap}
            )
    if (
        workspace is not None
        and http_config is None
        and not isinstance(provider, OpenAICompatibleProvider)
    ):
        raise ServiceError("workspace_unavailable")
    primary_settings = config
    if workspace is not None and isinstance(provider, OpenAICompatibleProvider) and not live:
        primary_settings = config.model_copy(update={"max_tools_per_step": 1, "max_tool_calls": 1})
    if live:
        specified = config.model_fields_set
        try:
            config = Settings.model_validate(config.model_dump())
        except ValueError:
            raise ProviderError("provider_config", 422) from None
        if provider is not None:
            if http_config is not None or not isinstance(provider, OpenAICompatibleProvider):
                raise ProviderError("provider_config", 422)
            http_config = provider.config
            if provider.binding_token() != provider._sealed:
                raise ProviderError("provider_config", 422)
        if http_config is None:
            raise ProviderError("provider_config", 422)
    if http_config is not None:
        try:
            http_config = OpenAICompatibleConfig.model_validate(http_config.model_dump())
        except ValueError:
            raise ProviderError("provider_config", 422) from None
    if agent_http:
        assert http_config is not None
        primary_settings = Settings.model_validate(
            config.model_dump()
            | {
                "system_prompt": config.system_prompt
                if "system_prompt" in specified
                else ("你是 Krim Agent。使用获授权工具完成用户任务；依据实际结果回答。"),
                "target_version": config.target_version
                if "target_version" in specified
                else ("agent-http/1"),
                "max_tools_per_step": min(config.max_tools_per_step, http_config.max_tool_calls),
                "max_request_bytes": min(config.max_request_bytes, http_config.max_request_bytes),
            }
        )
    elif live:
        assert http_config is not None
        prompt = "你是 Krim Agent。使用受控工具完成用户任务，保持数值精度。"
        version = "demo-live-xhigh/1"
        if (
            http_config.model != "gpt-6.1-sol"
            or http_config.reasoning_effort != "xhigh"
            or not http_config.api_key.get_secret_value()
            or not 60 <= http_config.read_timeout_seconds <= 90
            or not 180 <= config.run_timeout <= 240
            or config.max_active_runs != 1
            or config.max_steps < 2
            or "system_prompt" in specified
            and config.system_prompt != prompt
            or "target_version" in specified
            and config.target_version != version
        ):
            raise ProviderError("provider_config", 422)
        try:
            primary_settings = Settings.model_validate(
                config.model_dump()
                | {
                    "system_prompt": prompt,
                    "target_version": version,
                    "max_steps": 2,
                    "max_tools_per_step": 1,
                    "max_tool_calls": 1,
                    "max_input_bytes": min(4096, config.max_input_bytes),
                    "max_output_bytes": min(8192, config.max_output_bytes),
                    "max_request_bytes": min(32768, config.max_request_bytes),
                    "tool_timeout": min(2, config.tool_timeout),
                }
            )
        except ValueError:
            raise ProviderError("provider_config", 422) from None
    elif (
        http_config is not None
        and http_config.read_timeout_seconds > 5
        or isinstance(provider, OpenAICompatibleProvider)
        and provider.config.read_timeout_seconds > 5
    ):
        raise ProviderError("provider_config", 422)
    if access_policy is not None and config.access_policy_file is not None:
        raise ServiceError("access_policy_invalid", 422)
    try:
        active_access = (
            AccessPolicy.model_validate(deepcopy(access_policy.model_dump()))
            if access_policy is not None
            else AccessPolicy.load(config.access_policy_file)
            if config.access_policy_file is not None
            else None
        )
    except ValueError:
        raise ServiceError("access_policy_invalid", 422) from None
    if active_access is not None and config.api_key.get_secret_value():
        raise ServiceError("access_policy_invalid", 422)
    tools = registry if registry is not None else builtin_registry()
    if tool_config is not None and config.tool_config_file is not None:
        raise ServiceError("invalid_input", 422)
    external_config = (
        tool_config
        if tool_config is not None
        else ToolRuntimeConfig.load(config.tool_config_file)
        if config.tool_config_file is not None
        else None
    )
    evaluation_path = (
        _evaluation_database_path(path, config.product_eval_database)
        if config.product_eval_enabled
        else None
    )
    store = await SQLiteStore.open(path, max_events=config.max_events)
    owned: list[ModelProvider] = []
    artifacts = None
    runner = None
    operations = None
    application: Application | None = None
    tool_runtime: ToolRuntime | None = None
    try:
        identities = await IdentityService.open(store)
        if identities.managed and (
            config.identity_mode == "legacy"
            or active_access is not None
            or config.api_key.get_secret_value()
        ):
            raise ServiceError("identity_mode_conflict", 422)
        if config.identity_mode == "managed" and not identities.managed:
            raise ServiceError("accounts_not_initialized", 422)
        if sandbox_configuration is not None and not identities.managed:
            raise ServiceError("accounts_not_initialized", 422)
        if external_config is not None:
            tool_runtime = await ToolRuntime.open(tools, external_config, policy)
            tools, policy = tool_runtime.registry, tool_runtime.policy
        if workspace is None:
            # 只读判定恢复需求；缺可信工作区资源时不先写 interrupted/paused。
            async with store.engine.connect() as conn:
                pending = (
                    await conn.execute(select(runs.c.request).where(runs.c.status.not_in(TERMINAL)))
                ).scalars()
                needs_workspace = any(
                    isinstance(body, dict) and body.get("target") == "target-workspace"
                    for body in pending
                )
                if store.schema_version in {3, 4, 5, 6, 7, 8, 9, 10}:
                    queued = await conn.scalar(
                        select(turns.c.id)
                        .join(sessions, turns.c.session_id == sessions.c.id)
                        .where(
                            sessions.c.target_key == "target-workspace",
                            turns.c.status.in_(["queued", "running"]),
                        )
                        .limit(1)
                    )
                    control = await conn.scalar(
                        select(session_controls.c.request_key)
                        .join(sessions, session_controls.c.session_id == sessions.c.id)
                        .where(
                            sessions.c.target_key == "target-workspace",
                            session_controls.c.status == "pending",
                        )
                        .limit(1)
                    )
                    needs_workspace = needs_workspace or queued is not None or control is not None
                if store.schema_version in {4, 5, 6, 7, 8, 9, 10}:
                    active_effect = await conn.scalar(
                        select(tool_effects.c.call_id)
                        .where(tool_effects.c.phase.in_(["intent", "dispatch_intent"]))
                        .limit(1)
                    )
                    needs_workspace = needs_workspace or active_effect is not None
            if needs_workspace:
                raise ServiceError("workspace_unavailable")
        else:
            artifacts = ArtifactStore(store, workspace.artifact_root)
            operations = WorkspaceOperations(
                store,
                artifacts,
                workspace.profile,
                access_policy=active_access,
                identities=identities if identities.managed else None,
            )
            # 构造器用原票据/inode/UID完成真实资源恢复；失败则仅 finally 收已拥有资源。
            runner = SandboxRunner(
                workspace.execution, hook=operations.runner_hook, fence=operations.runner_fence
            )
            operations.runner = runner
        await store.recover()
        selected = (
            provider
            if provider is not None
            else (
                OpenAICompatibleProvider(http_config, max_connections=config.max_active_runs)
                if live and http_config is not None
                else FakeProvider()
            )
        )
        if provider is None:
            owned.append(selected)
        executor = ToolExecutor(tools, policy, pre, post)
        loop = AgentLoop(store, selected, executor, primary_settings)
        target: dict[str, JsonValue] = {
            "version": config.target_version,
            "system_prompt": config.system_prompt,
            "tools": cast(JsonValue, tools.schemas()),
            "limits": config.model_dump(
                mode="json", exclude={"database", "api_key", "system_prompt", *SESSION_FIELDS}
            ),
        }
        bindings = make_eval_bindings(store, config)
        owned.extend(b.loop.provider for b in bindings.values())
        bindings["demo"] = RunBinding.freeze("demo", loop, primary_settings)
        if live:
            bindings["target-http"] = RunBinding.freeze("target-http", loop, primary_settings)
        elif http_config is not None:
            http_provider = OpenAICompatibleProvider(
                http_config, max_connections=config.max_active_runs
            )
            owned.append(http_provider)
            bindings["target-http"] = (
                _make_http_binding(store, config, http_provider, registry=tools, policy=policy)
                if tool_runtime is not None
                else _make_http_binding(store, config, http_provider)
            )
        if workspace is not None:
            if "target-http" not in bindings and isinstance(provider, OpenAICompatibleProvider):
                bindings["target-http"] = (
                    _make_http_binding(store, config, provider, registry=tools, policy=policy)
                    if tool_runtime is not None
                    else _make_http_binding(store, config, provider)
                )
            http_binding = bindings.get("target-http")
            if http_binding is None or not isinstance(
                http_binding.loop.provider, OpenAICompatibleProvider
            ):
                raise ServiceError("workspace_unavailable")
            assert artifacts is not None and operations is not None and runner is not None
            work_settings = config.model_copy(
                update={
                    "system_prompt": (
                        "你是 Krim Agent。使用获授权工作区工具完成任务；模型陈述不等于完成核验。"
                    ),
                    "target_version": "workspace/1",
                    "max_request_bytes": min(
                        config.max_request_bytes,
                        http_binding.loop.provider.config.max_request_bytes,
                    ),
                }
            )
            work_loop = AgentLoop(
                store,
                http_binding.loop.provider,
                ToolExecutor(
                    workspace_registry(operations),
                    operations.policy,
                    pre,
                    post,
                    middleware=workspace_middleware,
                    wrap=workspace_wrap,
                ),
                work_settings,
                work=operations,
            )
            bindings["target-workspace"] = RunBinding.freeze(
                "target-workspace", work_loop, work_settings
            )
        sandbox_service = (
            SandboxService(store, identities, sandbox_configuration)
            if sandbox_configuration is not None
            else None
        )
        if sandbox_service is not None and sandbox_service.configuration.command_image is not None:
            base_binding = bindings.get("target-http")
            if base_binding is None:
                raise ServiceError("sandbox_config_invalid", 422)
            sandbox_operations = SandboxOperations(sandbox_service)
            sandbox_settings = base_binding.settings.model_copy(
                update={"target_version": "sandbox/1", "run_timeout": 180.0, "tool_timeout": 30.0}
            )
            sandbox_loop = AgentLoop(
                store,
                base_binding.loop.provider,
                ToolExecutor(sandbox_operations.registry(), sandbox_operations.policy, pre, post),
                sandbox_settings,
                sandbox=sandbox_operations,
            )
            bindings["target-http"] = RunBinding.freeze(
                "target-http", sandbox_loop, sandbox_settings
            )
        service = RunService(store, loop, config, target, bindings)
        application = Application(
            store,
            service,
            selected,
            config,
            provider is None,
            tuple({id(p): p for p in owned}.values()),
            EvaluationService(store, service),
            SessionService(
                store, service, config, identities=identities if identities.managed else None
            ),
            active_access,
            work=operations,
            artifacts=artifacts,
            runner=runner,
            tool_runtime=tool_runtime,
            identities=identities,
            sandbox=sandbox_service,
            extensions=ExtensionService(store, identities if identities.managed else None),
        )
        if evaluation_path is not None:
            adapters = {
                "krim-script-" + key.removeprefix("target-"): AdapterLease(
                    NeutralKrimAdapter(
                        store,
                        service,
                        key,
                        neutral_key="krim-script-" + key.removeprefix("target-"),
                    ),
                    owned=True,
                )
                for key in ("target-v1", "target-v2", "target-bad")
                if key in service.bindings
            }
            neutral = await open_eval(evaluation_path, adapters=adapters)
            application.product_evaluations = neutral
            assert application.sessions is not None
            application.evaluation_product = EvaluationProduct(
                neutral, service, application.sessions
            )
            if config.evaluation_automation_enabled:
                principals = (
                    tuple(
                        PrincipalContext(
                            item.principal_id, item.generation, item.memberships, item.legacy
                        )
                        for item in active_access.credentials
                    )
                    if active_access is not None
                    else (LOCAL,)
                )
                application.evaluation_automation = EvaluationAutomation(
                    application.evaluation_product,
                    principals,
                    identities=identities if identities.managed else None,
                )
                await application.evaluation_automation.start()

        async def identity_changed(principal_id: str, invalidate: bool, action: str) -> None:
            if not invalidate or action == "enable":
                return
            assert application is not None and application.sessions is not None
            # 身份快照已提交，先阻断后续派发，再等待已有执行按原取消协议收束。
            await application.sessions.revoke_principal(principal_id)
            if application.evaluation_automation is not None:
                await application.evaluation_automation.cancel_principal(principal_id)
            # 兼容数据只有一个显式legacy主体。无Session关联的v1运行与旧评测
            # 全部属于该兼容域，不把它们误归平台管理员或普通账号。
            from krim_agent.storage.account_models import accounts

            async with store.lock, store.engine.begin() as conn:
                legacy = await conn.scalar(
                    select(accounts.c.legacy).where(accounts.c.principal_id == principal_id)
                )
            if legacy:
                if application.evaluations is not None:
                    for eval_id, task in list(application.evaluations.tasks.items()):
                        if not task.done():
                            await application.evaluations.cancel(eval_id)
                for run_id, task in list(service.tasks.items()):
                    if not task.done() and (await store.snapshot(run_id)).session_context is None:
                        await service.cancel(run_id)

        identities.on_change = identity_changed
        return application
    except BaseException:
        if application is not None:
            await settled(application.close())
            raise
        try:
            try:
                if operations is not None:
                    await settled(operations.close())
            finally:
                try:
                    if runner is not None:
                        await settled(cast(Callable[[], Awaitable[None]], runner.close)())
                finally:
                    if artifacts is not None:
                        await settled(artifacts.close())
        finally:
            try:
                try:
                    if tool_runtime is not None:
                        await settled(tool_runtime.close())
                finally:
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
    store: SQLiteStore,
    settings: Settings,
    provider: OpenAICompatibleProvider,
    *,
    registry: ToolRegistry | None = None,
    policy: Policy | None = controlled_policy,
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
    loop = AgentLoop(
        store,
        provider,
        ToolExecutor(registry if registry is not None else builtin_registry(), policy),
        config,
    )
    return RunBinding.freeze("target-http", loop, config)
