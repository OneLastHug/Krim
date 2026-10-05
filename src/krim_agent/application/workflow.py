"""Workflow 组合层：工具和模型使用已有权限/审计链，核心引擎不依赖它们。"""

from __future__ import annotations

import asyncio
import inspect
import json
from collections.abc import Awaitable, Callable, Mapping

from pydantic import JsonValue

from krim_agent.budgets import UsageBudgetConfig
from krim_agent.context_engine import ContextRunConfig
from krim_agent.observability import Tracer
from krim_agent.persistence.budgets import UsageBudgetStore
from krim_agent.sdk import PythonAI
from krim_agent.tools import ToolContext, ToolRegistry
from krim_agent.tools.core import Policy, ToolError
from krim_agent.workflow.builtins import input_value
from krim_agent.workflow.runners import ExecutionContext, RunnerBinding
from krim_agent.workflow.types import NodeOutput, WorkflowError, WorkflowGraph, WorkflowNode

TrustedCode = Callable[[JsonValue], JsonValue | Awaitable[JsonValue]]


class WorkflowServices:
    def __init__(
        self,
        *,
        tools: ToolRegistry | None = None,
        models: Mapping[str, PythonAI] | None = None,
        trusted_code: Mapping[str, TrustedCode] | None = None,
        policy: Policy | None = None,
        usage_budget: UsageBudgetConfig | None = None,
        budget_store: UsageBudgetStore | None = None,
        context_config: ContextRunConfig | None = None,
        tracer: Tracer | None = None,
    ) -> None:
        self.tools = tools
        self.models = dict(models or {})
        self.trusted_code = dict(trusted_code or {})
        self.policy = policy
        self.usage_budget = usage_budget
        self.budget_store = budget_store
        self.context_config = context_config
        self.tracer = tracer or next((ai.runtime.tracer for ai in self.models.values()), None)

    async def _budget_scope(self, context: ExecutionContext) -> tuple[str, ...]:
        first = next(iter(self.models.values()), None)
        store = self.budget_store or (first.runtime.budget_store if first else None)
        if store is None:
            if self.usage_budget is not None:
                raise WorkflowError("budget_store_required", "Workflow 预算需要可信账本")
            return ()
        parents = await first.runtime.ensure_usage_budget_binding() if first else ()
        return await store.bind(
            "workflow", context.root_run_id or context.run_id, self.usage_budget, parents
        )

    def bindings(self, graph: WorkflowGraph) -> dict[str, RunnerBinding]:
        bindings: dict[str, RunnerBinding] = {
            "tool": RunnerBinding(self.run_tool),
            "agent": RunnerBinding(self.run_model),
            "summary": RunnerBinding(self.run_model),
            "classifier": RunnerBinding(self.run_model),
        }
        if self.trusted_code:
            bindings["code"] = RunnerBinding(self.run_code, allow_code=True)
        # 已知只读工具可以按宿主声明安全重试；图不能将写工具改成只读。
        for node in graph.nodes:
            if node.kind == "tool" and self.tools is not None:
                name = node.config.get("name")
                if isinstance(name, str):
                    try:
                        definition = self.tools.get(name)
                    except ToolError:
                        raise WorkflowError(
                            "tool_unavailable", "Workflow tool 未注册", node_id=node.id
                        ) from None
                    bindings[node.id] = RunnerBinding(
                        self.run_tool, retry_safe=definition.risk == "read"
                    )
        return bindings

    async def run_tool(
        self, node: WorkflowNode, inputs: dict[str, JsonValue], context: ExecutionContext
    ) -> NodeOutput:
        if self.tools is None:
            raise WorkflowError(
                "tools_unavailable", "Workflow 未配置工具 registry", node_id=node.id
            )
        name = node.config.get("name")
        if not isinstance(name, str):
            raise WorkflowError(
                "invalid_tool_config", "tool.name 必须为已注册工具名称", node_id=node.id
            )
        arguments = input_value(inputs)
        if not isinstance(arguments, dict):
            raise WorkflowError("tool_input_type", "工具输入必须为参数对象", node_id=node.id)
        result = await self.tools.invoke(
            name,
            arguments,
            ToolContext(
                self.tools,
                run_id=context.run_id,
                topic_id="workflow",
                call_id=context.execution_id,
                timeout=node.timeout_seconds,
                policy=self.policy,
                tracer=self.tracer,
                trace_owner_kind="workflow",
                trace_owner_id=context.root_run_id or context.run_id,
                budget_account_ids=await self._budget_scope(context),
                context_config=(
                    self.context_config
                    or ContextRunConfig.from_settings(
                        next(iter(self.models.values())).runtime.settings
                    )
                ).model_dump(mode="json")
                if self.context_config is not None or self.models
                else None,
            ),
        )
        if result.is_error:
            raise WorkflowError(
                result.code or "tool_error", result.content[0].text, node_id=node.id
            )
        return NodeOutput(ports={"out": result.data})

    async def run_model(
        self, node: WorkflowNode, inputs: dict[str, JsonValue], context: ExecutionContext
    ) -> NodeOutput:
        model_name = node.config.get("model", "default")
        if not isinstance(model_name, str) or model_name not in self.models:
            raise WorkflowError(
                "model_unavailable", "Workflow 未配置指定的 PythonAI", node_id=node.id
            )
        value = input_value(inputs)
        prefix = node.config.get("prompt", "")
        if not isinstance(prefix, str):
            raise WorkflowError("invalid_model_config", "模型 prompt 必须为字符串", node_id=node.id)
        prompt = prefix + (
            value if isinstance(value, str) else json.dumps(value, ensure_ascii=False)
        )
        ai = self.models[model_name]
        accounts = await self._budget_scope(context)
        stream = await ai.stream_text(prompt, budget_account_ids=accounts)
        try:
            result = await stream.collect()
        except asyncio.CancelledError:
            await asyncio.shield(stream.abort())
            raise
        if result.finish_reason != "stop":
            raise WorkflowError(
                result.code or "model_failed", result.error or "模型未完成", node_id=node.id
            )
        return NodeOutput(
            ports={"out": result.text}, selection=result.text if node.kind == "classifier" else None
        )

    async def run_code(
        self, node: WorkflowNode, inputs: dict[str, JsonValue], context: ExecutionContext
    ) -> NodeOutput:
        name = node.config.get("runner")
        if not isinstance(name, str) or name not in self.trusted_code:
            raise WorkflowError("code_unavailable", "未注册该可信 code runner", node_id=node.id)
        value = self.trusted_code[name](input_value(inputs))
        output = await value if inspect.isawaitable(value) else value
        return NodeOutput(ports={"out": output})
