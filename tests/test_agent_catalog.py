import asyncio
from dataclasses import replace

import pytest
from sqlalchemy import select

from krim_agent.agents import AgentCatalog, AgentDefinition, AgentManager
from krim_agent.config import Settings
from krim_agent.persistence.database import RunRow
from krim_agent.protocol.domain import ChatRequest
from krim_agent.providers import FakeProvider, ModelText, ModelToolCall
from krim_agent.runtime import ChatRuntime
from krim_agent.sdk import PythonAI
from krim_agent.tools import ToolContext, ToolError, ToolRegistry, tool
from krim_agent.tools.builtins import add, echo


def read_tools():
    registry = ToolRegistry()
    registry.register_many(add, echo)
    return registry


class DelegatingProvider:
    name = "offline-model-delegation"

    def __init__(self, tool_name="agent_calculator", prompt="/add 2 3"):
        self.tool_name, self.prompt = tool_name, prompt
        self.seen_tools = []

    async def stream(self, messages, tools):
        self.seen_tools.append([definition.name for definition in tools])
        result = next((message for message in reversed(messages) if message.role == "tool"), None)
        if result is None:
            yield ModelToolCall(name=self.tool_name, arguments={"prompt": self.prompt})
        else:
            yield ModelText(text="父模型收到：" + result.content[:7000])


def catalog_entry(manager, *, name="calculator", tools=None, provider=None, **kwargs):
    catalog = AgentCatalog(manager)
    definition = AgentDefinition(
        name=name,
        provider=provider or FakeProvider(delay=0),
        tools=tools or read_tools(),
        **kwargs,
    )
    catalog.register(name, definition, description="离线计算助手", risk="read")
    return catalog


async def test_model_agent_tool_child_tool_parent_model_and_audits_are_real(repository):
    manager = AgentManager(repository)
    source = read_tools()
    root = ToolRegistry()
    seen = []

    async def pre(definition, context):
        seen.append((definition.name, context.agent_id, context.run_id))

    # 同一 Hook 来自父/子规则时只运行一次；子运行使用自己的真实 run_id。
    root.add_global_pre(pre)
    source.add_global_pre(pre)
    catalog = catalog_entry(manager, tools=source)
    descriptor = catalog.descriptors()[0]
    assert descriptor.tool_name == "agent_calculator" and descriptor.risk == "read"
    assert "provider" not in descriptor.model_dump() and "tools" not in descriptor.model_dump()
    assert catalog.install_tools(root) == ["agent_calculator"]
    model = DelegatingProvider()
    runtime = ChatRuntime(repository, model, root, Settings())
    ai = PythonAI(runtime)
    try:
        result = await ai.generate_text("委派计算")
        assert result.run.status == "completed" and "5.0" in result.text
        assert model.seen_tools == [["agent_calculator"], ["agent_calculator"]]
        assert [item[0] for item in seen] == ["agent_calculator", "add"]
        assert seen[0][1] is None and seen[1][1] is not None
        assert seen[0][2] == result.run.id and seen[1][2] != result.run.id
        parent_audits = await repository.audits(result.run.id)
        assert len(parent_audits) == 1 and parent_audits[0].name == "agent_calculator"
        child_data = result.steps[0].tool_results[0].data
        assert child_data["parent_tool_call_id"] == parent_audits[0].call_id
        assert child_data["agent_id"] == seen[1][1]
        child_audits = await repository.audits(child_data["run_id"])
        assert len(child_audits) == 1 and child_audits[0].name == "add"
        child_run = await repository.get_run(child_data["run_id"])
        assert child_run.topic_id != result.run.topic_id and child_run.status == "completed"
        assert not manager._handles  # 返回父模型前普通句柄已经销毁。
    finally:
        await ai.close()
        await runtime.close()
        await manager.close()


async def test_default_write_agent_is_denied_without_spawning(repository):
    manager = AgentManager(repository)
    catalog = AgentCatalog(manager)
    catalog.register(
        "calculator",
        AgentDefinition(name="calculator", provider=FakeProvider(delay=0), tools=read_tools()),
        description="默认 write 委派",
    )
    registry = ToolRegistry()
    catalog.install_tools(registry)
    result = await registry.invoke(
        "agent_calculator", {"prompt": "/add 1 2"}, ToolContext(registry)
    )
    assert result.is_error and result.code == "permission_denied"
    assert not manager._handles and not await repository.list_topics()
    await manager.close()


async def test_child_permissions_are_intersection_and_never_inherit_blanket_approval(repository):
    executed = []

    @tool(name="write_value", risk="write")
    async def write_value(value: str):
        executed.append(value)
        return value

    class WriteProvider:
        name = "offline-write-request"

        async def stream(self, messages, _tools):
            result = next(
                (message for message in reversed(messages) if message.role == "tool"), None
            )
            if result is None:
                yield ModelToolCall(name="write_value", arguments={"value": "forbidden"})
            else:
                yield ModelText(text=result.content)

    async def own_policy(_definition, _context):
        return "allow"

    async def caller_policy(definition, _context):
        return "allow" if definition.name == "agent_writer" else "deny"

    child_tools = ToolRegistry()
    child_tools.register(write_value)
    manager = AgentManager(repository)
    catalog = AgentCatalog(manager)
    catalog.register(
        "writer",
        AgentDefinition(
            name="writer", provider=WriteProvider(), tools=child_tools, policy=own_policy
        ),
        description="逐工具裁决",
    )
    root = ToolRegistry()
    catalog.install_tools(root)
    result = await root.call_tool(
        "agent_writer", {"prompt": "写入"}, ToolContext(root, policy=caller_policy)
    )
    assert result["status"] == "completed" and "没有执行许可" in result["text"]
    assert not executed
    audits = await repository.audits(result["run_id"])
    assert audits[0].decision == "deny" and audits[0].code == "permission_denied"
    assert not manager._handles
    await manager.close()


async def test_definition_policy_can_restrict_parent_allowed_read_tool(repository):
    decisions = []

    async def own_policy(definition, _context):
        decisions.append(definition.name)
        return "deny"

    async def caller_policy(_definition, _context):
        return "allow"

    manager = AgentManager(repository)
    catalog = catalog_entry(manager, policy=own_policy)
    registry = ToolRegistry()
    catalog.install_tools(registry)
    result = await registry.call_tool(
        "agent_calculator", {"prompt": "/add 1 2"}, ToolContext(registry, policy=caller_policy)
    )
    assert decisions == ["add"] and "没有执行许可" in result["text"]
    assert (await repository.audits(result["run_id"]))[0].decision == "deny"
    await manager.close()


async def test_parent_hook_changes_arguments_and_child_policy_rechecks(repository):
    policy_inputs = []

    async def policy(definition, context):
        if definition.name == "add":
            policy_inputs.append(context.arguments["left"])
            return "allow" if context.arguments["left"] < 10 else "deny"
        return "allow"

    async def pre(definition, context):
        if definition.name == "add":
            context.arguments["left"] = 900.0

    manager = AgentManager(repository)
    catalog = catalog_entry(manager)
    registry = ToolRegistry()
    registry.add_global_pre(pre)
    catalog.install_tools(registry)
    result = await registry.call_tool(
        "agent_calculator", {"prompt": "/add 1 2"}, ToolContext(registry, policy=policy)
    )
    assert policy_inputs == [1.0, 900.0]
    assert "没有执行许可" in result["text"]
    assert (await repository.audits(result["run_id"]))[0].code == "permission_denied"
    await manager.close()


@pytest.mark.parametrize("case", ["bad_name", "risk", "description", "persistent", "collision"])
async def test_registration_failure_is_atomic(repository, case):
    manager = AgentManager(repository)
    catalog = AgentCatalog(manager)
    definition = AgentDefinition(name="valid", provider=FakeProvider(delay=0), tools=read_tools())
    kwargs = {"description": "描述", "risk": "read"}
    name = "valid"
    if case == "bad_name":
        kwargs["tool_name"] = "bad-name"
    elif case == "risk":
        kwargs["risk"] = "invalid"
    elif case == "description":
        kwargs["description"] = " "
    elif case == "persistent":
        definition = replace(definition, persistent=True)
    elif case == "collision":
        catalog.register("valid", definition, description="首个", risk="read")
        name = "VALID"
        definition = replace(definition, name=name)
    before = catalog.descriptors()
    with pytest.raises(ValueError):
        catalog.register(name, definition, **kwargs)
    assert catalog.descriptors() == before
    await manager.close()


async def test_installation_conflict_does_not_leave_partial_tools_and_hidden_entry_is_explicit(
    repository,
):
    manager = AgentManager(repository)
    catalog = AgentCatalog(manager)
    for name in ["first", "second"]:
        catalog.register(
            name,
            AgentDefinition(name=name, provider=FakeProvider(delay=0), tools=read_tools()),
            description=name,
            risk="read",
        )
    registry = ToolRegistry()
    registry.register(catalog.as_tool("second"))
    with pytest.raises(ValueError, match="冲突"):
        catalog.install_tools(registry)
    assert [definition.name for definition in registry.definitions] == ["agent_second"]
    with pytest.raises(ValueError, match="重复"):
        catalog.install_tools(ToolRegistry(), names=["first", "first"])
    catalog.register(
        "hidden",
        AgentDefinition(
            name="hidden", provider=FakeProvider(delay=0), tools=read_tools(), persistent=True
        ),
        description="仅目录",
        expose_as_tool=False,
    )
    with pytest.raises(ToolError) as error:
        catalog.as_tool("hidden")
    assert error.value.code == "agent_not_exposed"
    with pytest.raises(ToolError) as error:
        catalog.get("missing")
    assert error.value.code == "agent_not_found"
    await manager.close()


async def test_readonly_risk_is_checked_again_after_host_registry_changes(repository):
    manager = AgentManager(repository)
    source = read_tools()
    catalog = catalog_entry(manager, tools=source)
    registry = ToolRegistry()
    catalog.install_tools(registry)

    @tool(risk="write")
    async def later_write():
        pytest.fail("不能执行")

    source.register(later_write)
    result = await registry.invoke("agent_calculator", {"prompt": "调用"}, ToolContext(registry))
    assert result.is_error and result.code == "agent_risk_changed"
    assert not manager._handles and not await repository.list_topics()
    await manager.close()


async def test_model_payload_cannot_select_provider_parent_or_budget(repository):
    manager = AgentManager(repository)
    catalog = catalog_entry(manager)
    registry = ToolRegistry()
    catalog.install_tools(registry)
    result = await registry.invoke(
        "agent_calculator",
        {"prompt": "调用", "parent_agent_id": "forged", "max_steps": 99},
        ToolContext(registry),
    )
    assert result.is_error and result.code == "invalid_input"
    assert not await repository.list_topics()
    await manager.close()


async def test_recursive_delegation_uses_real_parent_and_manager_depth_limit(repository):
    manager = AgentManager(repository, max_recursion_depth=2, max_concurrent=2)
    source = ToolRegistry()
    catalog = catalog_entry(
        manager,
        name="nested",
        tools=source,
        provider=DelegatingProvider("agent_nested", "递归"),
    )
    catalog.install_tools(source)
    result = await source.call_tool("agent_nested", {"prompt": "递归"})
    assert "递归深度超过限制" in result["text"]
    async with repository.database.sessions() as session:
        runs = list(await session.scalars(select(RunRow)))
    assert len(runs) == 2 and all(run.status == "completed" for run in runs)
    assert not manager._handles
    await manager.close()


async def test_explicit_parent_permission_allows_child_write_without_bypassing_audit(repository):
    values = []

    @tool(name="echo", risk="write")
    async def write_echo(text: str):
        values.append(text)
        return text

    async def allowed(_definition, _context):
        return "allow"

    source = ToolRegistry()
    source.register(write_echo)
    manager = AgentManager(repository)
    catalog = AgentCatalog(manager)
    catalog.register(
        "writer",
        AgentDefinition(name="writer", provider=FakeProvider(delay=0), tools=source),
        description="受控写工具",
    )
    registry = ToolRegistry()
    catalog.install_tools(registry)
    result = await registry.call_tool(
        "agent_writer", {"prompt": "/echo approved"}, ToolContext(registry, policy=allowed)
    )
    assert values == ["approved"] and "approved" in result["text"]
    audits = await repository.audits(result["run_id"])
    assert audits[0].risk == "write" and audits[0].decision == "allow" and audits[0].ok
    assert not manager._handles
    await manager.close()


class WaitingChild:
    name = "waiting-child"

    def __init__(self):
        self.entered = asyncio.Event()
        self.closed = asyncio.Event()

    async def stream(self, _messages, _tools):
        self.entered.set()
        try:
            yield ModelText(text="子 Agent 部分结果")
            await asyncio.Event().wait()
        finally:
            self.closed.set()


async def test_manager_close_during_agent_tool_does_not_cycle_with_scope_cleanup(repository):
    manager = AgentManager(repository)
    child = WaitingChild()
    catalog = catalog_entry(manager, provider=child)
    registry = ToolRegistry()
    catalog.install_tools(registry)
    task = asyncio.create_task(
        registry.call_tool(
            "agent_calculator", {"prompt": "等待"}, ToolContext(registry, timeout=10)
        )
    )
    await asyncio.wait_for(child.entered.wait(), 5)
    child_handle = next(iter(manager._handles.values()))
    run_id = child_handle.current_run.id
    await asyncio.wait_for(manager.close(), 5)
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 5)
    assert child.closed.is_set() and not manager._handles
    assert (await repository.get_run(run_id)).status == "aborted"
    assert child_handle.runtime.production_task(run_id).done()


async def test_parent_runtime_stop_waits_for_child_scope_stop_and_persistence(repository):
    manager = AgentManager(repository)
    child = WaitingChild()
    catalog = catalog_entry(manager, provider=child)
    registry = ToolRegistry()
    catalog.install_tools(registry)
    runtime = ChatRuntime(repository, DelegatingProvider(prompt="等待"), registry, Settings())
    topic = await repository.create_topic("父运行取消")
    run = await runtime.start(topic.id, ChatRequest(text="委派", idempotency_key="cancel"))
    try:
        await asyncio.wait_for(child.entered.wait(), 5)
        handle = next(iter(manager._handles.values()))
        child_run_id = handle.current_run.id
        stopped = await asyncio.wait_for(runtime.stop(run.id), 5)
        assert stopped.status == "aborted" and child.closed.is_set()
        assert (await repository.get_run(child_run_id)).status == "aborted"
        assert not manager._handles
        assert (await repository.audits(run.id))[0].code == "cancelled"
        history = await repository.run_messages(run.id)
        assert history[-1].role == "tool" and history[-1].details["code"] == "cancelled"
    finally:
        await runtime.close()
        await manager.close()


async def test_outer_tool_timeout_cleans_child_without_returning_completed(repository):
    manager = AgentManager(repository)
    child = WaitingChild()
    catalog = catalog_entry(manager, provider=child)
    registry = ToolRegistry()
    catalog.install_tools(registry)
    deadlines = []

    async def capture(definition, context):
        if definition.name == "agent_calculator":
            deadlines.append(context.execution_timeouts[-1].deadline)

    registry.add_global_pre(capture)
    # 本测试验证“运行中”的工具 timeout 清理，先等待真实 Provider 已进入。
    # 启动时取消由其它用例验证；不让 SQLite 调度耗时替代该测试的目标边界。
    task = asyncio.create_task(
        registry.invoke("agent_calculator", {"prompt": "等待"}, ToolContext(registry, timeout=5))
    )
    await asyncio.wait_for(child.entered.wait(), 5)
    deadlines[0].reschedule(asyncio.get_running_loop().time() + 0.05)
    result = await asyncio.wait_for(task, 5)
    assert result.is_error and result.code == "timeout"
    assert child.entered.is_set() and child.closed.is_set() and not manager._handles
    async with repository.database.sessions() as session:
        runs = list(await session.scalars(select(RunRow)))
    assert len(runs) == 1 and runs[0].status in {"aborted", "failed"}
    await manager.close()


async def test_agent_failure_is_tool_error_with_real_child_terminal_and_cleanup(repository):
    class BrokenChild:
        name = "broken-child"

        async def stream(self, _messages, _tools):
            raise RuntimeError("外部敏感正文")
            yield ModelText(text="不可达")

    manager = AgentManager(repository)
    catalog = catalog_entry(manager, provider=BrokenChild())
    registry = ToolRegistry()
    catalog.install_tools(registry)
    result = await registry.invoke("agent_calculator", {"prompt": "失败"}, ToolContext(registry))
    assert result.is_error and result.code == "execution_error" and result.data is None
    assert "敏感正文" not in str(result)
    assert not manager._handles
    async with repository.database.sessions() as session:
        runs = list(await session.scalars(select(RunRow)))
    assert len(runs) == 1 and runs[0].status == "failed"
    assert (await repository.list_events(runs[0].id))[-1].data["status"] == "failed"
    await manager.close()
