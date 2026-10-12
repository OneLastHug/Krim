"""独立安装包实际模型树循环、原回执前暂停、另一进程冷读/核实。"""

import asyncio
import json
import os
import sys
from pathlib import Path

from sqlalchemy import func, select

import krim_agent
from krim_agent.agents import AgentProfile, AgentProfileUpdate
from krim_agent.budgets import UsageBudgetConfig
from krim_agent.config import Settings
from krim_agent.context_engine import ContextRunConfig
from krim_agent.execution import CommandSpec, LinuxSandboxRunner
from krim_agent.execution.directory_publication import CommandDirectoryChangesRequest
from krim_agent.persistence.command_models import CommandRow
from krim_agent.persistence.database import (
    WorkspaceDirectoryAttemptRow,
    WorkspaceDirectoryProposalRow,
    WorkspaceEditAttemptRow,
    WorkspaceEditProposalRow,
    WorkspaceTreeModelSourceRow,
    WorkspaceTreePlanRow,
)
from krim_agent.protocol.domain import ServiceError
from krim_agent.providers import ModelText, ModelToolCall, ModelUsage
from krim_agent.sdk import (
    AgentCommandRequest,
    AgentSession,
    PythonAI,
    TreePlanRequest,
    TreePlanView,
)
from krim_agent.usage import TokenUsage
from krim_agent.workspace_directory import DirectoryQueryRequest, WorkspaceDirectoryWriter
from krim_agent.workspace_writer import WorkspaceWriter


class Provider:
    name = "offline-installed-model-tree-tools"

    def __init__(self):
        self.calls = 0
        self.request = None
        self.plan = None

    async def stream(self, messages, tools):
        self.calls += 1
        assert self.request is not None, "冷读/恢复不允许再次调用Provider"
        assert {"workspace_tree_model_prepare", "workspace_tree_start"} <= {
            item.name for item in tools
        }
        if self.calls == 1:
            name, arguments = "workspace_tree_model_prepare", self.request.model_dump(mode="json")
        elif self.calls == 2:
            self.plan = TreePlanView.model_validate_json(messages[-1].content)
            assert self.plan.status == "planned"
            name, arguments = "workspace_tree_start", {"plan_id": self.plan.plan_id}
        elif self.calls == 3:
            self.plan = TreePlanView.model_validate_json(messages[-1].content)
            assert self.plan.status == "done"
            name, arguments = "workspace_tree_query", {"kind": "plan", "plan_id": self.plan.plan_id}
        else:
            assert json.loads(messages[-1].content)["plan"] == self.plan.model_dump(mode="json")
            yield ModelText(text="安装包原模型树已确认")
            yield ModelUsage(usage=TokenUsage(input_tokens=10, output_tokens=1, total_tokens=11))
            return
        yield ModelToolCall(id=f"installed-tree-{self.calls}", name=name, arguments=arguments)
        yield ModelUsage(usage=TokenUsage(input_tokens=10, output_tokens=1, total_tokens=11))


async def counts(database):
    async with database.sessions() as sql:
        return [
            await sql.scalar(select(func.count()).select_from(model))
            for model in (
                CommandRow,
                WorkspaceTreePlanRow,
                WorkspaceDirectoryProposalRow,
                WorkspaceDirectoryAttemptRow,
                WorkspaceEditProposalRow,
                WorkspaceEditAttemptRow,
                WorkspaceTreeModelSourceRow,
            )
        ]


async def proof(service, agent, provider, source):
    plan = await agent.directory_tree_by_key("installed-model-tree")
    page = await agent.directory_tree_steps(plan.plan_id)
    async with service.workspace_edits.database.sessions() as sql:
        sources = [
            {"kind": item.kind, "run_id": item.run_id, "call_id": item.call_id}
            for item in await sql.scalars(
                select(WorkspaceTreeModelSourceRow)
                .where(WorkspaceTreeModelSourceRow.plan_id == plan.plan_id)
                .order_by(WorkspaceTreeModelSourceRow.kind)
            )
        ]
    assert len(sources) == 2 and sources[0]["run_id"] == sources[1]["run_id"]
    assert [item["call_id"] for item in sources] == ["installed-tree-1", "installed-tree-2"]
    return {
        "package": str(krim_agent.__file__),
        "pid": os.getpid(),
        "agent_id": agent.agent_id,
        "scope": plan.scope_id,
        "source": source,
        "view": plan.model_dump(mode="json"),
        "steps": page.model_dump(mode="json"),
        "sources": sources,
        "provider_calls": provider.calls,
        "counts": await counts(service.workspace_edits.database),
    }


async def produce(ai, root, phase, source, provider):
    service = ai.agents()
    await service.profile_put(
        AgentProfileUpdate(
            expected_revision=0,
            config=AgentProfile(
                name="model-tree-process",
                description="安装包原模型树强杀恢复",
                workspace_key="files",
                execution_key="sandbox",
                persistent=True,
                timeout_seconds=120,
                tool_timeout_seconds=30,
                tool_names=[
                    "command_execute",
                    "command_get",
                    "command_propose",
                    "workspace_change",
                    "workspace_reconcile",
                    "directory_source_query",
                    "directory_source_preview",
                    "directory_change",
                    "directory_query",
                    "directory_reconcile",
                    "workspace_tree_query",
                    "workspace_tree_model_prepare",
                    "workspace_tree_start",
                ],
                context_config=ContextRunConfig(
                    tokenizer="cl100k_base", reserved_output_tokens=128
                ),
                usage_budget=UsageBudgetConfig(max_total_tokens=100_000),
            ),
        )
    )
    agent = await ai.agent("model-tree-process", persistent=True)
    command = await agent.execute_command(
        AgentCommandRequest(
            request_key="source",
            command=CommandSpec(
                argv=[
                    "/usr/bin/python3",
                    "-I",
                    "-c",
                    "import os;os.makedirs('tree/sub');os.chmod('tree',0o750);"
                    "os.chmod('tree/sub',0o700);open('tree/sub/data.bin','wb').write(bytes([0,255]))",
                ]
            ),
        ),
        confirmed=True,
    )
    catalog = await agent.command_directory_changes(
        CommandDirectoryChangesRequest(command_id=command.command_id)
    )
    provider.request = TreePlanRequest(
        command_id=command.command_id,
        source_hash=catalog.source_hash,
        root_path="tree",
        request_key="installed-model-tree",
    )
    self_close = []

    async def inspect_close(definition, context):
        if context.tree_fence is not None:
            try:
                await ai.close()
            except ServiceError as error:
                assert error.code == "workspace_tree_model_reentrant"
                assert not ai._closed and not service._closed
                self_close.append(definition.name)
            else:
                raise AssertionError("模型树child不能先关闭SDK/数据库")

    if phase == "complete":
        service._handle(agent.agent_id).runtime.tools.add_global_pre(inspect_close)
        result = await agent.execute("准备/发布/查询原模型树", idempotency_key="model-tree-loop")
        assert result.status == "completed" and provider.calls == 4, result
        assert len(self_close) == 6 and result.token_usage.total_tokens == 44
        value = await proof(service, agent, provider, source)
        assert value["view"]["status"] == "done" and value["counts"] == [1, 1, 2, 2, 1, 1, 2]
        accounts = await service._handle(agent.agent_id).runtime.budget_store.accounts()
        assert accounts[0].spent_tokens == 44 and accounts[0].held_tokens == 0
        value["budget_spent"] = 44
        value["self_close_rejected"] = len(self_close)
        (root / "metadata.json").write_text(json.dumps(value))
        print(json.dumps(value), flush=True)
        return
    assert phase == "after-write"
    backend = (
        service.workspace_edits if source == "file" else service.manager.directory_changes.store
    )
    original_settle = backend.settle
    entered, release = asyncio.Event(), asyncio.Event()

    async def before_receipt(*args, **kwargs):
        entered.set()
        await release.wait()
        return await original_settle(*args, **kwargs)

    backend.settle = before_receipt
    executing = asyncio.create_task(
        agent.execute("原回执前强杀", idempotency_key="model-tree-loop")
    )
    try:
        await asyncio.wait_for(entered.wait(), 25)
        value = await proof(service, agent, provider, source)
        position = 0 if source == "directory" else 2
        assert value["steps"]["items"][position]["operation"]["status"] == "running"
        assert value["provider_calls"] == 2 and value["view"]["producer_status"] == "running"
        assert (root / "files/tree").is_dir()
        if source == "file":
            assert (root / "files/tree/sub/data.bin").read_bytes() == b"\0\xff"
        (root / "metadata.json").write_text(json.dumps(value))
        print(json.dumps(value), flush=True)
        assert (await asyncio.to_thread(sys.stdin.readline)).strip() == "release"
        release.set()
        await executing
    finally:
        release.set()
        await asyncio.gather(executing, return_exceptions=True)


async def recover(ai, root, phase, source, provider):
    original = json.loads((root / "metadata.json").read_text())
    replay = 0

    def forbidden(*_args, **_kwargs):
        nonlocal replay
        replay += 1
        raise AssertionError("另进程冷读/核实不允许Writer或隔离命令重放")

    WorkspaceWriter.apply = WorkspaceDirectoryWriter.apply = LinuxSandboxRunner.start = forbidden
    service = ai.agents()
    assert await service.restore_persistent() == 1
    agent = AgentSession(service, original["agent_id"])
    identifier = original["view"]["plan_id"]
    current = await agent.directory_tree(identifier)
    if phase == "recover":
        position = 0 if source == "directory" else 2
        operation_id = original["steps"]["items"][position]["operation"]["operation_id"]
        if source == "directory":
            # 原目录的到期状态由原operation查询持久核实，树不伪造其回执。
            queried = await agent.query_directory(
                DirectoryQueryRequest(kind="operation", identifier=operation_id)
            )
            assert queried.operation.status == "uncertain"
        page = await agent.directory_tree_steps(identifier)
        assert (
            current.producer_status == "interrupted" and page.items[position].status == "uncertain"
        ), (current.model_dump(), page.model_dump())
        resolved = await (
            agent.reconcile_file_change(operation_id)
            if source == "file"
            else agent.reconcile_directory_change(operation_id)
        )
        assert resolved.status == "done" and resolved.operation_id == operation_id
        current = await agent.directory_tree(identifier)
        assert current.done_count == position + 1
        page = await agent.directory_tree_steps(identifier)
        assert all(
            item.status == "skipped" and item.operation is None
            for item in page.items[position + 1 :]
        )
    else:
        assert phase == "cold" and current.model_dump(mode="json") == original["view"]
        assert (await agent.directory_tree_steps(identifier)).model_dump(mode="json") == original[
            "steps"
        ]
    assert await agent.start_directory_tree(identifier, confirmed=True) == current
    assert not service.workspace_trees._tasks and not service.manager.tree_models._tasks
    result = await proof(service, agent, provider, source)
    assert result["counts"] == original["counts"] and result["sources"] == original["sources"]
    assert replay == provider.calls == 0
    assert (root / "files/tree").stat().st_mode & 0o777 == 0o750
    if source == "directory" and phase == "recover":
        assert not (root / "files/tree/sub").exists()
    else:
        assert (root / "files/tree/sub/data.bin").read_bytes() == b"\0\xff"
    result["zero_replay"] = True
    print(json.dumps(result), flush=True)


async def main():
    assert "/site-packages/" in str(krim_agent.__file__)
    phase, source, folder = sys.argv[1:4]
    root, provider = Path(folder), Provider()
    async with await PythonAI.create(
        settings=Settings(database_url=f"sqlite+aiosqlite:///{root / 'state.sqlite'}"),
        provider=provider,
    ) as ai:

        async def allow(_definition, _context):
            return "allow"

        ai.runtime.tool_policy = allow
        service = ai.agents()
        service.register_workspace("files", root / "files", version="v1", writable=True)
        service.register_execution("sandbox", LinuxSandboxRunner(), version="v1", owned=True)
        service.register_tree_models()
        service.register_tree_queries()
        if phase in {"cold", "recover"}:
            await recover(ai, root, phase, source, provider)
        else:
            await produce(ai, root, phase, source, provider)


asyncio.run(main())
