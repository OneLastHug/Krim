"""独立安装包原树发布、实际合成模型读取及新进程冷读/新读取Run零发布重放。"""

import asyncio
import json
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

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
    WorkspaceTreePlanRow,
)
from krim_agent.providers import ModelText, ModelToolCall, ModelUsage
from krim_agent.sdk import (
    AgentCommandRequest,
    AgentSession,
    PythonAI,
    TreePlanQueryResult,
    TreePlanRequest,
    TreePlanView,
)
from krim_agent.usage import TokenUsage

SCRIPT = Path(__file__).resolve()


class Reader:
    name = "offline-installed-tree-reader"

    def __init__(self):
        self.calls = 0
        self.plan = None

    async def stream(self, messages, tools):
        self.calls += 1
        assert "workspace_tree_query" in {item.name for item in tools}
        if self.calls == 1:
            request = {"kind": "plan", "plan_id": self.plan.plan_id}
        else:
            result = TreePlanQueryResult.model_validate_json(messages[-1].content)
            if self.calls == 2:
                assert result.plan == self.plan
                request = {"kind": "by_key", "request_key": self.plan.request_key}
            elif self.calls == 3:
                assert result.plan == self.plan
                request = {"kind": "steps", "plan_id": self.plan.plan_id, "limit": 1}
            elif self.calls == 4:
                assert result.steps.items[0].position == result.steps.next_after == 0
                request = {"kind": "steps", "plan_id": self.plan.plan_id, "after": 0, "limit": 1}
            elif self.calls == 5:
                assert result.steps.items[0].position == result.steps.next_after == 1
                request = {"kind": "by_key", "request_key": "not-recorded"}
            else:
                assert result.plan is result.steps is None
                yield ModelText(text="安装包已读取原树")
                yield ModelUsage(
                    usage=TokenUsage(input_tokens=10, output_tokens=1, total_tokens=11)
                )
                return
        yield ModelToolCall(
            id=f"installed-tree-{self.calls}", name="workspace_tree_query", arguments=request
        )
        yield ModelUsage(usage=TokenUsage(input_tokens=10, output_tokens=1, total_tokens=11))


async def counts(database):
    async with database.sessions() as sql:
        return [
            await sql.scalar(select(func.count()).select_from(table))
            for table in (
                CommandRow,
                WorkspaceTreePlanRow,
                WorkspaceDirectoryProposalRow,
                WorkspaceDirectoryAttemptRow,
                WorkspaceEditProposalRow,
                WorkspaceEditAttemptRow,
            )
        ]


async def phase(kind, root):
    assert "/site-packages/" in str(krim_agent.__file__)
    provider = Reader()
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
        service.register_tree_queries()
        if kind == "produce":
            await service.profile_put(
                AgentProfileUpdate(
                    expected_revision=0,
                    config=AgentProfile(
                        name="reader",
                        description="安装包原树",
                        workspace_key="files",
                        execution_key="sandbox",
                        persistent=True,
                        tool_timeout_seconds=30,
                        tool_names=[
                            "command_execute",
                            "command_get",
                            "workspace_tree_prepare",
                            "workspace_tree_query",
                            "directory_source_query",
                            "directory_source_preview",
                            "directory_change",
                            "command_propose",
                            "workspace_change",
                        ],
                        usage_budget=UsageBudgetConfig(max_total_tokens=100_000),
                        context_config=ContextRunConfig(
                            tokenizer="cl100k_base", reserved_output_tokens=128
                        ),
                    ),
                )
            )
            agent = await ai.agent("reader", persistent=True)
            command = await agent.execute_command(
                AgentCommandRequest(
                    request_key="source",
                    command=CommandSpec(
                        argv=[
                            "/usr/bin/python3",
                            "-I",
                            "-c",
                            "import os;os.makedirs('tree/sub');"
                            "os.chmod('tree',0o750);os.chmod('tree/sub',0o700);"
                            "open('tree/sub/data.bin','wb').write(bytes([0,255]))",
                        ]
                    ),
                ),
                confirmed=True,
            )
            source = await agent.command_directory_changes(
                CommandDirectoryChangesRequest(command_id=command.command_id)
            )
            planned = await agent.prepare_directory_tree(
                TreePlanRequest(
                    command_id=command.command_id,
                    source_hash=source.source_hash,
                    root_path="tree",
                    request_key="original-tree",
                ),
                confirmed=True,
            )
            await agent.start_directory_tree(planned.plan_id, confirmed=True)
            provider.plan = await agent.wait_directory_tree(planned.plan_id)
            assert provider.plan.status == "done"
        else:
            metadata = json.loads((root / "metadata.json").read_text())
            assert await service.restore_persistent() == 1
            agent = AgentSession(service, metadata["agent_id"])
            provider.plan = TreePlanView.model_validate_json(json.dumps(metadata["plan"]))
            assert await agent.directory_tree(provider.plan.plan_id) == provider.plan
            assert await agent.directory_tree_by_key(provider.plan.request_key) == provider.plan
            assert provider.calls == 0
        before = await counts(ai.runtime.repository.database)
        assert before == [1, 1, 2, 2, 1, 1]
        result = await agent.execute("读取原目录树", idempotency_key=kind + "-read")
        assert (
            result.status == "completed"
            and result.text == "安装包已读取原树"
            and result.token_usage.total_tokens == 66
        ), result
        assert provider.calls == 6 and await counts(ai.runtime.repository.database) == before
        messages = await ai.runtime.repository.run_messages(result.run_id)
        assert [item.details["call_id"] for item in messages if item.role == "tool"] == [
            f"installed-tree-{index}" for index in range(1, 6)
        ]
        assert (root / "files/tree/sub/data.bin").read_bytes() == b"\0\xff"
        accounts = await service._handle(agent.agent_id).runtime.budget_store.accounts()
        assert len(accounts) == 1 and accounts[0].spent_tokens == (66 if kind == "produce" else 132)
        proof = {
            "package": str(krim_agent.__file__),
            "phase": kind,
            "agent_id": agent.agent_id,
            "run_id": result.run_id,
            "plan": provider.plan.model_dump(mode="json"),
            "counts": before,
            "provider_calls": provider.calls,
            "local_synthetic_provider_loop": True,
            "zero_command_writer_worker_replay": True,
            "separate_process_cold_sdk_zero_provider_calls": kind == "cold",
            "reported_budget_spent": accounts[0].spent_tokens,
        }
        if kind == "produce":
            (root / "metadata.json").write_text(json.dumps(proof))
        return proof


async def main():
    if len(sys.argv) == 3:
        print(json.dumps(await phase(sys.argv[1], Path(sys.argv[2]))))
        return
    assert "/site-packages/" in str(krim_agent.__file__)
    with TemporaryDirectory(prefix="krim-model-tree-query-wheel-", dir="/data/tmp/root") as folder:
        root = Path(folder)
        (root / "files").mkdir()
        results = []
        for name in ("produce", "cold"):
            process = await asyncio.create_subprocess_exec(
                sys.executable,
                "-I",
                str(SCRIPT),
                name,
                str(root),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            try:
                output, errors = await asyncio.wait_for(process.communicate(), 60)
                assert process.returncode == 0, errors.decode()
                results.append(json.loads(output))
            finally:
                if process.returncode is None:
                    process.kill()
                    await process.communicate()
        assert (
            results[0]["plan"] == results[1]["plan"]
            and results[0]["run_id"] != results[1]["run_id"]
        )
        print(json.dumps({"scenarios": results}, ensure_ascii=False))


asyncio.run(main())
