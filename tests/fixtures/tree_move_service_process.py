"""实际模型Registry树移动/保留的SQL回执前暂停，与新进程原核实。"""

import asyncio
import json
import os
import sys
from pathlib import Path

from sqlalchemy import func, select

import krim_agent
import krim_agent.workspace_tree_writer as move_module
from krim_agent.agents import AgentProfile, AgentProfileUpdate
from krim_agent.config import Settings
from krim_agent.context_engine import ContextRunConfig
from krim_agent.persistence.database import (
    WorkspaceTreeMoveAttemptRow,
    WorkspaceTreeMoveLockRow,
    WorkspaceTreeMoveModelSourceRow,
    WorkspaceTreeMoveProposalRow,
)
from krim_agent.persistence.workspace_tree_moves import WorkspaceTreeMoveStore
from krim_agent.protocol.domain import ServiceError
from krim_agent.providers import ModelText, ModelToolCall, ModelUsage
from krim_agent.sdk import AgentSession, PythonAI
from krim_agent.tools.tree_moves import tree_move_tools
from krim_agent.usage import TokenUsage
from krim_agent.workspace_tree_move import (
    TreeMoveModelSource,
    TreeMoveOperationRequest,
    TreeMoveQueryRequest,
)
from krim_agent.workspace_tree_writer import WorkspaceTreeWriter


class Provider:
    name = "offline-process-original-tree-move"

    def __init__(self, phase):
        self.phase, self.calls, self.proposal = phase, 0, None

    async def stream(self, messages, tools):
        self.calls += 1
        assert self.phase == "produce", "冷恢复不应重放Provider"
        value = json.loads(messages[-1].content) if self.calls > 1 else None
        if self.calls == 1:
            name, arguments = "tree_move_inspect", {"path": "tree"}
        elif self.calls == 2:
            name, arguments = (
                "tree_move_preview",
                {
                    "source_path": "tree",
                    "target_path": "dest/tree",
                    "expected_fingerprint": value["fingerprint"],
                    "request_key": "preview",
                },
            )
        elif self.calls == 3:
            self.proposal = value
            name, arguments = (
                "tree_move_change",
                {
                    "proposal_id": value["proposal_id"],
                    "action": "apply",
                    "expected_revision": value["revision"],
                    "request_key": "apply",
                },
            )
        elif self.calls == 4:
            assert value["status"] == "done", value
            name, arguments = (
                "tree_move_query",
                {"kind": "proposal", "identifier": self.proposal["proposal_id"]},
            )
        elif self.calls == 5:
            proposal = value["proposal"]
            name, arguments = (
                "tree_move_change",
                {
                    "proposal_id": proposal["proposal_id"],
                    "action": "keep",
                    "expected_revision": proposal["revision"],
                    "request_key": "keep",
                },
            )
        else:
            yield ModelText(text="原树操作完成")
            return
        yield ModelToolCall(id=f"original-{self.calls}", name=name, arguments=arguments)
        yield ModelUsage(usage=TokenUsage(input_tokens=10, output_tokens=1, total_tokens=11))


async def counts(database):
    async with database.sessions() as sql:
        return [
            await sql.scalar(select(func.count()).select_from(table))
            for table in (
                WorkspaceTreeMoveProposalRow,
                WorkspaceTreeMoveAttemptRow,
                WorkspaceTreeMoveLockRow,
                WorkspaceTreeMoveModelSourceRow,
            )
        ]


async def main():
    phase, kind, folder = sys.argv[1:4]
    root, provider = Path(folder), Provider(phase)
    async with await PythonAI.create(
        settings=Settings(database_url=f"sqlite+aiosqlite:///{root / 'state.sqlite'}"),
        provider=provider,
    ) as ai:
        service = ai.agents()
        service.register_workspace("files", root / "files", version="v1", writable=True)
        service.register_tree_moves()

        async def allow(_definition, _context):
            return "allow"

        ai.runtime.tool_policy = allow
        await service.profile_put(
            AgentProfileUpdate(
                expected_revision=0,
                config=AgentProfile(
                    name="move",
                    description="原模型树进程恢复",
                    workspace_key="files",
                    tool_names=[item.name for item in tree_move_tools()],
                    max_steps=8,
                    tool_timeout_seconds=30,
                    timeout_seconds=120,
                    context_config=ContextRunConfig(
                        tokenizer="cl100k_base", reserved_output_tokens=128
                    ),
                ),
            )
        ) if phase == "produce" else None
        store = service.manager.tree_moves.store
        if phase == "produce":
            agent = AgentSession(service, (await service.spawn("move", persistent=True)).agent_id)
            original_settle = store.settle

            async def blocked(scope, identifier, effect, **kwargs):
                operation = await store.operation(scope, identifier)
                if operation.view.action != kind:
                    return await original_settle(scope, identifier, effect, **kwargs)
                assert effect.receipt.status == "done"
                metadata = {
                    "package": str(krim_agent.__file__),
                    "pid": os.getpid(),
                    "agent_id": agent.agent_id,
                    "scope": scope,
                    "proposal_id": operation.view.proposal_id,
                    "operation_id": identifier,
                    "old_owner": store.owner_id,
                    "run_id": operation.model_source.run_id,
                    "call_id": operation.model_source.call_id,
                    "before": operation.prepared.before.fingerprint,
                    "counts": await counts(store.database),
                    "provider_calls": provider.calls,
                    "view": operation.view.model_dump(mode="json"),
                    "effect_before_receipt": True,
                    "binary_inode": (root / "files/dest/tree/sub/data.bin").stat().st_ino,
                }
                (root / "metadata.json").write_text(json.dumps(metadata))
                print(json.dumps(metadata), flush=True)
                assert (await asyncio.to_thread(sys.stdin.readline)).strip() == "release"
                return await original_settle(scope, identifier, effect, **kwargs)

            store.settle = blocked
            result = await agent.execute("原已有树移动后保留", idempotency_key="model-loop")
            assert result.status == "completed", result
            return
        assert phase == "recover"
        metadata = json.loads((root / "metadata.json").read_text())
        replays = 0

        def forbidden(*_args, **_kwargs):
            nonlocal replays
            replays += 1
            raise AssertionError("冷恢复禁止重发Provider/prepare/rename/apply")

        move_module.rename_at = forbidden
        WorkspaceTreeWriter.apply = forbidden
        WorkspaceTreeWriter.prepare = forbidden
        assert await service.restore_persistent() == 1
        agent = AgentSession(service, metadata["agent_id"])
        await service.recover_workspace_edits()
        original = await store.operation(metadata["scope"], metadata["operation_id"])
        assert original.view.status == "uncertain"
        assert original.model_source == TreeMoveModelSource(metadata["run_id"], metadata["call_id"])
        assert original.prepared.before.fingerprint == metadata["before"]
        stale = WorkspaceTreeMoveStore(service.workspace_edits)
        stale.owner_id = metadata["old_owner"]
        try:
            await stale.assert_execution(metadata["scope"], metadata["operation_id"])
        except ServiceError:
            fenced = True
        else:
            raise AssertionError("旧owner获得发布资格")
        result = await service.tree_move_reconcile(
            agent.agent_id, TreeMoveOperationRequest(operation_id=metadata["operation_id"])
        )
        assert result.status == "done" and result.receipt.changed == (kind == "apply")
        query = await service.tree_move_query(
            agent.agent_id, TreeMoveQueryRequest(kind="operation_by_key", request_key=kind)
        )
        assert query.operation == result
        proposal = await service.tree_move_query(
            agent.agent_id, TreeMoveQueryRequest(kind="by_key", request_key="preview")
        )
        assert proposal.proposal.status == ("applied" if kind == "apply" else "kept")
        assert await counts(store.database) == [
            1,
            1 if kind == "apply" else 2,
            0,
            2 if kind == "apply" else 3,
        ]
        binary = root / "files/dest/tree/sub/data.bin"
        assert (
            binary.read_bytes() == b"\0\xff\x80"
            and binary.stat().st_ino == metadata["binary_inode"]
        )
        assert replays == provider.calls == 0
        print(
            json.dumps(
                {
                    "package": str(krim_agent.__file__),
                    "pid": os.getpid(),
                    "view": result.model_dump(mode="json"),
                    "counts": await counts(store.database),
                    "original_run_call": True,
                    "original_sql_blob": True,
                    "old_owner_fenced": fenced,
                    "provider_calls": provider.calls,
                    "replay_calls": replays,
                }
            ),
            flush=True,
        )


asyncio.run(main())
