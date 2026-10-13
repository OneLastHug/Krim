"""0043原SQL材料/模型来源、实际效果后SIGKILL与新进程原operation核实。"""

import asyncio
import json
import os
import sys
from pathlib import Path

from sqlalchemy import func, select

import krim_agent
import krim_agent.workspace_tree_writer as move_module
from krim_agent.config import Settings
from krim_agent.persistence.database import (
    WorkspaceTreeMoveAttemptRow,
    WorkspaceTreeMoveLockRow,
    WorkspaceTreeMoveModelSourceRow,
    WorkspaceTreeMoveProposalRow,
)
from krim_agent.persistence.workspace_tree_moves import WorkspaceTreeMoveStore
from krim_agent.protocol.domain import ChatRequest, ServiceError
from krim_agent.providers import FakeProvider
from krim_agent.sdk import AgentSession, PythonAI
from krim_agent.workspace_tree_move import (
    TreeMoveAction,
    TreeMoveModelSource,
    TreeMovePreviewRequest,
)
from krim_agent.workspace_tree_writer import WorkspaceTreeWriter


class Provider(FakeProvider):
    def __init__(self):
        super().__init__(delay=0)
        self.calls = 0

    async def stream(self, messages, tools):
        self.calls += 1
        raise AssertionError("本存储专项不允许模型循环")
        yield


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
    phase, folder = sys.argv[1:3]
    root, provider = Path(folder), Provider()
    async with await PythonAI.create(
        settings=Settings(database_url=f"sqlite+aiosqlite:///{root / 'state.sqlite'}"),
        provider=provider,
    ) as ai:
        service = ai.agents()
        service.register_workspace(
            "files", root / "files", version="v1", writable=True, default=True
        )
        store = WorkspaceTreeMoveStore(service.workspace_edits)
        binding = service.resources.workspace("files")
        writer = WorkspaceTreeWriter(binding.workspace)
        if phase == "after-move":
            agent = AgentSession(
                service, (await service.spawn("default", persistent=True)).agent_id
            )
            scope = "session:" + agent.session_id
            snapshot = writer.reader.capture("tree")
            prepared = writer.prepare(
                "tree", "dest/tree", expected_fingerprint=snapshot.fingerprint
            )
            request = TreeMovePreviewRequest(
                source_path="tree",
                target_path="dest/tree",
                expected_fingerprint=snapshot.fingerprint,
                request_key="preview",
            )
            run, _ = await ai.runtime.repository.begin_run(
                service._handle(agent.agent_id).topic_id,
                ChatRequest(text="internal source", idempotency_key="source"),
                agent_session_id=agent.session_id,
                agent_id=agent.agent_id,
                agent_owner_id=store.owner_id,
            )
            proposal, _ = await store.preview(
                scope,
                binding,
                request,
                prepared,
                model_source=TreeMoveModelSource(run.id, "preview"),
            )
            attempt, _ = await store.reserve(
                scope,
                TreeMoveAction(
                    proposal_id=proposal.view.proposal_id,
                    action="apply",
                    expected_revision=0,
                    request_key="apply",
                ),
                binding.fingerprint,
                prepared,
                model_source=TreeMoveModelSource(run.id, "apply"),
            )
            loop = asyncio.get_running_loop()

            def authorize():
                asyncio.run_coroutine_threadsafe(
                    store.assert_execution(scope, attempt.view.operation_id), loop
                ).result(timeout=5)

            def intent(value):
                asyncio.run_coroutine_threadsafe(
                    store.record_intent(scope, attempt.view.operation_id, value), loop
                ).result(timeout=5)

            effect = await asyncio.to_thread(
                writer.apply, prepared, authorize=authorize, record_intent=intent
            )
            assert (
                effect.receipt.status == "done"
                and (root / "files/dest/tree/sub/data.bin").read_bytes() == b"\0\xff"
            )
            metadata = {
                "package": str(krim_agent.__file__),
                "pid": os.getpid(),
                "agent_id": agent.agent_id,
                "scope": scope,
                "proposal_id": proposal.view.proposal_id,
                "operation_id": attempt.view.operation_id,
                "old_owner": store.owner_id,
                "run_id": run.id,
                "view": attempt.view.model_dump(mode="json"),
                "before": prepared.before.fingerprint,
                "counts": await counts(store.database),
                "effect_before_receipt": True,
                "provider_calls": provider.calls,
            }
            (root / "metadata.json").write_text(json.dumps(metadata))
            print(json.dumps(metadata), flush=True)
            assert (await asyncio.to_thread(sys.stdin.readline)).strip() == "release"
            await store.settle(scope, attempt.view.operation_id, effect)
            return
        assert phase == "recover"
        metadata = json.loads((root / "metadata.json").read_text())
        replay = 0

        def forbidden(*_args, **_kwargs):
            nonlocal replay
            replay += 1
            raise AssertionError("原SQL冷读/核实禁止重发rename或apply")

        move_module.rename_at = forbidden
        WorkspaceTreeWriter.apply = forbidden
        assert await service.restore_persistent() == 1
        agent = AgentSession(service, metadata["agent_id"])
        assert "session:" + agent.session_id == metadata["scope"]
        # 已发布Agent restore先收束原move；再次恢复应为0，不能第二次接纳。
        assert await store.recover_interrupted() == 0
        operation = await store.operation(metadata["scope"], metadata["operation_id"])
        assert operation.view.status == "uncertain" and operation.intent_recorded
        assert operation.prepared.before.fingerprint == metadata["before"]
        assert operation.model_source == TreeMoveModelSource(metadata["run_id"], "apply")
        stale = WorkspaceTreeMoveStore(service.workspace_edits)
        stale.owner_id = metadata["old_owner"]
        with_owned_error = False
        try:
            await stale.assert_execution(metadata["scope"], metadata["operation_id"])
        except ServiceError:
            with_owned_error = True
        assert with_owned_error
        effect = writer.reconcile(operation.prepared)
        result = await store.settle(
            metadata["scope"], metadata["operation_id"], effect, resolving=True
        )
        assert result.view.status == "done" and result.view.operation_id == metadata["operation_id"]
        assert (await store.proposal_by_key(metadata["scope"], "preview")).view.status == "applied"
        assert await counts(store.database) == [1, 1, 0, 2]
        assert replay == provider.calls == 0
        print(
            json.dumps(
                {
                    "package": str(krim_agent.__file__),
                    "pid": os.getpid(),
                    "counts": await counts(store.database),
                    "view": result.view.model_dump(mode="json"),
                    "sources_original_run_call": True,
                    "old_owner_fenced": with_owned_error,
                    "original_sql_blob": True,
                    "replay_calls": replay,
                    "provider_calls": provider.calls,
                }
            ),
            flush=True,
        )


asyncio.run(main())
