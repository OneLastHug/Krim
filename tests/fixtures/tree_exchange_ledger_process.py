"""0044原双树SQL材料/模型来源、实际效果后SIGKILL与新进程原operation核实。"""

import asyncio
import json
import os
import sys
from pathlib import Path

from sqlalchemy import func, select

import krim_agent
import krim_agent.workspace_tree_exchange as exchange_module
from krim_agent.config import Settings
from krim_agent.persistence.database import (
    WorkspaceTreeExchangeAttemptRow,
    WorkspaceTreeExchangeLockRow,
    WorkspaceTreeExchangeModelSourceRow,
    WorkspaceTreeExchangeProposalRow,
)
from krim_agent.persistence.workspace_tree_exchanges import WorkspaceTreeExchangeStore
from krim_agent.protocol.domain import ChatRequest, ServiceError
from krim_agent.providers import FakeProvider
from krim_agent.sdk import AgentSession, PythonAI
from krim_agent.workspace_tree_exchange import WorkspaceTreeExchangeWriter
from krim_agent.workspace_tree_exchange_types import (
    TreeExchangeAction,
    TreeExchangeModelSource,
    TreeExchangePreviewRequest,
)


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
                WorkspaceTreeExchangeProposalRow,
                WorkspaceTreeExchangeAttemptRow,
                WorkspaceTreeExchangeLockRow,
                WorkspaceTreeExchangeModelSourceRow,
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
        store = WorkspaceTreeExchangeStore(service.workspace_edits)
        binding = service.resources.workspace("files")
        writer = WorkspaceTreeExchangeWriter(binding.workspace)
        if phase == "after-exchange":
            agent = AgentSession(
                service, (await service.spawn("default", persistent=True)).agent_id
            )
            scope = "session:" + agent.session_id
            left = writer.trees.reader.capture("left")
            right = writer.trees.reader.capture("right")
            prepared = writer.prepare(
                "left", "right", expected_left=left.fingerprint, expected_right=right.fingerprint
            )
            request = TreeExchangePreviewRequest(
                left_path="left",
                right_path="right",
                expected_left=left.fingerprint,
                expected_right=right.fingerprint,
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
                model_source=TreeExchangeModelSource(run.id, "preview"),
            )
            attempt, _ = await store.reserve(
                scope,
                TreeExchangeAction(
                    proposal_id=proposal.view.proposal_id,
                    action="apply",
                    expected_revision=0,
                    request_key="apply",
                ),
                binding.fingerprint,
                prepared,
                model_source=TreeExchangeModelSource(run.id, "apply"),
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
                and (root / "files/left/sub/data.bin").read_bytes() == b"new\0\xff"
                and (root / "files/right/sub/data.bin").read_bytes() == b"old\xff\0"
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
                "left_before": prepared.left.fingerprint,
                "right_before": prepared.right.fingerprint,
                "left_inode": effect.observed_left.entries[0].directory.inode,
                "right_inode": effect.observed_right.entries[0].directory.inode,
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

        exchange_module.rename_at = forbidden
        WorkspaceTreeExchangeWriter.apply = forbidden
        assert await service.restore_persistent() == 1
        agent = AgentSession(service, metadata["agent_id"])
        assert "session:" + agent.session_id == metadata["scope"]
        # 公共恢复已收束原过期操作；先直接核实SQL，不能靠查询再次过期掩盖断口。
        async with store.database.sessions() as sql:
            recovered = await sql.get(WorkspaceTreeExchangeAttemptRow, metadata["operation_id"])
            assert recovered.status == "uncertain"
            assert recovered.owner_id is None and recovered.lease_expires_at is None
        assert await store.recover_interrupted() == 0
        operation = await store.operation(metadata["scope"], metadata["operation_id"])
        assert operation.view.status == "uncertain" and operation.intent_recorded
        assert operation.prepared.left.fingerprint == metadata["left_before"]
        assert operation.prepared.right.fingerprint == metadata["right_before"]
        assert operation.model_source == TreeExchangeModelSource(metadata["run_id"], "apply")
        stale = WorkspaceTreeExchangeStore(service.workspace_edits)
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
                    "left_inode": result.effect.observed_left.entries[0].directory.inode,
                    "right_inode": result.effect.observed_right.entries[0].directory.inode,
                    "old_owner_fenced": with_owned_error,
                    "original_sql_blob": True,
                    "replay_calls": replay,
                    "provider_calls": provider.calls,
                }
            ),
            flush=True,
        )


asyncio.run(main())
