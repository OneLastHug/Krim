"""真实强杀后新进程的SDK/HTTP/TCP Client/CLI消费同一原回退。"""

import asyncio
import json
import os
import runpy
import sys
import time
from pathlib import Path

import pytest
from sqlalchemy import select

import krim_agent
import krim_agent.workspace_tree_writer as physical
from krim_agent.config import Settings
from krim_agent.persistence.database import (
    WorkspaceTreeMoveAttemptRow,
    WorkspaceTreeMoveModelSourceRow,
)
from krim_agent.providers import FakeProvider
from krim_agent.sdk import AgentSession, MissingDirectoryRollbackQuery, PythonAI
from krim_agent.workspace_tree_writer import WorkspaceTreeWriter

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from test_missing_directory_rollback_public_consumers import cli  # noqa: E402
from test_tree_move_public_consumers import api_for, tcp_for  # noqa: E402

phase, origin, point, directory = sys.argv[1:5]
if phase == "produce":
    runpy.run_path(
        str(Path(__file__).with_name("missing_baseline_rollback_control_process.py")),
        run_name="__main__",
    )
    raise SystemExit(0)


class NoProvider(FakeProvider):
    def __init__(self):
        super().__init__(delay=0)
        self.calls = 0

    async def stream(self, messages, tools):
        self.calls += 1
        raise AssertionError("公开冷恢复不得调用模型")
        yield


async def recover():
    root = Path(directory)
    metadata = json.loads((root / "metadata.json").read_text())
    observed = json.loads((root / "observed.json").read_text())
    async with await PythonAI.create(
        settings=Settings(database_url=f"sqlite+aiosqlite:///{root / 'state.sqlite'}"),
        provider=NoProvider(),
    ) as ai:

        async def policy(definition, context):
            if context.agent_id != metadata["agent_id"]:
                return "deny"
            if definition.name == "missing_directory_rollback_reconcile" and context.arguments == {
                "operation_id": metadata["operation_id"]
            }:
                return "ask"
            return "allow" if definition.risk == "read" else "deny"

        ai.runtime.tool_policy = policy
        service = ai.agents()
        service.register_workspace(
            "files", root / "files", version="1", writable=True, default=True
        )
        service.register_directory_checkpoints()
        service.register_missing_directory_restores()
        service.register_missing_directory_rollbacks()
        store = service.manager.tree_moves.store
        async with store.database.sessions() as sql:
            row = await sql.get(WorkspaceTreeMoveAttemptRow, metadata["operation_id"])
            deadline = row.lease_expires_at
        if deadline is not None:
            await asyncio.sleep(max(0, deadline - time.time()) + 0.05)
        replays = []

        def forbidden(*_args, **_kwargs):
            replays.append(True)
            raise AssertionError("公开恢复禁止prepare/apply/rename")

        WorkspaceTreeWriter.prepare = forbidden
        WorkspaceTreeWriter.apply = forbidden
        physical.rename_at = forbidden
        assert await service.restore_persistent() == 1
        assert await service.restore_persistent() == 0
        agent = AgentSession(service, metadata["agent_id"])
        original = await agent.query_missing_directory_rollback(
            MissingDirectoryRollbackQuery(kind="operation", identifier=metadata["operation_id"])
        )
        async with (
            api_for(ai.runtime, service, pytest.MonkeyPatch()) as (app, api),
            tcp_for(app) as url,
        ):
            base = f"/v1/agents/{agent.agent_id}/workspace/missing-directory-rollbacks"
            response = await api.post(
                base + f"/operations/{metadata['operation_id']}/reconcile",
                params={"confirmed": "true"},
            )
            assert response.status_code == 202, response.text
            resolved = response.json()
            assert resolved["operation"]["status"] == "done"
            assert resolved["operation"]["operation_id"] == metadata["operation_id"]
            assert resolved["source_plan_id"] == metadata["source_plan_id"]
            assert resolved["source_operation_id"] == metadata["source_operation_id"]
            sdk = await agent.query_missing_directory_rollback(
                MissingDirectoryRollbackQuery(kind="operation_by_key", request_key="rollback-apply")
            )
            assert sdk.operation.model_dump(mode="json") == resolved
            proposal = (
                await agent.query_missing_directory_rollback(
                    MissingDirectoryRollbackQuery(kind="by_key", request_key="rollback-preview")
                )
            ).proposal
            assert proposal.rollback_state == "preserved" and proposal.baseline.kind == "absent"
            assert (
                await api.get(
                    base + "/query",
                    params={"kind": "proposal", "identifier": metadata["proposal_id"]},
                )
            ).json()["proposal"] == proposal.model_dump(mode="json")

            def tcp_reads():
                from krim_agent.client import KrimClient

                with KrimClient(url) as client:
                    assert (
                        client.agent_missing_directory_rollback_query(
                            agent.agent_id,
                            MissingDirectoryRollbackQuery(
                                kind="operation", identifier=metadata["operation_id"]
                            ),
                        ).operation
                        == sdk.operation
                    )
                    assert (
                        client.agent_missing_directory_rollback_query(
                            agent.agent_id,
                            MissingDirectoryRollbackQuery(
                                kind="proposal", identifier=metadata["proposal_id"]
                            ),
                        ).proposal
                        == proposal
                    )

            await asyncio.to_thread(tcp_reads)
            assert (
                await cli(
                    url,
                    agent.agent_id,
                    "query",
                    None,
                    "--kind",
                    "operation_by_key",
                    "--request-key",
                    "rollback-apply",
                )
            )["operation"] == resolved
            assert (
                await cli(url, agent.agent_id, "reconcile", metadata["operation_id"], "--confirm")
                == resolved
            )
            assert (
                await cli(
                    url,
                    agent.agent_id,
                    "query",
                    None,
                    "--kind",
                    "history",
                    "--identifier",
                    metadata["proposal_id"],
                )
            )["history"]["items"] == [resolved]
        async with store.database.sessions() as sql:
            sources = list(
                await sql.scalars(
                    select(WorkspaceTreeMoveModelSourceRow).where(
                        WorkspaceTreeMoveModelSourceRow.proposal_id == metadata["proposal_id"]
                    )
                )
            )
        assert len(sources) == (2 if origin == "model" else 0)
        if origin == "model":
            assert {(item.run_id, item.call_id) for item in sources} == {
                (metadata["model_run_id"], "original-preview"),
                (metadata["model_run_id"], "original-change"),
            }
        preserved = root / "files/preserved"
        assert preserved.stat().st_ino == observed["inode"]
        assert (preserved / "nested/nul").read_bytes() == b"user\0modified\xff"
        assert (preserved / "user-extra").read_bytes() == b"preserve me"
        assert not (root / "files/tree").exists()
        assert not replays and ai.runtime.provider.calls == 0
        print(
            json.dumps(
                {
                    "pid": os.getpid(),
                    "origin": origin,
                    "point": point,
                    "operation_id": metadata["operation_id"],
                    "source_plan_id": metadata["source_plan_id"],
                    "source_operation_id": metadata["source_operation_id"],
                    "old_status": original.operation.operation.status,
                    "status": "done",
                    "natural_move_lease_expiry": deadline is not None,
                    "consumers_same": True,
                    "inode": preserved.stat().st_ino,
                    "replays": 0,
                    "provider_calls": 0,
                    "package": str(krim_agent.__file__),
                }
            ),
            flush=True,
        )


asyncio.run(recover())
