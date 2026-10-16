"""原缺失模型叶真实SQL后SIGKILL；新进程只读核实且不恢复旧写资格。"""

import asyncio
import json
import os
import sys
import time
from dataclasses import asdict
from pathlib import Path
from unittest.mock import patch

import httpx
from sqlalchemy import select

import krim_agent
import krim_agent.api.server as server
import krim_agent.workspace_missing_directory_writer as physical
from krim_agent.application.agents import AgentService
from krim_agent.config import Settings
from krim_agent.persistence.database import (
    WorkspaceMissingDirectoryRestoreModelSourceRow as Source,
)
from krim_agent.persistence.database import (
    WorkspaceMissingDirectoryRestorePlanRow as Plan,
)
from krim_agent.protocol.domain import ServiceError
from krim_agent.providers import FakeProvider, ModelText, ModelToolCall
from krim_agent.sdk import AgentSession, PythonAI
from krim_agent.workspace_checkpoint import CheckpointModelSource
from krim_agent.workspace_directory_absence import WorkspaceDirectoryAbsenceReader
from krim_agent.workspace_missing_directory_restore import MissingDirectoryRestoreFence
from krim_agent.workspace_missing_directory_restore_control import (
    MISSING_DIRECTORY_RESTORE_STEP_TOOLS,
)
from krim_agent.workspace_missing_directory_writer import WorkspaceMissingDirectoryRestoreWriter
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshotReader

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from test_missing_directory_restore_public_consumers import cli  # noqa: E402
from test_model_missing_directory_restores import VERIFY, Provider, seed_target, setup  # noqa: E402
from test_tree_move_public_consumers import tcp_for  # noqa: E402

from krim_agent.client import KrimClient  # noqa: E402


async def main():
    phase, point, folder = sys.argv[1:4]
    root = Path(folder)
    files = root / "files"
    async with await PythonAI.create(
        settings=Settings(database_url=f"sqlite+aiosqlite:///{root / 'state.sqlite'}"),
        provider=FakeProvider(delay=0),
    ) as ai:
        if phase == "produce":
            files.mkdir()
            provider = Provider()
            service, agent, _ = await setup(ai.runtime, files, provider, persistent=True)
            service.missing_directory_restore_store.lease_seconds = 5
            provider.target_id = await seed_target(agent, files)

            async def hold(definition, value, context):
                selected = MISSING_DIRECTORY_RESTORE_STEP_TOOLS[
                    "prepare" if point == "absence" else "execute"
                ]
                if definition.name != selected:
                    return value
                fence = context.directory_restore
                assert fence.model_source == CheckpointModelSource(
                    context.run_id, context.source_call_id
                )
                assert context.origin == "model" and context.run_id != "local"
                async with ai.runtime.repository.database.sessions() as sql:
                    plan = await sql.get(Plan, fence.plan_id)
                    metadata = {
                        "agent_id": agent.agent_id,
                        "scope": "session:" + agent.session_id,
                        "plan_id": plan.plan_id,
                        "operation_id": fence.operation_id,
                        "absence": plan.absence_fingerprint,
                        "run_id": context.run_id,
                        "call_id": context.source_call_id,
                        "fence": asdict(fence),
                        "inode": (files / "tree").stat().st_ino if point == "effect" else None,
                    }
                with (root / "metadata.json").open("w") as output:
                    json.dump(metadata, output)
                    output.flush()
                    os.fsync(output.fileno())
                print(
                    json.dumps(
                        {
                            "pid": os.getpid(),
                            "point": point,
                            "origin": context.origin,
                            "call_id": context.source_call_id,
                            "package": str(krim_agent.__file__),
                        }
                    ),
                    flush=True,
                )
                sys.stdin.readline()
                raise AssertionError("原真实模型叶后置Hook必须被SIGKILL")

            service._handle(agent.agent_id).runtime.tools.add_global_post(hold)
            await agent.execute("原缺失模型真实强杀", idempotency_key="model-sigkill")
            raise AssertionError("原生产者不能越过强杀窗口")

        metadata = json.loads((root / "metadata.json").read_text())
        await asyncio.sleep(max(0, metadata["lease"] - time.time()) + 0.02)

        async def allow(_definition, _context):
            return "allow"

        ai.runtime.provider, ai.runtime.tool_policy = Provider(), allow
        service = AgentService(ai.runtime, ai.runtime.settings)
        binding = service.register_workspace(
            "files", files, version="1", writable=True, default=True
        )
        service.register_directory_checkpoints()
        service.register_missing_directory_restores()
        service.register_missing_directory_checkpoint_restore_controls()
        replays = []

        def forbidden(*_args, **_kwargs):
            replays.append(True)
            raise AssertionError("冷恢复不能重放原Model/Reader/Writer/namespace")

        Provider.stream = FakeProvider.stream = forbidden
        WorkspaceDirectoryAbsenceReader.capture = forbidden
        WorkspaceTreeSnapshotReader.capture = forbidden
        WorkspaceMissingDirectoryRestoreWriter.prepare = forbidden
        WorkspaceMissingDirectoryRestoreWriter.apply = forbidden
        physical.rename_at = forbidden
        try:
            await service.restore(metadata["agent_id"])
            await service.recover_workspace_edits()
            current = await service.workspace_missing_directory_restore(
                metadata["agent_id"], metadata["plan_id"]
            )
            assert current.phase == ("interrupted" if point == "absence" else "uncertain")
            assert current.operation_id == metadata["operation_id"]
            assert current.baseline.fingerprint == metadata["absence"]
            assert (
                await service.workspace_missing_directory_restore_by_key(
                    metadata["agent_id"], "model-preview"
                )
                == current
            )
            store = service.missing_directory_restore_store
            source_phase = "preview" if point == "absence" else "execute"
            original = await store.source(metadata["scope"], current.plan_id, source_phase)
            assert original == CheckpointModelSource(metadata["run_id"], metadata["call_id"])
            old = MissingDirectoryRestoreFence(**{**metadata["fence"], "model_source": original})
            try:
                await store.renew(metadata["scope"], old, binding, qualify=lambda: None)
            except ServiceError:
                pass
            else:
                raise AssertionError("旧owner/token不能恢复资格")

            class VerifyProvider:
                name = Provider.name

                def __init__(self):
                    self.calls, self.reply = 0, None

                async def stream(self, messages, tools):
                    self.calls += 1
                    if self.calls == 1:
                        yield ModelToolCall(
                            id="cold-model-verify",
                            name=VERIFY,
                            arguments={"operation_id": current.operation_id},
                        )
                    else:
                        self.reply = json.loads(messages[-1].content)
                        yield ModelText(text="新进程仅核实原未知操作")

            verifier = VerifyProvider()
            if point == "effect":
                agent_id = metadata["agent_id"]
                service._handle(agent_id).runtime.provider = verifier
                try:
                    await AgentSession(service, agent_id).execute(
                        "只读核实原操作", idempotency_key="cold-verify"
                    )
                except ServiceError as error:
                    assert error.code == "incomplete_tool_group"
                else:
                    raise AssertionError("原未闭合工具组不能绕过原会话安全门禁")
                assert verifier.calls == 0
                await service.workspace_verify_missing_directory_restore(
                    agent_id, current.operation_id
                )
                await service.workspace_missing_directory_restore_wait(agent_id, current.plan_id)
                final = await service.workspace_missing_directory_restore(agent_id, current.plan_id)
                assert final.phase == "uncertain" and final.operation_id == current.operation_id
                assert (
                    final.receipt.error_code == "workspace_missing_directory_restore_unverifiable"
                )
                assert await store.source(metadata["scope"], current.plan_id, "execute") == original
                assert await store.source(metadata["scope"], current.plan_id, "verify") is None
                assert (files / "tree").stat().st_ino == metadata["inode"]
                assert (files / "tree/nested/nul").read_bytes() == b"\0\xff"
            else:
                assert not (files / "tree").exists()
            current = await service.workspace_missing_directory_restore(
                metadata["agent_id"], current.plan_id
            )
            session = AgentSession(service, metadata["agent_id"])
            assert await session.missing_directory_checkpoint_restore(current.plan_id) == current
            assert (
                await session.missing_directory_checkpoint_restore_by_key("model-preview")
                == current
            )
            expected = current.model_dump(mode="json")
            with patch.object(server, "AgentService", lambda *_args, **_kwargs: service):
                app = server.create_app(runtime=ai.runtime)
            base = f"/v1/agents/{session.agent_id}/workspace/missing-directory-restores"
            async with (
                httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
                ) as api,
                tcp_for(app) as url,
            ):
                assert (await api.get(base + "/" + current.plan_id)).json() == expected
                assert (
                    await api.get(base + "/by-key", params={"request_key": "model-preview"})
                ).json() == expected
                assert (await api.get(base)).json() == [expected]
                page = operation = None
                if point == "effect":
                    page = await session.missing_directory_checkpoint_restore_diff(current.plan_id)
                    operation = await session.missing_directory_checkpoint_restore_operation(
                        current.operation_id
                    )
                    assert (
                        await api.get(base + "/" + current.plan_id + "/diff")
                    ).json() == page.model_dump(mode="json")
                    assert (
                        await api.get(
                            base + "/operations/by-key", params={"request_key": "model-execute"}
                        )
                    ).json() == operation.model_dump(mode="json")

                def consume():
                    with KrimClient(url) as client:
                        assert (
                            client.agent_workspace_missing_directory_restore(
                                session.agent_id, current.plan_id
                            )
                            == current
                        )
                        assert (
                            client.agent_workspace_missing_directory_restore_by_key(
                                session.agent_id, "model-preview"
                            )
                            == current
                        )
                        assert client.agent_workspace_missing_directory_restores(
                            session.agent_id
                        ) == [current]
                        if point == "effect":
                            assert (
                                client.agent_workspace_missing_directory_restore_diff(
                                    session.agent_id, current.plan_id
                                )
                                == page
                            )
                            assert (
                                client.agent_workspace_missing_directory_restore_operation(
                                    session.agent_id, current.operation_id
                                )
                                == operation
                            )
                            assert (
                                client.agent_workspace_missing_directory_restore_operation_by_key(
                                    session.agent_id, "model-execute"
                                )
                                == operation
                            )

                await asyncio.to_thread(consume)
                assert await cli(url, session.agent_id, "get", current.plan_id) == expected
                assert await cli(url, session.agent_id, "by-key", "model-preview") == expected
                assert await cli(url, session.agent_id, "list") == [expected]
                if point == "effect":
                    assert await cli(
                        url, session.agent_id, "diff", current.plan_id
                    ) == page.model_dump(mode="json")
                    assert await cli(
                        url, session.agent_id, "operation", current.operation_id
                    ) == operation.model_dump(mode="json")
                    assert await cli(
                        url, session.agent_id, "operation-by-key", "model-execute"
                    ) == operation.model_dump(mode="json")
            async with store.database.sessions() as sql:
                rows = list(await sql.scalars(select(Source)))
            assert len(rows) == (2 if point == "effect" else 1)
            assert not replays and not service.missing_directory_restores._tasks
            assert store.database.engine.sync_engine.pool.checkedout() == 0
            print(
                json.dumps(
                    {
                        "pid": os.getpid(),
                        "point": point,
                        "plan_id": current.plan_id,
                        "operation_id": current.operation_id,
                        "original_source": True,
                        "natural_lease_expiry": time.time() > metadata["lease"],
                        "replays": len(replays),
                        "new_verify_calls": verifier.calls,
                        "public_consumers_same": True,
                        "package": str(krim_agent.__file__),
                    }
                )
            )
        finally:
            await service.close()


asyncio.run(main())
