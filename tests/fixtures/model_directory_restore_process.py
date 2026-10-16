"""真实模型生产者原Run/call后置强杀；新进程全部消费者零重放原事实。"""

import asyncio
import json
import os
import sys
import time
from dataclasses import asdict
from pathlib import Path
from unittest.mock import patch

import httpx

import krim_agent
import krim_agent.api.server as server
import krim_agent.workspace_directory_restore_writer as physical
from krim_agent.config import Settings
from krim_agent.persistence.database import (
    WorkspaceDirectoryRestoreAttemptRow,
    WorkspaceDirectoryRestorePlanRow,
)
from krim_agent.protocol.domain import ServiceError
from krim_agent.providers import FakeProvider
from krim_agent.sdk import PythonAI
from krim_agent.workspace_directory_restore import DirectoryRestoreFence
from krim_agent.workspace_directory_restore_control import DIRECTORY_RESTORE_STEP_TOOLS
from krim_agent.workspace_directory_restore_writer import WorkspaceDirectoryRestoreWriter
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshotReader

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from test_directory_restore_control import drain  # noqa: E402
from test_directory_restore_public_consumers import cli  # noqa: E402
from test_model_directory_restores import Provider, setup, target  # noqa: E402
from test_tree_move_public_consumers import tcp_for  # noqa: E402

from krim_agent.client import KrimClient  # noqa: E402
from krim_agent.workspace_checkpoint import CheckpointModelSource  # noqa: E402


async def main():
    phase, point, folder = sys.argv[1:4]
    root = Path(folder)
    async with await PythonAI.create(
        settings=Settings(database_url=f"sqlite+aiosqlite:///{root / 'state.sqlite'}"),
        provider=FakeProvider(delay=0),
    ) as ai:
        if phase == "produce":
            (root / "files").mkdir()
            provider = Provider()
            service, agent, binding = await setup(
                ai.runtime, root / "files", provider, persistent=True
            )
            # 与原公开强杀门禁相同：合成测试使用5秒租约并等待自然到期。
            service.directory_restore_store.lease_seconds = 5
            captured = await target(agent)
            provider.target_id = captured.checkpoint_id
            (binding.workspace.root / "tree/nested/nul").write_bytes(b"current\x80\0")

            async def hold(definition, value, context):
                if (
                    definition.name
                    == DIRECTORY_RESTORE_STEP_TOOLS["prepare" if point == "backup" else "execute"]
                ):
                    fence = context.directory_restore
                    assert context.origin == "model" and context.run_id != "local"
                    assert context.source_call_id == (
                        "original-dir-preview" if point == "backup" else "original-dir-execute"
                    )
                    assert fence.model_source == CheckpointModelSource(
                        context.run_id, context.source_call_id
                    )
                    store = service.directory_restore_store
                    async with store.database.sessions() as sql:
                        plan = await sql.get(WorkspaceDirectoryRestorePlanRow, fence.plan_id)
                        before = (await store._baseline(sql, plan)).snapshot
                        metadata = {
                            "pid": os.getpid(),
                            "point": point,
                            "agent_id": agent.agent_id,
                            "scope": "session:" + agent.session_id,
                            "plan_id": plan.plan_id,
                            "operation_id": fence.operation_id,
                            "target_id": captured.checkpoint_id,
                            "fence": asdict(fence),
                            "backup": before.fingerprint,
                            "before_inode": before.entries[0].directory.inode,
                            "run_id": context.run_id,
                            "source_call_id": context.source_call_id,
                        }
                        if point == "effect":
                            metadata["staged_inode"] = (
                                store._prepared(plan).staged.entries[0].directory.inode
                            )
                    (root / "metadata.json").write_text(json.dumps(metadata))
                    print(
                        json.dumps(
                            {
                                "pid": os.getpid(),
                                "point": point,
                                "package": str(krim_agent.__file__),
                                "origin": context.origin,
                                "source_call_id": context.source_call_id,
                            }
                        ),
                        flush=True,
                    )
                    sys.stdin.readline()
                    raise AssertionError("真实模型原后置Hook必须被SIGKILL")
                return value

            service._handle(agent.agent_id).runtime.tools.add_global_post(hold)
            await agent.execute(
                "原模型完整目录预览与原子发布", idempotency_key="original-model-directory-restore"
            )
            raise AssertionError("原模型生产者必须在暂停点被强杀")

        metadata = json.loads((root / "metadata.json").read_text())
        # 恢复冻结定义必须保留原Provider身份；禁止将它替换成fake新定义。
        ai.runtime.provider = Provider()
        service = ai.agents()
        binding = service.register_workspace("files", root / "files", version="1", writable=True)
        service.register_directory_checkpoints()
        service.register_directory_restores()
        service.register_directory_checkpoint_restore_controls()
        replay = []

        def forbidden(*_args, **_kwargs):
            replay.append(True)
            raise AssertionError("公开冷消费者不得重新调用Model/Reader/Writer/namespace")

        FakeProvider.stream = forbidden
        Provider.stream = forbidden
        WorkspaceTreeSnapshotReader.capture = forbidden
        WorkspaceTreeSnapshotReader.verify = forbidden
        WorkspaceDirectoryRestoreWriter.prepare = forbidden
        WorkspaceDirectoryRestoreWriter.apply = forbidden
        WorkspaceDirectoryRestoreWriter._write_tree = forbidden
        physical.rename_at = forbidden
        await asyncio.sleep(max(0, metadata["lease"] - time.time()) + 0.02)
        assert await service.restore_persistent() == 1, service.restore_errors
        agent = ai.agent_session(metadata["agent_id"])
        original = await agent.directory_checkpoint_restore(metadata["plan_id"])
        assert original.phase == ("interrupted" if point == "backup" else "uncertain")
        assert original.backup.snapshot_fingerprint == metadata["backup"]
        assert original.operation_id == metadata["operation_id"]
        try:
            await service.directory_restore_store.renew(
                metadata["scope"],
                DirectoryRestoreFence(
                    **{
                        **metadata["fence"],
                        "model_source": CheckpointModelSource(**metadata["fence"]["model_source"]),
                    }
                ),
                binding,
                qualify=lambda: None,
            )
        except ServiceError:
            pass
        else:
            raise AssertionError("旧owner/token必须被拒绝")
        with patch.object(server, "AgentService", lambda *_args, **_kwargs: service):
            app = server.create_app(runtime=ai.runtime)
        base = f"/v1/agents/{agent.agent_id}/workspace/directory-restores"
        async with (
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
            ) as api,
            tcp_for(app) as url,
        ):
            if point == "effect":
                response = await api.post(base + f"/operations/{original.operation_id}/verify")
                assert response.status_code == 202, response.text
                await drain(service)
                original = await agent.directory_checkpoint_restore(original.plan_id)
                assert (
                    original.status == "done" and original.operation_id == metadata["operation_id"]
                )
                async with service.directory_restore_store.database.sessions() as sql:
                    row = await sql.get(WorkspaceDirectoryRestorePlanRow, original.plan_id)
                    prepared = service.directory_restore_store._prepared(row)
                    operation = await sql.get(
                        WorkspaceDirectoryRestoreAttemptRow, original.operation_id
                    )
                    from krim_agent.persistence.directory_restore_codec import decode_effect

                    effect = decode_effect(prepared, operation.effect_metadata)
                    assert (
                        effect.observed_after.entries[0].directory.inode == metadata["staged_inode"]
                    )
                    assert (
                        effect.retained_before.entries[0].directory.inode
                        == metadata["before_inode"]
                    )
                page = await agent.directory_checkpoint_restore_diff(original.plan_id)
                assert (
                    await api.get(base + f"/{original.plan_id}/diff")
                ).json() == page.model_dump(mode="json")
            expected = original.model_dump(mode="json")
            assert (await api.get(base + f"/{original.plan_id}")).json() == expected
            assert (
                await api.get(base + "/by-key", params={"request_key": "model-preview"})
            ).json() == expected

            def client_read():
                with KrimClient(url) as client:
                    assert (
                        client.agent_workspace_directory_restore(agent.agent_id, original.plan_id)
                        == original
                    )
                    assert (
                        client.agent_workspace_directory_restore_by_key(
                            agent.agent_id, "model-preview"
                        )
                        == original
                    )
                    assert client.agent_workspace_directory_restores(agent.agent_id) == [original]
                    if point == "effect":
                        assert (
                            client.agent_workspace_directory_restore_operation_by_key(
                                agent.agent_id, "model-execute"
                            ).operation_id
                            == original.operation_id
                        )

            await asyncio.to_thread(client_read)
            assert await cli(url, agent.agent_id, "get", original.plan_id) == expected
            assert await cli(url, agent.agent_id, "by-key", "model-preview") == expected
            assert await cli(url, agent.agent_id, "list") == [expected]
            assert (
                await cli(
                    url,
                    agent.agent_id,
                    "wait",
                    original.plan_id,
                    "--timeout-seconds",
                    "0",
                    expected=1 if point == "backup" else 0,
                )
                == expected
            )
            assert await agent.directory_checkpoint_restore(original.plan_id) == original
            phase_name = "preview" if point == "backup" else "execute"
            assert await service.directory_restore_store.source(
                metadata["scope"], original.plan_id, phase_name
            ) == CheckpointModelSource(metadata["run_id"], metadata["source_call_id"])
        assert not replay
        print(
            json.dumps(
                {
                    "pid": os.getpid(),
                    "point": point,
                    "plan_id": original.plan_id,
                    "operation_id": original.operation_id,
                    "status": original.status,
                    "same_sdk_http_client_cli": True,
                    "replays": len(replay),
                    "binary_hex": (binding.workspace.root / "tree/nested/nul").read_bytes().hex(),
                    "package": str(krim_agent.__file__),
                    "restore_lease_natural": True,
                    "synthetic_handle_expired": True,
                    "original_model_source_preserved": True,
                }
            ),
            flush=True,
        )


asyncio.run(main())
