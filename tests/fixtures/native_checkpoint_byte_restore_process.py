"""原字节恢复ready/效果SQL提交后强杀，冷查询只消费同原事实。"""

import asyncio
import json
import os
import sqlite3
import sys
from pathlib import Path

import httpx

import krim_agent
import krim_agent.api.server as server_module
from krim_agent.config import Settings
from krim_agent.providers import FakeProvider
from krim_agent.sdk import CheckpointRestoreExecute, CheckpointRestorePreview, PythonAI
from krim_agent.workspace_writer import WorkspaceWriter

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from test_native_checkpoint_byte_capture import capture  # noqa: E402
from test_native_checkpoint_byte_restore import mixed_setup  # noqa: E402


async def main():
    phase, point, directory = sys.argv[1:4]
    root = Path(directory)
    async with await PythonAI.create(
        settings=Settings(database_url=f"sqlite+aiosqlite:///{root / 'state.sqlite'}"),
        provider=FakeProvider(delay=0),
    ) as ai:
        if phase == "produce":
            service, agent, _ = await mixed_setup(ai.runtime, root / "files", persistent=True)
            _, checkpoint = await capture(agent, ["file"])
            (root / "files/file").write_bytes(b"before\0\xff")
            store = service.workspace_restore_store
            request = CheckpointRestorePreview(
                checkpoint_id=checkpoint.checkpoint_id, request_key="restore-original"
            )

            async def hold(scope, plan_id, token):
                # ready已关闭原preparing资格；通知强杀前再await会与心跳取消竞争。
                # 在真正COMMIT返回后同步读取原SQL、立即写marker，之后才允许调度。
                with sqlite3.connect(root / "state.sqlite") as connection:
                    connection.row_factory = sqlite3.Row
                    row = connection.execute(
                        "SELECT plan_id,scope_id,checkpoint_id,baseline_checkpoint_id,token "
                        "FROM workspace_restore_plans WHERE plan_id=?",
                        (plan_id,),
                    ).fetchone()
                    items = connection.execute(
                        "SELECT operation_id FROM workspace_restore_items WHERE plan_id=? "
                        "ORDER BY position",
                        (plan_id,),
                    ).fetchall()
                    plan = {
                        key: row[key]
                        for key in (
                            "plan_id",
                            "scope_id",
                            "checkpoint_id",
                            "baseline_checkpoint_id",
                        )
                    }
                    plan["items"] = [
                        {
                            "operation": {"operation_id": item["operation_id"]}
                            if item["operation_id"] is not None
                            else None
                        }
                        for item in items
                    ]
                    if token is not None:
                        assert token == row["token"]
                    token = row["token"]
                metadata = {
                    "pid": os.getpid(),
                    "point": point,
                    "agent_id": agent.agent_id,
                    "plan": plan,
                    "token": token,
                    "inode": (root / "files/file").stat().st_ino,
                    "package": str(krim_agent.__file__),
                }
                (root / "metadata.json").write_text(json.dumps(metadata))
                print(json.dumps(metadata), flush=True)
                await asyncio.to_thread(sys.stdin.readline)
                raise AssertionError("应在原SQL落盘后被强杀")

            original_ready = store.ready

            async def pause_ready(scope, identifier, token, prepared):
                await original_ready(scope, identifier, token, prepared)
                if point == "ready":
                    await hold(scope, identifier, token)

            store.ready = pause_ready
            start = await agent.preview_file_checkpoint_restore(request)
            plan = await agent.wait_file_checkpoint_restore(start.plan_id, timeout_seconds=30)
            assert plan.phase == "ready", plan
            if point == "ready":
                await asyncio.Future()
            ready_revision = plan.revision
            original_settle = service.workspace_edits.settle

            async def pause_effect(*args, **kwargs):
                result = await original_settle(*args, **kwargs)
                (root / "execute-revision.txt").write_text(str(ready_revision))
                await hold(plan.scope_id, plan.plan_id, None)
                return result

            service.workspace_edits.settle = pause_effect
            await agent.execute_file_checkpoint_restore(
                CheckpointRestoreExecute(
                    plan_id=plan.plan_id,
                    expected_revision=ready_revision,
                    request_key="execute-original",
                )
            )
            await agent.wait_file_checkpoint_restore(plan.plan_id, timeout_seconds=30)
            return

        metadata_path = root / (
            "forensic-ready-metadata.json" if phase == "recover-forensic" else "metadata.json"
        )
        metadata = json.loads(metadata_path.read_text())
        service = ai.agents()
        service.register_workspace("files", root / "files", version="1", writable=True)
        service.register_checkpoint_bytes()
        calls = 0

        def forbidden(*_args, **_kwargs):
            nonlocal calls
            calls += 1
            raise AssertionError("冷查询不能重放Model/capture/verify/prepare/apply")

        FakeProvider.stream = forbidden
        for method in (
            "capture_file",
            "verify_captured",
            "prepare_checkpoint_restore",
            "prepare_restore",
            "apply",
        ):
            setattr(WorkspaceWriter, method, forbidden)
        assert await service.restore_persistent() == 1, service.restore_errors
        agent = ai.agent_session(metadata["agent_id"])
        original = await agent.file_checkpoint_restore(metadata["plan"]["plan_id"])
        assert original.baseline_capture_format == "bytes"
        assert original.phase == ("ready" if point == "ready" else "interrupted"), original
        assert original.status == ("ready" if point == "ready" else "done"), original
        assert original.checkpoint_id == metadata["plan"]["checkpoint_id"]
        assert original.baseline_checkpoint_id == metadata["plan"]["baseline_checkpoint_id"]
        assert (
            await agent.preview_file_checkpoint_restore(
                CheckpointRestorePreview(
                    checkpoint_id=original.checkpoint_id, request_key="restore-original"
                )
            )
            == original
        )
        if point == "effect":
            assert (
                original.items[0].operation.operation_id
                == metadata["plan"]["items"][0]["operation"]["operation_id"]
            )
            assert (
                await agent.execute_file_checkpoint_restore(
                    CheckpointRestoreExecute(
                        plan_id=original.plan_id,
                        expected_revision=int((root / "execute-revision.txt").read_text()),
                        request_key="execute-original",
                    )
                )
                == original
            )
        backup = await agent.file_checkpoint(original.baseline_checkpoint_id)
        saved = await service.workspace_checkpoint_store.captures(
            original.scope_id, backup.checkpoint_id
        )
        assert backup.capture_format == "bytes" and saved[0].snapshot.data == b"before\0\xff"
        expected = b"\0\xfftarget" if point == "effect" else b"before\0\xff"
        assert (root / "files/file").read_bytes() == expected
        assert (root / "files/file").stat().st_ino == metadata["inode"]
        server_module.AgentService = lambda *_args, **_kwargs: service
        app = server_module.create_app(runtime=ai.runtime)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
        ) as api:
            response = await api.get(
                f"/v1/agents/{agent.agent_id}/workspace/native-restores/by-key",
                params={"request_key": "restore-original"},
            )
            assert response.status_code == 200 and response.json() == original.model_dump(
                mode="json"
            )
        from krim_agent.protocol.domain import ServiceError

        try:
            await service.workspace_restore_store.renew(
                original.scope_id,
                original.plan_id,
                metadata["token"],
                "preparing" if point == "ready" else "executing",
            )
        except ServiceError as error:
            assert error.code == "workspace_restore_fenced"
        else:
            raise AssertionError("旧owner/token不得重新取得执行资格")
        assert calls == 0
        print(
            json.dumps(
                {
                    "point": point,
                    "plan_id": original.plan_id,
                    "baseline_checkpoint_id": backup.checkpoint_id,
                    "inode": metadata["inode"],
                    "replay_calls": calls,
                    "same_sdk_http": True,
                    "package": str(krim_agent.__file__),
                }
            ),
            flush=True,
        )


asyncio.run(main())
