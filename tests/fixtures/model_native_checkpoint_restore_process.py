"""模型恢复真实ready/逐文件效果SQL后SIGKILL；另进程只读原来源零重放。"""

import asyncio
import json
import os
import sqlite3
import sys
from pathlib import Path

import httpx
import pytest

import krim_agent
import krim_agent.api.server as server_module
from krim_agent.config import Settings
from krim_agent.protocol.domain import ServiceError
from krim_agent.sdk import AgentSession, PythonAI
from krim_agent.workspace_checkpoint import CheckpointModelSource
from krim_agent.workspace_writer import WorkspaceWriter

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from test_model_native_checkpoint_restores import RestoreProvider, setup  # noqa: E402


async def main():
    phase, point, directory = sys.argv[1:4]
    root = Path(directory)
    provider = RestoreProvider(root / "files", "bytes")
    async with await PythonAI.create(
        settings=Settings(database_url=f"sqlite+aiosqlite:///{root / 'state.sqlite'}"),
        provider=provider,
    ) as ai:
        if phase == "produce":
            service, agent = await setup(ai.runtime, root / "files", provider, persistent=True)
            store = service.workspace_restore_store

            async def hold(identifier):
                with sqlite3.connect(root / "state.sqlite") as sql:
                    sql.row_factory = sqlite3.Row
                    row = dict(
                        sql.execute(
                            "SELECT * FROM workspace_restore_plans WHERE plan_id=?", (identifier,)
                        ).fetchone()
                    )
                    sources = [
                        dict(item)
                        for item in sql.execute(
                            "SELECT phase,run_id,call_id "
                            "FROM workspace_restore_model_sources WHERE plan_id=? ORDER BY phase",
                            (identifier,),
                        )
                    ]
                    items = [
                        dict(item)
                        for item in sql.execute(
                            "SELECT operation_id,proposal_id "
                            "FROM workspace_restore_items WHERE plan_id=? ORDER BY position",
                            (identifier,),
                        )
                    ]
                    blobs = [
                        item[0].hex() if item[0] is not None else None
                        for item in sql.execute(
                            "SELECT content "
                            "FROM native_workspace_checkpoint_files "
                            "WHERE checkpoint_id=? ORDER BY position",
                            (row["baseline_checkpoint_id"],),
                        )
                    ]
                assert row["phase"] == ("ready" if point == "ready" else "executing")
                assert blobs == [b"current\n".hex(), b"changed\x00\xff".hex(), b"new\n".hex()]
                files = {
                    name: {"inode": path.stat().st_ino, "hex": path.read_bytes().hex()}
                    for name in ("plain", "binary", "absent")
                    if (path := root / "files" / name).exists()
                }
                metadata = {
                    "pid": os.getpid(),
                    "point": point,
                    "agent_id": agent.agent_id,
                    "plan_id": identifier,
                    "scope": row["scope_id"],
                    "baseline_id": row["baseline_checkpoint_id"],
                    "checkpoint_id": row["checkpoint_id"],
                    "token": row["token"],
                    "sources": sources,
                    "items": items,
                    "files": files,
                    "blobs": blobs,
                    "old_owner": store.owner_id,
                    "package": str(krim_agent.__file__),
                }
                (root / "metadata.json").write_text(json.dumps(metadata))
                print(json.dumps(metadata), flush=True)
                await asyncio.to_thread(sys.stdin.readline)
                raise AssertionError("应在真实模型恢复SQL后被强杀")

            original_ready = store.ready

            async def pause_ready(scope, identifier, token, values, **kwargs):
                await original_ready(scope, identifier, token, values, **kwargs)
                if point == "ready":
                    await hold(identifier)

            store.ready = pause_ready
            original_settle = service.workspace_edits.settle

            async def pause_effect(*args, **kwargs):
                result = await original_settle(*args, **kwargs)
                assert result.view.status == "done", result.view
                await hold(provider.preview.plan_id)
                return result

            if point == "effect":
                service.workspace_edits.settle = pause_effect
            await agent.execute("真实模型恢复SQL强杀", idempotency_key="process-original")
            raise AssertionError("应由原测试SIGKILL")

        metadata = json.loads((root / "metadata.json").read_text())
        service = ai.agents()

        async def allow(*_args):
            return "allow"

        ai.runtime.tool_policy = allow
        service.register_workspace(
            "files", root / "files", writable=True, version="1", default=True
        )
        service.register_checkpoint_bytes()
        service.register_checkpoint_controls()
        service.register_checkpoint_restore_controls()
        replay = []

        def forbidden(*_args, **_kwargs):
            replay.append(True)
            raise AssertionError("冷恢复不能调用模型或重放原捕获/准备/apply")

        provider.stream = forbidden
        for name in (
            "capture_file",
            "verify_captured",
            "prepare_checkpoint_restore",
            "prepare_restore",
            "apply",
        ):
            setattr(WorkspaceWriter, name, forbidden)
        service.workspace_checkpoints.create = forbidden
        service.workspace_checkpoints.create_model = forbidden
        assert await service.restore_persistent() == 1, service.restore_errors
        agent = AgentSession(service, metadata["agent_id"])
        plan = await agent.file_checkpoint_restore(metadata["plan_id"])
        assert plan.phase == ("ready" if point == "ready" else "interrupted"), plan
        assert plan.status == ("ready" if point == "ready" else "interrupted"), plan
        assert (
            plan.scope_id == metadata["scope"] and plan.checkpoint_id == metadata["checkpoint_id"]
        )
        assert plan.baseline_checkpoint_id == metadata["baseline_id"]
        assert await agent.file_checkpoint_restore_by_key("preview") == plan
        for source in metadata["sources"]:
            assert await service.workspace_restore_store.source(
                plan.scope_id, plan.plan_id, source["phase"]
            ) == CheckpointModelSource(source["run_id"], source["call_id"])
        preview = next(item for item in metadata["sources"] if item["phase"] == "preview")
        assert await service.workspace_checkpoint_store.source(
            plan.scope_id, metadata["baseline_id"]
        ) == CheckpointModelSource(preview["run_id"], preview["call_id"], plan.plan_id)
        captures = await service.workspace_checkpoint_store.captures(
            plan.scope_id, metadata["baseline_id"]
        )
        assert [
            item.snapshot.data.hex() if item.snapshot else None for item in captures
        ] == metadata["blobs"]
        if point == "effect":
            assert plan.items[0].operation.operation_id == metadata["items"][0]["operation_id"]
            assert plan.items[0].operation.status == "done"
            assert all(item.operation is None for item in plan.items[1:])
        for name, original in metadata["files"].items():
            path = root / "files" / name
            assert (
                path.read_bytes().hex() == original["hex"]
                and path.stat().st_ino == original["inode"]
            )
        server_module.AgentService = lambda *_args, **_kwargs: service
        app = server_module.create_app(runtime=ai.runtime)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
        ) as client:
            response = await client.get(
                f"/v1/agents/{agent.agent_id}/workspace/native-restores/{plan.plan_id}"
            )
            assert response.status_code == 200 and response.json() == plan.model_dump(mode="json")
        old_owner = service.workspace_restore_store.owner_id
        service.workspace_restore_store.owner_id = metadata["old_owner"]
        try:
            with pytest.raises(ServiceError) as fenced:
                await service.workspace_restore_store.execution(plan.scope_id, plan.plan_id)
            assert fenced.value.code == "workspace_restore_fenced"
        finally:
            service.workspace_restore_store.owner_id = old_owner
        assert not replay and provider.calls == 0
        print(
            json.dumps(
                {
                    "package": str(krim_agent.__file__),
                    "plan": plan.model_dump(mode="json"),
                    "sources": metadata["sources"],
                    "replay_calls": len(replay),
                    "provider_calls": provider.calls,
                    "same_sdk_http": True,
                    "old_owner_fenced": True,
                    "files": metadata["files"],
                }
            ),
            flush=True,
        )


asyncio.run(main())
