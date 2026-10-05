"""独立安装包的宿主 Checkpoint、真实文件、0014、API/UI 资源验证。"""

import asyncio
import json
from pathlib import Path
from tempfile import TemporaryDirectory

import httpx
from sqlalchemy import text

import krim_agent
from krim_agent.api import create_app
from krim_agent.app import LocalApp
from krim_agent.checkpoint import (
    CheckpointCreate,
    CheckpointExecution,
    CheckpointReceipt,
    CheckpointRollback,
)
from krim_agent.config import Settings
from krim_agent.persistence import Database, KrimRepository
from krim_agent.providers import FakeProvider
from krim_agent.runtime import ChatRuntime
from krim_agent.tools.builtins import builtin_registry


async def main() -> None:
    assert "/site-packages/" in krim_agent.__file__, krim_agent.__file__
    with TemporaryDirectory(prefix="krim-wheel-host-checkpoint-") as folder:
        root = Path(folder)
        resource, backup = root / "resource.txt", root / "snapshot.txt"
        await asyncio.to_thread(resource.write_text, "原值")
        database = Database(f"sqlite+aiosqlite:///{root / 'state.sqlite'}")
        runtime = ChatRuntime(
            KrimRepository(database), FakeProvider(delay=0), builtin_registry(), Settings()
        )
        local, app = LocalApp(runtime), create_app(runtime=runtime)

        async def create(execution: CheckpointExecution) -> CheckpointReceipt:
            await asyncio.to_thread(backup.write_text, await asyncio.to_thread(resource.read_text))
            return CheckpointReceipt(
                **execution.model_dump(
                    exclude={"title", "metadata", "snapshot_metadata", "snapshot_ref"}
                ),
                status="done",
                snapshot_ref="wheel-backup",
            )

        async def rollback(execution: CheckpointExecution) -> CheckpointReceipt:
            await asyncio.to_thread(resource.write_text, await asyncio.to_thread(backup.read_text))
            return CheckpointReceipt(
                **execution.model_dump(
                    exclude={"title", "metadata", "snapshot_metadata", "snapshot_ref"}
                ),
                status="done",
                snapshot_ref=execution.snapshot_ref,
            )

        local.handle.on_checkpoint_create(create)
        local.handle.on_checkpoint_rollback(rollback)
        try:
            async with (
                app.router.lifespan_context(app),
                httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
                ) as api,
            ):
                topic = await local.handle.create_topic("wheel checkpoint")
                created = await local.handle.create_checkpoint(
                    topic.id, CheckpointCreate(revision=0, idempotency_key="create")
                )
                await asyncio.to_thread(resource.write_text, "修改后")
                rolled = await local.handle.rollback_checkpoint(
                    topic.id,
                    CheckpointRollback(
                        revision=2,
                        checkpoint_id=created.execution.checkpoint_id,
                        idempotency_key="rollback",
                    ),
                )
                assert (
                    rolled.status == "done"
                    and await asyncio.to_thread(resource.read_text) == "原值"
                )
                assert (await api.get(f"/v1/topics/{topic.id}/checkpoints")).json()["checkpoints"][
                    0
                ]["status"] == "rolled_back"
                assert (await api.get("/ui/checkpoint-ui.js")).status_code == 200
                async with database.engine.connect() as connection:
                    head = (
                        await connection.execute(text("SELECT version_num FROM alembic_version"))
                    ).scalar()
                assert head == "0014_host_checkpoints"
                print(
                    json.dumps(
                        {
                            "import": "site-packages",
                            "head": head,
                            "resource": "原值",
                            "status": rolled.status,
                            "asset": "checkpoint-ui.js",
                        },
                        ensure_ascii=False,
                    )
                )
        finally:
            await local.close()
            await runtime.close()
            await database.close()


asyncio.run(main())
