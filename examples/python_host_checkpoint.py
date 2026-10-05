"""离线宿主 Checkpoint：保存真实文件备份、修改后回滚，并持久保存回执。"""

import asyncio
import json
from pathlib import Path
from tempfile import TemporaryDirectory

from krim_agent.app import LocalApp
from krim_agent.checkpoint import (
    CheckpointCanRollback,
    CheckpointCheck,
    CheckpointCheckResult,
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
    with TemporaryDirectory(prefix="krim-host-checkpoint-example-") as folder:
        root = Path(folder)
        resource, backup = root / "resource.txt", root / "snapshot.txt"
        await asyncio.to_thread(resource.write_text, "原值", encoding="utf-8")
        database = Database(f"sqlite+aiosqlite:///{root / 'state.sqlite'}")
        await database.initialize()
        runtime = ChatRuntime(
            KrimRepository(database), FakeProvider(delay=0), builtin_registry(), Settings()
        )
        local = LocalApp(runtime)

        async def create(execution: CheckpointExecution) -> CheckpointReceipt:
            # 宿主只处理自己登记的资源，metadata 不能指示任意路径读取。
            content = await asyncio.to_thread(resource.read_text, encoding="utf-8")
            await asyncio.to_thread(backup.write_text, content, encoding="utf-8")
            return CheckpointReceipt(
                **execution.model_dump(
                    exclude={"title", "metadata", "snapshot_metadata", "snapshot_ref"}
                ),
                status="done",
                snapshot_ref={"resource": "example-backup"},
            )

        async def rollback(execution: CheckpointExecution) -> CheckpointReceipt:
            assert execution.snapshot_ref == {"resource": "example-backup"}
            content = await asyncio.to_thread(backup.read_text, encoding="utf-8")
            await asyncio.to_thread(resource.write_text, content, encoding="utf-8")
            return CheckpointReceipt(
                **execution.model_dump(
                    exclude={"title", "metadata", "snapshot_metadata", "snapshot_ref"}
                ),
                status="done",
                snapshot_ref=execution.snapshot_ref,
            )

        async def can(check: CheckpointCheck) -> CheckpointCheckResult:
            exists = await asyncio.to_thread(backup.exists)
            return CheckpointCheckResult(
                topic_id=check.topic_id,
                checkpoint_id=check.checkpoint_id,
                revision=check.revision,
                can_rollback=exists,
            )

        local.handle.on_checkpoint_create(create)
        local.handle.on_checkpoint_rollback(rollback)
        local.handle.on_checkpoint_can_rollback(can)
        try:
            topic = await local.handle.create_topic("宿主快照示例")
            created = await local.handle.create_checkpoint(
                topic.id,
                CheckpointCreate(
                    revision=0, metadata={"source": "offline-example"}, idempotency_key="snapshot"
                ),
            )
            assert created.status == "done"
            await asyncio.to_thread(resource.write_text, "修改后", encoding="utf-8")
            available = await local.handle.can_rollback_checkpoint(
                topic.id,
                CheckpointCanRollback(revision=2, checkpoint_id=created.execution.checkpoint_id),
            )
            assert available.can_rollback
            rolled = await local.handle.rollback_checkpoint(
                topic.id,
                CheckpointRollback(
                    revision=2,
                    checkpoint_id=created.execution.checkpoint_id,
                    idempotency_key="rollback",
                ),
            )
            assert rolled.status == "done"
            assert await asyncio.to_thread(resource.read_text, encoding="utf-8") == "原值"
            print(
                json.dumps(
                    {
                        "head": "0014_host_checkpoints",
                        "status": rolled.status,
                        "resource": "原值",
                        "revision": (await local.handle.checkpoints(topic.id)).revision,
                    },
                    ensure_ascii=False,
                )
            )
        finally:
            await local.close()
            await runtime.close()
            await database.close()


asyncio.run(main())
