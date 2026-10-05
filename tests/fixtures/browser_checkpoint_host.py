"""浏览器宿主 Checkpoint：在临时目录保存并恢复真实文件。"""

import asyncio
import json
import os
import signal
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

from krim_agent.app_sdk import RemoteHostHandle
from krim_agent.checkpoint import CheckpointCheckResult, CheckpointExecution, CheckpointReceipt


async def main() -> None:
    stop = asyncio.Event()
    asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, stop.set)
    with TemporaryDirectory(prefix="krim-browser-checkpoint-") as folder:
        root = Path(folder)
        resource, backup = root / "resource.txt", root / "snapshot.txt"
        resource.write_text("原值", encoding="utf-8")
        mode = sys.argv[4]
        async with RemoteHostHandle(
            sys.argv[1], sys.argv[2], os.environ["KRIM_AGENT_API_KEY"]
        ) as host:

            async def create(execution: CheckpointExecution) -> CheckpointReceipt:
                backup.write_text(resource.read_text(encoding="utf-8"), encoding="utf-8")
                if mode == "create-error":
                    raise RuntimeError("private create failure")
                return CheckpointReceipt(
                    **execution.model_dump(
                        exclude={"title", "metadata", "snapshot_metadata", "snapshot_ref"}
                    ),
                    status="done",
                    snapshot_ref={"path": "snapshot.txt"},
                )

            async def rollback(execution: CheckpointExecution) -> CheckpointReceipt:
                resource.write_text(backup.read_text(encoding="utf-8"), encoding="utf-8")
                if mode == "rollback-error":
                    raise RuntimeError("private rollback failure")
                return CheckpointReceipt(
                    **execution.model_dump(
                        exclude={"title", "metadata", "snapshot_metadata", "snapshot_ref"}
                    ),
                    status="done",
                    snapshot_ref=execution.snapshot_ref,
                )

            async def can(check) -> CheckpointCheckResult:
                return CheckpointCheckResult(
                    topic_id=check.topic_id,
                    checkpoint_id=check.checkpoint_id,
                    revision=check.revision,
                    can_rollback=mode != "deny",
                )

            host.on_checkpoint_create(create)
            host.on_checkpoint_rollback(rollback)
            host.on_checkpoint_can_rollback(can)

            async def listen():
                async for _event in host.events():
                    pass

            listener = asyncio.create_task(listen())
            try:
                print(json.dumps({"event": "ready", "directory": folder}), flush=True)
                await stop.wait()
            finally:
                listener.cancel()
                await asyncio.gather(listener, return_exceptions=True)


asyncio.run(main())
