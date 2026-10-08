"""独立生产者：在发布边界暂停，以行JSON/stdin协议供源码和安装包验证复用。"""

import asyncio
import json
import os
import sys
from threading import Event

import krim_agent
import krim_agent.workspace_writer as writer_module
from krim_agent.application.workspace_changes import WorkspaceChangeService
from krim_agent.persistence import Database, KrimRepository
from krim_agent.persistence.workspace_edits import WorkspaceEditStore
from krim_agent.tools import ToolContext
from krim_agent.tools.builtins import builtin_registry
from krim_agent.workspace import Workspace, WorkspaceBinding
from krim_agent.workspace_writer import WorkspaceWriter


async def paused_renew(_context, _operation_id, _abort):
    # 模拟生产者停顿；过期由另一进程推进，不能靠心跳设置abort证明安全性。
    await asyncio.Event().wait()


async def main():
    database_path, root, mode = sys.argv[1:4]
    if mode not in {"before-lock", "after-authorization"}:
        raise ValueError("未知发布暂停点")
    database = Database(f"sqlite+aiosqlite:///{database_path}")
    service = WorkspaceChangeService(WorkspaceEditStore(database))
    service._renew = paused_renew
    entered, release = Event(), Event()
    original_apply, original_rename = WorkspaceWriter.apply, writer_module.rename_at
    task = None

    def wait_for_parent():
        entered.set()
        if not release.wait(45):
            raise TimeoutError("父进程未在期限内释放发布暂停点")

    def before_lock(self, *args, **kwargs):
        wait_for_parent()
        return original_apply(self, *args, **kwargs)

    def after_authorization(*args, **kwargs):
        wait_for_parent()
        return original_rename(*args, **kwargs)

    try:
        await database.initialize()
        topic = await KrimRepository(database).create_topic("跨进程发布资格")
        with Workspace(root, writable=True) as workspace:
            context = ToolContext(
                builtin_registry(),
                topic_id=topic.id,
                workspace=WorkspaceBinding("files", "v1", workspace, workspace.fingerprint),
                origin="host",
            )
            proposal = await service.tool_operation(
                "preview",
                {
                    "spec": {
                        "path": "file.txt",
                        "operation": "write",
                        "expected_sha256": workspace.read("file.txt").sha256,
                        "content": "new\n",
                    },
                    "request_key": "preview",
                },
                context,
            )
            if mode == "before-lock":
                WorkspaceWriter.apply = before_lock
            else:
                writer_module.rename_at = after_authorization
            task = asyncio.create_task(
                service.tool_operation(
                    "change",
                    {
                        "proposal_id": proposal["proposal_id"],
                        "action": "apply",
                        "expected_revision": 0,
                        "request_key": "apply",
                    },
                    context,
                )
            )
            try:
                if not await asyncio.to_thread(entered.wait, 20):
                    if task.done():
                        await task
                    raise TimeoutError("生产者未到达指定发布暂停点")
                attempt = (
                    await service.store.history(f"topic:{topic.id}", proposal["proposal_id"])
                )[0]
                print(
                    json.dumps(
                        {
                            "phase": "paused",
                            "mode": mode,
                            "pid": os.getpid(),
                            "topic_id": topic.id,
                            "proposal_id": proposal["proposal_id"],
                            "operation_id": attempt.operation_id,
                            "owner_id": service.store.owner_id,
                            "package": krim_agent.__file__,
                        }
                    ),
                    flush=True,
                )
                if (await asyncio.to_thread(sys.stdin.readline)).strip() != "release":
                    raise ValueError("父进程释放消息无效")
                release.set()
                result = await task
                print(json.dumps({"phase": "finished", "result": result}), flush=True)
            finally:
                # 必须在Workspace退出前收束仍使用根目录fd的线程。
                release.set()
                await asyncio.gather(task, return_exceptions=True)
    finally:
        release.set()
        WorkspaceWriter.apply, writer_module.rename_at = original_apply, original_rename
        await service.close()
        await database.close()


asyncio.run(main())
