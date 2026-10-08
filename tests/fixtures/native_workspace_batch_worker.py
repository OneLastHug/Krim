"""真实持久Agent批次生产者；JSON行握手及stdin控制，供源码/独立安装包复用。"""

import asyncio
import json
import os
import sys
from pathlib import Path
from threading import Event

import krim_agent
from krim_agent.config import Settings
from krim_agent.providers import FakeProvider
from krim_agent.sdk import FileChangeSpec, PythonAI, WorkspaceChangeAction
from krim_agent.workspace_list import NativeBatchRequest, NativeBatchTarget
from krim_agent.workspace_writer import WorkspaceWriter


class CountingProvider(FakeProvider):
    def __init__(self):
        super().__init__(delay=0)
        self.calls = 0

    async def stream(self, messages, tools):
        self.calls += 1
        async for item in super().stream(messages, tools):
            yield item


async def allow(_definition, _context):
    return "allow"


async def paused_renew(*_arguments):
    # 保持执行任务真实在途，不让心跳取消信号代替持久batch fence的裁决。
    await asyncio.Event().wait()


async def produce(ai, directory, mode, provider):
    agent = await ai.agent(persistent=True)
    service = ai.agents()
    proposals = []
    for name in ("first.txt", "second.txt"):
        proposal = await agent.preview_file_change(
            FileChangeSpec(
                path=name,
                operation="write",
                expected_sha256=(await agent.read_file(name)).sha256,
                content=f"new:{name}\n",
            ),
            request_key=f"preview-{name}",
        )
        result = await agent.change_file(
            WorkspaceChangeAction(
                proposal_id=proposal.proposal_id,
                action="apply",
                expected_revision=0,
                request_key=f"apply-{name}",
            )
        )
        assert result.status == "done"
        assert not (await agent.finalize_file_change(result.operation_id)).cleanup_pending
        proposals.append(proposal.proposal_id)
    page = await agent.file_change_list()
    request = NativeBatchRequest(
        action="undo",
        snapshot_revision=page.snapshot_revision,
        targets=[
            NativeBatchTarget(proposal_id=item.proposal_id, expected_revision=item.revision)
            for item in page.items
        ],
        request_key="process-batch",
    )
    entered, release = Event(), Event()
    original_apply = WorkspaceWriter.apply
    original_reserve = service.workspace_edits.reserve
    original_finish = service.workspace_batch_store.finish_child
    original_save = service.manager.workspace_changes._save

    def stop():
        entered.set()
        if not release.wait(60):
            raise TimeoutError("父进程未释放批次暂停点")

    async def before_reserve(*args, **kwargs):
        if kwargs.get("batch_fence") is not None:
            await asyncio.to_thread(stop)
        return await original_reserve(*args, **kwargs)

    def before_lock(self, *args, **kwargs):
        stop()
        return original_apply(self, *args, **kwargs)

    async def before_summary(*args, **kwargs):
        await asyncio.to_thread(stop)
        return await original_finish(*args, **kwargs)

    async def before_receipt(*args, **kwargs):
        await asyncio.to_thread(stop)
        return await original_save(*args, **kwargs)

    service.workspace_batches._renew = paused_renew
    service.manager.workspace_changes._renew = paused_renew
    if mode == "before-reserve":
        service.workspace_edits.reserve = before_reserve
    elif mode == "before-lock":
        WorkspaceWriter.apply = before_lock
    elif mode == "crash-after-done":
        service.workspace_batch_store.finish_child = before_summary
    else:
        assert mode == "crash-after-write"
        service.manager.workspace_changes._save = before_receipt
    worker = None
    try:
        batch = await agent.start_file_change_batch(request)
        worker = service.workspace_batches._tasks[batch.batch_id][1]
        if not await asyncio.to_thread(entered.wait, 25):
            if worker.done():
                await worker
            raise TimeoutError("批次未到达暂停点")
        current = await agent.file_change_batch(batch.batch_id)
        metadata = {
            "phase": "paused",
            "mode": mode,
            "package": krim_agent.__file__,
            "pid": os.getpid(),
            "agent_id": agent.agent_id,
            "scope_id": current.scope_id,
            "batch_id": batch.batch_id,
            "request": request.model_dump(mode="json"),
            "proposals": proposals,
            "batch": current.model_dump(mode="json"),
            "provider_calls": provider.calls,
        }
        (directory / "metadata.json").write_text(json.dumps(metadata), encoding="utf-8")
        print(json.dumps(metadata), flush=True)
        instruction = (await asyncio.to_thread(sys.stdin.readline)).strip()
        if instruction == "crash" and mode.startswith("crash-"):
            # 不执行finally、会话close或批次汇总；模拟真实进程消失。
            os._exit(23)
        if instruction != "release":
            raise ValueError("无效批次释放指令")
        release.set()
        await worker
        finished = await agent.file_change_batch(batch.batch_id)
        print(
            json.dumps(
                {
                    "phase": "finished",
                    "batch": finished.model_dump(mode="json"),
                    "histories": {
                        identifier: [
                            item.model_dump(mode="json")
                            for item in await agent.file_change_history(identifier)
                        ]
                        for identifier in proposals
                    },
                    "provider_calls": provider.calls,
                }
            ),
            flush=True,
        )
    finally:
        release.set()
        if worker is not None:
            await asyncio.gather(worker, return_exceptions=True)
        WorkspaceWriter.apply = original_apply


async def recover(ai, directory, provider):
    metadata = json.loads((directory / "metadata.json").read_text(encoding="utf-8"))
    writes = 0

    def no_replay(*_args, **_kwargs):
        nonlocal writes
        writes += 1
        raise AssertionError("批次查询或恢复错误地重放了文件动作")

    original_apply = WorkspaceWriter.apply
    WorkspaceWriter.apply = no_replay
    try:
        assert await ai.agents().restore_persistent() == 1
        agent = ai.agent_session(metadata["agent_id"])
        batch = await agent.file_change_batch(metadata["batch_id"])
        assert batch.producer_status == "interrupted"
        assert batch.items[1].status == "skipped" and batch.items[1].operation is None
        operation = batch.items[0].operation
        assert operation is not None
        assert operation.operation_id == metadata["batch"]["items"][0]["operation"]["operation_id"]
        if metadata["mode"] == "crash-after-write":
            assert batch.status == "uncertain" and operation.status == "uncertain"
        else:
            assert batch.status == "interrupted" and operation.status == "done"
        assert await agent.file_change_batch(batch.batch_id) == batch
        assert await agent.file_change_batch_by_key("process-batch") == batch
        repeated = await agent.start_file_change_batch(
            NativeBatchRequest.model_validate(metadata["request"])
        )
        assert repeated == batch
        if operation.status == "uncertain":
            resolved = await agent.reconcile_file_change(operation.operation_id)
            assert resolved.status == "done" and resolved.operation_id == operation.operation_id
        finished = await agent.wait_file_change_batch(batch.batch_id, timeout_seconds=1)
        assert finished.status == "interrupted" and finished.items[0].status == "done"
        assert finished.items[1].status == "skipped" and finished.items[1].operation is None
        assert (await agent.read_file("second.txt")).content == "old:second.txt\n"
        assert (await agent.read_file("first.txt")).content == "new:first.txt\n"
        histories = {}
        for item in finished.items:
            history = await agent.file_change_history(item.proposal_id)
            assert len(history) == (2 if item.position == 0 else 1)
            histories[item.proposal_id] = [attempt.model_dump(mode="json") for attempt in history]
            for attempt in history:
                assert not (await agent.finalize_file_change(attempt.operation_id)).cleanup_pending
        assert writes == 0 and provider.calls == 0
        assert not list((directory / "files").glob("._krim-tx-*"))
        print(
            json.dumps(
                {
                    "phase": "recovered",
                    "package": krim_agent.__file__,
                    "batch": finished.model_dump(mode="json"),
                    "original_status": batch.status,
                    "histories": histories,
                    "writes": writes,
                    "provider_calls": provider.calls,
                }
            ),
            flush=True,
        )
    finally:
        WorkspaceWriter.apply = original_apply


async def main():
    mode, path = sys.argv[1:3]
    directory = Path(path)
    provider = CountingProvider()
    async with await PythonAI.create(
        settings=Settings(database_url=f"sqlite+aiosqlite:///{directory / 'batch.sqlite'}"),
        provider=provider,
    ) as ai:
        ai.runtime.tool_policy = allow
        ai.agents().register_workspace(
            "files", directory / "files", version="v1", writable=True, default=True
        )
        if mode == "recover":
            await recover(ai, directory, provider)
        else:
            await produce(ai, directory, mode, provider)


asyncio.run(main())
