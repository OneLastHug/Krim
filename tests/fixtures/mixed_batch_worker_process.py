"""真实混合Worker生产/暂停/恢复进程；源码及项目外wheel共用。"""

import asyncio
import json
import os
import sys
from pathlib import Path
from threading import Event

import krim_agent
from krim_agent.config import Settings
from krim_agent.providers import FakeProvider
from krim_agent.sdk import FileChangeSpec, MixedBatchRequest, MixedBatchTarget, PythonAI
from krim_agent.workspace_directory import DirectoryChangeSpec, WorkspaceDirectoryWriter
from krim_agent.workspace_writer import WorkspaceWriter


class Provider(FakeProvider):
    def __init__(self):
        super().__init__(delay=0)
        self.calls = 0

    async def stream(self, messages, tools):
        self.calls += 1
        async for item in super().stream(messages, tools):
            yield item


async def allow(_definition, _context):
    return "allow"


async def paused(*_args):
    await asyncio.Event().wait()


async def produce(ai, root, phase, source, provider):
    agent = await ai.agent(persistent=True)
    service = ai.agents()
    file = await agent.preview_file_change(
        FileChangeSpec(path="file.txt", operation="write", content="data", expected_sha256=None),
        request_key="file",
    )
    directory = await agent.preview_directory_change(
        DirectoryChangeSpec(path="empty", operation="create", mode=0o750), request_key="directory"
    )
    targets = [
        MixedBatchTarget(source="file", proposal_id=file.proposal_id, expected_revision=0),
        MixedBatchTarget(
            source="directory", proposal_id=directory.proposal_id, expected_revision=0
        ),
    ]
    if source == "directory":
        targets.reverse()
    request = MixedBatchRequest(action="apply", request_key="mixed-process", targets=targets)
    entered, release = Event(), Event()

    def stop():
        entered.set()
        if not release.wait(60):
            raise TimeoutError("父进程没有释放暂停点")

    original_store = (
        service.workspace_edits if source == "file" else service.manager.directory_changes.store
    )
    writer = WorkspaceWriter if source == "file" else WorkspaceDirectoryWriter
    original_reserve, original_apply = original_store.reserve, writer.apply
    original_settle = original_store.settle
    original_finish = service.workspace_mixed_batch_store.finish_child

    async def before_reserve(*args, **kwargs):
        if kwargs.get("mixed_fence") is not None:
            await asyncio.to_thread(stop)
        return await original_reserve(*args, **kwargs)

    def before_lock(self, *args, **kwargs):
        stop()
        return original_apply(self, *args, **kwargs)

    async def before_receipt(*args, **kwargs):
        await asyncio.to_thread(stop)
        return await original_settle(*args, **kwargs)

    async def before_summary(*args, **kwargs):
        await asyncio.to_thread(stop)
        return await original_finish(*args, **kwargs)

    service.workspace_mixed_batches._renew = paused
    if source == "file":
        service.manager.workspace_changes._renew = paused
    else:
        service.manager.directory_changes._renew = paused
    if phase == "before-reserve":
        original_store.reserve = before_reserve
    elif phase == "before-lock":
        writer.apply = before_lock
    elif phase == "after-write":
        original_store.settle = before_receipt
    else:
        assert phase == "after-done"
        service.workspace_mixed_batch_store.finish_child = before_summary
    worker = None
    try:
        batch = await agent.start_mixed_change_batch(request)
        worker = service.workspace_mixed_batches._tasks[batch.batch_id][1]
        assert await asyncio.to_thread(entered.wait, 25)
        current = await agent.mixed_change_batch(batch.batch_id)
        metadata = {
            "phase": "paused",
            "pause": phase,
            "source": source,
            "package": krim_agent.__file__,
            "pid": os.getpid(),
            "agent_id": agent.agent_id,
            "scope_id": current.scope_id,
            "batch_id": batch.batch_id,
            "request": request.model_dump(mode="json"),
            "batch": current.model_dump(mode="json"),
            "provider_calls": provider.calls,
        }
        (root / "metadata.json").write_text(json.dumps(metadata))
        print(json.dumps(metadata), flush=True)
        instruction = (await asyncio.to_thread(sys.stdin.readline)).strip()
        if instruction != "release":
            raise ValueError("无效释放指令")
        release.set()
        await worker
        final = await agent.mixed_change_batch(batch.batch_id)
        print(
            json.dumps(
                {
                    "phase": "finished",
                    "batch": final.model_dump(mode="json"),
                    "provider_calls": provider.calls,
                }
            ),
            flush=True,
        )
    finally:
        release.set()
        if worker is not None:
            await asyncio.gather(worker, return_exceptions=True)
        writer.apply = original_apply


async def recover(ai, root, provider):
    metadata = json.loads((root / "metadata.json").read_text())
    replays = 0

    def reject_replay(*_args, **_kwargs):
        nonlocal replays
        replays += 1
        raise AssertionError("查询或恢复不得重发发布")

    file_apply, directory_apply = WorkspaceWriter.apply, WorkspaceDirectoryWriter.apply
    WorkspaceWriter.apply = WorkspaceDirectoryWriter.apply = reject_replay
    try:
        assert await ai.agents().restore_persistent() == 1
        agent = ai.agent_session(metadata["agent_id"])
        operation_id = metadata["batch"]["items"][0]["operation"]["operation_id"]
        if metadata["source"] == "directory":
            # 原目录查询负责租约到期投影；批次仍只引用原attempt。
            await ai.agents().manager.directory_changes.store.operation(
                metadata["scope_id"], operation_id
            )
        batch = await agent.mixed_change_batch(metadata["batch_id"])
        assert batch.producer_status == "interrupted"
        assert batch.items[0].operation.operation_id == operation_id
        assert batch.items[1].status == "skipped" and batch.items[1].operation is None
        assert await agent.mixed_change_batch_by_key("mixed-process") == batch
        assert (
            await agent.start_mixed_change_batch(
                MixedBatchRequest.model_validate(metadata["request"])
            )
            == batch
        )
        if batch.items[0].status == "uncertain":
            resolved = await (
                agent.reconcile_file_change(operation_id)
                if metadata["source"] == "file"
                else agent.reconcile_directory_change(operation_id)
            )
            assert resolved.status == "done"
        final = await agent.wait_mixed_change_batch(metadata["batch_id"], timeout_seconds=0)
        assert final.items[0].status == "done" and final.items[1].operation is None
        assert replays == provider.calls == 0
        print(
            json.dumps(
                {
                    "phase": "recovered",
                    "package": krim_agent.__file__,
                    "batch": final.model_dump(mode="json"),
                    "replays": replays,
                    "provider_calls": provider.calls,
                }
            ),
            flush=True,
        )
    finally:
        WorkspaceWriter.apply, WorkspaceDirectoryWriter.apply = file_apply, directory_apply


async def main():
    phase, source, folder = sys.argv[1:4]
    root = Path(folder)
    provider = Provider()
    async with await PythonAI.create(
        settings=Settings(database_url=f"sqlite+aiosqlite:///{root / 'state.sqlite'}"),
        provider=provider,
    ) as ai:
        ai.runtime.tool_policy = allow
        ai.agents().register_workspace(
            "files", root / "files", version="v1", writable=True, default=True
        )
        if phase == "recover":
            await recover(ai, root, provider)
        else:
            await produce(ai, root, phase, source, provider)


asyncio.run(main())
