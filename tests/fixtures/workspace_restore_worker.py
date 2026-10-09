"""恢复计划的真实进程暂停/崩溃/重启；支持项目外wheel验证。"""

import asyncio
import json
import os
import sys
from pathlib import Path
from threading import Event

import krim_agent
from krim_agent.config import Settings
from krim_agent.sdk import (
    CheckpointRestoreExecute,
    CheckpointRestorePreview,
    PythonAI,
    WorkspaceCheckpointCreate,
)
from krim_agent.workspace_writer import WorkspaceWriter


async def no_renew(*_args):
    await asyncio.Event().wait()


async def produce(ai, directory, mode):
    agent = await ai.agent(persistent=True)
    service = ai.agents()
    checkpoint = await agent.create_file_checkpoint(
        WorkspaceCheckpointCreate(paths=["file.txt"], request_key="target")
    )
    assert (await agent.wait_file_checkpoint(checkpoint.checkpoint_id)).status == "available"
    (directory / "files/file.txt").write_text("B")
    request = CheckpointRestorePreview(
        checkpoint_id=checkpoint.checkpoint_id, request_key="preview"
    )
    plan = await agent.preview_file_checkpoint_restore(request)
    plan = await agent.wait_file_checkpoint_restore(plan.plan_id)
    assert plan.phase == "ready", plan
    execute = CheckpointRestoreExecute(
        plan_id=plan.plan_id, expected_revision=plan.revision, request_key="execute"
    )
    metadata = {
        "phase": "ready",
        "pid": os.getpid(),
        "package": krim_agent.__file__,
        "mode": mode,
        "agent_id": agent.agent_id,
        "scope_id": plan.scope_id,
        "plan_id": plan.plan_id,
        "preview": request.model_dump(mode="json"),
        "execute": execute.model_dump(mode="json"),
    }
    if mode == "prepare":
        (directory / "metadata.json").write_text(json.dumps(metadata))
        print(json.dumps(metadata), flush=True)
        return
    entered, release = Event(), Event()
    original = WorkspaceWriter.apply
    service.workspace_restores._renew = no_renew
    service.manager.workspace_changes._renew = no_renew

    def paused(self, *args, **kwargs):
        entered.set()
        assert release.wait(40), "未收到父进程释放"
        return original(self, *args, **kwargs)

    async def crash_save(*_args, **_kwargs):
        entered.set()
        await asyncio.to_thread(release.wait, 40)
        os._exit(23)

    if mode == "before-lock":
        WorkspaceWriter.apply = paused
    elif mode == "before-save":
        service.manager.workspace_changes._save = crash_save
    else:
        raise ValueError(mode)
    await agent.execute_file_checkpoint_restore(execute, confirmed=True)
    assert await asyncio.to_thread(entered.wait, 20)
    view = await agent.file_checkpoint_restore(plan.plan_id)
    metadata.update(phase="paused", operation_id=view.items[0].operation.operation_id)
    (directory / "metadata.json").write_text(json.dumps(metadata))
    print(json.dumps(metadata), flush=True)
    assert (await asyncio.to_thread(sys.stdin.readline)).strip() == "release"
    release.set()
    if mode == "before-save":
        await asyncio.Event().wait()
    try:
        await asyncio.gather(*(entry[1] for entry in service.workspace_restores._tasks.values()))
        print(
            json.dumps(
                {
                    "phase": "finished",
                    "view": (await agent.file_checkpoint_restore(plan.plan_id)).model_dump(
                        mode="json"
                    ),
                }
            ),
            flush=True,
        )
    finally:
        WorkspaceWriter.apply = original


async def recover(ai, directory):
    metadata = json.loads((directory / "metadata.json").read_text())
    assert await ai.agents().restore_persistent() == 1
    agent = ai.agent_session(metadata["agent_id"])
    original_capture, original_apply = WorkspaceWriter.capture_file, WorkspaceWriter.apply

    def no_replay(*_args, **_kwargs):
        raise AssertionError("查询、原键重试与核实不得重读快照或重放文件动作")

    WorkspaceWriter.capture_file = no_replay
    WorkspaceWriter.apply = no_replay
    try:
        plan = await agent.file_checkpoint_restore(metadata["plan_id"])
        assert (await agent.file_checkpoint_restore_by_key("preview")) == plan
        assert (
            await agent.preview_file_checkpoint_restore(
                CheckpointRestorePreview.model_validate(metadata["preview"])
            )
        ) == plan
        if metadata["mode"] == "prepare":
            assert plan.phase == "ready"
            WorkspaceWriter.apply = original_apply
            await agent.execute_file_checkpoint_restore(
                CheckpointRestoreExecute.model_validate(metadata["execute"]), confirmed=True
            )
            plan = await agent.wait_file_checkpoint_restore(plan.plan_id)
            assert plan.status == "done", plan
        else:
            assert plan.phase == "interrupted" and plan.status == "uncertain", plan
            repeated = await agent.execute_file_checkpoint_restore(
                CheckpointRestoreExecute.model_validate(metadata["execute"]), confirmed=True
            )
            assert repeated == plan
            operation = await agent.reconcile_file_change(metadata["operation_id"], confirmed=True)
            assert operation.status == "done"
            plan = await agent.file_checkpoint_restore(plan.plan_id)
            assert plan.status == "done" and plan.phase == "interrupted", plan
            assert plan.items[0].operation.operation_id == metadata["operation_id"]
            await agent.finalize_file_change(operation.operation_id, confirmed=True)
        assert (directory / "files/file.txt").read_text() == "A"
        print(
            json.dumps(
                {
                    "phase": "recovered",
                    "package": krim_agent.__file__,
                    "view": plan.model_dump(mode="json"),
                }
            ),
            flush=True,
        )
    finally:
        WorkspaceWriter.capture_file, WorkspaceWriter.apply = original_capture, original_apply


async def main():
    mode, directory = sys.argv[1], Path(sys.argv[2])
    async with await PythonAI.create(
        settings=Settings(
            database_url=f"sqlite+aiosqlite:///{directory / 'restore.sqlite'}",
            interactive_tools=True,
        ),
        workspace_root=directory / "files",
        workspace_writable=True,
    ) as ai:
        if mode == "recover":
            await recover(ai, directory)
        else:
            await produce(ai, directory, mode)


asyncio.run(main())
