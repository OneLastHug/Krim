"""原生检查点的真实生产者暂停/退出；可复制到独立wheel环境验证。"""

import asyncio
import json
import os
import sys
from pathlib import Path
from threading import Event

import krim_agent
from krim_agent.config import Settings
from krim_agent.sdk import PythonAI, WorkspaceCheckpointCreate
from krim_agent.workspace_writer import WorkspaceWriter


async def no_renew(*_args):
    await asyncio.Event().wait()


async def produce(ai, directory, mode):
    agent = await ai.agent(persistent=True)
    service = ai.agents()
    entered, release = Event(), Event()
    original_capture = WorkspaceWriter.capture_file
    original_record = service.workspace_checkpoint_store.record

    def paused(self, *args, **kwargs):
        entered.set()
        assert release.wait(40), "未收到父进程释放"
        return original_capture(self, *args, **kwargs)

    async def after_record(*args, **kwargs):
        await original_record(*args, **kwargs)
        entered.set()
        await asyncio.to_thread(release.wait, 40)
        os._exit(23)

    service.workspace_checkpoints._renew = no_renew
    if mode == "before-lock":
        WorkspaceWriter.capture_file = paused
    else:
        service.workspace_checkpoint_store.record = after_record
    request = WorkspaceCheckpointCreate(paths=["first.txt", "missing.txt"], request_key="capture")
    started = await agent.create_file_checkpoint(request)
    assert await asyncio.to_thread(entered.wait, 15)
    metadata = {
        "phase": "paused",
        "pid": os.getpid(),
        "mode": mode,
        "agent_id": agent.agent_id,
        "checkpoint_id": started.checkpoint_id,
        "scope_id": started.scope_id,
        "request": request.model_dump(mode="json"),
        "package": krim_agent.__file__,
    }
    (directory / "metadata.json").write_text(json.dumps(metadata))
    print(json.dumps(metadata), flush=True)
    command = await asyncio.to_thread(sys.stdin.readline)
    assert command.strip() == "release"
    release.set()
    if mode == "after-record":
        await asyncio.Event().wait()
    try:
        await asyncio.gather(*(entry[1] for entry in service.workspace_checkpoints._tasks.values()))
        view = await agent.file_checkpoint(started.checkpoint_id)
        print(json.dumps({"phase": "finished", "view": view.model_dump(mode="json")}), flush=True)
    finally:
        WorkspaceWriter.capture_file = original_capture


async def recover(ai, directory):
    metadata = json.loads((directory / "metadata.json").read_text())
    assert await ai.agents().restore_persistent() == 1
    agent = ai.agent_session(metadata["agent_id"])

    def no_capture(*_args, **_kwargs):
        raise AssertionError("历史读取或原键重复创建不得重读文件")

    original = WorkspaceWriter.capture_file
    WorkspaceWriter.capture_file = no_capture
    try:
        view = await agent.file_checkpoint(metadata["checkpoint_id"])
        assert view.status == "interrupted"
        assert view.files[0].captured and not view.files[1].captured
        assert await agent.file_checkpoint_by_key("capture") == view
        assert (
            await agent.create_file_checkpoint(
                WorkspaceCheckpointCreate.model_validate(metadata["request"])
            )
            == view
        )
        assert (await agent.file_checkpoints())[0] == view
        print(
            json.dumps(
                {
                    "phase": "recovered",
                    "view": view.model_dump(mode="json"),
                    "package": krim_agent.__file__,
                }
            ),
            flush=True,
        )
    finally:
        WorkspaceWriter.capture_file = original


async def main():
    mode, directory = sys.argv[1], Path(sys.argv[2])
    settings = Settings(database_url=f"sqlite+aiosqlite:///{directory / 'capture.sqlite'}")
    async with await PythonAI.create(
        settings=settings, workspace_root=directory / "files", workspace_writable=True
    ) as ai:
        if mode == "recover":
            await recover(ai, directory)
        else:
            await produce(ai, directory, mode)


asyncio.run(main())
