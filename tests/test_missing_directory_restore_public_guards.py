"""公开Python确认必须真bool，HTTP取消必须等待原物理线程退出。"""

import asyncio
from threading import Event

import pytest
from test_missing_directory_restore_control import checkpoint, execute_request, ready, setup
from test_tree_move_public_consumers import api_for

from krim_agent.protocol.domain import ServiceError
from krim_agent.sdk import DirectoryRestoreTarget, MissingDirectoryRestorePreview
from krim_agent.workspace_directory_absence import WorkspaceDirectoryAbsenceReader


@pytest.mark.parametrize("confirmed", ["false", 1])
async def test_missing_sdk_truthy_confirmation_cannot_approve_original_ask(
    runtime, tmp_path, confirmed
):
    async def policy(definition, _context):
        return "allow" if definition.risk == "read" else "ask"

    service, agent, binding = await setup(runtime, tmp_path, policy=policy)
    try:
        _, planned = await ready(service, agent, binding)
        with pytest.raises(ServiceError) as denied:
            await agent.execute_missing_directory_checkpoint_restore(
                execute_request(planned), confirmed=confirmed
            )
        assert denied.value.status == 422
        assert await agent.missing_directory_checkpoint_restore(planned.plan_id) == planned
        assert not (tmp_path / "tree").exists() and not list(tmp_path.glob("._krim-tx-*"))
    finally:
        await service.close()


async def test_missing_http_cancel_waits_original_absence_thread_before_return(
    runtime, tmp_path, monkeypatch
):
    (
        service,
        agent,
        _,
    ) = await setup(runtime, tmp_path)
    entered, release, exited = Event(), Event(), Event()
    cancelled = asyncio.Event()
    original = WorkspaceDirectoryAbsenceReader.capture
    original_cancel = service.missing_directory_restore_store.cancel

    async def observe_cancel(*args, **kwargs):
        result = await original_cancel(*args, **kwargs)
        cancelled.set()
        return result

    def hold(*args, **kwargs):
        entered.set()
        try:
            assert release.wait(5), "测试主动释放原IO线程"
            return original(*args, **kwargs)
        finally:
            exited.set()

    request = None
    try:
        captured = await checkpoint(agent)
        (tmp_path / "tree").rename(tmp_path / "retained-original")
        monkeypatch.setattr(WorkspaceDirectoryAbsenceReader, "capture", hold)
        planned = await agent.preview_missing_directory_checkpoint_restore(
            MissingDirectoryRestorePreview(
                target=DirectoryRestoreTarget(identifier=captured.checkpoint_id),
                request_key="cancel-thread",
            )
        )
        assert await asyncio.to_thread(entered.wait, 3)
        monkeypatch.setattr(service.missing_directory_restore_store, "cancel", observe_cancel)
        async with api_for(runtime, service, monkeypatch) as (_app, api):
            request = asyncio.create_task(
                api.post(
                    f"/v1/agents/{agent.agent_id}/workspace/missing-directory-restores/{planned.plan_id}/cancel"
                )
            )
            await asyncio.wait_for(cancelled.wait(), 2)
            assert (
                await agent.missing_directory_checkpoint_restore(planned.plan_id)
            ).phase == "cancelled"
            assert not request.done() and not exited.is_set()
            release.set()
            response = await asyncio.wait_for(request, 4)
            assert response.status_code == 200 and response.json()["phase"] == "cancelled", (
                response.text
            )
            assert exited.is_set() and not service.missing_directory_restores._tasks
            assert (
                not service.missing_directory_restores._callbacks
                and not (tmp_path / "tree").exists()
            )
    finally:
        release.set()
        if request is not None and not request.done():
            request.cancel()
            await asyncio.gather(request, return_exceptions=True)
        await service.close()
