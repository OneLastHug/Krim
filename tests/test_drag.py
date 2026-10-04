import asyncio

import httpx
import pytest
from pydantic import ValidationError

from krim_agent.api import create_app
from krim_agent.app import LocalApp
from krim_agent.app_sdk import RemoteHostHandle
from krim_agent.client import KrimClient
from krim_agent.config import Settings
from krim_agent.drag import DragPreview
from krim_agent.prompt_context import MentionInput
from krim_agent.providers import FakeProvider
from krim_agent.runtime import ChatRuntime
from krim_agent.tools.builtins import builtin_registry


@pytest.mark.parametrize(
    "preview",
    [
        {"kind": "text", "label": "拖入文本", "metadata": {"size": 12}},
        {"kind": "file", "label": "notes.txt", "metadata": {"media_type": "text/plain"}},
    ],
)
def test_drag_preview_is_bounded(preview):
    assert DragPreview.model_validate(preview).label
    with pytest.raises(ValidationError):
        DragPreview(kind="text", label="x" * 121)


async def test_drag_enter_leave_do_not_write_and_drop_reuses_mention_ref(api, repository):
    topic = await repository.create_topic("拖放")
    entered = await api.post(
        f"/v1/topics/{topic.id}/drag/enter", json={"kind": "text", "label": "预览"}
    )
    assert entered.status_code == 200 and entered.json()["active"] is True
    assert not (await repository.get_prompt_context(topic.id)).mentions
    left = await api.post(f"/v1/topics/{topic.id}/drag/leave")
    assert left.json() == {"active": False, "preview": None}
    dropped = await api.post(
        f"/v1/topics/{topic.id}/drag/drop",
        json={"kind": "text", "label": "拖入", "content": "明确提供的文本"},
    )
    assert dropped.status_code == 201 and dropped.json()["id"]
    assert (await repository.get_prompt_context(topic.id)).mentions[0].content == "明确提供的文本"
    assert (
        await api.post(
            f"/v1/topics/{topic.id}/drag/drop", json={"kind": "file", "label": "x", "content": ""}
        )
    ).status_code == 422


async def test_local_and_remote_host_drag_channels_return_real_mention(repository):
    runtime = ChatRuntime(
        repository,
        FakeProvider(delay=0),
        builtin_registry(),
        Settings(),
    )
    local = LocalApp(runtime)
    app = create_app(runtime=runtime)
    try:
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
            ) as api,
        ):
            topic = await local.handle.create_topic("Host 拖放")
            assert (
                await local.handle.drag_enter(topic.id, DragPreview(kind="text", label="预览"))
            ).active
            mention = await local.handle.drag_drop(
                topic.id, MentionInput(label="drop", content="本地拖入")
            )
            assert mention.content == "本地拖入"
            sid = (await api.post("/v1/host/sessions")).json()["id"]
            await api.post(f"/v1/host/sessions/{sid}/ready")
            async with RemoteHostHandle(
                "http://127.0.0.1", sid, transport=httpx.ASGITransport(app=app)
            ) as host:
                pending = asyncio.create_task(
                    host.drag_drop(topic.id, MentionInput(label="remote", content="远程拖入"))
                )
                reader = app.state.host_sessions.events(sid, "app", 0)
                frame = await asyncio.wait_for(anext(reader), 3)
                assert frame is not None
                result = await local.handle.emit(frame.data["channel"], frame.data["data"])
                await api.post(
                    f"/v1/host/sessions/{sid}/reply",
                    json={
                        "protocol_version": 2,
                        "kind": "reply",
                        "id": frame.data["id"],
                        "channel": frame.data["channel"],
                        "data": result.model_dump(mode="json"),
                    },
                )
                dropped = await pending
                assert dropped.content == "远程拖入"
                assert dropped.id in {
                    item.id for item in (await repository.get_prompt_context(topic.id)).mentions
                }
                await reader.aclose()
    finally:
        await local.close()
        await runtime.close()


async def test_real_tcp_client_drag_and_drop(server_url):
    def scenario():
        with KrimClient(server_url) as client:
            topic = client.create_topic("TCP 拖放")
            assert client.drag_enter(topic.id, DragPreview(label="文本")).active
            mention = client.drag_drop(topic.id, MentionInput(label="TCP", content="TCP 文本"))
            assert mention.content == "TCP 文本"
            assert client.drag_leave(topic.id).active is False

    await asyncio.to_thread(scenario)
