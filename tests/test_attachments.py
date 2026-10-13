import base64
import hashlib

import pytest
from pydantic import ValidationError
from sqlalchemy import text

from krim_agent.attachment import AttachmentUpload
from krim_agent.client import KrimClient
from krim_agent.persistence import AttachmentRepository
from krim_agent.prompt_context import MentionInput
from krim_agent.protocol.domain import ChatRequest, ServiceError


def upload(filename="notes.bin", media_type="application/zip", content=b"\x00\x01binary"):
    return AttachmentUpload(
        filename=filename,
        media_type=media_type,
        content_base64=base64.b64encode(content).decode(),
        idempotency_key="upload",
    )


@pytest.mark.parametrize(
    "kwargs",
    [
        {"filename": "../evil.txt"},
        {"filename": "a\\b.txt"},
        {"filename": "a\n.txt"},
        {"media_type": "application/x-sh"},
        {"content_base64": "%%%"},
        {"content_base64": base64.b64encode(b"").decode()},
    ],
)
def test_attachment_upload_rejects_path_mime_base64_and_empty(kwargs):
    values = upload().model_dump()
    values.update(kwargs)
    with pytest.raises(ValidationError):
        AttachmentUpload.model_validate(values)


async def test_attachment_upload_download_hash_topic_isolation_delete_and_idempotency(repository):
    attachments = AttachmentRepository(repository.database)
    one, two = await repository.create_topic("one"), await repository.create_topic("two")
    request = upload("文档.zip")
    saved = await attachments.upload(one.id, request)
    assert saved.byte_size == 8 and saved.sha256 == hashlib.sha256(b"\x00\x01binary").hexdigest()
    assert await attachments.upload(one.id, request) == saved
    with pytest.raises(ServiceError) as conflict:
        await attachments.upload(
            one.id,
            request.model_copy(update={"content_base64": base64.b64encode(b"different").decode()}),
        )
    assert conflict.value.code == "idempotency_conflict"
    content = await attachments.content(one.id, saved.id)
    assert base64.b64decode(content.content_base64) == b"\x00\x01binary"
    with pytest.raises(ServiceError):
        await attachments.get(two.id, saved.id)
    with pytest.raises(ServiceError):
        await repository.add_mention(
            two.id,
            MentionInput(
                kind="file",
                label="文档.zip",
                content="@文档.zip",
                metadata={"attachment_id": saved.id},
            ),
        )
    mention = await repository.add_mention(
        one.id,
        MentionInput(
            kind="file", label="文档.zip", content="@文档.zip", metadata={"attachment_id": saved.id}
        ),
    )
    assert (
        mention.metadata["sha256"] == saved.sha256 and mention.metadata["size"] == saved.byte_size
    )
    await attachments.delete(one.id, saved.id)
    with pytest.raises(ServiceError):
        await attachments.content(one.id, saved.id)
    # 删除附件后，历史 mention 保留快照，但新的发送拒绝缺失资源，避免静默丢失。
    request = ChatRequest(
        text="@文档.zip",
        idempotency_key="send",
        mentions=[{"mention_id": mention.id, "start": 0, "end": 7}],
    )
    with pytest.raises(ServiceError) as missing:
        await repository.begin_run(one.id, request)
    assert missing.value.code == "attachment_not_found"


async def test_attachment_limits_and_client_http_api(api, server_url):
    topic = (await api.post("/v1/topics", json={"title": "HTTP 附件"})).json()["id"]
    request = upload("ok.txt", "text/plain", "明确文本".encode())
    response = await api.post(
        f"/v1/topics/{topic}/attachments", json=request.model_dump(mode="json")
    )
    assert response.status_code == 201
    view = response.json()
    assert (await api.get(f"/v1/topics/{topic}/attachments")).json()[0]["id"] == view["id"]
    assert (
        base64.b64decode(
            (await api.get(f"/v1/topics/{topic}/attachments/{view['id']}/content")).json()[
                "content_base64"
            ]
        )
        == "明确文本".encode()
    )
    assert (await api.delete(f"/v1/topics/{topic}/attachments/{view['id']}")).json() == {
        "deleted": True
    }

    def client_call():
        with KrimClient(server_url) as client:
            topic_view = client.create_topic("client attachment")
            uploaded = client.upload_attachment(
                topic_view.id, upload("client.txt", "text/plain", b"client")
            )
            assert client.attachments(topic_view.id)[0].id == uploaded.id
            assert (
                base64.b64decode(
                    client.attachment_content(topic_view.id, uploaded.id).content_base64
                )
                == b"client"
            )
            client.delete_attachment(topic_view.id, uploaded.id)

    import asyncio

    await asyncio.to_thread(client_call)


async def test_attachment_migration_head_and_downgrade_protection(repository):
    attachments = AttachmentRepository(repository.database)
    topic = await repository.create_topic("迁移")
    await attachments.upload(topic.id, upload("x.txt", "text/plain", b"x"))
    with pytest.raises(RuntimeError, match="二进制附件"):
        async with repository.database.engine.begin() as connection:
            from alembic import command

            from krim_agent.persistence.migration import migration_config

            await connection.run_sync(
                lambda sync: command.downgrade(migration_config(sync), "0014_host_checkpoints")
            )
    async with repository.database.engine.connect() as connection:
        assert (
            await connection.execute(text("SELECT version_num FROM alembic_version"))
        ).scalar() == "0044_tree_exchange_ledger"


async def test_large_attachment_body_is_bounded_without_expanding_other_json_routes(api):
    topic = (await api.post("/v1/topics", json={"title": "大附件"})).json()["id"]
    data = b"X" * 70000
    payload = upload("large.pdf", "application/pdf", data)
    created = await api.post(
        f"/v1/topics/{topic}/attachments", json=payload.model_dump(mode="json")
    )
    assert created.status_code == 201 and created.json()["byte_size"] == 70000
    assert (await api.post("/v1/topics", content=b" " * 70000)).status_code == 413
    identifier = created.json()["id"]
    downloaded = await api.get(f"/v1/topics/{topic}/attachments/{identifier}/download")
    assert downloaded.content == data and downloaded.headers["x-content-type-options"] == "nosniff"
    assert downloaded.headers["content-disposition"].startswith("attachment;")


async def test_local_and_remote_attachment_sdk_read_same_bytes(runtime, server_url):
    import httpx

    from krim_agent.app import LocalApp
    from krim_agent.app_sdk import RemoteHostHandle

    local = LocalApp(runtime)
    try:
        topic = await local.handle.create_topic("Local binary")
        created = await local.handle.upload_attachment(topic.id, upload("local.zip"))
        assert (
            base64.b64decode(
                (await local.handle.attachment_content(topic.id, created.id)).content_base64
            )
            == b"\x00\x01binary"
        )
        async with httpx.AsyncClient(base_url=server_url, trust_env=False) as api:
            sid = (await api.post("/v1/host/sessions")).json()["id"]
            async with RemoteHostHandle(server_url, sid) as remote:
                second_topic = (
                    await api.post("/v1/topics", json={"title": "remote binary"})
                ).json()["id"]
                second = await remote.upload_attachment(second_topic, upload("remote.zip"))
                assert (
                    base64.b64decode(
                        (await remote.attachment_content(second_topic, second.id)).content_base64
                    )
                    == b"\x00\x01binary"
                )
                assert (await remote.attachments(second_topic))[0].id == second.id
                await remote.delete_attachment(second_topic, second.id)
    finally:
        await local.close()


async def test_attachment_exact_limit_and_oversize_are_verified_at_decoded_bytes(repository):
    topic = await repository.create_topic("附件大小边界")
    attachments = AttachmentRepository(repository.database)
    raw = b"X" * (2 * 1024 * 1024)
    saved = await attachments.upload(topic.id, upload("limit.pdf", "application/pdf", raw))
    assert saved.byte_size == len(raw)
    assert base64.b64decode((await attachments.content(topic.id, saved.id)).content_base64) == raw
    with pytest.raises(ValidationError):
        upload("oversize.pdf", "application/pdf", raw + b"X")


async def test_attachment_count_quota_is_topic_scoped_and_retries_do_not_consume_it(repository):
    attachments = AttachmentRepository(repository.database)
    one, two = await repository.create_topic("quota"), await repository.create_topic("other")
    for index in range(64):
        request = upload(f"file-{index}.txt", "text/plain", b"x").model_copy(
            update={"idempotency_key": str(index)}
        )
        saved = await attachments.upload(one.id, request)
        assert await attachments.upload(one.id, request) == saved
    with pytest.raises(ServiceError) as count:
        await attachments.upload(one.id, upload("overflow.txt", "text/plain", b"x"))
    assert (
        count.value.code == "attachment_count_limit" and len(await attachments.list(one.id)) == 64
    )
    assert (
        await attachments.upload(two.id, upload("other.txt", "text/plain", b"x"))
    ).topic_id == two.id


async def test_attachment_snapshot_preserves_metadata_after_delete_and_rejects_forged_metadata(
    repository,
):
    attachments = AttachmentRepository(repository.database)
    topic = await repository.create_topic("快照")
    saved = await attachments.upload(topic.id, upload("snap.txt", "text/plain", b"explicit"))
    with pytest.raises(ServiceError) as forged:
        await repository.add_mention(
            topic.id,
            MentionInput(
                kind="file",
                label="snap.txt",
                content="描述",
                metadata={"attachment_id": saved.id, "sha256": "0" * 64},
            ),
        )
    assert forged.value.code == "attachment_reference_invalid"
    reference = await repository.add_mention(
        topic.id,
        MentionInput(
            kind="file", label="snap.txt", content="明确描述", metadata={"attachment_id": saved.id}
        ),
    )
    run, created = await repository.begin_run(
        topic.id,
        ChatRequest(
            text="@snap.txt",
            idempotency_key="saved",
            mentions=[{"mention_id": reference.id, "start": 0, "end": 9}],
        ),
    )
    assert created
    await repository.finish(run.id, "completed", None, [])
    await attachments.delete(topic.id, saved.id)
    messages = await repository.list_messages(topic.id)
    snapshot = messages[0].details["prompt_context"]["mentions"][0]
    assert (
        snapshot["metadata"]["attachment_id"] == saved.id
        and snapshot["metadata"]["sha256"] == saved.sha256
    )
