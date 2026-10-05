"""独立 wheel 的二进制附件/哈希/话题/Mention/API/UI/0015 验证。"""

import asyncio
import base64
import json
from pathlib import Path
from tempfile import TemporaryDirectory

import httpx
from sqlalchemy import text

import krim_agent
from krim_agent.api import create_app
from krim_agent.app import LocalApp
from krim_agent.attachment import AttachmentUpload
from krim_agent.config import Settings
from krim_agent.persistence import Database, KrimRepository
from krim_agent.prompt_context import MentionInput
from krim_agent.providers import FakeProvider
from krim_agent.runtime import ChatRuntime
from krim_agent.tools.builtins import builtin_registry


async def main() -> None:
    assert "/site-packages/" in krim_agent.__file__, krim_agent.__file__
    with TemporaryDirectory(prefix="krim-wheel-attachment-") as folder:
        database = Database(f"sqlite+aiosqlite:///{Path(folder) / 'attachments.sqlite'}")
        runtime = ChatRuntime(
            KrimRepository(database), FakeProvider(delay=0), builtin_registry(), Settings()
        )
        local, app = LocalApp(runtime), create_app(runtime=runtime)
        try:
            async with (
                app.router.lifespan_context(app),
                httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
                ) as api,
            ):
                topic = await local.handle.create_topic("wheel attachment")
                raw = b"PK\x03\x04\x00\x01binary"
                view = await local.handle.upload_attachment(
                    topic.id,
                    AttachmentUpload(
                        filename="wheel.zip",
                        media_type="application/zip",
                        content_base64=base64.b64encode(raw).decode(),
                        idempotency_key="upload",
                    ),
                )
                assert (
                    base64.b64decode(
                        (await local.handle.attachment_content(topic.id, view.id)).content_base64
                    )
                    == raw
                )
                reference = await local.handle.add_mention(
                    topic.id,
                    MentionInput(
                        kind="file",
                        label=view.filename,
                        content="明确描述",
                        metadata={"attachment_id": view.id},
                    ),
                )
                assert reference.metadata["sha256"] == view.sha256
                response = await api.get(f"/v1/topics/{topic.id}/attachments/{view.id}/download")
                assert (
                    response.content == raw
                    and response.headers["x-content-type-options"] == "nosniff"
                )
                assert (await api.get("/ui/attachments-ui.js")).status_code == 200
                async with database.engine.connect() as connection:
                    head = (
                        await connection.execute(text("SELECT version_num FROM alembic_version"))
                    ).scalar()
                assert head == "0015_attachments"
                print(
                    json.dumps(
                        {
                            "import": "site-packages",
                            "head": head,
                            "bytes": len(raw),
                            "attachment_id": view.id,
                            "mention_id": reference.id,
                            "asset": "attachments-ui.js",
                        },
                        ensure_ascii=False,
                    )
                )
        finally:
            await local.close()
            await runtime.close()
            await database.close()


asyncio.run(main())
