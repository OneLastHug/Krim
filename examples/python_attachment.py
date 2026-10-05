"""离线附件示例：保存/读取真实有限二进制，绑定话题内 Mention，不交给模型。"""

import asyncio
import base64
import json
from pathlib import Path
from tempfile import TemporaryDirectory

from krim_agent.app import LocalApp
from krim_agent.attachment import AttachmentUpload
from krim_agent.config import Settings
from krim_agent.persistence import Database, KrimRepository
from krim_agent.prompt_context import MentionInput
from krim_agent.providers import FakeProvider
from krim_agent.runtime import ChatRuntime
from krim_agent.tools.builtins import builtin_registry


async def main() -> None:
    with TemporaryDirectory(prefix="krim-attachment-example-") as folder:
        database = Database(f"sqlite+aiosqlite:///{Path(folder) / 'attachments.sqlite'}")
        await database.initialize()
        runtime = ChatRuntime(
            KrimRepository(database), FakeProvider(delay=0), builtin_registry(), Settings()
        )
        local = LocalApp(runtime)
        try:
            topic = await local.handle.create_topic("二进制附件示例")
            raw = b"PK\x03\x04\x00\x01explicit binary"
            view = await local.handle.upload_attachment(
                topic.id,
                AttachmentUpload(
                    filename="example.zip",
                    media_type="application/zip",
                    content_base64=base64.b64encode(raw).decode(),
                    idempotency_key="upload",
                ),
            )
            restored = await local.handle.attachment_content(topic.id, view.id)
            assert base64.b64decode(restored.content_base64) == raw
            reference = await local.handle.add_mention(
                topic.id,
                MentionInput(
                    kind="file",
                    label=view.filename,
                    content="用户明确提供的附件描述",
                    metadata={"attachment_id": view.id},
                ),
            )
            assert reference.metadata["sha256"] == view.sha256
            print(
                json.dumps(
                    {
                        "head": "0015_attachments",
                        "filename": view.filename,
                        "bytes": view.byte_size,
                        "mention_id": reference.id,
                        "model_input": "description only",
                    },
                    ensure_ascii=False,
                )
            )
        finally:
            await local.close()
            await runtime.close()
            await database.close()


asyncio.run(main())
