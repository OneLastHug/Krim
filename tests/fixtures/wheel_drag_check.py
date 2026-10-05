"""独立 wheel 的 Drag/Drop API、MentionRef、迁移与 drag-drop.js smoke。"""

import asyncio
import json
from pathlib import Path
from tempfile import TemporaryDirectory

import httpx
from sqlalchemy import text

import krim_agent
from krim_agent.api import create_app
from krim_agent.app import LocalApp
from krim_agent.config import Settings
from krim_agent.drag import DragPreview
from krim_agent.persistence import Database, KrimRepository
from krim_agent.prompt_context import MentionInput
from krim_agent.providers import FakeProvider
from krim_agent.runtime import ChatRuntime
from krim_agent.tools.builtins import builtin_registry


async def main() -> None:
    assert "/site-packages/" in krim_agent.__file__, krim_agent.__file__
    with TemporaryDirectory(prefix="krim-wheel-drag-") as directory:
        database = Database(f"sqlite+aiosqlite:///{Path(directory) / 'drag.sqlite'}")
        runtime = ChatRuntime(
            KrimRepository(database), FakeProvider(delay=0), builtin_registry(), Settings()
        )
        app, local = create_app(runtime=runtime), LocalApp(runtime)
        try:
            async with (
                app.router.lifespan_context(app),
                httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
                ) as client,
            ):
                topic = await local.handle.create_topic("wheel 拖放")
                assert (await local.handle.drag_enter(topic.id, DragPreview(label="预览"))).active
                mention = await local.handle.drag_drop(
                    topic.id, MentionInput(label="wheel", content="明确文本")
                )
                assert mention.content == "明确文本"
                assert (await client.get("/ui/drag-drop.js")).status_code == 200
                async with database.engine.connect() as connection:
                    head = (
                        await connection.execute(text("SELECT version_num FROM alembic_version"))
                    ).scalar()
                assert head == "0014_host_checkpoints"
                print(
                    json.dumps(
                        {
                            "import": "site-packages",
                            "head": head,
                            "mention_id": mention.id,
                            "asset": "drag-drop.js",
                        },
                        ensure_ascii=False,
                    )
                )
        finally:
            await local.close()
            await runtime.close()
            await database.close()


asyncio.run(main())
