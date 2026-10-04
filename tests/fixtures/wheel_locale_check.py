"""独立 wheel 的 Locale/迁移/原文/静态资源核验；通过 Python -I 从 /tmp 执行。"""

import asyncio
import importlib.metadata
import json
from pathlib import Path
from tempfile import TemporaryDirectory

import httpx
from sqlalchemy import text

import krim_agent
from krim_agent.api import create_app
from krim_agent.app import LocalApp
from krim_agent.config import Settings
from krim_agent.locale import resolve_locale
from krim_agent.persistence import Database, KrimRepository
from krim_agent.protocol.domain import ChatRequest
from krim_agent.providers import FakeProvider
from krim_agent.runtime import ChatRuntime
from krim_agent.tools.builtins import builtin_registry
from krim_agent.ui_locale import locale_resources


async def main() -> None:
    assert "/site-packages/" in krim_agent.__file__, krim_agent.__file__
    assert importlib.metadata.version("langcodes") == "3.5.1"
    assert resolve_locale("en-GB").effective == "en-US"
    assert locale_resources("en-US").strings["发送"] == "Send"
    with TemporaryDirectory(prefix="krim-locale-wheel-") as directory:
        url = f"sqlite+aiosqlite:///{Path(directory) / 'locale.sqlite'}"
        database = Database(url)
        runtime = ChatRuntime(
            KrimRepository(database), FakeProvider(delay=0), builtin_registry(), Settings()
        )
        app = create_app(runtime=runtime)
        local = LocalApp(runtime)
        try:
            async with (
                app.router.lifespan_context(app),
                httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
                ) as client,
            ):
                response = await client.post(
                    "/v1/topics", json={"title": "wheel 用户标题", "locale": "en-GB"}
                )
                assert response.status_code == 201, response.text
                topic = response.json()
                assert topic["locale"] == "en-US"
                value = (await client.get("/v1/locales?locale=en-GB")).json()
                assert value["requested"] == "en-GB" and value["strings"]["发送"] == "Send"
                assert len(value["strings"]) == 171
                assert (
                    await local.handle.set_topic_locale(topic["id"], "zh-CN")
                ).effective == "zh-CN"
                assert (
                    await local.handle.set_topic_locale(topic["id"], "en-GB")
                ).effective == "en-US"
                run = await runtime.start(
                    topic["id"],
                    ChatRequest(text="/echo 原始用户内容", idempotency_key="wheel-locale"),
                )
                task = runtime.production_task(run.id)
                assert task is not None
                await asyncio.wait_for(asyncio.shield(task), 5)
                before = await runtime.repository.run_messages(run.id)
                await runtime.repository.set_topic_locale(topic["id"], "zh-CN")
                assert await runtime.repository.run_messages(run.id) == before
                assert before[0].content == "/echo 原始用户内容"
                assert "原始用户内容" in before[-1].content
                await runtime.repository.set_topic_locale(topic["id"], "en-US")
                assets: dict[str, int] = {}
                for name in [
                    "index.html",
                    "app.js",
                    "i18n.js",
                    "prompt-context-ui.js",
                    "host.html",
                    "host-shell.js",
                ]:
                    resource = await client.get("/ui/" + name)
                    assert resource.status_code == 200, name
                    assert resource.headers["x-content-type-options"] == "nosniff"
                    assets[name] = len(resource.content)
                async with database.engine.connect() as connection:
                    assert (
                        await connection.execute(text("SELECT version_num FROM alembic_version"))
                    ).scalar() == "0011_locale"
            await database.close()
            reopened = Database(url)
            try:
                await reopened.initialize()
                saved = await KrimRepository(reopened).get_topic(topic["id"])
                assert saved.locale == "en-US" and saved.title == "wheel 用户标题"
            finally:
                await reopened.close()
            print(
                json.dumps(
                    {
                        "import": "site-packages",
                        "langcodes": "3.5.1",
                        "head": "0011_locale",
                        "locale": "en-US",
                        "resources": 171,
                        "assets": assets,
                    },
                    ensure_ascii=False,
                )
            )
        finally:
            await local.close()
            await runtime.close()
            await database.close()


asyncio.run(main())
