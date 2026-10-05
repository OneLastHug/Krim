"""独立 wheel 的 Link 解析/API/静态资源 smoke；由 Python -I 在 /tmp 执行。"""

import asyncio
import json
from pathlib import Path
from tempfile import TemporaryDirectory

import httpx
from sqlalchemy import text

import krim_agent
from krim_agent.api import create_app
from krim_agent.config import Settings
from krim_agent.link import LinkNavigate, normalize_link
from krim_agent.persistence import Database, KrimRepository
from krim_agent.providers import FakeProvider
from krim_agent.runtime import ChatRuntime
from krim_agent.tools.builtins import builtin_registry


async def main() -> None:
    assert "/site-packages/" in krim_agent.__file__, krim_agent.__file__
    assert (
        normalize_link("../docs#part", base_url="https://example.com/ui/")
        == "https://example.com/docs#part"
    )
    assert LinkNavigate(url="http://example.com/docs").url == "http://example.com/docs"
    with TemporaryDirectory(prefix="krim-wheel-link-") as directory:
        database = Database(f"sqlite+aiosqlite:///{Path(directory) / 'link.sqlite'}")
        runtime = ChatRuntime(
            KrimRepository(database), FakeProvider(delay=0), builtin_registry(), Settings()
        )
        app = create_app(runtime=runtime)
        try:
            async with (
                app.router.lifespan_context(app),
                httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
                ) as client,
            ):
                resolved = await client.post(
                    "/v1/links/resolve",
                    json={"url": "../docs#part", "base_url": "http://127.0.0.1/ui/"},
                )
                assert (
                    resolved.status_code == 200
                    and resolved.json()["url"] == "http://127.0.0.1/docs#part"
                )
                assert (await client.get("/ui/links.js")).status_code == 200
                async with database.engine.connect() as connection:
                    head = (
                        await connection.execute(text("SELECT version_num FROM alembic_version"))
                    ).scalar()
                    assert head == "0016_agent_handles"
                print(
                    json.dumps(
                        {
                            "import": "site-packages",
                            "head": head,
                            "url": resolved.json()["url"],
                            "asset": "links.js",
                        },
                        ensure_ascii=False,
                    )
                )
        finally:
            await runtime.close()
            await database.close()


asyncio.run(main())
