"""从 /tmp 以 -I 核验独立 wheel 的 Skills/快照/迁移和静态资源。"""

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
from krim_agent.persistence import Database, KrimRepository
from krim_agent.protocol.domain import RegenerateRequest
from krim_agent.providers import FakeProvider
from krim_agent.runtime import ChatRuntime
from krim_agent.sdk import PythonAI, SkillLoadedPart
from krim_agent.skills import SkillBatch, SkillDefinition, SkillResource
from krim_agent.tools.builtins import builtin_registry


async def main() -> None:
    assert "/site-packages/" in krim_agent.__file__, krim_agent.__file__
    with TemporaryDirectory(prefix="krim-wheel-skills-") as directory:
        url = f"sqlite+aiosqlite:///{Path(directory) / 'skills.sqlite'}"
        database = Database(url)
        repository = KrimRepository(database)
        runtime = ChatRuntime(repository, FakeProvider(delay=0), builtin_registry(), Settings())
        app, local = create_app(runtime=runtime), LocalApp(runtime)
        try:
            async with (
                app.router.lifespan_context(app),
                httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
                ) as api,
            ):
                topic = await local.handle.create_topic("wheel 技能", locale="en-GB")
                batch = SkillBatch(
                    skills=[
                        SkillDefinition(
                            name="wheel-review",
                            description="检查明确文本",
                            content="wheel 原始正文",
                            resources=[
                                SkillResource(path="references/check.md", content="wheel 原始资源")
                            ],
                        )
                    ]
                )
                await local.handle.inject_skills(topic.id, batch)
                catalog = (await api.get(f"/v1/topics/{topic.id}/skills")).json()
                assert catalog["skills"][0]["name"] == "wheel-review"
                assert "wheel 原始正文" not in json.dumps(catalog, ensure_ascii=False)
                read = await local.handle.read_skill(
                    topic.id, "wheel-review", "references/check.md"
                )
                assert read.content == "wheel 原始资源"
                async with PythonAI(runtime) as ai:
                    stream = await ai.stream_text(
                        "/skill wheel-review references/check.md", topic_id=topic.id
                    )
                    result = await stream.collect()
                    parts = [part async for part in stream.full_stream()]
                assert result.run.status == "completed" and "wheel 原始资源" in result.text
                loaded = [part for part in parts if isinstance(part, SkillLoadedPart)]
                assert len(loaded) == 1 and loaded[0].resource == "references/check.md"
                await local.handle.remove_skill(topic.id, "wheel-review")
                regenerated = await runtime.regenerate(
                    topic.id,
                    result.run.input_message_id,
                    RegenerateRequest(idempotency_key="wheel-regenerate"),
                )
                task = runtime.production_task(regenerated.id)
                assert task is not None
                await asyncio.wait_for(asyncio.shield(task), 10)
                assert (
                    "wheel 原始资源" in (await repository.run_messages(regenerated.id))[-1].content
                )
                assets: dict[str, int] = {}
                for name in ["index.html", "app.js", "skills-ui.js", "i18n.js", "styles.css"]:
                    response = await api.get("/ui/" + name)
                    assert (
                        response.status_code == 200
                        and response.headers["x-content-type-options"] == "nosniff"
                    )
                    assets[name] = len(response.content)
                async with database.engine.connect() as connection:
                    assert (
                        await connection.execute(text("SELECT version_num FROM alembic_version"))
                    ).scalar() == "0018_context_proposals"
                print(
                    json.dumps(
                        {
                            "import": "site-packages",
                            "head": "0018_context_proposals",
                            "skill": "wheel-review",
                            "loaded": 1,
                            "snapshot": "retained",
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
