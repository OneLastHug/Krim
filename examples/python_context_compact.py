"""离线上下文预览、确认和重开重建示例；只使用临时本地 SQLite 与 fake model。"""

import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory

import httpx

from krim_agent.api import create_app
from krim_agent.config import Settings
from krim_agent.context_engine import ContextOptions
from krim_agent.persistence import Database, KrimRepository


async def main() -> None:
    with TemporaryDirectory(prefix="krim-context-") as temporary:
        database_url = f"sqlite+aiosqlite:///{Path(temporary) / 'context.sqlite'}"
        app = create_app(Settings(database_url=database_url))
        # ASGI 是 Asynchronous Server Gateway Interface（异步服务网关接口）。
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
            ) as client:
                topic = (await client.post("/v1/topics", json={"title": "离线压缩示例"})).json()
                for index in range(4):
                    response = await client.post(
                        f"/v1/topics/{topic['id']}/messages",
                        json={"text": f"历史问题 {index}", "idempotency_key": str(index)},
                    )
                    assert response.status_code == 202, response.text
                    events = await client.get(f"/v1/runs/{response.json()['id']}/events")
                    assert '"status":"completed"' in events.text
                endpoint = f"/v1/topics/{topic['id']}/compact"
                preview = (await client.post(endpoint, json={"max_messages": 3})).json()
                assert preview["status"] == "proposed" and not preview["persisted"]
                response = await client.post(
                    endpoint,
                    json={
                        "max_messages": 3,
                        "confirm": True,
                        "expected_branch_key": preview["branch_key"],
                        "proposal_id": preview["proposal_id"],
                        "proposal_revision": preview["proposal_revision"],
                    },
                )
                assert response.status_code == 200, response.text
                completed = response.json()
                assert completed["status"] == "completed" and completed["persisted"]
                assert completed["summary"] == preview["summary"]
        reopened = Database(database_url)
        await reopened.initialize()
        try:
            repository = KrimRepository(reopened)
            # 从已安装的 Python Runtime 组合层重建；never 不产生新摘要。
            from krim_agent.application.context import PersistentContext
            from krim_agent.context_engine import ContextEngine

            context = PersistentContext(repository, ContextEngine())
            messages = await repository.list_messages(topic["id"])
            restored = await context.prepare(topic["id"], messages, ContextOptions(policy="never"))
            assert restored.source_message_ids == completed["source_message_ids"]
            assert restored.messages[0].role == "user"
            print(
                f"预览：{preview['status']}；确认：{completed['status']}；"
                f"来源：{len(restored.source_message_ids)}；重建消息：{len(restored.messages)}；"
                f"原消息保留：{len(messages)}"
            )
        finally:
            await reopened.close()


if __name__ == "__main__":
    asyncio.run(main())
