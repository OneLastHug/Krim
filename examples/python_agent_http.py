"""本地 Agent HTTP 创建/执行/继续/游标/销毁示例，使用临时 SQLite 和 fake provider。"""

import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory

import httpx

from krim_agent.api import create_app
from krim_agent.config import Settings


async def main() -> None:
    with TemporaryDirectory(prefix="krim-agent-api-") as temporary:
        config = Settings(database_url=f"sqlite+aiosqlite:///{Path(temporary) / 'agents.sqlite'}")
        app = create_app(config)
        # ASGI 为 Asynchronous Server Gateway Interface（异步服务网关接口），本例不使用外网。
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
            ) as client,
        ):
            catalog = (await client.get("/v1/agents/catalog")).json()
            assert catalog[0]["name"] == "default"
            response = await client.post("/v1/agents", json={"name": "default"})
            assert response.status_code == 201, response.text
            agent = response.json()
            path = f"/v1/agents/{agent['agent_id']}"
            request = {"prompt": "/add 2 3", "idempotency_key": "first"}
            first = (await client.post(path + "/execute", json=request)).json()
            retry = (await client.post(path + "/execute", json=request)).json()
            assert first["status"] == "completed" and "5.0" in first["result"]["text"]
            assert retry["current_run_id"] == first["current_run_id"] and retry["turn"] == 1
            resumed = (
                await client.post(
                    path + "/resume", json={"prompt": "/echo next", "idempotency_key": "second"}
                )
            ).json()
            assert resumed["status"] == "completed" and resumed["turn"] == 2
            events = await client.get(path + "/events", params={"from_seq": 1})
            assert events.headers["x-krim-agent-run-id"] == resumed["current_run_id"]
            assert "id: 1" in events.text and "event: end" in events.text
            stopped = (await client.post(path + "/stop")).json()
            assert stopped["status"] == "completed"
            destroyed = (await client.delete(path)).json()
            assert destroyed["status"] == "destroyed"
            run_id = resumed["current_run_id"]
        reopened = create_app(config)
        async with (
            reopened.router.lifespan_context(reopened),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=reopened), base_url="http://127.0.0.1"
            ) as client,
        ):
            assert (await client.get(path)).status_code == 404
            assert (await client.get(f"/v1/runs/{run_id}")).json()["status"] == "completed"
            assert "next" in (await client.get(f"/v1/runs/{run_id}/messages")).text
            print(
                "目录：default；执行：completed；幂等：True；继续：2；游标：1；销毁：destroyed；重开持久运行：completed"
            )


if __name__ == "__main__":
    asyncio.run(main())
