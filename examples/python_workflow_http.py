"""Workflow HTTP 幂等与 SSE 示例；ASGI 本地传输，不连接外部服务。"""

import asyncio

import httpx

from krim_agent.api import create_app
from krim_agent.config import Settings


async def main() -> None:
    app = create_app(Settings(database_url="sqlite+aiosqlite:///:memory:"))
    request = {
        "idempotency_key": "offline-workflow-api",
        "graph": {"nodes": [{"id": "add", "kind": "tool", "config": {"name": "add"}}]},
        "input_value": {"left": 2, "right": 3},
    }
    # 本地 ASGI（Asynchronous Server Gateway Interface，异步服务网关接口）传输。
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
        ) as client:
            first = await client.post("/v1/workflows/runs", json=request)
            assert first.status_code == 202
            run_id = first.json()["run_id"]
            second = await client.post("/v1/workflows/runs", json=request)
            assert second.json()["run_id"] == run_id
            events = await client.get(f"/v1/workflows/runs/{run_id}/events")
            assert "event: end" in events.text
            result = (await client.get(f"/v1/workflows/runs/{run_id}")).json()
            print(f"状态：{result['status']}；结果：{result['output']}；幂等：True")


if __name__ == "__main__":
    asyncio.run(main())
