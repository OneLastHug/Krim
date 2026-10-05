"""两个真实 Python 进程顺序恢复同一 Agent/topic，验证历史不被重放。"""

import asyncio
import json
import sys

import httpx

from krim_agent.api import create_app
from krim_agent.config import Settings


async def main() -> None:
    app = create_app(Settings(database_url=f"sqlite+aiosqlite:///{sys.argv[1]}"))
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
        ) as api,
    ):
        if sys.argv[2] == "write":
            view = (
                await api.post("/v1/agents", json={"name": "default", "persistent": True})
            ).json()
            result = await api.post(
                f"/v1/agents/{view['agent_id']}/execute",
                json={"prompt": "/echo first", "idempotency_key": "first"},
            )
        else:
            view = (await api.get(f"/v1/agents/{sys.argv[3]}")).json()
            assert view["turn"] == 1 and view["status"] == "completed"
            result = await api.post(
                f"/v1/agents/{view['agent_id']}/resume",
                json={"prompt": "/echo second", "idempotency_key": "second"},
            )
        assert result.status_code == 200, result.text
        state = result.json()
        assert state["status"] == "completed"
        print(
            json.dumps(
                {
                    "agent_id": state["agent_id"],
                    "topic_id": state["topic_id"],
                    "run_id": state["current_run_id"],
                    "turn": state["turn"],
                    "text": state["result"]["text"],
                }
            ),
            flush=True,
        )


asyncio.run(main())
