"""0017 旧 wheel 与新源码共用的 BPE 持久 Agent 真实进程升级验证。"""

import asyncio
import json
import sys

import httpx

from krim_agent.agents import AgentManager
from krim_agent.api import create_app
from krim_agent.config import Settings


async def main():
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{sys.argv[1]}",
        context_tokenizer="cl100k_base",
        context_reserved_output_tokens=16,
        model_context_window_tokens=2048,
    )
    app = create_app(settings)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
        ) as api,
    ):
        definition = app.state.agents.catalog.get("default").definition
        digest = AgentManager.definition_hash(definition)
        if sys.argv[2] == "write":
            view = (
                await api.post("/v1/agents", json={"name": "default", "persistent": True})
            ).json()
            response = await api.post(
                f"/v1/agents/{view['agent_id']}/execute",
                json={"prompt": "/echo first", "idempotency_key": "first"},
            )
        else:
            response = await api.get(f"/v1/agents/{sys.argv[3]}")
            assert response.status_code == 200, response.text
            view = response.json()
            assert view["turn"] == 1
            response = await api.post(
                f"/v1/agents/{view['agent_id']}/resume",
                json={"prompt": "/echo second", "idempotency_key": "second"},
            )
        assert response.status_code == 200, response.text
        result = response.json()
        assert result["status"] == "completed"
        print(
            json.dumps(
                {
                    "agent_id": result["agent_id"],
                    "topic_id": result["topic_id"],
                    "turn": result["turn"],
                    "definition_hash": digest,
                },
                ensure_ascii=False,
            )
        )


asyncio.run(main())
