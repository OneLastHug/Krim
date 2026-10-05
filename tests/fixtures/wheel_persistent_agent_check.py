"""独立安装包的 persistent Agent close/reopen/resume 与 0016 验证。"""

import asyncio
import json
from pathlib import Path
from tempfile import TemporaryDirectory

import httpx
from sqlalchemy import text

import krim_agent
from krim_agent.api import create_app
from krim_agent.config import Settings


async def main() -> None:
    assert "/site-packages/" in krim_agent.__file__, krim_agent.__file__
    with TemporaryDirectory(prefix="krim-wheel-persistent-agent-") as folder:
        config = Settings(database_url=f"sqlite+aiosqlite:///{Path(folder) / 'agents.sqlite'}")
        first = create_app(config)
        async with (
            first.router.lifespan_context(first),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=first), base_url="http://127.0.0.1"
            ) as api,
        ):
            agent = (
                await api.post("/v1/agents", json={"name": "default", "persistent": True})
            ).json()
            assert (
                await api.post(
                    f"/v1/agents/{agent['agent_id']}/execute", json={"prompt": "/echo first"}
                )
            ).json()["status"] == "completed"
        second = create_app(config)
        async with (
            second.router.lifespan_context(second),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=second), base_url="http://127.0.0.1"
            ) as api,
        ):
            restored = (await api.get(f"/v1/agents/{agent['agent_id']}")).json()
            assert restored["agent_id"] == agent["agent_id"] and restored["turn"] == 1
            resumed = (
                await api.post(
                    f"/v1/agents/{agent['agent_id']}/resume", json={"prompt": "/echo second"}
                )
            ).json()
            assert resumed["status"] == "completed" and resumed["turn"] == 2
            async with second.state.repository.database.engine.connect() as connection:
                head = (
                    await connection.execute(text("SELECT version_num FROM alembic_version"))
                ).scalar()
            assert head == "0018_context_proposals"
            assert (await api.delete(f"/v1/agents/{agent['agent_id']}")).status_code == 200
            print(
                json.dumps(
                    {
                        "import": "site-packages",
                        "head": head,
                        "agent_id": agent["agent_id"],
                        "turn": 2,
                        "status": resumed["status"],
                    }
                ),
                flush=True,
            )


asyncio.run(main())
