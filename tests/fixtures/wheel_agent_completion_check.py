"""项目外独立 wheel：配置、会话、恢复、SDK/评估与静态管理资源。"""

import asyncio
import json
from pathlib import Path
from tempfile import TemporaryDirectory

import httpx
from sqlalchemy import text

import krim_agent
from krim_agent.agents import AgentProfile, AgentProfileUpdate
from krim_agent.api import create_app
from krim_agent.config import Settings
from krim_agent.evaluation import EvalCase, EvalDataset
from krim_agent.evaluation.scorers import builtin_scorers
from krim_agent.evaluation.targets import AgentTarget
from krim_agent.providers import FakeProvider
from krim_agent.sdk import PythonAI


class Counting(FakeProvider):
    def __init__(self):
        super().__init__(delay=0)
        self.calls = 0

    async def stream(self, messages, tools):
        self.calls += 1
        async for part in super().stream(messages, tools):
            yield part


async def main():
    assert "/site-packages/" in krim_agent.__file__, krim_agent.__file__
    with TemporaryDirectory(prefix="agent-fixture-") as directory:
        settings = Settings(database_url=f"sqlite+aiosqlite:///{Path(directory) / 'agents.sqlite'}")
        warm_provider = Counting()
        async with await PythonAI.create(settings=settings, provider=warm_provider) as warm:
            service = warm.agents()
            service.register_tool_version("add", "builtin-v1")
            config = AgentProfile(
                name="worker",
                description="独立分发",
                instructions="旧规则",
                tool_names=["add", "echo"],
                persistent=True,
            )
            await service.profile_put(AgentProfileUpdate(expected_revision=0, config=config))
            session = await warm.agent("worker", profile_revision=1)
            first = await session.generate_text("/add 2 3", idempotency_key="one")
            assert first.run.status == "completed" and '"sum": 5.0' in first.text
            await session.close()
            await service.profile_put(
                AgentProfileUpdate(
                    expected_revision=1, config=config.model_copy(update={"instructions": "新规则"})
                )
            )
            identifier, first_run_id = session.agent_id, first.run.id
        cold_provider = Counting()
        async with await PythonAI.create(settings=settings, provider=cold_provider) as cold:
            service = cold.agents()
            service.register_tool_version("add", "builtin-v1")
            session = cold.agent_session(identifier)
            assert (await session.state()).status == "closed"
            restored = await session.restore()
            assert restored.profile_revision == 1 and restored.current_run_id == first_run_id
            assert cold_provider.calls == 0
            await session.reopen()
            second = await session.generate_text("/echo second", idempotency_key="two")
            assert second.text.endswith("second") and second.run.id != first_run_id
            assert [turn.turn for turn in await session.turns()] == [1, 2]
            original = await (await session.stream(run_id=first_run_id)).collect()
            assert original.run.id == first_run_id and cold_provider.calls == 2
            target = AgentTarget(
                service.manager, await service.definition("worker"), version="wheel-v1"
            )
            job = await cold.evaluation_service().start(
                EvalDataset(
                    name="agent-wheel",
                    cases=[EvalCase(id="sum", input="/add 8 9", expected='"sum": 17.0')],
                ),
                target,
                scorers=builtin_scorers(["contains"]),
            )
            await job.wait()
            report = await job.report()
            assert report.summary.passed == 1
            await session.destroy()
            assert (await session.state()).status == "destroyed"
            assert not await service.inventory()
            app = create_app(runtime=cold.runtime)
            async with (
                app.router.lifespan_context(app),
                httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
                ) as api,
            ):
                for resource in ["app.js", "agents-ui.js", "background-streams.js"]:
                    response = await api.get(f"/ui/{resource}")
                    assert (
                        response.status_code == 200
                        and "text/javascript" in response.headers["content-type"]
                    )
                assert any(
                    item["kind"] == "provider"
                    for item in (await api.get("/v1/agents/resources")).json()
                )
                archived = await api.get(
                    f"/v1/agents/{identifier}/events", params={"run_id": first_run_id}
                )
                assert archived.status_code == 200 and "event: end" in archived.text
                async with cold.runtime.repository.database.engine.connect() as connection:
                    head = (
                        await connection.execute(text("SELECT version_num FROM alembic_version"))
                    ).scalar()
                    assert head == "0040_mixed_batch_model_sources"
            print(
                json.dumps(
                    {
                        "package": krim_agent.__file__,
                        "migration": head,
                        "cold_restore_calls": 0,
                        "warm_calls": warm_provider.calls,
                        "cold_calls": cold_provider.calls,
                        "turns": 2,
                        "eval_passed": report.summary.passed,
                    },
                    ensure_ascii=False,
                )
            )


asyncio.run(main())
