"""项目外 python -I：持久任务/多轮/远端统计/显式评委/对象真实分发闭环。"""

import asyncio
import json
import tempfile
from pathlib import Path

import httpx
from pydantic import Field
from sqlalchemy import text

import krim_agent
from krim_agent.api import create_app
from krim_agent.config import Settings
from krim_agent.evaluation import (
    CallableTarget,
    EvalCase,
    EvalDataset,
    EvalOutput,
    EvalRemoteStartRequest,
    EvalTurn,
    ModelJudgeScorer,
    analyze_report,
)
from krim_agent.evaluation.remote import AsyncEvalClient
from krim_agent.protocol.models import StrictModel
from krim_agent.providers import FakeProvider, ModelText, ModelUsage
from krim_agent.sdk import PythonAI
from krim_agent.usage import TokenUsage


class Counting(FakeProvider):
    def __init__(self):
        super().__init__(delay=0)
        self.calls = 0

    async def stream(self, messages, tools):
        self.calls += 1
        async for event in super().stream(messages, tools):
            yield event


class OfflineJson:
    name = "wheel-object-judge"

    async def stream(self, messages, tools):
        if "JSON Schema" in messages[-1].content:
            yield ModelText(text='{"number":3}')
        else:
            assert not tools
            yield ModelText(text='{"score":1,"reason":"参考一致","evidence":["原回答一致"]}')
        yield ModelUsage(usage=TokenUsage(input_tokens=4, output_tokens=2, total_tokens=6))


class Answer(StrictModel):
    number: int = Field(ge=0)


async def main():
    assert "site-packages" in krim_agent.__file__
    assert "/workspace/Krim" not in krim_agent.__file__
    folder = await asyncio.to_thread(tempfile.mkdtemp, prefix="completion-fixture-", dir=".")
    path = await asyncio.to_thread((Path(folder) / "completion.sqlite").resolve)
    settings = Settings(database_url=f"sqlite+aiosqlite:///{path}")
    data = EvalDataset(
        name="wheel-completion",
        cases=[
            EvalCase(
                id="one",
                input="first",
                expected="[fake] 收到：second",
                turns=[EvalTurn(input="second", expected="[fake] 收到：second")],
                tags=["wheel"],
            )
        ],
    )
    provider = Counting()
    async with await PythonAI.create(provider=provider, settings=settings) as ai:
        handle = await ai.start_evaluation(data, conversation=True, request_key="wheel-batch")
        assert (await handle.wait()).status == "completed"
        report = await handle.report()
        assert report.summary.passed == 1 and provider.calls == 2
        assert analyze_report(report).tags["wheel"].passed == 1
        job_id = handle.id
        app = create_app(runtime=ai.runtime)
        async with (
            app.router.lifespan_context(app),
            AsyncEvalClient(transport=httpx.ASGITransport(app=app)) as client,
        ):
            remote = await client.start(
                EvalRemoteStartRequest(
                    target="conversation", dataset=data, scorers=["conversation-turns"]
                )
            )
            assert (await client.wait(remote.id)).status == "completed"
            assert (await client.statistics(remote.id)).overall.passed == 1
            assert len([event async for event in client.events(remote.id)]) > 2
        assert provider.calls == 4
        async with ai.runtime.repository.database.engine.connect() as connection:
            head = (
                await connection.execute(text("SELECT version_num FROM alembic_version"))
            ).scalar()
        assert head == "0027_native_workspace_batches"
    cold_provider = Counting()
    async with await PythonAI.create(settings=settings, provider=cold_provider) as cold:
        assert (await cold.evaluation(job_id).report()).summary.passed == 1
        assert cold_provider.calls == 0
    async with await PythonAI.create(provider=OfflineJson()) as ai:
        result = await ai.generate_object("返回数字", schema=Answer)
        assert result.value == {"number": 3} and result.generation.token_usage.total_tokens == 6

        async def echo(case, _attempt):
            return EvalOutput(value=case.input)

        grade = await ai.evaluation_service().start(
            EvalDataset(name="grade", cases=[EvalCase(id="grade", input="a", expected="a")]),
            CallableTarget("recorded", echo),
            scorers=[ModelJudgeScorer(ai, "参考一致")],
        )
        await grade.wait()
        graded = await grade.report()
        assert graded.summary.passed == 1
        assert analyze_report(graded).grading.total_tokens == 6
    print(
        json.dumps(
            {
                "package": krim_agent.__file__,
                "migration": head,
                "calls": provider.calls,
                "cold_calls": cold_provider.calls,
                "object": result.value,
                "grade_passed": graded.summary.passed,
            },
            ensure_ascii=False,
        )
    )


asyncio.run(main())
