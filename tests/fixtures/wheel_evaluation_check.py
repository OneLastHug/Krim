"""在项目外、python -I 的独立 wheel 环境中验证本地评估与零调用重读。"""

import asyncio
import json
from pathlib import Path

import krim_agent
from krim_agent.agents import AgentDefinition, AgentManager
from krim_agent.config import Settings
from krim_agent.evaluation import (
    AgentTarget,
    EvalCase,
    EvalConfig,
    EvalDataset,
    ExactMatchScorer,
    ToolCallsScorer,
    compare_reports,
    evaluate,
    load_report,
    rescore,
    save_dataset,
    save_report,
)
from krim_agent.providers import FakeProvider
from krim_agent.sdk import PythonAI


class CountingProvider(FakeProvider):
    def __init__(self):
        super().__init__(delay=0)
        self.calls = 0

    async def stream(self, messages, tools):
        self.calls += 1
        async for value in super().stream(messages, tools):
            yield value


async def main():
    assert (
        "site-packages" in krim_agent.__file__ and "/workspace/Krim" not in krim_agent.__file__
    )
    data = EvalDataset(
        name="wheel-eval",
        cases=[
            EvalCase(
                id="add",
                input="/add 2 3",
                expected='[fake] 工具结果：{"sum": 5.0}',
                metadata={
                    "expected_tools": [{"name": "add", "arguments": {"left": 2, "right": 3}}]
                },
            ),
            EvalCase(
                id="text",
                input="hello",
                expected="[fake] 收到：hello",
                metadata={"expected_tools": []},
            ),
        ],
    )
    save_dataset(data, "dataset.json")
    database_path = await asyncio.to_thread(Path("runs.sqlite").resolve)
    settings = Settings(database_url=f"sqlite+aiosqlite:///{database_path.as_posix()}")
    provider = CountingProvider()
    scorers = [ExactMatchScorer(), ToolCallsScorer()]
    async with await PythonAI.create(settings=settings, provider=provider) as ai:
        report = await ai.evaluate(data, scorers=scorers, config=EvalConfig(concurrency=2))
        assert report.summary.passed == 2 and provider.calls == 3
        save_report(report, "report.json")
        reread = await rescore(load_report("report.json"), scorers=scorers)
        assert reread.summary.passed == 2 and provider.calls == 3
        assert not compare_reports(report, reread).regressions
        manager = AgentManager(ai.runtime.repository, max_concurrent=2)
        try:
            agents = await evaluate(
                data,
                AgentTarget(
                    manager,
                    AgentDefinition(name="wheel_calc", provider=provider, tools=ai.runtime.tools),
                ),
                scorers=scorers,
            )
            assert agents.summary.passed == 2 and not manager._handles
        finally:
            await manager.close()
        assert provider.calls == 6
    cold_provider = CountingProvider()
    async with await PythonAI.create(settings=settings, provider=cold_provider) as cold:
        for row in report.results:
            restored = await cold.read_result(row.output.run_id)
            assert restored.text == row.output.value
            assert len(restored.model_steps) == row.output.model_steps
        assert cold_provider.calls == 0
    print(
        json.dumps(
            {
                "package": krim_agent.__file__,
                "passed": report.summary.passed,
                "provider_calls": provider.calls,
                "cold_read_calls": cold_provider.calls,
                "agent_handles_released": True,
            }
        )
    )


asyncio.run(main())
