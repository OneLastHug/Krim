"""真正评委完成后、评分提交前退出；父进程验证不重复模型调用。"""

import asyncio
import json
import os
import sys
from pathlib import Path

from krim_agent.config import Settings
from krim_agent.evaluation import (
    CallableTarget,
    EvalCase,
    EvalDataset,
    EvalOutput,
    ModelJudgeScorer,
)
from krim_agent.providers import ModelText, ModelUsage
from krim_agent.sdk import PythonAI
from krim_agent.usage import TokenUsage


class OfflineJudge:
    name = "offline-judge"

    async def stream(self, _messages, tools):
        assert tools == []
        yield ModelText(text='{"score":1,"reason":"参考一致","evidence":["回答与期望相同"]}')
        yield ModelUsage(usage=TokenUsage(input_tokens=10, output_tokens=5, total_tokens=15))


async def main():
    path, marker = Path(sys.argv[1]), Path(sys.argv[2])
    async with await PythonAI.create(
        provider=OfflineJudge(), settings=Settings(database_url=f"sqlite+aiosqlite:///{path}")
    ) as ai:
        service = ai.evaluation_service()
        service.store.lease_seconds = 10
        scorer = ModelJudgeScorer(ai, "参考一致")

        async def exit_before_score(result, _binding):
            view = (await service.store.list_jobs())[0]
            await asyncio.to_thread(
                marker.write_text,
                json.dumps({"job_id": view.id, "run_id": result.run.id}),
                encoding="utf-8",
            )
            os._exit(73)

        scorer._decision = exit_before_score

        async def echo(case, _attempt):
            return EvalOutput(value=case.input)

        handle = await service.start(
            EvalDataset(name="judge", cases=[EvalCase(id="one", input="a", expected="a")]),
            CallableTarget("echo", echo),
            scorers=[scorer],
        )
        await handle.wait()


asyncio.run(main())
