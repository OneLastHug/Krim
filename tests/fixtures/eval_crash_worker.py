"""模型执行完成、评估输出提交前强制退出，验证恢复不会重放工具。"""

import asyncio
import json
import os
import sys
from pathlib import Path

from krim_agent.config import Settings
from krim_agent.evaluation import EvalCase, EvalDataset, SuccessScorer
from krim_agent.providers import FakeProvider
from krim_agent.sdk import PythonAI


async def main():
    settings = Settings(database_url=f"sqlite+aiosqlite:///{Path(sys.argv[1]).as_posix()}")
    async with await PythonAI.create(settings=settings, provider=FakeProvider(delay=0)) as ai:
        service = ai.evaluation_service()
        service.store.lease_seconds = 10
        real = service.store.mutate

        async def crash(job_id, owner, epoch, ordinal, **kwargs):
            if kwargs.get("phase") == "executed":
                item = await service.store.item(job_id, ordinal)
                await asyncio.to_thread(
                    Path(sys.argv[2]).write_text,
                    json.dumps({"job_id": job_id, "bindings": item.bindings}),
                    encoding="utf-8",
                )
                os._exit(73)
            await real(job_id, owner, epoch, ordinal, **kwargs)

        service.store.mutate = crash
        handle = await ai.start_evaluation(
            EvalDataset(name="crash", cases=[EvalCase(id="add", input="/add 2 3")]),
            scorers=[SuccessScorer()],
        )
        view = await handle.wait()
        print(view.model_dump_json(), file=sys.stderr)


asyncio.run(main())
