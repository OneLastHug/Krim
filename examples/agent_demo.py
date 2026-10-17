"""阅读入口：唯一装配 → RunSDK → 受控工具 → 同源结果/Trace。"""

import argparse
import asyncio
import json
from pathlib import Path

from krim_agent.composition import open_application
from krim_agent.sdk import RunSDK


async def demo(database: Path) -> None:
    async with await open_application(database) as app:
        client = RunSDK(app.service)
        run = await client.start("/add 2 3", "agent-demo-add")
        await client.wait(run.id)
        result = await client.result(run.id)
        trace = await app.service.trace(run.id)
        print(
            json.dumps(
                {
                    "run_id": run.id,
                    "status": result.run.status,
                    "text": result.text,
                    "model_steps": result.model_steps,
                    "tools": len(result.tools),
                    "trace": trace.model_dump(mode="json"),
                },
                ensure_ascii=False,
                indent=2,
            )
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", type=Path, required=True)
    asyncio.run(demo(parser.parse_args().database))
