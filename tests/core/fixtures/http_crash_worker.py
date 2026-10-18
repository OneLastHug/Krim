"""HTTP真实强退窗口；计数文件只属于测试父进程，产品恢复不读取。"""

import argparse
import asyncio
import json
import os
import sys
from dataclasses import replace
from pathlib import Path

if os.environ.get("KRIM_GUIDE_SOURCE"):
    sys.path.insert(0, os.environ["KRIM_GUIDE_SOURCE"])

from krim_agent import composition
from krim_agent.contracts import RunRequest
from krim_agent.providers.openai_compatible import OpenAICompatibleConfig
from krim_agent.tools.builtins import builtin_registry
from krim_agent.tools.registry import ToolRegistry


def persist(path, value):
    """小型测试屏障事实同步fsync，先可见再进入异步等待，不创建取消后遗留线程。"""
    with path.open("w") as stream:
        json.dump(value, stream)
        stream.flush()
        os.fsync(stream.fileno())


def bump(counter):
    values = json.loads(counter.read_text())
    values["tools"] += 1
    persist(counter, values)


async def main(args):
    counter, ready = Path(args.counter), Path(args.ready)
    persist(counter, {"tools": 0})

    def tracked(spec):
        async def execute(arguments):
            bump(counter)
            return await spec.executor(arguments)

        return replace(spec, executor=execute)

    tools = ToolRegistry(tuple(tracked(spec) for spec, _ in builtin_registry()._frozen))
    composition.builtin_registry = lambda: tools
    async with await composition.open_application(
        Path(args.database),
        http_config=OpenAICompatibleConfig(base_url=args.base_url, model="fixture"),
    ) as app:
        original = app.store.append

        async def append(run_id, kind, data, **kwargs):
            if args.phase == "after" and kind == "tool_end":
                persist(ready, {"run_id": run_id, "call_id": data["call_id"]})
                await asyncio.Event().wait()
            record = await original(run_id, kind, data, **kwargs)
            if args.phase == "before" and kind == "tool_intent":
                persist(ready, {"run_id": run_id, "call_id": data["call_id"]})
                await asyncio.Event().wait()
            return record

        app.store.append = append
        view = await app.service.start(
            RunRequest(text="/add 2 3", request_key="http-crash-original", target="target-http")
        )
        if args.phase == "http":
            persist(ready, {"run_id": view.id})
        await app.service.wait(view.id)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    for name in ["database", "counter", "ready", "phase", "base-url"]:
        parser.add_argument("--" + name, required=True)
    asyncio.run(main(parser.parse_args()))
