"""真实强退夹具；计数文件从不提供给产品恢复逻辑。"""

import asyncio
import json
import os
import sys
from pathlib import Path

if os.environ.get("KRIM_GUIDE_SOURCE"):
    sys.path.insert(0, os.environ["KRIM_GUIDE_SOURCE"])
from krim_agent.composition import open_application
from krim_agent.contracts import RunRequest
from krim_agent.providers.fake import FakeProvider
from krim_agent.tools.builtins import EchoInput
from krim_agent.tools.core import ToolSpec
from krim_agent.tools.registry import ToolRegistry

database, counter, ready, phase = map(str, sys.argv[1:])
counter = Path(counter)
ready = Path(ready)
counter.write_text(json.dumps({"calls": 0, "provider_calls": 0}))


def bump(key):
    values = json.loads(counter.read_text())
    values[key] += 1
    with counter.open("w") as stream:
        json.dump(values, stream)
        stream.flush()
        os.fsync(stream.fileno())


async def execute(args):
    bump("calls")
    return args.text


class Provider(FakeProvider):
    async def stream(self, messages, tools):
        bump("provider_calls")
        source = super().stream(messages, tools)
        try:
            async for item in source:
                yield item
        finally:
            await source.aclose()


async def post(invocation, result):
    if phase == "after":
        ready.write_text(json.dumps({"run_id": invocation.run_id, "call_id": invocation.call_id}))
        await asyncio.Event().wait()


async def main():
    registry = ToolRegistry(
        (ToolSpec("echo", EchoInput, execute, {"type": "string"}, "fixture-1", "0" * 64),)
    )
    async with await open_application(
        Path(database), registry=registry, provider=Provider(), post=post
    ) as app:
        original = app.store.append

        async def append(run_id, kind, data, **kwargs):
            record = await original(run_id, kind, data, **kwargs)
            if phase == "before" and kind == "tool_intent":
                ready.write_text(json.dumps({"run_id": run_id, "call_id": data["call_id"]}))
                await asyncio.Event().wait()
            return record

        app.store.append = append
        run = await app.service.start(RunRequest(text="/echo real", request_key="crash-original"))
        await app.service.wait(run.id)


asyncio.run(main())
