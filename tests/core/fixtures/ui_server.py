"""新 UI 夹具：任务自有数据库，记录真实 Provider 进入及服务身份。"""

import argparse
import asyncio
import json
import os
import signal
import socket
import sys
from contextlib import nullcontext
from dataclasses import replace
from pathlib import Path

if os.environ.get("KRIM_GUIDE_SOURCE"):
    sys.path.insert(0, os.environ["KRIM_GUIDE_SOURCE"])
import uvicorn

import krim_agent
from krim_agent.api import create_app
from krim_agent.composition import open_application
from krim_agent.config import Settings
from krim_agent.providers.fake import FakeProvider
from krim_agent.tools.builtins import builtin_registry
from krim_agent.tools.registry import ToolRegistry

parser = argparse.ArgumentParser()
parser.add_argument("--database", required=True)
parser.add_argument("--port", required=True, type=int)
parser.add_argument("--evidence", required=True)
args = parser.parse_args()
root = Path(args.evidence)
root.mkdir(parents=True, exist_ok=True)


def record(name, value):
    with (root / name).open("a") as stream:
        stream.write(json.dumps(value, ensure_ascii=False) + "\n")
        stream.flush()
        os.fsync(stream.fileno())


class Provider(FakeProvider):
    async def stream(self, messages, tools):
        source = super().stream(messages, tools)
        record("provider-calls.jsonl", {"pid": os.getpid(), "entry": self.calls + 1})
        try:
            async for item in source:
                yield item
        finally:
            await source.aclose()


class FixtureServer(uvicorn.Server):
    def capture_signals(self):
        return nullcontext()


async def main():
    provider = Provider()

    def tracked(spec):
        async def execute(arguments):
            record("tool-calls.jsonl", {"pid": os.getpid(), "name": spec.name})
            return await spec.executor(arguments)

        return replace(spec, executor=execute)

    tools = ToolRegistry(tuple(tracked(spec) for spec, _ in builtin_registry()._frozen))
    listener = socket.socket()
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", args.port))
    listener.listen(32)
    listener.setblocking(False)
    actual_port = listener.getsockname()[1]
    try:
        async with await open_application(
            Path(args.database),
            settings=Settings.from_env().model_copy(update={"database": Path(args.database)}),
            provider=provider,
            registry=tools,
        ) as application:
            record(
                "server-starts.jsonl",
                {
                    "pid": os.getpid(),
                    "port": actual_port,
                    "module": krim_agent.__file__,
                    "executable": sys.executable,
                    "cwd": str(Path.cwd()),
                },
            )
            server = FixtureServer(
                uvicorn.Config(
                    create_app(application=application),
                    host="127.0.0.1",
                    port=args.port,
                    access_log=False,
                    log_level="warning",
                )
            )
            loop = asyncio.get_running_loop()
            loop.add_signal_handler(signal.SIGTERM, setattr, server, "should_exit", True)
            try:
                await server.serve(sockets=[listener])
            finally:
                loop.remove_signal_handler(signal.SIGTERM)
    finally:
        listener.close()
        await provider.close()
        record(
            "server-stops.jsonl",
            {
                "pid": os.getpid(),
                "active": provider.active,
                "calls": provider.calls,
                "streams_closed": provider.streams_closed,
                "provider_closed": provider.closed,
                "database_closed": application.store.closed,
                "tasks_done": all(task.done() for task in application.service.tasks.values()),
            },
        )


asyncio.run(main())
