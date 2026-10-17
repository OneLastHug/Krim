"""M2真实回环夹具：只增加外部进入计数，不替换答案/评分。"""

import argparse
import asyncio
import json
import os
import signal
import socket
import sys
from contextlib import nullcontext
from contextvars import ContextVar
from pathlib import Path


def prepare(args):
    if args.source_test:
        sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "src"))
    Path(args.evidence).mkdir(parents=True, exist_ok=True)


async def main(args):
    from dataclasses import replace

    import uvicorn

    import krim_agent
    from krim_agent.api import create_app
    from krim_agent.composition import open_application
    from krim_agent.config import Settings
    from krim_agent.evaluation import scorers, targets
    from krim_agent.providers.eval_script import EvalScriptProvider
    from krim_agent.providers.fake import FakeProvider
    from krim_agent.tools.builtins import builtin_registry
    from krim_agent.tools.registry import ToolRegistry

    root = Path(args.evidence)
    current = ContextVar("fixture_run", default=None)

    def record(name, data):
        with (root / name).open("a") as stream:
            stream.write(json.dumps(data, ensure_ascii=False) + "\n")
            stream.flush()
            os.fsync(stream.fileno())

    def event(kind, **values):
        record("entries.jsonl", {"kind": kind, "run_id": current.get(), **values})

    class DemoProvider(FakeProvider):
        async def stream(self, messages, tools):
            event("provider_entry", target="demo")
            source = super().stream(messages, tools)
            try:
                async for item in source:
                    yield item
            finally:
                await source.aclose()
                event("provider_exit", target="demo")

    stream_original = EvalScriptProvider.stream

    async def eval_stream(self, messages, tools):
        target = getattr(self, "fixture_target", self.mode)
        event("provider_entry", target=target)
        source = stream_original(self, messages, tools)
        try:
            async for item in source:
                yield item
        finally:
            await source.aclose()
            event("provider_exit", target=target)

    EvalScriptProvider.stream = eval_stream

    def tracked(spec):
        async def execute(arguments):
            event("tool_entry", name=spec.name)
            try:
                return await spec.executor(arguments)
            finally:
                event("tool_exit", name=spec.name)

        return replace(spec, executor=execute)

    def registry():
        return ToolRegistry(tuple(tracked(spec) for spec, _ in builtin_registry()._frozen))

    targets.builtin_registry = registry
    bad_original = targets.bad_add

    async def bad(arguments):
        event("tool_entry", name="add")
        try:
            return await bad_original(arguments)
        finally:
            event("tool_exit", name="add")

    targets.bad_add = bad
    score_original = scorers.score_rule

    async def score(*values):
        event("scorer_entry", rule=values[0].name)
        try:
            return await score_original(*values)
        finally:
            event("scorer_exit", rule=values[0].name)

    scorers.score_rule = score

    class Server(uvicorn.Server):
        def capture_signals(self):
            return nullcontext()

    listener = socket.socket()
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", args.port))
    listener.listen(32)
    listener.setblocking(False)
    port = listener.getsockname()[1]
    demo = DemoProvider()
    application = None
    try:
        settings = Settings.from_env().model_copy(update={"database": Path(args.database)})
        application = await open_application(
            Path(args.database), settings=settings, provider=demo, registry=registry()
        )
        for key, binding in application.service.bindings.items():
            if key != "demo":
                binding.loop.provider.fixture_target = key
        append_original = application.store.append

        async def append(run_id, *values, **kwargs):
            current.set(run_id)
            return await append_original(run_id, *values, **kwargs)

        application.store.append = append
        snapshot_original = application.store.snapshot

        async def snapshot(run_id):
            current.set(run_id)
            return await snapshot_original(run_id)

        application.store.snapshot = snapshot
        record(
            "server-starts.jsonl",
            {
                "pid": os.getpid(),
                "port": port,
                "module": krim_agent.__file__,
                "executable": sys.executable,
                "cwd": str(Path.cwd()),
                "auth_configured": bool(settings.api_key.get_secret_value()),
            },
        )
        api = create_app(application=application)

        async def observed(scope, receive, send):
            # 仅记录方法/路径作为进程进入屏障，不记录认证头或正文。
            if scope["type"] == "http":
                event("http_request", method=scope["method"], path=scope["path"])
            await api(scope, receive, send)

        server = Server(
            uvicorn.Config(
                observed,
                host="127.0.0.1",
                port=port,
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
        try:
            if application is not None:
                await application.close()
        finally:
            await demo.close()
            listener.close()
            if application is not None:
                record(
                    "server-stops.jsonl",
                    {
                        "pid": os.getpid(),
                        "providers": {
                            key: {
                                "calls": b.loop.provider.calls,
                                "streams_closed": b.loop.provider.streams_closed,
                                "active": b.loop.provider.active,
                                "closed": b.loop.provider.closed,
                            }
                            for key, b in application.service.bindings.items()
                        },
                        "tasks_done": all(t.done() for t in application.service.tasks.values()),
                        "eval_tasks_done": all(
                            t.done() for t in application.evaluations.tasks.values()
                        ),
                        "scorers_done": not application.evaluations.current,
                        "database_closed": application.store.closed,
                    },
                )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--evidence", required=True)
    parser.add_argument("--source-test", action="store_true")
    args = parser.parse_args()
    prepare(args)
    asyncio.run(main(args))
