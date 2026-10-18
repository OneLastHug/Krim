"""安装包HTTP浏览器夹具：只包装真实进入/退出计数，不替代业务或评分。"""

import argparse
import asyncio
import json
import os
import signal
import socket
import sys
from contextlib import nullcontext
from contextvars import ContextVar
from dataclasses import replace
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

import uvicorn

import krim_agent
from krim_agent import composition
from krim_agent.api import create_app
from krim_agent.config import Settings
from krim_agent.evaluation import scorers
from krim_agent.providers.openai_compatible import OpenAICompatibleConfig, OpenAICompatibleProvider
from krim_agent.tools.builtins import builtin_registry
from krim_agent.tools.registry import ToolRegistry


async def main(args):
    root = Path(args.evidence)
    await asyncio.to_thread(root.mkdir, parents=True, exist_ok=True)
    assert sys.flags.isolated and "site-packages" in Path(krim_agent.__file__).parts
    current = ContextVar("http_fixture_run", default=None)

    def record(name, data):
        with (root / name).open("a") as file:
            file.write(json.dumps(data, ensure_ascii=False) + "\n")
            file.flush()
            os.fsync(file.fileno())

    def event(kind, **values):
        record("entries.jsonl", {"kind": kind, "run_id": current.get(), **values})

    original_stream = OpenAICompatibleProvider.stream

    async def stream(self, messages, tools):
        event("provider_entry", target="target-http")
        source = original_stream(self, messages, tools)
        try:
            async for value in source:
                yield value
        finally:
            await source.aclose()
            event("provider_exit", target="target-http")

    OpenAICompatibleProvider.stream = stream

    def tracked(spec):
        async def execute(arguments):
            event("tool_entry", name=spec.name)
            try:
                return await spec.executor(arguments)
            finally:
                event("tool_exit", name=spec.name)

        return replace(spec, executor=execute)

    tools = ToolRegistry(tuple(tracked(spec) for spec, _ in builtin_registry()._frozen))
    composition.builtin_registry = lambda: tools
    original_score = scorers.score_rule

    async def score(*values):
        event("scorer_entry", rule=values[0].name)
        try:
            return await original_score(*values)
        finally:
            event("scorer_exit", rule=values[0].name)

    scorers.score_rule = score
    # 只按绝对文件载入标准库测试服务器，绝不把source加入产品sys.path。
    spec = spec_from_file_location(
        "local_http_fixture", Path(__file__).with_name("http_provider_server.py")
    )
    module = module_from_spec(spec)
    spec.loader.exec_module(module)

    class Fixture(module.HTTPFixture):
        def script(self, body):
            event("backend_request", ordinal=len(self.requests), max_tokens=body.get("max_tokens"))
            return super().script(body)

    class Server(uvicorn.Server):
        def capture_signals(self):
            return nullcontext()

    app = None
    listener = None
    async with Fixture(expected_key="SYNTHETIC_M3_OUTBOUND_KEY") as fixture:
        try:
            settings = Settings(database=Path(args.database), api_key="SYNTHETIC_M2_BROWSER_KEY")
            config = (
                None
                if args.readonly_history
                else OpenAICompatibleConfig(
                    base_url=fixture.url, model="fixture", api_key="SYNTHETIC_M3_OUTBOUND_KEY"
                )
            )
            app = await composition.open_application(
                Path(args.database), settings=settings, http_config=config
            )
            original_append, original_snapshot = app.store.append, app.store.snapshot

            async def append(run_id, *values, **kwargs):
                current.set(run_id)
                return await original_append(run_id, *values, **kwargs)

            async def snapshot(run_id):
                current.set(run_id)
                return await original_snapshot(run_id)

            app.store.append, app.store.snapshot = append, snapshot
            listener = socket.socket()
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind(("127.0.0.1", args.port))
            listener.listen(32)
            listener.setblocking(False)
            port = listener.getsockname()[1]
            record(
                "server-starts.jsonl",
                {
                    "pid": os.getpid(),
                    "port": port,
                    "provider_port": fixture.port,
                    "module": krim_agent.__file__,
                    "executable": sys.executable,
                    "cwd": str(Path.cwd()),
                    "readonly_history": args.readonly_history,
                },
            )
            (root / "ready.json").write_text(
                json.dumps(
                    {
                        "pid": os.getpid(),
                        "port": port,
                        "provider_port": fixture.port,
                        "base_url": f"http://127.0.0.1:{port}",
                    }
                )
            )
            server = Server(
                uvicorn.Config(create_app(application=app), log_level="warning", access_log=False)
            )
            loop = asyncio.get_running_loop()
            loop.add_signal_handler(signal.SIGTERM, setattr, server, "should_exit", True)
            try:
                await server.serve(sockets=[listener])
            finally:
                loop.remove_signal_handler(signal.SIGTERM)
        finally:
            if app is not None:
                await app.close()
            if listener is not None:
                listener.close()
    record(
        "backend-final.jsonl",
        {
            "requests": fixture.requests,
            "active": fixture.active,
            "closed_connections": fixture.closed_connections,
        },
    )
    if app is not None:
        record(
            "server-stops.jsonl",
            {
                "pid": os.getpid(),
                "tasks_done": all(t.done() for t in app.service.tasks.values()),
                "eval_tasks_done": all(t.done() for t in app.evaluations.tasks.values()),
                "scorers_done": not app.evaluations.current,
                "database_closed": app.store.closed,
                "providers_closed": all(p.closed for p in app.owned_providers),
                "streams_active": sum(p.active for p in app.owned_providers),
                "backend_active": fixture.active,
            },
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", required=True)
    parser.add_argument("--evidence", required=True)
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--provider-port", type=int, choices=[0], default=0)
    parser.add_argument("--readonly-history", action="store_true")
    asyncio.run(main(parser.parse_args()))
