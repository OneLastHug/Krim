"""合成故障进程；不伪造生产事实，父进程持创建句柄控制窗口。"""

import argparse
import asyncio
import hashlib
import json
import os
from dataclasses import replace
from pathlib import Path

from krim_agent.composition import open_application
from krim_agent.providers.openai_compatible import OpenAICompatibleConfig, OpenAICompatibleProvider
from krim_agent.security import LOCAL
from krim_agent.sessions.types import SessionCreate, TurnRequest
from krim_agent.tools.builtins import builtin_registry
from krim_agent.tools.registry import ToolRegistry


def save(path, value):
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value))
    os.replace(temporary, path)


def append_effect(path, name, result):
    with path.open("a") as stream:
        stream.write(json.dumps({"tool": name, "value": result}) + "\n")
        stream.flush()
        os.fsync(stream.fileno())


async def main(args):
    entered = asyncio.Event()
    hold = asyncio.Event()
    effects = Path(args.effects)
    await asyncio.to_thread(effects.write_text, "")
    digest = await asyncio.to_thread(
        lambda: hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    )
    original_registry = builtin_registry()
    specs = []
    for spec, _ in original_registry._frozen:

        def bind(original):
            async def execute(value):
                result = await original.executor(value)
                await asyncio.to_thread(append_effect, effects, original.name, result)
                if args.phase == "tool":
                    entered.set()
                    await hold.wait()
                return result

            return execute

        specs.append(
            replace(
                spec,
                executor=bind(spec),
                implementation_digest=digest,
            )
        )
    provider = OpenAICompatibleProvider(
        OpenAICompatibleConfig(base_url=args.base_url, model="fixture")
    )
    app = None
    try:
        app = await open_application(
            Path(args.database), provider=provider, registry=ToolRegistry(tuple(specs))
        )
        original = app.store._write

        async def write(operation):
            value = await original(operation)
            if args.phase == "link" and "begin_run" in operation.__qualname__:
                entered.set()
                await hold.wait()
            return value

        app.store._write = write
        session = await app.sessions.create(
            LOCAL, "local", SessionCreate(request_key="crash-session")
        )
        first = await app.sessions.submit(
            LOCAL,
            "local",
            session.id,
            TurnRequest(request_key="active", text="/add 2 3", expected_revision=0),
        )
        second = await app.sessions.submit(
            LOCAL,
            "local",
            session.id,
            TurnRequest(request_key="queued", text="不得重放", expected_revision=1),
        )
        if args.phase in {"link", "tool"}:
            await entered.wait()
        current = await app.sessions.turn(LOCAL, "local", session.id, first.id)
        save(
            Path(args.marker),
            {
                "phase": args.phase,
                "namespace_local_pid": os.getpid(),
                "session_id": session.id,
                "first": first.id,
                "queued": second.id,
                "run_id": current.run_id,
            },
        )
        await app.sessions.wait(LOCAL, "local", session.id, first.id)
    finally:
        hold.set()
        if app is not None:
            await app.close()
        await provider.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", required=True)
    parser.add_argument("--marker", required=True)
    parser.add_argument("--effects", required=True)
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--phase", choices=["link", "provider", "tool"], required=True)
    asyncio.run(main(parser.parse_args()))
