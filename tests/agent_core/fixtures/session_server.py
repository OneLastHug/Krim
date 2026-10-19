"""合成本机HTTP Provider与安装包服务；-I运行时不注入source/src。"""

import argparse
import asyncio
import hashlib
import json
import os
import re
import signal
import socket
from contextlib import asynccontextmanager, nullcontext
from dataclasses import replace
from pathlib import Path

import uvicorn
from pydantic import SecretStr
from sqlalchemy import event

from krim_agent import __version__
from krim_agent.api.server import create_app
from krim_agent.composition import open_application
from krim_agent.config import Settings
from krim_agent.providers.openai_compatible import OpenAICompatibleConfig, OpenAICompatibleProvider
from krim_agent.security import AccessPolicy, Credential, Membership
from krim_agent.storage.sqlite import SQLiteStore
from krim_agent.tools.builtins import builtin_registry
from krim_agent.tools.registry import ToolRegistry


class WireFixture:
    def __init__(self, evidence=None):
        self.evidence = evidence
        self.requests = []
        self.executions = []
        self.active = set()
        self.errors = []
        self.hold_at = set()
        self.fail_at = set()
        self.release = asyncio.Event()
        self.changed = asyncio.Condition()

    async def start(self):
        self.server = await asyncio.start_server(self.handle, "127.0.0.1", 0)
        self.port = self.server.sockets[0].getsockname()[1]
        self.record()
        return self

    async def handle(self, reader, writer):
        task = asyncio.current_task()
        self.active.add(task)
        try:
            header = await reader.readuntil(b"\r\n\r\n")
            length = int(
                next(
                    v.split(b":", 1)[1]
                    for v in header.split(b"\r\n")
                    if v.lower().startswith(b"content-length:")
                )
            )
            body = json.loads(await reader.readexactly(length))
            self.requests.append(body)
            index = len(self.requests)
            self.record()
            async with self.changed:
                self.changed.notify_all()
            last_user = next(
                (m["content"] for m in reversed(body["messages"]) if m["role"] == "user"), ""
            )
            if index in self.hold_at or last_user == "/hold":
                gate = asyncio.create_task(self.release.wait())
                disconnected = asyncio.create_task(reader.read(1))
                try:
                    done, _ = await asyncio.wait(
                        [gate, disconnected], return_when=asyncio.FIRST_COMPLETED
                    )
                    if disconnected in done:
                        return
                finally:
                    for waiter in (gate, disconnected):
                        if not waiter.done():
                            waiter.cancel()
                    await asyncio.gather(gate, disconnected, return_exceptions=True)
            if (
                index in self.fail_at
                or last_user == "/fail"
                and body["messages"][-1]["role"] == "tool"
            ):
                raw = b"fixture error"
                status = b"502 Bad Gateway"
                mime = b"text/plain"
            else:
                raw = self.response(body, index)
                status = b"200 OK"
                mime = b"text/event-stream"
            writer.write(
                b"HTTP/1.1 "
                + status
                + b"\r\nContent-Type: "
                + mime
                + b"\r\nContent-Length: "
                + str(len(raw)).encode()
                + b"\r\nConnection: close\r\n\r\n"
                + raw
            )
            await writer.drain()
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        except Exception as error:
            self.errors.append(type(error).__name__)
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except ConnectionError:
                pass
            self.active.discard(task)

    def response(self, body, index):
        messages = body["messages"]
        last = messages[-1]
        if last["role"] == "tool":
            value = json.loads(last["content"])
            content = (
                str(value["sum"]).removesuffix(".0")
                if isinstance(value, dict) and "sum" in value
                else str(value)
            )
            delta = {"content": content}
            finish = "stop"
        else:
            text = last["content"]
            name = None
            args = {}
            if text == "/fail":
                name = "add"
                args = dict(left=2.0, right=3.0)
            elif text.startswith("/note "):
                name = "lookup_note"
                args = dict(key=text[6:].strip())
            elif text.startswith("/echo "):
                name = "echo"
                args = dict(text=text[6:])
            elif text.startswith("/add "):
                numbers = [float(v) for v in text.split()[1:]]
                name = "add"
                args = dict(left=numbers[0], right=numbers[1])
            elif "基数" in text:
                name = "echo"
                args = dict(text=text)
            elif text == "与3相加":
                prior = next(
                    (
                        m["content"]
                        for m in reversed(messages[:-1])
                        if m["role"] == "user" and "基数" in m["content"]
                    ),
                    None,
                )
                if prior is not None:
                    number = float(re.search(r"\d+", prior)[0])
                    name = "add"
                    args = dict(left=number, right=3.0)
            if name:
                delta = {
                    "tool_calls": [
                        {
                            "index": 0,
                            "id": f"call-{index}",
                            "type": "function",
                            "function": {"name": name, "arguments": json.dumps(args)},
                        }
                    ]
                }
                finish = "tool_calls"
            else:
                delta = {"content": "缺少前文" if text == "与3相加" else "收到：" + text}
                finish = "stop"

        def frame(delta, finish):
            return (
                "data: "
                + json.dumps(
                    {"choices": [{"index": 0, "delta": delta, "finish_reason": finish}]},
                    ensure_ascii=False,
                )
                + "\n\n"
            )

        return (frame(delta, None) + frame({}, finish) + "data: [DONE]\n\n").encode()

    def record(self):
        if self.evidence is not None:
            save_json(
                self.evidence / "model-counts.json",
                {
                    "requests": self.requests,
                    "executions": self.executions,
                    "active": len(self.active),
                },
            )

    async def wait_requests(self, count):
        async with asyncio.timeout(5), self.changed:
            await self.changed.wait_for(lambda: len(self.requests) >= count)

    def registry(self):
        values = []
        for spec, _ in builtin_registry()._frozen:

            def bind(original):
                async def execute(args):
                    self.executions.append(
                        {"name": original.name, "arguments": args.model_dump(mode="json")}
                    )
                    self.record()
                    return await original.executor(args)

                return execute

            values.append(
                replace(
                    spec,
                    executor=bind(spec),
                    implementation_digest=hashlib.sha256(b"trusted-counting-fixture/1").hexdigest(),
                )
            )
        return ToolRegistry(tuple(values))

    @asynccontextmanager
    async def application(self, database, settings=None, access_policy=None):
        provider = OpenAICompatibleProvider(
            OpenAICompatibleConfig(base_url=f"http://127.0.0.1:{self.port}/v1", model="fixture")
        )
        app = None
        try:
            app = await open_application(
                database,
                provider=provider,
                registry=self.registry(),
                settings=settings or Settings(),
                access_policy=access_policy,
            )
            yield app
        finally:
            if app is not None:
                await app.close()
                assert all(t.done() for t in app.service.tasks.values())
                assert app.sessions._dispatcher.done()
            await provider.close()

    async def close(self):
        self.release.set()
        self.server.close()
        await self.server.wait_closed()
        if self.active:
            await asyncio.gather(*self.active)
        assert not self.active and not self.errors, self.errors
        try:
            _, writer = await asyncio.open_connection("127.0.0.1", self.port)
        except OSError:
            return
        writer.close()
        await writer.wait_closed()
        raise AssertionError("fixture port still open")


def save_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n")
    os.replace(temporary, path)


def record_api(path, method, url):
    with path.open("a") as stream:
        stream.write(json.dumps({"method": method, "path": url}) + "\n")


class Connections:
    def __init__(self, store):
        self.events = []
        self.pending = set()
        self.unsafe = 0

        def checkout(conn, record, proxy):
            self.pending.add(id(record))
            self.events.append({"kind": "checkout", "connection": id(record)})

        def checkin(conn, record):
            self.pending.discard(id(record))
            self.events.append({"kind": "checkin", "connection": id(record)})

        def reset(conn, record, state):
            self.unsafe += int(not state.asyncio_safe)
            self.events.append(
                {"kind": "reset", "connection": id(record), "asyncio_safe": state.asyncio_safe}
            )

        def close(conn, record):
            self.events.append({"kind": "close", "connection": id(record)})

        for name, callback in [
            ("checkout", checkout),
            ("checkin", checkin),
            ("reset", reset),
            ("close", close),
        ]:
            event.listen(store.engine.sync_engine, name, callback)


async def serve(args):
    evidence = Path(args.evidence)
    await asyncio.to_thread(evidence.mkdir, parents=True, exist_ok=True)
    wire = await WireFixture(evidence).start()
    provider = OpenAICompatibleProvider(
        OpenAICompatibleConfig(base_url=f"http://127.0.0.1:{wire.port}/v1", model="fixture")
    )
    policy = None
    if args.token:
        policy = AccessPolicy(
            credentials=(
                Credential(
                    principal_id="local",
                    token=SecretStr(args.token),
                    generation=args.generation,
                    memberships=(Membership(workspace_id="local", role="admin"),),
                    legacy=True,
                ),
            )
        )
    app = None
    task = None
    listener = None
    probe = None
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    class Server(uvicorn.Server):
        def capture_signals(self):
            return nullcontext()

    try:
        app = await open_application(
            Path(args.database),
            settings=Settings(max_active_runs=1),
            provider=provider,
            registry=wire.registry(),
            access_policy=policy,
        )
        probe = Connections(app.store)
        listener = socket.socket()
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", args.port))
        listener.listen(16)
        listener.setblocking(False)
        port = listener.getsockname()[1]
        api = create_app(application=app)

        async def observed(scope, receive, send):
            if scope["type"] == "http":
                record_api(evidence / "api-events.jsonl", scope["method"], scope["path"])
            await api(scope, receive, send)

        server = Server(uvicorn.Config(observed, access_log=False, log_level="warning"))
        task = asyncio.create_task(server.serve(sockets=[listener]), name="installed-session-api")
        async with asyncio.timeout(5):
            while not server.started:
                if task.done():
                    await task
                await asyncio.sleep(0.005)
        import krim_agent

        save_json(
            evidence / "ready.json",
            {
                "namespace_local_pid": os.getpid(),
                "api_port": port,
                "model_port": wire.port,
                "url": f"http://127.0.0.1:{port}",
                "module": krim_agent.__file__,
                "version": __version__,
                "generation": args.generation,
                "database": str(args.database),
            },
        )
        await stop.wait()
        server.should_exit = True
        await task
    finally:
        if task is not None and not task.done():
            server.should_exit = True
            await task
        if listener is not None:
            listener.close()
        if app is not None:
            await app.close()
        await provider.close()
        await wire.close()
        wire.record()
        if app is not None:
            probe_store = await SQLiteStore.open(Path(args.database))
            await probe_store.close()
            state = {
                "namespace_local_pid": os.getpid(),
                "provider_active": provider.active,
                "provider_closed": provider.closed,
                "streams_closed": provider.streams_closed,
                "provider_calls": provider.calls,
                "run_tasks_done": all(t.done() for t in app.service.tasks.values()),
                "controls_done": all(t.done() for t in app.sessions.controls.values()),
                "dispatcher_done": app.sessions._dispatcher.done(),
                "eval_tasks_done": all(t.done() for t in app.evaluations.tasks.values()),
                "connections_pending": list(probe.pending),
                "unsafe_gc": probe.unsafe,
                "connection_events": probe.events,
                "second_writer": True,
                "model_active": len(wire.active),
                "model_errors": wire.errors,
                "database_closed": app.store.closed,
                "model_requests": len(wire.requests),
                "tool_entries": wire.executions,
            }
            save_json(evidence / "closed.json", state)
            assert not probe.pending and probe.unsafe == 0
            assert (
                state["run_tasks_done"]
                and state["controls_done"]
                and state["dispatcher_done"]
                and state["eval_tasks_done"]
            )
            assert provider.active == 0 and not wire.active and not wire.errors


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", required=True)
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--evidence", required=True)
    parser.add_argument("--token", default="")
    parser.add_argument("--generation", type=int, default=1)
    asyncio.run(serve(parser.parse_args()))
