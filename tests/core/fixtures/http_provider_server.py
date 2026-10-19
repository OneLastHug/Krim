"""M3合成本机HTTP/SSE：真实socket、确定屏障与安全进入/关闭计数，不进产品包。"""

from __future__ import annotations

import argparse
import asyncio
import json
import signal
from pathlib import Path


def frame(value):
    raw = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False)
    return ("data: " + raw + "\n\n").encode()


def choice(delta=None, finish=None, usage=None):
    value = {"choices": [{"index": 0, "delta": delta or {}, "finish_reason": finish}]}
    if usage is not None:
        value["usage"] = usage
    return value


class HTTPFixture:
    def __init__(
        self,
        mode="normal",
        *,
        chunks=None,
        status=200,
        headers=None,
        expected_key="",
        delays=(),
        pause_requests=(),
        fail_request=None,
    ):
        self.mode, self.chunks, self.status = mode, chunks, status
        self.headers = headers or {}
        self.expected_key = expected_key
        assert len(delays) <= 2 and all(0 <= delay <= 8 for delay in delays)
        assert set(pause_requests) <= {1, 2} and fail_request in {None, 1, 2}
        self.delays, self.fail_request = delays, fail_request
        self.step_entered = [asyncio.Event(), asyncio.Event()]
        self.step_release = [asyncio.Event(), asyncio.Event()]
        for index, gate_event in enumerate(self.step_release, 1):
            if index not in pause_requests:
                gate_event.set()
        self.requests = []
        self.active = 0
        self.closed_connections = 0
        self.tasks = set()
        self.writers = set()
        self.entered, self.release = asyncio.Event(), asyncio.Event()
        self.idle = asyncio.Event()
        self.idle.set()
        self.server = None
        self.url = ""

    async def __aenter__(self):
        self.server = await asyncio.start_server(self.handle, "127.0.0.1", 0)
        self.port = self.server.sockets[0].getsockname()[1]
        self.url = f"http://127.0.0.1:{self.port}/v1"
        return self

    async def __aexit__(self, *args):
        self.server.close()
        await self.server.wait_closed()
        self.release.set()
        for gate_event in self.step_release:
            gate_event.set()
        for writer in tuple(self.writers):
            writer.close()
        await asyncio.gather(*tuple(self.tasks), return_exceptions=True)
        assert self.active == 0

    def script(self, body):
        messages = body["messages"]
        text = next((m["content"] for m in messages if m["role"] == "user"), "")
        mode = self.mode
        if "FIXTURE_BAD_FRAME" in text:
            return [frame(choice({"content": "部分"})), b"data: {bad}\n\n"]
        if messages[-1]["role"] == "tool":
            result = json.loads(messages[-1]["content"])
            if isinstance(result, dict) and "sum" in result:
                total = result["sum"]
                answer = str(int(total)) if float(total).is_integer() else str(total)
            else:
                answer = str(result)
            result = [frame(choice({"content": answer})), frame(choice(finish="stop"))]
        elif text.startswith(("/add ", "/echo ", "/note ")):
            if text.startswith("/add "):
                _, left, right = text.split(maxsplit=2)
                name, arguments = "add", json.dumps({"left": float(left), "right": float(right)})
            elif text.startswith("/echo "):
                name, arguments = "echo", json.dumps({"text": text[6:]}, ensure_ascii=False)
            else:
                name, arguments = "lookup_note", '{"key":"welcome"}'
            result = [
                frame(
                    choice(
                        {
                            "tool_calls": [
                                {
                                    "index": 0,
                                    "id": "wire-call-1",
                                    "type": "function",
                                    "function": {"name": name, "arguments": arguments},
                                }
                            ]
                        }
                    )
                ),
                frame(choice(finish="tool_calls")),
            ]
        else:
            result = [frame(choice({"content": "你好🙂"})), frame(choice(finish="stop"))]
        if mode != "no-usage" and "FIXTURE_NO_USAGE" not in text:
            result.append(
                frame(
                    {
                        "choices": [],
                        "usage": {"prompt_tokens": 3, "completion_tokens": 2, "total_tokens": 5},
                    }
                )
            )
        result.append(frame("[DONE]"))
        return result

    async def handle(self, reader, writer):
        task = asyncio.current_task()
        self.tasks.add(task)
        self.writers.add(writer)
        self.active += 1
        self.idle.clear()
        try:
            header = await reader.readuntil(b"\r\n\r\n")
            if len(header) > 65536:
                return
            lines = header.decode("ascii").split("\r\n")
            headers = dict(line.split(":", 1) for line in lines[1:] if ":" in line)
            headers = {k.lower(): v.strip() for k, v in headers.items()}
            size = int(headers.get("content-length", "0"))
            if not 0 <= size <= 1048576:
                return
            body = json.loads(await reader.readexactly(size))
            self.requests.append(
                {
                    "path": lines[0].split()[1],
                    "body": body,
                    "authorization_matches": headers.get("authorization", "")
                    == ("Bearer " + self.expected_key if self.expected_key else ""),
                    "accept_encoding": headers.get("accept-encoding"),
                }
            )
            self.entered.set()
            ordinal = len(self.requests)
            status = 502 if ordinal == self.fail_request else self.status
            response_headers = {
                "Content-Type": "text/event-stream",
                "Connection": "close",
            } | self.headers
            writer.write(
                (
                    f"HTTP/1.1 {status} Test\r\n"
                    + "".join(f"{k}: {v}\r\n" for k, v in response_headers.items())
                    + "\r\n"
                ).encode()
            )
            await writer.drain()
            if ordinal <= 2:
                self.step_entered[ordinal - 1].set()
                if ordinal <= len(self.delays):
                    await asyncio.sleep(self.delays[ordinal - 1])
                waiting = asyncio.create_task(self.step_release[ordinal - 1].wait())
                disconnected = asyncio.create_task(reader.read(1))
                try:
                    await asyncio.wait([waiting, disconnected], return_when=asyncio.FIRST_COMPLETED)
                    if disconnected.done():
                        return
                finally:
                    for pending in (waiting, disconnected):
                        pending.cancel()
                    await asyncio.gather(waiting, disconnected, return_exceptions=True)
            if status == 502 and ordinal == self.fail_request:
                writer.write(
                    b'{"error":{"message":"Upstream authentication failed",'
                    b'"type":"upstream_error"}}'
                )
                await writer.drain()
                return
            chunks = self.chunks if self.chunks is not None else self.script(body)
            if self.mode == "hold" or "FIXTURE_HOLD" in str(body):
                writer.write(frame(choice({"content": "等待"})))
                await writer.drain()
                release = asyncio.create_task(self.release.wait())
                disconnected = asyncio.create_task(reader.read(1))
                try:
                    await asyncio.wait([release, disconnected], return_when=asyncio.FIRST_COMPLETED)
                    if disconnected.done():
                        return
                finally:
                    for pending in (release, disconnected):
                        pending.cancel()
                    await asyncio.gather(release, disconnected, return_exceptions=True)
            for chunk in chunks:
                if self.mode == "bytewise":
                    for byte in chunk:
                        writer.write(bytes([byte]))
                        await writer.drain()
                        await asyncio.sleep(0)
                else:
                    writer.write(chunk)
                    await writer.drain()
        except (ConnectionError, asyncio.IncompleteReadError, asyncio.LimitOverrunError):
            pass
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except ConnectionError:
                pass
            self.writers.discard(writer)
            self.active -= 1
            self.closed_connections += 1
            self.tasks.discard(task)
            if self.active == 0:
                self.idle.set()


async def main(args):
    root = Path(args.evidence)
    await asyncio.to_thread(root.mkdir, parents=True, exist_ok=True)
    async with HTTPFixture(args.mode) as fixture:
        (root / "ready.json").write_text(json.dumps({"url": fixture.url, "port": fixture.port}))
        done = asyncio.Event()
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGTERM, done.set)
        try:
            await done.wait()
        finally:
            loop.remove_signal_handler(signal.SIGTERM)
    (root / "stopped.json").write_text(
        json.dumps(
            {
                "requests": fixture.requests,
                "active": fixture.active,
                "closed_connections": fixture.closed_connections,
            }
        )
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--evidence", required=True)
    parser.add_argument("--mode", default="normal")
    asyncio.run(main(parser.parse_args()))
