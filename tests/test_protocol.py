import asyncio
import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from krim_agent.protocol import ChannelResult, InProcessBridge, ProtocolEnvelope
from krim_agent.protocol.models import check_json


@pytest.mark.parametrize(
    "vector", json.loads(Path("tests/contracts/vectors.json").read_text("utf-8"))
)
def test_vectors(vector):
    model = ChannelResult if vector["model"] == "result" else ProtocolEnvelope
    if vector["valid"]:
        actual = model.model_validate(vector["input"]).model_dump(exclude_none=True)
        assert actual == vector["expected"]
    else:
        with pytest.raises(ValidationError):
            model.model_validate(vector["input"])


def test_json_limits():
    with pytest.raises(ValueError):
        check_json(float("nan"))
    with pytest.raises(ValueError):
        check_json("x" * 65537)
    value = None
    for _depth in range(34):
        value = [value]
    with pytest.raises(ValueError):
        check_json(value)


async def test_emit_notify_unregister_and_errors():
    host, app = InProcessBridge.pair()
    seen = asyncio.Event()

    async def handler(data):
        seen.set()
        return data

    unregister = app.on("echo", handler)
    result = await host.emit("echo", {"hello": "你好"})
    assert result.ok and result.data == {"hello": "你好"}
    host.notify("echo", "通知")
    await asyncio.wait_for(seen.wait(), 1)
    unregister()
    unregister()
    assert not (await host.emit("echo")).ok

    async def bad(_data):
        raise RuntimeError("private secret")

    app.on("bad", bad)
    assert "secret" not in (await host.emit("bad")).error

    async def own_timeout(_data):
        raise TimeoutError("handler error, not a bridge deadline")

    app.on("own_timeout", own_timeout)
    assert not (await host.emit("own_timeout")).ok
    await host.close()
    await app.close()
    assert not host.ready
    with pytest.raises(ConnectionError):
        await host.emit("echo")


async def test_timeout_and_cancel():
    host, app = InProcessBridge.pair()
    cancelled = asyncio.Event()

    async def wait(_data):
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    app.on("wait", wait)
    with pytest.raises(TimeoutError):
        await host.emit("wait", timeout=0.01)
    assert cancelled.is_set()
    task = asyncio.create_task(host.emit("wait"))
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await host.close()
    await app.close()


async def test_stream_order_and_terminal_errors():
    host, app = InProcessBridge.pair()

    async def values(_data):
        yield {"text": "一"}
        yield {"text": "二"}

    app.on_stream("tokens", values)
    frames = [frame async for frame in host.stream("tokens")]
    assert [frame.seq for frame in frames] == [0, 1, 2, 3]
    assert [frame.kind for frame in frames] == ["start", "chunk", "chunk", "end"]
    assert len({frame.stream_id for frame in frames}) == 1
    missing = [frame async for frame in host.stream("missing")]
    assert missing[-1].kind == "error"

    async def broken(_data):
        yield "partial"
        raise RuntimeError("secret")

    app.on_stream("broken", broken)
    errors = [frame async for frame in host.stream("broken")]
    assert [frame.seq for frame in errors] == [0, 1, 2]
    assert errors[-1].kind == "error"
    await host.close()
    await app.close()


def test_version_negotiation():
    with pytest.raises(ValueError):
        InProcessBridge(version=3)
