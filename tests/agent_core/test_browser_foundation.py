"""Host-side contracts for the isolated read-only browser foundation."""

import asyncio
import base64
import struct
import time
import zlib

import pytest

from krim_agent.browser.egress import BrowserEgressPolicy
from krim_agent.browser.manager import BrowserManager
from krim_agent.browser.protocol import (
    BrowserCommand,
    BrowserResult,
    BrowserWorkerPolicy,
    result_from_wire,
    validate_png,
)
from krim_agent.contracts import ServiceError


RUN_ID = "12345678-1234-4234-8234-123456789abc"
PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"


def run_async(test):
    def wrapper():
        asyncio.run(test())

    wrapper.__name__ = test.__name__
    return wrapper


def png_1x1() -> bytes:
    def chunk(kind: bytes, body: bytes) -> bytes:
        checksum = zlib.crc32(kind + body) & 0xFFFFFFFF
        return struct.pack(">I", len(body)) + kind + body + struct.pack(">I", checksum)

    header = struct.pack(">IIBBBBB", 1, 1, 8, 6, 0, 0, 0)
    pixels = zlib.compress(b"\x00\x00\x00\x00\x00")
    return PNG_SIGNATURE + chunk(b"IHDR", header) + chunk(b"IDAT", pixels) + chunk(b"IEND", b"")


class FakeWorker:
    def __init__(self, responses=None):
        self.responses = list(responses or [])
        self.commands = []
        self.closed = False

    async def command(self, message):
        self.commands.append(dict(message))
        if self.responses:
            response = self.responses.pop(0)
            if isinstance(response, BaseException):
                raise response
            return response
        return {"operation": message["operation"]}

    async def close(self):
        self.closed = True


def policy():
    return BrowserEgressPolicy(["https://example.com"])


def test_command_protocol_is_closed_and_only_navigate_accepts_url():
    assert BrowserCommand("navigate", "https://example.com/").wire() == {
        "operation": "navigate",
        "url": "https://example.com/",
    }
    for operation in ("click", "fill", "evaluate", "download", "login"):
        with pytest.raises((ValueError, ServiceError)):
            BrowserCommand(operation)  # type: ignore[arg-type]
    with pytest.raises(ServiceError, match="browser_invalid_input"):
        BrowserCommand("snapshot", "https://example.com/")
    with pytest.raises(ServiceError, match="browser_invalid_input"):
        BrowserCommand("navigate")  # type: ignore[arg-type]


def test_egress_policy_is_exact_https_origin_and_rejects_unsafe_addresses():
    checked = BrowserEgressPolicy(["https://example.com", "https://xn--bcher-kva.example:8443"])
    assert checked.authorize_url("https://EXAMPLE.com:443/a?q=hello") == ("example.com", 443)
    assert checked.authorize_url("wss://example.com/socket") == ("example.com", 443)
    assert checked.authorize_url("https://bücher.example:8443/path") == (
        "xn--bcher-kva.example",
        8443,
    )
    nested_idn = BrowserEgressPolicy(["https://www.xn--bcher-kva.example"])
    assert nested_idn.authorize_url("https://www.xn--bcher-kva.example/") == (
        "www.xn--bcher-kva.example",
        443,
    )
    assert checked.authorize_addresses(["93.184.216.34"]) == ("93.184.216.34",)
    for url in (
        "http://example.com/",
        "https://sub.example.com/",
        "https://example.com:444/",
        "https://user@example.com/",
        "file:///etc/passwd",
        "javascript:alert(1)",
        "https://127.0.0.1/",
    ):
        with pytest.raises(ServiceError):
            checked.authorize_url(url)
    for addresses in (["10.0.0.1"], ["93.184.216.34", "127.0.0.1"], []):
        with pytest.raises(ServiceError, match="browser_egress_denied"):
            checked.authorize_addresses(addresses)
    for origin in (
        "https://*.example.com",
        "https://example.com/path",
        "https://localhost",
        "https://127.1",
        "https://0x7f.0.0.1",
        "https://example.com..",
        "https://[::ffff:93.184.216.34]",
    ):
        with pytest.raises(ValueError):
            BrowserEgressPolicy([origin])


def test_png_validator_checks_dimensions_crc_and_bounded_decompression():
    def replace_ihdr_dimensions(data: bytes, *, width: int, height: int) -> bytes:
        header = struct.pack(">IIBBBBB", width, height, 8, 6, 0, 0, 0)
        ihdr = struct.pack(">I", len(header)) + b"IHDR" + header
        ihdr += struct.pack(">I", zlib.crc32(b"IHDR" + header) & 0xFFFFFFFF)
        return PNG_SIGNATURE + ihdr + data[33:]

    png = png_1x1()
    validate_png(png)
    validate_png(bytes(png))
    corrupt_crc = png[:-5] + bytes([png[-5] ^ 1]) + png[-4:]
    huge_dimensions = replace_ihdr_dimensions(png, width=5000, height=1)
    for invalid in (b"not png", png[:-12], corrupt_crc, huge_dimensions):
        with pytest.raises(ServiceError, match="browser_artifact_invalid"):
            validate_png(invalid)
    interlaced = bytearray(png)
    interlaced[28] = 1
    interlaced[29:33] = struct.pack(">I", zlib.crc32(b"IHDR" + interlaced[16:29]) & 0xFFFFFFFF)
    with pytest.raises(ServiceError, match="browser_artifact_invalid"):
        validate_png(bytes(interlaced))
    with pytest.raises(ServiceError, match="browser_artifact_invalid"):
        validate_png(png + b"trailing")


def test_worker_replies_are_operation_bound_bounded_and_strict():
    png = png_1x1()
    decoded = result_from_wire(
        "screenshot", {"operation": "screenshot", "screenshot_png_base64": base64.b64encode(png).decode()}
    )
    assert decoded.screenshot_png == png
    with pytest.raises(ServiceError, match="browser_protocol"):
        result_from_wire("snapshot", {"operation": "navigate", "final_url": "https://example.com"})
    with pytest.raises(ServiceError, match="browser_protocol"):
        result_from_wire("navigate", {"operation": "navigate", "debug": "leak"})
    with pytest.raises(ServiceError, match="browser_output_limit"):
        BrowserResult("snapshot", text="x" * 32769)
    with pytest.raises(ServiceError, match="browser_artifact_invalid"):
        result_from_wire("screenshot", {"operation": "screenshot", "screenshot_png_base64": "bm90cG5n"})


def test_worker_policy_fingerprint_binds_image_proxy_and_allowed_operations():
    first = BrowserWorkerPolicy("a" * 64, "b" * 64)
    assert first.digest != BrowserWorkerPolicy("c" * 64, "b" * 64).digest
    assert first.digest != BrowserWorkerPolicy("a" * 64, "d" * 64).digest
    with pytest.raises(ValueError):
        BrowserWorkerPolicy("not-a-digest", "b" * 64)


@run_async
async def test_manager_navigates_snapshots_screenshots_and_closes_once():
    png = png_1x1()
    worker = FakeWorker(
        [
            {"operation": "navigate", "final_url": "https://example.com/page", "title": "Hello"},
            {"operation": "snapshot", "final_url": "https://example.com/page",
             "text": "bounded page text", "truncated": False},
            {"operation": "screenshot", "final_url": "https://example.com/page",
             "screenshot_png_base64": base64.b64encode(png).decode()},
            {"operation": "close"},
        ]
    )
    manager = BrowserManager(RUN_ID, worker, policy())
    assert (await manager.navigate("https://example.com/start")).final_url == "https://example.com/page"
    assert (await manager.snapshot()).text == "bounded page text"
    assert (await manager.screenshot()).screenshot_png == png
    await manager.close()
    await manager.close()
    assert worker.commands == [
        {"operation": "navigate", "url": "https://example.com/start"},
        {"operation": "snapshot"},
        {"operation": "screenshot"},
        {"operation": "close"},
    ]
    assert worker.closed is True
    with pytest.raises(ServiceError, match="browser_closed"):
        await manager.snapshot()


@run_async
async def test_manager_rejects_redirect_escape_and_disposes_worker():
    worker = FakeWorker([{"operation": "navigate", "final_url": "https://evil.example/"}])
    manager = BrowserManager(RUN_ID, worker, policy())
    with pytest.raises(ServiceError, match="browser_egress_denied"):
        await manager.navigate("https://example.com/")
    assert manager.closed and worker.closed
    assert worker.commands == [{"operation": "navigate", "url": "https://example.com/"}]

    bad_responses = (
        {"operation": "snapshot", "final_url": "https://evil.example/", "text": "page text"},
        {"operation": "screenshot", "final_url": "https://evil.example/",
         "screenshot_png_base64": base64.b64encode(png_1x1()).decode()},
    )
    for response in bad_responses:
        operation = response["operation"]
        worker = FakeWorker([response])
        manager = BrowserManager(RUN_ID, worker, policy())
        with pytest.raises(ServiceError, match="browser_egress_denied"):
            await getattr(manager, operation)()
        assert manager.closed and worker.closed


@run_async
async def test_manager_rejects_unapproved_url_before_worker_call():
    worker = FakeWorker()
    manager = BrowserManager(RUN_ID, worker, policy())
    with pytest.raises(ServiceError, match="browser_egress_denied"):
        await manager.navigate("https://evil.example/")
    assert worker.commands == []
    assert not manager.closed
    await manager.close()


@run_async
async def test_manager_closes_worker_on_timeout_and_worker_failure():
    never = asyncio.Event()

    class SlowWorker(FakeWorker):
        async def command(self, message):
            self.commands.append(dict(message))
            await never.wait()
            return {"operation": message["operation"]}

    worker = SlowWorker()
    manager = BrowserManager(RUN_ID, worker, policy(), timeout_seconds=0.1)
    with pytest.raises(ServiceError, match="browser_timeout"):
        await manager.snapshot()
    assert worker.closed

    broken = FakeWorker([RuntimeError("worker error must not leak")])
    manager = BrowserManager(RUN_ID, broken, policy())
    with pytest.raises(ServiceError, match="browser_worker_failed") as error:
        await manager.snapshot()
    assert "worker error" not in str(error.value)
    assert broken.closed

    async def failing_close():
        raise RuntimeError("cleanup secret detail")

    leaking = FakeWorker([RuntimeError("operation failure")])
    leaking.close = failing_close
    manager = BrowserManager(RUN_ID, leaking, policy())
    with pytest.raises(ServiceError, match="browser_worker_failed") as error:
        await manager.snapshot()
    assert "cleanup secret" not in str(error.value)

    hanging = FakeWorker([TimeoutError("worker command timeout")])

    async def never_close():
        await asyncio.Event().wait()

    hanging.close = never_close
    manager = BrowserManager(RUN_ID, hanging, policy(), timeout_seconds=0.1)
    started = time.monotonic()
    with pytest.raises(ServiceError, match="browser_timeout"):
        await manager.snapshot()
    assert time.monotonic() - started < 0.5

    closing = FakeWorker()
    closing.close = never_close
    manager = BrowserManager(RUN_ID, closing, policy(), timeout_seconds=0.1)
    started = time.monotonic()
    with pytest.raises(ServiceError, match="browser_worker_failed"):
        await manager.close()
    assert time.monotonic() - started < 0.5


@run_async
async def test_manager_cancellation_joins_worker_cleanup():
    entered = asyncio.Event()
    release = asyncio.Event()

    class WaitingWorker(FakeWorker):
        async def command(self, message):
            self.commands.append(dict(message))
            if message["operation"] != "close":
                entered.set()
                await release.wait()
            return {"operation": message["operation"]}

    worker = WaitingWorker()
    manager = BrowserManager(RUN_ID, worker, policy(), timeout_seconds=10)
    task = asyncio.create_task(manager.snapshot())
    await asyncio.wait_for(entered.wait(), 1)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    assert worker.closed
    assert {item["operation"] for item in worker.commands} == {"snapshot"}
