"""One-shot deployment worker: IPC only, all browser actions use BrowserSession."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import os
import re
import time
from contextlib import suppress
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from stdio_channel import FDChannel, IPCError

from krim_agent.dev_browser import BrowserArtifact, BrowserError, BrowserLimits, BrowserSession
from krim_agent.dev_browser.context import open_context
from krim_agent.dev_browser.fixed_proxy import FixedProxy, check_authority
from krim_agent.dev_browser.policy import canonical_origin, trusted_control_origin

LIMITS = BrowserLimits()
CLEANUP_SECONDS = 3.0
ALLOWED = {
    "goto": {"op", "path"},
    "resize": {"op", "preset"},
    "download_context": {"op", "path", "cookie_mode"},
    "click": {"op", "selector"},
    "fill": {"op", "selector", "value"},
    "wait": {"op", "selector", "state"},
    "assert": {"op", "selector", "text", "count"},
    "upload": {"op", "selector", "name", "media_type", "data_base64"},
    "screenshot": {"op"},
    "diagnostics": {"op"},
    "close": {"op"},
}
TOKEN = re.compile(r"[A-Za-z0-9_-]{32,128}\Z")


async def dispatch(session: BrowserSession, value: dict[str, Any]) -> dict[str, object]:
    """No selector/path/action validation copy: invoke the sole adapter layer."""
    op = value.get("op")
    if not isinstance(op, str) or op not in ALLOWED or set(value) - ALLOWED[op]:
        raise BrowserError("browser_invalid_input")
    try:
        artifact: BrowserArtifact | None = None
        if op == "goto":
            await session.goto(value.get("path", "/"))
        elif op == "resize":
            await session.resize(value["preset"])
        elif op == "download_context":
            return await session.download_context(value["path"], value["cookie_mode"])
        elif op == "click":
            await session.click(value["selector"])
        elif op == "fill":
            await session.fill(value["selector"], value["value"])
        elif op == "wait":
            await session.wait(value["selector"], value.get("state", "visible"))
        elif op == "assert":
            await session.assert_dom(
                value["selector"], text=value.get("text"), count=value.get("count")
            )
        elif op == "upload":
            data = value["data_base64"]
            if not isinstance(data, str):
                raise BrowserError("browser_invalid_input")
            await session.upload(
                value["selector"],
                name=value["name"],
                media_type=value["media_type"],
                data=base64.b64decode(data, validate=True),
            )
        elif op == "screenshot":
            artifact = await session.screenshot()
        elif op == "diagnostics":
            return session.diagnostics()
        elif op == "close":
            await session.close()
        if artifact is not None:
            return {
                **artifact.metadata,
                "data_base64": base64.b64encode(artifact.data).decode("ascii"),
            }
        return {"done": True}
    except (KeyError, TypeError, ValueError):
        raise BrowserError("browser_invalid_input") from None


async def prepare_ticket(context: Any, ticket: str, origin: str, control_origin: str) -> None:
    """Private IPC ticket exchange, not an action or model-visible result."""
    if not TOKEN.fullmatch(ticket):
        raise BrowserError("browser_bootstrap_failed")
    response = None
    try:
        response = await context.request.post(
            origin + "/.krim-preview/session",
            form={"ticket": ticket},
            headers={"Origin": control_origin},
            max_redirects=0,
            max_retries=0,
            ignore_https_errors=False,
            fail_on_status_code=False,
            timeout=10000,
        )
        if (
            response.status != 303
            or response.headers.get("location") != "/"
            or response.url != origin + "/.krim-preview/session"
        ):
            raise BrowserError("browser_bootstrap_failed")
        cookies = await context.cookies()
        if len(cookies) != 1:
            raise BrowserError("browser_bootstrap_failed")
        cookie = cookies[0]
        if (
            cookie.get("name") != "__Host-Krim-Preview"
            or cookie.get("domain") != urlsplit(origin).hostname
            or cookie.get("path") != "/"
            or cookie.get("secure") is not True
            or cookie.get("httpOnly") is not True
            or cookie.get("sameSite") != "Lax"
            or not TOKEN.fullmatch(cookie.get("value", ""))
            or not time.time() < cookie.get("expires", 0) <= time.time() + 3601
        ):
            raise BrowserError("browser_bootstrap_failed")
    finally:
        if response is not None:
            async with asyncio.timeout(3):
                await response.dispose()


async def drive(channel: Any, session: BrowserSession) -> None:
    # The outer TTL also covers an idle or partial stdin line and blocked stdout.
    while True:
        command = await channel.receive()
        if command is None:
            return
        result = await dispatch(session, command)
        await channel.send({"ok": True, "result": result})
        if command.get("op") == "close":
            return


async def limited_drive(channel: Any, session: BrowserSession, seconds: float = 120) -> None:
    try:
        async with asyncio.timeout(seconds):
            await drive(channel, session)
    finally:
        # A timeout already delivered to this task does not bound its finally.
        # Keep cleanup independently bounded, too; the entrypoint has a Linux
        # 130s process watchdog for an uncooperative driver/third-party close.
        with suppress(Exception):
            async with asyncio.timeout(CLEANUP_SECONDS):
                await session.close()


def verify_ca() -> str:
    path = Path("/run/krim-ca/preview-ca.pem")
    expected = os.environ.get("KRIM_CA_SHA256", "")
    if not re.fullmatch(r"[a-f0-9]{64}", expected) or path.is_symlink() or not path.is_file():
        raise BrowserError("browser_ca_invalid")
    if path.stat().st_size > 65536 or hashlib.sha256(path.read_bytes()).hexdigest() != expected:
        raise BrowserError("browser_ca_invalid")
    return str(path)


async def serve(channel: FDChannel) -> None:
    # Imported only by the approved image entrypoint; tests do not install or
    # monkey-patch Playwright to make a fake browser look like real evidence.
    from playwright.async_api import async_playwright

    expected_origin = canonical_origin(os.environ["KRIM_PREVIEW_ORIGIN"])
    control_origin = trusted_control_origin(os.environ["KRIM_CONTROL_ORIGIN"])
    host = urlsplit(expected_origin).hostname
    ca = verify_ca()
    if os.environ.get("NODE_EXTRA_CA_CERTS") != ca:
        raise BrowserError("browser_ca_invalid")
    deadline = asyncio.get_running_loop().time() + 120
    async with asyncio.timeout_at(deadline):
        first = await channel.receive()
        if (
            first is None
            or set(first) != {"op", "ticket"}
            or first.get("op") != "bootstrap"
            or not isinstance(first.get("ticket"), str)
        ):
            raise BrowserError("browser_bootstrap_required")
        ticket = first["ticket"]
        async with FixedProxy("/run/krim-preview/bridge.sock", host):
            async with async_playwright() as playwright:
                browser = await playwright.chromium.launch(
                    headless=True,
                    chromium_sandbox=True,
                    proxy={"server": "http://127.0.0.1:18080", "bypass": "<-loopback>"},
                    args=[
                        "--disable-quic",
                        "--disable-background-networking",
                        "--disable-extensions",
                    ],
                )
                try:

                    async def check() -> None:
                        # Authority is enforced continuously by the host bridge
                        # and owning host process, not minted by an IPC message.
                        if not browser.is_connected():
                            raise BrowserError("browser_closed")
                        await check_authority("/run/krim-preview/bridge.sock", host)

                    async def prepare(context: Any) -> None:
                        await prepare_ticket(context, ticket, expected_origin, control_origin)

                    async with open_context(
                        browser, expected_origin, check, prepare, limits=LIMITS
                    ) as session:
                        await channel.send({"ok": True, "ready": True})
                        await limited_drive(
                            channel,
                            session,
                            seconds=max(0.001, deadline - asyncio.get_running_loop().time()),
                        )
                finally:
                    with suppress(Exception):
                        async with asyncio.timeout(3):
                            await browser.close()


async def main() -> int:
    channel = FDChannel(0, 1)
    try:
        await serve(channel)
        return 0
    except asyncio.CancelledError:
        raise
    except (BrowserError, IPCError) as exc:
        with suppress(Exception):
            async with asyncio.timeout(0.5):
                await channel.send({"ok": False, "error": str(exc)})
        return 78
    except TimeoutError:
        return 124
    except Exception:
        return 78
    finally:
        channel.close()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
