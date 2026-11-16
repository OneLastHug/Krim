"""Container-only fixed no-network Chromium probe; no app or external page."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path


async def main() -> None:
    from playwright.async_api import Error, async_playwright

    if os.getuid() != 1000 or os.getgid() != 1000:
        raise RuntimeError("probe_wrong_user")
    report: dict[str, object] = {
        "uid": os.getuid(),
        "gid": os.getgid(),
        "playwright": importlib.metadata.version("playwright"),
        "interfaces": network_state()[0],
        "ip_routes": network_state()[1],
        "process_status": selected_status(Path("/proc/self/status")),
    }
    assert report["interfaces"] == ["lo"]
    assert len(report["ip_routes"]) == 1
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=True,
            chromium_sandbox=True,
            args=["--disable-quic", "--disable-background-networking", "--disable-extensions"],
        )
        try:
            report["chromium"] = browser.version
            context = await browser.new_context(
                accept_downloads=False,
                service_workers="block",
                ignore_https_errors=False,
                viewport={"width": 1024, "height": 768},
                permissions=[],
            )
            try:
                page = await context.new_page()
                try:
                    await page.goto("chrome://sandbox", wait_until="domcontentloaded")
                    status = await page.locator("body").inner_text(timeout=10000)
                    report["chrome_sandbox_status"] = status[:8192]
                except Error as exc:
                    if "ERR_INVALID_URL" not in str(exc):
                        raise
                    report["chrome_sandbox_status"] = "internal_page_unavailable_in_headless_shell"
                # An unsupported chrome:// navigation may asynchronously load
                # chrome-error://. Close that page rather than race its fallback.
                await page.close()
                page = await context.new_page()
                # Fixed in-memory component fixture; no web server is started.
                await page.set_content(
                    "<!doctype html><title>Browser component check</title>"
                    '<label>Name<input id="name"></label><button id="go">Check</button>'
                    '<p id="result"></p><input type="file" id="file">'
                    '<script>document.querySelector("#go").onclick=()=>{'
                    'document.querySelector("#result").textContent='
                    'document.querySelector("#name").value;};</script>'
                )
                await page.locator("#name").fill("component-ok")
                await page.locator("#go").click()
                assert await page.locator("#result").text_content() == "component-ok"
                await page.locator("#file").set_input_files(
                    {"name": "fixture.txt", "mimeType": "text/plain", "buffer": b"fixture"}
                )
                data = await page.screenshot(type="png", full_page=False)
                assert data.startswith(b"\x89PNG\r\n\x1a\n") and len(data) <= 1048576
                report["fixture"] = {
                    "click_fill": True,
                    "upload": True,
                    "png_bytes": len(data),
                    "png_sha256": hashlib.sha256(data).hexdigest(),
                    "png_base64": base64.b64encode(data).decode("ascii"),
                }
                # Probe-only internal inspection via Playwright's existing pipe;
                # no CDP socket/server is exposed by the deployed worker.
                cdp = await browser.new_browser_cdp_session()
                info = await cdp.send("SystemInfo.getProcessInfo")
                await cdp.detach()
                report["processes"] = inspect_reported_processes(info["processInfo"])
                renderers = [p for p in report["processes"] if p["role"] == "renderer"]
                assert renderers, "probe_renderer_missing"
                outer_filters = int(report["process_status"].get("Seccomp_filters", "0"))
                for renderer in renderers:
                    assert renderer["status"].get("Seccomp") == "2"
                    assert renderer["status"].get("NoNewPrivs") == "1"
                report["renderer_additional_seccomp_filter"] = all(
                    int(p["status"].get("Seccomp_filters", "0")) > outer_filters for p in renderers
                )
                assert report["renderer_additional_seccomp_filter"]
                browser_process = next(p for p in report["processes"] if p["role"] == "browser")
                for renderer in renderers:
                    assert renderer["status"]["CapEff"] == "0000000000000000"
                    assert renderer["no_sandbox_flag"] is False
                    for kind in ("user", "pid", "net"):
                        assert renderer["namespaces"][kind] not in {
                            "unreadable",
                            browser_process["namespaces"][kind],
                        }
                assert browser_process["no_sandbox_flag"] is False
            finally:
                await context.close()
        finally:
            await browser.close()
    # Recheck known PIDs, rather than an executable-path heuristic which can
    # miss sandboxed renderer argv. No other program is spawned in the probe.
    known_pids = [p["pid"] for p in report["processes"]]
    remaining = remaining_processes(known_pids)
    for _ in range(60):
        if not remaining:
            break
        await asyncio.sleep(0.05)
        remaining = remaining_processes(known_pids)
    report["browser_processes_after_close"] = remaining
    assert not remaining
    print(json.dumps({"ok": True, "result": report}, sort_keys=True), flush=True)


def network_state() -> tuple[list[str], list[str]]:
    return (
        sorted(p.name for p in Path("/sys/class/net").iterdir()),
        Path("/proc/net/route").read_text().splitlines(),
    )


def selected_status(path: Path) -> dict[str, str]:
    keys = {"Name", "Uid", "Gid", "NoNewPrivs", "Seccomp", "Seccomp_filters", "CapEff"}
    return {
        k: value.strip()
        for line in path.read_text().splitlines()
        if ":" in line
        for k, value in [line.split(":", 1)]
        if k in keys
    }


def inspect_reported_processes(processes: list[dict[str, object]]) -> list[dict[str, object]]:
    result = []
    for process in processes:
        pid = int(process["id"])
        path = Path("/proc") / str(pid)
        item = {"pid": pid, "role": process["type"]}
        try:
            args = (path / "cmdline").read_bytes().split(b"\0")
            item["no_sandbox_flag"] = b"--no-sandbox" in args
        except (PermissionError, FileNotFoundError):
            item["no_sandbox_flag"] = "unreadable"
        try:
            item["status"] = selected_status(path / "status")
        except (PermissionError, FileNotFoundError):
            item["status"] = {"unavailable": "proc_status_unreadable"}
        namespaces = {}
        for kind in ("user", "pid", "net", "mnt"):
            try:
                namespaces[kind] = os.readlink(path / "ns" / kind)
            except (PermissionError, FileNotFoundError):
                namespaces[kind] = "unreadable"
        item["namespaces"] = namespaces
        result.append(item)
    return result


def remaining_processes(pids: list[int]) -> list[int]:
    return [pid for pid in pids if (Path("/proc") / str(pid)).exists()]


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as exc:
        # No external content is visited, but don't emit arbitrary driver argv
        # or stderr. The host can inspect the typed failure and fixed phase.
        markers = [
            marker
            for marker in (
                "Operation not permitted",
                "No usable sandbox",
                "Failed to move to new namespace",
                "Executable doesn't exist",
                "ERR_INVALID_URL",
                "ERR_FAILED",
                "Target closed",
                "error while loading shared libraries",
                "Running as root",
                "Permission denied",
            )
            if marker in str(exc)
        ]
        startup_lines = [
            line[:700] for line in str(exc).splitlines() if "[err]" in line or "exitCode=" in line
        ][:16]
        print(
            json.dumps(
                {
                    "ok": False,
                    "error_type": type(exc).__name__,
                    "markers": markers,
                    "startup_lines": startup_lines,
                    # Only fixed internal pages and the static fixture are used;
                    # no ticket, cookie, remote page or user data enters this probe.
                    "probe_error": "\n".join(str(exc).splitlines()[:6])[:2000],
                }
            ),
            flush=True,
        )
        raise SystemExit(1) from None
