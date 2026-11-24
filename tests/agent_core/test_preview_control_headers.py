"""Control UI referrer policy must preserve the native bootstrap POST Origin."""

from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI

from krim_agent.api.web import HEADERS, mount_browser_ui
from krim_agent.dev_preview.boundary import PreviewOrigins


@pytest.mark.parametrize("configured", [False, True])
async def test_control_ui_preserves_origin_only_for_configured_native_preview(configured):
    app = FastAPI()
    app.state.application = SimpleNamespace(
        development=SimpleNamespace(preview_app=object() if configured else None)
    )
    mount_browser_ui(app)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="https://control.test"
    ) as client:
        for path in ("/", "/ui/", "/ui/index.html", "/ui/assets/app.js"):
            response = await client.get(path, follow_redirects=False)
            assert response.status_code in (200, 307)
            assert response.headers["referrer-policy"] == (
                "strict-origin" if configured else "no-referrer"
            )
            assert response.headers["content-security-policy"] == HEADERS["Content-Security-Policy"]
            assert response.headers["cache-control"] == "no-store"
            assert response.headers["x-content-type-options"] == "nosniff"
    assert HEADERS["Referrer-Policy"] == "no-referrer"


async def test_same_site_preview_frames_are_scoped_to_configured_domain():
    app = FastAPI()
    origins = PreviewOrigins(
        "https://control.example.net", "control.example.net", "example.net",
        allow_same_site_subdomains=True, hostname_prefix="preview",
    )
    app.state.application = SimpleNamespace(
        development=SimpleNamespace(preview_app=SimpleNamespace(origins=origins))
    )
    mount_browser_ui(app)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url=origins.control_origin,
    ) as client:
        response = await client.get("/ui/")
        assert response.headers["content-security-policy"] == (
            HEADERS["Content-Security-Policy"] + "; frame-src https://*.example.net"
        )
        assert response.headers["referrer-policy"] == "strict-origin"
    assert "frame-src" not in HEADERS["Content-Security-Policy"]
