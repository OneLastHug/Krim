"""逐名提供批准的 UI 构建资源；不暴露源码、环境或数据库。"""

from collections.abc import Awaitable, Callable
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, RedirectResponse

from krim_agent.font_assets import load_font_assets
from krim_agent.script_assets import load_script_assets

FILES = ("index.html", "assets/app.js", "assets/styles.css", "THIRD-PARTY-NOTICES.txt")
PUBLIC_UI_PATHS = frozenset({"/", "/ui/", *("/ui/" + name for name in FILES)})
HEADERS = {
    "Content-Security-Policy": (
        "default-src 'self'; script-src 'self'; style-src 'self'; "
        "connect-src 'self'; img-src 'self' blob:; object-src 'none'; base-uri 'none'; "
        "frame-ancestors 'none'"
    ),
    "X-Content-Type-Options": "nosniff",
    "Referrer-Policy": "no-referrer",
    "Cache-Control": "no-store",
}


def mount_browser_ui(app: FastAPI, *, root: Path | None = None) -> None:
    root = root or (Path(__file__).resolve().parents[1] / "web")
    fonts = load_font_assets(root)
    scripts = load_script_assets(root)
    font_paths = frozenset("/ui/" + asset.path for asset in fonts)
    script_paths = frozenset("/ui/" + asset.path for asset in scripts)
    app.state.public_ui_paths = PUBLIC_UI_PATHS | font_paths | script_paths

    def headers(request: Request) -> dict[str, str]:
        application = getattr(request.app.state, "application", None)
        development = getattr(application, "development", None)
        preview_app = getattr(development, "preview_app", None)
        if preview_app is None:
            return HEADERS
        # no-referrer makes cross-origin form POSTs carry Origin:null in real
        # browsers. The one-use bootstrap requires the exact control Origin.
        # strict-origin exposes no path/query and suppresses downgrade referrers.
        result = {**HEADERS, "Referrer-Policy": "strict-origin"}
        origins = getattr(preview_app, "origins", None)
        if origins is not None and origins.allow_same_site_subdomains:
            result["Content-Security-Policy"] += (
                "; frame-src https://*." + origins.preview_domain
            )
        return result

    @app.get("/", include_in_schema=False)
    async def home(request: Request) -> RedirectResponse:
        return RedirectResponse("/ui/", headers=headers(request))

    def endpoint(
        name: str, media_type: str | None = None,
    ) -> Callable[[Request], Awaitable[FileResponse]]:
        async def serve(request: Request) -> FileResponse:
            return FileResponse(root / name, headers=headers(request), media_type=media_type)

        return serve

    app.add_api_route(
        "/ui/", endpoint("index.html"), methods=["GET", "HEAD"], include_in_schema=False
    )
    for name in FILES:
        app.add_api_route(
            "/ui/" + name, endpoint(name), methods=["GET", "HEAD"], include_in_schema=False
        )
    for asset in scripts:
        if asset.path in FILES:
            continue
        app.add_api_route(
            "/ui/" + asset.path,
            endpoint(asset.path, media_type="application/javascript"),
            methods=["GET", "HEAD"],
            include_in_schema=False,
        )
    for font_asset in fonts:
        app.add_api_route(
            "/ui/" + font_asset.path,
            endpoint(font_asset.path, media_type="font/woff2"),
            methods=["GET", "HEAD"],
            include_in_schema=False,
        )
