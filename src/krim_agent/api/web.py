"""逐名提供批准的 UI 构建资源；不暴露源码、环境或数据库。"""

from collections.abc import Awaitable, Callable
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse, RedirectResponse

FILES = ("index.html", "assets/app.js", "assets/styles.css", "THIRD-PARTY-NOTICES.txt")
PUBLIC_UI_PATHS = frozenset({"/", "/ui/", *("/ui/" + name for name in FILES)})
HEADERS = {
    "Content-Security-Policy": (
        "default-src 'self'; script-src 'self'; style-src 'self'; "
        "connect-src 'self'; object-src 'none'; base-uri 'none'; "
        "frame-ancestors 'none'"
    ),
    "X-Content-Type-Options": "nosniff",
    "Referrer-Policy": "no-referrer",
    "Cache-Control": "no-store",
}


def mount_browser_ui(app: FastAPI) -> None:
    root = Path(__file__).resolve().parents[1] / "web"

    @app.get("/", include_in_schema=False)
    async def home() -> RedirectResponse:
        return RedirectResponse("/ui/", headers=HEADERS)

    def endpoint(name: str) -> Callable[[], Awaitable[FileResponse]]:
        async def serve() -> FileResponse:
            return FileResponse(root / name, headers=HEADERS)

        return serve

    app.add_api_route(
        "/ui/", endpoint("index.html"), methods=["GET", "HEAD"], include_in_schema=False
    )
    for name in FILES:
        app.add_api_route(
            "/ui/" + name, endpoint(name), methods=["GET", "HEAD"], include_in_schema=False
        )
