"""浏览器静态资源白名单；不暴露项目源码、配置或数据库。"""

from collections.abc import Awaitable, Callable
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse, RedirectResponse

PUBLIC_UI_PATHS = frozenset(
    {
        "/",
        "/ui",
        "/ui/",
        "/ui/index.html",
        "/ui/host.html",
        "/ui/styles.css",
        "/ui/app.js",
        "/ui/api.js",
        "/ui/host.js",
        "/ui/host-shell.js",
    }
)
_CSP = (
    "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; "
    "img-src 'self' data:; object-src 'none'; base-uri 'none'; form-action 'self'; "
    "frame-ancestors 'self'"
)
_HEADERS = {
    "Content-Security-Policy": _CSP,
    "X-Content-Type-Options": "nosniff",
    "Referrer-Policy": "same-origin",
    "Cache-Control": "no-cache",
}


def mount_browser_ui(app: FastAPI) -> None:
    directory = Path(__file__).resolve().parents[1] / "web"
    resources = {
        "index.html": "text/html",
        "host.html": "text/html",
        "styles.css": "text/css",
        "app.js": "text/javascript",
        "api.js": "text/javascript",
        "host.js": "text/javascript",
        "host-shell.js": "text/javascript",
    }

    @app.get("/", include_in_schema=False)
    async def root() -> RedirectResponse:
        return RedirectResponse("/ui/", headers=_HEADERS)

    @app.get("/ui", include_in_schema=False)
    async def ui_redirect() -> RedirectResponse:
        return RedirectResponse("/ui/", headers=_HEADERS)

    async def index() -> FileResponse:
        return FileResponse(directory / "index.html", media_type="text/html", headers=_HEADERS)

    app.add_api_route("/ui/", index, methods=["GET", "HEAD"], include_in_schema=False)

    def resource_route(filename: str, media_type: str) -> Callable[[], Awaitable[FileResponse]]:
        async def serve() -> FileResponse:
            return FileResponse(directory / filename, media_type=media_type, headers=_HEADERS)

        return serve

    for name, media in resources.items():
        app.add_api_route(
            f"/ui/{name}",
            resource_route(name, media),
            methods=["GET", "HEAD"],
            include_in_schema=False,
        )
