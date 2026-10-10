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
        "/ui/agents-ui.js",
        "/ui/tasks-ui.js",
        "/ui/workspace-ui.js",
        "/ui/workspace-changes-ui.js",
        "/ui/commands-ui.js",
        "/ui/background-streams.js",
        "/ui/api.js",
        "/ui/host.js",
        "/ui/host-shell.js",
        "/ui/prompt-context.js",
        "/ui/prompt-context-ui.js",
        "/ui/i18n.js",
        "/ui/skills-ui.js",
        "/ui/links.js",
        "/ui/drag-drop.js",
        "/ui/change-list-ui.js",
        "/ui/checkpoint-ui.js",
        "/ui/native-checkpoint-ui.js",
        "/ui/attachments-ui.js",
        "/ui/mcp-ui.js",
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
        "agents-ui.js": "text/javascript",
        "tasks-ui.js": "text/javascript",
        "workspace-ui.js": "text/javascript",
        "workspace-changes-ui.js": "text/javascript",
        "commands-ui.js": "text/javascript",
        "background-streams.js": "text/javascript",
        "api.js": "text/javascript",
        "host.js": "text/javascript",
        "host-shell.js": "text/javascript",
        "prompt-context.js": "text/javascript",
        "prompt-context-ui.js": "text/javascript",
        "i18n.js": "text/javascript",
        "skills-ui.js": "text/javascript",
        "links.js": "text/javascript",
        "drag-drop.js": "text/javascript",
        "change-list-ui.js": "text/javascript",
        "checkpoint-ui.js": "text/javascript",
        "native-checkpoint-ui.js": "text/javascript",
        "attachments-ui.js": "text/javascript",
        "mcp-ui.js": "text/javascript",
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
