"""当前分支的真实本机 HTTP/MCP 与网页工作台合成夹具。"""

import argparse
import asyncio
import json
import socket
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from tempfile import TemporaryDirectory

import uvicorn
from fastapi import FastAPI, Request
from pydantic import SecretStr

from krim_agent.api import create_app
from krim_agent.composition import open_application
from krim_agent.config import Settings
from krim_agent.mcp import MCPServerConfig
from krim_agent.providers.types import ModelText, ModelToolCall
from krim_agent.tools.runtime import ToolRuntimeConfig
from krim_agent.web_access import WebAccessConfig


class WebModel:
    mode = "local-web-tools"

    async def stream(self, messages, tools):
        last_user = max(index for index, item in enumerate(messages) if item.role == "user")
        results = [item for item in messages[last_user + 1 :] if item.role == "tool"]
        if results:
            yield ModelText(text=results[-1].content)
            return
        text = messages[last_user].content
        fetch = text.startswith("fetch ")
        name = "web_fetch" if fetch else "web_search"
        spec = next(item for item in tools if item["name"] == name)
        if fetch:
            arguments = {"url": text[6:]}
        else:
            key = "query" if "query" in spec["input_schema"]["properties"] else "text"
            arguments = {key: text}
        yield ModelToolCall(id="fixture-call", name=name, arguments=arguments)

    async def close(self):
        pass


@asynccontextmanager
async def fixture(directory: Path, *, override=True, no_http_key=False):
    counts = {"search": 0, "extract": 0}
    upstream = FastAPI()

    @upstream.post("/search")
    async def search(request: Request):
        assert request.headers["authorization"] == "Bearer fixture-web-key"
        counts["search"] += 1
        body = await request.json()
        return {
            "results": [
                {
                    "title": "HTTP 搜索",
                    "url": "https://example.com/article",
                    "content": "HTTP source: " + body["query"],
                }
            ],
            "usage": {"credits": 1},
        }

    @upstream.post("/extract")
    async def extract(request: Request):
        assert request.headers["authorization"] == "Bearer fixture-web-key"
        counts["extract"] += 1
        body = await request.json()
        return {
            "results": [
                {
                    "title": "HTTP 阅读",
                    "url": body["urls"][0],
                    "raw_content": "HTTP article from local fixture",
                }
            ],
            "failed_results": [],
            "usage": {"credits": 1},
        }

    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(32)
    listener.setblocking(False)
    port = listener.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(upstream, log_level="error"))
    serving = asyncio.create_task(server.serve(sockets=[listener]))
    application = None
    model = WebModel()
    try:
        async with asyncio.timeout(10):
            while not server.started:
                if serving.done():
                    await serving
                await asyncio.sleep(0.01)
        source = MCPServerConfig(
            name="fixture",
            transport="stdio",
            command=sys.executable,
            args=(str(Path(__file__).parents[2] / "fixtures/agent_core_mcp_server.py"),),
            selected_tools=("echo",),
            readonly_tools=frozenset({"echo"}),
        )
        config = ToolRuntimeConfig(
            web=WebAccessConfig(
                endpoint=f"http://127.0.0.1:{port}",
                api_key=None if no_http_key else SecretStr("fixture-web-key"),
            ),
            mcp_servers=(source,),
            tool_overrides={"web_search": "fixture__echo"} if override else {},
            public_tools=("web_search", "web_fetch", "fixture__echo"),
        )
        database = directory / "web.sqlite"
        application = await open_application(
            database,
            settings=Settings(
                database=database,
                api_key=SecretStr("browser-test-key"),
                run_timeout=45,
                tool_timeout=15,
            ),
            provider=model,
            tool_config=config,
        )
        api = create_app(application=application)

        @api.get("/test/tool-counts")
        async def tool_counts():
            return counts

        yield api, application, counts
    finally:
        try:
            if application is not None:
                await application.close()
            await model.close()
        finally:
            server.should_exit = True
            await asyncio.wait_for(serving, 10)
            listener.close()


async def main(port):
    with TemporaryDirectory(prefix="krim-web-tools-") as folder:
        async with fixture(Path(folder)) as (api, application, counts):
            server = uvicorn.Server(
                uvicorn.Config(
                    api, host="127.0.0.1", port=port, access_log=False, log_level="warning"
                )
            )

            @api.post("/test/shutdown")
            async def shutdown():
                server.should_exit = True
                return {"stopping": True}

            print(
                json.dumps({"fixture": "web-tools", "port": port, "python": sys.executable}),
                flush=True,
            )
            await server.serve()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8878)
    asyncio.run(main(parser.parse_args().port))
