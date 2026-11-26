"""真实 API/SQLite/保险库的本地 UI 联调入口，仅连接合成 HTTP/SSE 上游。"""

from __future__ import annotations

import argparse
import asyncio
import json
import secrets
import tempfile
from pathlib import Path

import uvicorn
from fixtures.http_provider_server import HTTPFixture

from krim_agent.account_mcp.secrets import MasterKeyring
from krim_agent.accounts.service import IdentityService
from krim_agent.api import create_app
from krim_agent.composition import open_application
from krim_agent.config import Settings
from krim_agent.providers.openai_compatible import OpenAICompatibleConfig
from krim_agent.storage.sqlite import SQLiteStore


class DirectoryFixture(HTTPFixture):
    """A model-list endpoint in front of the unchanged chat/SSE fixture."""

    async def __aenter__(self):
        await super().__aenter__()
        self.directory_requests = []
        self.directory_server = await asyncio.start_server(self.directory, "127.0.0.1", 0)
        directory_port = self.directory_server.sockets[0].getsockname()[1]
        self.directory_url = f"http://127.0.0.1:{directory_port}/v1"
        return self

    async def __aexit__(self, *args):
        self.directory_server.close()
        await self.directory_server.wait_closed()
        await super().__aexit__(*args)

    async def directory(self, reader, writer):
        remote_writer = None
        try:
            header = await reader.readuntil(b"\r\n\r\n")
            lines = header.decode("ascii").split("\r\n")
            method, path, _ = lines[0].split()
            headers = {
                key.lower(): value.strip()
                for key, value in (line.split(":", 1) for line in lines[1:] if ":" in line)
            }
            if method == "GET":
                authenticated = headers.get("authorization") == "Bearer fixture-provider-key"
                self.directory_requests.append(
                    {"path": path, "authorization_matches": authenticated}
                )
                status = 200 if authenticated else 401
                if "unsupported" in path:
                    status = 404
                models = [
                    {"id": "custom-model-a", "name": "端点推理模型 A"},
                    {"id": "custom-model-b", "name": "端点模型 B"},
                    {
                        "id": "vendor/reasoning-model-with-a-long-name:latest",
                        "name": "用于检查窄屏换行的长名称推理模型",
                    },
                    {"id": "custom-model-a", "name": "duplicate"},
                ]
                payload = (
                    {"data": [] if "empty" in path else models}
                    if status == 200
                    else {"error": "fixture error"}
                )
                data = (
                    b"not-json"
                    if "invalid" in path
                    else json.dumps(payload, ensure_ascii=False).encode()
                )
                writer.write(
                    (
                        f"HTTP/1.1 {status} Fixture\r\n"
                        "Content-Type: application/json\r\n"
                        f"Content-Length: {len(data)}\r\n"
                        "Connection: close\r\n\r\n"
                    ).encode()
                    + data
                )
                await writer.drain()
                return
            body = await reader.readexactly(int(headers.get("content-length", "0")))
            remote_reader, remote_writer = await asyncio.open_connection("127.0.0.1", self.port)
            remote_writer.write(header + body)
            await remote_writer.drain()
            while chunk := await remote_reader.read(65536):
                writer.write(chunk)
                await writer.drain()
        finally:
            if remote_writer is not None:
                remote_writer.close()
            writer.close()


async def serve(port: int, evidence: Path) -> None:
    await asyncio.to_thread(evidence.mkdir, parents=True, exist_ok=True)
    private = Path(await asyncio.to_thread(tempfile.mkdtemp, prefix="krim-gateway-ui-"))
    database = private / "ui.sqlite"
    key_file = private / "master-key.json"
    MasterKeyring.create(secrets.token_bytes(32)).save(key_file)
    store = await SQLiteStore.open(database)
    try:
        identity = await IdentityService.open(store)
        bootstrap = await identity.bootstrap("网关联调账号", request_key="gateway-ui-bootstrap")
        token = bootstrap.token
        await identity.close()
    finally:
        await store.close()
    async with DirectoryFixture(expected_key="fixture-provider-key") as upstream:
        async with await open_application(
            database,
            settings=Settings(database=database),
            http_config=OpenAICompatibleConfig(
                base_url=upstream.url,
                model="host-model",
                api_key="fixture-provider-key",
            ),
            personal_mcp_key_file=key_file,
        ) as application:
            actor = application.identities.authenticate("Bearer " + token)
            api = create_app(application=application)

            @api.get("/__fixture__/requests")
            async def requests():
                return [
                    {
                        "model": item["body"].get("model"),
                        "reasoning_effort": item["body"].get("reasoning_effort"),
                        "thinking": item["body"].get("thinking"),
                        "max_tokens": item["body"].get("max_tokens"),
                        "authorization_matches": item["authorization_matches"],
                    }
                    for item in upstream.requests
                ]

            @api.get("/__fixture__/directory-requests")
            async def directory_requests():
                return upstream.directory_requests

            (evidence / "ready.json").write_text(
                json.dumps(
                    {
                        "fixture_key": token,
                        "workspace": actor.memberships[0].workspace_id,
                        "upstream": upstream.directory_url,
                        "database": str(database),
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            print(f"Gateway UI fixture ready on port {port}", flush=True)
            await uvicorn.Server(
                uvicorn.Config(api, host="0.0.0.0", port=port, access_log=False)
            ).serve()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=5198)
    parser.add_argument("--evidence", type=Path, default=Path(".cache/model-gateway-ui"))
    args = parser.parse_args()
    asyncio.run(serve(args.port, args.evidence))
