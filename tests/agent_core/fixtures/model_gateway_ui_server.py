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
    async with HTTPFixture(expected_key="fixture-provider-key") as upstream:
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
                        "authorization_matches": item["authorization_matches"],
                    }
                    for item in upstream.requests
                ]

            (evidence / "ready.json").write_text(
                json.dumps(
                    {
                        "fixture_key": token,
                        "workspace": actor.memberships[0].workspace_id,
                        "upstream": upstream.url,
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
