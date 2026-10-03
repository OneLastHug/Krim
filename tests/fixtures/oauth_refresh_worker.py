"""受控测试子进程：仅从 stdin 接收虚构配置/密钥，stdout 只输出状态。"""

import asyncio
import json
import sys

from krim_agent.mcp import (
    OAuthClient,
    OAuthConfig,
    OAuthHTTPRequester,
    OAuthStateStore,
    PersistentEncryptedTokenStore,
)
from krim_agent.persistence import Database


async def main() -> None:
    values = json.loads(sys.stdin.read())
    database = Database(values["database_url"])
    configured = values["config"]
    configured["allowed_hosts"] = set(configured["allowed_hosts"])
    config = OAuthConfig.model_validate(configured)
    try:
        await database.initialize()
        store = PersistentEncryptedTokenStore(database, values["key"])
        snapshot = await store.read(config, "subject")
        print(json.dumps({"ready": snapshot.revision}), flush=True)
        async with OAuthHTTPRequester(config.allowed_hosts) as request:
            token = await OAuthClient(OAuthStateStore(), store).access_token(
                config, "subject", request, refresh_skew_seconds=0
            )
        print(
            json.dumps(
                {
                    "ok": token.access_token.get_secret_value() == "process-fresh",
                    "revision": (await store.read(config, "subject")).revision,
                }
            ),
            flush=True,
        )
    finally:
        await database.close()


if __name__ == "__main__":
    asyncio.run(main())
