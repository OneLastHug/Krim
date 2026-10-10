"""受控子进程：共享 SQLite 状态，只输出就绪、成功或稳定错误码。"""

import asyncio
import json
import sys

from krim_agent.mcp import (
    OAuthClient,
    OAuthConfig,
    OAuthError,
    OAuthHTTPRequester,
    PersistentEncryptedStateStore,
    PersistentEncryptedTokenStore,
)
from krim_agent.persistence import Database


async def main() -> None:
    values = json.loads(sys.stdin.readline())
    database = Database(values["database_url"])
    values["config"]["allowed_hosts"] = set(values["config"]["allowed_hosts"])
    config = OAuthConfig.model_validate(values["config"])
    try:
        await database.initialize()
        client = OAuthClient(
            PersistentEncryptedStateStore(database, values["key"]),
            PersistentEncryptedTokenStore(database, values["key"]),
        )
        print(json.dumps({"ready": True}), flush=True)
        await asyncio.to_thread(sys.stdin.readline)
        async with OAuthHTTPRequester(config.allowed_hosts) as requester:
            try:
                await client.exchange(
                    values["state"], "process-code", "subject", requester, expected_config=config
                )
            except OAuthError as error:
                print(json.dumps({"error": error.code}), flush=True)
            else:
                print(json.dumps({"ok": True}), flush=True)
    finally:
        await database.close()


if __name__ == "__main__":
    asyncio.run(main())
