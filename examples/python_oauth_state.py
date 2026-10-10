"""离线 OAuth 状态接续：关闭原连接、轮换密钥、交换并拒绝重放。"""

import asyncio
import json
from pathlib import Path
from tempfile import TemporaryDirectory

import httpx
from cryptography.fernet import Fernet

from krim_agent.mcp import (
    OAuthClient,
    OAuthConfig,
    OAuthError,
    PersistentEncryptedStateStore,
    PersistentEncryptedTokenStore,
)
from krim_agent.persistence import Database


async def main() -> None:
    with TemporaryDirectory(prefix="krim-oauth-state-") as folder:
        database_url = f"sqlite+aiosqlite:///{Path(folder) / 'state.sqlite'}"
        original_key, next_key = Fernet.generate_key(), Fernet.generate_key()
        config = OAuthConfig(
            server="example",
            client_id="local-client",
            client_secret="fake-client-secret",
            authorization_endpoint="https://identity.example/authorize",
            token_endpoint="https://identity.example/token",
            redirect_uri="http://127.0.0.1:9000/callback",
            resource="https://mcp.example/mcp",
            allowed_hosts={"identity.example"},
        )
        first = Database(database_url)
        try:
            await first.initialize()
            attempt = await PersistentEncryptedStateStore(first, original_key).begin(
                config, subject="trusted-local-subject"
            )
        finally:
            await first.close()

        reopened = Database(database_url)
        try:
            await reopened.initialize()
            rotating = PersistentEncryptedStateStore(reopened, [original_key, next_key])
            assert await rotating.rotate_key(next_key) == 1
            # 新消费者只持新密钥；原 state、配置和到期时间保持不变。
            states = PersistentEncryptedStateStore(reopened, next_key)
            tokens = PersistentEncryptedTokenStore(reopened, next_key)
            client = OAuthClient(states, tokens)
            calls = 0

            async def endpoint(_url: str, form: dict[str, str]) -> httpx.Response:
                nonlocal calls
                calls += 1
                assert form["code_verifier"] == attempt.code_verifier
                assert form["client_secret"] == "fake-client-secret"
                return httpx.Response(200, json={"access_token": "fake-access", "expires_in": 600})

            await client.exchange(
                attempt.state,
                "fake-code",
                "trusted-local-subject",
                endpoint,
                expected_config=config,
            )
            try:
                await client.exchange(attempt.state, "fake-code", "trusted-local-subject", endpoint)
            except OAuthError as error:
                assert error.code == "invalid_state"
            else:
                raise AssertionError("原 state 不应再次交换")
            assert calls == 1
            assert (await tokens.read(config, "trusted-local-subject")).revision == 1
            print(
                json.dumps(
                    {"reopened": True, "rotated": True, "exchanges": calls, "replay_rejected": True}
                )
            )
        finally:
            await reopened.close()


if __name__ == "__main__":
    asyncio.run(main())
