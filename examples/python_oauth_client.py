"""离线DCR缓存接续：密文保存声明配置、重开/轮换复用原客户端。"""

import asyncio
import json
from pathlib import Path
from tempfile import TemporaryDirectory

from cryptography.fernet import Fernet

from krim_agent.mcp import OAuthClientBinding, OAuthConfig, PersistentOAuthClientRegistrationStore
from krim_agent.persistence import Database


async def main() -> None:
    with TemporaryDirectory(prefix="krim-oauth-client-") as folder:
        url = f"sqlite+aiosqlite:///{Path(folder) / 'client.sqlite'}"
        old, new = Fernet.generate_key(), Fernet.generate_key()
        binding = OAuthClientBinding(
            server="demo",
            subject="trusted-subject",
            resource="https://mcp.example/mcp",
            issuer="https://identity.example",
            redirect_uri="http://127.0.0.1:9000/callback",
            scopes=["tools"],
            allowed_hosts={"identity.example", "mcp.example"},
        )
        config = OAuthConfig(
            server="demo",
            client_id="cached-client",
            client_secret="fake-private-client-secret",
            issuer=binding.issuer,
            resource=binding.resource,
            redirect_uri=binding.redirect_uri,
            scopes=binding.scopes,
            authorization_endpoint="https://identity.example/authorize",
            token_endpoint="https://identity.example/token",
            allowed_hosts=binding.allowed_hosts,
        )
        first = Database(url)
        try:
            await first.initialize()
            store = PersistentOAuthClientRegistrationStore(first, old)
            async with store.lock(binding):
                await store.write(binding, config, expected_revision=0)
        finally:
            await first.close()
        second = Database(url)
        try:
            await second.initialize()
            store = PersistentOAuthClientRegistrationStore(second, [old, new])
            assert await store.rotate_key(new) == 1
            cached = await PersistentOAuthClientRegistrationStore(second, new).read(binding)
            assert cached.config.client_id == "cached-client" and cached.revision == 2
            assert cached.config.client_secret.get_secret_value() == "fake-private-client-secret"
            print(
                json.dumps(
                    {
                        "reopened": True,
                        "client_preserved": True,
                        "secret_preserved": True,
                        "revision": cached.revision,
                    }
                )
            )
        finally:
            await second.close()


if __name__ == "__main__":
    asyncio.run(main())
