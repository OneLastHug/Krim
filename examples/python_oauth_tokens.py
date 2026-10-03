"""离线 OAuth 密文、刷新与数据库重开示例；虚构令牌，不连接身份平台。"""

import asyncio
import tempfile
import time
from pathlib import Path

import httpx
from cryptography.fernet import Fernet

from krim_agent.mcp import (
    OAuthClient,
    OAuthConfig,
    OAuthStateStore,
    OAuthToken,
    PersistentEncryptedTokenStore,
)
from krim_agent.persistence import Database


async def main() -> None:
    # 示例密钥只在本进程；真实宿主应从已有密钥管理系统注入相同读密钥环。
    old_key, new_key = Fernet.generate_key(), Fernet.generate_key()
    config = OAuthConfig(
        server="demo",
        client_id="local-demo",
        authorization_endpoint="https://identity.example/authorize",
        token_endpoint="https://identity.example/token",
        redirect_uri="http://127.0.0.1:9000/callback",
        allowed_hosts={"identity.example"},
    )

    async def endpoint(_url: str, _form: dict[str, str]) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "access_token": "offline-fresh",
                "refresh_token": "offline-refresh",
                "expires_in": 600,
            },
        )

    with tempfile.TemporaryDirectory() as directory:
        url = f"sqlite+aiosqlite:///{(Path(directory) / 'oauth.sqlite').as_posix()}"
        database = Database(url)
        try:
            await database.initialize()
            store = PersistentEncryptedTokenStore(database, [old_key, new_key])
            await store.write(
                config,
                "demo-subject",
                OAuthToken(
                    access_token="offline-expired",
                    refresh_token="offline-refresh",
                    expires_at=time.time() - 1,
                ),
            )
            await OAuthClient(OAuthStateStore(), store).access_token(
                config, "demo-subject", endpoint
            )
            print(f"刷新版本：{(await store.read(config, 'demo-subject')).revision}")
            print(f"轮换数量：{await store.rotate_key(new_key)}")
        finally:
            await database.close()
        # 关闭后使用新密钥重开，验证密文持久化；输出不包含令牌或密钥。
        reopened = Database(url)
        try:
            await reopened.initialize()
            snapshot = await PersistentEncryptedTokenStore(reopened, new_key).read(
                config, "demo-subject"
            )
            assert snapshot.token is not None
            print(f"恢复版本：{snapshot.revision}；已恢复：{snapshot.token is not None}")
        finally:
            await reopened.close()


if __name__ == "__main__":
    asyncio.run(main())
