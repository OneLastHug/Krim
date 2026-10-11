"""受控DCR子进程：stdin接收虚构密钥，stdout只报告就绪/版本。"""

import asyncio
import json
import sys

from krim_agent.mcp import (
    OAuthClientBinding,
    OAuthDiscoveryClient,
    OAuthHTTPRequester,
    OAuthMetadata,
    PersistentOAuthClientRegistrationStore,
)
from krim_agent.persistence import Database


async def main() -> None:
    values = json.loads(sys.stdin.readline())
    binding = OAuthClientBinding.model_validate_json(json.dumps(values["binding"]))
    metadata = OAuthMetadata.model_validate_json(json.dumps(values["metadata"]))
    database = Database(values["database_url"])
    try:
        await database.initialize()
        store = PersistentOAuthClientRegistrationStore(database, values["key"])
        print(json.dumps({"ready": True}), flush=True)
        await asyncio.to_thread(sys.stdin.readline)
        async with store.lock(binding):
            cached = await store.read(binding)
            if cached.config is None:
                async with OAuthHTTPRequester(binding.allowed_hosts) as requester:
                    discovery = OAuthDiscoveryClient(requester)
                    registered = await discovery.register(
                        metadata,
                        redirect_uri=binding.redirect_uri,
                        scopes=binding.scopes,
                        token_endpoint_auth_method="client_secret_post",
                    )
                    config = discovery.configuration(
                        metadata, registered, server=binding.server, scopes=binding.scopes
                    )
                    config.resource = binding.resource
                    await store.write(binding, config, expected_revision=cached.revision)
            cached = await store.read(binding)
            assert cached.config.client_id == "process-registered-client"
        print(json.dumps({"revision": cached.revision, "ok": True}), flush=True)
    finally:
        await database.close()


if __name__ == "__main__":
    asyncio.run(main())
