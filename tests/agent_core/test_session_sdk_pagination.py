"""Session SDK shares the service's uncapped offset and bounded page size."""

from unittest.mock import AsyncMock

import pytest

from krim_agent.client import AsyncRunClient
from krim_agent.contracts import ServiceError
from krim_agent.sdk.sessions import SessionSDK


@pytest.mark.parametrize("method", ["list", "history"])
async def test_sdk_forwards_later_page_offsets(method):
    client = AsyncRunClient("http://127.0.0.1:8765", "")
    client.request = AsyncMock(return_value=[])
    sdk = SessionSDK(client)
    try:
        if method == "list":
            assert await sdk.list(offset=200001, limit=50) == []
            expected = "/v2/workspaces/local/sessions?offset=200001&limit=50"
        else:
            assert await sdk.history("session", offset=200001, limit=50) == []
            expected = "/v2/workspaces/local/sessions/session/turns?offset=200001&limit=50"
        client.request.assert_awaited_once_with("GET", expected)
    finally:
        await client.close()


@pytest.mark.parametrize("method", ["list", "history"])
@pytest.mark.parametrize("offset,limit", [(-1, 20), (True, 20), (1.5, 20), (0, True), (0, 51)])
async def test_sdk_rejects_bad_page_values_before_transport(method, offset, limit):
    client = AsyncRunClient("http://127.0.0.1:8765", "")
    client.request = AsyncMock()
    sdk = SessionSDK(client)
    try:
        with pytest.raises(ServiceError, match="invalid_input"):
            if method == "list":
                await sdk.list(offset, limit)
            else:
                await sdk.history("session", offset, limit)
        client.request.assert_not_awaited()
    finally:
        await client.close()
