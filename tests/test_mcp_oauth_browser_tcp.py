import asyncio
import io
import logging
import socket

import httpx
import uvicorn
from test_mcp_oauth_browser import begin, fixture

from krim_agent.mcp.browser import CALLBACK_PATH


async def test_real_uvicorn_callback_log_never_contains_code_state_or_provider_description(runtime):
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(128)
    listener.setblocking(False)
    base = f"http://127.0.0.1:{listener.getsockname()[1]}"
    app, _auth, requester, _tokens, calls, _config = await fixture(runtime, base)
    output = io.StringIO()
    handler = logging.StreamHandler(output)
    access = logging.getLogger("uvicorn.access")
    original = access.handlers[:]
    propagation = access.propagate
    level = access.level
    server = uvicorn.Server(uvicorn.Config(app, log_level="info", access_log=True))
    access.handlers = [handler]
    access.propagate = False
    access.setLevel(logging.INFO)
    serving = asyncio.create_task(server.serve(sockets=[listener]))
    try:
        async with asyncio.timeout(5):
            while not server.started:
                if serving.done():
                    await serving
                await asyncio.sleep(0.01)
        async with httpx.AsyncClient(base_url=base, trust_env=False) as api:
            view, state, _response = await begin(api)
            returned = await api.get(
                CALLBACK_PATH,
                params={"state": state, "code": "sensitive-code-for-log"},
                headers={"Authorization": ""},
            )
            assert returned.status_code == 303 and calls
            logged = output.getvalue()
            assert CALLBACK_PATH in logged
            assert (
                state not in logged
                and "sensitive-code-for-log" not in logged
                and "?state=" not in logged
            )
    finally:
        server.should_exit = True
        try:
            await asyncio.wait_for(serving, 5)
        finally:
            listener.close()
            await requester.close()
            access.handlers = original
            access.propagate = propagation
            access.setLevel(level)
