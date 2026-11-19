"""Managed preview-runner entry for the real Broker/XFS acceptance check."""

import asyncio
import json
from pathlib import Path

import uvicorn
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route

from tests.coding_agent.verify_development_broker import run

ROOT = Path("/srv/krim-sandbox/broker-verification-I-03")


async def main():
    status = {"state": "running"}

    async def health(request):
        return JSONResponse(status)

    server = uvicorn.Server(uvicorn.Config(
        Starlette(routes=[Route("/", health)]), host="127.0.0.1", port=5198,
        access_log=False, log_level="critical", proxy_headers=False,
    ))
    serving = asyncio.create_task(server.serve())
    try:
        result = await run(ROOT, start_broker=True)
        status.update(state=result["status"])
        print(json.dumps({"broker_verification": result["status"]}), flush=True)
    except Exception as error:
        status.update(state="failed", error_type=type(error).__name__)
        print(json.dumps(status), flush=True)
    try:
        async with asyncio.timeout(180):
            while not serving.done():
                if await asyncio.to_thread((ROOT / "stop").exists):
                    break
                await asyncio.sleep(0.2)
    except TimeoutError:
        pass
    finally:
        server.should_exit = True
        await serving


if __name__ == "__main__":
    asyncio.run(main())
