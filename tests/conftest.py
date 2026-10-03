import asyncio
import socket
from collections.abc import AsyncIterator
from pathlib import Path
from uuid import uuid4

import httpx
import pytest
import uvicorn

from krim_agent.api import create_app
from krim_agent.config import Settings
from krim_agent.persistence import Database, KrimRepository
from krim_agent.providers import FakeProvider
from krim_agent.runtime import ChatRuntime
from krim_agent.tools.builtins import builtin_registry


@pytest.fixture
async def repository() -> AsyncIterator[KrimRepository]:
    path = Path(".cache/test-data") / str(uuid4()) / "test.sqlite"
    database = Database(f"sqlite+aiosqlite:///{path.as_posix()}")
    await database.initialize()
    yield KrimRepository(database)
    await database.close()


@pytest.fixture
async def runtime(repository: KrimRepository) -> AsyncIterator[ChatRuntime]:
    service = ChatRuntime(repository, FakeProvider(delay=0), builtin_registry(), Settings())
    yield service
    await service.close()


@pytest.fixture
async def api(runtime: ChatRuntime) -> AsyncIterator[httpx.AsyncClient]:
    app = create_app(runtime=runtime)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1:8765"
        ) as client,
    ):
        yield client


@pytest.fixture
async def server_url(runtime):
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(128)
    listener.setblocking(False)
    port = listener.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(create_app(runtime=runtime), log_level="error"))
    task = asyncio.create_task(server.serve(sockets=[listener]))
    try:
        async with asyncio.timeout(10):
            while not server.started:
                if task.done():
                    await task
                await asyncio.sleep(0.01)
        yield f"http://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        await asyncio.wait_for(task, 10)
        listener.close()
