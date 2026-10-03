import asyncio
import json
import socket
import sys
import time
from pathlib import Path

import uvicorn
from cryptography.fernet import Fernet
from fastapi import FastAPI, Request

from krim_agent.mcp import OAuthConfig, OAuthToken, PersistentEncryptedTokenStore
from krim_agent.persistence import Database


async def test_two_real_processes_share_one_refresh_lease(tmp_path):
    entered, release = asyncio.Event(), asyncio.Event()
    calls = []
    app = FastAPI()

    @app.post("/token")
    async def endpoint(request: Request):
        calls.append((await request.body()).decode())
        entered.set()
        await release.wait()
        return {"access_token": "process-fresh", "refresh_token": "rotated", "expires_in": 600}

    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(128)
    listener.setblocking(False)
    base = f"http://127.0.0.1:{listener.getsockname()[1]}"
    server = uvicorn.Server(uvicorn.Config(app, log_level="error"))
    serving = asyncio.create_task(server.serve(sockets=[listener]))
    database_url = f"sqlite+aiosqlite:///{tmp_path / 'process.sqlite'}"
    database = Database(database_url)
    processes = []
    try:
        async with asyncio.timeout(5):
            while not server.started:
                if serving.done():
                    await serving
                await asyncio.sleep(0.01)
        await database.initialize()
        key = Fernet.generate_key()
        config = OAuthConfig(
            server="demo",
            client_id="client",
            authorization_endpoint=base + "/authorize",
            token_endpoint=base + "/token",
            redirect_uri=base + "/callback",
            allowed_hosts={"127.0.0.1"},
        )
        store = PersistentEncryptedTokenStore(database, key)
        await store.write(
            config,
            "subject",
            OAuthToken(
                access_token="process-old",
                refresh_token="process-refresh",
                expires_at=time.time() - 60,
            ),
        )
        payload = json.dumps(
            {
                "database_url": database_url,
                "config": config.model_dump(mode="json"),
                "key": key.decode(),
            }
        ).encode()
        script = Path(__file__).parent / "fixtures" / "oauth_refresh_worker.py"
        for _ in range(2):
            process = await asyncio.create_subprocess_exec(
                sys.executable,
                str(script),
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            processes.append(process)
            process.stdin.write(payload)
            await process.stdin.drain()
            process.stdin.close()
        for process in processes:
            line = await asyncio.wait_for(process.stdout.readline(), 8)
            assert json.loads(line) == {"ready": 1}
        await asyncio.wait_for(entered.wait(), 5)
        assert len(calls) == 1
        release.set()
        for process in processes:
            stdout, stderr = await asyncio.wait_for(process.communicate(), 8)
            assert process.returncode == 0, stderr.decode()
            assert json.loads(stdout) == {"ok": True, "revision": 2}
        assert len(calls) == 1 and "grant_type=refresh_token" in calls[0]
        assert (await store.read(config, "subject")).revision == 2
    finally:
        release.set()
        for process in processes:
            if process.returncode is None:
                process.kill()
            await process.wait()
        server.should_exit = True
        try:
            await asyncio.wait_for(serving, 5)
        finally:
            listener.close()
            await database.close()
