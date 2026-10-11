import asyncio
import json
import socket
import sys
from pathlib import Path

import uvicorn
from cryptography.fernet import Fernet
from fastapi import FastAPI, Request

from krim_agent.mcp import OAuthClientBinding, PersistentOAuthClientRegistrationStore
from krim_agent.persistence import Database


async def test_two_real_processes_only_register_once_with_same_encrypted_client(tmp_path):
    entered, release = asyncio.Event(), asyncio.Event()
    calls = 0
    app = FastAPI()

    @app.post("/register")
    async def register(request: Request):
        nonlocal calls
        calls += 1
        body = await request.json()
        entered.set()
        await release.wait()
        return {
            "client_id": "process-registered-client",
            "client_secret": "process-private-secret",
            "redirect_uris": body["redirect_uris"],
            "grant_types": body["grant_types"],
            "response_types": body["response_types"],
            "token_endpoint_auth_method": "client_secret_post",
            "scope": body["scope"],
        }

    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(128)
    listener.setblocking(False)
    base = f"http://127.0.0.1:{listener.getsockname()[1]}"
    server = uvicorn.Server(uvicorn.Config(app, log_level="error"))
    serving = asyncio.create_task(server.serve(sockets=[listener]))
    url = f"sqlite+aiosqlite:///{tmp_path / 'process.sqlite'}"
    database = Database(url)
    processes = []
    key = Fernet.generate_key()
    binding = OAuthClientBinding(
        server="demo",
        subject="subject",
        resource=base + "/mcp",
        issuer=base,
        redirect_uri="http://127.0.0.1:9000/callback",
        scopes=["tools"],
        allowed_hosts={"127.0.0.1"},
    )
    try:
        async with asyncio.timeout(5):
            while not server.started:
                if serving.done():
                    await serving
                await asyncio.sleep(0.01)
        await database.initialize()
        metadata = {
            "issuer": base,
            "authorization_endpoint": base + "/authorize",
            "token_endpoint": base + "/token",
            "registration_endpoint": base + "/register",
            "response_types_supported": ["code"],
            "grant_types_supported": ["authorization_code", "refresh_token"],
            "code_challenge_methods_supported": ["S256"],
            "token_endpoint_auth_methods_supported": ["client_secret_post"],
            "scopes_supported": ["tools"],
        }
        payload = (
            json.dumps(
                {
                    "database_url": url,
                    "key": key.decode(),
                    "binding": binding.model_dump(mode="json"),
                    "metadata": metadata,
                }
            ).encode()
            + b"\n"
        )
        script = Path(__file__).parent / "fixtures/oauth_client_worker.py"
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
            line = await asyncio.wait_for(process.stdout.readline(), 8)
            if not line:
                raise AssertionError((await process.stderr.read()).decode())
            assert json.loads(line) == {"ready": True}
        for process in processes:
            process.stdin.write(b"go\n")
            await process.stdin.drain()
            process.stdin.close()
        await asyncio.wait_for(entered.wait(), 5)
        assert calls == 1
        release.set()
        for process in processes:
            output, error = await asyncio.wait_for(process.communicate(), 10)
            assert process.returncode == 0, error.decode()
            assert json.loads(output) == {"revision": 1, "ok": True}
        assert calls == 1
        config = (await PersistentOAuthClientRegistrationStore(database, key).read(binding)).config
        assert config.client_secret.get_secret_value() == "process-private-secret"
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
