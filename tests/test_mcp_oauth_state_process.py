import asyncio
import base64
import hashlib
import json
import socket
import sys
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import uvicorn
from cryptography.fernet import Fernet
from fastapi import FastAPI, Request

from krim_agent.mcp import OAuthConfig, PersistentEncryptedStateStore, PersistentEncryptedTokenStore
from krim_agent.persistence import Database


async def test_two_real_processes_consume_same_reopened_state_and_exchange_once(tmp_path):
    calls, processes = [], []
    app = FastAPI()
    expected = {}

    @app.post("/token")
    async def endpoint(request: Request):
        form = parse_qs((await request.body()).decode())
        calls.append(form)
        challenge = (
            base64.urlsafe_b64encode(hashlib.sha256(form["code_verifier"][0].encode()).digest())
            .rstrip(b"=")
            .decode()
        )
        assert challenge == expected["challenge"]
        assert form["client_id"] == ["client"]
        assert form["client_secret"] == ["fake-secret"]
        assert form["redirect_uri"] == [expected["redirect"]]
        assert form["resource"] == [expected["resource"]]
        return {"access_token": "state-process-access", "expires_in": 600}

    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(128)
    listener.setblocking(False)
    base = f"http://127.0.0.1:{listener.getsockname()[1]}"
    server = uvicorn.Server(uvicorn.Config(app, log_level="error"))
    serving = asyncio.create_task(server.serve(sockets=[listener]))
    url = f"sqlite+aiosqlite:///{tmp_path / 'process.sqlite'}"
    database = Database(url)
    key = Fernet.generate_key()
    config = OAuthConfig(
        server="demo",
        client_id="client",
        client_secret="fake-secret",
        resource=base + "/mcp",
        authorization_endpoint=base + "/authorize",
        token_endpoint=base + "/token",
        redirect_uri=base + "/callback",
        allowed_hosts={"127.0.0.1"},
    )
    try:
        async with asyncio.timeout(5):
            while not server.started:
                if serving.done():
                    await serving
                await asyncio.sleep(0.01)
        await database.initialize()
        attempt = await PersistentEncryptedStateStore(database, key).begin(
            config, subject="subject"
        )
        expected.update(
            challenge=parse_qs(urlsplit(attempt.authorization_url).query)["code_challenge"][0],
            redirect=config.redirect_uri,
            resource=config.resource,
        )
        # 开始登录的数据库连接已经关闭；两个新的进程消费持久状态。
        await database.close()
        configured = config.model_dump(mode="json")
        configured["client_secret"] = "fake-secret"
        payload = (
            json.dumps(
                {
                    "database_url": url,
                    "key": key.decode(),
                    "config": configured,
                    "state": attempt.state,
                }
            ).encode()
            + b"\n"
        )
        script = Path(__file__).parent / "fixtures" / "oauth_state_worker.py"
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
            ready = await asyncio.wait_for(process.stdout.readline(), 8)
            if not ready:
                error = (await process.stderr.read()).decode()
                raise AssertionError(f"state worker exited: {error}")
            assert json.loads(ready) == {"ready": True}
        for process in processes:
            process.stdin.write(b"go\n")
            await process.stdin.drain()
            process.stdin.close()
        results = []
        for process in processes:
            stdout, stderr = await asyncio.wait_for(process.communicate(), 10)
            assert process.returncode == 0, stderr.decode()
            results.append(json.loads(stdout))
        assert sorted(results, key=lambda value: "ok" in value) == [
            {"error": "invalid_state"},
            {"ok": True},
        ]
        assert len(calls) == 1
        await database.initialize()
        token = (await PersistentEncryptedTokenStore(database, key).read(config, "subject")).token
        assert token.access_token.get_secret_value() == "state-process-access"
    finally:
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
