import asyncio
import socket

import httpx
import pytest
import uvicorn
from agent_helpers import pending

from krim_agent.api import create_app
from krim_agent.composition import open_application
from krim_agent.config import Settings


@pytest.mark.asyncio
async def test_api_private_result_trace_approval_and_safe_errors(tmp_path):
    async with await open_application(tmp_path / "api.db") as application:
        app = create_app(application=application)
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
            ) as client,
        ):
            response = await client.post(
                "/v1/runs", json={"text": "/add 2 3", "request_key": "api"}
            )
            assert response.status_code == 202
            run = response.json()
            await application.service.wait(run["id"])
            result = (await client.get("/v1/runs/" + run["id"] + "/result")).json()
            assert (
                result["text"] == "5" and result["model_steps"] == 2 and len(result["tools"]) == 1
            )
            trace = (await client.get("/v1/runs/" + run["id"] + "/trace")).json()
            assert trace["run_id"] == run["id"] and trace["complete"]
            response = await client.post(
                "/v1/runs", json={"text": "/note welcome", "request_key": "note"}
            )
            note = response.json()
            request = await pending(application.service, note["id"])
            reply = {
                "request_key": "approve",
                "allow": True,
                **{k: request[k] for k in ("call_id", "arguments_hash", "tool_hash")},
            }
            url = "/v1/runs/" + note["id"] + "/approvals/" + request["request_id"]
            assert (await client.post(url, json=reply)).status_code == 200
            await application.service.wait(note["id"])
            assert (await client.post(url, json=reply)).json()["replayed"] is True
            for body in [
                '{"text":"SYNTHETIC_SECRET","request_key":"bad","extra":"SYNTHETIC_SECRET"}',
                '{"text":"x","text":"y","request_key":"dup"}',
            ]:
                response = await client.post(
                    "/v1/runs", content=body, headers={"content-type": "application/json"}
                )
                assert response.status_code == 422 and "SYNTHETIC_SECRET" not in response.text
            assert (await client.post("/v1/runs", content="x" * 65537)).status_code == 413
            assert (
                await client.get("/healthz", headers={"origin": "https://evil.invalid"})
            ).status_code == 403
            assert (await client.get("/ui/host.html")).status_code == 404
            assert (await client.get("/v1/evals")).status_code == 404


@pytest.mark.asyncio
async def test_api_key_and_nonlocal_spoof_refused(tmp_path):
    async with await open_application(
        tmp_path / "key.db", settings=Settings(api_key="SYNTHETIC_KEY")
    ) as application:
        app = create_app(application=application)
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
            ) as client,
        ):
            assert (await client.get("/v1/runs/unknown")).status_code == 401
            assert (
                await client.get(
                    "/v1/runs/unknown", headers={"Authorization": "Bearer SYNTHETIC_KEY"}
                )
            ).status_code == 404
    async with await open_application(tmp_path / "local.db") as application:
        app = create_app(application=application)
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app, client=("198.51.100.4", 12)),
                base_url="http://127.0.0.1",
            ) as client,
        ):
            assert (await client.get("/healthz")).status_code == 403


@pytest.mark.asyncio
async def test_real_tcp_sse_disconnect_leaves_producer_then_cancel_reaps_it(tmp_path):
    async with await open_application(tmp_path / "tcp.db") as application:
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        listener.listen(16)
        listener.setblocking(False)
        port = listener.getsockname()[1]
        server = uvicorn.Server(
            uvicorn.Config(
                create_app(application=application), log_level="critical", access_log=False
            )
        )
        task = asyncio.create_task(server.serve(sockets=[listener]))
        try:
            async with asyncio.timeout(5):
                while not server.started:
                    if task.done():
                        await task
                    await asyncio.sleep(0.01)
            async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}", timeout=5) as client:
                run = (
                    await client.post(
                        "/v1/runs", json={"text": "/slow", "request_key": "disconnect"}
                    )
                ).json()
                async with client.stream("GET", "/v1/runs/" + run["id"] + "/stream") as response:
                    async for line in response.aiter_lines():
                        if line.startswith("data:"):
                            break
                assert (await client.get("/v1/runs/" + run["id"])).json()["status"] == "running"
                stopped = await client.post("/v1/runs/" + run["id"] + "/cancel")
                assert stopped.json()["status"] == "aborted" and application.provider.active == 0
        finally:
            server.should_exit = True
            await asyncio.wait_for(task, 5)
            listener.close()
            assert task.done()


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["executor", "post"])
async def test_exception_secret_is_private_across_trace_health_errors_and_app_logs(
    tmp_path, caplog, stage
):
    import logging

    from conftest import registry_for

    caplog.set_level(logging.DEBUG, logger="krim_agent")
    secret = "SYNTHETIC_EXCEPTION_AND_RESULT_827c"

    async def execute(args):
        if stage == "executor":
            raise RuntimeError(secret)
        return secret

    async def post(invocation, result):
        if stage == "post":
            raise RuntimeError(secret)

    async with await open_application(
        tmp_path / (stage + "-privacy.db"), registry=registry_for(execute), post=post
    ) as application:
        app = create_app(application=application)
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
            ) as client,
        ):
            response = await client.post(
                "/v1/runs", json={"text": "/echo " + secret, "request_key": stage}
            )
            run = response.json()
            await application.service.wait(run["id"])
            view = await client.get("/v1/runs/" + run["id"])
            trace = await client.get("/v1/runs/" + run["id"] + "/trace")
            health = await client.get("/healthz")
            assert (
                secret not in view.text and secret not in trace.text and secret not in health.text
            )
            assert secret not in caplog.text
            assert view.json()["status"] == "failed"
            assert view.json()["code"] == (
                "tool_error" if stage == "executor" else "post_hook_error"
            )
            private = await application.service.result(run["id"])
            assert private.tools[0].arguments == {"text": secret}
            if stage == "post":
                assert (
                    private.tools[0].value == secret and private.tools[0].result_available is True
                )


@pytest.mark.asyncio
@pytest.mark.parametrize("key", ["", "SYNTHETIC_UI_KEY"])
async def test_ui_static_exact_resources_headers_and_private_paths(tmp_path, key):
    database = tmp_path / "not-created.sqlite"
    app = create_app(Settings(database=database, api_key=key))
    expected_csp = (
        "default-src 'self'; script-src 'self'; style-src 'self'; "
        "connect-src 'self'; object-src 'none'; base-uri 'none'; "
        "frame-ancestors 'none'"
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
    ) as client:
        for name, mime in [
            ("index.html", "text/html"),
            ("assets/app.js", "javascript"),
            ("assets/styles.css", "text/css"),
            ("THIRD-PARTY-NOTICES.txt", "text/plain"),
        ]:
            for method in ["GET", "HEAD"]:
                response = await client.request(method, "/ui/" + name)
                assert response.status_code == 200, (name, method, response.text)
                assert mime in response.headers["content-type"]
                assert response.headers["content-security-policy"] == expected_csp
                assert response.headers["x-content-type-options"] == "nosniff"
                assert response.headers["referrer-policy"] == "no-referrer"
                assert response.headers["cache-control"] == "no-store"
                assert int(response.headers["content-length"]) > 0
                assert bool(response.content) is (method == "GET")
        assert (await client.get("/", follow_redirects=False)).headers["location"] == "/ui/"
        html = (await client.get("/ui/")).text
        assert "/ui/assets/app.js" in html and "/ui/assets/styles.css" in html
        headers = {"Authorization": "Bearer " + key} if key else {}
        for name in [
            "app.js",
            "api.js",
            "styles.css",
            "assets/app.js.map",
            ".env",
            "assets/anything.js",
            "missing",
            "%2e%2e/pyproject.toml",
            "database.sqlite",
        ]:
            assert (await client.get("/ui/" + name, headers=headers)).status_code == 404
        if key:
            assert (await client.get("/v1/runs/unknown")).status_code == 401
    assert not database.exists()
