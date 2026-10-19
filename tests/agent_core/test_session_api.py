"""身份/工作区必须覆盖旧出口，不能只保护新的URL。"""

import asyncio

import httpx
import pytest
from pydantic import SecretStr

from krim_agent.api.server import create_app
from krim_agent.security import AccessPolicy, Credential, Membership
from krim_agent.sessions.types import SessionCreate, TurnRequest


def access_policy():
    return AccessPolicy(
        credentials=(
            Credential(
                principal_id="alice",
                token=SecretStr("alice-synthetic"),
                memberships=(Membership(workspace_id="alpha"),),
                legacy=True,
            ),
            Credential(
                principal_id="bob",
                token=SecretStr("bob-synthetic"),
                memberships=(Membership(workspace_id="beta"),),
            ),
        )
    )


async def test_workspace_scope_covers_every_read_and_control(tmp_path, wire):
    policy = access_policy()
    bob = policy.authenticate("Bearer bob-synthetic")
    async with wire.application(tmp_path / "auth.sqlite", access_policy=policy) as app:
        session = await app.sessions.create(bob, "beta", SessionCreate(request_key="bob-session"))
        turn = await app.sessions.submit(
            bob,
            "beta",
            session.id,
            TurnRequest(request_key="bob-turn", text="/add 2 3", expected_revision=0),
        )
        turn = await app.sessions.wait(bob, "beta", session.id, turn.id)
        assert turn.status == "completed"
        api = create_app(application=app)
        async with (
            api.router.lifespan_context(api),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=api),
                base_url="http://127.0.0.1",
                headers={"Authorization": "Bearer alice-synthetic"},
            ) as client,
        ):
            before = len(wire.requests), len(wire.executions)
            response = await client.get(f"/v1/runs/{turn.run_id}/candidate?include_input=true")
            assert response.status_code == 404, response.text
            for suffix in ["", "/result", "/trace", "/events", "/stream", "/approvals"]:
                assert (await client.get(f"/v1/runs/{turn.run_id}" + suffix)).status_code == 404
            assert (await client.get(f"/v1/runs/by-key/ac-turn.{turn.id}")).status_code == 404
            assert (await client.post(f"/v1/runs/{turn.run_id}/cancel")).status_code == 404
            base = f"/v2/workspaces/beta/sessions/{session.id}"
            for suffix in [
                "",
                "/turns",
                f"/turns/{turn.id}",
                f"/turns/by-key/{turn.request_key}",
                f"/turns/{turn.id}/events",
                f"/turns/{turn.id}/stream",
                f"/turns/{turn.id}/result",
                f"/turns/{turn.id}/trace",
                f"/turns/{turn.id}/approvals",
                "/controls/by-key/unknown",
            ]:
                assert (await client.get(base + suffix)).status_code == 404, suffix
            for suffix in ["/close", "/resume", f"/turns/{turn.id}/cancel"]:
                assert (
                    await client.post(
                        base + suffix, json={"request_key": "deny", "expected_revision": 1}
                    )
                ).status_code == 404
            assert (
                await client.post(
                    base + "/turns",
                    json={"request_key": "deny", "text": "不应执行", "expected_revision": 1},
                )
            ).status_code == 404
            assert (
                await client.get(f"/v2/workspaces/alpha/sessions/{session.id}")
            ).status_code == 404
            assert (
                await client.post(
                    "/v1/runs", json={"text": "/add 2 3", "request_key": "ac-turn." + turn.id}
                )
            ).status_code == 422
            assert before == (len(wire.requests), len(wire.executions))


def reference():
    return dict(
        expected="5",
        expected_status="completed",
        expected_code=None,
        expected_tools=None,
        trajectory=None,
        expected_schema=None,
    )


@pytest.mark.parametrize("position", [0, 1, 2])
async def test_candidate_and_freeze_authorize_every_source(tmp_path, wire, position):
    from krim_agent.contracts import RunRequest

    policy = access_policy()
    bob = policy.authenticate("Bearer bob-synthetic")
    async with wire.application(tmp_path / "freeze-auth.sqlite", access_policy=policy) as app:
        session = await app.sessions.create(bob, "beta", SessionCreate(request_key="bob"))
        turn = await app.sessions.submit(
            bob,
            "beta",
            session.id,
            TurnRequest(request_key="turn", text="/add 2 3", expected_revision=0),
        )
        turn = await app.sessions.wait(bob, "beta", session.id, turn.id)
        good = await app.service.start(RunRequest(text="/add 2 3", request_key="legacy"))
        await app.service.wait(good.id)
        good_candidate = await app.evaluations.candidate(good.id, True)
        bad_candidate = await app.evaluations.candidate(turn.run_id, True)

        def case(identifier, candidate, index):
            return dict(
                case_id=f"case-{index}",
                source_kind="run",
                source_run_id=identifier,
                candidate_fingerprint=candidate.candidate_fingerprint,
                selected_input="/add 2 3",
                reason="合成参考",
                reference=reference(),
            )

        cases = [case(good.id, good_candidate, i) for i in range(3)]
        cases[position] = case(turn.run_id, bad_candidate, position)
        api = create_app(application=app)
        async with (
            api.router.lifespan_context(api),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=api),
                base_url="http://127.0.0.1",
                headers={"Authorization": "Bearer alice-synthetic"},
            ) as client,
        ):
            before = len(wire.requests), len(wire.executions)
            result = await client.post(
                "/v1/datasets", json=dict(name="mixed", version="1", cases=cases)
            )
            assert result.status_code == 404, result.text
            from krim_agent.contracts import ServiceError

            with pytest.raises(ServiceError, match="not_found"):
                await app.store.dataset("mixed", "1")
            assert before == (len(wire.requests), len(wire.executions))


async def test_freeze_authorizes_sources_before_existing_revision_hit(tmp_path):
    import json
    import os
    import shutil
    from pathlib import Path

    from krim_agent.composition import open_application
    from krim_agent.storage.sqlite import SQLiteStore

    saved = json.loads(
        Path(os.environ["KRIM_AGENT_CORE_LEGACY_BASELINE"])
        .with_name("legacy-facts-freeze.json")
        .read_text()
    )
    source = Path(saved["database"])
    before = await asyncio.to_thread(source.read_bytes)
    database = tmp_path / "historical-copy.sqlite"
    shutil.copyfile(source, database)
    policy = AccessPolicy(
        credentials=(
            Credential(
                principal_id="local",
                token=SecretStr("historical-owner"),
                memberships=(Membership(workspace_id="other", role="admin"),),
                legacy=True,
            ),
        )
    )
    async with await open_application(database, access_policy=policy) as app:
        revision = await app.store.dataset("session-source", "1")
        body = revision.selection().model_dump(mode="json")
        api = create_app(application=app)
        async with (
            api.router.lifespan_context(api),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=api),
                base_url="http://127.0.0.1",
                headers={"Authorization": "Bearer historical-owner"},
            ) as client,
        ):
            response = await client.post("/v1/datasets", json=body)
            assert response.status_code == 404, response.text
        assert (await app.store.dataset("session-source", "1")).fingerprint == revision.fingerprint
        assert app.provider.calls == 0
    assert await asyncio.to_thread(source.read_bytes) == before
    store = await SQLiteStore.open(database, readonly=True)
    await store.close()


async def test_invalid_access_policy_fails_before_open_or_network(tmp_path, wire):
    from krim_agent.composition import open_application
    from krim_agent.config import Settings
    from krim_agent.contracts import ServiceError

    for name, body in [
        ("duplicate", '{"credentials":[]}'),
        ("bad", '{"credentials":[{"principal_id":"x","token":"hidden"}]}'),
    ]:
        path = tmp_path / (name + ".json")
        path.write_text(body)
        path.chmod(0o600)
        database = tmp_path / (name + ".sqlite")
        with pytest.raises(ServiceError, match="access_policy_invalid"):
            await open_application(database, settings=Settings(access_policy_file=path))
        assert not database.exists()
    path = tmp_path / "good.json"
    path.write_text('{"credentials":[]}')
    path.chmod(0o644)
    with pytest.raises(ServiceError, match="access_policy_invalid"):
        await open_application(
            tmp_path / "permissions.sqlite", settings=Settings(access_policy_file=path)
        )
    link = tmp_path / "link.json"
    link.symlink_to(path)
    with pytest.raises(ServiceError, match="access_policy_invalid"):
        await open_application(tmp_path / "link.sqlite", settings=Settings(access_policy_file=link))
    with pytest.raises(ServiceError, match="access_policy_invalid"):
        await open_application(
            tmp_path / "conflict.sqlite",
            settings=Settings(api_key=SecretStr("synthetic")),
            access_policy=access_policy(),
        )
    assert wire.requests == []


async def test_local_remote_cli_sdk_ownership(tmp_path):
    import json
    import os
    import signal
    import sys
    from pathlib import Path

    from krim_agent.client import AsyncRunClient
    from krim_agent.sdk import SessionSDK
    from krim_agent.storage.sqlite import SQLiteStore

    evidence = tmp_path / "remote-server"
    script = Path(__file__).parent / "fixtures/session_server.py"
    server = await asyncio.create_subprocess_exec(
        sys.executable,
        "-B",
        str(script),
        "--database",
        str(tmp_path / "remote.sqlite"),
        "--evidence",
        str(evidence),
        "--port",
        "0",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    ledger = []

    async def wait_until(check):
        async with asyncio.timeout(8):
            while True:
                value = await asyncio.to_thread(check)
                if value:
                    return value
                if server.returncode is not None:
                    raise AssertionError("server exited")
                await asyncio.sleep(0.005)

    async def command(arguments, extra=None):
        argv = [sys.executable, "-B", "-m", "krim_agent", *arguments]
        child = await asyncio.create_subprocess_exec(
            *argv,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=tmp_path,
            env={**os.environ, **(extra or {})},
        )
        try:
            out, err = await asyncio.wait_for(child.communicate(), 8)
            record = {
                "argv": argv,
                "cwd": str(tmp_path),
                "namespace_pid": child.pid,
                "rc": child.returncode,
                "stdout": out.decode(),
                "stderr": err.decode(),
            }
            ledger.append(record)
            return child.returncode, json.loads(out)
        finally:
            if child.returncode is None:
                child.kill()
                await child.communicate()

    try:
        ready = await wait_until(
            lambda: (
                json.loads((evidence / "ready.json").read_text())
                if (evidence / "ready.json").exists()
                else None
            )
        )
        assert ready["namespace_local_pid"] == server.pid
        client = AsyncRunClient(ready["url"])
        try:
            sdk = SessionSDK(client)
            session = await sdk.create(SessionCreate(request_key="remote-cli"))
            rc, turn = await command(
                [
                    "turn",
                    "start",
                    "--url",
                    ready["url"],
                    "--session",
                    session.id,
                    "--key",
                    "remote-turn",
                    "--revision",
                    "0",
                    "--text",
                    "/hold",
                ]
            )
            assert rc == 0 and turn["status"] in {"queued", "running"}
            await wait_until(
                lambda: (
                    (evidence / "model-counts.json").exists()
                    and json.loads((evidence / "model-counts.json").read_text())["requests"]
                )
            )
            assert (await sdk.turn(session.id, turn["id"])).status == "running"
            path = f"/v2/workspaces/local/sessions/{session.id}/turns/{turn['id']}"
            baseline = (evidence / "api-events.jsonl").read_text().count(path)
            argv = [
                sys.executable,
                "-B",
                "-m",
                "krim_agent",
                "turn",
                "wait",
                "--url",
                ready["url"],
                "--session",
                session.id,
                "--id",
                turn["id"],
            ]
            waiter = await asyncio.create_subprocess_exec(
                *argv, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, cwd=tmp_path
            )
            try:
                await wait_until(
                    lambda: (evidence / "api-events.jsonl").read_text().count(path) > baseline
                )
                waiter.send_signal(signal.SIGINT)
                out, err = await asyncio.wait_for(waiter.communicate(), 5)
                ledger.append(
                    {
                        "argv": argv,
                        "namespace_pid": waiter.pid,
                        "signal": "SIGINT",
                        "rc": waiter.returncode,
                        "stdout": out.decode(),
                        "stderr": err.decode(),
                    }
                )
                assert waiter.returncode == 130
            finally:
                if waiter.returncode is None:
                    waiter.kill()
                    await waiter.communicate()
            assert (await sdk.turn(session.id, turn["id"])).status == "running"
            counts = json.loads((evidence / "model-counts.json").read_text())
            assert len(counts["requests"]) == 1 and counts["executions"] == []
            rc, stopped = await command(
                [
                    "turn",
                    "cancel",
                    "--url",
                    ready["url"],
                    "--session",
                    session.id,
                    "--id",
                    turn["id"],
                    "--key",
                    "explicit-stop",
                    "--revision",
                    "1",
                ]
            )
            assert rc == 0 and stopped["result"]["status"] == "cancelled"
        finally:
            await client.close()
        database = tmp_path / "local.sqlite"
        rc, session = await command(
            ["session", "create", "--database", str(database), "--key", "local"]
        )
        assert rc == 0
        rc, value = await command(
            [
                "turn",
                "start",
                "--database",
                str(database),
                "--session",
                session["id"],
                "--key",
                "local-turn",
                "--revision",
                "0",
                "--text",
                "/add 2 3",
            ]
        )
        assert rc == 0 and value["status"] == "completed"
        before = database.read_bytes()
        rc, facts = await command(
            [
                "turn",
                "result",
                "--database",
                str(database),
                "--session",
                session["id"],
                "--id",
                value["id"],
            ]
        )
        assert rc == 0 and facts["text"] == "5" and facts["model_steps"] == 2
        writer = await SQLiteStore.open(database)
        try:
            for operation in ["wait", "cancel"]:
                args = [
                    "turn",
                    operation,
                    "--database",
                    str(database),
                    "--session",
                    session["id"],
                    "--id",
                    value["id"],
                ]
                if operation == "cancel":
                    args += ["--key", "must-not-open-writer", "--revision", "1"]
                rc, rejected = await command(args)
                assert rc == 1 and rejected["error"]["code"] == "readonly"
        finally:
            await writer.close()
        assert database.read_bytes() == before
        optional = tmp_path / "optional-http.sqlite"
        host = {
            "KRIM_AGENT_HTTP_BASE_URL": f"http://127.0.0.1:{ready['model_port']}/v1",
            "KRIM_AGENT_HTTP_MODEL": "fixture",
        }
        rc, http_session = await command(
            [
                "session",
                "create",
                "--database",
                str(optional),
                "--key",
                "http-session",
                "--target",
                "target-http",
            ],
            host,
        )
        assert rc == 0
        for index, text in enumerate(["请记住基数17", "与3相加"]):
            rc, http_turn = await command(
                [
                    "turn",
                    "start",
                    "--database",
                    str(optional),
                    "--session",
                    http_session["id"],
                    "--key",
                    f"http-{index}",
                    "--revision",
                    str(index),
                    "--text",
                    text,
                ],
                host,
            )
            assert rc == 0 and http_turn["status"] == "completed"
        rc, result = await command(
            [
                "turn",
                "result",
                "--database",
                str(optional),
                "--session",
                http_session["id"],
                "--id",
                http_turn["id"],
            ]
        )
        assert rc == 0 and result["text"] == "20" and result["tools"][0]["value"] == {"sum": 20.0}
        # 本机中断在真实持久model_start窗口触发，不拿它冒充上游调用计数。
        import sqlite3

        argv = [
            sys.executable,
            "-B",
            "-m",
            "krim_agent",
            "turn",
            "start",
            "--database",
            str(database),
            "--session",
            session["id"],
            "--key",
            "local-interrupt",
            "--revision",
            "1",
            "--text",
            "/slow",
        ]
        interrupted = await asyncio.create_subprocess_exec(
            *argv, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, cwd=tmp_path
        )

        def durable_start():
            conn = sqlite3.connect(database)
            try:
                return (
                    conn.execute(
                        "SELECT count(*) FROM run_events e JOIN turns t ON e.run_id=t.run_id "
                        "WHERE t.request_key='local-interrupt' AND e.kind='model_start'"
                    ).fetchone()[0]
                    > 0
                )
            finally:
                conn.close()

        try:
            await wait_until(durable_start)
            interrupted.send_signal(signal.SIGINT)
            out, err = await asyncio.wait_for(interrupted.communicate(), 5)
            ledger.append(
                {
                    "argv": argv,
                    "namespace_pid": interrupted.pid,
                    "signal": "SIGINT",
                    "barrier": "durable model_start intent",
                    "rc": interrupted.returncode,
                    "stdout": out.decode(),
                    "stderr": err.decode(),
                }
            )
            assert interrupted.returncode == 130
        finally:
            if interrupted.returncode is None:
                interrupted.kill()
                await interrupted.communicate()
        store = await SQLiteStore.open(database, readonly=True)
        try:
            from krim_agent.storage.sessions import turn_by_key

            assert (
                await turn_by_key(store, session["id"], "local-interrupt")
            ).status == "cancelled"
        finally:
            await store.close()
    finally:
        if server.returncode is None:
            server.send_signal(signal.SIGTERM)
        out, err = await asyncio.wait_for(server.communicate(), 8)
        ledger.append(
            {
                "kind": "server",
                "creator_namespace_pid": os.getpid(),
                "owned_child_namespace_pid": server.pid,
                "rc": server.returncode,
                "stdout": out.decode(),
                "stderr": err.decode(),
            }
        )
        (tmp_path / "cli-ownership.json").write_text(
            json.dumps(ledger, ensure_ascii=False, indent=2)
        )
        assert server.returncode == 0, err.decode()
        closed = json.loads((evidence / "closed.json").read_text())
        assert (
            closed["unsafe_gc"] == 0
            and closed["connections_pending"] == []
            and closed["second_writer"]
        )
