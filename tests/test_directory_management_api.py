"""目录公开入口必须读取原账本，并保留权限、分页和CLI退出状态。"""

import asyncio
import json
import os
import socket
import sys
from dataclasses import replace
from pathlib import Path

import httpx
import pytest
import uvicorn
from pydantic import ValidationError
from sqlalchemy import func, select, update

from krim_agent.agents import AgentProfile, AgentProfileUpdate
from krim_agent.agents.types import AgentError
from krim_agent.api import create_app
from krim_agent.application.agents import AgentService
from krim_agent.client import ClientError, KrimClient
from krim_agent.config import Settings
from krim_agent.persistence.database import (
    AgentSessionRow,
    WorkspaceDirectoryAttemptRow,
    WorkspaceDirectoryProposalRow,
)
from krim_agent.sdk.agents import AgentSession
from krim_agent.workspace_directory import (
    DirectoryChangeAction,
    DirectoryChangeSpec,
    DirectoryPreviewRequest,
    WorkspaceDirectoryWriter,
)

NAMES = [
    "directory_inspect",
    "directory_preview",
    "directory_change",
    "directory_reconcile",
    "directory_finalize",
    "directory_query",
]
KEY = "directory-test-key"
pytestmark = pytest.mark.skipif(
    not sys.platform.startswith("linux"), reason="需要Linux目录条件事务"
)


async def policy(definition, _context):
    return "allow" if definition.risk == "read" else "ask"


@pytest.fixture
async def directory_server(tmp_path):
    root = tmp_path / "workspace"
    root.mkdir()
    app = create_app(
        Settings(database_url=f"sqlite+aiosqlite:///{tmp_path / 'tcp.sqlite'}", api_key=KEY),
        workspace_root=root,
        workspace_writable=True,
    )
    app.state.runtime.tool_policy = policy
    # 完整迁移/恢复先沿真实生命周期完成；原10秒只等待测试TCP服务就绪。
    async with app.router.lifespan_context(app):
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        listener.listen(128)
        listener.setblocking(False)
        url = f"http://127.0.0.1:{listener.getsockname()[1]}"
        server = uvicorn.Server(uvicorn.Config(app, log_level="error", lifespan="off"))
        task = asyncio.create_task(server.serve(sockets=[listener]))
        try:
            async with asyncio.timeout(10):
                while not server.started:
                    if task.done():
                        await task
                        raise AssertionError("测试HTTP服务在就绪前退出")
                    await asyncio.sleep(0.01)
            yield url, root, app
        finally:
            server.should_exit = True
            try:
                await asyncio.wait_for(task, 20)
            finally:
                listener.close()


def configure(client, *, name="directories", mode="agent", tool_names=None):
    client.agent_profile_put(
        AgentProfileUpdate(
            expected_revision=0,
            config=AgentProfile(
                name=name,
                description="目录公开管理验证",
                workspace_key="local",
                tool_names=NAMES if tool_names is None else tool_names,
                persistent=True,
                mode=mode,
            ),
        )
    )
    return client.agent_spawn(name, persistent=True).agent_id


async def run_cli(url, *args):
    env = {key: value for key, value in os.environ.items() if not key.startswith("KRIM_AGENT_")}
    env["KRIM_AGENT_API_KEY"] = KEY
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "krim_agent",
        "directories",
        *map(str, args),
        "--url",
        url,
        env=env,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), 20)
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()
    return process.returncode, stdout.decode(), stderr.decode()


async def test_history_honors_after_and_limit(directory_server):
    url, _root, _app = directory_server
    with KrimClient(url, KEY) as client:
        agent = await asyncio.to_thread(configure, client)
    base = f"/v1/agents/{agent}/workspace"
    async with httpx.AsyncClient(base_url=url, headers={"Authorization": f"Bearer {KEY}"}) as api:
        response = await api.post(
            base + "/directory-changes/preview",
            json={
                "spec": {"path": "empty", "operation": "create", "mode": 0o750},
                "request_key": "p",
            },
        )
        assert response.status_code == 200, response.text
        proposal = response.json()
        identifiers = []
        for action in ("apply", "keep", "undo"):
            current = (await api.get(base + "/directory-changes/" + proposal["proposal_id"])).json()
            response = await api.post(
                base + "/directory-changes",
                params={"confirmed": "true"},
                json={
                    "proposal_id": proposal["proposal_id"],
                    "action": action,
                    "expected_revision": current["revision"],
                    "request_key": action,
                },
            )
            assert response.status_code == 200 and response.json()["status"] == "done", (
                response.text
            )
            identifiers.append(response.json()["operation_id"])
        path = base + "/directory-changes/" + proposal["proposal_id"] + "/history"
        first = await api.get(path, params={"limit": 1})
        assert first.status_code == 200, first.text
        assert [item["operation_id"] for item in first.json()["history"]] == identifiers[:1]
        assert first.json()["next_after"] == identifiers[0]
        second = await api.get(path, params={"limit": 1, "after": first.json()["next_after"]})
        assert [item["operation_id"] for item in second.json()["history"]] == identifiers[1:2]
        assert second.json()["next_after"] == identifiers[1]


async def test_cli_dispatch_reads_directory_facts(directory_server):
    url, _root, _app = directory_server
    with KrimClient(url, KEY) as client:
        agent = await asyncio.to_thread(configure, client)
    code, stdout, stderr = await run_cli(url, "list", agent)
    assert code == 0, stderr
    assert json.loads(stdout)["kind"] == "proposals"


def intent(path="empty", key="preview"):
    return DirectoryPreviewRequest(
        spec=DirectoryChangeSpec(path=path, operation="create", mode=0o750), request_key=key
    )


async def counts(app):
    database = app.state.runtime.repository.database
    async with database.write_lock, database.sessions() as session:
        return (
            await session.scalar(select(func.count()).select_from(WorkspaceDirectoryProposalRow)),
            await session.scalar(select(func.count()).select_from(WorkspaceDirectoryAttemptRow)),
        )


async def test_sync_client_create_delete_undo_and_closed_history(directory_server, monkeypatch):
    url, root, app = directory_server

    def scenario():
        with KrimClient(url, KEY) as client:
            agent = configure(client)
            assert (
                client.agent_directory_query(
                    agent, kind="proposal_by_key", request_key="missing"
                ).proposal
                is None
            )
            assert (
                client.agent_directory_query(
                    agent, kind="operation_by_key", request_key="missing"
                ).operation
                is None
            )
            proposal = client.agent_directory_preview(agent, intent())
            with pytest.raises(ClientError) as approval:
                client.agent_directory_change(
                    agent,
                    DirectoryChangeAction(
                        proposal_id=proposal.proposal_id,
                        action="apply",
                        expected_revision=0,
                        request_key="apply",
                    ),
                )
            assert approval.value.code == "approval_required"
            created = client.agent_directory_change(
                agent,
                DirectoryChangeAction(
                    proposal_id=proposal.proposal_id,
                    action="apply",
                    expected_revision=0,
                    request_key="apply",
                ),
                confirmed=True,
            )
            assert created.status == "done" and root.joinpath("empty").is_dir()
            assert created.receipt is not None and created.receipt.after_fingerprint is not None
            identity = root.joinpath("empty").stat().st_ino
            deleted = client.agent_directory_preview(
                agent,
                DirectoryPreviewRequest(
                    spec=DirectoryChangeSpec(
                        path="empty",
                        operation="delete",
                        expected_fingerprint=created.receipt.after_fingerprint,
                    ),
                    request_key="delete-preview",
                ),
            )
            removed = client.agent_directory_change(
                agent,
                DirectoryChangeAction(
                    proposal_id=deleted.proposal_id,
                    action="apply",
                    expected_revision=0,
                    request_key="delete",
                ),
                confirmed=True,
            )
            assert removed.status == "done" and not root.joinpath("empty").exists()
            current = client.agent_directory_query(
                agent, kind="proposal", identifier=deleted.proposal_id
            ).proposal
            undone = client.agent_directory_change(
                agent,
                DirectoryChangeAction(
                    proposal_id=deleted.proposal_id,
                    action="undo",
                    expected_revision=current.revision,
                    request_key="undo",
                ),
                confirmed=True,
            )
            assert undone.status == "done" and root.joinpath("empty").stat().st_ino == identity
            for operation in (created, removed, undone):
                assert (
                    client.agent_directory_query(
                        agent, kind="operation", identifier=operation.operation_id
                    ).operation.operation_id
                    == operation.operation_id
                )
                assert (
                    client.agent_directory_reconcile(
                        agent, operation.operation_id, confirmed=True
                    ).status
                    == "done"
                )
            page = client.agent_directory_query(agent, limit=1)
            assert page.proposals[0].proposal_id == proposal.proposal_id
            assert page.next_after == proposal.proposal_id
            assert (
                client.agent_directory_query(agent, after=page.next_after, limit=1)
                .proposals[0]
                .proposal_id
                == deleted.proposal_id
            )
            client.agent_directory_finalize(agent, undone.operation_id, confirmed=True)
            session = client.agent_session(agent)
            client.agent_session_close(agent, session.revision)
            return agent, deleted.proposal_id, undone.operation_id

    agent, proposal_id, operation_id = await asyncio.to_thread(scenario)

    def forbidden(*_args, **_kwargs):
        raise AssertionError("目录历史查询不得执行Writer")

    monkeypatch.setattr(WorkspaceDirectoryWriter, "apply", forbidden)
    before = await counts(app)

    def history():
        with KrimClient(url, KEY) as client:
            assert client.agent_directory_query(
                agent, kind="history", identifier=proposal_id
            ).history
            assert (
                client.agent_directory_query(
                    agent, kind="operation", identifier=operation_id
                ).operation.status
                == "done"
            )
            with pytest.raises(ClientError) as closed:
                client.agent_directory_change(
                    agent,
                    DirectoryChangeAction(
                        proposal_id=proposal_id,
                        action="apply",
                        expected_revision=0,
                        request_key="closed",
                    ),
                    confirmed=True,
                )
            assert closed.value.code == "agent_session_closed"

    await asyncio.to_thread(history)
    assert await counts(app) == before


async def test_strict_rest_boundaries_scope_and_request_keys(directory_server):
    url, root, app = directory_server
    with KrimClient(url, KEY) as client:
        agent = await asyncio.to_thread(configure, client)
        sibling = await asyncio.to_thread(client.agent_spawn, "directories")
    base = f"/v1/agents/{agent}/workspace"
    async with httpx.AsyncClient(base_url=url, headers={"Authorization": f"Bearer {KEY}"}) as api:
        key = "审阅/../?confirmed=true#%"
        payload = intent(key=key).model_dump(mode="json")
        proposed = await api.post(base + "/directory-changes/preview", json=payload)
        assert proposed.status_code == 200, proposed.text
        proposal = proposed.json()
        for spec in (
            {"path": "/tmp/host", "operation": "create", "mode": 0o750},
            {"path": "../escape", "operation": "create", "mode": 0o750},
            {"path": ".", "operation": "create", "mode": 0o750},
            {"path": "empty", "operation": "create", "mode": 0o750, "root": str(root)},
        ):
            response = await api.post(
                base + "/directory-changes/preview", json={"spec": spec, "request_key": "invalid"}
            )
            assert response.status_code == 422, response.text
            assert str(root) not in response.text
        assert (
            await api.get(base + "/directory-changes/by-key", params={"request_key": key})
        ).json()["proposal_id"] == proposal["proposal_id"]
        for params in (
            {"after": ""},
            {"after": "../escape"},
            {"limit": "33"},
            {"limit": "0"},
            {"root": str(root)},
            {"limit": "NaN"},
            [("limit", "1"), ("limit", "2")],
        ):
            response = await api.get(base + "/directory-changes", params=params)
            assert response.status_code == 422, response.text
        missing = await api.get(base + "/directory-changes", params={"after": "missing"})
        assert missing.status_code == 404
        assert (
            await api.get(base + "/directory-changes/by-key", params={"request_key": "\0"})
        ).status_code == 422
        assert (
            await api.get(base + "/directory-changes", headers={"Authorization": ""})
        ).status_code == 401
        assert (
            await api.get(
                base + "/directory-changes", headers={"Origin": "https://invalid.example"}
            )
        ).status_code == 403
        assert (
            await api.post(base + "/directory-changes/preview", content=b"x" * 65537)
        ).status_code == 413
        cross = f"/v1/agents/{sibling.agent_id}/workspace/directory-changes"
        assert (await api.get(cross + "/" + proposal["proposal_id"])).status_code == 404
        assert (
            await api.get(cross + "/" + proposal["proposal_id"] + "/history")
        ).status_code == 404
        assert (await api.get(cross + "/by-key", params={"request_key": key})).json() is None
        assert (await api.get(cross, params={"after": proposal["proposal_id"]})).status_code == 404
        for suffix in ("reconcile", "finalize"):
            mismatch = await api.post(
                base + "/directory-operations/original/" + suffix,
                json={"operation_id": "other"},
                params={"confirmed": "true"},
            )
            assert mismatch.status_code == 422
        assert await counts(app) == (1, 0)
        assert not root.joinpath("empty").exists()


async def test_cli_lifecycle_exit_codes_and_size(directory_server, tmp_path):
    url, root, app = directory_server
    with KrimClient(url, KEY) as client:
        agent = await asyncio.to_thread(configure, client)
    file = tmp_path / "intent.json"
    file.write_text(intent().model_dump_json())
    code, stdout, stderr = await run_cli(url, "preview", agent, "--request", file)
    assert code == 0, stderr
    proposal = json.loads(stdout)
    argv = ["apply", agent, proposal["proposal_id"], "--revision", 0, "--request-key", "apply"]
    code, _stdout, stderr = await run_cli(url, *argv)
    assert code == 1 and "approval_required" in stderr
    code, stdout, stderr = await run_cli(url, *argv, "--confirm")
    assert code == 0, stderr
    applied = json.loads(stdout)
    assert applied["status"] == "done" and root.joinpath("empty").is_dir()
    for action, value in (
        ("get", proposal["proposal_id"]),
        ("operation", applied["operation_id"]),
        ("history", proposal["proposal_id"]),
        ("proposal-by-key", "preview"),
        ("operation-by-key", "apply"),
    ):
        code, stdout, stderr = await run_cli(url, action, agent, value)
        assert code == 0 and json.loads(stdout), stderr
    for action in ("keep", "undo"):
        code, stdout, stderr = await run_cli(url, "get", agent, proposal["proposal_id"])
        revision = json.loads(stdout)["proposal"]["revision"]
        code, stdout, stderr = await run_cli(
            url,
            action,
            agent,
            proposal["proposal_id"],
            "--revision",
            revision,
            "--request-key",
            action,
            "--confirm",
        )
        assert code == 0 and json.loads(stdout)["status"] == "done", stderr
        last = json.loads(stdout)
    assert not root.joinpath("empty").exists()
    for action in ("reconcile", "finalize"):
        code, stdout, stderr = await run_cli(url, action, agent, last["operation_id"], "--confirm")
        assert code == 0 and json.loads(stdout)["status"] == "done", stderr
    file.write_bytes(b"x" * 65537)
    code, _stdout, stderr = await run_cli(url, "preview", agent, "--request", file)
    assert code == 2 and "64KiB" in stderr
    code, _stdout, _stderr = await run_cli(
        url, "operation", agent, last["operation_id"], "--after", "a"
    )
    assert code == 2
    assert await counts(app) == (1, 3)


async def test_rejected_cli_action_has_nonzero_status(directory_server, tmp_path):
    url, root, _app = directory_server
    with KrimClient(url, KEY) as client:
        agent = await asyncio.to_thread(configure, client)
        proposal = await asyncio.to_thread(client.agent_directory_preview, agent, intent())
    root.joinpath("empty").mkdir()
    code, stdout, stderr = await run_cli(
        url,
        "apply",
        agent,
        proposal.proposal_id,
        "--revision",
        0,
        "--request-key",
        "conflict",
        "--confirm",
    )
    assert code == 1, stderr
    assert json.loads(stdout)["status"] == "rejected"
    assert root.joinpath("empty").is_dir()


async def test_query_confirmation_never_overrides_deny_and_hook(directory_server):
    url, _root, app = directory_server
    with KrimClient(url, KEY) as client:
        agent = await asyncio.to_thread(configure, client)
    handle = app.state.agents._handle(agent)
    decision = "ask"

    async def controlled(*_args):
        return decision

    handle.runtime.tool_policy = controlled

    def scenario():
        nonlocal decision
        with KrimClient(url, KEY) as client:
            with pytest.raises(ClientError) as approval:
                client.agent_directory_query(agent)
            assert approval.value.code == "approval_required"
            assert client.agent_directory_query(agent, confirmed=True).proposals == []
            decision = "deny"
            with pytest.raises(ClientError) as denial:
                client.agent_directory_query(agent, confirmed=True)
            assert denial.value.status == 403 and denial.value.code == "permission_denied"

    await asyncio.to_thread(scenario)


@pytest.mark.parametrize("mode", ["ask", "plan"])
async def test_profile_modes_and_tool_subset_cannot_expand_directory_permission(
    directory_server, mode
):
    url, root, _app = directory_server

    def scenario():
        with KrimClient(url, KEY) as client:
            agent = configure(client, mode=mode)
            if mode == "ask":
                with pytest.raises(ClientError) as rejected:
                    client.agent_directory_preview(agent, intent(), confirmed=True)
            else:
                proposal = client.agent_directory_preview(agent, intent())
                with pytest.raises(ClientError) as rejected:
                    client.agent_directory_change(
                        agent,
                        DirectoryChangeAction(
                            proposal_id=proposal.proposal_id,
                            action="apply",
                            expected_revision=0,
                            request_key="apply",
                        ),
                        confirmed=True,
                    )
            assert rejected.value.code == "permission_denied"
            read_agent = configure(client, name="query-only", tool_names=["directory_query"])
            with pytest.raises(ClientError) as missing:
                client.agent_directory_preview(read_agent, intent(), confirmed=True)
            assert missing.value.code == "tool_not_found"

    await asyncio.to_thread(scenario)
    assert not root.joinpath("empty").exists()


async def test_changed_resource_manifest_blocks_even_empty_history(directory_server):
    url, _root, app = directory_server
    with KrimClient(url, KEY) as client:
        agent = await asyncio.to_thread(configure, client)
        proposal = await asyncio.to_thread(client.agent_directory_preview, agent, intent())
    database = app.state.runtime.repository.database
    async with database.write_lock, database.sessions.begin() as session:
        row = await session.get(AgentSessionRow, agent)
        manifest = dict(row.permissions)
        manifest["workspace"] = {"key": "local", "version": "different"}
        row.permissions = manifest

    def scenario():
        with KrimClient(url, KEY) as client:
            with pytest.raises(ClientError) as changed:
                client.agent_directory_query(agent, kind="history", identifier=proposal.proposal_id)
            assert changed.value.status == 403 and changed.value.code == "workspace_binding_changed"

    await asyncio.to_thread(scenario)


async def test_real_child_cannot_expand_tools_or_act_after_parent_close(directory_server):
    url, root, app = directory_server
    with KrimClient(url, KEY) as client:
        parent = await asyncio.to_thread(configure, client)
        limited_parent = await asyncio.to_thread(
            configure, client, name="limited", tool_names=["directory_query"]
        )
    service = app.state.agents
    definition = await service.definition("directories")
    with pytest.raises(AgentError) as expansion:
        await service.manager.spawn(replace(definition, parent_agent_id=limited_parent))
    assert expansion.value.code == "agent_workspace_forbidden"
    child = await service.manager.spawn(replace(definition, parent_agent_id=parent))

    def scenario():
        with KrimClient(url, KEY) as client:
            proposal = client.agent_directory_preview(child.agent_id, intent())
            session = client.agent_session(parent)
            client.agent_session_close(parent, session.revision)
            with pytest.raises(ClientError) as closed:
                client.agent_directory_change(
                    child.agent_id,
                    DirectoryChangeAction(
                        proposal_id=proposal.proposal_id,
                        action="apply",
                        expected_revision=0,
                        request_key="parent-closed",
                    ),
                    confirmed=True,
                )
            assert closed.value.code == "agent_parent_session_closed"
            assert (
                client.agent_directory_query(
                    child.agent_id, kind="proposal", identifier=proposal.proposal_id
                ).proposal.status
                == "proposed"
            )

    await asyncio.to_thread(scenario)
    assert not root.joinpath("empty").exists()


async def test_expired_operation_by_key_reconcile_and_finalize_do_not_replay(
    directory_server, monkeypatch
):
    url, root, app = directory_server
    with KrimClient(url, KEY) as client:
        agent = await asyncio.to_thread(configure, client)
        proposal = await asyncio.to_thread(client.agent_directory_preview, agent, intent())
    handle = app.state.agents._handle(agent)
    binding = handle.definition.workspace
    store = app.state.agents.manager.directory_changes.store
    scope = "session:" + agent
    original = await store.get(scope, proposal.proposal_id)
    attempt, created = await store.reserve(
        scope,
        DirectoryChangeAction(
            proposal_id=proposal.proposal_id,
            action="apply",
            expected_revision=0,
            request_key="unknown",
        ),
        binding.fingerprint,
        original.prepared,
    )
    assert created
    loop = asyncio.get_running_loop()

    def record(reference):
        asyncio.run_coroutine_threadsafe(
            store.record_intent(scope, attempt.view.operation_id, reference), loop
        ).result(5)

    effect = await asyncio.to_thread(
        WorkspaceDirectoryWriter(binding.workspace).apply, original.prepared, record_intent=record
    )
    assert effect.receipt.status == "done" and root.joinpath("empty").is_dir()
    database = store.database
    async with database.write_lock, database.sessions.begin() as session:
        await session.execute(
            update(WorkspaceDirectoryAttemptRow)
            .where(WorkspaceDirectoryAttemptRow.operation_id == attempt.view.operation_id)
            .values(lease_expires_at=0)
        )

    def forbidden(*_args, **_kwargs):
        raise AssertionError("原未知操作只能核实，不能重做Writer.apply")

    monkeypatch.setattr(WorkspaceDirectoryWriter, "apply", forbidden)

    def scenario():
        with KrimClient(url, KEY) as client:
            found = client.agent_directory_query(
                agent, kind="operation_by_key", request_key="unknown"
            )
            assert found.operation.status == "uncertain" and found.operation.recovery_required
            with pytest.raises(ClientError) as unresolved:
                client.agent_directory_finalize(agent, attempt.view.operation_id, confirmed=True)
            assert unresolved.value.code == "workspace_directory_state"
            reconciled = client.agent_directory_reconcile(
                agent, attempt.view.operation_id, confirmed=True
            )
            assert reconciled.status == "done" and not reconciled.recovery_required
            finalized = client.agent_directory_finalize(
                agent, attempt.view.operation_id, confirmed=True
            )
            assert finalized.status == "done" and not finalized.cleanup_pending

    await asyncio.to_thread(scenario)
    assert await counts(app) == (1, 1)
    assert root.joinpath("empty").is_dir()


def test_client_nullable_and_strict_responses_validate_before_network():
    calls = []

    def respond(request):
        calls.append(request)
        return httpx.Response(200, text="null")

    with KrimClient(transport=httpx.MockTransport(respond)) as client:
        for kind in ("proposal_by_key", "operation_by_key"):
            value = client.agent_directory_query("agent", kind=kind, request_key="missing")
            assert value.proposal is None and value.operation is None
        for kwargs in (
            {"kind": "wrong"},
            {"limit": 33},
            {"kind": "history"},
            {"kind": "proposals", "identifier": "irrelevant"},
            {"kind": "proposal_by_key", "request_key": "\0"},
        ):
            with pytest.raises(ValidationError):
                client.agent_directory_query("agent", **kwargs)
        assert len(calls) == 2
    for body in (
        "{}",
        '{"root":"/tmp"}',
        '{"kind":"history"}',
        '{"kind":"proposals","next_after":"unknown"}',
    ):
        response_body = body
        with KrimClient(
            transport=httpx.MockTransport(
                lambda _r, response_body=response_body: httpx.Response(200, text=response_body)
            )
        ) as client:
            with pytest.raises((ValidationError, ClientError)):
                client.agent_directory_query("agent")


async def test_two_clients_and_processes_share_original_request_keys(directory_server, tmp_path):
    url, root, app = directory_server
    with KrimClient(url, KEY) as client:
        agent = await asyncio.to_thread(configure, client)
    request = intent(key="one-preview")

    def preview():
        with KrimClient(url, KEY) as client:
            return client.agent_directory_preview(agent, request)

    proposals = await asyncio.gather(asyncio.to_thread(preview), asyncio.to_thread(preview))
    assert len({item.proposal_id for item in proposals}) == 1
    action = DirectoryChangeAction(
        proposal_id=proposals[0].proposal_id,
        action="apply",
        expected_revision=0,
        request_key="one-action",
    )
    file = tmp_path / "action.json"
    file.write_text(action.model_dump_json())
    fixture = Path(__file__).with_name("fixtures") / "directory_public_client_worker.py"
    env = {key: value for key, value in os.environ.items() if not key.startswith("KRIM_AGENT_")}
    env["KRIM_AGENT_API_KEY"] = KEY

    async def worker():
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            str(fixture),
            url,
            agent,
            str(file),
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(process.communicate(), 20)
        finally:
            if process.returncode is None:
                process.kill()
                await process.wait()
        assert process.returncode == 0, stderr.decode()
        return json.loads(stdout)

    operations = await asyncio.gather(worker(), worker())
    assert len({item["operation_id"] for item in operations}) == 1
    assert root.joinpath("empty").is_dir()
    identity = root.joinpath("empty").stat().st_ino

    def cold():
        with KrimClient(url, KEY) as client:
            result = client.agent_directory_query(
                agent, kind="operation_by_key", request_key="one-action"
            )
            assert result.operation.status == "done"
            assert (
                client.agent_directory_change(agent, action, confirmed=True).operation_id
                == result.operation.operation_id
            )

    await asyncio.to_thread(cold)
    assert root.joinpath("empty").stat().st_ino == identity
    assert await counts(app) == (1, 1)


async def test_lost_http_response_recovers_by_original_key_without_post(directory_server):
    url, root, app = directory_server
    with KrimClient(url, KEY) as client:
        agent = await asyncio.to_thread(configure, client)
        proposal = await asyncio.to_thread(client.agent_directory_preview, agent, intent())

    class DropResponse(httpx.BaseTransport):
        def __init__(self):
            self.inner = httpx.HTTPTransport()
            self.posts = 0

        def handle_request(self, request):
            response = self.inner.handle_request(request)
            if request.method == "POST":
                self.posts += 1
                response.read()
                response.close()
                raise httpx.ReadError("模拟已完成操作的响应丢失", request=request)
            return response

        def close(self):
            self.inner.close()

    def scenario():
        transport = DropResponse()
        with KrimClient(url, KEY, transport=transport) as client:
            with pytest.raises(httpx.ReadError):
                client.agent_directory_change(
                    agent,
                    DirectoryChangeAction(
                        proposal_id=proposal.proposal_id,
                        action="apply",
                        expected_revision=0,
                        request_key="lost",
                    ),
                    confirmed=True,
                )
            found = client.agent_directory_query(agent, kind="operation_by_key", request_key="lost")
            assert found.operation.status == "done"
            assert (
                client.agent_directory_query(
                    agent, kind="proposal", identifier=proposal.proposal_id
                ).proposal.status
                == "applied"
            )
            assert transport.posts == 1

    await asyncio.to_thread(scenario)
    assert root.joinpath("empty").is_dir()
    assert await counts(app) == (1, 1)


async def test_live_inspection_drives_delete_and_preserves_original_identity(directory_server):
    url, root, _app = directory_server
    target = root / "existing"
    target.mkdir(mode=0o750)
    identity = target.stat().st_ino
    with KrimClient(url, KEY) as client:
        agent = await asyncio.to_thread(configure, client)
        state = await asyncio.to_thread(client.agent_directory_inspect, agent, "existing")
        assert state.found and state.empty and state.fingerprint and state.mode == 0o750
        missing = await asyncio.to_thread(client.agent_directory_inspect, agent, "missing")
        assert not missing.found and missing.fingerprint is None
        proposal = await asyncio.to_thread(
            client.agent_directory_preview,
            agent,
            DirectoryPreviewRequest(
                spec=DirectoryChangeSpec(
                    path="existing", operation="delete", expected_fingerprint=state.fingerprint
                ),
                request_key="delete-existing-preview",
            ),
        )
        deleted = await asyncio.to_thread(
            client.agent_directory_change,
            agent,
            DirectoryChangeAction(
                proposal_id=proposal.proposal_id,
                action="apply",
                expected_revision=0,
                request_key="delete-existing",
            ),
            confirmed=True,
        )
        assert deleted.status == "done" and not target.exists()
        current = await asyncio.to_thread(
            client.agent_directory_query, agent, kind="proposal", identifier=proposal.proposal_id
        )
        restored = await asyncio.to_thread(
            client.agent_directory_change,
            agent,
            DirectoryChangeAction(
                proposal_id=proposal.proposal_id,
                action="undo",
                expected_revision=current.proposal.revision,
                request_key="undo-existing",
            ),
            confirmed=True,
        )
        assert restored.status == "done" and target.stat().st_ino == identity


async def test_live_inspection_rejects_nonempty_and_closed_or_unsafe_access(directory_server):
    url, root, _app = directory_server
    target = root / "nonempty"
    target.mkdir()
    (target / "child").write_text("x")
    with KrimClient(url, KEY) as client:
        agent = await asyncio.to_thread(configure, client)
        state = await asyncio.to_thread(client.agent_directory_inspect, agent, "nonempty")
        assert state.found and not state.empty
        with pytest.raises(ClientError) as rejected:
            await asyncio.to_thread(
                client.agent_directory_preview,
                agent,
                DirectoryPreviewRequest(
                    spec=DirectoryChangeSpec(
                        path="nonempty", operation="delete", expected_fingerprint=state.fingerprint
                    ),
                    request_key="delete-nonempty",
                ),
            )
        assert rejected.value.code == "workspace_directory_nonempty"
        for path in ("/tmp/escape", "../escape", "."):
            with pytest.raises((ClientError, ValidationError)):
                await asyncio.to_thread(client.agent_directory_inspect, agent, path)
        session = await asyncio.to_thread(client.agent_session, agent)
        await asyncio.to_thread(client.agent_session_close, agent, session.revision)
        with pytest.raises(ClientError) as closed:
            await asyncio.to_thread(client.agent_directory_inspect, agent, "nonempty")
        assert closed.value.code == "agent_session_closed"


async def test_inspection_uses_readonly_workspace_and_original_sdk(runtime, tmp_path):
    service = AgentService(runtime, runtime.settings)
    readonly = tmp_path / "readonly"
    readonly.mkdir()
    target = readonly / "empty"
    target.mkdir()
    service.register_workspace("readonly", readonly, version="1", writable=False)
    await service.profile_put(
        AgentProfileUpdate(
            expected_revision=0,
            config=AgentProfile(
                name="directory-reader",
                description="只读目录检查",
                workspace_key="readonly",
                tool_names=["directory_inspect"],
            ),
        )
    )
    view = await service.spawn("directory-reader")
    try:
        inspected = await AgentSession(service, view.agent_id).inspect_directory("empty")
        assert inspected.found and inspected.empty
    finally:
        await service.close()


async def test_live_inspection_rest_rejects_links_files_sensitive_and_extra_parameters(
    directory_server,
):
    url, root, app = directory_server
    root.joinpath("ordinary.txt").write_text("x")
    root.joinpath("target").mkdir()
    root.joinpath("link").symlink_to(root / "target", target_is_directory=True)
    with KrimClient(url, KEY) as client:
        agent = await asyncio.to_thread(configure, client)
    path = f"/v1/agents/{agent}/workspace/directory-state"
    async with httpx.AsyncClient(base_url=url, headers={"Authorization": f"Bearer {KEY}"}) as api:
        for params in (
            {"path": "/tmp"},
            {"path": "."},
            {"path": "../escape"},
            {"path": "target", "root": str(root)},
            [("path", "target"), ("path", "link")],
            {"path": "ordinary.txt"},
            {"path": "link"},
            {"path": ".git"},
        ):
            result = await api.get(path, params=params)
            assert result.status_code in {403, 422}, result.text
            assert str(root) not in result.text
        result = await api.get(path, params={"path": "target"})
        assert result.status_code == 200 and result.json()["empty"]
        assert set(result.json()) == {"path", "found", "fingerprint", "mode", "empty"}
    assert await counts(app) == (0, 0)


async def test_inspection_cli_and_final_hook_keep_original_fingerprint(directory_server):
    url, root, app = directory_server
    root.joinpath("existing").mkdir()
    with KrimClient(url, KEY) as client:
        agent = await asyncio.to_thread(configure, client)
    code, stdout, stderr = await run_cli(url, "inspect", agent, "existing")
    assert code == 0, stderr
    original = json.loads(stdout)
    assert original["found"] and original["fingerprint"]
    code, stdout, stderr = await run_cli(url, "inspect", agent, "missing")
    assert code == 0 and not json.loads(stdout)["found"], stderr

    async def post(definition, value, _context):
        if definition.name == "directory_inspect":
            value["fingerprint"] = "0" * 64
        return value

    handle = app.state.agents._handle(agent)
    handle.runtime.tools.add_global_post(post)
    with KrimClient(url, KEY) as client:
        with pytest.raises(ClientError) as forged:
            await asyncio.to_thread(client.agent_directory_inspect, agent, "existing")
        assert forged.value.code == "workspace_directory_receipt_unverified"

    async def deny(*_args):
        return "deny"

    handle.runtime.tool_policy = deny
    code, _stdout, stderr = await run_cli(url, "inspect", agent, "existing", "--confirm")
    assert code == 1 and "permission_denied" in stderr
    assert await counts(app) == (0, 0)
