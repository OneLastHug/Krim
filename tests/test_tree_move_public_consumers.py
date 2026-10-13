"""SDK/REST与真实TCP Client/CLI消费同一0043原事实、冷键和控制。"""

import asyncio
import json
import os
import signal
import socket
import sys
from contextlib import asynccontextmanager
from threading import Event

import httpx
import pytest
import uvicorn
from test_tree_move_tools import Provider, preview, setup
from test_workspace_changes import wait_event
from test_workspace_tree_move_ledger import action, counts
from test_workspace_tree_snapshot import seed

from krim_agent.api import create_app
from krim_agent.client import ClientError, KrimClient
from krim_agent.sdk import (
    AgentSession,
    TreeMoveAction,
    TreeMoveOperationRequest,
    TreeMovePreviewRequest,
    TreeMoveQueryRequest,
)
from krim_agent.workspace_tree_writer import WorkspaceTreeWriter

pytestmark = pytest.mark.skipif(
    not sys.platform.startswith("linux"), reason="真实Linux树锁/namespace与服务进程演练"
)


async def test_rest_unknown_original_operation_reconcile_and_cold_read_zero_writer(
    runtime, tmp_path, monkeypatch
):
    service, agent, _ = await setup(runtime, tmp_path)
    try:
        proposal, _ = await preview(service, agent)
        mover = service.manager.tree_moves
        original_settle = mover.store.settle

        async def unavailable(*_args, **_kwargs):
            raise OSError("合成回执中断")

        monkeypatch.setattr(mover.store, "settle", unavailable)
        unknown = await agent.change_tree_move(action(proposal))
        assert unknown.status == "uncertain" and unknown.receipt is None
        monkeypatch.setattr(mover.store, "settle", original_settle)

        def forbidden(*_args, **_kwargs):
            pytest.fail("REST原unknown重发Writer")

        monkeypatch.setattr(WorkspaceTreeWriter, "apply", forbidden)
        monkeypatch.setattr(WorkspaceTreeWriter, "prepare", forbidden)
        async with api_for(runtime, service, monkeypatch) as (_app, api):
            base = f"/v1/agents/{agent.agent_id}/workspace"
            response = await api.post(
                base + f"/tree-move-operations/{unknown.operation_id}/reconcile"
            )
            assert response.status_code == 200, response.text
            resolved = response.json()
            assert resolved["status"] == "done" and resolved["operation_id"] == unknown.operation_id
            assert (
                await api.get(
                    base + "/tree-moves/query",
                    params={"kind": "operation_by_key", "request_key": "original-apply"},
                )
            ).json()["operation"] == resolved
            assert (
                await api.post(
                    base + "/tree-moves/change", json=action(proposal).model_dump(mode="json")
                )
            ).json() == resolved
        assert await counts(mover.store.database) == [1, 1, 0, 0]
    finally:
        await service.close()


@asynccontextmanager
async def api_for(runtime, service, monkeypatch):
    monkeypatch.setattr("krim_agent.api.server.AgentService", lambda *_args, **_kwargs: service)
    app = create_app(runtime=runtime)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
    ) as client:
        yield app, client


@asynccontextmanager
async def tcp_for(app):
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(128)
    listener.setblocking(False)
    port = listener.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(app, lifespan="off", log_level="error"))
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


async def cli(url, agent_id, command, value=None, *options):
    arguments = [sys.executable, "-I", "-m", "krim_agent", "tree-moves", command, agent_id]
    if value is not None:
        arguments.append(value)
    arguments.extend(["--url", url, *options])
    process = await asyncio.create_subprocess_exec(
        *arguments, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    try:
        output, errors = await asyncio.wait_for(process.communicate(), 15)
        assert process.returncode == 0, errors.decode()
        return json.loads(output)
    finally:
        if process.returncode is None:
            process.kill()
            await process.communicate()


async def test_sdk_real_binary_keep_undo_and_original_keys(runtime, tmp_path, monkeypatch):
    service, agent, _ = await setup(runtime, tmp_path)
    inode = (tmp_path / "tree/sub/data.bin").stat().st_ino
    try:
        inspection = await agent.inspect_tree("tree")
        request = TreeMovePreviewRequest(
            source_path="tree",
            target_path="dest/tree",
            expected_fingerprint=inspection.fingerprint,
            request_key="sdk-preview",
        )
        proposal = await agent.preview_tree_move(request)
        apply_request = action(proposal, key="sdk-apply")
        applied = await agent.change_tree_move(apply_request)
        assert (
            applied.status == "done"
            and (tmp_path / "dest/tree/sub/data.bin").stat().st_ino == inode
        )
        current = (
            await agent.query_tree_move(
                TreeMoveQueryRequest(kind="proposal", identifier=proposal.proposal_id)
            )
        ).proposal
        kept = await agent.change_tree_move(action(current, "keep", "sdk-keep"))
        assert kept.status == "done" and not kept.receipt.changed
        current = (
            await agent.query_tree_move(
                TreeMoveQueryRequest(kind="by_key", request_key=request.request_key)
            )
        ).proposal
        undo_request = action(current, "undo", "sdk-undo")
        undone = await agent.change_tree_move(undo_request)
        assert undone.status == "done" and (tmp_path / "tree/sub/data.bin").stat().st_ino == inode

        def forbidden(*_args, **_kwargs):
            pytest.fail("SDK冷读重放原Writer")

        for method in ("prepare", "apply", "prepare_undo", "reconcile"):
            monkeypatch.setattr(WorkspaceTreeWriter, method, forbidden)
        assert await agent.change_tree_move(apply_request) == applied
        assert await agent.change_tree_move(undo_request) == undone
        assert (await agent.preview_tree_move(request)).status == "undone"
        assert await agent.reconcile_tree_move(undone.operation_id) == undone
        history = await agent.query_tree_move(
            TreeMoveQueryRequest(kind="history", identifier=proposal.proposal_id)
        )
        assert {entry.operation_id for entry in history.history.items} == {
            applied.operation_id,
            kept.operation_id,
            undone.operation_id,
        }
        assert await counts(service.manager.tree_moves.store.database) == [1, 3, 0, 0]
    finally:
        await service.close()


async def test_rest_same_actual_model_facts_original_keys_and_zero_replay(
    runtime, tmp_path, monkeypatch
):
    provider = Provider()
    service, agent, _ = await setup(runtime, tmp_path, provider=provider)
    try:
        result = await agent.execute("原树消费", idempotency_key="public-model")
        assert result.status == "completed" and provider.calls == 9
        before = await counts(service.manager.tree_moves.store.database)

        def forbidden(*_args, **_kwargs):
            pytest.fail("REST冷读重放模型/Writer")

        for method in ("prepare", "apply", "prepare_undo", "reconcile"):
            monkeypatch.setattr(WorkspaceTreeWriter, method, forbidden)
        base = f"/v1/agents/{agent.agent_id}/workspace"
        async with api_for(runtime, service, monkeypatch) as (_app, api):
            for selection in (
                TreeMoveQueryRequest(kind="proposal", identifier=provider.proposal.proposal_id),
                TreeMoveQueryRequest(kind="by_key", request_key="model-preview"),
                TreeMoveQueryRequest(
                    kind="operation", identifier=provider.actions[0]["operation_id"]
                ),
                TreeMoveQueryRequest(kind="operation_by_key", request_key="model-apply"),
                TreeMoveQueryRequest(
                    kind="history", identifier=provider.proposal.proposal_id, limit=1
                ),
                TreeMoveQueryRequest(kind="proposals", limit=1),
            ):
                expected = await agent.query_tree_move(selection)
                params = {
                    key: value for key, value in selection.model_dump().items() if value is not None
                }
                response = await api.get(base + "/tree-moves/query", params=params)
                assert response.status_code == 200, response.text
                assert response.json() == expected.model_dump(mode="json")
            repeat = TreeMoveAction(
                proposal_id=provider.proposal.proposal_id,
                action="apply",
                expected_revision=0,
                request_key="model-apply",
            )
            response = await api.post(
                base + "/tree-moves/change", json=repeat.model_dump(mode="json")
            )
            assert response.json() == provider.actions[0]
            op_id = provider.actions[0]["operation_id"]
            response = await api.post(
                base + f"/tree-move-operations/{op_id}/reconcile",
                json=TreeMoveOperationRequest(operation_id=op_id).model_dump(mode="json"),
            )
            assert response.json() == provider.actions[0]
            assert (
                await api.post(base + f"/tree-moves/{provider.proposal.proposal_id}/cancel")
            ).json() == provider.proposal.model_dump(mode="json")
        assert await counts(service.manager.tree_moves.store.database) == before == [1, 3, 0, 4]
        assert provider.calls == 9
    finally:
        await service.close()


@pytest.mark.parametrize(
    "query",
    [
        "kind=proposals&scope_id=other",
        "kind=proposals&kind=history",
        "kind=by_key",
        "kind=history&identifier=x&limit=33",
        "kind=operation&identifier=x&request_key=y",
        "kind=proposal&identifier=x&after=y",
        "kind=proposals&limit=true",
    ],
)
async def test_rest_strict_unknown_duplicate_private_and_selector_inputs(
    runtime, tmp_path, monkeypatch, query
):
    service, agent, _ = await setup(runtime, tmp_path)
    try:
        base = f"/v1/agents/{agent.agent_id}/workspace"
        async with api_for(runtime, service, monkeypatch) as (_app, api):
            response = await api.get(base + "/tree-moves/query?" + query)
            assert response.status_code == 422, response.text
            assert (await api.get(base + "/tree-state?path=tree&path=dest")).status_code == 422
            assert (await api.get(base + "/tree-state?path=../tree")).status_code == 422
            assert (
                await api.post(
                    base + "/tree-moves/preview",
                    json={
                        "source_path": "tree",
                        "target_path": "dest/tree",
                        "expected_fingerprint": "a" * 64,
                        "request_key": "private",
                        "owner_id": "other",
                    },
                )
            ).status_code == 422
            assert (
                await api.post(base + "/tree-moves/x/cancel", json={"scope_id": "other"})
            ).status_code == 422
            assert (await api.post(base + "/tree-moves/x/cancel?confirmed=true")).status_code == 422
            assert (
                await api.post(
                    base + "/tree-move-operations/x/reconcile", json={"operation_id": "other"}
                )
            ).status_code == 422
        assert await counts(service.manager.tree_moves.store.database) == [0, 0, 0, 0]
    finally:
        await service.close()


async def test_rest_cross_session_and_confirmed_cannot_override_deny(
    runtime, tmp_path, monkeypatch
):
    service, agent, _ = await setup(runtime, tmp_path)
    try:
        proposal, _ = await preview(service, agent)
        other = await service.spawn("move")
        async with api_for(runtime, service, monkeypatch) as (_app, api):
            base = f"/v1/agents/{other.agent_id}/workspace"
            response = await api.get(
                base + "/tree-moves/query",
                params={"kind": "proposal", "identifier": proposal.proposal_id},
            )
            assert response.status_code == 404
            assert (
                await api.post(base + f"/tree-moves/{proposal.proposal_id}/cancel")
            ).status_code == 404

            async def deny(_definition, _context):
                return "deny"

            service._handle(agent.agent_id).runtime.tool_policy = deny
            base = f"/v1/agents/{agent.agent_id}/workspace"
            response = await api.post(
                base + "/tree-moves/change?confirmed=true",
                json=action(proposal).model_dump(mode="json"),
            )
            assert (
                response.status_code == 403
                and response.json()["error"]["code"] == "permission_denied"
            )
        assert await counts(service.manager.tree_moves.store.database) == [1, 0, 0, 0]
    finally:
        await service.close()


async def test_real_tcp_client_cli_publish_original_binary_cold_keys_history_and_cancel(
    runtime, tmp_path, monkeypatch
):
    service, agent, _ = await setup(runtime, tmp_path)
    try:
        async with api_for(runtime, service, monkeypatch) as (app, _api), tcp_for(app) as url:
            inspected = await cli(url, agent.agent_id, "inspect", "tree")
            request = TreeMovePreviewRequest(
                source_path="tree",
                target_path="dest/tree",
                expected_fingerprint=inspected["fingerprint"],
                request_key="tcp-preview",
            )
            intent = tmp_path / "intent.json"
            intent.write_text(request.model_dump_json())
            proposal = await cli(url, agent.agent_id, "preview", None, "--request", str(intent))
            apply_args = ("--revision", str(proposal["revision"]), "--request-key", "tcp-apply")
            applied = await cli(url, agent.agent_id, "apply", proposal["proposal_id"], *apply_args)
            assert (
                applied["status"] == "done"
                and (tmp_path / "dest/tree/sub/data.bin").read_bytes() == b"\0\xff\x80"
            )
            with KrimClient(url) as client:
                query = TreeMoveQueryRequest(kind="operation_by_key", request_key="tcp-apply")
                saved = await asyncio.to_thread(client.agent_tree_move_query, agent.agent_id, query)
                assert saved.operation.model_dump(mode="json") == applied
                assert (
                    await asyncio.to_thread(
                        client.agent_tree_move_reconcile, agent.agent_id, applied["operation_id"]
                    )
                ).model_dump(mode="json") == applied
            assert (
                await cli(url, agent.agent_id, "apply", proposal["proposal_id"], *apply_args)
                == applied
            )
            assert (await cli(url, agent.agent_id, "by-key", "tcp-preview"))["proposal"][
                "status"
            ] == "applied"
            history = await cli(
                url, agent.agent_id, "history", proposal["proposal_id"], "--limit", "1"
            )
            assert history["history"]["items"] == [applied]
            current = (await cli(url, agent.agent_id, "get", proposal["proposal_id"]))["proposal"]
            undone = await cli(
                url,
                agent.agent_id,
                "undo",
                proposal["proposal_id"],
                "--revision",
                str(current["revision"]),
                "--request-key",
                "tcp-undo",
            )
            assert (
                undone["status"] == "done"
                and (tmp_path / "tree/sub/data.bin").read_bytes() == b"\0\xff\x80"
            )
            assert (await cli(url, agent.agent_id, "cancel", proposal["proposal_id"]))[
                "status"
            ] == "undone"
            assert (await counts(service.manager.tree_moves.store.database)) == [1, 2, 0, 0]
    finally:
        await service.close()


async def test_rest_cancel_waits_original_admitted_sdk_writer(runtime, tmp_path, monkeypatch):
    service, agent, _ = await setup(runtime, tmp_path)
    entered, release = Event(), Event()
    original = WorkspaceTreeWriter.apply
    worker = cancelling = None
    try:
        proposal, _ = await preview(service, agent)

        def pause(writer, material, **kwargs):
            entered.set()
            assert release.wait(10)
            return original(writer, material, **kwargs)

        monkeypatch.setattr(WorkspaceTreeWriter, "apply", pause)
        worker = asyncio.create_task(agent.change_tree_move(action(proposal)))
        await wait_event(entered)
        mover, closed = service.manager.tree_moves, asyncio.Event()
        original_cancel = mover.store.cancel

        async def notified(scope, identifier):
            result = await original_cancel(scope, identifier)
            closed.set()
            return result

        monkeypatch.setattr(mover.store, "cancel", notified)
        async with api_for(runtime, service, monkeypatch) as (_app, api):
            cancelling = asyncio.create_task(
                api.post(
                    f"/v1/agents/{agent.agent_id}/workspace/tree-moves/{proposal.proposal_id}/cancel"
                )
            )
            await asyncio.wait_for(closed.wait(), 5)
            assert not cancelling.done()
            release.set()
            result = await worker
            response = await cancelling
            assert response.status_code == 200 and response.json()["status"] == "cancelled"
            assert result.status == "rejected" and result.cancelled
        assert not (tmp_path / "dest/tree").exists()
    finally:
        release.set()
        await asyncio.gather(
            *(task for task in (worker, cancelling) if task is not None), return_exceptions=True
        )
        await service.close()


@pytest.mark.parametrize("fault", ["scope", "kind", "extra", "selector", "cursor"])
async def test_client_rejects_foreign_or_forged_wire_facts(runtime, tmp_path, fault):
    service, agent, _ = await setup(runtime, tmp_path)
    try:
        proposal, _ = await preview(service, agent)
        selector = TreeMoveQueryRequest(kind="proposal", identifier=proposal.proposal_id)
        result = (await agent.query_tree_move(selector)).model_dump(mode="json")
        if fault == "scope":
            result["proposal"]["scope_id"] = "session:other"
        elif fault == "kind":
            result["kind"] = "by_key"
        elif fault == "extra":
            result["proposals"] = {"items": [], "next_after": None}
        elif fault == "selector":
            result["proposal"]["proposal_id"] = "other"
        else:
            selector = TreeMoveQueryRequest(kind="proposals", limit=1)
            result = {
                "kind": "proposals",
                "proposals": {"items": [proposal.model_dump(mode="json")], "next_after": "other"},
            }
        transport = httpx.MockTransport(lambda _request: httpx.Response(200, json=result))
        with (
            KrimClient("http://127.0.0.1", transport=transport) as client,
            pytest.raises(ClientError) as error,
        ):
            client.agent_tree_move_query(agent.agent_id, selector)
        assert error.value.code == "workspace_tree_move_response"
    finally:
        await service.close()


@pytest.mark.parametrize("enabled", [False, True])
async def test_actual_app_opt_in_default_freeze_and_authorized_workspace(
    runtime, tmp_path, enabled
):
    seed(tmp_path)
    (tmp_path / "dest").mkdir()

    async def policy(definition, _context):
        return "allow" if definition.risk == "read" else "ask"

    runtime.tool_policy = policy
    app = create_app(
        runtime=runtime,
        workspace_root=tmp_path,
        workspace_writable=True,
        tree_moves_enabled=enabled,
    )
    async with app.router.lifespan_context(app):
        service = app.state.agents
        agent = AgentSession(service, (await service.spawn("default")).agent_id)
        original_names = {
            item.name for item in service._handle(agent.agent_id).runtime.tools.definitions
        }
        assert ("tree_move_change" in original_names) == enabled
        if not enabled:
            service.register_tree_moves()
            assert {
                item.name for item in service._handle(agent.agent_id).runtime.tools.definitions
            } == original_names
            return
        inspection = await agent.inspect_tree("tree")
        proposal = await agent.preview_tree_move(
            TreeMovePreviewRequest(
                source_path="tree",
                target_path="dest/tree",
                expected_fingerprint=inspection.fingerprint,
                request_key="factory",
            )
        )
        done = await agent.change_tree_move(action(proposal, key="factory-apply"), confirmed=True)
        assert (
            done.status == "done"
            and (tmp_path / "dest/tree/sub/data.bin").read_bytes() == b"\0\xff\x80"
        )


async def test_real_serve_cli_opt_in_isolated_database_and_client_publication(tmp_path):
    seed(tmp_path)
    (tmp_path / "dest").mkdir()
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    port = listener.getsockname()[1]
    listener.close()
    url = f"http://127.0.0.1:{port}"
    environment = dict(os.environ)
    environment.update(
        {
            "KRIM_AGENT_DATABASE_URL": f"sqlite+aiosqlite:///{tmp_path / 'state.sqlite'}",
            "KRIM_AGENT_PROVIDER": "fake",
            "KRIM_AGENT_API_KEY": "",
            "KRIM_AGENT_INTERACTIVE_TOOLS": "true",
        }
    )
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-I",
        "-m",
        "krim_agent",
        "serve",
        "--workspace",
        str(tmp_path),
        "--workspace-write",
        "--tree-moves",
        "--port",
        str(port),
        env=environment,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        async with httpx.AsyncClient(timeout=1) as api, asyncio.timeout(25):
            while True:
                if process.returncode is not None:
                    _, errors = await process.communicate()
                    raise AssertionError(errors.decode())
                try:
                    response = await api.get(url + "/healthz")
                    if response.status_code == 200:
                        break
                except httpx.TransportError:
                    pass
                await asyncio.sleep(0.05)
        with KrimClient(url) as client:
            agent = await asyncio.to_thread(client.agent_spawn, "default")
            inspected = await asyncio.to_thread(
                client.agent_tree_move_inspect, agent.agent_id, "tree"
            )
            request = TreeMovePreviewRequest(
                source_path="tree",
                target_path="dest/tree",
                expected_fingerprint=inspected.fingerprint,
                request_key="serve-preview",
            )
            proposal = await asyncio.to_thread(
                client.agent_tree_move_preview, agent.agent_id, request
            )
            applied = await asyncio.to_thread(
                client.agent_tree_move_change,
                agent.agent_id,
                action(proposal, key="serve-apply"),
                confirmed=True,
            )
            assert (
                applied.status == "done"
                and (tmp_path / "dest/tree/sub/data.bin").read_bytes() == b"\0\xff\x80"
            )
            query = TreeMoveQueryRequest(kind="operation_by_key", request_key="serve-apply")
            assert (
                await asyncio.to_thread(client.agent_tree_move_query, agent.agent_id, query)
            ).operation == applied
            assert (await cli(url, agent.agent_id, "operation-by-key", "serve-apply"))[
                "operation"
            ] == applied.model_dump(mode="json")
    finally:
        if process.returncode is None:
            process.terminate()
            try:
                _, errors = await asyncio.wait_for(process.communicate(), 15)
                assert process.returncode in {0, -signal.SIGTERM}, errors.decode()
                assert "Application shutdown complete." in errors.decode(), errors.decode()
            except TimeoutError:
                process.kill()
                await process.communicate()
                raise
