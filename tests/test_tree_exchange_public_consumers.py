"""SDK/REST/真实TCP Client与CLI消费同一完整树原账本/来源，0冷重放。"""

import asyncio
import json
import os
import socket
import sys
from threading import Event

import httpx
import pytest
from test_tree_exchange_tools import NAMES, Provider, counts, preview, setup
from test_tree_move_public_consumers import api_for, tcp_for
from test_workspace_changes import wait_event
from test_workspace_tree_exchange import seed
from test_workspace_tree_exchange_ledger import action

from krim_agent.api import create_app
from krim_agent.client import ClientError, KrimClient
from krim_agent.protocol.domain import ServiceError
from krim_agent.sdk import (
    AgentSession,
    TreeExchangePreviewRequest,
    TreeExchangeQueryRequest,
    TreeExchangeSealRequest,
)
from krim_agent.tools.tree_exchanges import tree_exchange_tools
from krim_agent.workspace_tree_exchange import WorkspaceTreeExchangeWriter
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshotReader

pytestmark = pytest.mark.skipif(not sys.platform.startswith("linux"), reason="实际Linux树交换")


async def cli(url, agent_id, command, value=None, *options):
    arguments = [sys.executable, "-I", "-m", "krim_agent", "tree-exchanges", command, agent_id]
    if value is not None:
        arguments.append(value)
    process = await asyncio.create_subprocess_exec(
        *arguments,
        "--url",
        url,
        *options,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        out, err = await asyncio.wait_for(process.communicate(), 25)
        assert process.returncode == 0, err.decode()
        return json.loads(out)
    finally:
        if process.returncode is None:
            process.kill()
            await process.communicate()


async def test_sdk_binary_actual_after_keep_undo_and_same_keys_zero_second_operation(
    runtime, tmp_path, monkeypatch
):
    service, agent, _ = await setup(runtime, tmp_path)
    try:
        left = await agent.inspect_exchange_tree("holder/live")
        right = await agent.inspect_exchange_tree("new-parent/prepared")
        request = TreeExchangePreviewRequest(
            left_path=left.path,
            right_path=right.path,
            expected_left=left.fingerprint,
            expected_right=right.fingerprint,
            request_key="sdk-preview",
        )
        proposal = await agent.preview_tree_exchange(request)
        original = action(proposal)
        done = await agent.change_tree_exchange(original)
        assert done.status == "done" and done.receipt.changed
        current = (
            await agent.query_tree_exchange(
                TreeExchangeQueryRequest(kind="proposal", identifier=proposal.proposal_id)
            )
        ).proposal
        keep_request = action(current, "keep", "keep")
        kept = await agent.change_tree_exchange(keep_request)
        assert kept.status == "done" and not kept.receipt.changed
        current = (
            await agent.query_tree_exchange(
                TreeExchangeQueryRequest(kind="by_key", request_key=request.request_key)
            )
        ).proposal
        undo_request = action(current, "undo", "undo")
        undone = await agent.change_tree_exchange(undo_request)
        assert undone.status == "done" and undone.receipt.changed
        assert (tmp_path / "holder/live/sub/data.bin").read_bytes() == b"\0\xff\x80"

        def forbidden(*_args, **_kwargs):
            pytest.fail("SDK原键读取不得重准备/重交换")

        for method in ("prepare", "prepare_undo", "apply"):
            monkeypatch.setattr(WorkspaceTreeExchangeWriter, method, forbidden)
        assert (await agent.preview_tree_exchange(request)).proposal_id == proposal.proposal_id
        assert await agent.change_tree_exchange(original) == done
        assert await agent.change_tree_exchange(keep_request) == kept
        assert await agent.change_tree_exchange(undo_request) == undone
        page = await agent.query_tree_exchange(
            TreeExchangeQueryRequest(kind="history", identifier=proposal.proposal_id, limit=1)
        )
        assert (
            len(page.history.items) == 1
            and page.history.next_after == page.history.items[0].operation_id
        )
        assert await counts(service.workspace_edits.database) == [1, 3, 0, 0, 0, 0]
    finally:
        await service.close()


async def test_real_generated_source_seal_sdk_rest_client_cli_same_plan_and_zero_reobserve(
    runtime, tmp_path, monkeypatch
):
    from test_completed_tree_origins import complete

    runtime.tools.register_many(*tree_exchange_tools())
    service, agent, scope, trees, plan, binding, snapshot, outputs = await complete(
        runtime, tmp_path, extra_names=NAMES
    )
    try:
        service.register_tree_exchanges()
        request = TreeExchangeSealRequest(
            plan_id=plan.plan_id, expected_fingerprint=snapshot.fingerprint
        )
        sealed = await agent.seal_tree_exchange(request)
        assert sealed.plan_id == plan.plan_id and sealed.tree_fingerprint == snapshot.fingerprint

        def forbidden(*_args, **_kwargs):
            pytest.fail("已封存原plan的管理冷重试不可重观察当前树")

        monkeypatch.setattr(WorkspaceTreeSnapshotReader, "capture", forbidden)
        (tmp_path / "tree/sub/data.bin").write_bytes(b"changed")
        assert await agent.seal_tree_exchange(request) == sealed
        async with api_for(runtime, service, monkeypatch) as (app, api):
            response = await api.post(
                f"/v1/agents/{agent.agent_id}/workspace/tree-exchanges/seal",
                json=request.model_dump(mode="json"),
            )
            assert response.status_code == 200 and response.json() == sealed.model_dump(mode="json")
            async with tcp_for(app) as url:
                with KrimClient(url) as client:
                    assert (
                        await asyncio.to_thread(
                            client.agent_tree_exchange_seal, agent.agent_id, request
                        )
                        == sealed
                    )
                assert await cli(
                    url,
                    agent.agent_id,
                    "seal",
                    plan.plan_id,
                    "--expected-fingerprint",
                    snapshot.fingerprint,
                ) == sealed.model_dump(mode="json")
        assert await counts(service.workspace_edits.database) == [0, 0, 0, 0, 0, 1]
        saved = await service.manager.tree_exchanges.store.seals.get(scope, binding, plan.plan_id)
        assert saved.origin.operation_ids == tuple(item["operation_id"] for item in outputs)
    finally:
        await service.close()


async def test_rest_public_cancel_waits_same_admitted_sdk_operation(runtime, tmp_path, monkeypatch):
    service, agent, _ = await setup(runtime, tmp_path)
    entered, release = Event(), Event()
    original = WorkspaceTreeExchangeWriter.apply
    worker = cancelling = None
    try:
        proposal, _ = await preview(service, agent)
        request = action(proposal)

        def paused(writer, prepared, **kwargs):
            entered.set()
            assert release.wait(10)
            return original(writer, prepared, **kwargs)

        monkeypatch.setattr(WorkspaceTreeExchangeWriter, "apply", paused)
        worker = asyncio.create_task(agent.change_tree_exchange(request))
        await wait_event(entered)
        saved = asyncio.Event()
        original_cancel = service.manager.tree_exchanges.store.cancel

        async def note(scope, identifier):
            result = await original_cancel(scope, identifier)
            saved.set()
            return result

        monkeypatch.setattr(service.manager.tree_exchanges.store, "cancel", note)
        async with api_for(runtime, service, monkeypatch) as (_, api):
            cancelling = asyncio.create_task(
                api.post(
                    f"/v1/agents/{agent.agent_id}/workspace/tree-exchanges/{proposal.proposal_id}/cancel"
                )
            )
            async with asyncio.timeout(5):
                await saved.wait()
            assert not cancelling.done()
            release.set()
            done, response = await asyncio.gather(worker, cancelling)
            assert done.status == "rejected" and done.cancelled
            assert response.status_code == 200 and response.json()["status"] == "cancelled"
        assert (tmp_path / "holder/live/sub/data.bin").read_bytes() == b"\0\xff\x80"
    finally:
        release.set()
        await asyncio.gather(
            *(task for task in (worker, cancelling) if task is not None), return_exceptions=True
        )
        await service.close()


async def test_actual_serve_cli_explicit_exchange_enable_and_tcp_publication(tmp_path):
    seed(tmp_path)
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
        "--tree-exchanges",
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
                    if (await api.get(url + "/healthz")).status_code == 200:
                        break
                except httpx.TransportError:
                    pass
                await asyncio.sleep(0.05)
        with KrimClient(url) as client:
            agent = await asyncio.to_thread(client.agent_spawn, "default")
            left = await asyncio.to_thread(
                client.agent_tree_exchange_inspect, agent.agent_id, "holder/live"
            )
            right = await asyncio.to_thread(
                client.agent_tree_exchange_inspect, agent.agent_id, "new-parent/prepared"
            )
            proposal = await asyncio.to_thread(
                client.agent_tree_exchange_preview,
                agent.agent_id,
                TreeExchangePreviewRequest(
                    left_path=left.path,
                    right_path=right.path,
                    expected_left=left.fingerprint,
                    expected_right=right.fingerprint,
                    request_key="serve-preview",
                ),
            )
            done = await asyncio.to_thread(
                client.agent_tree_exchange_change, agent.agent_id, action(proposal), confirmed=True
            )
            assert (
                done.status == "done"
                and (tmp_path / "holder/live/sub/data.bin").read_bytes() == b"\x80\x00new"
            )
            assert (await cli(url, agent.agent_id, "operation-by-key", "original-apply"))[
                "operation"
            ] == done.model_dump(mode="json")
    finally:
        if process.returncode is None:
            process.terminate()
            try:
                await asyncio.wait_for(process.communicate(), 15)
            except TimeoutError:
                process.kill()
                await process.communicate()
                raise


async def test_rest_unknown_same_operation_current_scope_and_no_namespace_replay(
    runtime, tmp_path, monkeypatch
):
    service, agent, _ = await setup(runtime, tmp_path)
    try:
        proposal, _ = await preview(service, agent)
        exchange = service.manager.tree_exchanges
        original = exchange.store.settle

        async def unavailable(*_args, **_kwargs):
            raise OSError("实际回执丢失")

        monkeypatch.setattr(exchange.store, "settle", unavailable)
        unknown = await agent.change_tree_exchange(action(proposal))
        assert unknown.status == "uncertain"
        monkeypatch.setattr(exchange.store, "settle", original)

        def forbidden(*_args, **_kwargs):
            pytest.fail("REST unknown恢复不得重prepare/apply")

        monkeypatch.setattr(WorkspaceTreeExchangeWriter, "prepare", forbidden)
        monkeypatch.setattr(WorkspaceTreeExchangeWriter, "apply", forbidden)
        async with api_for(runtime, service, monkeypatch) as (_, api):
            base = f"/v1/agents/{agent.agent_id}/workspace"
            response = await api.post(
                base + f"/tree-exchange-operations/{unknown.operation_id}/reconcile"
            )
            assert response.status_code == 200, response.text
            result = response.json()
            assert result["operation_id"] == unknown.operation_id and result["status"] == "done"
            cached = await api.get(
                base + "/tree-exchanges/query",
                params={"kind": "operation_by_key", "request_key": "original-apply"},
            )
            assert cached.json()["operation"] == result
            second = await service.spawn("exchange")
            foreign = await api.get(
                f"/v1/agents/{second.agent_id}/workspace/tree-exchanges/query",
                params={"kind": "proposal", "identifier": proposal.proposal_id},
            )
            assert foreign.status_code == 404
        assert await counts(service.workspace_edits.database) == [1, 1, 0, 0, 0, 0]
    finally:
        await service.close()


@pytest.mark.parametrize(
    "route,params",
    [
        ("tree-exchange-state", [("path", "holder/live"), ("path", "new-parent/prepared")]),
        ("tree-exchange-state", {"path": "holder/live", "scope_id": "other"}),
        ("tree-exchanges/query", {"kind": "proposal"}),
        ("tree-exchanges/query", {"kind": "proposals", "identifier": "other"}),
        ("tree-exchanges/query", {"kind": "history", "identifier": "other", "limit": 0}),
        ("tree-exchanges/query", [("kind", "proposals"), ("kind", "history")]),
    ],
)
async def test_rest_rejects_unknown_duplicate_private_or_ambiguous_queries(
    runtime, tmp_path, monkeypatch, route, params
):
    service, agent, _ = await setup(runtime, tmp_path)
    try:
        async with api_for(runtime, service, monkeypatch) as (_, api):
            response = await api.get(
                f"/v1/agents/{agent.agent_id}/workspace/{route}", params=params
            )
            assert response.status_code == 422, response.text
        assert await counts(service.workspace_edits.database) == [0, 0, 0, 0, 0, 0]
    finally:
        await service.close()


async def test_rest_body_path_and_source_privates_do_not_expand_original_permission(
    runtime, tmp_path, monkeypatch
):
    service, agent, _ = await setup(runtime, tmp_path)
    try:
        proposal, request = await preview(service, agent)
        async with api_for(runtime, service, monkeypatch) as (_, api):
            base = f"/v1/agents/{agent.agent_id}/workspace"
            assert (
                await api.post(
                    base + "/tree-exchange-operations/operation/reconcile",
                    json={"operation_id": "other"},
                )
            ).status_code == 422
            assert (
                await api.post(
                    base + f"/tree-exchanges/{proposal.proposal_id}/cancel",
                    params={"confirmed": "true"},
                )
            ).status_code == 422
            assert (
                await api.post(
                    base + "/tree-exchanges/preview",
                    json={**request.model_dump(mode="json"), "owner_id": "private"},
                )
            ).status_code == 422
            assert (
                await api.post(
                    base + "/tree-exchanges/seal",
                    json={"plan_id": "plan", "expected_fingerprint": "a" * 64, "snapshot": {}},
                )
            ).status_code == 422
        assert await counts(service.workspace_edits.database) == [1, 0, 0, 0, 0, 0]
    finally:
        await service.close()


async def test_real_tcp_client_cli_inspect_preview_apply_keep_undo_history_and_original_keys(
    runtime, tmp_path, monkeypatch
):
    service, agent, _ = await setup(runtime, tmp_path)
    try:
        async with api_for(runtime, service, monkeypatch) as (app, _):
            async with tcp_for(app) as url:
                with KrimClient(url) as client:
                    left = await asyncio.to_thread(
                        client.agent_tree_exchange_inspect, agent.agent_id, "holder/live"
                    )
                    right = await asyncio.to_thread(
                        client.agent_tree_exchange_inspect, agent.agent_id, "new-parent/prepared"
                    )
                    request = TreeExchangePreviewRequest(
                        left_path=left.path,
                        right_path=right.path,
                        expected_left=left.fingerprint,
                        expected_right=right.fingerprint,
                        request_key="tcp-preview",
                    )
                    path = tmp_path / "preview.json"
                    path.write_text(request.model_dump_json())
                    proposal = await cli(
                        url, agent.agent_id, "preview", None, "--request", str(path)
                    )
                    done = await cli(
                        url,
                        agent.agent_id,
                        "apply",
                        proposal["proposal_id"],
                        "--revision",
                        str(proposal["revision"]),
                        "--request-key",
                        "tcp-apply",
                    )
                    assert done["status"] == "done" and done["receipt"]["changed"]
                    current = (await cli(url, agent.agent_id, "get", proposal["proposal_id"]))[
                        "proposal"
                    ]
                    kept = await cli(
                        url,
                        agent.agent_id,
                        "keep",
                        proposal["proposal_id"],
                        "--revision",
                        str(current["revision"]),
                        "--request-key",
                        "tcp-keep",
                    )
                    assert kept["status"] == "done" and not kept["receipt"]["changed"]
                    current = (
                        await asyncio.to_thread(
                            client.agent_tree_exchange_query,
                            agent.agent_id,
                            TreeExchangeQueryRequest(
                                kind="proposal", identifier=proposal["proposal_id"]
                            ),
                        )
                    ).proposal
                    undone = await asyncio.to_thread(
                        client.agent_tree_exchange_change,
                        agent.agent_id,
                        action(current, "undo", "tcp-undo"),
                    )
                    assert undone.status == "done"
                    history = await cli(
                        url, agent.agent_id, "history", proposal["proposal_id"], "--limit", "1"
                    )
                    assert len(history["history"]["items"]) == 1
                    assert (await cli(url, agent.agent_id, "by-key", "tcp-preview"))["proposal"][
                        "status"
                    ] == "undone"
                    assert (await cli(url, agent.agent_id, "operation-by-key", "tcp-apply"))[
                        "operation"
                    ] == done
                    assert (
                        await asyncio.to_thread(
                            client.agent_tree_exchange_preview, agent.agent_id, request
                        )
                    ).proposal_id == proposal["proposal_id"]
        assert (tmp_path / "holder/live/sub/data.bin").read_bytes() == b"\0\xff\x80"
        assert await counts(service.workspace_edits.database) == [1, 3, 0, 0, 0, 0]
    finally:
        await service.close()


async def test_model_original_facts_sdk_rest_tcp_cli_cold_with_zero_provider_or_writer(
    runtime, tmp_path, monkeypatch
):
    provider = Provider()
    service, agent, _ = await setup(runtime, tmp_path, provider=provider)
    try:
        result = await agent.execute("原模型完整交换保留撤销", idempotency_key="model-consumers")
        assert result.status == "completed"
        before = await counts(service.workspace_edits.database)

        def forbidden(*_args, **_kwargs):
            pytest.fail("所有管理消费者冷查必须0Provider/prepare/apply")

        monkeypatch.setattr(provider, "stream", forbidden)
        monkeypatch.setattr(WorkspaceTreeExchangeWriter, "prepare", forbidden)
        monkeypatch.setattr(WorkspaceTreeExchangeWriter, "apply", forbidden)
        selector = TreeExchangeQueryRequest(kind="by_key", request_key="model-preview")
        sdk = await agent.query_tree_exchange(selector)
        async with api_for(runtime, service, monkeypatch) as (app, api):
            assert (
                await api.get(
                    f"/v1/agents/{agent.agent_id}/workspace/tree-exchanges/query",
                    params={"kind": "by_key", "request_key": "model-preview"},
                )
            ).json() == sdk.model_dump(mode="json")
            async with tcp_for(app) as url:
                with KrimClient(url) as client:
                    assert (
                        await asyncio.to_thread(
                            client.agent_tree_exchange_query, agent.agent_id, selector
                        )
                        == sdk
                    )
                assert await cli(url, agent.agent_id, "by-key", "model-preview") == sdk.model_dump(
                    mode="json"
                )
        assert await counts(service.workspace_edits.database) == before == [1, 3, 0, 4, 0, 0]
    finally:
        await service.close()


@pytest.mark.parametrize("enabled", [False, True])
async def test_factory_explicit_feature_enable_and_existing_frozen_agent_stays_original(
    runtime, tmp_path, enabled
):
    seed(tmp_path)

    async def policy(definition, _context):
        return "allow" if definition.risk == "read" else "ask"

    runtime.tool_policy = policy
    app = create_app(
        runtime=runtime,
        workspace_root=tmp_path,
        workspace_writable=True,
        tree_exchanges_enabled=enabled,
    )
    async with app.router.lifespan_context(app):
        service = app.state.agents
        agent = AgentSession(service, (await service.spawn("default")).agent_id)
        original = {item.name for item in service._handle(agent.agent_id).runtime.tools.definitions}
        assert ("tree_exchange_change" in original) == enabled
        if not enabled:
            service.register_tree_exchanges()
            assert {
                item.name for item in service._handle(agent.agent_id).runtime.tools.definitions
            } == original
            return
        left = await agent.inspect_exchange_tree("holder/live")
        right = await agent.inspect_exchange_tree("new-parent/prepared")
        proposal = await agent.preview_tree_exchange(
            TreeExchangePreviewRequest(
                left_path=left.path,
                right_path=right.path,
                expected_left=left.fingerprint,
                expected_right=right.fingerprint,
                request_key="factory",
            )
        )
        assert (await agent.change_tree_exchange(action(proposal), confirmed=True)).status == "done"


@pytest.mark.parametrize(
    "fault", ["scope", "left", "right", "left_fingerprint", "right_fingerprint", "source", "origin"]
)
async def test_client_refuses_schema_valid_but_wrong_original_preview_return(
    runtime, tmp_path, fault
):
    service, agent, _ = await setup(runtime, tmp_path)
    try:
        proposal, request = await preview(service, agent)
        payload = proposal.model_dump(mode="json")
        field, value = {
            "scope": ("scope_id", "session:foreign"),
            "left": ("left_path", "other"),
            "right": ("right_path", "other"),
            "left_fingerprint": ("left_before_fingerprint", "a" * 64),
            "right_fingerprint": ("right_before_fingerprint", "b" * 64),
            "source": ("source_plan_id", "other"),
            "origin": ("origin", "completed_tree"),
        }[fault]
        payload[field] = value
        with KrimClient("http://127.0.0.1") as client:
            client._http.close()
            client._http = httpx.Client(
                transport=httpx.MockTransport(lambda _request: httpx.Response(200, json=payload)),
                base_url="http://127.0.0.1",
            )
            with pytest.raises(ClientError) as refused:
                client.agent_tree_exchange_preview(agent.agent_id, request)
            assert refused.value.code == "workspace_tree_exchange_response"
    finally:
        await service.close()


async def test_factory_readonly_explicit_flag_keeps_only_inspect_and_query(runtime, tmp_path):
    seed(tmp_path)
    app = create_app(runtime=runtime, workspace_root=tmp_path, tree_exchanges_enabled=True)
    async with app.router.lifespan_context(app):
        service = app.state.agents
        agent = AgentSession(service, (await service.spawn("default")).agent_id)
        names = {item.name for item in service._handle(agent.agent_id).runtime.tools.definitions}
        assert names & set(NAMES) == {"tree_exchange_inspect", "tree_exchange_query"}
        left = await agent.inspect_exchange_tree("holder/live")
        right = await agent.inspect_exchange_tree("new-parent/prepared")
        with pytest.raises(ServiceError) as refused:
            await agent.preview_tree_exchange(
                TreeExchangePreviewRequest(
                    left_path=left.path,
                    right_path=right.path,
                    expected_left=left.fingerprint,
                    expected_right=right.fingerprint,
                    request_key="readonly",
                )
            )
        assert refused.value.code == "tool_not_found"
        assert await counts(service.workspace_edits.database) == [0, 0, 0, 0, 0, 0]
