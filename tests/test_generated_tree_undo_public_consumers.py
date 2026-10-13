"""原生成根管理消费端；SDK同原Scope/实际operation、unknown与版本链。"""

import asyncio
import json
import os
import socket
import sys
from threading import Event

import httpx
import pytest
from test_generated_tree_undo_ledger import counts
from test_generated_tree_undo_tools import NAMES, Provider, action, setup
from test_tree_move_public_consumers import api_for, tcp_for
from test_workspace_changes import wait_event

from krim_agent.api import create_app
from krim_agent.budgets import UsageBudgetConfig
from krim_agent.cli import main as cli_main
from krim_agent.client import ClientError, KrimClient
from krim_agent.sdk import AgentSession, GeneratedTreeUndoPreviewRequest
from krim_agent.workspace_tree_move import TreeMoveQueryRequest
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshotReader
from krim_agent.workspace_tree_writer import WorkspaceTreeWriter


async def cli(url, agent_id, command, value=None, *options):
    args = [sys.executable, "-I", "-m", "krim_agent", "generated-tree-undos", command, agent_id]
    if value is not None:
        args.append(value)
    process = await asyncio.create_subprocess_exec(
        *args,
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


async def test_sdk_actual_root_unknown_same_operation_restore_new_version_cancel_and_cold_key(
    runtime, tmp_path, monkeypatch
):
    service, agent, scope, trees, plan, binding, snapshot, request = await setup(runtime, tmp_path)
    try:
        assert type(request) is GeneratedTreeUndoPreviewRequest
        proposal = await agent.preview_generated_tree_undo(request)
        assert (
            await agent.query_generated_tree_undo(
                TreeMoveQueryRequest(kind="by_key", request_key=request.request_key)
            )
        ).proposal == proposal
        store = service.manager.tree_moves.store
        original = store.settle

        async def unavailable(*_args, **_kwargs):
            raise OSError("原SDK根效果后丢保存回程")

        monkeypatch.setattr(store, "settle", unavailable)
        unknown = await agent.change_generated_tree_undo(action(proposal))
        assert unknown.operation.status == "uncertain" and unknown.source_plan_id == plan.plan_id
        assert (
            not (tmp_path / "tree").exists()
            and (tmp_path / "retained/sub/data.bin").read_bytes() == b"\0\xff"
        )
        monkeypatch.setattr(store, "settle", original)
        reconciled = await agent.reconcile_generated_tree_undo(unknown.operation.operation_id)
        assert (
            reconciled.operation.operation_id == unknown.operation.operation_id
            and reconciled.operation.status == "done"
        )
        current = (
            await agent.query_generated_tree_undo(
                TreeMoveQueryRequest(kind="proposal", identifier=proposal.move.proposal_id)
            )
        ).proposal
        restored = await agent.change_generated_tree_undo(
            action(current, "restore_root", "sdk-restore")
        )
        assert restored.operation.status == "done"
        actual = WorkspaceTreeSnapshotReader(binding.require()).capture("tree")
        repeat = request.model_copy(
            update={
                "expected_fingerprint": actual.fingerprint,
                "previous_restore_operation_id": restored.operation.operation_id,
                "request_key": "sdk-repeat",
            }
        )
        again = await agent.preview_generated_tree_undo(repeat)
        cancelled = await agent.cancel_generated_tree_undo(again.move.proposal_id)
        assert cancelled.root_state == "cancelled" and cancelled.generation_status == "done"
        assert await agent.preview_generated_tree_undo(repeat) == cancelled
        assert await trees.get(scope, plan.plan_id) == plan
    finally:
        await service.close()


async def test_rest_original_scope_strict_query_body_and_unknown_same_operation_zero_writer(
    runtime, tmp_path, monkeypatch
):
    service, agent, scope, trees, plan, binding, snapshot, request = await setup(runtime, tmp_path)
    try:
        async with api_for(runtime, service, monkeypatch) as (_, api):
            base = f"/v1/agents/{agent.agent_id}/workspace"
            response = await api.post(
                base + "/generated-tree-undos/preview", json=request.model_dump(mode="json")
            )
            assert response.status_code == 200, response.text
            proposal = await agent.preview_generated_tree_undo(request)
            assert response.json() == proposal.model_dump(mode="json")
            for params in (
                {"kind": "proposal"},
                {"kind": "proposals", "scope": "foreign"},
                [("kind", "proposals"), ("kind", "proposals")],
                {"kind": "proposal", "identifier": proposal.move.proposal_id, "limit": 1},
                {"kind": "proposals", "limit": 33},
            ):
                assert (
                    await api.get(base + "/generated-tree-undos/query", params=params)
                ).status_code == 422
            for path, body in (
                (
                    "/generated-tree-undos/preview",
                    {**request.model_dump(mode="json"), "source": {"plan_id": plan.plan_id}},
                ),
                (
                    "/generated-tree-undos/change",
                    {**action(proposal).model_dump(mode="json"), "scope_id": scope},
                ),
                (
                    f"/generated-tree-undos/{proposal.move.proposal_id}/cancel",
                    {"owner_id": "forged"},
                ),
            ):
                assert (await api.post(base + path, json=body)).status_code == 422
            other = await service.spawn("publisher")
            assert (
                await api.get(
                    f"/v1/agents/{other.agent_id}/workspace/generated-tree-undos/query",
                    params={"kind": "proposal", "identifier": proposal.move.proposal_id},
                )
            ).status_code == 404
            store = service.manager.tree_moves.store
            original = store.settle

            async def unavailable(*_args, **_kwargs):
                raise OSError("原HTTP根效果后回执无法保存")

            monkeypatch.setattr(store, "settle", unavailable)
            unknown = await api.post(
                base + "/generated-tree-undos/change", json=action(proposal).model_dump(mode="json")
            )
            assert (
                unknown.status_code == 200 and unknown.json()["operation"]["status"] == "uncertain"
            )
            identifier = unknown.json()["operation"]["operation_id"]
            monkeypatch.setattr(store, "settle", original)
            path = f"/generated-tree-undo-operations/{identifier}/reconcile"
            assert (await api.post(base + path, json={"operation_id": "other"})).status_code == 422
            assert (
                await api.post(base + path, params=[("confirmed", "true"), ("confirmed", "true")])
            ).status_code == 422

            def forbidden(*_args, **_kwargs):
                pytest.fail("HTTP根unknown核实/冷key禁止再次prepare或根移动")

            monkeypatch.setattr(WorkspaceTreeWriter, "apply", forbidden)
            monkeypatch.setattr(WorkspaceTreeWriter, "prepare", forbidden)
            resolved = await api.post(base + path)
            assert resolved.status_code == 200 and resolved.json()["operation"]["status"] == "done"
            assert resolved.json()["operation"]["operation_id"] == identifier
            cold = await api.get(
                base + "/generated-tree-undos/query",
                params={"kind": "operation_by_key", "request_key": "root-remove"},
            )
            assert cold.json()["operation"] == resolved.json()
            assert (
                await api.post(
                    base + "/generated-tree-undos/preview", json=request.model_dump(mode="json")
                )
            ).json()["root_state"] == "preserved"
            assert (await counts(store.database))[:4] == [1, 1, 1, 0]
    finally:
        await service.close()


async def test_real_tcp_client_cli_preview_remove_keep_restore_history_same_ids_and_cold_key(
    runtime, tmp_path, monkeypatch
):
    service, agent, scope, trees, plan, binding, snapshot, request = await setup(runtime, tmp_path)
    request_file = tmp_path / "root-request.json"
    request_file.write_text(request.model_dump_json())
    try:
        async with api_for(runtime, service, monkeypatch) as (app, _):
            async with tcp_for(app) as url:
                preview = await cli(
                    url, agent.agent_id, "preview", None, "--request", str(request_file)
                )
                identifier = preview["move"]["proposal_id"]
                removed = await cli(
                    url,
                    agent.agent_id,
                    "remove-root",
                    identifier,
                    "--revision",
                    str(preview["move"]["revision"]),
                    "--request-key",
                    "cli-remove",
                )
                assert (
                    removed["operation"]["status"] == "done"
                    and removed["source_plan_id"] == plan.plan_id
                )
                assert (
                    not (tmp_path / "tree").exists()
                    and (tmp_path / "retained/sub/data.bin").read_bytes() == b"\0\xff"
                )
                with KrimClient(url) as client:
                    current = (
                        await asyncio.to_thread(
                            client.agent_generated_tree_undo_query,
                            agent.agent_id,
                            TreeMoveQueryRequest(kind="proposal", identifier=identifier),
                        )
                    ).proposal
                    kept = await asyncio.to_thread(
                        client.agent_generated_tree_undo_change,
                        agent.agent_id,
                        action(current, "keep", "tcp-keep"),
                    )
                    assert kept.operation.status == "done" and not kept.operation.receipt.changed
                current = (await cli(url, agent.agent_id, "get", identifier))["proposal"]
                restored = await cli(
                    url,
                    agent.agent_id,
                    "restore-root",
                    identifier,
                    "--revision",
                    str(current["move"]["revision"]),
                    "--request-key",
                    "cli-restore",
                )
                assert restored["operation"]["status"] == "done"
                assert (await cli(url, agent.agent_id, "operation-by-key", "cli-remove"))[
                    "operation"
                ] == removed
                history = (await cli(url, agent.agent_id, "history", identifier, "--limit", "1"))[
                    "history"
                ]
                assert (
                    len(history["items"]) == 1
                    and history["next_after"] == history["items"][0]["operation"]["operation_id"]
                )
                assert (await cli(url, agent.agent_id, "by-key", "root-preview"))["proposal"][
                    "root_state"
                ] == "restored"

                def forbidden(*_args, **_kwargs):
                    pytest.fail("TCP/CLI原key/operation冷读不可prepare/重移动")

                monkeypatch.setattr(WorkspaceTreeWriter, "prepare", forbidden)
                monkeypatch.setattr(WorkspaceTreeWriter, "apply", forbidden)
                with KrimClient(url) as client:
                    cold = await asyncio.to_thread(
                        client.agent_generated_tree_undo_preview, agent.agent_id, request
                    )
                    assert cold.move.proposal_id == identifier and cold.root_state == "restored"
                assert (
                    await cli(
                        url, agent.agent_id, "reconcile", removed["operation"]["operation_id"]
                    )
                ) == removed
        assert (tmp_path / "tree/sub/data.bin").stat().st_ino == snapshot.entries[-1].file.inode
        assert (await counts(service.workspace_edits.database))[:4] == [1, 1, 3, 0]
    finally:
        await service.close()


async def test_model_facts_sdk_http_tcp_cli_all_cold_reads_zero_provider_or_writer(
    runtime, tmp_path, monkeypatch
):
    provider = Provider()
    runtime.provider = provider
    service, agent, scope, trees, plan, binding, snapshot, request = await setup(
        runtime, tmp_path, usage_budget=UsageBudgetConfig(max_total_tokens=100_000)
    )
    provider.request = request
    try:
        result = await agent.execute("管理端原Model根事实", idempotency_key="root-management-model")
        assert result.status == "completed"
        before = await counts(service.workspace_edits.database)

        def forbidden(*_args, **_kwargs):
            pytest.fail("Model原完成事实的管理消费不可重跑Provider/原树生成/Writer")

        monkeypatch.setattr(provider, "stream", forbidden)
        monkeypatch.setattr(WorkspaceTreeWriter, "prepare", forbidden)
        monkeypatch.setattr(WorkspaceTreeWriter, "apply", forbidden)
        monkeypatch.setattr(WorkspaceTreeSnapshotReader, "capture", forbidden)
        selector = TreeMoveQueryRequest(kind="by_key", request_key="root-preview")
        sdk = await agent.query_generated_tree_undo(selector)
        async with api_for(runtime, service, monkeypatch) as (app, api):
            assert (
                await api.get(
                    f"/v1/agents/{agent.agent_id}/workspace/generated-tree-undos/query",
                    params={"kind": "by_key", "request_key": "root-preview"},
                )
            ).json() == sdk.model_dump(mode="json")
            async with tcp_for(app) as url:
                with KrimClient(url) as client:
                    assert (
                        await asyncio.to_thread(
                            client.agent_generated_tree_undo_query, agent.agent_id, selector
                        )
                        == sdk
                    )
                assert await cli(url, agent.agent_id, "by-key", "root-preview") == sdk.model_dump(
                    mode="json"
                )
        assert provider.calls == 8 and await counts(service.workspace_edits.database) == before == [
            1,
            1,
            3,
            0,
            4,
        ]
    finally:
        await service.close()


async def test_rest_cancel_closes_same_sdk_admission_without_second_producer(
    runtime, tmp_path, monkeypatch
):
    service, agent, scope, trees, plan, binding, snapshot, request = await setup(runtime, tmp_path)
    entered, release = Event(), Event()
    worker = None
    original = WorkspaceTreeWriter.apply
    try:
        proposal = await agent.preview_generated_tree_undo(request)

        def paused(writer, prepared, **kwargs):
            entered.set()
            assert release.wait(10)
            return original(writer, prepared, **kwargs)

        monkeypatch.setattr(WorkspaceTreeWriter, "apply", paused)
        worker = asyncio.create_task(agent.change_generated_tree_undo(action(proposal)))
        await wait_event(entered)
        async with api_for(runtime, service, monkeypatch) as (_, api):
            response = await api.post(
                f"/v1/agents/{agent.agent_id}/workspace/generated-tree-undos/{proposal.move.proposal_id}/cancel"
            )
            assert response.status_code == 200, response.text
            assert response.json()["move"]["active_operation_id"] is not None
            assert (
                await service.manager.tree_moves.store.operation_by_key(scope, "root-remove")
            ).view.cancelled
            release.set()
            result = await worker
            assert result.operation.status == "rejected" and result.operation.cancelled
            assert (
                await agent.query_generated_tree_undo(
                    TreeMoveQueryRequest(kind="proposal", identifier=proposal.move.proposal_id)
                )
            ).proposal.root_state == "cancelled"
        assert not service.manager.tree_moves._active and (
            await counts(service.workspace_edits.database)
        )[:4] == [1, 1, 1, 0]
    finally:
        release.set()
        if worker is not None:
            await asyncio.gather(worker, return_exceptions=True)
        await service.close()


async def test_client_rejects_valid_schema_wrong_source_scope_action_cursor_and_selection(
    runtime, tmp_path
):
    service, agent, scope, trees, plan, binding, snapshot, request = await setup(runtime, tmp_path)
    try:
        proposal = await agent.preview_generated_tree_undo(request)
        removed = await agent.change_generated_tree_undo(action(proposal))
        selected = await agent.query_generated_tree_undo(
            TreeMoveQueryRequest(kind="proposal", identifier=proposal.move.proposal_id)
        )
        history = await agent.query_generated_tree_undo(
            TreeMoveQueryRequest(kind="history", identifier=proposal.move.proposal_id, limit=1)
        )
        page = await agent.query_generated_tree_undo(
            TreeMoveQueryRequest(kind="proposals", limit=1)
        )
        with KrimClient("http://127.0.0.1") as client:
            client._http.close()

            def respond(payload):
                client._http = httpx.Client(
                    transport=httpx.MockTransport(
                        lambda _request: httpx.Response(200, json=payload)
                    ),
                    base_url="http://127.0.0.1",
                )

            mutations = [
                ("source_plan_id", "other"),
                ("previous_restore_operation_id", "other"),
                ("sealed_fingerprint", "a" * 64),
            ]
            for field, value in mutations:
                payload = proposal.model_dump(mode="json")
                payload[field] = value
                respond(payload)
                with pytest.raises(ClientError):
                    client.agent_generated_tree_undo_preview(agent.agent_id, request)
                client._http.close()
            for field, value in [
                ("scope_id", "session:foreign"),
                ("target_path", "wrong"),
                ("before_fingerprint", "a" * 64),
            ]:
                payload = proposal.model_dump(mode="json")
                payload["move"][field] = value
                respond(payload)
                with pytest.raises(ClientError):
                    client.agent_generated_tree_undo_preview(agent.agent_id, request)
                client._http.close()
            for field, value in [
                ("proposal_id", "other"),
                ("expected_revision", 10),
                ("scope_id", "session:other"),
            ]:
                payload = removed.model_dump(mode="json")
                payload["operation"][field] = value
                respond(payload)
                with pytest.raises(ClientError):
                    client.agent_generated_tree_undo_change(agent.agent_id, action(proposal))
                client._http.close()
            payload = removed.model_dump(mode="json")
            payload["operation"]["operation_id"] = "other"
            respond(payload)
            with pytest.raises(ClientError):
                client.agent_generated_tree_undo_reconcile(
                    agent.agent_id, removed.operation.operation_id
                )
            client._http.close()
            invalid_queries = []
            payload = selected.model_dump(mode="json")
            payload["proposal"]["move"]["proposal_id"] = "other"
            invalid_queries.append(
                (
                    payload,
                    TreeMoveQueryRequest(kind="proposal", identifier=proposal.move.proposal_id),
                )
            )
            payload = history.model_dump(mode="json")
            payload["history"]["items"][0]["operation"]["proposal_id"] = "other"
            invalid_queries.append(
                (
                    payload,
                    TreeMoveQueryRequest(
                        kind="history", identifier=proposal.move.proposal_id, limit=1
                    ),
                )
            )
            payload = page.model_dump(mode="json")
            payload["proposals"]["next_after"] = "other"
            invalid_queries.append((payload, TreeMoveQueryRequest(kind="proposals", limit=1)))
            payload = selected.model_dump(mode="json")
            payload["kind"] = "by_key"
            invalid_queries.append(
                (
                    payload,
                    TreeMoveQueryRequest(kind="proposal", identifier=proposal.move.proposal_id),
                )
            )
            payload = selected.model_dump(mode="json")
            payload["operation"] = removed.model_dump(mode="json")
            invalid_queries.append(
                (
                    payload,
                    TreeMoveQueryRequest(kind="proposal", identifier=proposal.move.proposal_id),
                )
            )
            for payload, selector in invalid_queries:
                respond(payload)
                with pytest.raises(ClientError) as error:
                    client.agent_generated_tree_undo_query(agent.agent_id, selector)
                assert error.value.code == "workspace_generated_undo_response"
                client._http.close()
    finally:
        await service.close()


@pytest.mark.parametrize("writable,enabled", [(True, False), (True, True), (False, True)])
async def test_factory_root_feature_explicit_enable_readonly_and_frozen_agents(
    runtime, tmp_path, writable, enabled
):
    app = create_app(
        runtime=runtime,
        workspace_root=tmp_path,
        workspace_writable=writable,
        generated_tree_undos_enabled=enabled,
    )
    async with app.router.lifespan_context(app):
        service = app.state.agents
        agent = AgentSession(service, (await service.spawn("default")).agent_id)
        original = {item.name for item in service._handle(agent.agent_id).runtime.tools.definitions}
        assert original & set(NAMES) == (
            set(NAMES)
            if writable and enabled
            else {"generated_tree_undo_query"}
            if enabled
            else set()
        )
        if not enabled:
            service.register_generated_tree_undos()
            assert {
                item.name for item in service._handle(agent.agent_id).runtime.tools.definitions
            } == original


async def test_actual_serve_cli_explicit_root_enable_generates_seals_removes_and_reads_same_root(
    tmp_path,
):
    from krim_agent.agents import AgentProfile, AgentProfileUpdate
    from krim_agent.execution import CommandSpec
    from krim_agent.sdk import AgentCommandRequest, TreeExchangeSealRequest, TreePlanRequest

    files = tmp_path / "files"
    files.mkdir()
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
        str(files),
        "--workspace-write",
        "--execution",
        "--tree-exchanges",
        "--generated-tree-undos",
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
            tool_names = [
                "workspace_tree_prepare",
                "command_execute",
                "command_get",
                "directory_source_query",
                "command_propose",
                "directory_source_preview",
                "directory_change",
                "workspace_change",
                "workspace_reconcile",
                "directory_reconcile",
                "tree_exchange_inspect",
                "tree_exchange_seal",
                *NAMES,
            ]
            await asyncio.to_thread(
                client.agent_profile_put,
                AgentProfileUpdate(
                    expected_revision=0,
                    config=AgentProfile(
                        name="root-publisher",
                        description="真实serve原生成根完整消费",
                        workspace_key="local",
                        execution_key="local",
                        tool_names=tool_names,
                        tool_timeout_seconds=30,
                    ),
                ),
            )
            agent = await asyncio.to_thread(client.agent_spawn, "root-publisher")
            command = await asyncio.to_thread(
                client.agent_command_start,
                agent.agent_id,
                AgentCommandRequest(
                    request_key="serve-command",
                    command=CommandSpec(
                        argv=[
                            "/usr/bin/python3",
                            "-I",
                            "-c",
                            "import os;os.makedirs('tree/sub');"
                            "open('tree/sub/data.bin','wb').write(bytes([0,255]))",
                        ]
                    ),
                ),
                confirmed=True,
            )
            completed_command = await asyncio.to_thread(
                client.agent_command_wait,
                agent.agent_id,
                command.command_id,
                timeout_seconds=30,
                confirmed=True,
            )
            assert completed_command.phase == "finished", completed_command
            catalog = await asyncio.to_thread(
                client.agent_command_directory_changes,
                agent.agent_id,
                command.command_id,
            )
            tree_request = TreePlanRequest(
                command_id=command.command_id,
                source_hash=catalog.source_hash,
                root_path="tree",
                request_key="serve-tree",
            )
            plan = await asyncio.to_thread(
                client.agent_workspace_prepare_tree, agent.agent_id, tree_request, confirmed=True
            )
            await asyncio.to_thread(
                client.agent_workspace_start_tree, agent.agent_id, plan.plan_id, confirmed=True
            )
            completed = await asyncio.to_thread(
                client.agent_workspace_wait_tree, agent.agent_id, plan.plan_id, timeout_seconds=30
            )
            assert completed.status == "done", completed
            state = await asyncio.to_thread(
                client.agent_tree_exchange_inspect, agent.agent_id, "tree"
            )
            await asyncio.to_thread(
                client.agent_tree_exchange_seal,
                agent.agent_id,
                TreeExchangeSealRequest(
                    plan_id=plan.plan_id, expected_fingerprint=state.fingerprint
                ),
                confirmed=True,
            )
            request = GeneratedTreeUndoPreviewRequest(
                plan_id=plan.plan_id,
                preserve_path="retained",
                expected_fingerprint=state.fingerprint,
                request_key="serve-root",
            )
            proposal = await asyncio.to_thread(
                client.agent_generated_tree_undo_preview, agent.agent_id, request
            )
            removed = await cli(
                url,
                agent.agent_id,
                "remove-root",
                proposal.move.proposal_id,
                "--revision",
                str(proposal.move.revision),
                "--request-key",
                "serve-remove",
                "--confirm",
            )
            assert removed["operation"]["status"] == "done" and not (files / "tree").exists()
            assert (files / "retained/sub/data.bin").read_bytes() == b"\0\xff"
            assert (await cli(url, agent.agent_id, "operation-by-key", "serve-remove"))[
                "operation"
            ] == removed
            assert (await cli(url, agent.agent_id, "by-key", "serve-root"))["proposal"][
                "root_state"
            ] == "preserved"
    finally:
        if process.returncode is None:
            process.terminate()
            try:
                await asyncio.wait_for(process.communicate(), 15)
            except TimeoutError:
                process.kill()
                await process.communicate()
                raise


def test_cli_invalid_selection_bounded_private_preview_and_serve_scope_never_call_server(
    tmp_path, monkeypatch
):
    def forbidden(*_args, **_kwargs):
        pytest.fail("无效CLI参数不得联系服务器或进入Writer")

    for name in (
        "agent_generated_tree_undo_preview",
        "agent_generated_tree_undo_change",
        "agent_generated_tree_undo_query",
        "agent_generated_tree_undo_cancel",
        "agent_generated_tree_undo_reconcile",
    ):
        monkeypatch.setattr(KrimClient, name, forbidden)
    requests = [tmp_path / "too-large.json", tmp_path / "private.json"]
    requests[0].write_bytes(b"x" * 65537)
    requests[1].write_text(
        json.dumps(
            {
                "plan_id": "p",
                "preserve_path": "retained",
                "expected_fingerprint": "0" * 64,
                "request_key": "key",
                "scope_id": "private",
            }
        )
    )
    choices = [
        ["get", "a", "p", "--limit", "1"],
        ["list", "a", "p"],
        ["preview", "a", "p"],
        ["remove-root", "a", "p"],
        ["cancel", "a", "p", "--request-key", "key"],
        ["operation", "a", "p", "--after", "x"],
        ["preview", "a"],
        *[["preview", "a", "--request", str(path)] for path in requests],
    ]
    for arguments in choices:
        monkeypatch.setattr(sys, "argv", ["krim", "generated-tree-undos", *arguments])
        with pytest.raises(SystemExit) as error:
            cli_main()
        assert error.value.code == 2
    monkeypatch.setattr(sys, "argv", ["krim", "serve", "--generated-tree-undos"])
    with pytest.raises(SystemExit) as error:
        cli_main()
    assert error.value.code == 2
