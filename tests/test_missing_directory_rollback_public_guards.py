"""公开回退的拒绝、当前版本、未知回程与原物理线程收束。"""

import asyncio
import json
import sys
from threading import Event

import httpx
import pytest
from test_missing_baseline_rollback_control import NAMES, action, setup
from test_missing_directory_rollback_public_consumers import cli
from test_tree_move_public_consumers import api_for, tcp_for
from test_workspace_changes import wait_event

from krim_agent.agents import AgentProfile, AgentProfileUpdate
from krim_agent.api import create_app
from krim_agent.cli import main as cli_main
from krim_agent.client import KrimClient
from krim_agent.protocol.domain import ServiceError
from krim_agent.sdk import AgentSession, MissingDirectoryRollbackQuery
from krim_agent.workspace_tree_writer import WorkspaceTreeWriter


async def test_rest_strict_selection_private_body_cross_scope_and_unknown_reconcile(
    runtime, tmp_path, monkeypatch
):
    service, agent, _, source, request = await setup(runtime, tmp_path)
    try:
        proposal = await agent.preview_missing_directory_rollback(request)
        async with api_for(runtime, service, monkeypatch) as (_, api):
            base = f"/v1/agents/{agent.agent_id}/workspace/missing-directory-rollbacks"
            for params in (
                {"kind": "proposal"},
                {"kind": "proposals", "scope_id": "foreign"},
                [("kind", "proposals"), ("kind", "proposals")],
                {"kind": "proposal", "identifier": proposal.move.proposal_id, "limit": 1},
                {"kind": "proposals", "limit": 5},
                {"kind": "by_key", "request_key": "x", "identifier": "other"},
            ):
                response = await api.get(base + "/query", params=params)
                assert response.status_code == 422, response.text
            for method, path, body in (
                ("POST", "/preview", {**request.model_dump(mode="json"), "source_path": "tree"}),
                ("POST", "/change", {**action(proposal).model_dump(), "owner_id": "forged"}),
                ("POST", f"/{proposal.move.proposal_id}/cancel", {}),
                ("POST", "/operations/missing/reconcile", {"operation_id": "other"}),
                ("GET", "/query?kind=proposals", {}),
            ):
                response = await api.request(method, base + path, json=body)
                assert response.status_code == 422, response.text
            assert (
                await api.post(
                    base + "/preview",
                    params=[("confirmed", "true"), ("confirmed", "true")],
                    json=request.model_dump(mode="json"),
                )
            ).status_code == 422
            other = await service.spawn("missing-directory-restore")
            foreign = base.replace(agent.agent_id, other.agent_id)
            assert (
                await api.get(
                    foreign + "/query",
                    params={"kind": "proposal", "identifier": proposal.move.proposal_id},
                )
            ).status_code == 404
            assert (
                await api.post(foreign + "/change", json=action(proposal).model_dump())
            ).status_code == 404
            store = service.manager.tree_moves.store
            settle = store.settle

            async def lost(*_args, **_kwargs):
                raise OSError("原物理效果完成，SQL回程中断")

            monkeypatch.setattr(store, "settle", lost)
            response = await api.post(base + "/change", json=action(proposal).model_dump())
            assert response.status_code == 202, response.text
            unknown = response.json()
            assert unknown["operation"]["status"] == "uncertain"
            assert (tmp_path / "preserved/user-extra").read_bytes() == b"preserve me"
            monkeypatch.setattr(store, "settle", settle)

            def forbidden(*_args, **_kwargs):
                pytest.fail("原unknown核实不允许再次移动/准备")

            monkeypatch.setattr(WorkspaceTreeWriter, "apply", forbidden)
            monkeypatch.setattr(WorkspaceTreeWriter, "prepare", forbidden)
            operation_id = unknown["operation"]["operation_id"]
            resolved = await api.post(base + f"/operations/{operation_id}/reconcile")
            assert resolved.status_code == 202, resolved.text
            assert resolved.json()["operation"]["status"] == "done"
            assert resolved.json()["operation"]["operation_id"] == operation_id
            assert resolved.json()["source_plan_id"] == source.plan_id
            cold = await api.get(
                base + "/query",
                params={"kind": "operation_by_key", "request_key": "rollback-apply"},
            )
            assert cold.json()["operation"] == resolved.json()
    finally:
        await service.close()


@pytest.mark.parametrize("confirmed", ["false", 1])
async def test_sdk_confirmation_must_be_bool_before_policy_or_move(runtime, tmp_path, confirmed):
    service, agent, _, _, request = await setup(runtime, tmp_path)
    try:
        proposal = await agent.preview_missing_directory_rollback(request)

        async def ask(_definition, _context):
            return "ask"

        service._handle(agent.agent_id).runtime.tool_policy = ask
        with pytest.raises(ServiceError) as denied:
            await agent.change_missing_directory_rollback(action(proposal), confirmed=confirmed)
        assert denied.value.status == 422
        assert (tmp_path / "tree/user-extra").read_bytes() == b"preserve me"
        assert not (tmp_path / "preserved").exists()
    finally:
        await service.close()


async def test_sdk_http_ask_deny_plan_and_changed_resource_cannot_be_confirmed_away(
    runtime, tmp_path, monkeypatch
):
    service, agent, _, _, request = await setup(runtime, tmp_path)
    try:
        proposal = await agent.preview_missing_directory_rollback(request)
        handle = service._handle(agent.agent_id)

        async def ask(definition, _context):
            return "allow" if definition.risk == "read" else "ask"

        handle.runtime.tool_policy = ask
        with pytest.raises(ServiceError) as denied:
            await agent.change_missing_directory_rollback(action(proposal))
        assert denied.value.status == 409 and denied.value.code == "approval_required"

        async def deny(_definition, _context):
            return "deny"

        handle.runtime.tool_policy = deny
        async with api_for(runtime, service, monkeypatch) as (_, api):
            base = f"/v1/agents/{agent.agent_id}/workspace/missing-directory-rollbacks"
            response = await api.post(
                base + "/change?confirmed=true", json=action(proposal).model_dump()
            )
            assert response.status_code == 403
        handle.runtime.tool_policy = ask
        await service.profile_put(
            AgentProfileUpdate(
                expected_revision=0,
                config=AgentProfile(
                    name="rollback-plan",
                    description="计划模式拒绝回退写入",
                    mode="plan",
                    workspace_key="files",
                    tool_names=NAMES,
                ),
            )
        )
        planning = AgentSession(service, (await service.spawn("rollback-plan")).agent_id)
        with pytest.raises(ServiceError) as denied:
            await planning.change_missing_directory_rollback(action(proposal), confirmed=True)
        assert denied.value.code == "permission_denied"
        with pytest.raises(ServiceError) as conflict:
            service.register_workspace(
                "files", tmp_path, version="changed", writable=True, default=True
            )
        assert conflict.value.code == "workspace_key_conflict"
        service.resources.disable_workspace("files")
        with pytest.raises(ServiceError) as denied:
            await agent.change_missing_directory_rollback(action(proposal), confirmed=True)
        assert denied.value.status in {403, 409}
        assert (tmp_path / "tree/user-extra").read_bytes() == b"preserve me"
        assert not (tmp_path / "preserved").exists()
    finally:
        await service.close()


async def test_rest_cancel_waits_original_move_worker_without_second_writer(
    runtime, tmp_path, monkeypatch
):
    service, agent, _, _, request = await setup(runtime, tmp_path)
    entered, release, exited = Event(), Event(), Event()
    saved = asyncio.Event()
    worker = cancelling = None
    try:
        proposal = await agent.preview_missing_directory_rollback(request)
        original = WorkspaceTreeWriter.apply

        def held(*args, **kwargs):
            entered.set()
            try:
                assert release.wait(5)
                return original(*args, **kwargs)
            finally:
                exited.set()

        monkeypatch.setattr(WorkspaceTreeWriter, "apply", held)
        cancel = service.manager.tree_moves.store.cancel

        async def observed(*args, **kwargs):
            value = await cancel(*args, **kwargs)
            saved.set()
            return value

        monkeypatch.setattr(service.manager.tree_moves.store, "cancel", observed)
        worker = asyncio.create_task(agent.change_missing_directory_rollback(action(proposal)))
        await wait_event(entered)
        async with api_for(runtime, service, monkeypatch) as (_, api):
            cancelling = asyncio.create_task(
                api.post(
                    f"/v1/agents/{agent.agent_id}/workspace/missing-directory-rollbacks/"
                    f"{proposal.move.proposal_id}/cancel"
                )
            )
            await asyncio.wait_for(saved.wait(), 2)
            assert not cancelling.done() and not exited.is_set()
            release.set()
            result = await worker
            response = await cancelling
            assert response.status_code == 202, response.text
            assert response.json()["rollback_state"] == "cancelled"
            assert result.operation.cancelled and result.operation.status == "rejected"
            assert exited.is_set() and not service.manager.tree_moves._operations
        assert (tmp_path / "tree/user-extra").read_bytes() == b"preserve me"
        assert not (tmp_path / "preserved").exists()
    finally:
        release.set()
        await asyncio.gather(
            *(task for task in (worker, cancelling) if task is not None), return_exceptions=True
        )
        await service.close()


async def test_real_tcp_lost_change_reply_gets_original_key_once_without_post_retry(
    runtime, tmp_path, monkeypatch
):
    service, agent, _, _, request = await setup(runtime, tmp_path)
    calls = []

    class LostReply(httpx.BaseTransport):
        def __init__(self):
            self.original = httpx.HTTPTransport()

        def handle_request(self, request):
            calls.append(request.method)
            response = self.original.handle_request(request)
            if request.method == "POST":
                response.read()
                response.close()
                raise httpx.ReadError("原效果已完成，只有HTTP回程丢失", request=request)
            return response

        def close(self):
            self.original.close()

    try:
        proposal = await agent.preview_missing_directory_rollback(request)
        async with api_for(runtime, service, monkeypatch) as (app, _), tcp_for(app) as url:

            def consume():
                with KrimClient(url) as client:
                    client._http.close()
                    client._http = httpx.Client(base_url=url, transport=LostReply())
                    with pytest.raises(httpx.ReadError):
                        client.agent_missing_directory_rollback_change(
                            agent.agent_id, action(proposal)
                        )
                    return client.agent_missing_directory_rollback_query(
                        agent.agent_id,
                        MissingDirectoryRollbackQuery(
                            kind="operation_by_key", request_key="rollback-apply"
                        ),
                    ).operation

            original = await asyncio.to_thread(consume)
            assert original.operation.status == "done" and calls == ["POST", "GET"]

            def forbidden(*_args, **_kwargs):
                pytest.fail("丢回程后只能读取原事实")

            monkeypatch.setattr(WorkspaceTreeWriter, "prepare", forbidden)
            monkeypatch.setattr(WorkspaceTreeWriter, "apply", forbidden)
            assert (
                await cli(
                    url,
                    agent.agent_id,
                    "query",
                    None,
                    "--kind",
                    "operation_by_key",
                    "--request-key",
                    "rollback-apply",
                )
            )["operation"] == original.model_dump(mode="json")
            assert (tmp_path / "preserved/nested/nul").read_bytes() == b"user\0modified\xff"
    finally:
        await service.close()


@pytest.mark.parametrize("enabled,writable", [(False, True), (True, False), (True, True)])
async def test_factory_opt_in_and_frozen_tools_do_not_implicitly_grant_inspect(
    runtime, tmp_path, enabled, writable
):
    app = create_app(
        runtime=runtime,
        workspace_root=tmp_path,
        workspace_writable=writable,
        missing_directory_rollbacks_enabled=enabled,
    )
    async with app.router.lifespan_context(app):
        service = app.state.agents
        agent = await service.spawn("default")
        actual = {tool.name for tool in service._handle(agent.agent_id).runtime.tools.definitions}
        expected = (
            set(NAMES)
            if writable and enabled
            else {"missing_directory_rollback_query"}
            if enabled
            else set()
        )
        assert actual & set(NAMES) == expected
        assert "tree_move_inspect" not in actual
        service.register_missing_directory_rollbacks()
        assert {
            tool.name for tool in service._handle(agent.agent_id).runtime.tools.definitions
        } == actual


@pytest.mark.parametrize(
    "fault", ["selection", "missing", "traversal", "private", "oversize", "serve"]
)
def test_cli_rejects_invalid_selection_paths_and_private_files_before_network(
    tmp_path, monkeypatch, fault
):
    def forbidden(*_args, **_kwargs):
        pytest.fail("CLI本地无效输入不得调用HTTP")

    for suffix in ("preview", "change", "query", "cancel", "reconcile"):
        monkeypatch.setattr(KrimClient, "agent_missing_directory_rollback_" + suffix, forbidden)
    request = tmp_path / "request.json"
    payload = {
        "plan_id": "p",
        "preserve_path": "../outside" if fault == "traversal" else "preserved",
        "expected_fingerprint": "a" * 64,
        "request_key": "original",
    }
    if fault == "private":
        payload["token"] = "private"
    request.write_text(json.dumps(payload))
    if fault == "oversize":
        request.write_bytes(b" " * 65537)
    options = ["preview", "agent", "--request", str(request)]
    if fault == "selection":
        options = ["query", "agent", "--kind", "by_key", "--identifier", "wrong"]
    elif fault == "missing":
        options = ["reconcile", "agent"]
    arguments = (
        ["serve", "--missing-directory-rollbacks"]
        if fault == "serve"
        else ["missing-directory-rollbacks", *options]
    )
    monkeypatch.setattr(sys, "argv", ["krim", *arguments])
    with pytest.raises(SystemExit) as denied:
        cli_main()
    assert denied.value.code == 2
