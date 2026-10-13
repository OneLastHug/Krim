"""原seal只读get/工具/全部管理消费；旧六Schema不变、0物理观察或写入。"""

import asyncio
import subprocess
from pathlib import Path

import httpx
import pytest
from test_completed_tree_origins import complete
from test_tree_exchange_public_consumers import cli
from test_tree_exchange_tools import NAMES, context, counts, setup
from test_tree_move_public_consumers import api_for, tcp_for

from krim_agent.client import ClientError, KrimClient
from krim_agent.persistence.database import WorkspaceTreeSealRow
from krim_agent.protocol.domain import ServiceError
from krim_agent.sdk import CompletedTreeOriginLookupView, TreeExchangeSealRequest
from krim_agent.tools import ShortCircuit, ToolError
from krim_agent.tools.tree_exchanges import tree_exchange_tools
from krim_agent.workspace_tree_exchange import WorkspaceTreeExchangeWriter
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshotReader


def forbid(monkeypatch):
    def denied(*_args, **_kwargs):
        pytest.fail("只读原seal不能重观察/Writer/封存或生成")

    for name in ("capture", "verify"):
        monkeypatch.setattr(WorkspaceTreeSnapshotReader, name, denied)
    for name in ("prepare", "apply", "verify_keep"):
        monkeypatch.setattr(WorkspaceTreeExchangeWriter, name, denied)


async def sealed(runtime, root):
    runtime.tools.register_many(*tree_exchange_tools())
    service, agent, scope, trees, plan, binding, snapshot, outputs = await complete(
        runtime, root, extra_names=NAMES
    )
    service.register_tree_exchanges()
    view = await agent.seal_tree_exchange(
        TreeExchangeSealRequest(plan_id=plan.plan_id, expected_fingerprint=snapshot.fingerprint)
    )
    return service, agent, scope, plan, view


async def test_sdk_rest_client_cli_original_seal_changed_and_unavailable_zero_reobserve(
    runtime, tmp_path, monkeypatch
):
    service, agent, scope, plan, original = await sealed(runtime, tmp_path)
    try:
        before = await counts(service.workspace_edits.database)
        (tmp_path / "tree/sub/data.bin").write_bytes(b"changed")
        forbid(monkeypatch)

        async def denied(*_args, **_kwargs):
            pytest.fail("原查询不能隐式seal")

        monkeypatch.setattr(service.manager.tree_exchanges.store.seals, "seal", denied)
        sdk = await agent.query_tree_exchange_seal(plan.plan_id)
        assert sdk == CompletedTreeOriginLookupView(plan_id=plan.plan_id, origin=original)
        service.disable_workspace("files")
        assert await agent.query_tree_exchange_seal(plan.plan_id) == sdk
        async with api_for(runtime, service, monkeypatch) as (app, api):
            route = f"/v1/agents/{agent.agent_id}/workspace/tree-exchanges/seals/{plan.plan_id}"
            response = await api.get(route)
            assert response.status_code == 200 and response.json() == sdk.model_dump(mode="json")
            async with tcp_for(app) as url:
                with KrimClient(url) as client:
                    assert (
                        await asyncio.to_thread(
                            client.agent_tree_exchange_seal_query, agent.agent_id, plan.plan_id
                        )
                        == sdk
                    )
                assert await cli(url, agent.agent_id, "seal-query", plan.plan_id) == sdk.model_dump(
                    mode="json"
                )
        assert await counts(service.workspace_edits.database) == before == [0, 0, 0, 0, 0, 1]
    finally:
        await service.close()


async def test_missing_seal_returns_original_plan_null_without_observation_or_write(
    runtime, tmp_path, monkeypatch
):
    service, agent, _ = await setup(runtime, tmp_path)
    try:
        forbid(monkeypatch)
        assert await agent.query_tree_exchange_seal("missing") == CompletedTreeOriginLookupView(
            plan_id="missing", origin=None
        )
        async with api_for(runtime, service, monkeypatch) as (_, api):
            assert (
                await api.get(f"/v1/agents/{agent.agent_id}/workspace/tree-exchanges/seals/missing")
            ).json() == {"plan_id": "missing", "origin": None}
        assert await counts(service.workspace_edits.database) == [0, 0, 0, 0, 0, 0]
    finally:
        await service.close()


@pytest.mark.parametrize("fault", ["scope", "resource", "proof", "bytes"])
async def test_original_seal_scope_resource_or_material_damage_never_projects_new_origin(
    runtime, tmp_path, monkeypatch, fault
):
    service, agent, scope, plan, original = await sealed(runtime, tmp_path)
    try:
        async with service.workspace_edits.database.sessions.begin() as sql:
            row = await sql.get(WorkspaceTreeSealRow, plan.plan_id)
            if fault == "scope":
                row.scope_id = "session:other"
            elif fault == "resource":
                row.resource_fingerprint = "a" * 64
            elif fault == "proof":
                row.proof = {}
            else:
                row.payload = b"broken"
        forbid(monkeypatch)
        with pytest.raises(ServiceError):
            await agent.query_tree_exchange_seal(plan.plan_id)
    finally:
        await service.close()


@pytest.mark.parametrize("fault", ["redirect", "backend", "short", "post", "marker"])
async def test_seal_query_keeps_original_registry_identity_parameters_and_canonical_reply(
    runtime, tmp_path, fault
):
    service, agent, _ = await setup(runtime, tmp_path)
    try:
        ctx = await context(service, agent)

        async def before(_definition, child):
            if fault == "redirect":
                child.arguments["plan_id"] = "other"
            elif fault == "backend":
                child.tree_exchanges = None
            elif fault == "short":
                return ShortCircuit({"plan_id": "missing", "origin": None})

        async def after(_definition, value, child):
            if fault == "post":
                return {"plan_id": "other", "origin": None}
            if fault == "marker":
                child._tree_exchange_response = ("seal_query", "forged")
            return value

        ctx.registry.add_global_pre(before)
        ctx.registry.add_global_post(after)
        with pytest.raises(ToolError) as refused:
            await ctx.registry.call_tool("tree_exchange_seal_query", {"plan_id": "missing"}, ctx)
        assert refused.value.code in {
            "workspace_tree_exchange_context",
            "workspace_tree_exchange_unverified",
        }
        assert ctx.registry.audit[-1].call_id == ctx.call_id
        assert await counts(service.workspace_edits.database) == [0, 0, 0, 0, 0, 0]
    finally:
        await service.close()


@pytest.mark.parametrize(
    "params",
    [[("confirmed", "false"), ("confirmed", "true")], {"scope_id": "other"}, {"plan_id": "other"}],
)
async def test_rest_seal_get_rejects_duplicate_unknown_private_parameters(
    runtime, tmp_path, monkeypatch, params
):
    service, agent, _ = await setup(runtime, tmp_path)
    try:
        async with api_for(runtime, service, monkeypatch) as (_, api):
            assert (
                await api.get(
                    f"/v1/agents/{agent.agent_id}/workspace/tree-exchanges/seals/missing",
                    params=params,
                )
            ).status_code == 422
    finally:
        await service.close()


@pytest.mark.parametrize("fault", ["outer-plan", "inner-plan", "scope"])
async def test_client_refuses_schema_valid_but_cross_original_plan_or_scope(
    runtime, tmp_path, fault
):
    service, agent, scope, plan, original = await sealed(runtime, tmp_path)
    try:
        payload = CompletedTreeOriginLookupView(plan_id=plan.plan_id, origin=original).model_dump(
            mode="json"
        )
        if fault == "outer-plan":
            payload["plan_id"] = "other"
        elif fault == "inner-plan":
            payload["origin"]["plan_id"] = "other"
        else:
            payload["origin"]["scope_id"] = "session:other"
        with KrimClient(
            "http://127.0.0.1",
            transport=httpx.MockTransport(lambda _request: httpx.Response(200, json=payload)),
        ) as client:
            with pytest.raises(ClientError) as refused:
                client.agent_tree_exchange_seal_query(agent.agent_id, plan.plan_id)
            assert refused.value.code == "workspace_tree_exchange_response"
    finally:
        await service.close()


async def test_original_six_tool_schemas_from_published_commit_are_exactly_unchanged():
    previous = await asyncio.to_thread(
        subprocess.check_output,
        ["git", "show", "09367e2:src/krim_agent/tools/tree_exchanges.py"],
        text=True,
        cwd=Path(__file__).parents[1],
    )
    # 发布源码只依赖原六请求/View；加载原factory核实真实Schema而非字符串片段。
    environment = {
        "__name__": "krim_agent.tools.original_exchanges",
        "__package__": "krim_agent.tools",
    }
    exec(compile(previous, "09367e2-tree-exchanges.py", "exec"), environment)
    old = {
        item.name: item.schema().model_dump(mode="json")
        for item in environment["tree_exchange_tools"]()
    }
    current = {item.name: item.schema().model_dump(mode="json") for item in tree_exchange_tools()}
    assert len(old) == 6 and len(current) == 7 and {name: current[name] for name in old} == old
