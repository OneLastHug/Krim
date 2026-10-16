"""真实Host/离线模型消费独立缺失来源，经原Registry与原Move发布。"""

import json

import pytest
from sqlalchemy import select
from test_missing_directory_restore_control import execute_request, ready
from test_missing_directory_restore_control import setup as missing_setup

import krim_agent.workspace_tree_writer as physical
from krim_agent.persistence.database import WorkspaceTreeMoveModelSourceRow
from krim_agent.protocol.domain import ServiceError
from krim_agent.providers import ModelText, ModelToolCall
from krim_agent.tools import ShortCircuit
from krim_agent.tools.missing_directory_rollback import missing_directory_rollback_tools
from krim_agent.workspace_missing_directory_restore_control import (
    MISSING_DIRECTORY_RESTORE_STEP_TOOLS,
)
from krim_agent.workspace_missing_directory_rollback import (
    MissingDirectoryRollbackAction,
    MissingDirectoryRollbackAttemptView,
    MissingDirectoryRollbackPreview,
    MissingDirectoryRollbackView,
)
from krim_agent.workspace_missing_directory_rollback_control import (
    MissingDirectoryRollbackCancel,
    MissingDirectoryRollbackQuery,
    MissingDirectoryRollbackQueryResult,
)
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshotReader
from krim_agent.workspace_tree_writer import WorkspaceTreeWriter

NAMES = [item.name for item in missing_directory_rollback_tools()]


async def setup(runtime, root, *, policy=None, persistent=False):
    runtime.tools.register_many(*missing_directory_rollback_tools())
    service, agent, binding = await missing_setup(
        runtime,
        root,
        policy=policy,
        persistent=persistent,
        names=[
            "workspace_directory_checkpoint_capture",
            *MISSING_DIRECTORY_RESTORE_STEP_TOOLS.values(),
            *NAMES,
        ],
    )
    service.register_missing_directory_rollbacks()
    service.register_missing_directory_rollbacks()
    try:
        _, planned = await ready(service, agent, binding)
        await service.workspace_start_missing_directory_restore(
            agent.agent_id, execute_request(planned)
        )
        finished = await service.workspace_missing_directory_restore_wait(
            agent.agent_id, planned.plan_id, timeout_seconds=10
        )
        assert finished.phase == "finished", finished
        (root / "tree/nested/nul").write_bytes(b"user\0modified\xff")
        (root / "tree/user-extra").write_bytes(b"preserve me")
        current = WorkspaceTreeSnapshotReader(binding.workspace).capture("tree")
        request = MissingDirectoryRollbackPreview(
            plan_id=finished.plan_id,
            preserve_path="preserved",
            expected_fingerprint=current.fingerprint,
            request_key="rollback-preview",
        )
        return service, agent, binding, finished, request
    except BaseException:
        await service.close()
        raise


def action(view, kind="return_to_absence", key="rollback-apply"):
    return MissingDirectoryRollbackAction(
        proposal_id=view.move.proposal_id,
        action=kind,
        expected_revision=view.move.revision,
        request_key=key,
    )


async def query(service, agent, kind, **kwargs):
    return await service.missing_directory_rollback_query(
        agent.agent_id, MissingDirectoryRollbackQuery(kind=kind, **kwargs)
    )


async def test_host_preserves_current_tree_keep_reverse_and_cold_keys_zero_replay(
    runtime, tmp_path, monkeypatch
):
    service, agent, binding, source, request = await setup(runtime, tmp_path)
    original, calls = physical.rename_at, []

    def counted(*args, **kwargs):
        calls.append(True)
        return original(*args, **kwargs)

    monkeypatch.setattr(physical, "rename_at", counted)
    try:
        proposal = await service.missing_directory_rollback_preview(agent.agent_id, request)
        assert (
            proposal.source_plan_id == source.plan_id
            and proposal.source_operation_id == source.operation_id
        )
        attempt = await service.missing_directory_rollback_change(agent.agent_id, action(proposal))
        assert attempt.operation.status == "done" and len(calls) == 1
        assert not (tmp_path / "tree").exists()
        assert (tmp_path / "preserved/nested/nul").read_bytes() == b"user\0modified\xff"
        current = (
            await query(service, agent, "proposal", identifier=proposal.move.proposal_id)
        ).proposal
        kept = await service.missing_directory_rollback_change(
            agent.agent_id, action(current, "keep", "keep")
        )
        assert kept.operation.status == "done" and not kept.operation.receipt.changed
        current = (await query(service, agent, "by_key", request_key=request.request_key)).proposal
        reversed = await service.missing_directory_rollback_change(
            agent.agent_id, action(current, "restore_tree", "reverse")
        )
        assert reversed.operation.status == "done" and len(calls) == 2
        assert (tmp_path / "tree/user-extra").read_bytes() == b"preserve me"
        assert not (tmp_path / "preserved").exists()
        assert (
            await service.workspace_missing_directory_restore(agent.agent_id, source.plan_id)
        ) == source

        def forbidden(*_args, **_kwargs):
            pytest.fail("冷原键/历史禁止Writer或读取新树")

        monkeypatch.setattr(WorkspaceTreeWriter, "prepare", forbidden)
        monkeypatch.setattr(WorkspaceTreeWriter, "apply", forbidden)
        monkeypatch.setattr(WorkspaceTreeSnapshotReader, "capture", forbidden)
        replay = await service.missing_directory_rollback_preview(agent.agent_id, request)
        assert replay.rollback_state == "restored"
        assert (
            await service.missing_directory_rollback_change(agent.agent_id, action(proposal))
        ).operation == attempt.operation
        history = await query(service, agent, "history", identifier=proposal.move.proposal_id)
        assert len(history.history.items) == 3
        assert (
            await query(service, agent, "operation_by_key", request_key="reverse")
        ).operation == reversed
        assert len(calls) == 2
    finally:
        await service.close()


class Provider:
    name = "offline-missing-baseline-rollback"

    def __init__(self, request):
        self.calls, self.request, self.proposal, self.attempt = 0, request, None, None

    async def stream(self, messages, tools):
        self.calls += 1
        assert set(NAMES) <= {tool.name for tool in tools}
        if self.calls == 1:
            yield ModelToolCall(
                id="original-preview", name=NAMES[0], arguments=self.request.model_dump()
            )
        elif self.calls == 2:
            self.proposal = MissingDirectoryRollbackView.model_validate_json(messages[-1].content)
            yield ModelToolCall(
                id="original-change",
                name="missing_directory_rollback_change",
                arguments=action(self.proposal).model_dump(),
            )
        elif self.calls == 3:
            self.attempt = MissingDirectoryRollbackAttemptView.model_validate_json(
                messages[-1].content
            )
            assert self.attempt.operation.status == "done"
            yield ModelToolCall(
                id="original-query",
                name="missing_directory_rollback_query",
                arguments={"kind": "proposal", "identifier": self.proposal.move.proposal_id},
            )
        else:
            result = MissingDirectoryRollbackQueryResult.model_validate_json(messages[-1].content)
            assert result.proposal.rollback_state == "preserved"
            yield ModelText(text="原缺失基线回退已完整保留")


async def test_real_offline_model_run_call_sources_and_host_cold_query(
    runtime, tmp_path, monkeypatch
):
    service, agent, binding, source, request = await setup(runtime, tmp_path)
    provider = Provider(request)
    service._handle(agent.agent_id).runtime.provider = provider
    try:
        result = await agent.execute("保留当前整树并回到缺失", idempotency_key="model-rollback")
        assert result.status == "completed" and provider.calls == 4, result
        async with runtime.repository.database.sessions() as sql:
            rows = list(await sql.scalars(select(WorkspaceTreeMoveModelSourceRow)))
        assert {(row.run_id, row.call_id, row.kind) for row in rows} == {
            (result.run_id, "original-preview", "preview"),
            (result.run_id, "original-change", "apply"),
        }
        assert (tmp_path / "preserved/nested/nul").read_bytes() == b"user\0modified\xff"

        def forbidden(*_args, **_kwargs):
            pytest.fail("Host冷查询不得重放模型/Writer")

        monkeypatch.setattr(provider, "stream", forbidden)
        monkeypatch.setattr(WorkspaceTreeWriter, "apply", forbidden)
        actual = await query(service, agent, "by_key", request_key=request.request_key)
        assert actual.proposal.source_plan_id == source.plan_id
        assert (
            await query(
                service, agent, "operation", identifier=provider.attempt.operation.operation_id
            )
        ).operation == provider.attempt
        assert provider.calls == 4
    finally:
        await service.close()


@pytest.mark.parametrize("fault", ["arguments", "source", "budget", "short", "response", "marker"])
async def test_hooks_cannot_replace_source_budget_parameters_or_backend_response(
    runtime, tmp_path, fault
):
    service, agent, *_ = await setup(runtime, tmp_path)
    registry = service._handle(agent.agent_id).runtime.tools
    try:

        async def pre(_definition, child):
            if fault == "arguments":
                child.arguments["limit"] = 1
            elif fault == "source":
                child.call_id = "forged"
            elif fault == "budget":
                child.execution_timeouts[-1].deadline.reschedule(None)
            elif fault == "short":
                return ShortCircuit(
                    {"kind": "proposals", "proposals": {"items": [], "next_after": None}}
                )

        async def post(_definition, value, child):
            if fault == "response":
                value["kind"] = "history"
            elif fault == "marker":
                child._tree_move_response = (
                    "missing_rollback_query",
                    json.dumps(value, sort_keys=True) + " ",
                )
            return value

        registry.add_global_pre(pre)
        registry.add_global_post(post)
        with pytest.raises(ServiceError):
            await query(service, agent, "proposals")
    finally:
        await service.close()


async def test_policy_cannot_extend_budget_before_any_new_preview_or_source(
    runtime, tmp_path, monkeypatch
):
    async def policy(definition, context):
        if definition.name.startswith("missing_directory_rollback_"):
            context.execution_timeouts[-1].deadline.reschedule(None)
        return "allow"

    service, agent, _, _, request = await setup(runtime, tmp_path, policy=policy)
    try:

        def forbidden(*_args, **_kwargs):
            pytest.fail("策略改预算后不能读取或准入原树")

        monkeypatch.setattr(WorkspaceTreeWriter, "prepare", forbidden)
        with pytest.raises(ServiceError) as denied:
            await service.missing_directory_rollback_preview(agent.agent_id, request)
        assert denied.value.code == "workspace_missing_directory_rollback_budget"
        assert (tmp_path / "tree/user-extra").read_bytes() == b"preserve me"
    finally:
        await service.close()


async def test_cancel_closes_original_preview_without_moving_tree(runtime, tmp_path):
    service, agent, _, _, request = await setup(runtime, tmp_path)
    try:
        proposed = await service.missing_directory_rollback_preview(agent.agent_id, request)
        actual = await service.missing_directory_rollback_cancel(
            agent.agent_id, MissingDirectoryRollbackCancel(proposal_id=proposed.move.proposal_id)
        )
        assert actual.rollback_state == "cancelled"
        with pytest.raises(ServiceError):
            await service.missing_directory_rollback_change(agent.agent_id, action(proposed))
        assert (tmp_path / "tree/user-extra").read_bytes() == b"preserve me"
    finally:
        await service.close()
