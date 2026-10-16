"""权限/冻结工具与最后原预算、真实输出边界，不放宽副作用安全断言。"""

import copy

import pytest
from test_missing_baseline_rollback_control import action, query, setup

from krim_agent.agents import AgentProfile, AgentProfileUpdate
from krim_agent.application.agents import AgentService
from krim_agent.protocol.domain import ServiceError
from krim_agent.tools import ToolContext, ToolError
from krim_agent.tools.builtins import builtin_registry
from krim_agent.tools.core import ToolDefinition
from krim_agent.tools.missing_directory_rollback import missing_directory_rollback_tools
from krim_agent.workspace_missing_directory_rollback import MissingDirectoryRollbackAction
from krim_agent.workspace_missing_directory_rollback_control import MissingDirectoryRollbackQuery


@pytest.mark.parametrize("fault", ["inactive", "unlimited", "extended"])
async def test_last_store_qualification_cannot_replace_or_extend_original_budget(
    runtime, tmp_path, monkeypatch, fault
):
    service, agent, _, _, request = await setup(runtime, tmp_path)
    store = service.manager.tree_moves.store
    try:
        proposed = await service.missing_directory_rollback_preview(agent.agent_id, request)
        contexts = []

        async def observed(_definition, child):
            contexts.append(child)

        service._handle(agent.agent_id).runtime.tools.add_global_pre(observed)
        original = store.assert_execution

        async def changed(*args):
            await original(*args)
            budget = contexts[-1].execution_timeouts[-1]
            if fault == "inactive":
                budget.active = False
            elif fault == "unlimited":
                budget.deadline.reschedule(None)
            else:
                budget.deadline.reschedule(budget.deadline.when() + 100)

        monkeypatch.setattr(store, "assert_execution", changed)
        with pytest.raises(ServiceError):
            await service.missing_directory_rollback_change(agent.agent_id, action(proposed))
        assert (tmp_path / "tree/user-extra").read_bytes() == b"preserve me", (
            "改变预算后不得移动原目录"
        )
        assert not (tmp_path / "preserved").exists()
        actual = await query(service, agent, "operation_by_key", request_key="rollback-apply")
        assert actual.operation.operation.status == "rejected"
        assert not actual.operation.operation.receipt.changed
    finally:
        await service.close()


async def test_ask_confirmation_deny_and_other_agent_scope_keep_original_tree(runtime, tmp_path):
    service, agent, *_rest, request = await setup(runtime, tmp_path)
    handle = service._handle(agent.agent_id)
    try:

        async def ask(_definition, _context):
            return "ask"

        handle.runtime.tool_policy = ask
        with pytest.raises(ServiceError) as needed:
            await service.missing_directory_rollback_preview(agent.agent_id, request)
        assert needed.value.code == "approval_required"
        proposed = await service.missing_directory_rollback_preview(
            agent.agent_id, request, confirmed=True
        )
        with pytest.raises(ServiceError) as needed:
            await service.missing_directory_rollback_change(agent.agent_id, action(proposed))
        assert needed.value.code == "approval_required"

        async def deny(_definition, _context):
            return "deny"

        handle.runtime.tool_policy = deny
        with pytest.raises(ServiceError) as denied:
            await service.missing_directory_rollback_change(
                agent.agent_id, action(proposed), confirmed=True
            )
        assert denied.value.code == "permission_denied"
        other = await service.spawn("missing-directory-restore")
        with pytest.raises(ServiceError):
            await service.missing_directory_rollback_query(
                other.agent_id,
                MissingDirectoryRollbackQuery(
                    kind="proposal", identifier=proposed.move.proposal_id
                ),
            )
        assert (tmp_path / "tree/user-extra").read_bytes() == b"preserve me"
        assert not (tmp_path / "preserved").exists()
    finally:
        await service.close()


async def test_registration_preserves_old_agent_and_conflict_is_atomic(runtime, tmp_path):
    service = AgentService(runtime, runtime.settings)
    service.register_workspace("files", tmp_path, version="1", writable=True)
    agent = await service.spawn("default")
    before = tuple(service._handle(agent.agent_id).runtime.tools.definitions)
    try:
        service.register_missing_directory_rollbacks()
        service.register_missing_directory_rollbacks()
        assert tuple(service._handle(agent.agent_id).runtime.tools.definitions) == before
        with pytest.raises(ServiceError):
            await service.missing_directory_rollback_query(
                agent.agent_id, MissingDirectoryRollbackQuery(kind="proposals")
            )
        other_runtime = copy.copy(runtime)
        other_runtime.tools = builtin_registry()
        conflict = missing_directory_rollback_tools()[2]
        other_runtime.tools.register(
            ToolDefinition(conflict.name, "已有定义", conflict.parameters, lambda *_: None, "write")
        )
        other = AgentService(other_runtime, runtime.settings)
        try:
            with pytest.raises(ValueError):
                other.register_missing_directory_rollbacks()
            names = {item.name for item in missing_directory_rollback_tools()}
            assert names & {item.name for item in other_runtime.tools.definitions} == {
                conflict.name
            }
        finally:
            await other.close()
    finally:
        await service.close()


@pytest.mark.parametrize(
    "mode,writable,expected",
    [("plan", True, "permission_denied"), ("agent", False, "workspace_read_only")],
)
async def test_plan_and_readonly_reject_before_source_or_move(
    runtime, tmp_path, mode, writable, expected
):
    service = AgentService(runtime, runtime.settings)
    binding = service.register_workspace("files", tmp_path, version="1", writable=writable)
    service.register_missing_directory_rollbacks()
    names = [item.name for item in missing_directory_rollback_tools()]
    await service.profile_put(
        AgentProfileUpdate(
            expected_revision=0,
            config=AgentProfile(
                name="rollback-mode",
                description="原模式拒绝",
                workspace_key="files",
                mode=mode,
                tool_names=names if writable else ["missing_directory_rollback_query"],
            ),
        )
    )
    agent = await service.spawn("rollback-mode")
    try:
        if writable:
            with pytest.raises(ServiceError) as denied:
                await service.missing_directory_rollback_change(
                    agent.agent_id,
                    MissingDirectoryRollbackAction(
                        proposal_id="missing",
                        action="return_to_absence",
                        expected_revision=0,
                        request_key="plan-write",
                    ),
                    confirmed=True,
                )
        else:
            context = ToolContext(
                runtime.tools,
                topic_id=agent.topic_id,
                agent_id=agent.agent_id,
                session_id=agent.session_id,
                workspace=binding,
                tree_moves=service.manager.tree_moves,
                origin="host",
            )
            with pytest.raises(ToolError) as denied:
                await runtime.tools.call_tool(
                    "missing_directory_rollback_preview",
                    {
                        "plan_id": "missing",
                        "preserve_path": "preserved",
                        "expected_fingerprint": "a" * 64,
                        "request_key": "readonly",
                    },
                    context,
                )
        assert denied.value.code == expected
    finally:
        await service.close()


async def test_closed_session_disabled_resource_history_has_no_new_writer(
    runtime, tmp_path, monkeypatch
):
    from krim_agent.workspace_tree_writer import WorkspaceTreeWriter

    service, agent, *_rest, request = await setup(runtime, tmp_path)
    try:
        proposed = await service.missing_directory_rollback_preview(agent.agent_id, request)
        state = await agent.state()
        await agent.close(expected_revision=state.revision)
        service.disable_workspace("files")

        def forbidden(*_args, **_kwargs):
            pytest.fail("关闭后的历史不能重新准备或发布")

        monkeypatch.setattr(WorkspaceTreeWriter, "prepare", forbidden)
        monkeypatch.setattr(WorkspaceTreeWriter, "apply", forbidden)
        actual = await query(service, agent, "proposal", identifier=proposed.move.proposal_id)
        assert actual.proposal == proposed
    finally:
        await service.close()


@pytest.mark.parametrize(
    "field",
    [
        "directory_checkpoint_capture",
        "directory_checkpoint_qualify",
        "directory_restore",
        "directory_restore_qualify",
    ],
)
async def test_other_directory_transaction_context_cannot_open_rollback(
    runtime, tmp_path, monkeypatch, field
):
    from krim_agent.workspace_tree_writer import WorkspaceTreeWriter

    service, agent, _, _, request = await setup(runtime, tmp_path)
    handle = service._handle(agent.agent_id)
    try:
        context = ToolContext(
            handle.runtime.tools,
            topic_id=handle.topic_id,
            agent_id=agent.agent_id,
            session_id=agent.session_id,
            workspace=handle.definition.workspace,
            tree_moves=service.manager.tree_moves,
            policy=handle.runtime.tool_policy,
            origin="host",
            timeout=1,
        )
        setattr(context, field, (lambda: None) if field.endswith("qualify") else object())

        def forbidden(*_args, **_kwargs):
            pytest.fail("其他目录事务围栏不能开启新的回退准备")

        monkeypatch.setattr(WorkspaceTreeWriter, "prepare", forbidden)
        with pytest.raises(ToolError):
            await context.registry.call_tool(
                "missing_directory_rollback_preview", request.model_dump(), context
            )
        assert (tmp_path / "tree/user-extra").read_bytes() == b"preserve me"
    finally:
        await service.close()


async def test_unfinished_source_rejection_keeps_original_live_ready_plan_usable(runtime, tmp_path):
    from test_missing_directory_restore_control import execute_request, ready
    from test_missing_directory_restore_control import setup as missing_setup

    from krim_agent.workspace_missing_directory_restore_control import (
        MISSING_DIRECTORY_RESTORE_STEP_TOOLS,
    )
    from krim_agent.workspace_missing_directory_rollback import MissingDirectoryRollbackPreview

    definitions = missing_directory_rollback_tools()
    runtime.tools.register_many(*definitions)
    service, agent, binding = await missing_setup(
        runtime,
        tmp_path,
        names=[
            "workspace_directory_checkpoint_capture",
            *MISSING_DIRECTORY_RESTORE_STEP_TOOLS.values(),
            *[item.name for item in definitions],
        ],
    )
    service.register_missing_directory_rollbacks()
    try:
        _, planned = await ready(service, agent, binding)
        with pytest.raises(ServiceError):
            await service.missing_directory_rollback_preview(
                agent.agent_id,
                MissingDirectoryRollbackPreview(
                    plan_id=planned.plan_id,
                    preserve_path="preserved",
                    expected_fingerprint="a" * 64,
                    request_key="incomplete-source",
                ),
            )
        actual = await service.workspace_missing_directory_restore(agent.agent_id, planned.plan_id)
        assert actual == planned and actual.phase == "ready", (
            "拒绝回退不能中断仍持有原观察的恢复计划"
        )
        await service.workspace_start_missing_directory_restore(
            agent.agent_id, execute_request(planned)
        )
        finished = await service.workspace_missing_directory_restore_wait(
            agent.agent_id, planned.plan_id, timeout_seconds=10
        )
        assert finished.phase == "finished"
        assert (tmp_path / "tree/nested/nul").read_bytes() == b"\0\xff"
    finally:
        await service.close()


@pytest.mark.parametrize("invalid", ["false", 1])
async def test_confirmation_requires_actual_boolean_before_new_write(runtime, tmp_path, invalid):
    service, agent, *_rest, request = await setup(runtime, tmp_path)
    try:
        proposed = await service.missing_directory_rollback_preview(agent.agent_id, request)

        async def ask(_definition, _context):
            return "ask"

        service._handle(agent.agent_id).runtime.tool_policy = ask
        with pytest.raises(ServiceError) as denied:
            await service.missing_directory_rollback_change(
                agent.agent_id, action(proposed), confirmed=invalid
            )
        assert denied.value.status == 422
        assert (tmp_path / "tree/user-extra").read_bytes() == b"preserve me"
        assert not (tmp_path / "preserved").exists()
    finally:
        await service.close()
