"""缺失模型父/私有叶的身份、预算与原实际回程不可由Hook改写。"""

import asyncio
import copy
import json

import pytest
from test_model_missing_directory_restores import PREVIEW, START, phase_setup

from krim_agent.tools import ShortCircuit
from krim_agent.workspace_missing_directory_restore_control import (
    MISSING_DIRECTORY_RESTORE_STEP_TOOLS,
)


@pytest.mark.parametrize(
    "action,target,phase,fault",
    [
        *[
            ("preview", "root", "pre", fault)
            for fault in ("arguments", "run", "backend", "missing-backend", "budget", "short")
        ],
        *[
            ("preview", "leaf", "pre", fault)
            for fault in ("token", "source", "call", "qualifier", "backend", "budget", "short")
        ],
        *[("preview", "leaf", "post", fault) for fault in ("data", "marker", "parent-backend")],
        ("start", "leaf", "pre", "token"),
        ("start", "leaf", "pre", "budget"),
        *[("start", "leaf", "post", fault) for fault in ("data", "budget", "parent-backend")],
    ],
)
async def test_missing_parent_and_leaf_hook_refusals_keep_original_namespace_and_audit(
    runtime, tmp_path, action, target, phase, fault
):
    service, agent, _, provider, planned = await phase_setup(runtime, tmp_path, action)
    root = PREVIEW if action == "preview" else START
    leaf = MISSING_DIRECTORY_RESTORE_STEP_TOOLS["prepare" if action == "preview" else "execute"]
    selected = root if target == "root" else leaf
    parent = []

    def corrupt(context):
        if fault == "arguments":
            context.arguments["request_key"] = "forged"
        elif fault == "run":
            context.run_id = "other-run"
        elif fault == "backend":
            if target == "root":
                context.workspace_checkpoint_controls = None
            else:
                context.workspace_directory_restores = None
        elif fault == "missing-backend":
            context.workspace_missing_directory_restores = None
        elif fault == "token":
            object.__setattr__(context.directory_restore, "token", "0" * 64)
        elif fault == "source":
            object.__setattr__(context.directory_restore.model_source, "call_id", "forged")
        elif fault == "call":
            context.source_call_id = "forged"
        elif fault == "qualifier":
            context.directory_restore_qualify = lambda: None
        elif fault == "budget":
            context.execution_timeouts[-1].deadline.reschedule(
                asyncio.get_running_loop().time() + 60
            )
        elif fault == "parent-backend":
            parent[0].workspace_missing_directory_restores = None

    async def pre(definition, context):
        if definition.name == root:
            parent.append(context)
        if definition.name == selected and phase == "pre":
            if fault == "short":
                return ShortCircuit({})
            corrupt(context)

    async def post(definition, value, context):
        if definition.name == selected and phase == "post":
            if fault in {"budget", "parent-backend"}:
                corrupt(context)
            else:
                value = copy.deepcopy(value)
                value["target_fingerprint"] = "0" * 64
                if fault == "marker":
                    context._directory_restore_response = json.dumps(value, sort_keys=True)
        return value

    registry = service._handle(agent.agent_id).runtime.tools
    registry.add_global_pre(pre)
    registry.add_global_post(post)
    try:
        result = await agent.execute("缺失恢复Hook不可信输入", idempotency_key="missing-hooks")
        assert result.status == "completed", (result, provider.reply)
        final = await service.workspace_missing_directory_restore_by_key(
            agent.agent_id, "boundary-preview"
        )
        if target == "root" and phase == "pre":
            assert final == planned
        else:
            assert final.phase == (
                "uncertain" if action == "start" and phase == "post" else "failed"
            ), final
            assert final.cleanup_pending == (action == "start" and phase == "post")
        if action == "start" and phase == "post":
            assert (tmp_path / "tree/nested/nul").read_bytes() == b"\0\xff"
        else:
            assert not (tmp_path / "tree").exists()
        audits = await runtime.repository.audits(result.run_id)
        assert any(row.name == selected and not row.ok for row in audits), audits
        controller = service.missing_directory_restores
        assert not controller._tasks and not controller._fences
        assert not controller._invocations and not controller._pending
        assert service.workspace_edits.database.engine.sync_engine.pool.checkedout() == 0
    finally:
        await service.close()
