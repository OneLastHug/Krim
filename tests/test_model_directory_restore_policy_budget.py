"""权限策略修改真实deadline也必须在原副作用准入之前拒绝。"""

import asyncio

import pytest
from test_model_directory_restores import PREVIEW, START, phase_setup

from krim_agent.workspace_directory_restore_control import DIRECTORY_RESTORE_STEP_TOOLS


@pytest.mark.parametrize(
    "action,level",
    [(action, level) for action in ("preview", "start") for level in ("root", "leaf")],
)
async def test_policy_cannot_extend_actual_original_deadline_before_admission_or_io(
    runtime, tmp_path, action, level
):
    service, agent, binding, provider, original = await phase_setup(runtime, tmp_path, action)
    selected = (
        (PREVIEW if action == "preview" else START)
        if level == "root"
        else DIRECTORY_RESTORE_STEP_TOOLS["prepare" if action == "preview" else "execute"]
    )
    changed = []

    async def policy(definition, context):
        if definition.name == selected:
            context.execution_timeouts[-1].deadline.reschedule(
                asyncio.get_running_loop().time() + 60
            )
            changed.append(True)
        return "allow"

    service._handle(agent.agent_id).runtime.tool_policy = policy
    try:
        result = await agent.execute("策略不得延长原deadline", idempotency_key="policy-budget")
        final = await agent.directory_checkpoint_restore_by_key("boundary-preview")
        assert result.status == "completed" and changed == [True], (result, provider.reply)
        if level == "root":
            assert final == original, final
        else:
            assert (
                final.phase == "failed"
                and final.error_code == "workspace_directory_restore_context"
            ), final
            assert not final.cleanup_pending
        assert (binding.workspace.root / "tree/nested/nul").read_bytes() == b"current\x80\0"
        audits = await runtime.repository.audits(result.run_id)
        expected = (
            "workspace_checkpoint_model_budget"
            if level == "root"
            else "workspace_directory_restore_context"
        )
        assert any(
            row.name == selected and not row.ok and row.code == expected for row in audits
        ), audits
        assert not service.directory_restores._tasks and not service.directory_restores._pending
    finally:
        await service.close()
