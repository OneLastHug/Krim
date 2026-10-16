"""工作区能力投影反映原冻结Missing叶，不借全局注册提升旧Agent。"""

import pytest
from test_missing_directory_restore_control import setup

from krim_agent.workspace_missing_directory_restore_control import (
    MISSING_DIRECTORY_RESTORE_STEP_TOOLS,
)


@pytest.mark.parametrize("selection", ["all", "read", "none"])
async def test_workspace_info_exposes_only_original_frozen_missing_leaves(
    runtime, tmp_path, selection
):
    leaves = list(MISSING_DIRECTORY_RESTORE_STEP_TOOLS.values())
    selected = (
        leaves
        if selection == "all"
        else [name for name in leaves if name != MISSING_DIRECTORY_RESTORE_STEP_TOOLS["execute"]]
        if selection == "read"
        else []
    )
    names = ["workspace_directory_checkpoint_capture", *selected]
    service, agent, binding = await setup(runtime, tmp_path, names=names)
    try:
        registry = service._handle(agent.agent_id).runtime.tools
        frozen = tuple(registry.definitions)
        before = await agent.workspace()
        assert before.tool_names == sorted(names), before
        service.register_missing_directory_restores()
        service.register_missing_directory_checkpoint_restore_controls()
        after = await agent.workspace()
        assert after == before and tuple(registry.definitions) == frozen
        assert after.fingerprint == binding.fingerprint and after.available and after.writable
        assert (tmp_path / "tree/nested/nul").read_bytes() == b"\0\xff"
        assert (
            await service.missing_directory_restore_store.list("session:" + agent.session_id) == []
        )
        assert not service.missing_directory_restores._tasks
    finally:
        await service.close()
