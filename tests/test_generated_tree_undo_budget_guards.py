"""原生成根同类最后期限断口，不能在发生根移动后才报告预算错误。"""

import pytest
from test_generated_tree_undo_tools import action, setup

from krim_agent.protocol.domain import ServiceError


@pytest.mark.parametrize("fault", ["unlimited", "extended"])
async def test_generated_root_last_authority_retains_original_deadline(
    runtime, tmp_path, monkeypatch, fault
):
    service, agent, scope, _, _, _, _, request = await setup(runtime, tmp_path)
    try:
        proposed = await service.generated_tree_undo_preview(agent.agent_id, request)
        contexts = []

        async def observe(_definition, child):
            contexts.append(child)

        service._handle(agent.agent_id).runtime.tools.add_global_pre(observe)
        store = service.manager.tree_moves.store
        original = store.assert_execution

        async def changed(*args):
            await original(*args)
            deadline = contexts[-1].execution_timeouts[-1].deadline
            deadline.reschedule(None if fault == "unlimited" else deadline.when() + 100)

        monkeypatch.setattr(store, "assert_execution", changed)
        with pytest.raises(ServiceError):
            await service.generated_tree_undo_change(agent.agent_id, action(proposed))
        assert (tmp_path / "tree/sub/data.bin").read_bytes() == b"\0\xff"
        assert not (tmp_path / "retained").exists()
        attempt = await store.operation_by_key(scope, "root-remove")
        assert attempt.view.status == "rejected" and not attempt.view.receipt.changed
    finally:
        await service.close()
