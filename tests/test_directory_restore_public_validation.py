"""公开Python确认与未知回程不能用truthy字符串冒领原Ask批准。"""

import pytest
from test_directory_restore_control import execute_request, ready, setup

from krim_agent.protocol.domain import ServiceError


async def test_truthy_false_string_cannot_confirm_original_write(runtime, tmp_path):
    async def policy(definition, _context):
        return "allow" if definition.risk == "read" else "ask"

    service, agent, binding = await setup(runtime, tmp_path, policy=policy)
    try:
        _, planned = await ready(service, agent, binding)
        with pytest.raises(ServiceError) as denied:
            await agent.execute_directory_checkpoint_restore(
                execute_request(planned), confirmed="false"
            )
        assert denied.value.status == 422
        assert (await agent.directory_checkpoint_restore(planned.plan_id)).phase == "ready"
        assert not list(binding.workspace.root.glob("._krim-tx-*"))
    finally:
        await service.close()
