"""管理与模型入口都在后置Hook之后校验有限JSON和工作区输出Schema。"""

import os

import pytest

from krim_agent.agents import AgentProfile, AgentProfileUpdate
from krim_agent.application.agents import AgentService
from krim_agent.protocol.domain import ServiceError
from krim_agent.tools import ToolContext
from krim_agent.tools.workspace import ReadFileInput


@pytest.mark.skipif(os.name != "posix", reason="需要POSIX工作区文件边界")
@pytest.mark.parametrize(
    "invalid,code", [(None, "invalid_output_schema"), ("large", "invalid_output")]
)
async def test_management_and_model_share_final_output_validation(runtime, tmp_path, invalid, code):
    (tmp_path / "file.txt").write_text("body", encoding="utf-8")

    async def replace_output(_definition, value, _context):
        return None if invalid is None else {**value, "content": "x" * 70000}

    runtime.tools.add_global_post(replace_output)
    service = AgentService(runtime, runtime.settings)
    try:
        binding = service.register_workspace("files", tmp_path, version="v1")
        await service.profile_put(
            AgentProfileUpdate(
                expected_revision=0,
                config=AgentProfile(
                    name="reader",
                    description="输出边界",
                    workspace_key="files",
                    tool_names=["workspace_read"],
                ),
            )
        )
        view = await service.spawn("reader")
        with pytest.raises(ServiceError) as rejected:
            await service.workspace_read(view.agent_id, ReadFileInput(path="file.txt"))
        assert rejected.value.code == code and rejected.value.status == 502
        tools = service.manager._handles[view.agent_id].runtime.tools
        assert not tools.audit[-1].ok and tools.audit[-1].code == code
        model = await tools.invoke(
            "workspace_read", {"path": "file.txt"}, ToolContext(tools, workspace=binding)
        )
        assert model.is_error and model.code == code
    finally:
        await service.close()
