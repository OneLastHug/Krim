"""私有材料先分类，不以真假值降级；原绑定文件限额继续约束保存。"""

import pytest
from test_directory_checkpoint_materials import count
from test_directory_checkpoint_materials import material_context as material_context

from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace import Workspace, WorkspaceBinding


async def test_falsey_shaped_material_is_rejected_without_reading_attributes(material_context):
    store, scope, binding, _snapshot, request = material_context
    observed = []

    class Falsey:
        def __bool__(self):
            observed.append("truth")
            return False

        def __getattr__(self, name):
            observed.append(name)
            raise AssertionError("坏材料不得被当作缺省或读取私有字段")

    with pytest.raises(ServiceError) as invalid:
        await store.save_captured(scope, binding, request, Falsey())
    assert invalid.value.code == "workspace_directory_checkpoint_material"
    assert observed == [] and await count(store) == 0


async def test_actual_narrower_workspace_file_limit_refuses_supplied_larger_material(
    material_context,
):
    store, scope, binding, snapshot, request = material_context
    with Workspace(binding.workspace.root, max_file_bytes=1) as workspace:
        limited = WorkspaceBinding("files", "limited", workspace, "b" * 64)
        with pytest.raises(ServiceError) as denied:
            await store.save_captured(scope, limited, request, snapshot)
        assert denied.value.code == "workspace_directory_checkpoint_limit"
    assert await count(store) == 0
