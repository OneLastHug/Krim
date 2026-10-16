"""独立私有叶协议/原子注册及新旧家族混用，输入不接私有材料。"""

from dataclasses import replace

import pytest
from pydantic import ValidationError

from krim_agent.application.agents import AgentService
from krim_agent.tools import ToolContext
from krim_agent.tools.directory_restore import directory_restore_tools
from krim_agent.tools.missing_directory_restore import missing_directory_restore_tools
from krim_agent.workspace_directory_restore import DirectoryRestoreFence
from krim_agent.workspace_directory_restore_control import (
    DIRECTORY_RESTORE_STEP_TOOLS,
    DirectoryRestoreStepView,
)
from krim_agent.workspace_missing_directory_restore import MissingDirectoryRestoreFence
from krim_agent.workspace_missing_directory_restore_control import (
    MISSING_DIRECTORY_RESTORE_STEP_TOOLS,
    MissingDirectoryRestoreStepInput,
    MissingDirectoryRestoreStepView,
)


@pytest.mark.parametrize(
    "field", ["root_path", "token", "payload", "observation_id", "source", "kind"]
)
def test_leaf_input_does_not_accept_uploaded_private_material_or_redirect(field):
    with pytest.raises(ValidationError):
        MissingDirectoryRestoreStepInput.model_validate(
            {"plan_id": "original-plan", "operation_id": "original-operation", field: "forbidden"}
        )


def actual_step():
    return {
        "plan_id": "original-plan",
        "operation_id": "original-operation",
        "source": "workspace_missing_directory_restore_step",
        "kind": "execute",
        "root_path": "tree",
        "target_fingerprint": "a" * 64,
        "before_absence_fingerprint": "b" * 64,
        "receipt": {
            "root_path": "tree",
            "status": "done",
            "before_absence_fingerprint": "b" * 64,
            "target_fingerprint": "a" * 64,
            "after_fingerprint": "c" * 64,
            "changed": True,
            "error_code": None,
            "recovery_required": False,
        },
    }


@pytest.mark.parametrize(
    "fault", ["source", "baseline", "target", "root", "operation", "fake-backup"]
)
def test_actual_step_cannot_borrow_old_tree_protocol_or_other_material(fault):
    value = actual_step()
    assert MissingDirectoryRestoreStepView.model_validate(value).kind == "execute"
    with pytest.raises(ValidationError):
        DirectoryRestoreStepView.model_validate(value)
    if fault == "source":
        value["source"] = "workspace_directory_restore_step"
    elif fault == "baseline":
        value["before_absence_fingerprint"] = "d" * 64
    elif fault == "target":
        value["target_fingerprint"] = "d" * 64
    elif fault == "root":
        value["root_path"] = "other"
    elif fault == "operation":
        value["operation_id"] = value["plan_id"]
    else:
        value["backup"] = {"entry_count": 0, "byte_size": 0}
    with pytest.raises(ValidationError):
        MissingDirectoryRestoreStepView.model_validate(value)


async def test_registration_is_atomic_if_a_middle_private_tool_name_is_occupied(runtime):
    service = AgentService(runtime, runtime.settings)
    definitions = missing_directory_restore_tools()
    conflict = replace(definitions[1])
    runtime.tools.register(conflict)
    before = {tool.name: id(tool) for tool in runtime.tools.definitions}
    try:
        with pytest.raises(ValueError, match="缺失目录恢复工具名称"):
            service.register_missing_directory_restores()
        assert {tool.name: id(tool) for tool in runtime.tools.definitions} == before
        assert definitions[0].name not in before and definitions[2].name not in before
    finally:
        await service.close()


@pytest.mark.parametrize("family", ["old-to-new", "new-to-old", "new-wrong-stage"])
async def test_old_and_missing_private_fences_never_cross_registry_families(runtime, family):
    service = AgentService(runtime, runtime.settings)
    service.register_directory_restores()
    service.register_missing_directory_restores()
    registry = runtime.tools
    if family == "old-to-new":
        name = MISSING_DIRECTORY_RESTORE_STEP_TOOLS["execute"]
        fence = DirectoryRestoreFence("original-plan", "original-operation", "execute", "a" * 64)
        backend = service.directory_restores
    else:
        name = (
            DIRECTORY_RESTORE_STEP_TOOLS["execute"]
            if family == "new-to-old"
            else MISSING_DIRECTORY_RESTORE_STEP_TOOLS["prepare"]
        )
        fence = MissingDirectoryRestoreFence(
            "original-plan", "original-operation", "execute", "a" * 64
        )
        backend = service.missing_directory_restores
    try:
        result = await registry.invoke(
            name,
            {"plan_id": "original-plan", "operation_id": "original-operation"},
            ToolContext(
                registry,
                origin="host",
                run_id="local",
                topic_id="original-topic",
                session_id="original-session",
                agent_id="original-agent",
                directory_restore=fence,
                workspace_directory_restores=backend,
            ),
        )
        assert result.is_error and result.code == "workspace_missing_directory_restore_context"
        assert list(registry.audit)[-1].code == result.code
        assert (
            not service.directory_restores._tasks and not service.missing_directory_restores._tasks
        )
    finally:
        await service.close()


def test_cached_definitions_keep_families_and_canonical_fact_schemas_separate():
    old = directory_restore_tools()
    new = missing_directory_restore_tools()
    assert old is directory_restore_tools() and new is missing_directory_restore_tools()
    assert {tool.name for tool in old}.isdisjoint(tool.name for tool in new)
    assert [(tool.name, tool.risk) for tool in new] == [
        (MISSING_DIRECTORY_RESTORE_STEP_TOOLS["prepare"], "read"),
        (MISSING_DIRECTORY_RESTORE_STEP_TOOLS["execute"], "write"),
        (MISSING_DIRECTORY_RESTORE_STEP_TOOLS["verify"], "read"),
    ]
