"""私有Host叶必须保留真实类别/阶段/主体/预算；Model与Hook不可伪装。"""

import pytest

from krim_agent.tools.container_retention import container_retention_tools
from krim_agent.tools.core import ToolContext
from krim_agent.tools.registry import ToolRegistry
from krim_agent.workspace_container_retention import ContainerRetentionFence
from krim_agent.workspace_container_retention_control import ContainerRetentionInspection

NAME = "workspace_container_retention_inspect"


class Backend:
    called = 0
    validated = 0

    async def tool_step(self, kind, payload, context):
        self.called += 1
        return {
            "source": {
                "kind": "workspace_missing_directory_restore",
                "source_plan_id": "original-source",
                "source_operation_id": "original-operation",
                "scope_id": "session:agent",
                "topic_id": "topic",
                "root_path": "tree",
                "workspace_key": "files",
                "resource_fingerprint": "a" * 64,
                "source_hash": "b" * 64,
                "published_fingerprint": "c" * 64,
            },
            "status": "empty",
            "fingerprint": "d" * 64,
            "error_code": None,
            "existing_operation_id": None,
        }

    async def validate_step(self, kind, value, context):
        self.validated += 1


def actor():
    registry = ToolRegistry()
    for definition in container_retention_tools():
        registry.register(definition)
    backend = Backend()
    context = ToolContext(
        registry,
        run_id="local",
        origin="host",
        agent_id="agent",
        session_id="agent",
        topic_id="topic",
        workspace_container_retentions=backend,
        container_retention=ContainerRetentionInspection("inspection", "original-source", "a" * 64),
    )
    return registry, backend, context


async def test_valid_host_private_selection_reaches_only_its_backend_and_output_guard():
    registry, backend, context = actor()
    value = await registry.invoke(NAME, {"source_plan_id": "original-source"}, context)
    assert not value.is_error and backend.called == 1 and backend.validated == 1
    assert value.data["status"] == "empty"


@pytest.mark.parametrize(
    "fault",
    [
        "missing",
        "falsey",
        "model",
        "source-call",
        "session",
        "mixed",
        "wrong-kind",
        "bool-epoch",
        "empty-session",
        "empty-topic",
    ],
)
async def test_invalid_host_private_context_never_enters_backend(fault):
    registry, backend, context = actor()
    selected = NAME
    arguments = {"source_plan_id": "original-source"}
    if fault == "missing":
        context.container_retention = None
    elif fault == "falsey":

        class Falsey:
            def __bool__(self):
                return False

        context.container_retention = Falsey()
    elif fault == "model":
        context.origin = "model"
    elif fault == "source-call":
        context.source_call_id = "fake-parent"
    elif fault == "session":
        context.session_id = "other"
    elif fault == "empty-session":
        context.agent_id = context.session_id = ""
    elif fault == "empty-topic":
        context.topic_id = ""
    elif fault == "mixed":
        context.directory_restore = object()
    elif fault == "wrong-kind":
        context.container_retention = ContainerRetentionFence("operation", 0, "a" * 64, "remove")
    else:
        context.container_retention = ContainerRetentionFence("operation", True, "a" * 64, "remove")
        selected, arguments = "workspace_container_retention_remove", {"operation_id": "operation"}
    result = await registry.invoke(selected, arguments, context)
    assert result.is_error and result.code == "workspace_container_retention_context"
    assert backend.called == backend.validated == 0


@pytest.mark.parametrize("fault", ["inplace", "shape", "budget", "scope"])
async def test_pre_hook_cannot_replace_original_private_selection_or_budget(fault):
    registry, backend, context = actor()

    async def changed(_definition, child):
        if fault == "inplace":
            object.__setattr__(child.container_retention, "source_plan_id", "different-source")
        elif fault == "shape":
            child.container_retention = object()
        elif fault == "budget":
            child.timeout = 100
        else:
            child.topic_id = "another-topic"

    registry.add_global_pre(changed)
    value = await registry.invoke(NAME, {"source_plan_id": "original-source"}, context)
    assert value.is_error and value.code == "workspace_container_retention_context"
    assert backend.called == 0


async def test_post_hook_cannot_forge_other_source_or_done_output():
    registry, backend, context = actor()

    async def forged(_definition, value, _child):
        value["source"]["scope_id"] = "session:other"
        return value

    registry.add_global_post(forged)
    result = await registry.invoke(NAME, {"source_plan_id": "original-source"}, context)
    assert result.is_error and result.code == "workspace_container_retention_unverified"
    assert backend.called == 1 and backend.validated == 0


async def test_private_stage_or_owner_cannot_be_uploaded_in_tool_json():
    registry, backend, context = actor()
    result = await registry.invoke(
        NAME, {"source_plan_id": "original-source", "owner_id": "not-public", "stage": {}}, context
    )
    assert result.is_error and backend.called == 0
