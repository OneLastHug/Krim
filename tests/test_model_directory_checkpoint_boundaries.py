"""公开模型参数与入口坏来源必须受控拒绝，不进入捕获/来源落库。"""

import pytest
from test_directory_checkpoint_capture import make_tree
from test_model_directory_checkpoints import LEAF, OneShotProvider, setup, source_count

from krim_agent.tools import ToolContext
from krim_agent.workspace_directory_checkpoint import DirectoryCheckpointCaptureFence
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshotReader


@pytest.mark.parametrize(
    "field", ["scope_id", "run_id", "source_call_id", "model_source", "token", "snapshot"]
)
async def test_model_input_cannot_supply_private_scope_source_fence_or_snapshot(
    runtime, tmp_path, monkeypatch, field
):
    make_tree(tmp_path)
    provider = OneShotProvider(
        arguments={"root_path": "tree", "request_key": "private-input", field: "forged"}
    )
    service, agent = await setup(runtime, tmp_path, provider)
    monkeypatch.setattr(
        WorkspaceTreeSnapshotReader,
        "capture",
        lambda *_args, **_kwargs: pytest.fail("公开参数不可上传私有来源后访问Reader"),
    )
    try:
        result = await agent.execute("拒绝私有模型参数", idempotency_key="private-input")
        assert result.status == "completed"
        assert await source_count(runtime) == 0 and await agent.directory_checkpoints() == []
        audits = await runtime.repository.audits(result.run_id)
        assert len(audits) == 1 and audits[0].code == "invalid_input" and not audits[0].ok
    finally:
        await service.close()


class FalseSource:
    def __bool__(self):
        return False


@pytest.mark.parametrize("source", [object(), FalseSource(), False])
async def test_ingress_bad_or_falsey_directory_source_is_audited_before_attributes_or_io(
    runtime, tmp_path, monkeypatch, source
):
    service, agent = await setup(runtime, tmp_path, OneShotProvider())
    handle = service._handle(agent.agent_id)
    registry = handle.runtime.tools
    monkeypatch.setattr(
        WorkspaceTreeSnapshotReader,
        "capture",
        lambda *_args, **_kwargs: pytest.fail("坏来源不能访问目录"),
    )
    try:
        for origin in ("host", "model"):
            context = ToolContext(
                registry,
                origin=origin,
                agent_id=agent.agent_id,
                session_id=agent.session_id,
                topic_id=handle.topic_id,
                workspace=handle.definition.workspace,
                workspace_directory_checkpoints=service.directory_checkpoints,
                directory_checkpoint_capture=DirectoryCheckpointCaptureFence(
                    "bad", "0" * 64, source
                ),
            )
            result = await registry.invoke(LEAF, {"root_path": "tree"}, context)
            assert result.is_error and result.code == "workspace_directory_capture_model_context", (
                result
            )
        assert len(registry.audit) == 2 and all(not audit.ok for audit in registry.audit)
        assert await source_count(runtime) == 0 and await agent.directory_checkpoints() == []
    finally:
        await service.close()


async def test_original_registry_depth_limit_rejects_nested_directory_reader(
    runtime, tmp_path, monkeypatch
):
    make_tree(tmp_path)
    service, agent = await setup(runtime, tmp_path, OneShotProvider())
    handle = service._handle(agent.agent_id)
    handle.runtime.tools.max_depth = 1
    monkeypatch.setattr(
        WorkspaceTreeSnapshotReader,
        "capture",
        lambda *_args, **_kwargs: pytest.fail("深度资格不足不能进入Reader"),
    )
    try:
        result = await agent.execute("保留原嵌套深度", idempotency_key="depth")
        final = await agent.directory_checkpoint_by_key("one-directory")
        assert result.status == "completed" and final.status == "failed", (result, final)
        assert final.error_code == "max_depth" and final.snapshot is None
        assert await source_count(runtime) == 1
    finally:
        await service.close()
