"""离线真实模型父Run/call、原缺失私有叶和Host事实，不重跑发布。"""

import pytest
from sqlalchemy import select
from test_directory_checkpoint_capture import make_tree

from krim_agent.agents import AgentProfile, AgentProfileUpdate
from krim_agent.application.agents import AgentService
from krim_agent.persistence.database import WorkspaceMissingDirectoryRestoreModelSourceRow
from krim_agent.providers import ModelText, ModelToolCall
from krim_agent.sdk import AgentSession, DirectoryCheckpointCreate
from krim_agent.tools.workspace_checkpoints import missing_directory_restore_control_tools
from krim_agent.workspace_missing_directory_restore import (
    MissingDirectoryRestoreExecute,
    MissingDirectoryRestoreView,
)
from krim_agent.workspace_missing_directory_restore_control import (
    MISSING_DIRECTORY_RESTORE_STEP_TOOLS,
)
from krim_agent.workspace_missing_directory_restore_model import MissingDirectoryRestoreQueryResult
from krim_agent.workspace_missing_directory_writer import WorkspaceMissingDirectoryRestoreWriter

PUBLIC = [item.name for item in missing_directory_restore_control_tools()]
PREVIEW, START, QUERY, CANCEL, VERIFY = PUBLIC


async def setup(
    runtime,
    root,
    provider,
    *,
    names=None,
    persistent=False,
    with_complete=False,
    tool_timeout=10,
    round_timeout=60,
    mode="agent",
):
    make_tree(root)

    async def allow(_definition, _context):
        return "allow"

    runtime.provider, runtime.tool_policy = provider, allow
    service = AgentService(runtime, runtime.settings)
    binding = service.register_workspace("files", root, version="1", writable=True, default=True)
    service.register_directory_checkpoints()
    service.register_missing_directory_restores()
    service.register_missing_directory_checkpoint_restore_controls()
    service.register_missing_directory_checkpoint_restore_controls()
    defaults = [
        "workspace_directory_checkpoint_capture",
        *MISSING_DIRECTORY_RESTORE_STEP_TOOLS.values(),
        *PUBLIC,
    ]
    if with_complete:
        from krim_agent.tools.workspace_checkpoints import directory_restore_control_tools
        from krim_agent.workspace_directory_restore_control import DIRECTORY_RESTORE_STEP_TOOLS

        service.register_directory_restores()
        service.register_directory_checkpoint_restore_controls()
        defaults.extend(item.name for item in directory_restore_control_tools())
        defaults.extend(DIRECTORY_RESTORE_STEP_TOOLS.values())
    await service.profile_put(
        AgentProfileUpdate(
            expected_revision=0,
            config=AgentProfile(
                name="model-missing-directory-restore",
                description="原模型缺失恢复父调用",
                workspace_key="files",
                persistent=persistent,
                tool_timeout_seconds=tool_timeout,
                timeout_seconds=round_timeout,
                mode=mode,
                tool_names=names if names is not None else defaults,
            ),
        )
    )
    agent = AgentSession(service, (await service.spawn("model-missing-directory-restore")).agent_id)
    return service, agent, binding


class Provider:
    name = "offline-model-missing-directory-restores"

    def __init__(self):
        self.calls, self.target_id = 0, None
        self.planned = self.finished = self.query = None
        self.replies = []

    async def stream(self, messages, tools):
        self.calls += 1
        if self.calls > 1:
            self.replies.append(messages[-1].content)
        assert set(PUBLIC) <= {item.name for item in tools}
        if self.calls == 1:
            yield ModelToolCall(
                id="original-missing-preview",
                name=PREVIEW,
                arguments={
                    "target": {"kind": "checkpoint", "identifier": self.target_id},
                    "request_key": "model-preview",
                },
            )
        elif self.calls == 2:
            self.planned = MissingDirectoryRestoreView.model_validate_json(messages[-1].content)
            assert self.planned.phase == "ready" and self.planned.baseline.kind == "absent"
            yield ModelToolCall(
                id="original-missing-start",
                name=START,
                arguments=MissingDirectoryRestoreExecute(
                    plan_id=self.planned.plan_id,
                    expected_revision=self.planned.revision,
                    request_key="model-execute",
                ).model_dump(),
            )
        elif self.calls == 3:
            self.finished = MissingDirectoryRestoreView.model_validate_json(messages[-1].content)
            assert self.finished.phase == "finished" and self.finished.receipt.status == "done"
            yield ModelToolCall(
                id="original-missing-query",
                name=QUERY,
                arguments={
                    "kind": "diff",
                    "plan_id": self.finished.plan_id,
                    "limit": 128,
                },
            )
        else:
            self.query = MissingDirectoryRestoreQueryResult.model_validate_json(
                messages[-1].content
            )
            assert self.query.diff.baseline == self.finished.baseline
            assert all(
                item.operation == "create" and item.before is None
                for item in self.query.diff.entries
            )
            yield ModelText(text="原缺失目录已恢复，差异保留真实缺失baseline")


async def test_original_missing_model_preview_publish_diff_sources_and_host_zero_replay(
    runtime, tmp_path, monkeypatch
):
    provider = Provider()
    service, agent, binding = await setup(runtime, tmp_path, provider)
    leaves = []

    async def observe(definition, context):
        if definition.name in MISSING_DIRECTORY_RESTORE_STEP_TOOLS.values():
            leaves.append(
                (
                    definition.name,
                    context.run_id,
                    context.call_id,
                    context.source_call_id,
                    context.origin,
                    context.depth,
                    context.workspace_missing_directory_restores
                    is service.missing_directory_restores,
                )
            )

    try:
        captured = await agent.create_directory_checkpoint(
            DirectoryCheckpointCreate(root_path="tree", request_key="target")
        )
        captured = await agent.wait_directory_checkpoint(captured.checkpoint_id, timeout_seconds=10)
        assert captured.status == "available"
        provider.target_id = captured.checkpoint_id
        (tmp_path / "tree").rename(tmp_path / "retained-original")
        service._handle(agent.agent_id).runtime.tools.add_global_pre(observe)
        result = await agent.execute(
            "在原Run内缺失预览、发布并查询差异", idempotency_key="model-missing-loop"
        )
        assert result.status == "completed" and provider.calls == 4, (result, provider.replies)
        assert len(leaves) == 2
        assert all(
            item[1] == result.run_id and item[2] != item[3] and item[4:] == ("model", 2, True)
            for item in leaves
        )
        async with runtime.repository.database.sessions() as sql:
            sources = list(
                await sql.scalars(select(WorkspaceMissingDirectoryRestoreModelSourceRow))
            )
        assert {(row.phase, row.run_id, row.call_id) for row in sources} == {
            ("preview", result.run_id, "original-missing-preview"),
            ("execute", result.run_id, "original-missing-start"),
        }
        assert (tmp_path / "tree/nested/nul").read_bytes() == b"\0\xff"
        assert provider.query.diff.plan_id == provider.finished.plan_id

        def forbidden(*_args, **_kwargs):
            pytest.fail("Host历史不能重放模型或缺失Writer")

        monkeypatch.setattr(provider, "stream", forbidden)
        monkeypatch.setattr(WorkspaceMissingDirectoryRestoreWriter, "prepare", forbidden)
        monkeypatch.setattr(WorkspaceMissingDirectoryRestoreWriter, "apply", forbidden)
        current = await service.workspace_missing_directory_restore_by_key(
            agent.agent_id, "model-preview"
        )
        operation = await service.workspace_missing_directory_restore_operation_by_key(
            agent.agent_id, "model-execute"
        )
        assert (
            current == provider.finished
            and operation.operation_id == provider.finished.operation_id
        )
        assert operation.status == "done" and provider.calls == 4
    finally:
        await service.close()


class BothProvider(Provider):
    async def stream(self, messages, tools):
        if self.calls < 2:
            async for event in super().stream(messages, tools):
                yield event
        elif self.calls == 2:
            self.calls += 1
            self.finished = MissingDirectoryRestoreView.model_validate_json(messages[-1].content)
            yield ModelToolCall(
                id="original-complete-preview",
                name="workspace_directory_checkpoint_restore_preview",
                arguments={
                    "target": {"kind": "checkpoint", "identifier": self.target_id},
                    "request_key": "complete-preview",
                },
            )
        else:
            from krim_agent.workspace_directory_restore import DirectoryRestoreView

            self.calls += 1
            self.complete = DirectoryRestoreView.model_validate_json(messages[-1].content)
            assert self.complete.phase == "ready" and self.complete.backup is not None
            yield ModelText(text="同一Agent原模型已分别使用缺失和完整恢复Backend")


async def seed_target(agent, root):
    captured = await agent.create_directory_checkpoint(
        DirectoryCheckpointCreate(root_path="tree", request_key="target")
    )
    captured = await agent.wait_directory_checkpoint(captured.checkpoint_id, timeout_seconds=10)
    assert captured.status == "available"
    (root / "tree").rename(root / "retained-original")
    return captured.checkpoint_id


async def test_same_agent_can_load_both_restore_families_without_replacing_old_backend(
    runtime, tmp_path
):
    from krim_agent.persistence.database import WorkspaceDirectoryRestoreModelSourceRow

    provider = BothProvider()
    service, agent, _ = await setup(runtime, tmp_path, provider, with_complete=True)
    try:
        provider.target_id = await seed_target(agent, tmp_path)
        handle = service._handle(agent.agent_id)
        assert handle.runtime.workspace_directory_restores is service.directory_restores
        assert (
            handle.runtime.workspace_missing_directory_restores
            is service.missing_directory_restores
        )
        result = await agent.execute(
            "先缺失恢复，再审阅完整恢复", idempotency_key="both-restore-families"
        )
        assert result.status == "completed" and provider.calls == 4, (result, provider.replies)
        assert provider.finished.phase == "finished" and provider.complete.phase == "ready"
        assert provider.finished.baseline.kind == "absent" and provider.complete.backup is not None
        async with runtime.repository.database.sessions() as sql:
            missing = list(
                await sql.scalars(select(WorkspaceMissingDirectoryRestoreModelSourceRow))
            )
            complete = list(await sql.scalars(select(WorkspaceDirectoryRestoreModelSourceRow)))
        assert len(missing) == 2 and len(complete) == 1
        assert {row.run_id for row in [*missing, *complete]} == {result.run_id}
        assert complete[0].call_id == "original-complete-preview"
    finally:
        await service.close()


class OneShotProvider:
    name = "offline-one-missing-parent"

    def __init__(self, name=PREVIEW, arguments=None, call_id="one-original-preview"):
        self.calls, self.target_id, self.reply = 0, None, None
        self.name_value, self.arguments, self.call_id = name, arguments, call_id

    async def stream(self, messages, tools):
        self.calls += 1
        if self.calls == 1:
            yield ModelToolCall(
                id=self.call_id,
                name=self.name_value,
                arguments=self.arguments
                if self.arguments is not None
                else {
                    "target": {"kind": "checkpoint", "identifier": self.target_id},
                    "request_key": "one-preview",
                },
            )
        else:
            self.reply = messages[-1].content
            yield ModelText(text="读取原模型工具裁决")


async def phase_setup(runtime, root, action="preview", **kwargs):
    from krim_agent.workspace_directory_restore import DirectoryRestoreTarget
    from krim_agent.workspace_missing_directory_restore import MissingDirectoryRestorePreview

    provider = OneShotProvider(
        PREVIEW if action == "preview" else START, call_id="original-missing-" + action
    )
    service, agent, binding = await setup(runtime, root, provider, **kwargs)
    try:
        target = await seed_target(agent, root)
        preview = MissingDirectoryRestorePreview(
            target=DirectoryRestoreTarget(identifier=target), request_key="boundary-preview"
        )
        planned = None
        if action == "start":
            planned = await service.workspace_preview_missing_directory_restore(
                agent.agent_id, preview
            )
            planned = await service.workspace_missing_directory_restore_wait(
                agent.agent_id, planned.plan_id, timeout_seconds=10
            )
            assert planned.phase == "ready"
            provider.arguments = MissingDirectoryRestoreExecute(
                plan_id=planned.plan_id,
                expected_revision=planned.revision,
                request_key="boundary-execute",
            ).model_dump()
        else:
            provider.arguments = preview.model_dump()
        return service, agent, binding, provider, planned
    except BaseException:
        await service.close()
        raise


async def test_model_parent_without_frozen_private_leaf_cannot_capture_missing_baseline(
    runtime, tmp_path, monkeypatch
):
    provider = OneShotProvider()
    names = [
        "workspace_directory_checkpoint_capture",
        *PUBLIC,
        *[name for kind, name in MISSING_DIRECTORY_RESTORE_STEP_TOOLS.items() if kind != "prepare"],
    ]
    service, agent, _ = await setup(runtime, tmp_path, provider, names=names)
    try:
        provider.target_id = await seed_target(agent, tmp_path)

        def forbidden(*_args, **_kwargs):
            pytest.fail("原私有叶未冻结授权，不得取得缺失观察")

        from krim_agent.workspace_directory_absence import WorkspaceDirectoryAbsenceReader

        monkeypatch.setattr(WorkspaceDirectoryAbsenceReader, "capture", forbidden)
        result = await agent.execute("未授权叶必须拒绝", idempotency_key="no-missing-prepare-leaf")
        assert result.status == "completed" and provider.calls == 2
        audits = await runtime.repository.audits(result.run_id)
        assert any(
            item.name == PREVIEW
            and not item.ok
            and item.code == "workspace_missing_directory_restore_unavailable"
            for item in audits
        )
        assert provider.reply
        assert (
            await service.missing_directory_restore_store.list("session:" + agent.session_id) == []
        )
        assert not (tmp_path / "tree").exists()
    finally:
        await service.close()


async def test_parent_hook_cannot_switch_missing_backend_to_complete_family(
    runtime, tmp_path, monkeypatch
):
    from krim_agent.workspace_directory_absence import WorkspaceDirectoryAbsenceReader

    provider = OneShotProvider()
    service, agent, _ = await setup(runtime, tmp_path, provider, with_complete=True)
    try:
        provider.target_id = await seed_target(agent, tmp_path)

        async def corrupt(definition, context):
            if definition.name == PREVIEW:
                context.workspace_missing_directory_restores = service.directory_restores

        def forbidden(*_args, **_kwargs):
            pytest.fail("坏Backend不能进入缺失观察")

        service._handle(agent.agent_id).runtime.tools.add_global_pre(corrupt)
        monkeypatch.setattr(WorkspaceDirectoryAbsenceReader, "capture", forbidden)
        result = await agent.execute("Backend身份必须冻结", idempotency_key="bad-missing-backend")
        assert result.status == "completed" and provider.calls == 2
        audits = await runtime.repository.audits(result.run_id)
        assert any(
            item.name == PREVIEW
            and not item.ok
            and item.code == "workspace_checkpoint_model_context"
            for item in audits
        )
        assert provider.reply
        assert (
            await service.missing_directory_restore_store.list("session:" + agent.session_id) == []
        )
    finally:
        await service.close()
