"""真实离线模型原Run/call预览、整树发布与安全查询，不接受Host回调替身。"""

from sqlalchemy import select
from test_directory_checkpoint_capture import make_tree

from krim_agent.agents import AgentProfile, AgentProfileUpdate
from krim_agent.application.agents import AgentService
from krim_agent.persistence.database import WorkspaceDirectoryRestoreModelSourceRow
from krim_agent.providers import ModelText, ModelToolCall
from krim_agent.sdk import (
    AgentSession,
    DirectoryCheckpointCreate,
    DirectoryRestoreQueryResult,
    DirectoryRestoreView,
)
from krim_agent.workspace import WORKSPACE_DIRECTORY_RESTORE_CONTROL_TOOL_NAMES
from krim_agent.workspace_checkpoint import CheckpointModelSource
from krim_agent.workspace_directory_restore_control import DIRECTORY_RESTORE_STEP_TOOLS

PREVIEW = "workspace_directory_checkpoint_restore_preview"
START = "workspace_directory_checkpoint_restore_start"
QUERY = "workspace_directory_checkpoint_restore_query"
CANCEL = "workspace_directory_checkpoint_restore_cancel"
VERIFY = "workspace_directory_checkpoint_restore_verify"
CAPTURE = "workspace_directory_checkpoint_capture"
NAMES = [
    *sorted(WORKSPACE_DIRECTORY_RESTORE_CONTROL_TOOL_NAMES),
    *DIRECTORY_RESTORE_STEP_TOOLS.values(),
    CAPTURE,
]


async def setup(
    runtime,
    root,
    provider,
    *,
    policy=None,
    names=None,
    mode="agent",
    persistent=False,
    tool_timeout=10,
    round_timeout=60,
):
    make_tree(root)

    async def allow(_definition, _context):
        return "allow"

    runtime.provider, runtime.tool_policy = provider, policy or allow
    service = AgentService(runtime, runtime.settings)
    binding = service.register_workspace("files", root, version="1", writable=True, default=True)
    service.register_directory_checkpoints()
    service.register_directory_restores()
    service.register_directory_checkpoint_restore_controls()
    service.register_directory_checkpoint_restore_controls()
    try:
        await service.profile_put(
            AgentProfileUpdate(
                expected_revision=0,
                config=AgentProfile(
                    name="model-directory-restore",
                    description="原模型完整目录恢复",
                    workspace_key="files",
                    tool_names=NAMES if names is None else names,
                    mode=mode,
                    persistent=persistent,
                    tool_timeout_seconds=tool_timeout,
                    timeout_seconds=round_timeout,
                ),
            )
        )
        agent = AgentSession(service, (await service.spawn("model-directory-restore")).agent_id)
        return service, agent, binding
    except BaseException:
        await service.close()
        raise


async def target(agent):
    captured = await agent.create_directory_checkpoint(
        DirectoryCheckpointCreate(root_path="tree", request_key="host-target")
    )
    captured = await agent.wait_directory_checkpoint(captured.checkpoint_id, timeout_seconds=10)
    assert captured.status == "available", captured
    return captured


async def phase_setup(runtime, root, action="preview", **kwargs):
    provider = OneShotProvider(PREVIEW if action == "preview" else START)
    service, agent, binding = await setup(runtime, root, provider, **kwargs)
    try:
        captured = await target(agent)
        (binding.workspace.root / "tree/nested/nul").write_bytes(b"current\x80\0")
        from krim_agent.sdk import (
            DirectoryRestoreExecute,
            DirectoryRestorePreview,
            DirectoryRestoreTarget,
        )

        request = DirectoryRestorePreview(
            target=DirectoryRestoreTarget(identifier=captured.checkpoint_id),
            request_key="boundary-preview",
        )
        planned = None
        if action == "start":
            planned = await agent.preview_directory_checkpoint_restore(request)
            planned = await agent.wait_directory_checkpoint_restore(
                planned.plan_id, timeout_seconds=10
            )
            assert planned.phase == "ready", planned
            provider.arguments = DirectoryRestoreExecute(
                plan_id=planned.plan_id,
                expected_revision=planned.revision,
                request_key="boundary-execute",
            ).model_dump()
        else:
            provider.arguments = request.model_dump()
        return service, agent, binding, provider, planned
    except BaseException:
        await service.close()
        raise


class Provider:
    name = "offline-original-directory-restores"

    def __init__(self):
        self.calls, self.target_id = 0, None
        self.preview, self.finished, self.query = None, None, None
        self.replies = []

    async def stream(self, messages, tools):
        self.calls += 1
        if self.calls > 1:
            self.replies.append(messages[-1].content)
        if self.calls == 1:
            assert {PREVIEW, START, QUERY, VERIFY, CANCEL} <= {item.name for item in tools}
            yield ModelToolCall(
                id="original-dir-preview",
                name=PREVIEW,
                arguments={
                    "target": {"kind": "checkpoint", "identifier": self.target_id},
                    "request_key": "model-preview",
                },
            )
        elif self.calls == 2:
            self.preview = DirectoryRestoreView.model_validate_json(messages[-1].content)
            assert self.preview.phase == "ready"
            yield ModelToolCall(
                id="original-dir-execute",
                name=START,
                arguments={
                    "plan_id": self.preview.plan_id,
                    "expected_revision": self.preview.revision,
                    "request_key": "model-execute",
                },
            )
        elif self.calls == 3:
            self.finished = DirectoryRestoreView.model_validate_json(messages[-1].content)
            assert self.finished.phase == "finished"
            yield ModelToolCall(
                id="original-dir-query",
                name=QUERY,
                arguments={"kind": "diff", "plan_id": self.finished.plan_id, "limit": 128},
            )
        else:
            self.query = DirectoryRestoreQueryResult.model_validate_json(messages[-1].content)
            yield ModelText(text="原模型目录恢复与差异已完成")


class OneShotProvider:
    name = "offline-model-directory-restore-boundary"

    def __init__(self, name=PREVIEW, arguments=None, call_id="original-directory-restore"):
        self.name_value, self.arguments, self.call_id = name, arguments or {}, call_id
        self.calls, self.reply = 0, None

    async def stream(self, messages, _tools):
        self.calls += 1
        if self.calls == 1:
            yield ModelToolCall(id=self.call_id, name=self.name_value, arguments=self.arguments)
        else:
            self.reply = messages[-1].content
            yield ModelText(text="原目录恢复控制结果已读取")


async def test_original_model_preview_atomic_start_query_and_public_ledger_same_fact(
    runtime, tmp_path
):
    provider = Provider()
    service, agent, binding = await setup(runtime, tmp_path, provider)
    leaves = []

    async def observe(definition, context):
        if definition.name in DIRECTORY_RESTORE_STEP_TOOLS.values():
            leaves.append(
                (
                    definition.name,
                    context.run_id,
                    context.call_id,
                    context.source_call_id,
                    context.origin,
                    context.depth,
                    callable(context.directory_restore_qualify),
                )
            )

    try:
        captured = await target(agent)
        provider.target_id = captured.checkpoint_id
        (binding.workspace.root / "tree/nested/nul").write_bytes(b"current\x80\0")
        service._handle(agent.agent_id).runtime.tools.add_global_pre(observe)
        result = await agent.execute(
            "读取完整备份、原子恢复并查询差异", idempotency_key="original-model-directory-restore"
        )
        assert result.status == "completed" and provider.calls == 4, (result, provider.replies)
        finished = await agent.directory_checkpoint_restore_by_key("model-preview")
        assert finished == provider.finished and finished.status == "done"
        assert provider.query.diff.plan_id == finished.plan_id
        assert provider.query.diff.entries[0].path == "tree/nested/nul"
        assert (binding.workspace.root / "tree/nested/nul").read_bytes() == b"\0\xff"
        assert [item[0] for item in leaves] == [
            DIRECTORY_RESTORE_STEP_TOOLS["prepare"],
            DIRECTORY_RESTORE_STEP_TOOLS["execute"],
        ]
        assert [item[3] for item in leaves] == ["original-dir-preview", "original-dir-execute"]
        assert all(
            item[1] == result.run_id and item[2] != item[3] and item[4:] == ("model", 2, True)
            for item in leaves
        )
        assert await service.directory_restore_store.source(
            finished.scope_id, finished.plan_id, "preview"
        ) == CheckpointModelSource(result.run_id, "original-dir-preview")
        assert await service.directory_restore_store.source(
            finished.scope_id, finished.plan_id, "execute"
        ) == CheckpointModelSource(result.run_id, "original-dir-execute")
        async with runtime.repository.database.sessions() as sql:
            rows = list(await sql.scalars(select(WorkspaceDirectoryRestoreModelSourceRow)))
            assert {(row.phase, row.run_id, row.call_id) for row in rows} == {
                ("preview", result.run_id, "original-dir-preview"),
                ("execute", result.run_id, "original-dir-execute"),
            }
        audits = await runtime.repository.audits(result.run_id)
        assert len(audits) == 5 and all(audit.ok for audit in audits), audits
        assert (
            not service.directory_restores._tasks
            and not service.directory_restores._pending
            and not service.directory_restores._fences
        )
    finally:
        await service.close()
