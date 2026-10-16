"""显式可调用拒绝策略不能因bool为False被默认read许可替换。"""

import pytest
from sqlalchemy import func, select
from test_directory_checkpoint_capture import make_tree

from krim_agent.agents import AgentProfile, AgentProfileUpdate
from krim_agent.agents.permissions import profile_policy
from krim_agent.application.agents import AgentService
from krim_agent.persistence.database import WorkspaceDirectoryCheckpointRow
from krim_agent.sdk import AgentSession, DirectoryCheckpointCreate
from krim_agent.tools import ToolContext
from krim_agent.tools.builtins import builtin_registry
from krim_agent.tools.missing_directory_restore import missing_directory_restore_tools
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshotReader


class FalseyDeny:
    def __init__(self):
        self.calls = []
        self.truth_checks = 0

    def __bool__(self):
        self.truth_checks += 1
        return False

    async def __call__(self, definition, context):
        self.calls.append((definition.name, context.origin))
        return "deny"


@pytest.mark.parametrize("mode", ["agent", "auto", "plan", "ask"])
@pytest.mark.parametrize("risk", ["read", "write"])
async def test_callable_host_denial_is_consulted_without_truthiness_except_mode_rejection(
    mode, risk
):
    host = FalseyDeny()
    definition = missing_directory_restore_tools()[0 if risk == "read" else 1]
    context = ToolContext(builtin_registry(), origin="host")
    decision = await profile_policy(mode, host)(definition, context)
    assert decision == "deny"
    assert len(host.calls) == (0 if mode == "ask" or mode == "plan" and risk == "write" else 1)
    assert host.truth_checks == 0


@pytest.mark.parametrize("mode", ["agent", "auto"])
async def test_real_sdk_capture_deny_prevents_reader_and_material_publication(
    runtime, tmp_path, monkeypatch, mode
):
    make_tree(tmp_path)
    host = FalseyDeny()
    runtime.tool_policy = host
    service = AgentService(runtime, runtime.settings)
    service.register_workspace("files", tmp_path, version="1", writable=True, default=True)
    service.register_directory_checkpoints()
    try:
        await service.profile_put(
            AgentProfileUpdate(
                expected_revision=0,
                config=AgentProfile(
                    name="falsey-policy",
                    description="真实可调用拒绝策略",
                    workspace_key="files",
                    mode=mode,
                    tool_names=["workspace_directory_checkpoint_capture"],
                ),
            )
        )
        agent = await service.spawn("falsey-policy")
        sdk = AgentSession(service, agent.agent_id)
        captured = []
        original = WorkspaceTreeSnapshotReader.capture

        def capture(*args, **kwargs):
            captured.append(True)
            return original(*args, **kwargs)

        monkeypatch.setattr(WorkspaceTreeSnapshotReader, "capture", capture)
        started = await sdk.create_directory_checkpoint(
            DirectoryCheckpointCreate(
                root_path="tree",
                request_key="explicit-deny",
            )
        )
        result = await sdk.wait_directory_checkpoint(started.checkpoint_id, timeout_seconds=10)
        assert result.status == "failed" and result.error_code == "permission_denied"
        assert host.calls == [("workspace_directory_checkpoint_capture", "host")]
        assert not captured
        async with runtime.repository.database.sessions() as sql:
            assert (
                await sql.scalar(select(func.count()).select_from(WorkspaceDirectoryCheckpointRow))
                == 0
            )
        assert (tmp_path / "tree/nested/nul").read_bytes() == b"\0\xff"
    finally:
        await service.close()
