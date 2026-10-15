"""坏Checkpoint围栏在工具/Hook执行前受控拒绝，保留原审计身份。"""

from types import SimpleNamespace

import pytest

from krim_agent.tools import ToolContext, ToolError, ToolRegistry, tool


class FalseyFence:
    def __bool__(self):
        return False

    def __repr__(self):
        raise AssertionError("坏围栏的repr不得进入日志或错误")


@pytest.mark.parametrize("field", ["checkpoint_capture", "checkpoint_restore"])
@pytest.mark.parametrize("entry", ["call_tool", "invoke"])
@pytest.mark.parametrize(
    "factory",
    [
        pytest.param(object, id="opaque"),
        pytest.param(lambda: SimpleNamespace(model_source=None), id="fake-host-shape"),
        pytest.param(FalseyFence, id="falsey"),
    ],
)
async def test_malformed_checkpoint_context_refuses_before_callbacks_and_audits_original_identity(
    field, entry, factory
):
    seen = []

    @tool(risk="read", name="fence_probe")
    async def probe() -> str:
        seen.append("body")
        return "must not execute"

    async def policy(_definition, _context):
        seen.append("policy")
        return "allow"

    async def before(_definition, _context):
        seen.append("before")

    registry = ToolRegistry()
    registry.register(probe)
    registry.add_global_pre(before)
    context = ToolContext(
        registry,
        run_id="original-run",
        call_id="original-call",
        depth=1,
        policy=policy,
    )
    setattr(context, field, factory())
    try:
        if entry == "call_tool":
            with pytest.raises(ToolError) as error:
                await registry.call_tool("fence_probe", {}, context)
            assert error.value.code == "workspace_checkpoint_context"
        else:
            result = await registry.invoke("fence_probe", {}, context)
            assert result.is_error and result.code == "workspace_checkpoint_context"
        assert seen == []
        assert len(registry.audit) == 1
        audit = registry.audit[0]
        assert (audit.call_id, audit.run_id, audit.name, audit.depth) == (
            "original-call",
            "original-run",
            "fence_probe",
            2,
        )
        assert not audit.ok and audit.decision == "deny" and audit.risk == "read"
        assert audit.code == "workspace_checkpoint_context"
    finally:
        await registry.close()
