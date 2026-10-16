"""原模型父/叶Hook改变参数、身份、预算或真实回程时受控拒绝。"""

import asyncio
import copy
import json
from dataclasses import replace

import pytest
from test_model_directory_restores import PREVIEW, START, phase_setup

from krim_agent.tools import ShortCircuit
from krim_agent.workspace_directory_restore_control import DIRECTORY_RESTORE_STEP_TOOLS


class FalseSource:
    def __bool__(self):
        return False


@pytest.mark.parametrize(
    "action,target,phase,fault",
    [
        *[
            ("preview", "root", "pre", fault)
            for fault in (
                "arguments",
                "origin",
                "run",
                "call",
                "session",
                "backend",
                "directory-backend",
                "timeout",
                "budget",
                "short",
            )
        ],
        *[("preview", "root", "post", fault) for fault in ("data", "marker", "budget")],
        *[
            ("preview", "leaf", "pre", fault)
            for fault in (
                "arguments",
                "origin",
                "source_call",
                "source-none",
                "source-false",
                "qualifier",
                "budget",
                "short",
                "token-in-place",
                "source-in-place",
                "both-none",
            )
        ],
        *[
            ("preview", "leaf", "post", fault)
            for fault in ("data", "marker", "budget", "parent-backend")
        ],
        *[
            ("start", "leaf", phase, fault)
            for phase, fault in (
                ("pre", "arguments"),
                ("pre", "token-in-place"),
                ("pre", "source-in-place"),
                ("post", "data"),
                ("post", "budget"),
                ("post", "parent-backend"),
            )
        ],
    ],
)
async def test_parent_and_leaf_hooks_preserve_original_source_budget_and_fact(
    runtime, tmp_path, action, target, phase, fault
):
    service, agent, binding, provider, _ = await phase_setup(runtime, tmp_path, action)
    parent = []
    root = PREVIEW if action == "preview" else START
    selected = (
        root
        if target == "root"
        else DIRECTORY_RESTORE_STEP_TOOLS["prepare" if action == "preview" else "execute"]
    )

    def corrupt(context):
        if fault == "arguments":
            context.arguments["request_key" if target == "root" else "plan_id"] = "forged"
        elif fault == "origin":
            context.origin = "host"
        elif fault == "run":
            context.run_id = "other-run"
        elif fault == "call":
            context.call_id = "other-call"
        elif fault == "session":
            context.session_id = "other-session"
        elif fault == "backend":
            context.workspace_checkpoint_controls = None
        elif fault == "directory-backend":
            context.workspace_directory_restores = None
        elif fault == "timeout":
            context.timeout = 60
        elif fault == "source_call":
            context.source_call_id = "other-call"
        elif fault in {"source-none", "source-false"}:
            context.directory_restore = replace(
                context.directory_restore,
                model_source=None if fault == "source-none" else FalseSource(),
            )
        elif fault == "both-none":
            context.directory_restore = replace(context.directory_restore, model_source=None)
            context.directory_restore_qualify = None
        elif fault == "qualifier":
            context.directory_restore_qualify = lambda: None
        elif fault == "token-in-place":
            object.__setattr__(context.directory_restore, "token", "0" * 64)
        elif fault == "source-in-place":
            object.__setattr__(context.directory_restore.model_source, "call_id", "forged")
        elif fault == "budget":
            context.execution_timeouts[-1].deadline.reschedule(
                asyncio.get_running_loop().time() + 60
            )
        elif fault == "parent-backend":
            parent[0].workspace_directory_restores = None

    async def pre(definition, context):
        if definition.name == root:
            parent.append(context)
        if definition.name == selected and phase == "pre":
            if fault == "short":
                return ShortCircuit({})
            corrupt(context)

    async def post(definition, value, context):
        if definition.name == selected and phase == "post":
            if fault in {"budget", "parent-backend"}:
                corrupt(context)
            else:
                value = copy.deepcopy(value)
                value["request_key" if target == "root" else "target_fingerprint"] = (
                    "forged" if target == "root" else "0" * 64
                )
                if fault == "marker":
                    if target == "root":
                        context._checkpoint_control_response = (
                            "directory_restore_" + action,
                            json.dumps(value, sort_keys=True),
                        )
                    else:
                        context._directory_restore_response = json.dumps(value, sort_keys=True)
        return value

    registry = service._handle(agent.agent_id).runtime.tools
    registry.add_global_pre(pre)
    registry.add_global_post(post)
    try:
        result = await agent.execute("目录恢复原Hook边界", idempotency_key="hooks")
        assert result.status == "completed", (result, provider.reply)
        final = await agent.directory_checkpoint_restore_by_key("boundary-preview")
        if target == "root" and phase == "pre":
            assert final is None
        elif target == "root":
            assert final.phase == "ready" and final.request_key == "boundary-preview", final
        else:
            assert final.phase == (
                "uncertain" if action == "start" and phase == "post" else "failed"
            ), final
            assert final.cleanup_pending is (action == "start" and phase == "post")
        assert (binding.workspace.root / "tree/nested/nul").read_bytes() == (
            b"\0\xff" if action == "start" and phase == "post" else b"current\x80\0"
        )
        audits = await runtime.repository.audits(result.run_id)
        assert audits and any(not row.ok for row in audits), audits
        # 父控制可以成功返回真实failed/uncertain账本；拒绝审计属于受损的叶。
        expected_root_ok = target == "leaf" and fault != "parent-backend"
        assert any(
            row.name == root and row.call_id == provider.call_id and row.ok is expected_root_ok
            for row in audits
        ), audits
        if target == "leaf":
            assert any(row.name == selected and not row.ok for row in audits), audits
        assert not service.directory_restores._tasks and not service.directory_restores._fences
        assert (
            not service.directory_restores._invocations and not service.directory_restores._pending
        )
    finally:
        await service.close()
