"""原父/叶Hook篡改不得改源、延预算、伪回程或宣告可用。"""

import asyncio
import copy
import json
from dataclasses import replace

import pytest
from test_directory_checkpoint_capture import make_tree
from test_model_directory_checkpoints import CREATE, LEAF, OneShotProvider, setup, source_count

from krim_agent.tools import ShortCircuit


class FalseSource:
    def __bool__(self):
        return False


@pytest.mark.parametrize(
    "target,phase,fault",
    [
        ("root", "pre", "arguments"),
        ("root", "pre", "origin"),
        ("root", "pre", "run"),
        ("root", "pre", "call"),
        ("root", "pre", "session"),
        ("root", "pre", "backend"),
        ("root", "pre", "directory-backend"),
        ("root", "pre", "timeout"),
        ("root", "pre", "budget"),
        ("root", "pre", "short"),
        ("root", "post", "data"),
        ("root", "post", "marker"),
        ("root", "post", "budget"),
        ("leaf", "pre", "arguments"),
        ("leaf", "pre", "origin"),
        ("leaf", "pre", "source_call"),
        ("leaf", "pre", "source-none"),
        ("leaf", "pre", "source-false"),
        ("leaf", "pre", "qualifier"),
        ("leaf", "pre", "budget"),
        ("leaf", "pre", "short"),
        ("leaf", "post", "data"),
        ("leaf", "post", "marker"),
        ("leaf", "post", "budget"),
        ("leaf", "post", "parent-backend"),
    ],
)
async def test_root_leaf_source_budget_and_fact_hooks_fail_closed(
    runtime, tmp_path, target, phase, fault
):
    make_tree(tmp_path)
    service, agent = await setup(runtime, tmp_path, OneShotProvider())
    registry = service._handle(agent.agent_id).runtime.tools
    selected = CREATE if target == "root" else LEAF
    parent = []

    def corrupt(context):
        if fault == "arguments":
            context.arguments["root_path"] = "other"
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
            context.workspace_directory_checkpoints = None
        elif fault == "timeout":
            context.timeout = 60
        elif fault == "source_call":
            context.source_call_id = "other-source"
        elif fault == "source-none":
            context.directory_checkpoint_capture = replace(
                context.directory_checkpoint_capture, model_source=None
            )
        elif fault == "source-false":
            context.directory_checkpoint_capture = replace(
                context.directory_checkpoint_capture, model_source=FalseSource()
            )
        elif fault == "qualifier":
            context.directory_checkpoint_qualify = lambda: None
        elif fault == "budget":
            context.execution_timeouts[-1].deadline.reschedule(
                asyncio.get_running_loop().time() + 60
            )
        elif fault == "parent-backend":
            parent[0].workspace_directory_checkpoints = None

    async def pre(definition, context):
        if definition.name == CREATE:
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
                value["request_key" if target == "root" else "snapshot_fingerprint"] = (
                    "forged" if target == "root" else "0" * 64
                )
                if fault == "marker":
                    if target == "root":
                        context._checkpoint_control_response = (
                            "directory_create",
                            json.dumps(value, sort_keys=True),
                        )
                    else:
                        context._directory_checkpoint_capture_response = json.dumps(
                            value, sort_keys=True
                        )
        return value

    registry.add_global_pre(pre)
    registry.add_global_post(post)
    try:
        result = await agent.execute("目录原Hook边界", idempotency_key="hook")
        assert result.status == "completed", result
        final = await agent.directory_checkpoint_by_key("one-directory")
        if target == "root" and phase == "pre":
            assert final is None and await source_count(runtime) == 0
        elif target == "root":
            assert final.status == "available" and final.request_key == "one-directory"
            assert await source_count(runtime) == 1
        else:
            assert final.status == "failed" and await source_count(runtime) == 1, final
            assert (final.snapshot is not None) == (phase == "post")
        audits = await runtime.repository.audits(result.run_id)
        assert audits and all(audit.run_id == result.run_id for audit in audits)
        assert any(
            audit.name == CREATE and audit.call_id == "directory-one-call" for audit in audits
        )
        assert any(
            not audit.ok
            and audit.code
            in {
                "workspace_checkpoint_model_context",
                "workspace_checkpoint_model_unverified",
                "workspace_directory_capture_context",
                "workspace_directory_capture_model_context",
                "workspace_directory_capture_unverified",
            }
            for audit in audits
        ), audits
    finally:
        await service.close()
