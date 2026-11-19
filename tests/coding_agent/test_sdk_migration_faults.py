"""Crash-shaped host facts must not become another SDK side effect."""

from __future__ import annotations

import pytest

from krim_agent.composition import open_application
from krim_agent.config import Settings
from krim_agent.contracts import ApprovalReply, RunRequest, ToolObservation
from krim_agent.runtime.service import RunBinding, RunService
from krim_agent.tools.executor import ToolExecutor
from krim_agent.tools.registry import ToolRegistry
from tests.coding_agent.test_sdk_loop_migration import MigrationProvider, write_spec


@pytest.mark.parametrize("fact", ["intent", "unknown", "completed"])
async def test_generic_checkpoint_uses_existing_operation_facts_without_redispatch(tmp_path, fact):
    effects = []

    async def effect(args):
        effects.append(args.value)
        return args.value

    async def ask(_):
        return "ask"

    provider = MigrationProvider((("effect", "effect-id", 7),))
    async with await open_application(
        tmp_path / f"{fact}.db", provider=provider,
        registry=ToolRegistry((write_spec("effect", effect),)), policy=ask,
        settings=Settings(max_tools_per_step=1),
    ) as app:
        run = await app.service.start(RunRequest(text="go", request_key="fault"))
        assert (await app.service.wait(run.id)).status == "awaiting"
        request, = await app.service.approvals(run.id)
        checkpoint, _ = await app.store.read_checkpoint(run.id)
        invocation = checkpoint["entries"]["effect-id"]["invocation"]
        # Seed the same durable facts a crash after intention/output would leave,
        # while retaining the earlier interrupted SDK checkpoint.
        await app.store.append(run.id, "tool_intent", {
            "call_id": invocation["call_id"], "span_id": invocation["span_id"],
            "step_index": invocation["step_index"], "decision": "ask",
        })
        if fact != "intent":
            observation = ToolObservation(
                call_id=invocation["call_id"], provider_call_id="effect-id", name="effect",
                span_id=invocation["span_id"], step_index=invocation["step_index"],
                arguments={"value": 7}, status="ok" if fact == "completed" else "unknown",
                executor_started=True if fact == "completed" else None,
                result_available=True if fact == "completed" else None,
                value=7 if fact == "completed" else None, decision="ask",
            )
            await app.store.append(run.id, "tool_end", observation.model_dump(mode="json"))
        await app.service.reply_approval(run.id, request["request_id"], ApprovalReply(
            request_key="fault", call_id=request["call_id"], tool_hash=request["tool_hash"],
            arguments_hash=request["arguments_hash"], allow=True,
        ))
        result = await app.service.wait(run.id)
        assert effects == []
        if fact == "completed":
            assert result.status == "completed", result.code
            assert provider.calls == 2
        else:
            assert result.status == "failed" and result.code == "facts_corrupt"
            assert provider.calls == 1


async def test_denied_generic_tools_never_enter_the_execution_wrap(tmp_path):
    wrapped = []

    async def effect(args):
        raise AssertionError("denied effect executed")

    async def deny(_):
        return "deny"

    async def wrap(invocation, next_once):
        wrapped.append(invocation.call_id)
        return await next_once()

    provider = MigrationProvider((("effect", "effect-id", 7),))
    async with await open_application(
        tmp_path / "deny-wrap.db", provider=provider,
        registry=ToolRegistry((write_spec("effect", effect),)), policy=deny,
    ) as app:
        # Rebind the same host pipeline before admission; do not alter a live run.
        loop = app.service.bindings["demo"].loop
        loop.executor = ToolExecutor(loop.executor.registry, deny, wrap=wrap)
        service = RunService(
            app.store, loop, loop.settings, RunBinding.describe("demo", loop, loop.settings),
        )
        try:
            run = await service.start(RunRequest(text="go", request_key="deny"))
            result = await service.wait(run.id)
            assert result.status == "completed", result.code
            assert wrapped == []
            facts = await service.result(run.id)
            assert facts.tools[0].status == "denied" and facts.tools[0].executor_started is False
        finally:
            await service.close()


async def test_batch_approval_window_starts_after_all_allowed_tools_complete(tmp_path):
    async def effect(args):
        return args.value

    async def policy(invocation):
        return "ask" if invocation.name == "ask" else "allow"

    provider = MigrationProvider((("ask", "first", 1), ("ready", "second", 2)))
    async with await open_application(
        tmp_path / "approval-order.db", provider=provider,
        registry=ToolRegistry((write_spec("ask", effect), write_spec("ready", effect))),
        policy=policy, settings=Settings(max_tools_per_step=2),
    ) as app:
        run = await app.service.start(RunRequest(text="go", request_key="order"))
        result = await app.service.wait(run.id)
        assert result.status == "awaiting", result.code
        events = await app.service.events(run.id)
        ready_end = next(event.seq for event in events
                         if event.kind == "tool_end" and event.data["name"] == "ready")
        request = next(event.seq for event in events if event.kind == "approval_request")
        assert ready_end < request
