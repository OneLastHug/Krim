"""Behavioral migration tests using the real pinned SDK and SQLite host facts."""

from __future__ import annotations

import asyncio
from dataclasses import replace

import pytest

from krim_agent.composition import open_application
from krim_agent.config import Settings
from krim_agent.contracts import ApprovalReply, RunRequest, ServiceError, fingerprint
from krim_agent.providers.types import ModelText, ModelToolCall
from krim_agent.runtime.recovery import sdk_resume
from krim_agent.tools.core import ToolSource, ToolSpec
from krim_agent.tools.registry import ToolRegistry
from tests.coding_agent.sdk_fixtures import Args


def write_spec(name, execute):
    async def unavailable(_):
        raise AssertionError("use the registered external executor")

    async def external(_invocation, args):
        return await execute(args)

    return ToolSpec(
        name, Args, unavailable, {"type": "integer"}, "1", "b" * 64, risk="write",
        external_executor=external, source=ToolSource("mcp", "migration-fixture", name, "c" * 64),
    )


class MigrationProvider:
    def __init__(self, calls=()):
        self.calls = 0
        self.batch = calls
        self.histories = []

    async def stream(self, messages, tools):
        self.calls += 1
        self.histories.append(list(messages))
        if self.calls == 1 and self.batch:
            for name, identifier, value in self.batch:
                yield ModelToolCall(id=identifier, name=name, arguments={"value": value})
        else:
            yield ModelText(text="done")

    async def close(self):
        pass


async def test_read_groups_are_bounded_writes_wait_and_results_keep_declaration_order(tmp_path):
    entered, release = asyncio.Event(), asyncio.Event()
    active = 0
    maximum = 0
    completed = []
    writes = []

    async def read(args):
        nonlocal active, maximum
        active += 1
        maximum = max(maximum, active)
        if active == 2:
            entered.set()
        await release.wait()
        # Deliberately finish the second declaration first.
        if args.value == 1:
            await asyncio.sleep(0.01)
        active -= 1
        completed.append(args.value)
        return args.value

    async def write(args):
        assert active == 0 and len(completed) >= 3
        writes.append(args.value)
        return args.value

    async def allow(_):
        return "allow"

    schema = {"type": "integer"}
    registry = ToolRegistry((
        ToolSpec("read", Args, read, schema, "1", "a" * 64, concurrency="parallel"),
        write_spec("write", write),
    ))
    provider = MigrationProvider((
        ("read", "r1", 1), ("read", "r2", 2), ("read", "r3", 3), ("write", "w1", 4),
    ))
    async with await open_application(
        tmp_path / "parallel.db", provider=provider, registry=registry, policy=allow,
        settings=Settings(max_tools_per_step=4, read_parallelism=2),
    ) as app:
        run = await app.service.start(RunRequest(text="go", request_key="parallel"))
        try:
            await asyncio.wait_for(entered.wait(), 3)
            assert not writes
        finally:
            release.set()
        finished = await app.service.wait(run.id)
        assert finished.status == "completed", finished.code
        assert maximum == 2 and writes == [4]
        assert completed[:2] == [2, 1]
        assert [m.call_id for m in provider.histories[1] if m.role == "tool"] == [
            "r1", "r2", "r3", "w1",
        ]


async def test_multiple_asks_park_together_and_resume_without_repeating_allowed_effect(tmp_path):
    effects = []

    async def write(args):
        effects.append(args.value)
        return args.value

    async def policy(invocation):
        return "allow" if invocation.name == "ready" else "ask"

    spec = write_spec("ready", write)
    provider = MigrationProvider((("ready", "ready", 0), ("ask", "a1", 1), ("ask", "a2", 2)))
    async with await open_application(
        tmp_path / "asks.db", provider=provider,
        registry=ToolRegistry((spec, replace(spec, name="ask"))), policy=policy,
        settings=Settings(max_tools_per_step=3),
    ) as app:
        run = await app.service.start(RunRequest(text="go", request_key="asks"))
        parked = await app.service.wait(run.id)
        assert parked.status == "awaiting", parked.code
        requests = await app.service.approvals(run.id)
        assert len(requests) == 2 and effects == [0] and provider.calls == 1
        assert requests[0]["expires_at"] == requests[1]["expires_at"]
        saved, stored = await app.store.read_checkpoint(run.id)
        assert saved["format"] == "sdk-host-v2" and fingerprint(saved) == stored
        for request in requests:
            await app.service.reply_approval(run.id, request["request_id"], ApprovalReply(
                request_key="asks", call_id=request["call_id"], tool_hash=request["tool_hash"],
                arguments_hash=request["arguments_hash"], allow=True,
            ))
        finished = await app.service.wait(run.id)
        assert finished.status == "completed", finished.code
        assert effects == [0, 1, 2] and provider.calls == 2


@pytest.mark.parametrize("old_state", ["awaiting", "accepted", "running", "unknown"])
def test_legacy_dispatch_facts_cannot_be_converted_to_sdk_replay(old_state):
    old = {"schema_version": 1, "state": old_state, "pending": [{"call_id": "old-effect"}]}
    with pytest.raises(ServiceError, match="facts_corrupt"):
        sdk_resume(old, fingerprint(old), {"old-effect": True})
