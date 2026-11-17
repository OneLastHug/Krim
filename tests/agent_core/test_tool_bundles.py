"""Composition retains tool authority and has no implicit catalog cap."""

import pytest
from pydantic import BaseModel

from krim_agent.contracts import StrictModel
from krim_agent.tools.bundles import compose_executor
from krim_agent.tools.core import Invocation, ToolSpec
from krim_agent.tools.executor import ToolExecutor
from krim_agent.tools.registry import ToolRegistry
from krim_agent.tools.session import ToolBundle


class Empty(StrictModel):
    pass


async def run(_value: BaseModel):
    return {}


def tool(name):
    return ToolSpec(name, Empty, run, {"type": "object"}, "test/1", "a" * 64)


def test_catalog_accepts_more_than_old_limit():
    registry = ToolRegistry(tuple(tool(f"read_{i}") for i in range(80)))
    assert len(registry.schemas()) == 80


def test_duplicate_name_is_still_rejected():
    with pytest.raises(ValueError):
        ToolRegistry((tool("read_a"), tool("read_a")))


@pytest.mark.asyncio
async def test_bundle_policy_retains_each_source_and_guard():
    async def deny(_call):
        return "deny"

    async def allow(_call):
        return "allow"

    async def guard(call):
        return "ask" if call.name == "read_new" else "allow"

    async def pre(_call):
        return None

    async def post(_call, _result):
        pass

    async def middleware(_call):
        return None

    old, new = tool("read_old"), tool("read_new")
    base = ToolExecutor(ToolRegistry((old,)), deny, pre, post, middleware=(middleware,))
    combined = compose_executor(base, (ToolBundle((new,), allow),), guard=guard)
    assert combined.pre is pre and combined.post is post
    assert combined.middleware == (middleware,)
    assert combined.policy is not None
    old_call = Invocation("r", "c", "s", 0, old.name, "{}", old.fingerprint)
    new_call = Invocation("r", "d", "s", 0, new.name, "{}", new.fingerprint)
    assert await combined.policy(old_call) == "deny"
    assert await combined.policy(new_call) == "ask"


def test_bundle_cannot_shadow_existing_tool():
    async def allow(_call):
        return "allow"

    existing = tool("read_existing")
    with pytest.raises(ValueError):
        compose_executor(ToolExecutor(ToolRegistry((existing,)), allow),
                         (ToolBundle((tool(existing.name),), allow),))
