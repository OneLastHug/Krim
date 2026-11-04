import asyncio
import time
from types import SimpleNamespace

import pytest

from krim_agent.contracts import ServiceError, encoded, fingerprint
from krim_agent.runtime.service import RunService
from krim_agent.tools.core import Invocation


class ApprovalStore:
    def __init__(self, target):
        self.target = target
        self.appended = []
        self.requested = asyncio.Event()

    async def snapshot(self, run_id):
        assert run_id == "run-1"
        return SimpleNamespace(
            target=self.target,
            request=SimpleNamespace(target="demo"),
            run=SimpleNamespace(created_at=time.time()),
            events=(),
            work=None,
        )

    async def append(self, run_id, kind, data):
        assert run_id == "run-1"
        assert kind == "approval_request"
        self.appended.append(data)
        self.requested.set()


def service_with(specs):
    service = object.__new__(RunService)
    service.store = ApprovalStore({"tools": specs})
    service.settings = SimpleNamespace(approval_timeout=30, run_timeout=60)
    service._pending = {}
    service.bindings = {}
    return service


def invocation(tool_hash):
    return Invocation(
        run_id="run-1",
        call_id="internal-call",
        span_id="span-1",
        step_index=0,
        name="lookup_note",
        arguments_json=encoded({"query": "synthetic"}),
        tool_hash=tool_hash,
        provider_call_id="provider-call",
    )


@pytest.mark.parametrize(
    ("spec", "expected"),
    [
        (
            {"name": "lookup_note", "risk": "read", "parameters": {"type": "object"}},
            {"risk": "read"},
        ),
        (
            {
                "name": "lookup_note",
                "risk": "write",
                "source": {"kind": "mcp", "server": "synthetic", "tool": "lookup"},
                "parameters": {"type": "object"},
            },
            {
                "risk": "write",
                "source": {"kind": "mcp", "server": "synthetic", "tool": "lookup"},
            },
        ),
        ({"name": "lookup_note", "parameters": {"type": "object"}}, {}),
    ],
)
async def test_approval_request_adds_only_frozen_matching_risk_and_source(spec, expected):
    service = service_with([spec])
    call = invocation(fingerprint(spec))
    task = asyncio.create_task(service._approve(call, 10))
    try:
        await asyncio.wait_for(service.store.requested.wait(), timeout=1)
        record = service.store.appended[0]
        assert {key: record[key] for key in expected} == expected
        assert ("risk" in record) is ("risk" in expected)
        assert ("source" in record) is ("source" in expected)
        service._pending[record["request_id"]].set_result(True)
        assert await task is True
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_approval_rejects_same_name_tool_when_frozen_hash_differs():
    spec = {"name": "lookup_note", "risk": "read", "parameters": {"type": "object"}}
    service = service_with([spec])
    with pytest.raises(ServiceError, match="target_binding_changed"):
        await service._approve(invocation("0" * 64), 10)
    assert service.store.appended == []
    assert service._pending == {}


async def test_approval_does_not_match_another_tool_by_name_alone():
    service = service_with([{"name": "other_tool", "risk": "write", "parameters": {}}])
    call = invocation("f" * 64)
    task = asyncio.create_task(service._approve(call, 10))
    try:
        await asyncio.wait_for(service.store.requested.wait(), timeout=1)
        record = service.store.appended[0]
        assert "risk" not in record and "source" not in record
        service._pending[record["request_id"]].set_result(False)
        assert await task is False
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
