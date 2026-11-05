import time
from types import SimpleNamespace

import pytest

from krim_agent.contracts import ServiceError, encoded, fingerprint
from krim_agent.runtime.service import RunService
from krim_agent.tools.core import Invocation


def service_with(specs):
    service = object.__new__(RunService)
    service.settings = SimpleNamespace(approval_timeout=1, run_timeout=60)
    service.bindings = {"demo": SimpleNamespace(
        settings=SimpleNamespace(approval_timeout=30))}
    return service


class FrozenTools:
    """approval_request 的目标档案读侧替身：只提供 tools 声明。"""

    def __init__(self, tools):
        self.tools = tools


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


def provenance_store(specs):
    from types import SimpleNamespace

    return SimpleNamespace(
        target={"tools": specs},
        request=SimpleNamespace(target="demo"),
        run=SimpleNamespace(created_at=time.time()),
        events=(),
        work=None,
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
    service.store = store_with([spec])
    call = invocation(fingerprint(spec))
    record = await service._approval_request(call, None)
    assert {key: record[key] for key in expected} == expected
    assert ("risk" in record) is ("risk" in expected)
    assert ("source" in record) is ("source" in expected)


def store_with(specs):
    """异步 snapshot 替身：_approval_request 会 await 它。"""
    from types import SimpleNamespace

    async def snapshot(run_id):
        return provenance_store(specs)

    return SimpleNamespace(snapshot=snapshot)



async def test_approval_window_uses_frozen_binding_over_service_settings():
    spec = {"name": "lookup_note", "risk": "read", "parameters": {"type": "object"}}
    service = service_with([spec])
    service.store = store_with([spec])
    call = invocation(fingerprint(spec))
    record = await service._approval_request(call, None)
    # 服务级 settings.approval_timeout=1 被忽略；冻结绑定的 30 秒生效（ADR-0015）。
    expires_in = record["expires_at"] - time.time()
    assert 29 < expires_in <= 30


async def test_approval_rejects_same_name_tool_when_frozen_hash_differs():
    spec = {"name": "lookup_note", "risk": "read", "parameters": {"type": "object"}}
    service = service_with([spec])
    service.store = store_with([spec])
    with pytest.raises(ServiceError, match="target_binding_changed"):
        await service._approval_request(invocation("0" * 64), None)


async def test_approval_does_not_match_another_tool_by_name_alone():
    service = service_with([{"name": "other_tool", "risk": "write", "parameters": {}}])
    service.store = store_with([{"name": "other_tool", "risk": "write", "parameters": {}}])
    call = invocation("f" * 64)
    record = await service._approval_request(call, None)
    assert "risk" not in record and "source" not in record
