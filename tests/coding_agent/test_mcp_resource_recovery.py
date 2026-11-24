"""Missing local MCP sources return a correction path without ending the SDK run."""

import json

import ai
from ai import events, messages

from krim_agent.agent_sdk import SDKAdapter
from krim_agent.security import LOCAL
from krim_agent.tools.executor import ToolExecutor
from krim_agent.tools.mcp_resources import MCPResourceService
from krim_agent.tools.registry import ToolRegistry
from krim_agent.tools.session import SessionToolScope
from tests.coding_agent.sdk_fixtures import Scripted, done, model, setup


def resource_call(name, call_id, arguments):
    raw = json.dumps(arguments)
    part = messages.ToolCallPart(tool_call_id=call_id, tool_name=name, tool_args=raw)
    return [
        events.StreamStart(),
        events.ToolStart(tool_call_id=call_id, tool_name=name),
        events.ToolDelta(tool_call_id=call_id, chunk=raw),
        events.ToolEnd(tool_call_id=call_id, tool_call=part),
        events.StreamEnd(finish_reason="tool_call"),
    ]


async def test_missing_source_then_discovery_continues_same_fail_on_tool_error_run():
    async def resolve(_call):
        return SessionToolScope(LOCAL, "local", "session", "turn", "run")

    @ai.tool
    async def read_mcp_resource(server: str, uri: str, max_chars: int = 1000):
        return None

    @ai.tool
    async def list_mcp_resources():
        return None

    bundle = MCPResourceService(()).bundle(resolve)
    harness = setup(approval=False)
    adapter = SDKAdapter(
        store=harness.store, operations=harness.operations,
        executor=ToolExecutor(ToolRegistry(bundle.specs), bundle.policy),
        tools=[read_mcp_resource, list_mcp_resources],
        environment=harness.environment, ownership=harness.ownership,
        read_result=harness.store.read_result,
        approval_request=harness.adapter().approval_request, fail_on_tool_error=True,
    )
    provider = Scripted([
        resource_call("read_mcp_resource", "missing-source", {
            "server": "skills", "uri": "catalog", "max_chars": 1000,
        }),
        resource_call("list_mcp_resources", "discover-sources", {}),
        done(),
    ])

    result = await adapter.run(model(provider), [ai.user_message("Continue the project")])

    assert result.status == "completed" and len(provider._seen) == 3
    missing = next(part.result for message in provider._seen[1]
                   for part in message.tool_results if part.tool_call_id == "missing-source")
    assert missing["value"]["ok"] is False
    assert missing["value"]["code"] == "not_found"
    assert "list_mcp_resources" in missing["value"]["next_action"]
    discovered = next(part.result for message in provider._seen[2]
                      for part in message.tool_results if part.tool_call_id == "discover-sources")
    assert discovered["value"]["sources"] == []
    assert len([event for event in harness.store.events if event.kind == "tool_end"]) == 2
    assert harness.operations.trace == []
