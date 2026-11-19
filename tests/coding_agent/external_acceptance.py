"""Configuration, preflight and evidence checks for opt-in external acceptance.

Imported by the runner and offline tests. Importing this module opens no network
connection and creates no host resources. Only synthetic project data is sent.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlsplit

from ai import messages as sdk
from pydantic import BaseModel, ConfigDict, Field, JsonValue, field_validator

from krim_agent.agent_sdk.host_media import HostImageMessage
from krim_agent.contracts import ServiceError, encoded
from krim_agent.mcp import MCPConnection, MCPServerConfig
from krim_agent.providers.openai_compatible import OpenAICompatibleConfig, OpenAICompatibleProvider
from krim_agent.providers.types import ModelText, ModelToolCall, ModelUsage
from krim_agent.tools.mcp import mcp_tools
from krim_agent.tools.runtime import ToolRuntimeConfig
from krim_agent.web_access import TavilyWebAccess
from tests.coding_agent.external_challenge import make_challenge, parse_vision_answer


class AcceptanceError(Exception):
    """A local, fixed message safe to persist without upstream exception text."""


class Scenario(BaseModel):
    model_config = ConfigDict(
        extra="forbid", frozen=True, allow_inf_nan=False, hide_input_in_errors=True
    )

    tool_config_file: Path
    mcp_server: str = Field(pattern=r"^[a-z][a-z0-9_-]{0,31}$")
    mcp_tool: str = Field(min_length=1)
    mcp_arguments: dict[str, JsonValue] = Field(default_factory=dict)
    mcp_resource_uri: str | None = None
    mcp_task: str = Field(min_length=1)
    mcp_scope: Literal["host", "account"] = "host"
    web_query: str = "Python csv module official documentation quoting newline"
    web_url: str = "https://docs.python.org/3/library/csv.html"
    # Harness stopping conditions, not production defaults or sandbox quotas.
    timeout_seconds: float = Field(default=900, gt=0)
    max_steps: int = Field(default=60, ge=1)
    max_tool_calls: int = Field(default=120, ge=1)

    @field_validator("web_url")
    @classmethod
    def public_reference(cls, value: str) -> str:
        parts = urlsplit(value)
        if (
            parts.scheme != "https"
            or not parts.hostname
            or parts.username is not None
            or parts.password is not None
            or parts.query
            or parts.fragment
        ):
            raise ValueError("reference must be an HTTPS URL without credentials or query")
        return value


@dataclass(frozen=True, repr=False)
class Services:
    scenario: Scenario
    provider: OpenAICompatibleConfig
    tools: ToolRuntimeConfig
    mcp: MCPServerConfig

    def summary(self) -> dict[str, Any]:
        # No URLs, credentials, tool arguments or upstream content in public output.
        return {
            "model": self.provider.model,
            "wire_api": self.provider.wire_api,
            "images": self.provider.allow_images,
            "mcp_server": self.mcp.name,
            "mcp_scope": self.scenario.mcp_scope,
            "mcp_tools": list(self.mcp.selected_tools),
            "mcp_resources_requested": self.scenario.mcp_resource_uri is not None,
            "web": "tavily-compatible",
            "external_calls_started": False,
        }


def load_services(path: Path) -> Services:
    """Load host configuration and environment references; perform no network I/O."""
    try:
        scenario = Scenario.model_validate_json(path.read_bytes())
        tools_path = scenario.tool_config_file
        if not tools_path.is_absolute():
            tools_path = path.absolute().parent / tools_path
        tools = ToolRuntimeConfig.load(tools_path)
        provider = OpenAICompatibleConfig.from_env()
    except Exception:
        raise AcceptanceError("invalid acceptance or service configuration") from None
    return validate_services(scenario, provider, tools)


def validate_services(
    scenario: Scenario,
    provider: OpenAICompatibleConfig | None,
    tools: ToolRuntimeConfig,
) -> Services:
    if provider is None or not provider.api_key.get_secret_value() or not provider.allow_images:
        raise AcceptanceError("a configured model credential and allow_images=true are required")
    if tools.web is None or tools.web.api_key is None:
        raise AcceptanceError("a Web credential is required")
    if urlsplit(tools.web.endpoint).scheme != "https":
        raise AcceptanceError("external Web acceptance requires HTTPS")
    if tools.tool_overrides or tools.public_tools is not None:
        raise AcceptanceError("acceptance requires unmodified production tool names")
    selected = [item for item in tools.mcp_servers if item.name == scenario.mcp_server]
    if len(selected) != 1:
        raise AcceptanceError("the named MCP connection must be configured")
    mcp = selected[0]
    if mcp.transport != "streamable_http" or urlsplit(mcp.url or "").scheme != "https":
        raise AcceptanceError("external MCP acceptance requires Streamable HTTP over HTTPS")
    if (
        scenario.mcp_tool not in mcp.selected_tools
        or not set(mcp.selected_tools) <= mcp.readonly_tools
    ):
        raise AcceptanceError("explicitly selected read-only MCP tools are required")
    return Services(scenario, provider, tools, mcp)


def digest(value: Any) -> str:
    return hashlib.sha256(encoded(value).encode()).hexdigest()


def failure(error: BaseException) -> dict[str, str]:
    # Never str(error): HTTP/MCP exceptions can contain headers, URLs or bodies.
    result = {
        "type": type(error).__name__,
        "code": error.code if isinstance(error, ServiceError) else "acceptance_failed",
    }
    if isinstance(error, AcceptanceError):
        result["check"] = str(error)  # Only fixed local messages use this exception.
    return result


async def preflight(services: Services) -> dict[str, Any]:
    """Use the actual production adapters, sequentially, without launching Docker."""
    result: dict[str, Any] = {}
    web = TavilyWebAccess(services.tools.web)
    try:
        search = await web.search(services.scenario.web_query, 3, scope="preflight")
        fetched = await web.fetch(services.scenario.web_url, 15000, scope="preflight")
        if not search["results"] or not fetched["results"]:
            raise AcceptanceError("Web returned no evidence")
        first = fetched["results"][0]
        if not first["content"]:
            raise AcceptanceError("Web returned empty extracted content")
        retained = await web.fetch(None, 15000, result_ref=first["result_ref"], scope="preflight")
        if retained["results"][0]["content"] != first["content"]:
            raise AcceptanceError("Web result reference did not roundtrip")
        result["web"] = {
            "passed": True,
            "search_results": len(search["results"]),
            "content_sha256": digest(first["content"]),
            "reference_read": True,
        }
    finally:
        await web.close()
    connection = MCPConnection(services.mcp)
    try:
        await connection.connect()
        value = await connection.call_tool(
            services.scenario.mcp_tool, services.scenario.mcp_arguments
        )
        if value is None or value == "":
            raise AcceptanceError("MCP tool returned empty content")
        specs = mcp_tools(
            connection, namespace=("account" if services.scenario.mcp_scope == "account" else None)
        )
        public_name = next(
            spec.name for spec in specs if spec.source.remote_name == services.scenario.mcp_tool
        )
        result["mcp"] = {
            "passed": True,
            "tool": services.scenario.mcp_tool,
            "public_name": public_name,
            "result_sha256": digest(value),
            "resources_supported": connection.supports_resources,
            "resource_read": "not_requested",
        }
        if services.scenario.mcp_resource_uri is not None:
            if not connection.supports_resources:
                raise AcceptanceError("configured MCP resource capability is absent")
            resources = await connection.list_resources()
            templates = await connection.list_resource_templates()
            resource = await connection.read_resource(services.scenario.mcp_resource_uri)
            if not resource.get("contents"):
                raise AcceptanceError("MCP resource returned no content")
            result["mcp"].update(
                resource_read="passed",
                resource_sha256=digest(resource),
                resources_listed=len(resources["resources"]),
                templates_listed=len(templates["resourceTemplates"]),
            )
    finally:
        await connection.close()
    challenge = make_challenge()
    provider = OpenAICompatibleProvider(services.provider)
    try:
        answer = ""
        usage = []
        message = HostImageMessage(
            role="user",
            content="Read the three colored rectangles left to right. "
            'Return only JSON {"order":["color","color","color"]}; '
            "use the lowercase names red, green and blue exactly once.",
            images=[
                sdk.FilePart(data=challenge.image, media_type="image/png", filename="challenge.png")
            ],
        )
        async for event in provider.stream([message], []):
            if isinstance(event, ModelText):
                answer += event.text
            elif isinstance(event, ModelUsage):
                usage.append(event.usage.model_dump(mode="json"))
            elif isinstance(event, ModelToolCall):
                raise AcceptanceError("vision preflight must not invoke tools")
        if parse_vision_answer(answer) != challenge.order:
            raise AcceptanceError("model did not identify the image correctly")
        result["vision"] = {
            "passed": True,
            "image_sha256": hashlib.sha256(challenge.image).hexdigest(),
            "answer_sha256": digest(answer),
            "usage": usage,
        }
    finally:
        await provider.close()
    return result


def successful_tools(events: list[Any]) -> list[dict[str, Any]]:
    return [
        event.data
        for event in events
        if event.kind == "tool_end" and event.data.get("status") == "ok"
    ]


def grade_evidence(
    events: list[Any],
    services: Services,
    *,
    restarted: bool,
    compactions: int,
    goal: dict[str, Any],
    plan: dict[str, Any],
    mcp_tool_name: str,
) -> dict[str, bool]:
    """Judge recorded outcomes, never a model's assertion of success."""
    ended = successful_tools(events)
    names = {item["name"] for item in ended}
    source = ("host:" if services.scenario.mcp_scope == "host" else "account:") + services.mcp.name
    checks = {
        "web_search": "web_search" in names,
        "web_fetch": "web_fetch" in names,
        "mcp_tool": mcp_tool_name in names,
        "tool_discovery": "tool_search" in names,
        "view_image": any(
            item["name"] == "view_image"
            and isinstance(item.get("value"), dict)
            and item["value"].get("status") == "ready"
            for item in ended
        ),
        "question_reopened": restarted and "ask_user" in names,
        "plan_confirmed": {"enter_plan_mode", "exit_plan_mode"} <= names,
        "plan_completed": plan.get("counts", {}).get("completed", 0) > 0
        and plan.get("counts", {}).get("pending", 0) == 0
        and plan.get("counts", {}).get("in_progress", 0) == 0,
        "goal_completed": goal.get("status") == "completed",
        "memory_written": "memory_write" in names,
        "shell_output_read": {"shell", "read_output"} <= names,
        "context_compacted": compactions > 0,
    }
    if services.scenario.mcp_resource_uri is not None:
        # Bind the resource evidence to the requested source and URI, not any read.
        checks["mcp_resource"] = any(
            item["name"] == "read_mcp_resource"
            and item.get("arguments", {}).get("server") == source
            and item.get("arguments", {}).get("uri") == services.scenario.mcp_resource_uri
            for item in ended
        )
    return checks
