"""会话模式在宿主能力范围内裁决；auto 不授予额外权限。"""

from typing import Literal

from krim_agent.tools.core import Decision, Policy, ToolContext, ToolDefinition


def profile_policy(mode: Literal["agent", "ask", "plan", "auto"], host: Policy | None) -> Policy:
    async def decide(definition: ToolDefinition, context: ToolContext) -> Decision:
        if mode == "ask" or (mode == "plan" and definition.risk != "read"):
            return "deny"
        result: Decision = (
            await host(definition, context)
            if host
            else ("allow" if definition.risk == "read" else "deny")
        )
        if mode == "auto" and result == "allow":
            return "audit"
        return result

    return decide
