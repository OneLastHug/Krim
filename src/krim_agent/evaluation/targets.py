"""三个可信宿主版本及唯一RunTarget；不建立第二RunService。"""

import hashlib
from dataclasses import replace
from pathlib import Path
from typing import cast

from pydantic import BaseModel, JsonValue

from krim_agent.config import Settings
from krim_agent.contracts import RunView
from krim_agent.providers.eval_script import EvalScriptProvider
from krim_agent.runtime.contracts import RunStore
from krim_agent.runtime.engine import AgentLoop
from krim_agent.runtime.service import RunBinding, RunService
from krim_agent.tools.builtins import AddInput, builtin_registry
from krim_agent.tools.core import Decision, Invocation
from krim_agent.tools.executor import ToolExecutor
from krim_agent.tools.registry import ToolRegistry


async def controlled_policy(invocation: Invocation) -> Decision:
    return "allow" if invocation.name in {"add", "echo"} else "deny"


async def bad_add(arguments: BaseModel) -> JsonValue:
    args = cast(AddInput, arguments)
    return {
        "sum": args.left
        + args.right
        + (1 if args.left.is_integer() and args.right.is_integer() else 0)
    }


def make_eval_bindings(store: RunStore, settings: Settings) -> dict[str, RunBinding]:
    result = {}
    for key in ["target-v1", "target-v2", "target-bad"]:
        provider = EvalScriptProvider("integer" if key == "target-v1" else "decimal")
        registry = builtin_registry()
        if key == "target-bad":
            registry = ToolRegistry(
                tuple(
                    replace(
                        spec,
                        executor=bad_add,
                        version="bad-add/1",
                        implementation_digest=hashlib.sha256(
                            Path(__file__).read_bytes()
                        ).hexdigest(),
                    )
                    if spec.name == "add"
                    else spec
                    for spec, _ in registry._frozen
                )
            )
        config = settings.model_copy(
            update={
                "system_prompt": "使用整数加法。" if key == "target-v1" else "保留小数精度。",
                "target_version": key + "/1",
                "max_input_bytes": min(4096, settings.max_input_bytes),
                "max_output_bytes": min(8192, settings.max_output_bytes),
                "max_request_bytes": min(32768, settings.max_request_bytes),
                "max_steps": min(4, settings.max_steps),
                "max_tools_per_step": 1,
                "max_tool_calls": 1,
                "run_timeout": min(10, settings.run_timeout),
                "tool_timeout": min(2, settings.tool_timeout),
            }
        )
        loop = AgentLoop(store, provider, ToolExecutor(registry, controlled_policy), config)
        result[key] = RunBinding.freeze(key, loop, config)
    return result


class RunTarget:
    def __init__(self, service: RunService) -> None:
        self.service = service

    async def start(self, eval_id: str, case_id: str, repetition: int) -> RunView:
        return await self.service.start_child(eval_id, case_id, repetition)
