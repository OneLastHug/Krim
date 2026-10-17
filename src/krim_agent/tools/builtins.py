"""保留原 add/echo 意图；资料仅为宿主固定合成数据。"""

import hashlib
from pathlib import Path
from typing import cast

from pydantic import BaseModel, Field, JsonValue

from krim_agent.contracts import StrictModel

from .core import Decision, Invocation, ToolSpec
from .registry import ToolRegistry


class AddInput(StrictModel):
    left: float
    right: float


class EchoInput(StrictModel):
    text: str = Field(max_length=8192)


class NoteInput(StrictModel):
    key: str = Field(pattern=r"^[a-z0-9_-]{1,32}$")


async def add(arguments: BaseModel) -> JsonValue:
    args = cast(AddInput, arguments)
    return {"sum": args.left + args.right}


async def echo(arguments: BaseModel) -> JsonValue:
    return cast(EchoInput, arguments).text


async def lookup_note(arguments: BaseModel) -> JsonValue:
    notes = {
        "welcome": "这是需要本次批准的合成演示资料。",
        "agent": "Agent 在有限循环中调用受控工具。",
    }
    return notes[cast(NoteInput, arguments).key]


async def demo_policy(invocation: Invocation) -> Decision:
    if invocation.name in {"add", "echo"}:
        return "allow"
    return "ask" if invocation.name == "lookup_note" else "deny"


def builtin_registry() -> ToolRegistry:
    digest = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    return ToolRegistry(
        (
            ToolSpec(
                "add",
                AddInput,
                add,
                {
                    "type": "object",
                    "required": ["sum"],
                    "properties": {"sum": {"type": "number"}},
                    "additionalProperties": False,
                },
                "1",
                digest,
            ),
            ToolSpec("echo", EchoInput, echo, {"type": "string"}, "1", digest),
            ToolSpec("lookup_note", NoteInput, lookup_note, {"type": "string"}, "1", digest),
        )
    )
