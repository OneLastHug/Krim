"""安全的 Python 内建工具包：plan/todo/question/skills。"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Annotated, Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, JsonValue, model_validator

from .core import ToolContext, ToolDefinition, ToolError


class PlanInput(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    operation: Literal["get", "replace", "complete"] = "get"
    items: list[Annotated[str, Field(min_length=1, max_length=200)]] = Field(
        default_factory=list, max_length=100
    )
    item: str = Field(default="", max_length=200)


class TodoInput(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    operation: Literal["list", "add", "complete", "remove"] = "list"
    item: str = Field(default="", max_length=200)
    item_id: str = Field(default="", max_length=64)


class QuestionInput(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    question: str = Field(min_length=1, max_length=2000)
    options: list[Annotated[str, Field(min_length=1, max_length=200)]] = Field(
        default_factory=list, max_length=20
    )


class SkillsInput(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    operation: Literal["list", "get", "put", "remove"] = "list"
    name: str | None = Field(default=None, pattern=r"^[a-z][a-z0-9_-]{0,63}$")
    content: str = Field(default="", max_length=32768)

    @model_validator(mode="after")
    def named_operation(self) -> SkillsInput:
        if self.operation != "list" and self.name is None:
            raise ValueError("技能 get/put/remove 需要 name")
        return self


@dataclass
class BuiltinPackState:
    plan: list[str] = field(default_factory=list)
    todo: list[dict[str, JsonValue]] = field(default_factory=list)
    skills: dict[str, str] = field(default_factory=dict)
    pending_questions: list[dict[str, JsonValue]] = field(default_factory=list)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


def builtin_pack(
    state: BuiltinPackState | None = None,
) -> tuple[list[ToolDefinition], BuiltinPackState]:
    state = state or BuiltinPackState()

    async def plan(args: BaseModel, _context: ToolContext) -> object:
        payload = PlanInput.model_validate(args)
        async with state.lock:
            if payload.operation == "replace":
                state.plan = list(payload.items)
            elif payload.operation == "complete":
                if payload.item not in state.plan:
                    raise ToolError("plan_item_not_found", "计划项不存在")
                state.plan.remove(payload.item)
            return {"items": list(state.plan), "operation": payload.operation}

    async def todo(args: BaseModel, _context: ToolContext) -> object:
        payload = TodoInput.model_validate(args)
        async with state.lock:
            if payload.operation == "add":
                if not payload.item:
                    raise ValueError("todo add 需要 item")
                entry: dict[str, JsonValue] = {
                    "id": str(uuid4()),
                    "item": payload.item,
                    "completed": False,
                }
                state.todo.append(entry)
            elif payload.operation == "complete":
                if not any(entry["id"] == payload.item_id for entry in state.todo):
                    raise ToolError("todo_not_found", "待办项不存在")
                for entry in state.todo:
                    if entry["id"] == payload.item_id:
                        entry["completed"] = True
            elif payload.operation == "remove":
                if not any(entry["id"] == payload.item_id for entry in state.todo):
                    raise ToolError("todo_not_found", "待办项不存在")
                state.todo = [entry for entry in state.todo if entry["id"] != payload.item_id]
            return {"items": [dict(entry) for entry in state.todo]}

    async def question(args: BaseModel, _context: ToolContext) -> object:
        payload = QuestionInput.model_validate(args)
        if _context.question_handler is not None:
            async with _context.human_wait():
                answer = await _context.question_handler(
                    payload.question, payload.options, _context
                )
            return {"status": "answered", "question": payload.question, "answer": answer}
        async with state.lock:
            if len(state.pending_questions) >= 128:
                raise ToolError("question_limit", "待处理问题超过容量")
            item: dict[str, JsonValue] = {
                "id": str(uuid4()),
                "question": payload.question,
                "options": list(payload.options),
                "status": "pending",
            }
            state.pending_questions.append(item)
            return {"status": "pending", "question": item}

    async def skills(args: BaseModel, _context: ToolContext) -> object:
        payload = SkillsInput.model_validate(args)
        async with state.lock:
            if payload.operation == "put":
                assert payload.name is not None
                state.skills[payload.name] = payload.content
            elif payload.operation == "remove":
                assert payload.name is not None
                if payload.name not in state.skills:
                    raise ToolError("skill_not_found", "技能不存在")
                state.skills.pop(payload.name, None)
            elif payload.operation == "get":
                assert payload.name is not None
                return {
                    "name": payload.name,
                    "content": state.skills.get(payload.name),
                    "found": payload.name in state.skills,
                }
            return {"skills": sorted(state.skills)}

    return [
        ToolDefinition("plan", "管理当前计划；状态保存在宿主内存", PlanInput, plan, "write"),
        ToolDefinition("todo", "管理待办事项；状态保存在宿主内存", TodoInput, todo, "write"),
        ToolDefinition(
            "question", "记录待用户回答的问题，不伪造回答", QuestionInput, question, "write"
        ),
        ToolDefinition("skills", "管理已显式提供的内存技能正文", SkillsInput, skills, "write"),
    ], state
