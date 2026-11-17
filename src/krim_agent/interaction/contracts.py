"""Durable user input and plan confirmation wire contracts."""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Literal

from pydantic import Field, model_validator

from krim_agent.contracts import StrictModel


class Option(StrictModel):
    id: str = Field(min_length=1)
    label: str = Field(min_length=1)
    description: str = ""


class Question(StrictModel):
    id: str = Field(min_length=1)
    prompt: str = Field(min_length=1)
    mode: Literal["single", "multiple", "text"] = "single"
    options: list[Option] = Field(default_factory=list)
    allow_text: bool = True

    @model_validator(mode="after")
    def valid_options(self) -> Question:
        if len({item.id for item in self.options}) != len(self.options):
            raise ValueError("Duplicate option id")
        if self.mode == "text" and self.options:
            raise ValueError("Text questions cannot have options")
        if self.mode != "text" and not self.options:
            raise ValueError("Choice questions require options")
        return self


class AskUserArgs(StrictModel):
    questions: list[Question] = Field(min_length=1)

    @model_validator(mode="after")
    def unique_questions(self) -> AskUserArgs:
        if len({item.id for item in self.questions}) != len(self.questions):
            raise ValueError("Duplicate question id")
        return self


class EnterPlanModeArgs(StrictModel):
    reason: str = ""


class ExitPlanModeArgs(StrictModel):
    plan: str = Field(min_length=1)


class Mutation(StrictModel):
    request_key: str = Field(min_length=1, max_length=128, pattern=r"^[!-~]+$")


class Answer(StrictModel):
    question_id: str = Field(min_length=1)
    option_ids: list[str] = Field(default_factory=list)
    text: str | None = None

    @model_validator(mode="after")
    def unique_options(self) -> Answer:
        if len(set(self.option_ids)) != len(self.option_ids):
            raise ValueError("Duplicate selected option")
        return self


class AnswerRequest(Mutation):
    expected_revision: int = Field(ge=1)
    answers: list[Answer] = Field(default_factory=list)
    confirmed: bool | None = None


class CancelRequest(Mutation):
    expected_revision: int = Field(ge=1)
    reason: str = ""


class RequestPending(Exception):
    """Persisted control-flow signal; never a tool error or blocking wait.

    Catch before generic exception handling. Persist ``request`` in the host
    parked-run descriptor. Resume with InteractionService.resume(), which
    rechecks ownership and raises this signal again if still unanswered.
    """

    def __init__(self, request: dict[str, Any]) -> None:
        self.request = deepcopy(request)
        self.request_id: str = request["id"]
        self.kind: str = request["kind"]
        super().__init__(self.kind)
