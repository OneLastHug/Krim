"""本地规则与可信回调评分器；不自动调用模型评委或远端 Schema。"""

from __future__ import annotations

import inspect
import re
from collections.abc import Awaitable, Callable
from typing import Protocol

from pydantic import Field, JsonValue, TypeAdapter

from krim_agent.protocol.models import StrictModel
from krim_agent.tools.output import OutputContract, OutputSchema

from .files import parse_json
from .types import EvalCase, EvalOutput, EvalScore, EvalScorerInfo, EvalToolCall


class EvalScorer(Protocol):
    def describe(self) -> EvalScorerInfo: ...

    async def score(self, case: EvalCase, output: EvalOutput) -> EvalScore: ...


class RuleScorer:
    def __init__(self, name: str, *, config: dict[str, JsonValue] | None = None) -> None:
        self.info = EvalScorerInfo(name=name, config=config or {})

    def describe(self) -> EvalScorerInfo:
        return self.info.model_copy(deep=True)

    def result(self, passed: bool, reason: str | None = None) -> EvalScore:
        return EvalScore(name=self.info.name, value=1.0 if passed else 0.0, reason=reason)

    def skipped(self, reason: str) -> EvalScore:
        return EvalScore(name=self.info.name, status="skipped", reason=reason)


def json_equal(left: JsonValue, right: JsonValue) -> bool:
    """JSON 数字可比较整数/浮点，布尔值不能冒充 0/1。"""
    if isinstance(left, bool) or isinstance(right, bool):
        return type(left) is type(right) and left == right
    if isinstance(left, dict) and isinstance(right, dict):
        return left.keys() == right.keys() and all(
            json_equal(value, right[key]) for key, value in left.items()
        )
    if isinstance(left, list) and isinstance(right, list):
        return len(left) == len(right) and all(
            json_equal(a, b) for a, b in zip(left, right, strict=True)
        )
    return left == right


class ExactMatchScorer(RuleScorer):
    def __init__(self, *, normalize_whitespace: bool = False) -> None:
        super().__init__("exact_match", config={"normalize_whitespace": normalize_whitespace})
        self.normalize_whitespace = normalize_whitespace

    async def score(self, case: EvalCase, output: EvalOutput) -> EvalScore:
        expected = case.expected
        if expected is None:
            return self.skipped("missing_reference")
        actual = output.value
        if self.normalize_whitespace and isinstance(actual, str) and isinstance(expected, str):
            actual, expected = (
                re.sub(r"\s+", " ", actual).strip(),
                re.sub(r"\s+", " ", expected).strip(),
            )
        return self.result(json_equal(actual, expected))


class ContainsScorer(RuleScorer):
    def __init__(self) -> None:
        super().__init__("contains")

    async def score(self, case: EvalCase, output: EvalOutput) -> EvalScore:
        expected = case.expected
        if expected is None or expected == []:
            return self.skipped("missing_reference")
        fragments = [expected] if isinstance(expected, str) else expected
        if (
            not isinstance(fragments, list)
            or not fragments
            or any(not isinstance(item, str) or not item for item in fragments)
        ):
            return EvalScore(name=self.info.name, status="error", reason="invalid_reference")
        return self.result(
            isinstance(output.value, str)
            and all(isinstance(item, str) and item in output.value for item in fragments)
        )


class JsonMatchScorer(RuleScorer):
    def __init__(self) -> None:
        super().__init__("json_match")

    async def score(self, case: EvalCase, output: EvalOutput) -> EvalScore:
        expected = case.expected
        if expected is None:
            return self.skipped("missing_reference")
        try:
            actual = parse_json(output.value) if isinstance(output.value, str) else output.value
        except ValueError:
            return self.result(False, "invalid_json")
        return self.result(json_equal(actual, expected))


class JsonSchemaScorer(RuleScorer):
    def __init__(self, schema: OutputSchema) -> None:
        self.contract = OutputContract(schema)
        super().__init__("json_schema", config={"schema": self.contract.schema()})

    async def score(self, case: EvalCase, output: EvalOutput) -> EvalScore:
        try:
            value = parse_json(output.value) if isinstance(output.value, str) else output.value
            self.contract.validate(value)
        except ValueError:
            return self.result(False, "invalid_json_or_schema")
        return self.result(True)


class SuccessScorer(RuleScorer):
    def __init__(self, *, require_tools_success: bool = True) -> None:
        super().__init__("success", config={"require_tools_success": require_tools_success})
        self.require_tools_success = require_tools_success

    async def score(self, case: EvalCase, output: EvalOutput) -> EvalScore:
        passed = output.status == case.expected_status and (
            case.expected_code is None or output.code == case.expected_code
        )
        if self.require_tools_success and output.status == "completed" and output.tools is not None:
            passed = passed and all(tool.ok is True for tool in output.tools)
        return self.result(passed)


class ConversationScorer(RuleScorer):
    def __init__(self) -> None:
        super().__init__("conversation_turns")

    async def score(self, case: EvalCase, output: EvalOutput) -> EvalScore:
        if not case.turns or not output.turns:
            return self.skipped("missing_conversation_observations")
        if len(output.turns) != len(case.turns) + 1:
            return self.result(False, "incomplete_conversation")
        return self.result(
            all(
                actual.output.status == expected.expected_status
                and (expected.expected_code is None or actual.output.code == expected.expected_code)
                and (
                    expected.expected is None or json_equal(actual.output.value, expected.expected)
                )
                for expected, actual in zip(case.turns, output.turns[1:], strict=True)
            )
        )


class ExpectedToolCall(StrictModel):
    name: str = Field(min_length=1, max_length=64)
    arguments: dict[str, JsonValue] | None = None


class ToolCallsScorer(RuleScorer):
    def __init__(self, *, ordered: bool = True, require_success: bool = True) -> None:
        super().__init__(
            "tool_calls", config={"ordered": ordered, "require_success": require_success}
        )
        self.ordered, self.require_success = ordered, require_success

    def _matches(self, expected: ExpectedToolCall, actual: EvalToolCall) -> bool:
        return (
            expected.name == actual.name
            and (expected.arguments is None or json_equal(expected.arguments, actual.arguments))
            and (not self.require_success or actual.ok is True)
        )

    async def score(self, case: EvalCase, output: EvalOutput) -> EvalScore:
        expected = case.metadata.get("expected_tools")
        if expected is None or output.tools is None:
            return self.skipped("missing_tool_reference_or_observation")
        try:
            values = TypeAdapter(list[str | ExpectedToolCall]).validate_python(
                expected, strict=True
            )
            calls = [
                ExpectedToolCall(name=value) if isinstance(value, str) else value
                for value in values
            ]
        except ValueError:
            return EvalScore(name=self.info.name, status="error", reason="invalid_tool_reference")
        if len(calls) != len(output.tools):
            return self.result(False)
        if self.ordered:
            return self.result(
                all(self._matches(a, b) for a, b in zip(calls, output.tools, strict=True))
            )
        unmatched = list(output.tools)
        for call in sorted(calls, key=lambda value: value.arguments is None):
            match = next(
                (index for index, actual in enumerate(unmatched) if self._matches(call, actual)),
                None,
            )
            if match is None:
                return self.result(False)
            unmatched.pop(match)
        return self.result(True)


ScoreFunction = Callable[
    [EvalCase, EvalOutput], EvalScore | float | bool | Awaitable[EvalScore | float | bool]
]


class CallableScorer:
    """可信同步短回调或合作式异步回调；版本/配置由调用方明确登记。"""

    def __init__(
        self,
        name: str,
        function: ScoreFunction,
        *,
        version: str = "1",
        config: dict[str, JsonValue] | None = None,
    ) -> None:
        self.info = EvalScorerInfo(name=name, version=version, config=config or {})
        self.function = function

    def describe(self) -> EvalScorerInfo:
        return self.info.model_copy(deep=True)

    async def score(self, case: EvalCase, output: EvalOutput) -> EvalScore:
        value = self.function(case, output)
        if inspect.isawaitable(value):
            value = await value
        if isinstance(value, EvalScore):
            return value
        if not isinstance(value, (int, float, bool)):
            raise ValueError("评分回调必须返回有限数值、布尔值或 EvalScore")
        return EvalScore(name=self.info.name, value=float(value))


def builtin_scorer(name: str) -> EvalScorer:
    """只返回服务端内建评分器；远端请求不能反序列化 Python callable。"""
    values: dict[str, Callable[[], EvalScorer]] = {
        "exact-match": ExactMatchScorer,
        "contains": ContainsScorer,
        "json-match": JsonMatchScorer,
        "success": SuccessScorer,
        "tool-calls": ToolCallsScorer,
        "conversation-turns": ConversationScorer,
    }
    factory = values.get(name)
    if factory is None:
        raise ValueError("不支持的内建评估评分器")
    return factory()


def builtin_scorers(names: list[str]) -> list[EvalScorer]:
    if not names or len(names) > 32 or len(set(names)) != len(names):
        raise ValueError("评估评分器名称必须唯一且不超过32个")
    return [builtin_scorer(name) for name in names]
