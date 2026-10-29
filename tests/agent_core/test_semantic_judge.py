from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Sequence
from typing import Any, Literal, cast

import pytest
from pydantic import ValidationError

from krim_agent.contracts import Message
from krim_agent.evaluation.domain import Budget, TrialSpec
from krim_agent.evaluation.metrics import MetricRegistry
from krim_agent.evaluation.runner import ItemWork, score_observation
from krim_agent.evaluation.semantic_judge import (
    ManualSemanticLabel,
    calibrate_threshold,
    register_semantic_scorer,
)
from krim_agent.providers.types import ModelEvent, ModelText, ModelToolCall


class SyntheticProvider:
    def __init__(self, *events: ModelEvent, wait: asyncio.Event | None = None) -> None:
        self.events = events
        self.wait = wait
        self.calls = 0
        self.streams_closed = 0
        self.tools: list[dict[str, Any]] | None = None
        self.messages: Sequence[Message] | None = None
        self.closed = False
        self.started = asyncio.Event()

    async def stream(
        self, messages: Sequence[Message], tools: list[dict[str, Any]]
    ) -> AsyncIterator[ModelEvent]:
        self.calls += 1
        self.messages = tuple(messages)
        self.tools = tools
        self.started.set()
        try:
            if self.wait is not None:
                await self.wait.wait()
            for event in self.events:
                yield event
        finally:
            self.streams_closed += 1

    async def close(self) -> None:
        self.closed = True


def sample_case(**changes: Any) -> dict[str, Any]:
    case = {
        "instruction": "按参考核对本次结果是否完整、准确。",
        "reference": {"expected": "账目已对平", "exit_code": 0},
    }
    case.update(changes)
    return case


def sample_output(**changes: Any) -> dict[str, Any]:
    output = {
        "cleanup": "closed",
        "exit_code": 0,
        "stdout": "账目已对平",
        "stderr": "private diagnostic, must not be sent",
        "process": {"artifact_error": False, "output_complete": True},
        "artifacts": {},
    }
    output.update(changes)
    return output


def register(
    provider: SyntheticProvider, *, threshold: int = 700
) -> tuple[MetricRegistry, dict[str, Any]]:
    registry = MetricRegistry()
    spec = register_semantic_scorer(
        registry,
        provider=provider,
        provider_id="synthetic",
        model_id="local-test",
        rubric="按用户意图、参考结果和实际结果评估语义一致性。",
        pass_threshold_milli=threshold,
    )
    return registry, spec.model_dump(mode="json")


def test_registered_semantic_metric_is_explicitly_available_in_target_catalogs():
    registry = MetricRegistry()
    groups = ("reference", "native", "krim")
    before = {group: {spec.name for spec in registry.specs(group)} for group in groups}
    assert all("semantic_judge" not in names for names in before.values())
    provider = SyntheticProvider()
    spec = register_semantic_scorer(
        registry,
        provider=provider,
        provider_id="local",
        model_id="fixture",
        rubric="核对人工参考与实际答案。",
    )
    for group in groups:
        assert {item.name for item in registry.specs(group)} == before[group] | {spec.name}
    assert provider.calls == 0
    with pytest.raises(ValueError, match="metric_group"):
        registry.specs("unknown")


@pytest.mark.asyncio
async def test_registry_extension_scores_frozen_bounded_input_without_tools() -> None:
    provider = SyntheticProvider(ModelText(text='{"score_milli":900,"rationale":"结果与参考一致"}'))
    registry, rule = register(provider, threshold=800)

    assert provider.calls == 0  # 注册不会主动发起外部请求
    result = await registry.score(rule, sample_case(), sample_output())

    assert result["status"] == "scored"
    assert result["value"] == 1
    assert result["rule"]["name"] == "semantic_judge"
    assert provider.calls == 1
    assert provider.tools == []
    assert provider.streams_closed == 1
    assert provider.closed is False  # Provider生命周期仍由宿主拥有
    assert provider.messages is not None
    assert [message.role for message in provider.messages] == ["system", "user"]
    sent = json.loads(provider.messages[1].content)
    assert sent["instruction"] == sample_case()["instruction"]
    assert sent["result"]["stdout"] == "账目已对平"
    assert "stderr" not in provider.messages[1].content
    assert "private diagnostic" not in provider.messages[1].content


@pytest.mark.asyncio
async def test_fitted_reject_all_threshold_remains_usable() -> None:
    provider = SyntheticProvider(ModelText(text='{"score_milli":1000,"rationale":"top score"}'))
    registry, rule = register(provider, threshold=1001)

    result = await registry.score(rule, sample_case(), sample_output())

    assert result["status"] == "scored"
    assert result["value"] == 0
    assert provider.calls == 1


@pytest.mark.asyncio
async def test_judge_payload_is_frozen_before_provider_await() -> None:
    wait = asyncio.Event()
    provider = SyntheticProvider(
        ModelText(text='{"score_milli":900,"rationale":"frozen"}'), wait=wait
    )
    registry, rule = register(provider)
    case, output = sample_case(), sample_output()
    original_instruction, original_stdout = case["instruction"], output["stdout"]
    task = asyncio.create_task(registry.score(rule, case, output))
    await provider.started.wait()
    case["instruction"] = "mutated after request began"
    output["stdout"] = "mutated after request began"
    wait.set()

    result = await task

    assert result["status"] == "scored"
    assert provider.messages is not None
    payload = json.loads(provider.messages[1].content)
    assert payload["instruction"] == original_instruction
    assert payload["result"]["stdout"] == original_stdout


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("case", "output", "reason"),
    [
        (sample_case(), sample_output(cleanup="unknown"), "execution_unknown"),
        (
            sample_case(reference={"expected": "账目已对平", "files": {"out.txt": "ok"}}),
            sample_output(artifacts={"out.txt": {"text": "mismatch"}}),
            "hard_gate_failed",
        ),
        (
            sample_case(),
            sample_output(process={"artifact_error": True, "output_complete": True}),
            "hard_gate_failed",
        ),
        (
            sample_case(reference={"expected": "expected text", "exit_code": 0}),
            sample_output(target_result={"status": "completed", "text": "different text"}),
            "hard_gate_failed",
        ),
        (
            sample_case(),
            sample_output(native={"complete": True, "tools": []}),
            "hard_gate_failed",
        ),
        (
            sample_case(_continuous={"known_hard_failure": True}),
            sample_output(),
            "hard_gate_failed",
        ),
        (sample_case(instruction=""), sample_output(), "instruction_missing"),
    ],
)
async def test_missing_or_failed_hard_gates_skip_provider(
    case: dict[str, Any], output: dict[str, Any], reason: str
) -> None:
    provider = SyntheticProvider(ModelText(text='{"score_milli":1000,"rationale":"pass"}'))
    registry, rule = register(provider)

    result = await registry.score(rule, case, output)

    assert result["status"] == "skipped"
    assert result["value"] is None
    assert result["reason"] == reason
    assert provider.calls == 0
    assert provider.streams_closed == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "event",
    [
        ModelText(text='{"score_milli":1,"score_milli":1000,"rationale":"duplicate"}'),
        ModelText(text='{"score_milli":1001,"rationale":"out of range"}'),
        ModelToolCall(id="call-1", name="lookup", arguments={}),
    ],
)
async def test_bad_json_or_tool_event_fails_without_retry(event: ModelEvent) -> None:
    provider = SyntheticProvider(event)
    registry, rule = register(provider)

    result = await registry.score(rule, sample_case(), sample_output())

    assert result["status"] == "error"
    assert result["value"] is None
    assert provider.calls == 1
    assert provider.tools == []
    assert provider.streams_closed == 1


@pytest.mark.asyncio
async def test_runner_scorer_timeout_cancels_and_closes_real_stream() -> None:
    never = asyncio.Event()
    provider = SyntheticProvider(
        ModelText(text='{"score_milli":1000,"rationale":"late"}'), wait=never
    )
    registry, rule = register(provider)
    case, output = sample_case(), sample_output()
    trial = TrialSpec(task_id="task-1", case_id="case-1", repetition=1, case=case, target={})

    scores = await score_observation(
        trial,
        [rule],
        ItemWork(adapter=None),
        registry,
        output,
        scorer_timeout_seconds=0.01,
        fence=lambda: None,
    )

    assert scores[0]["status"] == "error"
    assert scores[0]["value"] is None
    assert scores[0]["reason"] == "scorer_timeout"
    assert provider.calls == 1
    assert provider.streams_closed == 1


def label(
    sample_id: str,
    split: Literal["calibration", "holdout"],
    score: int,
    outcome: Literal["pass", "fail"],
) -> ManualSemanticLabel:
    return ManualSemanticLabel(
        sample_id=sample_id,
        split=split,
        rubric_version="rubric-a",
        score_milli=score,
        label=outcome,
        label_source="manual",
    )


def test_calibration_uses_only_manual_calibration_labels_and_reports_holdout() -> None:
    calibration = [
        label("c-fail-1", "calibration", 100, "fail"),
        label("c-fail-2", "calibration", 200, "fail"),
        label("c-pass-1", "calibration", 800, "pass"),
        label("c-pass-2", "calibration", 900, "pass"),
    ]
    holdout = [
        label("h-pass-1", "holdout", 850, "pass"),
        label("h-pass-2", "holdout", 700, "pass"),
        label("h-fail-1", "holdout", 750, "fail"),
        label("h-fail-2", "holdout", 100, "fail"),
    ]

    report = calibrate_threshold(calibration, holdout)
    changed_holdout = [
        row.model_copy(update={"label": "fail" if row.label == "pass" else "pass"})
        for row in holdout
    ]
    changed_report = calibrate_threshold(calibration, changed_holdout)

    assert report.status == "calibrated"
    assert report.threshold_milli == 800
    assert report.calibration_sample_count == 4
    assert report.holdout_sample_count == 4
    assert report.holdout_agreement == 0.75
    assert report.holdout_false_positive_rate == 0.0
    assert report.holdout_false_negative_rate == 0.5
    assert changed_report.threshold_milli == report.threshold_milli
    assert changed_report.holdout_agreement != report.holdout_agreement


def test_calibration_rejects_cross_split_ids_and_marks_insufficient_data() -> None:
    with pytest.raises(ValueError, match="calibration_holdout_id_overlap"):
        calibrate_threshold(
            [label("same", "calibration", 900, "pass")],
            [label("same", "holdout", 100, "fail")],
        )

    report = calibrate_threshold(
        [label("c-pass", "calibration", 900, "pass"), label("c-fail", "calibration", 100, "fail")],
        [],
    )
    assert report.status == "insufficient_data"
    assert report.threshold_milli is None
    assert report.calibration_sufficient is False
    assert report.holdout_sufficient is False
    assert report.calibration_sample_count == 2
    assert report.holdout_sample_count == 0
    assert report.insufficient_reasons == [
        "calibration_needs_two_manual_labels_per_class",
        "holdout_needs_two_manual_labels_per_class",
    ]


def test_scorer_budget_is_strict_integer_with_one_to_thirty_second_bound() -> None:
    assert Budget().scorer_timeout_seconds == 1
    assert Budget(scorer_timeout_seconds=30).scorer_timeout_seconds == 30
    for invalid in (0, 31, 1.5, True):
        with pytest.raises(ValidationError):
            Budget(scorer_timeout_seconds=cast(Any, invalid))
