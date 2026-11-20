"""Pure scoring and failure-fact classification; no synthetic model execution."""

import pytest

from krim_agent.contracts import ServiceError
from krim_agent.evaluation.scorers import classify, score_rule
from krim_agent.evaluation.trace_cases import observe_snapshot
from krim_agent.evaluation.types import EvalOutput, Reference, RuleSpec, rule


def output(**updates):
    value = EvalOutput(
        run_id="synthetic-run",
        request_key="synthetic-request",
        trace_id="synthetic-trace",
        status="completed",
        code=None,
        snapshot_fingerprint="0" * 64,
        last_seq=0,
        facts_complete=True,
        text="null",
        model_steps=2,
        tools=None,
        token_usage=None,
        usage_complete=False,
    )
    return EvalOutput.model_validate(value.model_dump(mode="json") | updates)


def reference(**updates):
    value = Reference(
        expected="5",
        expected_status="completed",
        expected_code=None,
        expected_tools=None,
        trajectory=None,
        expected_schema=None,
    )
    return Reference.model_validate(value.model_dump(mode="json") | updates)


def test_rule_vectors_and_unknown_facts():
    actual = output()
    json_score = yield_score(rule("json_match"), reference(expected=None), actual)
    assert (json_score.status, json_score.value) == ("scored", 1)
    assert yield_score(rule("exact_match"), reference(expected=None), actual).status == "skipped"
    assert yield_score(rule("json_schema"), reference(expected=None), actual).status == "skipped"
    assert yield_score(rule("tool_calls"), reference(expected=None), actual).status == "skipped"
    assert yield_score(rule("trajectory"), reference(expected=None), actual).status == "skipped"
    for text, expected, passed in [
        ("true", 1, 0),
        ("false", 0, 0),
        ("1", 1.0, 1),
        ('{"x":1,"x":2}', {"x": 2}, 0),
        ("NaN", None, 0),
    ]:
        changed = actual.model_copy(update={"text": text})
        assert yield_score(
            rule("json_match"), reference(expected=expected), changed
        ).value == passed
    schema = reference(expected_schema={"type": "integer"})
    assert yield_score(rule("json_schema"), schema, actual).value == 0
    assert yield_score(
        rule("json_schema"), schema, actual.model_copy(update={"text": "1"})
    ).value == 1


def test_unknown_tool_fact_is_skipped_not_guessed():
    ref = reference(
        expected=None,
        expected_status="failed",
        expected_code="permission_denied",
        expected_tools=[
            {
                "name": "lookup_note",
                "arguments": None,
                "executor_started": False,
                "status": "denied",
            }
        ],
        trajectory={"exact_steps": 1, "tool_names": ["lookup_note"]},
    )
    unknown = output(
        status="failed",
        text=None,
        model_steps=None,
        tools=[
            {
                "call_id": "call",
                "provider_call_id": "provider-call",
                "span_id": "span",
                "step_index": 0,
                "name": "lookup_note",
                "arguments": None,
                "status": "unknown",
                "executor_started": None,
                "result_available": None,
                "value_fingerprint": None,
                "code": None,
            }
        ],
    )
    skipped = yield_score(rule("tool_calls"), ref, unknown)
    assert skipped.status == "skipped" and skipped.value is None
    assert classify([skipped]) == "unscored"
    assert yield_score(rule("trajectory"), ref, unknown).status == "skipped"
    assert yield_score(
        rule("status_code"),
        ref,
        unknown.model_copy(update={"code": "permission_denied"}),
    ).value == 1


def test_denied_tool_is_a_real_process_fact():
    denied = output(
        text="5",
        tools=[
            {
                "call_id": "call",
                "provider_call_id": "provider-call",
                "span_id": "span",
                "step_index": 0,
                "name": "lookup_note",
                "arguments": {"key": "welcome"},
                "status": "denied",
                "executor_started": False,
                "result_available": False,
                "value_fingerprint": None,
                "code": "permission_denied",
            }
        ],
    )
    result = yield_score(
        rule("tool_calls"),
        Reference(
            expected="5",
            expected_status="completed",
            expected_code=None,
            expected_tools=[
                {
                    "name": "lookup_note",
                    "arguments": {"key": "welcome"},
                    "executor_started": False,
                    "status": "denied",
                }
            ],
            trajectory=None,
            expected_schema=None,
        ),
        denied,
    )
    # A denied call is a persisted execution fact, never an invented success.
    assert result.status == "scored"


def yield_score(rule_spec: RuleSpec, ref: Reference, value: EvalOutput):
    import asyncio

    return asyncio.run(score_rule(rule_spec, ref, value))


def test_observe_snapshot_rejects_missing_root_start():
    from krim_agent.contracts import RunView
    from krim_agent.runtime.contracts import RunSnapshot

    snapshot = RunSnapshot(
        RunView(
            id="run",
            request_key="request",
            status="completed",
            trace_id="trace",
            root_span_id="span",
            created_at=0.0,
            finished_at=1.0,
        ),
        {},
        {},
        (),
    )
    with pytest.raises(ServiceError, match="facts_corrupt"):
        observe_snapshot(snapshot)