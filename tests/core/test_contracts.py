import pytest
from pydantic import ValidationError

from krim_agent.contracts import RunRequest, check_json, parse_json
from krim_agent.usage import TokenUsage


@pytest.mark.parametrize("raw", ['{"x":1,"x":2}', "NaN", '{"x":Infinity}'])
def test_strict_json_rejects_ambiguous_content(raw):
    with pytest.raises(ValueError):
        parse_json(raw)


@pytest.mark.parametrize("value", [float("nan"), float("inf"), {1: "bad"}, [[[[]]]]])
def test_finite_json_rejects_bad_values(value):
    with pytest.raises(ValueError):
        check_json(value, max_depth=2)


def test_request_rejects_unknown_fields_and_byte_overflow():
    with pytest.raises(ValidationError):
        RunRequest(text="x", request_key="k", provider_url="secret")
    with pytest.raises(ValidationError):
        RunRequest(text="中" * 12000, request_key="k")


@pytest.mark.parametrize(
    "fields", [{}, {"input_tokens": 5, "total_tokens": 1}, {"output_tokens": 3, "total_tokens": 1}]
)
def test_usage_does_not_accept_empty_or_inconsistent_partial_reports(fields):
    with pytest.raises(ValidationError):
        TokenUsage(**fields)


def test_shared_wire_vectors_match_python_contract():
    import json
    from pathlib import Path

    from krim_agent.contracts import RunView

    for vector in json.loads(Path(__file__).with_name("contracts.json").read_text()):
        if vector["valid"]:
            RunView.model_validate(vector["value"])
        else:
            with pytest.raises(ValidationError):
                RunView.model_validate(vector["value"])
