import pytest
from pydantic import ValidationError

from krim_agent.contracts import RunRequest, check_json, parse_json
from krim_agent.usage import TokenUsage


@pytest.mark.parametrize("raw", ['{"x":1,"x":2}', "NaN", '{"x":Infinity}'])
def test_strict_json_rejects_ambiguous_content(raw):
    with pytest.raises(ValueError):
        parse_json(raw)


@pytest.mark.parametrize("value", [float("nan"), float("inf"), {1: "bad"}, [[[[]]]],
                                   "\ud800", {"\ud800": "bad"}])
def test_finite_json_rejects_bad_values(value):
    with pytest.raises(ValueError):
        check_json(value, max_depth=2)


def test_json_content_has_no_default_byte_quota_and_explicit_windows_work():
    from krim_agent.contracts import encoded, fingerprint

    value = {"content": "中😀" * 400000}
    check_json(value)
    serialized = encoded(value)
    assert len(serialized.encode()) > 1048576
    assert parse_json(serialized) == value
    assert fingerprint(value) == fingerprint(parse_json(serialized))
    with pytest.raises(ValueError, match="字节超限"):
        check_json(value, max_bytes=65536)


def test_request_rejects_unknown_fields_and_empty_text_but_has_no_byte_quota():
    with pytest.raises(ValidationError):
        RunRequest(text="x", request_key="k", provider_url="secret")
    assert len(RunRequest(text="中" * 12000, request_key="k").text) == 12000
    with pytest.raises(ValidationError):
        RunRequest(text=" \n\t", request_key="k")


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
