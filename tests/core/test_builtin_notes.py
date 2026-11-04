import pytest

from krim_agent.contracts import encoded, parse_json
from krim_agent.tools.builtins import NoteInput, builtin_registry, lookup_note
from krim_agent.tools.output import OutputContract


@pytest.mark.parametrize(
    ("key", "expected"),
    [
        ("welcome", "这是需要本次批准的合成演示资料。"),
        ("agent", "Agent 在有限循环中调用受控工具。"),
        ("context", {"found": False, "key": "context"}),
    ],
)
async def test_lookup_note_returns_a_normal_hit_or_miss(key, expected):
    result = await lookup_note(NoteInput(key=key))
    spec = builtin_registry().get("lookup_note")
    OutputContract(spec.output_schema).validate(result)
    assert result == expected
    assert parse_json(encoded(result)) == expected
    assert spec.version == "2"
    assert "welcome" in spec.description and "found=false" in spec.description


@pytest.mark.parametrize("value", [{"found": True, "key": "context"}, {"found": False}, 0])
def test_note_output_contract_does_not_accept_ambiguous_or_invalid_results(value):
    spec = builtin_registry().get("lookup_note")
    with pytest.raises(ValueError):
        OutputContract(spec.output_schema).validate(value)
