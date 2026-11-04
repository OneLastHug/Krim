import pytest

from krim_agent.contracts import Message, encoded
from krim_agent.providers.types import ModelToolCall


@pytest.mark.parametrize("value", [None, False, 0])
def test_provider_tool_call_dto_matches_canonical_session_transcript(value):
    call = ModelToolCall(id="provider-1", name="lookup", arguments={"query": "x"})
    dumped_call = call.model_dump(mode="json")
    assert dumped_call == {
        "kind": "tool_call",
        "id": "provider-1",
        "name": "lookup",
        "arguments": {"query": "x"},
    }

    assistant = Message(role="assistant", content="调用工具", tool_calls=[dumped_call])
    tool_result = Message(role="tool", content=encoded(value), call_id=call.id)
    canonical = [assistant.model_dump(mode="json"), tool_result.model_dump(mode="json")]
    assert canonical[0]["tool_calls"] == [dumped_call]
    assert canonical[1]["call_id"] == "provider-1"
    assert canonical[1]["content"] == encoded(value)
