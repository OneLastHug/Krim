"""同步消费者严格核对原恢复身份/请求/回执，坏JSON不能当null。"""

import copy

import httpx
import pytest

from krim_agent.client import ClientError, KrimClient
from krim_agent.conversation_recovery import (
    ConversationRecoveryRequest,
    recovery_message_identifier,
)


def request():
    return ConversationRecoveryRequest(
        source_run_id="original-run",
        expected_path_hash="a" * 64,
        expected_session_revision=0,
        request_key="original-key",
    )


def record():
    return {
        "version": 1,
        "kind": "conversation_recovery_branch",
        "status": "context_recovered",
        "recovery_message_id": recovery_message_identifier("topic", "original-key"),
        "request": request().model_dump(mode="json"),
        "source": {
            "topic_id": "topic",
            "session_id": "agent",
            "agent_id": "agent",
            "session_revision": 0,
            "available": True,
            "source_path_hash": "a" * 64,
            "source_branch_key": "b" * 64,
            "source_run_id": "original-run",
            "source_run_status": "interrupted",
            "anchor_message_id": "input",
            "excluded_message_ids": ["assistant"],
            "pending_calls": [
                {
                    "call_id": "pending",
                    "name": "echo",
                    "arguments_hash": "c" * 64,
                    "selectors": {"request_key": "original-tool-key"},
                }
            ],
            "answered_calls": [],
        },
        "recovered_branch_key": "d" * 64,
        "session_revision": 1,
        "created_at": "2026-10-16T00:00:00Z",
    }


@pytest.mark.parametrize(
    "fault", ["scope", "run", "path", "revision", "identifier", "topic", "date", "private", "done"]
)
def test_client_rejects_wrong_public_original_recovery_even_with_valid_structure(fault):
    value = copy.deepcopy(record())
    if fault == "scope":
        value["source"]["session_id"] = value["source"]["agent_id"] = "other"
    elif fault == "run":
        value["request"]["source_run_id"] = value["source"]["source_run_id"] = "other-run"
    elif fault == "path":
        value["request"]["expected_path_hash"] = value["source"]["source_path_hash"] = "e" * 64
    elif fault == "revision":
        value["request"]["expected_session_revision"] = value["source"]["session_revision"] = 2
        value["session_revision"] = 3
    elif fault == "identifier":
        value["recovery_message_id"] = "other"
    elif fault == "topic":
        value["source"]["topic_id"] = "other-topic"
    elif fault == "date":
        value["created_at"] = "2026-10-16T00:00:00"
    elif fault == "private":
        value["uploaded_tool_receipt"] = {"status": "done"}
    else:
        value["status"] = "done"
    calls = []
    with KrimClient("http://127.0.0.1") as client:
        client._http.close()
        client._http = httpx.Client(
            base_url="http://127.0.0.1",
            transport=httpx.MockTransport(
                lambda message: calls.append(message) or httpx.Response(200, json=value)
            ),
        )
        with pytest.raises(ClientError) as error:
            client.agent_conversation_recovery_apply("agent", request(), confirmed=True)
        assert error.value.code == "conversation_recovery_response" and error.value.status == 502
        assert len(calls) == 1 and calls[0].method == "POST"


@pytest.mark.parametrize("fault", ["agent", "key", "confirmation", "revision", "run"])
def test_client_rejects_invalid_local_input_before_any_http(fault):
    calls = []
    with KrimClient("http://127.0.0.1") as client:
        client._http.close()
        client._http = httpx.Client(
            base_url="http://127.0.0.1",
            transport=httpx.MockTransport(lambda message: calls.append(message)),
        )
        with pytest.raises(ClientError) as error:
            if fault == "agent":
                client.agent_conversation_recovery_preview(False)
            elif fault == "key":
                client.agent_conversation_recovery_by_key("agent", False)
            elif fault == "confirmation":
                client.agent_conversation_recovery_apply("agent", request(), confirmed="false")
            else:
                payload = request().model_copy(
                    update={"expected_session_revision": None}
                    if fault == "revision"
                    else {"source_run_id": "../other"}
                )
                client.agent_conversation_recovery_apply("agent", payload, confirmed=True)
        assert error.value.status == 422 and calls == []


@pytest.mark.parametrize("raw,missing", [(b"null", True), (b"{bad", False), (b"", False)])
def test_optional_original_key_lookup_distinguishes_null_from_broken_json(raw, missing):
    with KrimClient("http://127.0.0.1") as client:
        client._http.close()
        client._http = httpx.Client(
            base_url="http://127.0.0.1",
            transport=httpx.MockTransport(lambda _message: httpx.Response(200, content=raw)),
        )
        if missing:
            assert client.agent_conversation_recovery_by_key("agent", "original-key") is None
        else:
            with pytest.raises(ClientError) as error:
                client.agent_conversation_recovery_by_key("agent", "original-key")
            assert error.value.status == 502


def test_client_original_key_rejects_other_consistent_record_key():
    value = record()
    value["request"]["request_key"] = "other"
    value["recovery_message_id"] = recovery_message_identifier("topic", "other")
    with KrimClient("http://127.0.0.1") as client:
        client._http.close()
        client._http = httpx.Client(
            base_url="http://127.0.0.1",
            transport=httpx.MockTransport(lambda _message: httpx.Response(200, json=value)),
        )
        with pytest.raises(ClientError) as error:
            client.agent_conversation_recovery_by_key("agent", "original-key")
        assert error.value.status == 502
