"""回退公共客户端拒绝错来源/状态/参数，并保留空过滤页的可靠游标。"""

import copy

import httpx
import pytest

from krim_agent.client import ClientError, KrimClient
from krim_agent.sdk import (
    MissingDirectoryRollbackAction,
    MissingDirectoryRollbackPreview,
    MissingDirectoryRollbackQuery,
)


def proposal():
    return {
        "source": "workspace_missing_directory_baseline_rollback",
        "source_plan_id": "original-plan",
        "source_operation_id": "original-operation",
        "source_hash": "c" * 64,
        "baseline": {"kind": "absent", "fingerprint": "d" * 64},
        "published_fingerprint": "e" * 64,
        "approved_fingerprint": "a" * 64,
        "rollback_state": "awaiting_approval",
        "move": {
            "proposal_id": "proposal",
            "scope_id": "session:agent",
            "workspace_key": "files",
            "resource_fingerprint": "f" * 64,
            "source_path": "tree",
            "target_path": "preserved",
            "before_fingerprint": "a" * 64,
            "entry_count": 1,
            "byte_size": 0,
            "revision": 0,
            "status": "proposed",
            "expires_at": 2_000_000_000.0,
            "active_operation_id": None,
            "created_at": "2026-10-16T00:00:00Z",
            "updated_at": "2026-10-16T00:00:00Z",
        },
    }


def preview():
    return MissingDirectoryRollbackPreview(
        plan_id="original-plan",
        preserve_path="preserved",
        expected_fingerprint="a" * 64,
        request_key="original",
    )


@pytest.mark.parametrize(
    "fault",
    [
        "source-plan",
        "scope",
        "fingerprint",
        "target",
        "private",
        "source",
        "date",
        "identifier",
        "root",
    ],
)
def test_rollback_client_rejects_structured_but_wrong_original_preview(fault):
    value = copy.deepcopy(proposal())
    if fault == "source-plan":
        value["source_plan_id"] = "other-plan"
    elif fault == "scope":
        value["move"]["scope_id"] = "session:other"
    elif fault == "fingerprint":
        value["approved_fingerprint"] = value["move"]["before_fingerprint"] = "b" * 64
    elif fault == "target":
        value["move"]["target_path"] = "other"
    elif fault == "private":
        value["token"] = "private"
    elif fault == "source":
        value["source"] = "workspace_generated_tree_undo"
    elif fault == "date":
        value["move"]["created_at"] = "2026-10-16T00:00:00"
    elif fault == "identifier":
        value["move"]["proposal_id"] = "../wrong"
    else:
        value["move"]["source_path"] = "../root"
    calls = []

    def transport(request):
        calls.append(request)
        return httpx.Response(200, json=value)

    with KrimClient("http://127.0.0.1") as client:
        client._http.close()
        client._http = httpx.Client(
            base_url="http://127.0.0.1", transport=httpx.MockTransport(transport)
        )
        with pytest.raises(ClientError) as denied:
            client.agent_missing_directory_rollback_preview("agent", preview())
        assert (
            denied.value.status == 502
            and denied.value.code == "workspace_missing_directory_rollback_response"
        )
        assert len(calls) == 1 and calls[0].method == "POST"


@pytest.mark.parametrize("kind", ["by_key", "operation_by_key"])
def test_rollback_optional_query_corrupt_json_is_not_a_missing_key(kind):
    with KrimClient("http://127.0.0.1") as client:
        client._http.close()
        client._http = httpx.Client(
            base_url="http://127.0.0.1",
            transport=httpx.MockTransport(lambda _r: httpx.Response(200, content=b"{bad")),
        )
        with pytest.raises(ClientError) as denied:
            client.agent_missing_directory_rollback_query(
                "agent", MissingDirectoryRollbackQuery(kind=kind, request_key="original")
            )
        assert denied.value.status == 502


@pytest.mark.parametrize(
    "after,following,valid",
    [
        ("cursor0", "cursor1", True),
        ("cursor0", "cursor0", False),
        ("cursor1", "cursor0", False),
        ("cursor0", "../outside", False),
    ],
)
def test_rollback_filtered_empty_page_cursor_must_advance_without_requiring_full_page(
    after, following, valid
):
    body = {"kind": "proposals", "proposals": {"items": [], "next_after": following}}
    with KrimClient("http://127.0.0.1") as client:
        client._http.close()
        client._http = httpx.Client(
            base_url="http://127.0.0.1",
            transport=httpx.MockTransport(lambda _r: httpx.Response(200, json=body)),
        )
        request = MissingDirectoryRollbackQuery(kind="proposals", after=after)
        if valid:
            result = client.agent_missing_directory_rollback_query("agent", request)
            assert result.proposals.items == [] and result.proposals.next_after == following
        else:
            with pytest.raises(ClientError) as denied:
                client.agent_missing_directory_rollback_query("agent", request)
            assert denied.value.status == 502


@pytest.mark.parametrize("fault", ["agent", "confirmed", "proposal", "operation", "query-bool"])
def test_rollback_invalid_local_input_makes_zero_http_calls(fault):
    calls = []
    with KrimClient("http://127.0.0.1") as client:
        client._http.close()
        client._http = httpx.Client(
            base_url="http://127.0.0.1", transport=httpx.MockTransport(lambda r: calls.append(r))
        )
        with pytest.raises(ClientError) as denied:
            if fault == "agent":
                client.agent_missing_directory_rollback_preview(False, preview())
            elif fault == "confirmed":
                client.agent_missing_directory_rollback_preview(
                    "agent", preview(), confirmed="false"
                )
            elif fault == "proposal":
                client.agent_missing_directory_rollback_cancel("agent", "../outside")
            elif fault == "operation":
                client.agent_missing_directory_rollback_reconcile("agent", "../outside")
            else:
                client.agent_missing_directory_rollback_query(
                    "agent", MissingDirectoryRollbackQuery(kind="proposals"), confirmed=1
                )
        assert denied.value.status == 422 and calls == []


def test_rollback_done_without_receipt_is_controlled_fake_success_error():
    body = {
        "source_plan_id": "original-plan",
        "source_operation_id": "original-operation",
        "action": "return_to_absence",
        "operation": {
            "operation_id": "operation",
            "proposal_id": "proposal",
            "scope_id": "session:agent",
            "action": "apply",
            "expected_revision": 0,
            "status": "done",
            "receipt": None,
            "cancelled": False,
            "recovery_required": False,
            "created_at": "2026-10-16T00:00:00Z",
            "updated_at": "2026-10-16T00:00:00Z",
        },
    }
    with KrimClient("http://127.0.0.1") as client:
        client._http.close()
        client._http = httpx.Client(
            base_url="http://127.0.0.1",
            transport=httpx.MockTransport(lambda _r: httpx.Response(200, json=body)),
        )
        with pytest.raises(ClientError) as denied:
            client.agent_missing_directory_rollback_change(
                "agent",
                MissingDirectoryRollbackAction(
                    proposal_id="proposal",
                    action="return_to_absence",
                    expected_revision=0,
                    request_key="apply",
                ),
            )
        assert denied.value.status == 502


@pytest.mark.parametrize("fault", ["after", "changed", "recovery", "overlap"])
def test_rollback_done_receipt_must_describe_actual_complete_move(fault):
    receipt = {
        "source_path": "tree",
        "target_path": "preserved",
        "status": "done",
        "before_fingerprint": "a" * 64,
        "after_fingerprint": "b" * 64,
        "changed": True,
        "error_code": None,
        "recovery_required": False,
    }
    body = {
        "source_plan_id": "original-plan",
        "source_operation_id": "original-operation",
        "action": "return_to_absence",
        "operation": {
            "operation_id": "operation",
            "proposal_id": "proposal",
            "scope_id": "session:agent",
            "action": "apply",
            "expected_revision": 0,
            "status": "done",
            "receipt": receipt,
            "cancelled": False,
            "recovery_required": fault == "recovery",
            "created_at": "2026-10-16T00:00:00Z",
            "updated_at": "2026-10-16T00:00:00Z",
        },
    }
    if fault == "after":
        receipt["after_fingerprint"] = None
    elif fault == "changed":
        receipt["changed"] = False
    elif fault == "overlap":
        receipt["target_path"] = "tree/child"
    with KrimClient("http://127.0.0.1") as client:
        client._http.close()
        client._http = httpx.Client(
            base_url="http://127.0.0.1",
            transport=httpx.MockTransport(lambda _r: httpx.Response(200, json=body)),
        )
        with pytest.raises(ClientError) as denied:
            client.agent_missing_directory_rollback_change(
                "agent",
                MissingDirectoryRollbackAction(
                    proposal_id="proposal",
                    action="return_to_absence",
                    expected_revision=0,
                    request_key="original",
                ),
            )
        assert denied.value.status == 502
