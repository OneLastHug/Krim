"""同步Client拒绝合法结构但错原请求的回程，以及损坏的可空查找结果。"""

import httpx
import pytest
from test_model_missing_directory_restore_output import synthetic_history

from krim_agent.client import ClientError, KrimClient
from krim_agent.sdk import (
    DirectoryRestoreTarget,
    MissingDirectoryRestoreExecute,
    MissingDirectoryRestorePreview,
)


def plan():
    value = synthetic_history(0).model_dump()
    value.update(
        plan_id="original-plan",
        operation_id="original-plan",
        root_path="tree",
        request_key="preview",
        execute_key=None,
        revision=1,
        phase="ready",
        status="ready",
        receipt=None,
    )
    value["target"]["identifier"] = "original-checkpoint"
    return value


@pytest.mark.parametrize("lookup", ["plan", "operation"])
def test_optional_lookup_corrupt_json_is_controlled_response_error(lookup):
    with KrimClient("http://127.0.0.1") as client:
        client._http.close()
        client._http = httpx.Client(
            base_url="http://127.0.0.1",
            transport=httpx.MockTransport(
                lambda request: httpx.Response(200, content=b"{corrupt", request=request)
            ),
        )
        with pytest.raises(ClientError) as denied:
            if lookup == "plan":
                client.agent_workspace_missing_directory_restore_by_key("agent", "key")
            else:
                client.agent_workspace_missing_directory_restore_operation_by_key("agent", "key")
        assert (
            denied.value.code == "workspace_missing_directory_restore_response"
            and denied.value.status == 502
        )


@pytest.mark.parametrize(
    "fault",
    [
        "scope",
        "identifier",
        "key",
        "target",
        "execute",
        "verify",
        "duplicate",
        "date",
        "done",
        "private",
    ],
)
def test_client_refuses_mismatched_request_identity_or_fake_success(fault):
    value = plan()
    value["scope_id"] = "session:agent"
    value["created_at"] = value["created_at"].isoformat()
    value["updated_at"] = value["updated_at"].isoformat()
    if fault == "scope":
        value["scope_id"] = "session:other"
    elif fault == "identifier":
        value["plan_id"] = value["operation_id"] = "other-plan"
    elif fault == "key":
        value["request_key"] = "other-key"
    elif fault == "target":
        value["target"]["identifier"] = "other-checkpoint"
    elif fault == "date":
        value["created_at"] = "2026-10-15T00:00:00"
    elif fault == "done":
        value.update(phase="finished", status="done")
    elif fault == "private":
        value["token"] = "not-public"
    with KrimClient("http://127.0.0.1") as client:
        client._http.close()
        client._http = httpx.Client(
            base_url="http://127.0.0.1",
            transport=httpx.MockTransport(
                lambda request: httpx.Response(
                    200, json=[value, value] if fault == "duplicate" else value, request=request
                )
            ),
        )
        with pytest.raises(ClientError) as denied:
            if fault == "key":
                client.agent_workspace_missing_directory_restore_by_key("agent", "preview")
            elif fault == "target":
                client.agent_workspace_preview_missing_directory_restore(
                    "agent",
                    MissingDirectoryRestorePreview(
                        target=DirectoryRestoreTarget(identifier="original-checkpoint"),
                        request_key="preview",
                    ),
                )
            elif fault == "execute":
                client.agent_workspace_execute_missing_directory_restore(
                    "agent",
                    MissingDirectoryRestoreExecute(
                        plan_id="original-plan", expected_revision=1, request_key="execute"
                    ),
                )
            elif fault == "verify":
                client.agent_workspace_verify_missing_directory_restore(
                    "agent", "original-execution"
                )
            elif fault == "duplicate":
                client.agent_workspace_missing_directory_restores("agent")
            else:
                client.agent_workspace_missing_directory_restore("agent", "original-plan")
        assert (
            denied.value.code == "workspace_missing_directory_restore_response"
            and denied.value.status == 502
        )


@pytest.mark.parametrize("fault", ["scope", "plan", "offset", "limit", "duplicate", "outside_root"])
def test_diff_page_binds_original_identity_and_ordered_bounded_paths(fault):
    value = {
        "plan_id": "original-plan",
        "scope_id": "session:agent",
        "root_path": "tree",
        "workspace_key": "files",
        "resource_fingerprint": "a" * 64,
        "target_fingerprint": "b" * 64,
        "source": "workspace_missing_directory_restore_diff",
        "baseline": {"kind": "absent", "fingerprint": "c" * 64},
        "total": 1,
        "next_offset": None,
        "offset": 0,
        "limit": 128,
        "entries": [
            {
                "path": "tree",
                "operation": "create",
                "before": None,
                "after": {
                    "kind": "directory",
                    "mode": 493,
                    "uid": 1,
                    "gid": 1,
                    "byte_size": 0,
                    "sha256": None,
                },
            }
        ],
    }
    if fault == "scope":
        value["scope_id"] = "session:other"
    elif fault == "plan":
        value["plan_id"] = "other-plan"
    elif fault == "offset":
        value["offset"] = 1
    elif fault == "limit":
        value["limit"] = 32
    else:
        entry = {
            "path": "tree/file" if fault == "duplicate" else "other/file",
            "operation": "create",
            "before": None,
            "after": {
                "kind": "file",
                "mode": 0o640,
                "uid": 1,
                "gid": 1,
                "byte_size": 0,
                "sha256": "d" * 64,
            },
        }
        value["entries"] = [entry, entry] if fault == "duplicate" else [entry]
    with KrimClient("http://127.0.0.1") as client:
        client._http.close()
        client._http = httpx.Client(
            base_url="http://127.0.0.1",
            transport=httpx.MockTransport(
                lambda request: httpx.Response(200, json=value, request=request)
            ),
        )
        with pytest.raises(ClientError) as denied:
            client.agent_workspace_missing_directory_restore_diff("agent", "original-plan")
        assert (
            denied.value.code == "workspace_missing_directory_restore_response"
            and denied.value.status == 502
        )


@pytest.mark.parametrize("fault", ["agent", "plan", "key", "confirm", "offset", "limit", "wait"])
def test_invalid_local_inputs_make_zero_http_requests(fault):
    sent = []
    with KrimClient("http://127.0.0.1") as client:
        client._http.close()
        client._http = httpx.Client(
            base_url="http://127.0.0.1",
            transport=httpx.MockTransport(lambda request: sent.append(request)),
        )
        with pytest.raises(ClientError) as denied:
            if fault == "agent":
                client.agent_workspace_missing_directory_restore(False, "original-plan")
            elif fault == "plan":
                client.agent_workspace_missing_directory_restore("agent", "../outside")
            elif fault == "key":
                client.agent_workspace_missing_directory_restore_by_key("agent", "bad\x7fkey")
            elif fault == "confirm":
                client.agent_workspace_preview_missing_directory_restore(
                    "agent",
                    MissingDirectoryRestorePreview(
                        target=DirectoryRestoreTarget(identifier="target"), request_key="preview"
                    ),
                    confirmed="false",
                )
            elif fault == "offset":
                client.agent_workspace_missing_directory_restores("agent", offset=True)
            elif fault == "limit":
                client.agent_workspace_missing_directory_restore_diff("agent", "plan", limit=129)
            else:
                client.agent_workspace_wait_missing_directory_restore(
                    "agent", "plan", timeout_seconds=float("nan")
                )
        assert denied.value.status == 422 and sent == []
