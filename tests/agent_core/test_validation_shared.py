"""Regression coverage for validators shared across workspace and sandbox code."""

import importlib.util
import os
import sys

import pytest
from pydantic import ValidationError

from krim_agent.contracts import ServiceError
from krim_agent.validation.completion import CompletionCondition, condition
from krim_agent.validation.files import directory_fd, path_parts


def test_live_consumers_import_shared_workspace_validation():
    from krim_agent.development import browser_artifacts
    from krim_agent.sandbox import tasks
    from krim_agent.workspace import artifacts as workspace_artifacts
    from krim_agent.workspace import types as workspace_types
    from krim_agent.workspace import verification as workspace_verification

    assert workspace_types.path_parts is path_parts
    assert workspace_types.CompletionCondition is CompletionCondition
    assert workspace_artifacts.path_parts is path_parts
    assert workspace_artifacts.directory_fd is directory_fd
    assert browser_artifacts.directory_fd is directory_fd
    assert importlib.util.find_spec("krim_agent.tools.workspace") is None
    assert tasks.path_parts is path_parts
    assert tasks.CompletionCondition is CompletionCondition
    assert tasks.evaluate_condition is condition
    assert workspace_verification._condition is condition


def test_shared_path_validator_preserves_workspace_security_rules():
    assert path_parts("src/main.py") == ("src", "main.py")
    for invalid in (
        "../escape",
        "/absolute",
        "a//b",
        "a\\b",
        ".env",
        ".env.production",
        "private/credentials",
        "id_rsa",
        "private/ID_ED25519",
        "é.txt",
    ):
        with pytest.raises(ServiceError, match="artifact_invalid"):
            path_parts(invalid)
    with pytest.raises(ServiceError, match="artifact_invalid"):
        path_parts(17)  # type: ignore[arg-type]


def test_shared_completion_condition_keeps_strict_json_and_ast_semantics():
    contract = CompletionCondition(
        id="answer",
        kind="json_equal",
        path="result.json",
        pointer="/answer",
        expected=1,
    )
    assert contract.path == "result.json"
    assert condition(
        "json_equal", b'{"answer":1}', pointer="/answer", expected=1, symbols=()
    )
    assert not condition(
        "json_equal", b'{"answer":true}', pointer="/answer", expected=1, symbols=()
    )
    assert condition(
        "ast_symbols", b"def answer():\n    return 1\n", pointer="", expected=None,
        symbols=("answer",),
    )
    assert not condition(
        "ast_symbols", b"def outer():\n    def answer():\n        pass\n", pointer="",
        expected=None, symbols=("answer",),
    )
    with pytest.raises(ValueError):
        condition("json_equal", b'{"answer":1}', pointer="/answer~2", expected=1, symbols=())
    with pytest.raises(ValidationError):
        CompletionCondition(id="answer", kind="unknown", path="result.json")


@pytest.mark.skipif(sys.platform != "linux", reason="requires Linux dirfd and O_NOFOLLOW semantics")
def test_directory_fd_creates_directories_and_rejects_symlink_components(tmp_path):
    root = tmp_path / "objects" / "nested"
    fd = directory_fd(root, create=True)
    try:
        assert os.path.isdir(f"/proc/self/fd/{fd}")
    finally:
        os.close(fd)

    target = tmp_path / "target"
    target.mkdir()
    alias = tmp_path / "alias"
    alias.symlink_to(target, target_is_directory=True)
    with pytest.raises(ServiceError, match="artifact_invalid"):
        directory_fd(alias)


@pytest.mark.asyncio
async def test_workspace_sdk_rejects_all_new_write_operations():
    from krim_agent.sdk.workspace import WorkspaceSDK

    sdk = WorkspaceSDK(object())
    for operation in (sdk.start("session", None), sdk.input("session", None)):
        with pytest.raises(ServiceError, match="workspace_unavailable") as caught:
            await operation
        assert caught.value.status == 422


@pytest.mark.asyncio
async def test_workspace_sdk_history_read_uses_artifacts_without_executor(monkeypatch):
    from types import SimpleNamespace

    from krim_agent.sdk.sessions import SessionSDK
    from krim_agent.sdk.workspace import WorkspaceSDK
    from krim_agent.security import LOCAL

    authorized: list[str] = []

    async def authorize(_sdk, session_id):
        authorized.append(session_id)

    class ReadModel:
        async def head(self, access):
            assert access.session_id == "session"
            return "historic-revision"

    monkeypatch.setattr(SessionSDK, "get", authorize)
    backend = SimpleNamespace(sessions=object(), artifacts=ReadModel())
    result = await WorkspaceSDK(backend, principal=LOCAL).head("session")
    assert result == "historic-revision"
    assert authorized == ["session"]


@pytest.mark.asyncio
async def test_legacy_artifact_store_read_only_mode_blocks_mutation(tmp_path):
    from krim_agent.storage.sqlite import SQLiteStore
    from krim_agent.workspace.artifacts import ArtifactStore

    store = await SQLiteStore.open(tmp_path / "readonly-artifacts.sqlite")
    root = tmp_path / "legacy-objects"
    root.mkdir()
    artifacts = ArtifactStore(store, root, read_only=True)
    try:
        with pytest.raises(ServiceError, match="readonly"):
            await artifacts.input(None, None)
        with pytest.raises(ServiceError, match="readonly"):
            await artifacts.publish(
                None, key="effect", expected=0, run_id="run", call_id="call", receipt=None
            )
    finally:
        await artifacts.close()
        await store.close()


@pytest.mark.asyncio
async def test_workspace_api_keeps_reads_but_rejects_new_writes():
    import httpx
    from fastapi import FastAPI
    from fastapi.responses import JSONResponse

    from krim_agent.api.workspace import mount_workspace

    app = FastAPI()

    async def service_error(_request, error):
        return JSONResponse(status_code=error.status, content={"error": {"code": error.code}})

    app.add_exception_handler(ServiceError, service_error)
    mount_workspace(app)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://localhost"
    ) as client:
        capabilities = await client.get("/v3/workspace/capabilities")
        assert capabilities.json() == {
            "schema_version": 1,
            "available": False,
            "profile": None,
        }
        for path in (
            "/v3/workspaces/local/sessions/session/turns",
            "/v3/workspaces/local/sessions/session/inputs",
        ):
            response = await client.post(path, json={})
            assert response.status_code == 422
            assert response.json()["error"]["code"] == "workspace_unavailable"
