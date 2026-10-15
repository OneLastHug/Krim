"""0048字节捕获内部基础；原发布包向量/真实SQL/格式围栏，未宣称公共消费。"""

import copy
import json
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pytest
from alembic import command
from pydantic import ValidationError
from sqlalchemy import text
from test_workspace_changes import setup
from test_workspace_checkpoint_capture import captured

from krim_agent.persistence.database import NativeWorkspaceCheckpointFileRow
from krim_agent.persistence.migration import migration_config
from krim_agent.persistence.workspace_checkpoints import decode_capture, encode_capture
from krim_agent.persistence.workspace_edits import digest
from krim_agent.protocol.domain import ServiceError
from krim_agent.sdk import WorkspaceCheckpointCreate
from krim_agent.workspace_checkpoint import WorkspaceCheckpointFileView, WorkspaceCheckpointView
from krim_agent.workspace_writer import CapturedFile, FileSnapshot, WorkspaceWriter

DATA = {"text": b"hello\n", "nul": b"\0\xff", "invalid": b"\xff", "empty": b"", "absent": None}
GOLDEN = json.loads(
    (Path(__file__).parent / "fixtures/native_checkpoint_legacy_vectors.json").read_text()
)


def vector(data):
    snapshot = FileSnapshot(data, 0o640, 11, 22, 33, 44, 55, 66) if data is not None else None
    return CapturedFile("file.bin", "a" * 64, (11, 99), snapshot)


@pytest.mark.parametrize("name", DATA)
def test_real_published_codec_json_hash_and_native_kind_only_opt_in(name):
    capture = vector(DATA[name])
    legacy = encode_capture(capture, allow_binary=True)
    assert legacy == GOLDEN[name] and "content_kind" not in legacy
    assert decode_capture(legacy, DATA[name]) == capture
    native = encode_capture(capture, allow_binary=True, native_bytes=True)
    expected = "absent" if name == "absent" else "binary" if name in {"nul", "invalid"} else "text"
    assert native["content_kind"] == expected and native["snapshot_hash"] != legacy["snapshot_hash"]
    assert decode_capture(native, DATA[name]) == capture
    wrong = copy.deepcopy(native)
    wrong["content_kind"] = "text" if expected == "binary" else "binary"
    wrong["snapshot_hash"] = digest(
        {key: value for key, value in wrong.items() if key != "snapshot_hash"}
    )
    with pytest.raises(ServiceError, match="检查点"):
        decode_capture(wrong, DATA[name])


def test_legacy_public_json_and_text_entry_points_stay_closed_to_bytes():
    original = GOLDEN["public_view"]
    view = WorkspaceCheckpointView.model_validate_json(json.dumps(original))
    assert view.model_dump(mode="json") == original and view.capture_format == "text"
    file = WorkspaceCheckpointFileView(
        path="file.bin", captured=True, exists=True, sha256="b" * 64, byte_size=2, mode=0o640
    )
    assert "content_kind" not in file.model_dump(mode="json")
    enabled = WorkspaceCheckpointCreate.model_validate(
        {"paths": ["file.bin"], "request_key": "public-explicit", "capture_format": "bytes"}
    )
    assert enabled.capture_format == "bytes"
    with pytest.raises(ValidationError):
        WorkspaceCheckpointCreate.model_validate(
            {"paths": ["file.bin"], "request_key": "wrong-format", "capture_format": "binary"}
        )
    for data in (b"\0\xff", b"\xff"):
        with pytest.raises(ServiceError):
            encode_capture(vector(data))
    with pytest.raises(ServiceError):
        encode_capture(vector(b"abc"), native_bytes=True)
    with pytest.raises(ServiceError):
        encode_capture(vector(b"x" * 8388609), allow_binary=True, native_bytes=True)
    assert view.created_at == datetime(2026, 10, 13, tzinfo=UTC)


async def private_bytes(store, scope, binding, paths, key="bytes"):
    request = WorkspaceCheckpointCreate(paths=paths, request_key=key)
    view, created = await store.reserve(
        scope,
        request,
        binding.key,
        binding.fingerprint,
        binding.workspace.fingerprint,
        capture_format="bytes",
    )
    assert created and view.capture_format == "bytes"
    return view, await store.execution(scope, view.checkpoint_id)


async def test_actual_private_mixed_capture_format_fence_blob_free_view_and_cold_key(
    runtime, tmp_path
):
    for name, data in DATA.items():
        if data is not None:
            (tmp_path / name).write_bytes(data)
    service, agent, binding = await setup(runtime, tmp_path)
    scope, store = f"session:{agent.session_id}", service.workspace_checkpoint_store
    try:
        view, fences = await private_bytes(store, scope, binding, list(DATA))
        writer = WorkspaceWriter(binding.workspace)
        for name, fence in zip(DATA, fences, strict=True):
            assert fence.capture_format == "bytes"
            value = writer.capture_file(name, allow_binary=True)
            bad = replace(fence, capture_format="text")
            for callback in (store.assert_capture, store.renew, store.finish):
                with pytest.raises(ServiceError) as denied:
                    await callback(scope, bad)
                assert denied.value.code == "workspace_checkpoint_fenced"
            await store.record(scope, fence, value)
            assert await store.captured(scope, fence) == value
        final = await store.finish(scope, fences[0])
        assert final.status == "available" and final.capture_format == "bytes"
        assert [item.content_kind for item in final.files] == [
            "text",
            "binary",
            "binary",
            "text",
            "absent",
        ]
        actual = await store.captures(scope, final.checkpoint_id)
        assert [item.snapshot.data if item.snapshot else None for item in actual] == list(
            DATA.values()
        )
        assert (
            "hello" not in final.model_dump_json() and str(tmp_path) not in final.model_dump_json()
        )
        assert await store.by_key(scope, "bytes") == final
        repeated, created = await store.reserve(
            scope,
            WorkspaceCheckpointCreate(paths=list(DATA), request_key="bytes"),
            binding.key,
            binding.fingerprint,
            binding.workspace.fingerprint,
            capture_format="bytes",
        )
        assert not created and repeated == final
        with pytest.raises(ServiceError) as conflict:
            await store.reserve(
                scope,
                WorkspaceCheckpointCreate(paths=list(DATA), request_key="bytes"),
                binding.key,
                binding.fingerprint,
                binding.workspace.fingerprint,
            )
        assert conflict.value.code == "workspace_checkpoint_key_conflict"
        async with store.database.sessions.begin() as sql:
            row = await sql.get(NativeWorkspaceCheckpointFileRow, (view.checkpoint_id, 1))
            value = copy.deepcopy(row.capture_metadata)
            value["content_kind"] = "text"
            value["snapshot_hash"] = digest(
                {key: item for key, item in value.items() if key != "snapshot_hash"}
            )
            row.capture_metadata = value
        with pytest.raises(ServiceError) as corrupt:
            await store.captures(scope, view.checkpoint_id)
        assert corrupt.value.code == "workspace_checkpoint_corrupt"
    finally:
        await service.close()


async def test_binary_format_scope_quota_and_token_never_save_partial_blob(runtime, tmp_path):
    (tmp_path / "large").write_bytes(b"\xff" * 1025)
    service, agent, binding = await setup(runtime, tmp_path)
    scope, store = f"session:{agent.session_id}", service.workspace_checkpoint_store
    try:
        view, fences = await private_bytes(store, scope, binding, ["large"])
        value = WorkspaceWriter(binding.workspace).capture_file("large", allow_binary=True)
        with pytest.raises(ServiceError):
            await store.record("session:another", fences[0], value)
        with pytest.raises(ServiceError):
            await store.record(scope, replace(fences[0], token="other"), value)
        store.max_snapshot_bytes = 1024
        with pytest.raises(ServiceError) as quota:
            await store.record(scope, fences[0], value)
        assert quota.value.code == "workspace_checkpoint_bytes"
        async with store.database.sessions() as sql:
            row = await sql.get(NativeWorkspaceCheckpointFileRow, (view.checkpoint_id, 0))
            assert not row.captured and row.content is None and row.capture_metadata is None
        assert (await store.get(scope, view.checkpoint_id)).status == "capturing"
    finally:
        await service.close()


async def test_actual_0047_roundtrip_preserves_text_hash_blob_json_and_refuses_bytes_loss(
    runtime, tmp_path
):
    (tmp_path / "text").write_text("legacy")
    service, agent, binding = await setup(runtime, tmp_path)
    database, store = runtime.repository.database, service.workspace_checkpoint_store
    scope = f"session:{agent.session_id}"
    try:
        original = await captured(agent, ["text"], key="legacy")
        async with database.engine.connect() as connection:
            before = (
                await connection.execute(
                    text(
                        "SELECT request_hash FROM native_workspace_checkpoints "
                        "WHERE checkpoint_id=:id"
                    ),
                    {"id": original.checkpoint_id},
                )
            ).scalar_one()
            payload = (
                await connection.execute(
                    text(
                        "SELECT capture_metadata,content FROM native_workspace_checkpoint_files "
                        "WHERE checkpoint_id=:id"
                    ),
                    {"id": original.checkpoint_id},
                )
            ).one()
        async with database.engine.begin() as connection:
            await connection.run_sync(
                lambda sync: command.downgrade(
                    migration_config(sync), "0047_generated_tree_root_undos"
                )
            )
            assert not (await connection.execute(text("PRAGMA foreign_key_check"))).all()
        await database.initialize()
        assert (await store.get(scope, original.checkpoint_id)).model_dump(
            mode="json"
        ) == original.model_dump(mode="json")
        async with database.engine.connect() as connection:
            assert (
                await connection.execute(
                    text(
                        "SELECT request_hash FROM native_workspace_checkpoints "
                        "WHERE checkpoint_id=:id"
                    ),
                    {"id": original.checkpoint_id},
                )
            ).scalar_one() == before
            assert (
                await connection.execute(
                    text(
                        "SELECT capture_metadata,content FROM native_workspace_checkpoint_files "
                        "WHERE checkpoint_id=:id"
                    ),
                    {"id": original.checkpoint_id},
                )
            ).one() == payload
        (tmp_path / "raw").write_bytes(b"\0\xff")
        binary, fences = await private_bytes(store, scope, binding, ["raw"])
        await store.record(
            scope,
            fences[0],
            WorkspaceWriter(binding.workspace).capture_file("raw", allow_binary=True),
        )
        await store.finish(scope, fences[0])
        with pytest.raises(RuntimeError, match="原生字节检查点不能降级"):
            async with database.engine.begin() as connection:
                await connection.run_sync(
                    lambda sync: command.downgrade(
                        migration_config(sync), "0047_generated_tree_root_undos"
                    )
                )
        assert (await store.get(scope, binary.checkpoint_id)).capture_format == "bytes"
    finally:
        await service.close()
