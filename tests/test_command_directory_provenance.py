"""目录来源必须来自实际收束的副本，旧历史与损坏/残缺集合不能冒充完整。"""

import json
import shutil
import sys
from dataclasses import replace
from uuid import uuid4

import pytest
from sqlalchemy import update
from test_command_store import LIMITS, RESOURCE, receipt, request
from test_command_store import ledger as ledger  # noqa: F401
from test_isolated_execution import run

from krim_agent.execution import (
    ExecutionArtifact,
    ExecutionDirectory,
    LinuxSandboxRunner,
    SandboxLimits,
)
from krim_agent.execution.directories import freeze_directory_manifest, read_directory_manifest
from krim_agent.execution.records import CommandRequest
from krim_agent.execution.service import DurableCommandHandle, DurableCommandService
from krim_agent.persistence.command_models import CommandRow
from krim_agent.persistence.commands import CommandStore, digest
from krim_agent.protocol.domain import ServiceError

real_linux = pytest.mark.skipif(
    not sys.platform.startswith("linux") or shutil.which("bwrap") is None,
    reason="需要真实Linux监督器",
)


def test_legacy_directory_absence_is_distinct_from_new_empty_manifest():
    assert freeze_directory_manifest(None, {"legacy/nested/file"}, LIMITS, complete=True) is None
    assert read_directory_manifest({"artifacts_complete": True}, set(), LIMITS) is None
    manifest = freeze_directory_manifest((), set(), LIMITS, complete=True)
    result = read_directory_manifest(
        {"artifacts_complete": True, "directory_manifest": manifest}, set(), LIMITS
    )
    assert result is not None and result.complete and result.entries == []


@pytest.mark.parametrize(
    "directories,files",
    [
        ((ExecutionDirectory("../escape", 0o755),), set()),
        ((ExecutionDirectory(".git", 0o755),), set()),
        ((ExecutionDirectory(".", 0o755),), set()),
        ((ExecutionDirectory("dir", 0o1777),), set()),
        ((ExecutionDirectory("dir", True),), set()),
        ((ExecutionDirectory("dir", 0o755), ExecutionDirectory("dir", 0o700)), set()),
        ((ExecutionDirectory("dir", 0o755),), {"dir"}),
        ((ExecutionDirectory("parent/child", 0o755),), set()),
        ((), {"parent/file"}),
    ],
)
def test_manifest_rejects_bad_paths_modes_duplicates_conflicts_and_missing_parents(
    directories, files
):
    with pytest.raises(ServiceError) as rejected:
        freeze_directory_manifest(directories, files, LIMITS, complete=True)
    assert rejected.value.code == "execution_artifact_invalid"


@pytest.mark.parametrize("kind", ["count", "bytes", "unicode"])
def test_directory_manifest_has_independent_count_and_metadata_capacity(kind):
    limits = LIMITS.model_copy(update={"artifact_files": 1 if kind == "count" else 256})
    names = {
        "count": [f"dir-{index}" for index in range(5)],
        "bytes": [str(index) + "a" * 450 for index in range(3)],
        "unicode": ["目录" * 75 + str(index) for index in range(2)],
    }[kind]
    with pytest.raises(ServiceError) as rejected:
        freeze_directory_manifest(
            tuple(ExecutionDirectory(name, 0o755) for name in names), set(), limits, complete=True
        )
    assert rejected.value.code == "execution_directory_limit"


def test_partial_manifest_preserves_known_entries_without_claiming_parent_closure():
    manifest = freeze_directory_manifest(
        (ExecutionDirectory("unknown/known", 0o755),), {"unknown/file"}, LIMITS, complete=False
    )
    read = read_directory_manifest(
        {"artifacts_complete": False, "directory_manifest": manifest}, {"unknown/file"}, LIMITS
    )
    assert read is not None and not read.complete and read.entries[0].path == "unknown/known"


async def finish(store, scope, *, directories=(), artifacts=(), **changes):
    reserved = await store.reserve(scope, request(), RESOURCE, LIMITS)
    sandbox = str(uuid4())
    await store.authorize(
        scope, reserved.view.command_id, reserved.token, sandbox_id=sandbox, dispatch=True
    )
    result = replace(receipt(sandbox, artifacts=artifacts), directories=directories, **changes)
    await store.finish(scope, reserved.view.command_id, reserved.token, result)
    return reserved, result


@pytest.mark.parametrize("directories", [None, ()])
async def test_store_preserves_legacy_capability_and_new_empty_directory_result(
    ledger, directories
):
    store, scope = ledger
    reserved, _result = await finish(store, scope, directories=directories)
    catalog = await store.directory_catalog(scope, reserved.view.command_id)
    assert catalog.recorded == (directories is not None)
    assert catalog.complete == (directories is not None) and catalog.entries == []
    async with store.database.sessions() as sql:
        row = await sql.get(CommandRow, reserved.view.command_id)
        assert ("directory_manifest" in row.result) == (directories is not None)


async def test_directory_store_preserves_exact_modes_canonical_order_and_original_receipt(ledger):
    store, scope = ledger
    entries = (ExecutionDirectory("z", 0o700), ExecutionDirectory("a", 0o750))
    reserved, result = await finish(store, scope, directories=entries)
    catalog = await store.directory_catalog(scope, reserved.view.command_id)
    assert [(item.path, item.mode) for item in catalog.entries] == [("a", 0o750), ("z", 0o700)]
    catalog.entries[0].path = "mutated by caller"
    assert (await store.directory_catalog(scope, reserved.view.command_id)).entries[0].path == "a"
    previous = await store.get(scope, reserved.view.command_id)
    duplicate = await store.finish(scope, reserved.view.command_id, reserved.token, result)
    assert duplicate.revision == previous.revision
    with pytest.raises(ServiceError) as changed:
        await store.finish(
            scope,
            reserved.view.command_id,
            reserved.token,
            replace(result, directories=(ExecutionDirectory("a", 0o700),)),
        )
    assert changed.value.code == "execution_receipt_conflict"
    async with store.database.sessions() as sql:
        row = await sql.get(CommandRow, reserved.view.command_id)
        assert row.reserved_bytes == len(json.dumps(row.result["directory_manifest"])) + len(
            result.stdout
        )


@pytest.mark.parametrize("fault", ["hash", "mode", "parent", "complete", "format"])
async def test_cold_directory_read_rejects_corrupt_metadata_and_complete_flag(ledger, fault):
    store, scope = ledger
    reserved, _result = await finish(
        store,
        scope,
        directories=(
            ExecutionDirectory("parent", 0o750),
            ExecutionDirectory("parent/child", 0o700),
        ),
    )
    async with store.database.sessions.begin() as sql:
        row = await sql.get(CommandRow, reserved.view.command_id)
        payload = json.loads(json.dumps(row.result))
        manifest = payload["directory_manifest"]
        if fault == "hash":
            manifest["sha256"] = "0" * 64
        elif fault == "mode":
            manifest["entries"][0]["mode"] = 0o1777
            manifest["sha256"] = digest(
                {key: value for key, value in manifest.items() if key != "sha256"}
            )
        elif fault == "parent":
            manifest["entries"] = manifest["entries"][1:]
            manifest["sha256"] = digest(
                {key: value for key, value in manifest.items() if key != "sha256"}
            )
        elif fault == "complete":
            payload["artifacts_complete"] = False
        else:
            manifest["version"] = 2
        await sql.execute(
            update(CommandRow).where(CommandRow.command_id == row.command_id).values(result=payload)
        )
    with pytest.raises(ServiceError) as rejected:
        await store.directory_catalog(scope, reserved.view.command_id)
    assert rejected.value.code == "execution_record_corrupt"


@pytest.mark.parametrize(
    "status", ["failed", "output_limit", "cancelled", "timed_out", "uncertain"]
)
async def test_directory_catalog_requires_reliable_collection_and_original_scope(
    ledger, repository, status
):
    store, scope = ledger
    complete = status in {"failed", "output_limit"}
    reserved, _result = await finish(
        store,
        scope,
        directories=(ExecutionDirectory("empty", 0o750),),
        status=status,
        artifacts_complete=complete,
    )
    catalog = await store.directory_catalog(scope, reserved.view.command_id)
    assert catalog.recorded and catalog.complete == complete
    other = await repository.create_topic("other scope")
    with pytest.raises(ServiceError) as hidden:
        await store.directory_catalog("topic:" + other.id, reserved.view.command_id)
    assert hidden.value.code == "execution_not_found"


@real_linux
async def test_real_collector_includes_empty_nested_explicit_and_input_parent_directories():
    result = await run(
        "import os,json,pathlib;os.mkdir('empty');os.chmod('empty',0o750);"
        "os.makedirs('new/nested');os.chmod('new/nested',0o755);"
        "open('new/file.bin','wb').write(bytes([0,255]));"
        "print(json.dumps({str(path):path.stat().st_mode&511 "
        "for path in pathlib.Path('.').rglob('*') if path.is_dir()}))",
        files={"input/sub/file.txt": b"original"},
        directories=("explicit/empty",),
    )
    assert result.status == "completed" and result.artifacts_complete
    modes = {item.path: item.mode for item in result.directories}
    assert modes == json.loads(result.stdout)
    assert set(modes) == {
        "empty",
        "new",
        "new/nested",
        "input",
        "input/sub",
        "explicit",
        "explicit/empty",
    }
    assert modes["empty"] == 0o750 and modes["new/nested"] == 0o755
    assert {item.path for item in result.artifacts} == {"input/sub/file.txt", "new/file.bin"}


@real_linux
@pytest.mark.parametrize(
    "fault", ["duplicate", "missing_parent", "file_collision", "late", "before_exit", "mode_type"]
)
async def test_real_control_channel_rejects_malformed_directory_frames(fault):
    async with LinuxSandboxRunner() as runner:
        source = runner._broker_code
        emit = b'emit("directory", path=relative, mode=mode)'
        if fault == "duplicate":
            source = source.replace(emit, emit + b"\n                        " + emit)
        elif fault == "missing_parent":
            source = source.replace(emit, b"pass")
        elif fault == "file_collision":
            source = source.replace(emit, b'emit("directory", path="collision", mode=mode)')
        elif fault == "mode_type":
            source = source.replace(emit, b'emit("directory", path=relative, mode=True)')
        elif fault == "late":
            source = source.replace(
                b'emit("finished", artifacts_complete=complete)',
                b'emit("finished", artifacts_complete=complete)\n'
                b'    emit("directory", path="late", mode=493)',
            )
        else:
            source = source.replace(
                b'emit("exit", code=code, output_limit=limited, descendants_stopped=stopped)',
                b'emit("directory", path="early", mode=493)\n'
                b'    emit("exit", code=code, output_limit=limited, descendants_stopped=stopped)',
            )
        assert source != runner._broker_code
        runner._broker_code = source
        intent = request(
            code="import os;os.mkdir('parent');open('parent/file','w').write('x');"
            "open('collision','w').write('y')"
        ).command
        result = await (await runner.start(intent)).wait()
        assert result.status == "uncertain" and not result.artifacts_complete
        assert result.error_code == "execution_protocol_failed"


@real_linux
@pytest.mark.parametrize(
    "fault", ["sensitive", "special_mode", "symlink", "depth", "count", "metadata"]
)
async def test_real_directory_collection_rejects_unsafe_or_excessive_sets(fault):
    code = {
        "sensitive": "import os;os.mkdir('.git')",
        "special_mode": "import os;os.mkdir('empty');os.chmod('empty',0o1777)",
        "symlink": "import os;os.mkdir('empty');os.symlink('empty','link')",
        "depth": "import os;os.makedirs('/'.join(['x']*17))",
        "count": "import os;[os.mkdir('dir'+str(i)) for i in range(5)]",
        "metadata": "import os;[os.mkdir(str(i)+'x'*200) for i in range(6)]",
    }[fault]
    result = await run(
        code,
        limits=SandboxLimits(artifact_files=1 if fault == "count" else 256, artifact_bytes=1024),
    )
    assert result.status in {"failed", "uncertain"} and not result.artifacts_complete


@real_linux
async def test_real_persistent_directory_catalog_cold_read_never_restarts_runner(
    repository, monkeypatch
):
    topic = await repository.create_topic("directory provenance")
    scope = "topic:" + topic.id
    runner = LinuxSandboxRunner()
    service = DurableCommandService(CommandStore(repository.database), runner, owned=True)
    try:
        request_value = CommandRequest(
            request_key="empty-directory",
            command=request(code="import os;os.mkdir('empty')").command,
        )
        handle = await service.start(scope, request_value)
        assert (await handle.wait()).outcome == "completed"
        catalog = await handle.directory_catalog()
        assert catalog.recorded and catalog.complete and catalog.entries[0].path == "empty"

        async def forbidden(*_args, **_kwargs):
            raise AssertionError("冷读取不能启动新命令")

        monkeypatch.setattr(runner, "start", forbidden)
        cold = DurableCommandHandle(CommandStore(repository.database), scope, handle.command_id)
        assert (await cold.directory_catalog()) == catalog
        assert (await service.start(scope, request_value)).command_id == handle.command_id
        assert (await cold.directory_catalog()) == catalog
        assert not runner._active
    finally:
        await service.close()


@real_linux
async def test_real_cancel_before_collection_keeps_directory_observation_unknown():
    async with LinuxSandboxRunner() as runner:
        handle = await runner.start(request(code="import time;time.sleep(30)").command)
        assert await handle.wait_started()
        handle.cancel()
        result = await handle.wait()
        assert result.status == "cancelled" and not result.artifacts_complete
        assert result.directories is None


@pytest.mark.parametrize("fault", ["duplicate", "count"])
async def test_directory_read_rejects_inconsistent_file_manifest_before_parent_checks(
    ledger, fault
):
    store, scope = ledger
    reserved, _result = await finish(
        store, scope, artifacts=(ExecutionArtifact("file", b"content", 0o600),)
    )
    async with store.database.sessions.begin() as sql:
        row = await sql.get(CommandRow, reserved.view.command_id)
        payload = json.loads(json.dumps(row.result))
        if fault == "duplicate":
            payload["artifacts"].append(payload["artifacts"][0])
            payload["artifact_files"] = 2
        else:
            payload["artifact_files"] = 0
        await sql.execute(
            update(CommandRow).where(CommandRow.command_id == row.command_id).values(result=payload)
        )
    with pytest.raises(ServiceError) as rejected:
        await store.directory_catalog(scope, reserved.view.command_id)
    assert rejected.value.code == "execution_record_corrupt"
