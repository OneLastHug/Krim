"""D4真实API/数据消费者；不以收集或材料成功代替行为验证。"""

import asyncio
import base64
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import httpx
import pytest
from fixtures.workspace_server import serving, workspace_application
from test_session_read_sides import Connections
from test_tool_pipeline import data_case as data_case

from krim_agent.api.server import create_app
from krim_agent.client import AsyncRunClient
from krim_agent.contracts import ServiceError
from krim_agent.execution.linux import fds
from krim_agent.sdk.workspace import WorkspaceSDK
from krim_agent.security import LOCAL
from krim_agent.sessions.types import SessionCreate
from krim_agent.storage.sqlite import SQLiteStore
from krim_agent.workspace.types import (
    Completion,
    InputFile,
    InputSnapshotRequest,
    WorkspaceTurnRequest,
)


@pytest.fixture(scope="module", autouse=True)
def preserve_failed_scope_worker() -> Any:
    """冻结本轮超时原件；只在副本上做一致只读backup，不recover原库。"""
    import ast
    import os
    import shutil
    import sqlite3
    import stat
    from contextlib import closing

    root = Path(os.environ["KRIM_D4_ROOT"])
    old = root / "evidence/d4/d4-scope-behavior-01"
    if not old.exists():
        return
    command = json.loads((old / "command.json").read_bytes())
    assert command["label"] == "d4-scope-behavior-01" and command["rc"] == 1
    statuses = [
        json.loads(line)
        for line in (old / "stdout.txt").read_text().splitlines()
        if line.startswith('{"id":')
    ]
    assert statuses and statuses[-1]["exit_code"] == -15 and statuses[-1]["timeout"] is True
    snapshot = old / "source-files/tests/agent_core/test_workspace_api.py"
    tree = ast.parse(snapshot.read_bytes())
    test = next(
        node
        for node in tree.body
        if isinstance(node, ast.AsyncFunctionDef)
        and node.name == "test_scope_all_work_artifact_and_input_paths"
    )
    decorator = test.decorator_list[0]
    assert isinstance(decorator, ast.Call)
    parameters = ast.literal_eval(decorator.args[1])
    prefix = "tests/agent_core/test_workspace_api.py::test_scope_all_work_artifact_and_input_paths"
    completed = [
        json.loads(path.read_bytes())
        for path in (old / "cases").iterdir()
        if stat.S_ISREG(path.lstat().st_mode) and path.suffix == ".json"
    ]
    assert {row["node"] for row in completed} == {
        prefix + "[" + key + "]" for key in parameters[:4]
    }
    source = old / "tmp/test_scope_all_work_artifact_a4/pipeline.sqlite"
    assert stat.S_ISREG(source.lstat().st_mode) and parameters[4] == "v1-v2-reject"
    out = Path(os.environ["KRIM_D4_EVIDENCE"]) / "prior-worker"
    out.mkdir()
    hashes: dict[str, Any] = {}
    for path in [
        source,
        source.with_name(source.name + "-wal"),
        source.with_name(source.name + "-shm"),
    ]:
        if not path.exists():
            continue
        assert stat.S_ISREG(path.lstat().st_mode) and path.lstat().st_nlink == 1
        hashes[str(path)] = hashlib.sha256(path.read_bytes()).hexdigest()
        shutil.copyfile(path, out / path.name)
    shadow = out / source.name
    backup = out / "consistent-readonly-backup.sqlite"
    with closing(sqlite3.connect(shadow.as_uri() + "?mode=ro", uri=True)) as reader:
        with closing(sqlite3.connect(backup)) as writer, writer:
            reader.backup(writer)
        rows = reader.execute("SELECT id,status,code FROM runs").fetchall()
        turns = reader.execute("SELECT id,status,run_id,request_key FROM turns").fetchall()
        effects = reader.execute("SELECT call_id,phase,receipt FROM tool_effects").fetchall()
    assert all(
        hashlib.sha256(Path(path).read_bytes()).hexdigest() == digest
        for path, digest in hashes.items()
    )
    (out / "mapping.json").write_text(
        json.dumps(
            {
                "source": str(source),
                "source_hashes": hashes,
                "node": prefix + "[v1-v2-reject]",
                "mapping_basis": ("frozen ordered parameters; 4 case receipts; unique tmp index4"),
                "source_manifest": command["source_manifest"],
                "worker_status": statuses[-1],
                "internal_finally": "unknown",
                "qualifying_green": False,
                "recover_calls": 0,
                "runs": rows,
                "turns": turns,
                "effects": effects,
                "readonly_backup": str(backup),
                "backup_sha256": hashlib.sha256(backup.read_bytes()).hexdigest(),
            },
            indent=2,
        )
    )


async def linked_frame(
    app: Any,
    notes: Any,
    key: Any,
    *,
    completion: Any = None,
    context_policy: Any = None,
) -> Any:
    """实际同事务关联；不从字典伪造Run、revision或conditions。"""
    from krim_agent.config import Settings
    from krim_agent.storage import sessions as session_sql
    from krim_agent.workspace.types import ExecutionAccess

    async with app.service._admission:
        session = await app.sessions.create(
            LOCAL,
            "local",
            SessionCreate(
                request_key=key, target_key="target-workspace", context_policy=context_policy
            ),
        )
        body = WorkspaceTurnRequest(
            request_key=key + ".turn",
            text="只冻结实际条件",
            expected_revision=0,
            completion=completion or Completion(),
        )
        access = ExecutionAccess(LOCAL, "local", session.id)
        turn = await session_sql.submit(app.store, session.id, body, Settings(), access=access)
        request, _ = await app.store.turn_request(turn.id)
        binding = app.service.bindings["target-workspace"]
        run, fresh = await app.store.begin_turn_run(
            turn.id, request, json.loads(binding.descriptor_json), binding.settings
        )
        assert fresh
        await app.store.set_running(run.id)
    notes.setdefault("manual_runs", []).append((app.store, run.id))
    return run, binding, access, await app.work.prepare(run.id)


@pytest.mark.parametrize(
    "case",
    [
        "same-binding",
        "runtime-matrix",
        "environment-matrix",
        "context-matrix",
        "verifier-contract",
        "completion",
        "entry-replace",
        "legacy-cold-eval",
    ],
)
async def test_subject_conditions_and_legacy_eval_boundary(
    data_case: Any, tmp_path: Any, monkeypatch: Any, request: Any, case: Any
) -> Any:
    from dataclasses import replace

    from krim_agent.contracts import fingerprint
    from krim_agent.workspace import verification
    from krim_agent.workspace.targets import WorkspaceConfiguration, environment
    from krim_agent.workspace.types import CompletionCondition, WorkLimits, WorkspaceProfile

    app, wire, provider = await workspace_application(data_case, tmp_path)
    notes = data_case[1]
    run, binding, access, frame = await linked_frame(app, notes, "base")
    assert "workspace_verification" not in frame.subject
    assert frame.conditions["verification"]["contract"] == binding.loop._verifier.descriptor()
    assert frame.conditions["verification"]["contract"]["implementation_digest"] == (
        hashlib.sha256(await asyncio.to_thread(Path(verification.__file__).read_bytes)).hexdigest()
    )
    assert {value["key"] for value in app.evaluations.targets()} == {
        "target-v1",
        "target-v2",
        "target-bad",
        "target-http",
    }
    await app.store.finish(run.id, "failed", "completion_unverified")
    comparisons: list[Any] = []
    if case in {"same-binding", "completion"}:
        completion = (
            Completion(
                conditions=(
                    CompletionCondition(
                        id="nested",
                        kind="json_equal",
                        path="data.json",
                        pointer="/value",
                        expected={"nested": [False, None, {"v": [1, 2]}]},
                    ),
                )
            )
            if case == "completion"
            else Completion()
        )
        other, _, _, actual = await linked_frame(app, notes, "second", completion=completion)
        assert actual.subject == frame.subject
        assert (actual.conditions == frame.conditions) is (case == "same-binding")
        assert (actual.effective_fingerprint == frame.effective_fingerprint) is (
            case == "same-binding"
        )
        if case == "completion":
            assert actual.conditions["verification"]["completion"] == completion.model_dump(
                mode="json"
            )
            assert actual.conditions["verification"]["completion_fingerprint"] == fingerprint(
                completion.model_dump(mode="json")
            )
        await app.store.finish(other.id, "failed", "completion_unverified")
    elif case in {"entry-replace", "verifier-contract"}:
        other, _, _, _ = await linked_frame(app, notes, "entry")
        entries: list[Any] = []
        verifier = binding.loop._verifier
        original = verifier.verify

        async def replacement(*args: Any, **kwargs: Any) -> Any:
            entries.append("replacement")
            raise ServiceError("target_binding_changed")

        monkeypatch.setattr(verifier, "verify", replacement)
        with pytest.raises(ServiceError, match="target_binding_changed"):
            await binding.loop.run(other.id, app.service._approve, binding.assert_current)
        assert entries == []
        await app.store.finish(other.id, "failed", "target_binding_changed")
        monkeypatch.setattr(verifier, "verify", original)
        assert frame.conditions["verification"]["contract"]["limits_digest"] == fingerprint(
            verification.LIMITS
        )
        notes["verifier_replacement_entries"] = entries
    elif case == "legacy-cold-eval":
        from krim_agent.observability import project_trace
        from krim_agent.runtime.facts import project_facts

        old_database, seed = request.getfixturevalue("legacy_schema3")
        before = hashlib.sha256(old_database.read_bytes()).hexdigest()
        reader = await SQLiteStore.open(old_database, readonly=True)
        notes.setdefault("store_probes", []).append((reader, cast(Any, Connections)(reader)))
        try:
            snapshot = await reader.snapshot(seed["run_id"])
            assert project_facts(snapshot).model_dump(mode="json") == seed["facts"]
            assert project_trace(snapshot).model_dump(mode="json") == seed["trace"]
            prior = await reader.by_key("complete")
            assert prior is not None and prior.id == seed["run_id"]
            assert (await reader.eval_snapshot(seed["eval_id"]))["report"] == seed["report"]
            assert reader.schema_version == 3
        finally:
            await reader.close()
        assert hashlib.sha256(old_database.read_bytes()).hexdigest() == before
        notes["legacy"] = {
            "database": str(old_database),
            "seed": seed,
            "sha256_before_after": before,
            "current_provider_calls": provider.calls,
            "recover_called": False,
        }
    else:
        original_profile = app.work.profile
        original_execution = app.runner.profile
        profiles: list[Any] = []
        if case == "runtime-matrix":
            for field, value in [
                ("run_timeout_seconds", 179.0),
                ("tool_timeout_seconds", 89.0),
                ("approval_timeout_seconds", 29.0),
                ("model_steps", 11),
                ("tool_calls", 31),
                ("read_parallelism", 3),
            ]:
                profiles.append(
                    (
                        field,
                        original_execution,
                        WorkspaceProfile.model_validate(
                            original_profile.model_dump()
                            | {
                                "runtime": WorkLimits.model_validate(
                                    original_profile.runtime.model_dump() | {field: value}
                                )
                            }
                        ),
                    )
                )
        elif case == "environment-matrix":
            for field, value in [
                ("memory", original_execution.memory - 1048576),
                ("pids", original_execution.pids - 1),
                ("output", original_execution.output - 1024),
            ]:
                execution = replace(original_execution, **{field: value})
                profiles.append(
                    (
                        field,
                        execution,
                        WorkspaceProfile.model_validate(
                            original_profile.model_dump() | {"environment": environment(execution)}
                        ),
                    )
                )
        else:
            from krim_agent.context.types import CompactionPolicy

            for field, value in [
                ("trigger_ratio", 0.7),
                ("keep_turns", 3),
                ("summary_bytes", 2047),
            ]:
                profiles.append(
                    (
                        field,
                        original_execution,
                        WorkspaceProfile.model_validate(
                            original_profile.model_dump()
                            | {
                                "compaction": CompactionPolicy.model_validate(
                                    original_profile.compaction.model_dump() | {field: value}
                                )
                            }
                        ),
                    )
                )
        await app.close()
        for index, (field, execution, profile) in enumerate(profiles):
            configuration = WorkspaceConfiguration(profile, execution, tmp_path / "objects")
            active, _, _ = await workspace_application(
                data_case, tmp_path, configuration=configuration, provider=provider, wire=wire
            )
            other, _, _, actual = await linked_frame(active, notes, "variant-" + str(index))
            assert actual.subject == frame.subject
            assert actual.conditions != frame.conditions
            assert actual.effective_fingerprint != frame.effective_fingerprint
            comparisons.append(
                {
                    "field": field,
                    "conditions": actual.conditions,
                    "effective_fingerprint": actual.effective_fingerprint,
                }
            )
            await active.store.finish(other.id, "failed", "completion_unverified")
            await active.close()
        assert comparisons
    notes["conditions_matrix"] = {
        "case": case,
        "baseline": frame.model_dump(mode="json"),
        "comparisons": comparisons,
    }
    assert provider.calls == len(wire.requests) == 0


@pytest.mark.parametrize(
    "case",
    [
        "owner-member",
        "viewer-read",
        "cross-workspace",
        "cross-owner",
        "v1-v2-reject",
        "busy-original-key",
    ],
)
async def test_scope_all_work_artifact_and_input_paths(
    data_case: Any,
    tmp_path: Any,
    case: Any,
) -> Any:
    from pydantic import SecretStr
    from test_tool_pipeline import os_profile

    from krim_agent.security import AccessPolicy, Credential, Membership, PrincipalContext
    from krim_agent.sessions.types import ControlRequest
    from krim_agent.workspace.targets import WorkspaceConfiguration, environment
    from krim_agent.workspace.types import ExecutionAccess, WorkspaceProfile

    owner_role: Any = "viewer" if case == "viewer-read" else "member"
    policy = AccessPolicy(
        credentials=(
            Credential(
                principal_id="owner",
                token=SecretStr("synthetic-owner"),
                legacy=True,
                memberships=(Membership(workspace_id="local", role=owner_role),),
            ),
            Credential(
                principal_id="peer",
                token=SecretStr("synthetic-peer"),
                memberships=(Membership(workspace_id="local"),),
            ),
            Credential(
                principal_id="other",
                token=SecretStr("synthetic-other"),
                memberships=(Membership(workspace_id="other"),),
            ),
        )
    )
    execution = cast(Any, os_profile)(tmp_path)
    configuration = WorkspaceConfiguration(
        WorkspaceProfile(max_tools_per_step=4, environment=environment(execution)),
        execution,
        tmp_path / "objects",
    )
    app, wire, provider = await workspace_application(
        data_case, tmp_path, configuration=configuration, access_policy=policy
    )
    # 可信fixture先写历史；viewer负例模拟该owner被撤销写权限后的只读访问。
    seed = PrincipalContext("owner", 1, (Membership(workspace_id="local"),), True)
    session = await app.sessions.create(
        seed, "local", SessionCreate(request_key="d4-api", target_key="target-workspace")
    )
    base = f"/v3/workspaces/local/sessions/{session.id}"
    content = b'{"answer":42}'
    body = {
        "schema_version": 1,
        "request_key": "input-1",
        "expected_head_revision": 0,
        "files": [
            {
                "path": "answer.json",
                "encoding": "base64",
                "data": base64.b64encode(content).decode(),
                "sha256": hashlib.sha256(content).hexdigest(),
            }
        ],
    }
    seeded = await app.artifacts.input(
        ExecutionAccess(seed, "local", session.id),
        InputSnapshotRequest.model_validate_json(json.dumps(body)),
    )
    api = create_app(application=app)
    actor = {"cross-workspace": "other", "cross-owner": "peer"}.get(case, "owner")
    async with api.router.lifespan_context(api):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=api),
            base_url="http://localhost",
            headers={"Authorization": "Bearer synthetic-" + actor},
        ) as client:
            response = await client.get("/v3/workspace/capabilities")
            assert response.status_code == 200
            turn_body = WorkspaceTurnRequest(
                request_key="queued",
                text="被动工作请求",
                expected_revision=0,
                completion=Completion(),
            )
            async with app.service._admission:
                queued = await app.sessions.submit(seed, "local", session.id, turn_body)
                paths = [
                    "/work",
                    "/turns/" + queued.id + "/work",
                    "/turns/by-key/queued",
                    "/inputs/by-key/input-1",
                    "/artifacts/" + seeded.revision_id,
                    "/artifacts/" + seeded.revision_id + "/files/answer.json?offset=0&length=16384",
                ]
                try:
                    for path in paths:
                        value = await client.get(base + path)
                        assert value.status_code == (
                            404 if case in {"cross-workspace", "cross-owner"} else 200
                        ), value.text
                        assert "synthetic-owner" not in value.text
                        assert str(configuration.artifact_root) not in value.text
                    if case in {"cross-workspace", "cross-owner", "viewer-read"}:
                        expected = 403 if case == "viewer-read" else 404
                        assert (await client.post(base + "/inputs", json=body)).status_code == (
                            expected
                        )
                        assert (
                            await client.post(
                                base + "/turns", json=turn_body.model_dump(mode="json")
                            )
                        ).status_code == expected
                    elif case == "v1-v2-reject":
                        v2 = await client.post(
                            f"/v2/workspaces/local/sessions/{session.id}/turns",
                            json={
                                "text": "不能空条件提交",
                                "request_key": "v2",
                                "expected_revision": 1,
                            },
                        )
                        assert v2.json()["error"]["code"] == "workspace_contract_required"
                    else:
                        original = await client.post(
                            base + "/turns", json=turn_body.model_dump(mode="json")
                        )
                        assert original.status_code == 202 and original.json()["id"] == queued.id
                        conflict = turn_body.model_dump(mode="json") | {"text": "同键不同内容"}
                        assert (await client.post(base + "/turns", json=conflict)).json()["error"][
                            "code"
                        ] == "key_conflict"
                        busy = await client.post(
                            base + "/inputs",
                            json=body
                            | {"request_key": "new-while-busy", "expected_head_revision": 1},
                        )
                        assert busy.json()["error"]["code"] == "workspace_busy"
                        known = await client.post(base + "/inputs", json=body)
                        assert known.json()["revision_id"] == seeded.revision_id
                finally:
                    current = await app.sessions.get(seed, "local", session.id)
                    await app.sessions.control(
                        seed,
                        "local",
                        session.id,
                        "cancel_turn",
                        ControlRequest(
                            request_key="cancel-queued", expected_revision=current.revision
                        ),
                        turn_id=queued.id,
                    )
            if case == "v1-v2-reject":
                # v1本身经过原admission锁；此处已释放测试屏障，不能制造自等。
                async with asyncio.timeout(5):
                    v1 = await client.post(
                        "/v1/runs",
                        json={
                            "text": "不能直接执行",
                            "request_key": "raw-run",
                            "target": "target-workspace",
                        },
                    )
                assert v1.json()["error"]["code"] == "workspace_contract_required"
            if case in {"cross-workspace", "cross-owner", "viewer-read", "v1-v2-reject"}:
                assert provider.calls == len(wire.requests) == 0
                return
            uploaded = await client.post(base + "/inputs", json=body)
            assert uploaded.status_code == 200, uploaded.text
            revision = uploaded.json()
            known = await client.get(base + "/inputs/by-key/input-1")
            assert known.json() == revision
            fetched = await client.get(base + "/artifacts/" + revision["revision_id"])
            assert fetched.json() == revision
            chunk = await client.get(
                base
                + "/artifacts/"
                + revision["revision_id"]
                + "/files/answer.json?offset=0&length=16384"
            )
            assert chunk.status_code == 200
            assert base64.b64decode(chunk.json()["data"]) == content
            assert (await client.post(base + "/inputs", json=body)).json() == revision
            assert (
                await client.get(
                    base
                    + "/artifacts/"
                    + revision["revision_id"]
                    + "/files/answer.json?offset=0&length=16385"
                )
            ).status_code == 422
            assert (await client.post(base + "/inputs", content="x" * 65537)).status_code == 413
            invalid = body | {"schema_version": True}
            assert (await client.post(base + "/inputs", json=invalid)).status_code == 422
    assert provider.calls == 0
    assert wire.requests == []


@pytest.mark.parametrize(
    "case",
    [
        "local-readonly",
        "borrowed-owner",
        "remote-detach",
        "unknown-original-key",
        "writer-flags-reject",
    ],
)
async def test_sdk_cli_cold_reads_and_control_ownership(
    data_case: Any,
    tmp_path: Any,
    monkeypatch: Any,
    case: Any,
) -> Any:
    app, wire, provider = await workspace_application(data_case, tmp_path)
    notes = data_case[1]
    session = await app.sessions.create(
        LOCAL, "local", SessionCreate(request_key="sdk-session", target_key="target-workspace")
    )
    body = InputSnapshotRequest(
        request_key="sdk-input",
        expected_head_revision=0,
        files=(
            InputFile(
                path="hello.txt", data="aGVsbG8=", sha256=hashlib.sha256(b"hello").hexdigest()
            ),
        ),
    )
    sdk = WorkspaceSDK(app)
    revision = await sdk.input(session.id, body)
    if case == "writer-flags-reject":
        from krim_agent.cli import execute_workspace

        opened: list[Any] = []
        actual = SQLiteStore.open

        async def observed(cls: Any, *args: Any, **kwargs: Any) -> Any:
            opened.append(str(args[0]))
            return await actual(*args, **kwargs)

        monkeypatch.setattr(SQLiteStore, "open", classmethod(observed))
        forbidden = tmp_path / "must-not-create.sqlite"
        for operation in ["start", "input"]:
            with pytest.raises(ServiceError, match="readonly"):
                await execute_workspace(
                    cast(
                        Any,
                        SimpleNamespace(
                            operation=operation,
                            url=None,
                            database=str(forbidden),
                            request="missing.json",
                        ),
                    )
                )
        assert opened == [] and not forbidden.exists()
        notes["cli_writer_open_calls"] = opened
    elif case == "local-readonly":
        root = app.artifacts.root
        files_before = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in root.iterdir()}
        await app.close()
        database = Path(notes["database"])
        database_hash = hashlib.sha256(await asyncio.to_thread(database.read_bytes)).hexdigest()
        readonly = await SQLiteStore.open(database, readonly=True)
        probe = cast(Any, Connections)(readonly)
        notes.setdefault("store_probes", []).append((readonly, probe))
        before = cast(Any, fds)()
        try:
            reader = WorkspaceSDK(readonly, artifact_root=root)
            assert (await reader.artifact(session.id, revision.revision_id)) == revision
            assert (await reader.input_by_key(session.id, body.request_key)) == revision
            chunk = await reader.chunk(session.id, revision.revision_id, "hello.txt")
            assert base64.b64decode(chunk.data) == b"hello"
            with pytest.raises(ServiceError):
                await reader.chunk(session.id, revision.revision_id, "../escape")
            from krim_agent.workspace.artifacts import ArtifactStore

            original_get = ArtifactStore.get
            entered, release = asyncio.Event(), asyncio.Event()
            blocked = False

            async def getting(artifacts: Any, *args: Any, **kwargs: Any) -> Any:
                value = await original_get(artifacts, *args, **kwargs)
                if blocked:
                    entered.set()
                    await release.wait()
                return value

            monkeypatch.setattr(ArtifactStore, "get", getting)
            for cancellation in ["repeat-cancel", "repeat-close"]:
                entered.clear()
                release.clear()
                blocked = True
                task = asyncio.create_task(
                    reader.chunk(session.id, revision.revision_id, "hello.txt")
                )
                try:
                    async with asyncio.timeout(5):
                        await entered.wait()
                    if cancellation == "repeat-cancel":
                        task.cancel()
                        task.cancel()
                    else:
                        await asyncio.gather(reader.close(), reader.close())
                    async with asyncio.timeout(5):
                        await asyncio.gather(task, return_exceptions=True)
                    assert task.done() and not reader._reads and not reader._artifacts
                    assert cast(Any, fds)() == before
                finally:
                    blocked = False
                    release.set()
                    await asyncio.gather(task, return_exceptions=True)
            await reader.close()
            await reader.close()
            assert not readonly.closed
            assert cast(Any, fds)() == before
            second = await SQLiteStore.open(database)
            await second.close()
        finally:
            await readonly.close()
        assert hashlib.sha256(await asyncio.to_thread(database.read_bytes)).hexdigest() == (
            database_hash
        )
        assert {
            p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in root.iterdir()
        } == files_before
        notes["readonly"] = {
            "database_hash": database_hash,
            "files": files_before,
            "closed": readonly.closed,
            "FD_equal": True,
            "borrowed_store_preserved": True,
        }
    elif case == "borrowed-owner":
        await sdk.close()
        assert not app.store.closed and not app.service.closed
        assert (await sdk.artifact(session.id, revision.revision_id)) == revision
        request = WorkspaceTurnRequest(
            request_key="borrowed-execution",
            text="普通模型提议",
            expected_revision=0,
            completion=Completion(),
        )
        turn = await sdk.start(session.id, request)
        finished = await app.sessions.wait(LOCAL, "local", session.id, turn.id)
        assert finished.code == "completion_unverified"
        assert (await sdk.by_key(session.id, request.request_key)).turn_id == turn.id
        assert provider.calls == len(wire.requests) == 1
        return
    else:
        async with serving(app) as url:
            client = AsyncRunClient(url)
            try:
                remote = WorkspaceSDK(client)
                assert (await remote.artifact(session.id, revision.revision_id)) == revision
                await remote.close()
                assert not client.http.is_closed and not app.store.closed
                if case == "unknown-original-key":
                    request = WorkspaceTurnRequest(
                        request_key="accepted-return-unknown",
                        text="原键不能重发",
                        expected_revision=0,
                        completion=Completion(),
                    )
                    counts = {"POST": 0, "GET": 0}
                    actual_request = client.request

                    async def lost(method: Any, path: Any, body: Any = None) -> Any:
                        counts[method] += 1
                        value = await actual_request(method, path, body)
                        if method == "POST":
                            raise ServiceError("execution_unknown")
                        return value

                    monkeypatch.setattr(client, "request", lost)
                    async with app.service._admission:
                        with pytest.raises(ServiceError, match="execution_unknown"):
                            await remote.start(session.id, request)
                        found = await remote.by_key(session.id, request.request_key)
                        assert found.original_request == request
                        assert found.request_key == request.request_key
                        assert counts == {"POST": 1, "GET": 1}
                        from krim_agent.sessions.types import ControlRequest

                        state = await app.sessions.get(LOCAL, "local", session.id)
                        await app.sessions.control(
                            LOCAL,
                            "local",
                            session.id,
                            "cancel_turn",
                            ControlRequest(
                                request_key="cancel-queued", expected_revision=state.revision
                            ),
                            turn_id=found.turn_id,
                        )
                    notes["unknown_counts"] = counts
            finally:
                await client.close()
    assert provider.calls == len(wire.requests) == 0


@pytest.mark.parametrize(
    "case",
    [
        method + "-" + window
        for method in ("verify", "descriptor", "binding_token", "assert_current")
        for window in ("before", "after-read", "terminal")
    ],
)
async def test_verifier_method_code_is_sealed(
    data_case: Any, tmp_path: Any, monkeypatch: Any, case: Any
) -> Any:
    """保持同一函数对象；三个真实消费窗口只替换安全计数拒绝代码。"""
    import time

    from fixtures.workspace_server import WorkspaceWire
    from sqlalchemy.ext.asyncio import AsyncConnection
    from test_tool_pipeline import approve_fixture

    from krim_agent.workspace import verification

    method, window = (
        case.rsplit("-", 1)
        if not case.endswith("after-read")
        else (case.removesuffix("-after-read"), "after-read")
    )
    app, wire, provider = await workspace_application(data_case, tmp_path, wire=WorkspaceWire())
    notes = data_case[1]
    function = getattr(verification.Verifier, method)
    original_code = function.__code__
    entries: list[str] = []
    timeline: list[Any] = []
    monkeypatch.setattr(verification, "_repair_code_entries", entries, raising=False)

    def rejected(*args: Any, **kwargs: Any) -> Any:
        globals()["_repair_code_entries"].append("replacement-entered")
        raise ServiceError("target_binding_changed")

    async def rejected_async(*args: Any, **kwargs: Any) -> Any:
        globals()["_repair_code_entries"].append("replacement-entered")
        raise ServiceError("target_binding_changed")

    def mutate() -> None:
        assert getattr(verification.Verifier, method) is function
        function.__code__ = rejected_async.__code__ if method == "verify" else rejected.__code__
        assert getattr(verification.Verifier, method) is function
        assert function.__code__ is not original_code
        timeline.append({"window": window, "same_function": True})

    try:
        if window == "terminal":
            cast(Any, approve_fixture)(app, monkeypatch)
            execute = AsyncConnection.execute

            async def query(conn: Any, statement: Any, *args: Any, **kwargs: Any) -> Any:
                value = await execute(conn, statement, *args, **kwargs)
                fields = {
                    getattr(key, "name", key): getattr(value, "value", None)
                    for key, value in getattr(statement, "_values", {}).items()
                }
                proof = fields.get("verification")
                if (
                    not timeline
                    and getattr(statement, "is_update", False)
                    and getattr(getattr(statement, "table", None), "name", None) == "work_turns"
                    and isinstance(proof, dict)
                    and proof["state"] == "passed"
                ):
                    mutate()
                    timeline[-1].update(
                        {
                            "actual_UPDATE_returned": True,
                            "in_transaction": conn.in_transaction(),
                            "connection": id(conn),
                        }
                    )
                return value

            monkeypatch.setattr(AsyncConnection, "execute", query)
            session = await app.sessions.create(
                LOCAL,
                "local",
                SessionCreate(request_key="code-session", target_key="target-workspace"),
            )
            body = WorkspaceTurnRequest(
                request_key="code-turn",
                text="D4_TASK",
                expected_revision=0,
                completion=Completion(required_paths=("calc.py", "answer.json")),
            )
            turn = await app.sessions.submit(LOCAL, "local", session.id, body)
            finished = await app.sessions.wait(LOCAL, "local", session.id, turn.id)
            from krim_agent.sdk.workspace import WorkspaceSDK

            view = await WorkspaceSDK(app).get(session.id, turn.id)
            notes["code_terminal"] = {
                "run_id": finished.run_id,
                "turn_id": turn.id,
                "view": view.model_dump(mode="json"),
                "finished": finished.model_dump(mode="json"),
            }
            assert timeline and timeline[-1]["in_transaction"]
            assert finished.status == "failed" and finished.code == "target_binding_changed"
            assert view.verification is not None and view.verification.state != "passed"
            assert len([effect for effect in view.effects if effect.kind == "command"]) == 1
            assert len(wire.requests) == provider.calls == 4
            before = provider.calls
            assert (await WorkspaceSDK(app).by_key(session.id, body.request_key)).turn_id == turn.id
            assert provider.calls == before
        else:
            run, binding, access, frame = await linked_frame(app, notes, "code-binding")
            verifier = binding.loop._verifier
            assert verifier is not None
            if window == "before":
                mutate()
                with pytest.raises(ServiceError, match="target_binding_changed"):
                    binding.assert_current()
            else:
                actual_read = app.store.work_read

                async def read(*args: Any, **kwargs: Any) -> Any:
                    value = await actual_read(*args, **kwargs)
                    if not timeline:
                        mutate()
                        timeline[-1]["actual_work_read_returned"] = True
                    return value

                monkeypatch.setattr(app.store, "work_read", read)
                with pytest.raises(ServiceError, match="target_binding_changed"):
                    await verifier.verify(
                        run.id,
                        frame,
                        access,
                        check=binding.assert_current,
                        deadline=time.monotonic() + 5,
                    )
                assert timeline and timeline[-1]["actual_work_read_returned"]
            assert provider.calls == len(wire.requests) == 0
            assert not app.runner.jobs and not app.runner._resources
        assert entries == []
    finally:
        function.__code__ = original_code
        notes["method_code_seal"] = {
            "case": case,
            "timeline": timeline,
            "replacement_entries": entries,
            "function_restored": function.__code__ is original_code,
            "provider_calls": provider.calls,
            "wire_requests": len(wire.requests),
        }


@pytest.mark.parametrize("case", ["input-no-turn", "input-old-turn", "turn"])
async def test_confirmed_workspace_identities_are_get_only_recoverable(
    data_case: Any, tmp_path: Any, case: Any
) -> Any:
    """真实九路由按原身份读取；组件传输夹具另测，绝不将其当后端证据。"""
    from krim_agent.sessions.types import ControlRequest

    app, wire, provider = await workspace_application(data_case, tmp_path)
    notes = data_case[1]
    session = await app.sessions.create(
        LOCAL, "local", SessionCreate(request_key="known-session", target_key="target-workspace")
    )
    base = f"/v3/workspaces/local/sessions/{session.id}"
    api = create_app(application=app)
    counts = {"GET": 0, "POST": 0}
    async with api.router.lifespan_context(api):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=api), base_url="http://localhost"
        ) as client:

            async def request(method: str, path: str, body: Any = None) -> Any:
                counts[method] += 1
                value = (
                    await client.request(method, base + path, json=body)
                    if body
                    else (await client.request(method, base + path))
                )
                assert value.status_code in {200, 202}, value.text
                return value.json()

            async with app.service._admission:
                old = None
                if case in {"input-old-turn", "turn"}:
                    body = WorkspaceTurnRequest(
                        request_key="known-turn",
                        text="原工作请求",
                        expected_revision=0,
                        completion=Completion(),
                    ).model_dump(mode="json")
                    old = await request("POST", "/turns", body)
                if case == "turn":
                    assert old is not None
                    accepted = await request("GET", "/turns/by-key/known-turn")
                    assert accepted["turn_id"] == old["id"]
                    identity = accepted["turn_id"]
                    for _ in range(2):
                        found = await request("GET", "/turns/" + identity + "/work")
                        assert found == accepted and found["original_request"] == body
                        assert (await request("GET", "/turns/by-key/known-turn")) == found
                else:
                    if old is not None:
                        current = await app.sessions.get(LOCAL, "local", session.id)
                        await app.sessions.control(
                            LOCAL,
                            "local",
                            session.id,
                            "cancel_turn",
                            ControlRequest(
                                request_key="cancel-old", expected_revision=current.revision
                            ),
                            turn_id=old["id"],
                        )
                    content = b"known original input"
                    body = {
                        "schema_version": 1,
                        "request_key": "known-input",
                        "expected_head_revision": 0,
                        "files": [
                            {
                                "path": "known.txt",
                                "encoding": "base64",
                                "data": base64.b64encode(content).decode(),
                                "sha256": hashlib.sha256(content).hexdigest(),
                            }
                        ],
                    }
                    accepted = await request("POST", "/inputs", body)
                    identity = accepted["revision_id"]
                    for _ in range(2):
                        found = await request("GET", "/artifacts/" + identity)
                        assert found == accepted and found["source_kind"] == "input"
                        assert found["request_key"] == "known-input"
                        assert found["manifest_hash"] == accepted["manifest_hash"]
                        assert (await request("GET", "/inputs/by-key/known-input")) == found
                if case == "turn":
                    assert old is not None
                    current = await app.sessions.get(LOCAL, "local", session.id)
                    await app.sessions.control(
                        LOCAL,
                        "local",
                        session.id,
                        "cancel_turn",
                        ControlRequest(
                            request_key="cancel-known", expected_revision=current.revision
                        ),
                        turn_id=old["id"],
                    )
    assert counts["POST"] == (2 if case == "input-old-turn" else 1)
    assert provider.calls == len(wire.requests) == 0
    assert not app.runner.jobs and not app.runner._resources
    notes["known_identity"] = {
        "case": case,
        "accepted": accepted,
        "identity": identity,
        "counts": counts,
        "provider_calls": provider.calls,
        "exec_entries": 0,
    }
