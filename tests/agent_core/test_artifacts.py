"""D1_DATA：真实对象、持久revision、取消及有界出口。"""

import base64
import hashlib

import pytest
from fixtures.workspace_crash_worker import create_work

from krim_agent.security import LOCAL
from krim_agent.storage.sqlite import SQLiteStore
from krim_agent.workspace.artifacts import ArtifactStore
from krim_agent.workspace.types import ExecutionAccess, InputFile, InputSnapshotRequest


def bundle(key="input", revision=0, path="hello.txt", data=b"hello"):
    return InputSnapshotRequest(
        request_key=key,
        expected_head_revision=revision,
        files=(
            InputFile(
                path=path,
                data=base64.b64encode(data).decode(),
                sha256=hashlib.sha256(data).hexdigest(),
            ),
        ),
    )


@pytest.mark.parametrize(
    "phase",
    [
        "input",
        "receipt_snapshot",
        "export_chunk-cancel",
        "export_chunk-repeat",
        "export_chunk-close",
        "before_cas-cancel",
        "before_cas-repeat",
        "before_cas-close",
        "after_cas-cancel",
        "after_cas-repeat",
        "after_cas-close",
    ],
)
async def test_snapshot_publication_and_bounded_reads(tmp_path, phase, monkeypatch):
    if phase == "receipt_snapshot":
        await receipt_snapshot(tmp_path)
        return
    if phase != "input":
        await publication_cancel(tmp_path, phase)
        return
    store = await SQLiteStore.open(tmp_path / "artifacts.sqlite")
    artifacts = ArtifactStore(store, tmp_path / "objects")
    try:
        session = await create_work(store)
        access = ExecutionAccess(LOCAL, "local", session.id)
        revision = await artifacts.input(access, bundle())
        assert revision.revision == 1

        from fixtures.workspace_crash_worker import start_turn
        from pydantic import ValidationError

        from krim_agent.contracts import ServiceError
        from krim_agent.security import Membership, PrincipalContext

        assert await artifacts.input(access, bundle()) == revision
        assert await artifacts.by_key(access, "input") == revision
        assert (
            base64.b64decode(
                (
                    await artifacts.chunk(
                        access, revision.revision_id, "hello.txt", offset=1, length=2
                    )
                ).data
            )
            == b"el"
        )
        for offset, length in [(False, 1), (0, False), (-1, 1), (0, 16385)]:
            with pytest.raises(ServiceError, match="invalid_input"):
                await artifacts.chunk(
                    access, revision.revision_id, "hello.txt", offset=offset, length=length
                )
        with pytest.raises(ServiceError, match="invalid_input"):
            await artifacts.chunk(
                access, revision.revision_id, "hello.txt", length=2049, model=True
            )
        with pytest.raises(ServiceError, match="artifact_conflict"):
            await artifacts.input(access, bundle(data=b"different"))
        with pytest.raises(ValidationError):
            bundle(data=b"x" * 32769)
        valid = bundle(key="unicode", path="你好.txt")
        assert (
            valid.request_hash()
            == "66eaffed680357956a85490cdeee689f771a91194a173ed6831923e1bb3266bc"
        )
        with pytest.raises((ValidationError, ServiceError)):
            InputSnapshotRequest.model_validate_json(
                valid.model_dump_json().replace("aGVsbG8=", "aGVsbG8=\\n")
            )
        original_write = store._write
        lost = []

        async def lose(operation):
            value = await original_write(operation)
            if ".input." in operation.__qualname__ and not lost:
                lost.append(operation.__qualname__)
                raise OSError("committed input ack lost")
            return value

        monkeypatch.setattr(store, "_write", lose)
        accepted = await artifacts.input(access, bundle(key="ack", revision=1, data=b"ack"))
        assert lost and accepted.revision == 2
        assert (
            await artifacts.input(access, bundle(key="ack", revision=1, data=b"ack"))
        ) == accepted
        assert store.healthy
        monkeypatch.setattr(store, "_write", original_write)
        other = ExecutionAccess(
            PrincipalContext("other", 1, (Membership(workspace_id="local"),)), "local", session.id
        )
        wrong_workspace = ExecutionAccess(LOCAL, "different", session.id)
        for unauthorized in [other, wrong_workspace]:
            for operation in [
                lambda unauthorized=unauthorized: artifacts.get(unauthorized, revision.revision_id),
                lambda unauthorized=unauthorized: artifacts.head(unauthorized),
                lambda unauthorized=unauthorized: artifacts.by_key(unauthorized, "input"),
                lambda unauthorized=unauthorized: artifacts.chunk(
                    unauthorized, revision.revision_id, "hello.txt"
                ),
                lambda unauthorized=unauthorized: artifacts.input(unauthorized, bundle()),
            ]:
                with pytest.raises(ServiceError, match="not_found"):
                    await operation()
        viewer = ExecutionAccess(
            PrincipalContext("local", 1, (Membership(workspace_id="local", role="viewer"),)),
            "local",
            session.id,
        )
        assert await artifacts.get(viewer, revision.revision_id) == revision
        with pytest.raises(ServiceError, match="permission_denied"):
            await artifacts.input(viewer, bundle())
        await start_turn(store, access)
        assert await artifacts.input(access, bundle()) == revision
        with pytest.raises(ServiceError, match="workspace_busy"):
            await artifacts.input(access, bundle(key="busy", revision=2))
    finally:
        await artifacts.close()
        await store.close()


@pytest.mark.parametrize(
    "kind",
    [
        "secret",
        "traversal",
        "absolute",
        "leaf-link",
        "parent-link",
        "hardlink",
        "fifo",
        "socket",
        "device",
        "depth",
        "size",
        "total",
        "entries",
        "files",
        "replace-link",
        "replace-file",
        "concurrent",
    ],
)
async def test_nonregular_paths_capacity_and_concurrent_publish(tmp_path, kind, monkeypatch):
    import asyncio
    import os
    import socket
    from pathlib import Path

    from pydantic import ValidationError

    from krim_agent.contracts import ServiceError
    from krim_agent.execution.linux import fds
    from krim_agent.workspace import artifacts as implementation

    if kind in {"secret", "traversal", "absolute"}:
        with pytest.raises((ValueError, ValidationError, ServiceError)):
            bundle(path={"secret": ".env.production", "traversal": "../x", "absolute": "/x"}[kind])
        return
    before = fds()
    store = await SQLiteStore.open(tmp_path / "objects.sqlite")
    artifacts = ArtifactStore(store, tmp_path / "objects")
    sock = None
    try:
        session = await create_work(store)
        access = ExecutionAccess(LOCAL, "local", session.id)
        r0 = await artifacts.head(access)
        if kind == "concurrent":
            reached = 0
            release = asyncio.Event()

            async def hold(phase):
                nonlocal reached
                if phase == "before_cas":
                    reached += 1
                    if reached == 2:
                        release.set()
                    await release.wait()

            artifacts.hook = hold
            results = await asyncio.gather(
                artifacts.input(access, bundle(key="a", data=b"A")),
                artifacts.input(access, bundle(key="b", data=b"B")),
                return_exceptions=True,
            )
            good = [r for r in results if not isinstance(r, BaseException)]
            bad = [r for r in results if isinstance(r, ServiceError)]
            assert len(good) == len(bad) == 1
            assert bad[0].code == "input_revision_conflict"
            assert (await artifacts.head(access)).revision_id == good[0].revision_id
            assert (await artifacts.get(access, r0.revision_id)) == r0
            return
        tree = tmp_path / "input"
        tree.mkdir()
        path = tree / "blocked"
        if kind == "leaf-link":
            (tmp_path / "target").write_text("never open")
            path.symlink_to(tmp_path / "target")
        elif kind == "parent-link":
            alias = tmp_path / "alias"
            alias.symlink_to(tree)
            tree = alias
        elif kind == "hardlink":
            path.write_text("linked")
            os.link(path, tree / "linked")
        elif kind == "fifo":
            os.mkfifo(path)
        elif kind == "socket":
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            directory = os.open(tree, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
            try:
                sock.bind(f"/proc/self/fd/{directory}/blocked")
            finally:
                os.close(directory)
        elif kind == "device":
            tree = Path("/dev")
            original = os.listdir
            identity = tree.stat()

            def only_null(fd):
                st = os.fstat(fd) if type(fd) is int else None
                if st and (st.st_ino, st.st_dev) == (identity.st_ino, identity.st_dev):
                    return ["null"]
                return original(fd)

            monkeypatch.setattr(implementation.os, "listdir", only_null)
        elif kind == "depth":
            for _ in range(17):
                path.mkdir()
                path = path / "deep"
        elif kind == "size":
            with path.open("wb") as f:
                f.truncate(1048577)
        elif kind == "total":
            for i in range(9):
                (tree / str(i)).write_bytes(b"x" * 1048576)
        elif kind == "entries":
            for i in range(129):
                (tree / str(i)).mkdir()
        elif kind == "files":
            for i in range(33):
                (tree / str(i)).write_bytes(b"x")
        elif kind in {"replace-link", "replace-file"}:
            path.write_bytes(b"old")

            async def replace(phase):
                if phase == "lstat_open" and path.exists():
                    path.rename(tmp_path / "old")
                    if kind == "replace-link":
                        path.symlink_to(tmp_path / "old")
                    else:
                        path.write_bytes(b"new")

            artifacts.hook = replace
        original_open = os.open
        nonregular_open = []

        def checked_open(name, flags, *args, **kwargs):
            if (
                kind in {"fifo", "socket"}
                and name == "blocked"
                or kind == "device"
                and name == "null"
            ):
                nonregular_open.append(str(name))
            return original_open(name, flags, *args, **kwargs)

        monkeypatch.setattr(implementation.os, "open", checked_open)
        with pytest.raises(ServiceError, match="artifact_invalid|artifact_limit"):
            await artifacts.snapshot_directory(tree)
        assert nonregular_open == []
        assert (await artifacts.head(access)).revision_id == r0.revision_id
    finally:
        if sock is not None:
            sock.close()
        await artifacts.close()
        await store.close()
    assert not artifacts.jobs
    assert fds() == before


async def publication_cancel(tmp_path, phase):
    import asyncio
    import json

    from fixtures.workspace_crash_worker import (
        effect_intent,
        profile_for,
        resource_events,
        start_turn,
        watch_store,
    )

    from krim_agent.contracts import ServiceError
    from krim_agent.execution.linux import SandboxRunner, fds
    from krim_agent.execution.types import Request
    from krim_agent.storage import work

    point, action = phase.rsplit("-", 1)
    before_fds = fds()
    store = await SQLiteStore.open(tmp_path / "cancel.sqlite")
    pool = watch_store(store)
    artifacts = ArtifactStore(store, tmp_path / "objects")
    reached = asyncio.Event()

    async def hold(current):
        if current == point:
            reached.set()
            await asyncio.Event().wait()

    profile = profile_for("publication-" + tmp_path.name)
    runner = SandboxRunner(profile, hook=hold)
    task = None
    try:
        session = await create_work(store)
        access = ExecutionAccess(LOCAL, "local", session.id)
        r0 = await artifacts.input(access, bundle(path="keep.txt", data=b"retained"))
        turn, run, body, _ = await start_turn(store, access)
        call_id = await effect_intent(store, access, run)
        artifacts.hook = hold

        async def command_and_publish():
            chunk = await artifacts.chunk(access, r0.revision_id, "keep.txt")
            result = await runner.run(
                Request(
                    ("-c", "from pathlib import Path;Path('result.txt').write_text('real-output')"),
                    files={"keep.txt": base64.b64decode(chunk.data)},
                )
            )
            assert result.exit_code == 0 and result.output_complete and result.cleanup == "clean"
            return await artifacts.publish(
                access, key="effect", expected=1, run_id=run.id, call_id=call_id, receipt=result
            )

        task = asyncio.create_task(command_and_publish())
        async with asyncio.timeout(20):
            await reached.wait()
        if action == "close":
            await artifacts.close()
            await artifacts.close()
            await runner.close()
        else:
            task.cancel()
            if action == "repeat":
                task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        await runner.close()
        await runner.close()
        if not artifacts.closed:
            await artifacts.close()
        artifacts = ArtifactStore(store, tmp_path / "objects")
        assert await artifacts.get(access, r0.revision_id) == r0
        if point == "after_cas":
            r1 = await artifacts.by_key(access, "effect", kind="effect")
            assert r1.revision == 2 and r1.parent_id == r0.revision_id
            assert (
                base64.b64decode((await artifacts.chunk(access, r1.revision_id, "result.txt")).data)
                == b"real-output"
            )
        else:
            with pytest.raises(ServiceError, match="not_found"):
                await artifacts.by_key(access, "effect", kind="effect")
            assert (await artifacts.head(access)).revision_id == r0.revision_id
        effect = await store.work_read(lambda conn: work.effect(conn, access, call_id))
        if point != "export_chunk":
            assert effect["receipt"]["executor_started"] is True
            assert effect["receipt"]["exit_code"] == 0
            assert effect["receipt"]["cleanup"] == "clean"
        await store.recover()
        effect_after = await store.work_read(lambda conn: work.effect(conn, access, call_id))
        if point != "export_chunk":
            assert effect_after["receipt"]["exit_code"] == 0
        events = resource_events(profile)
        assert sum(r["kind"] == "gate_opened" for r in events) == 1
        assert (
            sum(r["kind"] == "process_created" for r in events)
            == sum(r["kind"] == "pidfd_closed" for r in events)
            == 1
        )
        assert any(r["kind"] == "populated_zero" for r in events)
        assert any(r["kind"] == "leaf_removed" for r in events)
        assert not runner.jobs and not runner._resources
        rid = (await artifacts.head(access)).revision_id
        await artifacts.close()
        await store.close()
        reader = await SQLiteStore.open(tmp_path / "cancel.sqlite", readonly=True)
        artifacts = ArtifactStore(reader, tmp_path / "objects")
        try:
            assert (await artifacts.get(access, r0.revision_id)) == r0
            assert (await artifacts.head(access)).revision_id == rid
        finally:
            await artifacts.close()
            await reader.close()
    finally:
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await runner.close()
        await artifacts.close()
        await store.close()
    assert pool.count("checkout") == pool.count("checkin")
    assert fds() == before_fds
    (tmp_path / "publication-resources.json").write_text(
        json.dumps(
            {
                "phase": phase,
                "pool": pool,
                "runner_closed": runner.closed,
                "artifact_closed": artifacts.closed,
                "jobs": len(runner.jobs),
                "fds_equal": True,
                "execution_count": 1,
                "store_closed": store.closed,
                "pidfd_closed": True,
            }
        )
    )


async def receipt_snapshot(tmp_path):
    from fixtures.workspace_crash_worker import effect_intent, execute, start_turn

    store = await SQLiteStore.open(tmp_path / "snapshot.sqlite")
    artifacts = ArtifactStore(store, tmp_path / "objects")
    try:
        session = await create_work(store)
        access = ExecutionAccess(LOCAL, "local", session.id)
        turn, run, body, _ = await start_turn(store, access)
        call = await effect_intent(store, access, run)
        receipt = await execute("snapshot-" + tmp_path.name)

        async def mutate(phase):
            if phase == "before_stage":
                receipt.files["result.txt"] = b"late mutation"

        artifacts.hook = mutate
        revision = await artifacts.publish(
            access, key="snapshot", expected=0, run_id=run.id, call_id=call, receipt=receipt
        )
        chunk = await artifacts.chunk(access, revision.revision_id, "result.txt")
        assert base64.b64decode(chunk.data) == b"real-output"
        await store.finish(run.id, "failed", "completion_unverified")
    finally:
        await artifacts.close()
        await store.close()


async def test_readonly_input_has_no_filesystem_effect(tmp_path, monkeypatch, request):
    import stat

    from fixtures.workspace_crash_worker import repair_close_artifacts, repair_observe

    from krim_agent.contracts import ServiceError

    def objects(root):
        result = {}
        for file in root.iterdir():
            info = file.lstat()
            assert stat.S_ISREG(info.st_mode)
            result[file.name] = hashlib.sha256(file.read_bytes()).hexdigest()
        return result

    async with repair_observe(tmp_path, request.node.nodeid, monkeypatch) as (watch, notes):
        database, root = tmp_path / "readonly.sqlite", tmp_path / "objects"
        writer = watch(await SQLiteStore.open(database), database)
        artifacts = ArtifactStore(writer, root)
        try:
            session = await create_work(writer)
            access = ExecutionAccess(LOCAL, "local", session.id)
            accepted = await artifacts.input(access, bundle())
        finally:
            await repair_close_artifacts(artifacts, notes)
            await writer.close()
        reader = watch(await SQLiteStore.open(database, readonly=True), database)
        artifacts = ArtifactStore(reader, root)
        try:
            before = database.read_bytes()
            old_files = objects(root)
            stage_entries = []
            actual_stage = artifacts._stage

            def stage(files):
                stage_entries.append(list(files))
                return actual_stage(files)

            monkeypatch.setattr(artifacts, "_stage", stage)
            with pytest.raises(ServiceError, match="readonly"):
                await artifacts.input(
                    access,
                    bundle(key="readonly-new", revision=1, data=b"SYNTHETIC-NEW-READONLY-CONTENT"),
                )
            notes.update(
                {
                    "database": str(database),
                    "objects_before": old_files,
                    "objects_after": objects(root),
                    "stage_entries": stage_entries,
                }
            )
            assert database.read_bytes() == before
            assert objects(root) == old_files
            assert stage_entries == []
            assert not any(name.startswith(".stage-") for name in objects(root))
            assert await artifacts.get(access, accepted.revision_id) == accepted
            assert await artifacts.head(access) == accepted
            assert await artifacts.by_key(access, "input") == accepted
            assert await artifacts.input(access, bundle()) == accepted
            assert (
                base64.b64decode(
                    (await artifacts.chunk(access, accepted.revision_id, "hello.txt")).data
                )
                == b"hello"
            )
            with pytest.raises(ServiceError, match="not_found"):
                await artifacts.by_key(access, "readonly-new")
            assert database.read_bytes() == before and objects(root) == old_files
            notes["exec_count"] = 0
        finally:
            await repair_close_artifacts(artifacts, notes)
            await reader.close()


@pytest.mark.parametrize("terminal", ["finish", "recover"])
async def test_rejected_output_keeps_known_receipt(tmp_path, monkeypatch, request, terminal):
    from fixtures.workspace_crash_worker import (
        effect_intent,
        profile_for,
        repair_close_artifacts,
        repair_observe,
        resource_events,
        start_turn,
    )

    from krim_agent.contracts import ServiceError
    from krim_agent.execution.linux import SandboxRunner
    from krim_agent.execution.types import Request
    from krim_agent.storage import work

    async with repair_observe(tmp_path, request.node.nodeid, monkeypatch) as (watch, notes):
        database, root = tmp_path / "rejected.sqlite", tmp_path / "objects"
        store = watch(await SQLiteStore.open(database), database)
        artifacts = ArtifactStore(store, root)
        profile = profile_for("rejected-" + tmp_path.name)
        notes["runner_profiles"].append(str(profile.evidence))
        runner = SandboxRunner(profile)
        try:
            session = await create_work(store)
            access = ExecutionAccess(LOCAL, "local", session.id)
            r0 = await artifacts.input(access, bundle())
            turn, run, body, _ = await start_turn(store, access)
            call_id = await effect_intent(store, access, run)
            result = await runner.run(
                Request(
                    (
                        "-c",
                        "from pathlib import Path;"
                        "Path('.env.production').write_text('SYNTHETIC-NOT-A-CREDENTIAL')",
                    )
                )
            )
            assert result.exit_code == 0 and result.output_complete and result.cleanup == "clean"
            assert result.files == {".env.production": b"SYNTHETIC-NOT-A-CREDENTIAL"}
            with pytest.raises(ServiceError, match="artifact_invalid"):
                await artifacts.publish(
                    access,
                    key="rejected",
                    expected=1,
                    run_id=run.id,
                    call_id=call_id,
                    receipt=result,
                )
            ledger = await store.work_read(lambda conn: work.effect(conn, access, call_id))
            notes.update(
                {
                    "database": str(database),
                    "run_id": run.id,
                    "call_id": call_id,
                    "known_runner_receipt": {
                        "exit_code": result.exit_code,
                        "output_complete": result.output_complete,
                        "cleanup": result.cleanup,
                    },
                    "receipt_at_rejection": ledger["receipt"],
                }
            )
            assert ledger["receipt"] is not None
            assert ledger["receipt"]["exit_code"] == 0
            assert ledger["receipt"]["output_complete"] is True
            assert ledger["receipt"]["cleanup"] == "clean"
            assert (await artifacts.head(access)).revision_id == r0.revision_id
            with pytest.raises(ServiceError, match="not_found"):
                await artifacts.by_key(access, "rejected", kind="effect")
            await runner.close()
            if terminal == "finish":
                await store.finish(run.id, "failed", "artifact_invalid")
            else:
                await store.recover()
            after = await store.work_read(lambda conn: work.effect(conn, access, call_id))
            assert after["phase"] == "settled" and after["receipt"]["exit_code"] == 0
            assert (
                after["receipt"]["output_complete"] is True
                and after["receipt"]["cleanup"] == "clean"
            )
            await repair_close_artifacts(artifacts, notes)
            await store.close()
            reader = watch(await SQLiteStore.open(database, readonly=True), database)
            cold = ArtifactStore(reader, root)
            try:
                persisted = await reader.work_read(lambda conn: work.effect(conn, access, call_id))
                assert persisted["receipt"] == after["receipt"]
                assert await cold.head(access) == r0
                assert await cold.get(access, r0.revision_id) == r0
            finally:
                await repair_close_artifacts(cold, notes)
                await reader.close()
            events = resource_events(profile)
            assert sum(e["kind"] == "gate_opened" for e in events) == 1
            assert sum(e["kind"] == "process_created" for e in events) == 1
            assert sum(e["kind"] == "process_waited" for e in events) == 1
            assert sum(e["kind"] == "pidfd_closed" for e in events) == 1
            assert any(e["kind"] == "populated_zero" for e in events)
            assert any(e["kind"] == "leaf_removed" for e in events)
            notes.update({"exec_count": 1, "exec_replayed": 0, "terminal": terminal})
        finally:
            await runner.close()
            await repair_close_artifacts(artifacts, notes)
            await store.close()


@pytest.mark.parametrize(
    "case",
    [
        "conflict-once",
        "conflict-repeated",
        "storage-once",
        "storage-repeated",
        "fd-retry",
    ],
)
async def test_close_failed_transaction_keeps_cleanup_owner(tmp_path, monkeypatch, request, case):
    import asyncio
    import os

    from fixtures.workspace_crash_worker import repair_close_artifacts, repair_observe
    from sqlalchemy.ext.asyncio import AsyncConnection

    from krim_agent.contracts import ServiceError
    from krim_agent.workspace import artifacts as module

    async with repair_observe(tmp_path, request.node.nodeid, monkeypatch) as (watch, notes):
        database = tmp_path / "close.sqlite"
        store = watch(await SQLiteStore.open(database), database)
        artifacts = ArtifactStore(store, tmp_path / "objects")
        publish_task = None
        closers = []
        entered, release = asyncio.Event(), asyncio.Event()
        close_join = asyncio.Event()
        actual_gather = module.asyncio.gather

        def joining(*tasks, **kwargs):
            if any(task in artifacts.jobs and task.cancelling() for task in tasks):
                close_join.set()
            return actual_gather(*tasks, **kwargs)

        monkeypatch.setattr(module.asyncio, "gather", joining)
        actual_execute = AsyncConnection.execute
        inserts = []
        fd = artifacts.fd
        close_attempts, fd_success = [], []
        real_close = os.close

        def close_descriptor(number):
            if number == fd:
                close_attempts.append(number)
                if case == "fd-retry" and len(close_attempts) == 1:
                    raise OSError("controlled own-root close obstacle before syscall")
                fd_success.append(number)
            return real_close(number)

        try:
            session = await create_work(store)
            access = ExecutionAccess(LOCAL, "local", session.id)
            r0 = await artifacts.input(access, bundle())

            async def blocked(conn, statement, *args, **kwargs):
                value = await actual_execute(conn, statement, *args, **kwargs)
                if (
                    conn.engine is store.engine
                    and getattr(statement, "is_insert", False)
                    and (
                        getattr(getattr(statement, "table", None), "name", None)
                        == "artifact_revisions"
                    )
                ):
                    inserts.append("actual artifact INSERT before commit")
                    entered.set()
                    await release.wait()
                    if case.startswith("conflict"):
                        raise ServiceError("input_revision_conflict")
                    if case.startswith("storage"):
                        raise OSError("controlled actual transaction failure")
                return value

            monkeypatch.setattr(AsyncConnection, "execute", blocked)
            publish_task = asyncio.create_task(
                artifacts.input(
                    access, bundle(key="closing", revision=1, data=b"bounded closing input")
                )
            )
            async with asyncio.timeout(5):
                await entered.wait()
            monkeypatch.setattr(module.os, "close", close_descriptor)
            closers = [
                asyncio.create_task(artifacts.close()),
                asyncio.create_task(artifacts.close()),
            ]
            async with asyncio.timeout(5):
                await close_join.wait()
            assert any(t.cancelling() for t in artifacts.jobs)
            assert not any(t.done() for t in closers)
            notes["shared_close_task"] = id(artifacts._close_task)
            if case.endswith("repeated"):
                for task in closers:
                    task.cancel()
                    task.cancel()
            release.set()
            outcomes = await asyncio.gather(publish_task, *closers, return_exceptions=True)
            notes["outcomes"] = [type(x).__name__ for x in outcomes]
            assert all(t.done() for t in [publish_task, *closers])
            if case == "fd-retry":
                assert not artifacts.closed
                info = os.fstat(fd)
                assert (info.st_dev, info.st_ino) == (
                    artifacts.identity.st_dev,
                    artifacts.identity.st_ino,
                )
            # 允许第一次报告业务异常，但后续close必须能完成同一资源的收束。
            try:
                await artifacts.close()
            except (ServiceError, OSError, asyncio.CancelledError) as error:
                notes["repeat_close_error"] = type(error).__name__
            assert artifacts.closed
            with pytest.raises(OSError):
                os.fstat(fd)
            await artifacts.close()
            assert not artifacts.jobs
            assert len(inserts) == 1 and len(fd_success) == 1
            assert len(close_attempts) == (2 if case == "fd-retry" else 1)
            notes.update(
                {
                    "root_fd_closed": True,
                    "close_attempts": close_attempts,
                    "fd_success": fd_success,
                    "publishes_entered": len(inserts),
                    "exec_count": 0,
                }
            )
            # 原owner已经close；此后新目录FD可复用同一整数，停止原owner计数。
            monkeypatch.setattr(module.os, "close", real_close)
            readback = ArtifactStore(store, tmp_path / "objects")
            try:
                assert await readback.get(access, r0.revision_id) == r0
                if case != "fd-retry":
                    assert await readback.head(access) == r0
                    with pytest.raises(ServiceError, match="not_found"):
                        await readback.by_key(access, "closing")
                else:
                    found = await readback.by_key(access, "closing")
                    assert found.revision == 2
            finally:
                # 恢复计数器，避免将新的读者FD复用误记为原owner二次close。
                monkeypatch.setattr(module.os, "close", real_close)
                await repair_close_artifacts(readback, notes)
        finally:
            release.set()
            if publish_task is not None and not publish_task.done():
                publish_task.cancel()
            await asyncio.gather(
                *([publish_task] if publish_task is not None else []),
                *closers,
                return_exceptions=True,
            )
            monkeypatch.setattr(module.os, "close", real_close)
            await repair_close_artifacts(artifacts, notes)
            await store.close()
