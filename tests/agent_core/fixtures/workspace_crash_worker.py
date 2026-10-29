"""D1_DATA 的有限真实生产消费者；不复制 Store、Artifact 或 Runner 实现。"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import sys
from contextlib import asynccontextmanager
from pathlib import Path


def root() -> Path:
    return Path(os.environ["KRIM_D1_DATA_ROOT"])


def save_marker(path: Path, value) -> None:
    temporary = path.with_suffix(".tmp")
    with temporary.open("x") as stream:
        stream.write(json.dumps(value))
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def profile_for(name: str):
    from krim_agent.execution.types import Profile

    c = root()
    name = Path(os.environ["KRIM_D1_EVIDENCE"]).name + "-" + name
    base = c / "evidence/prep-base/BASE-QUALIFIED.json"
    original = Path(
        "/artifacts/krim-agent-core-20261019/acceptance/ac-m2-vm-base-20261019T164700/evidence/prep-base/BASE-QUALIFIED.json"
    )
    raw = original.read_bytes()
    assert (
        hashlib.sha256(raw).hexdigest()
        == "15ed3606c75cd3e41f15e0be6541ad71d419e83c8cf95702ab3dc726f63bf82e"
    )
    if not base.exists():
        base.parent.mkdir(parents=True, exist_ok=True)
        with base.open("xb") as f:
            f.write(raw)
    assert base.read_bytes() == raw
    evidence = c / "evidence/d1-os" / name
    evidence.mkdir(parents=True, exist_ok=True)
    return Profile(
        rootfs=Path(json.loads(raw)["rootfs"]["path"]),
        base=base,
        cgroup_root=Path("/sys/fs/cgroup/system.slice/krim-task.service"),
        material=c / "materials/d1-os" / name,
        evidence=evidence,
    )


async def seed_legacy(database: Path, output: Path) -> None:
    from krim_agent.composition import open_application
    from krim_agent.contracts import RunRequest, fingerprint
    from krim_agent.evaluation.types import EvalRequest, FreezeRequest, rule
    from krim_agent.observability import project_trace
    from krim_agent.runtime.facts import project_facts
    from krim_agent.security import LOCAL
    from krim_agent.sessions.types import SessionCreate, TurnRequest

    async with await open_application(database) as app:
        run = await app.service.start(RunRequest(text="/echo keep", request_key="complete"))
        await app.service.wait(run.id)
        session = await app.sessions.create(
            LOCAL, "local", SessionCreate(request_key="legacy-session")
        )
        turn = await app.sessions.submit(
            LOCAL,
            "local",
            session.id,
            TurnRequest(request_key="turn", text="/echo history", expected_revision=0),
        )
        await app.sessions.wait(LOCAL, "local", session.id, turn.id)
        dataset = await app.evaluations.freeze(
            FreezeRequest.model_validate(
                {
                    "name": "legacy",
                    "version": "1",
                    "cases": [
                        {
                            "case_id": "one",
                            "source_kind": "manual",
                            "input": "/echo keep",
                            "reason": "synthetic",
                            "reference": {
                                "expected": "keep",
                                "expected_status": "completed",
                                "expected_code": None,
                                "expected_tools": None,
                                "trajectory": None,
                                "expected_schema": None,
                            },
                        }
                    ],
                }
            )
        )
        evaluation = await app.evaluations.start(
            EvalRequest.model_validate(
                {
                    "request_key": "legacy-eval",
                    "dataset": {k: getattr(dataset, k) for k in ["name", "version", "fingerprint"]},
                    "target_key": "target-v2",
                    "rules": [rule("status_code").model_dump(mode="json")],
                    "execution": {
                        "repetitions": 1,
                        "concurrency": 1,
                        "item_timeout_seconds": 12,
                        "scorer_timeout_seconds": 1,
                        "batch_timeout_seconds": 600,
                    },
                }
            )
        )
        await app.evaluations.wait(evaluation.id)
        active, _ = await app.store.begin(
            RunRequest(text="not dispatched", request_key="active"), {}
        )
        facts = project_facts(await app.store.snapshot(run.id)).model_dump(mode="json")
        trace = project_trace(await app.store.snapshot(run.id)).model_dump(mode="json")
        report = await app.evaluations.report(evaluation.id)
        await asyncio.to_thread(
            output.write_text,
            json.dumps(
                {
                    "version": app.store.schema_version,
                    "run_id": run.id,
                    "active_id": active.id,
                    "session_id": session.id,
                    "turn_id": turn.id,
                    "eval_id": evaluation.id,
                    "facts": facts,
                    "trace": trace,
                    "report": report,
                    "facts_hash": fingerprint(facts),
                    "trace_hash": fingerprint(trace),
                    "source": __import__(
                        "krim_agent.storage.sqlite", fromlist=["__file__"]
                    ).__file__,
                },
                ensure_ascii=False,
            ),
        )


def watch_store(store):
    from sqlalchemy import event

    events = []

    @event.listens_for(store.engine.sync_engine, "checkout")
    def checkout(*_):
        events.append("checkout")

    @event.listens_for(store.engine.sync_engine, "checkin")
    def checkin(*_):
        events.append("checkin")

    return events


def target_for():
    from dataclasses import asdict

    from krim_agent.execution import linux, seccomp
    from krim_agent.workspace.types import WorkspaceProfile
    from krim_agent.workspace.verification import descriptor

    p = profile_for("identity")
    base = json.loads(p.base.read_text())
    modules = {
        n: hashlib.sha256(Path(linux.__file__).with_name(n + ".py").read_bytes()).hexdigest()
        for n in ["__init__", "linux", "_broker", "cgroups", "seccomp", "types"]
    }
    projection = {
        k: v
        for k, v in asdict(p).items()
        if k in {"wall", "cpu_seconds", "memory", "pids", "output"}
    }
    projection.update(
        policy="python-unittest-x86_64/1",
        modules=modules,
        filter=hashlib.sha256(seccomp.program()).hexdigest(),
        rootfs_manifest=base["rootfs"]["manifest_sha256"],
        bwrap=base["namespace"]["sha256"],
    )
    return {
        "key": "target-workspace",
        "tools": [],
        "workspace_verification": descriptor(),
        "workspace_profile": WorkspaceProfile(
            max_tools_per_step=4, environment=projection
        ).model_dump(mode="json"),
    }


async def create_work(store):
    from krim_agent.config import Settings
    from krim_agent.context.types import ContextPolicy
    from krim_agent.sessions.types import SessionCreate
    from krim_agent.storage import sessions as sql

    return await sql.create_session(
        store,
        "local",
        "local",
        SessionCreate(request_key="work", target_key="target-workspace"),
        target_for(),
        ContextPolicy(),
        Settings(),
    )


async def start_turn(store, access, *, key="turn", expected=0, selector=None):
    from krim_agent.config import Settings
    from krim_agent.storage import sessions as sql
    from krim_agent.workspace.types import Completion, InputSelector, WorkspaceTurnRequest

    body = WorkspaceTurnRequest(
        request_key=key,
        text="write bounded artifact",
        expected_revision=expected,
        input=selector or InputSelector(),
        completion=Completion(),
    )
    turn = await sql.submit(store, access.session_id, body, Settings(), access=access)
    request, _ = await store.turn_request(turn.id)
    run, created = await store.begin_turn_run(turn.id, request, target_for(), Settings())
    return turn, run, body, created


async def effect_intent(store, access, run, *, key="effect", call_id=None):
    import uuid

    from krim_agent.contracts import fingerprint
    from krim_agent.storage import work

    call_id = call_id or str(uuid.uuid4())

    async def operation(conn):
        return await work.intent(
            conn,
            access,
            run.id,
            call_id=call_id,
            operation_key=key,
            args_hash=fingerprint({"program": "python"}),
            tool_hash=fingerprint("command/1"),
            kind="command",
        )

    async def confirm():
        return await store.work_read(lambda conn: work.effect(conn, access, call_id))

    await store.work_write(operation, confirm)

    async def dispatch(conn):
        await work.dispatch(conn, call_id, access)
        return True

    async def dispatched():
        row = await confirm()
        return True if row and row["phase"] == "dispatch_intent" else None

    await store.work_write(dispatch, dispatched)
    return call_id


async def execute(name, *, hook=None):
    from krim_agent.execution.linux import SandboxRunner
    from krim_agent.execution.types import Request

    runner = SandboxRunner(profile_for(name), hook=hook)
    try:
        result = await runner.run(
            Request(
                (
                    "-c",
                    "from pathlib import Path;Path('result.txt').write_text('real-output');"
                    "print('effect',flush=True)",
                )
            )
        )
        assert result.cleanup == "clean" and result.exit_code == 0
        assert result.files == {"result.txt": b"real-output"}
        return result
    finally:
        await runner.close()
        assert runner.closed and not runner.jobs and not runner._resources


async def crash(database: Path, marker: Path, phase: str, name: str) -> None:
    from krim_agent.execution.linux import SandboxRunner
    from krim_agent.execution.types import Request
    from krim_agent.security import LOCAL
    from krim_agent.storage.sqlite import SQLiteStore
    from krim_agent.workspace.artifacts import ArtifactStore
    from krim_agent.workspace.types import ExecutionAccess

    store = await SQLiteStore.open(database)
    artifacts = ArtifactStore(store, database.parent / "objects")
    session = await create_work(store)
    access = ExecutionAccess(LOCAL, "local", session.id)
    initial = await artifacts.head(access)
    turn, run, body, _ = await start_turn(store, access)
    from krim_agent.config import Settings
    from krim_agent.storage import sessions as sql
    from krim_agent.workspace.types import Completion, WorkspaceTurnRequest

    await sql.submit(
        store,
        session.id,
        WorkspaceTurnRequest(
            request_key="queued-next",
            text="must remain paused",
            expected_revision=1,
            completion=Completion(),
        ),
        Settings(),
        access=access,
    )
    call_id = await effect_intent(store, access, run)
    facts = {
        "session_id": session.id,
        "turn_id": turn.id,
        "run_id": run.id,
        "call_id": call_id,
        "initial": initial.model_dump(mode="json"),
        "body": body.model_dump(mode="json"),
        "phase": phase,
        "provider_calls": 0,
        "source": __import__("krim_agent.storage.sqlite", fromlist=["__file__"]).__file__,
    }

    async def hold(current):
        if current == phase:
            await asyncio.to_thread(save_marker, marker, facts)
            await asyncio.Event().wait()

    runner = SandboxRunner(profile_for(name), hook=hold)
    artifacts.hook = hold
    try:
        receipt = await runner.run(
            Request(
                (
                    "-c",
                    "from pathlib import Path;Path('result.txt').write_text('crash-output');"
                    "print('effect',flush=True)",
                )
            )
        )
        facts["receipt"] = {
            "exit_code": receipt.exit_code,
            "output_complete": receipt.output_complete,
            "cleanup": receipt.cleanup,
        }
        await artifacts.publish(
            access, key="effect", expected=0, run_id=run.id, call_id=call_id, receipt=receipt
        )
    finally:
        await runner.close()
        await artifacts.close()
        await store.close()


def resource_events(profile):
    import stat

    result = []
    for file in sorted(profile.evidence.glob("job-*/resources.jsonl")):
        assert stat.S_ISREG(file.lstat().st_mode)
        result.extend(json.loads(line) for line in file.read_text().splitlines())
    return result


async def verify_crash(tmp_path: Path, phase: str):
    import signal

    from krim_agent.config import Settings
    from krim_agent.execution.linux import SandboxRunner
    from krim_agent.security import LOCAL
    from krim_agent.storage import sessions as sql
    from krim_agent.storage import work
    from krim_agent.storage.sqlite import SQLiteStore
    from krim_agent.workspace.artifacts import ArtifactStore
    from krim_agent.workspace.types import ExecutionAccess, WorkspaceTurnRequest

    database, marker = tmp_path / "crash.sqlite", tmp_path / "marker.json"
    name = "crash-" + tmp_path.name
    profile = profile_for(name)
    argv = [
        sys.executable,
        "-I",
        "-B",
        str(Path(__file__)),
        "crash",
        str(database),
        str(marker),
        phase,
        name,
    ]
    env = {
        "PATH": "/usr/local/bin:/usr/bin:/bin",
        "LD_LIBRARY_PATH": str(Path(sys.base_prefix) / "lib"),
        "TMPDIR": os.environ["TMPDIR"],
        "KRIM_D1_DATA_ROOT": str(root()),
        "KRIM_D1_EVIDENCE": os.environ["KRIM_D1_EVIDENCE"],
        "PYTHONDONTWRITEBYTECODE": "1",
    }
    with (
        (tmp_path / "worker.stdout").open("xb") as stdout,
        (tmp_path / "worker.stderr").open("xb") as stderr,
    ):
        spawn = asyncio.create_task(
            asyncio.create_subprocess_exec(
                *argv, cwd=Path.cwd(), env=env, stdout=stdout, stderr=stderr
            )
        )
        try:
            proc = await asyncio.shield(spawn)
        except asyncio.CancelledError:
            proc = await spawn
            proc.kill()
            await proc.wait()
            raise
        pidfd = os.pidfd_open(proc.pid)
        try:
            async with asyncio.timeout(20):
                while not marker.exists():
                    assert proc.returncode is None, (tmp_path / "worker.stderr").read_text()
                    await asyncio.sleep(0.02)
            signal.pidfd_send_signal(pidfd, signal.SIGKILL)
            async with asyncio.timeout(5):
                assert await proc.wait() == -signal.SIGKILL
        finally:
            if proc.returncode is None:
                signal.pidfd_send_signal(pidfd, signal.SIGKILL)
                await proc.wait()
            os.close(pidfd)
    facts = json.loads(marker.read_text())
    before = resource_events(profile)
    runner = SandboxRunner(profile)
    recovered = runner.recovered
    try:
        if phase != "after_cas":
            assert recovered and all(
                r["leaf_removed"] and r["exit_code"] is None and r["status"] == "unknown"
                for r in recovered
            )
            assert all("populated 0" in r["events_after"] for r in recovered)
        assert not list((profile.material / "tickets").glob("*.json"))
    finally:
        await runner.close()
    store = await SQLiteStore.open(database)
    pool = watch_store(store)
    artifacts = ArtifactStore(store, tmp_path / "objects")
    try:
        access = ExecutionAccess(LOCAL, "local", facts["session_id"])
        prior = await artifacts.get(access, facts["initial"]["revision_id"])
        assert prior.model_dump(mode="json") == facts["initial"]
        await store.recover()
        snap = await store.snapshot(facts["run_id"])
        assert snap.run.status == "interrupted"
        assert snap.work.phase == "interrupted"
        assert (await sql.get_session(store, facts["session_id"])).dispatch_paused is True
        body = WorkspaceTurnRequest.model_validate_json(json.dumps(facts["body"]))
        assert (
            await sql.submit(store, facts["session_id"], body, Settings(), access=access)
        ).id == facts["turn_id"]
        ledger = await store.work_read(lambda conn: work.effect(conn, access, facts["call_id"]))
        if phase == "after_cas":
            assert ledger["phase"] == "settled"
            r1 = await artifacts.by_key(access, "effect", kind="effect")
            assert r1.revision == 1 and r1.revision_id == ledger["receipt"]["artifact_revision_id"]
        else:
            assert ledger["phase"] == "unknown" and ledger["receipt"]["exit_code"] is None
            assert (await artifacts.head(access)).revision_id == prior.revision_id
        assert resource_events(profile) == before
        assert facts["provider_calls"] == 0
        assert sum(r["kind"] == "gate_opened" for r in before) == (phase != "ready")
    finally:
        await artifacts.close()
        await store.close()
    assert pool.count("checkout") == pool.count("checkin")
    second = await SQLiteStore.open(database)
    await second.close()
    report = {
        "phase": phase,
        "worker": {
            "argv": argv,
            "env": env,
            "cwd": str(Path.cwd()),
            "pid": proc.pid,
            "returncode": proc.returncode,
            "waited": True,
            "pidfd_closed": True,
        },
        "worker_internal_finally": "unknown",
        "recovery": recovered,
        "pool": pool,
        "provider_replayed": 0,
        "execution_replayed": 0,
        "events_unchanged": True,
    }
    (tmp_path / "crash-resources.json").write_text(json.dumps(report, indent=2))


async def seed_schema2(database: Path):
    import krim_agent
    from krim_agent.composition import open_application
    from krim_agent.contracts import RunRequest
    from krim_agent.evaluation.trace_cases import collect_candidate
    from krim_agent.providers.fake import FakeProvider

    provider = FakeProvider()
    app = await open_application(database, provider=provider)
    try:
        complete = await app.service.start(
            RunRequest(text="/add 2 3", request_key="ac-turn.completed")
        )
        await app.service.wait(complete.id)
        snapshot = await app.store.snapshot(complete.id)
        facts = (await app.service.result(complete.id)).model_dump(mode="json")
        trace = (await app.service.trace(complete.id)).model_dump(mode="json")
        (database.parent / "candidate.json").write_text(
            json.dumps({"candidate": collect_candidate(snapshot, True).model_dump(mode="json")})
        )
        provider.entered.clear()
        active = await app.service.start(RunRequest(text="/slow", request_key="ac-turn.active"))
        async with asyncio.timeout(5):
            await provider.entered.wait()
        assert provider.calls == 3 and provider.active == 1
        pending = await app.store.snapshot(active.id)
        (database.parent / "seed.json").write_text(
            json.dumps(
                {
                    "version": krim_agent.__version__,
                    "module": krim_agent.__file__,
                    "completed_id": complete.id,
                    "active_id": active.id,
                    "provider_calls": provider.calls,
                    "actual_active_stream": provider.active,
                    "facts": facts,
                    "trace": trace,
                    "active_events": [e.model_dump(mode="json") for e in pending.events],
                }
            )
        )
        await asyncio.Event().wait()
    finally:
        await app.close()


if __name__ == "__main__":
    mode = sys.argv[1]
    if mode == "legacy":
        sys.path.insert(0, str(root() / "materials/product/m1-source/src"))
        asyncio.run(seed_legacy(Path(sys.argv[2]), Path(sys.argv[3])))
    elif mode == "legacy2":
        sys.path.insert(0, str(root() / "materials/product/schema2-source/src"))
        asyncio.run(seed_schema2(Path(sys.argv[2])))
    elif mode == "crash":
        sys.path.insert(0, str(root() / "source/src"))
        asyncio.run(crash(Path(sys.argv[2]), Path(sys.argv[3]), sys.argv[4], sys.argv[5]))
    else:
        raise SystemExit("unknown fixture mode")


# D1_DATA_REPAIR_01：仅实际资源观测与失败库归档，不复制业务实现。


@asynccontextmanager
async def repair_observe(tmp_path, node, monkeypatch):
    import sqlite3
    import stat
    from contextlib import closing

    from test_session_read_sides import Connections

    from krim_agent.execution.linux import fds
    from krim_agent.providers.fake import FakeProvider
    from krim_agent.providers.openai_compatible import OpenAICompatibleProvider

    def consumer_files():
        consumers = {}
        for name in (
            "krim_agent.storage.sessions",
            "krim_agent.storage.work",
            "krim_agent.workspace.artifacts",
            "krim_agent.execution.linux",
        ):
            module_path = Path(sys.modules[name].__file__)
            assert module_path.is_absolute()
            assert module_path.is_relative_to(root() / "source/src")
            assert stat.S_ISREG(module_path.lstat().st_mode)
            consumers[name] = {
                "path": str(module_path),
                "sha256": hashlib.sha256(module_path.read_bytes()).hexdigest(),
            }
        return consumers

    consumers = consumer_files()
    before = fds()
    existing = asyncio.all_tasks()
    tracked = []
    notes = {
        "node": node,
        "uid": os.getuid(),
        "consumers": consumers,
        "provider_stream_entries": 0,
        "runner_profiles": [],
        "emergency_root_fd_closes": [],
    }

    def watch(store, database):
        tracked.append((store, database, Connections(store)))
        return store

    async def no_provider(*args, **kwargs):
        notes["provider_stream_entries"] += 1
        raise AssertionError("D1 data repair must not call a Provider")
        yield  # 保持原async-generator调用形状，绝不调用被替换的网络实现。

    monkeypatch.setattr(FakeProvider, "stream", no_provider)
    monkeypatch.setattr(OpenAICompatibleProvider, "stream", no_provider)
    failure = None
    try:
        yield watch, notes
    except BaseException as error:
        failure = type(error).__name__
        raise
    finally:
        # 此时测试finally已按原句柄收束；不以GC/过滤FD掩盖错误。
        after = fds()
        pending = [
            t.get_name()
            for t in asyncio.all_tasks()
            if t is not asyncio.current_task() and t not in existing and not t.done()
        ]
        path = Path(os.environ["KRIM_D1_EVIDENCE"]) / "cases"
        path.mkdir(exist_ok=True)
        record = notes | {
            "failure": failure,
            "fds_before": before,
            "fds_after": after,
            "FD_equal": before == after,
            "unexpected_tasks": pending,
            "stores": [
                {
                    "database": str(db),
                    "closed": store.closed,
                    "pending": sorted(probe.pending),
                    "unsafe_gc": probe.unsafe,
                    "events": probe.events,
                }
                for store, db, probe in tracked
            ],
        }
        backups = []
        if failure:
            for store, database, _ in tracked:
                if not database.exists() or not store.closed:
                    continue
                assert stat.S_ISREG(database.lstat().st_mode)
                dst = path / (
                    hashlib.sha256((node + str(database)).encode()).hexdigest() + ".sqlite"
                )
                if dst.exists():
                    continue
                original_hash = hashlib.sha256(database.read_bytes()).hexdigest()
                with closing(
                    sqlite3.connect(database.absolute().as_uri() + "?mode=ro", uri=True)
                ) as src:
                    with closing(sqlite3.connect(dst)) as dest, dest:
                        src.backup(dest)
                assert hashlib.sha256(database.read_bytes()).hexdigest() == original_hash
                backups.append(
                    {
                        "source": str(database),
                        "uri": database.absolute().as_uri() + "?mode=ro",
                        "source_sha256": original_hash,
                        "backup": str(dst),
                        "backup_sha256": hashlib.sha256(dst.read_bytes()).hexdigest(),
                    }
                )
        record["failure_backups"] = backups
        (path / (hashlib.sha256(node.encode()).hexdigest() + ".json")).write_text(
            json.dumps(record, indent=2)
        )
        assert notes["provider_stream_entries"] == 0
        assert all(s.closed and not p.pending and p.unsafe == 0 for s, _, p in tracked)
        assert before == after
        assert not pending


async def repair_close_artifacts(artifacts, notes):
    """RED的产品close缺陷也必须真实收束；外部兜底与产品closed分别记录。"""
    try:
        await artifacts.close()
    except (Exception, asyncio.CancelledError) as error:
        notes.setdefault("close_errors", []).append(type(error).__name__)
    if not artifacts.closed:
        try:
            info = os.fstat(artifacts.fd)
        except OSError:
            return
        if (info.st_dev, info.st_ino) != (artifacts.identity.st_dev, artifacts.identity.st_ino):
            raise AssertionError("refuse closing an unowned reused FD")
        os.close(artifacts.fd)
        notes["emergency_root_fd_closes"].append(
            {"fd": artifacts.fd, "product_closed": False, "actual_root_fd_closed": True}
        )
