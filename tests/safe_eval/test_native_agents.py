"""真实SQLite/进程的离线适配合同；fixture从不标记为真实原生CLI。"""

import asyncio
import copy
import hashlib
import inspect
import json
import os
import sqlite3
import sys
from importlib.resources import files
from pathlib import Path

import pytest
import pytest_asyncio

from krim_agent.contracts import ServiceError, fingerprint
from krim_agent.evaluation.adapters import codex_events, process
from krim_agent.evaluation.adapters.claude_code import ClaudeCodeAdapter
from krim_agent.evaluation.adapters.codex_cli import CodexCLIAdapter
from krim_agent.evaluation.adapters.profiles import (
    CampaignBootstrap,
    NativeProfile,
    file_digest,
    program_materials,
    slot_for,
)
from krim_agent.evaluation.app import open_eval
from krim_agent.evaluation.domain import Budget, DatasetSpec, TaskSpec
from krim_agent.evaluation.metrics import MetricRegistry
from krim_agent.evaluation.reports import gate_reports
from krim_agent.evaluation.verifiers import NativeVerifier

DATABASES = []
APPLICATIONS = []


@pytest_asyncio.fixture(autouse=True)
async def record_node(request, tmp_path, recwarn):
    import stat
    import time

    from sqlalchemy import event
    from sqlalchemy.pool import Pool

    DATABASES.clear()
    APPLICATIONS.clear()
    (tmp_path / "node-identification.json").write_text(
        json.dumps({"node": request.node.nodeid, "directory": str(tmp_path), "consumer": __file__})
    )
    loop = asyncio.get_running_loop()
    before_tasks = asyncio.all_tasks()
    before_fd = len(os.listdir("/proc/self/fd"))
    unexpected = []
    previous = loop.get_exception_handler()
    connections = []
    listeners = []
    for kind in ["connect", "checkout", "reset", "checkin", "close"]:

        def listener(*args, kind=kind):
            connections.append({"event": kind, "connection": id(args[0]), "at": time.monotonic()})

        event.listen(Pool, kind, listener)
        listeners.append((kind, listener))
    loop.set_exception_handler(lambda current, context: unexpected.append(context))
    try:
        yield
    finally:
        await asyncio.sleep(0)
        records, backup_errors = [], []
        for index, database in enumerate(DATABASES):
            if not database.exists():
                records.append({"database": str(database), "state": "not_created"})
                continue
            st = database.lstat()
            assert stat.S_ISREG(st.st_mode) and st.st_nlink == 1
            backup = tmp_path / f"readonly-backup-{index}.sqlite"
            source = destination = None
            try:
                source = sqlite3.connect(database.as_uri() + "?mode=ro", uri=True)
                destination = sqlite3.connect(backup)
                source.backup(destination)
                assert source.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
                names = {
                    r[0]
                    for r in source.execute("SELECT name FROM sqlite_master WHERE type='table'")
                }
                links = []
                if "eval_tasks" in names:
                    for ident, raw in source.execute("SELECT id,body FROM eval_tasks"):
                        body = json.loads(raw)
                        links.append(
                            {
                                "task_id": ident,
                                "request_hash": body["request_hash"],
                                "items": [
                                    {
                                        "case_id": i["case_id"],
                                        "ref": i["execution_ref"],
                                        "status": i["status"],
                                    }
                                    for i in body["items"]
                                ],
                            },
                        )
                records.append(
                    {
                        "database": str(database),
                        "backup": str(backup),
                        "tasks": links,
                        "sha256": hashlib.sha256(backup.read_bytes()).hexdigest(),
                    }
                )
            except sqlite3.Error as error:
                # 不触发原失败库的写恢复；先保存静止原件和热日志，再使采证失败可见。
                bundle = tmp_path / f"raw-database-{index}"
                bundle.mkdir()
                raw_files = []
                for suffix in ["", "-journal", "-wal", "-shm"]:
                    original = Path(str(database) + suffix)
                    try:
                        st = await asyncio.to_thread(original.lstat)
                    except FileNotFoundError:
                        continue
                    assert stat.S_ISREG(st.st_mode) and st.st_nlink == 1
                    data = await asyncio.to_thread(original.read_bytes)
                    copied = bundle / original.name
                    copied.write_bytes(data)
                    raw_files.append(
                        {
                            "original": str(original),
                            "copy": str(copied),
                            "sha256": hashlib.sha256(data).hexdigest(),
                        }
                    )
                backup_errors.append(str(error))
                records.append(
                    {
                        "database": str(database),
                        "state": "consistent_backup_unavailable",
                        "error": str(error),
                        "raw_files": raw_files,
                    }
                )
            finally:
                if destination is not None:
                    destination.close()
                if source is not None:
                    source.close()
        resources = []
        for app in APPLICATIONS:
            resources.append(
                {
                    "database": str(app.repository.path),
                    "closed": app.repository.closed,
                    "connections": app.repository.resources,
                    "kernel_tasks": {k: t.done() for k, t in app.kernel.tasks.items()},
                    "processes": []
                    if app.owner is None
                    else [
                        {
                            "ref": j.ref.id,
                            "task_id": j.prepared.spec.task_id,
                            "pid": j.process.pid,
                            "returncode": j.process.returncode,
                            "pidfd_closed": j.pidfd == -1,
                            "collector_joined": j.collector.done() if j.collector else False,
                            "stdout_eof": j.stdout_eof,
                            "stderr_eof": j.stderr_eof,
                            "stream_tasks_joined": all(t.done() for t in j.stream_tasks),
                            "cleanup_receipts": j.cleanup_receipts,
                        }
                        for j in app.owner.jobs.values()
                    ],
                }
            )
        pending = asyncio.all_tasks() - before_tasks - {asyncio.current_task()}
        (tmp_path / "node-evidence.json").write_text(
            json.dumps(
                {
                    "node": request.node.nodeid,
                    "uid": os.getuid(),
                    "databases": records,
                    "resources": resources,
                    "all_connections": connections,
                    "fd_before": before_fd,
                    "fd_after": len(os.listdir("/proc/self/fd")),
                    "pending": [repr(t) for t in pending],
                    "unexpected": [str(c) for c in unexpected],
                    "backup_errors": backup_errors,
                    "warnings": [str(w.message) for w in recwarn],
                    "real_cli_launches": (
                        None if request.node.name.startswith("test_real_") else 0
                    ),
                    "model_requests": (None if request.node.name.startswith("test_real_") else 0),
                    "consumer": __file__,
                },
                indent=2,
            )
        )
        loop.set_exception_handler(previous)
        for kind, listener in listeners:
            event.remove(Pool, kind, listener)
        for app in APPLICATIONS:
            assert app.repository.closed
            events = [v["event"] for v in app.repository.resources]
            assert events.count("checkout") == events.count("checkin")
            assert events.count("connect") == events.count("close")
            assert events.count("reset") == events.count("checkin")
            assert all(t.done() for t in app.kernel.tasks.values())
        assert all(app.repository.closed for app in APPLICATIONS)
        assert all(all(t.done() for t in app.kernel.tasks.values()) for app in APPLICATIONS)
        events = [r["event"] for r in connections]
        assert events.count("connect") == events.count("close")
        assert events.count("checkout") == events.count("reset") == events.count("checkin")
        assert not backup_errors, backup_errors
        assert not pending, pending
        assert not unexpected, unexpected
        assert not recwarn, [str(w.message) for w in recwarn]


def repair_dataset():
    return DatasetSpec.model_validate_json(
        files("krim_agent.evaluation.examples").joinpath("native_repair.v1.json").read_bytes()
    )


def environment():
    return {
        k: os.environ[k]
        for k in ["PATH", "LANG", "LD_LIBRARY_PATH", "PYTHONDONTWRITEBYTECODE"]
        if k in os.environ
    }


async def native_app(tmp_path, family="codex", *, mode="normal", item_timeout=20, extra_keys=()):
    dataset = repair_dataset()
    registry = MetricRegistry()
    request = TaskSpec(
        request_key="fixture-normal",
        target_key="fixture-" + family,
        dataset=dataset.ref(),
        metrics=registry.specs("native"),
        execution=Budget(item_timeout_seconds=item_timeout),
    )
    other_family = "claude" if family == "codex" else "codex"
    compare_request = request.model_copy(
        update={"request_key": "fixture-compare", "target_key": "fixture-" + other_family}
    )
    requests = [request, compare_request] + [
        request.model_copy(update={"request_key": key}) for key in extra_keys
    ]
    root = tmp_path / "campaign"
    database = root / "eval.sqlite"
    DATABASES.append(database)
    bootstrap = CampaignBootstrap(root, {r.request_key: slot_for(r, dataset) for r in requests})
    seeded = await open_eval(database, campaign_bootstrap=bootstrap)
    APPLICATIONS.append(seeded)
    binding = seeded.campaign_binding
    await seeded.close()
    assert binding is not None
    worker = tmp_path / "registered-worker.py"
    source_worker = Path(__file__).parent / "fixtures/native_cli_worker.py"
    worker.write_bytes(source_worker.read_bytes())
    (tmp_path / "worker-material.json").write_text(
        json.dumps(
            {"source": str(source_worker), "copy": str(worker), "sha256": file_digest(worker)}
        )
    )
    subject = {
        "family": "fixture-" + family,
        "cli_version": "fixture-worker/1",
        "adapter_digest": file_digest(
            Path(inspect.getfile(CodexCLIAdapter if family == "codex" else ClaudeCodeAdapter))
        ),
        "codec_digest": file_digest(Path(codex_events.__file__)),
        "process_owner_digest": file_digest(Path(process.__file__)),
        "model": "none",
        "effort": "none",
        "service_speed": "standard",
        "tier": "not_applicable",
        "provider_protocol": "fixture-jsonl/1",
        "endpoint_role": "none",
        "auth_lease_id": "none",
        "auth_lease_epoch": 0,
        "fixture_mode": mode,
    }
    subject.update(program_materials(family, worker))
    subject["subject_config_digest"] = fingerprint(subject)
    other_subject = copy.deepcopy(subject)
    other_subject["family"] = "fixture-" + other_family
    other_subject["adapter_digest"] = file_digest(
        Path(inspect.getfile(CodexCLIAdapter if other_family == "codex" else ClaudeCodeAdapter))
    )
    other_subject.update(program_materials(other_family, worker))
    other_subject.pop("subject_config_digest")
    other_subject["subject_config_digest"] = fingerprint(other_subject)
    common = {
        "dataset_fingerprint": dataset.ref().fingerprint,
        "reset_digest": fingerprint(dataset.model_dump(mode="json")),
        "verifier_digest": NativeVerifier().digest,
        "metric_fingerprint": fingerprint([m.model_dump(mode="json") for m in request.metrics]),
        "budget": request.execution.model_dump(mode="json"),
        "ordinary_isolation_digest": hashlib.sha256(
            b"trusted-offline-fixture-in-dedicated-vm"
        ).hexdigest(),
        "public_environment": environment(),
        "path_template_version": "owned-trial/1",
        "allowed_subject_fingerprints": sorted([fingerprint(subject), fingerprint(other_subject)]),
    }
    profile = NativeProfile(
        "fixture-" + family, family, worker, environment(), common, subject, binding
    )
    other_profile = NativeProfile(
        "fixture-" + other_family,
        other_family,
        worker,
        environment(),
        common,
        other_subject,
        binding,
    )
    app = await open_eval(database, native_profiles=[profile, other_profile])
    app._fixture_profiles = {profile.key: profile, other_profile.key: other_profile}
    app._fixture_requests = {
        request.request_key: request,
        compare_request.request_key: compare_request,
    }
    APPLICATIONS.append(app)
    assert Path(inspect.getfile(type(app))).is_relative_to(
        Path(os.environ["KRIM_PN2_ROOT"]) / "source/src"
    )
    await app.repository.freeze(dataset)
    adapter = app.repository.registry[profile.key]
    return app, adapter, request, profile


@pytest.mark.parametrize("target", ["fixture-codex", "fixture-claude", "krim-script-v2"])
async def test_same_kernel_native_trial_contract(tmp_path, target):
    if target == "krim-script-v2":
        from krim_agent.composition import open_application
        from krim_agent.evaluation.adapters.krim import NeutralKrimAdapter
        from krim_agent.evaluation.ports import AdapterLease

        host = await open_application(tmp_path / "runs.sqlite")
        neutral = NeutralKrimAdapter(host.store, host.service, root=tmp_path / "krim-input")
        database = tmp_path / "neutral.sqlite"
        DATABASES.extend([database, tmp_path / "runs.sqlite"])
        app = await open_eval(database, adapters={target: AdapterLease(neutral, owned=False)})
        APPLICATIONS.append(app)
        try:
            dataset = DatasetSpec.model_validate(
                {
                    "name": "krim-neutral",
                    "version": "1",
                    "cases": [
                        {
                            "case_id": "sum",
                            "instruction": "/add 2 3",
                            "files": {"INPUT.txt": "/add 2 3"},
                            "reference": {"expected": "5"},
                            "verifier": "krim-script/1",
                        }
                    ],
                }
            )
            await app.repository.freeze(dataset)
            request = TaskSpec(
                request_key="neutral-krim",
                target_key=target,
                dataset=dataset.ref(),
                metrics=app.repository.metric_registry.specs("krim"),
            )
            view = await app.run(request)
            result = await app.kernel.wait(view["id"])
            item = result["items"][0]
            assert result["status"] == "completed", result
            assert item["quality"] == "passed", item
            assert item["output"]["exit_code"] is None and item["output"]["process"] is None
            assert item["execution_ref"] == item["output"]["target_result"]["run_id"]
            assert (await app.run(request))["id"] == view["id"]
            assert neutral.execute_calls == 1
        finally:
            await app.close()
            assert not host.store.closed and not host.service.closed
            await neutral.close()
            await host.close()
        return
    family = target.removeprefix("fixture-")
    app, adapter, request, profile = await native_app(tmp_path, family)
    try:
        view = await app.run(request)
        result = await app.kernel.wait(view["id"])
        report = await app.repository.report(view["id"])
        item = result["items"][0]
        assert result["status"] == "completed", result
        assert item["quality"] == "passed", item
        assert item["output"]["native"]["complete"] is True
        assert item["output"]["process"]["fixture_only"] is True
        assert item["output"]["verification"]["passed"] is True
        assert item["output"]["exit_code"] == 0
        assert len(item["output"]["native"]["tools"]) == 2
        assert item["output"]["artifacts"]["outputs/ledger.json"]["sha256"]
        receipt = result["environment"]["native_admission"]
        assert (
            receipt["task_id"] == view["id"] and receipt["request_hash"] == result["request_hash"]
        )
        assert receipt["reserved"] is True and receipt["slot_id"] == request.request_key
        assert adapter.ref_calls == adapter.execute_calls == 1
        assert (await app.run(request))["id"] == view["id"]
        assert adapter.ref_calls == adapter.execute_calls == 1
        assert gate_reports(report, report)["status"] == "pass"
    finally:
        await app.close()

    reader = await open_eval(profile.binding.database, readonly=True)
    APPLICATIONS.append(reader)
    try:
        cold = await reader.repository.by_key(request.model_dump(mode="json"))
        assert (
            cold["id"] == view["id"] and cold["items"][0]["execution_ref"] == item["execution_ref"]
        )
        assert await reader.repository.report(view["id"]) == report
        assert reader.adapter is None and reader.repository.metric_registry.calls == 0
    finally:
        await reader.close()


async def until(predicate):
    loop = asyncio.get_running_loop()
    ready = asyncio.Event()
    handle = None

    def poll():
        nonlocal handle
        if predicate():
            ready.set()
        else:
            handle = loop.call_later(0.005, poll)

    try:
        poll()
        await asyncio.wait_for(ready.wait(), 2)
    finally:
        if handle is not None:
            handle.cancel()


@pytest.mark.parametrize(
    "case",
    [
        "before_ref",
        "streaming",
        "verifier",
        "repeat_cancel",
        "timeout",
        "close",
        "return_lost",
        "cleanup_unknown",
        "after_ref_before_collect",
        "initial_verifier",
    ],
)
async def test_native_lifecycle_join_and_unknown(tmp_path, monkeypatch, case):
    app, adapter, request, profile = await native_app(
        tmp_path,
        mode="live" if case in {"streaming", "repeat_cancel", "timeout", "close"} else "normal",
        item_timeout=1 if case == "timeout" else 20,
        extra_keys=("must-not-replay",),
    )
    entered, release, cleaning = asyncio.Event(), asyncio.Event(), asyncio.Event()
    from krim_agent.evaluation.runner import ItemWork

    original_cleanup = ItemWork.cleanup

    async def measured_cleanup(work):
        cleaning.set()
        await original_cleanup(work)

    monkeypatch.setattr(ItemWork, "cleanup", measured_cleanup)
    pending = []
    try:
        original_gate = adapter._phase_gate
        target_phase = {"verifier": "verifier", "initial_verifier": "initial_verifier"}.get(case)
        if case == "before_ref":
            original_execute = adapter.execute

            async def hold_ref(prepared):
                ref = await original_execute(prepared)
                entered.set()
                await release.wait()
                return ref

            monkeypatch.setattr(adapter, "execute", hold_ref)
            adapter._methods = adapter._identities()
        if target_phase:

            async def gate(phase):
                await original_gate(phase)
                if phase == target_phase:
                    entered.set()
                    await release.wait()

            monkeypatch.setattr(adapter, "_phase_gate", gate)
            adapter._methods = adapter._identities()
        if case == "after_ref_before_collect":

            async def refuse_attach(*args):
                raise ServiceError("execution_unknown")

            monkeypatch.setattr(app.repository, "attach", refuse_attach)
        if case == "return_lost":
            original_write = app.repository._write
            lost = False

            async def lose_result(operation):
                nonlocal lost
                result = await original_write(operation)
                snapshot = await app.repository.by_key(request.model_dump(mode="json"))
                if (
                    snapshot is not None
                    and snapshot["items"][0]["content_hash"] is not None
                    and not lost
                ):
                    lost = True
                    raise OSError("SYNTHETIC_RESULT_RETURN_LOST")
                return result

            monkeypatch.setattr(app.repository, "_write", lose_result)
        if case == "cleanup_unknown":
            original_cancel = adapter.cancel

            async def conservative(ref):
                from krim_agent.evaluation.domain import CleanupResult

                result = await original_cancel(ref)
                return CleanupResult(False, "synthetic_unconfirmed_cleanup", result.observation)

            monkeypatch.setattr(adapter, "cancel", conservative)
            adapter._methods = adapter._identities()

            # 让原runner实际进入cleanup，不伪造成功终态。
            async def refuse(*args):
                raise ServiceError("execution_unknown")

            monkeypatch.setattr(app.repository, "attach", refuse)
        view = await app.run(request)
        if case in {"return_lost", "after_ref_before_collect", "cleanup_unknown"}:
            result = await app.kernel.wait(view["id"])
            if case == "return_lost":
                assert lost and result["items"][0]["quality"] == "passed"
                assert (await app.run(request))["id"] == view["id"]
                assert adapter.execute_calls == 1
            else:
                assert result["status"] == "failed" and result["code"] == "execution_unknown"
                assert adapter.execute_calls == 0
                assert all(not j.resources for j in adapter.jobs.values())
                if case == "cleanup_unknown":
                    assert app.kernel.cleanup_unknown
                    with pytest.raises(ServiceError, match="storage_unavailable"):
                        await app.run(request.model_copy(update={"request_key": "must-not-replay"}))
                    monkeypatch.setattr(adapter, "cancel", original_cancel)
        elif case == "timeout":
            result = await app.kernel.wait(view["id"])
            assert result["items"][0]["quality"] == "timed_out"
            assert result["items"][0]["output"]["cleanup"] == "closed"
            assert adapter.execute_calls <= 1
        else:
            if target_phase or case == "before_ref":
                await asyncio.wait_for(entered.wait(), 2)
            else:
                await until(
                    lambda: any(
                        (Path(j.prepared.directory) / ".fixture-ready").exists()
                        for j in adapter.jobs.values()
                    )
                )
            if case == "close":
                pending.append(asyncio.create_task(app.close()))
            else:
                pending.append(asyncio.create_task(app.kernel.cancel(view["id"])))
                if case == "repeat_cancel":
                    pending.append(asyncio.create_task(app.kernel.cancel(view["id"])))
                    pending[0].cancel()
                    pending[0].cancel()
            await asyncio.wait_for(cleaning.wait(), 2)
            if case == "before_ref":
                assert all(not task.done() for task in pending)
                assert adapter.execute_calls == 0
            release.set()
            await asyncio.gather(*pending, return_exceptions=True)
            if case != "close":
                result = await app.repository.snapshot(view["id"])
                assert result["status"] == "aborted"
                assert result["items"][0]["output"]["cleanup"] == "closed"
            if case in {"streaming", "repeat_cancel", "close", "verifier"}:
                assert adapter.execute_calls == 1
            else:
                assert adapter.execute_calls == 0
        assert all(j.collector is None or j.collector.done() for j in adapter.jobs.values())
    finally:
        release.set()
        await asyncio.gather(*pending, return_exceptions=True)
        await app.close()


@pytest.mark.parametrize(
    "case",
    [
        "argv",
        "env",
        "executable",
        "auth_epoch",
        "subject_axis",
        "common_drift",
        "missing_conditions",
        "owner_entry",
        "codec_entry",
        "verifier_entry",
    ],
)
async def test_frozen_native_conditions_and_compare(tmp_path, monkeypatch, case):
    from krim_agent.evaluation.domain import neutral_fingerprint
    from krim_agent.evaluation.reports import validate_neutral_report

    app, adapter, request, profile = await native_app(tmp_path, extra_keys=("fixture-second",))
    entered, release = asyncio.Event(), asyncio.Event()
    calls = 0

    def replacement(*args, **kwargs):
        nonlocal calls
        calls += 1
        raise AssertionError("changed entry must never be called")

    try:
        if case in {"subject_axis", "common_drift", "missing_conditions"}:
            view = await app.run(request)
            await app.kernel.wait(view["id"])
            before = await app.repository.report(view["id"])
            if case == "missing_conditions":
                old = copy.deepcopy(before)
                old["environment"].pop("execution_conditions")
                old["target_descriptor"].pop("execution_conditions")
                old["target_fingerprint"] = neutral_fingerprint(old["target_descriptor"])
                old["report_hash"] = neutral_fingerprint(
                    {k: v for k, v in old.items() if k != "report_hash"}
                )
                assert validate_neutral_report(old) == old
                assert gate_reports(old, old)["status"] == "invalid"
                assert "execution_conditions" not in old["environment"]
            elif case == "subject_axis":
                other = app._fixture_requests["fixture-compare"]
                candidate = await app.run(other)
                await app.kernel.wait(candidate["id"])
                after = await app.repository.report(candidate["id"])
                assert before["target_fingerprint"] != after["target_fingerprint"]
                assert gate_reports(before, after)["status"] == "pass"
                assert app.repository.registry[other.target_key].execute_calls == 1
            else:
                await app.close()
                env = dict(profile.environment, LANG="C")
                common = copy.deepcopy(profile.common)
                common["public_environment"] = env
                candidate_profile = NativeProfile(
                    profile.key,
                    profile.family,
                    profile.worker,
                    env,
                    common,
                    profile.subject,
                    profile.binding,
                )
                candidate_app = await open_eval(
                    profile.binding.database, native_profiles=[candidate_profile]
                )
                APPLICATIONS.append(candidate_app)
                try:
                    candidate = request.model_copy(update={"request_key": "fixture-second"})
                    view = await candidate_app.run(candidate)
                    await candidate_app.kernel.wait(view["id"])
                    after = await candidate_app.repository.report(view["id"])
                    actual = after["items"][0]["output"]["process"]["stages"]
                    assert (
                        next(s for s in actual if s["phase"] == "agent")["facts"]["env"]["LANG"]
                        == "C"
                    )
                    assert (
                        after["environment"]["execution_conditions"]["common"][
                            "public_environment"
                        ]["LANG"]
                        == "C"
                    )
                    assert gate_reports(before, after)["status"] == "invalid"
                finally:
                    await candidate_app.close()
            return
        original_gate = adapter._phase_gate

        async def gate(phase):
            await original_gate(phase)
            if phase == "setup":
                entered.set()
                await release.wait()

        monkeypatch.setattr(adapter, "_phase_gate", gate)
        adapter._methods = adapter._identities()
        view = await app.run(request)
        await asyncio.wait_for(entered.wait(), 2)
        if case == "argv":
            monkeypatch.setattr(profile, "argv", replacement)
        elif case == "env":
            profile.environment["LANG"] = "C"
        elif case == "executable":
            profile.worker.write_text(profile.worker.read_text() + "\n# changed material\n")
        elif case == "auth_epoch":
            profile.subject["auth_lease_epoch"] += 1
        elif case == "owner_entry":
            monkeypatch.setattr(app.owner, "spawn", replacement)
        elif case == "codec_entry":
            monkeypatch.setattr(adapter.codec_type, "feed", replacement)
        else:
            monkeypatch.setattr(adapter.verifier, "argv", replacement)
        release.set()
        result = await app.kernel.wait(view["id"])
        assert result["status"] == "failed" and result["code"] == "target_binding_changed", result
        assert adapter.execute_calls == 0 and app.owner.spawn_calls == 0
        assert calls == 0
        assert result["environment"]["native_admission"]["reserved"] is True
    finally:
        release.set()
        await app.close()


@pytest.mark.parametrize(
    "case",
    [
        "same_store_key",
        "other_store",
        "parent_symlink",
        "leaf_symlink",
        "hardlink",
        "dotdot",
        "concurrent_instances",
        "concurrent_kernels",
        "restart_completed",
        "restart_unknown",
        "admission_return_lost",
        "reserved_before_intent",
        "replaced_database",
        "missing_database",
        "campaign_conflict",
    ],
)
async def test_native_slot_persistence_domain(tmp_path, monkeypatch, case):
    from krim_agent.evaluation.adapters.profiles import CampaignStoreBinding
    from krim_agent.evaluation.kernel import EvalKernel
    from krim_agent.evaluation.repository import Repository

    app, adapter, request, profile = await native_app(tmp_path)
    database = profile.binding.database
    pending = []
    try:
        if case in {"same_store_key", "restart_completed", "replaced_database", "missing_database"}:
            view = await app.run(request)
            snapshot = await app.kernel.wait(view["id"])
            assert snapshot["items"][0]["quality"] == "passed"
            assert adapter.execute_calls == 1
            same = await app.run(request)
            assert same["id"] == view["id"] and adapter.execute_calls == 1
            if case != "same_store_key":
                await app.close()
                if case in {"replaced_database", "missing_database"}:
                    saved = tmp_path / "original-readonly.sqlite"
                    database.rename(saved)
                    DATABASES.append(saved)
                    if case == "replaced_database":
                        foreign = await Repository.open(database)
                        await foreign.close()
                    with pytest.raises(ServiceError, match="target_binding_changed"):
                        await open_eval(database, native_profiles=[profile])
                    original = await open_eval(saved, readonly=True)
                    APPLICATIONS.append(original)
                    try:
                        found = await original.repository.by_key(request.model_dump(mode="json"))
                        assert found["id"] == view["id"]
                        assert (
                            found["items"][0]["execution_ref"]
                            == snapshot["items"][0]["execution_ref"]
                        )
                    finally:
                        await original.close()
                    if case == "missing_database":
                        assert not database.exists()
                else:
                    reopened = await open_eval(database, native_profiles=[profile])
                    APPLICATIONS.append(reopened)
                    try:
                        assert (await reopened.run(request))["id"] == view["id"]
                        assert reopened.repository.registry[profile.key].execute_calls == 0
                        assert reopened.repository.registry[profile.key].ref_calls == 0
                    finally:
                        await reopened.close()
        elif case == "other_store":
            other = tmp_path / "unapproved/eval.sqlite"
            with pytest.raises(ServiceError, match="target_binding_changed"):
                await open_eval(other, native_profiles=[profile])
            assert not other.exists() and not await asyncio.to_thread(
                Path(str(other) + ".lock").exists
            )
            foreign = await Repository.open(other)
            DATABASES.append(other)
            try:
                foreign.register(profile.key, adapter)
                await foreign.freeze(repair_dataset())
                with pytest.raises(ServiceError, match="target_binding_changed"):
                    await foreign.begin(request.model_dump(mode="json"))
                assert await foreign.by_key(request.model_dump(mode="json")) is None
                assert adapter.execute_calls == adapter.ref_calls == 0
            finally:
                await foreign.close()
        elif case in {"parent_symlink", "leaf_symlink", "hardlink", "dotdot"}:
            if case == "parent_symlink":
                alias = tmp_path / "alias"
                alias.symlink_to(database.parent, target_is_directory=True)
                path = alias / "eval.sqlite"
            elif case == "dotdot":
                path = database.parent / ".." / database.parent.name / "eval.sqlite"
            else:
                path = tmp_path / "alias.sqlite"
                if case == "leaf_symlink":
                    path.symlink_to(database)
                else:
                    os.link(database, path)
            try:
                with pytest.raises(ServiceError, match="target_binding_changed"):
                    await open_eval(path, native_profiles=[profile])
                assert adapter.execute_calls == adapter.ref_calls == 0
            finally:
                if case == "hardlink":
                    path.unlink()
        elif case == "concurrent_instances":
            with pytest.raises(ServiceError, match="database_busy"):
                await open_eval(database, native_profiles=[profile])
            assert adapter.execute_calls == adapter.ref_calls == 0
            await app.close()
            next_writer = await open_eval(database, native_profiles=[profile])
            APPLICATIONS.append(next_writer)
            await next_writer.close()
        elif case == "concurrent_kernels":
            one, two = EvalKernel(app.repository), EvalKernel(app.repository)
            original = app.repository.by_key
            barrier = asyncio.Event()
            calls = 0

            async def both_read_empty(value):
                nonlocal calls
                old = await original(value)
                if old is None and calls < 2:
                    calls += 1
                    if calls == 2:
                        barrier.set()
                    await barrier.wait()
                return old

            monkeypatch.setattr(app.repository, "by_key", both_read_empty)
            try:
                replies = await asyncio.wait_for(
                    asyncio.gather(
                        one.submit(request.model_dump(mode="json")),
                        two.submit(request.model_dump(mode="json")),
                    ),
                    2,
                )
                assert replies[0]["id"] == replies[1]["id"] and calls == 2
                await one.wait(replies[0]["id"])
                await two.wait(replies[0]["id"])
                assert len(one.tasks) + len(two.tasks) == 1
                assert adapter.ref_calls == adapter.execute_calls == 1
                (tmp_path / "two-kernels.json").write_text(
                    json.dumps(
                        {
                            "task_id": replies[0]["id"],
                            "one": {k: t.done() for k, t in one.tasks.items()},
                            "two": {k: t.done() for k, t in two.tasks.items()},
                            "execute_calls": adapter.execute_calls,
                        }
                    )
                )
            finally:
                await one.close()
                await two.close()
        elif case in {"admission_return_lost", "reserved_before_intent"}:
            original = app.repository._write
            lost = False

            async def lose_after_task_commit(operation):
                nonlocal lost
                value = await original(operation)
                if case == "admission_return_lost" and not lost and isinstance(value, tuple):
                    lost = True
                    raise OSError("SYNTHETIC_TASK_COMMIT_RETURN_LOST")
                return value

            monkeypatch.setattr(app.repository, "_write", lose_after_task_commit)
            snapshot, created = await app.repository.begin(request.model_dump(mode="json"))
            assert created and snapshot["environment"]["native_admission"]["reserved"] is True
            assert snapshot["items"][0]["status"] == "planned"
            assert (await app.run(request))["id"] == snapshot["id"]
            assert adapter.ref_calls == adapter.execute_calls == 0
            if case == "admission_return_lost":
                assert lost
            await app.close()
            reopened = await open_eval(database, native_profiles=[profile])
            APPLICATIONS.append(reopened)
            try:
                old = await reopened.run(request)
                assert old["id"] == snapshot["id"] and old["status"] == "interrupted"
                assert old["environment"]["native_admission"]["reserved"] is True
                assert reopened.repository.registry[profile.key].execute_calls == 0
            finally:
                await reopened.close()
        elif case == "restart_unknown":
            import signal

            await app.close()
            config = tmp_path / "owner-fixture.json"
            config.write_text(
                json.dumps(
                    {
                        "key": profile.key,
                        "family": profile.family,
                        "environment": profile.environment,
                        "common": profile.common,
                        "subject": profile.subject,
                        "admission": profile.binding.admission,
                        "request": request.model_dump(mode="json"),
                        "database": str(database),
                        "descriptor": profile.descriptor(),
                        "source_root": str(Path(os.environ["KRIM_PN2_ROOT"]) / "source"),
                    }
                )
            )
            worker = await asyncio.create_subprocess_exec(
                sys.executable,
                "-I",
                "-B",
                str(profile.worker),
                "--owner-intent",
                str(config),
                cwd=tmp_path,
                env=environment(),
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                start_new_session=True,
            )
            descriptor = os.pidfd_open(worker.pid)
            try:
                # 冷导入是独立owner前置；两个具名同步阶段各2秒，原Task/命令预算不变。
                imported = json.loads(await asyncio.wait_for(worker.stdout.readline(), 2))
                assert imported["owner_import_ready"] is True
                assert Path(imported["consumer"]).is_relative_to(
                    Path(os.environ["KRIM_PN2_ROOT"]) / "source/src"
                )
                worker.stdin.write(b"continue\n")
                await worker.stdin.drain()
                worker.stdin.close()
                await worker.stdin.wait_closed()
                ready = await asyncio.wait_for(worker.stdout.readline(), 2)
                value = json.loads(ready)
                assert value["execute_calls"] == 0
                assert Path(value["consumer"]).is_relative_to(
                    Path(os.environ["KRIM_PN2_ROOT"]) / "source/src"
                )
            finally:
                if worker.returncode is None:
                    signal.pidfd_send_signal(descriptor, signal.SIGKILL)
                stdout, stderr = await worker.communicate()
                os.close(descriptor)
                (tmp_path / "owner-process-finally.json").write_text(
                    json.dumps(
                        {
                            "pid": worker.pid,
                            "wait_returncode": worker.returncode,
                            "pidfd_closed": True,
                            "stdout": stdout.decode(errors="replace"),
                            "stderr": stderr.decode(errors="replace"),
                            "python_finally": "unknown_after_SIGKILL",
                        }
                    )
                )
            assert worker.returncode == -9 and not stderr, stderr
            (tmp_path / "owner-kill.json").write_text(
                json.dumps(
                    {
                        "pid": worker.pid,
                        "wait_returncode": worker.returncode,
                        "pidfd_closed": True,
                        "stdout_eof": True,
                        "stderr_eof": True,
                        "python_finally": "unknown_after_SIGKILL",
                        "before_kill": value,
                    }
                )
            )
            with pytest.raises(ServiceError, match="execution_unknown"):
                await open_eval(database, native_profiles=[profile])
            reader = await open_eval(database, readonly=True)
            APPLICATIONS.append(reader)
            try:
                found = await reader.repository.by_key(request.model_dump(mode="json"))
                assert found["id"] == value["id"]
                assert found["status"] == "interrupted" and found["items"][0]["status"] == "unknown"
                assert found["environment"]["native_admission"]["reserved"] is True
                assert not reader.repository.healthy and reader.adapter is None
                assert adapter.execute_calls == 0
            finally:
                await reader.close()
        else:
            import copy

            admission = copy.deepcopy(profile.binding.admission)
            admission["campaign_id"] = "another-campaign"
            admission["store_binding_digest"] = fingerprint(
                {
                    k: admission[k]
                    for k in [
                        "campaign_id",
                        "campaign_store_id",
                        "store_binding",
                        "scope",
                        "slot_policy_digest",
                    ]
                }
            )
            different = NativeProfile(
                profile.key,
                profile.family,
                profile.worker,
                profile.environment,
                profile.common,
                profile.subject,
                CampaignStoreBinding(admission),
            )
            await app.close()
            with pytest.raises(ServiceError, match="target_binding_changed"):
                await open_eval(database, native_profiles=[different])
            assert adapter.execute_calls == adapter.ref_calls == 0
    finally:
        await asyncio.gather(*pending, return_exceptions=True)
        await app.close()


@pytest.mark.parametrize(
    "case",
    [
        "wall",
        "request_quota",
        "tokens",
        "cost",
        "output",
        "capability",
        "dataset",
        "scope",
        "unapproved_key",
        "repetitions",
    ],
)
async def test_native_budget_limits_before_launch(tmp_path, case):
    from krim_agent.evaluation.adapters.profiles import require_m0, validate_quota

    app, adapter, request, profile = await native_app(tmp_path)
    try:
        if case in {"request_quota", "tokens", "cost"}:
            limits = {
                "model_requests": 12,
                "input_tokens": 12000,
                "output_tokens": 8000,
                "total_tokens": 20000,
                "usd": 1,
                "cli_launches": 1,
            }
            key = {"request_quota": "model_requests", "tokens": "total_tokens", "cost": "usd"}[case]
            limits[key] += 1
            with pytest.raises(ValueError, match="native_not_ready"):
                validate_quota(limits)
            with pytest.raises(ValueError, match="native_not_ready"):
                require_m0({"inference_authorized": True, "hard_quota": {"limits": limits}})
        elif case == "capability":
            with pytest.raises(ValueError, match="native_not_ready"):
                NativeProfile(
                    "codex-cli",
                    "codex",
                    profile.worker,
                    profile.environment,
                    profile.common,
                    profile.subject,
                    profile.binding,
                    fixture_only=False,
                    m0={"inference_authorized": True},
                )
        else:
            value = request.model_dump(mode="json")
            if case == "wall":
                value["execution"]["item_timeout_seconds"] = 181
            elif case == "output":
                value["execution"]["stdout_bytes"] = 262145
            elif case == "dataset":
                value["dataset"]["fingerprint"] = "0" * 64
            elif case == "scope":
                value["scope"] = "another"
            elif case == "unapproved_key":
                value["request_key"] = "unapproved-new-slot"
            else:
                value["execution"]["repetitions"] = 2
            with pytest.raises((ValueError, ServiceError)):
                await app.kernel.submit(value)
        assert adapter.ref_calls == adapter.execute_calls == 0
        assert app.owner.spawn_calls == 0
        assert await app.repository.by_key(request.model_dump(mode="json")) is None
    finally:
        await app.close()


@pytest.mark.parametrize("case", ["start", "return_lost", "cancel", "capacity"])
async def test_krim_neutral_uses_real_run_and_borrowed_close(tmp_path, monkeypatch, case):
    from krim_agent.config import Settings
    from krim_agent.contracts import RunRequest, parse_json
    from krim_agent.evaluation.adapters.krim import NeutralKrimAdapter
    from krim_agent.evaluation.ports import AdapterLease
    from krim_agent.evaluation.targets import make_eval_bindings
    from krim_agent.runtime.service import RunBinding, RunService
    from krim_agent.storage.sqlite import SQLiteStore

    run_database = tmp_path / "runs.sqlite"
    DATABASES.append(run_database)
    settings = Settings(database=run_database, max_active_runs=1)
    entered, release = asyncio.Event(), asyncio.Event()
    store = runs = neutral = app = None
    bindings = {}
    lost = False
    blocker = None
    try:
        store = await SQLiteStore.open(run_database)
        await store.recover()
        bindings = make_eval_bindings(store, settings)
        provider = bindings["target-v2"].loop.provider
        if case in {"cancel", "capacity"}:
            original_stream = provider.stream

            async def hold(messages, tools):
                entered.set()
                await release.wait()
                async for event in original_stream(messages, tools):
                    yield event

            monkeypatch.setattr(provider, "stream", hold)
            base = bindings["target-v2"]
            bindings["target-v2"] = RunBinding.freeze("target-v2", base.loop, base.settings)
        base = bindings["target-v2"]
        # 仅一个真实RunService；在其MappingProxy封存之前完成可信测试装配。
        runs = RunService(store, base.loop, settings, parse_json(base.descriptor_json), bindings)
        neutral = NeutralKrimAdapter(store, runs, root=tmp_path / "inputs")
        database = tmp_path / "eval.sqlite"
        DATABASES.append(database)
        app = await open_eval(
            database, adapters={"krim-script-v2": AdapterLease(neutral, owned=False)}
        )
        APPLICATIONS.append(app)
        dataset = DatasetSpec.model_validate(
            {
                "name": "krim",
                "version": "1",
                "cases": [
                    {
                        "case_id": "sum",
                        "instruction": "/add 2 3",
                        "files": {"INPUT.txt": "/add 2 3"},
                        "reference": {"expected": "5"},
                        "verifier": "krim-script/1",
                    }
                ],
            }
        )
        await app.repository.freeze(dataset)
        request = TaskSpec(
            dataset=dataset.ref(),
            request_key="krim-link",
            target_key="krim-script-v2",
            metrics=app.repository.metric_registry.specs("krim"),
        )
        if case == "return_lost":
            original_start = runs.start

            async def lose_start(value):
                nonlocal lost
                await original_start(value)
                lost = True
                raise OSError("SYNTHETIC_RUN_START_RETURN_LOST")

            monkeypatch.setattr(runs, "start", lose_start)
        if case == "capacity":
            blocker = await runs.start(
                RunRequest(text="/add 1 1", request_key="blocker", target="target-v2")
            )
            await asyncio.wait_for(entered.wait(), 2)
        view = await app.run(request)
        if case == "cancel":
            await asyncio.wait_for(entered.wait(), 2)
            result = await app.kernel.cancel(view["id"])
            assert result["status"] == "aborted"
        else:
            result = await app.kernel.wait(view["id"])
            if case == "capacity":
                assert result["status"] == "failed" and result["code"] == "capacity"
                assert result["items"][0]["execution_ref"] is None
                assert provider.calls == 0
            else:
                item = result["items"][0]
                assert (
                    item["quality"] == "passed" and item["output"]["target_result"]["text"] == "5"
                )
                assert item["output"]["process"] is None and item["output"]["exit_code"] is None
                assert len(item["output"]["target_result"]["tools"]) == 1
                assert item["output"]["target_result"]["tools"][0]["executor_started"] is True
                before_calls = provider.calls
                assert (await app.run(request))["id"] == view["id"]
                assert provider.calls == before_calls == 2
                assert neutral.execute_calls == 1
                if case == "return_lost":
                    assert lost
                linked = await store.by_key(f"pn2.krim.{view['id']}.sum.0")
                assert linked.id == item["execution_ref"]
    finally:
        if blocker is not None and runs is not None:
            await runs.cancel(blocker.id)
        release.set()
        if app is not None:
            await app.close()
            assert not store.closed and not runs.closed
        if neutral is not None:
            await neutral.close()
        if runs is not None:
            await runs.close()
        for provider in {id(b.loop.provider): b.loop.provider for b in bindings.values()}.values():
            await provider.close()
        if store is not None:
            await store.close()
    assert all(task.done() for task in runs.tasks.values())


@pytest.mark.parametrize("case", ["borrowed_adapter", "owned_application", "failed_init"])
async def test_shared_process_owner_closes_only_owned_leases(tmp_path, case):
    from krim_agent.evaluation.adapters.process import ProcessOwner
    from krim_agent.evaluation.repository import Repository

    if case == "borrowed_adapter":
        owner = ProcessOwner()
        apps = []
        try:
            source = json.loads(
                files("krim_agent.evaluation.examples")
                .joinpath("reconciliation.v1.json")
                .read_text()
            )
            source["cases"] = [source["cases"][0]]
            dataset = DatasetSpec.model_validate(source)
            for index in [0, 1]:
                database = tmp_path / f"borrowed-{index}.sqlite"
                DATABASES.append(database)
                app = await open_eval(database, process_owner=owner)
                apps.append(app)
                APPLICATIONS.append(app)
                actual = app.adapter.worker
                wrapper = tmp_path / f"reference-{index}.py"
                wrapper.write_text(
                    "import runpy,time,pathlib\n"
                    + f"try: runpy.run_path({str(actual)!r},run_name='__main__')\n"
                    + "except SystemExit as result:\n    if result.code not in (0,None): raise\n"
                    + "pathlib.Path('ready').write_text('actual process')\n"
                    + "while not pathlib.Path('release').exists(): time.sleep(.005)\n"
                )
                app.adapter.worker = wrapper
                app.adapter._descriptor["implementation_digest"] = file_digest(wrapper)
                await app.repository.freeze(dataset)
                request = TaskSpec(
                    dataset=dataset.ref(),
                    request_key="borrowed",
                    metrics=app.repository.metric_registry.specs(),
                )
                view = await app.run(request)
                await until(
                    lambda app=app: any(
                        (Path(j.prepared.directory) / "ready").exists()
                        for j in app.adapter.jobs.values()
                    )
                )
                app._fixture_task_id = view["id"]
            await apps[0].close()
            assert not owner.closed and not apps[1].adapter.closed
            assert all(j.process.returncode is None for j in apps[1].adapter.jobs.values())
            for job in apps[1].adapter.jobs.values():
                (Path(job.prepared.directory) / "release").write_text("continue")
            result = await apps[1].kernel.wait(apps[1]._fixture_task_id)
            assert result["items"][0]["quality"] == "passed"
        finally:
            for app in apps:
                await app.close()
            closed = await owner.close()
            assert closed.closed
        return
    app, adapter, request, profile = await native_app(tmp_path)
    try:
        if case == "owned_application":
            view = await app.run(request)
            await app.kernel.wait(view["id"])
            await app.close()
            await app.close()
            assert adapter.closed and app.owner.closed and app.repository.closed
            assert adapter.execute_calls == 1
        else:
            await app.close()
            with pytest.raises(ServiceError, match="invalid_input"):
                await open_eval(profile.binding.database, native_profiles=[profile, profile])
            writer = await Repository.open(profile.binding.database)
            try:
                assert writer.healthy
            finally:
                await writer.close()
            assert adapter.execute_calls == 0
    finally:
        await app.close()


REAL_SLOT_DRIVER = (
    "\n"
    "import asyncio,json,os,sys\n"
    "from pathlib import Path\n"
    "from importlib.resources import files\n"
    "from krim_agent.evaluation.app import open_eval\n"
    "from krim_agent.evaluation.adapters.profiles import load_profile\n"
    "from krim_agent.evaluation.domain import Budget,DatasetSpec,TaskSpec\n"
    "async def main():\n"
    '    root=Path(os.environ["KRIM_PN2_ROOT"])\n'
    '    authority=json.loads((root/"release/PROFILE-READY.json").read_te'
    "xt())\n"
    "    target,kind=sys.argv[1:3]\n"
    '    profile=load_profile(Path(authority["profiles"][target]["path"]),tar'
    "get)\n"
    "    app=await open_eval(profile.binding.database,native_profiles=[profil"
    "e])\n"
    "    try:\n"
    '        dataset=DatasetSpec.model_validate_json(files("krim_agent.evalua'
    'tion.examples").joinpath("native_repair.v1.json").read_bytes())\n'
    "        await app.repository.freeze(dataset)\n"
    '        request=TaskSpec(dataset=dataset.ref(),request_key="pn2-01-"+tar'
    'get+"-"+kind,\n'
    "            target_key=target,metrics=app.repository.metric_registry.spe"
    'cs("native"),\n'
    "            execution=Budget(item_timeout_seconds=180,batch_timeout_seco"
    "nds=240))\n"
    "        view=await app.run(request)\n"
    "        adapter=app.repository.registry[target]\n"
    '        if kind=="cancel":\n'
    "            while not any(j.codec.tools for j in adapter.jobs.values()):"
    "\n"
    '                task=app.kernel.tasks.get(view["id"])\n'
    "                if task is not None and task.done():\n"
    '                    raise AssertionError("target ended before completed-'
    'tool cancel checkpoint")\n'
    "                await asyncio.sleep(.005)\n"
    '            result=await app.kernel.cancel(view["id"])\n'
    "        else:\n"
    '            result=await app.kernel.wait(view["id"])\n'
    '        item=result["items"][0]\n'
    '        report=await app.repository.report(view["id"])\n'
    "        again=await app.run(request)\n"
    '        assert again["id"]==view["id"] and adapter.execute_calls==1\n'
    '        payload={"id":view["id"],"database":str(profile.binding.database'
    "),\n"
    '            "execution_ref":item["execution_ref"],"report_hash":report["'
    'report_hash"],\n'
    '            "status":result["status"],"quality":item["quality"],\n'
    '            "output":item["output"],"target_executions":adapter.execute_'
    "calls,\n"
    '            "module":sys.modules["krim_agent.evaluation.app"].__file__,\n'
    '            "connections":app.repository.resources}\n'
    "    finally:\n"
    "        await app.close()\n"
    '    payload["resources"]=[{"pid":j.process.pid,"wait":j.process.returnco'
    "de,\n"
    '        "pidfd_closed":j.pidfd==-1,"collector_joined":j.collector.done()'
    "}\n"
    "        for j in app.owner.jobs.values()]\n"
    '    payload["repository_closed"]=app.repository.closed\n'
    "    print(json.dumps(payload))\n"
    "asyncio.run(main())\n"
)


async def real_slot(tmp_path, target, kind):
    import subprocess

    # 本轮固定harness不会执行此函数；缺M0原件显式失败，不skip、不借fixture替代。
    root = Path(os.environ["KRIM_PN2_ROOT"])
    authority = root / "release/PROFILE-READY.json"
    if not authority.is_file():
        raise ValueError("native_not_ready")
    result = await asyncio.to_thread(
        subprocess.run,
        [str(root / "installed/bin/python"), "-I", "-B", "-c", REAL_SLOT_DRIVER, target, kind],
        cwd=root / "outside",
        env=dict(os.environ),
        capture_output=True,
        text=True,
        timeout=240,
    )
    (tmp_path / "installed-real-command.json").write_text(
        json.dumps(
            {
                "argv": [
                    str(root / "installed/bin/python"),
                    "-I",
                    "-B",
                    "-c",
                    REAL_SLOT_DRIVER,
                    target,
                    kind,
                ],
                "cwd": str(root / "outside"),
                "exit": result.returncode,
                "stdout": result.stdout,
                "stderr": result.stderr,
            }
        )
    )
    assert result.returncode == 0, result.stderr
    value = json.loads(result.stdout)
    assert Path(value["module"]).is_relative_to(root / "installed/lib/python3.12/site-packages")
    assert value["repository_closed"] and value["target_executions"] == 1
    assert all(
        r["wait"] is not None and r["pidfd_closed"] and r["collector_joined"]
        for r in value["resources"]
    )
    (tmp_path / "real-trial.json").write_text(json.dumps(value, indent=2))
    return value


@pytest.mark.parametrize("target", ["codex-cli", "claude-code"])
async def test_real_native_repository_repair(tmp_path, target):
    value = await real_slot(tmp_path, target, "normal")
    assert value["status"] == "completed" and value["quality"] == "passed"
    assert value["output"]["native"]["complete"] is True
    assert value["output"]["process"]["fixture_only"] is False
    assert value["output"]["verification"]["passed"] is True
    assert value["output"]["artifacts"]["outputs/ledger.json"]["sha256"]


@pytest.mark.parametrize("target", ["codex-cli", "claude-code"])
async def test_real_native_cancel_after_completed_tool(tmp_path, target):
    value = await real_slot(tmp_path, target, "cancel")
    assert value["status"] == "aborted"
    assert value["output"]["cleanup"] == "closed"
    assert value["output"]["native"]["tools"]
    assert value["output"]["process"]["fixture_only"] is False


async def test_pn2_repair_snapshot_receipt(tmp_path):
    dataset = DatasetSpec.model_validate_json(
        files("krim_agent.evaluation.examples").joinpath("reconciliation.v1.json").read_bytes()
    )
    database = tmp_path / "snapshot.sqlite"
    DATABASES.append(database)
    app = await open_eval(database)
    APPLICATIONS.append(app)
    try:
        await app.repository.freeze(dataset)
        request = TaskSpec(
            dataset=dataset.ref(),
            request_key="snapshot-receipt",
            metrics=app.repository.metric_registry.specs(),
        )
        view = await app.run(request)
        await app.kernel.wait(view["id"])
        job = next(iter(app.adapter.jobs.values()))
        original = copy.deepcopy(job.collector.result())
        digest = fingerprint(original)
        first = await app.adapter.cancel(job.ref)
        second = await app.adapter.cancel(job.ref)
        assert first.observation == second.observation == original
        assert fingerprint(job.collector.result()) == digest
        receipts = job.cleanup_receipts
        assert receipts and all(r["closed"] for r in receipts)
        assert all(r["wait_returncode"] is not None and r["collector_joined"] for r in receipts)
        assert all(
            r["stdout_eof"] and r["stderr_eof"] and r["pidfd_closed"] and r["group_empty"]
            for r in receipts
        )
        await app.close()
        await app.close()
        assert fingerprint(job.collector.result()) == digest
        (tmp_path / "cleanup-receipts.json").write_text(json.dumps(receipts, indent=2))
    finally:
        await app.close()


async def test_pn2_repair_cleanup_budget(tmp_path, monkeypatch):
    import time

    dataset = DatasetSpec.model_validate_json(
        files("krim_agent.evaluation.examples").joinpath("reconciliation.v1.json").read_bytes()
    )
    database = tmp_path / "budget.sqlite"
    DATABASES.append(database)
    app = await open_eval(database)
    APPLICATIONS.append(app)
    release = asyncio.Event()
    original = app.adapter._collect

    async def held(work):
        await release.wait()
        return await original(work)

    monkeypatch.setattr(app.adapter, "_collect", held)
    pending = None
    timely = False
    try:
        await app.repository.freeze(dataset)
        request = TaskSpec(
            dataset=dataset.ref(),
            request_key="cleanup-budget",
            metrics=app.repository.metric_registry.specs(),
        )
        view, created = await app.repository.begin(request.model_dump(mode="json"))
        assert created
        trial = app.repository.plan(view).trials[0]
        prepared = await app.adapter.prepare(trial)
        await app.repository.intent(trial)
        ref = await app.adapter.execute(prepared)
        await app.repository.attach(trial, ref)
        job = app.adapter.jobs[ref.id]
        started = time.monotonic()
        pending = asyncio.create_task(app.adapter.cancel(ref))
        done, _ = await asyncio.wait({pending}, timeout=30.3)
        elapsed = time.monotonic() - started
        timely = pending in done
        if timely:
            result = pending.result()
            assert not result.closed and result.reason == "cleanup_unknown"
            assert job.collector is not None and not job.collector.done()
            assert job.pidfd >= 0 and app.owner.jobs[job.ref.id] is job
            assert job.cleanup_receipts[-1]["collector_joined"] is False
        (tmp_path / "cleanup-budget.json").write_text(
            json.dumps(
                {
                    "seconds": elapsed,
                    "bounded_before_release": timely,
                    "pid": job.process.pid,
                    "collector_done": job.collector.done(),
                }
            )
        )
    finally:
        release.set()
        if pending is not None:
            await asyncio.gather(pending, return_exceptions=True)
        await app.close()
    assert timely, "30-second cleanup budget was bypassed by collector wait"
    assert job.collector.done() and job.process.returncode is not None and job.pidfd == -1
    assert app.owner.closed and app.adapter.closed
    assert job.cleanup_receipts[-1]["closed"]
    (tmp_path / "cleanup-budget-receipts.json").write_text(
        json.dumps(job.cleanup_receipts, indent=2)
    )


async def test_pn2_repair_collect_unknown_blocks(tmp_path, monkeypatch):
    app, adapter, request, profile = await native_app(
        tmp_path, extra_keys=("blocked-after-unknown",)
    )
    release = asyncio.Event()
    original = adapter._settle

    async def conservative(job):
        closed = await original(job)
        return closed and release.is_set()

    monkeypatch.setattr(adapter, "_settle", conservative)
    try:
        view = await app.run(request)
        result = await app.kernel.wait(view["id"])
        assert result["status"] == "failed" and result["code"] == "execution_unknown"
        assert app.kernel.cleanup_unknown
        assert adapter.execute_calls == 1
        with pytest.raises(ServiceError, match="storage_unavailable"):
            await app.run(request.model_copy(update={"request_key": "blocked-after-unknown"}))
        assert adapter.execute_calls == 1
        assert all(j.resources for j in adapter.jobs.values())
        assert result["items"][0]["output"]["cleanup"] == "unknown"
    finally:
        release.set()
        await app.close()
    assert adapter.closed and app.owner.closed


@pytest.mark.parametrize(
    "case",
    [
        "verifier-nonzero",
        "verifier-missing",
        "artifact-oversize",
        "artifact-fifo",
        "tests-undiscoverable",
        "tests-empty",
        "tests-fail",
        "command-unrelated",
        "normal",
    ],
)
async def test_pn2_repair_verifier_hard_gates(tmp_path, monkeypatch, case):
    app, adapter, request, profile = await native_app(
        tmp_path, mode=case if not case.startswith("verifier-") else "normal"
    )
    if case.startswith("verifier-"):
        script = tmp_path / "controlled-verifier.py"
        checks = {
            k: True
            for k in [
                "inputs_unchanged",
                "gold_0",
                "gold_1",
                "gold_2",
                "reject_0",
                "reject_1",
                "reject_2",
                "source_changed",
                "tests_changed",
                "effective_tests",
                "ledger",
                "summary",
            ]
        }
        value = {
            "schema_version": "native-verifier/1",
            "phase": "final",
            "passed": True,
            "checks": checks,
            "environment_error": False,
            "baseline_checks_complete": True,
            "test_run": {
                "discovered": 2,
                "executed": 2,
                "failures": 0,
                "errors": 0,
                "skipped": 0,
                "asserting_tests": 2,
            },
        }
        if case == "verifier-missing":
            value = {"passed": True}
        script.write_text(
            "import json\nprint("
            + repr(json.dumps(value))
            + ")\nraise SystemExit("
            + str(7 if case == "verifier-nonzero" else 0)
            + ")\n"
        )
        original = adapter.verifier.argv

        def controlled(profile, directory, initial_file, *, final):
            if final:
                return [sys.executable, "-I", "-B", str(script), "--phase", "final"]
            return original(profile, directory, initial_file, final=final)

        monkeypatch.setattr(adapter.verifier, "argv", controlled)
        adapter._verifier_methods = (
            controlled,
            getattr(adapter.verifier.result, "__func__", adapter.verifier.result),
        )
    try:
        view = await app.run(request)
        result = await app.kernel.wait(view["id"])
        item = result["items"][0]
        assert adapter.execute_calls == 1
        if case == "normal":
            assert item["quality"] == "passed", item
            assert item["output"]["verification"]["test_run"]["executed"] >= 2
            assert item["output"]["verification"]["test_run"]["asserting_tests"] >= 2
        else:
            assert item["quality"] != "passed", item
            assert any(s["value"] != 1 for s in item["scores"])
        if case == "verifier-nonzero":
            stages = item["output"]["process"]["stages"]
            assert (
                next(s for s in stages if s["phase"] == "verifier")["facts"]["wait_returncode"] == 7
            )
        if case.startswith("artifact-"):
            assert item["output"]["process"]["artifact_error"]
            assert item["output"]["native"]["tools"]
        if case == "command-unrelated":
            job = next(iter(adapter.jobs.values()))
            actual = json.loads(
                (Path(job.prepared.directory) / ".fixture-command.json").read_text()
            )
            assert actual["argv"] == ["/usr/bin/echo", "unrelated"]
            assert actual["returncode"] == 0
            assert item["output"]["verification"]["test_run"]["executed"] == 2
        assert (await app.run(request))["id"] == view["id"] and adapter.execute_calls == 1
    finally:
        await app.close()


async def test_pn2_repair_krim_snapshot_failure(tmp_path, monkeypatch):
    from krim_agent.config import Settings
    from krim_agent.contracts import RunRequest, parse_json
    from krim_agent.evaluation.adapters.krim import NeutralKrimAdapter
    from krim_agent.evaluation.ports import AdapterLease
    from krim_agent.evaluation.targets import make_eval_bindings
    from krim_agent.runtime.service import RunBinding, RunService
    from krim_agent.storage.sqlite import SQLiteStore

    database = tmp_path / "runs.sqlite"
    DATABASES.append(database)
    settings = Settings(database=database, max_active_runs=1)
    store = await SQLiteStore.open(database)
    await store.recover()
    bindings = make_eval_bindings(store, settings)
    provider = bindings["target-v2"].loop.provider
    stream = provider.stream
    entered, release = asyncio.Event(), asyncio.Event()

    async def held(messages, tools):
        entered.set()
        await release.wait()
        async for event in stream(messages, tools):
            yield event

    monkeypatch.setattr(provider, "stream", held)
    base = bindings["target-v2"]
    bindings["target-v2"] = RunBinding.freeze("target-v2", base.loop, base.settings)
    base = bindings["target-v2"]
    runs = RunService(store, base.loop, settings, parse_json(base.descriptor_json), bindings)
    neutral = NeutralKrimAdapter(store, runs, root=tmp_path / "inputs")
    app = await open_eval(
        tmp_path / "eval.sqlite", adapters={"krim-script-v2": AdapterLease(neutral, owned=False)}
    )
    APPLICATIONS.append(app)
    DATABASES.append(tmp_path / "eval.sqlite")
    original_snapshot = store.snapshot
    failed = False

    async def snapshot_failure(ident):
        nonlocal failed
        if not failed and ident in runs.tasks:
            await asyncio.wait_for(entered.wait(), 2)
            failed = True
            raise OSError("CONTROLLED_POST_START_SNAPSHOT_FAILURE")
        return await original_snapshot(ident)

    original_start = runs.start

    async def start_then_fault(value):
        run = await original_start(value)
        await asyncio.wait_for(entered.wait(), 2)
        monkeypatch.setattr(store, "snapshot", snapshot_failure)
        return run

    monkeypatch.setattr(runs, "start", start_then_fault)
    try:
        dataset = DatasetSpec.model_validate(
            {
                "name": "repair-krim",
                "version": "1",
                "cases": [
                    {
                        "case_id": "sum",
                        "instruction": "/add 2 3",
                        "files": {"INPUT.txt": "/add 2 3"},
                        "reference": {"expected": "5"},
                        "verifier": "krim-script/1",
                    }
                ],
            }
        )
        await app.repository.freeze(dataset)
        request = TaskSpec(
            dataset=dataset.ref(),
            request_key="snapshot-failure",
            target_key="krim-script-v2",
            metrics=app.repository.metric_registry.specs("krim"),
        )
        view = await app.run(request)
        result = await app.kernel.wait(view["id"])
        assert failed and result["status"] == "failed"
        owned = await store.by_key(f"pn2.krim.{view['id']}.sum.0")
        assert runs.tasks[owned.id].done(), "owned Run must join before borrowed service close"
        assert not runs.closed and not store.closed
        assert not any(not t.done() for t in runs.tasks.values())
        monkeypatch.setattr(runs, "start", original_start)
        other = await runs.start(
            RunRequest(text="/add 1 1", request_key="other-run", target="target-v1")
        )
        await runs.wait(other.id)
        assert (await store.snapshot(other.id)).run.status == "completed"
        assert (await app.run(request))["id"] == view["id"] and neutral.execute_calls == 1
        (tmp_path / "pre-borrowed-close.json").write_text(
            json.dumps(
                {
                    "owned": owned.id,
                    "joined": runs.tasks[owned.id].done(),
                    "other": other.id,
                    "other_status": (await store.snapshot(other.id)).run.status,
                    "runs_closed": runs.closed,
                    "provider_calls": provider.calls,
                }
            )
        )
    finally:
        release.set()
        await app.close()
        await neutral.close()
        await runs.close()
        for item in {id(b.loop.provider): b.loop.provider for b in bindings.values()}.values():
            await item.close()
        await store.close()


async def test_pn2_repair_vacuous_tests_are_rejected(tmp_path):
    app, adapter, request, profile = await native_app(tmp_path, mode="tests-vacuous")
    try:
        view = await app.run(request)
        result = await app.kernel.wait(view["id"])
        item = result["items"][0]
        assert adapter.execute_calls == 1
        assert item["quality"] != "passed", item
        assert item["output"]["verification"]["checks"]["effective_tests"] is False
        assert (await app.run(request))["id"] == view["id"] and adapter.execute_calls == 1
    finally:
        await app.close()


@pytest.mark.parametrize("backend", ["reference", "native"])
async def test_pn2_reentry_runner_cleanup_budget(tmp_path, monkeypatch, backend):
    from krim_agent.evaluation.adapters.process import ProcessOwner
    from krim_agent.evaluation.repository import Repository
    from krim_agent.evaluation.runner import ItemWork

    release = asyncio.Event()
    entered = asyncio.Event()
    cleanup_started = asyncio.Event()
    loop = asyncio.get_running_loop()
    held = []
    attempts = []
    runner_calls = []
    observed_reads = set()
    original_observe = ProcessOwner.observe
    original_cancel = ProcessOwner._cancel
    original_cleanup = ItemWork.cleanup
    app = adapter = waiting = None
    bounded = False
    before_close = None
    close_evidence = {}

    async def observe(owner, work, **kwargs):
        is_target = backend == "reference" or "--family" in work.argv
        if is_target and not held:
            held.append(work)
            for name in ["stdout", "stderr"]:
                stream = getattr(work.process, name)
                read = stream.read

                async def gated(size=-1, *, name=name, read=read):
                    observed_reads.add(name)
                    if observed_reads == {"stdout", "stderr"}:
                        entered.set()
                    await release.wait()
                    return await read(size)

                monkeypatch.setattr(stream, "read", gated)
        return await original_observe(owner, work, **kwargs)

    async def measured_cancel(owner, work):
        if held and work is held[0]:
            attempts.append(
                {
                    "time": loop.time(),
                    "task_identity": id(asyncio.current_task()),
                    "pid": work.process.pid,
                }
            )
            cleanup_started.set()
        return await original_cancel(owner, work)

    async def measured_cleanup(work):
        if work.adapter is adapter:
            runner_calls.append(
                {
                    "time": loop.time(),
                    "admission_done": work.admission is not None and work.admission.done(),
                    "scorer": None if work.scorer is None else work.scorer.done(),
                    "ref": None if work.ref is None else work.ref.id,
                }
            )
        return await original_cleanup(work)

    monkeypatch.setattr(ProcessOwner, "observe", observe)
    monkeypatch.setattr(ProcessOwner, "_cancel", measured_cancel)
    monkeypatch.setattr(ItemWork, "cleanup", measured_cleanup)
    try:
        if backend == "native":
            app, adapter, request, profile = await native_app(
                tmp_path, extra_keys=("must-not-dispatch",)
            )
            database = profile.binding.database
        else:
            database = tmp_path / "runner-reference.sqlite"
            DATABASES.append(database)
            app = await open_eval(database)
            APPLICATIONS.append(app)
            adapter = app.adapter
            dataset = DatasetSpec.model_validate_json(
                files("krim_agent.evaluation.examples")
                .joinpath("reconciliation.v1.json")
                .read_bytes()
            )
            await app.repository.freeze(dataset)
            request = TaskSpec(
                dataset=dataset.ref(),
                request_key="full-runner",
                metrics=app.repository.metric_registry.specs(),
            )
        assert request.execution.item_timeout_seconds == 20
        view = await app.run(request)
        waiting = asyncio.create_task(app.kernel.wait(view["id"]))
        await asyncio.wait_for(entered.wait(), 5)
        work = held[0]
        assert work.pidfd >= 0 and work.collector is not None
        assert len(work.stream_tasks) == 3
        assert observed_reads == {"stdout", "stderr"}
        await asyncio.wait_for(cleanup_started.wait(), 25)
        done, _ = await asyncio.wait({waiting}, timeout=31)
        bounded = waiting in done
        result = waiting.result() if bounded else await app.repository.snapshot(view["id"])
        before_close = {
            "backend": backend,
            "task_id": view["id"],
            "database": str(database),
            "bounded_before_release": bounded,
            "seconds_from_first_cleanup": loop.time() - attempts[0]["time"],
            "result": result,
            "runner_calls": copy.deepcopy(runner_calls),
            "attempts": copy.deepcopy(attempts),
            "cleanup_receipts": copy.deepcopy(work.cleanup_receipts),
            "collector_done": work.collector.done(),
            "stream_tasks_done": [t.done() for t in work.stream_tasks],
            "pidfd_retained": work.pidfd >= 0,
            "same_owned_object": app.owner.jobs.get(work.ref.id) is work,
            "execute_calls": adapter.execute_calls,
        }
        (tmp_path / "runner-before-release.json").write_text(json.dumps(before_close, indent=2))
        if bounded:
            assert result["status"] == "failed" and result["code"] == "execution_unknown"
            assert app.kernel.cleanup_unknown
            assert len(attempts) == 1 and len(runner_calls) == 2
            assert not work.collector.done() and work.pidfd >= 0
            assert not work.stdout_eof and not work.stderr_eof
            assert not all(t.done() for t in work.stream_tasks)
            assert app.owner.jobs[work.ref.id] is work
            assert work.cleanup_receipts[-1]["reason"] == "cleanup_unknown"
            first_task = work.cancel_task
            unknown = await app.owner.cancel(work, work.lease)
            assert not unknown.closed and work.cancel_task is first_task
            assert len(attempts) == 1
            with pytest.raises(ServiceError, match="permission_denied"):
                await app.owner.cancel(work, object())
            assert len(attempts) == 1
            with pytest.raises(ServiceError, match="storage_unavailable"):
                await app.run(request)
            with pytest.raises(ServiceError, match="storage_unavailable"):
                await app.run(request.model_copy(update={"request_key": "must-not-dispatch"}))
            found = await app.repository.by_key(request.model_dump(mode="json"))
            assert found["id"] == view["id"]
            with pytest.raises(ServiceError, match="database_busy"):
                await Repository.open(database)
    finally:
        release.set()
        if waiting is not None:
            await asyncio.gather(waiting, return_exceptions=True)
        if held:
            work = held[0]
            await asyncio.wait_for(asyncio.shield(work.collector), 5)
            original = copy.deepcopy(work.collector.result())
            original_hash = fingerprint(original)
            published = None
            if backend == "native":
                published = copy.deepcopy(next(iter(adapter.jobs.values())).observation)
            published_hash = fingerprint(published)
            calls = adapter.execute_calls
            task_before = await app.repository.snapshot(view["id"])
            await app.close()
            await app.close()
            assert work.process.returncode is not None and work.pidfd == -1
            assert work.collector.done() and all(t.done() for t in work.stream_tasks)
            assert work.stdout_eof and work.stderr_eof
            assert work.cleanup_receipts[-1]["closed"]
            assert work.cleanup_receipts[-1]["group_empty"]
            assert fingerprint(work.collector.result()) == original_hash
            if backend == "native":
                assert fingerprint(next(iter(adapter.jobs.values())).observation) == published_hash
            assert adapter.execute_calls == calls
            repeated = await app.owner.cancel(work, work.lease)
            assert repeated.closed and repeated.observation == original
            reader = await open_eval(database, readonly=True)
            APPLICATIONS.append(reader)
            try:
                cold = await reader.repository.by_key(request.model_dump(mode="json"))
                assert cold["id"] == view["id"] and cold == task_before
            finally:
                await reader.close()
            writer = await Repository.open(database)
            await writer.close()
            close_evidence = {
                "backend": backend,
                "task_id": view["id"],
                "same_pid": work.process.pid,
                "wait": work.process.returncode,
                "collector_done": work.collector.done(),
                "stream_tasks_done": [t.done() for t in work.stream_tasks],
                "stdout_eof": work.stdout_eof,
                "stderr_eof": work.stderr_eof,
                "pidfd_closed": work.pidfd == -1,
                "cleanup_receipts": work.cleanup_receipts,
                "original_observation_hash": original_hash,
                "published_observation_hash": published_hash,
                "execute_calls_before_close": calls,
                "execute_calls_after_close": adapter.execute_calls,
                "second_writer_closed": writer.closed,
                "second_writer_connections": writer.resources,
                "runner_calls": runner_calls,
                "attempts": attempts,
            }
            (tmp_path / "runner-explicit-close.json").write_text(
                json.dumps(close_evidence, indent=2)
            )
        elif app is not None:
            await app.close()
    assert bounded, "runner automatically renewed cleanup after the first 30-second budget"
    assert before_close["seconds_from_first_cleanup"] < 31
    assert before_close["execute_calls"] == close_evidence["execute_calls_after_close"] == 1
