"""111项同一新包的实际安装、300矩阵、冷评分与取消收束消费者。"""

import base64
import csv
import hashlib
import io
import json
import os
import subprocess
import tomllib
import zipfile
from pathlib import Path

import pytest
from test_continuous_eval import continuous_resources as continuous_resources


async def test_scale_wheel_identity(tmp_path):
    import asyncio

    from test_eval_scale import _run_scale_evidence_process

    def verify_package():
        root = Path(os.environ["KRIM_PN3_ROOT"])
        source = Path(__file__).resolve().parents[2]
        shipped = tomllib.loads((source / "pyproject.toml").read_text())["tool"]["hatch"]["build"][
            "targets"
        ]["wheel"]["only-include"]
        expected = json.loads(
            Path(
                "/artifacts/krim-agent-core-20261019/vm-pn3-scale-implement"
                "ation-20261023T152826061485/package-files.json"
            ).read_text()
        )
        assert shipped == expected and len(shipped) == len(set(shipped)) == 111
        wheel = root / "dist/krim_agent-0.4.0a1-py3-none-any.whl"
        site = root / "installed/lib/python3.12/site-packages"
        with zipfile.ZipFile(wheel) as bundle:
            assert {n for n in bundle.namelist() if n.startswith("krim_agent/")} == {
                n.removeprefix("src/") for n in shipped
            }
            records = {
                r[0]: r[1:]
                for r in csv.reader(
                    io.StringIO(bundle.read("krim_agent-0.4.0a1.dist-info/RECORD").decode())
                )
            }
            installed_records = {
                r[0]: r[1:]
                for r in csv.reader(
                    io.StringIO((site / "krim_agent-0.4.0a1.dist-info/RECORD").read_text())
                )
            }
            for name in shipped:
                relative = name.removeprefix("src/")
                data = (source / name).read_bytes()
                assert data == bundle.read(relative) == (site / relative).read_bytes()
                digest = (
                    base64.urlsafe_b64encode(hashlib.sha256(data).digest()).decode().rstrip("=")
                )
                assert (
                    records[relative]
                    == installed_records[relative]
                    == ["sha256=" + digest, str(len(data))]
                )
            assert records["krim_agent-0.4.0a1.dist-info/RECORD"] == ["", ""]
            entries = bundle.read("krim_agent-0.4.0a1.dist-info/entry_points.txt").decode()
            assert "krim-eval = krim_agent.evaluation.cli:main" in entries
            assert (
                "Version: 0.4.0a1" in bundle.read("krim_agent-0.4.0a1.dist-info/METADATA").decode()
            )
        return root, source, site, wheel

    root, source, site, wheel = await asyncio.to_thread(verify_package)
    code = (
        r"""import hashlib,json,os,stat,sys,time
from pathlib import Path
root=Path(sys.argv[1])
def _write_scale_checkpoint(stage, status, *, _defer_cleanup_error=False, **known):
    active = sys.exception()
    try:
        identity = getattr(_write_scale_checkpoint, 'identity', None)
        if identity is None:
            path = root / 'header12-process.json'
            st = path.lstat()
            if not stat.S_ISREG(st.st_mode) or st.st_nlink != 1:
                raise RuntimeError('checkpoint_identity_not_regular')
            process = json.loads(path.read_text())
"""
        "            identity = {key: process[key] for key in ['run_ide"
        "ntity', 'driver_sha256', 'helper_ast_sha256', 'checkpoint_ast_"
        "sha256']}"
        r"""
            _write_scale_checkpoint.identity = identity
        sequence = getattr(_write_scale_checkpoint, 'sequence', 0) + 1
        if sequence > 128:
            raise RuntimeError('checkpoint_record_limit')
        fields = {}
        for key, value in known.items():
"""
        "            if not isinstance(key, str) or len(key) > 64 or (v"
        "alue is not None and type(value) not in (str, int, float, bool"
        ")):"
        r"""
                raise RuntimeError('checkpoint_non_scalar')
            fields[key] = value[:512] if isinstance(value, str) else value
"""
        "        row = dict(identity, sequence=sequence, stage=stage, s"
        "tatus=status, monotonic_ns=time.monotonic_ns(), time_ns=time.t"
        "ime_ns(), pid=os.getpid(), uid=os.getuid(), known=fields)"
        r"""
        raw = (json.dumps(row, ensure_ascii=False, allow_nan=False) + '\n').encode()
        if len(raw) > 4096:
            raise RuntimeError('checkpoint_record_bytes')
"""
        "        fd = os.open(root / 'header12-checkpoints.jsonl', os.O"
        "_WRONLY | os.O_CREAT | os.O_APPEND | os.O_NOFOLLOW, 384)"
        r"""
        try:
            st = os.fstat(fd)
            if not stat.S_ISREG(st.st_mode) or st.st_nlink != 1 or st.st_uid != os.getuid():
                raise RuntimeError('checkpoint_output_not_regular')
            if st.st_size + len(raw) > 128 * 4096:
                raise RuntimeError('checkpoint_total_bytes')
            view = memoryview(raw)
            while view:
                count = os.write(fd, view)
                if count <= 0:
                    raise RuntimeError('checkpoint_short_write')
                view = view[count:]
            os.fsync(fd)
        finally:
            os.close(fd)
        _write_scale_checkpoint.sequence = sequence
    except BaseException as evidence_error:
        errors = getattr(_write_scale_checkpoint, 'evidence_errors', None)
        if errors is None:
            errors = []
            _write_scale_checkpoint.evidence_errors = errors
        if len(errors) < 128:
"""
        "            errors.append(dict(stage=stage, status=status, typ"
        "e=type(evidence_error).__name__, message=str(evidence_error)[:"
        "512], pid=os.getpid(), active_type=None if active is None else"
        " type(active).__name__, deferred=_defer_cleanup_error, monoton"
        "ic_ns=time.monotonic_ns()))"
        r"""
        else:
"""
        "            _write_scale_checkpoint.error_overflow = getattr(_"
        "write_scale_checkpoint, 'error_overflow', 0) + 1"
        r"""
        if _defer_cleanup_error:
            if isinstance(globals().get('facts'), dict):
                facts['checkpoint_errors'] = list(errors)
            return
        if active is not None:
            raise active from evidence_error
        raise
_write_scale_checkpoint('bootstrap','start')
from importlib.metadata import version
import krim_agent.evaluation.scale_store as m
actual=dict(module=m.__file__,path=sys.path,version=version('krim-agent'))
(root/'header12-consumer.json').write_text(json.dumps(actual))
print(json.dumps(actual))
_write_scale_checkpoint('consumer','done')
"""
    )
    compile(code, "installed-scale-identity", "exec")
    command = [str(root / "installed/bin/python"), "-I", "-B", "-c", code, str(tmp_path)]
    primary = None
    returned = None
    try:
        returned = await _run_scale_evidence_process(
            command, tmp_path, execution_timeout_seconds=10, cwd=root / "outside"
        )
    except BaseException as error:
        primary = error
        raise
    finally:
        secondary = []

        def regular_bytes(path):
            import stat

            actual = path.lstat()
            if not stat.S_ISREG(actual.st_mode) or actual.st_nlink != 1:
                raise RuntimeError("identity_evidence_not_regular")
            return path.read_bytes()

        async def capture(stage, action):
            try:
                return await asyncio.to_thread(action)
            except Exception as error:
                secondary.append(
                    {"stage": stage, "type": type(error).__name__, "message": str(error)}
                )
                return None

        process = await capture(
            "process_read_parse",
            lambda: json.loads(regular_bytes(tmp_path / "header12-process.json")),
        )
        memory_process = (
            None if primary is None else getattr(primary, "scale_evidence_process", None)
        )
        stdout = await capture(
            "stdout_read", lambda: regular_bytes(tmp_path / "header12-stdout.txt")
        )
        stderr = await capture(
            "stderr_read", lambda: regular_bytes(tmp_path / "header12-stderr.txt")
        )
        record = {
            "argv": command,
            "cwd": str(root / "outside"),
            "process_from_file": process,
            "helper_exception_memory_only": memory_process,
            "primary_error": None
            if primary is None
            else {
                "type": type(primary).__name__,
                "message": str(primary),
                "timeout": isinstance(primary, (TimeoutError, subprocess.TimeoutExpired)),
            },
            "stdout_present": stdout is not None,
            "stderr_present": stderr is not None,
            "secondary_errors": secondary,
        }
        if stdout is not None:
            await capture(
                "stdout_save", lambda: (tmp_path / "identity-stdout.txt").write_bytes(stdout)
            )
        if stderr is not None:
            await capture(
                "stderr_save", lambda: (tmp_path / "identity-stderr.txt").write_bytes(stderr)
            )
        await capture(
            "record_save",
            lambda: (tmp_path / "identity-command.json").write_text(json.dumps(record, indent=2)),
        )
        if secondary:
            detail = "identity_secondary_evidence_errors:" + json.dumps(secondary)
            if primary is not None:
                primary.add_note(detail)
            else:
                raise RuntimeError(detail)
    assert returned is not None and stdout is not None and stderr is not None
    child, actual_process, facts = returned
    assert child.returncode == 0, stderr.decode()
    actual = json.loads(stdout)
    assert Path(actual["module"]).is_relative_to(site) and actual["version"] == "0.4.0a1"
    assert all(str(source) not in p for p in actual["path"])
    assert facts == actual and actual_process["cwd"] == str(root / "outside")
    assert (
        actual_process["pidfd_close_verified"]
        and actual_process["stdout_eof"]
        and actual_process["stderr_eof"]
    )
    assert not actual_process["signal_operations"]
    await asyncio.to_thread(
        (tmp_path / "wheel-identity.json").write_text,
        json.dumps(
            {
                "sha256": hashlib.sha256(await asyncio.to_thread(wheel.read_bytes)).hexdigest(),
                "identity": actual,
                "process": actual_process,
            },
            indent=2,
        ),
    )


async def test_installed_scale_100x3(tmp_path):
    import asyncio

    from test_eval_scale import _run_scale_evidence_process

    root = Path(os.environ["KRIM_PN3_ROOT"])
    site = root / "installed/lib/python3.12/site-packages"
    destination = root / "outside" / Path(os.environ["KRIM_PN3_EVIDENCE"]).name / tmp_path.name
    await asyncio.to_thread(destination.mkdir)
    driver = (
        r"""import asyncio,collections,hashlib,json,os,sqlite3,sys,time
from pathlib import Path
from contextlib import closing
from importlib.resources import files
source,root=map(Path,sys.argv[1:3])
import stat
def _write_scale_checkpoint(stage, status, *, _defer_cleanup_error=False, **known):
    active = sys.exception()
    try:
        identity = getattr(_write_scale_checkpoint, 'identity', None)
        if identity is None:
            path = root / 'header12-process.json'
            st = path.lstat()
            if not stat.S_ISREG(st.st_mode) or st.st_nlink != 1:
                raise RuntimeError('checkpoint_identity_not_regular')
            process = json.loads(path.read_text())
"""
        "            identity = {key: process[key] for key in ['run_ide"
        "ntity', 'driver_sha256', 'helper_ast_sha256', 'checkpoint_ast_"
        "sha256']}"
        r"""
            _write_scale_checkpoint.identity = identity
        sequence = getattr(_write_scale_checkpoint, 'sequence', 0) + 1
        if sequence > 128:
            raise RuntimeError('checkpoint_record_limit')
        fields = {}
        for key, value in known.items():
"""
        "            if not isinstance(key, str) or len(key) > 64 or (v"
        "alue is not None and type(value) not in (str, int, float, bool"
        ")):"
        r"""
                raise RuntimeError('checkpoint_non_scalar')
            fields[key] = value[:512] if isinstance(value, str) else value
"""
        "        row = dict(identity, sequence=sequence, stage=stage, s"
        "tatus=status, monotonic_ns=time.monotonic_ns(), time_ns=time.t"
        "ime_ns(), pid=os.getpid(), uid=os.getuid(), known=fields)"
        r"""
        raw = (json.dumps(row, ensure_ascii=False, allow_nan=False) + '\n').encode()
        if len(raw) > 4096:
            raise RuntimeError('checkpoint_record_bytes')
"""
        "        fd = os.open(root / 'header12-checkpoints.jsonl', os.O"
        "_WRONLY | os.O_CREAT | os.O_APPEND | os.O_NOFOLLOW, 384)"
        r"""
        try:
            st = os.fstat(fd)
            if not stat.S_ISREG(st.st_mode) or st.st_nlink != 1 or st.st_uid != os.getuid():
                raise RuntimeError('checkpoint_output_not_regular')
            if st.st_size + len(raw) > 128 * 4096:
                raise RuntimeError('checkpoint_total_bytes')
            view = memoryview(raw)
            while view:
                count = os.write(fd, view)
                if count <= 0:
                    raise RuntimeError('checkpoint_short_write')
                view = view[count:]
            os.fsync(fd)
        finally:
            os.close(fd)
        _write_scale_checkpoint.sequence = sequence
    except BaseException as evidence_error:
        errors = getattr(_write_scale_checkpoint, 'evidence_errors', None)
        if errors is None:
            errors = []
            _write_scale_checkpoint.evidence_errors = errors
        if len(errors) < 128:
"""
        "            errors.append(dict(stage=stage, status=status, typ"
        "e=type(evidence_error).__name__, message=str(evidence_error)[:"
        "512], pid=os.getpid(), active_type=None if active is None else"
        " type(active).__name__, deferred=_defer_cleanup_error, monoton"
        "ic_ns=time.monotonic_ns()))"
        r"""
        else:
"""
        "            _write_scale_checkpoint.error_overflow = getattr(_"
        "write_scale_checkpoint, 'error_overflow', 0) + 1"
        r"""
        if _defer_cleanup_error:
            if isinstance(globals().get('facts'), dict):
                facts['checkpoint_errors'] = list(errors)
            return
        if active is not None:
            raise active from evidence_error
        raise
_write_scale_checkpoint('bootstrap','start')
from sqlalchemy import event
from sqlalchemy.pool import Pool
from krim_agent.evaluation.app import open_eval
from krim_agent.evaluation.repository import Repository
from krim_agent.evaluation.domain import Budget,CaseSpec,DatasetSpec,DatasetRef,TaskSpec
"""
        "from krim_agent.evaluation.continuous_types import Confirmatio"
        "nRequest,ContinuousRequest,FreezeConfirmed,ImportRequest,Sourc"
        "eSelection,ComparisonPolicy"
        r"""
from krim_agent.evaluation.continuous_metrics import continuous_rules
from krim_agent.evaluation.adapters.local_process import LocalProcessAdapter
facts=dict(uid=os.getuid(),connections=[],retired=[],error=None)
facts['installed_identity']=dict(module=sys.modules[Repository.__module__].__file__,path=sys.path,cwd=str(Path.cwd()),executable=sys.executable)
assert Path(facts['installed_identity']['module']).is_relative_to(source)
assert str(Path.cwd())==str(root.parent.parent) and all('/source' not in x for x in sys.path)
for name in ['connect','checkout','reset','checkin','close']:
"""
        "    def record(*args,name=name):facts['connections'].append(di"
        "ct(event=name,connection=id(args[0]),at=time.monotonic()))"
        r"""
    event.listen(Pool,name,record)
retire=LocalProcessAdapter.retire
def retired(adapter,ref,receipt):
    facts['retired'].append(receipt)
    return retire(adapter,ref,receipt)
LocalProcessAdapter.retire=retired
async def seed(tmp_path):
    app=await open_eval(tmp_path/'installed.sqlite')
    body=json.loads(files('krim_agent.evaluation.examples').joinpath('reconciliation.v1.json').read_text())
    data=DatasetSpec.model_validate(body|{'cases':body['cases'][:1]})
    await app.repository.freeze(data)
    request=TaskSpec(request_key='installed-seed',dataset=data.ref(),metrics=app.repository.metric_registry.specs())
    view=await app.run(request)
    assert (await app.kernel.wait(view['id']))['status']=='completed'
    return app,data,SourceSelection(task_id=view['id'],case_id=data.cases[0].case_id),app.continuous
def scale_cases(count=100):
"""
        "    body = json.loads(files('krim_agent.evaluation.examples')."
        "joinpath('scale_reconciliation.v1.json').read_text())"
        r"""
    return [CaseSpec.model_validate(c) for c in body['cases'][:count]]
async def confirmed_scale(tmp_path, count=100):
    app, initial, selection, flow = await seed(tmp_path)
    try:
        flow.redaction['artifact_paths'] = ['ledger.json', 'summary.md']
        confirmations = []
        for index, case in enumerate(scale_cases(count)):
            flow.sampling['salt_id'] = 'pn3s-human-' + str(index)
"""
        "            imported = await flow.import_sources(ImportRequest"
        "(request_key='scale-import-' + str(index), sources=[selection]"
        "))"
        r"""
            draft = await app.repository.candidate('local', imported[0]['id'])
"""
        "            row = await flow.confirm_case(ConfirmationRequest("
        "request_key='scale-confirm-' + str(index), candidate_id=draft["
        "'candidate_id'], expected_revision=draft['revision'], expected"
        "_hash=draft['hash'], case=case, context='manual_reconstructed'"
        ", reason='\u56fa\u5b9a\u6574\u6570gold\u4e0e\u72ec\u7acb\u521d"
        "\u6001\uff1b\u6b64\u6b21\u6267\u884c\u624d\u80fd\u5efa\u7acb"
        "\u65b0Case\u7684\u771f\u5b9econtext\u8bc1\u660e\u3002'))"
        r"""
            assert row['status'] == 'confirmed'
            assert row['context'] == 'manual_reconstructed'
            confirmations.append(row['id'])
"""
        "        frozen = await flow.freeze_dataset(FreezeConfirmed(req"
        "uest_key='scale-freeze', name='pn3-scale-reconciliation', vers"
        "ion='1', confirmations=confirmations))"
        r"""
        assert len(set(confirmations)) == count
"""
        "        request = ContinuousRequest(request_key='scale-execute"
        "', mode='execute', dataset=DatasetRef.model_validate(frozen['d"
        "ataset_ref']), dataset_envelope_fp=frozen['dataset_envelope_fp"
        "'], metrics=continuous_rules(app.repository.metric_registry), "
        "execution=Budget(repetitions=3, concurrency=2))"
        r"""
        return (app, flow, request)
    except BaseException:
        await app.close()
        raise
def overlap_target(app, tmp_path):
    gate = tmp_path / 'process-gate'
    gate.mkdir()
    real = app.adapter.worker
"""
        '    code = f"import os,runpy,time\\nfrom pathlib import Path\\'
        "ngate=Path({str(gate)!r})\\nname=Path.cwd().name\\nif name in "
        "{{'scale-000-0','scale-000-1'}}:\\n (gate/name).write_text(str"
        "(os.getpid()))\\n end=time.monotonic()+2\\n while not (gate/'r"
        "elease').exists():\\n  if time.monotonic()>=end: raise Runtime"
        "Error('fixture_gate_unreleased')\\n  time.sleep(.005)\\nrunpy."
        "run_path({str(real)!r},run_name='__main__')\\n\""
        r"""
    compile(code, 'fixed-scale-reference-wrapper', 'exec')
    worker = tmp_path / 'scale-reference-wrapper.py'
    worker.write_text(code)
    app.adapter.worker = worker
"""
        "    app.adapter._descriptor['implementation_digest'] = hashlib"
        ".sha256(worker.read_bytes()).hexdigest()"
        r"""
    app.adapter._descriptor['real_worker_digest'] = hashlib.sha256(real.read_bytes()).hexdigest()
    receipts = []
    intervals = []
    collect = app.adapter._collect

    async def observed(work):
"""
        "        interval = {'task_id': work.prepared.spec.task_id, 're"
        "f': work.ref.id, 'pid': work.process.pid, 'start_monotonic': t"
        "ime.monotonic()}"
        r"""
        intervals.append(interval)
        try:
            output = await collect(work)
        finally:
"""
        "            interval.update(end_monotonic=time.monotonic(), re"
        "turncode=work.process.returncode, pidfd=work.pidfd, collector_"
        "done=work.collector.done(), stdout_eof=work.stdout_eof, stderr"
        "_eof=work.stderr_eof)"
        r"""
"""
        "        receipts.append({'task_id': work.prepared.spec.task_id"
        ", 'case_id': work.prepared.spec.case_id, 'repetition': work.pr"
        "epared.spec.repetition, 'ref': work.ref.id, 'pid': work.proces"
        "s.pid, 'wait_returncode': work.process.returncode, 'pidfd': wo"
        "rk.pidfd, 'stdout_eof': work.stdout_eof, 'stderr_eof': work.st"
        "derr_eof, 'streams_joined': all((t.done() for t in work.stream"
        "_tasks)), 'started_at': output['process']['started_at'], 'fini"
        "shed_at': output['process']['finished_at'], 'group_empty': out"
        "put['process']['group_empty'], 'uid': output['process']['uid']"
        "})"
        r"""
        return output
    app.adapter._collect = observed
    return (gate, receipts, intervals)
async def all_report_rows(repo, task_id):
    rows, cursor = ([], None)
    while True:
        page = await repo.report_page('local', task_id, cursor=cursor, limit=16)
        assert 0 < len(page.rows) <= 16
        assert len(json.dumps(page.rows).encode()) <= 2097152
        rows.extend(page.rows)
        cursor = page.next_cursor
        if cursor is None:
            return rows
async def main():
    app=None;gate=None;before=asyncio.all_tasks();facts['fd_before']=len(os.listdir('/proc/self/fd'))
    try:
        app,flow,request=await confirmed_scale(root)
        facts.update(seed_target_calls=app.owner.spawn_calls,database=str(app.repository.path))
        assert app.owner.spawn_calls==1
        gate,receipts,intervals=overlap_target(app,root)
        start=app.owner.spawn_calls;scored=app.repository.metric_registry.calls
        view=await flow.submit(request);facts['task_id']=view['id']
        async with asyncio.timeout(2):
"""
        "            while not all((gate/name).exists() for name in ['s"
        "cale-000-0','scale-000-1']):await asyncio.sleep(.005)"
        r"""
        live=[w for w in app.owner.jobs.values() if w.process.returncode is None and w.pidfd>=0]
        assert len(live)==2
        facts['overlap']=[dict(pid=w.process.pid,pidfd=w.pidfd) for w in live]
        (gate/'release').write_text('release actual installed workers')
        state=await app.kernel.wait(view['id']);report=await app.repository.report(view['id'])
        rows=await all_report_rows(app.repository,view['id'])
        assert state['status']=='completed' and state['planned_count']==300
        assert len(rows)==300 and all(r['quality']=='passed' for r in rows)
        assert len({r['execution_ref'] for r in rows})==300
        assert report['summary']['planned_cells']==1200 and report['summary']['scored_cells']==1200
"""
        "        assert app.owner.spawn_calls-start==300 and app.reposi"
        "tory.metric_registry.calls-scored==1200"
        r"""
        assert (await flow.submit(request))['id']==view['id'] and app.owner.spawn_calls-start==300
        policy=ComparisonPolicy(baseline_id=view['id'],candidate_id=view['id'],baseline_hash=report['report_hash'],candidate_hash=report['report_hash'],allowed_subjects=[report['subject_fingerprint']])
        assert (await app.repository.compare_paged('local',policy))['status']=='pass'
        actual=[r for r in facts['retired'] if r['task_id']==view['id']]
"""
        "        assert len(actual)==300 and all(r['closed'] and r['pid"
        "fd_closed'] and r['group_empty'] for r in actual)"
        r"""
        facts.update(target_calls=app.owner.spawn_calls-start,metric_calls=app.repository.metric_registry.calls-scored,
            report_hash=report['report_hash'],summary=report['summary'],high_water=app.kernel._high_water[view['id']],receipts=receipts)
        assert facts['high_water']['workers']==facts['high_water']['inflight']==2
    finally:
        if gate is not None:(gate/'release').write_text('fixture final release')
        if app is not None:
            await app.close()
"""
        "            facts.update(closed=app.repository.closed and app."
        "owner.closed,writer_fd=app.repository._writer_fd,"
        r"""
"""
        "                producer_done=all(t.done() for t in app.kernel"
        ".tasks.values()),inflight=len(app.kernel._inflight),resources="
        "app.repository.resources)"
        r"""
"""
        "            assert facts['closed'] and facts['writer_fd'] is N"
        "one and facts['producer_done'] and not facts['inflight']"
        r"""
            with closing(sqlite3.connect(app.repository.path.as_uri()+'?mode=ro',uri=True)) as db:
                db.execute('PRAGMA query_only=ON')
                with closing(sqlite3.connect(root/'installed-backup.sqlite')) as out:db.backup(out)
            facts['backup']=str(root/'installed-backup.sqlite')
"""
        "        facts['pending']=[repr(t) for t in asyncio.all_tasks()"
        " if t not in before and t is not asyncio.current_task() and no"
        "t t.done()]"
        r"""
        facts['fd_after']=len(os.listdir('/proc/self/fd'))
        (root/'header12-consumer.json').write_text(json.dumps(facts,indent=2))
        assert not facts['pending'] and facts['fd_after']<=facts['fd_before']
asyncio.run(main())
"""
    )
    compile(driver, "installed-scale-consumer", "exec")
    argv = [
        str(root / "installed/bin/python"),
        "-I",
        "-B",
        "-c",
        driver,
        str(site),
        str(destination),
    ]
    returned = None
    primary = None
    try:
        returned = await _run_scale_evidence_process(
            argv,
            destination,
            execution_timeout_seconds=240,
            cwd=root / "outside",
            source_database=None,
            destination_database=str(destination / "installed.sqlite"),
        )
    except BaseException as error:
        primary = error
        raise
    finally:
        secondary = []

        def regular_bytes(path):
            import stat

            actual = path.lstat()
            if not stat.S_ISREG(actual.st_mode) or actual.st_nlink != 1:
                raise RuntimeError("installed_evidence_not_regular")
            return path.read_bytes()

        async def capture(stage, action):
            try:
                return await asyncio.to_thread(action)
            except Exception as error:
                secondary.append(
                    {"stage": stage, "type": type(error).__name__, "message": str(error)}
                )
                return None

        process_path = destination / "header12-process.json"
        process = await capture(
            "process_read_parse", lambda: json.loads(regular_bytes(process_path))
        )
        memory_process = (
            None if primary is None else getattr(primary, "scale_evidence_process", None)
        )
        stdout = await capture(
            "stdout_read", lambda: regular_bytes(destination / "header12-stdout.txt")
        )
        stderr = await capture(
            "stderr_read", lambda: regular_bytes(destination / "header12-stderr.txt")
        )
        record = {
            "argv": argv,
            "cwd": str(root / "outside"),
            "process_receipt": str(process_path),
            "process_from_file": process,
            "helper_exception_memory_only": memory_process,
            "primary_error": None
            if primary is None
            else {
                "type": type(primary).__name__,
                "message": str(primary),
                "timeout": isinstance(primary, (TimeoutError, subprocess.TimeoutExpired)),
            },
            "driver_sha256": hashlib.sha256(driver.encode()).hexdigest(),
            "consumer": str(destination / "header12-consumer.json"),
            "stdout_present": stdout is not None,
            "stderr_present": stderr is not None,
            "secondary_errors": secondary,
        }
        if stdout is not None:
            await capture(
                "stdout_save", lambda: (tmp_path / "installed-stdout.txt").write_bytes(stdout)
            )
        if stderr is not None:
            await capture(
                "stderr_save", lambda: (tmp_path / "installed-stderr.txt").write_bytes(stderr)
            )
        await capture(
            "record_save",
            lambda: (tmp_path / "installed-command.json").write_text(json.dumps(record, indent=2)),
        )
        if secondary:
            detail = "installed_secondary_evidence_errors:" + json.dumps(secondary)
            if primary is not None:
                primary.add_note(detail)
            else:
                raise RuntimeError(detail)
    assert returned is not None and stdout is not None and stderr is not None
    child, actual_process, facts = returned
    assert child.returncode == 0, stdout.decode() + stderr.decode()
    assert actual_process["cwd"] == str(root / "outside")
    assert actual_process["driver_sha256"] == hashlib.sha256(driver.encode()).hexdigest()
    assert (
        actual_process["pidfd_close_verified"]
        and actual_process["stdout_eof"]
        and actual_process["stderr_eof"]
    )
    assert not actual_process["signal_operations"] and actual_process["cleanup_status"] == "closed"
    assert Path(facts["installed_identity"]["module"]).is_relative_to(site)
    assert facts["installed_identity"]["cwd"] == str(root / "outside")
    assert facts["target_calls"] == 300 and facts["metric_calls"] == 1200


@pytest.mark.parametrize("mode", ["observed_score", "rescore"])
async def test_installed_scale_cold_zero_execution(tmp_path, mode):
    import asyncio

    contract = Path(
        "/artifacts/krim-agent-core-20261019/vm-pn3-scale-implementatio"
        "n-20261023T152826061485/static-supplement-03/completion20-child/APPROVED-C"
        "ONTRACT.json"
    )
    selected = json.loads(await asyncio.to_thread(contract.read_text))["readonly_existing_inputs"][
        "main_300_source"
    ]
    from test_eval_scale import _run_scale_evidence_process

    root = Path(os.environ["KRIM_PN3_ROOT"])
    site = root / "installed/lib/python3.12/site-packages"
    destination = root / "outside" / Path(os.environ["KRIM_PN3_EVIDENCE"]).name / tmp_path.name
    await asyncio.to_thread(destination.mkdir)
    driver = (
        "import asyncio,collections,fcntl,hashlib,json,os,sqlite3,stat,"
        "sys,threading,time,traceback"
        r"""
from pathlib import Path
from contextlib import closing
source,root=map(Path,sys.argv[1:3]);selected=json.loads(sys.argv[3]);mode=sys.argv[4]
def _write_scale_checkpoint(stage, status, *, _defer_cleanup_error=False, **known):
    active = sys.exception()
    try:
        identity = getattr(_write_scale_checkpoint, 'identity', None)
        if identity is None:
            path = root / 'header12-process.json'
            st = path.lstat()
            if not stat.S_ISREG(st.st_mode) or st.st_nlink != 1:
                raise RuntimeError('checkpoint_identity_not_regular')
            process = json.loads(path.read_text())
"""
        "            identity = {key: process[key] for key in ['run_ide"
        "ntity', 'driver_sha256', 'helper_ast_sha256', 'checkpoint_ast_"
        "sha256']}"
        r"""
            _write_scale_checkpoint.identity = identity
        sequence = getattr(_write_scale_checkpoint, 'sequence', 0) + 1
        if sequence > 128:
            raise RuntimeError('checkpoint_record_limit')
        fields = {}
        for key, value in known.items():
"""
        "            if not isinstance(key, str) or len(key) > 64 or (v"
        "alue is not None and type(value) not in (str, int, float, bool"
        ")):"
        r"""
                raise RuntimeError('checkpoint_non_scalar')
            fields[key] = value[:512] if isinstance(value, str) else value
"""
        "        row = dict(identity, sequence=sequence, stage=stage, s"
        "tatus=status, monotonic_ns=time.monotonic_ns(), time_ns=time.t"
        "ime_ns(), pid=os.getpid(), uid=os.getuid(), known=fields)"
        r"""
        raw = (json.dumps(row, ensure_ascii=False, allow_nan=False) + '\n').encode()
        if len(raw) > 4096:
            raise RuntimeError('checkpoint_record_bytes')
"""
        "        fd = os.open(root / 'header12-checkpoints.jsonl', os.O"
        "_WRONLY | os.O_CREAT | os.O_APPEND | os.O_NOFOLLOW, 384)"
        r"""
        try:
            st = os.fstat(fd)
            if not stat.S_ISREG(st.st_mode) or st.st_nlink != 1 or st.st_uid != os.getuid():
                raise RuntimeError('checkpoint_output_not_regular')
            if st.st_size + len(raw) > 128 * 4096:
                raise RuntimeError('checkpoint_total_bytes')
            view = memoryview(raw)
            while view:
                count = os.write(fd, view)
                if count <= 0:
                    raise RuntimeError('checkpoint_short_write')
                view = view[count:]
            os.fsync(fd)
        finally:
            os.close(fd)
        _write_scale_checkpoint.sequence = sequence
    except BaseException as evidence_error:
        errors = getattr(_write_scale_checkpoint, 'evidence_errors', None)
        if errors is None:
            errors = []
            _write_scale_checkpoint.evidence_errors = errors
        if len(errors) < 128:
"""
        "            errors.append(dict(stage=stage, status=status, typ"
        "e=type(evidence_error).__name__, message=str(evidence_error)[:"
        "512], pid=os.getpid(), active_type=None if active is None else"
        " type(active).__name__, deferred=_defer_cleanup_error, monoton"
        "ic_ns=time.monotonic_ns()))"
        r"""
        else:
"""
        "            _write_scale_checkpoint.error_overflow = getattr(_"
        "write_scale_checkpoint, 'error_overflow', 0) + 1"
        r"""
        if _defer_cleanup_error:
            if isinstance(globals().get('facts'), dict):
                facts['checkpoint_errors'] = list(errors)
            return
        if active is not None:
            raise active from evidence_error
        raise
_write_scale_checkpoint('bootstrap','start')
assert str(Path.cwd())==str(root.parent.parent)
from sqlalchemy import event
from sqlalchemy.pool import Pool
from krim_agent.contracts import ServiceError
from krim_agent.evaluation.app import open_eval
from krim_agent.evaluation.repository import Repository
from krim_agent.evaluation.sources import EvalSourceReader
from krim_agent.evaluation.adapters.local_process import LocalProcessAdapter
from krim_agent.evaluation.domain import Budget,CaseSpec,DatasetRef,neutral_fingerprint
from krim_agent.evaluation.continuous_types import (
    ConfirmationRequest,ContinuousRequest,FreezeConfirmed,ImportRequest,ScheduleSpec,
    ScoreSource,SourceSelection,
)
from krim_agent.evaluation.continuous_metrics import continuous_rules
backup=Path(selected['backup']['path']);destination=root/'header12-destination.sqlite'
for path in [backup,*backup.parents]:
    assert not path.is_symlink()
known=backup.lstat()
assert stat.S_ISREG(known.st_mode) and known.st_nlink==1
before_hash=hashlib.sha256(backup.read_bytes()).hexdigest()
assert before_hash==selected['backup']['sha256']
with closing(sqlite3.connect(backup.as_uri()+'?mode=ro',uri=True)) as db:
    db.execute('PRAGMA query_only=ON')
    assert db.execute('PRAGMA query_only').fetchone()[0]==1
    source_header=json.loads(db.execute('SELECT body FROM eval_tasks WHERE id=? AND scope=?',
        (selected['task_id'],'local')).fetchone()[0])
    source_report=json.loads(db.execute('SELECT body FROM eval_reports WHERE task_id=?',
        (selected['task_id'],)).fetchone()[0])
    ref=source_header['dataset_ref']
    cases=[json.loads(row[0]) for row in db.execute(
        'SELECT body FROM dataset_cases WHERE name=? AND version=? ORDER BY case_id COLLATE BINARY',
        (ref['name'],ref['version']))]
    trials=[json.loads(row[0]) for row in db.execute(
        'SELECT body FROM eval_trials WHERE task_id=? ORDER BY case_id COLLATE BINARY,repetition',
        (selected['task_id'],))]
assert source_header['status']=='completed' and len(cases)==100 and len(trials)==300
assert source_report['report_hash']==selected['report_hash']==source_header['report_hash']
facts=dict(node='tests/safe_eval/test_eval_scale.py::test_shared_modes_scale['+mode+']',
    mode=mode,uid=os.getuid(),source_database=str(backup),source_hash_before=before_hash,
    source_task_id=selected['task_id'],source_report_hash=selected['report_hash'],
    destination_database=str(destination),stage='bootstrap',error=None,close_errors=[],
    audit_errors=[],connections=[],source_query_only=[],calls={},phases=[],
    parent=None,result=None,source_execution_calls_this_command=0)
codes={getattr(LocalProcessAdapter,name).__code__:'target_'+name
       for name in ['prepare','execute','collect','cancel']}
codes[Repository.adapter.__code__]='adapter_lookup'
facts['calls']={name:0 for name in codes.values()}
facts['calls']['provider_stream']=0
def profile(frame,kind,value):
    if kind=='call':
        label=codes.get(frame.f_code)
        if label is not None:facts['calls'][label]+=1
"""
        "        if frame.f_globals.get('__name__','').startswith('krim"
        "_agent.providers.') and frame.f_code.co_name=='stream':"
        r"""
            facts['calls']['provider_stream']+=1
def connected(dbapi,record):
    cur=dbapi.cursor()
    try:
        cur.execute('PRAGMA database_list');name=cur.fetchone()[2]
        record.info['completion20_role']='source' if name==str(backup) else 'destination'
        if name==str(backup):cur.execute('PRAGMA query_only=ON')
    finally:cur.close()
def checkout(dbapi,record,*args):
    if record.info.get('completion20_role')=='source':
        cur=dbapi.cursor()
        try:
            cur.execute('PRAGMA query_only');facts['source_query_only'].append(cur.fetchone()[0])
        finally:cur.close()
event.listen(Pool,'connect',connected);event.listen(Pool,'checkout',checkout)
listeners=[]
for name in ['connect','checkout','reset','checkin','close']:
    def observed(*args,name=name):
        facts['connections'].append(dict(event=name,connection=id(args[0]),
            role=args[1].info.get('completion20_role'),at=time.monotonic()))
    event.listen(Pool,name,observed);listeners.append((name,observed))
_write_scale_checkpoint('bootstrap','done')
async def main():
    app=source_repo=None
    before=asyncio.all_tasks();facts['fd_before']=len(os.listdir('/proc/self/fd'))
    facts['threads_before']=[(t.ident,t.name) for t in threading.enumerate()]
    loop=asyncio.get_running_loop();old_handler=loop.get_exception_handler()
    facts['unexpected']=[]
    loop.set_exception_handler(lambda current,context:facts['unexpected'].append(str(context)))
    previous=sys.getprofile();sys.setprofile(profile)
    clock_value=[100000.0]
    clock=lambda:clock_value[0]
    try:
        facts['stage']='open';_write_scale_checkpoint('open','start')
        source_repo=await Repository.open(backup,readonly=True)
        reader=EvalSourceReader(source_repo,scope='local',include_context=True)
        app=await open_eval(destination,execution_enabled=False,
            sources={'accepted-scale':reader},clock=clock)
        assert app.adapter is None and app.owner is None and app.kernel.repository is app.repository
        _write_scale_checkpoint('open','done')
        flow=app.continuous;flow.redaction['artifact_paths']=['ledger.json','summary.md']
        selected_scores=[];drafts={}
        facts['stage']='import';_write_scale_checkpoint('import','start')
        for offset in range(0,300,32):
            batch=trials[offset:offset+32]
"""
        "            rows=await flow.import_sources(ImportRequest(reque"
        "st_key='completion20-import-'+str(offset),"
        r"""
                source_alias='accepted-scale',sources=[SourceSelection(task_id=selected['task_id'],
                case_id=row['case_id'],repetition=row['repetition'],revision=source_header['revision'],
                evidence_hash=row['output_digest']) for row in batch]))
            assert len(rows)==len(batch)
            for row in rows:
                assert row['eligibility']=='eligible' and row['source_proof']['complete']
                selected_scores.append(ScoreSource(case_id=row['source']['case_id'],
                    repetition=row['source']['repetition'],observation_id=row['id'],
                    observation_hash=row['content_hash']))
                if row['source']['repetition']==0:drafts[row['source']['case_id']]=row['id']
            del rows
        assert len(selected_scores)==300 and len(drafts)==100
        _write_scale_checkpoint('import','done',sources=300)
        facts['stage']='confirm';_write_scale_checkpoint('confirm','start')
        confirmations=[]
        for case in cases:
            draft=await app.repository.candidate('local',drafts[case['case_id']])
"""
        "            value=await flow.confirm_case(ConfirmationRequest("
        "request_key='completion20-confirm-'+case['case_id'],"
        r"""
                candidate_id=draft['candidate_id'],expected_revision=draft['revision'],
                expected_hash=draft['hash'],case=CaseSpec.model_validate(case),context='complete',
                reason='固定真实来源及未改gold，独立确认本fixture。'))
            assert value['status']=='confirmed'
            confirmations.append(value['id'])
        frozen=await flow.freeze_dataset(FreezeConfirmed(request_key='completion20-freeze',
            name='completion20-'+mode,version='1',confirmations=confirmations))
        _write_scale_checkpoint('confirm','done',cases=100)
        rules=continuous_rules(app.repository.metric_registry)
        budget=Budget(repetitions=3,concurrency=2)
        assert budget.batch_timeout_seconds==120 and budget.item_timeout_seconds==20
        assert budget.scorer_timeout_seconds==1 and len(rules)==4
        observed_request=ContinuousRequest(request_key='completion20-parent',mode='observed_score',
            dataset=DatasetRef.model_validate(frozen['dataset_ref']),
            dataset_envelope_fp=frozen['dataset_envelope_fp'],metrics=rules,
            target_key=source_header['target']['key'],execution=budget,observations=selected_scores)
        assert all(value==0 for value in facts['calls'].values())
        if mode=='rescore':
            facts['stage']='parent';_write_scale_checkpoint('parent','start')
            started_count=app.repository.metric_registry.calls
            parent_view=await flow.submit(observed_request)
            parent_done=await app.kernel.wait(parent_view['id'])
            parent_report=await app.repository.report(parent_view['id'])
"""
        "            assert parent_done['status']=='completed' and pare"
        "nt_report['summary']['scored_cells']==1200"
        r"""
            parent_cells=app.repository.metric_registry.calls-started_count
            assert parent_cells==1200
            assert (await flow.submit(observed_request))['id']==parent_view['id']
            assert app.repository.metric_registry.calls-started_count==1200
            facts['parent']=dict(id=parent_view['id'],report_hash=parent_report['report_hash'],
                score_cells=parent_cells,planned_trials=parent_done['planned_count'],
                database=str(destination),kind=parent_report['kind'])
            _write_scale_checkpoint('parent','done',task_id=parent_view['id'],score_cells=1200)
            request=ContinuousRequest(request_key='completion20-rescore',mode='rescore',
                dataset=observed_request.dataset,dataset_envelope_fp=observed_request.dataset_envelope_fp,
                target_key=observed_request.target_key,metrics=rules,execution=budget,
                parent_task_id=parent_view['id'],parent_report_hash=parent_report['report_hash'])
            baseline_cells=app.repository.metric_registry.calls
            facts['stage']='submit';_write_scale_checkpoint('submit','start')
            view=await flow.submit(request)
        elif mode=='observed_score':
            request=observed_request;baseline_cells=app.repository.metric_registry.calls
            assert baseline_cells==0
            facts['stage']='submit';_write_scale_checkpoint('submit','start')
            view=await flow.submit(request)
        else:
            spec=ScheduleSpec(schedule_id='completion20-scoring-window',version='1',
                template=observed_request,epoch=100000,interval_seconds=60,max_windows=1)
            facts['stage']='schedule';_write_scale_checkpoint('schedule','start')
            schedule=await flow.create_schedule(spec)
            assert (await flow.create_schedule(spec))['spec_hash']==schedule['spec_hash']
            facts['schedule']=schedule
            _write_scale_checkpoint('schedule','done')
            baseline_cells=app.repository.metric_registry.calls
            assert baseline_cells==0
            facts['stage']='submit';_write_scale_checkpoint('submit','start')
            view=await flow.tick(spec.schedule_id)
            assert (await flow.tick(spec.schedule_id))['id']==view['id']
            request=None
        facts['task_id']=view['id'];_write_scale_checkpoint('submit','done',task_id=view['id'])
        facts['stage']='wait';_write_scale_checkpoint('wait','start',task_id=view['id'])
        done=await app.kernel.wait(view['id']);report=await app.repository.report(view['id'])
        assert done['status']=='completed' and done['planned_count']==300
        assert report['summary']['planned_cells']==report['summary']['scored_cells']==1200
        assert report['summary']['passed']==300 and report['summary']['token_usage'] is None
        assert app.repository.metric_registry.calls-baseline_cells==1200
        assert done['continuous']['schema_version']=='continuous-eval/2'
        if mode=='rescore':
            assert done['continuous']['parent']['task_id']==facts['parent']['id']
            assert done['continuous']['parent']['report_hash']==facts['parent']['report_hash']
        elif mode=='schedule':
            assert done['continuous']['schedule']['window_index']==0
            assert done['continuous']['schedule']['spec_hash']==facts['schedule']['spec_hash']
        facts['result']=dict(id=view['id'],report_hash=report['report_hash'],kind=report['kind'],
            status=done['status'],score_cells=app.repository.metric_registry.calls-baseline_cells,
            planned_trials=done['planned_count'],execution=done['execution'],
            inputs_root=done['continuous']['inputs_root'],parent=done['continuous']['parent'])
        _write_scale_checkpoint('wait','done',task_id=view['id'],score_cells=1200)
        rows=[];cursor=None
        while True:
            page=await app.repository.report_page('local',view['id'],cursor=cursor,limit=16)
            rows.extend(page.rows);cursor=page.next_cursor
            if cursor is None:break
        assert len(rows)==300 and {(r['case_id'],r['repetition']) for r in rows}=={
            (c['case_id'],r) for c in cases for r in range(3)}
        facts['report_pages_verified']=len(rows)
        with closing(sqlite3.connect(destination.as_uri()+'?mode=ro',uri=True)) as db:
            db.execute('PRAGMA query_only=ON')
"""
        "            count=db.execute('SELECT count(*) FROM continuous_"
        "inputs WHERE task_id=?',(view['id'],)).fetchone()[0]"
        r"""
        assert count==300
        before_key=app.repository.metric_registry.calls
"""
        "        if mode in {'rescore','observed_score'}:assert (await "
        "flow.submit(request))['id']==view['id']"
        r"""
        else:
            assert (await flow.tick(spec.schedule_id))['id']==view['id']
            clock_value[0]=100060.0
            try:await flow.tick(spec.schedule_id)
            except ServiceError as error:
                assert error.code=='closed';facts['over_window_code']=error.code
            else:raise AssertionError('out_of_window_was_admitted')
        assert app.repository.metric_registry.calls==before_key
        assert all(value==0 for value in facts['calls'].values())
        facts['total_scoring_calls']=app.repository.metric_registry.calls
        facts['original_key_score_increment']=app.repository.metric_registry.calls-before_key
        facts['stage']='completed';_write_scale_checkpoint('original_key','done',task_id=view['id'])
    except BaseException as error:
        facts['error']=dict(type=type(error).__name__,code=getattr(error,'code',None),
            message=str(error),stage=facts['stage'],frames=[dict(file=t.filename,
            function=t.name,line=t.lineno) for t in traceback.extract_tb(error.__traceback__)])
        raise
    finally:
        _write_scale_checkpoint('cleanup','start',_defer_cleanup_error=True)
        for name,value in [('application',app),('source_repository',source_repo)]:
            if value is not None:
                try:await value.close()
                except BaseException as error:
                    facts['close_errors'].append(dict(owner=name,type=type(error).__name__,message=str(error)))
        sys.setprofile(previous);loop.set_exception_handler(old_handler)
        await asyncio.sleep(0)
        facts['source_closed']=source_repo is not None and source_repo.closed
        facts['destination_closed']=app is not None and app.repository.closed
        facts['kernel_closed']=app is not None and app.kernel.closed
"""
        "        facts['kernel_tasks']=[] if app is None else [dict(id="
        "k,done=t.done(),cancelled=t.cancelled())"
        r"""
            for k,t in app.kernel.tasks.items()]
        facts['inflight']=None if app is None else len(app.kernel._inflight)
        facts['writer_fd']=None if app is None else app.repository._writer_fd
        facts['pending']=[repr(t) for t in asyncio.all_tasks() if t not in before and not t.done()]
        facts['fd_after']=len(os.listdir('/proc/self/fd'))
        facts['threads_after']=[(t.ident,t.name) for t in threading.enumerate()]
        facts['source_hash_after']=hashlib.sha256(backup.read_bytes()).hexdigest()
        safe=(facts['source_closed'] and facts['destination_closed'] and facts['kernel_closed']
            and facts['inflight']==0 and facts['writer_fd'] is None
            and not facts['close_errors'] and not facts['pending'])
        if safe and destination.exists():
            fd=os.open(str(destination)+'.lock',os.O_RDWR|os.O_NOFOLLOW)
            try:
                fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB);facts['writer_lock_released']=True
                fcntl.flock(fd,fcntl.LOCK_UN)
            finally:os.close(fd)
            try:
                with closing(sqlite3.connect(destination.as_uri()+'?mode=ro',uri=True)) as db:
                    db.execute('PRAGMA query_only=ON')
"""
        "                    facts['Task_rows']=[json.loads(row[0]) for"
        " row in db.execute('SELECT body FROM eval_tasks')]"
        r"""
"""
        "                    facts['window_count']=db.execute('SELECT c"
        "ount(*) FROM eval_schedule_windows').fetchone()[0]"
        r"""
                    path=root/'header12-destination-backup.sqlite'
                    with closing(sqlite3.connect(path)) as out:db.backup(out)
                facts['backup']=dict(path=str(path),sha256=hashlib.sha256(path.read_bytes()).hexdigest())
            except BaseException as error:
                facts['audit_errors'].append(dict(type=type(error).__name__,message=str(error)))
        for name,listener in listeners:event.remove(Pool,name,listener)
        event.remove(Pool,'checkout',checkout);event.remove(Pool,'connect',connected)
        (root/'header12-consumer.json').write_text(json.dumps(facts,indent=2))
        _write_scale_checkpoint('consumer','done',_defer_cleanup_error=True)
        assert facts['source_hash_after']==facts['source_hash_before']
        assert safe and facts['writer_lock_released'] and not facts['audit_errors']
"""
        "        assert not facts['pending'] and not facts['unexpected'"
        "] and facts['fd_after']<=facts['fd_before']"
        r"""
        counts=collections.Counter(row['event'] for row in facts['connections'])
"""
        "        assert counts['connect']==counts['close'] and counts['"
        "checkout']==counts['reset']==counts['checkin']"
        r"""
        assert facts['source_query_only'] and all(value==1 for value in facts['source_query_only'])
        if getattr(_write_scale_checkpoint,'evidence_errors',[]):
            raise RuntimeError('checkpoint_publication_failed')
facts['installed_identity']=dict(module=sys.modules[Repository.__module__].__file__,path=sys.path,cwd=str(Path.cwd()),executable=sys.executable)
assert Path(facts['installed_identity']['module']).is_relative_to(source)
assert all('/source' not in x for x in sys.path)
asyncio.run(main())
"""
    )
    compile(driver, "installed-scale-consumer", "exec")
    argv = [
        str(root / "installed/bin/python"),
        "-I",
        "-B",
        "-c",
        driver,
        str(site),
        str(destination),
        json.dumps(selected),
        mode,
    ]
    returned = None
    primary = None
    try:
        returned = await _run_scale_evidence_process(
            argv,
            destination,
            execution_timeout_seconds=450 if mode == "rescore" else 330,
            cwd=root / "outside",
            source_database=selected["backup"]["path"],
            destination_database=str(destination / "header12-destination.sqlite"),
        )
    except BaseException as error:
        primary = error
        raise
    finally:
        secondary = []

        def regular_bytes(path):
            import stat

            actual = path.lstat()
            if not stat.S_ISREG(actual.st_mode) or actual.st_nlink != 1:
                raise RuntimeError("installed_evidence_not_regular")
            return path.read_bytes()

        async def capture(stage, action):
            try:
                return await asyncio.to_thread(action)
            except Exception as error:
                secondary.append(
                    {"stage": stage, "type": type(error).__name__, "message": str(error)}
                )
                return None

        process_path = destination / "header12-process.json"
        process = await capture(
            "process_read_parse", lambda: json.loads(regular_bytes(process_path))
        )
        memory_process = (
            None if primary is None else getattr(primary, "scale_evidence_process", None)
        )
        stdout = await capture(
            "stdout_read", lambda: regular_bytes(destination / "header12-stdout.txt")
        )
        stderr = await capture(
            "stderr_read", lambda: regular_bytes(destination / "header12-stderr.txt")
        )
        record = {
            "argv": argv,
            "cwd": str(root / "outside"),
            "process_receipt": str(process_path),
            "process_from_file": process,
            "helper_exception_memory_only": memory_process,
            "primary_error": None
            if primary is None
            else {
                "type": type(primary).__name__,
                "message": str(primary),
                "timeout": isinstance(primary, (TimeoutError, subprocess.TimeoutExpired)),
            },
            "driver_sha256": hashlib.sha256(driver.encode()).hexdigest(),
            "consumer": str(destination / "header12-consumer.json"),
            "stdout_present": stdout is not None,
            "stderr_present": stderr is not None,
            "secondary_errors": secondary,
        }
        if stdout is not None:
            await capture(
                "stdout_save", lambda: (tmp_path / "installed-stdout.txt").write_bytes(stdout)
            )
        if stderr is not None:
            await capture(
                "stderr_save", lambda: (tmp_path / "installed-stderr.txt").write_bytes(stderr)
            )
        await capture(
            "record_save",
            lambda: (tmp_path / "installed-command.json").write_text(json.dumps(record, indent=2)),
        )
        if secondary:
            detail = "installed_secondary_evidence_errors:" + json.dumps(secondary)
            if primary is not None:
                primary.add_note(detail)
            else:
                raise RuntimeError(detail)
    assert returned is not None and stdout is not None and stderr is not None
    child, actual_process, facts = returned
    assert child.returncode == 0, stdout.decode() + stderr.decode()
    assert actual_process["cwd"] == str(root / "outside")
    assert actual_process["driver_sha256"] == hashlib.sha256(driver.encode()).hexdigest()
    assert (
        actual_process["pidfd_close_verified"]
        and actual_process["stdout_eof"]
        and actual_process["stderr_eof"]
    )
    assert not actual_process["signal_operations"] and actual_process["cleanup_status"] == "closed"
    assert Path(facts["installed_identity"]["module"]).is_relative_to(site)
    assert facts["installed_identity"]["cwd"] == str(root / "outside")
    assert facts["stage"] == "completed"


async def test_installed_scale_cancel_join(tmp_path):
    import asyncio

    from test_eval_scale import _run_scale_evidence_process

    root = Path(os.environ["KRIM_PN3_ROOT"])
    site = root / "installed/lib/python3.12/site-packages"
    destination = root / "outside" / Path(os.environ["KRIM_PN3_EVIDENCE"]).name / tmp_path.name
    await asyncio.to_thread(destination.mkdir)
    driver = (
        r"""import asyncio,fcntl,hashlib,json,os,sqlite3,stat,sys,time
from pathlib import Path
from contextlib import closing
source,root=map(Path,sys.argv[1:3]);kind='repeat_cancel'
def _write_scale_checkpoint(stage, status, *, _defer_cleanup_error=False, **known):
    active = sys.exception()
    try:
        identity = getattr(_write_scale_checkpoint, 'identity', None)
        if identity is None:
            path = root / 'header12-process.json'
            st = path.lstat()
            if not stat.S_ISREG(st.st_mode) or st.st_nlink != 1:
                raise RuntimeError('checkpoint_identity_not_regular')
            process = json.loads(path.read_text())
"""
        "            identity = {key: process[key] for key in ['run_ide"
        "ntity', 'driver_sha256', 'helper_ast_sha256', 'checkpoint_ast_"
        "sha256']}"
        r"""
            _write_scale_checkpoint.identity = identity
        sequence = getattr(_write_scale_checkpoint, 'sequence', 0) + 1
        if sequence > 128:
            raise RuntimeError('checkpoint_record_limit')
        fields = {}
        for key, value in known.items():
"""
        "            if not isinstance(key, str) or len(key) > 64 or (v"
        "alue is not None and type(value) not in (str, int, float, bool"
        ")):"
        r"""
                raise RuntimeError('checkpoint_non_scalar')
            fields[key] = value[:512] if isinstance(value, str) else value
"""
        "        row = dict(identity, sequence=sequence, stage=stage, s"
        "tatus=status, monotonic_ns=time.monotonic_ns(), time_ns=time.t"
        "ime_ns(), pid=os.getpid(), uid=os.getuid(), known=fields)"
        r"""
        raw = (json.dumps(row, ensure_ascii=False, allow_nan=False) + '\n').encode()
        if len(raw) > 4096:
            raise RuntimeError('checkpoint_record_bytes')
"""
        "        fd = os.open(root / 'header12-checkpoints.jsonl', os.O"
        "_WRONLY | os.O_CREAT | os.O_APPEND | os.O_NOFOLLOW, 384)"
        r"""
        try:
            st = os.fstat(fd)
            if not stat.S_ISREG(st.st_mode) or st.st_nlink != 1 or st.st_uid != os.getuid():
                raise RuntimeError('checkpoint_output_not_regular')
            if st.st_size + len(raw) > 128 * 4096:
                raise RuntimeError('checkpoint_total_bytes')
            view = memoryview(raw)
            while view:
                count = os.write(fd, view)
                if count <= 0:
                    raise RuntimeError('checkpoint_short_write')
                view = view[count:]
            os.fsync(fd)
        finally:
            os.close(fd)
        _write_scale_checkpoint.sequence = sequence
    except BaseException as evidence_error:
        errors = getattr(_write_scale_checkpoint, 'evidence_errors', None)
        if errors is None:
            errors = []
            _write_scale_checkpoint.evidence_errors = errors
        if len(errors) < 128:
"""
        "            errors.append(dict(stage=stage, status=status, typ"
        "e=type(evidence_error).__name__, message=str(evidence_error)[:"
        "512], pid=os.getpid(), active_type=None if active is None else"
        " type(active).__name__, deferred=_defer_cleanup_error, monoton"
        "ic_ns=time.monotonic_ns()))"
        r"""
        else:
"""
        "            _write_scale_checkpoint.error_overflow = getattr(_"
        "write_scale_checkpoint, 'error_overflow', 0) + 1"
        r"""
        if _defer_cleanup_error:
            if isinstance(globals().get('facts'), dict):
                facts['checkpoint_errors'] = list(errors)
            return
        if active is not None:
            raise active from evidence_error
        raise
_write_scale_checkpoint('bootstrap','start')
assert str(Path.cwd())==str(root.parent.parent)
from importlib.resources import files
from sqlalchemy import event
from sqlalchemy.pool import Pool
from krim_agent.contracts import ServiceError
from krim_agent.evaluation.app import open_eval
from krim_agent.evaluation.domain import Budget,DatasetSpec,TaskSpec
from krim_agent.evaluation.adapters.local_process import LocalProcessAdapter
from krim_agent.evaluation.metrics import MetricRegistry
facts=dict(kind=kind,uid=os.getuid(),database=str(root/'lifecycle.sqlite'),
    barriers=[],physical=[],retired=[],connections=[],error=None,close_errors=[])
for name in ['connect','checkout','reset','checkin','close']:
"""
        "    def record(*args,name=name):facts['connections'].append(di"
        "ct(event=name,connection=id(args[0]),at=time.monotonic()))"
        r"""
    event.listen(Pool,name,record)
ready=release=None
async def barrier(stage):
    facts['barriers'].append(stage);ready.set();await release.wait()
prepare=LocalProcessAdapter.prepare;execute=LocalProcessAdapter.execute;collect=LocalProcessAdapter.collect
score=MetricRegistry.score;retire=LocalProcessAdapter.retire
def physical(job):
    return dict(ref=job.ref.id,pid=job.process.pid,returncode=job.process.returncode,
        pidfd=job.pidfd,collector_done=job.collector.done(),stdout_eof=job.stdout_eof,
        stderr_eof=job.stderr_eof,streams_done=all(t.done() for t in job.stream_tasks),
        cleanup=job.cleanup_receipts)
async def prepared(adapter,trial):
    value=await prepare(adapter,trial)
    if kind=='before_intent':await barrier('after_real_prepare_before_intent')
    return value
async def executed(adapter,value):
    ref=await execute(adapter,value)
    facts['actual_ref']=ref.id
    if kind=='admission_before_ref':await barrier('actual_spawn_before_ref_return')
    return ref
async def collected(adapter,ref):
"""
        "    if kind in {'collect','close','repeat_cancel','timeout'}:a"
        "wait barrier('actual_ref_before_collect')"
        r"""
    result=await collect(adapter,ref)
    if kind=='worker_error':
        facts['actual_observation']=result;raise OSError('controlled_worker_result_IO_failure')
    return result
async def scored(registry,rule,case,output):
    value=await score(registry,rule,case,output)
    if kind=='scorer':await barrier('actual_score_before_return')
    return value
def retired(adapter,ref,receipt):
    facts['retired'].append(physical(adapter.jobs[ref.id]))
    return retire(adapter,ref,receipt)
LocalProcessAdapter.prepare=prepared;LocalProcessAdapter.execute=executed
LocalProcessAdapter.collect=collected;LocalProcessAdapter.retire=retired;MetricRegistry.score=scored
async def main():
    global ready,release
    ready=asyncio.Event();release=asyncio.Event();app=None;before=asyncio.all_tasks()
    facts['fd_before']=len(os.listdir('/proc/self/fd'))
    try:
        app=await open_eval(root/'lifecycle.sqlite')
        body=json.loads(files('krim_agent.evaluation.examples').joinpath('scale_reconciliation.v1.json').read_text())
        data=DatasetSpec.model_validate(body|{'cases':body['cases'][:1]})
        await app.repository.freeze(data)
        original_attach=app.repository.attach
        async def attached(*args,**kwargs):
            result=await original_attach(*args,**kwargs)
            if kind=='after_ref':await barrier('after_actual_ref_attach')
            return result
        app.repository.attach=attached
        original_write=app.repository._write;original_snapshot=app.repository._snapshot;lost=False
        async def snapshot(*args,**kwargs):
            if kind=='cleanup_unknown' and lost:raise OSError('controlled_confirmation_unavailable')
            return await original_snapshot(*args,**kwargs)
        async def written(operation):
            nonlocal lost
            value=await original_write(operation)
"""
        "            if kind in {'commit_return_lost','cleanup_unknown'"
        "} and 'commit_with_receipt' in operation.__qualname__ and not "
        "lost:"
        r"""
                lost=True;facts['actual_commit_before_lost_return']=True
                raise OSError('controlled_return_lost_after_commit')
            return value
        app.repository._write=written;app.repository._snapshot=snapshot
        request=TaskSpec(request_key='lifecycle-'+kind,dataset=data.ref(),
            metrics=app.repository.metric_registry.specs(),execution=Budget(concurrency=2,
            item_timeout_seconds=1 if kind=='timeout' else 20))
        view=await app.run(request);facts['task_id']=view['id']
        if kind not in {'worker_error','commit_return_lost','cleanup_unknown'}:
            async with asyncio.timeout(3):await ready.wait()
            if kind=='admission_before_ref':
                callers=[w for w in app.kernel._inflight.values() if w.admission is not None]
                assert callers and any(not w.admission.done() for w in callers)
            if kind=='close':
                close_task=asyncio.create_task(app.close())
                async with asyncio.timeout(3):
                    while not app.kernel.tasks[view['id']].cancelling():await asyncio.sleep(.001)
                release.set();await close_task
                state=None
            elif kind=='timeout':
                state=await app.kernel.wait(view['id']);release.set()
            else:
                cancelling=asyncio.create_task(app.kernel.cancel(view['id']))
                async with asyncio.timeout(3):
                    while not app.kernel.tasks[view['id']].cancelling():await asyncio.sleep(.001)
                release.set()
                state=await cancelling
                if kind=='repeat_cancel':
                    assert (await app.kernel.cancel(view['id']))['id']==view['id']
                    assert (await app.kernel.cancel(view['id']))['id']==view['id']
        else:
            try:state=await app.kernel.wait(view['id'])
            except ServiceError as error:
                assert kind=='cleanup_unknown' and error.code=='storage_unavailable'
                facts['durable_cleanup']='unknown';facts['error']=error.code;state=None
                assert not facts['retired'] and len(app.adapter.jobs)==1
        facts['state']=state
        if state is not None:
"""
        "            if kind=='commit_return_lost':assert state['status"
        "']=='completed' and lost and len(facts['retired'])==1"
        r"""
            elif kind=='timeout':
                assert state['status']=='completed'
                row=await app.repository.read_trial('local',view['id'],data.cases[0].case_id,0)
                assert row['quality']=='timed_out' and row['code']=='run_timeout'
"""
        "            elif kind=='worker_error':assert state['status']=="
        "'failed' and state['code']=='execution_error'"
        r"""
            else:assert state['status']=='aborted' and state['code']=='cancelled'
            assert (await app.run(request))['id']==view['id']
        assert app.owner.spawn_calls==(0 if kind=='before_intent' else 1)
        facts['spawn_calls']=app.owner.spawn_calls;facts['metric_calls']=app.repository.metric_registry.calls
        assert all(t.done() for t in app.kernel.tasks.values()) and not app.kernel._inflight
    finally:
        release.set()
        if app is not None:
            try:await app.close()
"""
        "            except BaseException as error:facts['close_errors'"
        "].append(dict(type=type(error).__name__,code=getattr(error,'co"
        "de',None)))"
        r"""
            facts.update(repository_closed=app.repository.closed,writer_fd=app.repository._writer_fd,
                owner_closed=app.owner.closed,kernel_closed=app.kernel.closed,
                repository_resources=app.repository.resources)
            facts['physical']=[physical(job) for job in app.owner.jobs.values()]
            assert app.repository.closed and app.repository._writer_fd is None and app.owner.closed
            assert all(r['returncode'] is not None and r['pidfd']==-1 and r['collector_done']
"""
        "                and r['stdout_eof'] and r['stderr_eof'] and r["
        "'streams_done'] for r in facts['physical'])"
        r"""
            assert not facts['close_errors']
"""
        "        facts['pending']=[repr(t) for t in asyncio.all_tasks()"
        " if t is not asyncio.current_task() and t not in before and no"
        "t t.done()]"
        r"""
        facts['fd_after']=len(os.listdir('/proc/self/fd'))
        assert not facts['pending'] and facts['fd_after']<=facts['fd_before']
        # 持久确认unknown的库只保留原件与hash；绝不SQL-open/recover/backup。
        if facts.get('durable_cleanup')!='unknown':
"""
        "            with closing(sqlite3.connect((root/'lifecycle.sqli"
        "te').as_uri()+'?mode=ro',uri=True)) as db:"
        r"""
                db.execute('PRAGMA query_only=ON')
"""
        "                with closing(sqlite3.connect(root/'lifecycle-b"
        "ackup.sqlite')) as dest:db.backup(dest)"
        r"""
            facts['backup']=str(root/'lifecycle-backup.sqlite')
        facts['database_sha256']=hashlib.sha256((root/'lifecycle.sqlite').read_bytes()).hexdigest()
        (root/'header12-consumer.json').write_text(json.dumps(facts,indent=2))
facts['installed_identity']=dict(module=sys.modules[open_eval.__module__].__file__,path=sys.path,cwd=str(Path.cwd()),executable=sys.executable)
assert Path(facts['installed_identity']['module']).is_relative_to(source)
assert all('/source' not in x for x in sys.path)
asyncio.run(main())
"""
    )
    compile(driver, "installed-scale-consumer", "exec")
    argv = [
        str(root / "installed/bin/python"),
        "-I",
        "-B",
        "-c",
        driver,
        str(site),
        str(destination),
    ]
    returned = None
    primary = None
    try:
        returned = await _run_scale_evidence_process(
            argv,
            destination,
            execution_timeout_seconds=60,
            cwd=root / "outside",
            source_database=None,
            destination_database=str(destination / "lifecycle.sqlite"),
        )
    except BaseException as error:
        primary = error
        raise
    finally:
        secondary = []

        def regular_bytes(path):
            import stat

            actual = path.lstat()
            if not stat.S_ISREG(actual.st_mode) or actual.st_nlink != 1:
                raise RuntimeError("installed_evidence_not_regular")
            return path.read_bytes()

        async def capture(stage, action):
            try:
                return await asyncio.to_thread(action)
            except Exception as error:
                secondary.append(
                    {"stage": stage, "type": type(error).__name__, "message": str(error)}
                )
                return None

        process_path = destination / "header12-process.json"
        process = await capture(
            "process_read_parse", lambda: json.loads(regular_bytes(process_path))
        )
        memory_process = (
            None if primary is None else getattr(primary, "scale_evidence_process", None)
        )
        stdout = await capture(
            "stdout_read", lambda: regular_bytes(destination / "header12-stdout.txt")
        )
        stderr = await capture(
            "stderr_read", lambda: regular_bytes(destination / "header12-stderr.txt")
        )
        record = {
            "argv": argv,
            "cwd": str(root / "outside"),
            "process_receipt": str(process_path),
            "process_from_file": process,
            "helper_exception_memory_only": memory_process,
            "primary_error": None
            if primary is None
            else {
                "type": type(primary).__name__,
                "message": str(primary),
                "timeout": isinstance(primary, (TimeoutError, subprocess.TimeoutExpired)),
            },
            "driver_sha256": hashlib.sha256(driver.encode()).hexdigest(),
            "consumer": str(destination / "header12-consumer.json"),
            "stdout_present": stdout is not None,
            "stderr_present": stderr is not None,
            "secondary_errors": secondary,
        }
        if stdout is not None:
            await capture(
                "stdout_save", lambda: (tmp_path / "installed-stdout.txt").write_bytes(stdout)
            )
        if stderr is not None:
            await capture(
                "stderr_save", lambda: (tmp_path / "installed-stderr.txt").write_bytes(stderr)
            )
        await capture(
            "record_save",
            lambda: (tmp_path / "installed-command.json").write_text(json.dumps(record, indent=2)),
        )
        if secondary:
            detail = "installed_secondary_evidence_errors:" + json.dumps(secondary)
            if primary is not None:
                primary.add_note(detail)
            else:
                raise RuntimeError(detail)
    assert returned is not None and stdout is not None and stderr is not None
    child, actual_process, facts = returned
    assert child.returncode == 0, stdout.decode() + stderr.decode()
    assert actual_process["cwd"] == str(root / "outside")
    assert actual_process["driver_sha256"] == hashlib.sha256(driver.encode()).hexdigest()
    assert (
        actual_process["pidfd_close_verified"]
        and actual_process["stdout_eof"]
        and actual_process["stderr_eof"]
    )
    assert not actual_process["signal_operations"] and actual_process["cleanup_status"] == "closed"
    assert Path(facts["installed_identity"]["module"]).is_relative_to(site)
    assert facts["installed_identity"]["cwd"] == str(root / "outside")
    assert facts["owner_closed"] and facts["repository_closed"] and not facts["pending"]
