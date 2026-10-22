"""当前99项wheel、项目外安装与入口身份；不重跑安装或业务例子。"""

import asyncio
import base64
import csv
import hashlib
import io
import json
import os
import subprocess
import zipfile
from pathlib import Path

import pytest


def test_current_wheel_installed_entry_and_cold_queries(tmp_path):
    root = Path(os.environ["KRIM_SAFE_EVAL_ROOT"])
    source = Path(__file__).resolve().parents[2]
    import tomllib

    package_files = tomllib.loads((source / "pyproject.toml").read_text())["tool"]["hatch"][
        "build"
    ]["targets"]["wheel"]["only-include"]
    assert len(package_files) == 99 and len(set(package_files)) == 99
    wheel = root / "dist/krim_agent-0.4.0a1-py3-none-any.whl"
    site = root / "installed/lib/python3.12/site-packages"
    with zipfile.ZipFile(wheel) as bundle:
        shipped = {name for name in bundle.namelist() if name.startswith("krim_agent/")}
        assert shipped == {name.removeprefix("src/") for name in package_files}
        record = bundle.read("krim_agent-0.4.0a1.dist-info/RECORD").decode()
        records = {row[0]: row[1:] for row in csv.reader(io.StringIO(record))}
        identities = []
        for filename in package_files:
            relative = filename.removeprefix("src/")
            data = (source / filename).read_bytes()
            assert bundle.read(relative) == data == (site / relative).read_bytes()
            expected = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).decode().rstrip("=")
            assert records[relative] == ["sha256=" + expected, str(len(data))]
            identities.append({"path": relative, "sha256": hashlib.sha256(data).hexdigest()})
        metadata = bundle.read("krim_agent-0.4.0a1.dist-info/METADATA").decode()
        assert "Version: 0.4.0a1\n" in metadata
        assert (
            "krim-eval = krim_agent.evaluation.cli:main"
            in bundle.read("krim_agent-0.4.0a1.dist-info/entry_points.txt").decode()
        )
        assert (site / "krim_agent-0.4.0a1.dist-info/METADATA").read_text() == metadata
    probe = (
        "import sys,json,importlib.metadata as m,importlib.resources as r"
        ";import krim_agent.evaluation.cli as c;print(json.dumps({'prefix"
        "':sys.prefix,'base_prefix':sys.base_prefix,'path':sys.path,'modu"
        "le':c.__file__,'versions':[d.version for d in m.distributions() "
        "if d.metadata['Name']=='krim-agent'],'resource':str(r.files('kri"
        "m_agent.evaluation.examples').joinpath('reconciliation.v1.json')"
        "),'forbidden':[k for k in sys.modules if any(k==p or k.startswit"
        "h(p+'.') for p in ['krim_agent.composition','krim_agent.runtime'"
        ",'krim_agent.execution','krim_agent.workspace','krim_agent.provi"
        "ders'])]}))"
    )
    process = subprocess.run(
        [str(root / "installed/bin/python"), "-I", "-B", "-c", probe],
        cwd=root / "outside",
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert process.returncode == 0, process.stderr
    identity = json.loads(process.stdout)
    assert identity["prefix"] == str(root / "installed")
    assert identity["base_prefix"] == str(root / "materials/python")
    assert Path(identity["module"]).is_relative_to(site)
    assert Path(identity["resource"]).is_relative_to(site)
    assert identity["versions"] == ["0.4.0a1"] and identity["forbidden"] == []
    assert all(str(source) not in item for item in identity["path"])
    command = [
        str(root / "installed/bin/krim-eval"),
        "--root",
        str(root),
        "task",
        "by-key",
        "example-01",
    ]
    cold = subprocess.run(
        command,
        cwd=root / "outside",
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert cold.returncode == 0, cold.stderr
    result = json.loads(cold.stdout)
    assert result["status"] == "completed" and len(result["items"]) == 3
    assert all(item["quality"] == "passed" for item in result["items"])
    assert result["items"][2]["output"]["exit_code"] == 2
    (tmp_path / "installed-identity.json").write_text(
        json.dumps(
            {
                "identity": identity,
                "wheel_sha256": hashlib.sha256(wheel.read_bytes()).hexdigest(),
                "files": identities,
                "cold_argv": command,
                "cold_result_id": result["id"],
            },
            indent=2,
        )
    )


@pytest.mark.parametrize("kind", ["large_snapshot", "timeout_receipt"])
async def test_installed_repair_queries_preserve_capacity_and_known_timeout(tmp_path, kind):
    from test_neutral_eval import pn1_repair_worker, requested, resources

    from krim_agent.evaluation.app import open_eval
    from krim_agent.evaluation.domain import Budget

    root = Path(os.environ["KRIM_SAFE_EVAL_ROOT"])
    database = tmp_path / "eval.sqlite"
    app = await open_eval(database)
    try:
        pn1_repair_worker(
            app, tmp_path, live=kind == "timeout_receipt", large=kind == "large_snapshot"
        )
        request = await requested(
            app, execution=Budget(item_timeout_seconds=1 if kind == "timeout_receipt" else 20)
        )
        view = await app.run(request)
        await app.kernel.wait(view["id"])
        report = await app.repository.report(view["id"])
        assert app.adapter.execute_calls == 1
        assert report["items"][0]["quality"] == (
            "timed_out" if kind == "timeout_receipt" else "passed"
        )
    finally:
        await app.close()
    resources(app, tmp_path, "installed-repair")
    code = """import asyncio,json,sys
from pathlib import Path
import krim_agent.evaluation.app as module
from krim_agent.evaluation.reports import gate_reports
assert Path(module.__file__).is_relative_to(Path(sys.argv[3]))
async def main():
    async with await module.open_eval(Path(sys.argv[1]),readonly=True) as app:
        snapshot=await app.repository.snapshot(sys.argv[2])
        report=await app.repository.report(sys.argv[2])
        found=await app.repository.by_key(snapshot['request_json'])
        assert found['id']==snapshot['id']
        assert app.adapter is None and app.repository.metric_registry.calls==0
        item=report['items'][0]
        assert item['execution_ref'] is not None and item['output'] is not None
        if sys.argv[4]=='large_snapshot':
            assert len(json.dumps(snapshot).encode())>1048576
            assert len(item['output']['artifacts']['blob.txt']['text'])==1048000
            assert gate_reports(report,report)['status']=='pass'
        else:
            assert item['quality']=='timed_out' and item['output']['exit_code']==-9
            assert item['output']['artifacts']['ledger.json']['sha256']
            assert report['summary']['skipped']==report['summary']['planned_cells']==2
        print(json.dumps({'id':snapshot['id'],'report_hash':report['report_hash'],
                          'execution_ref':item['execution_ref'],'module':module.__file__}))
asyncio.run(main())
"""
    argv = [
        str(root / "installed/bin/python"),
        "-I",
        "-B",
        "-c",
        code,
        str(database),
        view["id"],
        str(root / "installed/lib/python3.12/site-packages"),
        kind,
    ]
    process = await asyncio.to_thread(
        subprocess.run, argv, cwd=root / "outside", capture_output=True, text=True, timeout=15
    )
    assert process.returncode == 0, process.stderr
    result = json.loads(process.stdout)
    assert result["id"] == view["id"] and result["report_hash"] == report["report_hash"]
    assert result["execution_ref"] == report["items"][0]["execution_ref"]
    (tmp_path / "installed-repair-query.json").write_text(
        json.dumps(
            {
                "argv": argv,
                "result": result,
                "rc": process.returncode,
                "source_execute_calls": app.adapter.execute_calls,
            },
            indent=2,
        )
    )
