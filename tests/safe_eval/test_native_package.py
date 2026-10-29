"""本轮99项包身份与项目外installed真实fixture消费者；不运行真实CLI。"""

import base64
import csv
import hashlib
import io
import json
import os
import sqlite3
import subprocess
import tomllib
import zipfile
from pathlib import Path


def test_native_package_identity_and_cold_read(tmp_path):
    root = Path(os.environ["KRIM_PN2_ROOT"])
    source = Path(__file__).resolve().parents[2]
    shipped = tomllib.loads((source / "pyproject.toml").read_text())["tool"]["hatch"]["build"][
        "targets"
    ]["wheel"]["only-include"]
    assert len(shipped) == len(set(shipped)) == 99
    site = root / "installed/lib/python3.12/site-packages"
    wheel = root / "dist/krim_agent-0.4.0a1-py3-none-any.whl"
    with zipfile.ZipFile(wheel) as bundle:
        assert {n for n in bundle.namelist() if n.startswith("krim_agent/")} == {
            n.removeprefix("src/") for n in shipped
        }
        record = {
            r[0]: r[1:]
            for r in csv.reader(
                io.StringIO(bundle.read("krim_agent-0.4.0a1.dist-info/RECORD").decode())
            )
        }
        for name in shipped:
            path = name.removeprefix("src/")
            data = (source / name).read_bytes()
            assert data == bundle.read(path) == (site / path).read_bytes()
            encoded = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).decode().rstrip("=")
            assert record[path] == ["sha256=" + encoded, str(len(data))]
    code = (
        "import json,sys,importlib.metadata as m\n"
        "import krim_agent.evaluation.cli as c\n"
        "import krim_agent.evaluation.adapters.codex_cli as x\n"
        "import krim_agent.evaluation.adapters.claude_code as y\n"
        "from importlib.resources import files\n"
        "print(json.dumps({'modules':[c.__file__,x.__file__,y.__file__], 'path':s"
        "ys.path,\n"
        " 'version':m.version('krim-agent'),'resource':str(files('krim_agent.eval"
        "uation.examples').joinpath('native_repair.v1.json')),\n"
        " 'forbidden':[n for n in sys.modules if any(n==p or n.startswith(p+'.') "
        "for p in ['krim_agent.composition','krim_agent.runtime','krim_agent.prov"
        "iders','krim_agent.workspace','krim_agent.execution'])]}))\n"
    )
    command = [str(root / "installed/bin/python"), "-I", "-B", "-c", code]
    result = subprocess.run(
        command,
        cwd=root / "outside",
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 0, result.stderr
    identity = json.loads(result.stdout)
    assert identity["version"] == "0.4.0a1" and identity["forbidden"] == []
    assert all(Path(n).is_relative_to(site) for n in identity["modules"])
    assert Path(identity["resource"]).is_relative_to(site)
    assert all(str(source) not in n for n in identity["path"])
    command = [
        str(root / "installed/bin/krim-eval"),
        "--root",
        str(tmp_path / "cold-targets"),
        "targets",
    ]
    cold = subprocess.run(command, cwd=root / "outside", capture_output=True, text=True, timeout=10)
    assert cold.returncode == 0, cold.stderr
    targets = json.loads(cold.stdout)
    assert {t["key"] for t in targets["targets"]} == {
        "reconcile-reference",
        "codex-cli",
        "claude-code",
        "krim-script-v2",
    }
    assert not (tmp_path / "cold-targets").exists()
    (tmp_path / "package-identity.json").write_text(
        json.dumps(
            {
                "wheel_sha256": hashlib.sha256(wheel.read_bytes()).hexdigest(),
                "identity": identity,
                "argv": command,
                "cold_targets": targets,
            },
            indent=2,
        )
    )


INSTALLED_FIXTURE = (
    "\n"
    "import asyncio,hashlib,json,os,sys\n"
    "from pathlib import Path\n"
    "from importlib.resources import files\n"
    "import krim_agent.evaluation.app as module\n"
    "from krim_agent.evaluation.adapters.profiles import CampaignBootstrap,Na"
    "tiveProfile,slot_for,file_digest,program_materials\n"
    "from krim_agent.evaluation.app import open_eval\n"
    "from krim_agent.evaluation.domain import DatasetSpec,TaskSpec\n"
    "from krim_agent.evaluation.metrics import MetricRegistry\n"
    "from krim_agent.evaluation.kernel import EvalKernel\n"
    "from krim_agent.evaluation.verifiers import NativeVerifier\n"
    "from krim_agent.contracts import fingerprint\n"
    "async def main():\n"
    "    root=Path(sys.argv[1]); worker=Path(sys.argv[2]); env={k:os.environ["
    "k] for k in ['PATH','LANG','LD_LIBRARY_PATH','PYTHONDONTWRITEBYTECODE'] "
    "if k in os.environ}\n"
    "    dataset=DatasetSpec.model_validate_json(files('krim_agent.evaluation"
    ".examples').joinpath('native_repair.v1.json').read_bytes())\n"
    "    request=TaskSpec(dataset=dataset.ref(),request_key='installed-native"
    "-fixture',target_key='fixture-codex',metrics=MetricRegistry().specs('nat"
    "ive'))\n"
    "    boot=CampaignBootstrap(root,{request.request_key:slot_for(request,da"
    "taset)})\n"
    "    seeded=await open_eval(root/'eval.sqlite',campaign_bootstrap=boot)\n"
    "    binding=seeded.campaign_binding\n"
    "    await seeded.close()\n"
    "    subject={'family':'fixture-codex','cli_version':'fixture-worker/1','"
    "model':'none','effort':'none','service_speed':'standard','tier':'not_app"
    "licable','provider_protocol':'fixture-jsonl/1','endpoint_role':'none','a"
    "uth_lease_id':'none','auth_lease_epoch':0,\n"
    "      'executable_chain_digest':file_digest(worker),'runtime_digest':fil"
    "e_digest(Path(sys.executable).resolve()),'adapter_digest':file_digest(Pa"
    "th(module.__file__)),'codec_digest':file_digest(Path(module.__file__)),'"
    "process_owner_digest':file_digest(Path(module.__file__))}\n"
    "    subject.update(program_materials('codex',worker))\n"
    "    subject['subject_config_digest']=fingerprint(subject)\n"
    "    common={'dataset_fingerprint':dataset.ref().fingerprint,'reset_diges"
    "t':fingerprint(dataset.model_dump(mode='json')),'verifier_digest':Native"
    "Verifier().digest,\n"
    "      'metric_fingerprint':fingerprint([m.model_dump(mode='json') for m "
    "in request.metrics]),'budget':request.execution.model_dump(mode='json'),"
    "\n"
    "      'ordinary_isolation_digest':hashlib.sha256(b'trusted-offline-insta"
    "lled-fixture').hexdigest(),'public_environment':env,'path_template_versi"
    "on':'owned-trial/1','allowed_subject_fingerprints':[fingerprint(subject)"
    "]}\n"
    "    profile=NativeProfile('fixture-codex','codex',worker,env,common,subj"
    "ect,binding)\n"
    "    app=await open_eval(root/'eval.sqlite',native_profiles=[profile])\n"
    "    try:\n"
    "        assert type(app.kernel) is EvalKernel\n"
    "        await app.repository.freeze(dataset)\n"
    "        view=await app.run(request)\n"
    "        result=await app.kernel.wait(view['id'])\n"
    "        report=await app.repository.report(view['id'])\n"
    "        assert result['items'][0]['quality']=='passed',result\n"
    "        assert (await app.run(request))['id']==view['id']\n"
    "        adapter=app.repository.registry['fixture-codex']\n"
    "        assert adapter.execute_calls==1\n"
    "        payload={'id':view['id'],'database':str(root/'eval.sqlite'),'rep"
    "ort_hash':report['report_hash'],'ref':result['items'][0]['execution_ref'"
    "],'module':module.__file__,'kernel_module':sys.modules[EvalKernel.__modu"
    "le__].__file__,'result':result,\n"
    "          'fixture_executions':adapter.execute_calls,'real_cli_launches'"
    ":0,'model_requests':0}\n"
    "    finally:\n"
    "        await app.close()\n"
    "    payload['resources']={'closed':app.repository.closed,'connections':a"
    "pp.repository.resources,\n"
    "       'processes':[{'pid':j.process.pid,'wait':j.process.returncode,'pi"
    "dfd_closed':j.pidfd==-1,'collector_joined':j.collector.done()} for j in "
    "app.owner.jobs.values()]}\n"
    "    print(json.dumps(payload))\n"
    "asyncio.run(main())\n"
)


def test_installed_native_entry_uses_same_kernel(tmp_path):
    root = Path(os.environ["KRIM_PN2_ROOT"])
    worker = Path(__file__).parent / "fixtures/native_cli_worker.py"
    command = [
        str(root / "installed/bin/python"),
        "-I",
        "-B",
        "-c",
        INSTALLED_FIXTURE,
        str(tmp_path / "installed-campaign"),
        str(worker),
    ]
    result = subprocess.run(
        command,
        cwd=root / "outside",
        capture_output=True,
        text=True,
        timeout=30,
    )
    (tmp_path / "installed-command.json").write_text(
        json.dumps(
            {
                "argv": command,
                "cwd": str(root / "outside"),
                "exit": result.returncode,
                "stdout": result.stdout,
                "stderr": result.stderr,
            },
        ),
    )
    assert result.returncode == 0, result.stderr
    value = json.loads(result.stdout)
    site = root / "installed/lib/python3.12/site-packages"
    assert Path(value["module"]).is_relative_to(site) and Path(
        value["kernel_module"]
    ).is_relative_to(site)
    assert (
        value["fixture_executions"] == 1
        and value["real_cli_launches"] == value["model_requests"] == 0
    )
    assert value["resources"]["closed"]
    assert all(
        j["wait"] is not None and j["pidfd_closed"] and j["collector_joined"]
        for j in value["resources"]["processes"]
    )
    database = Path(value["database"])
    source = sqlite3.connect(database.as_uri() + "?mode=ro", uri=True)
    destination = sqlite3.connect(tmp_path / "readonly-installed-backup.sqlite")
    try:
        source.backup(destination)
        raw = source.execute("SELECT body FROM eval_tasks WHERE id=?", (value["id"],)).fetchone()[0]
        saved = json.loads(raw)
        assert saved["items"][0]["execution_ref"] == value["ref"]
        assert saved["environment"]["native_admission"]["reserved"] is True
    finally:
        source.close()
        destination.close()
    (tmp_path / "installed-result.json").write_text(json.dumps(value, indent=2))


def test_pn2_repair_installed_cleanup_receipts(tmp_path):
    root = Path(os.environ["KRIM_PN2_ROOT"])
    insertion = """
        receipts=[]
        for job in app.owner.jobs.values():
            snapshot=json.loads(json.dumps(job.collector.result()))
            digest=hashlib.sha256(json.dumps(snapshot,sort_keys=True).encode()).hexdigest()
            first=await app.owner.cancel(job,job.lease)
            second=await app.owner.cancel(job,job.lease)
            assert first.closed and second.closed
            assert first.observation==second.observation==snapshot
            current=json.dumps(job.collector.result(),sort_keys=True).encode()
            assert hashlib.sha256(current).hexdigest()==digest
            assert job.cleanup_receipts and job.cleanup_receipts[-1]['closed']
            keys=['collector_joined','stdout_eof','stderr_eof','pidfd_closed','group_empty']
            assert all(job.cleanup_receipts[-1][k] for k in keys)
            receipts.append({'ref':job.ref.id,'pid':job.process.pid,'snapshot_hash':digest,'cleanup_receipts':job.cleanup_receipts})
        payload['cleanup_receipts']=receipts
"""
    anchor = "    finally:\n        await app.close()"
    assert INSTALLED_FIXTURE.count(anchor) == 1
    code = INSTALLED_FIXTURE.replace(anchor, insertion + anchor, 1)
    compile(code, "pn2-reentry02-installed-cleanup.py", "exec")
    worker = Path(__file__).parent / "fixtures/native_cli_worker.py"
    argv = [
        str(root / "installed/bin/python"),
        "-I",
        "-B",
        "-c",
        code,
        str(tmp_path / "installed-repair-campaign"),
        str(worker),
    ]
    process = subprocess.run(argv, cwd=root / "outside", capture_output=True, text=True, timeout=30)
    (tmp_path / "installed-repair-command.json").write_text(
        json.dumps(
            {
                "argv": argv,
                "cwd": str(root / "outside"),
                "rc": process.returncode,
                "stdout": process.stdout,
                "stderr": process.stderr,
            },
            indent=2,
        )
    )
    assert process.returncode == 0, process.stderr
    value = json.loads(process.stdout)
    assert value["resources"]["closed"] and len(value["cleanup_receipts"]) == 4
    assert value["real_cli_launches"] == value["model_requests"] == 0
    assert Path(value["module"]).is_relative_to(root / "installed/lib/python3.12/site-packages")
    (tmp_path / "installed-repair-result.json").write_text(json.dumps(value, indent=2))
