"""107项新包与项目外真实CLI链；源码预检复用完全相同的业务脚本。"""

import base64
import csv
import hashlib
import io
import json
import os
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest
from test_continuous_eval import DATABASES
from test_continuous_eval import continuous_resources as continuous_resources

CLI_ENTRY = r"""import json,os,sys
from pathlib import Path
source=sys.argv.pop(1);receipt=Path(sys.argv.pop(1))
if source!='-':sys.path.insert(0,source)
from sqlalchemy import event
from sqlalchemy.pool import Pool
from krim_agent.evaluation import cli
opened=[];
events=[];
calls={'adapter_lookup':0,
    'adapter_prepare':0,
    'adapter_execute':0,
    'adapter_collect':0,
    'adapter_cancel':0,
    'provider_stream':0,
    'subprocess':0}
for kind in ['connect','checkout','reset','checkin','close']:
    def observe(*args,kind=kind):events.append({'event':kind,'connection':id(args[0])})
    event.listen(Pool,kind,observe)
original=cli.open_eval
async def tracked(*args,**kwargs):
    app=await original(*args,**kwargs);opened.append(app);return app
cli.open_eval=tracked
def profile(frame,kind,arg):
    if kind!='call':return
    module=frame.f_globals.get('__name__','');name=frame.f_code.co_name
    if (module.startswith('krim_agent.evaluation.adapters.')
        and name in ['prepare',
        'execute',
        'collect',
        'cancel']):
        calls['adapter_'+name]+=1
    if module=='krim_agent.evaluation.repository' and name=='adapter':calls['adapter_lookup']+=1
    if module.startswith('krim_agent.providers.') and name=='stream':calls['provider_stream']+=1
    if module=='asyncio.subprocess' and name=='create_subprocess_exec':calls['subprocess']+=1
before=len(os.listdir('/proc/self/fd'));sys.setprofile(profile)
try:cli.main()
finally:
    sys.setprofile(None)
    apps=[]
    for app in opened:
        jobs=[] if app.owner is None else [{'pid':w.process.pid,
            'returncode':w.process.returncode,
            'pidfd':w.pidfd,
            'collector_joined':w.collector is not None
            and w.collector.done(),
            'stdout_eof':w.stdout_eof,
            'stderr_eof':w.stderr_eof,
            'streams_joined':all(t.done()
            for t in w.stream_tasks),
            'cleanup_receipts':w.cleanup_receipts}
            for w in app.owner.jobs.values()]
        apps.append({'closed':app.repository.closed,
            'writer_fd':app.repository._writer_fd,
            'tasks_joined':all(t.done()
            for t in app.kernel.tasks.values()),
            'owner_closed':app.owner is None
            or app.owner.closed,
            'spawn_calls':0
            if app.owner is None else app.owner.spawn_calls,
            'jobs':jobs})
    body={'module':cli.__file__,
        'apps':apps,
        'connections':events,
        'calls':calls,
        'fd_before':before,
        'fd_after':len(os.listdir('/proc/self/fd'))}
    receipt.write_text(json.dumps(body))
    assert (all((a['closed']
        and a['writer_fd'] is None
        and a['tasks_joined']
        and a['owner_closed']
        for a in apps))) , body
    assert (all((j['returncode'] is not None
        and j['pidfd'] == -1
        and j['collector_joined']
        and j['stdout_eof']
        and j['stderr_eof']
        and j['streams_joined']
        for a in apps
        for j in a['jobs']))) , body
    names=[e['event'] for e in events]
    assert (names.count('connect') == names.count('close')
        and names.count('checkout') == names.count('reset') == names.count('checkin')) , body
"""


CLI_FLOW = r"""import hashlib,json,os,subprocess,sys,time
from pathlib import Path
root=Path(sys.argv[1]);
source=sys.argv[2];
expected=Path(sys.argv[3]);
entry=sys.argv[4];
selection=sys.argv[5]
if source!='-':sys.path.insert(0,source)
from importlib.resources import files
from krim_agent.evaluation.domain import DatasetSpec
from krim_agent.evaluation.metrics import MetricRegistry
from krim_agent.evaluation.continuous_metrics import register_metrics,continuous_rules
import krim_agent.evaluation.continuous as module
assert Path(module.__file__).is_relative_to(expected)
root.mkdir(exist_ok=True);commands=root/'commands';commands.mkdir();records=[]
def write(name,body):
    path=root/name;path.write_text(json.dumps(body,ensure_ascii=False));return str(path)
def call(label,args,zero=False):
    receipt=commands/(label+'-resources.json')
    argv=[sys.executable,'-I','-B','-c',entry,source,str(receipt),'--root',str(root),*args]
    result=subprocess.run(argv,cwd=Path.cwd(),capture_output=True,text=True,timeout=30)
    (commands/(label+'-stdout.txt')).write_text(result.stdout);
    (commands/(label+'-stderr.txt')).write_text(result.stderr)
    records.append({'label':label,
        'argv':argv,
        'cwd':str(Path.cwd()),
        'rc':result.returncode,
        'entry_hash':hashlib.sha256(entry.encode()).hexdigest()})
    assert result.returncode==0,(label,result.stdout,result.stderr)
    resource=json.loads(receipt.read_text());
    assert Path(resource['module']).is_relative_to(expected)
    if zero:assert all(v==0 for v in resource['calls'].values()),(label,resource)
    return json.loads(result.stdout)
example=json.loads(files('krim_agent.evaluation.examples').joinpath('continuous.v1.json').read_text())
body=example['dataset'];data=call('seed-dataset',['dataset','freeze',write('seed.json',body)])
seed=call('seed-execute',
    ['task',
    'run',
    data['name'],
    data['version'],
    data['fingerprint'],
    '--key',
    'cli-seed'])
row=call('import',
    ['continuous',
    'observe',
    'import',
    write('import.json',
    {'sources':[{'task_id':seed['eval_id'],
    'case_id':'basic'}]}),
    '--key',
    'cli-import'],
    True)['observations'][0]
draft=call('candidate',['continuous','candidate','get',row['id']],True)
confirmation=call('confirm',
    ['continuous',
    'candidate',
    'confirm',
    write('confirmation.json',
    {'candidate_id':draft['candidate_id'],
    'expected_revision':draft['revision'],
    'expected_hash':draft['hash'],
    'case':body['cases'][0],
    'context':'complete',
    'reason':example['review_reason']}),
    '--key',
    'cli-confirm'],
    True)
dataset=call('freeze',
    ['continuous',
    'dataset',
    'freeze',
    write('confirmed.json',
    {'name':'cli-confirmed',
    'version':'1',
    'confirmations':[confirmation['id']]}),
    '--key',
    'cli-freeze'],
    True)
registry=MetricRegistry();register_metrics(registry)
request={'schema_version':'continuous-task/1',
    'mode':'execute',
    'request_key':'cli-execute',
    'dataset':dataset['dataset_ref'],
    'dataset_envelope_fp':dataset['dataset_envelope_fp'],
    'metrics':[m.model_dump(mode='json')
    for m in continuous_rules(registry)]}
executed=call('execute',['continuous','task','submit',write('execute.json',request)])
assert executed['kind']=='execute' and executed['items'][0]['quality']=='passed'
assert (call('execute-key',
    ['continuous',
    'task',
    'submit',
    str(root / 'execute.json')],
    True)['eval_id'] == executed['eval_id'])
rescore=request|{'mode':'rescore',
    'request_key':'cli-rescore',
    'parent_task_id':executed['eval_id'],
    'parent_report_hash':executed['report_hash']}
rescored=call('rescore',['continuous','task','submit',write('rescore.json',rescore)],True)
assert rescored['kind']=='rescore' and rescored['items'][0]['quality']=='passed'
again=call('rescore-again',
    ['continuous',
    'task',
    'submit',
    write('rescore-again.json',
    rescore|{'request_key':'cli-rescore-again'})],
    True)
policy={'baseline_id':rescored['eval_id'],
    'candidate_id':again['eval_id'],
    'baseline_hash':rescored['report_hash'],
    'candidate_hash':again['report_hash'],
    'allowed_subjects':[rescored['subject_fingerprint']]}
assert (call('compare',
    ['continuous',
    'compare',
    write('compare.json',
    policy)],
    True)['status'] == 'pass')
observed=request|{'mode':'observed_score',
    'request_key':'cli-observed',
    'observations':[{'case_id':'basic',
    'repetition':0,
    'observation_id':row['id'],
    'observation_hash':row['content_hash']}]}
observed_report=call('observed',
    ['continuous',
    'task',
    'submit',
    write('observed.json',
    observed)],
    True)
assert observed_report['kind']=='observed_score' and observed_report['summary']['cell_coverage']<1
assert observed_report['items'][0]['quality']=='unscored'
schedule={'schedule_id':'cli-window',
    'version':'1',
    'template':observed|{'request_key':'window-template'},
    'epoch':int(time.time()),
    'interval_seconds':86400,
    'max_windows':3}
call('schedule-create',['continuous','schedule','create',write('schedule.json',schedule)],True)
window=call('schedule-tick',['continuous','schedule','tick','cli-window'],True)
assert (call('schedule-key',
    ['continuous',
    'schedule',
    'tick',
    'cli-window'],
    True)['eval_id'] == window['eval_id'])
call('schedule-disable',
    ['continuous',
    'schedule',
    'disable',
    'cli-window',
    '--revision',
    '0',
    '--key',
    'disable-once'],
    True)
assert (call('cold-report',
    ['report',
    executed['eval_id']],
    True)['report_hash'] == executed['report_hash'])
assert call('cold-key',['task','by-key','cli-rescore'],True)['id']==rescored['eval_id']
if selection in ['rescore','observed_score']:
    chosen=rescore if selection=='rescore' else observed
    before=call('source-report',['report',executed['eval_id']],True)
    report=call('cold-new-score',
        ['continuous',
        'task',
        'submit',
        write('cold-score.json',
        chosen|{'request_key':'cold-'+selection})],
        True)
    assert (report['kind'] == selection
        and call('source-after',
        ['report',
        executed['eval_id']],
        True) == before)
(root/'flow-commands.json').write_text(json.dumps(records,indent=2))
resources=[json.loads(p.read_text()) for p in commands.glob('*-resources.json')]
assert sum(a['spawn_calls'] for r in resources for a in r['apps'])==2
print(json.dumps({'root':str(root),
    'database':str(root/'eval.sqlite'),
    'module':module.__file__,
    'execute':executed['eval_id'],
    'rescore':rescored['eval_id'],
    'observed':observed_report['eval_id'],
    'window':window['eval_id'],
    'commands':len(records),
    'entry_hash':hashlib.sha256(entry.encode()).hexdigest(),
    'target_spawns':2,
    'paid_cli_calls':0}))
"""


def run_cli_flow(tmp_path, *, installed, selection="all"):
    root = Path(os.environ["KRIM_PN3_ROOT"])
    tmp_path.mkdir(parents=True, exist_ok=True)
    source = Path(__file__).resolve().parents[2]
    python = str(root / "installed/bin/python") if installed else sys.executable
    expected = root / "installed/lib/python3.12/site-packages" if installed else source / "src"
    source_argument = "-" if installed else str(source / "src")
    database = tmp_path / "cli/eval.sqlite"
    DATABASES.append(database)
    compile(CLI_ENTRY, "actual-cli-entry", "exec")
    compile(CLI_FLOW, "actual-cli-flow", "exec")
    argv = [
        python,
        "-I",
        "-B",
        "-c",
        CLI_FLOW,
        str(tmp_path / "cli"),
        source_argument,
        str(expected),
        CLI_ENTRY,
        selection,
    ]
    result = subprocess.run(argv, cwd=root / "outside", capture_output=True, text=True, timeout=90)
    (tmp_path / "flow-stdout.txt").write_text(result.stdout)
    (tmp_path / "flow-stderr.txt").write_text(result.stderr)
    (tmp_path / "flow-command.json").write_text(
        json.dumps(
            {
                "argv": argv,
                "cwd": str(root / "outside"),
                "rc": result.returncode,
                "flow_hash": hashlib.sha256(CLI_FLOW.encode()).hexdigest(),
                "entry_hash": hashlib.sha256(CLI_ENTRY.encode()).hexdigest(),
            },
            indent=2,
        )
    )
    assert result.returncode == 0, result.stdout + result.stderr
    return json.loads(result.stdout)


def test_continuous_wheel_identity(tmp_path):
    import tomllib

    root = Path(os.environ["KRIM_PN3_ROOT"])
    source = Path(__file__).resolve().parents[2]
    shipped = tomllib.loads((source / "pyproject.toml").read_text())["tool"]["hatch"]["build"][
        "targets"
    ]["wheel"]["only-include"]
    assert len(shipped) == len(set(shipped)) == 107
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
        for name in shipped:
            relative = name.removeprefix("src/")
            data = (source / name).read_bytes()
            assert data == bundle.read(relative) == (site / relative).read_bytes()
            digest = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).decode().rstrip("=")
            assert records[relative] == ["sha256=" + digest, str(len(data))]
    code = (
        "import sys,json;import krim_agent.evaluation.continuous as c;from impo"
        "rtlib.metadata import version;print(json.dumps({'module':c.__file__,'p"
        "ath':sys.path,'version':version('krim-agent'),'forbidden':[n for n in "
        "sys.modules if any(n==p or n.startswith(p+'.') for p in ['krim_agent.c"
        "omposition','krim_agent.runtime','krim_agent.providers','krim_agent.wo"
        "rkspace','krim_agent.execution'])]}))"
    )
    result = subprocess.run(
        [str(root / "installed/bin/python"), "-I", "-B", "-c", code],
        cwd=root / "outside",
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 0, result.stderr
    actual = json.loads(result.stdout)
    assert Path(actual["module"]).is_relative_to(site) and actual["version"] == "0.4.0a1"
    assert actual["forbidden"] == [] and all(str(source) not in p for p in actual["path"])
    (tmp_path / "wheel-identity.json").write_text(
        json.dumps(
            {"sha256": hashlib.sha256(wheel.read_bytes()).hexdigest(), "identity": actual}, indent=2
        )
    )


def test_installed_continuous_roundtrip(tmp_path):
    result = run_cli_flow(tmp_path, installed=True)
    assert result["target_spawns"] == 2 and result["execute"] != result["rescore"]


@pytest.mark.parametrize("mode", ["rescore", "observed_score"])
def test_installed_cold_score_has_zero_execution(tmp_path, mode):
    result = run_cli_flow(tmp_path, installed=True, selection=mode)
    assert result["target_spawns"] == 2
