"""完整项目外同包消费者草稿；initial阶段不构建/安装/执行。"""

import hashlib
import json
import os
import stat
import tomllib
import zipfile
from pathlib import Path

INSTALLED_DRIVER = (
    "import asyncio,hashlib,json,os,signal,sqlite3,sys\nfrom contextlib import closing\nfro"
    "m importlib.resources import files\nfrom pathlib import Path\nsite,root=map(Path,sys.a"
    "rgv[1:3])\nassert sys.flags.isolated==1 and sys.dont_write_bytecode\nassert str(Path.c"
    "wd())==str(root.parent.parent)\nsys.path.insert(0,str(site))\nassert all('/source' not"
    " in p for p in sys.path)\nfrom sqlalchemy import event,select\nfrom sqlalchemy.pool im"
    "port Pool\nfrom krim_agent.composition import open_application\nfrom krim_agent.evalua"
    "tion.app import open_eval\nfrom krim_agent.evaluation.repository import Repository,met"
    "a\nfrom krim_agent.evaluation.adapters.krim_outbox import KrimOutboxPort,ActorViews\nf"
    "rom krim_agent.evaluation.hot_observations import HotObservationFlow\nfrom krim_agent."
    "evaluation.hot_types import CapturePolicy,FeedbackSignal\nfrom krim_agent.evaluation.c"
    "ontinuous_types import ConfirmationRequest,FreezeConfirmed,ContinuousRequest\nfrom kri"
    "m_agent.evaluation.continuous_metrics import continuous_rules\nfrom krim_agent.evaluat"
    "ion.domain import DatasetSpec,DatasetRef\nfrom krim_agent.security import LOCAL,Member"
    "ship,PrincipalContext\nfrom krim_agent.sessions.types import SessionCreate,TurnRequest"
    "\nassert Path(sys.modules[Repository.__module__].__file__).is_relative_to(site)\nfacts"
    "={'uid':os.getuid(),'module':sys.modules[Repository.__module__].__file__,\n    'cwd':s"
    "tr(Path.cwd()),'sys_path':sys.path,'connections':[],'error':None}\nlisteners=[]\nfor k"
    "ind in ['connect','checkout','reset','checkin','close']:\n    def record(*args,kind=ki"
    "nd):\n        facts['connections'].append({'event':kind,'connection':id(args[0])})\n  "
    "  event.listen(Pool,kind,record);listeners.append((kind,record))\nasync def main():\n "
    "   source=evaluation=actual_provider=None;before=asyncio.all_tasks();facts['fd_before'"
    "]=len(os.listdir('/proc/self/fd'))\n    actual_source_calls=None\n    asyncio.get_runn"
    "ing_loop().add_signal_handler(signal.SIGTERM,asyncio.current_task().cancel)\n    try:"
    "\n        source=await open_application(root/'source.sqlite')\n        actual_provider"
    "=source.service.bindings['target-v2'].loop.provider\n        facts['source_provider_bi"
    "nding']='target-v2'\n        facts['source_provider_id']=id(actual_provider)\n        "
    "evaluation=await open_eval(root/'eval.sqlite')\n        async with evaluation.reposito"
    "ry.engine.begin() as conn:\n            marker=(await conn.execute(select(meta.c.body)"
    ")).scalar_one()\n        alice=PrincipalContext('alice',1,(Membership(workspace_id='lo"
    "cal',role='admin'),))\n        port=KrimOutboxPort(source.store,source.sessions,LOCAL,"
    "CapturePolicy(enabled=True))\n        await port.configure()\n        views=ActorViews"
    "(port,evaluation.repository,evaluation.kernel,{'local':LOCAL,'alice':alice})\n        "
    "flow=HotObservationFlow(port,evaluation.repository,evaluation.kernel,marker['repositor"
    "y_id'],views.resolve,views.binding,1)\n        session=await source.sessions.create(LO"
    "CAL,'local',SessionCreate(request_key='outside-source',target_key='target-v2'))\n     "
    "   turn=await source.sessions.submit(LOCAL,'local',session.id,TurnRequest(request_key="
    "'outside-turn',text='/add 2 3',expected_revision=0))\n        ended=await source.sessi"
    "ons.wait(LOCAL,'local',session.id,turn.id)\n        assert ended.status=='completed' a"
    "nd ended.run_id is not None\n        source_calls=source.provider.calls\n        actua"
    "l_source_calls=actual_provider.calls\n        assert actual_source_calls==2\n        a"
    "wait port.feedback(alice,ended.run_id,FeedbackSignal(key='human-one',reason='incorrect"
    "'))\n        receipts=await flow.consume_once()\n        assert len(receipts)==2 and l"
    "en({r.observation_id for r in receipts})==1\n        feedback=next(r for r in receipts"
    " if r.feedback_key is not None)\n        assert feedback.actor=='alice'\n        draft"
    "=await evaluation.repository.candidate('local',feedback.candidate_id)\n        assert "
    "draft['eligibility']=='needs_context' and draft['signals'][0]['actor']=='alice'\n     "
    "   original=await evaluation.repository.observation('local',feedback.observation_id)\n"
    "        assert original['output']['cleanup']=='unknown'\n        body=json.loads(files"
    "('krim_agent.evaluation.examples').joinpath('reconciliation.v1.json').read_text())\n  "
    "      data=DatasetSpec.model_validate(body|{'cases':body['cases'][:1]})\n        confi"
    "rmation=await evaluation.continuous.confirm_case(ConfirmationRequest(\n            req"
    "uest_key='outside-confirm',candidate_id=feedback.candidate_id,\n            expected_r"
    "evision=draft['revision'],expected_hash=draft['hash'],case=data.cases[0],\n           "
    " context='manual_reconstructed',reason='真实用户独立提供新初态与参考'))\n        froze"
    "n=await evaluation.continuous.freeze_dataset(FreezeConfirmed(request_key='outside-free"
    "ze',\n            name='outside-hot',version='1',confirmations=[confirmation['id']]))"
    "\n        request=ContinuousRequest(request_key='outside-execute',mode='execute',\n   "
    "         dataset=DatasetRef.model_validate(frozen['dataset_ref']),dataset_envelope_fp="
    "frozen['dataset_envelope_fp'],\n            metrics=continuous_rules(evaluation.reposi"
    "tory.metric_registry))\n        task=await evaluation.continuous.submit(request)\n    "
    "    terminal=await evaluation.kernel.wait(task['id'])\n        report=await evaluation"
    ".repository.report(task['id'])\n        assert terminal['status']=='completed' and rep"
    "ort['planned_count']==1\n        assert report['summary']['cell_coverage']==1 and repo"
    "rt['summary']['pass_rate']==1\n        assert evaluation.owner.spawn_calls==1\n       "
    " assert (await evaluation.continuous.submit(request))['id']==task['id']\n        asser"
    "t await flow.consume_once()==[]\n        assert source.provider.calls==source_calls an"
    "d evaluation.owner.spawn_calls==1\n        assert actual_provider.calls==actual_source"
    "_calls and actual_provider.active==0\n        assert (await evaluation.repository.obse"
    "rvation('local',feedback.observation_id))['content_hash']==original['content_hash']\n "
    "       facts.update(run_id=ended.run_id,task_id=task['id'],report_hash=report['report_"
    "hash'],\n            source_calls=actual_source_calls,main_provider_calls=source.provi"
    "der.calls,target_calls=evaluation.owner.spawn_calls,\n            original_observation"
    "_hash=original['content_hash'],receipt_keys=[r.key for r in receipts])\n    except Bas"
    "eException as error:\n        facts['error']={'type':type(error).__name__,'message':st"
    "r(error)}\n        raise\n    finally:\n        if evaluation is not None:\n          "
    "  await evaluation.close()\n            facts.update(eval_closed=evaluation.repository"
    ".closed,eval_writer_fd=evaluation.repository._writer_fd,\n                eval_produce"
    "rs_done=all(t.done() for t in evaluation.kernel.tasks.values()),\n                proc"
    "esses=[{'pid':w.process.pid,'wait':w.process.returncode,'pidfd':w.pidfd,\n            "
    "        'collector_done':w.collector is not None and w.collector.done(),\n            "
    "        'stdout_eof':w.stdout_eof,'stderr_eof':w.stderr_eof,\n                    'str"
    "eams_done':all(t.done() for t in w.stream_tasks)} for w in evaluation.owner.jobs.value"
    "s()])\n            assert facts['eval_closed'] and facts['eval_writer_fd'] is None and"
    " facts['eval_producers_done']\n            assert all(p['wait'] is not None and p['pid"
    "fd']==-1 and p['collector_done'] and p['stdout_eof']\n                and p['stderr_eo"
    "f'] and p['streams_done'] for p in facts['processes'])\n        if source is not None:"
    "\n            await source.close()\n            facts.update(source_closed=source.stor"
    "e.closed,source_producers_done=all(t.done() for t in source.service.tasks.values()),\n"
    "                dispatcher_done=source.sessions._dispatcher.done(),provider_active=sou"
    "rce.provider.active,\n                actual_provider_active=None if actual_provider i"
    "s None else actual_provider.active,\n                actual_provider_closed=None if ac"
    "tual_provider is None else actual_provider.closed,\n                actual_provider_st"
    "reams_closed=None if actual_provider is None else actual_provider.streams_closed)\n   "
    "         assert facts['source_closed'] and facts['source_producers_done'] and facts['d"
    "ispatcher_done'] and facts['provider_active']==0\n            assert facts['actual_pro"
    "vider_active']==0\n            if actual_source_calls is not None:\n                as"
    "sert facts['actual_provider_closed'] and facts['actual_provider_streams_closed']==actu"
    "al_source_calls\n        facts['pending']=[repr(t) for t in asyncio.all_tasks() if t n"
    "ot in before and t is not asyncio.current_task() and not t.done()]\n        facts['fd_"
    "after']=len(os.listdir('/proc/self/fd'))\n        for name in ['source','eval']:\n    "
    "        path=root/(name+'.sqlite')\n            if path.exists():\n                bac"
    "kup=root/(name+'-backup.sqlite')\n                with closing(sqlite3.connect(path.as"
    "_uri()+'?mode=ro',uri=True)) as reader:\n                    with closing(sqlite3.conn"
    "ect(backup)) as writer:reader.backup(writer)\n                facts[name+'_backup']={'"
    "path':str(backup),'sha256':hashlib.sha256(backup.read_bytes()).hexdigest()}\n        ("
    "root/'consumer.json').write_text(json.dumps(facts,indent=2))\n        for kind,record "
    "in listeners:event.remove(Pool,kind,record)\n        assert not facts['pending'] and f"
    "acts['fd_after']<=facts['fd_before']\nasyncio.run(main())\n"
)


async def test_installed_hot_observation_roundtrip(tmp_path):
    from test_hot_observation_storage import run_program

    root = Path(os.environ["KRIM_PN3_HOT_ROOT"])
    source = (
        Path(__file__).resolve()  # noqa: ASYNC240  # 固定本地身份检查
    ).parents[2]
    expected = json.loads(
        Path(  # noqa: ASYNC240  # 固定本地身份检查
            "/artifacts/krim-agent-core-20261019/"
            "vm-pn3-scale-implementation-20261023T152826061485/"
            "static-supplement-03/pn3-h1-initial-implementation01/package-files.json"
        ).read_text()
    )
    shipped = tomllib.loads((source / "pyproject.toml").read_text())["tool"]["hatch"]["build"][
        "targets"
    ]["wheel"]["only-include"]
    assert shipped == expected and len(shipped) == len(set(shipped)) == 115
    site = root / "installed/lib/python3.12/site-packages"
    wheel = root / "dist/krim_agent-0.4.0a1-py3-none-any.whl"
    with zipfile.ZipFile(wheel) as bundle:
        for name in shipped:
            relative = name.removeprefix("src/")
            assert (
                (source / name).read_bytes()
                == bundle.read(relative)
                == (site / relative).read_bytes()
            )
        assert "Version: 0.4.0a1" in bundle.read("krim_agent-0.4.0a1.dist-info/METADATA").decode()
    label = Path(os.environ["KRIM_PN3_HOT_EVIDENCE"]).name
    parent = root / "outside" / label
    parent.mkdir(exist_ok=True)
    destination = parent / tmp_path.name
    destination.mkdir()
    receipt = await run_program(
        INSTALLED_DRIVER,
        [site, destination],
        tmp_path / "installed-process",
        timeout=180,
        cwd=root / "outside",
        interpreter=root / "installed/bin/python",
    )
    assert stat.S_ISREG((destination / "consumer.json").lstat().st_mode)
    consumer = json.loads((destination / "consumer.json").read_text())
    assert receipt["driver_sha256"] == hashlib.sha256(INSTALLED_DRIVER.encode()).hexdigest()
    assert receipt["wait_returncode"] == 0 and not receipt["signals"]
    assert consumer["error"] is None and consumer["target_calls"] == 1
    assert consumer["uid"] == os.getuid() != 0 and consumer["cwd"] == str(root / "outside")
    assert Path(consumer["module"]).is_relative_to(site)
    assert consumer["source_closed"] and consumer["eval_closed"] and not consumer["pending"]
