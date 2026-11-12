import importlib.util
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
import subprocess
import zipfile

spec=importlib.util.spec_from_file_location('deploy',Path(__file__).with_name('autodeploy.py'))
d=importlib.util.module_from_spec(spec);spec.loader.exec_module(d)

class DeployTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name)
        self.state=self.root/'state';self.state.mkdir();self.releases=self.root/'releases';self.base=self.releases/'base';(self.base/'venv').mkdir(parents=True);(self.base/'python').mkdir();self.drop=self.root/'drop';self.drop.mkdir();self.old=self.drop/'zzzzz-qa-auto-old.conf';self.old.write_text('previous effective release\n');self.original=self.old.read_bytes();self.db=self.root/'db';self.db.mkdir();self.events=[]
        for name,value in [('STATE',self.state),('RELEASES',self.releases),('BASE',self.base),('DROPIN_DIR',self.drop),('DBDIR',self.db)]:
            p=patch.object(d,name,value);p.start();self.addCleanup(p.stop)
        for name,value in [('log',lambda event,**kw:self.events.append(event)),('TESTS_WHITELIST',[])]:
            p=patch.object(d,name,value);p.start();self.addCleanup(p.stop)
        self.sha='a'*40

    def test_build_failure_preserves_running_release_and_never_restarts(self):
        with patch.object(d,'download_tarball',side_effect=RuntimeError('download failed')),patch.object(d.subprocess,'run') as run:
            self.assertFalse(d.deploy(self.sha))
        self.assertTrue(self.old.exists(),'failed build removed last healthy drop-in')
        self.assertEqual(self.old.read_bytes(),self.original)
        self.assertFalse(run.called,'build failure must not restart the serving process')

    def fake_download(self,sha,path):path.write_bytes(b'fake archive')
    def fake_extract(self,tar,work):(work/'Krim-source').mkdir()
    def fake_run(self,cmd,**kwargs):
        self.commands.append(cmd)
        if 'venv' in cmd:Path(cmd[-1]).mkdir(parents=True,exist_ok=True)
        if '--wheel' in cmd:
            work=Path(cmd[cmd.index('--outdir')+1]);wheel=work/'krim_agent-0.0.0-py3-none-any.whl'
            with zipfile.ZipFile(wheel,'w') as z:z.writestr('krim_agent/__init__.py','')
        return subprocess.CompletedProcess(cmd,0,'ok','')

    def test_final_release_installs_dependencies_and_checks_cli(self):
        self.commands=[]
        with patch.object(d,'download_tarball',self.fake_download),patch.object(d,'extract',self.fake_extract),patch.object(d,'run',self.fake_run):
            self.assertTrue(d.deploy(self.sha,first=True))
        final=str(self.releases/('agent-core-qa-'+self.sha[:7]+'-auto')/'venv/bin/python')
        self.assertTrue(any(c[0]==final and c[1:4]==['-m','pip','install'] for c in self.commands),'final release never installs declared dependencies')
        self.assertTrue(any(c[0]==final and c[1:]==['-m','pip','check'] for c in self.commands),'final release dependency integrity is unchecked')
        self.assertTrue(any(c[0]==final and 'import krim_agent.cli' in ' '.join(c) for c in self.commands),'CLI import must be checked in final release')

    def test_failed_database_preflight_never_switches(self):
        self.commands=[]
        with patch.object(d,'download_tarball',self.fake_download),patch.object(d,'extract',self.fake_extract),patch.object(d,'run',self.fake_run),patch.object(d,'preflight',side_effect=RuntimeError('incompatible database'),create=True) as probe,patch.object(d.subprocess,'run') as system:
            self.assertFalse(d.deploy(self.sha))
        self.assertEqual(probe.call_count,1)
        self.assertEqual(self.old.read_bytes(),self.original)
        self.assertFalse(system.called)

    def test_failed_cutover_restores_exact_previous_dropin(self):
        self.commands=[]
        with patch.object(d,'download_tarball',self.fake_download),patch.object(d,'extract',self.fake_extract),patch.object(d,'run',self.fake_run),patch.object(d,'preflight',return_value=None),patch.object(d.subprocess,'run'),patch.object(d.urllib.request,'urlopen',side_effect=OSError('unhealthy')),patch.object(d.time,'sleep'),patch.object(d,'http_json',side_effect=OSError('unhealthy')):
            self.assertFalse(d.deploy(self.sha))
        self.assertEqual(self.old.read_bytes(),self.original)
        self.assertEqual(list(self.drop.iterdir()),[self.old])
        self.assertIn('rollback-failed',self.events)
        self.assertNotIn('rolled-back',self.events,'do not announce recovery without a healthy readback')

if __name__=='__main__':unittest.main(verbosity=2)
