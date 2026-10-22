"""AC-M2 PREP_BASE：标准库、固定受信程序；不是产品Runner或安全负例。

只经本任务krim-vm-exec执行。每轮独立证据，未全部合格绝不写BASE-QUALIFIED。
"""
from __future__ import annotations

import argparse
import ast
import hashlib
import json
import os
import re
import select
import shutil
import signal
import stat
import subprocess
import sys
import time
import uuid
import xml.etree.ElementTree as ET
from pathlib import Path

R = Path('/artifacts/krim-agent-core-20261019')
GRANT = R / 'vm-prep-base-20261019T170838/grant.json'
SOURCE_ADD = 'tests/agent_core/fixtures/prepare_m2_materials.py'
SOURCE_DOC = 'docs/agent-core/SOURCES.md'


class NotReady(Exception):
    """材料缺项，不能标作产品RED。"""


def require(condition: bool, message: str) -> None:
    if not condition:
        raise NotReady(message)


def sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def write_json(path: Path, value: object) -> None:
    # 排他创建，任何失败/旧收据都不能被覆盖。
    with path.open('x', encoding='utf-8') as handle:
        json.dump(value, handle, ensure_ascii=False, indent=2)
        handle.write('\n')
        handle.flush()
        os.fsync(handle.fileno())


def status() -> dict[str, str]:
    wanted = {'Uid', 'Gid', 'CapEff', 'CapPrm', 'CapAmb', 'NoNewPrivs', 'Seccomp'}
    return {k: v.strip() for line in Path('/proc/self/status').read_text().splitlines()
            if ':' in line for k, v in [line.split(':', 1)] if k in wanted}


def fds() -> dict[str, str]:
    result = {}
    for fd in os.listdir('/proc/self/fd'):
        try:
            result[fd] = os.readlink('/proc/self/fd/' + fd)
        except FileNotFoundError:
            pass  # /proc枚举本身的短生命周期FD。
    return result


class Preparation:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.grant = json.loads(GRANT.read_text())
        activation = json.loads((GRANT.parent / 'ACTIVATED.json').read_text())
        require(activation['launch_authorized'] and sha(GRANT) == activation['grant_sha256'],
                'grant/activation不匹配')
        self.e = Path(self.grant['acceptance_root'])
        self.source = self.e / 'source'
        require(Path.cwd() == self.source, '必须在E/source经执行通道运行')
        for value, expected in [(args.root, self.e / 'materials/base'),
                                (args.rootfs, self.e / 'materials/base/rootfs'),
                                (args.evidence, self.e / 'evidence/prep-base')]:
            require(Path(value).absolute() == expected, '材料路径超出本阶段精确根')
        require(args.cgroup_root == self.grant['cgroup_root'], 'cgroup根与grant不一致')
        self.root = Path(args.root)
        self.rootfs = Path(args.rootfs)
        self.evidence = Path(args.evidence)
        self.run = self.evidence / ('run-' + time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())
                                    + '-' + uuid.uuid4().hex[:8])
        self.run.mkdir(parents=True, exist_ok=False)
        self.env = {'PATH': '/usr/local/bin:/usr/bin:/bin', 'LANG': 'C.UTF-8',
                    'LD_LIBRARY_PATH': str(self.e / 'python/lib'),
                    'TMPDIR': str(self.e / 'tmp'), 'PYTHONDONTWRITEBYTECODE': '1'}
        self.commands: list[dict] = []
        self.events: list[dict] = []
        self.materials: dict[str, dict] = {}
        self.serial = 0
        self.p = str(self.e / 'dev-env/bin/python')

    def event(self, kind: str, **data: object) -> None:
        record = {'kind': kind, 'monotonic_ns': time.monotonic_ns(), **data}
        self.events.append(record)
        with (self.run / 'resources.jsonl').open('a', encoding='utf-8') as handle:
            handle.write(json.dumps(record, ensure_ascii=False) + '\n')

    def command(self, label: str, argv: list[str], timeout: int = 30,
                accepted: tuple[int, ...] = (0,)) -> str:
        self.serial += 1
        stem = self.run / f'{self.serial:03d}-{label}'
        record = {'argv': argv, 'cwd': str(self.source), 'env': self.env,
                  'started_ns': time.time_ns(), 'source_driver_sha256': sha(Path(__file__))}
        proc = None
        out, err = b'', b''
        try:
            proc = subprocess.Popen(argv, cwd=self.source, env=self.env, stdin=subprocess.DEVNULL,
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True)
            self.event('process_created', label=label, pid=proc.pid)
            try:
                out, err = proc.communicate(timeout=timeout)
            except subprocess.TimeoutExpired:
                self.event('process_timeout', label=label, pid=proc.pid)
                proc.terminate()  # 只操作本函数Popen精确创建的句柄。
                try:
                    out, err = proc.communicate(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    out, err = proc.communicate(timeout=5)
                raise NotReady(label + ':固定受信命令超时')
            (stem.with_suffix('.stdout')).write_bytes(out)
            (stem.with_suffix('.stderr')).write_bytes(err)
            record.update(rc=proc.returncode, stdout=str(stem.with_suffix('.stdout')),
                          stderr=str(stem.with_suffix('.stderr')))
            require(proc.returncode in accepted, label + ':rc=' + str(proc.returncode))
            return out.decode('utf-8', errors='strict')
        finally:
            if proc is not None:
                if proc.poll() is None:
                    proc.kill()
                    proc.communicate(timeout=5)
                record['rc'] = proc.returncode
                self.event('process_waited', label=label, pid=proc.pid, rc=proc.returncode)
            (stem.with_suffix('.stdout')).write_bytes(out)
            (stem.with_suffix('.stderr')).write_bytes(err)
            record.update(stdout=str(stem.with_suffix('.stdout')), stderr=str(stem.with_suffix('.stderr')))
            record['finished_ns'] = time.time_ns()
            write_json(stem.with_suffix('.command.json'), record)
            self.commands.append(record)

    def source_identity(self) -> dict:
        require(sys.version_info[:3] == (3, 12, 13), '父解释器版本不符')
        require(Path(sys.base_prefix) == self.e / 'python', '父解释器不是本轮独立CPython')
        require(not any(n == 'krim_agent' or n.startswith('krim_agent.') for n in sys.modules),
                '不能导入产品')
        source = json.loads(Path(self.grant['source_manifest']).read_text())
        protected = json.loads(Path(self.grant['protected_manifest']).read_text())
        for key in ['source_manifest', 'protected_manifest', 'environment_authorization']:
            require(sha(Path(self.grant[key])) == self.grant[key + '_sha256'], key + '绑定改变')
        require(sha(Path(self.grant['approval_file'])) == self.grant['approval_sha256'], '批准身份改变')
        for path, digest in self.grant['plan_hashes'].items():
            require(sha(self.source / path) == digest, '冻结方案改变')
        current = {}
        for path in [*source, SOURCE_ADD]:
            p = self.source / path
            st = p.lstat()
            require(stat.S_ISREG(st.st_mode), '源码不是普通文件:' + path)
            current[path] = {'sha256': sha(p), 'bytes': st.st_size,
                             'source_mode': stat.S_IMODE(st.st_mode)}
            if path in protected:
                require(current[path] == protected[path], '受保护文件改变:' + path)
        require(len(protected) == 1601, '保护集合数量不符')
        write_json(self.run / 'source-manifest.json', current)
        shutil.copyfile(Path(__file__), self.run / 'driver-source.py')
        shutil.copyfile(self.source / SOURCE_DOC, self.run / 'SOURCES-snapshot.md')
        identity = {'revision': self.grant['base_revision'], 'manifest': str(self.run / 'source-manifest.json'),
                    'manifest_sha256': sha(self.run / 'source-manifest.json'),
                    'driver_sha256': current[SOURCE_ADD]['sha256'], 'protected_count': len(protected),
                    'grant_sha256': sha(GRANT), 'approval_sha256': self.grant['approval_sha256']}
        return identity

    def qualify_identity(self) -> dict:
        st = status()
        require(os.getuid() == os.geteuid() == self.grant['execution_uid'] != 0,
                '非任务普通用户身份')
        require(all(int(st[k], 16) == 0 for k in ['CapEff', 'CapPrm', 'CapAmb']), '父进程cap非零')
        require(st['NoNewPrivs'] == '1', '父进程NNP不是1')
        cg = Path('/proc/self/cgroup').read_text().strip()
        require(cg == '0::' + self.grant['cgroup_root'].removeprefix('/sys/fs/cgroup') + '/app',
                '未通过委派app通道运行')
        result = {'uid': os.getuid(), 'gid': os.getgid(), 'status': st, 'cgroup': cg,
                  'namespaces': {n: os.readlink('/proc/self/ns/' + n)
                                 for n in ['user', 'mnt', 'pid', 'net', 'ipc', 'uts']},
                  'uid_map': Path('/proc/self/uid_map').read_text(),
                  'kernel': list(os.uname()), 'boot_id': Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
                  'python': {'version': sys.version, 'executable': sys.executable,
                             'base_prefix': sys.base_prefix, 'sha256': sha(Path(sys.executable))},
                  'channel_sha256': sha(Path(self.grant['product_execution_channel']))}
        write_json(self.run / 'identity.json', result)
        return result

    def copy_material(self, source: Path, relative: str, role: str) -> None:
        # 只复制受信CPython/系统ELF及许可；物化source链接，rootfs不含用户链接。
        require(not relative.startswith('/') and '..' not in Path(relative).parts, '非法rootfs相对名')
        actual = source.resolve(strict=True)
        require(stat.S_ISREG(actual.stat().st_mode), '材料不是普通文件:' + str(source))
        destination = self.rootfs / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        digest = sha(actual)
        if destination.exists():
            require(not destination.is_symlink() and sha(destination) == digest,
                    '已存在材料字节不符，不覆盖:' + relative)
        else:
            with actual.open('rb') as src, destination.open('xb') as dst:
                shutil.copyfileobj(src, dst)
            destination.chmod(0o555 if actual.stat().st_mode & 0o111 else 0o444)
        self.materials[relative] = {'sha256': digest, 'bytes': actual.stat().st_size,
                                    'source': str(source), 'resolved_source': str(actual), 'role': role,
                                    'mode': stat.S_IMODE(destination.stat().st_mode)}

    def build_rootfs(self) -> dict:
        self.rootfs.mkdir(exist_ok=True)
        prefix = self.e / 'python'
        python = prefix / 'bin/python3.12'
        self.copy_material(python, 'opt/python/bin/python3.12', 'CPython3.12.13')
        for file in sorted((prefix / 'lib').iterdir()):
            if file.is_file() and file.name.startswith('libpython'):
                self.copy_material(file, 'opt/python/lib/' + file.name, 'CPython shared runtime')
        stdlib = prefix / 'lib/python3.12'
        for directory, names, files in os.walk(stdlib, followlinks=False):
            names[:] = sorted(n for n in names if n not in {'site-packages', '__pycache__'})
            for name in sorted(files):
                file = Path(directory) / name
                if file.suffix in {'.pyc', '.pyo'}:
                    continue
                if file.is_symlink():
                    require(file.resolve().is_relative_to(prefix), 'CPython链接出闭包')
                self.copy_material(file, 'opt/python/lib/python3.12/' + str(file.relative_to(stdlib)),
                                   'CPython stdlib/extension')
        elf_sources = [python, prefix / 'lib/libpython3.12.so.1.0', *sorted((stdlib / 'lib-dynload').glob('*.so'))]
        dependencies = set()
        missing = []
        for file in elf_sources:
            output = self.command('ldd', ['/usr/bin/ldd', str(file)])
            missing.extend({'extension': str(file), 'needed': line.strip()}
                           for line in output.splitlines() if 'not found' in line)
            for line in output.splitlines():
                hit = re.search(r'(?:=>\s*)?(/[^\s()]+)\s+\(', line)
                if hit:
                    dependency = Path(hit.group(1))
                    if not dependency.is_relative_to(prefix):
                        dependencies.add(dependency)
        require(dependencies, 'ELF依赖集合为空')
        packages = {}
        for library in sorted(dependencies):
            self.copy_material(library, str(library).lstrip('/'), 'system ELF dependency')
            # merge-/usr系统dpkg可能登记原/lib或/usr/lib名；一次命令同时查询两个可信名称。
            output = self.command('dpkg-owner', ['/usr/bin/dpkg-query', '-S', str(library),
                                               str(library.resolve())], accepted=(0, 1))
            lines = [line for line in output.splitlines()
                     if re.fullmatch(r'[a-z0-9][a-z0-9+.-]*(?::[a-z0-9-]+)?: /.+', line)]
            require(lines, '找不到系统库许可来源:' + str(library))
            package = lines[0].split(': /', 1)[0].split(',')[0].split(':')[0]
            copyright_file = Path('/usr/share/doc') / package / 'copyright'
            require(copyright_file.is_file(), '缺包copyright:' + package)
            self.copy_material(copyright_file, 'usr/share/doc/' + package + '/copyright', 'original copyright')
            packages[str(library)] = {'package': package, 'copyright': str(copyright_file),
                                      'copyright_sha256': sha(copyright_file)}
        for file in sorted(Path('/usr/share/common-licenses').iterdir()):
            if file.is_file():
                self.copy_material(file, 'usr/share/common-licenses/' + file.name, 'original license text')
        require((stdlib / 'LICENSE.txt').is_file(), '缺CPython许可')
        for folder in ['proc', 'dev', 'tmp', 'work']:
            (self.rootfs / folder).mkdir(exist_ok=True)
        manifest = {'files': self.materials, 'libraries': packages, 'site_packages_included': False,
                    'no_host_directory_mount_required': True, 'broker_or_seccomp_included': False, 'missing_dependencies': missing,
                    'complete': not missing}
        write_json(self.run / 'rootfs-manifest.json', manifest)
        require(not missing, 'ELF闭包缺库，详见rootfs-manifest.json: ' + repr(missing))
        for directory, _, _ in os.walk(self.rootfs, topdown=False):
            Path(directory).chmod(0o555)
        return {'path': str(self.rootfs), 'manifest': str(self.run / 'rootfs-manifest.json'),
                'manifest_sha256': sha(self.run / 'rootfs-manifest.json'), 'files': len(self.materials)}

    def qualify_namespace(self, parent: dict) -> dict:
        bwrap = Path('/usr/local/bin/bwrap')
        st = bwrap.lstat()
        require(stat.S_ISREG(st.st_mode) and st.st_uid == 0 and not st.st_mode & 0o6022,
                'bwrap owner/mode不可信')
        version = self.command('bwrap-version', [str(bwrap), '--version']).strip()
        require(version == 'bubblewrap 0.11.0', 'bwrap版本不符')
        license_file = Path('/usr/share/doc/bubblewrap/copyright')
        require(license_file.is_file(), '缺bwrap版权来源')
        shutil.copyfile(license_file, self.run / 'bubblewrap-copyright.txt')
        payload = '''import os,sys,json,hashlib,ssl,ctypes,zlib,bz2,lzma,unittest,xml.etree.ElementTree
wanted={'Uid','Gid','CapEff','NoNewPrivs'}
s={k:v.strip() for line in open('/proc/self/status') if ':' in line for k,v in [line.split(':',1)] if k in wanted}
print(json.dumps({'version':list(sys.version_info[:3]),'executable':sys.executable,'prefix':sys.prefix,'stdlib':os.__file__,'executable_sha256':hashlib.sha256(open(sys.executable,'rb').read()).hexdigest(),'uid':os.getuid(),'status':s,'uid_map':open('/proc/self/uid_map').read(),'namespaces':{n:os.readlink('/proc/self/ns/'+n) for n in ['user','mnt','pid','net','ipc','uts']},'mountinfo':open('/proc/self/mountinfo').read(),'modules':{n:getattr(sys.modules[n],'__file__',None) for n in ['ssl','ctypes','zlib','bz2','lzma','unittest']}}))'''
        argv = [str(bwrap), '--unshare-all', '--uid', '0', '--gid', '0', '--cap-drop', 'ALL',
                '--new-session', '--die-with-parent', '--ro-bind', str(self.rootfs), '/',
                '--proc', '/proc', '--remount-ro', '/proc', '--dev', '/dev',
                '--size', '4194304', '--tmpfs', '/tmp', '--size', '16777216', '--tmpfs', '/work',
                '--clearenv', '--setenv', 'LD_LIBRARY_PATH', '/opt/python/lib',
                '--setenv', 'PATH', '/opt/python/bin', '--chdir', '/work',
                '--', '/opt/python/bin/python3.12', '-I', '-B', '-c', payload]
        child = json.loads(self.command('bwrap-fixed-python', argv))
        require(child['version'] == [3, 12, 13] and child['prefix'] == '/opt/python', '子解释器身份不符')
        require(child['executable_sha256'] == parent['python']['sha256'], '父子解释器字节不同')
        require(child['status']['NoNewPrivs'] == '1' and int(child['status']['CapEff'], 16) == 0,
                '子进程cap/NNP不符')
        require(all(child['namespaces'][n] != parent['namespaces'][n] for n in parent['namespaces']),
                '没有建立不同namespace')
        require(child['uid'] == 0 and child['uid_map'].split() == ['0', str(os.getuid()), '1'],
                'rootless单UID映射不符')
        mounts = [line.split() for line in child['mountinfo'].splitlines()]
        root_mount = next(m for m in mounts if m[4] == '/')
        require('ro' in root_mount[5].split(','), 'rootfs未只读挂载')
        require(not any(m[4].startswith(('/usr', '/opt', '/home', '/data', '/sys')) for m in mounts),
                '出现非基础rootfs挂载')
        write_json(self.run / 'namespace.json', child)
        return {'version': version, 'binary': str(bwrap), 'sha256': sha(bwrap), 'mode': stat.S_IMODE(st.st_mode),
                'copyright_sha256': sha(license_file), 'child': str(self.run / 'namespace.json'),
                'base_namespace_only_not_security_gate': True}

    def qualify_cgroup_pidfd(self) -> dict:
        root = Path(self.args.cgroup_root)
        require(not (root / 'cgroup.procs').read_text().strip(), '任务根有进程')
        require({'cpu', 'memory', 'pids'} <= set((root / 'cgroup.subtree_control').read_text().split()),
                '任务根controller未委派')
        require(hasattr(os, 'pidfd_open') and hasattr(signal, 'pidfd_send_signal'), '父P缺pidfd API')
        leaf = root / ('prep-' + uuid.uuid4().hex)
        leaf.mkdir()
        identity = {'path': str(leaf), 'dev': leaf.stat().st_dev, 'inode': leaf.stat().st_ino,
                    'root_dev': root.stat().st_dev, 'root_inode': root.stat().st_ino}
        self.event('leaf_created', **identity)
        proc = None
        fd = None
        result = {'identity': identity, 'limits': {}, 'cleanup': {}}
        try:
            limits = {'cpu.max': '50000 100000', 'memory.max': '134217728',
                      'memory.swap.max': '0', 'memory.oom.group': '1', 'pids.max': '16'}
            for name, value in limits.items():
                (leaf / name).write_text(value)
                actual = (leaf / name).read_text().strip()
                require(actual == value, 'cgroup限额读回不符:' + name)
                result['limits'][name] = actual
            payload = "import os,sys,json;print('READY',flush=True);sys.stdin.readline();print(json.dumps({'pid':os.getpid(),'cgroup':open('/proc/self/cgroup').read().strip()}),flush=True);sys.stdin.read()"
            argv = [self.p, '-I', '-B', '-c', payload]
            proc = subprocess.Popen(argv, cwd=self.source, env=self.env, stdin=subprocess.PIPE,
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True)
            self.event('inert_child_created', pid=proc.pid, argv=argv, env=self.env, cwd=str(self.source))
            require(select.select([proc.stdout], [], [], 5)[0] and proc.stdout.readline() == b'READY\n',
                    '固定child未ready')
            fd = os.pidfd_open(proc.pid)
            signal.pidfd_send_signal(fd, 0)
            self.event('pidfd_bound', pid=proc.pid, pidfd=fd,
                       start_identity=Path(f'/proc/{proc.pid}/stat').read_text())
            (leaf / 'cgroup.procs').write_text(str(proc.pid))
            require(str(proc.pid) in (leaf / 'cgroup.procs').read_text().split(), 'child未加入自有叶')
            proc.stdin.write(b'go\n')
            proc.stdin.flush()
            require(select.select([proc.stdout], [], [], 5)[0], 'child无cgroup回程')
            child = json.loads(proc.stdout.readline())
            require(child['pid'] == proc.pid and child['cgroup'].endswith('/' + leaf.name), 'child归属不符')
            result['child'] = child
            self.event('inert_child_joined', **child)
            (leaf / 'cgroup.kill').write_text('1')
            self.event('cgroup_kill_requested', leaf=str(leaf))
            out, err = proc.communicate(timeout=5)
            (self.run / 'cgroup-child.stdout').write_bytes(out)
            (self.run / 'cgroup-child.stderr').write_bytes(err)
            result['rc'] = proc.returncode
            require(proc.returncode == -signal.SIGKILL, 'cgroup.kill未得到实际SIGKILL退出')
            self.event('inert_child_waited', pid=proc.pid, rc=proc.returncode)
        finally:
            if proc is not None:
                if proc.poll() is None:
                    if fd is not None:
                        signal.pidfd_send_signal(fd, signal.SIGKILL)
                    else:
                        proc.kill()
                    proc.communicate(timeout=5)
                for stream in [proc.stdin, proc.stdout, proc.stderr]:
                    if stream is not None:
                        stream.close()
                result['cleanup']['waited_rc'] = proc.returncode
            if fd is not None:
                os.close(fd)
                self.event('pidfd_closed', pidfd=fd)
            deadline = time.monotonic() + 5
            while 'populated 1' in (leaf / 'cgroup.events').read_text() and time.monotonic() < deadline:
                time.sleep(0.02)
            events = (leaf / 'cgroup.events').read_text()
            require('populated 0' in events, '自有cgroup仍有进程')
            result['cleanup']['events'] = events
            self.event('populated_zero', events=events, leaf=str(leaf))
            leaf.rmdir()
            result['cleanup']['leaf_removed'] = not leaf.exists()
            self.event('leaf_removed', leaf=str(leaf))
            write_json(self.run / 'cgroup-pidfd.json', result)
        require(result['cleanup']['leaf_removed'], '叶未移除')
        return result

    def execute(self) -> int:
        initial_fds = fds()
        record = {'kind': 'PREP_BASE qualification, not product/OS-GATE', 'status': 'NOT_READY',
                  'argv': sys.orig_argv, 'cwd': str(Path.cwd()), 'env': self.env,
                  'run_directory': str(self.run), 'started_ns': time.time_ns(),
                  'does_not_claim': self.grant['allowed_stage_contract']['does_not_claim']}
        try:
            record['source'] = self.source_identity()
            record['identity'] = self.qualify_identity()
            record['bwrap_version'] = self.command('bwrap-version', ['/usr/local/bin/bwrap', '--version']).strip()
            require(record['bwrap_version'] == 'bubblewrap 0.11.0', 'bwrap版本不符')
            record['cgroup_pidfd'] = self.qualify_cgroup_pidfd()
            record['rootfs'] = self.build_rootfs()
            record['namespace'] = self.qualify_namespace(record['identity'])
            require(initial_fds == fds(), '父进程FD未归还')
            require(not Path('/proc/self/task/' + str(os.getpid()) + '/children').read_text().strip(),
                    '父进程仍有未收束child')
            record['cleanup'] = {'fd_before': initial_fds, 'fd_after': fds(), 'owned_children': [],
                                 'all_popen_waited': True, 'namespace_process_completed': True,
                                 'cgroup_leaf_removed': True, 'unsafe_gc': 0}
            record['status'] = 'BASE-QUALIFIED'
        except Exception as error:
            record['error'] = {'type': type(error).__name__, 'message': str(error)}
            record['fd_after_failure'] = fds()
        finally:
            record['finished_ns'] = time.time_ns()
            write_json(self.run / 'commands.json', self.commands)
            write_json(self.run / 'result.json', record)
            if record['status'] == 'BASE-QUALIFIED':
                write_json(self.evidence / 'BASE-QUALIFIED.json', record)
            else:
                write_json(self.run / 'NOT_READY.json', record)
            print(json.dumps({'status': record['status'], 'result': str(self.run / 'result.json'),
                              'sha256': sha(self.run / 'result.json')}, ensure_ascii=False), flush=True)
        return 0 if record['status'] == 'BASE-QUALIFIED' else 1


class ProductGate:
    """封存证据的标准库只读聚合；不执行材料、产品或隔离代码。

    固定许可是信任锚。重定位仅用于资格副本，不能重封输入hash，亦不能生成正式收据。
    原suite的cwd、解释器、时间与source始终按原件解释。
    """

    OWNER = R / 'vm-product-gate-rebind-01-20261019T141133155200'

    SEAL = '06bf003bc7240c2d868d5a2f6b4d22973ff56d56e91ffcdde5614263f3e31306'


    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.checked: dict[str, dict] = {}
        self.relocations: dict[str, str] = {}
        self.run: Path | None = None
        require(args.supplement == str(self.OWNER / 'supplement.json')
                and args.seal == str(self.OWNER / 'supplement.sha256'), '许可路径不是固定Supplement')
        raw, _ = self.file_bytes(Path(args.supplement))
        sealed, _ = self.file_bytes(Path(args.seal))
        require(sealed.decode().strip() == self.SEAL and self.digest(raw) == self.SEAL,
                'Supplement seal不匹配')
        self.s = self.decode(raw)
        self.c = Path(self.s['acceptance_root'])
        self.e = Path(self.s['parent_acceptance_root'])
        self.n = Path(self.s['new_evidence_root'])
        self.product_root = self.n / 'product-root'
        require(args.acceptance_root == str(self.c) and self.c.parent == self.e / 'supplements',
                '只能使用本补充唯一子写根')
        require(Path.cwd() == self.c / 'source', '聚合只能在C/source执行')
        self.directory(self.c)
        self.directory(self.c / 'evidence')
        require(self.s['operations'] == {'add': [], 'rewrite': [SOURCE_ADD], 'delete': []}
                and not self.s['product_imports_allowed'] and not self.s['product_tests_allowed']
                and not self.s['subprocess_in_product_gate_allowed']
                and not self.s['preparation_execution_allowed']
                and not self.s['downstream_stages_allowed'], 'Supplement边界不符')
        self.run = self.c / 'evidence' / ('aggregate-' + str(time.time_ns()) + '-' + uuid.uuid4().hex[:8])
        self.run.mkdir()
        self.bound = {v['path']: {'sha256': v['sha256']} for v in self.s['sealed_files'].values()}
        self.inputs = self.decode(self.read(self.s['input_files_file'], indexed=False))
        self.excluded = self.decode(self.read(self.s['sealed_files']['excluded']['path'], indexed=False))
        excluded = {v['path'] for v in self.excluded}
        require(len(self.inputs) == 27275, '固定输入集合数量不符')
        for p in self.inputs:
            require(self.allowed_original(p) and p not in excluded, '冻结输入越界或错误豁免: ' + p)
        self.prior = self.data(self.s['sealed_files']['prior_supplement']['path'])
        self.old_e = Path(self.prior['parent_acceptance_root'])
        if args.fixture_map:
            require(args.qualify, '重定位仅供资格，不能写正式收据')
            mapping = self.local_json(args.fixture_map)
            require(set(mapping) == {'kind', 'files'} and mapping['kind'] == 'explicit-read-relocation/1'
                    and isinstance(mapping['files'], dict), '重定位schema不符')
            for original, replacement in mapping['files'].items():
                require(original in self.inputs, '重定位源不是冻结输入')
                self.local_path(replacement, self.c / 'fixtures')
                self.relocations[original] = replacement
            for original in self.relocations:
                self.read(original)


    @staticmethod
    def digest(raw: bytes) -> str:
        return hashlib.sha256(raw).hexdigest()

    @staticmethod
    def path(value: str | Path) -> Path:
        text = str(value)
        p = Path(text)
        require(p.is_absolute() and '..' not in p.parts and str(p) == text,
                '路径必须为无归一化歧义的绝对路径')
        return p

    @classmethod
    def open_checked(cls, value: Path, *, directory: bool = False) -> int:
        """逐级lstat + openat(O_NOFOLLOW)，在读之前拒绝特殊文件及祖先链接。"""
        p = cls.path(value)
        fd = os.open('/', os.O_RDONLY | os.O_DIRECTORY)
        try:
            for i, part in enumerate(p.parts[1:]):
                st = os.stat(part, dir_fd=fd, follow_symlinks=False)
                last = i == len(p.parts) - 2
                is_dir = not last or directory
                require(stat.S_ISDIR(st.st_mode) if is_dir else stat.S_ISREG(st.st_mode),
                        '非正规文件/目录或链接: ' + str(p))
                flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
                if is_dir:
                    flags |= os.O_DIRECTORY
                child = os.open(part, flags, dir_fd=fd)
                observed = os.fstat(child)
                if (observed.st_dev, observed.st_ino, observed.st_mode) != (
                        st.st_dev, st.st_ino, st.st_mode):
                    os.close(child)
                    raise NotReady('读取期间路径身份改变: ' + str(p))
                os.close(fd)
                fd = child
            result, fd = fd, -1
            return result
        finally:
            if fd >= 0:
                os.close(fd)

    @classmethod
    def file_bytes(cls, path: Path) -> tuple[bytes, dict]:
        fd = cls.open_checked(path)
        with os.fdopen(fd, 'rb') as handle:
            st = os.fstat(handle.fileno())
            require(st.st_size <= 128 * 1024 * 1024, '输入超过有界元数据读取容量')
            raw = handle.read(128 * 1024 * 1024 + 1)
            after = os.fstat(handle.fileno())
            require((st.st_size, st.st_mtime_ns, st.st_ctime_ns) == (
                after.st_size, after.st_mtime_ns, after.st_ctime_ns)
                    and len(raw) == st.st_size, '读取期间输入变化')
            return raw, {'sha256': cls.digest(raw), 'bytes': len(raw),
                         'source_mode': stat.S_IMODE(st.st_mode)}

    @classmethod
    def directory(cls, path: Path) -> dict:
        fd = cls.open_checked(path, directory=True)
        try:
            st = os.fstat(fd)
            return {'dev': st.st_dev, 'inode': st.st_ino, 'uid': st.st_uid,
                    'names': sorted(os.listdir(fd))}
        finally:
            os.close(fd)

    @staticmethod
    def decode(raw: bytes):
        def unique(pairs):
            result = {}
            for k, v in pairs:
                require(k not in result, 'JSON重复键')
                result[k] = v
            return result
        return json.loads(raw, object_pairs_hook=unique)

    def allowed_original(self, value: str) -> bool:
        p = self.path(value)
        return value in self.s['read_exact_files'] or any(
            p.is_relative_to(Path(root)) for root in self.s['read_roots'])

    def read(self, value: str | Path, *, indexed: bool = True) -> bytes:
        p = str(self.path(value))
        records = self.inputs if indexed else self.bound
        require(p in records, '未冻结读取路径: ' + p)
        raw, observed = self.file_bytes(Path(self.relocations.get(p, p)))
        expected = records[p]
        require(all(observed[k] == v for k, v in expected.items()
                    if k in ('sha256', 'bytes', 'source_mode')), '输入hash/size/mode不符: ' + p)
        self.checked[p] = observed
        return raw

    def data(self, value: str | Path):
        return self.decode(self.read(value, indexed=str(value) not in self.bound))

    def local_path(self, value: str | Path, root: Path) -> Path:
        p = self.path(value)
        require(p.is_relative_to(root) and p != root, '资格/输出路径越界')
        return p

    def local_json(self, value: str):
        p = self.local_path(value, self.c / 'fixtures')
        return self.decode(self.file_bytes(p)[0])

    def check_bindings(self) -> tuple[dict, dict, dict]:
        for p in self.bound:
            self.read(p, indexed=False)
        freeze = self.data(self.s['sealed_files']['freeze']['path'])
        review = self.data(self.s['review_approval_file'])
        grant = self.data(self.s['parent_grant_file'])
        require(freeze['candidate_revision'] == self.s['candidate_revision']
                and freeze['candidate_parent'] == review['reviewed_revision']
                and freeze['source_manifest_sha256'] == self.s['source_manifest_sha256']
                and freeze['repair_grant_sha256'] == self.s['parent_grant_sha256']
                and freeze['review_sha256'] == self.s['review_approval_sha256']
                and not freeze['semantic_approval'] and not freeze['downstream_authorized'],
                '固定修复候选/freeze不匹配')
        require(review['verdict'] == 'CHANGES_REQUESTED'
                and review['approved_scope'] == self.s['approved_scope']
                and review['required_checks'] == self.s['required_checks']
                and 'ProductGate' in review['approved_scope'][3]
                and grant['review_sha256'] == self.s['review_approval_sha256']
                and grant['source_head'] == self.s['source_head'], '审查/许可绑定不匹配')
        sealed = self.s['sealed_files']
        require(grant['base_sha256'] == sealed['base']['sha256'] == self.s['base_sha256']
                and grant['plan_approval_sha256'] == sealed['plan_approval']['sha256']
                and grant['authorization_sha256'] == sealed['authorization']['sha256'],
                '父grant/BASE/方案/授权绑定不匹配')
        result = self.data(sealed['sol_result']['path'])
        require(result['phase'] == 'repair' and result['status'] == 'MILESTONE_COMPLETE'
                and 'REPAIR_01_COMPLETE' in result['summary'], '冻结repair交付身份不符')
        base = self.data(self.s['base_file'])
        require(base['status'] == 'BASE-QUALIFIED'
                and base['source']['revision'] == self.s['source_head']
                and base['source']['driver_sha256'] == '1db2ad0c887da8a743c9f0724750cc8f6c31d1124e2f05f7b727373a92646db5'
                and base['identity']['uid'] == self.s['execution_uid'], 'BASE身份不匹配')
        actual_base = self.product_root / 'evidence/prep-base/BASE-QUALIFIED.json'
        require(self.digest(self.read(actual_base)) == self.s['base_sha256']
                and self.data(actual_base) == base, '受测子根BASE非原字节')
        require(self.digest(self.read(base['rootfs']['manifest'])) == base['rootfs']['manifest_sha256'],
                'rootfs manifest不匹配')
        rootfs = self.data(base['rootfs']['manifest'])
        require(len(rootfs['files']) == base['rootfs']['files'] == 915, 'rootfs闭包不符')
        for name, item in rootfs['files'].items():
            p = str(Path(base['rootfs']['path']) / name)
            self.read(p)
            require(self.checked[p]['sha256'] == item['sha256']
                    and self.checked[p]['bytes'] == item['bytes']
                    and self.checked[p]['source_mode'] == item['mode'], 'rootfs字节/模式变化')
        profile = self.data(self.n / 'audit-02/profile-binding.json')
        self.verify_profile(profile, base)
        self.old_profile = self.data(self.old_e / 'materials/d1-os/profile.json')
        require(self.old_profile['rootfs_manifest_sha256'] == profile['rootfs_manifest_sha256']
                and self.old_profile['seccomp_program_sha256'] == profile['seccomp_program_sha256']
                and self.old_profile['bwrap_sha256'] == profile['binary_sha256'], '旧安全材料身份不闭合')
        return grant, base, profile


    def verify_profile(self, profile: dict, base: dict) -> None:
        require(profile['rootfs_manifest_sha256'] == base['rootfs']['manifest_sha256']
                and profile['rootfs'] == base['rootfs']['path']
                and profile['base_sha256'] == self.s['base_sha256']
                and profile['binary_sha256'] == base['namespace']['sha256']
                and profile['cgroup_root'] == self.s['resource_metadata']['cgroup_root']
                and profile['material'] == str(self.product_root / 'materials/d1-os')
                and profile['evidence_root'] == str(self.product_root / 'evidence/d1-os')
                and profile['limits'] == {'wall': 20, 'cpu_seconds': 2, 'cpu_max': '50000 100000',
                                         'memory': 134217728, 'pids': 16, 'output': 262144},
                'profile/BASE/有限执行条件不匹配')
        code = profile['code_files']
        frozen = self.data(self.s['source_manifest_file'])
        require(set(code) == {n for n in frozen if n.startswith('src/krim_agent/execution/')}
                and len(code) == 6, '六execution消费者集合不匹配')
        for name, digest in code.items():
            require(digest == frozen[name]['sha256']
                    and self.digest(self.read(self.e / 'source' / name)) == digest,
                    'profile消费者不匹配: ' + name)


    def check_sources(self, manifests: list[dict]) -> dict:
        frozen = self.data(self.s['source_manifest_file'])
        protected = self.data(self.s['protected_source_file'])
        require(len(manifests) == 3 and all(set(m) == set(frozen) for m in manifests), '阶段source集合不符')
        docs = {'docs/agent-core/AC-M2-ADR.md', 'docs/agent-core/AC-M2-REPORT.md', 'docs/agent-core/SOURCES.md'}
        python = {'src/krim_agent/execution/linux.py', 'src/krim_agent/execution/_broker.py',
                  'tests/agent_core/test_sandbox.py', 'tests/agent_core/fixtures/sandbox_cases.py'}
        delta = {n for n in frozen if manifests[2][n] != frozen[n]}
        require(delta == docs, '最终受测到冻结候选不止三份文档变化')
        require(all({n for n in frozen if m[n] != frozen[n]} <= docs | python for m in manifests),
                '早期阶段超出修复文件差异')
        require(manifests[0]['tests/agent_core/test_sandbox.py'] == manifests[1]['tests/agent_core/test_sandbox.py'],
                'RED/GREEN测试字节不是同一份')
        old = self.old_manifests[0]
        for n in ['src/krim_agent/execution/linux.py', 'src/krim_agent/execution/_broker.py']:
            require(manifests[0][n] == old[n], 'RED不是未修产品字节')
        require(set(protected) == set(frozen) - {SOURCE_ADD} and len(protected) == 1612,
                '本轮保护source集合不匹配')
        current = {}
        for n in frozen:
            original = self.e / 'source' / n
            self.read(original)
            require(self.checked[str(original)] == frozen[n], '父repair/source并非冻结候选')
            _, a = self.file_bytes(Path(self.s['source']) / n)
            _, b = self.file_bytes(self.c / 'source' / n)
            require(a == b and a['source_mode'] == frozen[n]['source_mode'], 'S/C字节/模式不匹配: ' + n)
            if n != SOURCE_ADD:
                require(a == protected[n], '保护文件变化: ' + n)
            current[n] = a
        driver = self.file_bytes(self.c / 'source' / SOURCE_ADD)[0]
        self.verify_mask(self.read(self.s['sealed_files']['original_driver']['path'], indexed=False), driver)
        require(self.file_bytes(Path(__file__))[1]['sha256'] == current[SOURCE_ADD]['sha256'], '实际driver身份不符')
        # 整体实现digest确有变化；只对原批准的不变安全切片继承旧动态范围。
        inherited = []
        for n in ['src/krim_agent/execution/__init__.py', 'src/krim_agent/execution/cgroups.py',
                  'src/krim_agent/execution/seccomp.py', 'src/krim_agent/execution/types.py']:
            require(old[n] == frozen[n], '所继承安全模块并非逐字不变')
            inherited.append({'path': n, 'sha256': frozen[n]['sha256'], 'scope': 'whole file byte-identical'})
        for n, allowed in [('src/krim_agent/execution/linux.py', {'run', 'close', '_close', '_cleanup', '_execute'}),
                           ('src/krim_agent/execution/_broker.py', {'reap', 'main'})]:
            def outside(raw):
                lines = raw.splitlines(keepends=True)
                tree = ast.parse(raw)
                cls = next((x for x in tree.body if isinstance(x, ast.ClassDef) and x.name == 'SandboxRunner'), None)
                ranges = [(x.lineno - 1, x.end_lineno) for x in (cls.body if cls else tree.body)
                          if isinstance(x, (ast.FunctionDef, ast.AsyncFunctionDef)) and x.name in allowed]
                if cls:
                    ranges.append((next(i for i, line in enumerate(lines) if line.startswith(b'        self.jobs =')),
                                   next(i + 1 for i, line in enumerate(lines) if line.startswith(b'        self.closing ='))))
                result, cursor = [], 0
                for start, end in sorted(ranges):
                    result.append(b''.join(lines[cursor:start])); cursor = end
                result.append(b''.join(lines[cursor:]))
                return result
            a = outside(self.read(self.old_e / 'source' / n))
            b = outside(self.read(self.e / 'source' / n))
            require(a == b, '旧门禁继承安全切片发生变化: ' + n)
            inherited.append({'path': n, 'scope': 'only bytes outside exact repair methods/job-state region',
                              'excluded_changed_methods': sorted(allowed), 'segment_hashes': [self.digest(x) for x in a]})
        write_json(self.run / 'source-manifest.json', current)
        return {'candidate_revision': self.s['candidate_revision'], 'candidate_manifest_sha256': self.s['source_manifest_sha256'],
                'current_manifest_sha256': self.file_bytes(self.run / 'source-manifest.json')[1]['sha256'],
                'driver_sha256': current[SOURCE_ADD]['sha256'], 'changed': [SOURCE_ADD],
                'protected_source_count': 1612, 'source_count': 1613, 'final_execution_doc_only_delta': sorted(delta),
                'inherited_safety_slices': inherited, 'whole_runtime_digest_changed': True,
                'new_revision_pending_release': True}


    def verify_mask(self, old: bytes, new: bytes) -> None:
        mask = self.data(self.s['symbol_mask_file'])
        def parts(raw):
            lines = raw.splitlines(keepends=True)
            tree = ast.parse(raw)
            cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == 'ProductGate')
            methods = {n.name: b''.join(lines[min([n.lineno] + [d.lineno for d in n.decorator_list]) - 1:n.end_lineno])
                       for n in cls.body if isinstance(n, ast.FunctionDef)}
            return (b''.join(lines[:cls.lineno - 1]), b''.join(lines[cls.end_lineno:]), methods, tree)
        a, b = parts(old), parts(new)
        require(a[:2] == b[:2] and self.digest(b[0]) == mask['prefix_sha256']
                and self.digest(b[1]) == mask['suffix_sha256'], 'ProductGate class外字节变化')
        for name, digest in mask['protected_methods'].items():
            require(name in b[2] and b[2][name] == a[2][name] and self.digest(b[2][name]) == digest,
                    '保护安全helper变化: ' + name)
        symbols = [n.name if isinstance(n, (ast.ClassDef, ast.FunctionDef)) else type(n).__name__ for n in b[3].body]
        require(symbols == mask['top_level_symbols'], '顶层符号变化')
        cls = next(n for n in b[3].body if isinstance(n, ast.ClassDef) and n.name == 'ProductGate')
        forbidden = {'subprocess', 'signal', 'Preparation', 'Runner', 'status', 'fds', 'ctypes'}
        require(not any(isinstance(n, ast.Name) and n.id in forbidden for n in ast.walk(cls)), '聚合器含执行/动态ABI通道')


    @staticmethod
    def literal(node, names):
        if isinstance(node, ast.Name):
            require(node.id in names, '静态参数引用未定义')
            return names[node.id]
        if isinstance(node, (ast.List, ast.Tuple)):
            result = []
            for n in node.elts:
                if isinstance(n, ast.Starred):
                    result.extend(ProductGate.literal(n.value, names))
                else:
                    result.append(ProductGate.literal(n, names))
            return result
        return ast.literal_eval(node)

    def collection(self) -> tuple[list[str], str]:
        frozen = self.data(self.old_e / 'materials/d1-os/collection-26.json')
        require(frozen['source_manifest'] == str(self.old_e / 'evidence/d1-os/collect-26/source-manifest.json')
                and self.digest(self.read(frozen['source_manifest'])) == frozen['source_sha256'], '旧collection/source不闭合')
        names = {}
        trees = [ast.parse(self.read(self.old_e / 'source/tests/agent_core/fixtures/sandbox_cases.py')),
                 ast.parse(self.read(self.old_e / 'source/tests/agent_core/test_sandbox.py'))]
        for tree in trees:
            for n in tree.body:
                if isinstance(n, ast.Assign) and isinstance(n.targets[0], ast.Name):
                    name = n.targets[0].id
                    if name in {'DENIED', 'CASES', 'CANCEL_PHASES', 'FENCES', 'EXPORT_CASES'}:
                        names[name] = self.literal(n.value, names)
        expected = []
        for n in trees[1].body:
            if isinstance(n, ast.AsyncFunctionDef) and n.decorator_list:
                require(len(n.decorator_list) == 1, '旧参数decorator不符')
                d = n.decorator_list[0]
                require(ast.unparse(d.func) == 'pytest.mark.parametrize', '旧收集器不符')
                expected.extend('tests/agent_core/test_sandbox.py::' + n.name + '[' + v + ']'
                                for v in self.literal(d.args[1], names))
        require(len(expected) == len(set(expected)) == 78 and set(expected) == set(frozen['nodeids'])
                and len(frozen['nodeids']) == 78
                and {n.split('[')[0] for n in expected} == set(self.prior['required_node_ids']), '旧78参数集合不符')
        return frozen['nodeids'], frozen['source_sha256']


    def verify_command(self, cmd: dict, run: Path, selectors: list[str], digest: str) -> None:
        old = run.is_relative_to(self.old_e)
        label = run.name
        suffix = ['-W', 'error', '-x'] if old else [
            '--basetemp=' + str(self.e / 'tmp' / ('recovery01-pytest-' + label)), '-W', 'error']
        expected = [str(self.old_e / 'dev-env/bin/python'), '-B', '-m', 'pytest', *selectors,
                    '-q', '--junitxml=' + str(run / 'results.xml'), *suffix]
        env = {'PATH': '/usr/local/bin:/usr/bin:/bin', 'LD_LIBRARY_PATH': str(self.old_e / 'python/lib'),
               'TMPDIR': str((self.old_e if old else self.e) / 'tmp'), 'PYTHONDONTWRITEBYTECODE': '1',
               'LANG': 'C.UTF-8', 'KRIM_D1_EVIDENCE': str(run if old else self.product_root / 'evidence/d1-os' / label)}
        if not old:
            env['KRIM_D1_REPAIR_ROOT'] = str(self.product_root)
            require(selectors == self.s['new_required_selectors'][label], '新固定参数集合不匹配')
        require(cmd['rc'] == (1 if label == 'red-02' else 0) and cmd['argv'] == expected and cmd['env'] == env
                and cmd['cwd'] == str((self.old_e if old else self.e) / 'source')
                and cmd['source_manifest_sha256'] == digest and type(cmd['start_ns']) is int
                and cmd['end_ns'] > cmd['start_ns'], '命令rc/argv/cwd/env/time/source不匹配')
        if not old:
            plan_path = self.n / 'plans' / ('plan-' + label + '.json')
            require(cmd['test_plan'] == str(plan_path)
                    and cmd['test_plan_sha256'] == self.digest(self.read(plan_path)), '命令计划hash不闭合')
            require(cmd['waited'] is True and cmd['pidfd_closed'] is True and cmd['pid'] > 0,
                    '新pytest进程未实际wait/关闭pidfd')


    @staticmethod
    def junit(raw: bytes, *, red: bool = False) -> list[str]:
        require(b'<!DOCTYPE' not in raw and b'<!ENTITY' not in raw, 'JUnit不允许DTD/ENTITY')
        root = ET.fromstring(raw)
        require(root.tag == 'testsuites' and len(root) == 1 and root[0].tag == 'testsuite', 'JUnit根schema不符')
        suite = root[0]
        cases = suite.findall('testcase')
        require(int(suite.attrib['tests']) == len(cases)
                and int(suite.attrib.get('failures', 0)) == (len(cases) if red else 0)
                and all(int(suite.attrib.get(k, 0)) == 0 for k in ['errors', 'skipped']), 'JUnit结果矩阵不符')
        nodes = []
        for case in cases:
            require(set(case.attrib) == {'classname', 'name', 'time'}
                    and case.attrib['classname'] == 'tests.agent_core.test_sandbox', 'JUnit消费者不符')
            if red:
                require(len(case) == 1 and case[0].tag == 'failure'
                        and 'AssertionError' in (case[0].text or '')
                        and 'AssertionError' in case[0].attrib.get('message', ''), 'RED不是指定行为断言')
            else:
                require(not list(case), 'GREEN含失败/skip/xfail/xpass/重试内容')
            nodes.append(case.attrib['classname'].replace('.', '/') + '.py::' + case.attrib['name'])
        require(len(nodes) == len(set(nodes)), 'JUnit重复node')
        return nodes


    def check_suites(self) -> tuple[list[dict], list[dict]]:
        collected, digest = self.collection()
        self.old_manifests, self.stage_manifests, rows, covered = [], {}, [], []
        selectors = self.prior['required_node_ids']
        for label, chosen, count in [('final-ownership-27', [selectors[1], selectors[3], selectors[2]], 35),
                                     ('final-isolation-28', [selectors[0]], 43)]:
            run = self.old_e / 'evidence/d1-os' / label
            cmd = self.data(run / 'command.json')
            self.verify_command(cmd, run, chosen, digest)
            mr = self.read(run / 'source-manifest.json')
            require(self.digest(mr) == digest, '旧执行manifest不匹配')
            manifest = self.decode(mr)
            for consumer in self.prior['consumer_paths']:
                require(self.digest(self.read(run / 'source' / consumer)) == manifest[consumer]['sha256'], '旧消费者快照不符')
            nodes = self.junit(self.read(run / 'results.xml'))
            require(len(nodes) == count and set(nodes) == {n for n in collected if n.split('[')[0] in chosen}, '旧参数缺失')
            out = self.read(run / 'stdout').decode()
            require(re.fullmatch(r'\.+\s+\[100%\]\n' + str(count) + r' passed in [\d.]+s \([\d:]+\)\n', out)
                    and not self.read(run / 'stderr'), '旧stdout/stderr非完整通过')
            native = self.data(self.old_e / 'evidence/d1-os/channel-status' / (label + '.json'))
            require(native['status'] == 'exited' and native['exit_code'] == 0
                    and native['started_at'] <= cmd['start_ns'] / 1e9
                    and native['finished_at'] >= cmd['end_ns'] / 1e9, '旧native终态不符')
            self.old_manifests.append(manifest); covered.extend(nodes)
            rows.append({'layer': 'legacy', 'run': str(run), 'command': cmd, 'nodeids': nodes})
        require(self.old_manifests[0] == self.old_manifests[1]
                and len(covered) == len(set(covered)) == 78 and set(covered) == set(collected), '旧78覆盖不符')
        for label, count in [('red-02', 6), ('green-02', 6), ('final-01', 25)]:
            run = self.n / 'commands' / label
            mr = self.read(run / 'source-manifest.json'); manifest = self.decode(mr)
            plan = self.data(self.n / 'plans' / ('plan-' + label + '.json'))
            chosen = self.s['new_required_selectors'][label]
            require(len(chosen) == len(set(chosen)) == count and plan['selectors'] == chosen
                    and plan['source_manifest_sha256'] == self.digest(mr), '新冻结计划/参数/source不符')
            cmd = self.data(run / 'command.json')
            self.verify_command(cmd, run, chosen, self.digest(mr))
            for consumer in self.prior['consumer_paths']:
                p = str(run / 'source' / consumer)
                if p in self.inputs:
                    require(self.digest(self.read(p)) == manifest[consumer]['sha256'], '新消费者快照不符')
                else:
                    require(manifest[consumer] == self.data(self.s['source_manifest_file'])[consumer],
                            '未保存的消费者不是未变字节')
            nodes = self.junit(self.read(run / 'results.xml'), red=label == 'red-02')
            require(len(nodes) == count and set(nodes) == set(chosen), '新JUnit缺失/多余参数')
            stdout = self.read(run / 'stdout').decode()
            require(not self.read(run / 'stderr') and not any(x in stdout for x in
                    ['SAWarning', 'Task was destroyed', 'ResourceWarning', 'never retrieved', 'unraisable exception']),
                    '新pytest stderr或资源异常')
            require(re.search(r'\b' + str(count) + (' failed in ' if label == 'red-02' else ' passed in '), stdout),
                    '新stdout汇总不符')
            native_path = self.n / 'channel' / ('status-red02' if label == 'red-02' else label) / 'stdout'
            native = self.decode(self.read(native_path).splitlines()[-1])
            require(native['status'] == 'exited' and native['exit_code'] == cmd['rc']
                    and native['id'] == ('ac-m2-repair-recovery01-red02' if label == 'red-02' else 'ac-m2-repair-recovery01-' + label)
                    and native['started_at'] <= cmd['start_ns'] / 1e9
                    and native['finished_at'] >= cmd['end_ns'] / 1e9, '新native终态/时间不符')
            self.stage_manifests[label] = manifest
            rows.append({'layer': 'repair', 'stage': label, 'run': str(run), 'command': cmd,
                         'nodeids': nodes, 'native_job': native,
                         'pure_state_parameters': [n for n in nodes if 'wait_status_boundary' in n]})
        return list(self.stage_manifests.values()), rows


    @staticmethod
    def one(rows: list[dict], kind: str) -> dict:
        found = [r for r in rows if r['kind'] == kind]
        require(len(found) == 1, '资源事件缺失/重复: ' + kind)
        return found[0]

    def resource_job(self, path: str, profile: dict, base: dict) -> dict:
        if Path(path).is_relative_to(self.n):
            raw = self.read(path)
            require(raw.endswith(b'\n'), '新资源JSONL截断')
            rows = [self.decode(line) for line in raw.splitlines()]
            require([r['monotonic_ns'] for r in rows] == sorted(r['monotonic_ns'] for r in rows), '新资源顺序不符')
            identity = {k: v for k, v in self.one(rows, 'leaf_created').items() if k not in ('kind', 'monotonic_ns')}
            require((identity['root_dev'], identity['root_inode'], identity['uid'], identity['boot']) ==
                    (self.s['resource_metadata']['root_dev'], self.s['resource_metadata']['root_inode'],
                     self.s['execution_uid'], base['identity']['boot_id'])
                    and re.fullmatch(r'krim-d1-[0-9a-f]{32}', identity['name']), '新leaf身份不符')
            stage = Path(path).relative_to(self.product_root / 'evidence/d1-os').parts[0]
            require(stage in self.stage_manifests, '资源不属于固定三阶段')
            made = [r for r in rows if r['kind'] == 'process_created']
            waited = [r for r in rows if r['kind'] == 'process_waited']
            closed = [r for r in rows if r['kind'] == 'pidfd_closed']
            gates = [r for r in rows if r['kind'] == 'gate_opened']
            require(len(made) <= 1 and len(gates) <= 1, '资源重复创建或重复gate')
            material = self.product_root / 'materials/d1-os' / Path(path).parent.name
            assets = {n: self.digest(self.read(material / n)) for n in ['broker.py', 'filter.bin', 'request.json']}
            require(assets['broker.py'] == self.stage_manifests[stage]['src/krim_agent/execution/_broker.py']['sha256'],
                    '保存broker与本stage源码不符')
            if made:
                joined = self.one(rows, 'joined')
                require(made[0]['pid'] > 0 and made[0]['pidfd'] >= 0 and joined['pid'] == made[0]['pid']
                        and joined['monotonic_ns'] > made[0]['monotonic_ns'], '新Popen/join身份不符')
                cmd = self.one(rows, 'command')
                require(cmd['cwd'] == str(self.e / 'source')
                        and cmd['argv'][0] == str(self.old_e / 'dev-env/bin/python')
                        and cmd['argv'][-4:] == ['/opt/python/bin/python3.12', '-I', '-B', '/broker.py']
                        and cmd['env'] == {'PATH': '/usr/local/bin:/usr/bin:/bin',
                                           'LD_LIBRARY_PATH': str(self.old_e / 'python/lib'), 'LANG': 'C.UTF-8'},
                        '新资源实际命令/环境不符')
            if gates:
                require(assets['filter.bin'] == profile['seccomp_program_sha256']
                        and self.data(material / 'request.json')['output'] == 262144, '实际gate材料不匹配')
            result = {'path': path, 'stage': stage, 'identity': identity, 'made': made,
                      'wait': waited, 'pidfd_closed_events': len(closed), 'assets': assets}
            if '/restart-' in path:
                require(len(made) == 1 and not waited and not closed
                        and not any(r['kind'] in {'cleanup_complete', 'leaf_removed'} for r in rows), '新SIGKILL补造内部终态')
                parent = Path(path).parent.parent
                worker = self.data(parent / 'worker-command.json')
                require(worker['argv'] == [str(self.old_e / 'dev-env/bin/python'), '-B',
                        'tests/agent_core/fixtures/sandbox_cases.py', str(self.product_root), str(parent),
                        parent.name.removeprefix('restart-')]
                        and worker['cwd'] == str(self.e / 'source') and worker['pid'] > 0
                        and worker['exit_code'] == -9 and worker['waited'] is True
                        and worker['pidfd_closed'] is True and worker['pidfd'] >= 0
                        and worker['started_ns'] < made[0]['monotonic_ns'] < worker['finished_ns'], '新worker外部收束不符')
                recovery = self.data(parent / 'recovery.json')
                require(len(recovery) == 1, '新recovery数量不符')
                r = recovery[0]
                require(r['identity'] == identity and r['leaf'] == identity['name']
                        and r['exit_code'] is None and r['status'] == 'unknown'
                        and r['cleanup'] == 'clean' and r['leaf_removed'] is True
                        and 'populated 0\n' in r['events_after'], '新SIGKILL unknown/清理被改写')
                require(str(Path(path).parent / 'receipt.json') not in self.inputs, '新SIGKILL伪内部receipt')
                result.update(kind='repair-SIGKILL', worker=worker, recovery=r,
                              internal_exit_code=None, internal_finally_observed=False)
                return result
            receipt = self.data(Path(path).parent / 'receipt.json')
            if stage == 'red-02' and '/cleanup-fault-' in path:
                seam = Path(path).parent.parent.name.removeprefix('cleanup-fault-')
                require(seam in {'wait', 'leaf'}, 'RED故障窗口不符')
                em = self.data(Path(path).parent.parent / ('cleanup-fault-' + seam + '.json'))
                require(len(made) == 1 and em['emergency_cleanup'] is True and em['fd_returned'] is True
                        and em['tickets_empty'] is True and em['leaf_absent'] is True
                        and receipt['cleanup'] == 'unknown'
                        and not any(r['kind'] == 'cleanup_complete' for r in rows), 'RED内部unknown/外部finally不符')
                if seam == 'wait':
                    require(not waited and not closed and em['pid'] == made[0]['pid']
                            and em['waited_rc'] == -9 and em['emergency_pidfd_closed'] is True, 'RED wait外部配对缺失')
                else:
                    require(len(waited) == len(closed) == 1 and waited[0]['pid'] == made[0]['pid'], 'RED leaf原wait/关闭缺失')
                require('populated 0\n' in self.one(rows, 'populated_zero')['events']
                        and self.one(rows, 'leaf_removed'), 'RED finally叶未收束')
                result.update(kind='repair-RED-finally', receipt=receipt, emergency=em,
                              internal_cleanup_success_not_invented=True)
                return result
            require(receipt['cleanup'] == 'clean' and receipt['gate_sent'] is bool(gates)
                    and len(made) == len(waited) == len(closed), 'GREEN/最终正常资源未收束')
            if made:
                require(waited[0]['pid'] == made[0]['pid'] and type(waited[0]['rc']) is int
                        and made[0]['monotonic_ns'] < waited[0]['monotonic_ns'] < self.one(rows, 'pidfd_closed')['monotonic_ns'],
                        '新正常wait/关闭顺序不符')
            times = [self.one(rows, k)['monotonic_ns'] for k in ['cgroup_kill', 'populated_zero', 'leaf_removed', 'cleanup_complete']]
            require(times == sorted(times) and rows[-1]['kind'] == 'cleanup_complete'
                    and 'populated 0\n' in self.one(rows, 'populated_zero')['events'], '新正常清理链不符')
            if assets['filter.bin'] != profile['seccomp_program_sha256']:
                require(not gates and receipt['status'] == 'failed', '篡改BPF未在gate前拒绝')
            if stage != 'red-02':
                for row in rows:
                    if row['kind'] == 'payload_exit' and row['message']['rc'] is not None:
                        msg = row['message']
                        require(msg['waited_pid'] > 1 and os.waitstatus_to_exitcode(msg['wait_status']) == msg['rc'], '新payload rc无真实wait来源')
            result.update(kind='repair-normal', receipt=receipt)
            return result
        raw = self.read(path)
        require(raw.endswith(b'\n'), '资源JSONL截断')
        rows = [self.decode(line) for line in raw.splitlines()]
        require(all(type(r['monotonic_ns']) is int for r in rows)
                and [r['monotonic_ns'] for r in rows] == sorted(r['monotonic_ns'] for r in rows),
                '资源时间顺序不符')
        identity = {k: v for k, v in self.one(rows, 'leaf_created').items()
                    if k not in ('kind', 'monotonic_ns')}
        require(identity['root_dev'] == self.s['resource_metadata']['root_dev']
                and identity['root_inode'] == self.s['resource_metadata']['root_inode']
                and identity['uid'] == self.s['execution_uid']
                and identity['boot'] == base['identity']['boot_id']
                and re.fullmatch(r'krim-d1-[0-9a-f]{32}', identity['name']) is not None,
                'leaf根/boot/uid身份不符')
        made = [r for r in rows if r['kind'] == 'process_created']
        waited = [r for r in rows if r['kind'] == 'process_waited']
        require(len(made) <= 1, '同资源作业重复Popen')
        crash = '/restart-' in path
        if made:
            require(made[0]['pid'] > 0 and made[0]['pidfd'] >= 0, 'Popen/pidfd身份不符')
            joined = self.one(rows, 'joined')
            require(joined['pid'] == made[0]['pid']
                    and joined['monotonic_ns'] > made[0]['monotonic_ns'], 'Popen/cgroup加入身份不符')
            command = self.one(rows, 'command')
            require(command['cwd'] == str(self.old_e / 'source')
                    and command['argv'][0] == str(self.old_e / 'dev-env/bin/python')
                    and command['argv'][-4:] == ['/opt/python/bin/python3.12', '-I', '-B', '/broker.py'],
                    '资源命令消费者不符')
            material = self.old_e / 'materials/d1-os' / Path(path).parent.name
            if not crash:
                require(self.digest(self.read(material / 'broker.py')) == profile['broker_sha256'],
                        '保存broker不是已冻结消费者')
            if not crash and any(r['kind'] == 'gate_opened' for r in rows):
                require(self.digest(self.read(material / 'filter.bin')) == profile['seccomp_program_sha256'],
                        '实际gate打开的BPF不匹配')
                request = self.data(material / 'request.json')
                require(set(request) == {'argv', 'cwd', 'output'} and request['cwd'] == '.'
                        and request['output'] == profile['output_bytes']
                        and isinstance(request['argv'], list), '实际gate请求不匹配')
        if crash:
            require(len(made) == 1 and not waited
                    and not any(r['kind'] in {'pidfd_closed', 'cleanup_complete', 'leaf_removed'}
                                for r in rows), 'SIGKILL内部终态被补造')
            parent = Path(path).parent.parent
            worker = self.data(parent / 'worker-command.json')
            window = parent.name.removeprefix('restart-')
            require(worker['argv'] == [str(self.old_e / 'dev-env/bin/python'), '-B',
                    'tests/agent_core/fixtures/sandbox_cases.py', str(self.old_e), str(parent), window]
                    and worker['cwd'] == str(self.old_e / 'source') and worker['pid'] > 0
                    and worker['pidfd'] >= 0 and worker['exit_code'] == -9
                    and worker['waited'] is True and worker['pidfd_closed'] is True
                    and worker['started_ns'] < made[0]['monotonic_ns'] < worker['finished_ns'],
                    'SIGKILL外部worker wait/pidfd未闭合')
            recovery = self.data(parent / 'recovery.json')
            require(len(recovery) == 1, 'SIGKILL recovery数不符')
            r = recovery[0]
            require(r['identity'] == identity and r['leaf'] == identity['name']
                    and r['exit_code'] is None and r['status'] == 'unknown'
                    and r['cleanup'] == 'clean' and r['leaf_removed'] is True
                    and 'populated 0\n' in r['events_after'], 'SIGKILL unknown/清理/身份不符')
            require(str(Path(path).parent / 'receipt.json') not in self.inputs, '故障worker不能补造内部receipt')
            return {'path': path, 'kind': 'SIGKILL', 'identity': identity,
                    'external_worker': worker, 'recovery': r, 'internal_exit_code': None,
                    'internal_status': 'unknown', 'internal_finally_observed': False,
                    'material_binding': 'crash保存broker/filter/request未在冻结exact inputs中；仅验证原command及受测实现，不读取未授权路径或声称已核对故障材料字节'}
        receipt = self.data(Path(path).parent / 'receipt.json')
        require(receipt['status'] in {'exited', 'failed', 'cancelled', 'timeout'}
                and receipt['cleanup'] == 'clean', '正常receipt未收束')
        gates = [r for r in rows if r['kind'] == 'gate_opened']
        require(len(gates) <= 1 and receipt['gate_sent'] is bool(gates), 'receipt/gate事件不符')
        require(len(made) == len(waited), 'Popen/wait缺配对')
        if made:
            require(waited[0]['pid'] == made[0]['pid']
                    and type(waited[0]['rc']) is int
                    and made[0]['monotonic_ns'] < waited[0]['monotonic_ns'], 'wait不是创建的进程')
            closed = self.one(rows, 'pidfd_closed')
            require(closed['monotonic_ns'] > waited[0]['monotonic_ns'], 'pidfd在wait前关闭')
        else:
            require(not any(r['kind'] == 'pidfd_closed' for r in rows), '零Popen却有pidfd关闭')
        kinds = ['cgroup_kill', 'populated_zero', 'leaf_removed', 'cleanup_complete']
        times = [self.one(rows, k)['monotonic_ns'] for k in kinds]
        require(times == sorted(times) and rows[-1]['kind'] == 'cleanup_complete'
                and 'populated 0\n' in self.one(rows, 'populated_zero')['events'], '清理链缺口')
        return {'path': path, 'kind': 'normal', 'identity': identity,
                'Popen': len(made), 'wait': waited, 'receipt': receipt}


    def check_resources(self, profile: dict, base: dict) -> dict:
        old_roots = [str(self.old_e / 'evidence/d1-os' / name) + '/'
                     for name in ['final-ownership-27', 'final-isolation-28']]
        old_files = sorted(p for p in self.inputs if any(p.startswith(r) for r in old_roots) and p.endswith('/resources.jsonl'))
        require(len(old_files) == 75, '旧资源集合缺失')
        old_rows = [self.resource_job(p, self.old_profile, base) for p in old_files]
        require(sum(r['kind'] == 'normal' for r in old_rows) == 73
                and sum(r.get('Popen', 0) for r in old_rows) == 71
                and sum(r['kind'] == 'SIGKILL' for r in old_rows) == 2, '旧资源分类不符')
        new_roots = [str(self.product_root / 'evidence/d1-os' / name) + '/'
                     for name in ['red-02', 'green-02', 'final-01']]
        files = sorted(p for p in self.inputs if any(p.startswith(r) for r in new_roots) and p.endswith('/resources.jsonl'))
        require(len(files) == 41, '新41资源集合缺失')
        rows = [self.resource_job(p, profile, base) for p in files]
        require({n: sum(r['stage'] == n for r in rows) for n in self.stage_manifests} ==
                {'red-02': 3, 'green-02': 5, 'final-01': 33}
                and sum(r['kind'] == 'repair-RED-finally' for r in rows) == 2
                and sum(r['kind'] == 'repair-SIGKILL' for r in rows) == 2, '新资源矩阵不符')
        self.current_resources(old_rows + rows)
        write_json(self.run / 'resource-verification.json', {'legacy': old_rows, 'repair': rows})
        return {'legacy_raw_jobs': 75, 'repair_raw_jobs': 41, 'RED_finally_records': 2,
                'new_crash_unknown': [r for r in rows if r['kind'] == 'repair-SIGKILL'],
                'old_crash_unknown': [r for r in old_rows if r['kind'] == 'SIGKILL'],
                'resource_records': str(self.run / 'resource-verification.json'),
                'FD_Task_boundary': 'frozen source assertions + passed JUnit; no fabricated per-FD/GC events'}


    def current_resources(self, rows: list[dict]) -> None:
        meta = self.s['resource_metadata']; root = Path(meta['cgroup_root'])
        current = self.directory(root)
        require((current['dev'], current['inode'], current['uid']) ==
                (meta['root_dev'], meta['root_inode'], meta['uid']), '当前cgroup身份变化')
        require(not [n for n in current['names'] if n.startswith('krim-d1-')], '当前任务leaf有遗留')
        tickets = self.directory(Path(meta['tickets']))
        require((tickets['dev'], tickets['inode'], tickets['uid']) ==
                (meta['tickets_dev'], meta['tickets_inode'], meta['uid']) and not tickets['names'], '当前tickets不闭合')
        for row in rows:
            p = root / row['identity']['name']
            try: p.lstat()
            except FileNotFoundError: continue
            raise NotReady('已记录自有leaf仍存在: ' + str(p))


    def audit_inputs(self, label: str) -> dict:
        for p in self.inputs:
            self.read(p)
        snapshot = {p: self.checked[p] for p in self.inputs}
        write_json(self.run / ('inputs-' + label + '.json'), snapshot)
        raw = json.dumps(snapshot, sort_keys=True, separators=(',', ':')).encode()
        return {'count': len(snapshot), 'canonical_sha256': self.digest(raw),
                'frozen_input_files_sha256': self.s['input_files_sha256']}

    def aggregate(self) -> dict:
        grant, base, profile = self.check_bindings()
        before = self.audit_inputs('before')
        manifests, suites = self.check_suites()
        source = self.check_sources(manifests)
        resources = self.check_resources(profile, base)
        old_abi = self.data(self.old_e / 'evidence/d1-os/abi-29/result.json')
        require(len(old_abi['expected']) == 24 and old_abi['expected'] == old_abi['actual']
                and old_abi['native_headers_present'] is False, '旧24项ABI历史不符')
        abi_path = self.e / 'evidence/repair/abi-02/verification.json'
        abi = self.data(abi_path)
        require(len(abi['mapping']) == 149 and abi['allowed_count'] == 144 and abi['special_count'] == 5
                and len(abi['constants']) == 37 and not abi['kernel_headers_compiled']
                and not abi['native_headers_installed'] and abi['official_UAPI_texts_checked']
                and abi['upstream_commit'] == 'adc218676eef25575469234709c2d87185ca223a', '完整ABI范围不符')
        code = self.read(self.e / 'source/src/krim_agent/execution/seccomp.py')
        tree = ast.parse(code)
        allowed = next(self.literal(n.value, {}) for n in tree.body if isinstance(n, ast.Assign)
                       and isinstance(n.targets[0], ast.Name) and n.targets[0].id == 'ALLOWED')
        require({r['number'] for r in abi['mapping'] if r['policy'] == 'ALLOWED'} == set(allowed)
                and {r['number'] for r in abi['mapping'] if r['policy'] == 'special-branch'} == {56, 435, 157, 133, 259}
                and abi['policy_source_sha256'] == self.digest(code) == self.old_profile['code_files']['src/krim_agent/execution/seccomp.py'],
                'ABI数值与未变实际策略不符')
        for r in abi['mapping']:
            src = r['source']; raw = self.read(src['path'])
            require(self.digest(raw) == src['sha256'] and r['roundtrip_number'] == r['number'], 'ABI来源/解析号不符')
            fields = raw.decode().splitlines()[src['line'] - 1].split()
            require(int(fields[0]) == r['number'] and fields[1] in {'common', '64'} and fields[2] == r['uapi_name'], '官方号表行不符')
        for r in abi['constants']:
            p = self.e / 'materials/abi/linux-v6.12' / r['file']
            raw = self.read(p)
            require(self.digest(raw) == r['source_sha256'] and raw.decode().splitlines()[r['line'] - 1] == r['source_line'], 'ABI常量来源行不符')
        lint = self.data(self.n / 'lint-baseline-qualification.json')
        current_lint = self.data(self.n / 'commands/static-02/lint.command.json')
        baseline_lint = self.data(self.n / 'commands/static-02/baseline-lint.command.json')
        require(current_lint['rc'] == baseline_lint['rc'] == 1
                and self.data(self.n / 'commands/static-02/format-check.command.json')['rc'] == 0, '静态限定被改写为全绿')
        def diagnostics(name):
            counts = {}
            for d in self.data(self.n / 'commands/static-02' / (name + '.stdout')):
                p = d['filename']; line = self.read(p).decode().splitlines()[d['location']['row'] - 1].strip()
                key = (Path(p).name, d['code'], line); counts[key] = counts.get(key, 0) + 1
            return counts
        current, previous = diagnostics('lint'), diagnostics('baseline-lint')
        require(sum(current.values()) == 8 and sum(previous.values()) == 12
                and all(v <= previous.get(k, 0) for k, v in current.items())
                and lint['new_diagnostics'] == [], '静态基线限定不符')
        after = self.audit_inputs('after')
        require(before == after, '聚合前后原输入变化')
        return {'kind': 'fixed D1_OS repair ProductGate rebind; not semantic approval',
                'status': 'METADATA-QUALIFIED' if self.args.qualify else 'EVIDENCE-AGGREGATED',
                'candidate_revision': self.s['candidate_revision'], 'candidate_manifest_sha256': self.s['source_manifest_sha256'],
                'semantic_approval': False, 'downstream_authorized': False, 'semantic_review_pending': True,
                'supplement_sha256': self.SEAL, 'supplement': str(self.OWNER / 'supplement.json'),
                'parent_grant_sha256': self.s['parent_grant_sha256'], 'review_sha256': self.s['review_approval_sha256'],
                'base_sha256': self.s['base_sha256'], 'BASE_driver_sha256_unchanged': base['source']['driver_sha256'],
                'source': source, 'suites': suites, 'resources': resources,
                'repair_matrix': {'red_assertions': 6, 'green_same_tests': 6, 'final_passed_unique': 25,
                                  'do_not_sum_to_37_independent_acceptances': True,
                                  'three_exit_variants_are_safe_pure_state_boundary': True},
                'legacy_inheritance': {'original_passed_parameters': 78, 'current_candidate_did_not_rerun_78': True,
                    'scope': 'only exact unchanged security slices recorded in source; changed runtime paths use new25 regression',
                    'old_receipt_locations': [self.s['old_aggregation_receipt'], self.s['inherited_original_receipt']],
                    'old_receipts_read': False, 'reason': 'not indexed in this Supplement; preserved read-only, review and raw old evidence bound'},
                'ABI': {'old24_preserved': True, 'complete_source': str(abi_path), 'sha256': self.digest(self.read(abi_path)),
                        'allowed': 144, 'special': 5, 'constants': 37, 'kernel_headers_compiled': False,
                        'native_headers_installed': False, 'official_UAPI_texts_checked': True,
                        'no_dynamic_ABI_execution_in_rebind': True},
                'static_limitation': {'full_rc': 1, 'current_diagnostics': 8, 'baseline_diagnostics': 12,
                                      'new_diagnostics': 0, 'format_rc': 0, 'not_full_lint_green': True},
                'inputs_before': before, 'inputs_after': after, 'input_index': self.s['input_files_file'],
                'original_suite_cwd': str(self.old_e / 'source'), 'repair_suite_cwd': str(self.e / 'source'),
                'aggregation_cwd': str(self.c / 'source'), 'fixture_relocations': self.relocations,
                'formal_receipt_successor': self.s['receipt'], 'product_execution_in_this_supplement': False}


    def execute(self) -> int:
        record = {'argv': sys.argv, 'cwd': str(Path.cwd()), 'started_ns': time.time_ns(),
                  'env_whitelist': {'TMPDIR': str(self.c / 'tmp'), 'PYTHONDONTWRITEBYTECODE': '1'},
                  'identity': {'uid': os.getuid(), 'gid': os.getgid()},
                  'source_driver_sha256': self.file_bytes(Path(__file__))[1]['sha256']}
        try:
            result = self.aggregate()
            require(os.getuid() == self.s['execution_uid'], '聚合进程身份不匹配')
            # 正式收据不可指定其他位置，必须排他创建；资格永远不写通过收据。
            write_json(self.run / 'result.json', result)
            if not self.args.qualify:
                target = self.path(self.s['receipt'])
                require(target == self.c / 'OS-GATE-PASSED.json', '收据路径超界')
                self.directory(target.parent)
                write_json(target, result)
            record['rc'] = 0
            print(json.dumps({'status': result['status'], 'result': str(self.run / 'result.json'),
                              'receipt': None if self.args.qualify else self.s['receipt']}, ensure_ascii=False))
        except Exception as error:
            record['rc'] = 1
            record['error'] = {'type': type(error).__name__, 'message': str(error)}
            write_json(self.run / 'NOT_READY.json', record)
            print(json.dumps({'status': 'NOT_READY', 'error': record['error']}, ensure_ascii=False))
        finally:
            record['finished_ns'] = time.time_ns()
            write_json(self.run / 'command.json', record)
        return record['rc']


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--stage', choices=['base', 'product-gate'], required=True)
    for name in ['root', 'cgroup-root', 'rootfs', 'evidence']:
        parser.add_argument('--' + name)
    for name in ['supplement', 'seal', 'acceptance-root', 'fixture-map']:
        parser.add_argument('--' + name)
    parser.add_argument('--qualify', action='store_true')
    args = parser.parse_args()
    if args.stage == 'base':
        for name in ['root', 'cgroup_root', 'rootfs', 'evidence']:
            if getattr(args, name) is None:
                parser.error('--' + name.replace('_', '-') + ' is required for base')
        if any(getattr(args, n) for n in ['supplement', 'seal', 'acceptance_root', 'fixture_map', 'qualify']):
            parser.error('product-gate options cannot be used for base')
        return Preparation(args).execute()
    for name in ['supplement', 'seal', 'acceptance_root']:
        if getattr(args, name) is None:
            parser.error('--' + name.replace('_', '-') + ' is required for product-gate')
    if any(getattr(args, n) is not None for n in ['root', 'cgroup_root', 'rootfs', 'evidence']):
        parser.error('base material options cannot be used for product-gate')
    try:
        return ProductGate(args).execute()
    except Exception as error:
        print(json.dumps({'status': 'NOT_READY', 'error': {'type': type(error).__name__,
                                                       'message': str(error)}}, ensure_ascii=False))
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
