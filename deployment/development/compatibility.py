"""Exercise one approved Docker/runsc development image with real CLI tools.

The trusted host runs this probe. Only newly created probe directories are mounted;
no Docker socket, user credentials, existing project or host home enters the sandbox.
Service startup is separate so the preview runner can inspect the actual process.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import time
import uuid
from pathlib import Path

from krim_agent.contracts import fingerprint
from krim_agent.development.contracts import (
    AuthorizationRef,
    CommandRequest,
    EnvironmentHandle,
    RunOwnership,
)

SERVER = """const http = require('node:http');
const fs = require('node:fs');
http.createServer((req, res) => {
  if (req.url === '/health') { res.end('ready'); return; }
  if (req.url === '/bundle.js') {
    res.setHeader('Content-Type', 'text/javascript');
    res.end(fs.readFileSync('/workspace/dist/bundle.js')); return;
  }
  res.setHeader('Content-Type', 'text/html; charset=utf-8');
  res.end('<!doctype html><html><head><title>Krim environment probe</title></head>' +
    '<body><main><h1>Development environment ready</h1><p id="result"></p></main>' +
    '<script src="/bundle.js"></script></body></html>');
}).listen(5189, '0.0.0.0', () => console.log('probe listening on 5189'));
"""


class Probe:
    def __init__(self, image: str, evidence: Path) -> None:
        self.image = subprocess.check_output(
            ['docker', 'image', 'inspect', image, '--format', '{{.Id}}'], text=True,
        ).strip()
        if re.fullmatch(r'sha256:[0-9a-f]{64}', self.image) is None:
            raise ValueError('expected an installed immutable image ID')
        self.evidence = evidence
        self.evidence.mkdir(parents=True, exist_ok=True)
        self.tag = uuid.uuid4().hex[:12]
        self.root = Path('/srv/krim-sandbox') / f'coding-agent-probe-{self.tag}'
        self.containers: list[str] = []
        self.environments: dict[str, EnvironmentHandle] = {}
        self.run_number = 1
        self.quota_number: int | None = None
        self.results: list[dict[str, object]] = []
        self.requests: list[dict[str, object]] = []
        self.root.mkdir(mode=0o755)

    def command(self, label: str, argv: list[str], *, timeout: int = 120,
                expected: int = 0) -> str:
        started = time.monotonic()
        result = subprocess.run(argv, capture_output=True, text=True, timeout=timeout)
        (self.evidence / f'{label}.log').write_text(result.stdout + result.stderr)
        self.results.append({'label': label, 'command': argv, 'exit_code': result.returncode,
                             'expected_exit_code': expected,
                             'seconds': round(time.monotonic() - started, 3)})
        self.save()
        print(json.dumps(self.results[-1]), flush=True)
        if result.returncode != expected:
            raise RuntimeError(f'{label} returned {result.returncode}, expected {expected}')
        return result.stdout

    def save(self) -> None:
        (self.evidence / 'compatibility-results.json').write_text(json.dumps({
            'image': self.image, 'root': str(self.root), 'containers': self.containers,
            'steps': self.results, 'c0_requests': self.requests,
            'full_project_acceptance': False,
        }, indent=2))

    def create(self, suffix: str, directory: Path, *, port: bool = False) -> str:
        name = f'krim-dev-probe-{self.tag}-{suffix}'
        directory.mkdir(mode=0o700, exist_ok=True)
        os.chown(directory, 1000, 1000)
        args = ['docker', 'create', '--name', name, '--pull=never', '--runtime=runsc',
                '--user=1000:1000', '--cap-drop=ALL', '--security-opt=no-new-privileges',
                '--read-only', '--memory=2g', '--memory-swap=2g', '--cpus=2',
                '--pids-limit=256', '--tmpfs=/tmp:rw,nosuid,nodev,size=256m',
                '--tmpfs=/home/node:rw,nosuid,nodev,size=64m,uid=1000,gid=1000',
                '--mount', f'type=bind,source={directory},target=/workspace',
                '--env=HOME=/home/node', '--env=npm_config_cache=/workspace/.cache/npm',
                '--env=npm_config_registry=https://registry.npmjs.org',
                '--workdir=/workspace']
        if port:
            args += ['-p', '127.0.0.1:5189:5189']
        args += [self.image, 'sleep', 'infinity']
        self.command(f'{suffix}-create', args)
        self.containers.append(name)
        self.environments[name] = EnvironmentHandle(
            owner_id='probe-owner', project_id=f'{self.tag}-{directory.name}',
            environment_id=f'environment-{self.tag}-{directory.name}',
            generation=2 if suffix == 'restored' else 1, state='ready',
        )
        self.command(f'{suffix}-start', ['docker', 'start', name])
        return name

    def shell(self, label: str, container: str, script: str, *, timeout: int = 120,
              expected: int = 0) -> str:
        environment = self.environments[container]
        request = CommandRequest(
            operation_id=f'probe-{label}', environment=environment,
            ownership=RunOwnership(owner_id='probe-owner',
                                   session_id=f'probe-session-{self.run_number}',
                                   run_id=f'probe-run-{self.tag}-{self.run_number}'),
            authorization=AuthorizationRef(
                actor_id='probe-owner', actor_generation=1, project_id=environment.project_id,
                arguments_digest=fingerprint({'command': script, 'cwd': '.'}),
                network_policy_ref='probe-approved-package-egress',
            ),
            command=script, cwd='.', timeout_seconds=float(timeout),
        )
        self.requests.append(request.model_dump(mode='json'))
        return self.command(label, ['docker', 'exec', '--workdir', '/workspace',
                                   container, 'bash', '-lc', request.command],
                            timeout=timeout, expected=expected)

    def execute(self) -> None:
        project = self.root / 'project-a'
        primary = self.create('a', project, port=True)
        self.shell('versions', primary, 'set -eu; node --version; npm --version; '
                   'git --version; gcc -dumpversion; python3 --version; '
                   'python3 -m pip --version; test ! -e /var/run/docker.sock')
        self.shell('python-venv', primary,
                   'python3 -m venv .python-venv && .python-venv/bin/python -m pip --version')
        package = {'name': 'krim-environment-probe', 'version': '1.0.0', 'private': True,
                   'scripts': {'build': 'esbuild app.js --bundle --outfile=dist/bundle.js'},
                   'devDependencies': {'esbuild': '0.25.12'}}
        for name, text in {'package.json': json.dumps(package),
                           'app.js': ("document.querySelector('#result').textContent = "
                                      "'Built with esbuild';\n"),
                           'server.cjs': SERVER,
                           'owner-marker': self.tag,
                           '.gitignore': ('node_modules/\ndist/\n.cache/\n.runtimes/\n'
                                          '.python-venv/\n')}.items():
            path = project / name
            path.write_text(text)
            os.chown(path, 1000, 1000)
        self.shell('npm-install', primary, 'npm install --no-audit --no-fund --foreground-scripts',
                   timeout=600)
        self.shell('npm-build', primary, 'npm run build && test -s dist/bundle.js')
        self.shell('runtime-install', primary,
                   'npm install --prefix .runtimes --no-audit --no-fund --foreground-scripts '
                   'node@24.15.0 && .runtimes/node_modules/.bin/node --version', timeout=600)
        self.shell('git-cli', primary,
                   'set -eu; git init -q -b main; git add package.json package-lock.json '
                   'app.js server.cjs .gitignore; git diff --cached --stat; '
                   'git diff --cached --check')
        self.command('stop', ['docker', 'stop', '-t', '5', primary])
        self.command('restart', ['docker', 'start', primary])
        self.run_number += 1
        self.shell('retained-after-restart', primary,
                   'set -eu; test -s package-lock.json; test -s dist/bundle.js; '
                   'test -s owner-marker; npm run build; '
                   '.runtimes/node_modules/.bin/node --version')
        self.command('remove-environment-processes', ['docker', 'rm', '-f', primary])
        self.containers.remove(primary)
        primary = self.create('restored', project, port=True)
        self.shell('retained-after-recreate', primary,
                   'set -eu; test -s owner-marker; npm run build; '
                   '.runtimes/node_modules/.bin/node --version; git diff --cached --check')
        secondary = self.create('b', self.root / 'project-b')
        self.shell('project-isolation', secondary,
                   'set -eu; test ! -e /workspace/owner-marker; test ! -e /workspace/node_modules; '
                   f'test ! -e {project}; test ! -e /var/run/docker.sock; '
                   'test ! -e /root/.ssh; test ! -e /home/node/.git-credentials')
        quota_dir = self.root / 'quota'
        quota_dir.mkdir(mode=0o700)
        os.chown(quota_dir, 1000, 1000)
        # Separate from the existing account/checkpoint allocation ranges.
        number = 2_000_000_000 + int(self.tag[:7], 16)
        self.quota_number = number
        self.command('quota-setup', ['xfs_quota', '-x', '-c',
                     f'project -s -p {quota_dir} {number}', '/srv/krim-sandbox'])
        self.command('quota-limit', ['xfs_quota', '-x', '-c',
                     f'limit -p bhard=32m ihard=1000 {number}', '/srv/krim-sandbox'])
        quota_container = self.create('quota', quota_dir)
        self.shell('quota-enforced', quota_container,
                   "python3 -c 'import errno,os; f=open(\"limit.bin\",\"wb\"); "
                   "chunk=b\"x\"*(1024*1024)\ntry:\n"
                   " for _ in range(64): f.write(chunk); f.flush(); os.fsync(f.fileno())\n"
                   "except OSError as e:\n print(e.errno, str(e)); "
                   "assert e.errno in (errno.EDQUOT,errno.ENOSPC)\n"
                   "else: raise AssertionError(\"quota not enforced\")'", timeout=120)
        self.command('quota-observed', ['xfs_quota', '-x', '-c',
                     f'quota -p -b -n -N {number}', '/srv/krim-sandbox'])
        for container in [secondary, quota_container]:
            self.command('cleanup-' + container.rsplit('-', 1)[-1],
                         ['docker', 'rm', '-f', container])
            self.containers.remove(container)
        self.cleanup_quota()
        self.save()
        (self.evidence / 'preview-binding.json').write_text(json.dumps({
            'container': primary, 'port': 5189, 'image': self.image,
            'project': str(project), 'command': ['node', '/workspace/server.cjs'],
        }, indent=2))

    def cleanup_quota(self) -> None:
        if self.quota_number is None:
            return
        quota_dir = self.root / 'quota'
        if quota_dir.exists():
            shutil.rmtree(quota_dir)
        self.command('quota-clear', ['xfs_quota', '-x', '-c',
                     f'limit -p bhard=0 ihard=0 {self.quota_number}', '/srv/krim-sandbox'])
        self.quota_number = None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--image', required=True)
    parser.add_argument('--evidence', type=Path, required=True)
    args = parser.parse_args()
    probe = Probe(args.image, args.evidence)
    try:
        probe.execute()
    except Exception as exc:
        (args.evidence / 'failure.txt').write_text(repr(exc))
        cleanup = []
        for name in probe.containers:
            result = subprocess.run(['docker', 'rm', '-f', name], capture_output=True,
                                    text=True, check=False, timeout=45)
            cleanup.append({'container': name, 'exit_code': result.returncode,
                            'stdout': result.stdout, 'stderr': result.stderr})
        if all(item['exit_code'] == 0 for item in cleanup):
            probe.cleanup_quota()
        (args.evidence / 'cleanup.json').write_text(json.dumps({
            'containers': cleanup, 'retained_diagnostic_project': str(probe.root),
            'reason': 'retain failed source, dependencies and quota state for diagnosis',
        }, indent=2))
        raise
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
