from __future__ import annotations

import time
from dataclasses import replace
from types import SimpleNamespace

import httpx
import pytest
from test_dev_environment_execution import (
    ExecutionRunner,
    MemoryEnvironmentRepository,
    MemoryProcessRepository,
    make_process,
    make_record,
    make_request,
)

from krim_agent.dev_environment import (
    ContainerExecutions,
    DevelopmentBackend,
    EnvironmentError,
    ProjectLocks,
    docker,
)
from krim_agent.dev_environment.model import RuntimeSpec
from krim_agent.dev_network.broker import DownloadBroker
from krim_agent.dev_network.manager import DownloadNetworks
from krim_agent.dev_network.policy import bind
from krim_agent.project_access.egress import ProjectEgressPolicy
from krim_agent.project_access.types import NetworkGrant


class NetworkInspectRunner(ExecutionRunner):
    """Docker inspect double; include the immutable proxy config being verified."""

    drift = False

    def inspect_value(self, spec, cid, state, exit_code=None):
        value = super().inspect_value(spec, cid, state, exit_code)
        value['Config']['Env'] += [f'{k}={v}' for k, v in docker.network_env(spec).items()]
        if self.drift and spec.request is not None:
            value['HostConfig']['NetworkMode'] = 'host'
        return value


@pytest.fixture
def observation_env(tmp_path, monkeypatch):
    record = make_record(tmp_path / 'project', container_id='a' * 64)
    repository = MemoryEnvironmentRepository(record)
    runner = NetworkInspectRunner()
    runner.add_container(RuntimeSpec(record), container_id=record.container_id)
    request = make_request(record.environment)
    request = request.model_copy(update={'authorization': request.authorization.model_copy(
        update={'network_policy_ref': 'policy-1'})})
    row = make_process(request)
    processes = MemoryProcessRepository(row)
    runner.register(RuntimeSpec(record, request, row.handle.process_id))
    source = SimpleNamespace(
        grant=NetworkGrant(owner_id=record.environment.owner_id,
                           project_id=record.environment.project_id, policy_id='policy-1',
                           revision=1, hosts=('registry.npmjs.org',),
                           expires_at=time.time() + 600),
        available=True, calls=0, deny_read=False,
    )

    async def resolver(_spec):
        source.calls += 1
        if not source.available:
            raise EnvironmentError('network_authorization_expired')
        return ProjectEgressPolicy(source.grant)

    async def authority(environment, action):
        await repository.authorize(environment, action)
        if source.deny_read and action == 'inspect':
            raise EnvironmentError('read_permission_denied')

    downloads = DownloadNetworks(resolver, runner=runner)
    real_arm = downloads.arm

    async def fake_arm(spec, _cid):
        # Unit tests do not spawn a real Docker CLI. Actual policy checking is
        # unchanged, while the exec transport is covered by B2 real acceptance.
        await downloads.check(spec)

    monkeypatch.setattr(downloads, 'arm', fake_arm)
    locks = tmp_path / 'locks'
    locks.mkdir(mode=0o700)
    backend = DevelopmentBackend(repository, ProjectLocks(locks), authority,
                                 runner=runner, mount_check=lambda _spec: None,
                                 downloads=downloads)
    executions = ContainerExecutions(backend, processes)
    return SimpleNamespace(record=record, repository=repository, runner=runner,
                           request=request, row=row, processes=processes, source=source,
                           downloads=downloads, real_arm=real_arm, backend=backend,
                           executions=executions, resolver=resolver)


async def prepare(rig):
    async with rig.executions.prepare(rig.request, rig.row.handle.process_id) as descriptor:
        rig.processes.persist_execution(rig.row.handle.process_id, descriptor.execution_id)
    return descriptor


def invalidate(rig, reason):
    if reason == 'revoked':
        rig.source.grant = rig.source.grant.model_copy(update={'revoked': True})
    elif reason == 'expired':
        rig.source.grant = rig.source.grant.model_copy(update={'expires_at': time.time() - 1})
    else:
        # Models I's run-completed or canonical-row-exited resolver rejection.
        rig.source.available = False


@pytest.mark.asyncio
@pytest.mark.parametrize('reason', ['revoked', 'expired', 'run_completed', 'row_exited'])
async def test_network_invalidation_does_not_hide_container_exit_or_block_cleanup(
    observation_env, reason,
):
    rig = observation_env
    descriptor = await prepare(rig)
    spec, _state, _code = rig.runner.containers[descriptor.execution_id]
    binding = await bind(spec, rig.resolver)
    invalidate(rig, reason)
    saved = rig.processes.rows[rig.row.handle.process_id]
    persisted_state = 'exited' if reason == 'row_exited' else 'running'
    # A stale persisted receipt is not substituted for the observed Docker exit.
    rig.processes.set_row(replace(saved, handle=saved.handle.model_copy(update={
        'state': persisted_state, 'exit_code': 99 if persisted_state == 'exited' else None,
    }), revision=saved.revision + 1))
    rig.runner.containers[descriptor.execution_id] = (spec, 'exited', 17)
    calls = rig.source.calls
    status = await rig.executions.inspect(descriptor)
    assert status.state == 'exited' and status.exit_code == 17
    assert rig.source.calls == calls
    assert rig.repository.authorized[-1] == 'inspect'

    sent = []

    async def upstream(_request):
        sent.append('network')
        raise AssertionError('invalidated grant must never reach HTTP transport')

    async def send(_message):
        raise AssertionError('invalidated grant must never deliver data')

    broker = DownloadBroker(binding, transport=httpx.MockTransport(upstream))
    try:
        with pytest.raises(EnvironmentError, match='network_authorization_expired'):
            await broker.download({'id': 1, 'method': 'GET',
                                   'url': 'https://registry.npmjs.org/esbuild',
                                   'headers': {}}, send)
    finally:
        await broker.close()
    assert sent == []

    request = rig.request.model_copy(update={'operation_id': 'operation-2'})
    pending = make_process(request, process_id='process-2')
    rig.processes.set_row(pending)
    pending_spec = RuntimeSpec(rig.record, request, pending.handle.process_id)
    rig.runner.register(pending_spec)
    created = rig.runner.create_attempts
    with pytest.raises(EnvironmentError, match='network_authorization_expired'):
        async with rig.executions.prepare(request, pending.handle.process_id):
            raise AssertionError('new execution must not be accepted')
    with pytest.raises(EnvironmentError, match='network_authorization_expired'):
        await rig.real_arm(pending_spec, 'b' * 64)
    assert rig.runner.create_attempts == created
    assert rig.runner.start_calls == []
    await rig.executions.release(descriptor)
    assert descriptor.execution_id not in rig.runner.containers
    assert rig.repository.authorized[-1] == 'stop'


@pytest.mark.asyncio
@pytest.mark.parametrize('reason', ['revoked', 'expired'])
async def test_cancel_running_execution_is_not_blocked_by_download_policy(observation_env, reason):
    rig = observation_env
    descriptor = await prepare(rig)
    spec, _state, _code = rig.runner.containers[descriptor.execution_id]
    rig.runner.containers[descriptor.execution_id] = (spec, 'running', None)
    invalidate(rig, reason)
    calls = rig.source.calls
    status = await rig.executions.cancel(descriptor)
    assert status.state == 'exited' and status.exit_code == rig.runner.stop_exit_code
    assert descriptor.execution_id not in rig.runner.containers
    assert rig.source.calls == calls


@pytest.mark.asyncio
async def test_expired_download_does_not_bypass_current_reader_authority(observation_env):
    rig = observation_env
    descriptor = await prepare(rig)
    invalidate(rig, 'expired')
    rig.source.deny_read = True
    with pytest.raises(EnvironmentError, match='read_permission_denied'):
        await rig.executions.inspect(descriptor)


@pytest.mark.asyncio
@pytest.mark.parametrize('drift', ['container_network', 'canonical_request', 'execution_id'])
async def test_observation_still_rejects_binding_and_network_configuration_drift(
    observation_env, drift,
):
    rig = observation_env
    descriptor = await prepare(rig)
    invalidate(rig, 'revoked')
    if drift == 'container_network':
        rig.runner.drift = True
        error = 'container_binding_drift'
    elif drift == 'canonical_request':
        saved = rig.processes.rows[rig.row.handle.process_id]
        rig.processes.set_row(replace(saved, request_digest='c' * 64))
        error = 'execution_binding_drift'
    else:
        descriptor = replace(descriptor, execution_id='d' * 64)
        error = 'execution_binding_drift'
    with pytest.raises(EnvironmentError, match=error):
        await rig.executions.inspect(descriptor)


@pytest.mark.asyncio
async def test_real_runsc_exit_is_observable_after_network_invalidation():
    import asyncio
    import hashlib
    import json
    import os
    import re
    import shutil
    import uuid
    from pathlib import Path

    if os.environ.get('KRIM_DEV_NETWORK_REAL_OBSERVATION') != '1':
        pytest.skip('explicit real Docker/runsc acceptance only')
    assert os.geteuid() == 0
    evidence = Path(os.environ['KRIM_DEV_NETWORK_OBSERVATION_EVIDENCE'])
    assert evidence.is_absolute()
    assert str(await asyncio.to_thread(evidence.resolve)).startswith('/mnt/f/')
    await asyncio.to_thread(evidence.mkdir, parents=True, exist_ok=False)
    token = uuid.uuid4().hex
    root = Path('/srv/krim-sandbox') / ('b2-observation-' + token)
    root.mkdir(mode=0o755)
    project = root / 'content'
    project.mkdir(mode=0o700)
    os.chown(project, 65532, 65532)
    locks = root / 'locks'
    locks.mkdir(mode=0o700)
    base = make_record(project)
    record = replace(base, image_id=(
        'sha256:82098beca2bd1c8887f82deb8d2fba13f87da04938463881a5e95b3d346834dc'),
        environment=base.environment.model_copy(update={
            'owner_id': 'b2-observer-' + token, 'project_id': 'project-' + token,
            'environment_id': 'environment-' + token,
        }))
    repository = MemoryEnvironmentRepository(record)
    request = make_request(record.environment, command='exit 31')
    request = request.model_copy(update={'authorization': request.authorization.model_copy(
        update={'network_policy_ref': 'policy-1'})})
    row = make_process(request)
    processes = MemoryProcessRepository(row)
    grant = NetworkGrant(owner_id=record.environment.owner_id,
                         project_id=record.environment.project_id,
                         policy_id='policy-1', revision=1, hosts=('registry.npmjs.org',),
                         expires_at=time.time() + 60)
    calls = 0

    async def resolver(_spec):
        nonlocal calls
        calls += 1
        return ProjectEgressPolicy(grant)

    runner = docker.DockerCLI()
    downloads = DownloadNetworks(resolver, runner=runner)
    backend = DevelopmentBackend(repository, ProjectLocks(locks), repository.authorize,
                                 runner=runner, downloads=downloads)
    executions = ContainerExecutions(backend, processes)
    spec = RuntimeSpec(record, request, row.handle.process_id)
    cid = None
    result = {'production_authorization': False, 'network_requests': 0,
              'resolver': 'in-memory grant test substitute', 'observations': [],
              'container_removed': False, 'directory_removed': False, 'passed': False}
    test_path = await asyncio.to_thread(Path(__file__).resolve)
    source = test_path.parents[2] / 'src/krim_agent/dev_environment/execution.py'
    result['execution_sha256'] = hashlib.sha256(source.read_bytes()).hexdigest()
    try:
        await docker.check_image(runner, spec)
        created = await docker.checked(runner, docker.create_args(spec))
        assert re.fullmatch(r'[0-9a-f]{64}', created)
        cid = created
        result['created_id'] = cid
        spec = replace(spec, execution_id=cid)
        processes.persist_execution(row.handle.process_id, cid)
        value = await docker.inspect(runner, spec)
        assert value is not None and docker.verify(spec, value) == 'stopped'
        await downloads.arm(spec, cid)
        reply = await runner.run(('start', '--attach', '--interactive', cid))
        assert reply.code == 31, reply
        descriptor = executions._descriptor(spec, cid)
        for reason in ('revoked', 'expired'):
            grant = grant.model_copy(update={
                'revoked': reason == 'revoked',
                'expires_at': time.time() - 1 if reason == 'expired' else time.time() + 60,
            })
            await downloads.invalidate(record.environment.owner_id, record.environment.project_id)
            with pytest.raises(EnvironmentError, match='network_authorization_expired'):
                await downloads.check(spec)
            before = calls
            status = await executions.inspect(descriptor)
            assert status.state == 'exited' and status.exit_code == 31
            assert calls == before
            result['observations'].append({'reason': reason, 'state': status.state,
                                           'exit_code': status.exit_code,
                                           'network_resolution_during_inspect': calls - before})
        saved = processes.rows[row.handle.process_id]
        processes.set_row(replace(saved, handle=saved.handle.model_copy(update={
            'state': 'exited', 'exit_code': 99,
        }), revision=saved.revision + 1))
        assert (await executions.inspect(descriptor)).exit_code == 31
        await executions.release(descriptor)
        assert await docker.inspect(runner, spec) is None
        result['passed'] = True
    finally:
        await downloads.close()
        if cid is not None:
            value = await docker.inspect(runner, spec)
            if value is not None:
                docker.verify(spec, value)
                assert value['Id'] == cid and value['Config']['Labels'] == docker.labels(spec)
                await docker.checked(runner, ('rm', '--force', cid))
            assert await docker.inspect(runner, spec) is None
            result['container_removed'] = True
        assert root.parent == Path('/srv/krim-sandbox') and not root.is_symlink()
        shutil.rmtree(root)
        result['directory_removed'] = not root.exists()
        assert result['execution_sha256'] == hashlib.sha256(source.read_bytes()).hexdigest()
        (evidence / 'results.json').write_text(json.dumps(result, indent=2))
