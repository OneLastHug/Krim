from __future__ import annotations

import asyncio
from dataclasses import replace

import pytest
from test_dev_environment_backend import FakeRunner, MemoryBindings, make_record

from krim_agent.dev_environment import DevelopmentBackend, EnvironmentError, ProjectLocks, docker
from krim_agent.dev_environment.model import RuntimeSpec
from krim_agent.dev_network.manager import DownloadNetworks, _Lease
from krim_agent.dev_network.policy import bind
from krim_agent.project_access.egress import ProjectEgressPolicy
from krim_agent.project_access.types import NetworkGrant


@pytest.mark.asyncio
@pytest.mark.parametrize('operation', ['invalidate', 'close'])
async def test_pending_resolution_cannot_arm_after_revocation_boundary(tmp_path, operation):
    spec = RuntimeSpec(replace(make_record(tmp_path / 'project'),
                               network_policy_ref='policy-1'))
    entered, resume = asyncio.Event(), asyncio.Event()
    policy = ProjectEgressPolicy(NetworkGrant(
        owner_id='owner-1', project_id='project-1', policy_id='policy-1',
        revision=1, hosts=('registry.npmjs.org',), expires_at=4_000_000_000.0))

    async def resolver(_spec):
        entered.set()
        await resume.wait()
        return policy

    runner = FakeRunner()
    manager = DownloadNetworks(resolver, runner=runner)
    arm = asyncio.create_task(manager.arm(spec, 'a' * 64))
    await entered.wait()
    if operation == 'invalidate':
        await manager.invalidate('owner-1', 'project-1')
    else:
        await manager.close()
    resume.set()
    with pytest.raises(EnvironmentError, match='network_authorization_expired'):
        await arm
    assert manager._leases == {}
    assert runner.calls == []


@pytest.mark.asyncio
async def test_pending_container_inspect_cannot_arm_after_invalidate(tmp_path):
    spec = RuntimeSpec(replace(make_record(tmp_path / 'project'),
                               network_policy_ref='policy-1'))
    policy = ProjectEgressPolicy(NetworkGrant(
        owner_id='owner-1', project_id='project-1', policy_id='policy-1',
        revision=1, hosts=('registry.npmjs.org',), expires_at=4_000_000_000.0))

    async def resolver(_spec):
        return policy

    entered, resume = asyncio.Event(), asyncio.Event()

    class DelayedRunner(FakeRunner):
        async def run(self, args):
            if args[:2] == ('container', 'inspect'):
                entered.set()
                await resume.wait()
            reply = await super().run(args)
            if args[:2] == ('container', 'inspect'):
                import json
                value = json.loads(reply.stdout)
                value[0]['Config']['Env'] += [f'{k}={v}'
                                               for k, v in docker.network_env(spec).items()]
                return docker.Reply(0, json.dumps(value))
            return reply

    runner = DelayedRunner()
    cid = runner.add_container(spec)
    manager = DownloadNetworks(resolver, runner=runner)
    arm = asyncio.create_task(manager.arm(spec, cid))
    await entered.wait()
    await manager.invalidate('owner-1', 'project-1')
    resume.set()
    with pytest.raises(EnvironmentError, match='network_authorization_expired'):
        await arm
    assert manager._leases == {}
    assert not any(call[0] == 'exec' for call in runner.calls)


@pytest.mark.asyncio
async def test_policy_reference_without_enforcer_fails_before_docker(tmp_path):
    record = replace(make_record(tmp_path / 'project'), network_policy_ref='policy-1')
    repository = MemoryBindings(record)
    runner = FakeRunner()
    (tmp_path / 'locks').mkdir(mode=0o700)
    backend = DevelopmentBackend(repository, ProjectLocks(tmp_path / 'locks'),
                                 repository.authorize, runner=runner,
                                 mount_check=lambda _spec: None)
    with pytest.raises(EnvironmentError, match='network_enforcement_required'):
        await backend.create(record.environment)
    assert runner.calls == []


def test_policy_reference_cannot_enable_bridge_or_change_security_flags(tmp_path):
    record = replace(make_record(tmp_path / 'project'), network_policy_ref='policy-1')
    spec = RuntimeSpec(record)
    args = docker.create_args(spec)
    assert '--network=none' in args
    assert '--cap-drop=ALL' in args
    assert '--read-only' in args
    assert '--user=65532:65532' in args
    assert '--security-opt=no-new-privileges' in args
    assert not any(x.startswith(('--publish', '--add-host', '--privileged')) for x in args)
    env = docker.network_env(spec)
    value = FakeRunner.inspect_value(spec, 'c' * 64, 'running')
    value['Config']['Env'] += [f'{k}={v}' for k, v in env.items()]
    assert docker.verify(spec, value) == 'ready'
    value['Config']['Env'] = [x for x in value['Config']['Env']
                              if not x.startswith('NODE_EXTRA_CA_CERTS=')]
    with pytest.raises(EnvironmentError, match='container_binding_drift'):
        docker.verify(spec, value)
    with pytest.raises(EnvironmentError, match='uncontrolled_network_forbidden'):
        RuntimeSpec(replace(record, network_id='d' * 64))


@pytest.mark.asyncio
async def test_invalidate_closes_only_exact_scope_and_waits_for_stream_cleanup(tmp_path):
    async def denied(_spec):
        raise AssertionError('cleanup does not reauthorize revoked users')

    manager = DownloadNetworks(denied)
    source = replace(make_record(tmp_path / 'project'), network_policy_ref='policy-1')
    specs = [RuntimeSpec(source), RuntimeSpec(replace(source, network_policy_ref='policy-2')),
             RuntimeSpec(replace(source, environment=source.environment.model_copy(
                 update={'project_id': 'other-project'})))]
    cleaned = set()

    async def running(cid):
        try:
            await asyncio.Event().wait()
        finally:
            await asyncio.sleep(0)
            cleaned.add(cid)

    for number, spec in enumerate(specs, start=1):
        cid = str(number) * 64
        manager._leases[cid] = _Lease(spec, task=asyncio.create_task(running(cid)))
    await asyncio.sleep(0)
    await manager.invalidate(source.environment.owner_id, source.environment.project_id,
                             policy_id='policy-1')
    assert cleaned == {'1' * 64}
    assert set(manager._leases) == {'2' * 64, '3' * 64}
    await manager.close()
    assert cleaned == {'1' * 64, '2' * 64, '3' * 64}
    assert manager._leases == {}


@pytest.mark.asyncio
async def test_concurrent_release_and_cancel_wait_for_single_cleanup(tmp_path):
    async def denied(_spec):
        raise AssertionError()

    manager = DownloadNetworks(denied)
    spec = RuntimeSpec(make_record(tmp_path / 'project'))
    cleanup_entered, allow_cleanup = asyncio.Event(), asyncio.Event()
    finished = []

    async def running():
        try:
            await asyncio.Event().wait()
        finally:
            cleanup_entered.set()
            await allow_cleanup.wait()
            finished.append('cleaned')

    unit = asyncio.create_task(running())
    manager._leases['a' * 64] = _Lease(spec, task=unit)
    await asyncio.sleep(0)
    first = asyncio.create_task(manager.release('a' * 64))
    await cleanup_entered.wait()
    second = asyncio.create_task(manager.release('a' * 64))
    first.cancel()
    await asyncio.sleep(0)
    first.cancel()
    await asyncio.sleep(0)
    assert not first.done() and not second.done()
    allow_cleanup.set()
    with pytest.raises(asyncio.CancelledError):
        await first
    await second
    assert finished == ['cleaned']
    assert unit.done() and manager._leases == {}


@pytest.mark.asyncio
async def test_naturally_failed_session_reclaims_capacity_without_release(tmp_path):
    spec = RuntimeSpec(replace(make_record(tmp_path / 'project'),
                               network_policy_ref='policy-1'))
    policy = ProjectEgressPolicy(NetworkGrant(
        owner_id='owner-1', project_id='project-1', policy_id='policy-1',
        revision=1, hosts=('registry.npmjs.org',), expires_at=4_000_000_000.0))
    revoked = False

    async def resolver(_spec):
        if revoked:
            raise EnvironmentError('revoked')
        return policy

    binding = await bind(spec, resolver)
    manager = DownloadNetworks(resolver, max_sessions=1)
    lease = _Lease(spec)
    manager._leases['a' * 64] = lease
    revoked = True
    await manager._run(lease, 'a' * 64, binding)
    assert lease.closed and lease.ready.is_set()
    assert manager._leases == {}


def test_download_manager_refuses_nonlocal_daemon():
    class RemoteRunner:
        prefix = ('/usr/bin/docker', '--host=tcp://other:2375')

    with pytest.raises(ValueError, match='fixed local Docker socket'):
        DownloadNetworks(lambda _spec: None, runner=RemoteRunner())


@pytest.mark.asyncio
async def test_wait_ready_reports_failed_session_and_does_not_reopen(tmp_path):
    async def denied(_spec):
        raise AssertionError()

    manager = DownloadNetworks(denied)
    source = replace(make_record(tmp_path / 'project'), network_policy_ref='policy-1')
    lease = _Lease(RuntimeSpec(source), closed=True)
    lease.ready.set()
    manager._leases['a' * 64] = lease
    with pytest.raises(EnvironmentError, match='network_session_unavailable'):
        await manager.wait_ready('a' * 64)
    await manager.release('a' * 64)
    with pytest.raises(EnvironmentError, match='network_session_missing'):
        await manager.wait_ready('a' * 64)
