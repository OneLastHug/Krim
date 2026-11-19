from __future__ import annotations

import base64
from dataclasses import replace

import httpx
import pytest
from test_dev_environment_backend import make_record
from test_dev_environment_execution import make_request

from krim_agent.dev_environment import EnvironmentError, docker
from krim_agent.dev_environment.bootstrap import CWD_EXEC
from krim_agent.dev_environment.model import RuntimeSpec
from krim_agent.dev_network.broker import DownloadBroker
from krim_agent.dev_network.policy import bind
from krim_agent.dev_network.shim import WAIT_CODE
from krim_agent.project_access.egress import ProjectEgressPolicy
from krim_agent.project_access.types import NetworkGrant


def command_spec(tmp_path, *, env_ref=None, command_ref=None):
    record = replace(make_record(tmp_path / 'project'), network_policy_ref=env_ref)
    request = make_request(record.environment)
    request = request.model_copy(update={
        'authorization': request.authorization.model_copy(update={
            'network_policy_ref': command_ref,
        }),
    })
    return RuntimeSpec(record, request, 'process-1')


@pytest.mark.parametrize(('env_ref', 'command_ref'), [
    (None, None), (None, 'policy-1'), ('policy-1', 'policy-1'),
])
def test_execution_uses_only_original_command_policy(tmp_path, env_ref, command_ref):
    spec = command_spec(tmp_path, env_ref=env_ref, command_ref=command_ref)
    assert spec.request.authorization.network_policy_ref == command_ref
    assert spec.network.policy_ref == command_ref
    assert spec.network.network_id is None
    assert '--network=none' in docker.create_args(spec)
    assert ('--env=HTTPS_PROXY=http://127.0.0.1:39119' in docker.create_args(spec)) == bool(
        command_ref)
    # The same persisted environment remains offline when it has no policy.
    assert RuntimeSpec(spec.record).network.policy_ref == env_ref


@pytest.mark.parametrize(('env_ref', 'command_ref'), [
    ('policy-1', None), ('policy-1', 'policy-2'),
])
def test_environment_policy_never_overrides_or_fills_command_policy(
    tmp_path, env_ref, command_ref,
):
    with pytest.raises(EnvironmentError, match='network_binding_drift'):
        command_spec(tmp_path, env_ref=env_ref, command_ref=command_ref)


@pytest.mark.parametrize('invalid', ['', 'bad/ref', 'bad ref', 'bad\nref', 'a' * 129, 7])
@pytest.mark.parametrize('source', ['environment', 'command'])
def test_invalid_network_policy_fails_closed_even_for_unvalidated_records(
    tmp_path, invalid, source,
):
    # model_copy deliberately bypasses Pydantic to exercise the private runtime
    # boundary independently of I's canonical-record validation.
    with pytest.raises(EnvironmentError, match='network_binding_invalid'):
        command_spec(tmp_path, env_ref=invalid if source == 'environment' else None,
                     command_ref=invalid if source == 'command' else None)


def test_effective_policy_changes_fingerprint_and_enables_wait_only_for_command(tmp_path):
    offline = command_spec(tmp_path)
    first = command_spec(tmp_path, command_ref='policy-1')
    second = command_spec(tmp_path, command_ref='policy-2')
    assert len({offline.fingerprint, first.fingerprint, second.fingerprint}) == 3
    assert docker.command(offline)[1][-3] == CWD_EXEC
    assert docker.command(first)[1][-3] == WAIT_CODE + CWD_EXEC
    assert docker.command(second)[1][-3] == WAIT_CODE + CWD_EXEC
    assert docker.network_env(offline) == {}
    assert docker.network_env(first)['HTTPS_PROXY'] == 'http://127.0.0.1:39119'
    assert RuntimeSpec(first.record).fingerprint == RuntimeSpec(second.record).fingerprint
    assert RuntimeSpec(first.record).network.policy_ref is None


@pytest.mark.asyncio
async def test_registry_download_binds_command_ref_with_offline_environment(tmp_path):
    spec = command_spec(tmp_path, command_ref='policy-1')
    policy = ProjectEgressPolicy(NetworkGrant(
        owner_id=spec.handle.owner_id, project_id=spec.handle.project_id,
        policy_id='policy-1', revision=1, hosts=('registry.npmjs.org',),
        expires_at=4_000_000_000.0,
    ))
    resolved = []

    async def resolver(current):
        assert current.record.network_policy_ref is None
        assert current.request == spec.request
        assert current.network.policy_ref == current.request.authorization.network_policy_ref
        resolved.append(current)
        return policy

    class Body(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b'{"name":"esbuild"}'

    async def upstream(request):
        assert str(request.url) == 'https://registry.npmjs.org/esbuild'
        return httpx.Response(200, stream=Body())

    # This HTTP response is a mock; verify.py separately exercises real runsc
    # with record.network_policy_ref=None and an explicit command policy.
    broker = DownloadBroker(await bind(spec, resolver), transport=httpx.MockTransport(upstream))
    output = []

    async def send(message):
        output.append(message)

    try:
        await broker.download({'id': 1, 'method': 'GET',
                               'url': 'https://registry.npmjs.org/esbuild', 'headers': {}}, send)
    finally:
        await broker.close()
    assert resolved
    assert output[0]['status'] == 200
    assert b''.join(base64.b64decode(m['data']) for m in output if 'data' in m) == (
        b'{"name":"esbuild"}')
    assert output[-1] == {'id': 1, 'end': True}
