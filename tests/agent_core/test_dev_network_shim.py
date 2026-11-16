from __future__ import annotations

import asyncio
import base64
import json
import socket
import ssl
import sys
import time
from contextlib import asynccontextmanager

import httpx
import pytest

from krim_agent.dev_network.shim import SHIM_CODE, WAIT_CODE
from krim_agent.dev_network.tls import certificate
from krim_agent.project_access.egress import ProjectEgressPolicy
from krim_agent.project_access.types import NetworkGrant


@asynccontextmanager
async def proxy(tmp_path):
    with socket.socket() as reserved:
        reserved.bind(('127.0.0.1', 0))
        port = reserved.getsockname()[1]
    root = str(tmp_path / 'proxy')
    policy = ProjectEgressPolicy(NetworkGrant(
        owner_id='owner-1', project_id='project-1', policy_id='policy-1',
        hosts=('registry.npmjs.org',), expires_at=time.time() + 60, revision=1,
    ))
    cert, key = certificate(policy, 30)
    code = SHIM_CODE.replace('PORT=39119', f'PORT={port}').replace(
        "ROOT='/home/krim/.krim-network'", f'ROOT={root!r}')
    process = await asyncio.create_subprocess_exec(
        sys.executable, '-I', '-c', code, stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    assert process.stdin and process.stdout and process.stderr
    process.stdin.write(json.dumps({'cert': cert, 'key': key, 'age': 30}).encode() + b'\n')
    await process.stdin.drain()
    try:
        async with asyncio.timeout(5):
            ready = await process.stdout.readline()
        if not ready:
            raise AssertionError((await process.stderr.read()).decode())
        assert json.loads(ready) == {'ready': True}
        context = ssl.create_default_context(cafile=root + '/ca.pem')
        async with httpx.AsyncClient(proxy=f'http://127.0.0.1:{port}', verify=context,
                                     trust_env=False, timeout=3) as client:
            yield process, client, port
    finally:
        process.stdin.close()
        try:
            async with asyncio.timeout(3):
                await process.wait()
        except TimeoutError:
            process.kill()
            await process.wait()
        assert process.returncode == 0, (await process.stderr.read()).decode()


@pytest.mark.asyncio
async def test_real_loopback_connect_tls_http_and_download_frames(tmp_path):
    async with proxy(tmp_path) as (process, client, _port):
        request = asyncio.create_task(client.get('https://registry.npmjs.org/esbuild'))
        async with asyncio.timeout(4):
            frame = json.loads(await process.stdout.readline())
        assert frame['method'] == 'GET'
        assert frame['url'] == 'https://registry.npmjs.org:443/esbuild'
        rid = frame['id']
        for message in [
            {'id': rid, 'status': 200, 'headers': {'content-type': 'application/json'}},
            {'id': rid, 'data': base64.b64encode(b'{"version":"0.25.12"}').decode()},
            {'id': rid, 'end': True},
        ]:
            process.stdin.write(json.dumps(message).encode() + b'\n')
        await process.stdin.drain()
        response = await request
        assert response.json() == {'version': '0.25.12'}


@pytest.mark.asyncio
@pytest.mark.parametrize('method,headers', [('POST', {}), ('GET', {'Host': 'attacker.invalid'})])
async def test_real_shim_rejects_method_and_authority_confusion(tmp_path, method, headers):
    async with proxy(tmp_path) as (_process, client, _port):
        response = await client.request(
            method, 'https://registry.npmjs.org/esbuild', headers=headers)
        assert response.status_code == 403


@pytest.mark.asyncio
async def test_real_shim_pipe_failure_closes_listener(tmp_path):
    async with proxy(tmp_path) as (process, _client, port):
        process.stdin.close()
        await asyncio.wait_for(process.wait(), 3)
        with pytest.raises(OSError):
            await asyncio.open_connection('127.0.0.1', port)


def test_embedded_programs_compile():
    compile(SHIM_CODE, 'shim', 'exec')
    compile(WAIT_CODE, 'wait', 'exec')
