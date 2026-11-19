"""Opt-in B runsc relay + E HTTP codec integration with a fixed protocol fixture.

The container only serves predetermined test bytes over loopback. No application,
Web preview, public listener, dependency install or browser is created here.
Persistence/authorization use B's explicit fixture; production I wiring is separate.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import shlex
import shutil
import tempfile
import uuid
from dataclasses import replace
from pathlib import Path

import httpx

from krim_agent.dev_environment import ContainerExecutions, DevelopmentBackend, ProjectLocks
from krim_agent.dev_environment.preview import DockerPreviewRoutes
from krim_agent.dev_preview.http_transport import ConnectedHTTPTransport
from krim_agent.development.contracts import (
    AuthorizationRef,
    CommandRequest,
    EnvironmentHandle,
    ProcessHandle,
    RunOwnership,
)
from krim_agent.development.ports import EnvironmentRecord, ProcessRecord, command_digest
from krim_agent.development.preview_ports import RouteBinding
from krim_agent.security import PrincipalContext

# Explicit opt-in is also required by the reusable Docker test helpers.
from tests.agent_core.test_dev_environment_preview_docker import IMAGE, Recorder, Repo, Source

# Bounded, predetermined HTTP test peer, not an application or general HTTP server.
PEER = r"""import socket, threading
payload=bytes(range(256))*2000
listener=socket.socket()
listener.bind(('127.0.0.1',8123))
listener.listen(8)
open('/workspace/listening','w').write('yes')
def serve(peer):
 try:
  peer.settimeout(10)
  reader=peer.makefile('rb')
  first=reader.readline(8193)
  if not first: return
  if first not in (b'GET /binary HTTP/1.1\r\n', b'POST /binary HTTP/1.1\r\n'): return
  total=len(first)
  headers={}
  while True:
   line=reader.readline(8193);total+=len(line)
   if not line or total>16384: return
   if line==b'\r\n': break
   name,value=line.split(b':',1);headers[name.lower()]=value.strip()
  if headers.get(b'host')!=b'fixture.invalid': return
  if first.startswith(b'POST'):
   if headers.get(b'content-length')!=str(len(payload)).encode(): return
   if reader.read(len(payload))!=payload: return
  with open('/workspace/requests','ab') as log: log.write(first)
  peer.sendall(b'HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\n'
   +b'Content-Length: '+str(len(payload)).encode()+b'\r\nConnection: close\r\n\r\n')
  peer.sendall(payload)
  peer.shutdown(socket.SHUT_WR)
 finally:
  peer.close()
while True:
 peer,_=listener.accept()
 threading.Thread(target=serve,args=(peer,),daemon=True).start()
"""


def directories(evidence: Path) -> tuple[Path, Path, Path]:
    evidence.mkdir(mode=0o700, parents=True, exist_ok=False)
    root = Path(tempfile.mkdtemp(prefix="coding-agent-preview-http-", dir="/srv/krim-sandbox"))
    content = root / "content"
    content.mkdir(mode=0o700)
    os.chown(content, 65532, 65532)
    locks = root / "locks"
    locks.mkdir(mode=0o700)
    return root, content, locks


async def run(evidence: Path) -> None:
    root, content, locks = await asyncio.to_thread(directories, evidence)
    info = content.stat()
    environment = EnvironmentHandle(
        owner_id="preview-http-integration", project_id=uuid.uuid4().hex,
        environment_id=uuid.uuid4().hex, generation=1, state="creating",
    )
    repo = Repo(EnvironmentRecord(
        environment, IMAGE, content, info.st_dev, info.st_ino,
        memory_bytes=256 * 1024 * 1024, pids_limit=64,
    ))
    docker = Recorder(evidence)
    backend = DevelopmentBackend(
        repo, ProjectLocks(locks), repo.authorize, runner=docker, processes=repo,
    )
    executions = ContainerExecutions(backend, repo)
    source = Source(repo)
    routes = DockerPreviewRoutes(backend, source)
    attachment = None
    observations = []

    def record(label: str, **values: object) -> None:
        observations.append({"step": label, **values})
        (evidence / "results.json").write_text(json.dumps({
            "image": IMAGE, "observations": observations,
            "persistence_authorization": "explicit fixture",
            "transport": "actual runsc exec relay + httpcore/httpx",
            "full_application_acceptance": False,
        }, indent=2))
        print(json.dumps(observations[-1]), flush=True)

    try:
        await backend.start(environment)
        request = CommandRequest(
            operation_id=uuid.uuid4().hex, environment=repo.environment.environment,
            ownership=RunOwnership(
                owner_id=environment.owner_id, session_id="fixture", run_id="fixture",
            ),
            authorization=AuthorizationRef(
                actor_id="actor", actor_generation=1,
                project_id=environment.project_id, arguments_digest="a" * 64,
            ),
            command="exec /usr/bin/python3 -I -u -c " + shlex.quote(PEER),
            lifecycle="project_service", timeout_seconds=None, wait_seconds=0,
        )
        handle = ProcessHandle(
            process_id=uuid.uuid4().hex, operation_id=request.operation_id,
            environment=request.environment, ownership=request.ownership,
            lifecycle="project_service", state="accepted", log_ref="fixture-log",
        )
        row = ProcessRecord(request, handle, command_digest(request))
        repo.rows[handle.process_id] = row
        async with executions.prepare(request, handle.process_id) as execution:
            await repo.save(row, replace(row, execution_id=execution.execution_id,
                                         revision=row.revision + 1))
            attachment = await asyncio.create_subprocess_exec(
                *execution.attach_argv, stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
            )
            async with asyncio.timeout(10):
                while not (content / "listening").exists():
                    if attachment.returncode is not None:
                        raise RuntimeError("protocol fixture exited before ready")
                    await asyncio.sleep(0.025)
        row = repo.rows[handle.process_id]
        row = await repo.save(row, replace(
            row, handle=row.handle.model_copy(update={"state": "running"}),
            revision=row.revision + 1,
        ))
        viewer = PrincipalContext("viewer", 1, ())
        instance = await routes.observe(row.handle, 8123, viewer)
        binding = RouteBinding(
            **instance.model_dump(), preview_id=uuid.uuid4().hex,
            owner_id=environment.owner_id, project_id=environment.project_id,
            environment_id=environment.environment_id, generation=1,
            service_id=handle.process_id, process_id=handle.process_id,
            operation_id=request.operation_id, request_digest=row.request_digest,
            port=8123, route_epoch=1,
        )
        source.binding = binding
        payload = bytes(range(256)) * 2000
        for method in ("GET", "POST"):
            async with routes.acquire(binding, viewer) as lease:
                async with (
                    routes.connect(lease) as endpoint,
                    httpx.AsyncClient(
                        transport=ConnectedHTTPTransport(endpoint, "fixture.invalid"),
                        trust_env=False, follow_redirects=False, timeout=4,
                    ) as client,
                ):
                    response = await client.request(
                        method, "http://fixture.invalid/binary",
                        content=payload if method == "POST" else None,
                    )
                    assert response.status_code == 200
                    assert response.content == payload
                    assert response.headers["content-length"] == str(len(payload))
                    await routes.check(lease)
            assert (await executions.inspect(execution)).state == "running"
            record("http-" + method.lower(), status=200, bytes=len(response.content),
                   sha256=hashlib.sha256(response.content).hexdigest(), service_running=True)
        assert (content / "requests").read_bytes().splitlines() == [
            b"GET /binary HTTP/1.1", b"POST /binary HTTP/1.1",
        ]
        record("no-transparent-replay", accepted_requests=2)
        inspected = await docker.run(("container", "inspect", execution.execution_id))
        value = json.loads(inspected.stdout)[0]
        assert value["HostConfig"]["NetworkMode"] == "none"
        assert not value["HostConfig"]["PortBindings"]
        assert value["Config"]["User"] == "65532:65532"
        record("isolation", network="none", published_ports=False, uid=65532)
    finally:
        await routes.aclose()
        await docker.cleanup()
        if attachment is not None:
            await asyncio.wait_for(attachment.communicate(), 10)
        await asyncio.to_thread(shutil.rmtree, root)
        removed = not await asyncio.to_thread(root.exists)
        record("cleanup", fixture_root_removed=removed)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", required=True, type=Path)
    args = parser.parse_args()
    asyncio.run(run(args.evidence))
