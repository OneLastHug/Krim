"""Opt-in real runsc/none raw-TCP fixture. No HTTP server or published port.

Supply KRIM_PREVIEW_SHARED_SRC=I's src and KRIM_RUN_PREVIEW_DOCKER=1 under the
trusted Linux Docker user. Uses only the installed approved development image.
"""
# ruff: noqa: E402, ASYNC240, I001
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import shlex
import shutil
import socket
import tempfile
import time
import uuid
from dataclasses import replace
from pathlib import Path

import pytest

import krim_agent.development

_shared = os.environ.get("KRIM_PREVIEW_SHARED_SRC")
if _shared:
    krim_agent.development.__path__.append(str(Path(_shared) / "krim_agent/development"))
pytestmark = pytest.mark.skipif(
    os.environ.get("KRIM_RUN_PREVIEW_DOCKER") != "1" or not _shared,
    reason="explicit real-Docker preview acceptance only",
)
if not _shared:
    pytest.skip("requires I's shared preview contract namespace", allow_module_level=True)

from krim_agent.dev_environment import ContainerExecutions, DevelopmentBackend, ProjectLocks
from krim_agent.dev_environment.docker import DockerCLI
from krim_agent.dev_environment.model import identity
from krim_agent.dev_environment.preview import DockerPreviewRoutes
from krim_agent.dev_environment.preview_control import PreviewLimits
from krim_agent.development.contracts import (
    AuthorizationRef,
    CommandRequest,
    EnvironmentHandle,
    ProcessHandle,
    RunOwnership,
)
from krim_agent.development.ports import EnvironmentRecord, ProcessRecord, command_digest
from krim_agent.development.preview_ports import PreviewRouteError, RouteBinding
from krim_agent.security import PrincipalContext

IMAGE = "sha256:82098beca2bd1c8887f82deb8d2fba13f87da04938463881a5e95b3d346834dc"
# A raw TCP fixture, not a Web/dev server. No host networking is used.
SERVER = """import socket,threading,time,os
s=socket.socket(); s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
s.bind(('127.0.0.1',8123));s.listen(16)
open('/workspace/listening','w').write('yes')
def client(c):
 try:
  mode=c.recv(1)
  if mode==b'E':
   while True:
    data=c.recv(32768)
    if not data: break
    c.sendall(data)
   c.shutdown(socket.SHUT_WR)
  elif mode==b'H':
   data=bytearray()
   while True:
    part=c.recv(32768)
    if not part:break
    data.extend(part)
    open('/workspace/half-size','w').write(str(len(data)))
   open('/workspace/half-eof','w').write('yes')
   c.sendall(bytes(data)[::-1]); c.shutdown(socket.SHUT_WR)
  elif mode==b'F':
   while True:c.sendall(b'z'*65536)
  elif mode==b'S':
   time.sleep(8)
 except OSError:pass
 finally:c.close()
while True:
 c,_=s.accept(); threading.Thread(target=client,args=(c,),daemon=True).start()
"""


class Repo:
    def __init__(self, environment):
        self.environment = environment
        self.rows = {}

    async def resolve(self, handle):
        assert identity(handle) == identity(self.environment.environment)
        return self.environment

    async def compare_and_swap(self, expected, updated):
        assert expected == self.environment and updated.revision == expected.revision + 1
        self.environment = updated
        return updated

    async def authorize(self, handle, action):
        assert identity(handle) == identity(self.environment.environment)

    async def get(self, process_id):
        return self.rows[process_id]

    async def save(self, expected, updated):
        assert self.rows[expected.handle.process_id] == expected
        assert updated.revision == expected.revision + 1
        self.rows[expected.handle.process_id] = updated
        return updated

    async def list_environment(self, handle):
        return tuple(row for row in self.rows.values()
                     if identity(row.request.environment) == identity(handle))


class Source:
    def __init__(self, repo):
        self.repo = repo
        self.binding = None
        self.revoked = False
        self.slow = False
        self.viewers = []

    async def registration(self, process, port, principal):
        assert port == 8123 and principal.generation == 1
        return await self.repo.get(process.process_id)

    async def resolve(self, binding, principal):
        self.viewers.append(principal)
        if self.slow:
            await asyncio.sleep(5)
        if self.revoked or self.binding != binding:
            raise PreviewRouteError("preview_route_invalidated")
        return await self.repo.get(binding.process_id)


class Recorder(DockerCLI):
    def __init__(self, evidence):
        self.evidence = evidence
        self.created = {}
        self.calls = []

    async def run(self, args):
        result = await super().run(args)
        if args[0] == "create" and result.code == 0:
            cid = result.stdout.strip()
            assert len(cid) == 64
            self.created[cid] = dict(args[i + 1].split("=", 1)
                                     for i, value in enumerate(args) if value == "--label")
        self.calls.append({"argv": list(args), "code": result.code,
                           "stdout": result.stdout, "stderr": result.stderr})
        (self.evidence / "docker.json").write_text(json.dumps(self.calls, indent=2))
        return result

    async def cleanup(self):
        results = []
        for cid, expected in self.created.items():
            reply = await self.run(("container", "inspect", cid))
            if reply.code:
                assert "No such" in reply.stderr
                results.append({"id": cid, "missing": True})
                continue
            value, = json.loads(reply.stdout)
            assert value["Id"] == cid and value["Image"] == IMAGE
            assert all(value["Config"]["Labels"].get(k) == v for k, v in expected.items())
            result = await self.run(("rm", "--force", cid))
            assert result.code == 0
            results.append({"id": cid, "removed": True})
        (self.evidence / "cleanup.json").write_text(json.dumps(results, indent=2))


async def receive_all(sock):
    data = bytearray()
    while part := await asyncio.get_running_loop().sock_recv(sock, 65536):
        data.extend(part)
    return bytes(data)


@pytest.mark.asyncio
async def test_real_runsc_preview_relay_lifecycle_and_backpressure():
    evidence = Path(os.environ["KRIM_PREVIEW_EVIDENCE"])
    evidence.mkdir(parents=True, exist_ok=True)
    root = Path(tempfile.mkdtemp(prefix="coding-agent-probe-preview-", dir="/srv/krim-sandbox"))
    content = root / "content"
    content.mkdir(mode=0o700)
    os.chown(content, 65532, 65532)
    lock_root = root / "locks"
    lock_root.mkdir(mode=0o700)
    info = content.stat()
    env = EnvironmentHandle(owner_id="b-preview-acceptance", project_id=uuid.uuid4().hex,
                            environment_id=uuid.uuid4().hex, generation=1, state="creating")
    repo = Repo(EnvironmentRecord(env, IMAGE, content, info.st_dev, info.st_ino,
                                 memory_bytes=256 * 1024 * 1024, pids_limit=64))
    runner = Recorder(evidence)
    backend = DevelopmentBackend(repo, ProjectLocks(lock_root), repo.authorize,
                                 runner=runner, processes=repo)
    executions = ContainerExecutions(backend, repo)
    source = Source(repo)
    limits = PreviewLimits(lease_seconds=.7, heartbeat_seconds=.1,
                           max_age_seconds=30, idle_seconds=10)
    routes = DockerPreviewRoutes(backend, source, limits=limits)
    process_id = uuid.uuid4().hex
    request = None
    attachment = None
    steps = []

    def passed(label, **details):
        steps.append({"label": label, "passed": True, **details})
        (evidence / "results.json").write_text(json.dumps({"root": str(root), "steps": steps,
            "limits": vars(limits)}, indent=2))

    async def relay_absent(pid):
        started = time.monotonic()
        for _ in range(50):
            result = await runner.run(("exec", "--user=65532:65532", execution.execution_id,
                                       "/usr/bin/python3", "-I", "-c",
                                       "import os,sys; sys.exit(int(os.path.exists(sys.argv[1])))",
                                       f"/proc/{pid}"))
            if result.code == 0:
                return time.monotonic() - started
            await asyncio.sleep(.025)
        raise AssertionError("relay still alive after close bound")

    try:
        await backend.start(env)
        request = CommandRequest(operation_id=uuid.uuid4().hex,
            environment=repo.environment.environment,
            ownership=RunOwnership(owner_id=env.owner_id, session_id="session", run_id="run"),
            authorization=AuthorizationRef(actor_id="actor", actor_generation=1,
                project_id=env.project_id, arguments_digest="a" * 64),
            command="exec /usr/bin/python3 -I -u -c " + shlex.quote(SERVER),
            lifecycle="project_service", timeout_seconds=None, wait_seconds=0)
        handle = ProcessHandle(process_id=process_id, operation_id=request.operation_id,
            environment=request.environment, ownership=request.ownership,
            lifecycle="project_service",
            state="accepted", log_ref=process_id)
        row = ProcessRecord(request, handle, command_digest(request))
        repo.rows[process_id] = row
        async with executions.prepare(request, process_id) as execution:
            await repo.save(row, replace(row, execution_id=execution.execution_id,
                                        revision=row.revision + 1))
            attachment = await asyncio.create_subprocess_exec(*execution.attach_argv,
                stdin=asyncio.subprocess.DEVNULL, stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE)
            for _ in range(100):
                if (content / "listening").exists():
                    break
                await asyncio.sleep(.025)
            assert (content / "listening").exists()
        row = repo.rows[process_id]
        await repo.save(row, replace(row, handle=row.handle.model_copy(update={"state": "running"}),
                                    revision=row.revision + 1))
        viewer = PrincipalContext("viewer", 1, ())
        instance = await routes.observe(repo.rows[process_id].handle, 8123, viewer)
        binding = RouteBinding(**instance.model_dump(), preview_id=uuid.uuid4().hex,
            owner_id=env.owner_id, project_id=env.project_id, environment_id=env.environment_id,
            generation=1, process_id=process_id, service_id=process_id,
            operation_id=request.operation_id, request_digest=command_digest(request),
            port=8123, route_epoch=1)
        source.binding = binding
        inspected = await runner.run(("container", "inspect", execution.execution_id))
        raw = json.loads(inspected.stdout)[0]
        assert raw["HostConfig"]["NetworkMode"] == "none" and not raw["HostConfig"]["PortBindings"]
        assert raw["Config"]["User"] == "65532:65532" and not raw["HostConfig"]["CapAdd"]
        passed("observe-ready-no-network-publication")
        async with routes.acquire(binding, None) as host_lease:
            assert await routes.ready(host_lease) is True
        refused = binding.model_copy(update={"port": 8124, "preview_id": "not-listening"})
        source.binding = refused
        async with routes.acquire(refused, None) as host_lease:
            assert await routes.ready(host_lease) is False
        source.binding = binding
        assert not routes._connections and not routes._service_counts
        passed("host-readiness-connect-close-and-not-listening")
        baseline_fds = len(list(Path("/proc/self/fd").iterdir()))
        payload = (b"x" * 512000 if os.environ.get("KRIM_PREVIEW_ASCII_DATA") == "1"
                   else bytes(range(256)) * 2000)
        # Binary echo, FIN -> complete response. Renew independently of IO.
        async with routes.acquire(binding, viewer) as lease:
            renewing = True

            async def renew():
                while renewing:
                    await asyncio.sleep(.2)
                    if renewing:
                        await routes.check(lease)

            task = asyncio.create_task(renew())
            try:
                async with routes.connect(lease) as sock:
                    relay = next(iter(routes._leases[lease.lease_id].relays))
                    async def send():
                        await asyncio.get_running_loop().sock_sendall(sock, b"H" + payload)
                        sock.shutdown(socket.SHUT_WR)
                    sent = asyncio.create_task(send())
                    try:
                        reply = await asyncio.wait_for(receive_all(sock), 10)
                    except BaseException:
                        (evidence / "halfclose-state.json").write_text(json.dumps({
                            "credit": relay._credit, "uncredited": relay._uncredited,
                            "incoming": len(relay._incoming), "remote_eof": relay._remote_eof,
                            "closing": relay._closing, "sent_done": sent.done(),
                            "returncode": relay.process.returncode,
                            "tcp_received": (content / "half-size").read_text()
                                if (content / "half-size").exists() else None,
                            "tcp_eof": (content / "half-eof").exists(),
                        }, indent=2))
                        sent.cancel()
                        await asyncio.gather(sent, return_exceptions=True)
                        raise
                    (evidence / "halfclose-completion.json").write_text(json.dumps({
                        "reply": len(reply), "credit": relay._credit,
                        "uncredited": relay._uncredited, "incoming": len(relay._incoming),
                        "closing": relay._closing, "failure": relay._failure_type,
                        "stderr": relay._stderr_hint.decode(errors="replace"),
                        "tcp_received": (content / "half-size").read_text()
                            if (content / "half-size").exists() else None,
                        "tcp_eof": (content / "half-eof").exists(),
                    }, indent=2))
                    await sent
                    assert reply == payload[::-1]
                    relay_pid = relay.pid
                    close_started = time.monotonic()
            finally:
                renewing = False
                await task
        probe_time = await relay_absent(relay_pid)
        bound = time.monotonic() - close_started
        assert bound < 2
        passed("binary-half-close-and-relay-exit", bytes=len(payload),
               close_to_proc_absent_seconds=bound, proc_probe_seconds=probe_time)
        # Slow receiver creates genuine relay stdout/TCP backpressure; lease TTL
        # must still close the endpoint and the container process within a bound.
        async with routes.acquire(binding, viewer) as lease:
            async with routes.connect(lease) as sock:
                relay = next(iter(routes._leases[lease.lease_id].relays))
                await asyncio.get_running_loop().sock_sendall(sock, b"F")
                await asyncio.sleep(1.2)
                assert lease.lease_id not in routes._leases
                try:
                    await routes.check(lease)
                except PreviewRouteError as error:
                    assert error.code == "preview_route_invalidated"
                else:
                    raise AssertionError("expired lease revived")
                relay_pid = relay.pid
        assert await relay_absent(relay_pid) < 2
        assert (await executions.inspect(execution)).state == "running"
        passed("backpressure-ttl-closes-relay-not-service")
        async with routes.acquire(binding, viewer) as lease:
            async with routes.connect(lease) as sock:
                relay = next(iter(routes._leases[lease.lease_id].relays))
                sending = asyncio.create_task(asyncio.get_running_loop().sock_sendall(
                    sock, b"S" + b"x" * (8 * 1024 * 1024)))
                await asyncio.sleep(1.2)
                assert lease.lease_id not in routes._leases
                await asyncio.gather(sending, return_exceptions=True)
                relay_pid = relay.pid
        assert await relay_absent(relay_pid) < 2
        assert (await executions.inspect(execution)).state == "running"
        passed("upload-backpressure-does-not-block-expiry")
        async with routes.acquire(binding, viewer) as lease:
            async with routes.connect(lease) as sock:
                relay = next(iter(routes._leases[lease.lease_id].relays))
                await asyncio.get_running_loop().sock_sendall(sock, b"S")
                killed_at = time.monotonic()
                relay.process.kill()  # deliberately kill only this local Docker attachment
                await relay.process.wait()
                await relay_absent(relay.pid)
                assert time.monotonic() - killed_at < 2
        assert (await executions.inspect(execution)).state == "running"
        passed("host-cli-death-still-reaps-container-relay-not-service")
        for label, values in (("absolute-age", {"max_age_seconds": 1.0}),
                              ("business-idle", {"idle_seconds": .6})):
            bounded = DockerPreviewRoutes(backend, source, limits=PreviewLimits(
                heartbeat_seconds=.1, **values))
            try:
                async with bounded.acquire(binding, viewer) as lease:
                    async with bounded.connect(lease) as sock:
                        relay = next(iter(bounded._leases[lease.lease_id].relays))
                        await asyncio.get_running_loop().sock_sendall(sock, b"S")
                        async with asyncio.timeout(2.5):
                            assert await asyncio.get_running_loop().sock_recv(sock, 1) == b""
                        relay_pid = relay.pid
                assert await relay_absent(relay_pid) < 2
                assert (await executions.inspect(execution)).state == "running"
                passed("real-relay-" + label + "-exit")
            finally:
                await bounded.aclose()
        async with routes.acquire(binding, viewer) as lease:
            async with routes.connect(lease) as sock:
                relay = next(iter(routes._leases[lease.lease_id].relays))
                source.slow = True
                checking = asyncio.create_task(routes.check(lease))
                await asyncio.sleep(1.0)
                assert lease.lease_id not in routes._leases
                checking.cancel()
                await asyncio.gather(checking, return_exceptions=True)
                source.slow = False
                relay_pid = relay.pid
        await relay_absent(relay_pid)
        passed("blocked-source-does-not-block-expiry")
        async with routes.acquire(binding, viewer) as lease:
            async with routes.connect(lease):
                source.revoked = True
                try:
                    await routes.check(lease)
                except PreviewRouteError:
                    pass
                else:
                    raise AssertionError("revoked route accepted")
                assert lease.lease_id not in routes._leases
                source.revoked = False
        passed("original-viewer-revocation-closes-stream")
        # A persistent stopping state denies new routes without touching service.
        previous = repo.environment
        repo.environment = replace(previous,
            environment=previous.environment.model_copy(update={"state": "stopping"}))
        try:
            async with routes.acquire(binding, viewer):
                raise AssertionError("stopping environment routed")
        except PreviewRouteError:
            pass
        repo.environment = previous
        drift = binding.model_copy(update={"started_at": "2000-01-01T00:00:00.000000000Z"})
        source.binding = drift
        try:
            async with routes.acquire(drift, viewer):
                raise AssertionError("wrong instance routed")
        except PreviewRouteError:
            pass
        source.binding = binding
        passed("persistent-stop-and-startedat-drift-refused")
        async with routes.acquire(binding, viewer) as lease:
            async with routes.connect(lease):
                # Trusted test-only manual restart of this exact created ID.
                result = await runner.run(("restart", execution.execution_id))
                assert result.code == 0
                try:
                    await routes.check(lease)
                except PreviewRouteError:
                    pass
                else:
                    raise AssertionError("old lease followed a restarted container")
        try:
            async with routes.acquire(binding, viewer):
                raise AssertionError("registered old instance silently refreshed")
        except PreviewRouteError:
            pass
        fresh = await routes.observe(repo.rows[process_id].handle, 8123, viewer)
        assert fresh.started_at != binding.started_at
        # A new authorized registration, never an implicit lease retarget.
        binding = binding.model_copy(update={"preview_id": uuid.uuid4().hex,
                                             "started_at": fresh.started_at})
        source.binding = binding
        passed("same-container-id-restart-invalidates-old-instance")
        async with routes.acquire(binding, viewer) as lease:
            async with routes.connect(lease):
                await asyncio.wait_for(backend.stop(env), 3)
                assert repo.environment.environment.state == "stopped"
        passed("live-stream-does-not-hold-stop-lock")
        await routes.aclose()
        assert not routes._connections and not routes._service_counts
        await asyncio.sleep(.2)
        final_fds = len(list(Path("/proc/self/fd").iterdir()))
        assert final_fds <= baseline_fds + 2, (baseline_fds, final_fds)
        passed("all-capacity-and-host-fds-released", before=baseline_fds, after=final_fds)
    except BaseException as error:
        (evidence / "failure.txt").write_text(repr(error))
        raise
    finally:
        await routes.aclose()
        await runner.cleanup()
        if attachment is not None:
            await asyncio.wait_for(attachment.communicate(), 10)
        shutil.rmtree(root)
        sources = Path(__file__).resolve().parents[2] / "src/krim_agent/dev_environment"
        hashes = {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                  for p in sources.glob("*.py")}
        (evidence / "source-hashes.json").write_text(json.dumps(hashes, indent=2))
