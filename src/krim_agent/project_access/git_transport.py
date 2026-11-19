"""Git-only Docker/runsc transport over a bounded host stdio HTTP bridge.

Every container retains network=none. Git speaks its mature smart-HTTP protocol
to a credential-free loopback HTTP server; HTTPX/PinnedAsyncTransport owns the
real HTTPS connection. The bridge restricts exact repository endpoints and never
interprets pack files or supplies a general proxy/CONNECT tunnel.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
import re
import shutil
import stat
import subprocess
import sys
import tempfile
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx

from krim_agent.contracts import ServiceError, fingerprint, parse_json
from krim_agent.dev_environment.docker import DockerCLI, Runner
from krim_agent.development.contracts import AuthorizationRef, EnvironmentHandle
from krim_agent.network.egress_policy import EgressPolicy
from krim_agent.network.http_transport import PinnedAsyncTransport
from krim_agent.project_access.egress import ProjectEgressPolicy
from krim_agent.project_access.git import GitOperation
from krim_agent.project_access.git_credentials import NativeGitCredentialCache, helper_request
from krim_agent.project_access.git_execution import TrustedGitRunner
from krim_agent.project_access.types import AccessIntent, ResolvedAccess

_PREFIX = DockerCLI.prefix
_CLEAN = {"PATH": "/usr/bin:/bin", "HOME": "/nonexistent", "LC_ALL": "C"}
_LABEL = "io.krim.git-transport.binding"
_IMAGE = re.compile(r"sha256:[0-9a-f]{64}\Z")
_ID = re.compile(r"[0-9a-f]{64}\Z")


@dataclass(frozen=True)
class GitNetworkAuthorization:
    """Authoritative current projection, resolved by I from original intent/ref."""

    access: ResolvedAccess
    policy: ProjectEgressPolicy
    environment: EnvironmentHandle
    image_id: str


@dataclass(frozen=True)
class GitTransportBinding:
    environment: EnvironmentHandle
    authorization: AuthorizationRef
    intent: AccessIntent
    image_id: str

    def __post_init__(self) -> None:
        environment, intent = self.environment, self.intent
        if (
            (environment.owner_id, environment.project_id) != (intent.owner_id, intent.project_id)
            or self.authorization.project_id != intent.project_id
            or self.authorization.arguments_digest != intent.digest(self.authorization)
            or not _IMAGE.fullmatch(self.image_id)
            or intent.action not in ("git.clone", "git.push")
        ):
            raise ServiceError("permission_denied", 403)

    @property
    def digest(self) -> str:
        environment = self.environment.model_dump(mode="json")
        environment.pop("state", None)
        return fingerprint(
            {
                "environment": environment,
                "authorization": self.authorization.model_dump(mode="json"),
                "intent": self.intent.digest(self.authorization),
                "image": self.image_id,
            }
        )


class GitHttpBroker:
    """Small operation-scoped HTTPS adapter; HTTP is parsed by standard libraries."""

    def __init__(
        self,
        operation: GitOperation,
        initial: GitNetworkAuthorization,
        resolve: Callable[[], Awaitable[GitNetworkAuthorization]],
        *,
        authorization: str | None = None,
        max_bytes: int = 16 * 1024 * 1024,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        if type(max_bytes) is not int or not 1 <= max_bytes <= 16 * 1024 * 1024:
            raise ValueError("invalid Git transfer bound")
        self.operation = operation
        self.initial = initial
        self.resolve = resolve
        self.authorization = authorization
        self.max_bytes = max_bytes
        self.closed = False
        self.total = 0
        self.started = time.monotonic()
        self._client = httpx.AsyncClient(
            transport=transport
            or PinnedAsyncTransport(
                EgressPolicy(initial.policy.grant.hosts, initial.policy.grant.ports)
            ),
            trust_env=False,
            follow_redirects=False,
            timeout=30,
        )

    async def check(self) -> None:
        if self.closed or time.monotonic() - self.started > 120:
            raise ServiceError("permission_denied", 403)
        current = await asyncio.wait_for(self.resolve(), 5)
        if (
            current != self.initial
            or current.policy.grant.revoked
            or current.policy.grant.expires_at <= time.time()
        ):
            raise ServiceError("permission_denied", 403)
        current.policy.validate_target(self.operation.repository)

    def target(self, method: str, path: str) -> str:
        service = "git-upload-pack" if self.operation.action == "git.clone" else "git-receive-pack"
        # git ls-remote for push queries upload-pack before the leased push.
        services = (
            {service, "git-upload-pack"} if self.operation.action == "git.push" else {service}
        )
        if method == "GET" and path in {f"/info/refs?service={s}" for s in services}:
            return self.operation.repository + path
        if method == "POST" and path == f"/{service}":
            return self.operation.repository + path
        raise ServiceError("permission_denied", 403)

    async def request(self, value: dict[str, Any]) -> dict[str, Any]:
        failed = False
        try:
            return await self._request(value)
        except asyncio.CancelledError:
            raise
        except Exception:
            failed = True
        if failed:
            self.authorization = None
            self.closed = True
        raise ServiceError("permission_denied", 403)

    async def _request(self, value: dict[str, Any]) -> dict[str, Any]:
        await self.check()
        method, path = value.get("method"), value.get("path")
        if not isinstance(method, str) or not isinstance(path, str):
            raise ServiceError("invalid_input", 422)
        url = self.target(method, path)
        raw_headers = value.get("headers", {})
        if not isinstance(raw_headers, dict) or any(
            k.lower()
            in {
                "authorization",
                "proxy-authorization",
                "transfer-encoding",
                "x-http-method-override",
                "cookie",
            }
            for k in raw_headers
        ):
            raise ServiceError("permission_denied", 403)
        encoded_body = value.get("body", "")
        if not isinstance(encoded_body, str) or len(encoded_body) > 2 * self.max_bytes:
            raise ServiceError("input_limit", 422)
        body = base64.b64decode(encoded_body, validate=True)
        if len(body) > self.max_bytes or (method == "GET" and body):
            raise ServiceError("input_limit", 422)
        headers = {"Accept-Encoding": "identity"}
        for name in ("content-type", "accept", "git-protocol"):
            if name in raw_headers:
                item = raw_headers[name]
                if not isinstance(item, str) or len(item) > 256 or any(c in item for c in "\r\n"):
                    raise ServiceError("invalid_input", 422)
                headers[name] = item
        service = path.rsplit("/", 1)[-1]
        if method == "POST" and headers.get("content-type") != f"application/x-{service}-request":
            raise ServiceError("permission_denied", 403)
        if self.authorization is not None:
            headers["Authorization"] = self.authorization
        self.total += len(body)
        if self.total > 4 * self.max_bytes:
            raise ServiceError("input_limit", 422)

        async def guarded_body() -> AsyncIterator[bytes]:
            for offset in range(0, len(body), 65536):
                await self.check()
                yield body[offset : offset + 65536]

        headers["Content-Length"] = str(len(body))
        async with self._client.stream(
            method, url, headers=headers, content=guarded_body()
        ) as response:
            # Redirects are never followed; credentials remain on exact repo.
            if (
                response.status_code != 200
                or response.headers.get("content-encoding", "identity") != "identity"
            ):
                raise ServiceError("execution_error", 502)
            data = bytearray()
            async for chunk in response.aiter_bytes():
                await self.check()
                if len(data) + len(chunk) > self.max_bytes:
                    raise ServiceError("input_limit", 422)
                data.extend(chunk)
            await self.check()
            self.total += len(data)
            if self.total > 4 * self.max_bytes:
                raise ServiceError("input_limit", 422)
            expected_type = (
                f"application/x-{service}-result"
                if method == "POST"
                else f"application/x-{path.partition('service=')[2]}-advertisement"
            )
            if response.headers.get("content-type", "").split(";", 1)[0] != expected_type:
                raise ServiceError("permission_denied", 403)
            return {
                "id": value["id"],
                "status": 200,
                "headers": {"content-type": expected_type, "cache-control": "no-cache"},
                "body": base64.b64encode(data).decode("ascii"),
            }

    async def close(self) -> None:
        self.closed = True
        self.authorization = None
        await self._client.aclose()


class DockerGitTransport:
    """Prepared single-operation GitTransport, never an arbitrary Shell lease.

    prepare/close run outside the SQLite guard; spawn is short and synchronous.
    The privileged trusted host owns DockerCLI. No new socket permissions or
    daemon configuration is needed. I owns authoritative environment resolution
    and broadcasts close/invalidate at committed revocation.
    """

    def __init__(
        self,
        binding: GitTransportBinding,
        *,
        stage_root: Path,
        repository_root: Path,
        resolve: Callable[[], Awaitable[GitNetworkAuthorization]],
        cache: NativeGitCredentialCache | None = None,
        runner: Runner | None = None,
        http_transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        if os.name != "posix" or not stage_root.is_absolute() or stage_root.resolve() != stage_root:
            raise ServiceError("invalid_input", 422)
        if any(c in str(stage_root) for c in (",", "\n", "\r")):
            raise ServiceError("invalid_input", 422)
        self.binding = binding
        args = parse_json(binding.intent.arguments_json)
        if not isinstance(args, dict) or "git" not in args:
            raise ServiceError("invalid_input", 422)
        self.operation = GitOperation.model_validate(args["git"])
        if self.operation.action != binding.intent.action:
            raise ServiceError("permission_denied", 403)
        self.resolve = resolve
        self.cache = cache
        self.runner = runner or DockerCLI()
        if self.runner.prefix != _PREFIX:
            raise ServiceError("permission_denied", 403)
        if not repository_root.is_absolute() or repository_root.resolve() != repository_root:
            raise ServiceError("permission_denied", 403)
        TrustedGitRunner._secure_dir(repository_root, private=True)
        self.repository_root = repository_root
        self._root_identity = (repository_root.stat().st_dev, repository_root.stat().st_ino)
        self.stage_root = stage_root
        self.http_transport = http_transport
        self.directory: Path | None = None
        self.container_id: str | None = None
        self.bridge: asyncio.subprocess.Process | None = None
        self.broker: GitHttpBroker | None = None
        self.port: int | None = None
        self._serve_task: asyncio.Task[None] | None = None
        self._watch_task: asyncio.Task[None] | None = None
        self._workers: set[subprocess.Popen[bytes]] = set()
        self._commands: set[str] = set()
        self._closed = False
        self._prepared = False
        self._repository: Path | None = None
        self._cleanup_complete = False
        self._cleanup_task: asyncio.Task[None] | None = None
        self._preparing = False
        self._prepare_done = asyncio.Event()
        self._prepare_done.set()
        self._close_lock = asyncio.Lock()

    async def prepare(self, repository: Path) -> None:
        if self._preparing or self._prepared or self._closed:
            raise ServiceError("permission_denied", 403)
        self._preparing = True
        self._prepare_done.clear()
        failed = False
        cancelled = False
        try:
            await self._prepare(repository)
            if self._closed:
                failed = True
        except asyncio.CancelledError:
            cancelled = True
        except Exception:
            failed = True
        finally:
            self._preparing = False
            self._prepare_done.set()
        if failed or cancelled:
            await asyncio.shield(self.close())
            if cancelled:
                raise asyncio.CancelledError
            raise ServiceError("execution_error", 500)

    async def _prepare(self, repository: Path) -> None:
        if self._prepared or self._closed:
            raise ServiceError("permission_denied", 403)
        TrustedGitRunner._secure_dir(self.stage_root, private=True)
        self._repository = repository
        expected_name = (
            hashlib.sha256(self.operation.repository.encode("ascii")).hexdigest() + ".git"
        )
        owner = TrustedGitRunner._safe_component(self.binding.intent.owner_id)
        project = TrustedGitRunner._safe_component(self.binding.intent.project_id)
        root_info = self.repository_root.stat()
        if (
            repository != self.repository_root / owner / project / expected_name
            or self.repository_root.resolve() != self.repository_root
            or (root_info.st_dev, root_info.st_ino) != self._root_identity
        ):
            raise ServiceError("permission_denied", 403)
        await self._resolve_current()
        self.directory = Path(tempfile.mkdtemp(prefix="gt-", dir=self.stage_root))
        staging = asyncio.create_task(
            asyncio.to_thread(stage_repository, repository, self.directory, self.operation)
        )
        try:
            await asyncio.shield(staging)
        except asyncio.CancelledError:
            await asyncio.shield(staging)
            raise
        work = self.directory / "work"
        initial = await self._resolve_current()
        authorization = await self._read_credential() if self.cache is not None else None
        self.broker = GitHttpBroker(
            self.operation,
            initial,
            self._resolve_current,
            authorization=authorization,
            transport=self.http_transport,
        )
        await self.broker.check()
        create = (
            "create",
            "--runtime=runsc",
            "--network=none",
            "--cap-drop=ALL",
            "--security-opt=no-new-privileges:true",
            "--read-only",
            "--user=65532:65532",
            "--memory=268435456",
            "--cpus=0.5",
            "--pids-limit=64",
            "--tmpfs=/tmp:rw,nosuid,nodev,size=33554432,mode=1777",
            "--tmpfs=/home/krim:rw,nosuid,nodev,size=8388608,uid=65532,gid=65532,mode=700",
            "--mount",
            f"type=bind,src={work},dst=/git",
            "--label",
            f"{_LABEL}={self.binding.digest}",
            "--entrypoint=/usr/bin/sleep",
            self.binding.image_id,
            "120",
        )
        try:
            creation = asyncio.create_task(self.runner.run(create))
            cancelled = False
            while not creation.done():
                try:
                    await asyncio.shield(creation)
                except asyncio.CancelledError:
                    cancelled = True
            reply = creation.result()
            identifier = reply.stdout.strip()
            if reply.code or not _ID.fullmatch(identifier):
                raise ServiceError("execution_error", 500)
            self.container_id = identifier
            if cancelled:
                raise asyncio.CancelledError
            await self._inspect()
            reply = await self.runner.run(("start", identifier))
            if reply.code:
                raise ServiceError("execution_error", 500)
            source = await asyncio.to_thread(
                Path(__file__).with_name("git_transport_shim.py").read_text
            )
            self.bridge = await asyncio.create_subprocess_exec(
                *_PREFIX,
                "exec",
                "-i",
                identifier,
                "/usr/bin/python3",
                "-I",
                "-u",
                "-c",
                source,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
                env=_CLEAN,
                limit=34 * 1024 * 1024,
            )
            await self._write({"max_bytes": 16 * 1024 * 1024})
            ready = await self._read()
            if (
                set(ready) != {"ready"}
                or type(ready["ready"]) is not int
                or not 1 <= ready["ready"] <= 65535
            ):
                raise ServiceError("execution_error", 500)
            self.port = ready["ready"]
            self._prepared = True
            self._serve_task = asyncio.create_task(self._serve())
            self._watch_task = asyncio.create_task(self._watch())
        except BaseException:
            raise

    async def _resolve_current(self) -> GitNetworkAuthorization:
        if self._closed:
            raise ServiceError("permission_denied", 403)
        current = await asyncio.wait_for(self.resolve(), 5)
        if self._closed:
            raise ServiceError("permission_denied", 403)
        ref, intent = self.binding.authorization, self.binding.intent
        receipt = current.access
        expected_environment = self.binding.environment.model_dump(exclude={"state"})
        if (
            current.environment.model_dump(exclude={"state"}) != expected_environment
            or current.image_id != self.binding.image_id
            or (
                receipt.actor_id,
                receipt.actor_generation,
                receipt.project_id,
                receipt.operation_id,
                receipt.arguments_digest,
            )
            != (
                ref.actor_id,
                ref.actor_generation,
                intent.project_id,
                intent.operation_id,
                ref.arguments_digest,
            )
            or (
                current.policy.grant.owner_id,
                current.policy.grant.project_id,
                current.policy.grant.policy_id,
            )
            != (intent.owner_id, intent.project_id, ref.network_policy_ref)
            or receipt.network_revision != current.policy.grant.revision
            or current.policy.grant.revoked
            or current.policy.grant.expires_at <= time.time()
            or current.environment.state != "ready"
        ):
            raise ServiceError("permission_denied", 403)
        if self.cache is not None:
            binding = self.cache.binding
            if receipt.secret_revisions != ((binding.secret_id, binding.revision),):
                raise ServiceError("permission_denied", 403)
        elif receipt.secret_revisions:
            raise ServiceError("permission_denied", 403)
        return current

    async def _read_credential(self) -> str:
        assert self.cache is not None and self.directory is not None
        if (
            self.cache.binding.intent != self.binding.intent
            or self.cache.binding.authorization != self.binding.authorization
        ):
            raise ServiceError("permission_denied", 403)
        process = await asyncio.create_subprocess_exec(
            "/usr/bin/git",
            "-c",
            "credential.helper=",
            "-c",
            f"credential.helper={self.cache.helper}",
            "-c",
            "credential.useHttpPath=true",
            "credential",
            "fill",
            cwd=self.directory,
            env={
                **_CLEAN,
                "GIT_CONFIG_NOSYSTEM": "1",
                "GIT_CONFIG_GLOBAL": "/dev/null",
                "GIT_TERMINAL_PROMPT": "0",
                "GIT_ASKPASS": "/bin/false",
            },
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        try:
            raw, _ = await asyncio.wait_for(process.communicate(helper_request(self.operation)), 5)
            if process.returncode or len(raw) > 16384:
                raise ServiceError("execution_error", 500)
            fields = dict(line.split(b"=", 1) for line in raw.splitlines() if b"=" in line)
            username, password = fields[b"username"], fields[b"password"]
            return "Basic " + base64.b64encode(username + b":" + password).decode("ascii")
        finally:
            if process.returncode is None:
                process.kill()
            await process.wait()

    async def _inspect(self) -> None:
        assert self.container_id is not None and self.directory is not None
        reply = await self.runner.run(("inspect", self.container_id))
        try:
            item = json.loads(reply.stdout)[0]
            host, config = item["HostConfig"], item["Config"]
            mount = item["Mounts"]
            if (
                reply.code
                or item["Id"] != self.container_id
                or item["Image"] != self.binding.image_id
                or config["Labels"].get(_LABEL) != self.binding.digest
                or config["User"] != "65532:65532"
                or host["NetworkMode"] != "none"
                or host["Runtime"] != "runsc"
                or host["Privileged"]
                or host.get("CapAdd")
                or host["CapDrop"] != ["ALL"]
                or not host["ReadonlyRootfs"]
                or "no-new-privileges:true" not in host["SecurityOpt"]
                or host["Memory"] != 268435456
                or host["NanoCpus"] != 500000000
                or host["PidsLimit"] != 64
                or len(mount) != 1
                or mount[0]["Type"] != "bind"
                or mount[0]["Source"] != str(self.directory / "work")
                or mount[0]["Destination"] != "/git"
            ):
                raise ValueError
        except (KeyError, ValueError, TypeError, IndexError):
            raise ServiceError("permission_denied", 403) from None

    async def _write(self, value: dict[str, Any]) -> None:
        assert self.bridge is not None and self.bridge.stdin is not None
        self.bridge.stdin.write(json.dumps(value, separators=(",", ":")).encode() + b"\n")
        await self.bridge.stdin.drain()

    async def _read(self) -> dict[str, Any]:
        assert self.bridge is not None and self.bridge.stdout is not None
        # Streams default line limit is raised by explicit bounded readuntil
        # on the subprocess factory in production; frames also validate sizes.
        raw = await asyncio.wait_for(self.bridge.stdout.readline(), 30)
        if not raw or len(raw) > 34 * 1024 * 1024:
            raise ServiceError("input_limit", 422)
        value = json.loads(raw)
        if not isinstance(value, dict):
            raise ServiceError("invalid_input", 422)
        return value

    async def _serve(self) -> None:
        assert self.broker is not None
        try:
            while not self._closed:
                value = await self._read()
                result = await self.broker.request(value)
                await self._write(result)
        except asyncio.CancelledError:
            raise
        except Exception:
            self._schedule_close()

    async def _watch(self) -> None:
        assert self.broker is not None
        try:
            while not self._closed:
                await asyncio.sleep(0.1)
                await self.broker.check()
                if any(process.poll() not in (None, 0) for process in self._workers):
                    self._schedule_close()
                    return
        except asyncio.CancelledError:
            raise
        except Exception:
            self._schedule_close()

    def spawn(
        self,
        operation: GitOperation,
        argv: tuple[str, ...],
        *,
        cwd: Path,
        env: dict[str, str],
        stdin: int,
        stdout: int,
        stderr: int,
    ) -> subprocess.Popen[bytes]:
        assert self.directory is not None and self._repository is not None
        assert self.container_id is not None and self.port is not None
        if (
            self._closed
            or not self._prepared
            or operation != self.operation
            or (self.broker is not None and self.broker.closed)
            or any(p.poll() is None for p in self._workers)
            or argv.count(operation.repository) != 1
        ):
            raise ServiceError("permission_denied", 403)
        # Actual argv remains a typed runner product. Drop credential helper
        # settings; credentials exist only on the host broker side.
        command_at = next(
            (i for i, item in enumerate(argv) if item in {"clone", "push", "ls-remote"}), None
        )
        if command_at is None:
            raise ServiceError("permission_denied", 403)
        command = argv[command_at]
        if command in self._commands or (command == "push" and "ls-remote" not in self._commands):
            raise ServiceError("permission_denied", 403)
        expected: tuple[str, ...]
        if command == "clone":
            expected = (
                "clone",
                "--bare",
                "--single-branch",
                "--no-tags",
                "--no-recurse-submodules",
                "--branch",
                TrustedGitRunner._clone_branch(operation.ref),
                operation.repository,
                str(self._repository),
            )
            expected_cwd = self._repository.parent
        elif command == "push":
            expected = (
                "push",
                "--porcelain",
                f"--force-with-lease={operation.ref}:{operation.expected_old_id}",
                operation.repository,
                f"{operation.object_id}:{operation.ref}",
            )
            expected_cwd = self._repository
        else:
            expected = ("ls-remote", "--refs", operation.repository, operation.ref)
            expected_cwd = self._repository
        if (
            tuple(argv[command_at:]) != expected
            or cwd != expected_cwd
            or (operation.action == "git.clone" and command != "clone")
            or (operation.action == "git.push" and command not in ("push", "ls-remote"))
        ):
            raise ServiceError("permission_denied", 403)
        args = [
            f"http://127.0.0.1:{self.port}/repo.git" if item == operation.repository else item
            for item in expected
        ]
        promote = None
        if command == "clone":
            target = Path(args[-1])
            if target != self._repository:
                raise ServiceError("permission_denied", 403)
            args[-1] = "/git/repo.git"
            args.insert(1, "--template=")
            info = target.lstat()
            promote = {
                "source": str(self.directory / "work/repo.git"),
                "target": str(target),
                "device": info.st_dev,
                "inode": info.st_ino,
                "ref": operation.ref,
                "object_id": operation.object_id,
            }
        docker_argv = [
            *_PREFIX,
            "exec",
            "-i",
            "-w",
            "/git" if command == "clone" else "/git/repo.git",
            self.container_id,
            "/usr/bin/env",
            "-i",
            "PATH=/usr/bin:/bin",
            "HOME=/home/krim",
            "GIT_CONFIG_NOSYSTEM=1",
            "GIT_CONFIG_GLOBAL=/dev/null",
            "GIT_TERMINAL_PROMPT=0",
            "GIT_ASKPASS=/bin/false",
            "LC_ALL=C",
            "/usr/bin/git",
            "--no-pager",
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "credential.helper=",
            "-c",
            "http.followRedirects=false",
            "-c",
            "http.postBuffer=16777216",
            "-c",
            "protocol.allow=never",
            "-c",
            "protocol.http.allow=always",
            "-c",
            "submodule.recurse=false",
            "-c",
            "protocol.version=0",
            *args,
        ]
        config = self.directory / f"worker-{len(self._workers)}.json"
        config.write_text(json.dumps({"argv": docker_argv, "promote": promote}))
        config.chmod(0o600)
        process = subprocess.Popen(
            (
                sys.executable,
                "-I",
                str(Path(__file__).with_name("git_transport_worker.py")),
                str(config),
            ),
            cwd=self.directory,
            env=_CLEAN,
            stdin=stdin,
            stdout=stdout,
            stderr=stderr,
            start_new_session=True,
            close_fds=True,
        )
        self._workers.add(process)
        self._commands.add(command)
        return process

    def _schedule_close(self) -> None:
        self._closed = True
        if self.broker is not None:
            self.broker.closed = True
        if self._cleanup_task is None or self._cleanup_task.done():
            self._cleanup_task = asyncio.create_task(self.close())
            self._cleanup_task.add_done_callback(
                lambda task: None if task.cancelled() else task.exception()
            )

    async def close(self) -> None:
        """Permanently stop admissions; cleanup can be retried after a daemon error."""
        self._closed = True
        if self.broker is not None:
            self.broker.closed = True
            self.broker.authorization = None
        await self._prepare_done.wait()
        async with self._close_lock:
            if self._cleanup_complete:
                return
            self._closed = True
            failed = False
            current = asyncio.current_task()
            tasks = [
                t
                for t in (self._serve_task, self._watch_task)
                if t is not None and t is not current
            ]
            for task in tasks:
                task.cancel()
            if self.broker is not None:
                try:
                    await self.broker.close()
                except Exception:
                    failed = True
            if self.cache is not None:
                try:
                    await asyncio.to_thread(self.cache.close)
                except Exception:
                    failed = True
            if self.container_id is not None:
                try:
                    await self._inspect()
                    reply = await self.runner.run(("rm", "-f", self.container_id))
                    if reply.code:
                        raise ServiceError("execution_error", 500)
                    self.container_id = None
                except Exception:
                    failed = True
            if self.bridge is not None:
                if self.bridge.returncode is None:
                    self.bridge.kill()
                await self.bridge.wait()
            for process in self._workers:
                if process.poll() is None:
                    try:
                        os.killpg(process.pid, 9)
                    except ProcessLookupError:
                        pass
                await asyncio.to_thread(process.wait)
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            if not failed and self.directory is not None:
                shutil.rmtree(self.directory)
                self.directory = None
            if failed:
                raise ServiceError("execution_error", 500)
            self._cleanup_complete = True


def check_repository(repository: Path) -> None:
    TrustedGitRunner._secure_dir(repository, private=True)
    size = 0
    count = 0
    for base, dirs, files in os.walk(repository, followlinks=False):
        for name in (*dirs, *files):
            path = Path(base) / name
            info = path.lstat()
            if (
                not (stat.S_ISDIR(info.st_mode) or stat.S_ISREG(info.st_mode))
                or info.st_uid != os.geteuid()
                or info.st_mode & 0o022
            ):
                raise ServiceError("permission_denied", 403)
            if stat.S_ISREG(info.st_mode):
                size += info.st_size
            count += 1
            if size > 128 * 1024 * 1024 or count > 65536:
                raise ServiceError("input_limit", 422)
    for relative in (
        "objects/info/alternates",
        "objects/info/http-alternates",
        "commondir",
        "config.worktree",
    ):
        if (repository / relative).exists():
            raise ServiceError("permission_denied", 403)


def stage_repository(repository: Path, directory: Path, operation: GitOperation) -> None:
    if repository.resolve() != repository:
        raise ServiceError("permission_denied", 403)
    if operation.action == "git.clone":
        TrustedGitRunner._secure_dir(repository.parent, private=True)
        if repository.exists() or repository.is_symlink():
            raise ServiceError("permission_denied", 403)
    else:
        check_repository(repository)
        config = subprocess.run(
            (
                "/usr/bin/git",
                "-C",
                str(repository),
                "config",
                "--local",
                "--no-includes",
                "--null",
                "--list",
            ),
            env={**_CLEAN, "GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": "/dev/null"},
            capture_output=True,
            timeout=10,
            check=True,
        )
        TrustedGitRunner._validate_config(
            TrustedGitRunner._parse_config(config.stdout), operation.repository
        )
    work = directory / "work"
    work.mkdir(mode=0o700)
    if operation.action == "git.push":
        shutil.copytree(repository, work / "repo.git")
    for base, _dirs, files in os.walk(work):
        os.chown(base, 65532, 65532)
        os.chmod(base, 0o700)
        for name in files:
            os.chown(Path(base) / name, 65532, 65532)
            os.chmod(Path(base) / name, 0o600)
