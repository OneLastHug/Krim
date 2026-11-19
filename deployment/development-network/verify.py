"""Real Docker/runsc acceptance for the development download network.

This is a trusted-root host probe.  It creates only this run's idle containers,
keeps Docker on the fixed local Unix socket, and exercises the production
``RuntimeSpec``/``create_args``/``docker.verify`` and ``DownloadNetworks``
implementations.  The mutable ``NetworkGrant`` resolver below is deliberately a
small test substitute for the production authorization service; it is marked as
such in the evidence and is never presented as a production grant.

The probe never runs Docker itself at import time.  Run it from a WSL Python
interpreter with a fresh F-drive evidence directory, for example::

    MSYS_NO_PATHCONV=1 python3 \
      /mnt/f/vscode\\ project/Krim/.claude/worktrees/relaxed-dijkstra-3e2dd1/\
      deployment/development-network/verify.py \
      --evidence /mnt/f/vscode\\ project/Krim/.claude/worktrees/relaxed-dijkstra-3e2dd1/\
      deployment/development-network/evidence

The image, Docker socket, sandbox root, and policy are intentionally fixed in
this file.  There is no ``--image`` override.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import shutil
import stat
import sys
import time
import uuid
from dataclasses import replace
from pathlib import Path
from typing import Any

# Make an invocation independent of the caller's current directory.  This does
# not install anything and still leaves the actual package imports to Python.
_REPOSITORY = Path(__file__).resolve().parents[2]
_SRC = _REPOSITORY / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from krim_agent.dev_environment import docker, locking  # noqa: E402
from krim_agent.dev_environment.model import EnvironmentError, RuntimeSpec  # noqa: E402
from krim_agent.dev_network.manager import DownloadNetworks  # noqa: E402
from krim_agent.development.contracts import (  # noqa: E402
    AuthorizationRef,
    CommandRequest,
    EnvironmentHandle,
    RunOwnership,
)
from krim_agent.development.ports import EnvironmentRecord  # noqa: E402
from krim_agent.project_access.egress import ProjectEgressPolicy  # noqa: E402
from krim_agent.project_access.types import NetworkGrant  # noqa: E402

IMAGE_ID = "sha256:82098beca2bd1c8887f82deb8d2fba13f87da04938463881a5e95b3d346834dc"
SANDBOX_PARENT = Path("/srv/krim-sandbox")
SANDBOX_PREFIX = "b2-network-"
OWNER_ID = "b2-network-verifier"
POLICY_ID = "b2-network-registry"
REGISTRY_HOST = "registry.npmjs.org"
PACKAGE = "esbuild"
PACKAGE_VERSION = "0.25.12"
PROXY_PORT = 39119

_HEX64 = re.compile(r"^[0-9a-f]{64}$")
_RUN_ROOT = re.compile(r"^b2-network-[0-9a-f-]{36}$")
_LABEL = re.compile(r"[^A-Za-z0-9_.-]+")
_PEM = re.compile(
    r"-----BEGIN [^-\r\n]+-----.*?-----END [^-\r\n]+-----", re.DOTALL
)
_SECRET = re.compile(
    r"(?i)(authorization|proxy-authorization|password|passwd|token|secret|"
    r"private[_-]?key)\s*[:=]\s*[^\s,;]+"
)


class VerificationFailure(RuntimeError):
    """A bounded probe failure whose evidence contains the detailed command."""



def _scrub(value: str) -> str:
    """Keep evidence useful without copying credentials or private keys."""
    value = _PEM.sub("[REDACTED-PEM]", value)
    return _SECRET.sub(r"\1=[REDACTED]", value)



def _safe_label(value: str) -> str:
    value = _LABEL.sub("-", value).strip("-.")
    return value[:80] or "step"



def _error_token(error: BaseException) -> str:
    code = getattr(error, "code", None)
    if isinstance(code, str) and re.fullmatch(r"[A-Za-z0-9_.:-]+", code):
        return code
    if isinstance(error, VerificationFailure):
        return str(error)[:160]
    return type(error).__name__


class Evidence:
    """Append-only command evidence plus a live aggregate JSON document."""

    def __init__(self, path: Path, *, run_root: Path, project_id: str) -> None:
        self.path = path
        self.run_root = run_root
        self.project_id = project_id
        self.steps: list[dict[str, Any]] = []
        self.sequence = 0
        self.status = "running"
        self.failure: str | None = None
        self.containers: list[str] = []
        self.sandbox_removed = False
        self.cleanup_errors: list[str] = []
        self.metadata: dict[str, Any] = {
            "image": IMAGE_ID,
            "sandbox_parent": str(SANDBOX_PARENT),
            "sandbox_root": str(run_root),
            "project_id": project_id,
            "approved_hosts": [REGISTRY_HOST],
            "approved_ports": [443],
            "tested_package": {"name": PACKAGE, "version": PACKAGE_VERSION},
            "network_mode": "none",
            "docker_socket": "unix:///var/run/docker.sock",
            "runtime": "runsc",
            "uid_gid": "65532:65532",
            "network_grant_is_test_substitute": True,
            "resolver_is_mutable_test_double": True,
            "production_authorization_service_used": False,
            "notes": [
                "NetworkGrant and its mutable resolver stand in for production D/I authorization.",
                "All off-list and metadata probes are sent only to the local session proxy.",
                "No example.com or other off-list external connection is attempted.",
            ],
        }
        self.path.mkdir(parents=True, exist_ok=True)
        self._save()

    def _aggregate(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "failure": self.failure,
            "image": IMAGE_ID,
            "sandbox_root": str(self.run_root),
            "sandbox_removed": self.sandbox_removed,
            "containers": list(self.containers),
            "cleanup_errors": list(self.cleanup_errors),
            "policy": self.metadata,
            "steps": self.steps,
        }

    def _save(self) -> None:
        (self.path / "verification-results.json").write_text(
            json.dumps(self._aggregate(), indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

    def set_status(self, status: str, failure: str | None = None) -> None:
        self.status = status
        self.failure = _scrub(failure) if failure else None
        self._save()

    def add_container(self, container_id: str) -> None:
        self.containers.append(container_id)
        self._save()

    def remove_container(self, container_id: str) -> None:
        if container_id in self.containers:
            self.containers.remove(container_id)
        self._save()

    def add_cleanup_error(self, value: str) -> None:
        self.cleanup_errors.append(_scrub(value))
        self._save()

    def record(
        self,
        label: str,
        command: list[str] | tuple[str, ...],
        exit_code: int,
        expected_exit_code: int | str | None,
        stdout: str = "",
        stderr: str = "",
        *,
        details: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Store separate redacted streams and their hashes for one step."""
        self.sequence += 1
        safe_label = _safe_label(label)
        prefix = f"{self.sequence:04d}-{safe_label}"
        safe_stdout = _scrub(stdout)
        safe_stderr = _scrub(stderr)
        stdout_file = f"{prefix}.stdout"
        stderr_file = f"{prefix}.stderr"
        (self.path / stdout_file).write_text(safe_stdout, encoding="utf-8")
        (self.path / stderr_file).write_text(safe_stderr, encoding="utf-8")
        stdout_hash = hashlib.sha256(safe_stdout.encode("utf-8")).hexdigest()
        stderr_hash = hashlib.sha256(safe_stderr.encode("utf-8")).hexdigest()
        item: dict[str, Any] = {
            "step": self.sequence,
            "label": label,
            "command": [_scrub(str(part)) for part in command],
            "exit": exit_code,
            "expected_exit": expected_exit_code,
            "stdout_file": stdout_file,
            "stderr_file": stderr_file,
            "stdout_sha256": stdout_hash,
            "stderr_sha256": stderr_hash,
            "stdout_bytes": len(safe_stdout.encode("utf-8")),
            "stderr_bytes": len(safe_stderr.encode("utf-8")),
            # Keep short command output directly searchable in the summary.  The
            # complete, equally redacted streams remain in the two files above.
            "stdout": safe_stdout if len(safe_stdout) <= 65536 else "[see stdout_file]",
            "stderr": safe_stderr if len(safe_stderr) <= 65536 else "[see stderr_file]",
        }
        if details:
            item["details"] = json.loads(_scrub(json.dumps(details, ensure_ascii=False)))
        self.steps.append(item)
        self._save()
        print(
            json.dumps(
                {
                    "step": self.sequence,
                    "label": label,
                    "exit": exit_code,
                    "expected_exit": expected_exit_code,
                    "stdout_sha256": stdout_hash,
                    "stderr_sha256": stderr_hash,
                },
                ensure_ascii=False,
            ),
            flush=True,
        )
        return item

    def event(
        self,
        label: str,
        command: list[str] | tuple[str, ...],
        *,
        ok: bool,
        details: dict[str, Any] | None = None,
        stdout: str = "",
        stderr: str = "",
        **extra_details: Any,
    ) -> None:
        if extra_details:
            details = {**(details or {}), **extra_details}
        self.record(label, command, 0 if ok else 1, 0, stdout, stderr, details=details)


class RecordingDockerCLI:
    """A recorder around the production DockerCLI, never a fake runner."""

    prefix = docker.DockerCLI.prefix

    def __init__(self, evidence: Evidence) -> None:
        self.evidence = evidence
        self.real = docker.DockerCLI()
        if self.prefix != ("/usr/bin/docker", "--host=unix:///var/run/docker.sock"):
            raise VerificationFailure("DockerCLI prefix drifted from the fixed Unix socket")

    async def run(self, args: tuple[str, ...]) -> docker.Reply:
        reply = await self.real.run(args)
        self.evidence.record(
            "docker-cli",
            [*self.prefix, *args],
            reply.code,
            None,
            reply.stdout,
            reply.stderr,
            details={"source": "production DockerCLI"},
        )
        return reply

    async def expected(
        self,
        label: str,
        args: tuple[str, ...],
        expected: int | str,
    ) -> docker.Reply:
        reply = await self.real.run(args)
        self.evidence.record(
            label,
            [*self.prefix, *args],
            reply.code,
            expected,
            reply.stdout,
            reply.stderr,
            details={"source": "production DockerCLI"},
        )
        return reply


class MutableGrantResolver:
    """Test-only mutable resolver standing in for production D/I resolution."""

    def __init__(self, grant: NetworkGrant) -> None:
        self.grant = grant
        self.calls = 0

    async def __call__(self, spec: RuntimeSpec) -> ProjectEgressPolicy:
        self.calls += 1
        if spec.network.policy_ref != self.grant.policy_id:
            raise EnvironmentError("network_policy_binding_drift")
        # A fresh policy value on every resolution is intentional.  The manager
        # must compare the live grant and reject revocation/expiry, rather than
        # treating the initial NetworkGrant as a bearer capability.
        return ProjectEgressPolicy(self.grant)

    def set(self, grant: NetworkGrant) -> None:
        self.grant = grant


class Verifier:
    def __init__(self, evidence_path: Path) -> None:
        if os.name != "posix":
            raise VerificationFailure("run this verifier inside WSL/Linux")
        if not evidence_path.is_absolute():
            raise VerificationFailure("--evidence must be an absolute WSL path")
        evidence_resolved = evidence_path.resolve()
        if not evidence_resolved.as_posix().lower().startswith("/mnt/f/"):
            raise VerificationFailure("--evidence must be on the WSL F drive under /mnt/f")
        if evidence_resolved == SANDBOX_PARENT or SANDBOX_PARENT in evidence_resolved.parents:
            raise VerificationFailure("evidence must not be inside the sandbox parent")
        if evidence_path.exists() and evidence_path.is_symlink():
            raise VerificationFailure("--evidence must not be a symlink")

        self.run_uuid = str(uuid.uuid4())
        self.project_id = f"b2-network-project-{uuid.uuid4().hex}"
        self.run_root = SANDBOX_PARENT / f"{SANDBOX_PREFIX}{self.run_uuid}"
        self.content = self.run_root / "content"
        self.evidence = Evidence(
            evidence_resolved,
            run_root=self.run_root,
            project_id=self.project_id,
        )
        self.runner = RecordingDockerCLI(self.evidence)
        self.containers: list[str] = []
        self.specs: dict[str, RuntimeSpec] = {}
        self.manager: DownloadNetworks | None = None
        self.resolver: MutableGrantResolver | None = None
        self.cleanup_started = False

    def _host_event(self, label: str, command: list[str], *, ok: bool, **details: Any) -> None:
        self.evidence.event(label, command, ok=ok, details=details or None)

    def setup_sandbox(self) -> None:
        if os.geteuid() != 0:
            raise VerificationFailure("trusted root host invocation is required")
        self._host_event("root-preflight", ["id", "-u"], ok=True, uid=os.geteuid())
        if self.runner.prefix != ("/usr/bin/docker", "--host=unix:///var/run/docker.sock"):
            raise VerificationFailure("fixed DockerCLI Unix socket is not in use")
        self._host_event(
            "docker-cli-preflight",
            list(self.runner.prefix),
            ok=True,
            fixed_socket="unix:///var/run/docker.sock",
        )
        if not SANDBOX_PARENT.is_dir() or SANDBOX_PARENT.is_symlink():
            raise VerificationFailure("/srv/krim-sandbox must already be a real directory")
        if not _RUN_ROOT.fullmatch(self.run_root.name) or self.run_root.exists():
            raise VerificationFailure("run sandbox name collision or invalid name")

        # This is the only new path under /srv/krim-sandbox.  The parent is
        # provisioned by deployment and is never made by this probe.
        self.run_root.mkdir(mode=0o700)
        os.chown(self.run_root, 0, 0)
        self.content.mkdir(mode=0o700)
        os.chown(self.content, 65532, 65532)
        metadata = self.content.stat()
        mode = stat.S_IMODE(metadata.st_mode)
        if (
            metadata.st_uid != 65532
            or metadata.st_gid != 65532
            or not mode & stat.S_IWUSR
            or mode & 0o077
        ):
            raise VerificationFailure("content is not private and writable by 65532")
        self._host_event(
            "sandbox-mkdir",
            ["mkdir", str(self.content)],
            ok=True,
            sandbox_root=str(self.run_root),
            content=str(self.content),
            mode=oct(mode),
            uid=metadata.st_uid,
            gid=metadata.st_gid,
        )

    def _new_spec(self, environment_id: str) -> RuntimeSpec:
        metadata = self.content.stat()
        handle = EnvironmentHandle(
            owner_id=OWNER_ID,
            project_id=self.project_id,
            environment_id=environment_id,
            generation=1,
            state="creating",
        )
        record = EnvironmentRecord(
            environment=handle,
            image_id=IMAGE_ID,
            content_path=self.content,
            content_device=metadata.st_dev,
            content_inode=metadata.st_ino,
            uid=65532,
            gid=65532,
            network_id=None,
            network_policy_ref=POLICY_ID,
        )
        return RuntimeSpec(record)

    @staticmethod
    def _assert_create_args(spec: RuntimeSpec, args: tuple[str, ...]) -> None:
        required = {
            "--pull=never",
            "--runtime=runsc",
            "--user=65532:65532",
            "--cap-drop=ALL",
            "--security-opt=no-new-privileges",
            "--read-only",
            "--network=none",
        }
        if not required.issubset(args):
            raise VerificationFailure("create_args omitted a required sandbox restriction")
        if any(
            arg in {"-p", "--publish", "--publish-all", "--privileged", "--cap-add"}
            or arg.startswith("--publish=")
            or arg.startswith("--cap-add=")
            or "/var/run/docker.sock" in arg
            for arg in args
        ):
            raise VerificationFailure(
                "create_args contains a host port, socket mount, or capability")
        if sum(arg == "--mount" for arg in args) != 1:
            raise VerificationFailure("create_args did not select exactly one content mount")
        mount = args[args.index("--mount") + 1]
        if f"source={spec.mount.path}" not in mount or "target=/workspace" not in mount:
            raise VerificationFailure("create_args mounted a path other than this run's content")
        if spec.network.network_id is not None:
            raise VerificationFailure("network_id must remain None")

    async def create_and_start(self, label: str) -> tuple[str, RuntimeSpec]:
        spec = self._new_spec(f"environment-{label}-{uuid.uuid4().hex}")
        locking.verify_mount(spec)
        self.evidence.event(
            f"{label}-mount-check",
            ["python", "locking.verify_mount"],
            ok=True,
            path=str(spec.mount.path),
            uid=65532,
            gid=65532,
        )
        await docker.check_image(self.runner, spec)
        self.evidence.event(
            f"{label}-image-check",
            ["python", "docker.check_image", IMAGE_ID],
            ok=True,
            image=IMAGE_ID,
        )
        args = docker.create_args(spec)
        self._assert_create_args(spec, args)
        self.evidence.event(
            f"{label}-create-args-check",
            ["python", "docker.create_args"],
            ok=True,
            network="none",
            policy_ref=POLICY_ID,
            no_host_ports=True,
            no_socket_mount=True,
            cap_drop="ALL",
        )
        reply = await self.runner.expected(f"{label}-create", args, 0)
        if reply.code != 0:
            raise VerificationFailure(f"{label} docker create failed")
        container_id = reply.stdout.strip()
        if _HEX64.fullmatch(container_id) is None:
            raise VerificationFailure(f"{label} create did not return one exact 64-hex ID")
        # The ID is appended immediately after validation.  All later cleanup
        # is by this exact ID; no label-based enumeration is ever performed.
        self.containers.append(container_id)
        self.specs[container_id] = spec
        self.evidence.add_container(container_id)

        created = await docker.inspect(self.runner, spec)
        if created is None:
            raise VerificationFailure(f"{label} created ID could not be inspected")
        self._verify_inspect(f"{label}-created-verify", spec, created, expected="stopped")

        ready_record = replace(
            spec.record,
            environment=spec.handle.model_copy(update={"state": "ready"}),
            container_id=container_id,
        )
        ready_spec = RuntimeSpec(ready_record)
        self.specs[container_id] = ready_spec
        reply = await self.runner.expected(f"{label}-start", ("start", container_id), 0)
        if reply.code != 0:
            raise VerificationFailure(f"{label} docker start failed")
        running = await docker.inspect(self.runner, ready_spec)
        if running is None:
            raise VerificationFailure(f"{label} started ID could not be inspected")
        self._verify_inspect(f"{label}-running-verify", ready_spec, running, expected="ready")
        return container_id, ready_spec

    def _verify_inspect(
        self,
        label: str,
        spec: RuntimeSpec,
        value: dict[str, Any],
        *,
        expected: str | None,
    ) -> str:
        try:
            state = docker.verify(spec, value)
            labels = value["Config"]["Labels"]
            if labels != docker.labels(spec):
                raise VerificationFailure("container labels are not an exact production binding")
            if value["Image"] != IMAGE_ID or value["Config"]["Image"] != IMAGE_ID:
                raise VerificationFailure("container image identity drifted")
            if expected is not None and state != expected:
                raise VerificationFailure(f"container state was {state}, expected {expected}")
        except (EnvironmentError, VerificationFailure, KeyError, TypeError) as error:
            token = _error_token(error)
            self.evidence.event(
                label,
                ["python", "docker.verify"],
                ok=False,
                details={"error": token, "image": IMAGE_ID},
            )
            raise VerificationFailure(f"{label} failed: {token}") from None
        self.evidence.event(
            label,
            ["python", "docker.verify"],
            ok=True,
            details={"state": state, "labels_exact": True, "image": IMAGE_ID},
        )
        return state

    async def shell(
        self,
        label: str,
        container_id: str,
        script: str,
        *,
        expected: int | str = 0,
        require_proxy_403: bool = False,
    ) -> docker.Reply:
        args = (
            "exec",
            "--user=65532:65532",
            "--workdir=/workspace",
            container_id,
            "/bin/bash",
            "-lc",
            script,
        )
        reply = await self.runner.expected(label, args, expected)
        if expected == 0 and reply.code != 0:
            raise VerificationFailure(f"{label} returned a nonzero exit")
        if expected == "nonzero" and reply.code == 0:
            raise VerificationFailure(f"{label} unexpectedly succeeded")
        if require_proxy_403 and "403" not in (reply.stdout + reply.stderr):
            raise VerificationFailure(f"{label} did not receive the local proxy's 403")
        return reply

    async def manager_action(self, label: str, action: str, call: Any) -> None:
        try:
            await call
        except BaseException as error:
            token = _error_token(error)
            self.evidence.event(
                label,
                ["python", "DownloadNetworks", action],
                ok=False,
                details={"error": token},
            )
            raise VerificationFailure(f"{label} failed: {token}") from None
        self.evidence.event(
            label,
            ["python", "DownloadNetworks", action],
            ok=True,
            details={"container_session": "production DownloadNetworks"},
        )

    def set_grant(self, grant: NetworkGrant, label: str) -> None:
        assert self.resolver is not None
        self.resolver.set(grant)
        self.evidence.event(
            label,
            ["python", "MutableGrantResolver.set"],
            ok=True,
            details={
                "test_substitute": True,
                "revision": grant.revision,
                "revoked": grant.revoked,
                "expired": grant.expires_at <= time.time(),
                "hosts": list(grant.hosts),
                "ports": list(grant.ports),
            },
        )

    async def execute(self, *, command_only: bool = False) -> None:
        self.setup_sandbox()
        now = time.time()
        grant = NetworkGrant(
            owner_id=OWNER_ID,
            project_id=self.project_id,
            policy_id=POLICY_ID,
            revision=1,
            hosts=(REGISTRY_HOST,),
            ports=(443,),
            expires_at=now + 600,
            revoked=False,
        )
        self.resolver = MutableGrantResolver(grant)
        self.manager = DownloadNetworks(
            self.resolver,
            runner=self.runner,
            max_sessions=8,
            max_age=300,
        )
        self.evidence.event(
            "network-manager-init",
            ["python", "DownloadNetworks"],
            ok=True,
            details={
                "production_manager": True,
                "resolver_test_substitute": True,
                "max_age": 300,
                "network_id": None,
                "policy_ref": POLICY_ID,
            },
        )

        if command_only:
            await self.execution_bootstrap()
            return

        primary_id, primary_spec = await self.create_and_start("primary")
        await self.manager_action(
            "primary-arm",
            "arm",
            self.manager.arm(primary_spec, primary_id),
        )
        await self.manager_action(
            "primary-wait-ready",
            "wait_ready",
            self.manager.wait_ready(primary_id),
        )
        await self.shell(
            "primary-managed-shell",
            primary_id,
            "set -eu; test \"$(id -u)\" = 65532; "
            "test \"$(node --version)\" = v24.22.3; node --version; npm --version;"
            "command -v npm >/dev/null; command -v curl >/dev/null; "
            "command -v python3 >/dev/null; test ! -e /var/run/docker.sock; "
            "printf 'b2-write-ok\\n' > .b2-write-test; "
            "test \"$(cat .b2-write-test)\" = b2-write-ok; rm -f .b2-write-test",
        )
        await self.shell(
            "registry-head",
            primary_id,
            "set -eu; curl --fail --silent --show-error --head --max-time 30 "
            "https://registry.npmjs.org/esbuild",
        )
        await self.shell(
            "registry-get",
            primary_id,
            "set -eu; curl --fail --silent --show-error --max-time 30 "
            "--output /workspace/.b2-esbuild-metadata.json "
            "https://registry.npmjs.org/esbuild; "
            "test -s /workspace/.b2-esbuild-metadata.json; "
            "python3 -c 'import json; d=json.load(open(\"/workspace/.b2-esbuild-metadata.json\")); "
            "assert d.get(\"name\") == \"esbuild\"'",
        )
        await self.shell(
            "npm-install-esbuild",
            primary_id,
            "set -eu; "
            "npm_config_registry=https://registry.npmjs.org/ "
            "npm_config_ignore_scripts=false "
            "npm install --prefix /workspace esbuild@0.25.12 "
            "--no-audit --no-fund --foreground-scripts",
        )
        await self.shell(
            "node-require-esbuild",
            primary_id,
            "set -eu; node -e 'const esbuild=require(\"/workspace/node_modules/esbuild\"); "
            "if (esbuild.version !== \"0.25.12\") process.exit(2); "
            "const result=esbuild.transformSync(\"const answer: number = 42\", {loader: \"ts\"}); "
            "if (!result.code.includes(\"42\")) process.exit(3); "
            "console.log(JSON.stringify({version:esbuild.version, transform:true}));'",
        )
        await self.shell(
            "direct-public-tcp-blocked",
            primary_id,
            "python3 -c 'import socket,sys; s=socket.socket(); s.settimeout(3); "
            "\ntry: s.connect((\"8.8.8.8\",443))\n"
            "except OSError as e: print(\"blocked\",type(e).__name__)\n"
            "else: print(\"unexpected-connect\"); sys.exit(1)\n"
            "finally: s.close()'",
        )
        await self.shell(
            "direct-loopback-blocked",
            primary_id,
            "python3 -c 'import socket,sys; s=socket.socket(); s.settimeout(3); "
            "\ntry: s.connect((\"127.0.0.1\",443))\n"
            "except OSError as e: print(\"blocked\",type(e).__name__)\n"
            "else: print(\"unexpected-connect\"); sys.exit(1)\n"
            "finally: s.close()'",
        )
        await self.shell(
            "direct-docker-gateway-blocked",
            primary_id,
            "python3 -c 'import socket,sys; s=socket.socket(); s.settimeout(3); "
            "\ntry: s.connect((\"172.17.0.1\",443))\n"
            "except OSError as e: print(\"blocked\",type(e).__name__)\n"
            "else: print(\"unexpected-connect\"); sys.exit(1)\n"
            "finally: s.close()'",
        )
        await self.shell(
            "direct-public-dns-udp-blocked",
            primary_id,
            "python3 -c 'import socket,sys; s=socket.socket(socket.AF_INET,socket.SOCK_DGRAM); "
            "s.settimeout(3)\n"
            "try: s.sendto(b\"\\x00\"*12,(\"8.8.8.8\",53)); s.recvfrom(512)\n"
            "except OSError as e: print(\"blocked\",type(e).__name__)\n"
            "else: print(\"unexpected-dns-response\"); sys.exit(1)\n"
            "finally: s.close()'",
        )
        await self.shell(
            "no-proxy-direct-curl-blocked",
            primary_id,
            "env -i PATH=/usr/bin:/bin HOME=/nonexistent NO_PROXY='*' no_proxy='*' "
            "curl -k --fail --silent --show-error --connect-timeout 3 --max-time 5 "
            "https://registry.npmjs.org/esbuild",
            expected="nonzero",
        )
        await self.shell(
            "offlist-domain-local-deny",
            primary_id,
            "curl --proxy http://127.0.0.1:39119 -k --fail --silent --show-error "
            "--max-time 10 https://offlist.invalid/",
            expected="nonzero",
            require_proxy_403=True,
        )
        await self.shell(
            "metadata-url-local-deny",
            primary_id,
            "curl --proxy http://127.0.0.1:39119 --fail --silent --show-error "
            "--max-time 10 http://169.254.169.254/latest/meta-data/",
            expected="nonzero",
            require_proxy_403=True,
        )
        await self.shell(
            "authority-host-confusion-deny",
            primary_id,
            "curl --fail --silent --show-error --max-time 10 "
            "-H 'Host: offlist.invalid' https://registry.npmjs.org/esbuild",
            expected="nonzero",
            require_proxy_403=True,
        )
        await self.shell(
            "post-method-deny",
            primary_id,
            "curl --fail --silent --show-error --max-time 10 -X POST --data '' "
            "https://registry.npmjs.org/esbuild",
            expected="nonzero",
            require_proxy_403=True,
        )

        revoked = grant.model_copy(update={"revision": 2, "revoked": True})
        self.set_grant(revoked, "grant-revoked")
        await self.manager_action(
            "invalidate-after-revoke",
            "invalidate",
            self.manager.invalidate(OWNER_ID, self.project_id, policy_id=POLICY_ID),
        )
        await self.shell(
            "curl-after-revoke-denied",
            primary_id,
            "curl --fail --silent --show-error --max-time 5 "
            "https://registry.npmjs.org/esbuild",
            expected="nonzero",
        )

        active_for_expiry = grant.model_copy(
            update={"revision": 3, "expires_at": time.time() + 600, "revoked": False}
        )
        self.set_grant(active_for_expiry, "grant-restored-for-expiry-session")
        expiry_id, expiry_spec = await self.create_and_start("expiry")
        await self.manager_action(
            "expiry-arm",
            "arm",
            self.manager.arm(expiry_spec, expiry_id),
        )
        await self.manager_action(
            "expiry-wait-ready",
            "wait_ready",
            self.manager.wait_ready(expiry_id),
        )
        await self.shell(
            "expiry-session-before-change",
            expiry_id,
            "curl --fail --silent --show-error --max-time 30 "
            "https://registry.npmjs.org/esbuild",
        )
        expired = active_for_expiry.model_copy(
            update={"revision": 4, "expires_at": time.time() - 1, "revoked": False}
        )
        self.set_grant(expired, "grant-expired")
        await self.manager_action(
            "invalidate-after-expiry",
            "invalidate",
            self.manager.invalidate(OWNER_ID, self.project_id, policy_id=POLICY_ID),
        )
        await self.shell(
            "curl-after-expiry-denied",
            expiry_id,
            "curl --fail --silent --show-error --max-time 5 "
            "https://registry.npmjs.org/esbuild",
            expected="nonzero",
        )

        active_for_close = grant.model_copy(
            update={"revision": 5, "expires_at": time.time() + 600, "revoked": False}
        )
        self.set_grant(active_for_close, "grant-restored-for-close-session")
        close_id, close_spec = await self.create_and_start("close")
        await self.manager_action(
            "close-arm",
            "arm",
            self.manager.arm(close_spec, close_id),
        )
        await self.manager_action(
            "close-wait-ready",
            "wait_ready",
            self.manager.wait_ready(close_id),
        )
        await self.shell(
            "close-session-before-close",
            close_id,
            "curl --fail --silent --show-error --max-time 30 "
            "https://registry.npmjs.org/esbuild",
        )
        await self.execution_bootstrap()
        await self.manager_action("manager-close", "close", self.manager.close())
        await self.shell(
            "curl-after-manager-close-denied",
            close_id,
            "curl --fail --silent --show-error --max-time 5 "
            "https://registry.npmjs.org/esbuild",
            expected="nonzero",
        )

    async def execution_bootstrap(self) -> None:
        assert self.manager is not None
        original = self._new_spec(f"environment-execution-{uuid.uuid4().hex}")
        base = RuntimeSpec(replace(original.record, network_policy_ref=None))
        assert base.network.policy_ref is None
        assert docker.network_env(base) == {}
        self.evidence.event(
            "execution-offline-environment", ["python", "RuntimeSpec.network"], ok=True,
            details={"environment_policy_ref": None, "network_mode": "none",
                     "environment_proxy_env": {}},
        )
        request = CommandRequest(
            operation_id="operation-download", environment=base.handle,
            ownership=RunOwnership(owner_id=OWNER_ID, session_id="session-1", run_id="run-1"),
            authorization=AuthorizationRef(actor_id="actor-1", actor_generation=1,
                                           project_id=self.project_id,
                                           arguments_digest="b" * 64,
                                           network_policy_ref=POLICY_ID),
            command="curl --fail --silent --show-error --max-time 30 "
                    "--output /workspace/.execution-metadata.json "
                    "https://registry.npmjs.org/esbuild && "
                    "test -s /workspace/.execution-metadata.json && printf bootstrap-download-ok",
            timeout_seconds=60,
        )
        spec = RuntimeSpec(base.record, request, "process-download")
        assert spec.network.policy_ref == request.authorization.network_policy_ref == POLICY_ID
        locking.verify_mount(spec)
        await docker.check_image(self.runner, spec)
        self._assert_create_args(spec, docker.create_args(spec))
        self.evidence.event(
            "execution-command-policy", ["python", "RuntimeSpec.network"], ok=True,
            details={"environment_policy_ref": None,
                     "command_policy_ref": request.authorization.network_policy_ref,
                     "effective_policy_ref": spec.network.policy_ref,
                     "fingerprint": spec.fingerprint},
        )
        reply = await self.runner.expected("execution-create", docker.create_args(spec), 0)
        cid = reply.stdout.strip()
        if reply.code or not _HEX64.fullmatch(cid):
            raise VerificationFailure("execution create failed")
        self.containers.append(cid)
        self.specs[cid] = replace(spec, execution_id=cid)
        self.evidence.add_container(cid)
        await self.manager.arm(self.specs[cid], cid)
        reply = await self.runner.expected(
            "execution-start-attach", ("start", "--attach", "--interactive", cid), 0)
        if reply.code or "bootstrap-download-ok" not in reply.stdout:
            raise VerificationFailure("execution bootstrap download failed")
        await self.manager.release(cid)

    async def cleanup(self) -> None:
        if self.cleanup_started:
            return
        self.cleanup_started = True
        if self.manager is not None:
            try:
                await self.manager.close()
                self.evidence.event(
                    "cleanup-manager-close",
                    ["python", "DownloadNetworks.close"],
                    ok=True,
                )
            except BaseException as error:
                token = _error_token(error)
                self.evidence.add_cleanup_error(f"manager.close:{token}")
                self.evidence.event(
                    "cleanup-manager-close",
                    ["python", "DownloadNetworks.close"],
                    ok=False,
                    details={"error": token},
                )

        # Never enumerate Docker by a label.  Each ID here came from an exact
        # 64-hex docker create response in this process.
        for container_id in list(self.containers):
            spec = self.specs.get(container_id)
            if spec is None:
                self.evidence.add_cleanup_error(f"{container_id}:missing-spec")
                continue
            try:
                value = await docker.inspect(self.runner, spec)
                if value is None:
                    raise VerificationFailure("container ID disappeared before cleanup inspect")
                self._verify_inspect(
                    f"cleanup-{container_id[:12]}-inspect-verify",
                    spec,
                    value,
                    expected=None,
                )
                reply = await self.runner.expected(
                    f"cleanup-{container_id[:12]}-rm",
                    ("rm", "--force", container_id),
                    0,
                )
                if reply.code != 0:
                    raise VerificationFailure("docker rm returned nonzero")
                self.containers.remove(container_id)
                self.evidence.remove_container(container_id)
            except BaseException as error:
                token = _error_token(error)
                self.evidence.add_cleanup_error(f"{container_id}:{token}")
                self.evidence.event(
                    f"cleanup-{container_id[:12]}-failed",
                    ["python", "inspect-verify-then-rm"],
                    ok=False,
                    details={"error": token, "container_id": container_id},
                )

        if not self.containers and self.run_root.exists():
            try:
                if (
                    self.run_root.parent != SANDBOX_PARENT
                    or not _RUN_ROOT.fullmatch(self.run_root.name)
                    or self.run_root.is_symlink()
                ):
                    raise VerificationFailure("refusing to remove a path outside this run")
                shutil.rmtree(self.run_root)
                self.evidence.sandbox_removed = True
                self.evidence.event(
                    "sandbox-remove",
                    ["shutil.rmtree", str(self.run_root)],
                    ok=True,
                )
            except BaseException as error:
                token = _error_token(error)
                self.evidence.add_cleanup_error(f"sandbox:{token}")
                self.evidence.event(
                    "sandbox-remove",
                    ["shutil.rmtree", str(self.run_root)],
                    ok=False,
                    details={"error": token},
                )


async def _async_main(evidence_path: Path, *, command_only: bool = False) -> int:
    verifier = Verifier(evidence_path)
    failure: str | None = None
    try:
        await verifier.execute(command_only=command_only)
    except BaseException as error:
        failure = _error_token(error)
        verifier.evidence.set_status("failed", failure)
        print(json.dumps({"status": "failed", "error": failure}), file=sys.stderr, flush=True)
    finally:
        await verifier.cleanup()

    if failure is None and verifier.evidence.cleanup_errors:
        failure = "cleanup_failed"
    verifier.evidence.set_status("passed" if failure is None else "failed", failure)
    result = {
        "status": "passed" if failure is None else "failed",
        "failure": failure,
        "summary": str(verifier.evidence.path / "verification-results.json"),
        "sandbox_removed": verifier.evidence.sandbox_removed,
        "remaining_container_ids": list(verifier.containers),
    }
    print(json.dumps(result, ensure_ascii=False), flush=True)
    return 0 if failure is None else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--evidence",
        required=True,
        type=Path,
        help="required absolute WSL path on F: where redacted evidence is written",
    )
    parser.add_argument(
        "--command-only", action="store_true",
        help="exercise one explicit command policy with an offline environment record",
    )
    args = parser.parse_args()
    return asyncio.run(_async_main(args.evidence, command_only=args.command_only))


if __name__ == "__main__":
    raise SystemExit(main())
