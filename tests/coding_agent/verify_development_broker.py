"""Opt-in acceptance of the real sandbox-to-development preparation path.

This script is intentionally not a pytest test.  It creates a fresh SQLite
account and project, starts the production StorageBroker as a child process,
then drives the production SandboxService and DevelopmentService directly.
It does not invoke a model, an API listener, or a project command.

The command must be run by the trusted Linux host operator.  The normal
invocation owns the Broker child for the duration of the check; the lifecycle
context is also the entry point a preview runner can call instead of starting
a daemon through a shell.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import shutil
import signal
import sys
import tempfile
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from krim_agent.accounts.service import IdentityService
from krim_agent.development.configuration import DevelopmentConfiguration
from krim_agent.development.service import DevelopmentService
from krim_agent.sandbox.broker_client import BrokerClient
from krim_agent.sandbox.configuration import SandboxConfiguration
from krim_agent.sandbox.quota import QuotaLimits, QuotaPolicy
from krim_agent.sandbox.service import SandboxService
from krim_agent.sandbox.xfs import XfsAccounts
from krim_agent.storage.sqlite import SQLiteStore
from tests.agent_core.test_account_api import initialize
from tests.coding_agent.verify_sqlite_terminal import RecordingDocker

APPROVED_STORAGE_ROOT = Path("/srv/krim-sandbox")
APPROVED_IMAGE = "sha256:82098beca2bd1c8887f82deb8d2fba13f87da04938463881a5e95b3d346834dc"
EXPECTED_BYTES = 32 * 1024**2
EXPECTED_INODES = 1000
CONTAINER_USER = "65532:65532"
ROOT_PREFIX = "verify-development-broker-"


@dataclass(frozen=True)
class Paths:
    root: Path
    root_device: int
    root_inode: int
    broker_state: Path
    private_state: Path
    socket: Path
    database: Path
    environments: Path


class QuotaMismatch(RuntimeError):
    """The host applied a quota different from the approved test budget."""

    def __init__(self, observed: dict[str, Any]) -> None:
        self.observed = observed
        super().__init__(
            "approved quota mismatch: "
            f"expected byte_limit={EXPECTED_BYTES}, inode_limit={EXPECTED_INODES}; "
            f"observed byte_limit={observed.get('byte_limit')}, "
            f"inode_limit={observed.get('inode_limit')}; "
            "larger or differently rounded limits are rejected"
        )


def save_json(directory: Path, name: str, value: object) -> None:
    target = directory / name
    target.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    target.chmod(0o600)


def require_platform(storage_root: Path) -> Path:
    if os.name != "posix" or not hasattr(os, "geteuid"):
        raise RuntimeError("this acceptance requires the approved Linux WSL host")
    if os.geteuid() != 0:
        raise RuntimeError("the real XFS Broker must be launched by the trusted root operator")
    try:
        resolved = storage_root.resolve(strict=True)
    except OSError as error:
        raise RuntimeError(f"storage root is unavailable: {storage_root}") from error
    if resolved != APPROVED_STORAGE_ROOT or not resolved.is_dir():
        raise RuntimeError(
            f"storage root must be exactly {APPROVED_STORAGE_ROOT}; refusing another path"
        )
    return resolved


def prepare_evidence(path: Path, storage_root: Path) -> Path:
    evidence = path.absolute()
    if not evidence.is_relative_to(storage_root):
        raise RuntimeError(f"evidence must remain below {storage_root}")
    if evidence.exists():
        if not evidence.is_dir() or any(evidence.iterdir()):
            raise RuntimeError("evidence directory must be a new or empty directory")
    else:
        if not evidence.parent.is_dir():
            raise RuntimeError("evidence parent must already exist")
        evidence.mkdir(mode=0o700)
    evidence.chmod(0o700)
    return evidence


def new_paths(storage_root: Path) -> Paths:
    root = Path(tempfile.mkdtemp(prefix=ROOT_PREFIX, dir=storage_root))
    root.chmod(0o700)
    info = root.lstat()
    broker_state = root / "broker-state"
    broker_state.mkdir(mode=0o700)
    environments = root / "environments"
    environments.mkdir(mode=0o700)
    return Paths(
        root=root,
        root_device=info.st_dev,
        root_inode=info.st_ino,
        broker_state=broker_state,
        private_state=root / "development-private",
        socket=root / "broker.sock",
        database=root / "application.sqlite",
        environments=environments,
    )


def approved_limits() -> QuotaLimits:
    # Checkpoints are disabled because the approved environment budget is only
    # 32 MiB; QuotaLimits itself rejects a checkpoint budget over half the total.
    return QuotaLimits(
        environment_bytes=EXPECTED_BYTES,
        environment_inodes=EXPECTED_INODES,
        checkpoint_bytes=0,
        checkpoint_count=0,
    )


class ObservedSandboxService(SandboxService):
    """Observe the production provisioning receipt without replacing it."""

    def __init__(self, *args: Any, receipt_sink: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.provision_receipt: dict[str, Any] | None = None
        self.receipt_sink = receipt_sink

    async def provision(
        self,
        actor: Any,
        *,
        request_key: str,
        project_id: str | None = None,
    ) -> dict[str, Any]:
        result = await super().provision(
            actor, request_key=request_key, project_id=project_id
        )
        receipt = {
            "environment_id": result["environment_id"],
            "project_id": project_id,
            "status": result["status"],
        }
        self.provision_receipt = receipt
        self.receipt_sink(receipt)
        return result


async def wait_for_broker(process: asyncio.subprocess.Process, client: BrokerClient) -> None:
    deadline = asyncio.get_running_loop().time() + 30
    last_error = "socket has not become ready"
    while asyncio.get_running_loop().time() < deadline:
        if process.returncode is not None:
            raise RuntimeError(f"Broker exited before readiness with code {process.returncode}")
        try:
            health = await client.call("health")
            if health.get("storage_available") is not True:
                raise RuntimeError(f"Broker health rejected storage: {health!r}")
            return
        except Exception as error:
            last_error = str(error)
            await asyncio.sleep(0.1)
    raise TimeoutError(f"Broker did not become ready: {last_error}")


@asynccontextmanager
async def broker_lifecycle(paths: Paths) -> AsyncIterator[BrokerClient]:
    """Own one real Broker process for a preview runner invocation.

    This uses direct process execution rather than a shell or systemd command.
    The child creates the Unix socket, runs the production StorageBroker and
    XfsAccounts, and is terminated only after the caller has finished cleanup.
    """

    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "krim_agent.sandbox.broker",
        "--socket",
        str(paths.socket),
        "--storage",
        str(paths.root),
        "--state",
        str(paths.broker_state),
        "--allow-uid",
        str(os.geteuid()),
        "--socket-gid",
        str(os.getegid()),
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    client = BrokerClient(paths.socket, owner_uid=0)
    try:
        await wait_for_broker(process, client)
        yield client
    finally:
        if process.returncode is None:
            process.send_signal(signal.SIGTERM)
            try:
                await asyncio.wait_for(process.wait(), 10)
            except TimeoutError:
                process.kill()
                await process.wait()


def quota_observation(paths: Paths, environment_id: str) -> dict[str, Any]:
    accounts = XfsAccounts(paths.root, paths.environments)
    number = accounts.project_number(environment_id)
    usage = accounts.usage(number)
    return {"project_number": number, **usage}


def quota_preflight(paths: Paths, environment_id: str) -> dict[str, Any]:
    accounts = XfsAccounts(paths.root, paths.environments)
    number = accounts.project_number(environment_id)
    report = accounts.command(f"report -p -b -i -n -N -L {number} -U {number}")
    if report.strip():
        raise RuntimeError("project number already has a quota record")
    if (paths.environments / environment_id).exists():
        raise RuntimeError("new environment directory already exists")
    return {
        "project_number": number, "unallocated": True,
        "byte_limit": 0, "inode_limit": 0,
        "environment_bytes": 0, "environment_inodes": 0,
    }


def require_empty_quota(usage: dict[str, Any]) -> None:
    fields = ("byte_limit", "inode_limit", "environment_bytes", "environment_inodes")
    if any(usage.get(key) != 0 for key in fields):
        raise RuntimeError(
            "new environment project number is not empty; refusing provisioning: "
            f"byte_limit={usage.get('byte_limit')}, inode_limit={usage.get('inode_limit')}, "
            f"used_bytes={usage.get('environment_bytes')}, "
            f"used_inodes={usage.get('environment_inodes')}"
        )


def clear_quota(paths: Paths, environment_id: str) -> dict[str, Any]:
    """Clear a project quota only after this run recorded its successful creation."""

    accounts = XfsAccounts(paths.root, paths.environments)
    number = accounts.project_number(environment_id)
    accounts.command(f"limit -p bhard=0 ihard=0 {number}")
    cleared = accounts.usage(number)
    if cleared["byte_limit"] != 0 or cleared["inode_limit"] != 0:
        raise RuntimeError(f"temporary quota was not cleared: {cleared!r}")
    return {"project_number": number, "cleared": True, **cleared}


def container_binding_errors(
    value: dict[str, Any],
    *,
    container_id: str,
    image: str,
    owner_id: str,
    project_id: str,
    environment_id: str,
    content: Path,
) -> list[str]:
    config = value.get("Config")
    labels = config.get("Labels", {}) if isinstance(config, dict) else {}
    mounts = value.get("Mounts", [])
    workspace_mounts = [
        mount
        for mount in mounts
        if isinstance(mount, dict) and mount.get("Destination") == "/workspace"
    ]
    errors: list[str] = []
    if value.get("Id") != container_id:
        errors.append("container id changed")
    if (
        value.get("Image") != image
        or not isinstance(config, dict)
        or config.get("Image") != image
    ):
        errors.append("image binding changed")
    if not isinstance(config, dict) or config.get("User") != CONTAINER_USER:
        errors.append(f"container user is not {CONTAINER_USER}")
    if not isinstance(labels, dict) or labels.get("io.krim.development.owner") != owner_id:
        errors.append("owner label changed")
    if not isinstance(labels, dict) or labels.get("io.krim.development.project") != project_id:
        errors.append("project label changed")
    if (
        not isinstance(labels, dict)
        or labels.get("io.krim.development.environment") != environment_id
    ):
        errors.append("environment label changed")
    if len(workspace_mounts) != 1 or workspace_mounts[0].get("Source") != str(content):
        errors.append("project mount changed")
    return errors


async def inspect_container(docker: RecordingDocker, container_id: str) -> dict[str, Any]:
    reply = await docker.run(("inspect", container_id))
    if reply.code != 0:
        raise RuntimeError(f"container inspect failed for {container_id}: {reply.stderr}")
    try:
        values = json.loads(reply.stdout)
        value = values[0]
        if not isinstance(value, dict):
            raise ValueError
        return value
    except (IndexError, TypeError, ValueError, json.JSONDecodeError) as error:
        raise RuntimeError(f"invalid inspect response for {container_id}") from error


async def cleanup_containers(
    docker: RecordingDocker,
    *,
    image: str,
    owner_id: str | None,
    project_id: str | None,
    environment_id: str | None,
    content: Path | None,
) -> tuple[list[dict[str, Any]], list[str]]:
    outcomes: list[dict[str, Any]] = []
    blockers: list[str] = []
    for container_id in docker.created:
        if not re.fullmatch(r"[0-9a-f]{64}", container_id):
            blockers.append(f"refusing malformed tracked container id {container_id!r}")
            continue
        reply = await docker.run(("inspect", container_id))
        if reply.code != 0:
            missing = reply.stderr.casefold()
            if "no such container:" in missing or "no such object:" in missing:
                outcomes.append({"id": container_id, "already_absent": True})
                continue
            blockers.append(f"inspect failed for tracked container {container_id}: {reply.stderr}")
            continue
        try:
            values = json.loads(reply.stdout)
            value = values[0]
            if not isinstance(value, dict):
                raise ValueError
        except (IndexError, TypeError, ValueError, json.JSONDecodeError):
            blockers.append(f"invalid inspect response for tracked container {container_id}")
            continue
        if None in (owner_id, project_id, environment_id, content):
            blockers.append(f"missing binding for tracked container {container_id}")
            continue
        errors = container_binding_errors(
            value,
            container_id=container_id,
            image=image,
            owner_id=owner_id,
            project_id=project_id,
            environment_id=environment_id,
            content=content,
        )
        if errors:
            blockers.append(f"refusing cleanup of {container_id}: {', '.join(errors)}")
            continue
        removed = await docker.run(("rm", "--force", container_id))
        outcomes.append({"id": container_id, "exit_code": removed.code})
        if removed.code != 0:
            blockers.append(
                f"cleanup failed for tracked container {container_id}: {removed.stderr}"
            )
    return outcomes, blockers


def remove_test_root(paths: Paths, storage_root: Path) -> None:
    if (
        paths.root.parent != storage_root
        or not paths.root.name.startswith(ROOT_PREFIX)
        or paths.root.is_symlink()
    ):
        raise RuntimeError(f"refusing to remove an unrecognized test root: {paths.root}")
    info = paths.root.lstat()
    if (info.st_dev, info.st_ino) != (paths.root_device, paths.root_inode):
        raise RuntimeError(f"test root identity changed: {paths.root}")
    shutil.rmtree(paths.root)


async def run(
    evidence: Path,
    *,
    storage_root: Path = APPROVED_STORAGE_ROOT,
    start_broker: bool = False,
) -> dict[str, Any]:
    """Run one isolated acceptance; ``start_broker`` is an explicit opt-in gate."""

    if not start_broker:
        raise RuntimeError("pass --start-broker to opt into starting the real Broker child")
    storage_root = require_platform(storage_root)
    evidence = prepare_evidence(evidence, storage_root)
    paths = new_paths(storage_root)
    result: dict[str, Any] = {
        "approved_image": APPROVED_IMAGE,
        "approved_quota": {
            "environment_bytes": EXPECTED_BYTES,
            "environment_inodes": EXPECTED_INODES,
        },
        "storage_root": str(storage_root),
        "temporary_root": str(paths.root),
        "temporary_root_identity": {
            "device": paths.root_device,
            "inode": paths.root_inode,
        },
        "broker_lifecycle": "owned-child-process",
        "model_calls": 0,
        "commands_submitted": 0,
    }
    primary_error: Exception | None = None
    cleanup_warnings: list[str] = []
    cleanup_blockers: list[str] = []
    quota_environment_id: str | None = None
    quota_created = False
    expected_development_environment_id: str | None = None
    container_outcomes: list[dict[str, Any]] = []
    docker: RecordingDocker | None = None
    development: DevelopmentService | None = None
    sandbox: SandboxService | None = None
    identities: IdentityService | None = None
    store: SQLiteStore | None = None
    handle = None
    project: dict[str, Any] | None = None
    project_id: str | None = None
    actor_id: str | None = None
    content: Path | None = None

    def record_provision(receipt: dict[str, Any]) -> None:
        nonlocal quota_created
        result["provision_receipt"] = receipt
        save_json(evidence, "provision-receipt.json", receipt)
        quota_created = True

    try:
        async with broker_lifecycle(paths) as broker:
            try:
                token = await initialize(paths.database)
                store = await SQLiteStore.open(paths.database)
                identities = await IdentityService.open(store)
                actor = identities.authenticate("Bearer " + str(token))
                actor_id = actor.principal_id
                sandbox_configuration = SandboxConfiguration(
                    broker_socket=paths.socket,
                    broker_owner_uid=0,
                    quotas=QuotaPolicy(defaults=approved_limits()),
                )
                sandbox = ObservedSandboxService(
                    store,
                    identities,
                    sandbox_configuration,
                    receipt_sink=record_provision,
                )
                project = await sandbox.create_project(
                    actor,
                    {
                        "workspace_id": actor.memberships[0].workspace_id,
                        "title": "broker verification",
                    },
                    request_key="verify-broker-project",
                )
                project_id = project["project_id"]
                quota_environment_id = project["environment_id"]
                expected_development_environment_id = "dev-" + project_id
                content = (
                    paths.environments
                    / quota_environment_id
                    / "projects"
                    / project_id
                    / "content"
                )
                preflight = quota_preflight(paths, quota_environment_id)
                require_empty_quota(preflight)
                result["quota_preflight"] = preflight
                save_json(evidence, "quota-preflight.json", preflight)
                result.update(
                    {
                        "principal_id": actor.principal_id,
                        "project_id": project_id,
                        "sandbox_environment_id": quota_environment_id,
                        "development_environment_id": expected_development_environment_id,
                        "project_content": str(content),
                    }
                )
                development_configuration = DevelopmentConfiguration(
                    image_id=APPROVED_IMAGE,
                    broker_environments_root=paths.environments,
                    private_state_root=paths.private_state,
                    memory_bytes=64 * 1024**2,
                    pids_limit=32,
                )
                DevelopmentService.prepare_private(development_configuration)
                docker = RecordingDocker(evidence / "created-container-ids.json")
                development = DevelopmentService(
                    store, identities, sandbox, development_configuration
                )
                development.environments.runner = docker
                handle = await development.prepare_project(
                    actor, project_id, request_key="verify-broker-prepare"
                )
                if not quota_created:
                    raise RuntimeError("prepare_project returned without a provisioning receipt")
                if handle.state != "ready":
                    raise RuntimeError(f"prepared environment is not ready: {handle.state}")
                if handle.environment_id != expected_development_environment_id:
                    raise RuntimeError(
                        "development environment binding changed: "
                        f"expected={expected_development_environment_id}, "
                        f"observed={handle.environment_id}"
                    )
                ready_project = await sandbox.project(actor, project_id)
                environment_view = await sandbox.environment(actor)
                if environment_view["status"] != "ready":
                    raise RuntimeError(f"sandbox environment is not ready: {environment_view}")
                quota = await broker.call("usage", environment_id=quota_environment_id)
                observed = quota["usage"]
                if (
                    observed.get("byte_limit") != EXPECTED_BYTES
                    or observed.get("inode_limit") != EXPECTED_INODES
                ):
                    raise QuotaMismatch(observed)
                content_info = content.lstat()
                if (content_info.st_uid, content_info.st_gid) != (65532, 65532):
                    raise RuntimeError(
                        f"project content owner changed: uid={content_info.st_uid}, "
                        f"gid={content_info.st_gid}"
                    )
                if docker is None or len(docker.created) != 1:
                    raise RuntimeError(
                        f"expected one new container, got {docker and docker.created}"
                    )
                value = await inspect_container(docker, docker.created[0])
                errors = container_binding_errors(
                    value,
                    container_id=docker.created[0],
                    image=APPROVED_IMAGE,
                    owner_id=actor.principal_id,
                    project_id=project_id,
                    environment_id=handle.environment_id,
                    content=content,
                )
                if errors:
                    raise RuntimeError(
                        "container binding verification failed: " + "; ".join(errors)
                    )
                result.update(
                    {
                        "status": "prepared",
                        "principal_id": actor.principal_id,
                        "project_id": project_id,
                        "sandbox_environment_id": quota_environment_id,
                        "development_environment_id": handle.environment_id,
                        "container_id": docker.created[0],
                        "container_user": CONTAINER_USER,
                        "project_content": str(content),
                        "observed_quota": observed,
                        "project_status": ready_project["status"],
                        "environment_view": environment_view,
                    }
                )
            except Exception as error:
                primary_error = error
            finally:
                if development is not None and handle is not None:
                    try:
                        stopped = await development.environments.stop(handle)
                        result["environment_stop"] = stopped.state
                    except Exception as error:
                        cleanup_warnings.append(f"development stop failed: {error}")
                if docker is not None:
                    try:
                        outcomes, blockers = await cleanup_containers(
                            docker,
                            image=APPROVED_IMAGE,
                            owner_id=actor_id,
                            project_id=project_id,
                            environment_id=expected_development_environment_id,
                            content=content,
                        )
                        container_outcomes.extend(outcomes)
                        cleanup_blockers.extend(blockers)
                    except Exception as error:
                        cleanup_blockers.append(f"container cleanup failed: {error}")
                if development is not None:
                    try:
                        await development.close()
                    except Exception as error:
                        cleanup_warnings.append(f"development close failed: {error}")
                if sandbox is not None:
                    try:
                        await sandbox.close()
                    except Exception as error:
                        cleanup_warnings.append(f"sandbox close failed: {error}")
                if identities is not None:
                    try:
                        await identities.close()
                    except Exception as error:
                        cleanup_warnings.append(f"identity close failed: {error}")
                if store is not None:
                    try:
                        await store.close()
                    except Exception as error:
                        cleanup_warnings.append(f"SQLite close failed: {error}")
    except Exception as error:
        if primary_error is None:
            primary_error = error

    if quota_environment_id is not None and not quota_created:
        cleanup_blockers.append(
            "quota creation was not confirmed; refusing to clear this project number"
        )
    elif quota_environment_id is not None and paths.root.exists():
        try:
            before_cleanup = quota_observation(paths, quota_environment_id)
            result["quota_before_cleanup"] = before_cleanup
        except Exception as error:
            cleanup_blockers.append(f"quota observation failed: {error}")
        else:
            if (
                before_cleanup["byte_limit"] != EXPECTED_BYTES
                or before_cleanup["inode_limit"] != EXPECTED_INODES
            ):
                mismatch = QuotaMismatch(before_cleanup)
                result["quota_mismatch"] = str(mismatch)
                if primary_error is None:
                    primary_error = mismatch
            try:
                result["quota_cleanup"] = clear_quota(paths, quota_environment_id)
            except Exception as error:
                cleanup_blockers.append(f"quota cleanup failed: {error}")

    result["container_cleanup"] = container_outcomes
    if not cleanup_blockers and paths.root.exists():
        try:
            remove_test_root(paths, storage_root)
            result["temporary_root_removed"] = True
        except Exception as error:
            cleanup_blockers.append(f"temporary root cleanup failed: {error}")
            result["temporary_root_removed"] = False
            result["retained_temporary_root"] = str(paths.root)
    else:
        result["temporary_root_removed"] = False
        result["retained_temporary_root"] = str(paths.root)

    if cleanup_warnings:
        result["cleanup_warnings"] = cleanup_warnings
    if cleanup_blockers:
        result["cleanup_blockers"] = cleanup_blockers
    if primary_error is not None:
        result["error"] = str(primary_error)
        result["error_type"] = type(primary_error).__name__

    if cleanup_blockers and primary_error is None:
        primary_error = RuntimeError("cleanup did not complete: " + "; ".join(cleanup_blockers))
        result["error"] = str(primary_error)
        result["error_type"] = type(primary_error).__name__
    result["status"] = "passed" if primary_error is None else "failed"
    save_json(evidence, "results.json", result)
    if primary_error is not None:
        raise primary_error
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--storage-root", type=Path, default=APPROVED_STORAGE_ROOT)
    parser.add_argument(
        "--start-broker",
        action="store_true",
        help="explicitly allow the preview-runner-owned real Broker child",
    )
    args = parser.parse_args()
    asyncio.run(run(args.evidence, storage_root=args.storage_root, start_broker=args.start_broker))


if __name__ == "__main__":
    main()
