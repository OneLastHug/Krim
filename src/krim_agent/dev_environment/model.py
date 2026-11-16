"""Private Docker configuration derived from I's sole shared record definition."""
from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Literal

from krim_agent.development.contracts import CommandRequest, EnvironmentHandle
from krim_agent.development.ports import EnvironmentRecord, command_digest

Action = Literal["create", "start", "inspect", "stop", "recreate", "execute"]
_HEX = re.compile(r"^[0-9a-f]{64}$")
_IMAGE = re.compile(r"^sha256:[0-9a-f]{64}$")
_POLICY = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}")


class EnvironmentError(RuntimeError):
    """Bounded error code; raw daemon output stays on the trusted host."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


def identity(handle: EnvironmentHandle) -> tuple[str, str, str, int]:
    return (handle.owner_id, handle.project_id, handle.environment_id, handle.generation)


@dataclass(frozen=True)
class _Mount:
    path: Path
    device: int
    inode: int


@dataclass(frozen=True)
class _Limits:
    memory_bytes: int
    nano_cpus: int
    pids: int
    tmp_bytes: int = 32 * 1024 * 1024
    home_bytes: int = 64 * 1024 * 1024
    shm_bytes: int = 64 * 1024 * 1024


@dataclass(frozen=True)
class _Network:
    network_id: str | None
    policy_ref: str | None


@dataclass(frozen=True)
class RuntimeSpec:
    """Derived Docker spec, never another persistence record or shared protocol."""

    record: EnvironmentRecord
    request: CommandRequest | None = None
    process_id: str | None = None
    execution_id: str | None = None

    def __post_init__(self) -> None:
        r = self.record
        path = r.content_path
        if (not _IMAGE.fullmatch(r.image_id) or (r.uid, r.gid) != (65532, 65532)
                or not path.is_absolute() or path == Path("/") or ".." in path.parts
                or any(c in str(path) for c in (",", "\x00", "\n", "\r"))
                or r.content_device < 0 or r.content_inode < 1
                or not math.isfinite(r.cpu_cores) or not 0.001 <= r.cpu_cores <= 64
                or r.memory_bytes < 64 * 1024 * 1024 or not 8 <= r.pids_limit <= 65536
                or r.revision < 1 or type(r.max_processes) is not int
                or not 1 <= r.max_processes <= 32):
            raise EnvironmentError("environment_configuration_invalid")
        if r.container_id is not None and not _HEX.fullmatch(r.container_id):
            raise EnvironmentError("environment_configuration_invalid")
        # An arbitrary Docker bridge ID is not an egress enforcement facility.
        # Downloads use the host-owned stdio broker while Docker stays offline.
        if r.network_id is not None:
            raise EnvironmentError("uncontrolled_network_forbidden")
        if (self.request is None) != (self.process_id is None):
            raise EnvironmentError("execution_binding_invalid")
        if (self.request is not None
                and identity(self.request.environment) != identity(r.environment)):
            raise EnvironmentError("execution_binding_drift")
        if self.execution_id is not None and not _HEX.fullmatch(self.execution_id):
            raise EnvironmentError("execution_binding_invalid")
        for policy_ref in (r.network_policy_ref, self.network.policy_ref):
            if policy_ref is not None and (not isinstance(policy_ref, str)
                                           or _POLICY.fullmatch(policy_ref) is None):
                raise EnvironmentError("network_binding_invalid")
        if (self.request is not None and r.network_policy_ref is not None
                and r.network_policy_ref != self.network.policy_ref):
            raise EnvironmentError("network_binding_drift")

    @property
    def handle(self) -> EnvironmentHandle:
        return self.record.environment

    @property
    def image_id(self) -> str:
        return self.record.image_id

    @property
    def mount(self) -> _Mount:
        return _Mount(self.record.content_path, self.record.content_device,
                      self.record.content_inode)

    @property
    def limits(self) -> _Limits:
        return _Limits(self.record.memory_bytes, round(self.record.cpu_cores * 1_000_000_000),
                       self.record.pids_limit)

    @property
    def network(self) -> _Network:
        # An execution never inherits permission from its idle environment.
        policy_ref = (self.request.authorization.network_policy_ref
                      if self.request is not None else self.record.network_policy_ref)
        return _Network(self.record.network_id, policy_ref)

    @property
    def container_id(self) -> str | None:
        return self.execution_id if self.request is not None else self.record.container_id

    @property
    def fingerprint(self) -> str:
        r = self.record
        value = {"identity": identity(r.environment), "image": r.image_id,
                 "content": (str(r.content_path), r.content_device, r.content_inode),
                 "cpu": self.limits.nano_cpus, "memory": r.memory_bytes, "pids": r.pids_limit,
                 "network": (self.network.network_id, self.network.policy_ref),
                 "profile": "krim-development-runsc-v2-uid65532"}
        if self.request is not None:
            value["request"] = command_digest(self.request)
            value["process"] = self.process_id
        return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()

    @property
    def name(self) -> str:
        scope = (*identity(self.handle), self.process_id)
        digest = hashlib.sha256(json.dumps(scope, separators=(",", ":")).encode()).hexdigest()[:40]
        return ("krim-exec-" if self.request is not None else "krim-dev-") + digest

    def observed(self, state: str, container_id: str | None) -> RuntimeSpec:
        handle = EnvironmentHandle.model_validate({**self.handle.model_dump(), "state": state})
        return replace(self, record=replace(self.record, environment=handle,
                                            container_id=container_id))
