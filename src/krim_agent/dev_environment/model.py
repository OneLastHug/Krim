"""Private Docker configuration derived from I's sole shared record definition."""
from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass, field, replace
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
    memory_bytes: int | None
    nano_cpus: int | None
    pids: int
    tmp_bytes: int
    home_bytes: int
    shm_bytes: int


_LEGACY_TMP_BYTES = 32 * 1024 * 1024
_LEGACY_HOME_BYTES = 64 * 1024 * 1024
_LEGACY_SHM_BYTES = 64 * 1024 * 1024
_UNLIMITED_LIMITS = _Limits(None, None, -1, 0, 0, 0)
_MISSING = object()


def _resource_value(record: EnvironmentRecord, name: str) -> object:
    """Read a previous resource value from new or legacy record shapes."""
    resources = getattr(record, "resources", None)
    if resources is not None:
        if isinstance(resources, dict):
            return resources.get(name, _MISSING)
        return getattr(resources, name, _MISSING)
    return getattr(record, name, _MISSING)


def _bounded_cpu(value: object) -> int:
    """Validate the previous finite CPU range for legacy-spec matching."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise EnvironmentError("environment_configuration_invalid")
    cpu = float(value)
    if not math.isfinite(cpu) or not 0.001 <= cpu <= 64:
        raise EnvironmentError("environment_configuration_invalid")
    return round(cpu * 1_000_000_000)


def _cpu_limit(value: object) -> int:
    """Convert an explicit positive finite CPU limit without a product ceiling."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise EnvironmentError("environment_configuration_invalid")
    cpu = float(value)
    if not math.isfinite(cpu) or cpu <= 0:
        raise EnvironmentError("environment_configuration_invalid")
    scaled = cpu * 1_000_000_000
    if not math.isfinite(scaled):
        raise EnvironmentError("environment_configuration_invalid")
    nano_cpus = round(scaled)
    if nano_cpus <= 0:
        raise EnvironmentError("environment_configuration_invalid")
    return nano_cpus


def _legacy_limits(record: EnvironmentRecord) -> _Limits:
    cpu = _resource_value(record, "cpu_cores")
    memory = _resource_value(record, "memory_bytes")
    pids = _resource_value(record, "pids_limit")
    nano_cpus = _bounded_cpu(cpu)
    if (not isinstance(memory, int) or isinstance(memory, bool)
            or memory < 64 * 1024 * 1024
            or not isinstance(pids, int) or isinstance(pids, bool)
            or not 8 <= pids <= 65536):
        raise EnvironmentError("environment_configuration_invalid")
    return _Limits(memory, nano_cpus, pids,
                   _LEGACY_TMP_BYTES, _LEGACY_HOME_BYTES, _LEGACY_SHM_BYTES)


def _limited_limits(record: EnvironmentRecord) -> _Limits:
    cpu = _resource_value(record, "cpu_cores")
    memory = _resource_value(record, "memory_bytes")
    pids = _resource_value(record, "pids_limit")
    nano_cpus = None if cpu is _MISSING or cpu is None else _cpu_limit(cpu)
    if memory is _MISSING or memory is None:
        memory_bytes = None
    elif (not isinstance(memory, int) or isinstance(memory, bool) or memory <= 0):
        raise EnvironmentError("environment_configuration_invalid")
    else:
        memory_bytes = memory
    if pids is _MISSING or pids is None:
        pids_limit = -1
    elif not isinstance(pids, int) or isinstance(pids, bool) or pids <= 0:
        raise EnvironmentError("environment_configuration_invalid")
    else:
        pids_limit = pids
    return _Limits(memory_bytes, nano_cpus, pids_limit, 0, 0, 0)


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
    _legacy_resources: bool = field(default=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        r = self.record
        path = r.content_path
        policy = self.resource_policy
        if (not _IMAGE.fullmatch(r.image_id) or (r.uid, r.gid) != (65532, 65532)
                or not path.is_absolute() or path == Path("/") or ".." in path.parts
                or any(c in str(path) for c in (",", "\x00", "\n", "\r"))
                or r.content_device < 0 or r.content_inode < 1
                or r.revision < 1 or not isinstance(policy, str)
                or policy not in {"unlimited", "limited", "legacy"}):
            raise EnvironmentError("environment_configuration_invalid")
        if policy == "legacy":
            _legacy_limits(r)
        elif policy == "limited":
            _limited_limits(r)
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
    def resource_policy(self) -> str:
        if self._legacy_resources:
            return "legacy"
        value = getattr(self.record, "resource_policy", _MISSING)
        # Records written before the explicit policy field are bounded records;
        # missing metadata must never be upgraded to unlimited by assumption.
        if value is _MISSING:
            return "legacy"
        return value if isinstance(value, str) else "invalid"

    @property
    def legacy_profile(self) -> bool:
        return self._legacy_resources or self.resource_policy == "legacy"

    @classmethod
    def legacy(cls, record: EnvironmentRecord, request: CommandRequest | None = None,
               process_id: str | None = None, execution_id: str | None = None) -> RuntimeSpec:
        """Reconstruct the previous bounded spec solely for safe migration checks."""
        return cls(record, request, process_id, execution_id, _legacy_resources=True)

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
        if self.legacy_profile:
            return _legacy_limits(self.record)
        if self.resource_policy == "limited":
            return _limited_limits(self.record)
        return _UNLIMITED_LIMITS

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
        if self.legacy_profile:
            value = {"identity": identity(r.environment), "image": r.image_id,
                     "content": (str(r.content_path), r.content_device, r.content_inode),
                     "cpu": self.limits.nano_cpus, "memory": self.limits.memory_bytes,
                     "pids": self.limits.pids,
                     "network": (self.network.network_id, self.network.policy_ref),
                     "profile": "krim-development-runsc-v2-uid65532"}
        else:
            value = {"identity": identity(r.environment), "image": r.image_id,
                     "content": (str(r.content_path), r.content_device, r.content_inode),
                     "cpu": self.limits.nano_cpus, "memory": self.limits.memory_bytes,
                     "pids": self.limits.pids, "resource_policy": self.resource_policy,
                     "network": (self.network.network_id, self.network.policy_ref),
                     "profile": "krim-development-runsc-v3-uid65532"}
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
