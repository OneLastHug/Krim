"""Host-side dependency artifact verification for offline sandbox installs.

This module deliberately does not execute pip/npm and never enables sandbox
networking. A trusted control plane may fetch a locked artifact bundle here;
the dependency_install runtime remains unavailable until its image/profile is
built and a caller mounts the verified bundle into a network=none sandbox.
"""

from __future__ import annotations

import hashlib
import re
import ssl
from typing import Any, Literal
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import (
    HTTPRedirectHandler,
    HTTPSHandler,
    OpenerDirector,
    ProxyHandler,
    Request,
    build_opener,
)

from pydantic import Field, field_validator, model_validator

from krim_agent.contracts import ServiceError, StrictModel

HOST = re.compile(r"[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?\Z")
MAX_ARTIFACTS = 64
DEFAULT_MAX_BYTES = 512 * 1024 * 1024


class DependencyPolicy(StrictModel):
    """Trusted host policy; never constructed from model-facing command data."""

    allowed_hosts: tuple[str, ...] = Field(min_length=1, max_length=16)
    timeout_seconds: int = Field(default=30, ge=1, le=120)
    max_artifact_bytes: int = Field(default=DEFAULT_MAX_BYTES, ge=1024, le=DEFAULT_MAX_BYTES)
    max_bundle_bytes: int = Field(default=DEFAULT_MAX_BYTES, ge=1024, le=DEFAULT_MAX_BYTES)

    @field_validator("allowed_hosts", mode="before")
    @classmethod
    def freeze_hosts(cls, value: object) -> object:
        if type(value) is list:
            return tuple(value)
        return value

    @field_validator("allowed_hosts")
    @classmethod
    def normalize_hosts(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        result = tuple(host.lower() for host in value)
        if len(set(result)) != len(result) or any(not HOST.fullmatch(host) for host in result):
            raise ValueError("artifact host allowlist无效")
        return result


class LockedArtifact(StrictModel):
    """One immutable HTTPS artifact referenced by a dependency lock."""

    url: str
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    filename: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9._-]+$")
    size_bytes: int = Field(ge=1, le=DEFAULT_MAX_BYTES)

    @field_validator("filename")
    @classmethod
    def safe_filename(cls, value: str) -> str:
        if value in {".", ".."}:
            raise ValueError("artifact文件名无效")
        return value

    @field_validator("url")
    @classmethod
    def validate_https_url(cls, value: str) -> str:
        parsed = urlsplit(value)
        try:
            port = parsed.port
        except ValueError:
            raise ValueError("artifact URL端口无效") from None
        if (
            parsed.scheme != "https"
            or parsed.username is not None
            or parsed.password is not None
            or parsed.hostname is None
            or port not in (None, 443)
            or not parsed.path
            or parsed.fragment
        ):
            raise ValueError("artifact必须为无凭据的HTTPS URL")
        if not HOST.fullmatch(parsed.hostname.lower()):
            raise ValueError("artifact URL主机名无效")
        return value


class DependencyLock(StrictModel):
    """Lockfile digest plus the exact artifacts allowed for one program."""

    program: Literal["python", "node"]
    lockfile_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    artifacts: tuple[LockedArtifact, ...] = Field(min_length=1, max_length=MAX_ARTIFACTS)

    @field_validator("artifacts", mode="before")
    @classmethod
    def freeze_artifacts(cls, value: object) -> object:
        if type(value) is list:
            return tuple(value)
        return value

    @model_validator(mode="after")
    def unique_artifacts(self) -> DependencyLock:
        if len({artifact.url for artifact in self.artifacts}) != len(self.artifacts):
            raise ValueError("dependency artifact URL重复")
        if len({artifact.filename for artifact in self.artifacts}) != len(self.artifacts):
            raise ValueError("dependency artifact文件名重复")
        return self


def _check_allowed(url: str, policy: DependencyPolicy) -> None:
    parsed = urlsplit(url)
    try:
        port = parsed.port
    except ValueError:
        raise ServiceError("invalid_input", 422) from None
    if (
        parsed.scheme != "https"
        or parsed.hostname is None
        or parsed.hostname.lower() not in policy.allowed_hosts
        or port not in (None, 443)
        or parsed.username is not None
        or parsed.password is not None
    ):
        raise ServiceError("permission_denied", 403)


class _AllowlistedRedirect(HTTPRedirectHandler):
    def __init__(self, policy: DependencyPolicy) -> None:
        super().__init__()
        self.policy = policy

    def redirect_request(
        self,
        req: Request,
        fp: Any,
        code: int,
        msg: str,
        headers: Any,
        newurl: str,
    ) -> Request | None:
        _check_allowed(newurl, self.policy)
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def _opener(policy: DependencyPolicy) -> OpenerDirector:
    return build_opener(
        ProxyHandler({}),
        HTTPSHandler(context=ssl.create_default_context()),
        _AllowlistedRedirect(policy),
    )


def fetch_artifact(artifact: LockedArtifact, policy: DependencyPolicy) -> bytes:
    """Fetch one allowlisted artifact and verify size plus SHA-256 before return."""

    if artifact.size_bytes > policy.max_artifact_bytes:
        raise ServiceError("artifact_limit")
    _check_allowed(artifact.url, policy)
    request = Request(
        artifact.url,
        headers={"Accept": "application/octet-stream", "Cache-Control": "no-cache"},
        method="GET",
    )
    opener = _opener(policy)
    try:
        response = opener.open(request, timeout=policy.timeout_seconds)
        with response:
            length = response.headers.get("Content-Length")
            if length is not None:
                if type(length) is not str or re.fullmatch(r"[0-9]+", length) is None:
                    raise ServiceError("artifact_conflict")
                normalized_length = length.lstrip("0") or "0"
                max_length = str(policy.max_artifact_bytes)
                if len(normalized_length) > len(max_length) or (
                    len(normalized_length) == len(max_length) and normalized_length > max_length
                ):
                    raise ServiceError("artifact_limit")
                declared_length = int(normalized_length)
                if declared_length != artifact.size_bytes:
                    raise ServiceError("artifact_conflict")
            digest = hashlib.sha256()
            body = bytearray()
            while True:
                chunk = response.read(min(1024 * 1024, artifact.size_bytes - len(body) + 1))
                if not chunk:
                    break
                body.extend(chunk)
                digest.update(chunk)
                if len(body) > policy.max_artifact_bytes or len(body) > artifact.size_bytes:
                    raise ServiceError("artifact_limit")
            if len(body) != artifact.size_bytes or digest.hexdigest() != artifact.sha256:
                raise ServiceError("artifact_conflict")
            return bytes(body)
    except ServiceError:
        raise
    except (HTTPError, URLError, OSError, ValueError, TimeoutError):
        raise ServiceError("sandbox_backend_unavailable", 503) from None


def fetch_locked_bundle(
    lock: DependencyLock, lockfile: bytes, policy: DependencyPolicy
) -> dict[str, bytes]:
    """Verify lockfile and return filename-keyed artifacts for offline staging."""

    if hashlib.sha256(lockfile).hexdigest() != lock.lockfile_sha256:
        raise ServiceError("artifact_conflict")
    if any(artifact.size_bytes > policy.max_artifact_bytes for artifact in lock.artifacts):
        raise ServiceError("artifact_limit")
    if sum(artifact.size_bytes for artifact in lock.artifacts) > policy.max_bundle_bytes:
        raise ServiceError("artifact_limit")
    return {
        artifact.filename: fetch_artifact(artifact, policy)
        for artifact in lock.artifacts
    }
