"""Host dependency fetch stays allowlisted, digest-locked, and offline-ready."""

import hashlib
import io

import pytest
from pydantic import ValidationError

from krim_agent.contracts import ServiceError
from krim_agent.sandbox import dependencies as dependency_module
from krim_agent.sandbox.dependencies import (
    DependencyLock,
    DependencyPolicy,
    LockedArtifact,
    fetch_locked_bundle,
)


class Response:
    def __init__(self, body: bytes, *, content_length: str | None = None):
        self.body = io.BytesIO(body)
        self.headers = {} if content_length is None else {"Content-Length": content_length}

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def read(self, size=-1):
        return self.body.read(size)


def test_locked_https_bundle_checks_allowlist_lock_and_digest(monkeypatch):
    body = b"wheel-content"
    lockfile = b"python-lock-v1"
    artifact = LockedArtifact(
        url="https://packages.example.test/releases/pkg.whl",
        sha256=hashlib.sha256(body).hexdigest(),
        filename="pkg.whl",
        size_bytes=len(body),
    )
    lock = DependencyLock(
        program="python",
        lockfile_sha256=hashlib.sha256(lockfile).hexdigest(),
        artifacts=[artifact],
    )

    def open_url(request, _timeout):
        assert request.full_url == artifact.url
        return Response(body, content_length=str(len(body)))

    monkeypatch.setattr(
        dependency_module,
        "_opener",
        lambda policy: type(
            "Opener",
            (),
            {"open": lambda _self, _request, timeout: open_url(_request, timeout)},
        )(),
    )
    result = fetch_locked_bundle(
        lock, lockfile, DependencyPolicy(allowed_hosts=["packages.example.test"])
    )
    assert result == {"pkg.whl": body}

    with pytest.raises(ServiceError, match="artifact_conflict"):
        fetch_locked_bundle(
            lock, b"changed", DependencyPolicy(allowed_hosts=["packages.example.test"])
        )


@pytest.mark.parametrize(
    "length,code",
    [
        ("not-a-number", "artifact_conflict"),
        ("-1", "artifact_conflict"),
        ("+1", "artifact_conflict"),
        (" 1", "artifact_conflict"),
        ("9" * 5000, "artifact_limit"),
    ],
)
def test_content_length_errors_are_deterministic(monkeypatch, length, code):
    body = b"x"
    artifact = LockedArtifact(
        url="https://packages.example.test/pkg.whl",
        sha256=hashlib.sha256(body).hexdigest(),
        filename="pkg.whl",
        size_bytes=1,
    )
    response = Response(body, content_length=length)
    monkeypatch.setattr(
        dependency_module,
        "_opener",
        lambda policy: type(
            "Opener", (), {"open": lambda _self, _request, timeout: response}
        )(),
    )
    from krim_agent.sandbox.dependencies import fetch_artifact

    with pytest.raises(ServiceError, match=code):
        fetch_artifact(artifact, DependencyPolicy(allowed_hosts=["packages.example.test"]))


def test_oversized_content_length_is_an_artifact_limit(monkeypatch):
    body = b"x"
    artifact = LockedArtifact(
        url="https://packages.example.test/pkg.whl",
        sha256=hashlib.sha256(body).hexdigest(),
        filename="pkg.whl",
        size_bytes=1,
    )
    response = Response(body, content_length=str(2048))
    monkeypatch.setattr(
        dependency_module,
        "_opener",
        lambda policy: type(
            "Opener", (), {"open": lambda _self, _request, timeout: response}
        )(),
    )
    from krim_agent.sandbox.dependencies import fetch_artifact

    with pytest.raises(ServiceError, match="artifact_limit"):
        fetch_artifact(
            artifact,
            DependencyPolicy(
                allowed_hosts=["packages.example.test"], max_artifact_bytes=1024
            ),
        )


def test_dependency_fetch_refuses_non_https_or_non_allowlisted_urls():
    with pytest.raises(ValidationError):
        LockedArtifact(
            url="http://packages.example.test/pkg.whl",
            sha256="a" * 64,
            filename="pkg.whl",
            size_bytes=1,
        )
    artifact = LockedArtifact(
        url="https://other.example.test/pkg.whl",
        sha256="a" * 64,
        filename="pkg.whl",
        size_bytes=1,
    )
    from krim_agent.sandbox.dependencies import fetch_artifact

    with pytest.raises(ServiceError, match="permission_denied"):
        fetch_artifact(artifact, DependencyPolicy(allowed_hosts=["packages.example.test"]))


def test_lock_rejects_artifact_larger_than_policy_before_network(monkeypatch):
    body = b"x"
    artifact = LockedArtifact(
        url="https://packages.example.test/pkg.whl",
        sha256=hashlib.sha256(body).hexdigest(),
        filename="pkg.whl",
        size_bytes=2048,
    )
    lock = DependencyLock(
        program="python",
        lockfile_sha256=hashlib.sha256(b"lock").hexdigest(),
        artifacts=[artifact],
    )
    called = False

    def fail_opener(_policy):
        nonlocal called
        called = True
        raise AssertionError("network must not be opened")

    monkeypatch.setattr(dependency_module, "_opener", fail_opener)
    with pytest.raises(ServiceError, match="artifact_limit"):
        fetch_locked_bundle(
            lock,
            b"lock",
            DependencyPolicy(allowed_hosts=["packages.example.test"], max_artifact_bytes=1),
        )
    assert called is False


def test_dependency_artifact_bounds_and_duplicate_names_are_rejected():
    with pytest.raises(ValidationError):
        DependencyPolicy(allowed_hosts=["packages.example.test", "PACKAGES.example.test"])
    artifact = {
        "url": "https://packages.example.test/pkg.whl",
        "sha256": "a" * 64,
        "filename": "pkg.whl",
        "size_bytes": 1,
    }
    with pytest.raises(ValidationError):
        DependencyLock(
            program="python",
            lockfile_sha256="b" * 64,
            artifacts=[artifact, {**artifact, "url": "https://packages.example.test/other.whl"}],
        )
