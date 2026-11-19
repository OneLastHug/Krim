"""Git identity, exact operation binding and standard credential-helper framing.

These domain functions do not run Git or mint provider tokens. The host-only
Git runner is in git_execution and uses a separate native credential channel.
Matching a helper request proves the repository context only; the runner binds
ref/action/OIDs to its structured argv at guarded process acceptance.
"""

from __future__ import annotations

import re
from typing import Literal
from urllib.parse import urlsplit

from pydantic import Field, field_validator

from krim_agent.contracts import ServiceError, StrictModel
from krim_agent.development.contracts import Identifier
from krim_agent.network.egress_policy import EgressPolicy, EgressViolation

GitAction = Literal["git.clone", "git.diff", "git.commit", "git.push"]


def canonical_repo(url: str) -> str:
    """Conservative HTTPS repository identity; ambiguous encodings fail closed."""
    try:
        parsed = urlsplit(url)
        host = parsed.hostname
        if (
            not host
            or parsed.scheme != "https"
            or parsed.query
            or parsed.fragment
            or "?" in url
            or "#" in url
            or not url.isascii()
            or parsed.username is not None
            or parsed.password is not None
            or any(ord(c) < 33 or ord(c) == 127 for c in url)
            or any(c in url for c in ("%", "\\"))
            or parsed.port not in (None, 443)
            or not re.fullmatch(r"/[A-Za-z0-9_.-]+(?:/[A-Za-z0-9_.-]+)+", parsed.path)
            or any(p in (".", "..") for p in parsed.path.split("/")[1:])
        ):
            raise ValueError
        target = EgressPolicy((host,), (443,)).validate_target(url)
        return f"https://{target.host}{parsed.path}"
    except (ValueError, EgressViolation):
        raise ValueError("invalid Git repository") from None


def exact_ref(value: str) -> str:
    # Deliberately a safe subset of git-check-ref-format, not a shell parser.
    if (
        not re.fullmatch(r"refs/(heads|tags)/[A-Za-z0-9_./-]+", value)
        or any(x in value for x in ("..", "//", "@{"))
        or any(not p or p.startswith(".") or p.endswith((".", ".lock")) for p in value.split("/"))
    ):
        raise ValueError("Git requires an exact fully qualified ref")
    return value


class GitOperation(StrictModel):
    repository: str
    ref: str
    action: GitAction
    # Push is bound to the exact object, not a mutable local branch name.
    object_id: str | None = Field(default=None, pattern=r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")
    expected_old_id: str | None = Field(default=None, pattern=r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")

    _repo = field_validator("repository")(canonical_repo)
    _ref = field_validator("ref")(exact_ref)


class GitIdentity(StrictModel):
    name: str = Field(min_length=1, max_length=128)
    email: str = Field(min_length=3, max_length=254)

    @field_validator("name", "email")
    @classmethod
    def no_injection(cls, value: str) -> str:
        if any(ord(c) < 32 or ord(c) == 127 or c in "<>" for c in value):
            raise ValueError("invalid Git identity")
        return value


class GitGrant(StrictModel):
    owner_id: Identifier
    project_id: Identifier
    repository: str
    refs: tuple[str, ...] = Field(min_length=1)
    actions: tuple[GitAction, ...]
    identity: GitIdentity
    credential_secret_id: Identifier | None = None
    credential_username: str = Field(default="oauth2", min_length=1, max_length=128)
    expires_at: float
    revoked: bool = False

    _repo = field_validator("repository")(canonical_repo)

    @field_validator("refs")
    @classmethod
    def validate_refs(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(exact_ref(v) for v in values)

    @field_validator("credential_username")
    @classmethod
    def username(cls, value: str) -> str:
        if not re.fullmatch(r"[A-Za-z0-9_.@+-]+", value):
            raise ValueError("invalid credential username")
        return value


def check_git_operation(operation: GitOperation, grant: GitGrant) -> None:
    if (
        operation.repository != grant.repository
        or operation.ref not in grant.refs
        or operation.action not in grant.actions
        or (operation.action == "git.push" and operation.object_id is None)
    ):
        raise ServiceError("permission_denied", 403)


def check_helper_request(
    raw: bytes,
    repository: str,
    *,
    username: str | None = None,
) -> None:
    """Validate Git helper `get` input; no URL/token echoed into exceptions.

    useHttpPath=true is mandatory. Only the old stable protocol fields are
    accepted; unknown/multivalue auth extensions fail closed in this adapter.
    """
    try:
        # Git may terminate helper stdin with EOF after the final newline.
        if not raw or len(raw) > 8192 or not raw.endswith(b"\n"):
            raise ValueError
        text = raw.decode("ascii")
        body = text[:-2] if text.endswith("\n\n") else text[:-1]
        fields: dict[str, str] = {}
        for line in body.split("\n"):
            key, sep, value = line.partition("=")
            if (
                not sep
                or key not in {"protocol", "host", "path", "username"}
                or key in fields
                or not value
                or any(ord(c) < 32 or ord(c) == 127 for c in value)
            ):
                raise ValueError
            fields[key] = value
        if set(fields) - {"username"} != {"protocol", "host", "path"}:
            raise ValueError
        if fields["protocol"] != "https":
            raise ValueError
        if "username" in fields and fields["username"] != username:
            raise ValueError
        actual = canonical_repo(f"https://{fields['host']}/{fields['path']}")
        if actual != canonical_repo(repository):
            raise ValueError
    except (ValueError, UnicodeError):
        raise ServiceError("permission_denied", 403) from None


def credential_response(username: str, token: bytes) -> bytes:
    """Trusted channel framing only. Never return this from a model/tool API."""
    if (
        not token
        or len(token) > 12000
        or any(c < 33 or c > 126 for c in token)
        or not username
        or len(username) > 128
        or any(not (c.isascii() and (c.isalnum() or c in "_.@+-")) for c in username)
    ):
        raise ServiceError("invalid_input", 422)
    return b"username=" + username.encode("ascii") + b"\npassword=" + token + b"\n\n"
