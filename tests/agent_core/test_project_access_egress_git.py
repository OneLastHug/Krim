from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from krim_agent.contracts import ServiceError
from krim_agent.network.egress_policy import EgressViolation
from krim_agent.project_access.egress import ProjectEgressPolicy
from krim_agent.project_access.git import (
    GitOperation,
    canonical_repo,
    check_helper_request,
    credential_response,
    exact_ref,
)
from krim_agent.project_access.types import NetworkGrant


def policy():
    return ProjectEgressPolicy(
        NetworkGrant(
            owner_id="owner",
            project_id="project",
            policy_id="npm",
            revision=2,
            hosts=("registry.npmjs.org", "cdn.example"),
            ports=(443,),
            expires_at=2000.0,
        )
    )


def test_registry_cdn_and_query_share_domain_policy():
    p = policy()
    assert p.validate_target("https://registry.npmjs.org/pkg/-/pkg-1.0.tgz").port == 443
    assert p.validate_target("https://cdn.example/download?signature=fixture").host == "cdn.example"
    assert (
        p.validate_target("https://REGISTRY.NPMJS.ORG./other-package").host == "registry.npmjs.org"
    )
    assert p.approve_addresses(["1.1.1.1", "1.1.1.1"]) == ("1.1.1.1",)


@pytest.mark.parametrize(
    "url",
    [
        "https://evil.example/download",
        "https://cdn.example.evil.test/x",
        "http://cdn.example/x",
        "https://cdn.example:8443/x",
        "https://token@cdn.example/x",
        "https://cdn.example/x#fragment",
        "https://cdn.example\\@evil.example/x",
        "https://127.0.0.1/x",
        "https://cdn.example/x?fixture\nsecret",
        "https://cdn.example/x?fixture\u0085secret",
    ],
)
def test_unapproved_target_and_redirect_denied(url):
    with pytest.raises(EgressViolation) as exc:
        policy().validate_target(url)
    assert "fixture" not in str(exc.value)


@pytest.mark.parametrize(
    "addresses",
    [
        [],
        ["127.0.0.1"],
        ["169.254.169.254"],
        ["10.0.0.1"],
        ["::1"],
        ["::ffff:127.0.0.1"],
        ["1.1.1.1", "10.0.0.1"],
        ["not-an-ip"],
    ],
)
def test_existing_ssrf_rules_reused(addresses):
    with pytest.raises(EgressViolation):
        policy().approve_addresses(addresses)


@pytest.mark.parametrize(
    "url",
    [
        "ssh://git.example/team/repo",
        "git@git.example:team/repo",
        "/local/path",
        "https://token@git.example/team/repo",
        "https://git.example/team/repo?token=fixture",
        "https://git.example/team/%72epo",
        "https://git.example/team/../repo",
        "https://git.example/team//repo",
        "https://git.example:8443/team/repo",
        "https://git.example/team/repo#main",
        "https://git.example/team/repo\n",
    ],
)
def test_repository_rejects_credentials_and_ambiguity(url):
    with pytest.raises(ValueError, match="invalid Git repository"):
        canonical_repo(url)


@pytest.mark.parametrize(
    "ref",
    [
        "main",
        "HEAD",
        "refs/heads/*",
        "refs/heads/a..b",
        "refs/heads/-",
        "refs/heads/a.lock",
        "refs/heads/.a",
        "refs/heads/a//b",
        "refs/heads/a.",
        "refs/heads/a b",
    ],
)
def test_ref_is_exact_and_safe(ref):
    # '-' is valid in a component and cannot become an option with full refs/.
    if ref == "refs/heads/-":
        assert exact_ref(ref) == ref
    else:
        with pytest.raises(ValueError):
            exact_ref(ref)


def test_repo_canonicalization_and_push_object():
    assert (
        canonical_repo("https://GIT.EXAMPLE.:443/team/repo.git")
        == "https://git.example/team/repo.git"
    )
    request = GitOperation(
        repository="https://git.example/team/repo",
        ref="refs/heads/main",
        action="git.push",
        object_id="a" * 40,
    )
    assert request.ref == "refs/heads/main"


@pytest.mark.parametrize(
    "raw",
    [
        b"protocol=https\nhost=git.example\n\n",  # useHttpPath=false loses repo binding
        b"protocol=https\nhost=git.example\npath=other/repo\n\n",
        b"protocol=https\nhost=evil.example\npath=team/repo\n\n",
        b"protocol=https\nhost=git.example\npath=team/repo\npassword=fixture\n\n",
        b"protocol=https\nhost=git.example\nhost=evil\npath=team/repo\n\n",
        b"protocol=https\nhost=git.example\npath=team/repo",
        b"url=https://git.example/team/repo\n\n",
    ],
)
def test_helper_fails_closed(raw):
    with pytest.raises(ServiceError):
        check_helper_request(raw, "https://git.example/team/repo")


@pytest.mark.parametrize(
    "token", [b"", b"fixture\npassword=other", b"a\0b", b"secret value", b"\xff"]
)
def test_token_cannot_inject_helper_protocol(token):
    with pytest.raises(ServiceError):
        credential_response("oauth2", token)


def test_real_git_credential_protocol_local_only(tmp_path):
    """Git itself invokes a temporary helper. No remote repository is contacted."""
    helper = tmp_path / "git_helper.py"
    helper.write_text(
        "import sys\n"
        "from krim_agent.project_access.git import check_helper_request, credential_response\n"
        "if sys.argv[1] != 'get': sys.exit(0)\n"
        "try:\n"
        " check_helper_request(sys.stdin.buffer.read(), 'https://git.example/team/repo.git')\n"
        "except Exception: sys.exit(1)\n"
        "sys.stdout.buffer.write(credential_response('fixture-user', b'local-fixture-only'))\n",
        encoding="utf-8",
    )
    env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
    env.update(
        GIT_CONFIG_NOSYSTEM="1",
        GIT_CONFIG_GLOBAL=os.devnull,
        GIT_TERMINAL_PROMPT="0",
        HOME=str(tmp_path),
        PYTHONPATH=str(Path(__file__).resolve().parents[2] / "src"),
    )
    command = [
        "git",
        "-c",
        "credential.helper=",
        "-c",
        "credential.useHttpPath=true",
        "-c",
        f"credential.helper=!{sys.executable} {helper}",
        "credential",
        "fill",
    ]
    result = subprocess.run(
        command,
        input=b"url=https://git.example/team/repo.git\n\n",
        capture_output=True,
        env=env,
        cwd=tmp_path,
        timeout=10,
    )
    assert result.returncode == 0, result.stderr.decode()
    assert b"password=local-fixture-only" in result.stdout
    rejected = subprocess.run(
        command,
        input=b"url=https://git.example/team/other.git\n\n",
        capture_output=True,
        env=env,
        cwd=tmp_path,
        timeout=10,
    )
    assert rejected.returncode != 0
    assert b"local-fixture-only" not in rejected.stdout + rejected.stderr
