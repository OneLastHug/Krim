"""Native Git cache failure paths with only generated fixture material."""

import os
import subprocess

import pytest

from krim_agent.contracts import ServiceError
from krim_agent.project_access.git import credential_response
from krim_agent.project_access.git_credentials import NativeGitCredentialCache, helper_request
from krim_agent.project_access.secrets import DeliveryBinding, handoff
from krim_agent.project_access.service import ProjectAccessService
from tests.coding_agent.test_project_access_storage import Sink
from tests.coding_agent.test_project_credentials_delivery import git_access


@pytest.mark.skipif(os.name != "posix", reason="native Git Unix socket helper")
async def test_atomic_pipe_limit_cannot_mark_failed_delivery_ready(tmp_path, credential_cache_root):
    values = await git_access(tmp_path / "db.sqlite")
    store, _, _, ref, intent, secret, *_ = values
    binding = DeliveryBinding(intent, ref, secret.secret_id, secret.revision)
    cache = NativeGitCredentialCache(credential_cache_root, binding)
    try:
        with pytest.raises(ServiceError, match="execution_error"):
            handoff(cache, binding, credential_response("oauth2", b"x" * 5000))
        with pytest.raises(ServiceError):
            await cache.ready()
        with pytest.raises(ServiceError):
            _ = cache.binding
    finally:
        cache.close()
        await store.close()
    assert list((credential_cache_root).iterdir()) == []


@pytest.mark.skipif(os.name != "posix", reason="native Git Unix socket helper")
async def test_failed_cleanup_remains_closed_and_retry_targets_same_socket(
    tmp_path, monkeypatch, credential_cache_root,
):
    store, repo, _, ref, intent, secret, vault, operation = await git_access(tmp_path / "db.sqlite")
    binding = DeliveryBinding(intent, ref, secret.secret_id, secret.revision)
    cache = NativeGitCredentialCache(credential_cache_root, binding)
    service = ProjectAccessService(repo, vault, secret_sink=Sink(), git_sink=cache)
    original_run = subprocess.run
    try:
        await service.deliver_git_credential(
            ref, intent, helper_request(operation), helper_action="get"
        )
        await cache.ready()
        socket = cache.helper

        def fail_exit(*args, **kwargs):
            raise RuntimeError("fixture-sensitive-cleanup")

        monkeypatch.setattr(subprocess, "run", fail_exit)
        with pytest.raises(ServiceError, match="execution_error") as raised:
            cache.close()
        assert raised.value.__context__ is None
        assert "fixture-sensitive-cleanup" not in str(raised.value)
        with pytest.raises(ServiceError):
            _ = cache.binding
        with pytest.raises(ServiceError):
            _ = cache.helper
        with pytest.raises(ServiceError):
            await cache.ready()
        assert len(list((credential_cache_root).iterdir())) == 1
        assert "socket=" in socket
    finally:
        monkeypatch.setattr(subprocess, "run", original_run)
        cache.close()
        await store.close()
    assert list((credential_cache_root).iterdir()) == []
