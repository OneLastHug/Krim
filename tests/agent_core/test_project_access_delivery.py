from __future__ import annotations

import traceback

import pytest

from krim_agent.account_mcp.secrets import AuthenticatedSecretVault, MasterKeyring
from krim_agent.contracts import ServiceError, encoded
from krim_agent.development.contracts import AuthorizationRef
from krim_agent.project_access.delivery import (
    SealedDeliveryQueue,
    _binding_context,
)
from krim_agent.project_access.secrets import DeliveryBinding, SecretMaterial
from krim_agent.project_access.types import AccessIntent

TOKEN = b"fixture-transient-secret"


class FakeClock:
    def __init__(self, value: float = 100.0) -> None:
        self.value = value

    def __call__(self) -> float:
        return self.value


class Sink:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.calls: list[tuple[DeliveryBinding, bytes]] = []
        self.views = []

    def deliver(self, binding, material) -> None:
        self.calls.append((binding, bytes(material.borrow())))
        self.views.append(material.borrow())
        if self.fail:
            raise RuntimeError(TOKEN.decode())


def _binding(
    *,
    owner_id: str = "owner-a",
    project_id: str = "project-a",
    operation_id: str = "operation-a",
    secret_id: str = "secret-a",
    revision: int = 1,
) -> DeliveryBinding:
    intent = AccessIntent(
        owner_id=owner_id,
        workspace_id="workspace-a",
        project_id=project_id,
        operation_id=operation_id,
        action="secret.deliver",
        arguments_json=encoded({"destination": "trusted-fixture"}),
    )
    authorization = AuthorizationRef(
        actor_id=owner_id,
        actor_generation=3,
        project_id=project_id,
        approval_id="approval-a",
        arguments_digest="0" * 64,
        secret_refs=(secret_id,),
        network_policy_ref=None,
    )
    authorization = authorization.model_copy(
        update={"arguments_digest": intent.digest(authorization)}
    )
    return DeliveryBinding(intent, authorization, secret_id, revision)


@pytest.fixture
def vault() -> AuthenticatedSecretVault:
    return AuthenticatedSecretVault(MasterKeyring.create(b"k" * 32))


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


def test_deliver_encrypts_binds_and_erases_material(vault, clock):
    binding = _binding()
    material = SecretMaterial(TOKEN)
    queue = SealedDeliveryQueue(vault, clock=clock)

    queue.deliver(binding, material)

    assert bytes(material.borrow()) == b"\0" * len(TOKEN)
    assert TOKEN.decode() not in repr(queue)
    assert len(queue._entries) == 1
    entry = next(iter(queue._entries.values()))
    assert TOKEN.decode() not in entry.ciphertext
    assert binding == entry.binding

    _, context = _binding_context(binding)
    with pytest.raises(ServiceError) as wrong_scope:
        vault.decrypt(_binding_context(_binding(project_id="project-b"))[1], entry.ciphertext)
    assert (wrong_scope.value.code, wrong_scope.value.status) == ("invalid_input", 422)
    with pytest.raises(ServiceError) as wrong_revision:
        vault.decrypt(_binding_context(_binding(revision=2))[1], entry.ciphertext)
    assert (wrong_revision.value.code, wrong_revision.value.status) == ("invalid_input", 422)
    assert vault.decrypt(context, entry.ciphertext) == TOKEN


def test_release_is_exact_and_one_shot(vault, clock):
    binding = _binding()
    queue = SealedDeliveryQueue(vault, clock=clock)
    sink = Sink()
    queue.deliver(binding, SecretMaterial(TOKEN))

    wrong = _binding(operation_id="operation-other")
    with pytest.raises(ServiceError) as denied:
        queue.release(wrong, sink)
    assert denied.value.code == "permission_denied"
    assert not sink.calls

    queue.release(binding, sink)
    assert sink.calls == [(binding, TOKEN)]
    assert bytes(sink.views[0]) == b"\0" * len(TOKEN)

    with pytest.raises(ServiceError) as replay:
        queue.release(binding, sink)
    assert replay.value.code == "permission_denied"
    assert len(sink.calls) == 1


def test_duplicate_key_is_denied_until_consumed(vault, clock):
    binding = _binding()
    queue = SealedDeliveryQueue(vault, clock=clock)
    queue.deliver(binding, SecretMaterial(TOKEN))

    replacement = _binding(revision=2)
    with pytest.raises(ServiceError) as duplicate:
        queue.deliver(replacement, SecretMaterial(b"replacement"))
    assert duplicate.value.code == "permission_denied"

    queue.release(binding, Sink())
    queue.deliver(replacement, SecretMaterial(b"replacement"))


def test_capacity_and_total_bytes_are_bounded(vault, clock):
    first = _binding(secret_id="secret-first")
    second = _binding(secret_id="secret-second")
    queue = SealedDeliveryQueue(vault, capacity=1, clock=clock)
    queue.deliver(first, SecretMaterial(b"one"))
    with pytest.raises(ServiceError) as full:
        queue.deliver(second, SecretMaterial(b"two"))
    assert full.value.code == "capacity"

    queue.release(first, Sink())
    queue.deliver(second, SecretMaterial(b"two"))

    queue = SealedDeliveryQueue(vault, capacity=4, max_bytes=100, clock=clock)
    queue.deliver(first, SecretMaterial(b"one"))
    with pytest.raises(ServiceError) as bytes_full:
        queue.deliver(second, SecretMaterial(b"two"))
    assert bytes_full.value.code == "capacity"
    assert len(queue._entries) == 1


def test_expiry_evicts_entries_and_allows_key_reuse(vault, clock):
    binding = _binding()
    queue = SealedDeliveryQueue(vault, ttl=10, clock=clock)
    queue.deliver(binding, SecretMaterial(TOKEN))
    clock.value = 110

    with pytest.raises(ServiceError) as expired:
        queue.release(binding, Sink())
    assert expired.value.code == "permission_denied"
    assert not queue._entries

    queue.deliver(binding, SecretMaterial(b"fresh"))
    sink = Sink()
    queue.release(binding, sink)
    assert sink.calls == [(binding, b"fresh")]


def test_expiry_during_decrypt_is_denied_without_handoff(vault, clock):
    binding = _binding()

    class SlowVault:
        def encrypt(self, context, plaintext):
            return vault.encrypt(context, plaintext)

        def decrypt(self, context, ciphertext):
            result = vault.decrypt(context, ciphertext)
            clock.value = 110
            return result

    queue = SealedDeliveryQueue(SlowVault(), ttl=10, clock=clock)
    queue.deliver(binding, SecretMaterial(TOKEN))
    sink = Sink()
    with pytest.raises(ServiceError) as expired:
        queue.release(binding, sink)
    assert expired.value.code == "permission_denied"
    assert not sink.calls
    assert not queue._entries


def test_revoke_discards_only_matching_owner_project(vault, clock):
    matching = _binding(owner_id="owner-a", project_id="project-a")
    other_project = _binding(owner_id="owner-a", project_id="project-b")
    other_owner = _binding(owner_id="owner-b", project_id="project-a")
    queue = SealedDeliveryQueue(vault, clock=clock)
    for binding in (matching, other_project, other_owner):
        queue.deliver(binding, SecretMaterial(binding.secret_id.encode()))

    queue.revoke("owner-a", "project-a")
    with pytest.raises(ServiceError):
        queue.release(matching, Sink())

    project_sink = Sink()
    owner_sink = Sink()
    queue.release(other_project, project_sink)
    queue.release(other_owner, owner_sink)
    assert project_sink.calls == [(other_project, b"secret-a")]
    assert owner_sink.calls == [(other_owner, b"secret-a")]


def test_close_discards_all_and_fails_closed(vault, clock):
    binding = _binding()
    queue = SealedDeliveryQueue(vault, clock=clock)
    queue.deliver(binding, SecretMaterial(TOKEN))
    queue.close()
    queue.close()

    with pytest.raises(ServiceError) as release_error:
        queue.release(binding, Sink())
    assert release_error.value.code == "closed"
    with pytest.raises(ServiceError) as deliver_error:
        queue.deliver(binding, SecretMaterial(TOKEN))
    assert deliver_error.value.code == "closed"
    assert not queue._entries


def test_vault_and_sink_errors_are_sanitized_and_consuming(vault, clock):
    binding = _binding()

    class FailingDecryptVault:
        def encrypt(self, context, plaintext):
            return vault.encrypt(context, plaintext)

        def decrypt(self, context, ciphertext):
            raise RuntimeError(TOKEN.decode())

    queue = SealedDeliveryQueue(FailingDecryptVault(), clock=clock)
    queue.deliver(binding, SecretMaterial(TOKEN))
    with pytest.raises(ServiceError) as vault_error:
        queue.release(binding, Sink())
    assert vault_error.value.code == "execution_error"
    assert vault_error.value.__context__ is None
    assert TOKEN.decode() not in "".join(traceback.format_exception(vault_error.value))
    with pytest.raises(ServiceError):
        queue.release(binding, Sink())

    queue = SealedDeliveryQueue(vault, clock=clock)
    queue.deliver(binding, SecretMaterial(TOKEN))
    sink = Sink(fail=True)
    with pytest.raises(ServiceError) as sink_error:
        queue.release(binding, sink)
    assert sink_error.value.code == "execution_error"
    assert sink_error.value.__context__ is None
    assert TOKEN.decode() not in "".join(traceback.format_exception(sink_error.value))
    assert bytes(sink.views[0]) == b"\0" * len(TOKEN)
    with pytest.raises(ServiceError):
        queue.release(binding, Sink())


def test_vault_service_error_code_cannot_carry_plaintext(vault, clock):
    class FailingVault:
        def encrypt(self, context, plaintext):
            raise ServiceError(TOKEN.decode(), 500)

        def decrypt(self, context, ciphertext):
            raise AssertionError

    queue = SealedDeliveryQueue(FailingVault(), clock=clock)
    with pytest.raises(ServiceError) as error:
        queue.deliver(_binding(), SecretMaterial(TOKEN))
    assert error.value.code == "execution_error"
    assert error.value.__context__ is None
    assert TOKEN.decode() not in "".join(traceback.format_exception(error.value))


def test_configuration_must_be_positive_finite(vault):
    for kwargs in (
        {"capacity": 0},
        {"capacity": True},
        {"max_bytes": 0},
        {"max_bytes": True},
        {"ttl": 0},
        {"ttl": float("inf")},
        {"ttl": float("nan")},
        {"ttl": True},
    ):
        with pytest.raises(ServiceError) as raised:
            SealedDeliveryQueue(vault, **kwargs)
        assert raised.value.code == "invalid_input"
