"""个人MCP凭据保险库聚焦测试；真实AESGCM用例在缺cryptography时显式blocked失败，不静默跳过。"""

import base64
import importlib.util
import json
import os

import pytest

from krim_agent.account_mcp import secrets as krim_secrets
from krim_agent.account_mcp.contracts import SecretContext
from krim_agent.account_mcp.secrets import AuthenticatedSecretVault, MasterKeyring
from krim_agent.contracts import ServiceError


def _context(**overrides) -> SecretContext:
    values = {
        "principal_id": "principal-a",
        "connection_id": "connection-a",
        "credential_revision": 3,
        "purpose": "mcp:remote",
        **overrides,
    }
    return SecretContext(**values)


def _vault() -> AuthenticatedSecretVault:
    return AuthenticatedSecretVault(MasterKeyring.create(os.urandom(32)))


def _real_vault() -> AuthenticatedSecretVault:
    if importlib.util.find_spec("cryptography") is None:
        pytest.fail(
            "blocked: cryptography 未安装（依赖由主会话统一处理）；AESGCM 真实加解密测试未运行",
            pytrace=False,
        )
    return _vault()


def _payload_text(size: int) -> str:
    return base64.urlsafe_b64encode(b"\x01" * size).rstrip(b"=").decode("ascii")


def _envelope(key_id: str, decoded_size: int) -> str:
    return f"v1.{key_id}.{_payload_text(decoded_size)}"


def _write(path, data: bytes, mode: int = 0o600) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, mode)
    try:
        os.write(fd, data)
    finally:
        os.close(fd)


BAD_CONTEXTS = [
    {"principal_id": 7},
    {"principal_id": None},
    {"principal_id": ""},
    {"principal_id": "a" * 129},
    {"principal_id": "bad\nid"},
    {"principal_id": "bad\x1f"},
    {"principal_id": "surrogate\ud800"},
    {"connection_id": ""},
    {"connection_id": "a" * 129},
    {"purpose": ""},
    {"purpose": "a" * 65},
    {"purpose": "bad\x00"},
    {"credential_revision": True},
    {"credential_revision": 0},
    {"credential_revision": -1},
    {"credential_revision": 2**31},
    {"credential_revision": "3"},
    {"credential_revision": 3.0},
    {"credential_revision": None},
]


@pytest.mark.parametrize("overrides", BAD_CONTEXTS, ids=lambda item: repr(sorted(item)))
def test_context_fields_are_strict(overrides):
    vault = _vault()
    with pytest.raises(ServiceError) as raised:
        vault.encrypt(_context(**overrides), b"x")
    assert (raised.value.code, raised.value.status) == ("invalid_input", 422)
    with pytest.raises(ServiceError):
        vault.decrypt(_context(**overrides), _envelope(vault.active_key_id, 28))


def test_context_rejects_str_subclass():
    class Text(str):
        pass

    vault = _vault()
    with pytest.raises(ServiceError) as raised:
        vault.encrypt(_context(principal_id=Text("principal-a")), b"x")
    assert raised.value.code == "invalid_input"


def test_master_key_requires_32_bytes():
    for raw in (b"", b"\x01" * 31, b"\x01" * 33, bytearray(32), memoryview(b"\x01" * 32), "x" * 32):
        with pytest.raises(ServiceError) as raised:
            MasterKeyring.create(raw)
        assert raised.value.code == "invalid_input"


def test_key_id_derivation_is_stable_and_distinct():
    first = MasterKeyring.create(bytes(range(32)))
    assert first == MasterKeyring.create(bytes(range(32)))
    other = MasterKeyring.create(bytes(range(1, 33)))
    assert first.active_key_id != other.active_key_id
    assert len(first.active_key_id) == 16
    assert krim_secrets.KEY_ID.match(first.active_key_id)


def test_keyring_rejects_mismatched_key_material_and_caps_rotation():
    raw = os.urandom(32)
    key_id = MasterKeyring.create(raw).active_key_id
    with pytest.raises(ServiceError) as raised:
        MasterKeyring(key_id, {key_id: os.urandom(32)})
    assert raised.value.code == "invalid_input"

    ring = MasterKeyring.create(raw)
    for _ in range(krim_secrets.MAX_KEYS - 1):
        ring = ring.rotate(os.urandom(32))
    with pytest.raises(ServiceError) as raised:
        ring.rotate(os.urandom(32))
    assert (raised.value.code, raised.value.status) == ("input_limit", 422)


def test_rotate_keeps_old_keys():
    raw_old = os.urandom(32)
    ring = MasterKeyring.create(raw_old)
    raw_new = os.urandom(32)
    rotated = ring.rotate(raw_new)
    assert rotated.active_key_id != ring.active_key_id
    assert rotated.keys[ring.active_key_id] == raw_old
    assert rotated.keys[rotated.active_key_id] == raw_new
    assert len(rotated.keys) == 2
    with pytest.raises(ServiceError):
        ring.rotate(raw_new[:31])
    with pytest.raises(ServiceError):
        rotated.rotate(raw_new)


def _keyring_document(key: bytes) -> str:
    key_id = MasterKeyring.create(key).active_key_id
    encoded = base64.urlsafe_b64encode(key).decode("ascii")
    return json.dumps({"active_key_id": key_id, "keys": {key_id: encoded}})


def test_keyring_file_round_trip(tmp_path):
    ring = MasterKeyring.create(os.urandom(32)).rotate(os.urandom(32))
    path = tmp_path / "master-keys.json"
    ring.save(path)
    assert path.stat().st_mode & 0o777 == 0o600
    assert MasterKeyring.load(path) == ring


def test_load_rejects_untrusted_keyring_files(tmp_path):
    key = bytes(range(32))
    document = _keyring_document(key)
    good = tmp_path / "good.json"
    _write(good, document.encode())
    assert MasterKeyring.load(good).active_key_id == MasterKeyring.create(key).active_key_id

    encoded = json.loads(document)["keys"][MasterKeyring.create(key).active_key_id]
    bad_documents = [
        "",
        "not json",
        "[]",
        json.dumps({"keys": {MasterKeyring.create(key).active_key_id: encoded}}),
        json.dumps({"active_key_id": MasterKeyring.create(key).active_key_id}),
        json.dumps({"active_key_id": MasterKeyring.create(key).active_key_id, "keys": {}}),
        json.dumps({"active_key_id": "missing-key", "keys": {}}),
        json.dumps(
            {
                "active_key_id": MasterKeyring.create(key).active_key_id,
                "keys": {MasterKeyring.create(key).active_key_id: "not-base64!!"},
            }
        ),
        json.dumps(
            {
                "active_key_id": MasterKeyring.create(key).active_key_id,
                "keys": {"bad.id": encoded},
            }
        ),
        json.dumps(
            {"active_key_id": MasterKeyring.create(key).active_key_id, "keys": {"key-id": 123}}
        ),
        json.dumps(
            {
                "active_key_id": 5,
                "keys": {MasterKeyring.create(key).active_key_id: encoded},
            }
        ),
        json.dumps(
            {
                "active_key_id": MasterKeyring.create(key).active_key_id,
                "keys": {MasterKeyring.create(key).active_key_id: encoded},
                "extra": True,
            }
        ),
        json.dumps({"active_key_id": MasterKeyring.create(key).active_key_id, "keys": []}),
        '{"active_key_id": "a", "keys": {}, "active_key_id": "b"}',
        "零" * 3000,
    ]
    for text in bad_documents:
        variant = tmp_path / "variant.json"
        _write(variant, text.encode())
        with pytest.raises(ServiceError) as raised:
            MasterKeyring.load(variant)
        assert (raised.value.code, raised.value.status) == ("storage_unavailable", 503)

    oversize = tmp_path / "oversize.json"
    _write(oversize, b"0" * (krim_secrets.MAX_KEYRING_BYTES + 1))
    with pytest.raises(ServiceError) as raised:
        MasterKeyring.load(oversize)
    assert raised.value.code == "storage_unavailable"

    loose = tmp_path / "loose.json"
    _write(loose, document.encode(), 0o644)
    with pytest.raises(ServiceError) as raised:
        MasterKeyring.load(loose)
    assert raised.value.code == "storage_unavailable"

    mismatched_id = tmp_path / "mismatched-id.json"
    mismatched = json.loads(document)
    mismatched["keys"]["AAAAAAAAAAAAAAAA"] = encoded
    _write(mismatched_id, json.dumps(mismatched).encode())
    with pytest.raises(ServiceError) as raised:
        MasterKeyring.load(mismatched_id)
    assert raised.value.code == "storage_unavailable"

    linked = tmp_path / "linked.json"
    os.symlink(good, linked)
    with pytest.raises(ServiceError) as raised:
        MasterKeyring.load(linked)
    assert raised.value.code == "storage_unavailable"

    real_dir = tmp_path / "real-dir"
    real_dir.mkdir()
    _write(real_dir / "keys.json", document.encode())
    link_dir = tmp_path / "link-dir"
    os.symlink(real_dir, link_dir)
    with pytest.raises(ServiceError) as raised:
        MasterKeyring.load(link_dir / "keys.json")
    assert raised.value.code == "storage_unavailable"

    hard = tmp_path / "hard.json"
    os.link(good, hard)
    with pytest.raises(ServiceError) as raised:
        MasterKeyring.load(hard)
    assert raised.value.code == "storage_unavailable"

    with pytest.raises(ServiceError) as raised:
        MasterKeyring.load(tmp_path)
    assert raised.value.code == "storage_unavailable"

    with pytest.raises(ServiceError) as raised:
        MasterKeyring.load(tmp_path / "missing.json")
    assert raised.value.code == "storage_unavailable"


def test_save_refuses_symlink_paths(tmp_path):
    ring = MasterKeyring.create(os.urandom(32))
    target = tmp_path / "target.json"
    _write(target, b"x")
    link = tmp_path / "link.json"
    os.symlink(target, link)
    with pytest.raises(ServiceError) as raised:
        ring.save(link)
    assert raised.value.code == "storage_unavailable"
    real_dir = tmp_path / "real"
    real_dir.mkdir()
    link_dir = tmp_path / "link-dir"
    os.symlink(real_dir, link_dir)
    with pytest.raises(ServiceError) as raised:
        ring.save(link_dir / "keys.json")
    assert raised.value.code == "storage_unavailable"


def test_decrypt_rejects_malformed_envelopes():
    vault = _vault()
    key_id = vault.active_key_id
    cases = [
        "",
        "v1",
        f"v1.{key_id}",
        f"v1.{key_id}.",
        f"v2.{key_id}.{_payload_text(28)}",
        f"V1.{key_id}.{_payload_text(28)}",
        f"v1.unknown-key.{_payload_text(28)}",
        f"v1.k@bad.{_payload_text(28)}",
        f"v1.{key_id}.{'@' * 38}",
        f"v1.{key_id}.{_payload_text(28)}.extra",
        f"v1.{key_id}.{_payload_text(12)}",
        f"v1.{key_id}.{_payload_text(20000)}",
    ]
    for ciphertext in cases:
        with pytest.raises(ServiceError) as raised:
            vault.decrypt(_context(), ciphertext)
        assert (raised.value.code, raised.value.status) == ("invalid_input", 422)
    with pytest.raises(ServiceError):
        vault.decrypt(_context(), b"not-a-str")


def test_missing_backend_maps_to_stable_503(monkeypatch):
    vault = _vault()
    envelope = _envelope(vault.active_key_id, 40)
    monkeypatch.setattr(krim_secrets, "_BACKEND", None)
    real_import = importlib.import_module

    def missing(name, *args, **kwargs):
        if name.startswith("cryptography"):
            raise ModuleNotFoundError("cryptography blocked in test")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(importlib, "import_module", missing)
    with pytest.raises(ServiceError) as raised:
        vault.decrypt(_context(), envelope)
    assert (raised.value.code, raised.value.status) == ("mcp_unavailable", 503)
    with pytest.raises(ServiceError) as raised:
        vault.encrypt(_context(), b"secret")
    assert (raised.value.code, raised.value.status) == ("mcp_unavailable", 503)
    with pytest.raises(ServiceError) as raised:
        vault.reencrypt(_context(), envelope)
    assert (raised.value.code, raised.value.status) == ("mcp_unavailable", 503)


class _BrokenBackend:
    """模拟装上但损坏的cryptography：构造成功，任何调用都抛运行期异常。"""

    def __init__(self, key: bytes) -> None:
        self.key = key

    def encrypt(self, nonce: bytes, data: bytes, aad: bytes) -> bytes:
        raise RuntimeError("broken rust bindings")

    def decrypt(self, nonce: bytes, data: bytes, aad: bytes) -> bytes:
        raise RuntimeError("broken rust bindings")


def test_damaged_backend_import_maps_to_stable_503(monkeypatch):
    vault = _vault()
    envelope = _envelope(vault.active_key_id, 40)
    monkeypatch.setattr(krim_secrets, "_BACKEND", None)
    real_import = importlib.import_module

    def damaged(name, *args, **kwargs):
        if name.startswith("cryptography"):
            raise OSError("Cannot load native module 'cryptography.hazmat.bindings._rust'")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(importlib, "import_module", damaged)
    for call in (
        lambda: vault.encrypt(_context(), b"secret"),
        lambda: vault.decrypt(_context(), envelope),
        lambda: vault.reencrypt(_context(), envelope),
    ):
        with pytest.raises(ServiceError) as raised:
            call()
        assert (raised.value.code, raised.value.status) == ("mcp_unavailable", 503)


def test_damaged_backend_runtime_maps_to_stable_503(monkeypatch):
    vault = _vault()
    envelope = vault.encrypt(_context(), b"secret")
    monkeypatch.setattr(krim_secrets, "_BACKEND", _BrokenBackend)
    with pytest.raises(ServiceError) as raised:
        vault.encrypt(_context(), b"secret")
    assert (raised.value.code, raised.value.status) == ("mcp_unavailable", 503)
    with pytest.raises(ServiceError) as raised:
        vault.decrypt(_context(), envelope)
    assert (raised.value.code, raised.value.status) == ("mcp_unavailable", 503)
    with pytest.raises(ServiceError) as raised:
        vault.reencrypt(_context(), envelope)
    assert (raised.value.code, raised.value.status) == ("mcp_unavailable", 503)


def test_decrypt_maps_only_auth_failures_to_422(monkeypatch):
    if importlib.util.find_spec("cryptography") is None:
        pytest.fail("blocked: cryptography 未安装；InvalidTag 区分用例未运行", pytrace=False)
    from cryptography.exceptions import InvalidTag

    class _TamperedBackend:
        def __init__(self, key: bytes) -> None:
            self.key = key

        def encrypt(self, nonce: bytes, data: bytes, aad: bytes) -> bytes:
            raise RuntimeError("unused in this test")

        def decrypt(self, nonce: bytes, data: bytes, aad: bytes) -> bytes:
            raise InvalidTag()

    vault = _real_vault()
    envelope = vault.encrypt(_context(), b"secret")
    monkeypatch.setattr(krim_secrets, "_BACKEND", _TamperedBackend)
    with pytest.raises(ServiceError) as raised:
        vault.decrypt(_context(), envelope)
    assert (raised.value.code, raised.value.status) == ("invalid_input", 422)


def test_repr_and_errors_leak_no_key_material():
    marker = (b"SECRET-MARKER-" * 3)[:32]
    ring = MasterKeyring.create(marker)
    vault = AuthenticatedSecretVault(ring)
    rendered = " ".join((repr(ring), str(ring), repr(vault), str(vault)))
    assert "SECRET-MARKER" not in rendered
    assert base64.urlsafe_b64encode(marker).decode("ascii") not in rendered
    assert marker.hex() not in rendered
    assert vault.active_key_id in repr(vault)
    with pytest.raises(ServiceError) as raised:
        vault.decrypt(_context(), "junk")
    assert "SECRET-MARKER" not in str(raised.value)


def test_envelope_bounds_hold_for_worst_case_key_id():
    assert krim_secrets.MAX_ENVELOPE_BYTES == 16384
    overhead = len("v1.") + 32 + len(".") + 4 * ((28 + krim_secrets.MAX_PLAINTEXT_BYTES + 2) // 3)
    assert overhead <= krim_secrets.MAX_ENVELOPE_BYTES


def test_round_trip_with_real_aesgcm():
    vault = _real_vault()
    for payload in (b"", b"krim-secret", os.urandom(1024), os.urandom(12000)):
        envelope = vault.encrypt(_context(), payload)
        assert vault.decrypt(_context(), envelope) == payload


def test_envelope_shape_and_nonce_uniqueness():
    vault = _real_vault()
    first = vault.encrypt(_context(), b"same")
    second = vault.encrypt(_context(), b"same")
    assert first != second
    version, key_id, payload = first.split(".", 2)
    assert (version, key_id) == ("v1", vault.active_key_id)
    assert len(first) <= krim_secrets.MAX_ENVELOPE_BYTES
    decoded = base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4))
    assert len(decoded) == len(b"same") + krim_secrets.NONCE_BYTES + krim_secrets.TAG_BYTES


def test_context_binding_blocks_cross_use():
    vault = _real_vault()
    envelope = vault.encrypt(_context(), b"payload")
    mismatches = (
        {"principal_id": "principal-b"},
        {"connection_id": "connection-b"},
        {"purpose": "mcp:stdio"},
        {"credential_revision": 4},
    )
    for overrides in mismatches:
        with pytest.raises(ServiceError) as raised:
            vault.decrypt(_context(**overrides), envelope)
        assert (raised.value.code, raised.value.status) == ("invalid_input", 422)


def test_tampering_and_key_swap_rejected():
    raw_old = os.urandom(32)
    ring = MasterKeyring.create(raw_old)
    vault = AuthenticatedSecretVault(ring.rotate(os.urandom(32)))
    envelope = vault.encrypt(_context(), b"payload")
    version, key_id, payload = envelope.split(".", 2)
    decoded = base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4))
    flipped = base64.urlsafe_b64encode(
        decoded[:-1] + bytes([decoded[-1] ^ 1])
    ).rstrip(b"=").decode("ascii")
    for mutated in (
        envelope[:-1],
        envelope + "A",
        f"v1.{ring.active_key_id}.{payload}",
        f"{version}.{key_id}.{flipped}",
    ):
        with pytest.raises(ServiceError) as raised:
            vault.decrypt(_context(), mutated)
        assert (raised.value.code, raised.value.status) == ("invalid_input", 422)


def test_rotation_reads_old_writes_new():
    raw_old = os.urandom(32)
    old_vault = AuthenticatedSecretVault(MasterKeyring.create(raw_old))
    envelope = old_vault.encrypt(_context(), b"carry")
    new_vault = AuthenticatedSecretVault(MasterKeyring.create(raw_old).rotate(os.urandom(32)))
    assert new_vault.decrypt(_context(), envelope) == b"carry"
    assert new_vault.encrypt(_context(), b"next").startswith(f"v1.{new_vault.active_key_id}.")
    moved = new_vault.reencrypt(_context(), envelope)
    assert moved.startswith(f"v1.{new_vault.active_key_id}.")
    assert new_vault.decrypt(_context(), moved) == b"carry"
    assert old_vault.decrypt(_context(), envelope) == b"carry"


def test_plaintext_limit_with_real_aesgcm():
    vault = _real_vault()
    envelope = vault.encrypt(_context(), b"\x00" * krim_secrets.MAX_PLAINTEXT_BYTES)
    assert len(envelope) <= krim_secrets.MAX_ENVELOPE_BYTES
    assert len(vault.decrypt(_context(), envelope)) == krim_secrets.MAX_PLAINTEXT_BYTES
    with pytest.raises(ServiceError) as raised:
        vault.encrypt(_context(), b"\x00" * (krim_secrets.MAX_PLAINTEXT_BYTES + 1))
    assert (raised.value.code, raised.value.status) == ("input_limit", 422)


def test_saved_keyring_round_trips_real_ciphertexts(tmp_path):
    ring = MasterKeyring.create(os.urandom(32))
    vault = AuthenticatedSecretVault(ring)
    envelope = vault.encrypt(_context(), b"persist")
    path = tmp_path / "keys.json"
    ring.save(path)
    restored = AuthenticatedSecretVault(MasterKeyring.load(path))
    assert restored.decrypt(_context(), envelope) == b"persist"
