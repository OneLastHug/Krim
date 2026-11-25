"""模型网关配置域：重开保持、CAS、幂等、密钥保留/清除、跨账号与推理映射。"""

from __future__ import annotations

import pytest

from krim_agent.contracts import ServiceError
from krim_agent.model_gateway import GatewaySelection, ModelGatewayService
from krim_agent.storage.sqlite import SQLiteStore


class FakeVault:
    """上下文绑定 + 可逆编码的假保险库；不引入 cryptography。"""

    def __init__(self) -> None:
        self.writes: list[tuple[str, str, int]] = []

    def encrypt(self, context, plaintext: bytes) -> str:
        assert context.purpose == "model-gateway"
        self.writes.append(
            (context.principal_id, context.connection_id, context.credential_revision)
        )
        return (
            f"v1:{context.principal_id}:{context.connection_id}:"
            f"{context.credential_revision}:{plaintext.hex()}"
        )

    def decrypt(self, context, ciphertext: str) -> bytes:
        assert context.purpose == "model-gateway"
        prefix = (
            f"v1:{context.principal_id}:{context.connection_id}:{context.credential_revision}:"
        )
        assert ciphertext.startswith(prefix)
        return bytes.fromhex(ciphertext[len(prefix):])


def _model(model_id: str = "m1", *, levels=None, display_name: str | None = None):
    return {
        "id": model_id,
        "display_name": display_name or model_id,
        "allow_images": False,
        "max_output_tokens": 4096,
        "reasoning_levels": levels or {},
    }


def _gateway(
    *,
    gateway_id: str = "gw",
    models=None,
    name: str = "Gateway",
    base_url: str = "https://api.example.com/v1",
    wire_api: str = "chat_completions",
    **secret,
):
    return {
        "gateway_id": gateway_id,
        "name": name,
        "base_url": base_url,
        "wire_api": wire_api,
        "models": [_model()] if models is None else models,
        **secret,
    }


def _update(*, expected: int = 0, key: str = "req-1", gateway=None, gateways=None, default=None):
    return {
        "expected_revision": expected,
        "request_key": key,
        "gateways": gateways if gateways is not None else [gateway or _gateway()],
        "default_selection": default,
    }


async def _open(tmp_path, vault=None):
    store = await SQLiteStore.open(tmp_path / "app.db")
    return store, ModelGatewayService(store, vault)


@pytest.mark.asyncio
async def test_configuration_survives_reopen_and_view_hides_key(tmp_path):
    vault = FakeVault()
    store, service = await _open(tmp_path, vault)
    try:
        view = await service.update(
            "alice", _update(gateway=_gateway(api_key="sk-live-11111111"))
        )
        assert view.revision == 1
        assert view.gateways[0].api_key_set is True
        assert "sk-live-11111111" not in view.model_dump_json()
    finally:
        await store.close()

    store, service = await _open(tmp_path, vault)
    try:
        view = await service.get("alice")
        assert view.revision == 1
        assert view.gateways[0].api_key_set is True
        selection = GatewaySelection(gateway_id="gw", model_id="m1")
        config, resolved, revision = await service.resolve("alice", selection, None)
        assert revision == 1
        assert resolved == selection
        assert config.api_key.get_secret_value() == "sk-live-11111111"
        assert config.base_url == "https://api.example.com/v1"
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_cas_and_request_key_idempotency(tmp_path):
    store, service = await _open(tmp_path, FakeVault())
    try:
        first = await service.update("alice", _update(key="r1"))
        assert first.revision == 1

        with pytest.raises(ServiceError) as stale:
            await service.update(
                "alice", _update(key="r2", expected=0, gateways=[_gateway(name="x")])
            )
        assert stale.value.code == "revision_conflict"

        replay = await service.update("alice", _update(key="r1", expected=0))
        assert replay.revision == 1

        with pytest.raises(ServiceError) as conflict:
            await service.update(
                "alice", _update(key="r1", expected=0, gateways=[_gateway(name="changed")])
            )
        assert conflict.value.code == "key_conflict"
        assert (await service.get("alice")).revision == 1
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_blank_key_keeps_and_clear_key_removes(tmp_path):
    vault = FakeVault()
    store, service = await _open(tmp_path, vault)
    try:
        await service.update(
            "alice", _update(key="r1", gateway=_gateway(api_key="sk-keep-22222222"))
        )
        kept = await service.update(
            "alice", _update(key="r2", expected=1, gateway=_gateway(api_key=""))
        )
        assert kept.gateways[0].api_key_set is True
        selection = GatewaySelection(gateway_id="gw", model_id="m1")
        config, _, _ = await service.resolve("alice", selection, None)
        assert config.api_key.get_secret_value() == "sk-keep-22222222"

        cleared = await service.update(
            "alice", _update(key="r3", expected=2, gateway=_gateway(clear_api_key=True))
        )
        assert cleared.gateways[0].api_key_set is False
        config, _, _ = await service.resolve("alice", selection, None)
        assert config.api_key.get_secret_value() == ""
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_principals_are_isolated(tmp_path):
    store, service = await _open(tmp_path, FakeVault())
    try:
        await service.update("alice", _update(key="a1"))
        assert (await service.get("bob")).revision == 0
        await service.update(
            "bob", _update(key="b1", gateway=_gateway(gateway_id="bob-gw"))
        )
        assert (await service.get("bob")).revision == 1
        assert (await service.get("alice")).revision == 1
        with pytest.raises(ServiceError) as missing:
            await service.resolve("bob", GatewaySelection(gateway_id="gw", model_id="m1"), None)
        assert missing.value.code == "provider_config"
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_reasoning_level_mapping_and_unknown_level(tmp_path):
    levels = {
        "fast": {"effort": None, "thinking": "disabled"},
        "deep": {"effort": "xhigh", "thinking": None},
    }
    default = {"gateway_id": "gw", "model_id": "m1", "reasoning_level": "fast"}
    store, service = await _open(tmp_path, FakeVault())
    try:
        await service.update(
            "alice",
            _update(
                key="r1",
                gateway=_gateway(models=[_model(levels=levels)]),
                default=default,
            ),
        )
        config, selection, _ = await service.resolve("alice", None, None)
        assert selection.reasoning_level == "fast"
        assert config.thinking == "disabled"
        assert config.reasoning_effort is None

        deep = GatewaySelection(gateway_id="gw", model_id="m1", reasoning_level="deep")
        config, resolved, _ = await service.resolve("alice", deep, None)
        assert resolved == deep
        assert config.reasoning_effort == "xhigh"
        assert config.thinking is None

        with pytest.raises(ServiceError) as unknown:
            await service.resolve(
                "alice",
                GatewaySelection(gateway_id="gw", model_id="m1", reasoning_level="missing"),
                None,
            )
        assert unknown.value.code == "provider_config"
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_plaintext_key_requires_vault_and_env_reference_works(tmp_path, monkeypatch):
    store, service = await _open(tmp_path)
    try:
        with pytest.raises(ServiceError) as rejected:
            await service.update(
                "alice", _update(key="r1", gateway=_gateway(api_key="sk-reject-33333333"))
            )
        assert rejected.value.code == "provider_config"
        assert (await service.get("alice")).revision == 0

        monkeypatch.setenv("KRIM_TEST_GATEWAY_KEY", "sk-env-77777777")
        view = await service.update(
            "alice", _update(key="r2", gateway=_gateway(api_key_env="KRIM_TEST_GATEWAY_KEY"))
        )
        assert view.gateways[0].api_key_set is True
        assert view.gateways[0].api_key_env == "KRIM_TEST_GATEWAY_KEY"
        config, _, _ = await service.resolve(
            "alice", GatewaySelection(gateway_id="gw", model_id="m1"), None
        )
        assert config.api_key.get_secret_value() == "sk-env-77777777"

        monkeypatch.delenv("KRIM_TEST_GATEWAY_KEY")
        with pytest.raises(ServiceError) as missing_env:
            await service.resolve(
                "alice", GatewaySelection(gateway_id="gw", model_id="m1"), None
            )
        assert missing_env.value.code == "provider_config"
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_history_revision_keeps_old_model_and_key(tmp_path):
    vault = FakeVault()
    store, service = await _open(tmp_path, vault)
    try:
        await service.update(
            "alice",
            _update(
                key="r1",
                gateway=_gateway(
                    models=[_model("m1")],
                    base_url="https://one.example.com/v1",
                    api_key="sk-old-44444444",
                ),
            ),
        )
        await service.update(
            "alice",
            _update(
                key="r2",
                expected=1,
                gateway=_gateway(
                    models=[_model("m2")],
                    base_url="https://two.example.com/v1",
                    api_key="sk-new-55555555",
                ),
            ),
        )
        old_config, _, old_revision = await service.resolve(
            "alice", GatewaySelection(gateway_id="gw", model_id="m1"), 1
        )
        assert old_revision == 1
        assert old_config.base_url == "https://one.example.com/v1"
        assert old_config.api_key.get_secret_value() == "sk-old-44444444"

        new_config, _, new_revision = await service.resolve(
            "alice", GatewaySelection(gateway_id="gw", model_id="m2"), None
        )
        assert new_revision == 2
        assert new_config.base_url == "https://two.example.com/v1"
        assert new_config.api_key.get_secret_value() == "sk-new-55555555"
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_gateway_config_supports_discovery_without_models(tmp_path):
    store, service = await _open(tmp_path, FakeVault())
    try:
        await service.update(
            "alice",
            _update(
                key="r1",
                gateway=_gateway(models=[], api_key="sk-discovery-66666666"),
            ),
        )
        config = await service.gateway_config("alice", "gw")
        assert config.model == "discovery"
        assert config.base_url == "https://api.example.com/v1"
        assert config.api_key.get_secret_value() == "sk-discovery-66666666"

        with pytest.raises(ServiceError) as missing:
            await service.gateway_config("alice", "nope")
        assert missing.value.code == "not_found"
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_invalid_update_is_rejected(tmp_path):
    store, service = await _open(tmp_path, FakeVault())
    try:
        with pytest.raises(ServiceError) as bad_url:
            await service.update(
                "alice", _update(key="r1", gateway=_gateway(base_url="ftp://bad"))
            )
        assert bad_url.value.code == "invalid_input"

        with pytest.raises(ServiceError) as duplicates:
            await service.update(
                "alice", _update(key="r2", gateways=[_gateway(), _gateway()])
            )
        assert duplicates.value.code == "invalid_input"

        with pytest.raises(ServiceError) as bad_default:
            await service.update(
                "alice",
                _update(
                    key="r3",
                    default={
                        "gateway_id": "gw",
                        "model_id": "m1",
                        "reasoning_level": "missing",
                    },
                ),
            )
        assert bad_default.value.code == "invalid_input"

        with pytest.raises(ServiceError) as bad_effort:
            await service.update(
                "alice",
                _update(
                    key="r4",
                    gateway=_gateway(models=[_model(levels={"bad": {"effort": "high!"}})]),
                ),
            )
        assert bad_effort.value.code == "invalid_input"

        with pytest.raises(ServiceError) as bad_thinking:
            await service.update(
                "alice",
                _update(
                    key="r5",
                    gateway=_gateway(
                        wire_api="responses",
                        models=[_model(levels={"bad": {"thinking": "enabled"}})],
                    ),
                ),
            )
        assert bad_thinking.value.code == "invalid_input"
        assert (await service.get("alice")).revision == 0
    finally:
        await store.close()
