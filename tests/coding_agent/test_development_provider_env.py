"""Credential-free coverage for development provider environment options."""

from __future__ import annotations

import asyncio
import os
import sys

import pytest

from krim_agent.providers.openai_compatible import OpenAICompatibleConfig, ProviderError


@pytest.fixture(autouse=True)
def isolated_provider_environment(monkeypatch):
    for name in tuple(os.environ):
        if name.startswith("KRIM_AGENT_HTTP_"):
            monkeypatch.delenv(name)


def configure(monkeypatch, **values):
    monkeypatch.setenv("KRIM_AGENT_HTTP_BASE_URL", "http://127.0.0.1:1/v1")
    monkeypatch.setenv("KRIM_AGENT_HTTP_MODEL", "fixture")
    for name, value in values.items():
        monkeypatch.setenv("KRIM_AGENT_HTTP_" + name.upper(), value)
    return OpenAICompatibleConfig.from_env()


def test_absent_provider_environment_stays_disabled():
    assert OpenAICompatibleConfig.from_env() is None


def test_development_environment_preserves_small_text_defaults(monkeypatch):
    config = configure(monkeypatch)
    assert config is not None
    assert config.allow_images is False
    assert config.max_request_bytes == 32768
    assert config.max_tool_calls == 1
    assert config.max_output_tokens == 512
    assert config.read_timeout_seconds == 5


def test_explicit_development_image_and_tool_budget(monkeypatch):
    config = configure(
        monkeypatch,
        allow_images="true",
        max_request_bytes="1048576",
        max_tool_calls="4",
        max_output_tokens="4096",
        read_timeout_seconds="90",
    )
    assert config is not None
    assert config.allow_images is True
    assert config.max_request_bytes == 1048576
    assert config.max_tool_calls == 4
    assert config.max_output_tokens == 4096
    assert config.read_timeout_seconds == 90
    assert config.api_key.get_secret_value() == ""


def test_responses_protocol_loads_from_environment(monkeypatch):
    config = configure(
        monkeypatch,
        wire_api="responses",
        max_response_bytes="1048576",
        max_event_bytes="65536",
        max_events="16384",
        max_arguments_bytes="32768",
    )
    assert config is not None and config.wire_api == "responses"
    assert config.max_response_bytes == 1048576
    assert config.max_event_bytes == 65536 and config.max_events == 16384


def test_unknown_wire_protocol_fails_closed(monkeypatch):
    with pytest.raises(ProviderError, match="provider_config"):
        configure(monkeypatch, wire_api="unknown")


def test_explicit_false_does_not_enable_images(monkeypatch):
    config = configure(monkeypatch, allow_images="false")
    assert config is not None and config.allow_images is False


def test_serve_development_passes_explicit_media_budget(monkeypatch, tmp_path):
    from krim_agent import cli
    from krim_agent.development import serving

    configure(monkeypatch, allow_images="true", max_request_bytes="1048576")
    monkeypatch.setenv("KRIM_AGENT_PROVIDER", "http")
    monkeypatch.setenv("KRIM_AGENT_HTTP_MODE", "agent")
    monkeypatch.setenv("KRIM_AGENT_MAX_REQUEST_BYTES", "1048576")
    database = tmp_path / "not-created.sqlite"
    development = tmp_path / "development.json"
    calls = []

    async def capture(settings, http_config):
        calls.append((settings, http_config))

    monkeypatch.setattr(serving, "run_development", capture)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "krim",
            "serve-development",
            "--database",
            str(database),
            "--development-config",
            str(development),
        ],
    )
    cli.main()
    assert len(calls) == 1
    settings, provider = calls[0]
    assert settings.database == database
    assert settings.development_config_file == development
    assert settings.provider == "http" and settings.http_mode == "agent"
    assert settings.max_request_bytes == provider.max_request_bytes == 1048576
    assert provider.allow_images is True
    assert not database.exists()


def test_image_environment_reaches_real_development_composition(monkeypatch, tmp_path):
    import httpx

    from krim_agent.composition import open_application
    from krim_agent.config import Settings
    from krim_agent.development.configuration import DevelopmentConfiguration
    from krim_agent.development.service import DevelopmentService
    from krim_agent.sandbox.configuration import SandboxConfiguration

    config = configure(monkeypatch, allow_images="true", max_request_bytes="1048576")
    database = tmp_path / "media-composition.sqlite"
    broker = tmp_path / "sandbox.json"
    broker.write_text(
        SandboxConfiguration(broker_socket=tmp_path / "unused.sock").model_dump_json()
    )
    broker.chmod(0o600)
    calls = []

    async def no_network(*args, **kwargs):
        calls.append("network")
        raise AssertionError("composition must not issue HTTP")

    async def supervisor(_self):
        return None

    monkeypatch.setattr(httpx.AsyncClient, "send", no_network)
    monkeypatch.setattr(DevelopmentService, "start_supervisor", supervisor)

    async def check():
        from krim_agent.accounts.service import IdentityService
        from krim_agent.storage.sqlite import SQLiteStore

        store = await SQLiteStore.open(database)
        identities = await IdentityService.open(store)
        await identities.bootstrap("image configuration fixture", request_key="bootstrap")
        await identities.close()
        await store.close()
        async with await open_application(
            database,
            settings=Settings(
                database=database,
                provider="http",
                http_mode="agent",
                sandbox_config_file=broker,
                max_request_bytes=1048576,
            ),
            http_config=config,
            development_configuration=DevelopmentConfiguration(
                image_id="sha256:" + "a" * 64,
                broker_environments_root=tmp_path / "environments",
                private_state_root=tmp_path / "private",
                model_images=True,
                history_compaction=True,
            ),
        ) as app:
            binding = app.service.bindings["target-development"]
            assert binding.loop.development_media_for_run is not None
            assert binding.loop.provider.config.allow_images is True
            assert binding.settings.max_request_bytes == 1048576
            assert binding.settings.development_history_compaction is True

    asyncio.run(check())
    assert calls == []


@pytest.mark.parametrize(
    "name,value",
    [
        ("allow_images", "1"),
        ("allow_images", "yes"),
        ("allow_images", "TRUE"),
        ("allow_images", ""),
        ("max_request_bytes", "1048577"),
        ("max_request_bytes", "255"),
        ("max_request_bytes", "1e6"),
        ("max_request_bytes", " 32768"),
        ("max_request_bytes", "32768.0"),
        ("max_response_bytes", "1048577"),
        ("max_event_bytes", "1048577"),
        ("max_events", "16385"),
        ("max_arguments_bytes", "65537"),
        ("max_tool_calls", "0"),
        ("max_tool_calls", "5"),
        ("max_tool_calls", "true"),
        ("max_tool_calls", "1.0"),
    ],
)
def test_invalid_development_environment_fails_closed(monkeypatch, name, value):
    with pytest.raises(ProviderError, match="provider_config"):
        configure(monkeypatch, **{name: value})
