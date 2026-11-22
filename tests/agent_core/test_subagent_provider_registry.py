from __future__ import annotations

import pytest

from krim_agent.subagents.providers.base import (
    ProviderCapabilities,
    ProviderRegistry,
    ProviderUnavailable,
    UnsupportedCapability,
)


def test_replacement_invalidates_preparation_not_accepted_reference() -> None:
    registry = ProviderRegistry()
    first_provider = object()
    first = registry.register(
        "sdk", first_provider, version="v1", capabilities=ProviderCapabilities()
    )
    replacement = registry.register(
        "sdk", object(), version="v2", capabilities=ProviderCapabilities()
    )
    assert registry.resolve("sdk") is replacement
    with pytest.raises(ProviderUnavailable):
        registry.assert_current(first)
    assert first.provider is first_provider
    with pytest.raises(ProviderUnavailable):
        registry.resolve("sdk", version="v1")


def test_removal_does_not_silently_close_existing_provider() -> None:
    registry = ProviderRegistry()
    provider = object()
    entry = registry.register("sdk", provider, version="v1", capabilities=ProviderCapabilities())
    registry.remove("sdk")
    with pytest.raises(ProviderUnavailable):
        registry.resolve("sdk")
    assert entry.provider is provider


def test_one_shot_rejects_continuation_and_fork_before_start() -> None:
    capabilities = ProviderCapabilities(continuable=False, fork=False, depth_policy="leaf")
    with pytest.raises(UnsupportedCapability) as error:
        capabilities.require(continuable=True)
    assert error.value.capability == "continuable"
    with pytest.raises(UnsupportedCapability):
        capabilities.require(fork=True)
    capabilities.require()
    assert capabilities.descriptor()["depth_policy"] == "leaf"
