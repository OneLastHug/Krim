"""Provider registration is configuration, not authority to start an agent."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal


class UnsupportedCapability(ValueError):
    def __init__(self, capability: str) -> None:
        self.capability = capability
        super().__init__(capability)


class ProviderUnavailable(LookupError):
    pass


@dataclass(frozen=True)
class ProviderCapabilities:
    continuable: bool = True
    fork: bool = True
    structured_output: bool = False
    images: bool = False
    workspace_isolation: bool = False
    depth_policy: Literal["profile", "leaf"] = "profile"

    def require(
        self,
        *,
        continuable: bool = False,
        fork: bool = False,
        structured_output: bool = False,
        images: bool = False,
        workspace_isolation: bool = False,
    ) -> None:
        for name, requested in {
            "continuable": continuable,
            "fork": fork,
            "structured_output": structured_output,
            "images": images,
            "workspace_isolation": workspace_isolation,
        }.items():
            if requested and not getattr(self, name):
                raise UnsupportedCapability(name)

    def descriptor(self) -> dict[str, bool | str]:
        return {
            "continuable": self.continuable,
            "fork": self.fork,
            "structured_output": self.structured_output,
            "images": self.images,
            "workspace_isolation": self.workspace_isolation,
            "depth_policy": self.depth_policy,
        }


@dataclass(frozen=True)
class ProviderRegistration:
    name: str
    version: str
    provider: Any
    capabilities: ProviderCapabilities
    generation: int


class ProviderRegistry:
    """Keep accepted providers alive; replacements affect only new admission.

    An activation retains its registration, not merely a name that might later
    resolve to another instance. A caller must assert_current after asynchronous
    preparation and before publishing a new activation.
    """

    def __init__(self) -> None:
        self._providers: dict[str, ProviderRegistration] = {}
        self._generation = 0

    def register(
        self,
        name: str,
        provider: Any,
        *,
        version: str,
        capabilities: ProviderCapabilities,
    ) -> ProviderRegistration:
        if not name or len(name) > 64 or not version or len(version) > 128:
            raise ValueError("invalid provider registration")
        if provider is None:
            raise ValueError("missing provider implementation")
        self._generation += 1
        entry = ProviderRegistration(name, version, provider, capabilities, self._generation)
        self._providers[name] = entry
        return entry

    def resolve(self, name: str, *, version: str | None = None) -> ProviderRegistration:
        entry = self._providers.get(name)
        if entry is None or version is not None and entry.version != version:
            raise ProviderUnavailable(name)
        return entry

    def assert_current(self, entry: ProviderRegistration) -> None:
        if self._providers.get(entry.name) is not entry:
            raise ProviderUnavailable(entry.name)

    def remove(self, name: str) -> None:
        # Disposing a provider here would invalidate already accepted work. The
        # host owns shutdown and drains those activations before closing clients.
        self._providers.pop(name, None)

    def descriptors(self) -> list[dict[str, object]]:
        return [
            {
                "name": entry.name,
                "version": entry.version,
                "capabilities": entry.capabilities.descriptor(),
            }
            for entry in sorted(self._providers.values(), key=lambda item: item.name)
        ]
