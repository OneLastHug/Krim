"""Project-scoped browser interaction adapters; no browser is launched on import."""

from .policy import BrowserArtifact, BrowserError, BrowserLimits
from .session import BrowserSession

__all__ = ["BrowserArtifact", "BrowserError", "BrowserLimits", "BrowserSession"]
