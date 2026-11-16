"""Compatibility import; no deployment module is required by the host wheel."""

from krim_agent.dev_browser.launcher import DOCKER, SECCOMP_SHA256, WorkerSpec

__all__ = ["DOCKER", "SECCOMP_SHA256", "WorkerSpec"]
