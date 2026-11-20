"""Compatibility exports for persisted workspace verification records.

Legacy executor-side completion verification is retired. Terminal database handoff remains in
:mod:`krim_agent.workspace.terminal`; this module does not import or run workspace tools.
"""

from __future__ import annotations

from krim_agent.validation.completion import condition as _condition
from krim_agent.workspace.terminal import (
    SCHEMA,
    ConditionResult,
    VerificationCommitRejected,
    VerificationIdentity,
    VerificationResult,
)

__all__ = [
    "_condition",
    "SCHEMA",
    "ConditionResult",
    "VerificationCommitRejected",
    "VerificationIdentity",
    "VerificationResult",
]
