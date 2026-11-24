"""Compatibility exports for persisted workspace verification records.

Legacy executor-side completion verification is retired. Terminal database handoff remains in
:mod:`krim_agent.workspace.terminal`; this module does not import or run workspace tools.
"""

from __future__ import annotations

from krim_agent.validation.completion import condition as _condition
from krim_agent.workspace.terminal import (
    _TERMINAL_HELPERS,
    SCHEMA,
    ConditionResult,
    VerificationCommitRejected,
    VerificationIdentity,
    VerificationResult,
    terminal_attempt_same_conn,
)

__all__ = [
    "_TERMINAL_HELPERS",
    "_condition",
    "SCHEMA",
    "ConditionResult",
    "VerificationCommitRejected",
    "VerificationIdentity",
    "VerificationResult",
    "terminal_attempt_same_conn",
]
