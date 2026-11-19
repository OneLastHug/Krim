"""SDK recovery envelopes; old dispatch checkpoints are never translated.

RunService remains the sole run owner. It claims the run, restores the saved
execution budget and validates target/approval facts before SDK replay. Legacy
schema_version=1 checkpoints and uncertain operations remain stored, but cannot
be automatically migrated into a new dispatcher.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from krim_agent.contracts import ServiceError, fingerprint


@dataclass(frozen=True)
class SDKResume:
    checkpoint: dict[str, Any]
    fingerprint: str
    replies: Mapping[str, bool]


def sdk_resume(checkpoint: dict[str, Any], stored: str, replies: Mapping[str, bool]) -> SDKResume:
    """Accept only complete SDK checkpoints, never convert legacy operations."""
    if (checkpoint.get("format") not in {"sdk-probe-v1", "sdk-host-v2"}
            or fingerprint(checkpoint) != stored):
        raise ServiceError("facts_corrupt")
    return SDKResume(checkpoint, stored, replies)
