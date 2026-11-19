"""Portable worker wire helpers shared with the standard-library supervisor."""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any

MAX_MESSAGE = 262144


def canonical_digest(value: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
    ).encode()).hexdigest()


def checked_id(value: Any) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", value):
        raise ValueError("invalid process id")
    return hashlib.sha256(value.encode()).hexdigest()
