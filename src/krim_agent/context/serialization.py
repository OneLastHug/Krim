"""Canonical context JSON without application storage-size ceilings.

The canonical representation matches contracts.encoded/fingerprint so existing
frozen anchors remain valid. Request budgets are checked at the model boundary;
stored source descriptors and prepared context are not request-size budgets.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator


def chunks(value: object) -> Iterator[str]:
    return json.JSONEncoder(
        ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).iterencode(value)


def encoded(value: object) -> str:
    return "".join(chunks(value))


def fingerprint(value: object) -> str:
    digest = hashlib.sha256()
    for chunk in chunks(value):
        digest.update(chunk.encode())
    return digest.hexdigest()


def byte_size(value: object) -> int:
    return sum(len(chunk.encode()) for chunk in chunks(value))
