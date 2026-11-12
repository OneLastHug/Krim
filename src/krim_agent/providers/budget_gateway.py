"""Fail-closed, bounded OpenAI-compatible egress gateway for one approved model.

No environment lookup, CLI integration, automatic retry, redirect following, or logging is
performed here. Pricing and the SQLite ledger are mandatory before any outbound request.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import math
import re
import secrets
import sqlite3
import unicodedata
import uuid
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from decimal import ROUND_CEILING, Decimal
from pathlib import Path
from types import MappingProxyType
from typing import Any, cast
from urllib.parse import urlsplit

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse

_APPROVED_ORIGIN = "https://api.site"
_APPROVED_MODEL = "glm-5.3-flash"
_PRICE_UNIT = "CNY_PER_MILLION_TOKENS"
_MAX_REQUESTS = 120
_MAX_TRIAL_REQUESTS = 5
_MAX_INPUT_TOKENS = 1_500_000
_MAX_OUTPUT_TOKENS = 200_000
_MAX_OUTPUT_PER_REQUEST = 2_048
_MAX_COST_CNY = Decimal("20")
_MAX_REQUEST_BYTES = 512 * 1024
_MAX_RESPONSE_BYTES = 4 * 1024 * 1024
_MAX_SSE_FRAME_BYTES = 64 * 1024
_MAX_SSE_EVENTS = 16_384
_MAX_MESSAGES = 128
_MAX_TOOLS = 32
_MAX_TOOL_SCHEMA_BYTES = 64 * 1024
_MAX_TOOL_ARGUMENT_BYTES = 256 * 1024
_MAX_SCHEMA_NODES = 4_096
_MAX_SCHEMA_DEPTH = 16


def generate_trial_token() -> str:
    """Generate one opaque 256-bit bearer token for a single trial."""
    return secrets.token_urlsafe(32)


class _GatewayFault(Exception):
    """Internal stable error without vendor text, request values, or credentials."""

    def __init__(self, code: str, status: int = 400) -> None:
        super().__init__(code)
        self.code = code
        self.status = status


@dataclass(frozen=True, slots=True)
class PricePolicy:
    """Operator-supplied price and input-bound policy, bound to one model.

    The rate unit is always CNY per million tokens. The ``source`` and explicit structural
    overhead document the trusted evidence used by the operator; they are not inferred from
    a different vendor or model. Live forwarding requires the caller to explicitly confirm both
    that price data and the input bound were verified for this exact endpoint/model. Input
    reservation counts the complete UTF-8 JSON wire body (which is at least as large as its
    decoded text) plus configured, bounded protocol/tool overhead. This relies on byte-backed
    tokenization and the supplied overhead being a hard bound for the approved endpoint; usage
    above it poisons the campaign rather than extending the reservation.
    """

    model: str
    currency: str
    unit: str
    input_cny_per_million: Decimal
    output_cny_per_million: Decimal
    source: str
    price_verified: bool
    input_bound_verified: bool
    input_fixed_overhead_tokens: int
    input_per_message_overhead_tokens: int
    input_per_tool_overhead_tokens: int

    def __post_init__(self) -> None:
        if (
            self.model != _APPROVED_MODEL
            or self.currency != "CNY"
            or self.unit != _PRICE_UNIT
            or self.price_verified is not True
            or self.input_bound_verified is not True
            or not isinstance(self.input_cny_per_million, Decimal)
            or not isinstance(self.output_cny_per_million, Decimal)
            or not self.input_cny_per_million.is_finite()
            or not self.output_cny_per_million.is_finite()
            or self.input_cny_per_million <= 0
            or self.output_cny_per_million <= 0
            or not isinstance(self.source, str)
            or not self.source.strip()
            or len(self.source) > 512
            or any(
                type(value) is not int or value <= 0
                for value in (
                    self.input_fixed_overhead_tokens,
                    self.input_per_message_overhead_tokens,
                    self.input_per_tool_overhead_tokens,
                )
            )
        ):
            raise ValueError("invalid price policy")

    def input_upper_bound(self, wire_body: bytes, messages: int, tools: int) -> int:
        # 同时预留文本规范化扩张；不能把兼容字符/组合字符的原始字节数当上界。
        pending: list[Any] = [json.loads(wire_body)]
        normalized_bytes = 0
        while pending:
            value = pending.pop()
            if isinstance(value, str):
                variants = (
                    value,
                    value.casefold(),
                    unicodedata.normalize("NFKC", value),
                    unicodedata.normalize("NFKD", value).casefold(),
                )
                normalized_bytes += max(len(item.encode("utf-8")) for item in variants)
            elif isinstance(value, dict):
                pending.extend(value.keys())
                pending.extend(value.values())
            elif isinstance(value, list):
                pending.extend(value)
        return (
            len(wire_body)
            + normalized_bytes
            + self.input_fixed_overhead_tokens
            + self.input_per_message_overhead_tokens * messages
            + self.input_per_tool_overhead_tokens * tools
        )

    def cost_microcny(self, input_tokens: int, output_tokens: int) -> int:
        # 1 CNY = 1,000,000 micro-CNY; rates are per million tokens, so multiplication
        # directly yields the fractional micro-CNY amount. Round up to stay conservative.
        exact = (
            Decimal(input_tokens) * self.input_cny_per_million
            + Decimal(output_tokens) * self.output_cny_per_million
        )
        return int(exact.to_integral_value(rounding=ROUND_CEILING))

    def fingerprint_data(self) -> dict[str, Any]:
        return {
            "model": self.model,
            "currency": self.currency,
            "unit": self.unit,
            "input_cny_per_million": format(self.input_cny_per_million, "f"),
            "output_cny_per_million": format(self.output_cny_per_million, "f"),
            "source": self.source,
            "price_verified": self.price_verified,
            "input_bound_verified": self.input_bound_verified,
            "input_bound_policy_version": "normalized-wire-bytes/1",
            "input_fixed_overhead_tokens": self.input_fixed_overhead_tokens,
            "input_per_message_overhead_tokens": self.input_per_message_overhead_tokens,
            "input_per_tool_overhead_tokens": self.input_per_tool_overhead_tokens,
        }


@dataclass(frozen=True, slots=True)
class BudgetLimits:
    """Explicit campaign and per-trial ceilings; never defaulted by the gateway."""

    max_requests: int
    max_requests_per_trial: int
    max_input_tokens: int
    max_output_tokens: int
    max_output_tokens_per_request: int
    max_cost_cny: Decimal
    currency: str

    def __post_init__(self) -> None:
        integer_caps = (
            self.max_requests,
            self.max_requests_per_trial,
            self.max_input_tokens,
            self.max_output_tokens,
            self.max_output_tokens_per_request,
        )
        if (
            any(type(value) is not int or value <= 0 for value in integer_caps)
            or self.max_requests > _MAX_REQUESTS
            or self.max_requests_per_trial > _MAX_TRIAL_REQUESTS
            or self.max_input_tokens > _MAX_INPUT_TOKENS
            or self.max_output_tokens > _MAX_OUTPUT_TOKENS
            or self.max_output_tokens_per_request > _MAX_OUTPUT_PER_REQUEST
            or not isinstance(self.max_cost_cny, Decimal)
            or not self.max_cost_cny.is_finite()
            or self.max_cost_cny <= 0
            or self.max_cost_cny > _MAX_COST_CNY
            or self.currency != "CNY"
        ):
            raise ValueError("invalid budget limits")

    @property
    def max_cost_microcny(self) -> int:
        return int(
            (self.max_cost_cny * Decimal(1_000_000)).to_integral_value(rounding=ROUND_CEILING)
        )

    def fingerprint_data(self) -> dict[str, Any]:
        return {
            "max_requests": self.max_requests,
            "max_requests_per_trial": self.max_requests_per_trial,
            "max_input_tokens": self.max_input_tokens,
            "max_output_tokens": self.max_output_tokens,
            "max_output_tokens_per_request": self.max_output_tokens_per_request,
            "max_cost_cny": format(self.max_cost_cny, "f"),
            "currency": self.currency,
        }


@dataclass(frozen=True, slots=True)
class GatewayConfig:
    """Explicit one-origin, one-model configuration; credentials stay in process memory."""

    upstream_origin: str
    model: str
    api_key: str = field(repr=False)
    campaign_id: str
    ledger_path: str | Path
    trial_tokens: Mapping[str, str] = field(repr=False)
    price_policy: PricePolicy | None = None
    budget: BudgetLimits | None = None

    def __post_init__(self) -> None:
        if (
            self.upstream_origin != _APPROVED_ORIGIN
            or self.model != _APPROVED_MODEL
            or not isinstance(self.api_key, str)
            or not self.api_key
            or len(self.api_key) < 16
            or len(self.api_key) > 2048
            or not self.api_key.isascii()
            or any(not 33 <= ord(ch) <= 126 for ch in self.api_key)
            or not isinstance(self.campaign_id, str)
            or re.fullmatch(r"[A-Za-z0-9._-]{1,128}", self.campaign_id) is None
            or self.price_policy is None
            or self.budget is None
            or self.price_policy.model != self.model
            or not isinstance(self.trial_tokens, Mapping)
            or not 1 <= len(self.trial_tokens) <= 24
        ):
            raise ValueError("invalid gateway configuration")
        try:
            parsed = urlsplit(self.upstream_origin)
            if (
                parsed.scheme != "https"
                or parsed.netloc != "api.site"
                or parsed.path
                or parsed.query
                or parsed.fragment
                or parsed.username is not None
                or parsed.password is not None
            ):
                raise ValueError
        except (TypeError, ValueError):
            raise ValueError("invalid gateway configuration") from None
        if not self.ledger_path or str(self.ledger_path) == ":memory:":
            raise ValueError("invalid gateway configuration")
        ledger_path = Path(self.ledger_path)
        if str(ledger_path).startswith("file:") or not ledger_path.parent.exists():
            raise ValueError("invalid gateway configuration")
        if not self.trial_tokens:
            raise ValueError("invalid gateway configuration")
        seen: set[str] = set()
        for trial_id, token in self.trial_tokens.items():
            if (
                not isinstance(trial_id, str)
                or re.fullmatch(r"[A-Za-z0-9._-]{1,128}", trial_id) is None
                or not isinstance(token, str)
                or re.fullmatch(r"[A-Za-z0-9_-]{43,128}", token) is None
                or token in seen
            ):
                raise ValueError("invalid gateway configuration")
            seen.add(token)


@dataclass(frozen=True, slots=True)
class _RuntimeConfig:
    origin: str
    model: str
    api_key: str = field(repr=False)
    campaign_id: str
    ledger_path: str
    token_hashes: Mapping[bytes, str] = field(repr=False)
    policy: PricePolicy
    budget: BudgetLimits
    fingerprint: str


def _runtime_config(config: GatewayConfig) -> _RuntimeConfig:
    # Copy opaque credentials to private memory and retain only SHA-256 trial-token hashes.
    assert config.price_policy is not None and config.budget is not None
    token_hashes = {
        hashlib.sha256(token.encode("ascii")).digest(): trial_id
        for trial_id, token in config.trial_tokens.items()
    }
    safe_data = {
        "origin": config.upstream_origin,
        "model": config.model,
        "campaign_id": config.campaign_id,
        "request_policy_version": "text-functions-nonthinking/1",
        "price_policy": config.price_policy.fingerprint_data(),
        "budget": config.budget.fingerprint_data(),
    }
    encoded = json.dumps(safe_data, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return _RuntimeConfig(
        origin=config.upstream_origin,
        model=config.model,
        api_key=config.api_key,
        campaign_id=config.campaign_id,
        ledger_path=str(Path(config.ledger_path)),
        token_hashes=MappingProxyType(token_hashes),
        policy=config.price_policy,
        budget=config.budget,
        fingerprint=hashlib.sha256(encoded).hexdigest(),
    )


class _Ledger:
    """SQLite ledger: BEGIN IMMEDIATE makes reservations atomic across tasks/processes."""

    def __init__(self, config: _RuntimeConfig) -> None:
        self._path = config.ledger_path
        self._campaign_id = config.campaign_id
        self._fingerprint = config.fingerprint
        self._budget = config.budget
        self._closed = False
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        if self._closed:
            raise sqlite3.ProgrammingError("ledger closed")
        connection = sqlite3.connect(self._path, timeout=15, isolation_level=None)
        connection.execute("PRAGMA busy_timeout=15000")
        connection.execute("PRAGMA synchronous=FULL")
        return connection

    def _initialize(self) -> None:
        connection = self._connect()
        try:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute(
                """CREATE TABLE IF NOT EXISTS gateway_meta (
                    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                    campaign_id TEXT NOT NULL,
                    config_fingerprint TEXT NOT NULL,
                    poisoned INTEGER NOT NULL DEFAULT 0
                )"""
            )
            connection.execute(
                """CREATE TABLE IF NOT EXISTS gateway_reservations (
                    request_id TEXT PRIMARY KEY,
                    trial_id TEXT NOT NULL,
                    state TEXT NOT NULL CHECK(state IN ('reserved','unknown','settled','overrun')),
                    reserved_input INTEGER NOT NULL,
                    reserved_output INTEGER NOT NULL,
                    reserved_cost_microcny INTEGER NOT NULL,
                    actual_input INTEGER,
                    actual_output INTEGER,
                    actual_cost_microcny INTEGER,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )"""
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS gateway_reservations_trial_idx "
                "ON gateway_reservations(trial_id)"
            )
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT campaign_id, config_fingerprint FROM gateway_meta WHERE singleton=1"
            ).fetchone()
            if row is None:
                connection.execute(
                    "INSERT INTO gateway_meta(singleton,campaign_id,config_fingerprint) "
                    "VALUES(1,?,?)",
                    (self._campaign_id, self._fingerprint),
                )
            elif row != (self._campaign_id, self._fingerprint):
                raise ValueError("ledger configuration is immutable")
            connection.commit()
        except BaseException:
            if connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.close()

    def reserve(self, trial_id: str, input_upper: int, output_cap: int, cost_microcny: int) -> str:
        connection = self._connect()
        request_id = uuid.uuid4().hex
        try:
            connection.execute("BEGIN IMMEDIATE")
            meta = connection.execute(
                "SELECT poisoned FROM gateway_meta WHERE singleton=1"
            ).fetchone()
            if meta is None or meta[0]:
                raise _GatewayFault("GW_CAMPAIGN_CLOSED", 429)
            total = connection.execute(
                """SELECT COUNT(*),
                    COALESCE(SUM(CASE WHEN trial_id=? THEN 1 ELSE 0 END),0),
                    COALESCE(
                        SUM(CASE WHEN state='settled' THEN actual_input ELSE reserved_input END),0
                    ),
                    COALESCE(
                        SUM(CASE WHEN state='settled' THEN actual_output ELSE reserved_output END),0
                    ),
                    COALESCE(SUM(CASE WHEN state='settled' THEN actual_cost_microcny
                                      ELSE reserved_cost_microcny END),0)
                    FROM gateway_reservations""",
                (trial_id,),
            ).fetchone()
            assert total is not None
            requests, trial_requests, input_used, output_used, cost_used = total
            if (
                requests >= self._budget.max_requests
                or trial_requests >= self._budget.max_requests_per_trial
                or input_used + input_upper > self._budget.max_input_tokens
                or output_used + output_cap > self._budget.max_output_tokens
                or cost_used + cost_microcny > self._budget.max_cost_microcny
            ):
                raise _GatewayFault("GW_BUDGET_EXHAUSTED", 429)
            connection.execute(
                """INSERT INTO gateway_reservations
                   (request_id,trial_id,state,reserved_input,reserved_output,reserved_cost_microcny)
                   VALUES(?,?,'reserved',?,?,?)""",
                (request_id, trial_id, input_upper, output_cap, cost_microcny),
            )
            connection.commit()
            return request_id
        except BaseException:
            if connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.close()

    def mark_unknown(self, request_id: str) -> None:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                "UPDATE gateway_reservations SET state='unknown' "
                "WHERE request_id=? AND state='reserved'",
                (request_id,),
            )
            connection.commit()
        except BaseException:
            if connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.close()

    def settle(self, request_id: str, input_tokens: int, output_tokens: int, cost: int) -> bool:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """SELECT state,reserved_input,reserved_output,reserved_cost_microcny
                   FROM gateway_reservations WHERE request_id=?""",
                (request_id,),
            ).fetchone()
            if row is None:
                raise sqlite3.DatabaseError("reservation missing")
            state, reserved_input, reserved_output, reserved_cost = row
            if state == "settled":
                connection.commit()
                return True
            if state == "overrun":
                connection.commit()
                return False
            exceeded = (
                input_tokens > reserved_input
                or output_tokens > reserved_output
                or cost > reserved_cost
            )
            if exceeded:
                connection.execute(
                    "UPDATE gateway_reservations SET state='overrun',actual_input=?,"
                    "actual_output=?,actual_cost_microcny=? WHERE request_id=?",
                    (input_tokens, output_tokens, cost, request_id),
                )
                connection.execute("UPDATE gateway_meta SET poisoned=1 WHERE singleton=1")
                connection.commit()
                return False
            connection.execute(
                "UPDATE gateway_reservations SET state='settled',actual_input=?,"
                "actual_output=?,actual_cost_microcny=? WHERE request_id=?",
                (input_tokens, output_tokens, cost, request_id),
            )
            connection.commit()
            return True
        except BaseException:
            if connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.close()

    def close(self) -> None:
        self._closed = True


class _SSEReader:
    def __init__(self, max_bytes: int, max_frame_bytes: int, max_events: int) -> None:
        self.max_bytes = max_bytes
        self.max_frame_bytes = max_frame_bytes
        self.max_events = max_events

    async def frames(self, response: httpx.Response) -> AsyncIterator[bytes]:
        buffer = bytearray()
        total_bytes = 0
        count = 0
        async for chunk in response.aiter_raw():
            total_bytes += len(chunk)
            if total_bytes > self.max_bytes:
                raise _GatewayFault("GW_UPSTREAM_SIZE", 502)
            buffer.extend(chunk)
            while True:
                lf = buffer.find(b"\n\n")
                crlf = buffer.find(b"\r\n\r\n")
                candidates = [(lf, 2), (crlf, 4)]
                candidates = [(position, size) for position, size in candidates if position >= 0]
                if not candidates:
                    break
                position, delimiter_size = min(candidates, key=lambda item: item[0])
                frame = bytes(buffer[:position])
                del buffer[: position + delimiter_size]
                if len(frame) > self.max_frame_bytes:
                    raise _GatewayFault("GW_UPSTREAM_FRAME_LIMIT", 502)
                count += 1
                if count > self.max_events:
                    raise _GatewayFault("GW_UPSTREAM_EVENT_LIMIT", 502)
                yield frame
            if len(buffer) > self.max_frame_bytes:
                raise _GatewayFault("GW_UPSTREAM_FRAME_LIMIT", 502)
        if buffer:
            raise _GatewayFault("GW_UPSTREAM_INCOMPLETE", 502)


def _parse_sse_frame(frame: bytes) -> tuple[str | None, bytes]:
    normalized = frame.replace(b"\r\n", b"\n")
    if b"\r" in normalized:
        raise _GatewayFault("GW_UPSTREAM_PROTOCOL", 502)
    try:
        lines = normalized.decode("utf-8", errors="strict").split("\n")
    except UnicodeError:
        raise _GatewayFault("GW_UPSTREAM_PROTOCOL", 502) from None
    data_lines: list[str] = []
    for line in lines:
        if line.startswith("data:"):
            data_lines.append(line[5:].lstrip(" "))
        elif line and not line.startswith(":"):
            raise _GatewayFault("GW_UPSTREAM_PROTOCOL", 502)
    if not data_lines:
        return None, b": keep-alive\n\n" if lines and all(
            not line or line.startswith(":") for line in lines
        ) else b""
    return "\n".join(data_lines), b""


def _reject_json_constant(_value: str) -> None:
    raise ValueError("non-finite JSON number")


def _strict_json(data: bytes, *, error_code: str = "GW_BAD_REQUEST", status: int = 400) -> Any:
    def no_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate key")
            result[key] = value
        return result

    try:
        text = data.decode("utf-8", errors="strict")
        return json.loads(
            text,
            object_pairs_hook=no_duplicates,
            parse_constant=_reject_json_constant,
        )
    except (UnicodeError, ValueError, RecursionError):
        raise _GatewayFault(error_code, status) from None


def _json_bytes(value: Any) -> bytes:
    try:
        return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode(
            "utf-8"
        )
    except (TypeError, ValueError, RecursionError):
        raise _GatewayFault("GW_BAD_REQUEST", 400) from None


def _walk_json(value: Any, depth: int = 0, counter: list[int] | None = None) -> None:
    if counter is None:
        counter = [0]
    counter[0] += 1
    if counter[0] > _MAX_SCHEMA_NODES or depth > _MAX_SCHEMA_DEPTH:
        raise _GatewayFault("GW_BAD_REQUEST", 400)
    if value is None or type(value) in {bool, int, str}:
        return
    if type(value) is float:
        if not math.isfinite(value):
            raise _GatewayFault("GW_BAD_REQUEST", 400)
        return
    if isinstance(value, list):
        for item in value:
            _walk_json(item, depth + 1, counter)
        return
    if isinstance(value, dict):
        if any(not isinstance(key, str) for key in value):
            raise _GatewayFault("GW_BAD_REQUEST", 400)
        for item in value.values():
            _walk_json(item, depth + 1, counter)
        return
    raise _GatewayFault("GW_BAD_REQUEST", 400)


def _validate_tools(tools: Any) -> list[dict[str, Any]]:
    if tools is None:
        return []
    if not isinstance(tools, list) or len(tools) > _MAX_TOOLS:
        raise _GatewayFault("GW_BAD_REQUEST", 400)
    result: list[dict[str, Any]] = []
    names: set[str] = set()
    for tool in tools:
        if not isinstance(tool, dict) or set(tool) != {"type", "function"}:
            raise _GatewayFault("GW_BAD_REQUEST", 400)
        function = tool.get("function")
        if not isinstance(function, dict) or set(function) - {"name", "description", "parameters"}:
            raise _GatewayFault("GW_BAD_REQUEST", 400)
        name = function.get("name")
        description = function.get("description", "")
        parameters = function.get("parameters")
        if (
            tool.get("type") != "function"
            or not isinstance(name, str)
            or re.fullmatch(r"[A-Za-z0-9_-]{1,64}", name) is None
            or name in names
            or not isinstance(description, str)
            or len(description.encode("utf-8")) > 4096
            or not isinstance(parameters, dict)
        ):
            raise _GatewayFault("GW_BAD_REQUEST", 400)
        _walk_json(parameters)
        if len(_json_bytes(parameters)) > _MAX_TOOL_SCHEMA_BYTES:
            raise _GatewayFault("GW_BAD_REQUEST", 400)
        names.add(name)
        function_copy: dict[str, Any] = {"name": name, "parameters": parameters}
        if "description" in function:
            function_copy["description"] = description
        result.append({"type": "function", "function": function_copy})
    return result


def _validate_messages(messages: Any) -> list[dict[str, Any]]:
    if not isinstance(messages, list) or not 1 <= len(messages) <= _MAX_MESSAGES:
        raise _GatewayFault("GW_BAD_REQUEST", 400)
    result: list[dict[str, Any]] = []
    pending_tool_ids: set[str] = set()
    for message in messages:
        if not isinstance(message, dict):
            raise _GatewayFault("GW_BAD_REQUEST", 400)
        role = message.get("role")
        if not isinstance(role, str):
            raise _GatewayFault("GW_BAD_REQUEST", 400)
        if role in {"system", "developer", "user"}:
            if set(message) != {"role", "content"} or not isinstance(message.get("content"), str):
                raise _GatewayFault("GW_BAD_REQUEST", 400)
        elif role == "assistant":
            if (
                set(message) - {"role", "content", "tool_calls", "refusal", "reasoning_content"}
                or message.get("reasoning_content") not in (None, "")
                or (message.get("refusal") is not None and not isinstance(message["refusal"], str))
            ):
                raise _GatewayFault("GW_BAD_REQUEST", 400)
            content = message.get("content")
            calls = message.get("tool_calls", [])
            if calls is None:
                calls = []
            if content is not None and not isinstance(content, str):
                raise _GatewayFault("GW_BAD_REQUEST", 400)
            if not isinstance(calls, list) or len(calls) > _MAX_TOOLS or pending_tool_ids:
                raise _GatewayFault("GW_BAD_REQUEST", 400)
            for call in calls:
                if (
                    not isinstance(call, dict)
                    or set(call) != {"id", "type", "function"}
                    or call.get("type") != "function"
                    or not isinstance(call.get("id"), str)
                    or not 1 <= len(call["id"]) <= 128
                    or call["id"] in pending_tool_ids
                ):
                    raise _GatewayFault("GW_BAD_REQUEST", 400)
                function = call.get("function")
                if (
                    not isinstance(function, dict)
                    or set(function) != {"name", "arguments"}
                    or not isinstance(function.get("name"), str)
                    or re.fullmatch(r"[A-Za-z0-9_-]{1,64}", function["name"]) is None
                    or not isinstance(function.get("arguments"), str)
                    or len(function["arguments"].encode("utf-8")) > _MAX_TOOL_ARGUMENT_BYTES
                ):
                    raise _GatewayFault("GW_BAD_REQUEST", 400)
                try:
                    arguments = json.loads(function["arguments"])
                except (ValueError, RecursionError):
                    raise _GatewayFault("GW_BAD_REQUEST", 400) from None
                if not isinstance(arguments, dict):
                    raise _GatewayFault("GW_BAD_REQUEST", 400)
                pending_tool_ids.add(call["id"])
        elif role == "tool":
            if (
                set(message) != {"role", "content", "tool_call_id"}
                or not isinstance(message.get("content"), str)
                or not isinstance(message.get("tool_call_id"), str)
                or message["tool_call_id"] not in pending_tool_ids
            ):
                raise _GatewayFault("GW_BAD_REQUEST", 400)
            pending_tool_ids.remove(message["tool_call_id"])
        else:
            raise _GatewayFault("GW_BAD_REQUEST", 400)
        result.append(message)
    if pending_tool_ids:
        raise _GatewayFault("GW_BAD_REQUEST", 400)
    return result


def _validate_chat_body(body: Any, config: _RuntimeConfig) -> tuple[dict[str, Any], int, int]:
    allowed = {
        "model",
        "messages",
        "max_tokens",
        "stream",
        "tools",
        "tool_choice",
        "temperature",
        "top_p",
        "stop",
        "thinking",
        "stream_options",
        "parallel_tool_calls",
        "n",
    }
    if not isinstance(body, dict) or set(body) - allowed:
        raise _GatewayFault("GW_BAD_REQUEST", 400)
    if body.get("model") != config.model:
        raise _GatewayFault("GW_BAD_MODEL", 400)
    messages = _validate_messages(body.get("messages"))
    output_cap = body.get("max_tokens")
    if (
        type(output_cap) is not int
        or not 1 <= output_cap <= config.budget.max_output_tokens_per_request
    ):
        raise _GatewayFault("GW_BAD_MAX_TOKENS", 400)
    stream = body.get("stream", False)
    if type(stream) is not bool:
        raise _GatewayFault("GW_BAD_REQUEST", 400)
    if "thinking" in body and body["thinking"] != {"type": "disabled"}:
        raise _GatewayFault("GW_BAD_REQUEST", 400)
    if "stream_options" in body and (
        not stream or body["stream_options"] != {"include_usage": True}
    ):
        raise _GatewayFault("GW_BAD_REQUEST", 400)
    if "parallel_tool_calls" in body and type(body["parallel_tool_calls"]) is not bool:
        raise _GatewayFault("GW_BAD_REQUEST", 400)
    if "n" in body and (type(body["n"]) is not int or body["n"] != 1):
        raise _GatewayFault("GW_BAD_REQUEST", 400)
    tools = _validate_tools(body.get("tools"))
    names = {tool["function"]["name"] for tool in tools}
    choice = body.get("tool_choice")
    if choice is not None:
        if isinstance(choice, str):
            if choice not in {"none", "auto", "required"}:
                raise _GatewayFault("GW_BAD_REQUEST", 400)
        elif (
            not isinstance(choice, dict)
            or set(choice) != {"type", "function"}
            or choice.get("type") != "function"
            or not isinstance(choice.get("function"), dict)
            or set(choice["function"]) != {"name"}
            or choice["function"].get("name") not in names
        ):
            raise _GatewayFault("GW_BAD_REQUEST", 400)
    for key, minimum, maximum in (("temperature", 0, 2), ("top_p", 0, 1)):
        value = body.get(key)
        if value is not None and (
            type(value) not in {int, float}
            or not math.isfinite(value)
            or not minimum <= value <= maximum
        ):
            raise _GatewayFault("GW_BAD_REQUEST", 400)
    stop = body.get("stop")
    if stop is not None:
        stops = [stop] if isinstance(stop, str) else stop
        if (
            not isinstance(stops, list)
            or not 1 <= len(stops) <= 4
            or any(not isinstance(item, str) for item in stops)
        ):
            raise _GatewayFault("GW_BAD_REQUEST", 400)
    projected = dict(body)
    projected["thinking"] = {"type": "disabled"}
    projected["messages"] = messages
    if tools:
        projected["tools"] = tools
    else:
        projected.pop("tools", None)
    if stream:
        # Usage is required for settlement; callers cannot disable this protocol field.
        projected["stream_options"] = {"include_usage": True}
    return projected, output_cap, len(tools)


def _usage(value: Any) -> tuple[int, int]:
    if (
        not isinstance(value, dict)
        or type(value.get("prompt_tokens")) is not int
        or type(value.get("completion_tokens")) is not int
        or type(value.get("total_tokens")) is not int
    ):
        raise _GatewayFault("GW_USAGE_INVALID", 502)
    input_tokens = value["prompt_tokens"]
    output_tokens = value["completion_tokens"]
    if (
        input_tokens < 0
        or output_tokens < 0
        or value["total_tokens"] != input_tokens + output_tokens
    ):
        raise _GatewayFault("GW_USAGE_INVALID", 502)
    return input_tokens, output_tokens


def _validate_completion(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict) or "error" in value:
        raise _GatewayFault("GW_UPSTREAM_PROTOCOL", 502)
    choices = value.get("choices")
    if not isinstance(choices, list) or len(choices) != 1 or not isinstance(choices[0], dict):
        raise _GatewayFault("GW_UPSTREAM_PROTOCOL", 502)
    choice = choices[0]
    message = choice.get("message")
    if (
        choice.get("finish_reason") not in ("stop", "length", "tool_calls", "content_filter")
        or type(choice.get("index")) is not int
        or choice["index"] != 0
        or not isinstance(message, dict)
        or set(message)
        - {"role", "content", "tool_calls", "refusal", "reasoning_content", "annotations"}
        or message.get("annotations") not in (None, [])
        or message.get("reasoning_content") not in (None, "")
        or message.get("role") != "assistant"
        or (message.get("content") is not None and not isinstance(message.get("content"), str))
        or (message.get("refusal") is not None and not isinstance(message.get("refusal"), str))
    ):
        raise _GatewayFault("GW_UPSTREAM_PROTOCOL", 502)
    calls = message.get("tool_calls", [])
    if calls is None:
        calls = []
    if not isinstance(calls, list) or len(calls) > _MAX_TOOLS:
        raise _GatewayFault("GW_UPSTREAM_PROTOCOL", 502)
    for call in calls:
        if (
            not isinstance(call, dict)
            or set(call) != {"id", "type", "function"}
            or call.get("type") != "function"
            or not isinstance(call.get("id"), str)
            or not isinstance(call.get("function"), dict)
            or set(call["function"]) != {"name", "arguments"}
            or not isinstance(call["function"].get("name"), str)
            or re.fullmatch(r"[A-Za-z0-9_-]{1,64}", call["function"]["name"]) is None
            or not isinstance(call["function"].get("arguments"), str)
            or len(call["function"]["arguments"].encode("utf-8")) > _MAX_TOOL_ARGUMENT_BYTES
        ):
            raise _GatewayFault("GW_UPSTREAM_PROTOCOL", 502)
        try:
            arguments = json.loads(call["function"]["arguments"])
        except (ValueError, RecursionError):
            raise _GatewayFault("GW_UPSTREAM_PROTOCOL", 502) from None
        if not isinstance(arguments, dict):
            raise _GatewayFault("GW_UPSTREAM_PROTOCOL", 502)
    return value


def _scrub(value: Any, secret: str) -> Any:
    if isinstance(value, str):
        return value.replace(secret, "[redacted]") if secret else value
    if isinstance(value, list):
        return [_scrub(item, secret) for item in value]
    if isinstance(value, dict):
        return {
            key.replace(secret, "[redacted]") if secret else key: _scrub(item, secret)
            for key, item in value.items()
        }
    return value


class _IncrementalTextRedactor:
    """Bounded-delay secret filter over one streamed text/tool-argument field."""

    def __init__(self, secret: str) -> None:
        self._secret = secret
        self._pending = ""

    def feed(self, text: str) -> str:
        if not self._secret:
            return text
        combined = (self._pending + text).replace(self._secret, "[redacted]")
        keep = len(self._secret) - 1
        if len(combined) <= keep:
            self._pending = combined
            return ""
        safe = combined[:-keep] if keep else combined
        self._pending = combined[-keep:] if keep else ""
        return safe

    def finish(self) -> str:
        tail = self._pending.replace(self._secret, "[redacted]")
        self._pending = ""
        return tail


def _incremental_scrub_event(
    event: dict[str, Any],
    secret: str,
    filters: dict[tuple[str, int | None, str], _IncrementalTextRedactor],
) -> dict[str, Any]:
    """Redact full secrets even when an upstream splits one across SSE deltas."""
    safe_event = cast(dict[str, Any], _scrub(event, secret))
    choices = safe_event.get("choices")
    if not isinstance(choices, list):
        return safe_event
    for choice in choices:
        if not isinstance(choice, dict) or not isinstance(choice.get("delta"), dict):
            continue
        delta = choice["delta"]
        for field_name in ("content", "refusal"):
            value = delta.get(field_name)
            if isinstance(value, str):
                key = (field_name, None, field_name)
                redactor = filters.setdefault(key, _IncrementalTextRedactor(secret))
                delta[field_name] = redactor.feed(value)
        calls = delta.get("tool_calls", [])
        if isinstance(calls, list):
            for call in calls:
                if not isinstance(call, dict) or type(call.get("index")) is not int:
                    continue
                index = call["index"]
                for field_name in ("id",):
                    value = call.get(field_name)
                    if isinstance(value, str):
                        key = ("tool", index, field_name)
                        redactor = filters.setdefault(key, _IncrementalTextRedactor(secret))
                        call[field_name] = redactor.feed(value)
                function = call.get("function")
                if isinstance(function, dict):
                    for field_name in ("name", "arguments"):
                        value = function.get(field_name)
                        if isinstance(value, str):
                            key = ("tool", index, field_name)
                            redactor = filters.setdefault(key, _IncrementalTextRedactor(secret))
                            function[field_name] = redactor.feed(value)
        if choice.get("finish_reason") is not None:
            calls_out = delta.setdefault("tool_calls", [])
            for (kind, index, field_name), redactor in filters.items():
                tail = redactor.finish()
                if not tail:
                    continue
                if kind in {"content", "refusal"}:
                    current = delta.get(field_name, "")
                    delta[field_name] = (current if isinstance(current, str) else "") + tail
                    continue
                if index is None:
                    continue
                call_out = next(
                    (
                        item
                        for item in calls_out
                        if isinstance(item, dict) and item.get("index") == index
                    ),
                    None,
                )
                if call_out is None:
                    call_out = {"index": index}
                    calls_out.append(call_out)
                if field_name == "id":
                    call_out["id"] = call_out.get("id", "") + tail
                else:
                    function_out = call_out.setdefault("function", {})
                    current = function_out.get(field_name, "")
                    function_out[field_name] = (current if isinstance(current, str) else "") + tail
            if not calls_out:
                delta.pop("tool_calls", None)
    return safe_event


class _WireSecretFilter:
    """Bounded-delay SSE byte filter to prevent an API key crossing event boundaries."""

    def __init__(self, secret: str) -> None:
        self._secret = secret.encode("ascii")
        self._pending = bytearray()

    def feed(self, chunk: bytes) -> bytes:
        if not self._secret:
            return chunk
        self._pending.extend(chunk)
        combined = bytes(self._pending).replace(self._secret, b"[redacted]")
        safe_length = max(0, len(combined) - len(self._secret))
        if safe_length == 0:
            self._pending = bytearray(combined)
            return b""
        safe = combined[:safe_length]
        self._pending = bytearray(combined[safe_length:])
        return safe

    def finish(self) -> bytes:
        tail = bytes(self._pending)
        self._pending.clear()
        return tail.replace(self._secret, b"[redacted]") if self._secret else tail


def _error(code: str, status: int) -> JSONResponse:
    return JSONResponse({"error": {"code": code}}, status_code=status)


def _sse_error(code: str) -> bytes:
    encoded = json.dumps({"error": {"code": code}}, separators=(",", ":")).encode()
    return b"data: " + encoded + b"\n\n"


def _authorized_trial(request: Request, config: _RuntimeConfig) -> str:
    forbidden_headers = {
        "forwarded",
        "via",
        "proxy-authorization",
        "proxy-connection",
        "proxy",
        "x-api-key",
        "x-http-method-override",
        "x-real-ip",
        "x-original-uri",
        "x-original-url",
        "x-rewrite-url",
    }
    for name in request.headers.keys():
        lower = name.lower()
        if (
            lower in forbidden_headers
            or lower.startswith("x-forwarded-")
            or lower.startswith("x-envoy-")
            or lower.startswith("proxy-")
        ):
            raise _GatewayFault("GW_PROXY_HEADER_REJECTED", 400)
    if request.url.query:
        raise _GatewayFault("GW_QUERY_REJECTED", 400)
    auth = request.headers.get("authorization", "")
    parts = auth.split(" ", 1)
    if len(parts) != 2 or parts[0].lower() != "bearer" or not parts[1]:
        raise _GatewayFault("GW_BAD_TOKEN", 401)
    digest = hashlib.sha256(parts[1].encode("utf-8", errors="ignore")).digest()
    found: str | None = None
    for known_hash, trial_id in config.token_hashes.items():
        if hmac.compare_digest(digest, known_hash):
            found = trial_id
    if found is None:
        raise _GatewayFault("GW_BAD_TOKEN", 401)
    return found


def _ready(request: Request) -> tuple[_RuntimeConfig, _Ledger, httpx.AsyncClient]:
    config = getattr(request.app.state, "gateway_config", None)
    ledger = getattr(request.app.state, "gateway_ledger", None)
    client = getattr(request.app.state, "gateway_client", None)
    if config is None or ledger is None or client is None:
        raise _GatewayFault("GW_GATEWAY_DISABLED", 503)
    return config, ledger, client


def _check_json_content_type(request: Request) -> None:
    content_type = request.headers.get("content-type", "").split(";", 1)[0].strip().lower()
    if content_type != "application/json":
        raise _GatewayFault("GW_BAD_CONTENT_TYPE", 415)


async def _read_request_limited(request: Request, limit: int) -> bytes:
    result = bytearray()
    async for chunk in request.stream():
        if len(result) + len(chunk) > limit:
            raise _GatewayFault("GW_REQUEST_SIZE", 413)
        result.extend(chunk)
    return bytes(result)


async def _read_limited(response: httpx.Response, limit: int) -> bytes:
    result = bytearray()
    async for chunk in response.aiter_raw():
        if len(result) + len(chunk) > limit:
            raise _GatewayFault("GW_UPSTREAM_SIZE", 502)
        result.extend(chunk)
    return bytes(result)


def _check_upstream_headers(response: httpx.Response, expected_type: str) -> None:
    content_type = response.headers.get("content-type", "").split(";", 1)[0].strip().lower()
    encoding = response.headers.get("content-encoding", "identity").strip().lower()
    if content_type != expected_type or encoding not in {"", "identity"}:
        raise _GatewayFault("GW_UPSTREAM_PROTOCOL", 502)


async def _send_models(
    request: Request,
    trial_id: str,
    config: _RuntimeConfig,
    ledger: _Ledger,
    client: httpx.AsyncClient,
) -> Response:
    reservation: str | None = None
    response: httpx.Response | None = None
    try:
        reservation = ledger.reserve(trial_id, 0, 0, 0)
        outbound = client.build_request(
            "GET",
            "/v1/models",
            headers={
                "Authorization": "Bearer " + config.api_key,
                "Accept": "application/json",
                "Accept-Encoding": "identity",
            },
        )
        response = await client.send(outbound, stream=True, follow_redirects=False)
        if 300 <= response.status_code < 400:
            raise _GatewayFault("GW_UPSTREAM_REDIRECT", 502)
        if response.status_code != 200:
            raise _GatewayFault("GW_UPSTREAM_FAILURE", 502)
        _check_upstream_headers(response, "application/json")
        raw = await _read_limited(response, 256 * 1024)
        body = _strict_json(raw, error_code="GW_UPSTREAM_PROTOCOL", status=502)
        if not isinstance(body, dict) or not isinstance(body.get("data"), list):
            raise _GatewayFault("GW_UPSTREAM_PROTOCOL", 502)
        if not ledger.settle(reservation, 0, 0, 0):
            return _error("GW_USAGE_OVERRUN", 502)
        return JSONResponse(_scrub(body, config.api_key), status_code=200)
    except asyncio.CancelledError:
        if reservation is not None:
            _safe_mark_unknown(ledger, reservation)
        raise
    except _GatewayFault as fault:
        if reservation is not None:
            _safe_mark_unknown(ledger, reservation)
        return _error(fault.code, fault.status)
    except (httpx.HTTPError, OSError, sqlite3.Error):
        if reservation is not None:
            _safe_mark_unknown(ledger, reservation)
        return _error("GW_UPSTREAM_FAILURE", 502)
    except Exception:
        if reservation is not None:
            _safe_mark_unknown(ledger, reservation)
        return _error("GW_UPSTREAM_FAILURE", 502)
    finally:
        if response is not None:
            await _safe_close_response(response)


def _safe_mark_unknown(ledger: _Ledger, reservation: str) -> None:
    try:
        ledger.mark_unknown(reservation)
    except sqlite3.Error:
        # The reservation remains durable as reserved, which is the conservative state.
        pass


async def _safe_close_response(response: httpx.Response) -> None:
    try:
        await response.aclose()
    except Exception:
        # Closing is best-effort; never let transport error text escape through ASGI.
        pass


async def _send_chat_nonstream(
    config: _RuntimeConfig,
    ledger: _Ledger,
    client: httpx.AsyncClient,
    body: dict[str, Any],
    reservation: str,
) -> Response:
    response: httpx.Response | None = None
    settled = False
    try:
        outbound = client.build_request(
            "POST",
            "/v1/chat/completions",
            content=_json_bytes(body),
            headers={
                "Authorization": "Bearer " + config.api_key,
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Accept-Encoding": "identity",
            },
        )
        response = await client.send(outbound, stream=True, follow_redirects=False)
        if 300 <= response.status_code < 400:
            raise _GatewayFault("GW_UPSTREAM_REDIRECT", 502)
        if response.status_code != 200:
            raise _GatewayFault("GW_UPSTREAM_FAILURE", 502)
        _check_upstream_headers(response, "application/json")
        raw = await _read_limited(response, _MAX_RESPONSE_BYTES)
        result = _strict_json(raw, error_code="GW_UPSTREAM_PROTOCOL", status=502)
        result = _validate_completion(result)
        if "usage" not in result or result["usage"] is None:
            raise _GatewayFault("GW_USAGE_UNKNOWN", 502)
        input_tokens, output_tokens = _usage(result["usage"])
        cost = config.policy.cost_microcny(input_tokens, output_tokens)
        settled = ledger.settle(reservation, input_tokens, output_tokens, cost)
        if not settled:
            raise _GatewayFault("GW_USAGE_OVERRUN", 502)
        return JSONResponse(_scrub(result, config.api_key), status_code=200)
    except asyncio.CancelledError:
        _safe_mark_unknown(ledger, reservation)
        raise
    except _GatewayFault as fault:
        if not settled:
            _safe_mark_unknown(ledger, reservation)
        return _error(fault.code, fault.status)
    except (httpx.HTTPError, OSError, sqlite3.Error):
        if not settled:
            _safe_mark_unknown(ledger, reservation)
        return _error("GW_UPSTREAM_FAILURE", 502)
    except Exception:
        if not settled:
            _safe_mark_unknown(ledger, reservation)
        return _error("GW_UPSTREAM_FAILURE", 502)
    finally:
        if response is not None:
            await _safe_close_response(response)


def _drop_repeated_tool_metadata(
    call: dict[str, Any],
    function: dict[str, Any],
    buffer: dict[str, str],
    key: str,
    value: str,
) -> bool:
    """Report and strip a delta that re-sends the complete id/name buffered for its index.

    Some upstreams repeat the full id/name on every frame of one tool call instead of
    sending them only on the first frame. That exact repeat is idempotent: the buffered
    values stay unchanged and the repeated fields are removed from the forwarded delta so
    downstream first-frame reassembly and the incremental redactor see each value once.
    Dissimilar fragments always append (tool name and id stay splittable), and arguments
    are a strict append-only character stream that is never deduplicated.
    """
    if key == "arguments" or buffer[key] != value:
        return False
    (call if key == "id" else function).pop(key, None)
    return True


def _validate_stream_event(
    event: Any,
    finish_seen: bool,
    tool_buffers: dict[int, dict[str, str]],
) -> tuple[dict[str, Any], bool]:
    if not isinstance(event, dict) or "error" in event:
        raise _GatewayFault("GW_UPSTREAM_PROTOCOL", 502)
    choices = event.get("choices")
    if choices == []:
        if not finish_seen:
            raise _GatewayFault("GW_UPSTREAM_PROTOCOL", 502)
        return event, finish_seen
    if not isinstance(choices, list) or len(choices) != 1 or not isinstance(choices[0], dict):
        raise _GatewayFault("GW_UPSTREAM_PROTOCOL", 502)
    choice = choices[0]
    delta = choice.get("delta", {})
    if (
        type(choice.get("index")) is not int
        or choice["index"] != 0
        or not isinstance(delta, dict)
        or finish_seen
        or set(delta) - {"role", "content", "tool_calls", "refusal", "reasoning_content"}
        or delta.get("reasoning_content") not in (None, "")
        or delta.get("role", "assistant") != "assistant"
        or (delta.get("content") is not None and not isinstance(delta.get("content"), str))
        or (delta.get("refusal") is not None and not isinstance(delta.get("refusal"), str))
    ):
        raise _GatewayFault("GW_UPSTREAM_PROTOCOL", 502)
    calls = delta.get("tool_calls", [])
    if not isinstance(calls, list):
        raise _GatewayFault("GW_UPSTREAM_PROTOCOL", 502)
    for call in calls:
        if (
            not isinstance(call, dict)
            or set(call) - {"index", "id", "type", "function"}
            or type(call.get("index")) is not int
            or not 0 <= call["index"] < _MAX_TOOLS
            or call.get("type", "function") != "function"
        ):
            raise _GatewayFault("GW_UPSTREAM_PROTOCOL", 502)
        index = call["index"]
        buffer = tool_buffers.setdefault(index, {"id": "", "name": "", "arguments": ""})
        identifier = call.get("id")
        function = call.get("function", {})
        if not isinstance(function, dict) or set(function) - {"name", "arguments"}:
            raise _GatewayFault("GW_UPSTREAM_PROTOCOL", 502)
        name = function.get("name")
        arguments = function.get("arguments")
        for key, value in (("id", identifier), ("name", name), ("arguments", arguments)):
            if value is None:
                continue
            if not isinstance(value, str):
                raise _GatewayFault("GW_UPSTREAM_PROTOCOL", 502)
            if _drop_repeated_tool_metadata(call, function, buffer, key, value):
                continue
            buffer[key] += value
        if (
            len(buffer["id"]) > 128
            or len(buffer["name"]) > 64
            or len(buffer["arguments"].encode("utf-8")) > _MAX_TOOL_ARGUMENT_BYTES
        ):
            raise _GatewayFault("GW_UPSTREAM_SIZE", 502)
    reason = choice.get("finish_reason")
    if reason is not None:
        if reason not in {"stop", "tool_calls", "length", "content_filter"}:
            raise _GatewayFault("GW_UPSTREAM_PROTOCOL", 502)
        finish_seen = True
    return event, finish_seen


def _finish_tool_calls(buffers: dict[int, dict[str, str]]) -> None:
    if sorted(buffers) != list(range(len(buffers))):
        raise _GatewayFault("GW_UPSTREAM_PROTOCOL", 502)
    ids: set[str] = set()
    for call in buffers.values():
        if (
            not call["id"]
            or call["id"] in ids
            or re.fullmatch(r"[A-Za-z0-9_-]{1,64}", call["name"]) is None
        ):
            raise _GatewayFault("GW_UPSTREAM_PROTOCOL", 502)
        try:
            arguments = json.loads(call["arguments"])
        except (ValueError, RecursionError):
            raise _GatewayFault("GW_UPSTREAM_PROTOCOL", 502) from None
        if not isinstance(arguments, dict):
            raise _GatewayFault("GW_UPSTREAM_PROTOCOL", 502)
        ids.add(call["id"])


async def _stream_body(
    response: httpx.Response,
    config: _RuntimeConfig,
    ledger: _Ledger,
    reservation: str,
) -> AsyncIterator[bytes]:
    reader = _SSEReader(_MAX_RESPONSE_BYTES, _MAX_SSE_FRAME_BYTES, _MAX_SSE_EVENTS)
    secret_filter = _WireSecretFilter(config.api_key)
    usage_seen: tuple[int, int] | None = None
    finish_seen = False
    done_seen = False
    tool_buffers: dict[int, dict[str, str]] = {}
    text_filters: dict[tuple[str, int | None, str], _IncrementalTextRedactor] = {}
    settled = False
    try:
        async for frame in reader.frames(response):
            payload, passthrough = _parse_sse_frame(frame)
            if passthrough:
                safe = secret_filter.feed(passthrough)
                if safe:
                    yield safe
                continue
            if payload is None:
                continue
            if payload.strip() == "[DONE]":
                done_seen = True
                if not finish_seen:
                    raise _GatewayFault("GW_UPSTREAM_PROTOCOL", 502)
                _finish_tool_calls(tool_buffers)
                if usage_seen is None:
                    raise _GatewayFault("GW_USAGE_UNKNOWN", 502)
                input_tokens, output_tokens = usage_seen
                cost = config.policy.cost_microcny(input_tokens, output_tokens)
                settled = ledger.settle(reservation, input_tokens, output_tokens, cost)
                if not settled:
                    raise _GatewayFault("GW_USAGE_OVERRUN", 502)
                last = secret_filter.feed(b"data: [DONE]\n\n")
                if last:
                    yield last
                tail = secret_filter.finish()
                if tail:
                    yield tail
                return
            try:
                event = json.loads(payload, parse_constant=_reject_json_constant)
            except (ValueError, RecursionError):
                raise _GatewayFault("GW_UPSTREAM_PROTOCOL", 502) from None
            if not isinstance(event, dict):
                raise _GatewayFault("GW_UPSTREAM_PROTOCOL", 502)
            usage_value = event.get("usage")
            if usage_value is not None:
                current_usage = _usage(usage_value)
                if usage_seen is not None and usage_seen != current_usage:
                    raise _GatewayFault("GW_USAGE_INVALID", 502)
                usage_seen = current_usage
            elif "usage" in event and usage_value is not None:
                raise _GatewayFault("GW_USAGE_INVALID", 502)
            _, finish_seen = _validate_stream_event(event, finish_seen, tool_buffers)
            safe_event = _incremental_scrub_event(event, config.api_key, text_filters)
            outgoing = b"data: " + _json_bytes(safe_event) + b"\n\n"
            safe = secret_filter.feed(outgoing)
            if safe:
                yield safe
        if not done_seen:
            raise _GatewayFault("GW_UPSTREAM_INCOMPLETE", 502)
    except asyncio.CancelledError:
        _safe_mark_unknown(ledger, reservation)
        raise
    except _GatewayFault as fault:
        if not settled:
            _safe_mark_unknown(ledger, reservation)
        safe = secret_filter.feed(_sse_error(fault.code))
        if safe:
            yield safe
        tail = secret_filter.finish()
        if tail:
            yield tail
    except (httpx.HTTPError, OSError, sqlite3.Error):
        if not settled:
            _safe_mark_unknown(ledger, reservation)
        safe = secret_filter.feed(_sse_error("GW_UPSTREAM_FAILURE"))
        if safe:
            yield safe
        tail = secret_filter.finish()
        if tail:
            yield tail
    except Exception:
        if not settled:
            _safe_mark_unknown(ledger, reservation)
        safe = secret_filter.feed(_sse_error("GW_UPSTREAM_FAILURE"))
        if safe:
            yield safe
        tail = secret_filter.finish()
        if tail:
            yield tail
    finally:
        await _safe_close_response(response)


async def _send_chat_stream(
    config: _RuntimeConfig,
    ledger: _Ledger,
    client: httpx.AsyncClient,
    body: dict[str, Any],
    reservation: str,
) -> Response:
    response: httpx.Response | None = None
    try:
        outbound = client.build_request(
            "POST",
            "/v1/chat/completions",
            content=_json_bytes(body),
            headers={
                "Authorization": "Bearer " + config.api_key,
                "Content-Type": "application/json",
                "Accept": "text/event-stream",
                "Accept-Encoding": "identity",
            },
        )
        response = await client.send(outbound, stream=True, follow_redirects=False)
        if 300 <= response.status_code < 400:
            raise _GatewayFault("GW_UPSTREAM_REDIRECT", 502)
        if response.status_code != 200:
            raise _GatewayFault("GW_UPSTREAM_FAILURE", 502)
        _check_upstream_headers(response, "text/event-stream")
        streaming = StreamingResponse(
            _stream_body(response, config, ledger, reservation),
            status_code=200,
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )
        # Ownership transfers to the generator once StreamingResponse has been constructed.
        response = None
        return streaming
    except asyncio.CancelledError:
        _safe_mark_unknown(ledger, reservation)
        raise
    except _GatewayFault as fault:
        _safe_mark_unknown(ledger, reservation)
        return _error(fault.code, fault.status)
    except (httpx.HTTPError, OSError, sqlite3.Error):
        _safe_mark_unknown(ledger, reservation)
        return _error("GW_UPSTREAM_FAILURE", 502)
    except Exception:
        _safe_mark_unknown(ledger, reservation)
        return _error("GW_UPSTREAM_FAILURE", 502)
    finally:
        if response is not None:
            await _safe_close_response(response)


def create_gateway(
    config: GatewayConfig | None = None,
    *,
    transport: httpx.AsyncBaseTransport | None = None,
) -> FastAPI:
    """Create a deliberately disabled-by-default ASGI gateway.

    Pass explicit in-memory credentials, exact approved origin/model, price policy, budget,
    trial tokens and durable ledger path to enable it. Tests may inject ``MockTransport``;
    production code owns no default network target or credential source.
    """
    runtime = _runtime_config(config) if config is not None else None

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        app.state.gateway_config = None
        app.state.gateway_ledger = None
        app.state.gateway_client = None
        if runtime is not None:
            ledger = _Ledger(runtime)
            client = httpx.AsyncClient(
                base_url=runtime.origin,
                transport=transport,
                trust_env=False,
                follow_redirects=False,
                timeout=httpx.Timeout(connect=3, read=90, write=10, pool=3),
                limits=httpx.Limits(max_connections=8, max_keepalive_connections=4),
            )
            app.state.gateway_config = runtime
            app.state.gateway_ledger = ledger
            app.state.gateway_client = client
        try:
            yield
        finally:
            client = app.state.gateway_client
            ledger = app.state.gateway_ledger
            app.state.gateway_client = None
            app.state.gateway_ledger = None
            app.state.gateway_config = None
            if client is not None:
                await client.aclose()
            if ledger is not None:
                ledger.close()

    app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)
    app.router.redirect_slashes = False

    @app.middleware("http")
    async def enforce_gateway_methods(request: Request, call_next: Any) -> Response:
        expected = {
            "/models": "GET",
            "/v1/models": "GET",
            "/chat/completions": "POST",
            "/v1/chat/completions": "POST",
        }.get(request.url.path)
        if expected is not None and request.method != expected:
            return _error("GW_BAD_METHOD", 405)
        return cast(Response, await call_next(request))

    @app.get("/models")
    @app.get("/v1/models")
    async def models(request: Request) -> Response:
        try:
            runtime_config, ledger, client = _ready(request)
            trial_id = _authorized_trial(request, runtime_config)
            return await _send_models(request, trial_id, runtime_config, ledger, client)
        except _GatewayFault as fault:
            return _error(fault.code, fault.status)
        except (sqlite3.Error, ValueError, OSError):
            return _error("GW_LEDGER_UNAVAILABLE", 503)
        except Exception:
            return _error("GW_GATEWAY_FAILURE", 500)

    @app.post("/chat/completions")
    @app.post("/v1/chat/completions")
    async def chat_completions(request: Request) -> Response:
        try:
            runtime_config, ledger, client = _ready(request)
            trial_id = _authorized_trial(request, runtime_config)
            _check_json_content_type(request)
            content_length = request.headers.get("content-length")
            if (
                content_length is not None
                and content_length.isdecimal()
                and int(content_length) > _MAX_REQUEST_BYTES
            ):
                raise _GatewayFault("GW_REQUEST_SIZE", 413)
            raw_body = await _read_request_limited(request, _MAX_REQUEST_BYTES)
            body_value = _strict_json(raw_body)
            body, output_cap, tool_count = _validate_chat_body(body_value, runtime_config)
            message_count = len(body["messages"])
            wire_body = _json_bytes(body)
            if len(wire_body) > _MAX_REQUEST_BYTES:
                raise _GatewayFault("GW_REQUEST_SIZE", 413)
            input_upper = runtime_config.policy.input_upper_bound(
                wire_body, message_count, tool_count
            )
            cost_upper = runtime_config.policy.cost_microcny(input_upper, output_cap)
            reservation = ledger.reserve(trial_id, input_upper, output_cap, cost_upper)
            if body.get("stream") is True:
                return await _send_chat_stream(runtime_config, ledger, client, body, reservation)
            return await _send_chat_nonstream(runtime_config, ledger, client, body, reservation)
        except asyncio.CancelledError:
            raise
        except _GatewayFault as fault:
            return _error(fault.code, fault.status)
        except (sqlite3.Error, ValueError, OSError):
            return _error("GW_LEDGER_UNAVAILABLE", 503)
        except Exception:
            return _error("GW_GATEWAY_FAILURE", 500)

    @app.api_route(
        "/{unknown_path:path}",
        methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"],
    )
    async def unsupported_path(_request: Request, unknown_path: str) -> Response:
        del unknown_path
        return _error("GW_BAD_PATH", 404)

    return app
