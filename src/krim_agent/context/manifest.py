"""Metadata-only manifest for the exact serialized model request boundary."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Literal, cast

from pydantic import JsonValue

from krim_agent.contracts import Message, encoded

Category = Literal[
    "system", "tools", "history", "tool_result", "summary", "unknown", "files", "memory", "skill"
]
_CATEGORIES = frozenset(
    {"system", "tools", "history", "tool_result", "summary", "unknown", "files", "memory", "skill"}
)
_MAX_ITEMS = 128
_MAX_LABEL_CHARS = 64
_MAX_CALL_ID_CHARS = 128
_MAX_SOURCE_REFS = 2
_MAX_TOTAL_SOURCE_REFS = 16
_MAX_REF_TEXT_CHARS = 64
_MAX_MANIFEST_BYTES = 30720  # leave room for up to two actual compaction changes
_REF_FIELDS = frozenset(
    {"turn_id", "run_id", "last_seq", "fingerprint", "terminal_seq", "transcript_fingerprint"}
)


@dataclass(frozen=True)
class ItemMetadata:
    """Trusted non-body annotations for one request message."""

    category: Category | None = None
    label: str | None = None
    source_refs: tuple[Mapping[str, JsonValue], ...] = ()


def _bounded_label(label: str) -> str:
    if len(label) <= _MAX_LABEL_CHARS:
        return label
    return label[: _MAX_LABEL_CHARS - 1] + "…"


def _source_refs(
    refs: Sequence[Mapping[str, JsonValue]],
) -> tuple[list[dict[str, JsonValue]], bool]:
    result: list[dict[str, JsonValue]] = []
    truncated = len(refs) > _MAX_SOURCE_REFS
    for ref in refs[:_MAX_SOURCE_REFS]:
        safe: dict[str, JsonValue] = {}
        for key, value in ref.items():
            if key not in _REF_FIELDS:
                truncated = True
                continue
            if key in {"last_seq", "terminal_seq"}:
                if type(value) is int:
                    safe[key] = value
                else:
                    truncated = True
            elif isinstance(value, str):
                safe[key] = _bounded_label(value[:_MAX_REF_TEXT_CHARS])
                if len(value) > _MAX_REF_TEXT_CHARS:
                    truncated = True
            elif value is None and key == "run_id":
                safe[key] = None
            else:
                truncated = True
        if safe:
            result.append(safe)
        else:
            truncated = True
    return result, truncated


def _tool_definition_name(definition: Mapping[str, JsonValue]) -> str | None:
    name = definition.get("name")
    if isinstance(name, str):
        return name
    function = definition.get("function")
    if isinstance(function, dict) and isinstance(function.get("name"), str):
        return cast(str, function["name"])
    return None


def _message_category(role: str) -> str:
    return {
        "system": "system",
        "tool": "tool_result",
        "user": "history",
        "assistant": "history",
    }.get(role, "unknown")


def _component_item(
    *,
    key: str,
    category: str,
    label: str,
    value: object,
    message_index: int | None = None,
    tool_index: int | None = None,
    call_id: str | None = None,
    source_refs: Sequence[Mapping[str, JsonValue]] = (),
) -> dict[str, JsonValue]:
    raw = encoded(value).encode("utf-8")
    item: dict[str, JsonValue] = {
        "key": key,
        "category": category if category in _CATEGORIES else "unknown",
        "label": _bounded_label(label),
        "content_hash": hashlib.sha256(raw).hexdigest(),
        "input_bytes": len(raw),
        "estimated_tokens": (len(raw) + 3) // 4,
    }
    if message_index is not None:
        item["message_index"] = message_index
    if tool_index is not None:
        item["tool_index"] = tool_index
    if call_id is not None:
        item["call_id"] = call_id[:_MAX_CALL_ID_CHARS]
        # Message.call_id is the provider protocol ID, not Krim's invocation ID.
        item["provider_call_id"] = call_id[:_MAX_CALL_ID_CHARS]
        if len(call_id) > _MAX_CALL_ID_CHARS:
            item["call_id_hash"] = hashlib.sha256(call_id.encode("utf-8")).hexdigest()
            item["call_id_truncated"] = True
    if source_refs:
        safe_refs, truncated = _source_refs(source_refs)
        if safe_refs:
            item["source_refs"] = cast(JsonValue, safe_refs)
        if truncated:
            item["source_refs_truncated"] = True
    return item


def build_context_manifest(
    messages: Sequence[Message],
    tools: Sequence[dict[str, JsonValue]],
    *,
    message_metadata: Sequence[ItemMetadata | None] = (),
    model_context_window: int | None = None,
    reserved_output_tokens: int | None = None,
    input_budget_bytes: int | None = None,
) -> dict[str, JsonValue]:
    """Describe canonical request bytes without retaining request bodies.

    The provider may translate this application representation to its wire protocol;
    approx-utf8/1 is not a provider tokenizer or a measured native token window.
    """
    message_values = [message.model_dump(mode="json") for message in messages]
    tool_values = list(tools)
    request_value = {"messages": message_values, "tools": tool_values}
    total_bytes = len(encoded(request_value).encode("utf-8"))
    tool_calls_by_id: dict[str, str] = {}
    for message in messages:
        if message.role != "assistant":
            continue
        for call in message.tool_calls:
            call_id = call.get("id")
            call_name = call.get("name")
            # Historical call metadata remains valid when the current budget removes
            # all tool definitions from the next request.
            if isinstance(call_id, str) and isinstance(call_name, str):
                tool_calls_by_id[call_id] = call_name

    candidates: list[dict[str, JsonValue]] = []
    for index, (message, value) in enumerate(zip(messages, message_values, strict=True)):
        metadata = message_metadata[index] if index < len(message_metadata) else None
        role = message.role
        category = metadata.category if metadata and metadata.category else _message_category(role)
        if role == "tool":
            tool_name = tool_calls_by_id.get(message.call_id or "")
            label = f"Tool result: {tool_name}" if tool_name else "Tool result"
        elif metadata is not None and metadata.label:
            label = metadata.label
        else:
            label = {
                "system": "System message",
                "user": "User message",
                "assistant": "Assistant message",
            }.get(role, "Message")
        refs = metadata.source_refs if metadata else ()
        candidates.append(
            _component_item(
                key=f"message:{index}:{role}",
                category=category,
                label=label,
                value=value,
                message_index=index,
                call_id=message.call_id if role == "tool" else None,
                source_refs=refs,
            )
        )

    for index, value in enumerate(tool_values):
        name = _tool_definition_name(value)
        candidates.append(
            _component_item(
                key=f"tool:{index}",
                category="tools",
                label=f"Tool: {name}" if name else f"Tool definition {index}",
                value=value,
                tool_index=index,
            )
        )

    source_ref_count = 0
    source_refs_truncated = any(item.get("source_refs_truncated") is True for item in candidates)
    for item in candidates:
        item_refs = item.get("source_refs")
        if not isinstance(item_refs, list):
            continue
        remaining_refs = max(0, _MAX_TOTAL_SOURCE_REFS - source_ref_count)
        if len(item_refs) > remaining_refs:
            source_refs_truncated = True
            if remaining_refs:
                item["source_refs"] = item_refs[:remaining_refs]
            else:
                item.pop("source_refs", None)
            item["source_refs_truncated"] = True
        source_ref_count += min(len(item_refs), remaining_refs)

    attributed_bytes = sum(cast(int, item["input_bytes"]) for item in candidates)
    wrapper_bytes = total_bytes - attributed_bytes
    # JSON message/tool arrays and the top-level request object are protocol overhead,
    # not content. Keep their bytes in the unknown area so category totals reconcile.
    wrapper_raw = encoded(
        {"input_bytes": wrapper_bytes, "key": "unknown:request-envelope"}
    ).encode()
    candidates.append(
        {
            "key": "unknown:request-envelope",
            "category": "unknown",
            "label": "Canonical JSON wrappers",
            "content_hash": hashlib.sha256(wrapper_raw).hexdigest(),
            "input_bytes": wrapper_bytes,
            "estimated_tokens": (wrapper_bytes + 3) // 4,
        }
    )

    initial_keep_count = min(len(candidates), _MAX_ITEMS)
    if len(candidates) > _MAX_ITEMS:
        initial_keep_count = _MAX_ITEMS - 1
    keep_count = initial_keep_count
    while True:
        omitted = candidates[keep_count:]
        visible_items = candidates[:keep_count]
        if omitted:
            omitted_bytes = sum(cast(int, item["input_bytes"]) for item in omitted)
            aggregate_input = [
                {
                    "key": item["key"],
                    "content_hash": item["content_hash"],
                    "input_bytes": item["input_bytes"],
                }
                for item in omitted
            ]
            aggregate_raw = encoded(aggregate_input).encode()
            visible_items = [
                *visible_items,
                {
                    "key": "unknown:truncated",
                    "category": "unknown",
                    "label": "Omitted context items (truncated)",
                    "content_hash": hashlib.sha256(aggregate_raw).hexdigest(),
                    "input_bytes": omitted_bytes,
                    "estimated_tokens": (omitted_bytes + 3) // 4,
                },
            ]
        # Partition the same byte-based estimate, rather than independently rounding
        # every fragment up and making the displayed parts exceed the request total.
        estimated_total = (total_bytes + 3) // 4
        for item in visible_items:
            item["estimated_tokens"] = cast(int, item["input_bytes"]) // 4
        remainder = estimated_total - sum(
            cast(int, item["estimated_tokens"]) for item in visible_items
        )
        ranked = sorted(
            range(len(visible_items)),
            key=lambda index: (-(cast(int, visible_items[index]["input_bytes"]) % 4), index),
        )
        for index in ranked[:remainder]:
            visible_items[index]["estimated_tokens"] = (
                cast(int, visible_items[index]["estimated_tokens"]) + 1
            )
        items_truncated = bool(omitted)
        visible_source_refs_truncated = source_refs_truncated or any(
            bool(item.get("source_refs")) for item in omitted
        )
        result: dict[str, JsonValue] = {
            "version": 1,
            "counter_version": "approx-utf8/1",
            "input_bytes": total_bytes,
            "estimated_tokens": (total_bytes + 3) // 4,
            "items": cast(JsonValue, visible_items),
            "model_context_window": (
                model_context_window
                if type(model_context_window) is int and model_context_window > 0
                else None
            ),
            "items_truncated": items_truncated,
            "omitted_items": len(omitted),
            "source_refs_truncated": visible_source_refs_truncated,
        }
        if type(reserved_output_tokens) is int and reserved_output_tokens >= 0:
            result["reserved_output_tokens"] = reserved_output_tokens
        if type(input_budget_bytes) is int and input_budget_bytes >= 0:
            result["input_budget_bytes"] = input_budget_bytes
        if len(encoded(result).encode("utf-8")) <= _MAX_MANIFEST_BYTES or keep_count == 0:
            return result
        keep_count -= 1


def session_message_metadata(
    messages: Sequence[Message], sources: Sequence[object]
) -> list[ItemMetadata | None]:
    """Associate frozen turn references with exact message groups by their stored digest."""
    result: list[ItemMetadata | None] = [None] * len(messages)
    cursor = 1 if messages and messages[0].role == "system" else 0
    serialized = [encoded(message.model_dump(mode="json")).encode("utf-8") for message in messages]
    for source in sources:
        expected = getattr(source, "fingerprint", None)
        if not isinstance(expected, str):
            break
        match_end: int | None = None
        digest = hashlib.sha256(b"[")
        for end in range(cursor + 1, len(messages) + 1):
            if end > cursor + 1:
                digest.update(b",")
            digest.update(serialized[end - 1])
            candidate = digest.copy()
            candidate.update(b"]")
            if candidate.hexdigest() == expected:
                match_end = end
                break
        if match_end is None:
            break
        source_ref: dict[str, JsonValue] = {}
        for key in ("turn_id", "run_id", "last_seq", "fingerprint"):
            value = getattr(source, key, None)
            if key == "run_id" and value is None:
                source_ref[key] = None
            elif key in {"last_seq"} and type(value) is int:
                source_ref[key] = value
            elif isinstance(value, str):
                source_ref[key] = value
        metadata = ItemMetadata(source_refs=(source_ref,))
        for index in range(cursor, match_end):
            result[index] = metadata
        cursor = match_end
    for index in range(cursor, max(cursor, len(messages) - 1)):
        message = messages[index]
        if message.role != "user":
            continue
        if message.content.startswith("[不可信技能内容"):
            category: Category = "skill"
            label = "Frozen Skill snapshot"
        elif message.content.startswith("[不可信长期记忆内容"):
            category = "memory"
            label = "Frozen private memory snapshot"
        else:
            continue
        result[index] = ItemMetadata(
            category=category,
            label=label,
            source_refs=(
                {
                    "fingerprint": hashlib.sha256(
                        encoded(message.model_dump(mode="json")).encode("utf-8")
                    ).hexdigest()
                },
            ),
        )
    return result
