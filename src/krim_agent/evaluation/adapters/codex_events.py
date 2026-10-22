"""有界JSONL与Codex原生事件；schema资格由独立M0提供，fixture不冒充发行认证。"""

from __future__ import annotations

import copy
import json
from typing import Any


def _pairs(values: list[tuple[str, Any]]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in values:
        if key in out:
            raise ValueError("duplicate_json_key")
        out[key] = value
    return out


def _counter(value: Any) -> int | None:
    return value if type(value) is int and 0 <= value <= 9007199254740991 else None


class _JSONEvents:
    family = "abstract"

    def __init__(self) -> None:
        self.buffer = b""
        self.bytes = 0
        self.count = 0
        self.error: str | None = None
        self.initialized = False
        self.ended = False
        self.pending: dict[str, dict[str, Any]] = {}
        self.tools: list[dict[str, Any]] = []
        self.proposals: list[dict[str, Any]] = []
        self.usage: dict[str, Any] | None = None
        self.cost: float | None = None
        self.text = ""
        self._finished: dict[str, Any] | None = None

    def _fail(self, reason: str) -> None:
        self.error = self.error or reason
        raise ValueError(reason)

    def feed(self, data: bytes) -> None:
        if self._finished is not None:
            self._fail("feed_after_eof")
        self.bytes += len(data)
        if self.bytes > 262144:
            self._fail("stream_limit")
        if self.error:
            return
        self.buffer += data
        while b"\n" in self.buffer:
            line, self.buffer = self.buffer.split(b"\n", 1)
            self._line(line)
        if len(self.buffer) > 65536:
            self._fail("line_limit")

    def _line(self, line: bytes) -> None:
        if len(line) > 65536:
            self._fail("line_limit")
        self.count += 1
        if self.count > 256:
            self._fail("event_limit")
        try:
            frame = json.loads(
                line.decode("utf-8"),
                object_pairs_hook=_pairs,
                parse_constant=lambda value: (_ for _ in ()).throw(ValueError("nonfinite")),
            )
            if not isinstance(frame, dict) or not isinstance(frame.get("type"), str):
                raise ValueError("frame_schema")
            if self.ended:
                raise ValueError("frame_after_terminal")
            self._consume(frame)
        except (ValueError, KeyError, TypeError, RecursionError, UnicodeError):
            self._fail("protocol_error")

    def _consume(self, frame: dict[str, Any]) -> None:
        raise NotImplementedError

    def _start(self, ident: Any, item: dict[str, Any]) -> None:
        if not isinstance(ident, str) or not 1 <= len(ident) <= 128:
            raise ValueError("call_id")
        if ident in self.pending or any(t["id"] == ident for t in self.tools):
            raise ValueError("duplicate_call")
        self.pending[ident] = copy.deepcopy(item)
        self.proposals.append({"id": ident, "kind": item["kind"]})

    def finish(self) -> dict[str, Any]:
        if self._finished is not None:
            return copy.deepcopy(self._finished)
        if self.buffer and not self.error:
            try:
                self._line(self.buffer)
            except ValueError:
                pass
        self.buffer = b""
        if not self.error and (not self.initialized or not self.ended or self.pending):
            self.error = "terminal_missing"
        complete = self.error is None
        result = {
            "schema_version": "native-events/1",
            "family": self.family,
            "complete": complete,
            "reason": self.error,
            "tools": copy.deepcopy(self.tools),
            "proposals": copy.deepcopy(self.proposals),
            "usage": self.usage,
            "usage_complete": self.usage is not None,
            "cost": self.cost,
            "cost_basis": "native_reported" if self.cost is not None else None,
            "request_count": None,
            "assistant_text": self.text or None,
            "terminal_seen": self.ended,
        }
        if len(json.dumps(result, ensure_ascii=False).encode()) > 131072:
            result["complete"], result["reason"] = False, "facts_limit"
            result["assistant_text"] = None
            result["tools"] = [
                {
                    "id": t["id"],
                    "kind": t["kind"],
                    "status": t["status"],
                    "arguments": None,
                    "arguments_complete": False,
                    "exit_code": t["exit_code"],
                    "is_error": t["is_error"],
                }
                for t in self.tools
            ]
        self._finished = result
        return copy.deepcopy(result)


class CodexEvents(_JSONEvents):
    family = "codex"

    def __init__(self) -> None:
        super().__init__()
        self.turn_started = False

    def _consume(self, frame: dict[str, Any]) -> None:
        kind = frame["type"]
        if kind == "thread.started":
            if (
                self.initialized
                or not isinstance(frame.get("thread_id"), str)
                or not frame["thread_id"]
            ):
                raise ValueError("thread_schema")
            self.initialized = True
            return
        if not self.initialized:
            raise ValueError("thread_missing")
        if kind == "turn.started":
            if self.turn_started:
                raise ValueError("turn_duplicate")
            self.turn_started = True
        elif kind.startswith("item."):
            if not self.turn_started:
                raise ValueError("turn_missing")
            item = frame["item"]
            ident, item_type = item["id"], item["type"]
            if item_type not in {"command_execution", "file_change", "agent_message"}:
                return
            if item_type == "agent_message":
                if kind == "item.completed":
                    if not isinstance(item.get("text"), str):
                        raise ValueError("message_schema")
                    self.text += item["text"]
                return
            mapped = "command" if item_type == "command_execution" else "edit"
            if kind == "item.started":
                self._start(
                    ident,
                    {
                        "kind": mapped,
                        "type": item_type,
                        "arguments": {"command": item.get("command")}
                        if mapped == "command"
                        else {"changes": item.get("changes")},
                    },
                )
            elif kind == "item.updated":
                if ident not in self.pending or self.pending[ident]["type"] != item_type:
                    raise ValueError("update_without_start")
            elif kind == "item.completed":
                prior = self.pending.pop(ident)
                if prior["type"] != item_type or item.get("status") not in {"completed", "failed"}:
                    raise ValueError("completion_schema")
                code = item.get("exit_code")
                if code is not None and type(code) is not int:
                    raise ValueError("exit_schema")
                self.tools.append(
                    {
                        "id": ident,
                        "kind": mapped,
                        "exit_code": code,
                        "status": item["status"],
                        "is_error": item["status"] == "failed",
                        "arguments": prior["arguments"],
                        "arguments_complete": True,
                    }
                )
        elif kind == "turn.completed":
            if not self.turn_started or self.pending:
                raise ValueError("incomplete_turn")
            self.ended = True
            raw = frame.get("usage", {})
            if isinstance(raw, dict):
                inp, cached, out = (
                    _counter(raw.get(k))
                    for k in ["input_tokens", "cached_input_tokens", "output_tokens"]
                )
                if inp is not None and cached is not None and out is not None and cached <= inp:
                    self.usage = {
                        "input_tokens": inp,
                        "cached_input_tokens": cached,
                        "cache_creation_input_tokens": None,
                        "output_tokens": out,
                        "total_input_tokens": inp,
                        "total_tokens": inp + out,
                    }
        elif kind in {"turn.failed", "error"}:
            self.ended = True
            self.error = "target_error"
