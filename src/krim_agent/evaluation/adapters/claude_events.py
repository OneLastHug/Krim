"""独立Claude消息/工具配对；只复用JSONL字节边界，不借Codex状态机。"""

from __future__ import annotations

import math
from typing import Any

from .codex_events import _counter, _JSONEvents


class ClaudeEvents(_JSONEvents):
    family = "claude"

    def _consume(self, frame: dict[str, Any]) -> None:
        kind = frame["type"]
        if kind == "system" and frame.get("subtype") == "init":
            if (
                self.initialized
                or not isinstance(frame.get("session_id"), str)
                or not frame["session_id"]
            ):
                raise ValueError("session_schema")
            self.initialized = True
            return
        if not self.initialized:
            raise ValueError("session_missing")
        if kind in {"assistant", "user"}:
            content = frame["message"]["content"]
            if not isinstance(content, list):
                raise ValueError("message_schema")
            for item in content:
                if kind == "assistant" and item["type"] == "tool_use":
                    if not isinstance(
                        item.get("name"),
                        str,
                    ) or not isinstance(item.get("input"), dict):
                        raise ValueError("tool_schema")
                    mapped = (
                        "command"
                        if item["name"] == "Bash"
                        else "edit"
                        if item["name"] in {"Edit", "Write", "MultiEdit"}
                        else "read"
                    )
                    self._start(
                        item["id"],
                        {"kind": mapped, "name": item["name"], "arguments": item["input"]},
                    )
                elif kind == "user" and item["type"] == "tool_result":
                    ident = item["tool_use_id"]
                    prior = self.pending.pop(ident)
                    failed = item.get("is_error", False)
                    if type(failed) is not bool:
                        raise ValueError("result_schema")
                    self.tools.append(
                        {
                            "id": ident,
                            "kind": prior["kind"],
                            "name": prior["name"],
                            "exit_code": None,
                            "is_error": failed,
                            "status": "failed" if failed else "completed",
                            "arguments": prior["arguments"],
                            "arguments_complete": True,
                        }
                    )
                elif kind == "assistant" and item["type"] == "text":
                    self.text += item["text"]
        elif kind == "result":
            if frame.get("is_error") is True:
                self.ended, self.error = True, "target_error"
                return
            if (
                self.pending
                or frame.get(
                    "subtype",
                )
                != "success"
                or frame.get("is_error") is not False
            ):
                raise ValueError("result_schema")
            self.ended = True
            raw = frame.get("usage", {})
            if isinstance(raw, dict):
                inp, cached, created, out = (
                    _counter(raw.get(k))
                    for k in [
                        "input_tokens",
                        "cache_read_input_tokens",
                        "cache_creation_input_tokens",
                        "output_tokens",
                    ]
                )
                if all(v is not None for v in [inp, cached, created, out]):
                    assert (
                        inp is not None
                        and cached is not None
                        and created is not None
                        and out is not None
                    )
                    self.usage = {
                        "input_tokens": inp,
                        "cached_input_tokens": cached,
                        "cache_creation_input_tokens": created,
                        "output_tokens": out,
                        "total_input_tokens": inp + cached + created,
                        "total_tokens": inp + cached + created + out,
                    }
            cost = frame.get("total_cost_usd")
            if (
                isinstance(
                    cost,
                    (int, float),
                )
                and not isinstance(cost, bool)
                and math.isfinite(cost)
                and cost >= 0
            ):
                self.cost = float(cost)
