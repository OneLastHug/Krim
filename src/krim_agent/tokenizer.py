"""可复现的离线 tiktoken BPE 计数；模型 provider usage 仍单独记录。"""

from __future__ import annotations

import base64
import hashlib
import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from functools import lru_cache
from importlib.resources import files
from typing import Any, Literal

import tiktoken

from krim_agent.protocol.domain import MessageView
from krim_agent.tools.core import ToolDefinition

TokenizerName = Literal["approx-v1", "cl100k_base", "o200k_base"]
MessageProjector = Callable[[Sequence[MessageView]], Sequence[Mapping[str, Any]]]
RANKS_SHA256 = {
    "cl100k_base": "223921b76ee99bde995b7ff738513eef100fb51d18c93597a113bcffe865b2a7",
    "o200k_base": "446a9538cb6c348e3516120d7c08b09f57c36495e2acfffe59a5bf8b0cfb1a2d",
}
MANIFEST_SHA256 = "bc0851911e1f179a086595c3432452ae05a562892610a860261ef72f1baf04fb"


class TokenizerError(ValueError):
    """资源缺失或损坏时失败，不联网下载，也不静默退回近似计数。"""

    code = "tokenizer_unavailable"


@dataclass(frozen=True)
class TokenizerSpec:
    name: TokenizerName
    reserved_output_tokens: int = field(default=0, kw_only=True)
    message_projector: MessageProjector | None = field(default=None, kw_only=True, repr=False)
    _encoding: tiktoken.Encoding | None = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        if self.name not in {"approx-v1", "cl100k_base", "o200k_base"}:
            raise ValueError("未知 tokenizer")
        if (
            type(self.reserved_output_tokens) is not int
            or not 0 <= self.reserved_output_tokens <= 1_000_000
        ):
            raise ValueError("输出 token 预留必须在 0–1,000,000")
        object.__setattr__(
            self, "_encoding", self._load(self.name) if self.name != "approx-v1" else None
        )

    @staticmethod
    @lru_cache(maxsize=2)
    def _load(name: TokenizerName) -> tiktoken.Encoding:
        try:
            resources = files("krim_agent.tokenizer_data")
            manifest_raw = resources.joinpath("manifest.json").read_bytes()
            if hashlib.sha256(manifest_raw).hexdigest() != MANIFEST_SHA256:
                raise ValueError("manifest")
            config = json.loads(manifest_raw)[name]
            raw = resources.joinpath(f"{name}.tiktoken").read_bytes()
            if hashlib.sha256(raw).hexdigest() != RANKS_SHA256[name]:
                raise ValueError("ranks")
            ranks = {
                base64.b64decode(token, validate=True): int(rank)
                for token, rank in (line.split() for line in raw.splitlines())
            }
            return tiktoken.Encoding(
                name=name,
                pat_str=config["pat_str"],
                mergeable_ranks=ranks,
                special_tokens={key: int(value) for key, value in config["special_tokens"].items()},
            )
        except (OSError, ValueError, KeyError, TypeError, ModuleNotFoundError) as error:
            raise TokenizerError("本地 tokenizer 资源缺失或完整性校验失败") from error

    def count(self, messages: Sequence[MessageView]) -> int:
        if self._encoding is not None and self.message_projector is not None:
            return sum(
                self.text_tokens(
                    json.dumps(item, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
                )
                for item in self.message_projector(messages)
            )
        total = 0
        chars = 0
        for message in messages:
            wire: dict[str, object] = {"role": message.role, "content": message.content}
            if message.role == "assistant" and message.details.get("tool_calls"):
                wire["tool_calls"] = message.details["tool_calls"]
            if message.role == "tool":
                wire["tool_call_id"] = message.details.get("call_id")
            serialized = json.dumps(
                wire, ensure_ascii=False, allow_nan=False, separators=(",", ":")
            )
            chars += len(serialized)
            if self._encoding is not None:
                total += len(self._encoding.encode_ordinary(serialized))
        return total if self._encoding is not None else (chars + 3) // 4

    def text_tokens(self, text: str) -> int:
        return (
            len(self._encoding.encode_ordinary(text))
            if self._encoding is not None
            else (len(text) + 3) // 4
        )

    def tool_tokens(self, tools: Sequence[ToolDefinition]) -> int:
        values = [
            {
                "type": "function",
                "function": {
                    "name": item.name,
                    "description": item.description,
                    "parameters": item.parameters.model_json_schema(),
                },
            }
            for item in tools
        ]
        return (
            self.text_tokens(
                json.dumps(values, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
            )
            if values
            else 0
        )

    def total_budget(self, messages: Sequence[MessageView]) -> int:
        return self.count(messages) + self.reserved_output_tokens
