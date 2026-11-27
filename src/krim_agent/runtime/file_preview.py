"""Read-only file-change previews from model input, never mutation receipts.

Only model-visible arguments and already-authorized tool history are inspected.
No filesystem reads, tool dispatch, or approval bypass occurs on this path.
"""
from __future__ import annotations

import difflib
import hashlib
import json
from collections import Counter
from dataclasses import dataclass, field
from typing import Any

from krim_agent.contracts import Message, ServiceError
from krim_agent.development.file_tools import FileInput

MAX_PREVIEW_BYTES = 1 << 20
INTERVAL = 0.2
_FIELDS = frozenset({"path", "file_path", "content", "old_string", "new_string",
                     "expected_sha256", "replace_all"})


class PartialFileArguments:
    """Incremental top-level JSON decoder; unfinished strings remain readable.

    Escapes and surrogate pairs may span arbitrary provider chunks. A key/path
    is used only once closed; duplicate fields or malformed input disable the
    preview instead of guessing which complete argument the executor will see.
    """

    def __init__(self) -> None:
        self.values: dict[str, Any] = {}
        self.closed: set[str] = set()
        self.invalid = False
        self.finished = False
        self.size = 0
        self._state = "start"
        self._key: str | None = None
        self._pieces: list[str] = []
        self._escape = False
        self._unicode: str | None = None
        self._high: int | None = None
        self._literal = ""
        self._seen: set[str] = set()

    def _character(self, value: str) -> None:
        number = ord(value)
        if self._high is not None:
            if 0xDC00 <= number <= 0xDFFF:
                self._pieces.append(chr(0x10000 + ((self._high - 0xD800) << 10)
                                        + number - 0xDC00))
                self._high = None
                return
            self.invalid = True
            return
        if 0xD800 <= number <= 0xDBFF:
            self._high = number
        elif 0xDC00 <= number <= 0xDFFF:
            self.invalid = True
        else:
            self._pieces.append(value)

    def feed(self, delta: str) -> None:
        if self.invalid:
            return
        self.size += len(delta.encode("utf-8"))
        if self.size > MAX_PREVIEW_BYTES:
            self.invalid = True
            return
        for char in delta:
            if self.invalid:
                break
            if self._state in {"key_string", "value_string"}:
                if self._unicode is not None:
                    if char not in "0123456789abcdefABCDEF":
                        self.invalid = True
                        continue
                    self._unicode += char
                    if len(self._unicode) == 4:
                        self._character(chr(int(self._unicode, 16)))
                        self._unicode = None
                    continue
                if self._escape:
                    self._escape = False
                    if char == "u":
                        self._unicode = ""
                    elif char in '"\\/bfnrt':
                        self._character({'"': '"', "\\": "\\", "/": "/", "b": "\b",
                                         "f": "\f", "n": "\n", "r": "\r", "t": "\t"}[char])
                    else:
                        self.invalid = True
                    continue
                if char == "\\":
                    self._escape = True
                elif char == '"':
                    if self._high is not None:
                        self.invalid = True
                        continue
                    value = "".join(self._pieces)
                    self._pieces = []
                    if self._state == "key_string":
                        if value in self._seen:
                            self.invalid = True
                            continue
                        self._key = value
                        self._seen.add(value)
                        self._state = "colon"
                    else:
                        if self._key in _FIELDS:
                            self.values[self._key] = value
                            self.closed.add(self._key)
                        self._state = "comma"
                elif ord(char) < 32:
                    self.invalid = True
                else:
                    self._character(char)
                continue
            if char.isspace():
                continue
            if self._state == "literal":
                if char not in ",}":
                    self._literal += char
                    if len(self._literal) > 8:
                        self.invalid = True
                    continue
                try:
                    value = json.loads(self._literal)
                except (ValueError, RecursionError):
                    self.invalid = True
                    continue
                if self._key in _FIELDS:
                    self.values[self._key] = value
                    self.closed.add(self._key)
                self._state = "comma"
            if self._state == "start" and char == "{":
                self._state = "key"
            elif self._state in {"key", "next_key"} and char == '"':
                self._pieces = []
                self._state = "key_string"
            elif self._state == "key" and char == "}":
                self._state, self.finished = "end", True
            elif self._state == "colon" and char == ":":
                self._state = "value"
            elif self._state == "value":
                if char == '"':
                    self._pieces = []
                    self._state = "value_string"
                elif char in "-0123456789tfn":
                    self._literal = char
                    self._state = "literal"
                else:
                    self.invalid = True
            elif self._state == "comma" and char == ",":
                self._state = "next_key"
            elif self._state == "comma" and char == "}":
                self._state, self.finished = "end", True
            else:
                self.invalid = True

    def snapshot(self) -> tuple[dict[str, Any], set[str]]:
        values = dict(self.values)
        if self._state == "value_string" and self._key in _FIELDS:
            values[self._key] = "".join(self._pieces)
        return values, set(self.closed)


@dataclass
class _Text:
    digest: str
    content: str


def _digest(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def _name(name: str) -> str:
    return name.removeprefix("dev_")


def history_files(messages: list[Message]) -> dict[str, _Text]:
    """Version-bound full text from successful prior tools already in context."""
    files: dict[str, _Text] = {}
    calls: dict[str, dict[str, Any]] = {}
    pages: dict[str, tuple[str, int, str]] = {}
    for message in messages:
        if message.role == "assistant":
            for call in message.tool_calls:
                if isinstance(call.get("id"), str):
                    calls[call["id"]] = call
        if message.role != "tool" or message.call_id not in calls:
            continue
        call = calls[message.call_id]
        name = _name(str(call.get("name", "")))
        args = call.get("arguments")
        if isinstance(args, str):
            try:
                args = json.loads(args)
            except ValueError:
                continue
        if not isinstance(args, dict):
            continue
        try:
            result = json.loads(message.content)
        except ValueError:
            continue
        if not isinstance(result, dict):
            continue
        if result.get("ok") is False:
            continue
        value = result.get("value", result)
        if not isinstance(value, dict) or value.get("ok") is False:
            continue
        if name == "shell":
            # Arbitrary commands may change files. Keep digest-bound snapshots:
            # previews only use them when the next mutation names that version,
            # and the actual worker still rejects a stale hash before writing.
            pages.clear()
            continue
        path = args.get("path")
        if not isinstance(path, str):
            continue
        digest = value.get("sha256")
        if name == "read" and isinstance(digest, str) and isinstance(value.get("text"), str):
            text, offset = value["text"], value.get("offset", 0)
            prior = pages.get(path)
            if offset == 0:
                prior = (digest, 0, "")
            if prior is not None and prior[0] == digest and prior[1] == offset:
                joined = prior[2] + text
                if len(joined.encode()) > MAX_PREVIEW_BYTES:
                    pages.pop(path, None)
                    continue
                pages[path] = (digest, offset + len(text.encode()), joined)
                if value.get("eof") is True and _digest(joined) == digest:
                    files[path] = _Text(digest, joined)
        elif name == "write":
            text = args.get("content")
            files.pop(path, None)
            if (isinstance(text, str) and len(text.encode()) <= MAX_PREVIEW_BYTES
                    and _digest(text) == digest):
                files[path] = _Text(digest, text)
        elif name == "edit":
            prior = files.pop(path, None)
            old, new = args.get("old_string"), args.get("new_string")
            if (prior and prior.digest == args.get("expected_sha256")
                    and isinstance(old, str) and old and isinstance(new, str)):
                text = prior.content.replace(old, new, -1 if args.get("replace_all") else 1)
                if len(text.encode()) <= MAX_PREVIEW_BYTES and _digest(text) == digest:
                    files[path] = _Text(digest, text)
    return files


def _lines(text: str) -> list[str]:
    parts = text.split("\n")
    return [part + "\n" for part in parts[:-1]] + ([parts[-1]] if parts[-1] else [])


def line_delta(before: str, after: str) -> tuple[int, int] | None:
    """Bounded preview equivalent of the worker's committed line comparison."""
    if before == after:
        return (0, 0)
    left, right = _lines(before), _lines(after)
    occurrences = Counter(right)
    if sum(occurrences[line] for line in left) > 250_000:
        return None
    added = removed = 0
    for tag, i, j, k, end in difflib.SequenceMatcher(
        None, left, right, autojunk=False,
    ).get_opcodes():
        if tag != "equal":
            added += end - k
            removed += j - i
    return added, removed


def _complete_prefix(text: str, complete: bool) -> str:
    return text if complete else text[:text.rfind("\n") + 1]


def _partial_delta(before: str, after: str, complete: bool) -> tuple[int, int] | None:
    """An unfinished replacement cannot delete the not-yet-generated suffix."""
    if complete:
        return line_delta(before, after)
    after = _complete_prefix(after, False)
    if not after:
        return (0, 0)
    left, right = _lines(before), _lines(after)
    occurrences = Counter(right)
    if sum(occurrences[line] for line in left) > 250_000:
        return None
    added = removed = 0
    for tag, i, j, k, end in difflib.SequenceMatcher(
        None, left, right, autojunk=False,
    ).get_opcodes():
        if tag == "equal":
            continue
        added += end - k
        if end < len(right):
            # Only a later matching line anchors a removal. Without that anchor
            # the unmatched original suffix may still arrive in later chunks;
            # count additions now, finalize trailing deletions at string close.
            removed += j - i
    return added, removed


@dataclass
class _Call:
    decoder: PartialFileArguments = field(default_factory=PartialFileArguments)
    name: str = ""
    revision: int = 0
    last_at: float = float("-inf")
    last_value: tuple[Any, ...] | None = None
    provider_id: str = ""
    emitted_id: str = ""


class FileChangePreviews:
    def __init__(self, messages: list[Message]) -> None:
        self.files = history_files(messages)
        self.calls: dict[str, _Call] = {}

    def update(self, identity: str, name: str, delta: str, complete: bool,
               arguments: dict[str, Any] | None, now: float,
               stream_id: str | None = None) -> dict[str, Any] | None:
        key = stream_id or identity
        call = self.calls.get(key)
        if call is None:
            call = next((item for item in self.calls.values()
                         if item.provider_id == identity), None)
        if call is None:
            call = _Call()
        self.calls[key] = call
        call.provider_id = identity
        if name:
            call.name = _name(name)
        call.decoder.feed(delta)
        if call.name not in {"write", "edit"}:
            return None
        if not complete and now - call.last_at < INTERVAL:
            return None
        values, closed = call.decoder.snapshot()
        invalid = call.decoder.invalid
        if complete and arguments is not None:
            values, closed = arguments, set(arguments)
            try:
                invalid = sum(len(value.encode("utf-8")) for value in values.values()
                              if isinstance(value, str)) > MAX_PREVIEW_BYTES
            except UnicodeError:
                invalid = True
        if invalid:
            if call.last_value is None:
                return None
            # A malformed/oversized continuation invalidates its prior live
            # numbers; clear them without changing or dispatching the tool.
            path = call.last_value[0]
            empty = (path, call.name, None, None, complete, identity)
            if call.last_value == empty:
                return None
            call.last_value, call.last_at = empty, now
            call.revision += 1
            return {"provider_call_id": identity, "name": call.name,
                    "path": path, "revision": call.revision,
                    "lines_added": None, "lines_removed": None, "complete": complete}
        path = values.get("path", values.get("file_path"))
        if not isinstance(path, str) or not ({"path", "file_path"} & closed):
            return None
        try:
            FileInput(path=path)
        except (ValueError, ServiceError):
            return None
        before = self.files.get(path)
        expected = values.get("expected_sha256")
        versioned = (before is not None and isinstance(expected, str)
                     and expected == before.digest and "expected_sha256" in closed)
        added: int | None = None
        removed: int | None = None
        scope = "file"
        if call.name == "write" and isinstance(values.get("content"), str):
            text = values["content"]
            ended = "content" in closed
            if versioned:
                counts = _partial_delta(before.content, text, ended)
                if counts is not None:
                    added, removed = counts
            elif expected is None:
                text = _complete_prefix(text, ended)
                added = text.count("\n") + (1 if text and not text.endswith("\n") else 0)
                removed = 0
        elif call.name == "edit":
            scope = "replacement"
            old, new = values.get("old_string"), values.get("new_string")
            # Until both operands are complete, these are the received removal
            # and insertion sides of this replacement, not an already-applied
            # full-file diff. Each side can arrive first and can decrease later
            # when common lines cancel out in the completed comparison.
            if isinstance(old, str):
                text = _complete_prefix(old, "old_string" in closed)
                removed = len(_lines(text))
            if isinstance(new, str):
                text = _complete_prefix(new, "new_string" in closed)
                added = len(_lines(text))
            if (isinstance(old, str) and old and "old_string" in closed
                    and isinstance(new, str) and "new_string" in closed):
                counts = line_delta(old, new)
                if counts is not None:
                    added, removed = counts
                else:
                    added = removed = None
            if values.get("replace_all") is True:
                if versioned and isinstance(old, str) and old and "old_string" in closed:
                    multiplier = before.content.count(old)
                    added = added * multiplier if added is not None else None
                    removed = removed * multiplier if removed is not None else None
                else:
                    added = removed = None
            if (versioned and isinstance(old, str) and old and "old_string" in closed
                    and isinstance(new, str) and "new_string" in closed):
                matches = before.content.count(old)
                if matches == 1 or matches and values.get("replace_all") is True:
                    text = before.content.replace(
                        old, new, -1 if values.get("replace_all") else 1)
                    counts = line_delta(before.content, text)
                    if counts is not None:
                        added, removed = counts
                        scope = "file"
        value = (path, call.name, added, removed, complete, identity)
        if value == call.last_value:
            return None
        call.last_at, call.last_value = now, value
        call.revision += 1
        event = {"provider_call_id": identity, "name": call.name, "path": path,
                 "revision": call.revision, "lines_added": added, "lines_removed": removed,
                 "complete": complete, "count_scope": scope}
        if call.emitted_id and call.emitted_id != identity:
            event["replaces_provider_call_id"] = call.emitted_id
        call.emitted_id = identity
        return event
