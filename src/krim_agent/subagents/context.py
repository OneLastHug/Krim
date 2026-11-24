"""Fresh and fork inputs built from host-validated, completed conversations."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from krim_agent.contracts import Message, encoded, fingerprint


@dataclass(frozen=True)
class ContextSnapshot:
    messages: tuple[Message, ...]
    source_run_id: str | None
    prefix_end: int
    source_fingerprint: str
    omitted_turns: int = 0

    def record(self) -> dict[str, object]:
        return {
            "messages": [message.model_dump(mode="json") for message in self.messages],
            "source_run_id": self.source_run_id,
            "prefix_end": self.prefix_end,
            "source_fingerprint": self.source_fingerprint,
            "omitted_turns": self.omitted_turns,
        }


def _size(history: Sequence[Message]) -> int:
    return len(encoded([message.model_dump(mode="json") for message in history]).encode("utf-8"))


def completed_turns(history: Sequence[Message]) -> list[tuple[int, int]]:
    """Return complete user-turn ranges, excluding any unfinished suffix.

    An assistant final message closes a turn only when every declared call has
    its result. Unknown-operation observations are preserved as observations;
    this function never turns them into successful results or runnable calls.
    """
    turns: list[tuple[int, int]] = []
    start: int | None = None
    pending: set[str] = set()
    seen: set[str] = set()
    finished = False
    for index, message in enumerate(history):
        if message.role == "system":
            if start is not None:
                raise ValueError("system message inside a forkable conversation")
            continue
        if message.role == "user":
            if pending:
                raise ValueError("user input crosses an unfinished tool exchange")
            if start is not None and not finished:
                # Several user inputs can belong to the same active turn, but
                # only after the preceding tool exchange has fully closed.
                continue
            start, finished = index, False
            seen.clear()
        elif message.role == "assistant":
            if start is None or pending or finished:
                raise ValueError("assistant message crosses an unfinished exchange")
            for call in message.tool_calls:
                call_id = call.get("id")
                if not isinstance(call_id, str) or not call_id or call_id in seen:
                    raise ValueError("invalid or repeated tool call identity")
                seen.add(call_id)
                pending.add(call_id)
            if not pending:
                finished = True
                turns.append((start, index + 1))
        elif message.role == "tool":
            if not message.call_id or message.call_id not in pending:
                raise ValueError("tool result has no matching pending call")
            pending.remove(message.call_id)
        else:
            raise ValueError("invalid history role")
    return turns


def fresh_context(
    task: str,
    *,
    system_prompt: str,
    background: str = "",
    max_bytes: int | None = None,
) -> ContextSnapshot:
    if not task.strip():
        raise ValueError("missing delegated task")
    content = task if not background else background + "\n\n" + task
    history = (
        Message(role="system", content=system_prompt),
        Message(role="user", content=content),
    )
    if max_bytes is not None and _size(history) > max_bytes:
        raise ValueError("delegated input exceeds the worker context limit")
    return ContextSnapshot(history, None, 0, fingerprint([]))


def fork_context(
    history: Sequence[Message],
    task: str,
    *,
    source_run_id: str,
    system_prompt: str,
    prefix_end: int | None = None,
    max_bytes: int | None = None,
    max_messages: int | None = None,
) -> ContextSnapshot:
    """Copy one fixed completed prefix and append the *new* task separately.

    Trimming removes whole oldest turns, never an individual tool observation.
    The source fingerprint describes the original selected prefix, not merely
    the trimmed worker input. No later parent messages are synchronized.
    """
    if not task.strip() or not source_run_id:
        raise ValueError("missing task or source execution")
    turns = completed_turns(history)
    if prefix_end is None:
        prefix_end = turns[-1][1] if turns else 0
    if prefix_end != 0 and prefix_end not in {end for _, end in turns}:
        raise ValueError("fork snapshot must end at a completed turn")
    selected = [(start, end) for start, end in turns if end <= prefix_end]
    original = [message.model_dump(mode="json") for message in history[:prefix_end]]
    system = Message(role="system", content=system_prompt)
    request = Message(role="user", content=task)
    omitted = 0
    while True:
        copied = [
            message.model_copy(deep=True)
            for start, end in selected
            for message in history[start:end]
        ]
        result = (system, *copied, request)
        fits = (max_bytes is None or _size(result) <= max_bytes) and (
            max_messages is None or len(result) <= max_messages
        )
        if fits:
            return ContextSnapshot(
                result,
                source_run_id,
                prefix_end,
                fingerprint(original),
                omitted,
            )
        if not selected:
            raise ValueError("delegated input exceeds the worker context limit")
        selected.pop(0)
        omitted += 1
