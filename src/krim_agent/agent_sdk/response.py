"""A complete-response gate around a real SDK provider, not an agent loop."""

from __future__ import annotations

from collections.abc import AsyncGenerator, Awaitable, Callable, Sequence
from typing import Any

import ai
from ai import events
from ai import messages as sdk_messages
from pydantic import PrivateAttr


class IncompleteTurn(ValueError):
    """The whole model response did not pass the side-effect boundary."""


class CompleteResponseProvider(ai.Provider):
    """Release tool events only after successful exhaustion and batch admission.

    The production gateway's wire/size/UTF-8/DONE checks remain necessary. This
    probe gate accepts SDK events and never claims to validate the underlying SSE.
    """

    provider_class_id: str = "krim-probe-complete-response"
    name: str = "krim-probe-complete-response"
    default_base_url: str = "http://unused.invalid"
    _source: ai.Model = PrivateAttr()
    _prepare: Callable[[Sequence[sdk_messages.ToolCallPart]], Awaitable[None]] = PrivateAttr()
    _commit: (
        Callable[[list[sdk_messages.Message], sdk_messages.Message], Awaitable[None]] | None
    ) = PrivateAttr(default=None)
    _guard: Callable[[], Awaitable[None]] | None = PrivateAttr(default=None)
    _prepare_input: (
        Callable[[list[sdk_messages.Message]], Awaitable[list[sdk_messages.Message]]] | None
    ) = PrivateAttr(default=None)
    _max_events: int | None = PrivateAttr(default=None)
    _max_bytes: int | None = PrivateAttr(default=None)

    def __init__(
        self,
        source: ai.Model,
        prepare: Callable[[Sequence[sdk_messages.ToolCallPart]], Awaitable[None]],
        *,
        commit: Callable[[list[sdk_messages.Message], sdk_messages.Message], Awaitable[None]]
        | None = None,
        guard: Callable[[], Awaitable[None]] | None = None,
        prepare_input: Callable[[list[sdk_messages.Message]],
                                Awaitable[list[sdk_messages.Message]]] | None = None,
        max_events: int | None = None,
        max_bytes: int | None = None,
    ) -> None:
        super().__init__()
        if any(value is not None and (type(value) is not int or value < 1)
               for value in (max_events, max_bytes)):
            raise ValueError("invalid response capacity")
        self._source, self._prepare = source, prepare
        self._commit, self._guard = commit, guard
        self._prepare_input = prepare_input
        self._max_events, self._max_bytes = max_events, max_bytes

    async def stream(
        self,
        model: ai.Model,
        messages: list[sdk_messages.Message],
        **kwargs: Any,
    ) -> AsyncGenerator[events.Event]:
        history = messages
        if self._guard is not None:
            await self._guard()
        if self._prepare_input is not None:
            history = await self._prepare_input(history)
        buffered: list[events.Event] = []
        starts: list[str] = []
        ends: list[str] = []
        ended_calls: list[sdk_messages.ToolCallPart] = []
        finish: str | None = None
        ended = False
        size = 0
        async with ai.stream(self._source, history, **kwargs) as source:
            async for event in source:
                if ended:
                    raise IncompleteTurn("event after response completion")
                # SDK snapshots accumulate the entire response. Count payloads,
                # not the repeated hydrated message, to keep the bound linear.
                size += len(event.model_dump_json(exclude={"message"}).encode())
                if (self._max_events is not None and len(buffered) >= self._max_events
                        or self._max_bytes is not None and size > self._max_bytes):
                    raise IncompleteTurn("response exceeds configured capacity")
                if isinstance(event, events.ToolStart):
                    starts.append(event.tool_call_id)
                elif isinstance(event, events.ToolEnd):
                    ends.append(event.tool_call.tool_call_id)
                    ended_calls.append(event.tool_call.model_copy(deep=True))
                elif isinstance(event, events.StreamEnd):
                    ended, finish = True, event.finish_reason
                # Drop the repeated hydrated snapshot before copying, avoiding
                # quadratic memory growth across a long stream.
                buffered.append(
                    event.model_copy(
                        update={
                            "message": sdk_messages.Message(
                                id=event.message.id, role="assistant", parts=[]
                            ),
                        }
                    ).model_copy(deep=True)
                )
            complete = source.message.model_copy(deep=True)
            calls = tuple(complete.tool_calls)
        # Check after leaving the source context as cleanup can also fail.
        ids = [call.tool_call_id for call in calls]
        if not ended or finish not in {"stop", "tool_call"}:
            raise IncompleteTurn("unsuccessful or missing finish reason")
        if bool(calls) != (finish == "tool_call"):
            raise IncompleteTurn("finish reason disagrees with tool batch")
        if starts != ids or ends != ids or len(set(ids)) != len(ids) or any(not i for i in ids):
            raise IncompleteTurn("incomplete or ambiguous tool batch")
        if any(
            (early.tool_name, early.tool_args) != (final.tool_name, final.tool_args)
            for early, final in zip(ended_calls, calls, strict=True)
        ):
            raise IncompleteTurn("tool changed after ToolEnd")
        await self._prepare(calls)
        if self._commit is not None:
            await self._commit(history, complete)
        for event in buffered:
            yield event
