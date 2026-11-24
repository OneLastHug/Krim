"""In-process wakeups over a durable mailbox; notifications are only hints."""

from __future__ import annotations

import asyncio
import math
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any


@dataclass
class _Signal:
    condition: asyncio.Condition = field(default_factory=asyncio.Condition)
    revision: int = 0
    closed: bool = False
    waiters: int = 0


class MailboxWakeups:
    """Race-free waiting without polling the model or RunService.wait.

    Persist before publish(). Every wait still reads storage: wakeup loss on
    restart cannot lose a message, and a notification does not imply consumption.
    """

    def __init__(self) -> None:
        self._signals: dict[str, _Signal] = {}
        self._closed = False

    def _signal(self, agent_id: str) -> _Signal:
        if not agent_id:
            raise ValueError("missing mailbox identity")
        signal = self._signals.setdefault(agent_id, _Signal())
        if self._closed:
            signal.closed = True
        return signal

    async def publish(self, agent_id: str) -> None:
        signal = self._signal(agent_id)
        async with signal.condition:
            signal.revision += 1
            signal.condition.notify_all()

    async def wait(
        self,
        agent_id: str,
        after_seq: int,
        read: Callable[[int], Awaitable[list[dict[str, Any]]]],
        *,
        seconds: float = 30.0,
        check: Callable[[], None] = lambda: None,
    ) -> list[dict[str, Any]]:
        if type(after_seq) is not int or after_seq < 0:
            raise ValueError("invalid mailbox cursor")
        if not math.isfinite(seconds) or seconds < 0 or seconds > 300:
            raise ValueError("mailbox timeout must be between zero and 300 seconds")
        signal = self._signal(agent_id)
        deadline = asyncio.get_running_loop().time() + seconds
        signal.waiters += 1
        try:
            while True:
                check()
                # Subscribe before the read. A sender that commits while read()
                # is in flight changes this revision, so we never miss its hint.
                async with signal.condition:
                    revision = signal.revision
                rows = await read(after_seq)
                check()
                if rows:
                    return rows
                async with signal.condition:
                    if signal.closed:
                        return []
                    if signal.revision != revision:
                        continue
                    remaining = deadline - asyncio.get_running_loop().time()
                    if remaining <= 0:
                        return []
                    try:
                        await asyncio.wait_for(signal.condition.wait(), remaining)
                    except TimeoutError:
                        # A commit may have raced the timeout. One final durable
                        # read establishes what this receipt actually contains.
                        rows = await read(after_seq)
                        check()
                        return rows
        finally:
            signal.waiters -= 1
            if signal.closed and not signal.waiters:
                if self._signals.get(agent_id) is signal:
                    self._signals.pop(agent_id, None)

    async def close(self, agent_id: str | None = None) -> None:
        if agent_id is None:
            self._closed = True
            signals = list(self._signals.items())
        else:
            signals = [(agent_id, self._signal(agent_id))]
        for identity, signal in signals:
            async with signal.condition:
                signal.closed = True
                signal.revision += 1
                signal.condition.notify_all()
                if not signal.waiters and self._signals.get(identity) is signal:
                    self._signals.pop(identity, None)
