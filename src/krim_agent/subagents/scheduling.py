"""Request permits are held during I/O, never while waiting for another agent.

Residency and process permits deliberately use separate pools. A parent that is
waiting for a message therefore cannot consume the worker's model permit.
"""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Literal


class CapacityUnavailable(RuntimeError):
    """Admission failed before accepting work; callers may retry later."""


class SchedulerClosed(RuntimeError):
    """The host is draining; no more requests may be admitted."""


@dataclass(eq=False)
class Permit:
    pool: RequestLimiter
    owner: str
    released: bool = False

    async def release(self) -> None:
        async with self.pool._condition:
            if not self.released:
                self.released = True
                self.pool._active.discard(self)
                self.pool._condition.notify_all()


@dataclass(eq=False, frozen=True)
class _Ticket:
    owner: str
    kind: Literal["root", "child"]


class RequestLimiter:
    """FIFO, cancellation-safe admission shared by root and worker requests.

    A permit covers one actual model request, not an agent's lifetime. FIFO also
    gives roots an opportunity under continuous worker load without permanently
    reserving the only connection when capacity is one.
    """

    def __init__(self, capacity: int) -> None:
        if type(capacity) is not int or capacity < 1:
            raise ValueError("request capacity must be a positive integer")
        self.capacity = capacity
        self._condition = asyncio.Condition()
        self._queue: deque[_Ticket] = deque()
        self._active: set[Permit] = set()
        self._closed = False

    @property
    def active(self) -> int:
        return len(self._active)

    @property
    def waiting(self) -> int:
        return len(self._queue)

    async def acquire(
        self,
        owner: str,
        *,
        kind: Literal["root", "child"] = "child",
        guard: Callable[[], None] = lambda: None,
    ) -> Permit:
        if not owner or kind not in {"root", "child"}:
            raise ValueError("missing request owner or invalid request kind")
        ticket = _Ticket(owner, kind)
        async with self._condition:
            if self._closed:
                raise SchedulerClosed()
            guard()
            self._queue.append(ticket)
            try:
                while True:
                    if self._closed:
                        raise SchedulerClosed()
                    guard()
                    if self._queue[0] is ticket and len(self._active) < self.capacity:
                        permit = Permit(self, owner)
                        self._queue.popleft()
                        self._active.add(permit)
                        self._condition.notify_all()
                        return permit
                    await self._condition.wait()
            except BaseException:
                if ticket in self._queue:
                    self._queue.remove(ticket)
                self._condition.notify_all()
                raise

    @asynccontextmanager
    async def slot(
        self,
        owner: str,
        *,
        kind: Literal["root", "child"] = "child",
        guard: Callable[[], None] = lambda: None,
    ) -> AsyncIterator[None]:
        permit = await self.acquire(owner, kind=kind, guard=guard)
        try:
            yield
        finally:
            # Admission accounting must finish even if the requesting task is
            # cancelled again while its network connection is closing.
            cleanup = asyncio.create_task(permit.release())
            try:
                await asyncio.shield(cleanup)
            except asyncio.CancelledError:
                await cleanup
                raise

    async def close(self) -> None:
        """Reject new/waiting requests; active I/O is drained by its owner."""
        async with self._condition:
            self._closed = True
            self._condition.notify_all()


class ResidentCapacity:
    """Fail-fast reservation; cleanup failures retain their reservation.

    Durable activation records remain the source of truth. This pool protects
    the preparation interval before an activation can be published in storage.
    """

    def __init__(self, capacity: int) -> None:
        if type(capacity) is not int or capacity < 1:
            raise ValueError("resident capacity must be a positive integer")
        self.capacity = capacity
        self._owners: set[str] = set()

    @property
    def active(self) -> int:
        return len(self._owners)

    def reserve(self, activation_id: str) -> None:
        if not activation_id:
            raise ValueError("missing activation identity")
        if activation_id in self._owners:
            return
        if len(self._owners) >= self.capacity:
            raise CapacityUnavailable()
        self._owners.add(activation_id)

    def release(self, activation_id: str) -> None:
        self._owners.discard(activation_id)
