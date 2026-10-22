"""从旧Eval抽取的取消等待；不依赖RunService。"""

import asyncio
from collections.abc import Awaitable


async def settled[T](operation: Awaitable[T]) -> T:
    task = asyncio.ensure_future(operation)
    cancelled = False
    while True:
        try:
            value = await asyncio.shield(task)
            break
        except asyncio.CancelledError:
            if task.cancelled():
                raise
            cancelled = True
    if cancelled:
        raise asyncio.CancelledError
    return value


def task_done(task: asyncio.Task[None]) -> None:
    if not task.cancelled():
        task.exception()
