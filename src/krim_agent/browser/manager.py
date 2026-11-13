"""Fail-closed per-run browser session manager around a trusted worker adapter."""

from __future__ import annotations

import asyncio
import re
from collections.abc import Mapping

from krim_agent.contracts import ServiceError

from .egress import BrowserEgressPolicy
from .protocol import BrowserCommand, BrowserResult, BrowserWorker, result_from_wire


class BrowserManager:
    """Own one ephemeral worker/page; manager is bound to one run identifier.

    A concrete worker transport is intentionally not included until deployment
    provisions a worker image and an enforced proxy route. Every operation passes
    through this closed protocol, and unknown/lost outcomes close the worker.
    """

    def __init__(
        self,
        run_id: str,
        worker: BrowserWorker,
        egress: BrowserEgressPolicy,
        *,
        timeout_seconds: float = 30.0,
    ) -> None:
        if not re.fullmatch(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", run_id):
            raise ValueError("browser run id invalid")
        if not 0.1 <= timeout_seconds <= 90:
            raise ValueError("browser timeout invalid")
        self.run_id = run_id
        self.worker = worker
        self.egress = egress
        self.timeout_seconds = timeout_seconds
        self._cleanup_timeout = min(5.0, timeout_seconds)
        self._lock = asyncio.Lock()
        self._closed = False
        self._close_task: asyncio.Task[None] | None = None

    @property
    def closed(self) -> bool:
        return self._closed

    async def navigate(self, url: str) -> BrowserResult:
        return await self._dispatch(BrowserCommand("navigate", url))

    async def snapshot(self) -> BrowserResult:
        return await self._dispatch(BrowserCommand("snapshot"))

    async def screenshot(self) -> BrowserResult:
        return await self._dispatch(BrowserCommand("screenshot"))

    async def close(self) -> None:
        task = self._start_cleanup("close")
        try:
            done, _ = await asyncio.wait({task}, timeout=self._cleanup_timeout * 2)
        except asyncio.CancelledError:
            task.add_done_callback(self._consume_cleanup_result)
            task.cancel()
            raise
        if not done:
            task.add_done_callback(self._consume_cleanup_result)
            task.cancel()
            raise ServiceError("browser_worker_failed", 503)
        try:
            task.result()
        except BaseException:
            raise ServiceError("browser_worker_failed", 503) from None

    def _start_cleanup(self, mode: str) -> asyncio.Task[None]:
        if self._close_task is None:
            self._closed = True
            cleanup = self._close_worker if mode == "close" else self._dispose_worker
            self._close_task = asyncio.create_task(cleanup())
            self._close_task.add_done_callback(self._consume_cleanup_result)
        return self._close_task

    @staticmethod
    def _consume_cleanup_result(task: asyncio.Task[None]) -> None:
        try:
            task.exception()
        except BaseException:
            pass

    async def _close_worker(self) -> None:
        failure: Exception | None = None
        try:
            async with asyncio.timeout(self._cleanup_timeout):
                async with self._lock:
                    raw = await self.worker.command(BrowserCommand("close").wire())
                if not isinstance(raw, Mapping):
                    raise ServiceError("browser_protocol", 502)
                result_from_wire("close", raw)
        except BaseException as error:
            failure = error
        try:
            async with asyncio.timeout(self._cleanup_timeout):
                await self.worker.close()
        except BaseException as error:
            if failure is None:
                failure = error
        if failure is not None:
            raise failure

    async def _dispatch(self, command: BrowserCommand) -> BrowserResult:
        if self._closed:
            raise ServiceError("browser_closed", 409)
        if command.operation == "navigate":
            if command.url is None:
                raise ServiceError("browser_invalid_input", 422)
            self.egress.authorize_url(command.url)
        try:
            async with self._lock:
                if self._closed:
                    raise ServiceError("browser_closed", 409)
                async with asyncio.timeout(self.timeout_seconds):
                    raw = await self.worker.command(command.wire())
                if not isinstance(raw, Mapping):
                    raise ServiceError("browser_protocol", 502)
                result = result_from_wire(command.operation, raw)
                if command.operation == "navigate" and result.final_url is None:
                    raise ServiceError("browser_protocol", 502)
                if result.final_url is not None:
                    self.egress.authorize_url(result.final_url)
                return result
        except asyncio.CancelledError:
            cleanup = asyncio.create_task(self._dispose())
            cleanup.add_done_callback(self._consume_cleanup_result)
            try:
                async with asyncio.timeout(self._cleanup_timeout):
                    await asyncio.shield(cleanup)
            except TimeoutError:
                cleanup.cancel()
            except asyncio.CancelledError:
                cleanup.cancel()
            raise
        except TimeoutError:
            await self._dispose()
            raise ServiceError("browser_timeout", 504) from None
        except ServiceError:
            await self._dispose()
            raise
        except Exception:
            await self._dispose()
            raise ServiceError("browser_worker_failed", 503) from None

    async def _dispose(self) -> None:
        task = self._start_cleanup("dispose")
        try:
            done, _ = await asyncio.wait({task}, timeout=self._cleanup_timeout)
        except asyncio.CancelledError:
            if not task.done():
                task.cancel()
            raise
        if not done:
            task.cancel()
        # A failed or stalled cleanup must never replace the original tool result.

    async def _dispose_worker(self) -> None:
        try:
            async with asyncio.timeout(self.timeout_seconds):
                await self.worker.close()
        except BaseException:
            # Preserve the initiating operation error/cancellation; cleanup is best effort.
            return
