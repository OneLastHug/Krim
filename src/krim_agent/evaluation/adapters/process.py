"""唯一普通进程资源owner；只拥有实际Popen/流/pidfd，不调度Eval Task。"""

from __future__ import annotations

import asyncio
import json
import os
import signal
import time
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from krim_agent.contracts import ServiceError

from ..domain import CleanupResult, ExecutionRef, PreparedTrial, clone
from ..lifecycle import settled


class StreamRedactor:
    """在任何落盘之前替换精确secret及JSON转义形式，保留跨chunk尾部。"""

    def __init__(self, secrets: tuple[str, ...] = ()) -> None:
        tokens = {v.encode() for v in secrets if v}
        tokens.update(json.dumps(v, ensure_ascii=True)[1:-1].encode() for v in secrets if v)
        tokens.update(json.dumps(v, ensure_ascii=False)[1:-1].encode() for v in secrets if v)
        self.tokens = sorted(tokens, key=len, reverse=True)
        self.hold = max((len(t) for t in self.tokens), default=1) - 1
        self.pending = b""

    def feed(self, data: bytes, *, final: bool = False) -> bytes:
        self.pending += data
        end = len(self.pending) if final else max(0, len(self.pending) - self.hold)
        out = bytearray()
        index = 0
        while index < end:
            token = next((t for t in self.tokens if self.pending.startswith(t, index)), None)
            if token is None:
                out.append(self.pending[index])
                index += 1
            else:
                out.extend(b"[REDACTED]")
                index += len(token)
        self.pending = self.pending[index:]
        return bytes(out)


@dataclass
class ProcessWork:
    ref: ExecutionRef
    prepared: PreparedTrial
    process: asyncio.subprocess.Process
    argv: list[str]
    environment: dict[str, str]
    started: float
    pidfd: int
    lease: object
    conditions: dict[str, Any]
    collector: asyncio.Task[dict[str, Any]] | None = None
    cancel_task: asyncio.Task[CleanupResult] | None = None
    close_task: asyncio.Task[CleanupResult] | None = None
    cleanup_receipts: list[dict[str, Any]] = field(default_factory=list)
    stdout_eof: bool = False
    stderr_eof: bool = False
    stream_tasks: list[asyncio.Task[Any]] = field(default_factory=list)


class ProcessOwner:
    def __init__(self) -> None:
        self.jobs: dict[str, ProcessWork] = {}
        self.closed = False
        self._close_task: asyncio.Task[CleanupResult] | None = None
        self.spawn_calls = 0

    def identities(self) -> tuple[object, ...]:
        return tuple(
            getattr(getattr(self, n), "__func__", getattr(self, n))
            for n in ["spawn", "observe", "cancel", "close", "retire"]
        )

    async def spawn(
        self,
        prepared: PreparedTrial,
        argv: list[str],
        env: dict[str, str],
        lease: object,
        conditions: dict[str, Any],
        collector: Callable[[ProcessWork], Awaitable[dict[str, Any]]],
        stdin_data: bytes | None = None,
        guard: Callable[[], None] | None = None,
        on_started: Callable[[ProcessWork], None] | None = None,
    ) -> ProcessWork:
        if self.closed:
            raise ServiceError("closed")
        if guard is not None:
            guard()
        process = await asyncio.create_subprocess_exec(
            *argv,
            cwd=prepared.directory,
            env=env,
            stdin=asyncio.subprocess.DEVNULL if stdin_data is None else asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=True,
        )
        self.spawn_calls += 1
        job = ProcessWork(
            ExecutionRef(str(uuid.uuid4())),
            prepared,
            process,
            list(argv),
            dict(env),
            time.time(),
            -1,
            lease,
            clone(conditions),
        )
        self.jobs[job.ref.id] = job

        async def collect_owned() -> dict[str, Any]:
            return await collector(job)

        job.collector = asyncio.create_task(collect_owned())
        try:
            if on_started is not None:
                on_started(job)
            job.pidfd = os.pidfd_open(process.pid)
            if guard is not None:
                guard()
            if stdin_data is not None:
                assert process.stdin is not None
                process.stdin.write(stdin_data)
                process.stdin.close()
                try:
                    await process.stdin.wait_closed()
                except (BrokenPipeError, ConnectionResetError):
                    pass
                if guard is not None:
                    guard()
        except BaseException:
            await settled(self.cancel(job, lease))
            raise
        return job

    async def observe(
        self,
        work: ProcessWork,
        *,
        secrets: tuple[str, ...] = (),
        on_stdout: Callable[[bytes], None] | None = None,
    ) -> dict[str, Any]:
        process = work.process

        def stop() -> None:
            # pgid来自本owner创建的新session，绝不接受外部PID/pgid参数。
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass

        async def read(
            stream: asyncio.StreamReader | None,
            limit: int,
            callback: Callable[[bytes], None] | None = None,
        ) -> tuple[bytes, bool]:
            assert stream is not None
            redactor = StreamRedactor(secrets)
            data = bytearray()
            count = 0
            overflow = False
            while chunk := await stream.read(16384):
                count += len(chunk)
                if count > limit:
                    overflow = True
                    stop()
                safe = redactor.feed(chunk)
                if callback is not None:
                    try:
                        callback(safe)
                    except ValueError:
                        pass  # codec已保存protocol_error，继续真实drain/cleanup。
                data.extend(safe[: max(0, limit - len(data))])
            if stream is process.stdout:
                work.stdout_eof = True
            else:
                work.stderr_eof = True
            tail = redactor.feed(b"", final=True)
            if callback is not None:
                try:
                    callback(tail)
                except ValueError:
                    pass
            data.extend(tail[: max(0, limit - len(data))])
            return bytes(data), overflow

        try:
            work.stream_tasks = [
                asyncio.create_task(read(process.stdout, 262144, on_stdout)),
                asyncio.create_task(read(process.stderr, 131072)),
                asyncio.create_task(process.wait()),
            ]
            stdout, stderr, code = await asyncio.gather(
                *work.stream_tasks,
            )
            # 同组后代仍存在则不能把主PID退出伪装为全体关闭。
            group_empty = False
            try:
                os.killpg(process.pid, 0)
            except ProcessLookupError:
                group_empty = True
            if not group_empty:
                stop()
            return {
                "argv": work.argv,
                "cwd": work.prepared.directory,
                "env": {
                    k: StreamRedactor(
                        secrets,
                    )
                    .feed(v.encode(), final=True)
                    .decode()
                    for k, v in work.environment.items()
                },
                "execution_conditions": clone(work.conditions),
                "pid": process.pid,
                "pgid": process.pid,
                "uid": os.getuid(),
                "started_at": work.started,
                "finished_at": time.time(),
                "wait_returncode": code,
                "stdout_eof": True,
                "stderr_eof": True,
                "pidfd_opened": work.pidfd >= 0,
                "pidfd_closed": True,
                "initial": work.prepared.initial,
                "output_complete": not stdout[1] and not stderr[1],
                "stdout": stdout[0].decode(errors="replace"),
                "stderr": stderr[0].decode(errors="replace"),
                "group_empty": group_empty,
            }
        finally:
            if work.pidfd >= 0 and all(t.done() for t in work.stream_tasks):
                os.close(work.pidfd)
                work.pidfd = -1

    async def cancel(self, job: ProcessWork, lease: object) -> CleanupResult:
        if job.lease is not lease or self.jobs.get(job.ref.id) is not job:
            raise ServiceError("permission_denied")
        # runner的except/finally属于同次取消，unknown也必须复用同一Task和绝对截止。
        if job.cancel_task is None:
            job.cancel_task = asyncio.create_task(self._cancel(job))
        observed = job.close_task
        if observed is not None and observed.done() and not observed.cancelled():
            if observed.exception() is None and observed.result().closed:
                return await settled(observed)
        return await settled(job.cancel_task)

    async def _cancel(self, job: ProcessWork) -> CleanupResult:
        assert job.collector is not None
        loop = asyncio.get_running_loop()
        started = loop.time()
        deadline = started + 30
        observation = None
        known_empty = any(r["group_empty"] for r in job.cleanup_receipts)
        if (
            job.collector.done()
            and not job.collector.cancelled()
            and job.collector.exception() is None
        ):
            observation = job.collector.result()
            known_empty = known_empty or observation.get("process", observation).get(
                "group_empty", False
            )
        # 只有仍被真实Popen/pidfd持有的自有组可以收束；确认消失的组不再按旧PID发信号。
        if not known_empty and (job.pidfd >= 0 or job.process.returncode is None):
            try:
                os.killpg(job.process.pid, signal.SIGKILL)
            except ProcessLookupError:
                known_empty = True
        closed = False
        while True:
            if not known_empty:
                try:
                    os.killpg(job.process.pid, 0)
                except ProcessLookupError:
                    known_empty = True
            joined = job.collector.done()
            if joined and not job.collector.cancelled() and job.collector.exception() is None:
                observation = job.collector.result()
            streams_joined = all(t.done() for t in job.stream_tasks)
            if joined and streams_joined and job.process.returncode is not None and job.pidfd >= 0:
                os.close(job.pidfd)
                job.pidfd = -1
            for task in job.stream_tasks:
                if task.done() and not task.cancelled():
                    task.exception()
            closed = (
                joined
                and streams_joined
                and job.process.returncode is not None
                and job.pidfd == -1
                and job.stdout_eof
                and job.stderr_eof
                and known_empty
            )
            remaining = deadline - loop.time()
            if closed or remaining <= 0:
                break
            unfinished = {t for t in [job.collector, *job.stream_tasks] if not t.done()}
            if unfinished:
                # wait不取消collector；unknown仍保留原Task/流/pidfd，后续close可继续join。
                await asyncio.wait(unfinished, timeout=remaining)
            else:
                await asyncio.sleep(min(0.005, remaining))
        job.cleanup_receipts.append(
            {
                "started_at": started,
                "checked_at": loop.time(),
                "deadline": deadline,
                "pid": job.process.pid,
                "wait_returncode": job.process.returncode,
                "collector_joined": job.collector.done(),
                "stream_tasks_joined": all(t.done() for t in job.stream_tasks),
                "stdout_eof": job.stdout_eof,
                "stderr_eof": job.stderr_eof,
                "pidfd_closed": job.pidfd == -1,
                "group_empty": known_empty,
                "closed": closed,
                "reason": None if closed else "cleanup_unknown",
            }
        )
        # 执行快照不含本次清理补充字段，closed与清理收据独立表达资源结论。
        return CleanupResult(closed, None if closed else "cleanup_unknown", clone(observation))

    async def close(self) -> CleanupResult:
        previous = self._close_task
        if (
            previous is None
            or previous.done()
            and (
                previous.cancelled()
                or previous.exception() is not None
                or not previous.result().closed
            )
        ):
            self._close_task = asyncio.create_task(self._close())
        assert self._close_task is not None
        return await settled(self._close_task)

    async def _close_work(self, job: ProcessWork, lease: object) -> CleanupResult:
        if job.lease is not lease or self.jobs.get(job.ref.id) is not job:
            raise ServiceError("permission_denied")
        first = job.cancel_task
        if first is None or not first.done():
            return await self.cancel(job, lease)
        if not first.cancelled() and first.exception() is None and first.result().closed:
            return await settled(first)
        previous = job.close_task
        if (
            previous is None
            or previous.done()
            and (
                previous.cancelled()
                or previous.exception() is not None
                or not previous.result().closed
            )
        ):
            # 显式观察另存Task；并发的自动cancel仍可立即核实原unknown，不等待新预算。
            job.close_task = asyncio.create_task(self._cancel(job))
        assert job.close_task is not None
        return await settled(job.close_task)

    async def _close(self) -> CleanupResult:
        results = await asyncio.gather(
            *(self._close_work(j, j.lease) for j in self.jobs.values()), return_exceptions=True
        )
        self.closed = all(isinstance(r, CleanupResult) and r.closed for r in results)
        return CleanupResult(self.closed, None if self.closed else "cleanup_unknown")

    def retire(self, job: ProcessWork, lease: object, receipt: dict[str, Any]) -> None:
        if job.lease is not lease or self.jobs.get(job.ref.id) is not job:
            raise ServiceError("permission_denied")
        if job.collector is None:
            raise ServiceError("execution_unknown")
        for task in [job.collector, *job.stream_tasks, job.cancel_task, job.close_task]:
            if task is not None:
                if not task.done() or task.cancelled():
                    raise ServiceError("execution_unknown")
                if task.exception() is not None:
                    raise ServiceError("execution_unknown")
        observation = job.collector.result()
        empty = (observation.get("process") or {}).get("group_empty") is True or any(
            r.get("group_empty") is True for r in job.cleanup_receipts
        )
        if (
            job.process.returncode is None
            or job.pidfd != -1
            or not job.stdout_eof
            or not job.stderr_eof
            or not empty
        ):
            raise ServiceError("execution_unknown")
        expected = {
            "execution_ref": job.ref.id,
            "task_id": job.prepared.spec.task_id,
            "case_id": job.prepared.spec.case_id,
            "repetition": job.prepared.spec.repetition,
            "pid": job.process.pid,
            "wait_returncode": job.process.returncode,
            "pidfd_closed": True,
            "stdout_eof": True,
            "stderr_eof": True,
            "collector_joined": True,
            "stream_tasks_joined": True,
            "cleanup_tasks_joined": True,
            "group_empty": True,
            "owned_lease": True,
            "closed": True,
        }
        if any(receipt.get(k) != v for k, v in expected.items()):
            raise ServiceError("facts_corrupt")
        del self.jobs[job.ref.id]
