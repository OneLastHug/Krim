"""唯一普通进程资源owner；只拥有实际Popen/流/pidfd，不调度Eval Task。"""

from __future__ import annotations

import asyncio
import json
import os
import signal
import time
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
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


class ProcessOwner:
    def __init__(self) -> None:
        self.jobs: dict[str, ProcessWork] = {}
        self.closed = False
        self._close_task: asyncio.Task[CleanupResult] | None = None
        self.spawn_calls = 0

    def identities(self) -> tuple[object, ...]:
        return tuple(
            getattr(getattr(self, n), "__func__", getattr(self, n))
            for n in ["spawn", "observe", "cancel", "close"]
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
            tail = redactor.feed(b"", final=True)
            if callback is not None:
                try:
                    callback(tail)
                except ValueError:
                    pass
            data.extend(tail[: max(0, limit - len(data))])
            return bytes(data), overflow

        try:
            stdout, stderr, code = await asyncio.gather(
                read(process.stdout, 262144, on_stdout),
                read(process.stderr, 131072),
                process.wait(),
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
            if work.pidfd >= 0:
                os.close(work.pidfd)
                work.pidfd = -1

    async def cancel(self, job: ProcessWork, lease: object) -> CleanupResult:
        if job.lease is not lease or self.jobs.get(job.ref.id) is not job:
            raise ServiceError("permission_denied")
        if job.cancel_task is None:
            job.cancel_task = asyncio.create_task(self._cancel(job))
        return await settled(job.cancel_task)

    async def _cancel(self, job: ProcessWork) -> CleanupResult:
        assert job.collector is not None
        known_empty = False
        if (
            job.collector.done()
            and not job.collector.cancelled()
            and job.collector.exception() is None
        ):
            prior = job.collector.result()
            known_empty = prior.get("process", prior).get("group_empty", False)
        # 已确认消失的旧group只返回收据，绝不以旧PID再发信号。
        if not known_empty:
            try:
                os.killpg(job.process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        await asyncio.gather(job.collector, return_exceptions=True)
        observation = None
        if not job.collector.cancelled() and job.collector.exception() is None:
            observation = job.collector.result()
        deadline = time.monotonic() + 30
        group_empty = known_empty
        while not group_empty:
            try:
                os.killpg(job.process.pid, 0)
            except ProcessLookupError:
                group_empty = True
                break
            if time.monotonic() >= deadline:
                break
            await asyncio.sleep(0.005)
        closed = job.process.returncode is not None and job.pidfd == -1 and group_empty
        if observation is not None:
            observation = clone(observation)
            facts = observation.get("process", observation)
            facts["cleanup_group_empty"] = group_empty
            facts["cleanup_checked_at"] = time.time()
            if "cleanup" in observation:
                observation["cleanup"] = "closed" if closed else "unknown"
        return CleanupResult(closed, None if closed else "cleanup_unknown", observation)

    async def close(self) -> CleanupResult:
        if (
            self._close_task is None
            or self._close_task.done()
            and self._close_task.exception() is not None
        ):
            self._close_task = asyncio.create_task(self._close())
        return await settled(self._close_task)

    async def _close(self) -> CleanupResult:
        results = await asyncio.gather(
            *(self.cancel(j, j.lease) for j in self.jobs.values()), return_exceptions=True
        )
        self.closed = all(isinstance(r, CleanupResult) and r.closed for r in results)
        return CleanupResult(self.closed, None if self.closed else "cleanup_unknown")
