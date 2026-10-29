"""宿主固定注册的单个对账参考进程owner；Dataset不提供argv/env。"""

from __future__ import annotations

import asyncio
import hashlib
import os
import sys
from pathlib import Path
from typing import Any, cast

from krim_agent.contracts import ServiceError, fingerprint

from ..domain import (
    CleanupResult,
    ExecutionRef,
    Observation,
    PreparedTrial,
    TrialSpec,
    clone,
    execution_conditions,
)
from ..evidence import bounded_files, initial_files
from ..lifecycle import settled
from .process import ProcessOwner
from .process import ProcessWork as _ProcessWork

# 保留原导入名，实际资源记录只由共享owner签发。

ProcessWork = _ProcessWork


class LocalProcessAdapter:
    def __init__(self, root: Path, owner: ProcessOwner | None = None) -> None:
        self.root = root.absolute()
        self.owner = owner or ProcessOwner()
        self._owns_owner = owner is None
        self._lease = object()
        self._owner_methods = self.owner.identities()
        self.worker = Path(__file__).resolve().parents[1] / "examples/reconcile.py"
        self.jobs: dict[str, ProcessWork] = {}
        self.execute_calls = 0
        self.closed = False
        self._close_task: asyncio.Task[CleanupResult] | None = None
        self._conditions = self._current_conditions()
        self._descriptor = {
            "key": "reconcile-reference",
            "version": "reconcile-reference/1",
            "kind": "non-model-reference",
            "max_concurrency": 2,
            "execution_conditions": clone(self._conditions),
            "implementation_digest": hashlib.sha256(self.worker.read_bytes()).hexdigest(),
            "adapter_digest": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "python_digest": hashlib.sha256(
                Path(sys.executable).resolve().read_bytes()
            ).hexdigest(),
        }
        self._methods = self._identities()

    def _current_conditions(self) -> dict[str, Any]:
        executable = Path(sys.executable)
        return execution_conditions(
            {
                "schema_version": "local-process/1",
                "python": {
                    "executable": str(executable),
                    "realpath": str(executable.resolve()),
                    "sha256": hashlib.sha256(executable.resolve().read_bytes()).hexdigest(),
                    "prefix": sys.prefix,
                    "base_prefix": sys.base_prefix,
                },
                "env": {name: os.environ.get(name) for name in ["PATH", "LANG", "LD_LIBRARY_PATH"]},
            }
        )

    def _identities(self) -> tuple[object, ...]:
        return tuple(
            getattr(getattr(self, name), "__func__", getattr(self, name))
            for name in [
                "descriptor",
                "prepare",
                "execute",
                "collect",
                "cancel",
                "close",
                "retirement_receipt",
                "retire",
            ]
        )

    def descriptor(self) -> dict[str, Any]:
        return cast(dict[str, Any], clone(self._descriptor))

    def assert_current(self, frozen: dict[str, Any]) -> None:
        try:
            current = self._current_conditions()
        except (OSError, ValueError, TypeError, RuntimeError):
            raise ServiceError("target_binding_changed") from None
        if (
            self.closed
            or self.owner.closed
            or self.owner.identities() != self._owner_methods
            or self._identities() != self._methods
            or current != self._conditions
            or self._descriptor["execution_conditions"] != self._conditions
            or fingerprint(self._descriptor) != fingerprint(frozen)
            or hashlib.sha256(self.worker.read_bytes()).hexdigest()
            != self._descriptor["implementation_digest"]
        ):
            raise ServiceError("target_binding_changed")

    async def prepare(self, trial: TrialSpec) -> PreparedTrial:
        directory = self.root / trial.task_id / f"{trial.case_id}-{trial.repetition}"
        directory.mkdir(parents=True, exist_ok=False)
        initial = initial_files(
            directory,
            trial.case["files"] | {"INSTRUCTIONS.md": trial.case["instruction"]},
        )
        (directory / "outputs").mkdir()
        return PreparedTrial(trial, str(directory), initial)

    async def execute(self, prepared: PreparedTrial) -> ExecutionRef:
        self.assert_current(prepared.spec.target)
        argv = [self._conditions["python"]["executable"], "-I", "-B", str(self.worker)]
        env = {k: v for k, v in self._conditions["env"].items() if v is not None}
        job = await self.owner.spawn(
            prepared,
            argv,
            env,
            self._lease,
            self._conditions,
            self._collect,
        )
        self.execute_calls += 1
        self.jobs[job.ref.id] = job
        return job.ref

    async def _collect(self, work: ProcessWork) -> dict[str, Any]:
        facts = await self.owner.observe(work)
        stdout, stderr = facts.pop("stdout"), facts.pop("stderr")
        try:
            artifacts = bounded_files(Path(str(work.prepared.directory)) / "outputs")
        except (ValueError, OSError, UnicodeError) as error:
            artifacts = {}
            facts["artifact_error"] = str(error)
        if not facts["output_complete"]:
            facts["artifact_error"] = "stream_limit"
        return Observation(
            execution_ref=work.ref.id,
            exit_code=facts["wait_returncode"],
            stdout=stdout,
            stderr=stderr,
            artifacts=artifacts,
            process=facts,
            cleanup="closed" if facts["group_empty"] else "unknown",
        ).model_dump(mode="json")

    async def collect(self, ref: ExecutionRef) -> dict[str, Any]:
        job = self.jobs[ref.id]
        assert job.ref is ref and job.collector is not None
        observation = cast(dict[str, Any], clone(await asyncio.shield(job.collector)))
        if observation["cleanup"] != "closed":
            raise ServiceError("execution_unknown")
        return observation

    async def cancel(self, ref: ExecutionRef) -> CleanupResult:
        job = self.jobs[ref.id]
        assert job.ref is ref
        return await self.owner.cancel(job, self._lease)

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

    async def _close(self) -> CleanupResult:
        outcomes = await asyncio.gather(
            *(self.owner._close_work(j, self._lease) for j in self.jobs.values()),
            return_exceptions=True,
        )
        closed = all(isinstance(v, CleanupResult) and v.closed for v in outcomes)
        if self._owns_owner:
            owner = await self.owner.close()
            closed = closed and owner.closed
        self.closed = closed
        return CleanupResult(closed, None if closed else "cleanup_unknown")

    def retirement_receipt(self, ref: ExecutionRef) -> dict[str, Any]:
        job = self.jobs.get(ref.id)
        if job is None or job.ref is not ref or self.owner.jobs.get(ref.id) is not job:
            raise ServiceError("permission_denied")
        if job.lease is not self._lease or job.collector is None:
            raise ServiceError("permission_denied")
        for task in [job.collector, *job.stream_tasks, job.cancel_task, job.close_task]:
            if task is None:
                continue
            if not task.done():
                raise ServiceError("execution_unknown")
            if task.cancelled() or task.exception() is not None:
                raise ServiceError("execution_unknown")
        observation = job.collector.result()
        known_empty = (observation.get("process") or {}).get("group_empty") is True or any(
            r.get("group_empty") is True for r in job.cleanup_receipts
        )
        if (
            job.process.returncode is None
            or job.pidfd != -1
            or not job.stdout_eof
            or not job.stderr_eof
            or not known_empty
        ):
            raise ServiceError("execution_unknown")
        receipt = {
            "schema_version": "local-process-retirement/1",
            "execution_ref": ref.id,
            "task_id": job.prepared.spec.task_id,
            "case_id": job.prepared.spec.case_id,
            "repetition": job.prepared.spec.repetition,
            "pid": job.process.pid,
            "wait_returncode": job.process.returncode,
            "pidfd_closed": job.pidfd == -1,
            "stdout_eof": job.stdout_eof,
            "stderr_eof": job.stderr_eof,
            "collector_joined": job.collector.done(),
            "stream_tasks_joined": all(t.done() for t in job.stream_tasks),
            "cleanup_tasks_joined": all(
                t is None or t.done() for t in (job.cancel_task, job.close_task)
            ),
            "owned_lease": job.lease is self._lease,
            "group_empty": known_empty,
            "cleanup_receipts": clone(job.cleanup_receipts),
            "closed": True,
        }
        from ..domain import neutral_encoded

        neutral_encoded(receipt, max_bytes=16384)
        return receipt

    def retire(self, ref: ExecutionRef, receipt: dict[str, Any]) -> None:
        if self.retirement_receipt(ref) != receipt:
            raise ServiceError("facts_corrupt")
        job = self.jobs[ref.id]
        self.owner.retire(job, self._lease, receipt)
        del self.jobs[ref.id]
