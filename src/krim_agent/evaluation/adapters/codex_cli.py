"""原生聚合ref的资源阶段；Eval Task调度仍只有受保护kernel/runner。"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast

from krim_agent.contracts import ServiceError, fingerprint

from ..domain import CleanupResult, ExecutionRef, Observation, PreparedTrial, TrialSpec, clone
from ..evidence import bounded_roots, initial_files
from ..lifecycle import settled
from ..verifiers import NativeVerifier
from .codex_events import CodexEvents
from .process import ProcessOwner, ProcessWork
from .profiles import NativeProfile


@dataclass
class NativeWork:
    ref: ExecutionRef
    prepared: PreparedTrial
    codec: Any
    active: bool = True
    collector: asyncio.Task[dict[str, Any]] | None = None
    pending: asyncio.Task[ProcessWork] | None = None
    phase: str = "reserved"
    pending_phase: str = ""
    resources: list[tuple[str, ProcessWork]] = field(default_factory=list)
    facts: dict[str, dict[str, Any]] = field(default_factory=dict)
    observation: dict[str, Any] | None = None
    cancel_task: asyncio.Task[CleanupResult] | None = None
    verification: dict[str, Any] | None = None
    finish_codec: Callable[[], dict[str, Any]] | None = field(default=None, repr=False)


class NativeCLIAdapter:
    family = "abstract"
    codec_type: Any = None

    def __init__(
        self,
        profile: NativeProfile,
        owner: ProcessOwner,
        reservation: Callable[[TrialSpec, ExecutionRef], Awaitable[None]],
    ) -> None:
        if profile.family != self.family:
            raise ServiceError("invalid_input")
        self.profile, self.owner = profile, owner
        self._reservation = reservation
        self._reservation_bound = reservation
        self._lease = object()
        self._cancel_process = owner.cancel
        self._observe = owner.observe
        self._owner_methods = owner.identities()
        self.verifier = NativeVerifier()
        self._verifier_methods = (
            getattr(self.verifier.argv, "__func__", self.verifier.argv),
            getattr(self.verifier.result, "__func__", self.verifier.result),
        )
        self._codec_methods = (self.codec_type.feed, self.codec_type.finish)
        self._profile_methods = tuple(
            getattr(getattr(profile, name), "__func__", getattr(profile, name))
            for name in ["descriptor", "assert_current", "argv", "agent_environment"]
        )
        self._descriptor = profile.descriptor()
        self._methods = self._identities()
        self.jobs: dict[str, NativeWork] = {}
        self._prepared: dict[int, PreparedTrial] = {}
        self.execute_calls = 0
        self.ref_calls = 0
        self.closed = False
        self._close_task: asyncio.Task[CleanupResult] | None = None

    def _identities(self) -> tuple[object, ...]:
        return tuple(
            getattr(getattr(self, n), "__func__", getattr(self, n))
            for n in [
                "descriptor",
                "assert_current",
                "prepare",
                "execute",
                "collect",
                "cancel",
                "close",
                "_phase_gate",
            ]
        )

    def descriptor(self) -> dict[str, Any]:
        return cast(dict[str, Any], clone(self._descriptor))

    def assert_current(self, frozen: dict[str, Any]) -> None:
        current = tuple(
            getattr(getattr(self.profile, name), "__func__", getattr(self.profile, name))
            for name in ["descriptor", "assert_current", "argv", "agent_environment"]
        )
        if current != self._profile_methods or self._reservation is not self._reservation_bound:
            raise ServiceError("target_binding_changed")
        self.profile.assert_current()
        if (
            self.closed
            or self.owner.closed
            or self._identities() != self._methods
            or self.owner.identities() != self._owner_methods
            or fingerprint(self._descriptor) != fingerprint(frozen)
            or self.profile.descriptor() != self._descriptor
            or (
                getattr(self.verifier.argv, "__func__", self.verifier.argv),
                getattr(self.verifier.result, "__func__", self.verifier.result),
            )
            != self._verifier_methods
            or (self.codec_type.feed, self.codec_type.finish) != self._codec_methods
        ):
            raise ServiceError("target_binding_changed")
        self.verifier.assert_current()

    async def prepare(self, trial: TrialSpec) -> PreparedTrial:
        self.assert_current(trial.target)
        root = (
            self.profile.binding.database.parent
            / "workspaces"
            / trial.task_id
            / f"{trial.case_id}-{trial.repetition}"
        )
        directory = root / "repo"
        directory.mkdir(parents=True, exist_ok=False)
        initial = initial_files(
            directory, trial.case["files"] | {"INSTRUCTIONS.md": trial.case["instruction"]}
        )
        (directory / "outputs").mkdir()
        (root / "home").mkdir()
        (root / "home/.codex").mkdir()
        (root / "home/.claude").mkdir()
        (root / "control").mkdir()
        (root / "control/initial.json").write_text(json.dumps(initial))
        prepared = PreparedTrial(trial, str(directory), initial)
        self._prepared[id(prepared)] = prepared
        return prepared

    async def execute(self, prepared: PreparedTrial) -> ExecutionRef:
        self.assert_current(prepared.spec.target)
        if self._prepared.pop(id(prepared), None) is not prepared:
            raise ServiceError("permission_denied")
        ref = ExecutionRef(str(uuid.uuid4()))
        codec = self.codec_type()
        self.jobs[ref.id] = NativeWork(ref, prepared, codec, finish_codec=codec.finish)
        self.ref_calls += 1
        return ref

    async def _phase_gate(self, name: str) -> None:
        await asyncio.sleep(0)

    def _fence(self, job: NativeWork) -> None:
        if not job.active:
            raise asyncio.CancelledError
        self.assert_current(job.prepared.spec.target)

    def _register(self, job: NativeWork, phase: str, process: ProcessWork) -> None:
        if all(work is not process for _, work in job.resources):
            job.resources.append((phase, process))
            if phase == "agent":
                self.execute_calls += 1

    async def _adopt(self, job: NativeWork) -> ProcessWork:
        assert job.pending is not None
        try:
            process = await settled(job.pending)
        except BaseException:
            # Popen已经发生而回程/后fence失败时，同步签发的实际对象仍由本job持有。
            for phase, process in reversed(job.resources):
                if phase == job.pending_phase:
                    return process
            raise
        self._register(job, job.pending_phase, process)
        return process

    async def _stage(
        self,
        job: NativeWork,
        phase: str,
        argv: list[str],
        *,
        prompt: bytes | None = None,
    ) -> dict[str, Any]:
        self._fence(job)
        await self._reservation(job.prepared.spec, job.ref)
        self._fence(job)
        job.phase = phase
        await self._phase_gate(phase)
        self._fence(job)
        await self._reservation(job.prepared.spec, job.ref)
        self._fence(job)
        directory = Path(str(job.prepared.directory))
        env, secrets = (
            self.profile.agent_environment()
            if phase == "agent"
            else (
                dict(self.profile.environment),
                self.profile.secrets,
            )
        )
        env["HOME"] = str(directory.parent / "home")
        env["CODEX_HOME"] = str(directory.parent / "home/.codex")
        env["CLAUDE_CONFIG_DIR"] = str(directory.parent / "home/.claude")
        env["PYTHONDONTWRITEBYTECODE"] = "1"
        if phase != "agent":
            env = {
                k: v
                for k, v in env.items()
                if k
                not in {
                    "OPENAI_API_KEY",
                    "ANTHROPIC_API_KEY",
                    "ANTHROPIC_AUTH_TOKEN",
                }
            }
        callback = job.codec.feed if phase == "agent" else None

        async def observe(work: ProcessWork) -> dict[str, Any]:
            return await self._observe(work, secrets=secrets, on_stdout=callback)

        job.pending_phase = phase
        job.pending = asyncio.create_task(
            self.owner.spawn(
                job.prepared,
                argv,
                env,
                self._lease,
                self._descriptor["execution_conditions"],
                observe,
                stdin_data=prompt,
                guard=lambda: self._fence(job),
                on_started=lambda process: self._register(job, phase, process),
            )
        )
        try:
            await asyncio.shield(job.pending)
            work = await self._adopt(job)
            job.pending = None
            assert work.collector is not None
            async with asyncio.timeout(150 if phase == "agent" else 10):
                facts = await asyncio.shield(work.collector)
            job.facts[phase] = facts
            self._fence(job)
            return facts
        finally:
            if job.pending is not None:
                try:
                    await self._adopt(job)
                finally:
                    job.pending = None

    async def _settle(self, job: NativeWork) -> bool:
        if job.pending is not None:
            try:
                await self._adopt(job)
            finally:
                job.pending = None
        closed = True
        for phase, work in job.resources:
            result = await self._cancel_process(work, self._lease)
            closed = closed and result.closed
            if result.observation is not None:
                job.facts[phase] = result.observation
        return closed

    def _observation(self, job: NativeWork, closed: bool) -> dict[str, Any]:
        assert job.finish_codec is not None
        native = job.finish_codec()
        agent = job.facts.get("agent", {})
        directory = Path(str(job.prepared.directory))
        artifact_error = None
        try:
            artifacts = bounded_roots(directory)
        except (OSError, ValueError, UnicodeError) as error:
            artifacts = {}
            artifact_error = type(error).__name__
        stages = []
        for phase, work in job.resources:
            actual = job.facts.get(phase, {})
            record = {k: v for k, v in actual.items() if k not in {"stdout", "stderr"}}
            for key in ["stdout", "stderr"]:
                text = actual.get(key)
                record[key + "_sha256"] = (
                    hashlib.sha256(
                        text.encode(),
                    ).hexdigest()
                    if isinstance(text, str)
                    else None
                )
            stages.append({"phase": phase, "process_ref": work.ref.id, "facts": record})
        process = {
            "stages": stages,
            "uid": os.getuid(),
            "fixture_only": self.profile.fixture_only,
            "output_complete": agent.get("output_complete", False),
            "artifact_error": artifact_error,
        }
        return Observation(
            execution_ref=job.ref.id,
            exit_code=agent.get("wait_returncode"),
            stdout=agent.get("stdout", ""),
            stderr=agent.get("stderr", ""),
            artifacts=artifacts,
            process=process,
            cleanup="closed" if closed else "unknown",
            native=native,
            verification=job.verification,
            token_usage=native["usage"],
            cost=native["cost"],
            usage_complete=native["usage_complete"],
        ).model_dump(mode="json")

    async def _run(self, job: NativeWork) -> dict[str, Any]:
        directory = Path(str(job.prepared.directory))
        initial_file = directory.parent / "control/initial.json"
        try:
            setup = await self._stage(job, "setup", ["/usr/bin/git", "init", "-q", "."])
            if setup["wait_returncode"] != 0:
                job.verification = {
                    "passed": False,
                    "environment_error": True,
                    "reason": "setup_failed",
                }
            else:
                initial = await self._stage(
                    job,
                    "initial_verifier",
                    self.verifier.argv(self.profile, directory, initial_file, final=False),
                )
                initial_result = self.verifier.result(initial)
                if (
                    initial_result.get("environment_error")
                    or initial_result.get("passed") is not False
                    or initial_result.get("baseline_checks_complete") is not True
                ):
                    job.verification = {
                        "passed": False,
                        "environment_error": True,
                        "reason": "initial_not_red",
                    }
                else:
                    prompt = (
                        None
                        if self.profile.fixture_only
                        else job.prepared.spec.case["instruction"].encode()
                    )
                    await self._stage(job, "agent", self.profile.argv(directory), prompt=prompt)
                    final = await self._stage(
                        job,
                        "verifier",
                        self.verifier.argv(self.profile, directory, initial_file, final=True),
                    )
                    job.verification = self.verifier.result(final)
        except asyncio.CancelledError:
            pass
        finally:
            closed = await settled(self._settle(job))
            job.observation = self._observation(job, closed)
        return cast(dict[str, Any], clone(job.observation))

    async def collect(self, ref: ExecutionRef) -> dict[str, Any]:
        job = self.jobs[ref.id]
        if job.ref is not ref:
            raise ServiceError("permission_denied")
        if job.collector is None:
            job.collector = asyncio.create_task(self._run(job))
        try:
            return cast(dict[str, Any], clone(await asyncio.shield(job.collector)))
        except asyncio.CancelledError:
            # 在原Runner退出timeout/uncancel之前，同步撤销所有后续effect。
            job.active = False
            raise

    async def cancel(self, ref: ExecutionRef) -> CleanupResult:
        job = self.jobs[ref.id]
        if job.ref is not ref:
            raise ServiceError("permission_denied")
        job.active = False
        if job.cancel_task is None:
            job.cancel_task = asyncio.create_task(self._cancel(job))
        return await settled(job.cancel_task)

    async def _cancel(self, job: NativeWork) -> CleanupResult:
        if job.collector is not None:
            if not job.collector.done():
                job.collector.cancel()
            await asyncio.gather(job.collector, return_exceptions=True)
        closed = await self._settle(job)
        if job.observation is None:
            job.observation = self._observation(job, closed)
        return CleanupResult(closed, None if closed else "cleanup_unknown", clone(job.observation))

    async def close(self) -> CleanupResult:
        if self._close_task is None:
            self._close_task = asyncio.create_task(self._close())
        return await settled(self._close_task)

    async def _close(self) -> CleanupResult:
        results = await asyncio.gather(
            *(self.cancel(j.ref) for j in self.jobs.values()),
            return_exceptions=True,
        )
        self.closed = all(isinstance(r, CleanupResult) and r.closed for r in results)
        return CleanupResult(self.closed, None if self.closed else "cleanup_unknown")


class CodexCLIAdapter(NativeCLIAdapter):
    family = "codex"
    codec_type = CodexEvents
