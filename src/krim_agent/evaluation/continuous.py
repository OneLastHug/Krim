"""持续评测薄门面；所有Task仍交给同一个EvalKernel。"""

from __future__ import annotations

import time
from typing import Any, cast

from krim_agent.contracts import ServiceError

from .continuous_types import ConfirmationRequest, ContinuousRequest, FreezeConfirmed, ImportRequest
from .domain import clone, neutral_encoded, neutral_fingerprint
from .kernel import EvalKernel
from .repository import Repository


class ContinuousEvaluation:
    def __init__(
        self,
        repository: Repository,
        kernel: EvalKernel,
        *,
        scope: str = "local",
        actor: str = "local-user",
        sources: dict[str, Any] | None = None,
        clock: Any = None,
    ) -> None:
        self.repository, self.kernel = repository, kernel
        self.scope, self.actor = scope, actor
        if clock is not None:
            if not callable(clock):
                raise ServiceError("invalid_input", 422)
            repository._continuous_clock = clock
        self.clock = getattr(repository, "_continuous_clock", time.time)
        self.sources: dict[str, Any] = dict(sources or {})
        self.sampling = {
            "version": "explicit-id-hash/1",
            "salt_id": "default",
            "numerator": 1,
            "denominator": 1,
        }
        self.redaction = {
            "version": "continuous-redact/1",
            "artifact_paths": [],
            "include_text": False,
            "include_arguments": False,
        }

    async def import_sources(self, request: ImportRequest) -> list[dict[str, Any]]:
        frozen = ImportRequest.model_validate(request.model_dump(mode="json"))
        if frozen.scope != self.scope or frozen.source_alias not in self.sources:
            raise ServiceError("not_found", 404)
        reader: Any = self.sources[frozen.source_alias]
        if reader is None:
            raise ServiceError("not_found", 404)
        selected_reader = cast(Any, reader)
        method = getattr(selected_reader.read, "__func__", selected_reader.read)
        binding = reader.binding()
        policies = clone({"sampling": self.sampling, "redaction": self.redaction})
        actor, scope = self.actor, self.scope

        def guard() -> None:
            if (
                self.scope != scope
                or self.actor != actor
                or self.sources.get(frozen.source_alias) is not reader
                or getattr(selected_reader.read, "__func__", selected_reader.read) is not method
                or selected_reader.binding() != binding
                or {"sampling": self.sampling, "redaction": self.redaction} != policies
            ):
                raise ServiceError("target_binding_changed")

        sampling = policies["sampling"]
        n, d = sampling["numerator"], sampling["denominator"]
        if type(n) is not int or type(d) is not int or not 0 <= n <= d <= 1000000 or d == 0:
            raise ServiceError("invalid_input", 422)
        request_hash = neutral_fingerprint(
            {
                "request": frozen.model_dump(mode="json"),
                "policies": policies,
                "feedback_actor": actor
                if any(s.feedback is not None for s in frozen.sources)
                else None,
            }
        )
        old = await self.repository.import_observations(scope, frozen.request_key, request_hash)
        guard()
        if old is not None:
            return old
        rows = []
        for selection in frozen.sources:
            value = await reader.read(scope, selection.model_dump(mode="json"))
            guard()
            neutral_encoded(value, max_bytes=10 * 1048576)
            source = value["source"]
            if neutral_fingerprint(value["output"]) != source["evidence_hash"]:
                raise ServiceError("facts_corrupt")
            sample = neutral_fingerprint({"source": source, "salt_id": sampling["salt_id"]})
            if int(sample, 16) % d >= n:
                continue
            output = clone(value["output"])
            reasons = []
            if not value["terminal"]:
                reasons.append("not_terminal")
            if value["case"] is None:
                reasons.append("context_missing")
            if output is None:
                reasons.append("output_missing")
            elif output["cleanup"] != "closed":
                reasons.append("cleanup_unknown")
            if output is not None:
                output["stdout"] = output["stderr"] = None
                output["artifacts"] = {
                    k: v
                    for k, v in output["artifacts"].items()
                    if k in policies["redaction"]["artifact_paths"]
                }
                if output["process"] is not None:
                    output["process"] = {
                        k: v
                        for k, v in output["process"].items()
                        if k in {"output_complete", "artifact_error", "wait_returncode"}
                    }
                for field in ["native", "target_result"]:
                    if not isinstance(output.get(field), dict):
                        continue
                    original = output[field]
                    clean = {
                        k: original[k]
                        for k in [
                            "schema_version",
                            "complete",
                            "status",
                            "code",
                            "run_id",
                            "model_steps",
                        ]
                        if k in original
                    }
                    if policies["redaction"]["include_text"] and "text" in original:
                        clean["text"] = original["text"]
                    if isinstance(original.get("tools"), list):
                        names = [
                            "kind",
                            "name",
                            "status",
                            "exit_code",
                            "executor_started",
                            "is_error",
                            "result_available",
                        ]
                        clean["tools"] = [
                            {k: tool[k] for k in names if k in tool}
                            | (
                                {"arguments": tool.get("arguments")}
                                if policies["redaction"]["include_arguments"]
                                else {}
                            )
                            for tool in original["tools"]
                        ]
                    output[field] = clean
                if isinstance(output.get("verification"), dict):
                    proof = output["verification"]
                    output["verification"] = {
                        k: proof[k]
                        for k in [
                            "schema_version",
                            "phase",
                            "passed",
                            "environment_error",
                            "checks",
                        ]
                        if k in proof
                    }
            identity = neutral_fingerprint({"scope": scope, "source": source, "policies": policies})
            row = {
                "kind": "observation",
                "id": identity,
                "scope": scope,
                "source": source,
                "target": value["target"],
                "environment": value["environment"],
                "eligibility": "ineligible"
                if "not_terminal" in reasons
                else "needs_context"
                if reasons
                else "eligible",
                "reasons": reasons or ["eligible"],
                "policies": policies,
                "output": output,
                "context_hash": neutral_fingerprint(
                    None
                    if value["case"] is None
                    else {k: value["case"][k] for k in ["instruction", "files", "verifier"]}
                ),
                "availability": {
                    "artifacts": list(policies["redaction"]["artifact_paths"]),
                    "text": policies["redaction"]["include_text"],
                    "arguments": policies["redaction"]["include_arguments"],
                },
            }
            row["source_proof"] = {
                "terminal": value["terminal"],
                "complete": source.get("item_status") == "finished"
                if source.get("kind") == "trial"
                else source.get("facts_complete") is True and source.get("trace_complete") is True,
                "eligibility": row["eligibility"],
                "reasons": clone(row["reasons"]),
                "context_hash": row["context_hash"],
                "known_hard_failure": source.get("item_quality") == "failed"
                or source.get("status") == "failed",
                "environment": clone(value["environment"]),
            }
            row["content_hash"] = neutral_fingerprint(row)
            if selection.feedback is not None:
                row["_feedback"] = selection.feedback.model_dump(mode="json") | {"actor": actor}
            rows.append(row)
        result = await self.repository.import_observations(
            scope, frozen.request_key, request_hash, rows, guard
        )
        guard()
        if result is None:
            raise ServiceError("facts_corrupt")
        return result

    async def submit(self, request: ContinuousRequest) -> dict[str, Any]:
        frozen = ContinuousRequest.model_validate(request.model_dump(mode="json"))
        if frozen.scope != self.scope:
            raise ServiceError("not_found", 404)
        value = frozen.model_dump(mode="json")
        scope, actor, repository, kernel = self.scope, self.actor, self.repository, self.kernel

        def guard() -> None:
            if (
                (self.scope, self.actor) != (scope, actor)
                or self.repository is not repository
                or self.kernel is not kernel
                or kernel.repository is not repository
            ):
                raise ServiceError("target_binding_changed")

        old = await repository.by_key(value)
        guard()
        if old is not None:
            if old["request_hash"] != neutral_fingerprint(value):
                raise ServiceError("key_conflict")
            return old
        result = await kernel.submit(value, host_guard=guard)
        guard()
        return result

    async def confirm_case(self, request: ConfirmationRequest) -> dict[str, Any]:
        frozen = ConfirmationRequest.model_validate(request.model_dump(mode="json"))
        if frozen.scope != self.scope:
            raise ServiceError("not_found", 404)
        scope, actor, repository = self.scope, self.actor, self.repository

        def guard() -> None:
            if self.scope != scope or self.actor != actor or self.repository is not repository:
                raise ServiceError("target_binding_changed")

        result = await repository.confirm_candidate(frozen.model_dump(mode="json"), actor, guard)
        guard()
        return result

    async def freeze_dataset(self, request: FreezeConfirmed) -> dict[str, Any]:
        frozen = FreezeConfirmed.model_validate(request.model_dump(mode="json"))
        if frozen.scope != self.scope:
            raise ServiceError("not_found", 404)
        scope, actor, repository = self.scope, self.actor, self.repository

        def guard() -> None:
            if self.scope != scope or self.actor != actor or self.repository is not repository:
                raise ServiceError("target_binding_changed")

        result = await repository.freeze_confirmed(frozen.model_dump(mode="json"), guard)
        guard()
        return result

    async def create_schedule(self, spec: Any) -> dict[str, Any]:
        from .continuous_types import ScheduleSpec

        frozen = ScheduleSpec.model_validate(spec.model_dump(mode="json"))
        if frozen.scope != self.scope:
            raise ServiceError("not_found", 404)
        scope, actor, repository = self.scope, self.actor, self.repository

        def guard() -> None:
            if (self.scope, self.actor) != (scope, actor) or self.repository is not repository:
                raise ServiceError("target_binding_changed")

        result = await repository.freeze_schedule(frozen.model_dump(mode="json"), actor, guard)
        guard()
        return result

    async def tick(self, schedule_id: str) -> dict[str, Any]:
        from .continuous_types import window_request

        scope, actor, repository, clock = self.scope, self.actor, self.repository, self.clock
        kernel = self.kernel

        def guard() -> None:
            if (
                (self.scope, self.actor) != (scope, actor)
                or self.repository is not repository
                or self.clock is not clock
                or self.kernel is not kernel
                or kernel.repository is not repository
                or getattr(repository, "_continuous_clock", time.time) is not clock
            ):
                raise ServiceError("target_binding_changed")

        row = await repository.schedule(scope, schedule_id)
        guard()
        spec = row["spec"]
        index = int((clock() - spec["epoch"]) // spec["interval_seconds"])
        if 0 <= index < spec["max_windows"]:
            request = window_request(row, index)
            old = await repository.by_key(request.model_dump(mode="json"))
            guard()
            if old is not None:
                if old["request_hash"] != neutral_fingerprint(request.model_dump(mode="json")):
                    raise ServiceError("key_conflict")
                return old
        observed = await repository.schedule(scope, schedule_id, observe=True, guard=guard)
        guard()
        if observed["spec_hash"] != row["spec_hash"]:
            raise ServiceError("snapshot_changed")
        if not observed["enabled"] or not 0 <= index < spec["max_windows"]:
            raise ServiceError("closed")
        result = await self.submit(window_request(observed, index))
        guard()
        return result

    async def disable(self, schedule_id: str, revision: int, key: str) -> dict[str, Any]:
        scope, repository = self.scope, self.repository

        def guard() -> None:
            if self.scope != scope or self.repository is not repository:
                raise ServiceError("target_binding_changed")

        result = await repository.disable_schedule(scope, schedule_id, revision, key, guard)
        guard()
        return result
