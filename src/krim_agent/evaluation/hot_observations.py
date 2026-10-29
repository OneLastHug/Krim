"""有限热交付门面草稿：借用原Repository/Kernel，不持有后台资源。"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, cast

from krim_agent.contracts import ServiceError

from .domain import clone, neutral_fingerprint
from .hot_types import (
    ActorViewBinding,
    Claim,
    OutboxPort,
    UnitReceipt,
    import_request,
    request_hash,
)


class HotObservationFlow:
    def __init__(
        self,
        port: OutboxPort,
        repository: Any,
        kernel: Any,
        destination_id: str,
        views: Callable[[str, int, Claim], ActorViewBinding],
        view_binding: Callable[[], tuple[object, ...]],
        observer_generation: int,
    ) -> None:
        self.port, self.repository, self.kernel = port, repository, kernel
        self.destination_id = destination_id
        self.views, self.view_binding = views, view_binding
        self.observer_generation = observer_generation

    def binding(self) -> tuple[object, ...]:
        return (
            id(self.port),
            self.port.binding(),
            id(self.repository),
            id(self.kernel),
            id(self.kernel.repository),
            self.destination_id,
            id(self.views),
            self.view_binding(),
            self.observer_generation,
        )

    def _guard(self, frozen: tuple[object, ...]) -> None:
        if self.binding() != frozen or self.kernel.repository is not self.repository:
            raise ServiceError("target_binding_changed")

    async def _receipt(
        self,
        request: Any,
        binding: ActorViewBinding,
        rows: list[dict[str, Any]],
        guard: Callable[[], None],
    ) -> UnitReceipt:
        binding.check()
        if len(rows) != 1:
            raise ServiceError("facts_corrupt")
        row = rows[0]
        candidate_id = row.get("candidate_id", row["id"])
        candidate = await self.repository.candidate(request.scope, candidate_id)
        guard()
        binding.check()
        feedback = request.sources[0].feedback
        if (
            candidate["observation_id"] != row["id"]
            or candidate["observation_hash"] != row["content_hash"]
        ):
            raise ServiceError("facts_corrupt")
        if feedback is not None:
            expected = {"key": feedback.key, "reason": feedback.reason, "actor": binding.actor}
            if candidate.get("signals") != [expected]:
                raise ServiceError("facts_corrupt")
        return UnitReceipt(
            key=request.request_key,
            request_hash=request_hash(request, binding.view),
            actor=binding.actor,
            feedback_key=None if feedback is None else feedback.key,
            observation_id=row["id"],
            observation_hash=row["content_hash"],
            candidate_id=candidate_id,
            candidate_hash=_imported_candidate_hash(candidate, row, feedback, binding.actor),
        )

    async def _units(
        self, claim: Claim, *, readonly: bool, guard: Callable[[], None]
    ) -> list[UnitReceipt]:
        from .continuous_types import ImportRequest
        from .hot_types import CaptureRecord, _unit_hash

        if (
            type(claim.generation) is not int
            or claim.generation < 1
            or type(claim.revision) is not int
            or not 0 <= claim.revision <= 8
        ):
            raise ServiceError("facts_corrupt")
        claim = Claim(
            CaptureRecord.model_validate(claim.record.model_dump(mode="json")),
            claim.generation,
            claim.revision,
            claim.token,
        )
        frozen_methods = _flow_methods(self)

        def check() -> None:
            guard()
            if _flow_methods(self) != frozen_methods:
                raise ServiceError("target_binding_changed")

        check()
        saved = await self.port.units(claim)
        check()
        record = claim.record
        if len(record.feedback) != claim.revision:
            raise ServiceError("facts_corrupt")
        actors = [(record.policy.observer, self.observer_generation, None)] + [
            (item.actor, item.generation, item) for item in record.feedback
        ]
        prepared: list[tuple[ImportRequest, ActorViewBinding, dict[str, Any]]] = []
        for actor, generation, feedback in actors:
            binding = self.views(actor, generation, claim)
            binding.check()
            check()
            if (
                binding.actor != actor
                or binding.generation != generation
                or binding.view.repository is not self.repository
                or binding.view.kernel is not self.kernel
            ):
                raise ServiceError("target_binding_changed")
            requested = import_request(record, self.destination_id, actor, feedback)
            policies = clone(
                {"sampling": binding.view.sampling, "redaction": binding.view.redaction}
            )
            old = saved.get(requested.request_key)
            request = requested if old is None else ImportRequest.model_validate(old["request"])
            digest = _unit_hash(request, policies, actor)
            if request.model_dump(mode="json") != requested.model_dump(mode="json") or (
                old is not None
                and (
                    old.get("generation") != generation
                    or old.get("actor") != actor
                    or old.get("policies") != policies
                    or old.get("request_hash") != digest
                )
            ):
                raise ServiceError("target_binding_changed")
            value = {
                "request": request.model_dump(mode="json"),
                "request_hash": digest,
                "actor": actor,
                "generation": generation,
                "policies": policies,
                "destination_id": self.destination_id,
                "state": "not_started",
            }
            prepared.append((request, binding, value))
        expected_keys = {request.request_key for request, _, _ in prepared}
        if set(saved) - expected_keys:
            raise ServiceError("facts_corrupt")
        receipts: dict[str, UnitReceipt] = {}
        # 先核所有可能已提交的原键；一个未知就停止，不启动其他新单元。
        for request, binding, value in prepared:
            old = saved.get(request.request_key)
            if old is None or old.get("state") == "not_started":
                continue
            if old.get("state") not in {"submitted", "complete"}:
                raise ServiceError("execution_unknown")
            rows = await self.repository.import_observations(
                request.scope, request.request_key, value["request_hash"], rows=None, guard=check
            )
            check()
            binding.check()
            if rows is None:
                raise ServiceError("execution_unknown")
            receipt = await self._receipt(request, binding, rows, check)
            check()
            if old.get("state") == "complete" and old.get("receipt_hash") != neutral_fingerprint(
                receipt.model_dump(mode="json")
            ):
                raise ServiceError("facts_corrupt")
            receipts[request.request_key] = receipt
        if readonly:
            if set(receipts) != expected_keys:
                raise ServiceError("execution_unknown")
            return [receipts[request.request_key] for request, _, _ in prepared]
        # 一次源事务先冻结整个有序集合；not_started必须有持久证明。
        first = prepared[0]
        await self.port.save_unit(
            claim,
            first[0].request_key,
            first[2] | {"manifest": [value for _, _, value in prepared]},
        )
        check()
        saved = await self.port.units(claim)
        check()
        if set(saved) != expected_keys:
            raise ServiceError("facts_corrupt")
        for request, binding, value in prepared:
            binding.check()
            check()
            key = request.request_key
            if key not in receipts:
                if saved[key].get("state") != "not_started":
                    raise ServiceError("execution_unknown")
                await self.port.save_unit(claim, key, value | {"state": "submitted"})
                check()
                binding.check()
                rows = await binding.view.import_sources(request)
                check()
                binding.check()
                receipts[key] = await self._receipt(request, binding, rows, check)
                check()
            await self.port.save_unit(
                claim,
                key,
                value | {"state": "complete", "receipt": receipts[key].model_dump(mode="json")},
            )
            check()
        result = [receipts[request.request_key] for request, _, _ in prepared]
        if len({receipt.observation_id for receipt in result}) != 1:
            raise ServiceError("facts_corrupt")
        return result

    async def recover_claim(self, claim: Claim) -> list[UnitReceipt]:
        frozen = self.binding()
        methods = _flow_methods(self)

        def check() -> None:
            self._guard(frozen)
            if _flow_methods(self) != methods:
                raise ServiceError("target_binding_changed")

        check()
        return await self._units(claim, readonly=True, guard=check)

    async def consume_once(self, *, limit: int = 16) -> list[UnitReceipt]:
        frozen = self.binding()
        methods = _flow_methods(self)

        def check() -> None:
            self._guard(frozen)
            if _flow_methods(self) != methods:
                raise ServiceError("target_binding_changed")

        check()
        claims = await self.port.claim(limit=limit)
        check()
        result = []
        for claim in claims:
            receipts = await self._units(claim, readonly=False, guard=check)
            check()
            await self.port.ack(claim, receipts)
            check()
            result.extend(receipts)
        return result

    def request_manifest(self, claim: Claim) -> dict[str, Any]:
        """只用于诊断原请求身份；不会生成目标效果或执行回放。"""
        record = claim.record
        requests = [import_request(record, self.destination_id, record.policy.observer)]
        requests.extend(
            import_request(record, self.destination_id, f.actor, f) for f in record.feedback
        )
        return cast(
            dict[str, Any],
            clone(
                {
                    "capture_id": record.capture_id,
                    "revision": claim.revision,
                    "requests": [r.model_dump(mode="json") for r in requests],
                    "hash": neutral_fingerprint([r.model_dump(mode="json") for r in requests]),
                }
            ),
        )


def _flow_methods(flow: Any) -> tuple[object, ...]:
    """实际被调用的方法身份进入逐await围栏，不创建新业务owner。"""
    values = [
        flow.port.claim,
        flow.port.units,
        flow.port.save_unit,
        flow.port.ack,
        flow.repository.import_observations,
        flow.repository.candidate,
        flow.binding,
        flow._guard,
        flow._receipt,
        flow._units,
        flow.views,
        flow.view_binding,
        flow.port.binding,
        getattr(flow.port, "_read", None),
        getattr(flow.port, "_write", None),
    ]
    return tuple(getattr(value, "__func__", value) for value in values)


def _imported_candidate_hash(
    candidate: dict[str, Any], row: dict[str, Any], feedback: Any, actor: str
) -> str:
    """投递收据绑定导入时revision0；后续人工确认不改变该原始候选身份。"""
    identity = row.get("candidate_id", row["id"])
    original: dict[str, Any] = {
        "candidate_id": identity,
        "revision": 0,
        "status": "pending",
        "observation_id": row["id"],
        "observation_hash": row["content_hash"],
        "eligibility": row["eligibility"],
        "reasons": row["reasons"] if feedback is None else ["human_feedback"],
    }
    if feedback is not None:
        original["signals"] = [{"key": feedback.key, "reason": feedback.reason, "actor": actor}]
    # 与原import_rows的revision0字段及指纹完全一致；不写候选、不重建管理层。
    if (
        type(candidate.get("revision")) is not int
        or candidate["revision"] < 0
        or any(
            candidate.get(key) != value
            for key, value in original.items()
            if key not in {"revision", "status"}
        )
        or neutral_fingerprint({key: value for key, value in candidate.items() if key != "hash"})
        != candidate.get("hash")
    ):
        raise ServiceError("facts_corrupt")
    digest = neutral_fingerprint(original)
    if candidate["revision"] == 0 and candidate != original | {"hash": digest}:
        raise ServiceError("facts_corrupt")
    return digest
