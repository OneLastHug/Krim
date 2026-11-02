"""本地薄 SDK；借用 RunService，不另装配数据库或循环。"""

from typing import Any

from krim_agent.contracts import RunFacts, RunRequest, RunView
from krim_agent.evaluation.service import EvaluationService
from krim_agent.evaluation.types import (
    Candidate,
    DatasetRevision,
    EvalRequest,
    EvalView,
    FreezeRequest,
)
from krim_agent.runtime.service import RunService


class RunSDK:
    def __init__(self, service: RunService) -> None:
        self.service = service

    async def start(self, text: str, request_key: str, *, target: str = "demo") -> RunView:
        return await self.service.start(
            RunRequest.model_validate({"text": text, "request_key": request_key, "target": target})
        )

    def tools(self, target: str = "demo") -> list[dict[str, Any]]:
        from krim_agent.contracts import ServiceError
        binding = self.service.bindings.get(target)
        if binding is None:
            raise ServiceError("not_found", 404)
        binding.assert_current()
        return [dict(spec.descriptor(), fingerprint=spec.fingerprint)
                for spec, _ in binding.loop.executor.registry._frozen]

    async def wait(self, run_id: str) -> RunView:
        return await self.service.wait(run_id)

    async def result(self, run_id: str) -> RunFacts:
        return await self.service.result(run_id)

    async def cancel(self, run_id: str) -> RunView:
        return await self.service.cancel(run_id)


class EvaluationSDK:
    """仅借服务；没有close或隐式Application所有权。"""

    def __init__(self, service: EvaluationService) -> None:
        self.service = service

    async def candidate(self, run_id: str, include_input: bool = False) -> Candidate:
        return await self.service.candidate(run_id, include_input)

    async def freeze(self, request: FreezeRequest) -> DatasetRevision:
        return await self.service.freeze(request)

    async def start(self, request: EvalRequest) -> EvalView:
        return await self.service.start(request)

    async def wait(self, eval_id: str) -> EvalView:
        return await self.service.wait(eval_id)

    async def report(self, eval_id: str) -> dict[str, Any]:
        return await self.service.report(eval_id)

    async def cancel(self, eval_id: str) -> EvalView:
        return await self.service.cancel(eval_id)

    async def compare(self, baseline: str, candidate: str) -> dict[str, Any]:
        a, b = await self.report(baseline), await self.report(candidate)
        return await self.service.compare(
            baseline, candidate, a["revision"], b["revision"], a["report_hash"], b["report_hash"]
        )
