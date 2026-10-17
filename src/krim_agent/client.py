"""远端薄客户端；不初始化库、不自动重发创建请求。"""

import asyncio
from typing import Any
from urllib.parse import quote, urlencode

import httpx

from krim_agent.contracts import RunFacts, RunRequest, RunView, ServiceError
from krim_agent.evaluation.types import (
    Candidate,
    DatasetRevision,
    EvalRequest,
    EvalView,
    FreezeRequest,
)


class AsyncRunClient:
    def __init__(self, base_url: str, api_key: str = "") -> None:
        self.http = httpx.AsyncClient(
            base_url=base_url,
            headers={"Authorization": "Bearer " + api_key} if api_key else {},
            follow_redirects=False,
            timeout=10,
            trust_env=False,
        )

    async def request(self, method: str, path: str, body: dict[str, Any] | None = None) -> Any:
        try:
            response = await self.http.request(method, path, json=body)
        except httpx.HTTPError:
            raise ServiceError("execution_error", 503) from None
        if response.status_code >= 400:
            raise ServiceError(
                response.json().get("error", {}).get("code", "execution_error"),
                response.status_code,
            )
        if response.status_code >= 300:
            raise ServiceError("execution_error")
        return response.json()

    async def start(self, request: RunRequest) -> RunView:
        return RunView.model_validate(
            await self.request("POST", "/v1/runs", request.model_dump(mode="json"))
        )

    async def wait(self, run_id: str) -> RunView:
        while True:
            run = RunView.model_validate(await self.request("GET", "/v1/runs/" + run_id))
            if run.status not in {"queued", "running"}:
                return run
            await asyncio.sleep(0.02)

    async def result(self, run_id: str) -> RunFacts:
        return RunFacts.model_validate(await self.request("GET", "/v1/runs/" + run_id + "/result"))

    async def close(self) -> None:
        await self.http.aclose()

    async def candidate(self, run_id: str, include_input: bool = False) -> Candidate:
        return Candidate.model_validate(
            await self.request(
                "GET",
                f"/v1/runs/{quote(run_id, safe='')}/candidate"
                f"?include_input={str(include_input).lower()}",
            )
        )

    async def freeze(self, request: FreezeRequest) -> DatasetRevision:
        return DatasetRevision.model_validate(
            await self.request("POST", "/v1/datasets", request.model_dump(mode="json"))
        )

    async def eval_start(self, request: EvalRequest) -> EvalView:
        return EvalView.model_validate(
            await self.request("POST", "/v1/evals", request.model_dump(mode="json"))
        )

    async def eval_get(self, eval_id: str) -> EvalView:
        return EvalView.model_validate(
            await self.request("GET", "/v1/evals/" + quote(eval_id, safe=""))
        )

    async def eval_by_key(self, key: str) -> EvalView:
        return EvalView.model_validate(
            await self.request("GET", "/v1/evals/by-key/" + quote(key, safe=""))
        )

    async def eval_wait(self, eval_id: str) -> EvalView:
        while True:
            view = await self.eval_get(eval_id)
            if view.status not in {"queued", "running"}:
                return view
            await asyncio.sleep(0.1)

    async def eval_cancel(self, eval_id: str) -> EvalView:
        return EvalView.model_validate(
            await self.request("POST", "/v1/evals/" + quote(eval_id, safe="") + "/cancel", {})
        )

    async def report(self, eval_id: str) -> dict[str, Any]:
        return dict(await self.request("GET", "/v1/evals/" + quote(eval_id, safe="") + "/report"))

    async def compare(self, baseline: str, candidate: str) -> dict[str, Any]:
        a, b = await self.report(baseline), await self.report(candidate)
        query = urlencode(
            {
                "baseline": baseline,
                "candidate": candidate,
                "baseline_revision": a["revision"],
                "candidate_revision": b["revision"],
                "baseline_hash": a["report_hash"],
                "candidate_hash": b["report_hash"],
            }
        )
        return dict(await self.request("GET", "/v1/evals/compare?" + query))
