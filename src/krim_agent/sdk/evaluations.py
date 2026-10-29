"""neutral/continuous评测v4工作区SDK；HTTP创建调用不自动重发。"""

from __future__ import annotations

from typing import Any, Literal, cast
from urllib.parse import quote, urlencode

from pydantic import BaseModel

from krim_agent.client import AsyncRunClient
from krim_agent.contracts import ServiceError
from krim_agent.security import LOCAL, PrincipalContext

from ..evaluation.product import EvaluationProduct
from ..evaluation.product_types import (
    FreezeConfirmedRequest,
    ProductComparisonRequest,
    ProductConfirmationRequest,
    ProductDatasetSpec,
    RunImportRequest,
)


def _json(value: BaseModel | dict[str, Any]) -> dict[str, Any]:
    return value.model_dump(mode="json") if isinstance(value, BaseModel) else value


class EvaluationSDK:
    """同进程门面或AsyncRunClient HTTP后端；身份和workspace始终显式绑定。"""

    def __init__(
        self,
        backend: EvaluationProduct | AsyncRunClient,
        *,
        principal: PrincipalContext = LOCAL,
        workspace: str = "local",
    ) -> None:
        self.backend, self.principal, self.workspace = backend, principal, workspace

    async def _request(
        self, method: str, path: str, body: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        assert isinstance(self.backend, AsyncRunClient)
        value = await self.backend.request(method, path, body)
        if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
            raise ServiceError("facts_corrupt", 502)
        return cast(dict[str, Any], value)

    async def _rows_request(
        self, method: str, path: str, body: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        assert isinstance(self.backend, AsyncRunClient)
        value = await self.backend.request(method, path, body)
        if not isinstance(value, list) or any(not isinstance(row, dict) for row in value):
            raise ServiceError("facts_corrupt", 502)
        return cast(list[dict[str, Any]], value)

    @property
    def path(self) -> str:
        return "/v4/workspaces/" + quote(self.workspace, safe="") + "/eval"

    @staticmethod
    def ident(value: str) -> str:
        return quote(value, safe="")

    async def targets(self) -> list[dict[str, Any]]:
        if isinstance(self.backend, AsyncRunClient):
            return await self._rows_request("GET", self.path + "/targets")
        return await self.backend.targets(self.principal, self.workspace)

    async def metrics(
        self,
        target_key: str,
        mode: Literal["manual", "continuous"] = "continuous",
        include_process: bool = False,
    ) -> list[dict[str, Any]]:
        query = urlencode(
            {
                "target_key": target_key,
                "mode": mode,
                "include_process": str(include_process).lower(),
            }
        )
        if isinstance(self.backend, AsyncRunClient):
            return await self._rows_request("GET", self.path + "/metrics?" + query)
        return await self.backend.metrics(
            self.principal,
            self.workspace,
            target_key,
            mode,
            include_process,
        )

    async def list_datasets(self, offset: int = 0, limit: int = 20) -> list[dict[str, Any]]:
        query = urlencode({"offset": offset, "limit": limit})
        if isinstance(self.backend, AsyncRunClient):
            return await self._rows_request("GET", self.path + "/datasets?" + query)
        return await self.backend.list_datasets(self.principal, self.workspace, offset, limit)

    async def dataset(self, name: str, version: str) -> dict[str, Any]:
        path = self.path + "/datasets/" + self.ident(name) + "/" + self.ident(version)
        if isinstance(self.backend, AsyncRunClient):
            return await self._request("GET", path)
        return await self.backend.dataset(self.principal, self.workspace, name, version)

    async def freeze_dataset(self, request: ProductDatasetSpec) -> dict[str, Any]:
        body = _json(request)
        if isinstance(self.backend, AsyncRunClient):
            return await self._request("POST", self.path + "/datasets", body)
        return await self.backend.freeze_dataset(self.principal, self.workspace, request)

    async def import_runs(self, request: RunImportRequest) -> list[dict[str, Any]]:
        body = _json(request)
        path = self.path + "/sources/runs/import"
        if isinstance(self.backend, AsyncRunClient):
            return await self._rows_request("POST", path, body)
        return await self.backend.import_runs(self.principal, self.workspace, request)

    async def import_receipt(self, request_key: str) -> dict[str, Any]:
        path = self.path + "/sources/runs/imports/by-key/" + self.ident(request_key)
        if isinstance(self.backend, AsyncRunClient):
            return await self._request("GET", path)
        return await self.backend.import_receipt(self.principal, self.workspace, request_key)

    async def candidate(self, candidate_id: str) -> dict[str, Any]:
        path = self.path + "/candidates/" + self.ident(candidate_id)
        if isinstance(self.backend, AsyncRunClient):
            return await self._request("GET", path)
        return await self.backend.candidate(self.principal, self.workspace, candidate_id)

    async def confirm_candidate(
        self, candidate_id: str, request: ProductConfirmationRequest
    ) -> dict[str, Any]:
        path = self.path + "/candidates/" + self.ident(candidate_id) + "/confirm"
        if isinstance(self.backend, AsyncRunClient):
            return await self._request("POST", path, _json(request))
        return await self.backend.confirm_candidate(
            self.principal, self.workspace, candidate_id, request
        )

    async def freeze_confirmed(self, request: FreezeConfirmedRequest) -> dict[str, Any]:
        path = self.path + "/datasets/confirmed"
        if isinstance(self.backend, AsyncRunClient):
            return await self._request("POST", path, _json(request))
        return await self.backend.freeze_confirmed(self.principal, self.workspace, request)

    async def list_tasks(self, offset: int = 0, limit: int = 20) -> list[dict[str, Any]]:
        query = urlencode({"offset": offset, "limit": limit})
        if isinstance(self.backend, AsyncRunClient):
            return await self._rows_request("GET", self.path + "/tasks?" + query)
        return await self.backend.list_tasks(self.principal, self.workspace, offset, limit)

    async def start_task(self, request: BaseModel | dict[str, Any]) -> dict[str, Any]:
        body = _json(request) if isinstance(request, BaseModel) else request
        if isinstance(self.backend, AsyncRunClient):
            # 不捕获网络未知并自动重放；调用者应先用原request_key查询。
            return await self._request("POST", self.path + "/tasks", body)
        return await self.backend.start_task(self.principal, self.workspace, body)

    async def task(self, task_id: str) -> dict[str, Any]:
        path = self.path + "/tasks/" + self.ident(task_id)
        if isinstance(self.backend, AsyncRunClient):
            return await self._request("GET", path)
        return await self.backend.task(self.principal, self.workspace, task_id)

    async def task_by_key(self, request_key: str) -> dict[str, Any]:
        path = self.path + "/tasks/by-key/" + self.ident(request_key)
        if isinstance(self.backend, AsyncRunClient):
            return await self._request("GET", path)
        return await self.backend.task_by_key(self.principal, self.workspace, request_key)

    async def cancel_task(self, task_id: str) -> dict[str, Any]:
        path = self.path + "/tasks/" + self.ident(task_id) + "/cancel"
        if isinstance(self.backend, AsyncRunClient):
            return await self._request("POST", path, {})
        return await self.backend.cancel_task(self.principal, self.workspace, task_id)

    async def report_page(
        self,
        task_id: str,
        *,
        cursor: str | None = None,
        offset: int = 0,
        limit: int = 16,
    ) -> dict[str, Any]:
        values: dict[str, Any] = {"offset": offset, "limit": limit}
        if cursor is not None:
            values["cursor"] = cursor
        path = self.path + "/tasks/" + self.ident(task_id) + "/report?" + urlencode(values)
        if isinstance(self.backend, AsyncRunClient):
            return await self._request("GET", path)
        return await self.backend.report_page(
            self.principal,
            self.workspace,
            task_id,
            cursor=cursor,
            offset=offset,
            limit=limit,
        )

    async def compare(self, request: ProductComparisonRequest) -> dict[str, Any]:
        path = self.path + "/tasks/compare"
        if isinstance(self.backend, AsyncRunClient):
            return await self._request("POST", path, _json(request))
        return await self.backend.compare(self.principal, self.workspace, request)
