"""v4工作区评测路由；宿主通过getter提供已装配的产品门面。"""

from __future__ import annotations

from collections.abc import Callable
from typing import Literal, cast

from fastapi import APIRouter, FastAPI, Query, Request
from pydantic import JsonValue

from krim_agent.security import PrincipalContext

from ..evaluation.product import EvaluationProduct
from ..evaluation.product_types import (
    FreezeConfirmedRequest,
    ProductComparisonRequest,
    ProductConfirmationRequest,
    ProductDatasetSpec,
    RunImportRequest,
)


def principal(request: Request) -> PrincipalContext:
    return cast(PrincipalContext, request.state.principal)


def mount_evaluations(
    app: FastAPI,
    get_facade: Callable[[Request], EvaluationProduct],
) -> None:
    """安装v4路由；getter由宿主绑定，避免模块级composition依赖和循环导入。"""
    router = APIRouter()
    base = "/v4/workspaces/{workspace_id}/eval"

    @router.get(base + "/targets")
    async def targets(workspace_id: str, request: Request) -> list[dict[str, str]]:
        return await get_facade(request).targets(principal(request), workspace_id)

    @router.get(base + "/metrics")
    async def metrics(
        workspace_id: str,
        request: Request,
        target_key: str = Query(min_length=1, max_length=64),
        mode: Literal["manual", "continuous"] = Query(default="continuous"),
        include_process: bool = Query(default=False),
    ) -> list[dict[str, JsonValue]]:
        return cast(
            list[dict[str, JsonValue]],
            await get_facade(request).metrics(
                principal(request), workspace_id, target_key, mode, include_process
            ),
        )

    @router.get(base + "/datasets")
    async def datasets(
        workspace_id: str,
        request: Request,
        offset: int = Query(default=0, ge=0, le=100000),
        limit: int = Query(default=20, ge=1, le=50),
    ) -> list[dict[str, JsonValue]]:
        return cast(
            list[dict[str, JsonValue]],
            await get_facade(request).list_datasets(
                principal(request), workspace_id, offset, limit
            ),
        )

    @router.post(base + "/datasets", status_code=201)
    async def freeze_dataset(
        workspace_id: str, body: ProductDatasetSpec, request: Request
    ) -> dict[str, JsonValue]:
        return cast(
            dict[str, JsonValue],
            await get_facade(request).freeze_dataset(principal(request), workspace_id, body),
        )

    @router.post(base + "/datasets/confirmed", status_code=201)
    async def freeze_confirmed(
        workspace_id: str, body: FreezeConfirmedRequest, request: Request
    ) -> dict[str, JsonValue]:
        return cast(
            dict[str, JsonValue],
            await get_facade(request).freeze_confirmed(principal(request), workspace_id, body),
        )

    @router.get(base + "/datasets/{name}/{version}")
    async def dataset(
        workspace_id: str, name: str, version: str, request: Request
    ) -> dict[str, JsonValue]:
        return cast(
            dict[str, JsonValue],
            await get_facade(request).dataset(principal(request), workspace_id, name, version),
        )

    @router.post(base + "/sources/runs/import", status_code=201)
    async def import_runs(
        workspace_id: str, body: RunImportRequest, request: Request
    ) -> list[dict[str, JsonValue]]:
        return cast(
            list[dict[str, JsonValue]],
            await get_facade(request).import_runs(principal(request), workspace_id, body),
        )

    @router.get(base + "/sources/runs/imports/by-key/{request_key}")
    async def import_receipt(
        workspace_id: str, request_key: str, request: Request
    ) -> dict[str, JsonValue]:
        return cast(
            dict[str, JsonValue],
            await get_facade(request).import_receipt(principal(request), workspace_id, request_key),
        )

    @router.get(base + "/candidates/{candidate_id}")
    async def candidate(
        workspace_id: str, candidate_id: str, request: Request
    ) -> dict[str, JsonValue]:
        return cast(
            dict[str, JsonValue],
            await get_facade(request).candidate(principal(request), workspace_id, candidate_id),
        )

    @router.post(base + "/candidates/{candidate_id}/confirm")
    async def confirm_candidate(
        workspace_id: str,
        candidate_id: str,
        body: ProductConfirmationRequest,
        request: Request,
    ) -> dict[str, JsonValue]:
        return cast(
            dict[str, JsonValue],
            await get_facade(request).confirm_candidate(
                principal(request), workspace_id, candidate_id, body
            ),
        )

    @router.get(base + "/tasks")
    async def list_tasks(
        workspace_id: str,
        request: Request,
        offset: int = Query(default=0, ge=0, le=100000),
        limit: int = Query(default=20, ge=1, le=50),
    ) -> list[dict[str, JsonValue]]:
        return cast(
            list[dict[str, JsonValue]],
            await get_facade(request).list_tasks(principal(request), workspace_id, offset, limit),
        )

    @router.post(base + "/tasks", status_code=202)
    async def start_task(
        workspace_id: str, body: dict[str, JsonValue], request: Request
    ) -> dict[str, JsonValue]:
        return cast(
            dict[str, JsonValue],
            await get_facade(request).start_task(principal(request), workspace_id, body),
        )

    @router.get(base + "/tasks/by-key/{request_key}")
    async def task_by_key(
        workspace_id: str, request_key: str, request: Request
    ) -> dict[str, JsonValue]:
        return cast(
            dict[str, JsonValue],
            await get_facade(request).task_by_key(principal(request), workspace_id, request_key),
        )

    @router.post(base + "/tasks/compare")
    async def compare(
        workspace_id: str, body: ProductComparisonRequest, request: Request
    ) -> dict[str, JsonValue]:
        return cast(
            dict[str, JsonValue],
            await get_facade(request).compare(principal(request), workspace_id, body),
        )

    @router.get(base + "/tasks/{task_id}/report")
    async def report(
        workspace_id: str,
        task_id: str,
        request: Request,
        cursor: str | None = Query(default=None, max_length=24000),
        offset: int = Query(default=0, ge=0, le=100000),
        limit: int = Query(default=16, ge=1, le=16),
    ) -> dict[str, JsonValue]:
        return cast(
            dict[str, JsonValue],
            await get_facade(request).report_page(
                principal(request),
                workspace_id,
                task_id,
                cursor=cursor,
                offset=offset,
                limit=limit,
            ),
        )

    @router.post(base + "/tasks/{task_id}/cancel")
    async def cancel_task(
        workspace_id: str, task_id: str, request: Request
    ) -> dict[str, JsonValue]:
        return cast(
            dict[str, JsonValue],
            await get_facade(request).cancel_task(principal(request), workspace_id, task_id),
        )

    @router.get(base + "/tasks/{task_id}")
    async def task(workspace_id: str, task_id: str, request: Request) -> dict[str, JsonValue]:
        return cast(
            dict[str, JsonValue],
            await get_facade(request).task(principal(request), workspace_id, task_id),
        )

    app.include_router(router)
