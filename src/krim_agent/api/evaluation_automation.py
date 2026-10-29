"""v4工作区评测自动化路由；服务生命周期由宿主持有。"""

from __future__ import annotations

from collections.abc import Callable
from typing import cast

from fastapi import APIRouter, FastAPI, Query, Request
from pydantic import Field, JsonValue

from krim_agent.contracts import StrictModel
from krim_agent.security import PrincipalContext

from ..evaluation.automation import EvaluationAutomation
from ..evaluation.continuous_types import ScheduleSpec
from ..evaluation.domain import Identity
from ..evaluation.hot_types import FeedbackSignal


class CaptureControl(StrictModel):
    enabled: bool


class ScheduleDisableRequest(StrictModel):
    revision: int = Field(ge=0)
    key: Identity


class ScheduleTickRequest(StrictModel):
    expected_window_index: int | None = Field(default=None, ge=0, le=2)


def principal(request: Request) -> PrincipalContext:
    return cast(PrincipalContext, request.state.principal)


def mount_evaluation_automation(
    app: FastAPI,
    get_automation: Callable[[Request], EvaluationAutomation],
) -> None:
    """安装自动化API；getter返回Application拥有的已启动服务。"""
    router = APIRouter()
    base = "/v4/workspaces/{workspace_id}/eval/automation"

    @router.get(base + "/status")
    async def status(workspace_id: str, request: Request) -> dict[str, JsonValue]:
        return cast(
            dict[str, JsonValue],
            await get_automation(request).status(principal(request), workspace_id),
        )

    @router.get(base + "/schedules")
    async def list_schedules(
        workspace_id: str,
        request: Request,
        offset: int = Query(default=0, ge=0, le=100000),
        limit: int = Query(default=20, ge=1, le=50),
    ) -> list[dict[str, JsonValue]]:
        return cast(
            list[dict[str, JsonValue]],
            await get_automation(request).list_schedules(
                principal(request), workspace_id, offset, limit
            ),
        )

    @router.get(base + "/schedules/{schedule_id}")
    async def schedule(
        workspace_id: str, schedule_id: str, request: Request
    ) -> dict[str, JsonValue]:
        return cast(
            dict[str, JsonValue],
            await get_automation(request).schedule(
                principal(request), workspace_id, schedule_id
            ),
        )

    @router.get(base + "/schedules/{schedule_id}/windows/current")
    async def current_schedule_window(
        workspace_id: str, schedule_id: str, request: Request
    ) -> dict[str, JsonValue]:
        return cast(
            dict[str, JsonValue],
            await get_automation(request).current_schedule_window(
                principal(request), workspace_id, schedule_id
            ),
        )

    @router.get(base + "/schedules/{schedule_id}/windows/{window_index}")
    async def schedule_window(
        workspace_id: str, schedule_id: str, window_index: int, request: Request
    ) -> dict[str, JsonValue]:
        return cast(
            dict[str, JsonValue],
            await get_automation(request).schedule_window(
                principal(request), workspace_id, schedule_id, window_index
            ),
        )

    @router.post(base + "/capture")
    async def capture(
        workspace_id: str, body: CaptureControl, request: Request
    ) -> dict[str, JsonValue]:
        return cast(
            dict[str, JsonValue],
            await get_automation(request).set_capture(
                principal(request), workspace_id, body.enabled
            ),
        )

    @router.post(base + "/feedback/{run_id}", status_code=201)
    async def feedback(
        workspace_id: str,
        run_id: str,
        body: FeedbackSignal,
        request: Request,
    ) -> dict[str, JsonValue]:
        return cast(
            dict[str, JsonValue],
            await get_automation(request).submit_feedback(
                principal(request), workspace_id, run_id, body
            ),
        )

    @router.get(base + "/feedback/{run_id}/by-key/{key}")
    async def feedback_receipt(
        workspace_id: str, run_id: str, key: str, request: Request
    ) -> dict[str, JsonValue]:
        return cast(
            dict[str, JsonValue],
            await get_automation(request).feedback_receipt(
                principal(request), workspace_id, run_id, key
            ),
        )

    @router.post(base + "/schedules", status_code=201)
    async def create_schedule(
        workspace_id: str, body: ScheduleSpec, request: Request
    ) -> dict[str, JsonValue]:
        return cast(
            dict[str, JsonValue],
            await get_automation(request).create_schedule(
                principal(request), workspace_id, body
            ),
        )

    @router.post(base + "/schedules/{schedule_id}/tick", status_code=202)
    async def tick_schedule(
        workspace_id: str,
        schedule_id: str,
        request: Request,
        body: ScheduleTickRequest | None = None,
    ) -> dict[str, JsonValue]:
        return cast(
            dict[str, JsonValue],
            await get_automation(request).tick_schedule(
                principal(request),
                workspace_id,
                schedule_id,
                None if body is None else body.expected_window_index,
            ),
        )

    @router.post(base + "/schedules/{schedule_id}/disable")
    async def disable_schedule(
        workspace_id: str,
        schedule_id: str,
        body: ScheduleDisableRequest,
        request: Request,
    ) -> dict[str, JsonValue]:
        return cast(
            dict[str, JsonValue],
            await get_automation(request).disable_schedule(
                principal(request),
                workspace_id,
                schedule_id,
                body.revision,
                body.key,
            ),
        )

    app.include_router(router)
