"""评测自动化SDK；复用宿主服务或同一HTTP client，不重试POST。"""

from __future__ import annotations

from typing import Any, TypeVar
from urllib.parse import quote, urlencode

from pydantic import Field, ValidationError

from krim_agent.client import AsyncRunClient
from krim_agent.contracts import ServiceError, StrictModel
from krim_agent.security import PrincipalContext

from ..evaluation.automation import EvaluationAutomation
from ..evaluation.continuous_types import ScheduleSpec
from ..evaluation.domain import Digest, Identity
from ..evaluation.hot_types import FeedbackSignal


class _AutomationState(StrictModel):
    running: bool
    last_error: str | None


class _CaptureCounters(StrictModel):
    seen: int = Field(ge=0)
    enqueued: int = Field(ge=0)
    dropped: int = Field(ge=0)
    acked: int = Field(ge=0)


class _CaptureState(StrictModel):
    configured: bool
    enabled: bool
    observer_ready: bool
    pending: int = Field(ge=0)
    claimed: int = Field(ge=0)
    acknowledged: int = Field(ge=0)
    counters: _CaptureCounters
    text_capture: bool
    argument_capture: bool
    artifact_count: int = Field(ge=0)


class _ScheduleCounts(StrictModel):
    total: int = Field(ge=0)
    enabled: int = Field(ge=0)


class _AutomationStatus(StrictModel):
    automation: _AutomationState
    capture: _CaptureState
    schedules: _ScheduleCounts


class _FeedbackReceipt(StrictModel):
    revision: int = Field(ge=1, le=8)
    key: Identity


class _ScheduleView(StrictModel):
    schedule_id: Identity
    version: Identity
    enabled: bool
    revision: int = Field(ge=0, le=1)
    observed_through: int = Field(ge=-1)
    missed_windows: int = Field(ge=0)


class _ScheduleDataset(StrictModel):
    name: Identity
    version: Identity
    fingerprint: Digest


class _ScheduleDetail(_ScheduleView):
    dataset: _ScheduleDataset
    target_key: Identity
    epoch: int = Field(ge=0)
    interval_seconds: int = Field(ge=60, le=86400)
    max_windows: int = Field(ge=1, le=3)


class _ScheduleTaskView(StrictModel):
    id: str
    status: str
    request_key: str | None = None
    code: str | None = None
    revision: int | None = None
    planned_count: int | None = None
    finished_count: int | None = None


class _DisableRequest(StrictModel):
    revision: int = Field(ge=0)
    key: Identity


_Response = TypeVar("_Response", bound=StrictModel)


class EvaluationAutomationSDK:
    """面向单一workspace的自动化门面。

    进程内模式将`principal`交给宿主服务核验；HTTP模式不会发送该字段，
    服务端从已认证请求中取得身份并再次校验成员、角色与Run所有权。
    """

    def __init__(
        self,
        backend: EvaluationAutomation | AsyncRunClient,
        *,
        principal: PrincipalContext,
        workspace: str,
    ) -> None:
        if not isinstance(backend, (EvaluationAutomation, AsyncRunClient)):
            raise ServiceError("invalid_input", 422)
        if (
            not isinstance(principal, PrincipalContext)
            or not isinstance(workspace, str)
            or not workspace
        ):
            raise ServiceError("invalid_input", 422)
        self.backend = backend
        self.principal = principal
        self.workspace = workspace

    @staticmethod
    def _path_segment(value: str) -> str:
        return quote(value, safe="")

    @property
    def path(self) -> str:
        workspace = self._path_segment(self.workspace)
        return f"/v4/workspaces/{workspace}/eval/automation"

    async def _http(
        self, method: str, suffix: str, body: dict[str, Any] | None = None
    ) -> Any:
        if not isinstance(self.backend, AsyncRunClient):
            raise ServiceError("target_binding_changed")
        try:
            # AsyncRunClient只发出一次请求；未知POST由调用方用原对象查询/恢复。
            return await self.backend.request(method, self.path + suffix, body)
        except ServiceError:
            raise
        except (TypeError, ValueError):
            raise ServiceError("execution_error", 502) from None

    @staticmethod
    def _response(model: type[_Response], value: object) -> dict[str, Any]:
        try:
            return model.model_validate(value).model_dump(mode="json")
        except (TypeError, ValueError, ValidationError):
            raise ServiceError("facts_corrupt", 502) from None

    @classmethod
    def _response_rows(
        cls, model: type[_Response], value: object
    ) -> list[dict[str, Any]]:
        if not isinstance(value, list):
            raise ServiceError("facts_corrupt", 502)
        return [cls._response(model, row) for row in value]

    async def status(self) -> dict[str, Any]:
        if isinstance(self.backend, EvaluationAutomation):
            value = await self.backend.status(self.principal, self.workspace)
        else:
            value = await self._http("GET", "/status")
        return self._response(_AutomationStatus, value)

    async def list_schedules(
        self, offset: int = 0, limit: int = 20
    ) -> list[dict[str, Any]]:
        if (
            type(offset) is not int
            or type(limit) is not int
            or not 0 <= offset <= 100000
            or not 1 <= limit <= 50
        ):
            raise ServiceError("invalid_input", 422)
        if isinstance(self.backend, EvaluationAutomation):
            value = await self.backend.list_schedules(
                self.principal, self.workspace, offset, limit
            )
        else:
            query = urlencode({"offset": offset, "limit": limit})
            value = await self._http("GET", "/schedules?" + query)
        return self._response_rows(_ScheduleDetail, value)

    async def schedule(self, schedule_id: str) -> dict[str, Any]:
        if not isinstance(schedule_id, str) or not schedule_id:
            raise ServiceError("invalid_input", 422)
        if isinstance(self.backend, EvaluationAutomation):
            value = await self.backend.schedule(
                self.principal, self.workspace, schedule_id
            )
        else:
            value = await self._http(
                "GET", "/schedules/" + self._path_segment(schedule_id)
            )
        return self._response(_ScheduleDetail, value)

    async def set_capture(self, enabled: bool) -> dict[str, Any]:
        if type(enabled) is not bool:
            raise ServiceError("invalid_input", 422)
        if isinstance(self.backend, EvaluationAutomation):
            value = await self.backend.set_capture(self.principal, self.workspace, enabled)
        else:
            value = await self._http("POST", "/capture", {"enabled": enabled})
        return self._response(_AutomationStatus, value)

    async def submit_feedback(
        self, run_id: str, signal: FeedbackSignal
    ) -> dict[str, Any]:
        if not isinstance(run_id, str) or not run_id:
            raise ServiceError("invalid_input", 422)
        try:
            frozen = FeedbackSignal.model_validate(signal.model_dump(mode="json"))
        except (AttributeError, TypeError, ValueError, ValidationError):
            raise ServiceError("invalid_input", 422) from None
        if isinstance(self.backend, EvaluationAutomation):
            value = await self.backend.submit_feedback(
                self.principal, self.workspace, run_id, frozen
            )
        else:
            value = await self._http(
                "POST",
                "/feedback/" + self._path_segment(run_id),
                frozen.model_dump(mode="json"),
            )
        return self._response(_FeedbackReceipt, value)

    async def create_schedule(self, request: ScheduleSpec) -> dict[str, Any]:
        try:
            frozen = ScheduleSpec.model_validate(request.model_dump(mode="json"))
        except (AttributeError, TypeError, ValueError, ValidationError):
            raise ServiceError("invalid_input", 422) from None
        if isinstance(self.backend, EvaluationAutomation):
            value = await self.backend.create_schedule(
                self.principal, self.workspace, frozen
            )
        else:
            value = await self._http(
                "POST", "/schedules", frozen.model_dump(mode="json")
            )
        return self._response(_ScheduleView, value)

    async def tick_schedule(self, schedule_id: str) -> dict[str, Any]:
        if not isinstance(schedule_id, str) or not schedule_id:
            raise ServiceError("invalid_input", 422)
        if isinstance(self.backend, EvaluationAutomation):
            value = await self.backend.tick_schedule(
                self.principal, self.workspace, schedule_id
            )
        else:
            value = await self._http(
                "POST", "/schedules/" + self._path_segment(schedule_id) + "/tick"
            )
        return self._response(_ScheduleTaskView, value)

    async def disable_schedule(
        self, schedule_id: str, *, revision: int, key: str
    ) -> dict[str, Any]:
        if not isinstance(schedule_id, str) or not schedule_id:
            raise ServiceError("invalid_input", 422)
        try:
            frozen = _DisableRequest(revision=revision, key=key)
        except (TypeError, ValueError, ValidationError):
            raise ServiceError("invalid_input", 422) from None
        if isinstance(self.backend, EvaluationAutomation):
            value = await self.backend.disable_schedule(
                self.principal,
                self.workspace,
                schedule_id,
                frozen.revision,
                frozen.key,
            )
        else:
            value = await self._http(
                "POST",
                "/schedules/" + self._path_segment(schedule_id) + "/disable",
                frozen.model_dump(mode="json"),
            )
        return self._response(_ScheduleView, value)
