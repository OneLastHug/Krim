"""工作区评测薄服务；执行、持久化和评分仍由现有EvalApplication拥有。"""

from __future__ import annotations

import hashlib
from typing import Any, Literal

from pydantic import TypeAdapter, ValidationError
from sqlalchemy import case, func, select

from krim_agent.contracts import ServiceError
from krim_agent.runtime.service import RunService
from krim_agent.security import PrincipalContext
from krim_agent.sessions.service import SessionService

from .adapters.krim_source import KrimSourceReader
from .app import EvalApplication
from .continuous import ContinuousEvaluation
from .continuous_metrics import continuous_rules
from .continuous_reports import compare as compare_reports
from .continuous_reports import partition
from .continuous_types import (
    ComparisonPolicy,
    ConfirmationRequest,
    ContinuousRequest,
    FreezeConfirmed,
    ImportRequest,
)
from .domain import DatasetRef, DatasetSpec, TaskSpec, neutral_encoded, neutral_fingerprint
from .product_types import (
    DisplayName,
    FreezeConfirmedRequest,
    ProductComparisonRequest,
    ProductConfirmationRequest,
    ProductDatasetSpec,
    RunImportRequest,
)
from .reports import gate_reports, regression_reasons, validate_report
from .repository import dataset_lineage, datasets, source_observations, tasks


class EvaluationProduct:
    """通过principal和workspace授权的neutral/continuous工作区评测接缝。

    构造只接收宿主已创建的评测应用、RunService和SessionService；本类不拥有它们的生命周期。
    """

    def __init__(
        self,
        evaluation: EvalApplication,
        runs: RunService,
        sessions: SessionService,
    ) -> None:
        self.evaluation, self.runs, self.sessions = evaluation, runs, sessions

    @staticmethod
    def _scope(workspace_id: str) -> str:
        # 稳定的workspace内部命名空间；权限仍由主体验证，不依赖哈希保密。
        return "w" + hashlib.sha256(workspace_id.encode("utf-8")).hexdigest()[:24]

    @classmethod
    def _dataset_prefix(cls, workspace_id: str) -> str:
        return cls._scope(workspace_id) + "-"

    def _authorize(
        self,
        principal: PrincipalContext,
        workspace_id: str,
        *,
        write: bool = False,
        approve: bool = False,
    ) -> str:
        principal.member(workspace_id, write=write, approve=approve)
        return self._scope(workspace_id)

    @staticmethod
    def _bounded(value: object, *, limit: int = 65536) -> None:
        try:
            neutral_encoded(value, max_bytes=limit)
        except (TypeError, ValueError, RecursionError):
            raise ServiceError("invalid_input", 422) from None

    @staticmethod
    def _display_name(value: str) -> DisplayName:
        try:
            return TypeAdapter(DisplayName).validate_python(value)
        except (ValidationError, TypeError, ValueError):
            # 内部数据若不符合产品显示名，视为存储损坏，不泄露其命名空间。
            raise ServiceError("facts_corrupt") from None

    def _internal_name(self, workspace_id: str, display_name: str) -> str:
        try:
            name = self._display_name(display_name)
        except ServiceError:
            raise ServiceError("invalid_input", 422) from None
        return self._dataset_prefix(workspace_id) + name

    def _public_name(self, workspace_id: str, internal_name: str) -> str:
        prefix = self._dataset_prefix(workspace_id)
        if not internal_name.startswith(prefix):
            raise ServiceError("facts_corrupt")
        return self._display_name(internal_name[len(prefix) :])

    def _public_ref(self, workspace_id: str, value: dict[str, Any]) -> dict[str, Any]:
        try:
            ref = DatasetRef.model_validate(value)
            name = self._public_name(workspace_id, ref.name)
        except (ValidationError, KeyError, TypeError):
            raise ServiceError("facts_corrupt") from None
        return {"name": name, "version": ref.version, "fingerprint": ref.fingerprint}

    def _target_descriptor(self, key: str) -> dict[str, Any]:
        adapter = self.evaluation.repository.registry.get(key)
        if adapter is None:
            raise ServiceError("invalid_input", 422)
        descriptor = adapter.descriptor()
        current = getattr(adapter, "assert_current", None)
        if current is not None:
            current(descriptor)
        return descriptor

    @staticmethod
    def _metric_group(descriptor: dict[str, Any]) -> str:
        conditions = descriptor.get("execution_conditions")
        schema = conditions.get("schema_version") if isinstance(conditions, dict) else None
        if schema == "native-cli/1":
            return "native"
        if schema == "krim-run/1" or descriptor.get("kind") == "krim-run":
            return "krim"
        return "reference"

    async def targets(self, principal: PrincipalContext, workspace_id: str) -> list[dict[str, str]]:
        self._authorize(principal, workspace_id)
        result: list[dict[str, str]] = []
        for key, _adapter in sorted(self.evaluation.repository.registry.items()):
            descriptor = self._target_descriptor(key)
            result.append(
                {
                    "key": key,
                    "version": str(descriptor.get("version", "1")),
                    "kind": str(descriptor.get("kind", "registered")),
                }
            )
        return result

    async def metrics(
        self,
        principal: PrincipalContext,
        workspace_id: str,
        target_key: str,
        mode: Literal["manual", "continuous"] = "continuous",
        include_process: bool = False,
    ) -> list[dict[str, Any]]:
        self._authorize(principal, workspace_id)
        group = self._metric_group(self._target_descriptor(target_key))
        registry = self.evaluation.repository.metric_registry
        specs = (
            registry.specs(group)
            if mode == "manual"
            else continuous_rules(registry, group, include_process=include_process)
        )
        return [spec.model_dump(mode="json") for spec in specs]

    async def list_datasets(
        self,
        principal: PrincipalContext,
        workspace_id: str,
        offset: int = 0,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        self._authorize(principal, workspace_id)
        if (
            type(offset) is not int
            or type(limit) is not int
            or not 0 <= offset <= 100000
            or not 1 <= limit <= 50
        ):
            raise ServiceError("invalid_input", 422)
        prefix = self._dataset_prefix(workspace_id)
        repository = self.evaluation.repository
        async with repository.lock:
            if repository.closed:
                raise ServiceError("closed")
            async with repository.engine.begin() as conn:
                bodies = (
                    (
                        await conn.execute(
                            select(datasets.c.body)
                            .where(datasets.c.name.startswith(prefix, autoescape=True))
                            .order_by(datasets.c.name, datasets.c.version)
                            .offset(offset)
                            .limit(limit)
                        )
                    )
                    .scalars()
                    .all()
                )
        rows = []
        for body in bodies:
            dataset = DatasetSpec.model_validate(body)
            rows.append(
                {
                    "name": self._public_name(workspace_id, dataset.name),
                    "version": dataset.version,
                    "fingerprint": dataset.ref().fingerprint,
                    "case_count": len(dataset.cases),
                }
            )
        return rows

    async def dataset(
        self,
        principal: PrincipalContext,
        workspace_id: str,
        name: str,
        version: str,
    ) -> dict[str, Any]:
        self._authorize(principal, workspace_id)
        internal = self._internal_name(workspace_id, name)
        value = await self.evaluation.repository.dataset(internal, version)
        body = value.model_dump(mode="json")
        body["name"] = self._public_name(workspace_id, value.name)
        body["fingerprint"] = value.ref().fingerprint
        repository = self.evaluation.repository
        async with repository.lock:
            if repository.closed:
                raise ServiceError("closed")
            async with repository.engine.begin() as conn:
                envelope = (
                    await conn.execute(
                        select(dataset_lineage.c.body).where(
                            dataset_lineage.c.dataset_name == internal,
                            dataset_lineage.c.version == version,
                        )
                    )
                ).scalar_one_or_none()
        body["dataset_envelope_fp"] = None
        if envelope is not None:
            lineage = envelope.get("lineage")
            if (
                not isinstance(lineage, dict)
                or lineage.get("scope") != self._scope(workspace_id)
                or envelope.get("dataset_ref") != value.ref().model_dump(mode="json")
                or envelope.get("dataset_envelope_fp")
                != neutral_fingerprint(
                    {
                        "dataset_ref": envelope["dataset_ref"],
                        "lineage": lineage,
                    }
                )
            ):
                raise ServiceError("facts_corrupt")
            body["dataset_envelope_fp"] = envelope["dataset_envelope_fp"]
        return body

    async def freeze_dataset(
        self,
        principal: PrincipalContext,
        workspace_id: str,
        request: ProductDatasetSpec,
    ) -> dict[str, Any]:
        self._authorize(principal, workspace_id, write=True)
        frozen = ProductDatasetSpec.model_validate(request.model_dump(mode="json"))
        self._bounded(frozen.model_dump(mode="json"))
        value = DatasetSpec.model_validate(
            frozen.model_dump(mode="json")
            | {"name": self._internal_name(workspace_id, frozen.name)}
        )
        result = await self.evaluation.repository.freeze(value)
        body = result.model_dump(mode="json")
        body["name"] = frozen.name
        body["fingerprint"] = result.ref().fingerprint
        return body

    def _source_reader(
        self,
        principal: PrincipalContext,
        workspace_id: str,
        scope: str,
    ) -> KrimSourceReader:
        store = getattr(self.runs, "store", None)
        if store is None or getattr(self.sessions, "store", None) is not store:
            raise ServiceError("workspace_unavailable", 503)
        return KrimSourceReader(
            store,
            self.sessions,
            principal,
            workspace_id=workspace_id,
            scope=scope,
            allow_legacy=principal.legacy and workspace_id == "local",
        )

    @staticmethod
    async def _reauthorize_imported_sources(
        reader: KrimSourceReader,
        scope: str,
        imported: list[dict[str, Any]],
    ) -> None:
        # 回执可在工作区共享；其中的Run来源仍按原会话所有者权限读取。
        for row in imported:
            source = row.get("source")
            if (
                not isinstance(source, dict)
                or source.get("kind") != "run"
                or not isinstance(source.get("run_id"), str)
                or not isinstance(source.get("evidence_hash"), str)
            ):
                raise ServiceError("facts_corrupt")
            current = await reader.read(
                scope,
                {"run_id": source["run_id"], "evidence_hash": source["evidence_hash"]},
            )
            if current.get("source") != source:
                raise ServiceError("source_changed")

    async def import_runs(
        self,
        principal: PrincipalContext,
        workspace_id: str,
        request: RunImportRequest,
    ) -> list[dict[str, Any]]:
        scope = self._authorize(principal, workspace_id, write=True)
        frozen = RunImportRequest.model_validate(request.model_dump(mode="json"))
        self._bounded(frozen.model_dump(mode="json"))
        reader = self._source_reader(principal, workspace_id, scope)
        continuous = ContinuousEvaluation(
            self.evaluation.repository,
            self.evaluation.kernel,
            scope=scope,
            actor=principal.principal_id,
            sources={"krim-runs": reader},
        )
        imported = await continuous.import_sources(
            ImportRequest(
                scope=scope,
                request_key=frozen.request_key,
                source_alias="krim-runs",
                sources=frozen.sources,
            )
        )
        await self._reauthorize_imported_sources(reader, scope, imported)
        return self._import_summary(imported)

    @staticmethod
    def _import_summary(imported: list[dict[str, Any]]) -> list[dict[str, Any]]:
        # 仅返回候选摘要；不把来源 Run 正文或事实包展开给浏览器。
        return [
            {
                "candidate_id": row.get("candidate_id", row["id"]),
                "observation_id": row["id"],
                "eligibility": row["eligibility"],
                "reasons": row["reasons"],
                "source": row["source"],
            }
            for row in imported
        ]

    async def import_receipt(
        self,
        principal: PrincipalContext,
        workspace_id: str,
        request_key: str,
    ) -> dict[str, Any]:
        scope = self._authorize(principal, workspace_id)
        if not 1 <= len(request_key) <= 64:
            raise ServiceError("invalid_input", 422)
        repository = self.evaluation.repository
        async with repository.lock:
            if repository.closed:
                raise ServiceError("closed")
            async with repository.engine.begin() as conn:
                receipt = (
                    await conn.execute(
                        select(source_observations.c.body).where(
                            source_observations.c.scope == scope,
                            source_observations.c.observation_id == "import:" + request_key,
                        )
                    )
                ).scalar_one_or_none()
        if receipt is None:
            raise ServiceError("not_found", 404)
        if (
            not isinstance(receipt, dict)
            or receipt.get("kind") != "import_receipt"
            or not isinstance(receipt.get("request_hash"), str)
        ):
            raise ServiceError("facts_corrupt")
        rows = await repository.import_observations(scope, request_key, receipt["request_hash"])
        if rows is None:
            raise ServiceError("facts_corrupt")
        reader = self._source_reader(principal, workspace_id, scope)
        await self._reauthorize_imported_sources(reader, scope, rows)
        return {"request_key": request_key, "candidates": self._import_summary(rows)}

    async def candidate(
        self,
        principal: PrincipalContext,
        workspace_id: str,
        candidate_id: str,
    ) -> dict[str, Any]:
        scope = self._authorize(principal, workspace_id)
        return await self.evaluation.repository.candidate(scope, candidate_id)

    async def confirm_candidate(
        self,
        principal: PrincipalContext,
        workspace_id: str,
        candidate_id: str,
        request: ProductConfirmationRequest,
    ) -> dict[str, Any]:
        scope = self._authorize(principal, workspace_id, write=True, approve=True)
        frozen = ProductConfirmationRequest.model_validate(request.model_dump(mode="json"))
        self._bounded(frozen.model_dump(mode="json"))
        try:
            confirmation = ConfirmationRequest(
                action=frozen.action,
                scope=scope,
                request_key=frozen.request_key,
                candidate_id=candidate_id,
                expected_revision=frozen.expected_revision,
                expected_hash=frozen.expected_hash,
                case=frozen.case,
                context=frozen.context,
                reason=frozen.reason,
            )
        except ValidationError:
            raise ServiceError("invalid_input", 422) from None
        return await self._continuous(principal, workspace_id).confirm_case(confirmation)

    async def freeze_confirmed(
        self,
        principal: PrincipalContext,
        workspace_id: str,
        request: FreezeConfirmedRequest,
    ) -> dict[str, Any]:
        scope = self._authorize(principal, workspace_id, write=True, approve=True)
        frozen = FreezeConfirmedRequest.model_validate(request.model_dump(mode="json"))
        self._bounded(frozen.model_dump(mode="json"))
        result = await self._continuous(principal, workspace_id).freeze_dataset(
            FreezeConfirmed(
                scope=scope,
                request_key=frozen.request_key,
                name=self._internal_name(workspace_id, frozen.name),
                version=frozen.version,
                confirmations=frozen.confirmations,
            )
        )
        lineage = result.get("lineage", {})
        return {
            "dataset": self._public_ref(workspace_id, result["dataset_ref"]),
            "dataset_envelope_fp": result["dataset_envelope_fp"],
            "lineage": {
                "reset_policy": lineage.get("reset_policy"),
                "cases": [
                    {key: case[key] for key in ("case_id", "context", "reset") if key in case}
                    for case in lineage.get("cases", [])
                ],
            },
        }

    def _continuous(self, principal: PrincipalContext, workspace_id: str) -> ContinuousEvaluation:
        return ContinuousEvaluation(
            self.evaluation.repository,
            self.evaluation.kernel,
            scope=self._scope(workspace_id),
            actor=principal.principal_id,
        )

    async def _dataset_ref(self, workspace_id: str, public_ref: DatasetRef) -> DatasetRef:
        internal = self._internal_name(workspace_id, public_ref.name)
        result = DatasetRef(
            name=internal,
            version=public_ref.version,
            fingerprint=public_ref.fingerprint,
        )
        dataset = await self.evaluation.repository.dataset(result.name, result.version)
        if dataset.ref() != result:
            raise ServiceError("invalid_input", 422)
        return result

    def _target(self, key: str) -> None:
        self._target_descriptor(key)

    async def start_task(
        self,
        principal: PrincipalContext,
        workspace_id: str,
        request: dict[str, Any],
    ) -> dict[str, Any]:
        scope = self._authorize(principal, workspace_id, write=True)
        if not isinstance(request, dict):
            raise ServiceError("invalid_input", 422)
        self._bounded(request)
        try:
            if request.get("schema_version") == "continuous-task/1":
                original = ContinuousRequest.model_validate(request)
                if original.schedule is not None:
                    raise ServiceError("invalid_input", 422)
                dataset = await self._dataset_ref(workspace_id, original.dataset)
                self._target(original.target_key)
                frozen = original.model_copy(update={"scope": scope, "dataset": dataset})
                result = await self._continuous(principal, workspace_id).submit(frozen)
            else:
                original_task = TaskSpec.model_validate(request)
                dataset = await self._dataset_ref(workspace_id, original_task.dataset)
                self._target(original_task.target_key)
                frozen_task = original_task.model_copy(update={"scope": scope, "dataset": dataset})
                result = await self.evaluation.run(frozen_task)
        except ValidationError:
            raise ServiceError("invalid_input", 422) from None
        return await self._task(principal, workspace_id, str(result["id"]))

    def _task_projection(self) -> Any:
        body = tasks.c.body
        request = body["request_json"]
        # 小批次把进度和报告保存在items/report内；大批次有独立计数字段。
        items = func.json_each(body, "$.items").table_valued("value").alias("product_items")
        finished = (
            select(func.count())
            .select_from(items)
            .where(func.json_extract(items.c.value, "$.status") == "finished")
            .correlate(tasks)
            .scalar_subquery()
        )
        return (
            tasks.c.id.label("id"),
            tasks.c.scope.label("scope"),
            tasks.c.request_key.label("request_key"),
            request["scope"].as_string().label("request_scope"),
            request["dataset"].as_json().label("dataset_ref"),
            request["target_key"].as_string().label("target_key"),
            request["metrics"].as_json().label("metrics"),
            request["mode"].as_string().label("mode"),
            request["schema_version"].as_string().label("schema_version"),
            body["layout"].as_integer().label("layout"),
            body["status"].as_string().label("status"),
            body["code"].as_string().label("code"),
            body["revision"].as_integer().label("revision"),
            body["planned_count"].as_integer().label("planned_count"),
            case(
                (func.coalesce(body["layout"].as_integer(), 2) == 2, finished),
                else_=body["finished_count"].as_integer(),
            ).label("finished_count"),
            body["created_at"].as_float().label("created_at"),
            body["finished_at"].as_float().label("finished_at"),
            func.coalesce(
                body["report_hash"].as_string(), body["report"]["report_hash"].as_string()
            ).label("report_hash"),
        )

    def _task_view(self, workspace_id: str, value: Any, expected_scope: str) -> dict[str, Any]:
        row = value._mapping if hasattr(value, "_mapping") else value
        if row["scope"] != expected_scope or row["request_scope"] != expected_scope:
            raise ServiceError("facts_corrupt")
        dataset_ref = row["dataset_ref"]
        if not isinstance(dataset_ref, dict):
            raise ServiceError("facts_corrupt")
        return {
            "id": row["id"],
            "request_key": row["request_key"],
            "status": row["status"],
            "code": row["code"],
            "revision": row["revision"],
            "mode": row["mode"] or "execute",
            "kind": "continuous" if row["schema_version"] == "continuous-task/1" else "manual",
            "layout": row["layout"] or 2,
            "dataset": self._public_ref(workspace_id, dataset_ref),
            "target_key": row["target_key"],
            "metrics": row["metrics"] or [],
            "planned_count": row["planned_count"],
            "finished_count": row["finished_count"],
            "created_at": row["created_at"],
            "finished_at": row["finished_at"],
            "report_hash": row["report_hash"],
        }

    async def _task(
        self, principal: PrincipalContext, workspace_id: str, task_id: str
    ) -> dict[str, Any]:
        scope = self._authorize(principal, workspace_id)
        repository = self.evaluation.repository
        async with repository.lock:
            if repository.closed:
                raise ServiceError("closed")
            async with repository.engine.begin() as conn:
                row = (
                    await conn.execute(
                        select(*self._task_projection()).where(
                            tasks.c.id == task_id,
                            tasks.c.scope == scope,
                        )
                    )
                ).first()
        if row is None:
            raise ServiceError("not_found", 404)
        return self._task_view(workspace_id, row, scope)

    async def task(
        self,
        principal: PrincipalContext,
        workspace_id: str,
        task_id: str,
    ) -> dict[str, Any]:
        self._authorize(principal, workspace_id)
        return await self._task(principal, workspace_id, task_id)

    async def task_by_key(
        self,
        principal: PrincipalContext,
        workspace_id: str,
        request_key: str,
    ) -> dict[str, Any]:
        scope = self._authorize(principal, workspace_id)
        repository = self.evaluation.repository
        async with repository.lock:
            if repository.closed:
                raise ServiceError("closed")
            async with repository.engine.begin() as conn:
                task_id = (
                    await conn.execute(
                        select(tasks.c.id).where(
                            tasks.c.scope == scope,
                            tasks.c.request_key == request_key,
                        )
                    )
                ).scalar_one_or_none()
        if task_id is None:
            raise ServiceError("not_found", 404)
        return await self._task(principal, workspace_id, task_id)

    async def list_tasks(
        self,
        principal: PrincipalContext,
        workspace_id: str,
        offset: int = 0,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        scope = self._authorize(principal, workspace_id)
        if (
            type(offset) is not int
            or type(limit) is not int
            or not 0 <= offset <= 100000
            or not 1 <= limit <= 50
        ):
            raise ServiceError("invalid_input", 422)
        repository = self.evaluation.repository
        async with repository.lock:
            if repository.closed:
                raise ServiceError("closed")
            async with repository.engine.begin() as conn:
                rows = (
                    await conn.execute(
                        select(*self._task_projection())
                        .where(tasks.c.scope == scope)
                        .order_by(tasks.c.request_key, tasks.c.id)
                        .offset(offset)
                        .limit(limit)
                    )
                ).all()
        return [self._task_view(workspace_id, row, scope) for row in rows]

    async def cancel_task(
        self,
        principal: PrincipalContext,
        workspace_id: str,
        task_id: str,
    ) -> dict[str, Any]:
        self._authorize(principal, workspace_id, write=True)
        # 先按scope核验归属，再让Kernel处理取消，未知ID不进入Kernel。
        await self._task(principal, workspace_id, task_id)
        await self.evaluation.kernel.cancel(task_id)
        return await self._task(principal, workspace_id, task_id)

    @staticmethod
    def _report_row(value: dict[str, Any]) -> dict[str, Any]:
        # 保留状态、逐指标评分和质量，不公开输出、文件路径或来源Run载荷。
        return {
            key: value[key]
            for key in ("case_id", "repetition", "status", "quality", "code", "scores")
            if key in value
        }

    async def report_page(
        self,
        principal: PrincipalContext,
        workspace_id: str,
        task_id: str,
        *,
        cursor: str | None = None,
        offset: int = 0,
        limit: int = 16,
    ) -> dict[str, Any]:
        scope = self._authorize(principal, workspace_id)
        task = await self._task(principal, workspace_id, task_id)
        if (
            type(offset) is not int
            or type(limit) is not int
            or not 0 <= offset <= 100000
            or not 1 <= limit <= 16
        ):
            raise ServiceError("invalid_input", 422)
        report = await self.evaluation.repository.report(task_id)
        summary = report.get("summary") or {}
        if report.get("schema_version") in {"safe-eval/scale/1", "safe-eval/scale/2"}:
            if offset != 0:
                raise ServiceError("invalid_input", 422)
            page = await self.evaluation.repository.report_page(
                scope, task_id, cursor=cursor, limit=limit
            )
            rows = [self._report_row(item) for item in page.rows]
            next_cursor = page.next_cursor
            next_offset = None
        else:
            if cursor is not None:
                raise ServiceError("invalid_input", 422)
            items = report.get("items")
            if not isinstance(items, list):
                raise ServiceError("facts_corrupt")
            selected = items[offset : offset + limit]
            rows = [self._report_row(item) for item in selected]
            next_cursor = None
            next_offset = offset + len(selected) if offset + len(selected) < len(items) else None
        planned = report.get("planned_count", summary.get("planned"))
        subject_fingerprint = report.get("subject_fingerprint")
        if subject_fingerprint is None and report.get("schema_version") == "safe-eval/1":
            subject, _ = partition(report["target_descriptor"], report["environment"])
            subject_fingerprint = neutral_fingerprint(subject)
        return {
            "eval_id": task_id,
            "revision": report.get("revision"),
            "report_hash": report.get("report_hash"),
            "kind": report.get("kind", task["kind"]),
            "subject_fingerprint": subject_fingerprint,
            "dataset": self._public_ref(workspace_id, report.get("dataset_ref", task["dataset"])),
            "execution": {
                "status": report.get("status"),
                "complete": report.get("complete"),
                "planned_count": planned,
                "usage_kind": report.get("usage_kind"),
            },
            "coverage": {
                key: summary.get(key)
                for key in [
                    "scored_cells",
                    "planned_cells",
                    "cell_coverage",
                    "item_coverage",
                    "skipped",
                    "errors",
                    "unexecuted_cells",
                ]
                if key in summary
            },
            "quality": {
                key: summary.get(key)
                for key in ["passed", "pass_rate", "metrics"]
                if key in summary
            },
            "rows": rows,
            "next_cursor": next_cursor,
            "next_offset": next_offset,
        }

    @staticmethod
    def _comparison_policy(request: ProductComparisonRequest) -> ComparisonPolicy:
        if not request.allowed_subjects:
            raise ServiceError("invalid_input", 422)
        return ComparisonPolicy(
            baseline_id=request.baseline_id,
            candidate_id=request.candidate_id,
            baseline_hash=request.baseline_hash,
            candidate_hash=request.candidate_hash,
            allowed_subjects=request.allowed_subjects,
        )

    @staticmethod
    def _invalid_comparison(policy: str, reason: str) -> dict[str, Any]:
        return {
            "status": "invalid",
            "policy": policy,
            "reasons": [{"code": reason}],
        }

    def _compare_krim(
        self,
        request: ProductComparisonRequest,
        baseline: dict[str, Any],
        candidate: dict[str, Any],
    ) -> dict[str, Any]:
        policy = "strict-krim-regression/1"
        try:
            before, after = validate_report(baseline), validate_report(candidate)
            for report in (before, after):
                if (
                    report["schema_version"] != "safe-eval/1"
                    or not report["complete"]
                    or report["status"] != "completed"
                    or report["summary"]["cell_coverage"] != 1
                ):
                    return self._invalid_comparison(policy, "incomplete_coverage")
            subject_before, conditions_before = partition(
                before["target_descriptor"], before["environment"]
            )
            subject_after, conditions_after = partition(
                after["target_descriptor"], after["environment"]
            )
            subjects = {neutral_fingerprint(subject_before), neutral_fingerprint(subject_after)}
            if request.allowed_subjects and subjects != set(request.allowed_subjects):
                return self._invalid_comparison(policy, "unapproved_subject")
            if len(subjects) == 1:
                return gate_reports(baseline, candidate)
            if not request.allowed_subjects:
                return self._invalid_comparison(policy, "unapproved_subject")
            if neutral_fingerprint(conditions_before) != neutral_fingerprint(conditions_after):
                return self._invalid_comparison(policy, "incompatible_conditions")
            for field in ("dataset_fingerprint", "rules_fingerprint", "execution_fingerprint"):
                if before[field] != after[field]:
                    return self._invalid_comparison(policy, "incompatible_" + field)
            if {(row["case_id"], row["repetition"]) for row in before["items"]} != {
                (row["case_id"], row["repetition"]) for row in after["items"]
            }:
                return self._invalid_comparison(policy, "incompatible_cases")
            reasons = regression_reasons(before, after)
            return {
                "status": "fail" if reasons else "pass",
                "policy": policy,
                "baseline": {"eval_id": before["eval_id"], "report_hash": before["report_hash"]},
                "candidate": {"eval_id": after["eval_id"], "report_hash": after["report_hash"]},
                "reasons": reasons,
            }
        except (ValueError, TypeError, KeyError, ServiceError):
            return self._invalid_comparison(policy, "report_invalid")

    async def compare(
        self,
        principal: PrincipalContext,
        workspace_id: str,
        request: ProductComparisonRequest,
    ) -> dict[str, Any]:
        scope = self._authorize(principal, workspace_id)
        frozen = ProductComparisonRequest.model_validate(request.model_dump(mode="json"))
        before = await self._task(principal, workspace_id, frozen.baseline_id)
        after = await self._task(principal, workspace_id, frozen.candidate_id)
        if before["kind"] != after["kind"]:
            raise ServiceError("invalid_input", 422)

        if before["layout"] == after["layout"] == 3:
            policy = self._comparison_policy(frozen)
            return await self.evaluation.repository.compare_paged(scope, policy)
        if before["layout"] == 3 or after["layout"] == 3:
            return self._invalid_comparison("strict-regression/1", "report_pages_required")

        baseline = await self.evaluation.repository.report(frozen.baseline_id)
        candidate = await self.evaluation.repository.report(frozen.candidate_id)
        if before["kind"] == "continuous":
            return compare_reports(self._comparison_policy(frozen), baseline, candidate)
        if (
            baseline.get("report_hash") != frozen.baseline_hash
            or candidate.get("report_hash") != frozen.candidate_hash
        ):
            return self._invalid_comparison("strict-regression/1", "report_changed")
        if all(
            report.get("environment", {}).get("execution_conditions", {}).get("schema_version")
            == "krim-run/1"
            for report in (baseline, candidate)
        ):
            return self._compare_krim(frozen, baseline, candidate)
        return gate_reports(baseline, candidate)
