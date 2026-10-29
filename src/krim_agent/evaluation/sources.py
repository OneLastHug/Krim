"""显式来源冷读；只借用已授权的Repository，不拥有源或执行资源。"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any, cast

from sqlalchemy import select

from krim_agent.contracts import ServiceError

from .domain import Observation, clone, neutral_fingerprint
from .reports import make_neutral_report, validate_neutral_report
from .repository import evidence, meta


class EvalSourceReader:
    def __init__(
        self,
        repository: Any,
        *,
        scope: str = "local",
        stable_source_id: str | None = None,
        include_context: bool = True,
    ) -> None:
        self.repository, self.scope = repository, scope
        self.stable_source_id = stable_source_id
        self.include_context = include_context
        self._initial_binding = self.binding()

    def binding(self) -> tuple[object, ...]:
        method = self.repository.snapshot
        readers = [self.repository.report, self.repository.read_trial, self.repository.case_page]
        st = self.repository.path.lstat()
        return (
            self.repository,
            self.repository.engine,
            self.scope,
            self.stable_source_id,
            self.include_context,
            getattr(method, "__func__", method),
            *(getattr(reader, "__func__", reader) for reader in readers),
            st.st_dev,
            st.st_ino,
            hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        )

    async def read(self, scope: str, selection: dict[str, Any]) -> dict[str, Any]:
        from .repository import tasks

        if scope != self.scope:
            raise ServiceError("not_found", 404)
        binding = self.binding()
        if binding != self._initial_binding:
            raise ServiceError("target_binding_changed")

        def guard() -> None:
            if self.repository.closed or self.binding() != binding:
                raise ServiceError("target_binding_changed")

        async with self.repository.lock, self.repository.engine.begin() as conn:
            allowed_scope = (
                await conn.execute(select(tasks.c.scope).where(tasks.c.id == selection["task_id"]))
            ).scalar_one_or_none()
            guard()
            if allowed_scope != scope:
                raise ServiceError("not_found", 404)
        guard()
        snapshot = await self.repository.snapshot(selection["task_id"])
        guard()
        if snapshot["request_json"]["scope"] != scope:
            raise ServiceError("not_found", 404)
        if selection.get("revision") not in {None, snapshot["revision"]}:
            raise ServiceError("source_changed")
        if snapshot.get("layout") == 3:
            from .domain import CaseSpec, neutral_encoded, neutral_parse
            from .repository import reports, trials
            from .scale_reports import validate_manifest, validate_page

            # 规模来源只读取一个完整Trial和它所在的真实页，不重建旧items/dataset。
            case_id, repetition = selection["case_id"], selection["repetition"]
            if not isinstance(case_id, str) or type(repetition) is not int:
                raise ServiceError("invalid_input", 422)
            if snapshot.get("report_hash") is None:
                raise ServiceError("source_not_eligible")
            try:
                snapshot_hash = neutral_fingerprint(snapshot, max_bytes=262144)
                if (
                    snapshot["id"] != selection["task_id"]
                    or snapshot["request_hash"] != neutral_fingerprint(snapshot["request_json"])
                    or snapshot["dataset_ref"] != snapshot["request_json"]["dataset"]
                    or snapshot["status"] in {"queued", "running"}
                ):
                    raise ServiceError("facts_corrupt")
                if (
                    case_id not in snapshot["case_index"]
                    or not 0 <= repetition < snapshot["execution"]["repetitions"]
                ):
                    raise ServiceError("not_found", 404)
                plan_hash = neutral_fingerprint(
                    {
                        "request_hash": snapshot["request_hash"],
                        "dataset_ref": snapshot["dataset_ref"],
                        "case_index": snapshot["case_index"],
                        "target": snapshot["target"],
                        "continuous": snapshot.get("continuous"),
                    }
                )
                if plan_hash != snapshot["plan_fingerprint"]:
                    raise ServiceError("facts_corrupt")
                root = await self.repository.report(snapshot["id"])
                guard()
                root = validate_manifest(root)
                context = snapshot.get("continuous") or {}
                expected_root = {
                    "eval_id": snapshot["id"],
                    "revision": snapshot["revision"],
                    "request_hash": snapshot["request_hash"],
                    "report_hash": snapshot["report_hash"],
                    "status": snapshot["status"],
                    "code": snapshot["code"],
                    "dataset_ref": snapshot["dataset_ref"],
                    "case_index": snapshot["case_index"],
                    "target_descriptor": snapshot["target"],
                    "environment": snapshot["environment"],
                    "rules": snapshot["rules"],
                    "execution": snapshot["execution"],
                    "planned_count": snapshot["planned_count"],
                    "plan_fingerprint": plan_hash,
                    "capacity": snapshot["capacity"],
                    "kind": context.get("mode", "execute"),
                    "dataset_envelope_fp": context.get("dataset_envelope_fp"),
                    "lineage_fingerprint": context.get("lineage_fingerprint"),
                    "source_inputs_fingerprint": context.get("inputs_fingerprint"),
                    "parent": context.get("parent"),
                    "seed_policy": context.get("seed_policy", "unseeded/1"),
                }
                if any(
                    neutral_fingerprint(root[key]) != neutral_fingerprint(value)
                    for key, value in expected_root.items()
                ):
                    raise ServiceError("facts_corrupt")
                descriptors = [
                    page
                    for page in root["pages"]
                    if tuple(page["first"]) <= (case_id, repetition) <= tuple(page["last"])
                ]
                if len(descriptors) != 1:
                    raise ServiceError("facts_corrupt")
                descriptor = descriptors[0]
                item = await self.repository.read_trial(scope, snapshot["id"], case_id, repetition)
                guard()
                fields = [
                    "case_id",
                    "repetition",
                    "status",
                    "execution_ref",
                    "output_digest",
                    "scores",
                    "quality",
                    "code",
                    "content_hash",
                ]
                item_hash = neutral_fingerprint({key: item[key] for key in fields})
                output = (
                    None
                    if item["output"] is None
                    else Observation.model_validate(item["output"]).model_dump(mode="json")
                )
                digest = neutral_fingerprint(output, max_bytes=10485760)
                if (
                    item["case_id"] != case_id
                    or type(item["repetition"]) is not int
                    or item["repetition"] != repetition
                    or item["output_digest"] != (None if output is None else digest)
                    or selection.get("evidence_hash") not in {None, digest}
                    or (output is not None and output["execution_ref"] != item["execution_ref"])
                ):
                    raise ServiceError("facts_corrupt")
                if item["content_hash"] is not None:
                    payload = {key: item[key] for key in ["scores", "quality", "code"]}
                    payload["output"] = output
                    if neutral_fingerprint(payload) != item["content_hash"]:
                        raise ServiceError("facts_corrupt")

                # 独立调用结束后在同一只读快照内复核归属、头、页、Trial与完整证据。
                async with self.repository.lock, self.repository.engine.begin() as conn:
                    guard()
                    await conn.exec_driver_sql("BEGIN")
                    guard()
                    current_scope = (
                        await conn.execute(
                            select(tasks.c.scope).where(tasks.c.id == snapshot["id"])
                        )
                    ).scalar_one_or_none()
                    guard()
                    if current_scope != allowed_scope:
                        raise ServiceError("not_found", 404)
                    current = (
                        await conn.execute(select(tasks.c.body).where(tasks.c.id == snapshot["id"]))
                    ).scalar_one_or_none()
                    guard()
                    if neutral_fingerprint(current, max_bytes=262144) != snapshot_hash:
                        raise ServiceError("source_changed")
                    current_root = (
                        await conn.execute(
                            select(reports.c.body).where(reports.c.task_id == snapshot["id"])
                        )
                    ).scalar_one_or_none()
                    guard()
                    if not isinstance(current_root, dict):
                        raise ServiceError("source_changed")
                    validate_manifest(current_root)
                    if neutral_fingerprint(current_root, max_bytes=262144) != neutral_fingerprint(
                        root, max_bytes=262144
                    ):
                        raise ServiceError("source_changed")
                    ref = snapshot["dataset_ref"]
                    case_row = (
                        await conn.exec_driver_sql(
                            "SELECT body, case_hash FROM dataset_cases "
                            "WHERE name=? AND version=? AND case_id=?",
                            (ref["name"], ref["version"], case_id),
                        )
                    ).one_or_none()
                    guard()
                    if case_row is None or len(case_row[0].encode()) > 1048576:
                        raise ServiceError("facts_corrupt")
                    case = CaseSpec.model_validate(neutral_parse(case_row[0])).model_dump(
                        mode="json"
                    )
                    case_hash = neutral_fingerprint(case, max_bytes=1048576)
                    if (
                        case["case_id"] != case_id
                        or case_hash != case_row[1]
                        or case_hash != snapshot["case_index"][case_id]
                    ):
                        raise ServiceError("facts_corrupt")
                    page_body = (
                        await conn.exec_driver_sql(
                            "SELECT body FROM eval_report_pages "
                            "WHERE task_id=? AND report_revision=? AND page_index=?",
                            (snapshot["id"], snapshot["revision"], descriptor["index"]),
                        )
                    ).scalar_one_or_none()
                    guard()
                    if page_body is None or len(page_body.encode()) > 2097152:
                        raise ServiceError("facts_corrupt")
                    page_items = validate_page(neutral_parse(page_body), descriptor, current_root)
                    matching = [
                        row
                        for row in page_items
                        if (row["case_id"], row["repetition"]) == (case_id, repetition)
                    ]
                    if len(matching) != 1:
                        raise ServiceError("facts_corrupt")
                    page_item_hash = neutral_fingerprint({key: matching[0][key] for key in fields})
                    if page_item_hash != item_hash:
                        raise ServiceError("facts_corrupt")
                    current_item = (
                        await conn.execute(
                            select(trials.c.body).where(
                                trials.c.task_id == snapshot["id"],
                                trials.c.case_id == case_id,
                                trials.c.repetition == repetition,
                            )
                        )
                    ).scalar_one_or_none()
                    guard()
                    if current_item is None:
                        raise ServiceError("source_changed")
                    current_item_hash = neutral_fingerprint(
                        {key: current_item[key] for key in fields}
                    )
                    if current_item_hash != item_hash:
                        raise ServiceError("source_changed")
                    marker = (
                        await conn.execute(select(meta.c.body).where(meta.c.id == 1))
                    ).scalar_one()
                    guard()
                    repository_id = marker.get("repository_id")
                    if not isinstance(repository_id, str) or not repository_id:
                        raise ServiceError("source_changed")
                    if output is not None:
                        canonical = (
                            await conn.execute(
                                select(evidence.c.body).where(evidence.c.digest == digest)
                            )
                        ).scalar_one_or_none()
                        guard()
                        if neutral_fingerprint(canonical, max_bytes=10485760) != digest:
                            raise ServiceError("facts_corrupt")
                    result = {
                        "source": {
                            "kind": "trial",
                            "repository_id": repository_id,
                            "reader_version": "cold-eval/scale/1",
                            "reader_digest": binding[-1],
                            "task_id": snapshot["id"],
                            "revision": snapshot["revision"],
                            "case_id": case_id,
                            "repetition": repetition,
                            "execution_ref": item["execution_ref"],
                            "evidence_hash": digest,
                            "report_hash": current_root["report_hash"],
                            "request_hash": snapshot["request_hash"],
                            "status": snapshot["status"],
                            "code": snapshot["code"],
                            "item_status": item["status"],
                            "item_quality": item["quality"],
                            "item_code": item["code"],
                        },
                        "target": snapshot["target"],
                        "environment": snapshot["environment"],
                        "output": output,
                        "terminal": True,
                        "case": case if self.include_context else None,
                    }
                    neutral_encoded(result)
                    guard()
                guard()
                return cast(dict[str, Any], clone(result))
            except (KeyError, TypeError, ValueError):
                raise ServiceError("facts_corrupt") from None
        report = validate_neutral_report(make_neutral_report(snapshot))
        item = next(
            (
                i
                for i in snapshot["items"]
                if (i["case_id"], i["repetition"])
                == (selection["case_id"], selection["repetition"])
            ),
            None,
        )
        if item is None:
            raise ServiceError("not_found", 404)
        output = (
            None
            if item["output"] is None
            else Observation.model_validate(item["output"]).model_dump(mode="json")
        )
        digest = neutral_fingerprint(output)
        if selection.get("evidence_hash") not in {None, digest}:
            raise ServiceError("facts_corrupt")
        if output is not None and item["content_hash"] is not None:
            payload = {k: item[k] for k in ["output", "scores", "quality", "code"]}
            if neutral_fingerprint(payload) != item["content_hash"]:
                raise ServiceError("facts_corrupt")
        async with self.repository.lock, self.repository.engine.begin() as conn:
            current_scope = (
                await conn.execute(select(tasks.c.scope).where(tasks.c.id == selection["task_id"]))
            ).scalar_one_or_none()
            guard()
            if current_scope != allowed_scope:
                raise ServiceError("not_found", 404)
            marker = (await conn.execute(select(meta.c.body))).scalar_one()
            guard()
            if output is not None:
                canonical = (
                    await conn.execute(select(evidence.c.body).where(evidence.c.digest == digest))
                ).scalar_one_or_none()
                guard()
                if canonical != output or neutral_fingerprint(canonical) != digest:
                    raise ServiceError("facts_corrupt")
        repository_id = marker.get("repository_id") or self.stable_source_id
        if not repository_id:
            raise ServiceError("source_changed")
        return cast(
            dict[str, Any],
            clone(
                {
                    "source": {
                        "kind": "trial",
                        "repository_id": repository_id,
                        "reader_version": "cold-eval/1",
                        "reader_digest": binding[-1],
                        "task_id": snapshot["id"],
                        "revision": snapshot["revision"],
                        "case_id": item["case_id"],
                        "repetition": item["repetition"],
                        "execution_ref": item["execution_ref"],
                        "evidence_hash": digest,
                        "report_hash": report["report_hash"],
                        "request_hash": snapshot["request_hash"],
                        "status": snapshot["status"],
                        "code": snapshot["code"],
                        "item_status": item["status"],
                        "item_quality": item["quality"],
                        "item_code": item["code"],
                    },
                    "target": snapshot["target"],
                    "environment": snapshot["environment"],
                    "output": output,
                    "terminal": snapshot["status"] not in {"queued", "running"},
                    "case": next(
                        c for c in snapshot["dataset"]["cases"] if c["case_id"] == item["case_id"]
                    )
                    if self.include_context
                    else None,
                }
            ),
        )
