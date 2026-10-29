"""持续评测连接内操作；不创建engine、事务、任务或执行资源。"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any, cast

from sqlalchemy import insert, select, update
from sqlalchemy.ext.asyncio import AsyncConnection

from krim_agent.contracts import ServiceError

from .domain import (
    DATASET_BYTES,
    OBSERVATION_BYTES,
    PERSISTED_TASK_BYTES,
    REPORT_BYTES,
    SNAPSHOT_BYTES,
    Budget,
    DatasetSpec,
    clone,
    execution_conditions,
    neutral_encoded,
    neutral_fingerprint,
)
from .repository import (
    case_candidates,
    case_confirmations,
    continuous_inputs,
    dataset_lineage,
    datasets,
    eval_schedule_windows,
    eval_schedules,
    evidence,
    source_observations,
    tasks,
)


async def observation(conn: AsyncConnection, scope: str, identity: str) -> dict[str, Any]:
    value = (
        await conn.execute(
            select(source_observations.c.body).where(
                source_observations.c.scope == scope,
                source_observations.c.observation_id == identity,
            )
        )
    ).scalar_one_or_none()
    if value is None or value.get("kind") != "observation":
        raise ServiceError("not_found", 404)
    return cast(dict[str, Any], clone(value))


async def candidate(conn: AsyncConnection, scope: str, identity: str) -> dict[str, Any]:
    rows = (
        (
            await conn.execute(
                select(case_candidates.c.body)
                .where(
                    case_candidates.c.scope == scope,
                    case_candidates.c.candidate_id == identity,
                )
                .order_by(case_candidates.c.revision.desc())
                .limit(1)
            )
        )
        .scalars()
        .all()
    )
    if not rows:
        raise ServiceError("not_found", 404)
    return cast(dict[str, Any], clone(rows[0]))


async def import_rows(
    conn: AsyncConnection,
    scope: str,
    key: str,
    request_hash: str,
    rows: list[dict[str, Any]] | None,
    guard: Callable[[], None],
) -> list[dict[str, Any]] | None:
    # An immutable typed operation receipt shares the source-import table. It
    # records even a fully sampled-out batch; it is never returned as evidence.
    receipt_id = "import:" + key
    old = (
        await conn.execute(
            select(source_observations.c.body).where(
                source_observations.c.scope == scope,
                source_observations.c.observation_id == receipt_id,
            )
        )
    ).scalar_one_or_none()
    guard()
    if old is not None:
        if old.get("kind") != "import_receipt" or old["request_hash"] != request_hash:
            raise ServiceError("key_conflict")
        result = []
        for identity, candidate_id in zip(
            old["observation_ids"], old["candidate_ids"], strict=True
        ):
            value = await observation(conn, scope, identity)
            guard()
            if candidate_id != identity:
                value["candidate_id"] = candidate_id
            result.append(value)
        return result
    if rows is None:
        return None
    neutral_encoded(rows, max_bytes=32 * 1048576)
    result = []
    for incoming in rows:
        row = clone(incoming)
        feedback = row.pop("_feedback", None)
        guard()
        identity = row["id"]
        present = (
            await conn.execute(
                select(source_observations.c.body).where(
                    source_observations.c.scope == scope,
                    source_observations.c.observation_id == identity,
                )
            )
        ).scalar_one_or_none()
        guard()
        if present is None:
            await conn.execute(
                insert(source_observations).values(
                    scope=scope,
                    observation_id=identity,
                    dedup_digest=identity,
                    body=row,
                )
            )
            guard()
            draft = {
                "candidate_id": identity,
                "revision": 0,
                "status": "pending",
                "observation_id": identity,
                "observation_hash": row["content_hash"],
                "eligibility": row["eligibility"],
                "reasons": row["reasons"],
            }
            draft["hash"] = neutral_fingerprint(draft)
            await conn.execute(
                insert(case_candidates).values(
                    scope=scope,
                    candidate_id=identity,
                    revision=0,
                    body=draft,
                )
            )
            guard()
            present = row
        elif present != row:
            raise ServiceError("source_changed")
        selected = clone(present)
        if feedback is not None:
            candidate_id = neutral_fingerprint(
                {
                    "observation_id": identity,
                    "actor": feedback["actor"],
                    "feedback_key": feedback["key"],
                }
            )
            prior = (
                await conn.execute(
                    select(case_candidates.c.body).where(
                        case_candidates.c.scope == scope,
                        case_candidates.c.candidate_id == candidate_id,
                        case_candidates.c.revision == 0,
                    )
                )
            ).scalar_one_or_none()
            guard()
            signal = {
                "candidate_id": candidate_id,
                "revision": 0,
                "status": "pending",
                "observation_id": identity,
                "observation_hash": row["content_hash"],
                "eligibility": row["eligibility"],
                "reasons": ["human_feedback"],
                "signals": [feedback],
            }
            signal["hash"] = neutral_fingerprint(signal)
            if prior is None:
                await conn.execute(
                    insert(case_candidates).values(
                        scope=scope, candidate_id=candidate_id, revision=0, body=signal
                    )
                )
                guard()
            elif prior != signal:
                raise ServiceError("key_conflict")
            selected["candidate_id"] = candidate_id
        result.append(selected)
    receipt = {
        "kind": "import_receipt",
        "request_hash": request_hash,
        "observation_ids": [r["id"] for r in result],
        "candidate_ids": [r.get("candidate_id", r["id"]) for r in result],
    }
    await conn.execute(
        insert(source_observations).values(
            scope=scope,
            observation_id=receipt_id,
            dedup_digest=neutral_fingerprint({"scope": scope, "receipt": key}),
            body=receipt,
        )
    )
    guard()
    return result


async def confirmation_receipt(
    conn: AsyncConnection,
    scope: str,
    key: str,
    request_hash: str,
) -> dict[str, Any] | None:
    value = (
        await conn.execute(
            select(case_confirmations.c.body).where(
                case_confirmations.c.scope == scope,
                case_confirmations.c.confirmation_key == key,
            )
        )
    ).scalar_one_or_none()
    if value is not None and value["request_hash"] != request_hash:
        raise ServiceError("key_conflict")
    return cast(dict[str, Any] | None, clone(value))


async def confirm_case(
    conn: AsyncConnection,
    request: dict[str, Any],
    actor: str,
    request_hash: str,
    guard: Callable[[], None],
) -> dict[str, Any]:
    scope, key = request["scope"], request["request_key"]
    old = await confirmation_receipt(conn, scope, key, request_hash)
    guard()
    if old is not None:
        return old
    draft = await candidate(conn, scope, request["candidate_id"])
    guard()
    if (
        draft["revision"] != request["expected_revision"]
        or draft["hash"] != request["expected_hash"]
        or draft["status"] != "pending"
    ):
        raise ServiceError("snapshot_changed")
    source = await observation(conn, scope, draft["observation_id"])
    guard()
    if request["action"] == "confirm" and (
        source["eligibility"] == "ineligible"
        or (
            source["eligibility"] == "needs_context"
            and request["context"] != "manual_reconstructed"
        )
    ):
        raise ServiceError("source_not_eligible")
    status = {"confirm": "confirmed", "edit": "pending", "reject": "rejected"}[request["action"]]
    body = {
        "status": status,
        "action": request["action"],
        "request_hash": request_hash,
        "actor": actor,
        "case": request["case"],
        "context": request["context"],
        "reason": request["reason"],
        "source_refs": {
            "observation_id": source["id"],
            "observation_hash": source["content_hash"],
            "source": source["source"],
        },
        "reset": {
            "version": "fresh-files/1",
            "fingerprint": neutral_fingerprint(request["case"]["files"]),
        },
        "policy": "human-confirmation/1",
    }
    body["id"] = neutral_fingerprint({"scope": scope, "key": key, "request_hash": request_hash})
    body["content_hash"] = neutral_fingerprint(body)
    await conn.execute(
        insert(case_confirmations).values(scope=scope, confirmation_key=key, body=body)
    )
    guard()
    updated = {k: v for k, v in draft.items() if k != "hash"}
    updated.update(
        revision=draft["revision"] + 1,
        status=status,
        confirmation_id=body["id"],
        draft_case=request["case"],
        draft_context=request["context"],
    )
    updated["hash"] = neutral_fingerprint(updated)
    await conn.execute(
        insert(case_candidates).values(
            scope=scope,
            candidate_id=draft["candidate_id"],
            revision=updated["revision"],
            body=updated,
        )
    )
    guard()
    return cast(dict[str, Any], clone(body))


def reserve_capacity(
    dataset: DatasetSpec, budget: Budget, context: dict[str, Any] | None = None
) -> dict[str, Any]:
    if len(dataset.cases) > 8 or budget.concurrency == 2:
        from .scale_store import capacity

        return capacity(dataset, budget, context)
    dataset_bytes = len(
        neutral_encoded(dataset.model_dump(mode="json"), max_bytes=DATASET_BYTES).encode()
    )
    extra = 0 if context is None else len(neutral_encoded(context, max_bytes=1048576).encode())
    planned = len(dataset.cases) * budget.repetitions
    report_reservation = dataset_bytes + planned * (OBSERVATION_BYTES + 65536) + 1048576 + extra
    if report_reservation > REPORT_BYTES or 2 * report_reservation > SNAPSHOT_BYTES:
        raise ServiceError("body_too_large", 422)
    storage_reservation = (
        4 * dataset_bytes + 5 * planned * OBSERVATION_BYTES + 16 * 1048576 + 3 * extra
    )
    if storage_reservation > PERSISTED_TASK_BYTES:
        raise ServiceError("body_too_large", 422)
    value = {
        "schema_version": "neutral-capacity/1",
        "dataset_bytes": dataset_bytes,
        "observation_bytes": OBSERVATION_BYTES,
        "report_bytes": REPORT_BYTES,
        "snapshot_bytes": SNAPSHOT_BYTES,
        "reserved_report_bytes": report_reservation,
        "reserved_storage_bytes": storage_reservation,
        "max_task_storage_bytes": PERSISTED_TASK_BYTES,
    }
    if context is not None:
        value["continuous_bytes"] = extra
    return value


async def _source_input(
    conn: AsyncConnection,
    selected: Any,
    request: Any,
    binding: dict[str, Any] | None,
    guard: Callable[[], None],
    *,
    legacy: bool = False,
) -> tuple[str | dict[str, Any], dict[str, Any], dict[str, Any]]:
    """在原连接内只持有一份来源正文；preview不写入，legacy至多24项。"""
    from .domain import Observation

    # 只有本次同连接input_for_trial/trial_facts核验所得对象可复用。
    verified_rescore_output: object = object()

    async def publish(
        output: dict[str, Any] | None,
        metadata: dict[str, Any],
        actual: dict[str, Any],
        environment: dict[str, Any],
    ) -> tuple[str | dict[str, Any], dict[str, Any], dict[str, Any]]:
        output_hash = neutral_fingerprint(output, max_bytes=OBSERVATION_BYTES)
        if metadata["output_hash"] != output_hash:
            raise ServiceError("facts_corrupt")
        if legacy:
            return dict(metadata, output=output), actual, environment
        if binding is None:
            # freeze_schedule只验证真实template，尚没有目的Task可签发输入包。
            return neutral_fingerprint(metadata, max_bytes=1048576), actual, environment
        packet = {
            "schema_version": "continuous-input/1",
            "binding_B": binding,
            "case_id": metadata["case_id"],
            "repetition": metadata["repetition"],
            "execution_ref": None if output is None else output["execution_ref"],
            "output_digest": None if output is None else output_hash,
            "metadata": metadata,
        }
        packet_hash = neutral_fingerprint(packet, max_bytes=1048576)
        guard()
        if output is not None and output is not verified_rescore_output:
            existing = (
                await conn.execute(select(evidence.c.body).where(evidence.c.digest == output_hash))
            ).scalar_one_or_none()
            guard()
            if existing is None:
                await conn.execute(insert(evidence).values(digest=output_hash, body=output))
                guard()
            elif neutral_fingerprint(existing, max_bytes=OBSERVATION_BYTES) != output_hash:
                raise ServiceError("facts_corrupt")
            del existing
        await conn.execute(
            insert(continuous_inputs).values(
                task_id=binding["task_id"],
                case_id=metadata["case_id"],
                repetition=metadata["repetition"],
                body=packet,
            )
        )
        guard()
        return packet_hash, actual, environment

    if request.mode == "rescore":
        from .repository import reports as report_rows
        from .scale_reports import validate_page
        from .scale_store import eval_report_pages, input_for_trial

        parent, root = selected["parent"], selected["root"]
        case_id, repetition = selected["case_id"], selected["repetition"]
        incoming = await input_for_trial(
            conn, parent, case_id, repetition, guard, lineage=selected["lineage"]
        )
        guard()
        descriptor = next(
            (
                page
                for page in root["pages"]
                if tuple(page["first"]) <= (case_id, repetition) <= tuple(page["last"])
            ),
            None,
        )
        if descriptor is None:
            raise ServiceError("facts_corrupt")
        page = (
            await conn.execute(
                select(eval_report_pages.c.body).where(
                    eval_report_pages.c.task_id == parent["id"],
                    eval_report_pages.c.report_revision == root["revision"],
                    eval_report_pages.c.page_index == descriptor["index"],
                )
            )
        ).scalar_one_or_none()
        guard()
        if page is None:
            raise ServiceError("facts_corrupt")
        rows = validate_page(page, descriptor, root)
        published = next(
            (row for row in rows if (row["case_id"], row["repetition"]) == (case_id, repetition)),
            None,
        )
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
        if published is None or neutral_fingerprint({k: published[k] for k in fields}) != (
            neutral_fingerprint({k: incoming[k] for k in fields})
        ):
            raise ServiceError("facts_corrupt")
        del published, rows, page
        current_root = (
            await conn.execute(
                select(report_rows.c.body["report_hash"].as_string()).where(
                    report_rows.c.task_id == parent["id"]
                )
            )
        ).scalar_one()
        guard()
        if current_root != root["report_hash"]:
            raise ServiceError("source_changed")
        output = incoming.pop("output")
        verified_rescore_output = output
        if output is not None:
            Observation.model_validate(output)
        frozen = incoming.get("_continuous") or {}
        proof, context_hash = clone(frozen.get("source_proof")), frozen.get("source_context_hash")
        if (parent.get("continuous") or {}).get("mode", "execute") == "execute":
            proof = {
                "terminal": parent["status"] not in {"queued", "running"},
                "complete": incoming["status"] == "finished"
                and output is not None
                and output["execution_ref"] == incoming["execution_ref"],
                "known_hard_failure": incoming["quality"] == "failed",
                "environment": clone(parent["environment"]),
            }
            context_hash = (
                neutral_fingerprint(
                    {key: selected["case"][key] for key in ["instruction", "files", "verifier"]}
                )
                if proof["complete"]
                else None
            )
        metadata = {
            "case_id": case_id,
            "repetition": repetition,
            "source_ref": {
                "parent": {
                    "task_id": parent["id"],
                    "report_hash": root["report_hash"],
                    "kind": root["kind"],
                },
                "execution_ref": incoming["execution_ref"],
            },
            "known_hard_failure": incoming["quality"] == "failed"
            or frozen.get("known_hard_failure", False),
            "availability": clone(frozen.get("availability")),
            "source_context_hash": context_hash,
            "source_proof": proof,
            "output_hash": neutral_fingerprint(output, max_bytes=OBSERVATION_BYTES),
        }
        actual, environment = clone(parent["target"]), clone(parent["environment"])
        environment.pop("native_admission", None)
        guard()
        return await publish(output, metadata, actual, environment)

    row = await observation(conn, request.scope, selected.observation_id)
    guard()
    if (
        row.get("scope") != request.scope
        or row["content_hash"] != selected.observation_hash
        or neutral_fingerprint({k: v for k, v in row.items() if k != "content_hash"})
        != row["content_hash"]
        or row["source"].get("case_id") != selected.case_id
        or type(row["source"].get("repetition")) is not int
        or row["source"]["repetition"] != selected.repetition
    ):
        raise ServiceError("facts_corrupt")
    output = row.pop("output")
    if output is not None:
        Observation.model_validate(output)
    metadata = {
        "case_id": selected.case_id,
        "repetition": selected.repetition,
        "source_ref": {
            "observation_id": row["id"],
            "observation_hash": row["content_hash"],
            "source": row["source"],
        },
        "known_hard_failure": row.get("source_proof", {}).get("known_hard_failure", False),
        "source_proof": row.get("source_proof"),
        "availability": row["availability"],
        "source_context_hash": row["context_hash"],
        "output_hash": neutral_fingerprint(output, max_bytes=OBSERVATION_BYTES),
    }
    actual, environment = clone(row["target"]), clone(row["environment"])
    environment.pop("native_admission", None)
    guard()
    return await publish(output, metadata, actual, environment)


async def prepare_task(
    conn: AsyncConnection,
    request: Any,
    target: dict[str, Any] | None,
    guard: Callable[[], None],
    *,
    task_id: str | None = None,
) -> tuple[DatasetSpec, dict[str, Any], dict[str, Any], dict[str, Any], list[dict[str, Any]]]:
    value = (
        await conn.execute(
            select(datasets.c.body).where(
                datasets.c.name == request.dataset.name,
                datasets.c.version == request.dataset.version,
            )
        )
    ).scalar_one_or_none()
    guard()
    frozen = (
        await conn.execute(
            select(dataset_lineage.c.body).where(
                dataset_lineage.c.dataset_name == request.dataset.name,
                dataset_lineage.c.version == request.dataset.version,
            )
        )
    ).scalar_one_or_none()
    guard()
    if value is None or frozen is None or frozen["lineage"]["scope"] != request.scope:
        raise ServiceError("not_found", 404)
    dataset = DatasetSpec.model_validate(value)
    expected_envelope = neutral_fingerprint(
        {"dataset_ref": dataset.ref().model_dump(mode="json"), "lineage": frozen["lineage"]}
    )
    if (
        dataset.ref() != request.dataset
        or frozen["dataset_ref"] != request.dataset.model_dump(mode="json")
        or expected_envelope != frozen["dataset_envelope_fp"]
    ):
        raise ServiceError("facts_corrupt")
    if request.dataset_envelope_fp != expected_envelope:
        raise ServiceError("source_changed")
    lineage_cases = {c["case_id"]: c for c in frozen["lineage"]["cases"]}
    for case in dataset.cases:
        link = lineage_cases[case.case_id]
        confirmation = (
            await conn.execute(
                select(case_confirmations.c.body).where(
                    case_confirmations.c.scope == request.scope,
                    case_confirmations.c.body["id"].as_string() == link["confirmation_id"],
                )
            )
        ).scalar_one_or_none()
        guard()
        if (
            confirmation is None
            or confirmation["case"] != case.model_dump(mode="json")
            or confirmation["content_hash"] != link["confirmation_hash"]
            or neutral_fingerprint({k: v for k, v in confirmation.items() if k != "content_hash"})
            != confirmation["content_hash"]
        ):
            raise ServiceError("facts_corrupt")
    required = {"fact_integrity", "context_coverage"}
    for case in dataset.cases:
        if "process_constraints" in case.reference:
            required.add("process_constraints")
        required.update(
            {"artifact_match", "exit_code"}
            if case.verifier == "reconciliation/1"
            else {"krim_result"}
            if case.verifier == "krim-script/1"
            else {"native_proof", "repository_checks"}
        )
    if not required <= {m.name for m in request.metrics}:
        raise ServiceError("invalid_input", 422)
    planned = {(c.case_id, r) for c in dataset.cases for r in range(request.execution.repetitions)}
    paged_parent = parent_root = parent_lineage = None
    if request.mode == "rescore":
        from .repository import reports as report_rows
        from .scale_reports import validate_manifest
        from .scale_store import checked_lineage, header

        parent_view = await header(conn, request.parent_task_id, request.scope, guard)
        guard()
        if parent_view["layout"] == 3:
            if (
                parent_view["status"] in {"queued", "running"}
                or (parent_view.get("continuous") or {}).get("dataset_envelope_fp")
                != expected_envelope
                or parent_view["dataset_ref"] != request.dataset.model_dump(mode="json")
                or parent_view["case_index"]
                != {
                    case.case_id: neutral_fingerprint(case.model_dump(mode="json"))
                    for case in dataset.cases
                }
                or parent_view["execution"]["repetitions"] != request.execution.repetitions
            ):
                raise ServiceError("source_not_eligible")
            raw_root = (
                await conn.execute(
                    select(report_rows.c.body).where(
                        report_rows.c.task_id == request.parent_task_id
                    )
                )
            ).scalar_one_or_none()
            guard()
            if raw_root is None:
                raise ServiceError("source_not_eligible")
            parent_root = validate_manifest(raw_root)
            if (
                parent_root["report_hash"] != request.parent_report_hash
                or parent_root["report_hash"] != parent_view.get("report_hash")
                or parent_root["eval_id"] != parent_view["id"]
                or parent_root["revision"] != parent_view["revision"]
                or parent_root["request_hash"] != parent_view["request_hash"]
            ):
                raise ServiceError("source_changed")
            if (
                parent_root["case_index"] != parent_view["case_index"]
                or parent_root["dataset_ref"] != parent_view["dataset_ref"]
            ):
                raise ServiceError("facts_corrupt")
            if parent_root["schema_version"] == "safe-eval/scale/2" and (
                parent_root["repository_id"] != parent_view["repository_id"]
                or parent_root["scope"] != request.scope
                or parent_root["inputs_root"] != parent_view["continuous"]["inputs_root"]
                or neutral_fingerprint(parent_root["inputs_index"])
                != neutral_fingerprint(parent_view["continuous"]["inputs_index"])
            ):
                raise ServiceError("facts_corrupt")
            parent_lineage = await checked_lineage(conn, parent_view, guard)
            guard()
            paged_parent = parent_view
    scaled_inputs = len(planned) > 24 and (
        len(dataset.cases) > 8 or request.execution.concurrency == 2
    )
    if request.mode in {"observed_score", "rescore"} and (
        scaled_inputs or paged_parent is not None
    ):
        from .repository import meta

        if task_id is not None and (not isinstance(task_id, str) or not task_id):
            raise ServiceError("facts_corrupt")
        if request.mode == "observed_score" and (
            len(request.observations) != len(planned)
            or {(i.case_id, i.repetition) for i in request.observations} != planned
            or len({i.observation_id for i in request.observations}) != len(planned)
        ):
            raise ServiceError("invalid_input", 422)
        if request.mode == "rescore" and paged_parent is None:
            raise ServiceError("source_not_eligible")
        marker = (await conn.execute(select(meta.c.body).where(meta.c.id == 1))).scalar_one()
        guard()
        if not isinstance(marker.get("repository_id"), str) or not marker["repository_id"]:
            raise ServiceError("facts_corrupt")
        binding = (
            None
            if task_id is None
            else {
                "repository_id": marker["repository_id"],
                "scope": request.scope,
                "task_id": task_id,
                "request_hash": neutral_fingerprint(request.model_dump(mode="json")),
                "dataset_ref": request.dataset.model_dump(mode="json"),
            }
        )
        index: dict[str, str] = {}
        legacy_inputs: list[dict[str, Any]] = []
        environment = None
        selected_inputs: Any = sorted(
            request.observations, key=lambda item: (item.case_id, item.repetition)
        )
        if request.mode == "rescore":
            selected_inputs = (
                {
                    "case_id": case.case_id,
                    "repetition": repetition,
                    "case": case.model_dump(mode="json"),
                    "parent": paged_parent,
                    "root": parent_root,
                    "lineage": parent_lineage,
                }
                for case in sorted(dataset.cases, key=lambda item: item.case_id)
                for repetition in range(request.execution.repetitions)
            )
        for selected in selected_inputs:
            digest, actual, env = await _source_input(
                conn, selected, request, binding, guard, legacy=not scaled_inputs
            )
            guard()
            if target is None:
                target, environment = actual, env
            elif neutral_fingerprint(target) != neutral_fingerprint(actual) or (
                neutral_fingerprint(environment) != neutral_fingerprint(env)
            ):
                raise ServiceError("source_changed")
            if scaled_inputs:
                if not isinstance(digest, str):
                    raise ServiceError("facts_corrupt")
                c, r = (
                    (selected["case_id"], selected["repetition"])
                    if request.mode == "rescore"
                    else (selected.case_id, selected.repetition)
                )
                index[f"{c}:{r}"] = digest
            else:
                if not isinstance(digest, dict):
                    raise ServiceError("facts_corrupt")
                legacy_inputs.append(digest)
            del digest
        if target is None or target.get("key") != request.target_key:
            raise ServiceError("source_changed")
        conditions = clone(execution_conditions(target.get("execution_conditions")))
        if (
            not isinstance(environment, dict)
            or environment.get("execution_conditions") != conditions
        ):
            raise ServiceError("facts_corrupt")
        if task_id is None:
            return dataset, target, conditions, {}, []
        parent_ref = None
        if paged_parent is not None:
            if parent_root is None:
                raise ServiceError("facts_corrupt")
            parent_ref = {
                "task_id": paged_parent["id"],
                "report_hash": parent_root["report_hash"],
                "kind": parent_root["kind"],
            }
        if not scaled_inputs:
            context = {
                "schema_version": "continuous-eval/1",
                "mode": request.mode,
                "dataset_envelope_fp": expected_envelope,
                "lineage": frozen["lineage"],
                "seed_policy": request.seed_policy,
                "parent": parent_ref,
                "inputs": {
                    f"{item['case_id']}:{item['repetition']}": {
                        key: value for key, value in item.items() if key != "output"
                    }
                    for item in legacy_inputs
                },
                "target_fp": neutral_fingerprint(target),
                "schedule": request.schedule,
                "source_environment": environment,
            }
            neutral_encoded(context, max_bytes=1048576)
            return dataset, target, conditions, context, legacy_inputs
        neutral_encoded(index, max_bytes=65536)
        leaves = [[c, r, index[f"{c}:{r}"]] for c, r in sorted(planned)]
        context = {
            "schema_version": "continuous-eval/2",
            "mode": request.mode,
            "dataset_envelope_fp": expected_envelope,
            "lineage": frozen["lineage"],
            "seed_policy": request.seed_policy,
            "parent": parent_ref,
            "target_fp": neutral_fingerprint(target),
            "schedule": request.schedule,
            "source_environment": environment,
            "inputs_count": len(planned),
            "inputs_index": index,
            "inputs_root": neutral_fingerprint(
                {
                    "schema_version": "continuous-input-index/1",
                    "binding": binding,
                    "count": len(planned),
                    "leaves": leaves,
                }
            ),
        }
        neutral_encoded(context, max_bytes=262144)
        guard()
        return dataset, target, conditions, context, []
    inputs, parent, environment = [], None, None
    if request.mode == "rescore":
        source = (
            await conn.execute(
                select(tasks.c.body).where(
                    tasks.c.id == request.parent_task_id,
                    tasks.c.scope == request.scope,
                )
            )
        ).scalar_one_or_none()
        guard()
        if (
            source is None
            or source.get("continuous", {}).get("dataset_envelope_fp") != expected_envelope
        ):
            raise ServiceError("source_not_eligible")
        from .reports import validate_neutral_report

        if source["report"] is None:
            raise ServiceError("source_not_eligible")
        report = validate_neutral_report(source["report"])
        if report["report_hash"] != request.parent_report_hash:
            raise ServiceError("source_changed")
        if (
            source["request_json"]["dataset"] != request.dataset.model_dump(mode="json")
            or source["dataset"] != value
        ):
            raise ServiceError("source_changed")
        target, environment = clone(source["target"]), clone(source["environment"])
        parent = {
            "task_id": source["id"],
            "report_hash": report["report_hash"],
            "kind": report["kind"],
        }
        for item in source["items"]:
            metadata = source["continuous"]["inputs"].get(
                f"{item['case_id']}:{item['repetition']}", {}
            )
            source_proof = clone(metadata.get("source_proof"))
            source_context_hash = metadata.get("source_context_hash")
            if source["continuous"]["mode"] == "execute":
                actual_output = item["output"]
                source_proof = {
                    "terminal": source["status"] not in {"queued", "running"},
                    "complete": item["status"] == "finished"
                    and actual_output is not None
                    and actual_output["execution_ref"] == item["execution_ref"],
                    "known_hard_failure": item["quality"] == "failed",
                    "environment": clone(source["environment"]),
                }
                source_context_hash = (
                    neutral_fingerprint(
                        {
                            k: next(c for c in value["cases"] if c["case_id"] == item["case_id"])[k]
                            for k in ["instruction", "files", "verifier"]
                        }
                    )
                    if source_proof["complete"]
                    else None
                )
            inputs.append(
                {
                    "case_id": item["case_id"],
                    "repetition": item["repetition"],
                    "output": clone(item["output"]),
                    "source_ref": {"parent": parent, "execution_ref": item["execution_ref"]},
                    "known_hard_failure": item["quality"] == "failed"
                    or metadata.get("known_hard_failure", False),
                    "availability": metadata.get("availability"),
                    "source_context_hash": source_context_hash,
                    "source_proof": source_proof,
                }
            )
    elif request.mode == "observed_score":
        if (
            len(request.observations) != len(planned)
            or {(i.case_id, i.repetition) for i in request.observations} != planned
        ):
            raise ServiceError("invalid_input", 422)
        identities = [i.observation_id for i in request.observations]
        if len(identities) != len(set(identities)):
            raise ServiceError("invalid_input", 422)
        for selected in request.observations:
            row = await observation(conn, request.scope, selected.observation_id)
            guard()
            if (
                row["content_hash"] != selected.observation_hash
                or neutral_fingerprint({k: v for k, v in row.items() if k != "content_hash"})
                != row["content_hash"]
            ):
                raise ServiceError("facts_corrupt")
            actual, env = clone(row["target"]), clone(row["environment"])
            env.pop("native_admission", None)
            if target is None:
                target, environment = actual, env
            elif target != actual or environment != env:
                raise ServiceError("source_changed")
            inputs.append(
                {
                    "case_id": selected.case_id,
                    "repetition": selected.repetition,
                    "output": clone(row["output"]),
                    "source_ref": {
                        "observation_id": row["id"],
                        "observation_hash": row["content_hash"],
                        "source": row["source"],
                    },
                    "known_hard_failure": row.get("source_proof", {}).get(
                        "known_hard_failure", False
                    ),
                    "source_proof": clone(row.get("source_proof")),
                    "availability": row["availability"],
                    "source_context_hash": row["context_hash"],
                }
            )
    if target is None or target.get("key") != request.target_key:
        raise ServiceError("source_changed")
    conditions = clone(execution_conditions(target.get("execution_conditions")))
    if environment is not None and environment.get("execution_conditions") != conditions:
        raise ServiceError("facts_corrupt")
    if inputs and {(i["case_id"], i["repetition"]) for i in inputs} != planned:
        raise ServiceError("facts_corrupt")
    context = {
        "schema_version": "continuous-eval/1",
        "mode": request.mode,
        "dataset_envelope_fp": expected_envelope,
        "lineage": frozen["lineage"],
        "seed_policy": request.seed_policy,
        "parent": parent,
        "inputs": {},
        "target_fp": neutral_fingerprint(target),
        "schedule": request.schedule,
        "source_environment": clone(environment),
    }
    for item in inputs:
        metadata = {k: v for k, v in item.items() if k != "output"}
        metadata["output_hash"] = neutral_fingerprint(item["output"])
        context["inputs"][f"{item['case_id']}:{item['repetition']}"] = metadata
    neutral_encoded(context, max_bytes=1048576)
    return dataset, target, conditions, context, inputs


async def freeze_inputs(
    conn: AsyncConnection,
    snapshot: dict[str, Any],
    inputs: list[dict[str, Any]],
    guard: Callable[[], None],
) -> None:
    context = snapshot.get("continuous") or {}
    version = context.get("schema_version")
    if version not in {None, "continuous-eval/1", "continuous-eval/2"}:
        raise ServiceError("facts_corrupt")
    if version == "continuous-eval/2":
        from .repository import meta

        if context["mode"] not in {"observed_score", "rescore"} or snapshot.get("layout") != 3:
            raise ServiceError("facts_corrupt")
        marker = (await conn.execute(select(meta.c.body).where(meta.c.id == 1))).scalar_one()
        guard()
        binding = {
            "repository_id": marker["repository_id"],
            "scope": snapshot["request_json"]["scope"],
            "task_id": snapshot["id"],
            "request_hash": snapshot["request_hash"],
            "dataset_ref": snapshot["dataset_ref"],
        }
        matrix = [
            (c, r)
            for c in sorted(snapshot["case_index"])
            for r in range(snapshot["execution"]["repetitions"])
        ]
        index = context.get("inputs_index")
        if (
            not isinstance(index, dict)
            or set(index) != {f"{c}:{r}" for c, r in matrix}
            or type(context.get("inputs_count")) is not int
            or context["inputs_count"] != len(matrix)
            or len(matrix) != snapshot["planned_count"]
            or snapshot.get("repository_id") != marker["repository_id"]
            or "inputs" in context
            or "inputs_fingerprint" in context
        ):
            raise ServiceError("facts_corrupt")
        neutral_encoded(index, max_bytes=65536)
        root = neutral_fingerprint(
            {
                "schema_version": "continuous-input-index/1",
                "binding": binding,
                "count": len(matrix),
                "leaves": [[c, r, index[f"{c}:{r}"]] for c, r in matrix],
            }
        )
        if root != context.get("inputs_root"):
            raise ServiceError("facts_corrupt")
        keys = (
            await conn.execute(
                select(continuous_inputs.c.case_id, continuous_inputs.c.repetition).where(
                    continuous_inputs.c.task_id == snapshot["id"]
                )
            )
        ).all()
        guard()
        if len(keys) != len(matrix) or set(map(tuple, keys)) != set(matrix):
            raise ServiceError("facts_corrupt")
        refs: set[str] = set()
        for c, r in matrix:
            packet = (
                await conn.execute(
                    select(continuous_inputs.c.body).where(
                        continuous_inputs.c.task_id == snapshot["id"],
                        continuous_inputs.c.case_id == c,
                        continuous_inputs.c.repetition == r,
                    )
                )
            ).scalar_one_or_none()
            guard()
            if (
                not isinstance(packet, dict)
                or set(packet)
                != {
                    "schema_version",
                    "binding_B",
                    "case_id",
                    "repetition",
                    "execution_ref",
                    "output_digest",
                    "metadata",
                }
                or packet["schema_version"] != "continuous-input/1"
                or neutral_fingerprint(packet, max_bytes=1048576) != index[f"{c}:{r}"]
                or neutral_fingerprint(packet["binding_B"]) != neutral_fingerprint(binding)
                or packet["case_id"] != c
                or type(packet["repetition"]) is not int
                or packet["repetition"] != r
                or not isinstance(packet["metadata"], dict)
            ):
                raise ServiceError("facts_corrupt")
            metadata = packet["metadata"]
            if (
                metadata.get("case_id") != c
                or type(metadata.get("repetition")) is not int
                or metadata["repetition"] != r
            ):
                raise ServiceError("facts_corrupt")
            output = None
            if packet["output_digest"] is not None:
                output = (
                    await conn.execute(
                        select(evidence.c.body).where(evidence.c.digest == packet["output_digest"])
                    )
                ).scalar_one_or_none()
                guard()
                if (
                    output is None
                    or neutral_fingerprint(output, max_bytes=OBSERVATION_BYTES)
                    != packet["output_digest"]
                ):
                    raise ServiceError("facts_corrupt")
                if (
                    output["execution_ref"] != packet["execution_ref"]
                    or packet["execution_ref"] in refs
                ):
                    raise ServiceError("facts_corrupt")
                refs.add(packet["execution_ref"])
            elif packet["execution_ref"] is not None:
                raise ServiceError("facts_corrupt")
            if metadata.get("output_hash") != neutral_fingerprint(
                output, max_bytes=OBSERVATION_BYTES
            ):
                raise ServiceError("facts_corrupt")
            del output, metadata, packet
        guard()
        return
    legacy_refs: list[str] = []
    for incoming in inputs:
        item = next(
            i
            for i in snapshot["items"]
            if (i["case_id"], i["repetition"]) == (incoming["case_id"], incoming["repetition"])
        )
        output = clone(incoming["output"])
        metadata = snapshot["continuous"]["inputs"][f"{item['case_id']}:{item['repetition']}"]
        if metadata["output_hash"] != neutral_fingerprint(output):
            raise ServiceError("facts_corrupt")
        if output is not None:
            item["execution_ref"] = output["execution_ref"]
            legacy_refs.append(item["execution_ref"])
            digest = neutral_fingerprint(output)
            old = (
                await conn.execute(select(evidence.c.body).where(evidence.c.digest == digest))
            ).scalar_one_or_none()
            guard()
            if old is None:
                await conn.execute(insert(evidence).values(digest=digest, body=output))
                guard()
            elif old != output:
                raise ServiceError("facts_corrupt")
        item["output"] = output
        await conn.execute(
            insert(continuous_inputs).values(
                task_id=snapshot["id"],
                case_id=item["case_id"],
                repetition=item["repetition"],
                body=metadata,
            )
        )
        guard()
    if len(legacy_refs) != len(set(legacy_refs)):
        raise ServiceError("invalid_input", 422)


async def reserve_window(
    conn: AsyncConnection,
    request: Any,
    context: dict[str, Any],
    task_id: str,
    clock: Callable[[], float],
    guard: Callable[[], None],
    *,
    insert_row: bool,
) -> None:
    if request.schedule is None:
        return
    from .continuous_types import window_request

    supplied = request.schedule
    row = await read_schedule(conn, request.scope, supplied.get("schedule_id", ""))
    guard()
    spec = row["spec"]
    index = int((clock() - spec["epoch"]) // spec["interval_seconds"])
    if not row["enabled"] or not 0 <= index < spec["max_windows"]:
        raise ServiceError("closed")
    expected = window_request(row, index)
    if expected.model_dump(mode="json") != request.model_dump(mode="json"):
        raise ServiceError("key_conflict")
    if (
        context["target_fp"] != row["target_fp"]
        or context["dataset_envelope_fp"] != spec["template"]["dataset_envelope_fp"]
    ):
        raise ServiceError("target_binding_changed")
    filters = [
        eval_schedule_windows.c.scope == request.scope,
        eval_schedule_windows.c.schedule_id_version == spec["schedule_id"] + "/" + spec["version"],
        eval_schedule_windows.c.window_index == index,
    ]
    existing = (
        await conn.execute(select(eval_schedule_windows.c.body).where(*filters))
    ).scalar_one_or_none()
    guard()
    if existing is not None:
        if existing.get("status") != "reserved" or existing["request_hash"] != neutral_fingerprint(
            request.model_dump(mode="json")
        ):
            raise ServiceError("key_conflict")
        actual = (
            await conn.execute(
                select(tasks.c.body).where(
                    tasks.c.id == existing["task_id"],
                    tasks.c.scope == request.scope,
                    tasks.c.request_key == request.request_key,
                )
            )
        ).scalar_one_or_none()
        guard()
        if actual is None or actual["request_hash"] != existing["request_hash"]:
            raise ServiceError("facts_corrupt")
        return
    if insert_row:
        await conn.execute(
            insert(eval_schedule_windows).values(
                scope=request.scope,
                schedule_id_version=spec["schedule_id"] + "/" + spec["version"],
                dataset_envelope_fp=context["dataset_envelope_fp"],
                target_fp=row["target_fp"],
                window_index=index,
                body={
                    "status": "reserved",
                    "task_id": task_id,
                    "window_index": index,
                    "spec_hash": row["spec_hash"],
                    "request_hash": neutral_fingerprint(request.model_dump(mode="json")),
                },
            )
        )
        guard()
    if int((clock() - spec["epoch"]) // spec["interval_seconds"]) != index:
        raise ServiceError("closed")


async def read_schedule(conn: AsyncConnection, scope: str, identity: str) -> dict[str, Any]:
    from .continuous_types import ScheduleSpec

    row = (
        await conn.execute(
            select(eval_schedules.c.body).where(
                eval_schedules.c.scope == scope,
                eval_schedules.c.schedule_id == identity,
            )
        )
    ).scalar_one_or_none()
    if row is None:
        raise ServiceError("not_found", 404)
    spec = ScheduleSpec.model_validate(row["spec"])
    if (
        row["spec_hash"] != neutral_fingerprint(spec.model_dump(mode="json"))
        or spec.scope != scope
        or spec.schedule_id != identity
    ):
        raise ServiceError("facts_corrupt")
    if row["control_revision"] not in {0, 1} or row["enabled"] != (row["control_revision"] == 0):
        raise ServiceError("facts_corrupt")
    return cast(dict[str, Any], clone(row))


async def create_schedule(
    conn: AsyncConnection, spec: Any, target: dict[str, Any], actor: str, guard: Callable[[], None]
) -> dict[str, Any]:
    value = spec.model_dump(mode="json")
    old = (
        await conn.execute(
            select(eval_schedules.c.body).where(
                eval_schedules.c.scope == spec.scope,
                eval_schedules.c.schedule_id == spec.schedule_id,
            )
        )
    ).scalar_one_or_none()
    guard()
    if old is not None:
        if old["spec"] != value or old["target_fp"] != neutral_fingerprint(target):
            raise ServiceError("key_conflict")
        return cast(dict[str, Any], clone(old))
    row = {
        "spec": value,
        "spec_hash": neutral_fingerprint(value),
        "target_fp": neutral_fingerprint(target),
        "actor": actor,
        "enabled": True,
        "control_revision": 0,
        "disable_receipt": None,
        "observed_through": -1,
        "missed": [],
    }
    await conn.execute(
        insert(eval_schedules).values(scope=spec.scope, schedule_id=spec.schedule_id, body=row)
    )
    guard()
    return cast(dict[str, Any], clone(row))


async def observe_schedule(
    conn: AsyncConnection,
    scope: str,
    identity: str,
    clock: Callable[[], float],
    guard: Callable[[], None] = lambda: None,
) -> dict[str, Any]:
    row = await read_schedule(conn, scope, identity)
    guard()
    spec = row["spec"]
    index = int((clock() - spec["epoch"]) // spec["interval_seconds"])
    through = min(max(index - 1, -1), spec["max_windows"] - 1)
    for window in range(through + 1):
        old = (
            await conn.execute(
                select(eval_schedule_windows.c.body).where(
                    eval_schedule_windows.c.scope == scope,
                    eval_schedule_windows.c.schedule_id_version == identity + "/" + spec["version"],
                    eval_schedule_windows.c.window_index == window,
                )
            )
        ).scalar_one_or_none()
        guard()
        if old is None:
            await conn.execute(
                insert(eval_schedule_windows).values(
                    scope=scope,
                    schedule_id_version=identity + "/" + spec["version"],
                    dataset_envelope_fp=spec["template"]["dataset_envelope_fp"],
                    target_fp=row["target_fp"],
                    window_index=window,
                    body={"status": "missed", "task_id": None, "window_index": window},
                )
            )
            guard()
            row["missed"].append(window)
    row["observed_through"] = max(row["observed_through"], through)
    await conn.execute(
        update(eval_schedules)
        .where(eval_schedules.c.scope == scope, eval_schedules.c.schedule_id == identity)
        .values(body=row)
    )
    guard()
    return cast(dict[str, Any], clone(row))


async def disable_schedule(
    conn: AsyncConnection,
    scope: str,
    identity: str,
    revision: int,
    key: str,
    guard: Callable[[], None],
) -> dict[str, Any]:
    row = await read_schedule(conn, scope, identity)
    guard()
    request_hash = neutral_fingerprint(
        {"scope": scope, "id": identity, "revision": revision, "key": key}
    )
    if row["disable_receipt"] is not None and row["disable_receipt"]["key"] == key:
        if row["disable_receipt"]["request_hash"] != request_hash:
            raise ServiceError("key_conflict")
        return row
    if row["control_revision"] != revision or not row["enabled"]:
        raise ServiceError("snapshot_changed")
    row.update(
        enabled=False,
        control_revision=1,
        disable_receipt={"key": key, "request_hash": request_hash},
    )
    await conn.execute(
        update(eval_schedules)
        .where(eval_schedules.c.scope == scope, eval_schedules.c.schedule_id == identity)
        .values(body=row)
    )
    guard()
    return cast(dict[str, Any], clone(row))


async def lineage_receipt(
    conn: AsyncConnection,
    request: dict[str, Any],
    request_hash: str,
) -> dict[str, Any] | None:
    key = neutral_fingerprint({"scope": request["scope"], "key": request["request_key"]})
    old = (
        await conn.execute(
            select(dataset_lineage.c.body).where(dataset_lineage.c.freeze_key == key)
        )
    ).scalar_one_or_none()
    if old is not None and old["request_hash"] != request_hash:
        raise ServiceError("key_conflict")
    return cast(dict[str, Any] | None, clone(old))


async def freeze_cases(
    conn: AsyncConnection,
    request: dict[str, Any],
    request_hash: str,
    freeze: Callable[[AsyncConnection, DatasetSpec], Awaitable[DatasetSpec]],
    guard: Callable[[], None],
) -> dict[str, Any]:
    old = await lineage_receipt(conn, request, request_hash)
    guard()
    if old is not None:
        return old
    if len(set(request["confirmations"])) != len(request["confirmations"]):
        raise ServiceError("invalid_input", 422)
    confirmed = []
    for identity in request["confirmations"]:
        value = (
            await conn.execute(
                select(case_confirmations.c.body).where(
                    case_confirmations.c.scope == request["scope"],
                    case_confirmations.c.body["id"].as_string() == identity,
                )
            )
        ).scalar_one_or_none()
        guard()
        if value is None:
            raise ServiceError("not_found", 404)
        if value["status"] != "confirmed":
            raise ServiceError("source_not_eligible")
        if value["content_hash"] != neutral_fingerprint(
            {k: v for k, v in value.items() if k != "content_hash"}
        ):
            raise ServiceError("facts_corrupt")
        confirmed.append(value)
    confirmed.sort(key=lambda c: c["case"]["case_id"])
    spec = DatasetSpec.model_validate(
        {
            "name": request["name"],
            "version": request["version"],
            "cases": [c["case"] for c in confirmed],
        }
    )
    present = (
        await conn.execute(
            select(datasets.c.body).where(
                datasets.c.name == spec.name, datasets.c.version == spec.version
            )
        )
    ).scalar_one_or_none()
    guard()
    if present is not None:
        raise ServiceError("revision_conflict")
    frozen = await freeze(conn, spec)
    guard()
    lineage = {
        "scope": request["scope"],
        "reset_policy": "fresh-files/1",
        "cases": [
            {
                "case_id": c["case"]["case_id"],
                "confirmation_id": c["id"],
                "confirmation_hash": c["content_hash"],
                "source_refs": c["source_refs"],
                "context": c["context"],
                "reset": c["reset"],
                "case_hash": neutral_fingerprint(c["case"]),
            }
            for c in confirmed
        ],
    }
    body = {
        "dataset_ref": frozen.ref().model_dump(mode="json"),
        "lineage": lineage,
        "request_hash": request_hash,
    }
    body["dataset_envelope_fp"] = neutral_fingerprint(
        {"dataset_ref": body["dataset_ref"], "lineage": lineage}
    )
    neutral_encoded(body, max_bytes=1048576)
    await conn.execute(
        insert(dataset_lineage).values(
            dataset_name=spec.name,
            version=spec.version,
            freeze_key=neutral_fingerprint(
                {"scope": request["scope"], "key": request["request_key"]}
            ),
            body=body,
        )
    )
    guard()
    return cast(dict[str, Any], clone(body))
