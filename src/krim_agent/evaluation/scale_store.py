"""规模存储的同连接操作；不创建engine、事务、锁、任务或进程。"""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any, cast

from sqlalchemy import (
    JSON,
    Column,
    Integer,
    String,
    Table,
    and_,
    exists,
    insert,
    or_,
    select,
    update,
)
from sqlalchemy.ext.asyncio import AsyncConnection

from krim_agent.contracts import ServiceError

from .domain import (
    DATASET_BYTES,
    SCALE_STORAGE_BYTES,
    Budget,
    DatasetSpec,
    clone,
    neutral_encoded,
    neutral_fingerprint,
)
from .paging import Page, decode_cursor, encode_cursor
from .repository import datasets, evidence, meta, metadata, tasks
from .repository import trials as trial_rows

dataset_cases = Table(
    "dataset_cases",
    metadata,
    Column("name", String, primary_key=True),
    Column("version", String, primary_key=True),
    Column("case_id", String, primary_key=True),
    Column("ordinal", Integer, nullable=False),
    Column("case_hash", String, nullable=False),
    Column("body", JSON, nullable=False),
)
eval_report_pages = Table(
    "eval_report_pages",
    metadata,
    Column("task_id", String, primary_key=True),
    Column("report_revision", Integer, primary_key=True),
    Column("page_index", Integer, primary_key=True),
    Column("body", JSON, nullable=False),
)
eval_resource_receipts = Table(
    "eval_resource_receipts",
    metadata,
    Column("task_id", String, primary_key=True),
    Column("case_id", String, primary_key=True),
    Column("repetition", Integer, primary_key=True),
    Column("body", JSON, nullable=False),
)


async def migrate(conn: AsyncConnection) -> None:
    """调用方已在BEGIN IMMEDIATE内；一份Dataset最多1MiB，旧JSON不重写。"""
    for table in (dataset_cases, eval_report_pages, eval_resource_receipts):
        await conn.run_sync(table.create)
    after: tuple[str, str] | None = None
    while True:
        query = select(datasets.c.name, datasets.c.version, datasets.c.body)
        if after is not None:
            query = query.where(
                or_(
                    datasets.c.name > after[0],
                    and_(datasets.c.name == after[0], datasets.c.version > after[1]),
                )
            )
        row = (
            await conn.execute(query.order_by(datasets.c.name, datasets.c.version).limit(1))
        ).first()
        if row is None:
            break
        spec = DatasetSpec.model_validate(row.body)
        for ordinal, case in enumerate(spec.cases):
            body = case.model_dump(mode="json")
            await conn.execute(
                insert(dataset_cases).values(
                    name=spec.name,
                    version=spec.version,
                    case_id=case.case_id,
                    ordinal=ordinal,
                    case_hash=neutral_fingerprint(body),
                    body=body,
                )
            )
        after = (row.name, row.version)


def capacity(
    dataset: DatasetSpec, budget: Budget, context: dict[str, Any] | None = None
) -> dict[str, Any]:
    dataset_bytes = len(
        neutral_encoded(dataset.model_dump(mode="json"), max_bytes=DATASET_BYTES).encode()
    )
    planned = len(dataset.cases) * budget.repetitions
    if not 1 <= planned <= 300 or budget.concurrency not in (1, 2):
        raise ServiceError("invalid_input", 422)
    reserved = (
        planned * (10 * 1048576 + 65536 + 16384 + 8192 + 1048576) + 16 * 1048576 + 3 * dataset_bytes
    )
    if reserved > SCALE_STORAGE_BYTES:
        raise ServiceError("body_too_large", 422)
    return {
        "schema_version": "neutral-capacity/2",
        "dataset_bytes": dataset_bytes,
        "observation_bytes": 10 * 1048576,
        "scores_bytes": 65536,
        "page_rows": 16,
        "page_bytes": 2097152,
        "report_bytes": 262144,
        "header_bytes": 262144,
        "max_task_storage_bytes": SCALE_STORAGE_BYTES,
        "reserved_storage_bytes": reserved,
        "required_free_bytes": 2 * SCALE_STORAGE_BYTES,
    }


async def header(
    conn: AsyncConnection,
    task_id: str,
    scope: str | None = None,
    guard: Callable[[], None] = lambda: None,
) -> dict[str, Any]:
    """先查最小scope。旧布局也只投影元数据，绝不把items带回Python。"""
    allowed = (
        await conn.execute(select(tasks.c.scope).where(tasks.c.id == task_id))
    ).scalar_one_or_none()
    guard()
    if allowed is None or (scope is not None and allowed != scope):
        raise ServiceError("not_found", 404)
    row = (
        await conn.execute(
            select(
                tasks.c.body["layout"].as_integer().label("layout"),
                tasks.c.body["revision"].as_integer().label("revision"),
                tasks.c.body["planned_count"].as_integer().label("planned_count"),
                tasks.c.body["execution"]["repetitions"].as_integer().label("repetitions"),
                tasks.c.body["request_json"]["dataset"].as_json().label("dataset_ref"),
                tasks.c.body["status"].as_string().label("status"),
            ).where(tasks.c.id == task_id)
        )
    ).one()
    guard()
    marker = (await conn.execute(select(meta.c.body).where(meta.c.id == 1))).scalar_one()
    guard()
    result: dict[str, Any] = {
        "id": task_id,
        "scope": allowed,
        "layout": row.layout or 2,
        "revision": row.revision,
        "planned_count": row.planned_count,
        "repetitions": row.repetitions,
        "dataset_ref": clone(row.dataset_ref),
        "status": row.status,
        "repository_id": marker.get("repository_id"),
    }
    if result["layout"] == 3:
        body = (await conn.execute(select(tasks.c.body).where(tasks.c.id == task_id))).scalar_one()
        guard()
        if len(neutral_encoded(body, max_bytes=262144).encode()) > 262144:
            raise ServiceError("facts_corrupt")
        # 此次JSON列反序列化已经产生独立对象；仅在当前读事务内使用。
        result.update(body)
        result["scope"] = allowed
        result["repository_id"] = marker.get("repository_id")
    context = result.get("continuous") or {}
    version = context.get("schema_version")
    if version not in {None, "continuous-eval/1", "continuous-eval/2"}:
        raise ServiceError("facts_corrupt")
    if version == "continuous-eval/2":
        try:
            index = context["inputs_index"]
            repetitions = result["execution"]["repetitions"]
            if (
                type(allowed) is not str
                or not allowed
                or type(marker.get("repository_id")) is not str
                or not marker["repository_id"]
            ):
                raise ValueError("input_authority")
            if (
                result["layout"] != 3
                or context["mode"] not in {"observed_score", "rescore"}
                or (
                    "repository_id" in body
                    and (
                        type(body["repository_id"]) is not str
                        or body["repository_id"] != marker["repository_id"]
                    )
                )
                or (
                    "scope" in body and (type(body["scope"]) is not str or body["scope"] != allowed)
                )
                or result["request_json"]["scope"] != allowed
                or result["id"] != task_id
                or type(repetitions) is not int
                or not 1 <= repetitions <= 3
                or type(result["planned_count"]) is not int
                or not 24 < result["planned_count"] <= 300
                or not isinstance(index, dict)
                or "inputs" in context
                or "inputs_fingerprint" in context
            ):
                raise ValueError("input_header")
            matrix = [(c, r) for c in sorted(result["case_index"]) for r in range(repetitions)]
            if (
                type(context["inputs_count"]) is not int
                or context["inputs_count"] != len(matrix)
                or len(matrix) != result["planned_count"]
                or set(index) != {f"{c}:{r}" for c, r in matrix}
                or any(
                    not isinstance(h, str)
                    or len(h) != 64
                    or any(x not in "0123456789abcdef" for x in h)
                    for h in index.values()
                )
            ):
                raise ValueError("input_matrix")
            neutral_encoded(index, max_bytes=65536)
            binding = {
                "repository_id": marker["repository_id"],
                "scope": allowed,
                "task_id": task_id,
                "request_hash": result["request_hash"],
                "dataset_ref": result["dataset_ref"],
            }
            if context["inputs_root"] != neutral_fingerprint(
                {
                    "schema_version": "continuous-input-index/1",
                    "binding": binding,
                    "count": len(matrix),
                    "leaves": [[c, r, index[f"{c}:{r}"]] for c, r in matrix],
                }
            ):
                raise ValueError("input_root")
            if result["request_hash"] != neutral_fingerprint(result["request_json"]):
                raise ValueError("request_hash")
            if result["plan_fingerprint"] != neutral_fingerprint(
                {
                    "request_hash": result["request_hash"],
                    "dataset_ref": result["dataset_ref"],
                    "case_index": result["case_index"],
                    "target": result["target"],
                    "continuous": context,
                }
            ):
                raise ValueError("input_plan")
        except (KeyError, TypeError, ValueError):
            raise ServiceError("facts_corrupt") from None
    return result


async def trials(
    conn: AsyncConnection,
    value: dict[str, Any],
    cursor: str | None,
    limit: int,
    *,
    plan: bool = False,
    guard: Callable[[], None] = lambda: None,
) -> Page:
    if type(limit) is not int or not 1 <= limit <= 16:
        raise ServiceError("invalid_input", 422)
    if not value.get("repository_id"):
        raise ServiceError("source_changed")
    binding = {
        "version": "eval-page/1",
        "repository_id": value["repository_id"],
        "scope": value["scope"],
        "task_id": value["id"],
        "dataset_fingerprint": value["dataset_ref"]["fingerprint"],
        "snapshot_binding": value["plan_fingerprint"] if plan else str(value["revision"]),
        "filter_fingerprint": neutral_fingerprint({"projection": "plan" if plan else "trials"}),
        "order": "case_id-binary,repetition",
    }
    fields = [
        "status",
        "execution_ref",
        "scores",
        "quality",
        "code",
        "content_hash",
        "output_digest",
    ]

    def projection() -> Any:
        return select(
            trial_rows.c.case_id,
            trial_rows.c.repetition,
            *[trial_rows.c.body[k].as_json().label(k) for k in fields],
        ).where(trial_rows.c.task_id == value["id"])

    async def projected(row: Any) -> dict[str, Any]:
        result = {"case_id": row.case_id, "repetition": row.repetition}
        result.update({k: row._mapping[k] for k in fields})
        if value["layout"] != 3 and result["execution_ref"] is not None:
            digest = (
                await conn.execute(
                    select(evidence.c.digest).where(
                        evidence.c.body["execution_ref"].as_string() == result["execution_ref"]
                    )
                )
            ).scalar_one_or_none()
            guard()
            result["output_digest"] = digest
        return result

    last = None
    if cursor is not None:
        decoded = decode_cursor(cursor, binding)
        last = decoded["last_key"]
        if len(last) != 2 or not isinstance(last[0], str) or type(last[1]) is not int:
            raise ServiceError("invalid_input", 422)
        row = (
            await conn.execute(
                projection().where(
                    trial_rows.c.case_id == last[0], trial_rows.c.repetition == last[1]
                )
            )
        ).one_or_none()
        guard()
        if row is None or neutral_fingerprint(await projected(row)) != decoded["last_row_hash"]:
            raise ServiceError("source_changed")
    query = projection()
    if last is not None:
        query = query.where(
            or_(
                trial_rows.c.case_id > last[0],
                and_(trial_rows.c.case_id == last[0], trial_rows.c.repetition > last[1]),
            )
        )
    selected = (
        await conn.execute(
            query.order_by(trial_rows.c.case_id.collate("BINARY"), trial_rows.c.repetition).limit(
                limit
            )
        )
    ).all()
    guard()
    rows: list[dict[str, Any]] = []
    for row in selected:
        item = await projected(row)
        if len(neutral_encoded(rows + [item], max_bytes=2097152).encode()) > 2097152:
            raise ServiceError("body_too_large", 422)
        rows.append(item)
    token = None
    if rows:
        final = rows[-1]
        more = (
            await conn.execute(
                select(
                    exists().where(
                        trial_rows.c.task_id == value["id"],
                        or_(
                            trial_rows.c.case_id > final["case_id"],
                            and_(
                                trial_rows.c.case_id == final["case_id"],
                                trial_rows.c.repetition > final["repetition"],
                            ),
                        ),
                    )
                )
            )
        ).scalar_one()
        guard()
        if more:
            token = encode_cursor(
                binding, [final["case_id"], final["repetition"]], neutral_fingerprint(final)
            )
    return Page(rows, token, binding, value["planned_count"])


async def trial_facts(
    conn: AsyncConnection,
    value: dict[str, Any],
    raw: dict[str, Any],
    guard: Callable[[], None] = lambda: None,
) -> dict[str, Any]:
    """仅核一份持久执行事实；评分来源装配由input_for_trial逐项核验。"""
    row = clone(raw)
    if value["layout"] != 3:
        return cast(dict[str, Any], row)
    digest = row.get("output_digest")
    output = None
    if digest is not None:
        output = (
            await conn.execute(select(evidence.c.body).where(evidence.c.digest == digest))
        ).scalar_one_or_none()
        guard()
        if (
            output is None
            or neutral_fingerprint(output) != digest
            or output["execution_ref"] != row["execution_ref"]
        ):
            raise ServiceError("facts_corrupt")
    row["output"] = clone(output)
    if (
        row.get("content_hash") is not None
        and neutral_fingerprint({k: row[k] for k in ["output", "scores", "quality", "code"]})
        != row["content_hash"]
    ):
        raise ServiceError("facts_corrupt")
    return cast(dict[str, Any], row)


async def checked_lineage(
    conn: AsyncConnection,
    value: dict[str, Any],
    guard: Callable[[], None],
) -> dict[str, Any]:
    """只在当前连接/读事务内复用；不跨页、事务或scope缓存。"""
    from .repository import dataset_lineage

    lineage = (
        await conn.execute(
            select(dataset_lineage.c.body).where(
                dataset_lineage.c.dataset_name == value["dataset_ref"]["name"],
                dataset_lineage.c.version == value["dataset_ref"]["version"],
            )
        )
    ).scalar_one()
    guard()
    if neutral_fingerprint(lineage["lineage"]) != value["continuous"]["lineage_fingerprint"]:
        raise ServiceError("facts_corrupt")
    return cast(dict[str, Any], lineage)


async def input_for_trial(
    conn: AsyncConnection,
    value: dict[str, Any],
    case_id: str,
    repetition: int,
    guard: Callable[[], None] = lambda: None,
    *,
    lineage: dict[str, Any] | None = None,
) -> dict[str, Any]:
    from .repository import continuous_inputs

    raw = (
        await conn.execute(
            select(trial_rows.c.body).where(
                trial_rows.c.task_id == value["id"],
                trial_rows.c.case_id == case_id,
                trial_rows.c.repetition == repetition,
            )
        )
    ).scalar_one_or_none()
    guard()
    if raw is None:
        raise ServiceError("not_found", 404)
    row = await trial_facts(conn, value, raw, guard)
    if value["layout"] != 3:
        return row
    if value.get("continuous") is not None:
        context = value["continuous"]
        metadata_row = (
            await conn.execute(
                select(continuous_inputs.c.body).where(
                    continuous_inputs.c.task_id == value["id"],
                    continuous_inputs.c.case_id == case_id,
                    continuous_inputs.c.repetition == repetition,
                )
            )
        ).scalar_one_or_none()
        guard()
        if context["mode"] != "execute":
            version = context.get("schema_version")
            if version == "continuous-eval/2":
                packet = metadata_row
                binding = {
                    "repository_id": value["repository_id"],
                    "scope": value["scope"],
                    "task_id": value["id"],
                    "request_hash": value["request_hash"],
                    "dataset_ref": value["dataset_ref"],
                }
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
                    or neutral_fingerprint(packet, max_bytes=1048576)
                    != context["inputs_index"].get(f"{case_id}:{repetition}")
                    or neutral_fingerprint(packet["binding_B"]) != neutral_fingerprint(binding)
                    or packet["case_id"] != case_id
                    or type(packet["repetition"]) is not int
                    or packet["repetition"] != repetition
                    or packet["execution_ref"] != row["execution_ref"]
                    or packet["output_digest"] != row["output_digest"]
                    or not isinstance(packet["metadata"], dict)
                ):
                    raise ServiceError("facts_corrupt")
                metadata_row = packet["metadata"]
                if (
                    metadata_row.get("case_id") != case_id
                    or type(metadata_row.get("repetition")) is not int
                    or metadata_row["repetition"] != repetition
                ):
                    raise ServiceError("facts_corrupt")
            elif version != "continuous-eval/1":
                raise ServiceError("facts_corrupt")
            else:
                inputs = context.get("inputs")
                if not isinstance(inputs, dict):
                    raise ServiceError("facts_corrupt")
                frozen_metadata = inputs.get(f"{case_id}:{repetition}")
                if (
                    not isinstance(metadata_row, dict)
                    or not isinstance(frozen_metadata, dict)
                    or neutral_fingerprint(metadata_row) != neutral_fingerprint(frozen_metadata)
                ):
                    raise ServiceError("facts_corrupt")
            if metadata_row["output_hash"] != neutral_fingerprint(row["output"]):
                raise ServiceError("facts_corrupt")
        if lineage is None:
            lineage = await checked_lineage(conn, value, guard)
        link = next((c for c in lineage["lineage"]["cases"] if c["case_id"] == case_id), None)
        if link is None:
            raise ServiceError("facts_corrupt")
        incoming = metadata_row or {}
        row["_continuous"] = {
            "lineage_case": clone(link),
            "mode": context["mode"],
            "known_hard_failure": incoming.get("known_hard_failure", False),
            "source_context_hash": incoming.get("source_context_hash"),
            "source_proof": clone(incoming.get("source_proof")),
            "availability": clone(incoming.get("availability")),
        }
    return row


async def mutate_trial(
    conn: AsyncConnection,
    task_id: str,
    case_id: str | None,
    repetition: int | None,
    action: str,
    payload: dict[str, Any],
    guard: Callable[[], None] = lambda: None,
    *,
    snapshot: dict[str, Any] | None = None,
) -> bool:
    from .repository import events

    facts_only = action in {"record", "finish", "resource"}
    fence: Callable[[], None] = (lambda: None) if facts_only else guard
    fence()
    # _mutate已在同一写锁/事务读取紧凑Task；不再次查scope/meta/body。
    value = await header(conn, task_id, guard=fence) if snapshot is None else snapshot
    if value["id"] != task_id:
        raise ServiceError("facts_corrupt")
    if value["layout"] != 3:
        raise ServiceError("facts_corrupt")
    row = None
    where = (
        trial_rows.c.task_id == task_id,
        trial_rows.c.case_id == case_id,
        trial_rows.c.repetition == repetition,
    )
    if case_id is not None:
        row = clone((await conn.execute(select(trial_rows.c.body).where(*where))).scalar_one())
        fence()
    if action == "running":
        if value["status"] == "running":
            return True
        if value["status"] != "queued":
            raise ServiceError("closed")
        value["status"] = "running"
    elif action == "stop":
        if value["stop_requested"]:
            return True
        value["stop_requested"] = True
    elif action == "intent":
        assert row is not None
        if row["status"] == "launch_intent":
            return True
        if row["status"] != "planned" or value["stop_requested"]:
            raise ServiceError("execution_unknown")
        row["status"] = "launch_intent"
    elif action == "attach":
        assert row is not None
        if row["execution_ref"] not in {None, payload["execution_ref"]}:
            raise ServiceError("key_conflict")
        if row["execution_ref"] == payload["execution_ref"]:
            return True
        if row["status"] != "launch_intent":
            raise ServiceError("execution_unknown")
        row.update(execution_ref=payload["execution_ref"], status="executing")
    elif action == "record":
        assert row is not None
        output = payload["output"]
        digest = neutral_fingerprint(output)
        if output["execution_ref"] != payload["execution_ref"] or row["execution_ref"] not in {
            None,
            payload["execution_ref"],
        }:
            raise ServiceError("facts_corrupt")
        if row.get("output_digest") is not None:
            if row["output_digest"] != digest:
                raise ServiceError("key_conflict")
            return True
        old = (
            await conn.execute(select(evidence.c.body).where(evidence.c.digest == digest))
        ).scalar_one_or_none()
        if old is None:
            await conn.execute(insert(evidence).values(digest=digest, body=output))
        elif old != output:
            raise ServiceError("facts_corrupt")
        row.update(execution_ref=payload["execution_ref"], output_digest=digest)
    elif action == "commit":
        assert row is not None
        # 提交只核已发生的完整output/scores hash；不重新装配评分context。
        original = await trial_facts(conn, value, row, fence)
        body = clone(payload)
        if original["output"] is not None:
            if body["output"] is None:
                body["output"] = clone(original["output"])
            elif neutral_fingerprint(body["output"]) != row["output_digest"]:
                raise ServiceError("key_conflict")
        if row.get("content_hash") is not None:
            if row["content_hash"] != neutral_fingerprint(body):
                raise ServiceError("key_conflict")
            return True
        if value["status"] not in {"queued", "running"}:
            raise ServiceError("closed")
        neutral_encoded(body["scores"], max_bytes=65536)
        row.update({k: body[k] for k in ["scores", "quality", "code"]})
        row.update(status="finished", content_hash=neutral_fingerprint(body))
        value["finished_count"] += 1
    else:
        raise ServiceError("facts_corrupt")
    value["revision"] += 1
    # repository_id/scope由真实列/元数据读取，不在紧凑header复制外层字段。
    saved = {k: v for k, v in value.items() if k not in {"repository_id", "scope"}}
    neutral_encoded(saved, max_bytes=262144)
    if row is not None:
        await conn.execute(update(trial_rows).where(*where).values(body=row))
        fence()
    await conn.execute(update(tasks).where(tasks.c.id == task_id).values(body=saved))
    fence()
    await conn.execute(
        insert(events).values(
            task_id=task_id,
            revision=value["revision"],
            body={
                "layout": 3,
                "status": value["status"],
                "case_id": case_id,
                "repetition": repetition,
                "action": action,
                "execution_ref": None if row is None else row["execution_ref"],
            },
        )
    )
    fence()
    return True


async def finalize(
    conn: AsyncConnection,
    value: dict[str, Any],
    guard: Callable[[], None] = lambda: None,
) -> dict[str, Any]:
    from .repository import reports
    from .scale_reports import fold_trial, make_manifest

    if (value.get("continuous") or {}).get("schema_version") == "continuous-eval/2":
        trusted = await header(conn, value["id"], guard=guard)
        guard()
        frozen_fields = [
            "id",
            "request_json",
            "request_hash",
            "dataset_ref",
            "case_index",
            "target",
            "continuous",
            "plan_fingerprint",
            "rules",
            "execution",
            "environment",
            "capacity",
            "planned_count",
        ]
        try:
            if any(
                neutral_fingerprint(value[key]) != neutral_fingerprint(trusted[key])
                for key in frozen_fields
            ):
                raise ValueError("finalize_identity")
            for key in ["scope", "repository_id"]:
                if key in value and (type(value[key]) is not str or value[key] != trusted[key]):
                    raise ValueError("finalize_authority")
        except (KeyError, TypeError, ValueError):
            raise ServiceError("facts_corrupt") from None
        # 只投影归属；待发布status/code/revision/finished_at保留在原局部值中。
        value = {**value, "scope": trusted["scope"], "repository_id": trusted["repository_id"]}
        guard()

    state: dict[str, Any] = {}
    descriptors: list[dict[str, Any]] = []
    after: tuple[str, int] | None = None
    refs: set[str] = set()
    count = 0
    while True:
        query = select(trial_rows.c.body).where(trial_rows.c.task_id == value["id"])
        if after is not None:
            query = query.where(
                or_(
                    trial_rows.c.case_id > after[0],
                    and_(trial_rows.c.case_id == after[0], trial_rows.c.repetition > after[1]),
                )
            )
        batch = (
            (
                await conn.execute(
                    query.order_by(
                        trial_rows.c.case_id.collate("BINARY"), trial_rows.c.repetition
                    ).limit(16)
                )
            )
            .scalars()
            .all()
        )
        guard()
        if not batch:
            break
        items = []
        for raw in batch:
            row = clone(raw)
            if (
                row["case_id"] not in value["case_index"]
                or not 0 <= row["repetition"] < value["execution"]["repetitions"]
            ):
                raise ServiceError("facts_corrupt")
            if (value.get("continuous") or {}).get("schema_version") == "continuous-eval/2":
                if count == 0:
                    input_lineage = await checked_lineage(conn, value, guard)
                    guard()
                actual = await input_for_trial(
                    conn, value, row["case_id"], row["repetition"], guard, lineage=input_lineage
                )
            else:
                actual = await trial_facts(conn, value, row, guard)
            guard()
            output = actual["output"]
            if row["execution_ref"] is not None:
                if row["execution_ref"] in refs:
                    raise ServiceError("facts_corrupt")
                refs.add(row["execution_ref"])
            if value["status"] == "completed" and row["status"] != "finished":
                raise ServiceError("facts_corrupt")
            row["usage_complete"] = output is not None and output.get("usage_complete") is True
            row["token_usage"] = None if output is None else clone(output.get("token_usage"))
            fold_trial(state, row, value["rules"])
            if (value.get("continuous") or {}).get("schema_version") == "continuous-eval/2":
                row["input_hash"] = value["continuous"]["inputs_index"][
                    f"{row['case_id']}:{row['repetition']}"
                ]
                del actual, output
            items.append(row)
            count += 1
        page = {
            "schema_version": "safe-eval/scale-page/2"
            if (value.get("continuous") or {}).get("schema_version") == "continuous-eval/2"
            else "safe-eval/scale-page/1",
            "task_id": value["id"],
            "revision": value["revision"],
            "index": len(descriptors),
            "items": items,
        }
        page["page_hash"] = neutral_fingerprint(page, max_bytes=2097152)
        await conn.execute(
            insert(eval_report_pages).values(
                task_id=value["id"],
                report_revision=value["revision"],
                page_index=page["index"],
                body=page,
            )
        )
        guard()
        descriptors.append(
            {
                "index": page["index"],
                "count": len(items),
                "hash": page["page_hash"],
                "first": [items[0]["case_id"], items[0]["repetition"]],
                "last": [items[-1]["case_id"], items[-1]["repetition"]],
            }
        )
        after = (items[-1]["case_id"], items[-1]["repetition"])
    if count != value["planned_count"]:
        raise ServiceError("facts_corrupt")
    root = make_manifest(value, descriptors, state)
    await conn.execute(insert(reports).values(task_id=value["id"], body=root))
    guard()
    return root


async def save_resource_receipt(
    conn: AsyncConnection,
    task_id: str,
    case_id: str,
    repetition: int,
    receipt: dict[str, Any],
    guard: Callable[[], None] = lambda: None,
) -> bool:
    neutral_encoded(receipt, max_bytes=16384)
    # 私有收据接缝只需本Task的layout/status；不搬运未消费的完整plan元数据。
    task = (
        await conn.execute(
            select(
                tasks.c.body["layout"].as_integer().label("layout"),
                tasks.c.body["status"].as_string().label("status"),
            ).where(tasks.c.id == task_id)
        )
    ).one_or_none()
    guard()
    if task is None:
        raise ServiceError("not_found", 404)
    row = (
        await conn.execute(
            select(trial_rows.c.body).where(
                trial_rows.c.task_id == task_id,
                trial_rows.c.case_id == case_id,
                trial_rows.c.repetition == repetition,
            )
        )
    ).scalar_one()
    guard()
    if task.layout != 3 or row["status"] != "finished" or row["content_hash"] is None:
        raise ServiceError("facts_corrupt")
    if (
        receipt.get("task_id"),
        receipt.get("case_id"),
        receipt.get("repetition"),
        receipt.get("execution_ref"),
    ) != (task_id, case_id, repetition, row["execution_ref"]):
        raise ServiceError("facts_corrupt")
    if receipt.get("closed") is not True:
        raise ServiceError("execution_unknown")
    where = (
        eval_resource_receipts.c.task_id == task_id,
        eval_resource_receipts.c.case_id == case_id,
        eval_resource_receipts.c.repetition == repetition,
    )
    old = (
        await conn.execute(select(eval_resource_receipts.c.body).where(*where))
    ).scalar_one_or_none()
    guard()
    if old is None:
        await conn.execute(
            insert(eval_resource_receipts).values(
                task_id=task_id, case_id=case_id, repetition=repetition, body=receipt
            )
        )
        guard()
    elif old != receipt:
        raise ServiceError("key_conflict")
    return True


async def recover(
    conn: AsyncConnection,
    value: dict[str, Any],
    status: str,
    code: str,
    *,
    _not_admitted: tuple[tuple[str, str, str, str, str, int], ...] = (),
) -> dict[str, Any]:
    """只将本轮非终态事实收束；没有claim、重领、执行或新事务。"""
    after: tuple[str, int] | None = None
    uncertain = False
    while True:
        query = select(trial_rows.c.body).where(trial_rows.c.task_id == value["id"])
        if after is not None:
            query = query.where(
                or_(
                    trial_rows.c.case_id > after[0],
                    and_(trial_rows.c.case_id == after[0], trial_rows.c.repetition > after[1]),
                )
            )
        batch = (
            (
                await conn.execute(
                    query.order_by(
                        trial_rows.c.case_id.collate("BINARY"), trial_rows.c.repetition
                    ).limit(16)
                )
            )
            .scalars()
            .all()
        )
        if not batch:
            break
        for raw in batch:
            row = clone(raw)
            if row["status"] != "finished":
                closed = False
                if row.get("output_digest") is not None:
                    cleanup = (
                        await conn.execute(
                            select(evidence.c.body["cleanup"].as_string()).where(
                                evidence.c.digest == row["output_digest"]
                            )
                        )
                    ).scalar_one_or_none()
                    closed = cleanup == "closed"
                unknown = row["status"] in {"launch_intent", "executing"} and not closed
                if (
                    unknown
                    and _not_admitted
                    and row["status"] == "launch_intent"
                    and not row.get("scores")
                    and row.get("code") is None
                    and all(
                        row.get(key) is None
                        for key in ["execution_ref", "output_digest", "content_hash", "output"]
                    )
                    and (
                        value["id"],
                        value["request_json"]["scope"],
                        value["request_hash"],
                        value["plan_fingerprint"],
                        row["case_id"],
                        row["repetition"],
                    )
                    in _not_admitted
                ):
                    corresponding = (
                        await conn.execute(
                            select(trial_rows.c.body).where(
                                trial_rows.c.task_id == value["id"],
                                trial_rows.c.case_id == row["case_id"],
                                trial_rows.c.repetition == row["repetition"],
                            )
                        )
                    ).scalar_one_or_none()
                    resource = (
                        await conn.execute(
                            select(eval_resource_receipts.c.body).where(
                                eval_resource_receipts.c.task_id == value["id"],
                                eval_resource_receipts.c.case_id == row["case_id"],
                                eval_resource_receipts.c.repetition == row["repetition"],
                            )
                        )
                    ).scalar_one_or_none()
                    if corresponding == raw and resource is None:
                        unknown = False
                        row["code"] = code
                uncertain = uncertain or unknown
                row["status"] = "unknown" if unknown else "interrupted"
                await conn.execute(
                    update(trial_rows)
                    .where(
                        trial_rows.c.task_id == value["id"],
                        trial_rows.c.case_id == row["case_id"],
                        trial_rows.c.repetition == row["repetition"],
                    )
                    .values(body=row)
                )
        after = (batch[-1]["case_id"], batch[-1]["repetition"])
    if uncertain:
        marker = (await conn.execute(select(meta.c.body).where(meta.c.id == 1))).scalar_one()
        await conn.execute(
            update(meta).where(meta.c.id == 1).values(body=marker | {"cleanup_unknown": True})
        )
    result = clone(value)
    result.update(
        status=status,
        code="execution_unknown" if uncertain else code,
        finished_at=time.time(),
        cleanup_unknown=uncertain,
    )
    return cast(dict[str, Any], result)
