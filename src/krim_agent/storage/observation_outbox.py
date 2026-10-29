"""同源连接的schema/有限读写草稿；本检查点不挂接terminal或反馈override。"""

from __future__ import annotations

import time
import uuid
from collections.abc import Callable
from typing import Any, cast

from sqlalchemy import (
    JSON,
    Column,
    Float,
    ForeignKey,
    Integer,
    MetaData,
    String,
    Table,
    UniqueConstraint,
    insert,
    select,
    update,
)
from sqlalchemy.ext.asyncio import AsyncConnection

from krim_agent.contracts import ServiceError
from krim_agent.evaluation.domain import clone, neutral_encoded, neutral_fingerprint
from krim_agent.evaluation.hot_types import (
    CapturePolicy,
    CaptureRecord,
    Claim,
    FeedbackEvent,
    FeedbackReason,
    UnitReceipt,
    receipts_root,
)
from krim_agent.evaluation.hot_types import (
    delivery_key as delivery_key,
)

from .models import runs

metadata = MetaData()
capture_meta = Table(
    "capture_meta",
    metadata,
    Column("id", String(64), primary_key=True),
    Column("body", JSON, nullable=False),
)
capture_policies = Table(
    "capture_policies",
    metadata,
    Column("workspace_id", String(64), primary_key=True),
    Column("scope", String(64), nullable=False),
    Column("revision", Integer, nullable=False),
    Column("body", JSON, nullable=False),
    Column("body_hash", String(64), nullable=False),
    Column("counters", JSON, nullable=False),
)
observation_outbox = Table(
    "observation_outbox",
    metadata,
    Column("capture_id", String(64), primary_key=True),
    Column("source_id", String(64), nullable=False),
    Column("workspace_id", String(64), ForeignKey(capture_policies.c.workspace_id), nullable=False),
    Column("run_id", String(36), ForeignKey(runs.c.id), nullable=False),
    Column("terminal_event_id", String(64), nullable=False),
    Column("policy_fingerprint", String(64), nullable=False),
    Column("body", JSON, nullable=False),
    Column("phase", String(24), nullable=False),
    Column("feedback_revision", Integer, nullable=False),
    Column("claim_generation", Integer, nullable=False),
    Column("claim_token", String(64)),
    Column("claim_until", Float),
    Column("delivery", JSON, nullable=False),
    Column("ack_root", String(64)),
    UniqueConstraint("source_id", "run_id", "terminal_event_id", "policy_fingerprint"),
)
TABLES = dict(metadata.tables)


async def create_schema(conn: AsyncConnection) -> None:
    """只借用已处在原BEGIN内的连接，不另起事务或打开库。"""
    await conn.run_sync(metadata.create_all)
    old = (
        await conn.execute(select(capture_meta.c.body).where(capture_meta.c.id == "source"))
    ).scalar_one_or_none()
    if old is None:
        await conn.execute(
            insert(capture_meta).values(
                id="source", body={"schema_version": "hot-source/1", "source_id": str(uuid.uuid4())}
            )
        )
    elif (
        not isinstance(old, dict)
        or old.get("schema_version") != "hot-source/1"
        or not old.get("source_id")
    ):
        raise ServiceError("database_invalid")


async def policy(conn: AsyncConnection, workspace: str) -> CapturePolicy | None:
    row = (
        (
            await conn.execute(
                select(capture_policies).where(capture_policies.c.workspace_id == workspace)
            )
        )
        .mappings()
        .first()
    )
    if row is None:
        return None
    result = CapturePolicy.model_validate(row["body"])
    if result.fingerprint() != row["body_hash"] or result.scope != row["scope"]:
        raise ServiceError("facts_corrupt")
    return result


async def configure(conn: AsyncConnection, value: CapturePolicy) -> None:
    frozen = CapturePolicy.model_validate(value.model_dump(mode="json"))
    old = await policy(conn, frozen.workspace_id)
    if old is not None and old == frozen:
        return
    pending = (
        await conn.execute(
            select(observation_outbox.c.capture_id)
            .where(
                observation_outbox.c.workspace_id == frozen.workspace_id,
                observation_outbox.c.phase.in_(["pending", "claimed"]),
            )
            .limit(1)
        )
    ).scalar_one_or_none()
    if pending is not None:
        raise ServiceError("capacity")
    values = dict(
        scope=frozen.scope, body=frozen.model_dump(mode="json"), body_hash=frozen.fingerprint()
    )
    if old is None:
        await conn.execute(
            insert(capture_policies).values(
                workspace_id=frozen.workspace_id,
                revision=1,
                counters={"seen": 0, "enqueued": 0, "dropped": 0, "acked": 0},
                **values,
            )
        )
    else:
        await conn.execute(
            update(capture_policies)
            .where(capture_policies.c.workspace_id == frozen.workspace_id)
            .values(revision=capture_policies.c.revision + 1, **values)
        )


async def rows(conn: AsyncConnection, workspace: str) -> list[dict[str, Any]]:
    return [
        dict(r)
        for r in (
            await conn.execute(
                select(observation_outbox)
                .where(observation_outbox.c.workspace_id == workspace)
                .order_by(observation_outbox.c.capture_id)
                .limit(4096)
            )
        ).mappings()
    ]


async def checked(conn: AsyncConnection, claim: Claim) -> dict[str, Any]:
    row = (
        (
            await conn.execute(
                select(observation_outbox).where(
                    observation_outbox.c.capture_id == claim.record.capture_id
                )
            )
        )
        .mappings()
        .first()
    )
    if row is None:
        raise ServiceError("not_found", 404)
    if (
        row["claim_generation"] != claim.generation
        or row["claim_token"] != claim.token
        or row["feedback_revision"] != claim.revision
        or row["phase"] != "claimed"
    ):
        raise ServiceError("revision_conflict")
    if row["claim_until"] is None or row["claim_until"] < time.time():
        raise ServiceError("revision_conflict")
    if CaptureRecord.model_validate(row["body"]) != claim.record:
        raise ServiceError("facts_corrupt")
    return dict(row)


async def claim_rows(
    conn: AsyncConnection, workspace: str, limit: int, guard: Callable[[], None]
) -> list[Claim]:
    if not 1 <= limit <= 16:
        raise ServiceError("invalid_input", 422)
    now = time.time()
    result = []
    candidates = await rows(conn, workspace)
    guard()
    for row in candidates:
        guard()
        if row["phase"] == "claimed":
            if row["claim_until"] is None:
                raise ServiceError("facts_corrupt")
            if row["claim_until"] > now:
                continue
        elif row["phase"] != "pending":
            continue
        record = CaptureRecord.model_validate(row["body"])
        if (
            record.workspace_id != workspace
            or len(record.feedback) != row["feedback_revision"]
            or any(
                row[key] != getattr(record, key)
                for key in (
                    "capture_id",
                    "source_id",
                    "run_id",
                    "terminal_event_id",
                    "policy_fingerprint",
                )
            )
        ):
            raise ServiceError("facts_corrupt")
        generation, token = row["claim_generation"] + 1, uuid.uuid4().hex
        changed = await conn.execute(
            update(observation_outbox)
            .where(
                observation_outbox.c.capture_id == row["capture_id"],
                observation_outbox.c.phase == row["phase"],
                observation_outbox.c.feedback_revision == row["feedback_revision"],
                observation_outbox.c.claim_generation == row["claim_generation"],
            )
            .values(
                phase="claimed",
                claim_generation=generation,
                claim_token=token,
                claim_until=now + 30,
            )
        )
        guard()
        if changed.rowcount != 1:
            raise ServiceError("revision_conflict")
        result.append(Claim(record, generation, row["feedback_revision"], token))
        if len(result) == limit:
            break
    return result


async def save_unit(conn: AsyncConnection, claim: Claim, key: str, value: dict[str, Any]) -> None:
    row = await checked(conn, claim)
    incoming = clone(value)
    manifest = incoming.pop("manifest", None)
    if manifest is not None:
        delivery = _delivery_freeze(claim.record, row["delivery"], manifest)
        if key not in delivery["units"]:
            raise ServiceError("facts_corrupt")
    else:
        delivery = clone(row["delivery"])
        expanded = expanded_units(delivery)
        if key not in expanded:
            raise ServiceError("execution_unknown")
        unit_key, common, unit = _delivery_unit(claim.record, incoming)
        if unit_key != key or common != delivery["common"]:
            raise ServiceError("key_conflict")
        old = delivery["units"][key]
        if any(
            old.get(name) != unit.get(name)
            for name in ("request_hash", "actor", "generation", "feedback")
        ):
            raise ServiceError("key_conflict")
        transitions = {
            ("not_started", "submitted"),
            ("submitted", "submitted"),
            ("submitted", "complete"),
            ("complete", "complete"),
        }
        if (old.get("state"), unit.get("state")) not in transitions:
            raise ServiceError("execution_unknown")
        if old.get("state") == "complete" and old != unit:
            raise ServiceError("key_conflict")
        delivery["units"][key] = unit
    try:
        _delivery_reserved_bytes(row["body"], delivery)
    except ValueError:
        raise ServiceError("capacity") from None
    changed = await conn.execute(
        update(observation_outbox)
        .where(
            observation_outbox.c.capture_id == claim.record.capture_id,
            observation_outbox.c.claim_generation == claim.generation,
            observation_outbox.c.feedback_revision == claim.revision,
            observation_outbox.c.claim_token == claim.token,
        )
        .values(delivery=delivery)
    )
    if changed.rowcount != 1:
        raise ServiceError("revision_conflict")


def expanded_units(delivery: dict[str, Any]) -> dict[str, Any]:
    from krim_agent.evaluation.continuous_types import ImportRequest
    from krim_agent.evaluation.hot_types import _unit_hash

    if not delivery:
        return {}
    if delivery.get("schema_version") != "hot-delivery/1" or not isinstance(
        delivery.get("units"), dict
    ):
        raise ServiceError("facts_corrupt")
    if not delivery["units"]:
        if "common" in delivery or "manifest" in delivery:
            raise ServiceError("execution_unknown")
        return {}
    manifest = delivery.get("manifest")
    common = delivery.get("common")
    if (
        delivery.get("schema_version") != "hot-delivery/1"
        or not isinstance(common, dict)
        or not isinstance(manifest, dict)
        or not isinstance(manifest.get("keys"), list)
        or type(manifest.get("revision")) is not int
        or not 0 <= manifest["revision"] <= 8
        or len(manifest["keys"]) != 1 + manifest["revision"]
        or len(manifest["keys"]) != len(set(manifest["keys"]))
        or set(manifest["keys"]) != set(delivery["units"])
    ):
        raise ServiceError("facts_corrupt")
    result = {}
    for key in manifest["keys"]:
        unit = delivery["units"][key]
        request = clone(common["request"])
        request["request_key"] = key
        request["sources"][0]["feedback"] = clone(unit["feedback"])
        typed = ImportRequest.model_validate(request)
        if (
            unit.get("state") not in {"not_started", "submitted", "complete"}
            or type(unit.get("generation")) is not int
            or unit["generation"] < 1
            or _unit_hash(typed, common["policies"], unit["actor"]) != unit.get("request_hash")
            or (unit["state"] == "complete" and not unit.get("receipt_hash"))
        ):
            raise ServiceError("facts_corrupt")
        result[key] = clone(unit) | {
            "request": typed.model_dump(mode="json"),
            "destination_id": common["destination_id"],
            "policies": clone(common["policies"]),
        }
    return result


async def ack(conn: AsyncConnection, claim: Claim, receipts: list[UnitReceipt]) -> None:
    row: Any = (
        (
            await conn.execute(
                select(observation_outbox).where(
                    observation_outbox.c.capture_id == claim.record.capture_id
                )
            )
        )
        .mappings()
        .first()
    )
    if row is None:
        raise ServiceError("not_found", 404)
    if row["phase"] != "acked":
        row = await checked(conn, claim)
    record = claim.record
    if (
        row["claim_generation"] != claim.generation
        or row["claim_token"] != claim.token
        or row["feedback_revision"] != claim.revision
        or len(record.feedback) != claim.revision
        or CaptureRecord.model_validate(row["body"]) != record
        or any(
            row[name] != getattr(record, name)
            for name in (
                "capture_id",
                "source_id",
                "workspace_id",
                "run_id",
                "terminal_event_id",
                "policy_fingerprint",
            )
        )
    ):
        raise ServiceError("revision_conflict")
    delivery = row["delivery"]
    required = delivery.get("units", {})
    destination = delivery.get("common", {}).get("destination_id")
    if (
        not isinstance(destination, str)
        or not destination
        or delivery.get("schema_version") != "hot-delivery/1"
    ):
        raise ServiceError("facts_corrupt")
    expected: dict[str, tuple[str, str | None, dict[str, str] | None]] = {
        delivery_key(
            record.source_id,
            record.capture_id,
            destination,
            record.scope,
            "base",
            record.policy.observer,
        ): (record.policy.observer, None, None)
    }
    for feedback in record.feedback:
        expected[
            delivery_key(
                record.source_id,
                record.capture_id,
                destination,
                record.scope,
                "feedback",
                feedback.actor,
                feedback.key,
            )
        ] = (feedback.actor, feedback.key, {"key": feedback.key, "reason": feedback.reason})
    by_key = {r.key: r for r in receipts}
    if (
        len(receipts) != 1 + claim.revision
        or len(by_key) != len(receipts)
        or set(by_key) != set(required)
        or set(by_key) != set(expected)
    ):
        raise ServiceError("facts_corrupt")
    for key, receipt in by_key.items():
        saved = required[key]
        actor, feedback_key, feedback_body = expected[key]
        if (
            receipt.actor != actor
            or receipt.feedback_key != feedback_key
            or saved.get("actor") != actor
            or saved.get("feedback") != feedback_body
            or saved.get("state") != "complete"
            or saved.get("request_hash") != receipt.request_hash
            or saved.get("receipt_hash") != neutral_fingerprint(receipt.model_dump(mode="json"))
        ):
            raise ServiceError("execution_unknown")
    root = receipts_root(receipts)
    if row["phase"] == "acked":
        if row["ack_root"] != root:
            raise ServiceError("key_conflict")
        return
    changed = await conn.execute(
        update(observation_outbox)
        .where(
            observation_outbox.c.capture_id == record.capture_id,
            observation_outbox.c.phase == "claimed",
            observation_outbox.c.claim_generation == claim.generation,
            observation_outbox.c.claim_token == claim.token,
            observation_outbox.c.feedback_revision == claim.revision,
        )
        .values(phase="acked", ack_root=root)
    )
    if changed.rowcount != 1:
        raise ServiceError("revision_conflict")
    await _feedback_counters(conn, record.workspace_id, record.policy_fingerprint, acked=1)


async def record_feedback(
    conn: AsyncConnection,
    capture_id: str,
    actor: str,
    generation: int,
    key: str,
    reason: FeedbackReason,
) -> FeedbackEvent:
    """反馈、重新入队与计数同事务；旧单元/原键保留，旧claim失效。"""
    row = (
        (
            await conn.execute(
                select(observation_outbox).where(observation_outbox.c.capture_id == capture_id)
            )
        )
        .mappings()
        .first()
    )
    if row is None:
        raise ServiceError("not_found", 404)
    record = CaptureRecord.model_validate(row["body"])
    if (
        row["feedback_revision"] != len(record.feedback)
        or row["phase"] not in {"sampled_out", "pending", "claimed", "acked"}
        or any(
            row[name] != getattr(record, name)
            for name in (
                "capture_id",
                "source_id",
                "workspace_id",
                "run_id",
                "terminal_event_id",
                "policy_fingerprint",
            )
        )
    ):
        raise ServiceError("facts_corrupt")
    body = record.model_dump(mode="json")
    events = body["feedback"]
    for event in events:
        if event["actor"] == actor and event["key"] == key:
            if event["reason"] != reason or event["generation"] != generation:
                raise ServiceError("key_conflict")
            return FeedbackEvent.model_validate(event)
    if len(events) >= 8:
        raise ServiceError("capacity")
    event = FeedbackEvent(
        revision=len(events) + 1, actor=actor, generation=generation, key=key, reason=reason
    )
    events.append(event.model_dump(mode="json"))
    try:
        _delivery_reserved_bytes(body, row["delivery"])
    except ValueError:
        raise ServiceError("capacity") from None
    updated = CaptureRecord.model_validate(body)
    pending = row["phase"] != "sampled_out" or record.policy.feedback_override
    entering = pending and row["phase"] in {"sampled_out", "acked"}
    try:
        delivery = _delivery_extend(updated, row["delivery"])
    except ValueError:
        raise ServiceError("capacity") from None
    await _feedback_capacity(conn, updated, delivery, entering=entering)
    values: dict[str, Any] = {"body": body, "feedback_revision": len(events), "delivery": delivery}
    if pending:
        values.update(phase="pending", claim_token=None, claim_until=None, ack_root=None)
    changed = await conn.execute(
        update(observation_outbox)
        .where(
            observation_outbox.c.capture_id == capture_id,
            observation_outbox.c.feedback_revision == row["feedback_revision"],
            observation_outbox.c.claim_generation == row["claim_generation"],
            observation_outbox.c.phase == row["phase"],
        )
        .values(**values)
    )
    if changed.rowcount != 1:
        raise ServiceError("revision_conflict")
    if entering:
        await _feedback_counters(conn, record.workspace_id, record.policy_fingerprint, enqueued=1)
    return event


async def _terminal_source_id(conn: AsyncConnection) -> str:
    body = (
        await conn.execute(select(capture_meta.c.body).where(capture_meta.c.id == "source"))
    ).scalar_one()
    if (
        not isinstance(body, dict)
        or body.get("schema_version") != "hot-source/1"
        or not isinstance(body.get("source_id"), str)
        or not 1 <= len(body["source_id"]) <= 64
    ):
        raise ServiceError("facts_corrupt")
    return cast(str, body["source_id"])


async def _terminal_link(conn: AsyncConnection, run_id: str) -> dict[str, str] | None:
    from .models import turns

    linked = (
        (
            await conn.execute(
                select(turns.c.id, turns.c.session_id, turns.c.workspace_id).where(
                    turns.c.run_id == run_id
                )
            )
        )
        .mappings()
        .first()
    )
    if linked is None:
        return None
    return {
        "turn_id": linked["id"],
        "session_id": linked["session_id"],
        "workspace_id": linked["workspace_id"],
    }


def _terminal_capture_id(
    source_id: str, workspace: str, run_id: str, event_id: str, policy_fp: str
) -> str:
    return neutral_fingerprint(
        {
            "schema_version": "hot-capture-id/1",
            "source_id": source_id,
            "workspace_id": workspace,
            "run_id": run_id,
            "terminal_event_id": event_id,
            "policy_fingerprint": policy_fp,
        }
    )


def _terminal_sample(source_id: str, run_id: str, event_id: str, frozen: CapturePolicy) -> bool:
    digest = neutral_fingerprint(
        {
            "schema_version": "hot-sample/1",
            "source_id": source_id,
            "run_id": run_id,
            "terminal_event_id": event_id,
            "salt_id": frozen.salt_id,
        }
    )
    return int(digest, 16) % frozen.denominator < frozen.numerator


async def _terminal_capacity(
    conn: AsyncConnection, workspace: str, frozen: CapturePolicy, selected: bool, payload_bytes: int
) -> bool:
    if payload_bytes > 4096:
        return False
    retained = (
        (
            await conn.execute(
                select(
                    observation_outbox.c.workspace_id,
                    observation_outbox.c.phase,
                    observation_outbox.c.body,
                    observation_outbox.c.delivery,
                ).limit(4097)
            )
        )
        .mappings()
        .all()
    )
    local = [row for row in retained if row["workspace_id"] == workspace]
    if len(retained) >= 4096 or len(local) >= frozen.retained_limit:
        return False
    active = [row for row in retained if row["phase"] in {"pending", "claimed"}]
    if selected and (
        len(active) >= 1024
        or sum(row["workspace_id"] == workspace for row in active) >= frozen.pending_limit
    ):
        return False
    total = sum(_delivery_reserved_bytes(row["body"], row["delivery"]) for row in retained)
    return total + payload_bytes <= 16 * 1024 * 1024


async def prepare_terminal(
    conn: AsyncConnection, run_id: str, status: str, code: str | None
) -> dict[str, Any] | None:
    """只冻结本次attempt的可信关联和处置；没有policy时不产生捕获写入。"""
    from sqlalchemy import func

    from krim_agent.contracts import TERMINAL

    from .models import run_events

    if status not in TERMINAL:
        raise ServiceError("facts_corrupt")
    linked = await _terminal_link(conn, run_id)
    if linked is None:
        return None
    frozen = await policy(conn, linked["workspace_id"])
    if frozen is None or not frozen.enabled:
        return None
    if frozen.workspace_id != linked["workspace_id"]:
        raise ServiceError("facts_corrupt")
    saved = (
        (
            await conn.execute(
                select(capture_policies).where(
                    capture_policies.c.workspace_id == frozen.workspace_id
                )
            )
        )
        .mappings()
        .one()
    )
    counters = clone(saved["counters"])
    if (
        set(counters) != {"seen", "enqueued", "dropped", "acked"}
        or any(type(value) is not int or value < 0 for value in counters.values())
        or saved["body"] != frozen.model_dump(mode="json")
        or saved["body_hash"] != frozen.fingerprint()
        or saved["revision"] < 1
    ):
        raise ServiceError("facts_corrupt")
    event_id = "terminal:" + run_id
    prior = (
        await conn.execute(
            select(run_events.c.event_id).where(
                run_events.c.run_id == run_id, run_events.c.event_id == event_id
            )
        )
    ).first()
    prior_capture = (
        await conn.execute(
            select(observation_outbox.c.capture_id)
            .where(observation_outbox.c.run_id == run_id)
            .limit(1)
        )
    ).first()
    if prior is not None or prior_capture is not None:
        raise ServiceError("facts_corrupt")
    source_id = await _terminal_source_id(conn)
    policy_fp = frozen.fingerprint()
    capture_id = _terminal_capture_id(source_id, frozen.workspace_id, run_id, event_id, policy_fp)
    seq = (
        await conn.execute(
            select(func.count()).select_from(run_events).where(run_events.c.run_id == run_id)
        )
    ).scalar_one()
    base_sampled = _terminal_sample(source_id, run_id, event_id, frozen)
    selected = base_sampled or (status != "completed" and frozen.failure_override)
    reason = "sampled" if base_sampled else "failure_override" if selected else "sampled_out"
    decision = {
        "kind": "enqueued" if selected else "sampled_out",
        "base_sampled": base_sampled,
        "selection_reason": reason,
    }
    fields = {
        "schema_version": "hot-reference/1",
        "capture_id": capture_id,
        "source_id": source_id,
        "workspace_id": frozen.workspace_id,
        "scope": frozen.scope,
        "run_id": run_id,
        "terminal_event_id": event_id,
        "terminal_seq": seq,
        "policy": frozen.model_dump(mode="json"),
        "policy_fingerprint": policy_fp,
        "feedback": [],
    }
    delivery = {"schema_version": "hot-delivery/1", "selection": clone(decision), "units": {}}
    # 最终摘要只预留字段和64字符的占用，不构造或持久化占位摘要。
    try:
        payload_bytes = _delivery_reserved_bytes(fields, delivery)
    except ValueError:
        payload_bytes = 4097
    if not await _terminal_capacity(conn, frozen.workspace_id, frozen, selected, payload_bytes):
        decision = {**decision, "kind": "overflow"}
    capture = {
        "record": fields,
        "link": linked,
        "status": status,
        "code": code,
        "policy_revision": saved["revision"],
        "counters": counters,
        "disposition": {"capture_id": capture_id, "policy_fp": policy_fp, "decision": decision},
        "delivery": delivery,
    }
    neutral_encoded(capture, max_bytes=8192)
    return capture


def _terminal_snapshot_hash(capture: dict[str, Any], snapshot: Any) -> str:
    import math

    from krim_agent.evaluation.trace_cases import observe_snapshot

    fields = capture["record"]
    context = snapshot.session_context
    terminals = [event for event in snapshot.events if event.kind == "terminal"]
    expected = {
        "status": capture["status"],
        "code": capture["code"],
        "observation_capture": capture["disposition"],
    }
    if (
        snapshot.run.id != fields["run_id"]
        or snapshot.run.status != capture["status"]
        or snapshot.run.code != capture["code"]
        or snapshot.run.finished_at is None
        or not math.isfinite(snapshot.run.finished_at)
        or context is None
        or {
            "turn_id": context.turn_id,
            "session_id": context.session_id,
            "workspace_id": context.workspace_id,
        }
        != capture["link"]
        or fields["workspace_id"] != context.workspace_id
        or len(terminals) != 1
    ):
        raise ServiceError("facts_corrupt")
    terminal = terminals[0]
    if (
        terminal.run_id != fields["run_id"]
        or terminal.event_id != "terminal:" + fields["run_id"]
        or terminal.event_id != fields["terminal_event_id"]
        or terminal.seq != fields["terminal_seq"]
        or terminal.seq != len(snapshot.events) - 1
        or terminal.data != expected
    ):
        raise ServiceError("facts_corrupt")
    return observe_snapshot(snapshot).snapshot_fingerprint


async def finalize_terminal(conn: AsyncConnection, capture: dict[str, Any], snapshot: Any) -> None:
    """最终snapshot已真实形成；所有写入仍属于原事务及原D4 SAVEPOINT。"""
    fields = capture["record"]
    frozen = CapturePolicy.model_validate(fields["policy"])
    if (
        not frozen.enabled
        or frozen.workspace_id != fields["workspace_id"]
        or frozen.scope != fields["scope"]
        or frozen.fingerprint() != fields["policy_fingerprint"]
        or await _terminal_source_id(conn) != fields["source_id"]
        or await _terminal_link(conn, fields["run_id"]) != capture["link"]
        or _terminal_capture_id(
            fields["source_id"],
            fields["workspace_id"],
            fields["run_id"],
            fields["terminal_event_id"],
            fields["policy_fingerprint"],
        )
        != fields["capture_id"]
    ):
        raise ServiceError("facts_corrupt")
    digest = _terminal_snapshot_hash(capture, snapshot)
    saved = (
        (
            await conn.execute(
                select(capture_policies).where(
                    capture_policies.c.workspace_id == frozen.workspace_id
                )
            )
        )
        .mappings()
        .one()
    )
    if (
        saved["body"] != fields["policy"]
        or saved["body_hash"] != frozen.fingerprint()
        or saved["scope"] != frozen.scope
        or saved["revision"] != capture["policy_revision"]
        or saved["counters"] != capture["counters"]
    ):
        raise ServiceError("facts_corrupt")
    kind = capture["disposition"]["decision"]["kind"]
    if kind not in {"enqueued", "sampled_out", "overflow"}:
        raise ServiceError("facts_corrupt")
    existing = (
        await conn.execute(
            select(observation_outbox.c.capture_id)
            .where(observation_outbox.c.run_id == fields["run_id"])
            .limit(1)
        )
    ).first()
    if existing is not None:
        raise ServiceError("key_conflict")
    if kind != "overflow":
        record = CaptureRecord.model_validate({**fields, "snapshot_hash": digest})
        body = record.model_dump(mode="json")
        neutral_encoded({"record": body, "delivery": capture["delivery"]}, max_bytes=4096)
        await conn.execute(
            insert(observation_outbox).values(
                capture_id=record.capture_id,
                source_id=record.source_id,
                workspace_id=record.workspace_id,
                run_id=record.run_id,
                terminal_event_id=record.terminal_event_id,
                policy_fingerprint=record.policy_fingerprint,
                body=body,
                phase="pending" if kind == "enqueued" else "sampled_out",
                feedback_revision=0,
                claim_generation=0,
                claim_token=None,
                claim_until=None,
                delivery=capture["delivery"],
                ack_root=None,
            )
        )
    counters = {
        **capture["counters"],
        "seen": capture["counters"]["seen"] + 1,
        "enqueued": capture["counters"]["enqueued"] + int(kind == "enqueued"),
        "dropped": capture["counters"]["dropped"] + int(kind == "overflow"),
    }
    changed = await conn.execute(
        update(capture_policies)
        .where(
            capture_policies.c.workspace_id == frozen.workspace_id,
            capture_policies.c.revision == capture["policy_revision"],
            capture_policies.c.body_hash == fields["policy_fingerprint"],
        )
        .values(counters=counters)
    )
    if changed.rowcount != 1:
        raise ServiceError("facts_corrupt")


async def confirm_terminal(
    conn: AsyncConnection, snapshot: Any, status: str, code: str | None
) -> bool:
    """丢回程只核同终点的冻结处置及不可变引用；不读最新policy、不补写。"""
    from krim_agent.contracts import RunEvent, RunRequest, RunView, fingerprint

    from .models import run_events

    row = (await conn.execute(select(runs).where(runs.c.id == snapshot.run.id))).mappings().first()
    if (
        row is None
        or snapshot.run.status != status
        or snapshot.run.code != code
        or RunView.model_validate({key: row[key] for key in RunView.model_fields}) != snapshot.run
    ):
        return False
    if fingerprint(
        RunRequest.model_validate(row["request"]).model_dump(mode="json")
    ) != fingerprint(snapshot.request.model_dump(mode="json")) or fingerprint(
        row["target"]
    ) != fingerprint(snapshot.target):
        return False
    actual_events = (
        (
            await conn.execute(
                select(run_events)
                .where(run_events.c.run_id == snapshot.run.id)
                .order_by(run_events.c.seq)
                .limit(len(snapshot.events) + 1)
            )
        )
        .mappings()
        .all()
    )
    if tuple(RunEvent.model_validate(dict(event)) for event in actual_events) != snapshot.events:
        return False
    terminals = [event for event in snapshot.events if event.kind == "terminal"]
    if (
        len(terminals) != 1
        or terminals[0].event_id != "terminal:" + snapshot.run.id
        or terminals[0].seq != len(snapshot.events) - 1
    ):
        return False
    terminal = terminals[0]
    found = (
        (
            await conn.execute(
                select(observation_outbox)
                .where(observation_outbox.c.run_id == snapshot.run.id)
                .limit(2)
            )
        )
        .mappings()
        .all()
    )
    if terminal.data == {"status": status, "code": code}:
        return not found
    disposition = terminal.data.get("observation_capture")
    if (
        not isinstance(disposition, dict)
        or set(disposition) != {"capture_id", "policy_fp", "decision"}
        or set(terminal.data) != {"status", "code", "observation_capture"}
    ):
        return False
    decision = disposition["decision"]
    if (
        not isinstance(decision, dict)
        or set(decision) != {"kind", "base_sampled", "selection_reason"}
        or decision["kind"] not in {"enqueued", "sampled_out", "overflow"}
        or type(decision["base_sampled"]) is not bool
        or decision["selection_reason"] not in {"sampled", "failure_override", "sampled_out"}
        or decision["base_sampled"] != (decision["selection_reason"] == "sampled")
        or (
            decision["kind"] != "overflow"
            and (decision["kind"] == "enqueued") != (decision["selection_reason"] != "sampled_out")
        )
    ):
        return False
    linked = await _terminal_link(conn, snapshot.run.id)
    policy_fp = disposition["policy_fp"]
    if (
        linked is None
        or not isinstance(policy_fp, str)
        or len(policy_fp) != 64
        or any(char not in "0123456789abcdef" for char in policy_fp)
    ):
        return False
    source_id = await _terminal_source_id(conn)
    if (
        _terminal_capture_id(
            source_id, linked["workspace_id"], snapshot.run.id, terminal.event_id, policy_fp
        )
        != disposition["capture_id"]
    ):
        return False
    if decision["kind"] == "overflow":
        context = snapshot.session_context
        return (
            not found
            and snapshot.run.finished_at is not None
            and context is not None
            and linked
            == {
                "turn_id": context.turn_id,
                "session_id": context.session_id,
                "workspace_id": context.workspace_id,
            }
            and terminal.data
            == {"status": status, "code": code, "observation_capture": disposition}
        )
    if len(found) != 1:
        return False
    saved = found[0]
    record = CaptureRecord.model_validate(saved["body"])
    frozen = record.policy
    base_sampled = _terminal_sample(source_id, snapshot.run.id, terminal.event_id, frozen)
    selected = base_sampled or (status != "completed" and frozen.failure_override)
    reason = "sampled" if base_sampled else "failure_override" if selected else "sampled_out"
    if (
        not frozen.enabled
        or record.source_id != source_id
        or record.run_id != snapshot.run.id
        or record.workspace_id != linked["workspace_id"]
        or record.terminal_event_id != terminal.event_id
        or record.capture_id != disposition["capture_id"]
        or record.policy_fingerprint != policy_fp
        or decision
        != {
            "kind": "enqueued" if selected else "sampled_out",
            "base_sampled": base_sampled,
            "selection_reason": reason,
        }
        or any(
            saved[key] != getattr(record, key)
            for key in (
                "capture_id",
                "source_id",
                "workspace_id",
                "run_id",
                "terminal_event_id",
                "policy_fingerprint",
            )
        )
        or saved["feedback_revision"] != len(record.feedback)
        or saved["phase"] not in {"pending", "claimed", "acked", "sampled_out"}
        or (decision["kind"] == "enqueued" and saved["phase"] == "sampled_out")
        or (
            decision["kind"] == "sampled_out"
            and saved["phase"] != "sampled_out"
            and not record.feedback
        )
        or saved["delivery"].get("schema_version") != "hot-delivery/1"
        or saved["delivery"].get("selection") != decision
    ):
        return False
    capture = {
        "record": record.model_dump(mode="json"),
        "link": linked,
        "status": status,
        "code": code,
        "disposition": clone(disposition),
    }
    if _terminal_snapshot_hash(capture, snapshot) != record.snapshot_hash:
        return False
    neutral_encoded({"record": saved["body"], "delivery": saved["delivery"]}, max_bytes=4096)
    return True


async def _feedback_capacity(
    conn: AsyncConnection, record: CaptureRecord, delivery: dict[str, Any], *, entering: bool
) -> None:
    """只读核容量；拒绝前不接受新反馈，也不清除已完成的投递单元。"""
    active_policy = await policy(conn, record.workspace_id)
    if active_policy is None or active_policy.fingerprint() != record.policy_fingerprint:
        raise ServiceError("target_binding_changed")
    try:
        replacement_bytes = _delivery_reserved_bytes(record.model_dump(mode="json"), delivery)
    except ValueError:
        raise ServiceError("capacity") from None
    retained = (
        (
            await conn.execute(
                select(
                    observation_outbox.c.capture_id,
                    observation_outbox.c.workspace_id,
                    observation_outbox.c.phase,
                    observation_outbox.c.body,
                    observation_outbox.c.delivery,
                ).limit(4097)
            )
        )
        .mappings()
        .all()
    )
    local = [row for row in retained if row["workspace_id"] == record.workspace_id]
    if (
        len(retained) > 4096
        or len(local) > record.policy.retained_limit
        or sum(row["capture_id"] == record.capture_id for row in retained) != 1
    ):
        raise ServiceError("capacity")
    active = [row for row in retained if row["phase"] in {"pending", "claimed"}]
    if (
        len(active) + int(entering) > 1024
        or sum(row["workspace_id"] == record.workspace_id for row in active) + int(entering)
        > record.policy.pending_limit
    ):
        raise ServiceError("capacity")
    try:
        retained_bytes = sum(
            _delivery_reserved_bytes(row["body"], row["delivery"])
            for row in retained
            if row["capture_id"] != record.capture_id
        )
    except ValueError:
        raise ServiceError("facts_corrupt") from None
    if retained_bytes + replacement_bytes > 16 * 1024 * 1024:
        raise ServiceError("capacity")


async def _feedback_counters(
    conn: AsyncConnection, workspace: str, policy_fp: str, *, enqueued: int = 0, acked: int = 0
) -> None:
    """只随当前原事务计入新入队/首次ack；重复请求不会调用本函数。"""
    if (
        type(enqueued) is not int
        or type(acked) is not int
        or enqueued not in {0, 1}
        or acked not in {0, 1}
    ):
        raise ServiceError("facts_corrupt")
    saved = (
        (
            await conn.execute(
                select(capture_policies).where(capture_policies.c.workspace_id == workspace)
            )
        )
        .mappings()
        .first()
    )
    if saved is None:
        raise ServiceError("facts_corrupt")
    frozen = CapturePolicy.model_validate(saved["body"])
    if (
        frozen.workspace_id != workspace
        or frozen.scope != saved["scope"]
        or frozen.fingerprint() != policy_fp
        or saved["body_hash"] != policy_fp
    ):
        raise ServiceError("target_binding_changed")
    counters = clone(saved["counters"])
    if set(counters) != {"seen", "enqueued", "dropped", "acked"} or any(
        type(value) is not int or value < 0 for value in counters.values()
    ):
        raise ServiceError("facts_corrupt")
    counters["enqueued"] += enqueued
    counters["acked"] += acked
    changed = await conn.execute(
        update(capture_policies)
        .where(
            capture_policies.c.workspace_id == workspace,
            capture_policies.c.revision == saved["revision"],
            capture_policies.c.body_hash == policy_fp,
        )
        .values(counters=counters)
    )
    if changed.rowcount != 1:
        raise ServiceError("revision_conflict")


def _delivery_unit(
    record: CaptureRecord, value: dict[str, Any]
) -> tuple[str, dict[str, Any], dict[str, Any]]:
    from krim_agent.evaluation.continuous_types import ImportRequest
    from krim_agent.evaluation.hot_types import _unit_hash

    request = ImportRequest.model_validate(value["request"])
    actor, generation, destination = value["actor"], value["generation"], value["destination_id"]
    policies = clone(value["policies"])
    if (
        type(generation) is not int
        or generation < 1
        or not isinstance(destination, str)
        or not destination
        or set(policies) != {"sampling", "redaction"}
        or policies["redaction"] != record.policy.redaction()
        or policies["sampling"].get("numerator") != 1
        or policies["sampling"].get("denominator") != 1
        or request.scope != record.scope
        or request.source_alias != "hot-krim"
        or len(request.sources) != 1
        or request.sources[0].run_id != record.run_id
    ):
        raise ServiceError("facts_corrupt")
    feedback = request.sources[0].feedback
    if feedback is None:
        if actor != record.policy.observer:
            raise ServiceError("facts_corrupt")
        feedback_body = None
    else:
        matching = [
            event for event in record.feedback if event.actor == actor and event.key == feedback.key
        ]
        if (
            len(matching) != 1
            or matching[0].generation != generation
            or matching[0].reason != feedback.reason
        ):
            raise ServiceError("facts_corrupt")
        feedback_body = feedback.model_dump(mode="json")
    key = delivery_key(
        record.source_id,
        record.capture_id,
        destination,
        record.scope,
        "base" if feedback is None else "feedback",
        actor,
        "base" if feedback is None else feedback.key,
    )
    if request.request_key != key or _unit_hash(request, policies, actor) != value["request_hash"]:
        raise ServiceError("facts_corrupt")
    body = request.model_dump(mode="json")
    body.pop("request_key")
    body["sources"][0].pop("feedback")
    common = {"request": body, "policies": policies, "destination_id": destination}
    try:
        _delivery_reserved_bytes(
            record.model_dump(mode="json"), {"common": common}, common_only=True
        )
    except ValueError:
        raise ServiceError("capacity") from None
    state = value["state"]
    if state not in {"not_started", "submitted", "complete"}:
        raise ServiceError("execution_unknown")
    unit = {
        "actor": actor,
        "generation": generation,
        "feedback": feedback_body,
        "request_hash": value["request_hash"],
        "state": state,
    }
    if state == "complete":
        receipt = UnitReceipt.model_validate(value["receipt"])
        if (
            receipt.key != key
            or receipt.actor != actor
            or receipt.request_hash != value["request_hash"]
            or receipt.feedback_key != (None if feedback is None else feedback.key)
        ):
            raise ServiceError("facts_corrupt")
        unit["receipt_hash"] = neutral_fingerprint(receipt.model_dump(mode="json"))
    return key, common, unit


def _delivery_freeze(
    record: CaptureRecord, previous: dict[str, Any], values: list[dict[str, Any]]
) -> dict[str, Any]:
    if len(values) != 1 + len(record.feedback):
        raise ServiceError("facts_corrupt")
    normalized = [_delivery_unit(record, value) for value in values]
    keys = [key for key, _, _ in normalized]
    common = normalized[0][1]
    expected = [
        delivery_key(
            record.source_id,
            record.capture_id,
            common["destination_id"],
            record.scope,
            "base",
            record.policy.observer,
        )
    ] + [
        delivery_key(
            record.source_id,
            record.capture_id,
            common["destination_id"],
            record.scope,
            "feedback",
            event.actor,
            event.key,
        )
        for event in record.feedback
    ]
    if (
        keys != expected
        or len(set(keys)) != len(keys)
        or any(shared != common or unit["state"] != "not_started" for _, shared, unit in normalized)
    ):
        raise ServiceError("facts_corrupt")
    delivery = clone(previous) or {"schema_version": "hot-delivery/1", "units": {}}
    old = expanded_units(delivery)
    old_manifest = delivery.get("manifest")
    if old_manifest is None:
        if old or delivery.get("common") is not None:
            raise ServiceError("execution_unknown")
    else:
        revision = old_manifest["revision"]
        if type(revision) is not int or not 0 <= revision <= len(record.feedback):
            raise ServiceError("facts_corrupt")
        if old_manifest["keys"] != expected[: 1 + revision] or delivery["common"] != common:
            raise ServiceError("key_conflict")
    units = {}
    for key, _, unit in normalized:
        if key in old:
            saved = delivery["units"][key]
            if any(
                saved.get(name) != unit.get(name)
                for name in ("actor", "generation", "feedback", "request_hash")
            ):
                raise ServiceError("key_conflict")
            units[key] = saved
        else:
            if old_manifest is not None and key in old_manifest["keys"]:
                raise ServiceError("execution_unknown")
            units[key] = unit
    delivery.update(
        common=common, units=units, manifest={"revision": len(record.feedback), "keys": keys}
    )
    try:
        _delivery_reserved_bytes(record.model_dump(mode="json"), delivery)
    except ValueError:
        raise ServiceError("capacity") from None
    return delivery


def _delivery_extend(record: CaptureRecord, previous: dict[str, Any]) -> dict[str, Any]:
    from krim_agent.evaluation.hot_types import _unit_hash, import_request

    if not previous.get("manifest"):
        try:
            _delivery_reserved_bytes(record.model_dump(mode="json"), previous)
        except ValueError:
            raise ServiceError("capacity") from None
    if not previous.get("manifest"):
        return cast(dict[str, Any], clone(previous))
    expanded = expanded_units(previous)
    common = previous["common"]
    base_key = previous["manifest"]["keys"][0]
    generation = expanded[base_key]["generation"]
    actors = [(record.policy.observer, generation, None)] + [
        (event.actor, event.generation, event) for event in record.feedback
    ]
    values = []
    for actor, generation, feedback in actors:
        request = import_request(record, common["destination_id"], actor, feedback)
        values.append(
            {
                "request": request.model_dump(mode="json"),
                "actor": actor,
                "generation": generation,
                "policies": common["policies"],
                "destination_id": common["destination_id"],
                "state": "not_started",
                "request_hash": _unit_hash(request, common["policies"], actor),
            }
        )
    return _delivery_freeze(record, previous, values)


def _delivery_reserved_bytes(
    record: dict[str, Any], delivery: dict[str, Any], *, common_only: bool = False
) -> int:
    """纯字节上界；保留原持久数据，为全部已接受单元预留完成态。"""
    policy_body = record["policy"]
    feedback = record["feedback"]
    if len(feedback) > 8:
        raise ValueError("delivery_feedback_capacity")
    # import_request的完整默认字段及ActorViews的原sampling/redaction装配。
    # 目的身份仅预留编码内容；Repository生成36字符ASCII UUID，界限允许64字节。
    common_shape = {
        "request": {
            "scope": record["scope"],
            "source_alias": "hot-krim",
            "sources": [
                {
                    "task_id": None,
                    "case_id": None,
                    "run_id": record["run_id"],
                    "repetition": 0,
                    "revision": None,
                    "evidence_hash": None,
                }
            ],
        },
        "policies": {
            "sampling": {
                "version": "explicit-id-hash/1",
                "salt_id": "default",
                "numerator": 1,
                "denominator": 1,
            },
            "redaction": {
                "version": "continuous-redact/1",
                "artifact_paths": policy_body["artifact_paths"],
                "include_text": policy_body["include_text"],
                "include_arguments": policy_body["include_arguments"],
            },
        },
        "destination_id": "",
    }
    shell = {
        key: value for key, value in delivery.items() if key not in {"common", "manifest", "units"}
    }
    shell["units"] = {}
    actors: list[tuple[str, int, dict[str, Any] | None]] = [
        (policy_body["observer"], 9007199254740991, None)
    ] + [
        (event["actor"], event["generation"], {"key": event["key"], "reason": event["reason"]})
        for event in feedback
    ]
    # 空串只计字段标点，两个摘要各另加64字节；不生成或保存任何假摘要。
    unit_shapes: list[dict[str, Any]] = [
        {
            "actor": actor,
            "generation": generation,
            "feedback": feedback_body,
            "request_hash": "",
            "state": "complete",
            "receipt_hash": "",
        }
        for actor, generation, feedback_body in actors
    ]
    saved_units: list[dict[str, Any]] = [
        delivery["units"][key] for key in delivery.get("manifest", {}).get("keys", [])
    ]
    if len(saved_units) > len(unit_shapes):
        raise ValueError("delivery_manifest_capacity")
    pieces: list[Any] = [
        {"record": record, "delivery": delivery},
        common_shape,
        delivery.get("common"),
        {"record": record, "delivery": shell},
        {"revision": len(feedback), "keys": []},
        *unit_shapes,
        *saved_units,
    ]
    sizes: list[int] = []
    for piece in pieces:
        # neutral_encoded保留原UTF-8和转义语义；另逐容器计默认分隔空格。
        # 字符串内逗号、冒号不计结构分隔，故同时覆盖check_json和紧凑编码。
        amount = len(neutral_encoded(piece).encode("utf-8"))
        stack: list[Any] = [piece]
        while stack:
            value = stack.pop()
            if isinstance(value, dict):
                amount += len(value) + max(0, len(value) - 1)
                stack.extend(value.values())
            elif isinstance(value, list):
                amount += max(0, len(value) - 1)
                stack.extend(value)
        sizes.append(amount)
    common_limit = sizes[1] + 64
    if delivery.get("common") is not None and sizes[2] > common_limit:
        raise ValueError("delivery_common_capacity")
    if common_only:
        return common_limit
    # 最终snapshot摘要字段在prepare时尚不存在，只计默认分隔的完整字段宽度。
    snapshot_bytes = 0 if "snapshot_hash" in record else len(b', "snapshot_hash": ""') + 64
    current_bytes = sizes[0] + snapshot_bytes
    count = len(unit_shapes)
    # hot1.加SHA256的43字符无填充base64url，键恒48个ASCII字节。
    key_bytes = len(b"hot1.") + (256 + 5) // 6
    units_bytes = 0
    for index in range(count):
        unit_bytes = sizes[5 + index] + 2 * 64
        if index < len(saved_units):
            saved = saved_units[index]
            growth = 0 if saved.get("state") == "complete" else len(b', "receipt_hash": ""') + 64
            unit_bytes = max(unit_bytes, sizes[5 + count + index] + growth)
        units_bytes += key_bytes + len(b'"": ') + unit_bytes
    units_bytes += len(b", ") * (count - 1)
    manifest_bytes = sizes[4] + count * (key_bytes + len(b'""')) + len(b", ") * (count - 1)
    reserved_bytes = (
        sizes[3]
        + snapshot_bytes
        + len(b', "common": ')
        + common_limit
        + units_bytes
        + len(b', "manifest": ')
        + manifest_bytes
    )
    reserved_bytes = max(current_bytes, reserved_bytes)
    if reserved_bytes > 4096:
        raise ValueError("delivery_capacity")
    return reserved_bytes
