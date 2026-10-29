"""D1_DATA 的 SQL 接缝；只借原 Store 的 AsyncConnection、锁与提交确认。"""

from __future__ import annotations

import time
import uuid
from typing import TYPE_CHECKING, Any, cast

from sqlalchemy import func, insert, select, update
from sqlalchemy.ext.asyncio import AsyncConnection

from krim_agent.contracts import ServiceError, encoded, fingerprint
from krim_agent.workspace.types import (
    ArtifactRevision,
    EffectIntent,
    EffectReceipt,
    ExecutionAccess,
    WorkFrame,
    WorkspaceProfile,
    WorkspaceTurnRequest,
)

from .models import (
    artifact_revisions,
    run_events,
    runs,
    sessions,
    tool_effects,
    turns,
    work_sessions,
    work_turns,
)

if TYPE_CHECKING:
    from .sqlite import SQLiteStore


def require(store: SQLiteStore) -> None:
    if store.schema_version not in {4, 5}:
        raise ServiceError("schema_upgrade_required")


async def row(
    conn: AsyncConnection, session_id: str, access: ExecutionAccess, *, write: bool = False
) -> Any:
    found = (
        (await conn.execute(select(work_sessions).where(work_sessions.c.session_id == session_id)))
        .mappings()
        .first()
    )
    if found is None:
        raise ServiceError("not_found", 404)
    access.authorize(found["workspace_id"], found["owner_id"], session_id, write=write)
    return found


def revision(found: Any) -> ArtifactRevision:
    body = {str(k): v for k, v in found.items()}
    body["revision_id"] = body.pop("id")
    body.pop("actor_id")
    body.pop("created_at")
    value = ArtifactRevision.model_validate_json(encoded(body))
    if (
        fingerprint([f.model_dump(mode="json") for f in value.manifest]) != value.manifest_hash
        or len({f.path for f in value.manifest}) != len(value.manifest)
        or sum(f.size for f in value.manifest) > 8388608
    ):
        raise ServiceError("facts_corrupt")
    return value


async def get_revision(
    conn: AsyncConnection, session_id: str, revision_id: str, access: ExecutionAccess
) -> ArtifactRevision:
    await row(conn, session_id, access)
    found = (
        (
            await conn.execute(
                select(artifact_revisions).where(
                    artifact_revisions.c.id == revision_id,
                    artifact_revisions.c.session_id == session_id,
                )
            )
        )
        .mappings()
        .first()
    )
    if found is None:
        raise ServiceError("not_found", 404)
    return revision(found)


async def by_key(
    conn: AsyncConnection, session_id: str, kind: str, key: str, access: ExecutionAccess
) -> ArtifactRevision | None:
    await row(conn, session_id, access)
    found = (
        (
            await conn.execute(
                select(artifact_revisions).where(
                    artifact_revisions.c.session_id == session_id,
                    artifact_revisions.c.source_kind == kind,
                    artifact_revisions.c.request_key == key,
                )
            )
        )
        .mappings()
        .first()
    )
    return None if found is None else revision(found)


async def initialize(conn: AsyncConnection, session: Any, target: Any) -> None:
    try:
        profile = WorkspaceProfile.model_validate_json(encoded(target["workspace_profile"]))
    except (KeyError, ValueError):
        raise ServiceError("workspace_unavailable") from None
    raw = profile.model_dump(mode="json")
    await conn.execute(
        insert(work_sessions).values(
            session_id=session["id"],
            workspace_id=session["workspace_id"],
            owner_id=session["owner_id"],
            profile_snapshot=raw,
            profile_hash=fingerprint(raw),
            head_revision=None,
            generation=0,
            created_at=time.time(),
        )
    )
    rid = str(uuid.uuid4())
    await conn.execute(
        insert(artifact_revisions).values(
            id=rid,
            session_id=session["id"],
            parent_id=None,
            actor_id=session["owner_id"],
            revision=0,
            source_kind="input",
            request_key="initial",
            request_hash=fingerprint({"initial": session["id"]}),
            producer_run_id=None,
            producer_call_id=None,
            manifest=[],
            manifest_hash=fingerprint([]),
            created_at=time.time(),
        )
    )
    await conn.execute(
        update(work_sessions)
        .where(work_sessions.c.session_id == session["id"])
        .values(head_revision=rid)
    )


async def enqueue(
    conn: AsyncConnection, turn: Any, body: WorkspaceTurnRequest, access: ExecutionAccess
) -> None:
    raw = body.model_dump(mode="json")
    await row(conn, turn["session_id"], access, write=True)
    await conn.execute(
        insert(work_turns).values(
            turn_id=turn["id"],
            run_id=None,
            session_id=turn["session_id"],
            actor_id=access.principal.principal_id,
            grant_snapshot=access.grant().model_dump(mode="json"),
            request_json=raw,
            request_hash=fingerprint(raw),
            requested_input=raw["input"],
            resolved_input_revision=None,
            source_refs=None,
            subject=None,
            conditions=None,
            effective_hash=None,
            prepared_context=None,
            plan={"steps": []},
            plan_revision=0,
            verification=None,
            phase="queued",
            created_at=time.time(),
            finished_at=None,
        )
    )


async def bind(
    conn: AsyncConnection, turn: Any, run_id: str, target: Any, previous: list[Any]
) -> dict[str, Any]:
    pending = (
        (await conn.execute(select(work_turns).where(work_turns.c.turn_id == turn["id"])))
        .mappings()
        .first()
    )
    session = (
        (
            await conn.execute(
                select(work_sessions).where(work_sessions.c.session_id == turn["session_id"])
            )
        )
        .mappings()
        .first()
    )
    if pending is None or session is None:
        raise ServiceError("facts_corrupt")
    request = WorkspaceTurnRequest.model_validate_json(encoded(pending["request_json"]))
    if (
        fingerprint(pending["request_json"]) != pending["request_hash"]
        or pending["request_hash"] != turn["request_hash"]
        or pending["requested_input"] != request.input.model_dump(mode="json")
        or fingerprint(session["profile_snapshot"]) != session["profile_hash"]
    ):
        raise ServiceError("facts_corrupt")
    selected = request.input.revision_id
    if selected is not None and selected != session["head_revision"]:
        raise ServiceError("input_revision_conflict")
    profile = WorkspaceProfile.model_validate_json(encoded(session["profile_snapshot"]))
    sources = [
        {
            "turn_id": p["id"],
            "run_id": p["run_id"],
            "transcript_fingerprint": fingerprint(p["transcript"]),
            "status": p["status"],
            "code": p["code"],
            "text_fingerprint": fingerprint(p["text"]),
            "terminal_seq": p["terminal_seq"],
        }
        for p in previous
    ]
    subject = {
        k: v
        for k, v in target.items()
        if k not in {"workspace_profile", "limits", "limits_fingerprint", "workspace_verification"}
    }
    conditions = {
        "runtime": profile.runtime.model_dump(mode="json")
        | {
            "max_tools_per_step": profile.max_tools_per_step,
            "http": target.get("limits", {}).get("http"),
        },
        "environment": profile.environment,
        "security": {
            "version": profile.security_version,
            "operations": pending["grant_snapshot"]["operations"],
        },
        "verification": {
            "schema": "completion-verification/1",
            "contract": target["workspace_verification"],
            "completion": request.completion.model_dump(mode="json"),
            "completion_fingerprint": fingerprint(request.completion.model_dump(mode="json")),
        },
        "context": {
            "policy": await conn.scalar(
                select(sessions.c.context_policy).where(sessions.c.id == turn["session_id"])
            ),
            "compaction": profile.compaction.model_dump(mode="json"),
        },
        "artifact": {
            "version": "artifact/1",
            "max_files": 32,
            "max_file_bytes": 1048576,
            "max_total_bytes": 8388608,
            "chunk_bytes": 16384,
            "model_chunk_bytes": 2048,
        },
    }
    effective = fingerprint({"subject": subject, "conditions": conditions})
    await conn.execute(
        update(work_turns)
        .where(work_turns.c.turn_id == turn["id"])
        .values(
            resolved_input_revision=session["head_revision"],
            source_refs=sources,
            subject=subject,
            conditions=conditions,
            effective_hash=effective,
            phase="preparing",
        )
    )
    return {
        "schema_version": 2,
        "effective_fingerprint": effective,
        "source_fingerprint": fingerprint(sources),
    }


async def load(conn: AsyncConnection, run_id: str) -> WorkFrame | None:
    found = (
        (await conn.execute(select(work_turns).where(work_turns.c.run_id == run_id)))
        .mappings()
        .first()
    )
    if found is None:
        return None
    turn = (
        (await conn.execute(select(turns).where(turns.c.id == found["turn_id"]))).mappings().one()
    )
    session = (
        (
            await conn.execute(
                select(work_sessions).where(work_sessions.c.session_id == found["session_id"])
            )
        )
        .mappings()
        .one()
    )
    source = (
        (
            await conn.execute(
                select(artifact_revisions).where(
                    artifact_revisions.c.id == found["resolved_input_revision"]
                )
            )
        )
        .mappings()
        .one()
    )
    if found["source_refs"] is None:
        raise ServiceError("facts_corrupt")
    for ref in found["source_refs"]:
        prior = (
            (await conn.execute(select(turns).where(turns.c.id == ref["turn_id"])))
            .mappings()
            .first()
        )
        if (
            prior is None
            or prior["session_id"] != found["session_id"]
            or prior["run_id"] != ref["run_id"]
            or prior["status"] != ref["status"]
            or prior["code"] != ref["code"]
            or fingerprint(prior["text"]) != ref["text_fingerprint"]
            or fingerprint(prior["transcript"]) != ref["transcript_fingerprint"]
        ):
            raise ServiceError("facts_corrupt")
        last = (
            await conn.scalar(
                select(func.max(run_events.c.seq)).where(run_events.c.run_id == ref["run_id"])
            )
            if ref["run_id"]
            else None
        )
        if (last if last is not None else -1) != ref["terminal_seq"]:
            raise ServiceError("facts_corrupt")
    digest = fingerprint({"subject": found["subject"], "conditions": found["conditions"]})
    if (
        found["request_hash"] != fingerprint(found["request_json"])
        or found["request_hash"] != turn["request_hash"]
        or turn["run_id"] != run_id
        or found["session_id"] != turn["session_id"]
        or source["session_id"] != found["session_id"]
        or session["workspace_id"] != turn["workspace_id"]
        or session["owner_id"] != turn["owner_id"]
        or digest != found["effective_hash"]
    ):
        raise ServiceError("facts_corrupt")
    return WorkFrame.model_validate_json(
        encoded(
            {
                "session_id": found["session_id"],
                "turn_id": found["turn_id"],
                "actor_id": found["actor_id"],
                "grant": found["grant_snapshot"],
                "original_request": found["request_json"],
                "input_revision": revision(source).model_dump(mode="json"),
                "source_refs": found["source_refs"],
                "subject": found["subject"],
                "conditions": found["conditions"],
                "effective_fingerprint": digest,
                "phase": found["phase"],
                "verification": found["verification"],
            }
        )
    )


async def publish(
    conn: AsyncConnection,
    session_id: str,
    access: ExecutionAccess,
    *,
    kind: str,
    key: str,
    digest: str,
    expected: int,
    manifest: list[Any],
    run_id: str | None = None,
    call_id: str | None = None,
    receipt: dict[str, Any] | None = None,
) -> ArtifactRevision:
    from .sessions import closing

    head = await row(conn, session_id, access, write=True)
    old = await by_key(conn, session_id, kind, key, access)
    if old is not None:
        if old.request_hash != digest:
            raise ServiceError("artifact_conflict")
        return old
    session = (
        (await conn.execute(select(sessions).where(sessions.c.id == session_id))).mappings().one()
    )
    if session["status"] != "open" or await closing(conn, session_id):
        raise ServiceError("session_closed")
    if kind == "input":
        busy = await conn.scalar(
            select(turns.c.id)
            .where(turns.c.session_id == session_id, turns.c.status.in_(["queued", "running"]))
            .limit(1)
        )
        if busy is not None:
            raise ServiceError("workspace_busy")
        if run_id is not None or call_id is not None:
            raise ServiceError("facts_corrupt")
    elif kind == "effect":
        effect = (
            (await conn.execute(select(tool_effects).where(tool_effects.c.call_id == call_id)))
            .mappings()
            .first()
        )
        if (
            effect is None
            or effect["run_id"] != run_id
            or effect["session_id"] != session_id
            or effect["phase"] != "dispatch_intent"
            or receipt is None
            or receipt.get("output_complete") is not True
            or receipt.get("cleanup") != "clean"
        ):
            raise ServiceError("execution_unknown")
    else:
        raise ServiceError("artifact_invalid")
    parent = (
        (
            await conn.execute(
                select(artifact_revisions).where(artifact_revisions.c.id == head["head_revision"])
            )
        )
        .mappings()
        .one()
    )
    if kind == "effect":
        assert effect is not None
        if effect["input_revision"] != parent["id"]:
            raise ServiceError("input_revision_conflict")
    if parent["revision"] != expected:
        raise ServiceError("input_revision_conflict")
    rid = str(uuid.uuid4())
    values = dict(
        id=rid,
        session_id=session_id,
        parent_id=parent["id"],
        actor_id=access.principal.principal_id,
        revision=expected + 1,
        source_kind=kind,
        request_key=key,
        request_hash=digest,
        producer_run_id=run_id,
        producer_call_id=call_id,
        manifest=manifest,
        manifest_hash=fingerprint(manifest),
        created_at=time.time(),
    )
    result = revision(values)
    await conn.execute(insert(artifact_revisions).values(**values))
    changed = await conn.execute(
        update(work_sessions)
        .where(
            work_sessions.c.session_id == session_id, work_sessions.c.head_revision == parent["id"]
        )
        .values(head_revision=rid, generation=head["generation"] + 1)
    )
    if changed.rowcount != 1:
        raise ServiceError("input_revision_conflict")
    if kind == "effect":
        assert receipt is not None
        await conn.execute(
            update(tool_effects)
            .where(tool_effects.c.call_id == call_id)
            .values(
                phase="settled",
                receipt=receipt | {"artifact_revision_id": rid},
                finished_at=time.time(),
            )
        )
    return result


async def intent(
    conn: AsyncConnection,
    access: ExecutionAccess,
    run_id: str,
    *,
    call_id: str,
    operation_key: str,
    args_hash: str,
    tool_hash: str,
    kind: str,
    input_revision: str | None = None,
) -> dict[str, Any]:
    frame = await load(conn, run_id)
    if frame is None:
        raise ServiceError("workspace_contract_required")
    await row(conn, frame.session_id, access, write=True)
    if access.grant() != frame.grant:
        raise ServiceError("target_binding_changed")
    old = (
        (
            await conn.execute(
                select(tool_effects).where(
                    tool_effects.c.run_id == run_id, tool_effects.c.operation_key == operation_key
                )
            )
        )
        .mappings()
        .first()
    )
    selected = input_revision or frame.input_revision.revision_id
    await get_revision(conn, frame.session_id, selected, access)
    values: dict[str, Any] = dict(
        call_id=call_id,
        run_id=run_id,
        operation_key=operation_key,
        session_id=frame.session_id,
        turn_id=frame.turn_id,
        args_hash=args_hash,
        tool_hash=tool_hash,
        grant_hash=fingerprint(frame.grant.model_dump(mode="json")),
        input_revision=selected,
        kind=kind,
    )
    EffectIntent.model_validate_json(encoded(values))
    if old is not None:
        if any(old[k] != v for k, v in values.items()):
            raise ServiceError("key_conflict")
        return dict(old)
    await effect_admission(conn, frame.session_id)
    current = await row(conn, frame.session_id, access, write=True)
    if kind != "read" and current["head_revision"] != selected:
        raise ServiceError("input_revision_conflict")
    status = await conn.scalar(select(runs.c.status).where(runs.c.id == run_id))
    if status not in {"queued", "running"}:
        raise ServiceError("run_closed")
    values.update(
        phase="intent", leaf_ticket=None, receipt=None, created_at=time.time(), finished_at=None
    )
    await conn.execute(insert(tool_effects).values(**values))
    return values


async def dispatch(conn: AsyncConnection, call_id: str, access: ExecutionAccess) -> None:
    effect = (
        (await conn.execute(select(tool_effects).where(tool_effects.c.call_id == call_id)))
        .mappings()
        .first()
    )
    if effect is None:
        raise ServiceError("not_found", 404)
    await row(conn, effect["session_id"], access, write=True)
    await effect_admission(conn, effect["session_id"])
    if effect["phase"] != "intent":
        raise ServiceError("execution_unknown")
    await conn.execute(
        update(tool_effects)
        .where(tool_effects.c.call_id == call_id)
        .values(phase="dispatch_intent")
    )


async def finish(conn: AsyncConnection, run_id: str, *, interrupted: bool = False) -> None:
    from krim_agent.workspace import verification
    frame = await load(conn, run_id)
    if frame is None:
        return
    await conn.execute(
        update(work_turns)
        .where(work_turns.c.run_id == run_id)
        .values(phase="interrupted" if interrupted else "settled", finished_at=time.time())
    )
    effects = (
        (
            await conn.execute(
                select(tool_effects).where(
                    tool_effects.c.run_id == run_id,
                    tool_effects.c.phase.in_(["intent", "dispatch_intent"]),
                )
            )
        )
        .mappings()
        .all()
    )
    for effect in effects:
        await conn.execute(
            update(tool_effects)
            .where(tool_effects.c.call_id == effect["call_id"])
            .values(
                phase="settled" if effect["receipt"] is not None else "unknown",
                receipt=effect["receipt"]
                if effect["receipt"] is not None
                else {
                    "executor_started": False if effect["phase"] == "intent" else None,
                    "exit_code": None,
                    "output_complete": None,
                    "cleanup": "unknown",
                },
                finished_at=time.time(),
            )
        )

    verification_contract = frame.conditions.get("verification")
    if (isinstance(verification_contract, dict)
            and verification_contract.get("schema") == "completion-verification/1"):
        await verification._TERMINAL_HELPERS[5](conn, run_id, interrupted=interrupted)


async def effect(
    conn: AsyncConnection, access: ExecutionAccess, call_id: str
) -> dict[str, Any] | None:
    found = (
        (await conn.execute(select(tool_effects).where(tool_effects.c.call_id == call_id)))
        .mappings()
        .first()
    )
    if found is None:
        return None
    await row(conn, found["session_id"], access)
    return dict(found)


async def record_result(
    conn: AsyncConnection,
    access: ExecutionAccess,
    call_id: str,
    receipt: dict[str, Any],
    *,
    run_id: str,
) -> dict[str, Any]:
    found = await effect(conn, access, call_id)
    if (
        found is None
        or found["run_id"] != run_id
        or found["phase"] not in {"dispatch_intent", "settled"}
    ):
        raise ServiceError("execution_unknown")
    await row(conn, found["session_id"], access, write=True)
    EffectReceipt.model_validate_json(encoded(receipt))
    if found["receipt"] is not None and found["receipt"] != receipt:
        # 已发布的receipt还含Artifact引用；不得以重试抹掉真实提交。
        if {k: v for k, v in found["receipt"].items() if k != "artifact_revision_id"} != receipt:
            raise ServiceError("artifact_conflict")
        return cast(dict[str, Any], found["receipt"])
    await conn.execute(
        update(tool_effects).where(tool_effects.c.call_id == call_id).values(receipt=receipt)
    )
    return receipt


async def effect_admission(conn: AsyncConnection, session_id: str) -> None:
    from .sessions import closing

    state = await conn.scalar(select(sessions.c.status).where(sessions.c.id == session_id))
    if state != "open" or await closing(conn, session_id):
        raise ServiceError("session_closed")
