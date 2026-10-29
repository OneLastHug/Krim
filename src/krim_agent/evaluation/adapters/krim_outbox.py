"""Krim源库的借用热观察边界；无新writer/Kernel/关闭权。"""

from __future__ import annotations

import hashlib
import time
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any, cast

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncConnection

from krim_agent.contracts import ServiceError
from krim_agent.security import PrincipalContext
from krim_agent.sessions.service import SessionService
from krim_agent.storage import observation_outbox as sql
from krim_agent.storage.models import runs, turns
from krim_agent.storage.sqlite import SQLiteStore

from ..continuous import ContinuousEvaluation
from ..domain import clone, neutral_fingerprint
from ..hot_types import (
    ActorViewBinding,
    CapturePolicy,
    CaptureRecord,
    Claim,
    FeedbackEvent,
    FeedbackSignal,
    UnitReceipt,
    receipts_root,
)
from .krim_source import KrimSourceReader


def method(value: Any) -> object:
    return getattr(value, "__func__", value)


class KrimOutboxPort:
    def __init__(
        self,
        store: SQLiteStore,
        sessions: SessionService,
        principal: PrincipalContext,
        policy: CapturePolicy,
    ) -> None:
        self.store, self.sessions, self.principal = store, sessions, principal
        self.policy = CapturePolicy.model_validate(policy.model_dump(mode="json"))

    def binding(self) -> tuple[object, ...]:
        return (
            id(self.store),
            id(self.store.engine),
            id(self.sessions),
            id(self.principal),
            self.principal.generation,
            self.policy.fingerprint(),
            method(self.store.work_read),
            method(self.store.work_write),
            method(self.store.snapshot),
            hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        )

    def guard(self, frozen: tuple[object, ...]) -> None:
        if self.binding() != frozen or self.store.closed:
            raise ServiceError("target_binding_changed")

    async def _read[T](self, operation: Callable[[AsyncConnection], Awaitable[T]]) -> T:
        frozen = self.binding()
        self.principal.member(self.policy.workspace_id)

        async def checked(conn: AsyncConnection) -> T:
            result = await operation(conn)
            self.guard(frozen)
            return result

        result = await self.store.work_read(checked)
        self.guard(frozen)
        return result

    async def _write[T, Witness](
        self,
        operation: Callable[[AsyncConnection], Awaitable[tuple[T, Witness]]],
        confirm: Callable[[AsyncConnection, Witness], Awaitable[Witness | None]],
    ) -> T:
        frozen = self.binding()
        principal, workspace = self.principal, self.policy.workspace_id
        identity = (principal.principal_id, principal.generation)
        boxed: tuple[T, Witness] | None = None

        def check() -> None:
            self.guard(frozen)
            if (principal.principal_id, principal.generation) != identity:
                raise ServiceError("target_binding_changed")
            principal.member(workspace, write=True)

        check()

        async def checked(conn: AsyncConnection) -> tuple[T, Witness]:
            nonlocal boxed
            check()
            value = await operation(conn)
            check()
            if value[1] is None:
                raise ServiceError("storage_unavailable", 503)
            boxed = value
            return value

        async def confirmed() -> tuple[T, Witness] | None:
            check()
            if boxed is None:
                return None
            original = boxed

            async def read(conn: AsyncConnection) -> tuple[T, Witness] | None:
                check()
                found = await confirm(conn, original[1])
                check()
                return original if found is not None and found == original[1] else None

            value = await self.store.work_read(read)
            check()
            return value

        value = await self.store.work_write(checked, confirmed)
        check()
        return value[0]

    async def configure(self) -> None:
        frozen = self.binding()
        policy = CapturePolicy.model_validate(self.policy.model_dump(mode="json"))
        body, digest = policy.model_dump(mode="json"), policy.fingerprint()

        def check() -> None:
            self.guard(frozen)
            if self.principal.member(policy.workspace_id, write=True).role != "admin":
                raise ServiceError("permission_denied", 403)

        check()

        async def read(conn: Any) -> dict[str, Any] | None:
            check()
            row = (
                (
                    await conn.execute(
                        select(sql.capture_policies).where(
                            sql.capture_policies.c.workspace_id == policy.workspace_id
                        )
                    )
                )
                .mappings()
                .first()
            )
            check()
            if (
                row is None
                or row["scope"] != policy.scope
                or row["body"] != body
                or row["body_hash"] != digest
                or row["revision"] < 1
            ):
                return None
            return {
                key: clone(row[key])
                for key in ("workspace_id", "scope", "body", "body_hash", "revision")
            }

        async def operation(conn: Any) -> tuple[None, Any]:
            check()
            await sql.configure(conn, policy)
            check()
            witness = await read(conn)
            check()
            if witness is None:
                raise ServiceError("facts_corrupt")
            return None, witness

        async def confirm(conn: Any, witness: Any) -> Any:
            row = await read(conn)
            check()
            return witness if row == witness else None

        await self._write(operation, confirm)

    async def rows(self) -> list[dict[str, Any]]:
        return await self._read(lambda conn: sql.rows(conn, self.policy.workspace_id))

    async def claim(self, *, limit: int = 16) -> list[Claim]:
        frozen = self.binding()
        policy = CapturePolicy.model_validate(self.policy.model_dump(mode="json"))

        def check() -> None:
            self.guard(frozen)
            self.principal.member(policy.workspace_id, write=True)

        check()

        async def inspect(conn: Any, claimed: Claim) -> dict[str, Any] | None:
            check()
            row = (
                (
                    await conn.execute(
                        select(sql.observation_outbox).where(
                            sql.observation_outbox.c.capture_id == claimed.record.capture_id
                        )
                    )
                )
                .mappings()
                .first()
            )
            check()
            self._scope(claimed)
            record = claimed.record
            if (
                row is None
                or row["phase"] != "claimed"
                or row["workspace_id"] != policy.workspace_id
                or row["policy_fingerprint"] != policy.fingerprint()
                or row["source_id"] != record.source_id
                or row["run_id"] != record.run_id
                or row["terminal_event_id"] != record.terminal_event_id
                or row["claim_generation"] != claimed.generation
                or row["claim_token"] != claimed.token
                or row["feedback_revision"] != claimed.revision
                or row["claim_until"] is None
                or row["claim_until"] <= time.time()
                or len(record.feedback) != claimed.revision
                or CaptureRecord.model_validate(row["body"]) != record
            ):
                return None
            return {
                key: clone(row[key])
                for key in (
                    "capture_id",
                    "source_id",
                    "workspace_id",
                    "run_id",
                    "terminal_event_id",
                    "policy_fingerprint",
                    "body",
                    "phase",
                    "feedback_revision",
                    "claim_generation",
                    "claim_token",
                    "claim_until",
                )
            }

        async def operation(conn: Any) -> tuple[list[Claim], Any]:
            check()
            values = await sql.claim_rows(conn, policy.workspace_id, limit, check)
            check()
            witness = []
            for claimed in values:
                row = await inspect(conn, claimed)
                check()
                if row is None:
                    raise ServiceError("facts_corrupt")
                witness.append(row)
            return values, witness

        async def confirm(conn: Any, witness: Any) -> Any:
            # 空批次没有持久批次键：正常[]可返回，丢回程不能猜成功。
            if not witness:
                return None
            for expected in witness:
                claimed = Claim(
                    CaptureRecord.model_validate(expected["body"]),
                    expected["claim_generation"],
                    expected["feedback_revision"],
                    expected["claim_token"],
                )
                row = await inspect(conn, claimed)
                check()
                if row != expected:
                    return None
            return witness

        return await self._write(operation, confirm)

    async def units(self, claim: Claim) -> dict[str, Any]:
        self._scope(claim)

        async def operation(conn: Any) -> dict[str, Any]:
            row = (
                (
                    await conn.execute(
                        select(sql.observation_outbox).where(
                            sql.observation_outbox.c.capture_id == claim.record.capture_id
                        )
                    )
                )
                .mappings()
                .first()
            )
            if row is None:
                raise ServiceError("not_found", 404)
            actual = CaptureRecord.model_validate(row["body"])
            immutable = actual.model_dump(mode="json", exclude={"feedback"})
            if (
                immutable != claim.record.model_dump(mode="json", exclude={"feedback"})
                or actual.feedback[: claim.revision] != claim.record.feedback
                or claim.revision > row["feedback_revision"]
                or claim.generation > row["claim_generation"]
            ):
                raise ServiceError("revision_conflict")
            values = sql.expanded_units(row["delivery"])
            if not values:
                return {}
            destination = row["delivery"]["common"]["destination_id"]
            keys = [
                sql.delivery_key(
                    actual.source_id,
                    actual.capture_id,
                    destination,
                    actual.scope,
                    "base",
                    actual.policy.observer,
                )
            ] + [
                sql.delivery_key(
                    actual.source_id,
                    actual.capture_id,
                    destination,
                    actual.scope,
                    "feedback",
                    event.actor,
                    event.key,
                )
                for event in claim.record.feedback
            ]
            all_keys = [keys[0]] + [
                sql.delivery_key(
                    actual.source_id,
                    actual.capture_id,
                    destination,
                    actual.scope,
                    "feedback",
                    item.actor,
                    item.key,
                )
                for item in actual.feedback
            ]
            if row["delivery"]["manifest"] != {
                "revision": len(actual.feedback),
                "keys": all_keys,
            } or any(key not in values for key in keys):
                raise ServiceError("execution_unknown")
            return {key: values[key] for key in keys}

        return await self._read(operation)

    def _scope(self, claim: Claim) -> None:
        if (
            claim.record.workspace_id != self.policy.workspace_id
            or claim.record.scope != self.policy.scope
            or claim.record.policy_fingerprint != self.policy.fingerprint()
        ):
            raise ServiceError("not_found", 404)

    async def save_unit(self, claim: Claim, key: str, value: dict[str, Any]) -> None:
        frozen = self.binding()
        claimed = Claim(
            CaptureRecord.model_validate(claim.record.model_dump(mode="json")),
            claim.generation,
            claim.revision,
            claim.token,
        )
        submitted = clone(value)

        def check() -> None:
            self.guard(frozen)
            self.principal.member(claimed.record.workspace_id, write=True)
            self._scope(claimed)

        check()

        async def inspect(conn: Any) -> dict[str, Any] | None:
            check()
            row = (
                (
                    await conn.execute(
                        select(sql.observation_outbox).where(
                            sql.observation_outbox.c.capture_id == claimed.record.capture_id
                        )
                    )
                )
                .mappings()
                .first()
            )
            check()
            if (
                row is None
                or row["phase"] not in {"claimed", "acked"}
                or row["claim_generation"] != claimed.generation
                or row["claim_token"] != claimed.token
                or row["feedback_revision"] != claimed.revision
                or CaptureRecord.model_validate(row["body"]) != claimed.record
            ):
                return None
            delivery = row["delivery"]
            if "manifest" in submitted:
                if (
                    sql._delivery_freeze(claimed.record, delivery, submitted["manifest"])
                    != delivery
                ):
                    return None
            else:
                unit_key, common, unit = sql._delivery_unit(claimed.record, submitted)
                if (
                    unit_key != key
                    or delivery.get("common") != common
                    or delivery.get("units", {}).get(key) != unit
                ):
                    return None
            return {
                "capture_id": row["capture_id"],
                "generation": row["claim_generation"],
                "revision": row["feedback_revision"],
                "token": row["claim_token"],
                "delivery": clone(delivery),
            }

        async def operation(conn: Any) -> tuple[None, Any]:
            check()
            await sql.save_unit(conn, claimed, key, submitted)
            check()
            witness = await inspect(conn)
            check()
            if witness is None:
                raise ServiceError("facts_corrupt")
            return None, witness

        async def confirm(conn: Any, witness: Any) -> Any:
            row = await inspect(conn)
            check()
            return witness if row == witness else None

        await self._write(operation, confirm)

    async def ack(self, claim: Claim, receipts: list[UnitReceipt]) -> None:
        frozen = self.binding()
        claimed = Claim(
            CaptureRecord.model_validate(claim.record.model_dump(mode="json")),
            claim.generation,
            claim.revision,
            claim.token,
        )
        values = [UnitReceipt.model_validate(r.model_dump(mode="json")) for r in receipts]
        root = receipts_root(values)
        by_key = {r.key: r for r in values}
        keys = set(by_key)

        def check() -> None:
            self.guard(frozen)
            self.principal.member(claimed.record.workspace_id, write=True)
            self._scope(claimed)

        check()

        async def inspect(conn: Any) -> dict[str, Any] | None:
            check()
            row = (
                (
                    await conn.execute(
                        select(sql.observation_outbox).where(
                            sql.observation_outbox.c.capture_id == claimed.record.capture_id
                        )
                    )
                )
                .mappings()
                .first()
            )
            check()
            record = claimed.record
            if (
                row is None
                or row["workspace_id"] != record.workspace_id
                or row["policy_fingerprint"] != record.policy_fingerprint
                or row["source_id"] != record.source_id
                or row["run_id"] != record.run_id
                or row["terminal_event_id"] != record.terminal_event_id
                or row["claim_generation"] != claimed.generation
                or row["claim_token"] != claimed.token
                or row["feedback_revision"] != claimed.revision
                or row["phase"] != "acked"
                or row["ack_root"] != root
                or CaptureRecord.model_validate(row["body"]) != record
            ):
                return None
            delivery = row["delivery"]
            units = delivery.get("units", {})
            destination = delivery.get("common", {}).get("destination_id")
            expected: dict[str, tuple[str, str | None, dict[str, str] | None]] = {
                sql.delivery_key(
                    record.source_id,
                    record.capture_id,
                    destination,
                    record.scope,
                    "base",
                    record.policy.observer,
                ): (record.policy.observer, None, None)
            }
            for event in record.feedback:
                expected[
                    sql.delivery_key(
                        record.source_id,
                        record.capture_id,
                        destination,
                        record.scope,
                        "feedback",
                        event.actor,
                        event.key,
                    )
                ] = (event.actor, event.key, {"key": event.key, "reason": event.reason})
            if (
                destination is None
                or len(record.feedback) != claimed.revision
                or len(keys) != len(values)
                or len(values) != 1 + claimed.revision
                or set(units) != keys
                or set(expected) != keys
            ):
                return None
            for key, receipt in by_key.items():
                actor, feedback_key, feedback = expected[key]
                unit = units[key]
                if (
                    receipt.actor != actor
                    or receipt.feedback_key != feedback_key
                    or unit.get("actor") != actor
                    or unit.get("feedback") != feedback
                    or unit.get("state") != "complete"
                    or unit.get("request_hash") != receipt.request_hash
                    or unit.get("receipt_hash")
                    != neutral_fingerprint(receipt.model_dump(mode="json"))
                ):
                    return None
            # 已acked无需续lease；只核原generation/R/root与完整原收据。
            return cast(dict[str, Any], clone({str(key): value for key, value in row.items()}))

        async def operation(conn: Any) -> tuple[None, Any]:
            check()
            await sql.ack(conn, claimed, values)
            check()
            witness = await inspect(conn)
            check()
            if witness is None:
                raise ServiceError("facts_corrupt")
            return None, witness

        async def confirm(conn: Any, witness: Any) -> Any:
            row = await inspect(conn)
            check()
            return witness if row == witness else None

        await self._write(operation, confirm)

    async def feedback(
        self, actor: PrincipalContext, run_id: str, signal: FeedbackSignal
    ) -> FeedbackEvent:
        frozen = self.binding()
        policy = CapturePolicy.model_validate(self.policy.model_dump(mode="json"))
        submitted = FeedbackSignal.model_validate(signal.model_dump(mode="json"))
        actor_identity = (actor.principal_id, actor.generation)

        def check() -> None:
            self.guard(frozen)
            if (actor.principal_id, actor.generation) != actor_identity:
                raise ServiceError("target_binding_changed")
            actor.member(policy.workspace_id, write=True)

        check()

        async def authorized(conn: Any) -> str:
            check()
            linked = (
                await conn.execute(
                    select(turns.c.owner_id, turns.c.workspace_id).where(turns.c.run_id == run_id)
                )
            ).first()
            check()
            if linked is None or linked.workspace_id != policy.workspace_id:
                raise ServiceError("not_found", 404)
            actor.owns(policy.workspace_id, linked.owner_id, write=True)
            terminal = (
                await conn.execute(select(runs.c.status).where(runs.c.id == run_id))
            ).scalar_one()
            check()
            actor.owns(policy.workspace_id, linked.owner_id, write=True)
            if terminal in {"queued", "running"}:
                raise ServiceError("invalid_input", 422)
            capture_id: str | None = (
                await conn.execute(
                    select(sql.observation_outbox.c.capture_id).where(
                        sql.observation_outbox.c.run_id == run_id,
                        sql.observation_outbox.c.workspace_id == policy.workspace_id,
                        sql.observation_outbox.c.policy_fingerprint == policy.fingerprint(),
                    )
                )
            ).scalar_one_or_none()
            check()
            actor.owns(policy.workspace_id, linked.owner_id, write=True)
            if capture_id is None:
                raise ServiceError("not_found", 404)
            return capture_id

        async def inspect(conn: Any, capture_id: str, event: FeedbackEvent) -> Any:
            check()
            row = (
                (
                    await conn.execute(
                        select(sql.observation_outbox).where(
                            sql.observation_outbox.c.capture_id == capture_id
                        )
                    )
                )
                .mappings()
                .first()
            )
            check()
            if (
                row is None
                or row["workspace_id"] != policy.workspace_id
                or row["run_id"] != run_id
                or row["policy_fingerprint"] != policy.fingerprint()
            ):
                return None
            record = CaptureRecord.model_validate(row["body"])
            matching = [
                value
                for value in record.feedback
                if value.actor == actor_identity[0] and value.key == submitted.key
            ]
            if (
                record.capture_id != capture_id
                or record.workspace_id != policy.workspace_id
                or record.scope != policy.scope
                or record.run_id != run_id
                or record.policy != policy
                or row["source_id"] != record.source_id
                or row["terminal_event_id"] != record.terminal_event_id
                or row["feedback_revision"] != len(record.feedback)
                or len(matching) != 1
                or matching[0] != event
                or event.actor != actor_identity[0]
                or event.generation != actor_identity[1]
                or event.key != submitted.key
                or event.reason != submitted.reason
            ):
                return None
            immutable = record.model_dump(mode="json")
            immutable.pop("feedback")
            return {
                "capture_id": capture_id,
                "record": immutable,
                "event": event.model_dump(mode="json"),
            }

        async def operation(conn: Any) -> tuple[FeedbackEvent, Any]:
            capture_id = await authorized(conn)
            check()
            # 只写原反馈事件，不接override/enqueue/candidate。
            event = await sql.record_feedback(
                conn,
                capture_id,
                actor_identity[0],
                actor_identity[1],
                submitted.key,
                submitted.reason,
            )
            check()
            witness = await inspect(conn, capture_id, event)
            check()
            if witness is None:
                raise ServiceError("facts_corrupt")
            return event, witness

        async def confirm(conn: Any, witness: Any) -> Any:
            capture_id = await authorized(conn)
            check()
            if capture_id != witness["capture_id"]:
                return None
            row = await inspect(conn, capture_id, FeedbackEvent.model_validate(witness["event"]))
            check()
            return witness if row == witness else None

        return await self._write(operation, confirm)


class KrimOutboxSourceReader:
    def __init__(self, port: KrimOutboxPort, principal: PrincipalContext, claim: Claim) -> None:
        self.port, self.principal, self.claim = port, principal, claim
        self.reader = KrimSourceReader(
            port.store,
            port.sessions,
            principal,
            workspace_id=port.policy.workspace_id,
            scope=port.policy.scope,
        )

    def binding(self) -> tuple[object, ...]:
        return (
            self.port.binding(),
            id(self.principal),
            self.principal.generation,
            self.reader.binding(),
            self.claim.record.capture_id,
            self.claim.record.snapshot_hash,
        )

    async def read(self, scope: str, selection: dict[str, Any]) -> dict[str, Any]:
        frozen = self.binding()
        record = self.claim.record
        if scope != record.scope or selection.get("run_id") != record.run_id:
            raise ServiceError("not_found", 404)
        value = await self.reader.read(scope, selection)
        if self.binding() != frozen:
            raise ServiceError("target_binding_changed")
        source = value["source"]
        if (
            source["terminal_event_id"] != record.terminal_event_id
            or source["last_seq"] != record.terminal_seq
            or source["snapshot_hash"] != record.snapshot_hash
        ):
            raise ServiceError("source_changed")
        # 不附actor/反馈revision/claim，保证同一Observation在各作者间身份不变。
        source["hot_capture"] = {
            "source_id": record.source_id,
            "capture_id": record.capture_id,
            "policy_fingerprint": record.policy_fingerprint,
        }
        return value


class ActorViews:
    def __init__(
        self,
        port: KrimOutboxPort,
        repository: Any,
        kernel: Any,
        actors: dict[str, PrincipalContext],
    ) -> None:
        self.port, self.repository, self.kernel = port, repository, kernel
        self.actors = dict(actors)

    def binding(self) -> tuple[object, ...]:
        return (
            self.port.binding(),
            id(self.repository),
            id(self.kernel),
            id(self.kernel.repository),
            method(self.repository.import_observations),
            method(self.repository.candidate),
            ContinuousEvaluation.import_sources,
            KrimOutboxSourceReader.read,
            KrimSourceReader.read,
            tuple(
                sorted(
                    (name, id(p), p.generation, method(p.member), method(p.owns))
                    for name, p in self.actors.items()
                )
            ),
        )

    def resolve(self, actor: str, generation: int, claim: Claim) -> ActorViewBinding:
        frozen = self.binding()
        principal = self.actors.get(actor)
        if (
            principal is None
            or principal.principal_id != actor
            or principal.generation != generation
        ):
            raise ServiceError("target_binding_changed")
        principal.member(claim.record.workspace_id)
        reader = KrimOutboxSourceReader(self.port, principal, claim)
        reader_identity = reader.binding()
        reader_methods = (
            method(reader.read),
            method(reader.binding),
            method(reader.reader.read),
            method(reader.reader.binding),
        )
        view = ContinuousEvaluation(
            self.repository,
            self.kernel,
            scope=claim.record.scope,
            actor=actor,
            sources={"hot-krim": reader},
        )
        view.redaction = claim.record.policy.redaction()
        sampling = clone(view.sampling)
        redaction = clone(view.redaction)
        importer = method(view.import_sources)

        def check() -> None:
            if (
                self.binding() != frozen
                or view.actor != actor
                or view.scope != claim.record.scope
                or view.repository is not self.repository
                or view.kernel is not self.kernel
                or method(view.import_sources) is not importer
                or view.sources != {"hot-krim": reader}
                or reader.binding() != reader_identity
                or (
                    method(reader.read),
                    method(reader.binding),
                    method(reader.reader.read),
                    method(reader.reader.binding),
                )
                != reader_methods
                or neutral_fingerprint(view.sampling) != neutral_fingerprint(sampling)
                or neutral_fingerprint(view.redaction) != neutral_fingerprint(redaction)
                or principal.generation != generation
            ):
                raise ServiceError("target_binding_changed")
            principal.member(claim.record.workspace_id)

        check()
        return ActorViewBinding(actor, generation, view, check)
