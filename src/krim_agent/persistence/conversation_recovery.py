"""同事务的Host恢复分支；原消息/Run/工具及副作用账本均保留。"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Literal, TypedDict, cast
from uuid import NAMESPACE_URL, uuid5

from sqlalchemy import literal, select
from sqlalchemy.ext.asyncio import AsyncSession

from krim_agent.conversation_recovery import (
    MAX_MESSAGES,
    ConversationRecoveryPreview,
    ConversationRecoveryRecord,
    ConversationRecoveryRequest,
    RecoveryAnsweredReference,
    canonical,
    incomplete_tail,
    path_fingerprint,
    recovery_error,
    recovery_notice,
    tool_reference,
)
from krim_agent.protocol.domain import MessageView, ServiceError
from krim_agent.protocol.models import utc_now

from .agent_sessions import assert_session_open, guard_agent_topic
from .database import AgentSessionRow, KrimRepository, MessageRow, RunRow, TopicRow
from .tree import ROOT, MessageTree


@dataclass(frozen=True)
class ConversationRecoveryScope:
    topic_id: str
    session_id: str | None = None
    agent_id: str | None = None
    owner_id: str | None = None


class _PreviewIdentity(TypedDict):
    topic_id: str
    session_id: str | None
    agent_id: str | None
    session_revision: int | None
    source_path_hash: str
    source_branch_key: str


class ConversationRecoveryStore:
    def __init__(self, repository: KrimRepository) -> None:
        self.repository = repository
        self.database = repository.database

    @staticmethod
    def message_id(scope: ConversationRecoveryScope, request_key: str) -> str:
        value = canonical(["krim-conversation-recovery-v1", scope.topic_id, request_key])
        return str(uuid5(NAMESPACE_URL, value.decode("utf-8")))

    @staticmethod
    def _request_hash(
        scope: ConversationRecoveryScope, request: ConversationRecoveryRequest
    ) -> str:
        return hashlib.sha256(
            canonical(
                {
                    "topic_id": scope.topic_id,
                    "session_id": scope.session_id,
                    "agent_id": scope.agent_id,
                    "request": request.model_dump(mode="json"),
                }
            )
        ).hexdigest()

    async def _guard(
        self, sql: AsyncSession, scope: ConversationRecoveryScope, *, lock_topic: bool = False
    ) -> tuple[TopicRow, AgentSessionRow | None]:
        topic = (
            await sql.scalar(
                select(TopicRow).where(TopicRow.id == scope.topic_id).with_for_update()
            )
            if lock_topic
            else await sql.get(TopicRow, scope.topic_id)
        )
        if topic is None:
            raise ServiceError("topic_not_found", "恢复话题不存在", 404)
        session = await guard_agent_topic(
            sql, scope.topic_id, scope.session_id, scope.agent_id, scope.owner_id
        )
        return topic, session

    async def _snapshot(
        self, sql: AsyncSession, scope: ConversationRecoveryScope
    ) -> tuple[
        TopicRow,
        AgentSessionRow | None,
        list[MessageView],
        list[MessageView],
        ConversationRecoveryPreview,
    ]:
        topic, session = await self._guard(sql, scope)
        await self.repository._require_idle(sql, scope.topic_id)
        rows = list(
            await sql.scalars(
                select(MessageRow)
                .where(MessageRow.topic_id == scope.topic_id)
                .order_by(MessageRow.sequence)
                .limit(MAX_MESSAGES + 1)
            )
        )
        if len(rows) > MAX_MESSAGES:
            raise recovery_error("limit", "恢复消息树超过4096节点边界", 413)
        messages = [MessageView.model_validate(row) for row in rows]
        tree = MessageTree(messages)
        path = tree.active_path(topic.active_branches)
        source_hash = path_fingerprint(path)
        branch_key = self.repository.context_branch_key(path)
        tail = incomplete_tail(path)
        arguments: _PreviewIdentity = dict(
            topic_id=scope.topic_id,
            session_id=scope.session_id,
            agent_id=scope.agent_id,
            session_revision=session.revision if session else None,
            source_path_hash=source_hash,
            source_branch_key=branch_key,
        )
        if tail is None:
            tree.require_complete_tools(path)
            preview = ConversationRecoveryPreview(
                **arguments,
                available=False,
                source_run_id=None,
                source_run_status=None,
                anchor_message_id=None,
                excluded_message_ids=[],
                pending_calls=[],
                answered_calls=[],
            )
            return topic, session, messages, path, preview
        original = await sql.get(RunRow, tail.run_id)
        if (
            original is None
            or original.topic_id != scope.topic_id
            or original.status not in {"interrupted", "failed", "aborted"}
        ):
            raise recovery_error("run", "未闭合工具组缺少原中断/失败/停止Run")
        prefix = path[: tail.group_index]
        tree.require_complete_tools(prefix)
        preview = ConversationRecoveryPreview(
            **arguments,
            available=True,
            source_run_id=original.id,
            source_run_status=cast(Literal["interrupted", "failed", "aborted"], original.status),
            anchor_message_id=prefix[-1].id if prefix else None,
            excluded_message_ids=[message.id for message in path[tail.group_index :]],
            pending_calls=[tool_reference(call) for call in tail.pending],
            answered_calls=[
                RecoveryAnsweredReference(
                    call_id=cast(str, message.details["call_id"]),
                    message_id=message.id,
                    message_hash=hashlib.sha256(
                        canonical(message.model_dump(mode="json"))
                    ).hexdigest(),
                )
                for message in tail.answered
            ],
        )
        recovery_notice(preview)
        return topic, session, messages, path, preview

    async def preview(self, scope: ConversationRecoveryScope) -> ConversationRecoveryPreview:
        async with self.database.write_lock, self.database.sessions() as sql:
            *_, preview = await self._snapshot(sql, scope)
            await self._guard(sql, scope)
            return preview

    async def _record(
        self, sql: AsyncSession, row: MessageRow, scope: ConversationRecoveryScope
    ) -> ConversationRecoveryRecord:
        try:
            if (
                set(row.details) != {"origin", "kind", "record", "request_hash"}
                or row.role != "system"
                or row.details.get("origin") != "host"
                or row.details.get("kind") != "conversation_recovery"
            ):
                raise ValueError("恢复消息标记不完整")
            record = ConversationRecoveryRecord.model_validate_json(
                json.dumps(row.details["record"], ensure_ascii=False, allow_nan=False)
            )
            if (
                row.id != record.recovery_message_id
                or row.id != self.message_id(scope, record.request.request_key)
                or row.topic_id != scope.topic_id
                or row.run_id != record.request.source_run_id
                or row.parent_id != record.source.anchor_message_id
            ):
                raise ValueError("恢复消息绑定不完整")
            if (record.source.topic_id, record.source.session_id, record.source.agent_id) != (
                scope.topic_id,
                scope.session_id,
                scope.agent_id,
            ) or row.details["request_hash"] != self._request_hash(scope, record.request):
                raise ValueError("恢复原身份或请求不一致")
            if MessageView.model_validate(
                row
            ).created_at != record.created_at or row.content != recovery_notice(record.source):
                raise ValueError("恢复时间或系统记录被改变")
            # 记录须重新对应真实原父链，不能只信可被同时修改的JSON和正文。
            table = MessageRow.__table__
            ancestry = (
                select(*table.c, literal(0).label("depth"))
                .where(table.c.id == record.source.excluded_message_ids[-1])
                .cte("recovery_original_path", recursive=True)
            )
            parent = table.alias("recovery_parent")
            ancestry = ancestry.union_all(
                select(*parent.c, (ancestry.c.depth + 1).label("depth")).where(
                    parent.c.id == ancestry.c.parent_id, ancestry.c.depth < MAX_MESSAGES
                )
            )
            rows = (
                (await sql.execute(select(ancestry).order_by(ancestry.c.depth.desc())))
                .mappings()
                .all()
            )
            if not rows or len(rows) > MAX_MESSAGES:
                raise ValueError("原恢复父链缺失、循环或越界")
            path = [
                MessageView.model_validate(
                    {name: value for name, value in item.items() if name != "depth"}
                )
                for item in rows
            ]
            if any(message.topic_id != scope.topic_id for message in path):
                raise ValueError("原恢复父链跨话题")
            MessageTree(path)
            tail = incomplete_tail(path)
            if (
                tail is None
                or tail.run_id != record.request.source_run_id
                or path_fingerprint(path) != record.source.source_path_hash
                or self.repository.context_branch_key(path) != record.source.source_branch_key
            ):
                raise ValueError("恢复源路径或工具组被改变")
            prefix = path[: tail.group_index]
            if (prefix[-1].id if prefix else None) != record.source.anchor_message_id or [
                message.id for message in path[tail.group_index :]
            ] != record.source.excluded_message_ids:
                raise ValueError("恢复分支没有保留原完整前缀和工具组")
            MessageTree(path).require_complete_tools(prefix)
            pending = [tool_reference(call) for call in tail.pending]
            answered = [
                RecoveryAnsweredReference(
                    call_id=cast(str, message.details["call_id"]),
                    message_id=message.id,
                    message_hash=hashlib.sha256(
                        canonical(message.model_dump(mode="json"))
                    ).hexdigest(),
                )
                for message in tail.answered
            ]
            if (
                pending != record.source.pending_calls
                or answered != record.source.answered_calls
                or self.repository.context_branch_key([*prefix, MessageView.model_validate(row)])
                != record.recovered_branch_key
            ):
                raise ValueError("恢复调用引用或新分支与原消息不一致")
            return record
        except (TypeError, ValueError, KeyError, ServiceError):
            raise recovery_error("corrupt", "原恢复记录损坏，不能冒充成功或原键缺失") from None

    async def by_key(
        self, scope: ConversationRecoveryScope, request_key: str
    ) -> ConversationRecoveryRecord | None:
        # 原键也沿严格请求协议校验；不把非法键当缺失。
        try:
            ConversationRecoveryRequest(
                source_run_id="validation",
                expected_path_hash="0" * 64,
                expected_session_revision=None,
                request_key=request_key,
            )
        except (ValueError, TypeError):
            raise recovery_error("parameters", "恢复原键不符合协议", 422) from None
        async with self.database.write_lock, self.database.sessions() as sql:
            await self._guard(sql, scope)
            row = await sql.get(MessageRow, self.message_id(scope, request_key))
            value = await self._record(sql, row, scope) if row is not None else None
            await self._guard(sql, scope)
            return value

    async def apply(
        self,
        scope: ConversationRecoveryScope,
        request: ConversationRecoveryRequest,
        *,
        confirmed: bool = False,
    ) -> ConversationRecoveryRecord:
        if type(confirmed) is not bool:
            raise recovery_error("confirmation", "恢复确认必须是实际bool", 422)
        if not confirmed:
            raise recovery_error("confirmation", "恢复会改变活动上下文，须明确确认")
        try:
            request = ConversationRecoveryRequest.model_validate(request.model_dump(mode="json"))
        except (TypeError, ValueError, AttributeError):
            raise recovery_error("parameters", "恢复请求不符合严格协议", 422) from None
        identifier = self.message_id(scope, request.request_key)
        async with self.database.write_lock, self.database.sessions.begin() as sql:
            await self._guard(sql, scope, lock_topic=True)
            existing = await sql.get(MessageRow, identifier)
            if existing is not None:
                record = await self._record(sql, existing, scope)
                if record.request != request:
                    raise recovery_error("idempotency_conflict", "同一恢复键不能更换原参数")
                await self._guard(sql, scope)
                return record
            topic, session, messages, path, preview = await self._snapshot(sql, scope)
            if session is not None:
                await assert_session_open(sql, session)
            if not preview.available:
                raise recovery_error("not_required", "当前路径工具组完整，无需恢复")
            if (
                request.source_run_id,
                request.expected_path_hash,
                request.expected_session_revision,
            ) != (preview.source_run_id, preview.source_path_hash, preview.session_revision):
                raise recovery_error("conflict", "恢复源路径、Run或会话版本已经改变")
            prefix = path[: len(path) - len(preview.excluded_message_ids)]
            now = utc_now()
            provisional = MessageView(
                id=identifier,
                topic_id=scope.topic_id,
                run_id=request.source_run_id,
                sequence=max((message.sequence for message in messages), default=0) + 1,
                role="system",
                content=recovery_notice(preview),
                details={},
                parent_id=preview.anchor_message_id,
                created_at=now,
            )
            record = ConversationRecoveryRecord(
                recovery_message_id=identifier,
                request=request,
                source=preview,
                recovered_branch_key=self.repository.context_branch_key([*prefix, provisional]),
                session_revision=preview.session_revision + 1
                if preview.session_revision is not None
                else None,
                created_at=now,
            )
            provisional.details = {
                "origin": "host",
                "kind": "conversation_recovery",
                "record": record.model_dump(mode="json"),
                "request_hash": self._request_hash(scope, request),
            }
            row = MessageRow(**provisional.model_dump())
            sql.add(row)
            choices = {
                **topic.active_branches,
                preview.anchor_message_id or ROOT: identifier,
                identifier: None,
            }
            active = MessageTree([*messages, provisional]).active_path(choices)
            MessageTree([*messages, provisional]).require_complete_tools(active)
            topic.active_branches = choices
            topic.updated_at = now
            if session is not None:
                session.revision += 1
            await sql.flush()
            # 不跨投影复用Owner/租约/父会话观察；最后再取当前资格。
            sql.expire_all()
            _, current_session = await self._guard(sql, scope)
            if current_session is not None:
                await assert_session_open(sql, current_session)
                if current_session.revision != record.session_revision:
                    raise recovery_error("conflict", "恢复提交前会话版本已经改变")
            await sql.refresh(row)
            return await self._record(sql, row, scope)
