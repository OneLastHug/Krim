"""SQLAlchemy 2 异步 SQLite 与消息树；迁移和分支边界见 ADR-0005。"""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Literal, cast
from uuid import uuid4

from pydantic import JsonValue, TypeAdapter
from sqlalchemy import (
    JSON,
    Boolean,
    CheckConstraint,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    LargeBinary,
    String,
    Text,
    UniqueConstraint,
    event,
    func,
    select,
    update,
)
from sqlalchemy.engine import Connection, make_url
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from krim_agent.attachment import validate_attachment_metadata
from krim_agent.interactions import InteractionKind, InteractionView
from krim_agent.locale import LocaleView, resolve_locale
from krim_agent.prompt_context import (
    ContextTagInput,
    MentionInput,
    MentionOccurrence,
    MentionRef,
    MentionSnapshot,
    PromptContext,
    PromptSnapshot,
)
from krim_agent.prompt_nodes import (
    PromptMentionNode,
    PromptSkillNode,
    prompt_node_mentions,
    validate_prompt_nodes,
)
from krim_agent.protocol.domain import (
    TERMINAL_STATUSES,
    ChatRequest,
    EditRequest,
    MessageTreeView,
    MessageView,
    RegenerateRequest,
    RunStatus,
    RunView,
    ServiceError,
    StreamEventView,
    TopicView,
)
from krim_agent.protocol.models import StreamFrame, utc_now
from krim_agent.skills import (
    SkillBatch,
    SkillCatalog,
    SkillCatalogView,
    SkillDefinition,
    SkillReadResult,
    SkillSnapshot,
)
from krim_agent.tools.core import ToolAudit
from krim_agent.usage import RunUsageView, usage_from_events

from .migration import upgrade_database
from .tree import ROOT, MessageTree


class Base(DeclarativeBase):
    pass


class TopicRow(Base):
    __tablename__ = "topics"
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    title: Mapped[str] = mapped_column(String(120))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)
    active_branches: Mapped[dict[str, str | None]] = mapped_column(JSON, default=dict)
    prompt_context: Mapped[dict[str, JsonValue]] = mapped_column(JSON, default=dict)
    locale: Mapped[str] = mapped_column(String(16), default="zh-CN")
    skills: Mapped[dict[str, JsonValue]] = mapped_column(JSON, default=dict)


class RunRow(Base):
    __tablename__ = "runs"
    __table_args__ = (UniqueConstraint("topic_id", "idempotency_key"),)
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    topic_id: Mapped[str] = mapped_column(ForeignKey("topics.id"), index=True)
    input_message_id: Mapped[str] = mapped_column(String(36))
    idempotency_key: Mapped[str] = mapped_column(String(128))
    input_hash: Mapped[str] = mapped_column(String(64))
    status: Mapped[str] = mapped_column(String(16), default="queued")
    stop_requested: Mapped[bool] = mapped_column(Boolean, default=False)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    action: Mapped[str] = mapped_column(String(16), default="send")
    source_message_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    error_code: Mapped[str | None] = mapped_column(String(64), nullable=True)


class MessageRow(Base):
    __tablename__ = "messages"
    __table_args__ = (UniqueConstraint("topic_id", "sequence"),)
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    topic_id: Mapped[str] = mapped_column(ForeignKey("topics.id"), index=True)
    run_id: Mapped[str] = mapped_column(ForeignKey("runs.id"), index=True)
    sequence: Mapped[int] = mapped_column(Integer)
    role: Mapped[str] = mapped_column(String(16))
    content: Mapped[str] = mapped_column(Text)
    details: Mapped[dict[str, JsonValue]] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)
    parent_id: Mapped[str | None] = mapped_column(ForeignKey("messages.id"), index=True)


class ToolAuditRow(Base):
    __tablename__ = "tool_audits"
    call_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    run_id: Mapped[str] = mapped_column(ForeignKey("runs.id"), primary_key=True, index=True)
    payload: Mapped[dict[str, JsonValue]] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)


class RunEventRow(Base):
    __tablename__ = "run_events"
    run_id: Mapped[str] = mapped_column(ForeignKey("runs.id"), primary_key=True, index=True)
    seq: Mapped[int] = mapped_column(Integer, primary_key=True)
    kind: Mapped[Literal["start", "chunk", "end", "error"]] = mapped_column(String(16))
    data: Mapped[JsonValue] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)


class OAuthTokenRow(Base):
    __tablename__ = "oauth_tokens"
    __table_args__ = (
        CheckConstraint("revision >= 0", name="ck_oauth_revision"),
        CheckConstraint(
            "(lease_owner IS NULL AND lease_expires_at IS NULL) OR "
            "(lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL)",
            name="ck_oauth_lease",
        ),
    )
    scope_hash: Mapped[str] = mapped_column(String(64), primary_key=True)
    subject: Mapped[str] = mapped_column(String(256), primary_key=True)
    server: Mapped[str] = mapped_column(String(32))
    ciphertext: Mapped[bytes | None] = mapped_column(LargeBinary, nullable=True)
    revision: Mapped[int] = mapped_column(Integer, default=0)
    lease_owner: Mapped[str | None] = mapped_column(String(36), nullable=True)
    lease_expires_at: Mapped[float | None] = mapped_column(Float, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)


class OAuthStateRow(Base):
    __tablename__ = "oauth_states"
    __table_args__ = (
        CheckConstraint(
            "status IN ('pending','consumed','cancelled','expired','invalidated')",
            name="ck_oauth_state_status",
        ),
        CheckConstraint(
            "(status = 'pending' AND ciphertext IS NOT NULL AND ended_at IS NULL) OR "
            "(status != 'pending' AND ciphertext IS NULL AND ended_at IS NOT NULL)",
            name="ck_oauth_state_payload",
        ),
        CheckConstraint("expires_at > created_at", name="ck_oauth_state_time"),
    )
    state_hash: Mapped[str] = mapped_column(String(64), primary_key=True)
    subject: Mapped[str] = mapped_column(String(256), index=True)
    scope_hash: Mapped[str] = mapped_column(String(64))
    ciphertext: Mapped[bytes | None] = mapped_column(LargeBinary, nullable=True)
    status: Mapped[str] = mapped_column(String(16))
    created_at: Mapped[float] = mapped_column(Float)
    expires_at: Mapped[float] = mapped_column(Float, index=True)
    ended_at: Mapped[float | None] = mapped_column(Float, nullable=True)


class MCPResourceProfileRow(Base):
    __tablename__ = "mcp_resource_profiles"
    __table_args__ = (
        CheckConstraint(
            "revision >= 1 AND revision <= 9007199254740991", name="ck_mcp_profile_revision"
        ),
    )
    scope: Mapped[str] = mapped_column(String(64), primary_key=True)
    name: Mapped[str] = mapped_column(String(32), primary_key=True)
    revision: Mapped[int] = mapped_column(Integer)
    resource_version: Mapped[str] = mapped_column(String(128))
    resource_hash: Mapped[str] = mapped_column(String(64))
    enabled: Mapped[bool] = mapped_column(Boolean)
    public_metadata: Mapped[dict[str, JsonValue]] = mapped_column(JSON)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)


class OAuthClientRegistrationRow(Base):
    __tablename__ = "oauth_client_registrations"
    __table_args__ = (
        CheckConstraint("revision >= 0", name="ck_oauth_client_revision"),
        CheckConstraint(
            "(lease_owner IS NULL AND lease_expires_at IS NULL) OR "
            "(lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL)",
            name="ck_oauth_client_lease",
        ),
    )
    binding_hash: Mapped[str] = mapped_column(String(64), primary_key=True)
    subject: Mapped[str] = mapped_column(String(256))
    server: Mapped[str] = mapped_column(String(32))
    ciphertext: Mapped[bytes | None] = mapped_column(LargeBinary, nullable=True)
    revision: Mapped[int] = mapped_column(Integer, default=0)
    lease_owner: Mapped[str | None] = mapped_column(String(36), nullable=True)
    lease_expires_at: Mapped[float | None] = mapped_column(Float, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)


class OAuthBrowserCallbackRow(Base):
    __tablename__ = "oauth_browser_callbacks"
    __table_args__ = (
        CheckConstraint(
            "phase IN ('pending','exchanging','complete','failed','cancelled','expired')",
            name="ck_oauth_browser_phase",
        ),
        CheckConstraint("expires_at > created_at", name="ck_oauth_browser_ttl"),
        CheckConstraint(
            "(phase IN ('pending','exchanging') AND ciphertext IS NOT NULL) OR "
            "(phase NOT IN ('pending','exchanging') AND ciphertext IS NULL)",
            name="ck_oauth_browser_intent",
        ),
    )
    attempt_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    state_hash: Mapped[str] = mapped_column(String(64), unique=True)
    scope: Mapped[str] = mapped_column(String(64))
    name: Mapped[str] = mapped_column(String(32))
    subject: Mapped[str] = mapped_column(String(256))
    resource_hash: Mapped[str] = mapped_column(String(64))
    browser_hash: Mapped[str] = mapped_column(String(64))
    ciphertext: Mapped[bytes | None] = mapped_column(LargeBinary, nullable=True)
    phase: Mapped[str] = mapped_column(String(16))
    owner_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    error_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    created_at: Mapped[float] = mapped_column(Float)
    expires_at: Mapped[float] = mapped_column(Float, index=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)


class WorkflowCheckpointRow(Base):
    __tablename__ = "workflow_checkpoints"
    __table_args__ = (
        CheckConstraint("revision >= 0", name="ck_workflow_revision"),
        CheckConstraint(
            "(owner_id IS NULL AND lease_expires_at IS NULL) OR "
            "(owner_id IS NOT NULL AND lease_expires_at IS NOT NULL)",
            name="ck_workflow_lease",
        ),
    )
    run_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    scope: Mapped[str] = mapped_column(String(128), index=True)
    graph_hash: Mapped[str] = mapped_column(String(64))
    status: Mapped[str] = mapped_column(String(16), index=True)
    payload: Mapped[dict[str, JsonValue]] = mapped_column(JSON)
    revision: Mapped[int] = mapped_column(Integer, default=0)
    owner_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    lease_expires_at: Mapped[float | None] = mapped_column(Float, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)


class ContextSummaryRow(Base):
    __tablename__ = "context_summaries"
    __table_args__ = (UniqueConstraint("topic_id", "branch_key"),)
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    topic_id: Mapped[str] = mapped_column(ForeignKey("topics.id"), index=True)
    branch_key: Mapped[str] = mapped_column(String(64))
    source_until_message_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    source_message_ids: Mapped[list[JsonValue]] = mapped_column(JSON)
    summary: Mapped[str] = mapped_column(Text)
    strategy: Mapped[str] = mapped_column(String(32))
    policy: Mapped[str] = mapped_column(String(16))
    never_update: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)


class ContextProposalRow(Base):
    __tablename__ = "context_compact_proposals"
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    topic_id: Mapped[str] = mapped_column(ForeignKey("topics.id"), index=True)
    run_id: Mapped[str | None] = mapped_column(ForeignKey("runs.id"), nullable=True, index=True)
    step: Mapped[int | None] = mapped_column(Integer, nullable=True)
    branch_key: Mapped[str] = mapped_column(String(64))
    snapshot_hash: Mapped[str] = mapped_column(String(64))
    prior_summary_hash: Mapped[str] = mapped_column(String(64))
    config_hash: Mapped[str] = mapped_column(String(64))
    payload: Mapped[dict[str, JsonValue]] = mapped_column(JSON)
    status: Mapped[str] = mapped_column(String(16), default="proposed")
    revision: Mapped[int] = mapped_column(Integer, default=0)
    expires_at: Mapped[float] = mapped_column(Float)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)


class InteractionRow(Base):
    __tablename__ = "interaction_requests"
    __table_args__ = (UniqueConstraint("run_id", "call_id", "kind", "arguments_hash"),)
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    topic_id: Mapped[str] = mapped_column(ForeignKey("topics.id"), index=True)
    run_id: Mapped[str] = mapped_column(ForeignKey("runs.id"), index=True)
    call_id: Mapped[str] = mapped_column(String(128))
    tool_name: Mapped[str] = mapped_column(String(64))
    kind: Mapped[str] = mapped_column(String(16))
    arguments_hash: Mapped[str] = mapped_column(String(64))
    payload: Mapped[dict[str, JsonValue]] = mapped_column(JSON)
    status: Mapped[str] = mapped_column(String(16), default="pending")
    response: Mapped[dict[str, JsonValue] | None] = mapped_column(JSON, nullable=True)
    expires_at: Mapped[float] = mapped_column(Float)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)


class WorkflowRequestRow(Base):
    __tablename__ = "workflow_requests"
    scope: Mapped[str] = mapped_column(String(128), primary_key=True)
    idempotency_key: Mapped[str] = mapped_column(String(128), primary_key=True)
    request_hash: Mapped[str] = mapped_column(String(64))
    run_id: Mapped[str] = mapped_column(ForeignKey("workflow_checkpoints.run_id"), unique=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)


class ChangeListRow(Base):
    __tablename__ = "change_lists"
    topic_id: Mapped[str] = mapped_column(ForeignKey("topics.id"), primary_key=True)
    revision: Mapped[int] = mapped_column(Integer, default=0)
    items: Mapped[list[JsonValue]] = mapped_column(JSON, default=list)
    active_operation_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)


class ChangeOperationRow(Base):
    __tablename__ = "change_operations"
    __table_args__ = (UniqueConstraint("topic_id", "idempotency_key"),)
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    topic_id: Mapped[str] = mapped_column(ForeignKey("topics.id"), index=True)
    idempotency_key: Mapped[str] = mapped_column(String(128))
    request_hash: Mapped[str] = mapped_column(String(64))
    execution: Mapped[dict[str, JsonValue]] = mapped_column(JSON)
    status: Mapped[str] = mapped_column(String(16))
    receipt: Mapped[dict[str, JsonValue] | None] = mapped_column(JSON, nullable=True)
    receipt_history: Mapped[list[JsonValue]] = mapped_column(JSON, default=list)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)


class HostCheckpointTopicRow(Base):
    __tablename__ = "host_checkpoint_topics"
    topic_id: Mapped[str] = mapped_column(ForeignKey("topics.id"), primary_key=True)
    revision: Mapped[int] = mapped_column(Integer, default=0)
    active_operation_id: Mapped[str | None] = mapped_column(String(36), nullable=True)


class HostCheckpointRow(Base):
    __tablename__ = "host_checkpoints"
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    topic_id: Mapped[str] = mapped_column(ForeignKey("topics.id"), index=True)
    title: Mapped[str] = mapped_column(String(120))
    snapshot_metadata: Mapped[dict[str, JsonValue]] = mapped_column("metadata", JSON)
    snapshot_ref: Mapped[JsonValue] = mapped_column(JSON, nullable=True)
    status: Mapped[str] = mapped_column(String(16))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)


class HostCheckpointOperationRow(Base):
    __tablename__ = "host_checkpoint_operations"
    __table_args__ = (UniqueConstraint("topic_id", "idempotency_key"),)
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    topic_id: Mapped[str] = mapped_column(ForeignKey("topics.id"), index=True)
    checkpoint_id: Mapped[str] = mapped_column(ForeignKey("host_checkpoints.id"))
    idempotency_key: Mapped[str] = mapped_column(String(128))
    request_hash: Mapped[str] = mapped_column(String(64))
    execution: Mapped[dict[str, JsonValue]] = mapped_column(JSON)
    status: Mapped[str] = mapped_column(String(16))
    receipt: Mapped[dict[str, JsonValue] | None] = mapped_column(JSON, nullable=True)
    receipt_history: Mapped[list[JsonValue]] = mapped_column(JSON, default=list)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)


class AttachmentRow(Base):
    __tablename__ = "attachments"
    __table_args__ = (UniqueConstraint("topic_id", "idempotency_key"),)
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    topic_id: Mapped[str] = mapped_column(ForeignKey("topics.id"), index=True)
    filename: Mapped[str] = mapped_column(String(120))
    media_type: Mapped[str] = mapped_column(String(120))
    byte_size: Mapped[int] = mapped_column(Integer)
    sha256: Mapped[str] = mapped_column(String(64))
    content: Mapped[bytes] = mapped_column(LargeBinary)
    idempotency_key: Mapped[str] = mapped_column(String(128))
    request_hash: Mapped[str] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)


class AgentHandleRow(Base):
    __tablename__ = "agent_handles"
    agent_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    name: Mapped[str] = mapped_column(String(64))
    topic_id: Mapped[str] = mapped_column(ForeignKey("topics.id"), index=True)
    persistent: Mapped[bool] = mapped_column(Boolean)
    turn: Mapped[int] = mapped_column(Integer, default=0)
    current_run_id: Mapped[str | None] = mapped_column(ForeignKey("runs.id"), nullable=True)
    status: Mapped[str] = mapped_column(String(16))
    owner_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    revision: Mapped[int] = mapped_column(Integer, default=0)
    lease_expires_at: Mapped[float | None] = mapped_column(Float, nullable=True)
    definition_hash: Mapped[str] = mapped_column(String(64))
    parent_agent_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    parent_tool_call_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    profile_revision: Mapped[int | None] = mapped_column(Integer, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)


class AgentSessionRow(Base):
    """一个 Agent 实例对应一个稳定 session；会话账本不保存 callable 或密钥。"""

    __tablename__ = "agent_sessions"
    session_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    agent_id: Mapped[str] = mapped_column(String(36), unique=True)
    topic_id: Mapped[str] = mapped_column(ForeignKey("topics.id"), unique=True)
    name: Mapped[str] = mapped_column(String(64))
    persistent: Mapped[bool] = mapped_column(Boolean)
    profile_revision: Mapped[int | None] = mapped_column(Integer, nullable=True)
    definition_hash: Mapped[str] = mapped_column(String(64))
    parent_session_id: Mapped[str | None] = mapped_column(String(36), nullable=True, index=True)
    status: Mapped[str] = mapped_column(String(16), default="open")
    revision: Mapped[int] = mapped_column(Integer, default=0)
    owner_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    expires_at: Mapped[float | None] = mapped_column(Float, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)
    closed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    permissions: Mapped[dict[str, JsonValue]] = mapped_column(JSON, default=dict)
    turn: Mapped[int] = mapped_column(Integer, default=0)
    current_run_id: Mapped[str | None] = mapped_column(ForeignKey("runs.id"), nullable=True)


class AgentSessionTurnRow(Base):
    __tablename__ = "agent_session_turns"
    __table_args__ = (UniqueConstraint("session_id", "turn"),)
    session_id: Mapped[str] = mapped_column(
        ForeignKey("agent_sessions.session_id"), primary_key=True
    )
    turn: Mapped[int] = mapped_column(Integer, primary_key=True)
    agent_id: Mapped[str] = mapped_column(String(36))
    run_id: Mapped[str] = mapped_column(ForeignKey("runs.id"), unique=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)


class AgentTaskHeadRow(Base):
    __tablename__ = "agent_task_heads"
    scope_id: Mapped[str] = mapped_column(String(80), primary_key=True)
    kind: Mapped[str] = mapped_column(String(16), primary_key=True)
    revision: Mapped[int] = mapped_column(Integer, default=0)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)


class AgentTaskItemRow(Base):
    __tablename__ = "agent_task_items"
    scope_id: Mapped[str] = mapped_column(String(80), primary_key=True)
    kind: Mapped[str] = mapped_column(String(16), primary_key=True)
    item_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    position: Mapped[int] = mapped_column(Integer)
    content: Mapped[str] = mapped_column(String(200))
    completed: Mapped[bool] = mapped_column(Boolean)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)


class AgentTaskOperationRow(Base):
    __tablename__ = "agent_task_operations"
    scope_id: Mapped[str] = mapped_column(String(80), primary_key=True)
    kind: Mapped[str] = mapped_column(String(16), primary_key=True)
    request_key: Mapped[str] = mapped_column(String(128), primary_key=True)
    request_hash: Mapped[str] = mapped_column(String(64))
    result: Mapped[dict[str, JsonValue]] = mapped_column(JSON)
    revision: Mapped[int] = mapped_column(Integer)
    operation: Mapped[str] = mapped_column(String(16))
    run_id: Mapped[str | None] = mapped_column(ForeignKey("runs.id"), nullable=True)
    call_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    tool_request_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)


class WorkspaceEditProposalRow(Base):
    __tablename__ = "workspace_edit_proposals"
    __table_args__ = (UniqueConstraint("scope_id", "request_key"),)
    proposal_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    scope_id: Mapped[str] = mapped_column(String(80), index=True)
    topic_id: Mapped[str] = mapped_column(ForeignKey("topics.id"), index=True)
    request_key: Mapped[str] = mapped_column(String(128))
    request_hash: Mapped[str] = mapped_column(String(64))
    resource_fingerprint: Mapped[str] = mapped_column(String(64))
    workspace_key: Mapped[str] = mapped_column(String(64))
    path: Mapped[str] = mapped_column(String(512))
    prepared: Mapped[dict[str, JsonValue]] = mapped_column(JSON)
    preview: Mapped[dict[str, JsonValue]] = mapped_column(JSON)
    before_content: Mapped[bytes | None] = mapped_column(LargeBinary, nullable=True)
    after_content: Mapped[bytes | None] = mapped_column(LargeBinary, nullable=True)
    status: Mapped[str] = mapped_column(String(16), default="proposed")
    revision: Mapped[int] = mapped_column(Integer, default=0)
    expires_at: Mapped[float] = mapped_column(Float)
    active_operation_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)


class WorkspaceEditAttemptRow(Base):
    __tablename__ = "workspace_edit_attempts"
    __table_args__ = (UniqueConstraint("scope_id", "request_key"),)
    operation_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    proposal_id: Mapped[str] = mapped_column(
        ForeignKey("workspace_edit_proposals.proposal_id"), index=True
    )
    scope_id: Mapped[str] = mapped_column(String(80))
    request_key: Mapped[str] = mapped_column(String(128))
    request_hash: Mapped[str] = mapped_column(String(64))
    action: Mapped[str] = mapped_column(String(16))
    expected_revision: Mapped[int] = mapped_column(Integer)
    previous_status: Mapped[str] = mapped_column(String(16))
    status: Mapped[str] = mapped_column(String(16), default="running")
    owner_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    lease_expires_at: Mapped[float | None] = mapped_column(Float, nullable=True)
    prepared: Mapped[dict[str, JsonValue]] = mapped_column(JSON)
    before_content: Mapped[bytes | None] = mapped_column(LargeBinary, nullable=True)
    after_content: Mapped[bytes | None] = mapped_column(LargeBinary, nullable=True)
    receipt: Mapped[dict[str, JsonValue] | None] = mapped_column(JSON, nullable=True)
    recovery: Mapped[dict[str, JsonValue] | None] = mapped_column(JSON, nullable=True)
    keep_witness: Mapped[dict[str, JsonValue] | None] = mapped_column(
        JSON(none_as_null=True), nullable=True
    )
    cleanup_pending: Mapped[bool] = mapped_column(Boolean, default=False)
    run_id: Mapped[str | None] = mapped_column(ForeignKey("runs.id"), nullable=True)
    call_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    receipt_history: Mapped[list[JsonValue]] = mapped_column(JSON, default=list)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)


class WorkspaceFileEffectRow(Base):
    __tablename__ = "workspace_file_effects"
    __table_args__ = (
        CheckConstraint(
            "effect_sequence >= 1 AND effect_sequence <= 9007199254740991",
            name="ck_workspace_effect_sequence",
        ),
        CheckConstraint("action IN ('apply','undo')", name="ck_workspace_effect_action"),
        {"sqlite_autoincrement": True},
    )
    effect_sequence: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    operation_id: Mapped[str] = mapped_column(
        ForeignKey("workspace_edit_attempts.operation_id"), unique=True
    )
    proposal_id: Mapped[str] = mapped_column(ForeignKey("workspace_edit_proposals.proposal_id"))
    scope_id: Mapped[str] = mapped_column(String(80))
    resource_fingerprint: Mapped[str] = mapped_column(String(64))
    file_key: Mapped[str] = mapped_column(String(64), index=True)
    action: Mapped[str] = mapped_column(String(16))
    observed_after: Mapped[dict[str, JsonValue] | None] = mapped_column(
        JSON(none_as_null=True), nullable=True
    )


class WorkspaceFileLockRow(Base):
    __tablename__ = "workspace_file_locks"
    file_key: Mapped[str] = mapped_column(String(64), primary_key=True)
    operation_id: Mapped[str] = mapped_column(
        ForeignKey("workspace_edit_attempts.operation_id"), unique=True
    )


class WorkspaceDirectoryProposalRow(Base):
    __tablename__ = "workspace_directory_proposals"
    __table_args__ = (UniqueConstraint("scope_id", "request_key"),)
    proposal_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    scope_id: Mapped[str] = mapped_column(String(80), index=True)
    topic_id: Mapped[str] = mapped_column(ForeignKey("topics.id"), index=True)
    request_key: Mapped[str] = mapped_column(String(128))
    request_hash: Mapped[str] = mapped_column(String(64))
    resource_fingerprint: Mapped[str] = mapped_column(String(64))
    workspace_key: Mapped[str] = mapped_column(String(64))
    path: Mapped[str] = mapped_column(String(512))
    prepared: Mapped[dict[str, JsonValue]] = mapped_column(JSON)
    status: Mapped[str] = mapped_column(String(16), default="proposed")
    revision: Mapped[int] = mapped_column(Integer, default=0)
    expires_at: Mapped[float] = mapped_column(Float)
    active_operation_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)


class WorkspaceDirectoryAttemptRow(Base):
    __tablename__ = "workspace_directory_attempts"
    __table_args__ = (UniqueConstraint("scope_id", "request_key"),)
    operation_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    proposal_id: Mapped[str] = mapped_column(
        ForeignKey("workspace_directory_proposals.proposal_id"), index=True
    )
    scope_id: Mapped[str] = mapped_column(String(80))
    request_key: Mapped[str] = mapped_column(String(128))
    request_hash: Mapped[str] = mapped_column(String(64))
    action: Mapped[str] = mapped_column(String(16))
    expected_revision: Mapped[int] = mapped_column(Integer)
    previous_status: Mapped[str] = mapped_column(String(16))
    status: Mapped[str] = mapped_column(String(16), default="running")
    owner_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    lease_expires_at: Mapped[float | None] = mapped_column(Float, nullable=True)
    prepared: Mapped[dict[str, JsonValue]] = mapped_column(JSON)
    observed_after: Mapped[dict[str, JsonValue] | None] = mapped_column(JSON, nullable=True)
    receipt: Mapped[dict[str, JsonValue] | None] = mapped_column(JSON, nullable=True)
    recovery: Mapped[dict[str, JsonValue] | None] = mapped_column(JSON, nullable=True)
    cleanup_pending: Mapped[bool] = mapped_column(Boolean, default=False)
    run_id: Mapped[str | None] = mapped_column(ForeignKey("runs.id"), nullable=True)
    call_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    receipt_history: Mapped[list[JsonValue]] = mapped_column(JSON, default=list)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)


class WorkspaceDirectoryLockRow(Base):
    __tablename__ = "workspace_directory_locks"
    lock_key: Mapped[str] = mapped_column(String(64), primary_key=True)
    operation_id: Mapped[str] = mapped_column(
        ForeignKey("workspace_directory_attempts.operation_id"), unique=True
    )
    directory_key: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)


class CommandDirectoryProposalRow(Base):
    __tablename__ = "command_directory_proposals"
    __table_args__ = (
        CheckConstraint("mode >= 0 AND mode <= 511", name="ck_command_directory_mode"),
        CheckConstraint("accept_failed IN (0,1)", name="ck_command_directory_accept"),
        CheckConstraint(
            "ttl_seconds > 0 AND ttl_seconds <= 86400", name="ck_command_directory_ttl"
        ),
    )
    proposal_id: Mapped[str] = mapped_column(
        ForeignKey("workspace_directory_proposals.proposal_id"), primary_key=True
    )
    command_id: Mapped[str] = mapped_column(ForeignKey("command_executions.command_id"), index=True)
    source_hash: Mapped[str] = mapped_column(String(64))
    path: Mapped[str] = mapped_column(String(512))
    mode: Mapped[int] = mapped_column(Integer)
    entry_key: Mapped[str] = mapped_column(String(64))
    ttl_seconds: Mapped[float] = mapped_column(Float)
    accept_failed: Mapped[int] = mapped_column(Integer)


class NativeWorkspaceBatchRow(Base):
    __tablename__ = "native_workspace_batches"
    __table_args__ = (
        UniqueConstraint("scope_id", "request_key"),
        CheckConstraint("action IN ('keep','undo')", name="ck_native_batch_action"),
        CheckConstraint(
            "producer_status IN ('running','finished','cancelled','interrupted')",
            name="ck_native_batch_producer_status",
        ),
    )
    batch_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    scope_id: Mapped[str] = mapped_column(String(80), index=True)
    resource_fingerprint: Mapped[str] = mapped_column(String(64))
    workspace_key: Mapped[str] = mapped_column(String(64))
    request_key: Mapped[str] = mapped_column(String(128))
    request_hash: Mapped[str] = mapped_column(String(64))
    snapshot_revision: Mapped[str] = mapped_column(String(64))
    action: Mapped[str] = mapped_column(String(16))
    owner_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    lease_expires_at: Mapped[float | None] = mapped_column(Float, nullable=True)
    token: Mapped[str] = mapped_column(String(64))
    producer_status: Mapped[str] = mapped_column(String(16), default="running")
    stop_reason: Mapped[str | None] = mapped_column(String(128), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)


class NativeWorkspaceBatchChildRow(Base):
    __tablename__ = "native_workspace_batch_children"
    __table_args__ = (
        UniqueConstraint("batch_id", "proposal_id"),
        CheckConstraint("position >= 0 AND position < 32", name="ck_native_batch_position"),
        CheckConstraint(
            "status IN ('pending','rejected','skipped')", name="ck_native_batch_child_status"
        ),
    )
    batch_id: Mapped[str] = mapped_column(
        ForeignKey("native_workspace_batches.batch_id"), primary_key=True
    )
    position: Mapped[int] = mapped_column(Integer, primary_key=True)
    proposal_id: Mapped[str] = mapped_column(ForeignKey("workspace_edit_proposals.proposal_id"))
    expected_revision: Mapped[int] = mapped_column(Integer)
    path: Mapped[str] = mapped_column(String(512))
    file_key: Mapped[str] = mapped_column(String(64))
    apply_sequence: Mapped[int | None] = mapped_column(Integer, nullable=True)
    request_key: Mapped[str] = mapped_column(String(128), unique=True)
    operation_id: Mapped[str | None] = mapped_column(
        ForeignKey("workspace_edit_attempts.operation_id"), unique=True, nullable=True
    )
    admission_open: Mapped[bool] = mapped_column(Boolean, default=True)
    status: Mapped[str] = mapped_column(String(16), default="pending")
    error_code: Mapped[str | None] = mapped_column(String(128), nullable=True)


class NativeWorkspaceCheckpointRow(Base):
    __tablename__ = "native_workspace_checkpoints"
    __table_args__ = (
        UniqueConstraint("scope_id", "request_key"),
        CheckConstraint(
            "status IN ('capturing','available','failed','cancelled','interrupted')",
            name="ck_native_checkpoint_status",
        ),
        CheckConstraint(
            "(status = 'capturing' AND owner_id IS NOT NULL AND lease_expires_at IS NOT NULL) "
            "OR (status != 'capturing' AND owner_id IS NULL AND lease_expires_at IS NULL)",
            name="ck_native_checkpoint_owner",
        ),
    )
    checkpoint_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    scope_id: Mapped[str] = mapped_column(String(80), index=True)
    topic_id: Mapped[str] = mapped_column(ForeignKey("topics.id"))
    workspace_key: Mapped[str] = mapped_column(String(64))
    resource_fingerprint: Mapped[str] = mapped_column(String(64))
    workspace_fingerprint: Mapped[str] = mapped_column(String(64))
    request_key: Mapped[str] = mapped_column(String(128))
    request_hash: Mapped[str] = mapped_column(String(64))
    title: Mapped[str] = mapped_column(String(120))
    status: Mapped[str] = mapped_column(String(16))
    owner_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    lease_expires_at: Mapped[float | None] = mapped_column(Float, nullable=True)
    token: Mapped[str] = mapped_column(String(64))
    error_code: Mapped[str | None] = mapped_column(String(128), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)


class NativeWorkspaceCheckpointFileRow(Base):
    __tablename__ = "native_workspace_checkpoint_files"
    __table_args__ = (
        UniqueConstraint("checkpoint_id", "path"),
        CheckConstraint("position >= 0 AND position < 32", name="ck_native_checkpoint_position"),
        CheckConstraint(
            "(captured = 1 AND capture_metadata IS NOT NULL) "
            "OR (captured = 0 AND capture_metadata IS NULL AND content IS NULL)",
            name="ck_native_checkpoint_capture",
        ),
    )
    checkpoint_id: Mapped[str] = mapped_column(
        ForeignKey("native_workspace_checkpoints.checkpoint_id"), primary_key=True
    )
    position: Mapped[int] = mapped_column(Integer, primary_key=True)
    path: Mapped[str] = mapped_column(String(512))
    captured: Mapped[bool] = mapped_column(Boolean, default=False)
    capture_metadata: Mapped[dict[str, JsonValue] | None] = mapped_column(
        JSON(none_as_null=True), nullable=True
    )
    content: Mapped[bytes | None] = mapped_column(LargeBinary, nullable=True)


class WorkspaceRestorePlanRow(Base):
    __tablename__ = "workspace_restore_plans"
    __table_args__ = (
        UniqueConstraint("scope_id", "request_key"),
        UniqueConstraint("scope_id", "execute_key"),
        CheckConstraint(
            "phase IN ('preparing','ready','executing','finished','failed',"
            "'cancelled','interrupted','expired')",
            name="ck_restore_phase",
        ),
        CheckConstraint(
            "(phase IN ('preparing','executing') AND owner_id IS NOT NULL "
            "AND lease_expires_at IS NOT NULL) OR (phase NOT IN ('preparing','executing') "
            "AND owner_id IS NULL AND lease_expires_at IS NULL)",
            name="ck_restore_owner",
        ),
        CheckConstraint(
            "revision >= 0 AND revision <= 9007199254740991", name="ck_restore_revision"
        ),
    )
    plan_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    scope_id: Mapped[str] = mapped_column(String(80), index=True)
    topic_id: Mapped[str] = mapped_column(ForeignKey("topics.id"))
    workspace_key: Mapped[str] = mapped_column(String(64))
    resource_fingerprint: Mapped[str] = mapped_column(String(64))
    workspace_fingerprint: Mapped[str] = mapped_column(String(64))
    checkpoint_id: Mapped[str] = mapped_column(
        ForeignKey("native_workspace_checkpoints.checkpoint_id")
    )
    baseline_checkpoint_id: Mapped[str | None] = mapped_column(
        ForeignKey("native_workspace_checkpoints.checkpoint_id"), nullable=True
    )
    baseline_key: Mapped[str] = mapped_column(String(128), unique=True)
    request_key: Mapped[str] = mapped_column(String(128))
    request_hash: Mapped[str] = mapped_column(String(64))
    execute_key: Mapped[str | None] = mapped_column(String(128), nullable=True)
    execute_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    phase: Mapped[str] = mapped_column(String(16))
    revision: Mapped[int] = mapped_column(Integer, default=0)
    expires_at: Mapped[float] = mapped_column(Float)
    owner_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    lease_expires_at: Mapped[float | None] = mapped_column(Float, nullable=True)
    token: Mapped[str] = mapped_column(String(64))
    error_code: Mapped[str | None] = mapped_column(String(128), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)


class WorkspaceRestoreItemRow(Base):
    __tablename__ = "workspace_restore_items"
    __table_args__ = (
        UniqueConstraint("plan_id", "path"),
        CheckConstraint("position >= 0 AND position < 32", name="ck_restore_position"),
        CheckConstraint(
            "status IN ('pending','rejected','skipped')", name="ck_restore_item_status"
        ),
    )
    plan_id: Mapped[str] = mapped_column(
        ForeignKey("workspace_restore_plans.plan_id"), primary_key=True
    )
    position: Mapped[int] = mapped_column(Integer, primary_key=True)
    path: Mapped[str] = mapped_column(String(512))
    proposal_id: Mapped[str | None] = mapped_column(
        ForeignKey("workspace_edit_proposals.proposal_id"), unique=True, nullable=True
    )
    operation_id: Mapped[str | None] = mapped_column(
        ForeignKey("workspace_edit_attempts.operation_id"), unique=True, nullable=True
    )
    expected_revision: Mapped[int] = mapped_column(Integer, default=0)
    file_key: Mapped[str | None] = mapped_column(String(64), nullable=True)
    effect_sequence: Mapped[int | None] = mapped_column(Integer, nullable=True)
    request_key: Mapped[str] = mapped_column(String(128), unique=True)
    admission_open: Mapped[bool] = mapped_column(Boolean, default=False)
    status: Mapped[str] = mapped_column(String(16), default="pending")
    current_mode: Mapped[int | None] = mapped_column(Integer, nullable=True)
    target_mode: Mapped[int | None] = mapped_column(Integer, nullable=True)
    error_code: Mapped[str | None] = mapped_column(String(128), nullable=True)


class WorkspaceLedgerGuardRow(Base):
    __tablename__ = "workspace_ledger_guards"
    guard_id: Mapped[str] = mapped_column(String(16), primary_key=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)


class UsageBudgetAccountRow(Base):
    __tablename__ = "usage_budget_accounts"
    __table_args__ = (UniqueConstraint("owner_kind", "owner_id"),)
    account_id: Mapped[str] = mapped_column(String(160), primary_key=True)
    owner_kind: Mapped[str] = mapped_column(String(16))
    owner_id: Mapped[str] = mapped_column(String(128))
    config: Mapped[dict[str, JsonValue]] = mapped_column(JSON)
    revision: Mapped[int] = mapped_column(Integer, default=0)
    spent_tokens: Mapped[int] = mapped_column(Integer, default=0)
    spent_cost_micros: Mapped[int] = mapped_column(Integer, default=0)
    closed: Mapped[bool] = mapped_column(Boolean, default=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)


class UsageBudgetRequestRow(Base):
    __tablename__ = "usage_budget_requests"
    __table_args__ = (
        UniqueConstraint("run_id", "step"),
        CheckConstraint("step >= 0 AND step <= 31", name="ck_usage_budget_request_step"),
    )
    request_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    run_id: Mapped[str] = mapped_column(ForeignKey("runs.id"), index=True)
    step: Mapped[int] = mapped_column(Integer)
    request_hash: Mapped[str] = mapped_column(String(64))
    state: Mapped[str] = mapped_column(String(16))
    revision: Mapped[int] = mapped_column(Integer, default=0)
    input_tokens: Mapped[int] = mapped_column(Integer)
    output_reserve: Mapped[int] = mapped_column(Integer)
    reported_usage: Mapped[dict[str, JsonValue] | None] = mapped_column(JSON, nullable=True)
    source: Mapped[str | None] = mapped_column(String(24), nullable=True)
    receipt: Mapped[str | None] = mapped_column(String(256), nullable=True)
    overdrawn: Mapped[bool] = mapped_column(Boolean, default=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)


class UsageBudgetAllocationRow(Base):
    __tablename__ = "usage_budget_allocations"
    request_id: Mapped[str] = mapped_column(
        ForeignKey("usage_budget_requests.request_id"), primary_key=True
    )
    account_id: Mapped[str] = mapped_column(
        ForeignKey("usage_budget_accounts.account_id"), primary_key=True
    )
    reserved_tokens: Mapped[int] = mapped_column(Integer)
    reserved_cost_micros: Mapped[int | None] = mapped_column(Integer, nullable=True)
    charged_tokens: Mapped[int | None] = mapped_column(Integer, nullable=True)
    charged_cost_micros: Mapped[int | None] = mapped_column(Integer, nullable=True)
    price: Mapped[dict[str, JsonValue] | None] = mapped_column(JSON, nullable=True)


class UsageBudgetBindingRow(Base):
    __tablename__ = "usage_budget_bindings"
    owner_kind: Mapped[str] = mapped_column(String(16), primary_key=True)
    owner_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    config_hash: Mapped[str] = mapped_column(String(64))
    parents: Mapped[list[JsonValue]] = mapped_column(JSON)
    account_ids: Mapped[list[JsonValue]] = mapped_column(JSON)


class TraceSpanRow(Base):
    __tablename__ = "trace_spans"
    span_id: Mapped[str] = mapped_column(String(16), primary_key=True)
    trace_id: Mapped[str] = mapped_column(String(32), index=True)
    owner_kind: Mapped[str] = mapped_column(String(16))
    owner_ref: Mapped[str] = mapped_column(String(64), index=True)
    payload: Mapped[dict[str, JsonValue]] = mapped_column(JSON)


class Database:
    def __init__(self, url: str) -> None:
        parsed = make_url(url)
        if parsed.drivername != "sqlite+aiosqlite":
            raise ValueError("M1 只支持 sqlite+aiosqlite")
        self.is_persistent = bool(
            parsed.database
            and parsed.database != ":memory:"
            and parsed.query.get("mode") != "memory"
        )
        if parsed.database and parsed.database != ":memory:":
            Path(parsed.database).parent.mkdir(parents=True, exist_ok=True)
        self.engine = create_async_engine(url, hide_parameters=True)

        @event.listens_for(self.engine.sync_engine, "connect")
        def foreign_keys(connection: Any, _record: Any) -> None:
            # 明确由 SQLAlchemy 开始事务，使 SQLite DDL 也能完整回滚。
            connection.isolation_level = None
            cursor = connection.cursor()
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()

        @event.listens_for(self.engine.sync_engine, "begin")
        def begin(connection: Connection) -> None:
            connection.exec_driver_sql("BEGIN")

        self.sessions = async_sessionmaker(self.engine, expire_on_commit=False)
        self.write_lock = asyncio.Lock()

    async def initialize(self) -> None:
        async with self.engine.begin() as connection:
            await connection.run_sync(upgrade_database)

    async def close(self) -> None:
        await self.engine.dispose()


class KrimRepository:
    def __init__(self, database: Database) -> None:
        self.database = database

    async def create_topic(self, title: str, locale: str = "zh-CN") -> TopicView:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            resolved = resolve_locale(locale)
            row = TopicRow(id=str(uuid4()), title=title, locale=resolved.effective)
            session.add(row)
            await session.flush()
            return TopicView.model_validate(row)

    async def set_topic_locale(self, topic_id: str, requested: str) -> LocaleView:
        resolved = resolve_locale(requested)
        async with self.database.write_lock, self.database.sessions.begin() as session:
            topic = await session.get(TopicRow, topic_id)
            if topic is None:
                raise ServiceError("topic_not_found", "话题不存在", 404)
            topic.locale = resolved.effective
            topic.updated_at = utc_now()
            return resolved

    async def get_topic(self, topic_id: str) -> TopicView:
        async with self.database.write_lock, self.database.sessions() as session:
            row = await session.get(TopicRow, topic_id)
            if row is None:
                raise ServiceError("topic_not_found", "话题不存在", 404)
            return await self._topic_view(session, row)

    @staticmethod
    async def _topic_view(session: AsyncSession, row: TopicRow) -> TopicView:
        session_id = await session.scalar(
            select(AgentSessionRow.session_id).where(AgentSessionRow.topic_id == row.id)
        )
        return TopicView.model_validate(row).model_copy(update={"agent_session_id": session_id})

    async def list_topics(self) -> list[TopicView]:
        async with self.database.write_lock, self.database.sessions() as session:
            rows = await session.scalars(
                select(TopicRow).order_by(TopicRow.created_at, TopicRow.id)
            )
            agents = {
                topic_id: session_id
                for topic_id, session_id in (
                    await session.execute(
                        select(AgentSessionRow.topic_id, AgentSessionRow.session_id)
                    )
                ).all()
            }
            return [
                TopicView.model_validate(row).model_copy(
                    update={"agent_session_id": agents.get(row.id)}
                )
                for row in rows
            ]

    async def get_prompt_context(self, topic_id: str) -> PromptContext:
        async with self.database.write_lock, self.database.sessions() as session:
            topic = await session.get(TopicRow, topic_id)
            if topic is None:
                raise ServiceError("topic_not_found", "话题不存在", 404)
            return PromptContext.model_validate(topic.prompt_context)

    async def skill_catalog(self, topic_id: str) -> SkillCatalogView:
        async with self.database.write_lock, self.database.sessions() as session:
            topic = await session.get(TopicRow, topic_id)
            if topic is None:
                raise ServiceError("topic_not_found", "话题不存在", 404)
            return SkillCatalog.model_validate(topic.skills).view()

    async def inject_skills(self, topic_id: str, request: SkillBatch) -> SkillCatalogView:
        request = SkillBatch.model_validate(request.model_dump(mode="json"))
        async with self.database.write_lock, self.database.sessions.begin() as session:
            topic = await session.get(TopicRow, topic_id)
            if topic is None:
                raise ServiceError("topic_not_found", "话题不存在", 404)
            current = {
                skill.name: skill for skill in SkillCatalog.model_validate(topic.skills).skills
            }
            current.update({skill.name: skill.model_copy(deep=True) for skill in request.skills})
            try:
                catalog = SkillCatalog(skills=list(current.values()))
            except ValueError as error:
                raise ServiceError("skill_limit", "技能目录超过容量或大小边界", 422) from error
            topic.skills = TypeAdapter(dict[str, JsonValue]).validate_python(
                catalog.model_dump(mode="json")
            )
            topic.updated_at = utc_now()
            return catalog.view()

    async def import_skill_package(
        self, topic_id: str, definition: SkillDefinition, *, expected_digest: str | None = None
    ) -> SkillCatalogView:
        """包导入使用同名内容比较/原digest更新；不影响既有Run快照。"""
        definition = SkillDefinition.model_validate(definition.model_dump(mode="json"))
        async with self.database.write_lock, self.database.sessions.begin() as session:
            topic = await session.get(TopicRow, topic_id)
            if topic is None:
                raise ServiceError("topic_not_found", "话题不存在", 404)
            current = {
                skill.name: skill for skill in SkillCatalog.model_validate(topic.skills).skills
            }
            previous = current.get(definition.name)

            def comparable(skill: SkillDefinition) -> dict[str, JsonValue]:
                value = TypeAdapter(dict[str, JsonValue]).validate_python(
                    skill.model_dump(mode="json")
                )
                value["metadata"] = {
                    key: item
                    for key, item in skill.metadata.items()
                    if key != "krim-package-source"
                }
                return value

            if previous is not None:
                if comparable(previous) == comparable(definition):
                    return SkillCatalog(skills=list(current.values())).view()
                if expected_digest != previous.summary().digest:
                    raise ServiceError(
                        "skill_package_conflict", "同名技能已存在，须提供原digest明确替换", 409
                    )
            elif expected_digest is not None:
                raise ServiceError("skill_package_conflict", "预期旧技能不存在", 409)
            current[definition.name] = definition
            try:
                catalog = SkillCatalog(skills=list(current.values()))
            except ValueError:
                raise ServiceError("skill_limit", "技能目录超过容量或大小边界", 422) from None
            topic.skills = TypeAdapter(dict[str, JsonValue]).validate_python(
                catalog.model_dump(mode="json")
            )
            topic.updated_at = utc_now()
            return catalog.view()

    async def remove_skill(self, topic_id: str, name: str | None = None) -> SkillCatalogView:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            topic = await session.get(TopicRow, topic_id)
            if topic is None:
                raise ServiceError("topic_not_found", "话题不存在", 404)
            catalog = SkillCatalog.model_validate(topic.skills)
            if name is not None and not any(skill.name == name for skill in catalog.skills):
                raise ServiceError("skill_not_found", "技能不在该话题目录中", 404)
            catalog = SkillCatalog(
                skills=[skill for skill in catalog.skills if skill.name != name] if name else []
            )
            topic.skills = TypeAdapter(dict[str, JsonValue]).validate_python(
                catalog.model_dump(mode="json")
            )
            topic.updated_at = utc_now()
            return catalog.view()

    async def read_skill(
        self, topic_id: str, name: str, resource: str | None = None
    ) -> SkillReadResult:
        async with self.database.write_lock, self.database.sessions() as session:
            topic = await session.get(TopicRow, topic_id)
            if topic is None:
                raise ServiceError("topic_not_found", "话题不存在", 404)
            try:
                return SkillCatalog.model_validate(topic.skills).read(name, resource)
            except KeyError as error:
                raise ServiceError(str(error.args[0]), "技能或资源不在该话题目录中", 404) from error

    async def run_skill_snapshot(self, run_id: str) -> SkillSnapshot:
        async with self.database.write_lock, self.database.sessions() as session:
            run = await session.get(RunRow, run_id)
            if run is None:
                raise ServiceError("run_not_found", "运行不存在", 404)
            message = await session.get(MessageRow, run.input_message_id)
            if message is None or message.topic_id != run.topic_id:
                raise ServiceError("run_input_invalid", "运行输入消息不可用", 409)
            return SkillSnapshot.model_validate(message.details.get("skills_snapshot", {}))

    async def add_mention(self, topic_id: str, request: MentionInput) -> MentionRef:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            topic = await session.get(TopicRow, topic_id)
            if topic is None:
                raise ServiceError("topic_not_found", "话题不存在", 404)
            try:
                attachment_identifier = validate_attachment_metadata(request.metadata)
            except ValueError as error:
                raise ServiceError("attachment_reference_invalid", str(error), 422) from error
            if attachment_identifier is not None:
                attachment = await session.get(AttachmentRow, attachment_identifier)
                if attachment is None or attachment.topic_id != topic_id:
                    raise ServiceError("attachment_not_found", "附件不在该话题中", 404)
                if request.kind != "file" or request.label != attachment.filename:
                    raise ServiceError(
                        "attachment_reference_invalid",
                        "附件引用必须绑定原始文件名和 file 类型",
                        422,
                    )
                expected_metadata = {
                    "attachment_id": attachment.id,
                    "filename": attachment.filename,
                    "media_type": attachment.media_type,
                    "size": attachment.byte_size,
                    "sha256": attachment.sha256,
                }
                if any(
                    key in request.metadata and request.metadata[key] != value
                    for key, value in expected_metadata.items()
                ):
                    raise ServiceError(
                        "attachment_reference_invalid", "附件引用元数据与实际资源不匹配", 422
                    )
                request = request.model_copy(
                    update={"metadata": {**request.metadata, **expected_metadata}}
                )
            context = PromptContext.model_validate(topic.prompt_context)
            reference = MentionRef(id=str(uuid4()), **request.model_dump())
            context.mentions.append(reference)
            await self._save_prompt_context(topic, context)
            return reference

    async def remove_mention(self, topic_id: str, mention_id: str | None = None) -> PromptContext:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            topic = await session.get(TopicRow, topic_id)
            if topic is None:
                raise ServiceError("topic_not_found", "话题不存在", 404)
            context = PromptContext.model_validate(topic.prompt_context)
            if mention_id is not None and not any(ref.id == mention_id for ref in context.mentions):
                raise ServiceError("mention_not_found", "引用不在该话题中", 404)
            context.mentions = (
                [ref for ref in context.mentions if ref.id != mention_id] if mention_id else []
            )
            await self._save_prompt_context(topic, context)
            return context

    async def add_context_tag(self, topic_id: str, request: ContextTagInput) -> PromptContext:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            topic = await session.get(TopicRow, topic_id)
            if topic is None:
                raise ServiceError("topic_not_found", "话题不存在", 404)
            context = PromptContext.model_validate(topic.prompt_context)
            context.context_tags = [tag for tag in context.context_tags if tag.name != request.name]
            context.context_tags.append(request.model_copy(deep=True))
            await self._save_prompt_context(topic, context)
            return context

    async def remove_context_tag(self, topic_id: str, name: str | None = None) -> PromptContext:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            topic = await session.get(TopicRow, topic_id)
            if topic is None:
                raise ServiceError("topic_not_found", "话题不存在", 404)
            context = PromptContext.model_validate(topic.prompt_context)
            if name is not None and not any(tag.name == name for tag in context.context_tags):
                raise ServiceError("context_tag_not_found", "背景标签不在该话题中", 404)
            context.context_tags = (
                [tag for tag in context.context_tags if tag.name != name] if name else []
            )
            await self._save_prompt_context(topic, context)
            return context

    @staticmethod
    async def _save_prompt_context(topic: TopicRow, context: PromptContext) -> None:
        try:
            checked = PromptContext.model_validate(context.model_dump(mode="json"))
        except ValueError as error:
            raise ServiceError("prompt_context_limit", "引用/背景标签超出目录边界", 422) from error
        topic.prompt_context = TypeAdapter(dict[str, JsonValue]).validate_python(
            checked.model_dump(mode="json")
        )
        topic.updated_at = utc_now()

    @staticmethod
    async def _prompt_snapshot(
        topic: TopicRow, text: str, occurrences: list[MentionOccurrence], session: AsyncSession
    ) -> PromptSnapshot:
        context = PromptContext.model_validate(topic.prompt_context)
        references = {ref.id: ref for ref in context.mentions}
        snapshots: list[MentionSnapshot] = []
        previous_end = 0
        for occurrence in sorted(occurrences, key=lambda item: (item.start, item.end)):
            reference = references.get(occurrence.mention_id)
            if reference is None:
                raise ServiceError("mention_not_found", "引用不在该话题中", 404)
            identifier = validate_attachment_metadata(reference.metadata)
            if identifier is not None:
                attachment = await session.get(AttachmentRow, identifier)
                if attachment is None or attachment.topic_id != topic.id:
                    raise ServiceError(
                        "attachment_not_found", "发送引用的附件已经删除或不属于当前话题", 404
                    )
            if occurrence.start < previous_end or occurrence.end > len(text):
                raise ServiceError("mention_position_invalid", "引用位置越界或重叠", 422)
            if text[occurrence.start : occurrence.end] != "@" + reference.label:
                raise ServiceError("mention_position_invalid", "引用位置不匹配正文标签", 422)
            previous_end = occurrence.end
            snapshots.append(
                MentionSnapshot(
                    **reference.model_dump(), start=occurrence.start, end=occurrence.end
                )
            )
        try:
            return PromptSnapshot(mentions=snapshots, context_tags=context.context_tags)
        except ValueError as error:
            raise ServiceError("prompt_context_limit", "发送引用/背景快照超过边界", 422) from error

    async def begin_run(
        self,
        topic_id: str,
        request: ChatRequest | EditRequest | RegenerateRequest,
        *,
        allow_new: bool = True,
        action: Literal["send", "edit", "regenerate"] = "send",
        source_message_id: str | None = None,
        agent_session_id: str | None = None,
        agent_id: str | None = None,
        agent_owner_id: str | None = None,
    ) -> tuple[RunView, bool]:
        text = "" if isinstance(request, RegenerateRequest) else request.text
        explicit_parent = request.parent_message_id if isinstance(request, ChatRequest) else None
        # 保留普通发送的 M1 幂等摘要；分支操作将动作与目标纳入摘要。
        fingerprint = (
            text
            if action == "send" and explicit_parent is None
            else json.dumps(
                {
                    "action": action,
                    "source": source_message_id,
                    "parent": explicit_parent,
                    "text": text,
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        occurrences = request.mentions if isinstance(request, (ChatRequest, EditRequest)) else []
        skill_names = request.skill_names if isinstance(request, (ChatRequest, EditRequest)) else []
        nodes = request.nodes if isinstance(request, (ChatRequest, EditRequest)) else []
        if nodes:
            try:
                validate_prompt_nodes(text, nodes)
            except ValueError as error:
                raise ServiceError("prompt_nodes_invalid", str(error), 422) from error
            if [item.model_dump(mode="json") for item in prompt_node_mentions(nodes)] != [
                item.model_dump(mode="json") for item in occurrences
            ]:
                raise ServiceError(
                    "prompt_nodes_invalid", "节点 Mention 与 occurrences 不一致", 422
                )
            node_skills = sorted(node.name for node in nodes if isinstance(node, PromptSkillNode))
            if node_skills != sorted(skill_names):
                raise ServiceError("prompt_nodes_invalid", "节点 Skill 与显式技能选择不一致", 422)
        if occurrences:
            fingerprint = json.dumps(
                {
                    "base": fingerprint,
                    "mentions": [item.model_dump(mode="json") for item in occurrences],
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        if skill_names:
            fingerprint = json.dumps(
                {"base": fingerprint, "skill_names": skill_names},
                ensure_ascii=False,
                sort_keys=True,
            )
        if nodes:
            fingerprint = json.dumps(
                {"base": fingerprint, "nodes": [item.model_dump(mode="json") for item in nodes]},
                ensure_ascii=False,
                sort_keys=True,
            )
        digest = hashlib.sha256(fingerprint.encode()).hexdigest()
        async with self.database.write_lock, self.database.sessions.begin() as session:
            from .agent_sessions import assert_session_open, guard_agent_topic

            topic = await session.get(TopicRow, topic_id)
            if topic is None:
                raise ServiceError("topic_not_found", "话题不存在", 404)
            agent_session = await guard_agent_topic(
                session, topic_id, agent_session_id, agent_id, agent_owner_id
            )
            existing = await session.scalar(
                select(RunRow).where(
                    RunRow.topic_id == topic_id, RunRow.idempotency_key == request.idempotency_key
                )
            )
            if existing is not None:
                if existing.input_hash != digest:
                    raise ServiceError("idempotency_conflict", "同一幂等键不能用于不同输入", 409)
                return RunView.model_validate(existing), False
            if agent_session is not None:
                await assert_session_open(session, agent_session)
                limit = agent_session.permissions.get("max_turns", 128)
                if isinstance(limit, int) and agent_session.turn >= limit:
                    raise ServiceError("max_turns", "Agent 超过会话轮次预算", 409)
            if nodes:
                context = PromptContext.model_validate(topic.prompt_context)
                references = {item.id: item for item in context.mentions}
                for node in nodes:
                    if isinstance(node, PromptMentionNode):
                        reference = references.get(node.mention_id)
                        if reference is None or reference.label != node.label:
                            raise ServiceError(
                                "prompt_nodes_invalid", "节点 Mention 不属于当前话题目录", 422
                            )
            if not allow_new:
                raise ServiceError("run_limit", "同时运行数量超过限制", 429)
            await self._require_idle(session, topic_id)
            messages = await self._messages(session, topic_id)
            tree = MessageTree(messages)
            path = tree.active_path(topic.active_branches)
            choices = dict(topic.active_branches)
            parent_id = path[-1].id if path else None
            input_id = str(uuid4())
            if action == "send" and explicit_parent is None:
                tree.require_complete_tools(path)
            if action == "edit":
                source = tree.nodes.get(source_message_id or "")
                if source is None:
                    raise ServiceError("message_not_found", "消息不在该话题中", 404)
                if source.role != "user":
                    raise ServiceError("message_not_editable", "只能编辑用户消息", 409)
                parent_id = source.parent_id
                choices = tree.select(source.id, choices)
            elif action == "regenerate":
                source = tree.user_ancestor(source_message_id or "")
                input_id = source.id
                choices = tree.select(source.id, choices)
                choices[source.id] = None
            elif explicit_parent is not None:
                source = tree.nodes.get(explicit_parent)
                await self._require_boundary(session, source)
                choices = tree.select(explicit_parent, choices)
                parent_id = explicit_parent
            run = RunRow(
                id=str(uuid4()),
                topic_id=topic_id,
                input_message_id=input_id,
                idempotency_key=request.idempotency_key,
                input_hash=digest,
                action=action,
                source_message_id=source_message_id,
            )
            session.add(run)
            await session.flush()
            if agent_session is not None:
                agent_session.turn += 1
                agent_session.current_run_id = run.id
                session.add(
                    AgentSessionTurnRow(
                        session_id=agent_session.session_id,
                        agent_id=agent_session.agent_id,
                        turn=agent_session.turn,
                        run_id=run.id,
                        created_at=utc_now(),
                    )
                )
            if action != "regenerate":
                snapshot = await self._prompt_snapshot(topic, text, occurrences, session)
                try:
                    skill_snapshot = SkillSnapshot.model_validate(
                        {**topic.skills, "selected_names": skill_names}
                    )
                except ValueError as error:
                    raise ServiceError(
                        "skill_selection_invalid", "本次技能选择不在目录中或重复", 422
                    ) from error
                input_details: dict[str, JsonValue] = (
                    {"edited_from": source_message_id} if action == "edit" else {}
                )
                if snapshot.mentions or snapshot.context_tags:
                    input_details["prompt_context"] = TypeAdapter(JsonValue).validate_python(
                        snapshot.model_dump(mode="json")
                    )
                if skill_snapshot.skills:
                    input_details["skills_snapshot"] = TypeAdapter(JsonValue).validate_python(
                        skill_snapshot.model_dump(mode="json")
                    )
                if nodes:
                    input_details["prompt_nodes"] = TypeAdapter(JsonValue).validate_python(
                        [item.model_dump(mode="json") for item in nodes]
                    )
                highest = max((message.sequence for message in messages), default=0)
                session.add(
                    MessageRow(
                        id=run.input_message_id,
                        topic_id=topic_id,
                        run_id=run.id,
                        sequence=highest + 1,
                        role="user",
                        content=text,
                        details=input_details,
                        parent_id=parent_id,
                    )
                )
                choices[parent_id or ROOT] = input_id
            topic.active_branches = choices
            topic.updated_at = utc_now()
            return RunView.model_validate(run), True

    async def _messages(self, session: AsyncSession, topic_id: str) -> list[MessageView]:
        rows = await session.scalars(
            select(MessageRow).where(MessageRow.topic_id == topic_id).order_by(MessageRow.sequence)
        )
        return [MessageView.model_validate(row) for row in rows]

    async def _require_idle(self, session: AsyncSession, topic_id: str) -> None:
        active = await session.scalar(
            select(RunRow.id).where(
                RunRow.topic_id == topic_id, RunRow.status.in_(("queued", "running"))
            )
        )
        if active:
            raise ServiceError("topic_busy", "话题已有活跃运行", 409)

    async def assert_topic_idle(self, topic_id: str) -> None:
        """在手动压缩前确认话题存在且没有 queued/running 运行。"""
        async with self.database.write_lock, self.database.sessions() as session:
            if await session.get(TopicRow, topic_id) is None:
                raise ServiceError("topic_not_found", "话题不存在", 404)
            await self._require_idle(session, topic_id)

    async def _require_boundary(self, session: AsyncSession, message: MessageView | None) -> None:
        if message is None:
            raise ServiceError("message_not_found", "消息不在该话题中", 404)
        if message.role == "tool" or message.details.get("tool_calls"):
            raise ServiceError("incomplete_tool_group", "不能从工具组中间切换或发送", 409)
        run = await session.get(RunRow, message.run_id)
        if run is None or run.status not in TERMINAL_STATUSES:
            raise ServiceError("message_not_ready", "消息所属运行尚未完成", 409)

    async def message_tree(self, topic_id: str) -> MessageTreeView:
        async with self.database.write_lock, self.database.sessions() as session:
            topic = await session.get(TopicRow, topic_id)
            if topic is None:
                raise ServiceError("topic_not_found", "话题不存在", 404)
            messages = await self._messages(session, topic_id)
            path = MessageTree(messages).active_path(topic.active_branches)
            return MessageTreeView(
                topic=await self._topic_view(session, topic),
                messages=messages,
                active_path=[message.id for message in path],
            )

    async def list_messages(self, topic_id: str) -> list[MessageView]:
        tree = await self.message_tree(topic_id)
        by_id = {message.id: message for message in tree.messages}
        return [by_id[message_id] for message_id in tree.active_path]

    @staticmethod
    def context_branch_key(messages: list[MessageView]) -> str:
        return hashlib.sha256(
            json.dumps([message.id for message in messages], separators=(",", ":")).encode()
        ).hexdigest()

    async def get_context_summary(self, topic_id: str, branch_key: str) -> ContextSummaryRow | None:
        async with self.database.write_lock, self.database.sessions() as session:
            return cast(
                ContextSummaryRow | None,
                await session.scalar(
                    select(ContextSummaryRow).where(
                        ContextSummaryRow.topic_id == topic_id,
                        ContextSummaryRow.branch_key == branch_key,
                    )
                ),
            )

    async def match_context_summary(
        self, topic_id: str, messages: list[MessageView]
    ) -> ContextSummaryRow | None:
        identifiers = [message.id for message in messages]
        async with self.database.write_lock, self.database.sessions() as session:
            rows = list(
                await session.scalars(
                    select(ContextSummaryRow).where(ContextSummaryRow.topic_id == topic_id)
                )
            )
            matches = [
                row
                for row in rows
                if row.source_message_ids
                and row.source_message_ids == identifiers[: len(row.source_message_ids)]
                and len(row.source_message_ids) < len(messages)
                and messages[len(row.source_message_ids)].role == "user"
                and row.source_until_message_id == row.source_message_ids[-1]
                and row.branch_key
                == self.context_branch_key(messages[: len(row.source_message_ids)])
            ]
            return max(matches, key=lambda row: len(row.source_message_ids), default=None)

    async def save_context_summary(
        self,
        topic_id: str,
        branch_key: str,
        source_message_ids: list[str],
        summary: str,
        strategy: str,
        policy: str,
        *,
        never_update: bool = False,
        require_idle: bool = False,
        expected_path_key: str | None = None,
    ) -> ContextSummaryRow:
        if not summary.strip() or len(summary.encode("utf-8")) > 65536:
            raise ServiceError("summary_invalid", "Context 摘要为空或超过大小限制", 400)
        if policy == "never":
            raise ServiceError("summary_policy_never", "never 策略不能保存新摘要", 400)
        if policy not in {"auto", "always"}:
            raise ServiceError("summary_policy_invalid", "摘要更新策略无效", 400)
        if strategy not in {"summarize_then_truncate", "annotated_compact"}:
            raise ServiceError("summary_strategy_invalid", "只有摘要策略可以保存历史摘要", 400)
        async with self.database.write_lock, self.database.sessions.begin() as session:
            topic = await session.get(TopicRow, topic_id)
            if topic is None:
                raise ServiceError("topic_not_found", "话题不存在", 404)
            if require_idle:
                await self._require_idle(session, topic_id)
            tree = MessageTree(await self._messages(session, topic_id))
            active = tree.active_path(topic.active_branches)
            if (
                expected_path_key is not None
                and self.context_branch_key(active) != expected_path_key
            ):
                raise ServiceError("summary_branch_changed", "摘要计算期间活动路径已经变化", 409)
            if not source_message_ids or source_message_ids != [
                message.id for message in active[: len(source_message_ids)]
            ]:
                raise ServiceError("summary_branch_changed", "摘要来源不属于当前活动路径前缀", 409)
            prefix = active[: len(source_message_ids)]
            tree.require_complete_tools(prefix)
            if (
                len(source_message_ids) >= len(active)
                or active[len(source_message_ids)].role != "user"
            ):
                raise ServiceError(
                    "summary_incomplete_group", "摘要不能拆开或省略最新完整消息组", 409
                )
            expected_key = self.context_branch_key(active[: len(source_message_ids)])
            if branch_key != expected_key:
                raise ServiceError("summary_source_mismatch", "摘要来源哈希不一致", 409)
            row = await session.scalar(
                select(ContextSummaryRow).where(
                    ContextSummaryRow.topic_id == topic_id,
                    ContextSummaryRow.branch_key == branch_key,
                )
            )
            if row is None:
                row = ContextSummaryRow(
                    id=str(uuid4()),
                    topic_id=topic_id,
                    branch_key=branch_key,
                    source_until_message_id=source_message_ids[-1] if source_message_ids else None,
                    source_message_ids=list(source_message_ids),
                    summary=summary,
                    strategy=strategy,
                    policy=policy,
                    never_update=never_update,
                )
                session.add(row)
            elif row.never_update:
                raise ServiceError("summary_frozen", "已有摘要禁止更新", 409)
            else:
                row.source_until_message_id = source_message_ids[-1] if source_message_ids else None
                row.source_message_ids = list(source_message_ids)
                row.summary, row.strategy, row.policy = summary, strategy, policy
                row.never_update = never_update
                row.updated_at = utc_now()
            await session.flush()
            return row

    async def create_interaction(
        self,
        topic_id: str,
        run_id: str,
        call_id: str,
        tool_name: str,
        kind: InteractionKind,
        arguments_hash: str,
        payload: dict[str, JsonValue],
        *,
        timeout_seconds: float = 300,
    ) -> InteractionView:
        if kind not in {"approval", "question"}:
            raise ServiceError("interaction_kind_invalid", "交互类型无效", 400)
        if not 0 < timeout_seconds <= 3600:
            raise ServiceError("interaction_timeout_invalid", "交互超时无效", 400)
        async with self.database.write_lock, self.database.sessions.begin() as session:
            topic = await session.get(TopicRow, topic_id)
            run = await session.get(RunRow, run_id)
            if topic is None or run is None or run.topic_id != topic_id:
                raise ServiceError("run_not_found", "交互所属运行不存在", 404)
            if run.status not in {"queued", "running"} or run.stop_requested:
                raise ServiceError("interaction_run_closed", "运行已结束或正在停止", 409)
            existing = await session.scalar(
                select(InteractionRow).where(
                    InteractionRow.run_id == run_id,
                    InteractionRow.call_id == call_id,
                    InteractionRow.kind == kind,
                    InteractionRow.arguments_hash == arguments_hash,
                )
            )
            if existing is not None:
                if existing.tool_name != tool_name or existing.payload != payload:
                    raise ServiceError("interaction_conflict", "相同交互来源不能改写请求", 409)
                if existing.status == "pending" and existing.expires_at <= time.time():
                    existing.status = "expired"
                    existing.updated_at = utc_now()
                return InteractionView.model_validate(existing)
            row = InteractionRow(
                id=str(uuid4()),
                topic_id=topic_id,
                run_id=run_id,
                call_id=call_id,
                tool_name=tool_name,
                kind=kind,
                arguments_hash=arguments_hash,
                payload=payload,
                status="pending",
                response=None,
                expires_at=time.time() + timeout_seconds,
            )
            session.add(row)
            await session.flush()
            return InteractionView.model_validate(row)

    async def list_interactions(
        self, run_id: str, *, pending_only: bool = False
    ) -> list[InteractionView]:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            run = await session.get(RunRow, run_id)
            if run is None:
                raise ServiceError("run_not_found", "运行不存在", 404)
            query = select(InteractionRow).where(InteractionRow.run_id == run_id)
            if pending_only:
                query = query.where(InteractionRow.status == "pending")
            rows = list(
                await session.scalars(query.order_by(InteractionRow.created_at, InteractionRow.id))
            )
            now = time.time()
            for row in rows:
                if row.status == "pending" and (
                    row.expires_at <= now or run.status in TERMINAL_STATUSES or run.stop_requested
                ):
                    row.status = "expired" if row.expires_at <= now else "cancelled"
                    row.updated_at = utc_now()
            return [
                InteractionView.model_validate(row)
                for row in rows
                if not pending_only or row.status == "pending"
            ]

    async def get_interaction(self, run_id: str, interaction_id: str) -> InteractionView:
        rows = await self.list_interactions(run_id)
        result = next((row for row in rows if row.id == interaction_id), None)
        if result is None:
            raise ServiceError("interaction_not_found", "交互请求不存在", 404)
        return result

    async def close_interaction(
        self, run_id: str, interaction_id: str, *, expired: bool = False
    ) -> None:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            row = await session.get(InteractionRow, interaction_id)
            if row is not None and row.run_id == run_id and row.status == "pending":
                row.status = "expired" if expired else "cancelled"
                row.updated_at = utc_now()

    async def reply_interaction(
        self,
        run_id: str,
        interaction_id: str,
        status: str,
        response: dict[str, JsonValue] | None,
    ) -> InteractionView:
        await self.get_interaction(run_id, interaction_id)
        if status not in {"approved", "denied", "answered", "cancelled"}:
            raise ServiceError("interaction_status_invalid", "交互回应状态无效", 400)
        async with self.database.write_lock, self.database.sessions.begin() as session:
            row = await session.get(InteractionRow, interaction_id)
            if row is None or row.run_id != run_id:
                raise ServiceError("interaction_not_found", "交互请求不存在", 404)
            if row.status != "pending":
                raise ServiceError("interaction_not_pending", "交互请求已经处理", 409)
            if row.expires_at <= time.time():
                raise ServiceError("interaction_expired", "交互请求已经过期", 409)
            run = await session.get(RunRow, run_id)
            if run is None or run.status in TERMINAL_STATUSES or run.stop_requested:
                raise ServiceError("interaction_run_closed", "交互所属运行已经关闭", 409)
            if (row.kind == "approval" and status == "answered") or (
                row.kind == "question" and status in {"approved", "denied"}
            ):
                raise ServiceError("interaction_reply_invalid", "回应与交互类型不匹配", 422)
            row.status = status
            row.response = response
            row.updated_at = utc_now()
            await session.flush()
            return InteractionView.model_validate(row)

    async def select_branch(self, topic_id: str, message_id: str) -> TopicView:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            topic = await session.get(TopicRow, topic_id)
            if topic is None:
                raise ServiceError("topic_not_found", "话题不存在", 404)
            await self._require_idle(session, topic_id)
            tree = MessageTree(await self._messages(session, topic_id))
            await self._require_boundary(session, tree.nodes.get(message_id))
            choices = tree.select(message_id, topic.active_branches)
            tree.active_path(choices)
            topic.active_branches = choices
            topic.updated_at = utc_now()
            return await self._topic_view(session, topic)

    async def add_message(
        self, run: RunView, role: str, content: str, details: dict[str, JsonValue] | None = None
    ) -> None:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            topic = await session.get(TopicRow, run.topic_id)
            if topic is None:
                raise ServiceError("topic_not_found", "话题不存在", 404)
            highest = await session.scalar(
                select(func.max(MessageRow.sequence)).where(MessageRow.topic_id == run.topic_id)
            )
            parent_id = (
                await session.scalar(
                    select(MessageRow.id)
                    .where(MessageRow.run_id == run.id)
                    .order_by(MessageRow.sequence.desc())
                    .limit(1)
                )
                or run.input_message_id
            )
            message_id = str(uuid4())
            payload = dict(details or {})
            if run.action == "regenerate":
                payload["regenerated_from"] = run.source_message_id
            session.add(
                MessageRow(
                    id=message_id,
                    topic_id=run.topic_id,
                    run_id=run.id,
                    sequence=(highest or 0) + 1,
                    role=role,
                    content=content,
                    details=payload,
                    parent_id=parent_id,
                )
            )
            topic.active_branches = {**topic.active_branches, parent_id: message_id}
            topic.updated_at = utc_now()

    async def get_run(self, run_id: str) -> RunView:
        async with self.database.write_lock, self.database.sessions() as session:
            row = await session.get(RunRow, run_id)
            if row is None:
                raise ServiceError("run_not_found", "运行不存在", 404)
            return RunView.model_validate(row)

    async def active_runs(self) -> list[RunView]:
        async with self.database.write_lock, self.database.sessions() as session:
            rows = await session.scalars(
                select(RunRow)
                .where(RunRow.status.in_(("queued", "running")))
                .order_by(RunRow.created_at, RunRow.id)
            )
            return [RunView.model_validate(row) for row in rows]

    async def run_messages(self, run_id: str) -> list[MessageView]:
        await self.get_run(run_id)
        async with self.database.write_lock, self.database.sessions() as session:
            rows = await session.scalars(
                select(MessageRow).where(MessageRow.run_id == run_id).order_by(MessageRow.sequence)
            )
            return [MessageView.model_validate(row) for row in rows]

    async def append_event(self, frame: StreamFrame) -> StreamFrame:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            run = await session.get(RunRow, frame.stream_id)
            if run is None:
                raise ServiceError("run_not_found", "运行不存在", 404)
            existing = await session.get(RunEventRow, (frame.stream_id, frame.seq))
            if existing is not None:
                current = StreamFrame(
                    stream_id=existing.run_id,
                    seq=existing.seq,
                    kind=existing.kind,
                    data=existing.data,
                    created_at=existing.created_at,
                )
                same = current.model_dump(mode="json") == frame.model_dump(mode="json")
                if frame.kind == "end":
                    current_data = current.data if isinstance(current.data, dict) else {}
                    frame_data = frame.data if isinstance(frame.data, dict) else {}
                    same = current.kind == frame.kind and {
                        key: value for key, value in current_data.items() if value is not None
                    } == {key: value for key, value in frame_data.items() if value is not None}
                if not same:
                    raise ServiceError("event_conflict", "流事件序号已经写入不同内容", 409)
                return current
            highest = await session.scalar(
                select(func.max(RunEventRow.seq)).where(RunEventRow.run_id == frame.stream_id)
            )
            if highest is not None and frame.seq != highest + 1:
                raise ServiceError("event_gap", "流事件序号不连续", 409)
            if highest is None and frame.seq != 0:
                raise ServiceError("event_gap", "流事件必须从序号 0 开始", 409)
            last_kind = await session.scalar(
                select(RunEventRow.kind)
                .where(RunEventRow.run_id == frame.stream_id)
                .order_by(RunEventRow.seq.desc())
                .limit(1)
            )
            if last_kind in {"end", "error"}:
                raise ServiceError("event_after_terminal", "终态帧之后不能追加流事件", 409)
            if frame.kind in {"end", "error"} and run.status not in TERMINAL_STATUSES:
                raise ServiceError("run_not_terminal", "必须先保存运行终态再发布终帧", 409)
            session.add(
                RunEventRow(
                    run_id=frame.stream_id,
                    seq=frame.seq,
                    kind=frame.kind,
                    data=frame.data,
                    created_at=frame.created_at,
                )
            )
            return frame

    async def list_events(self, run_id: str) -> list[StreamFrame]:
        await self.get_run(run_id)
        async with self.database.write_lock, self.database.sessions() as session:
            rows = await session.scalars(
                select(RunEventRow).where(RunEventRow.run_id == run_id).order_by(RunEventRow.seq)
            )
            return [
                StreamFrame(
                    stream_id=row.run_id,
                    seq=row.seq,
                    kind=row.kind,
                    data=row.data,
                    created_at=row.created_at,
                )
                for row in rows
            ]

    async def usage_view(self, run_id: str) -> RunUsageView:
        return usage_from_events(run_id, await self.list_events(run_id))

    async def event_page(self, run_id: str, from_seq: int, *, limit: int = 16) -> list[StreamFrame]:
        if from_seq < 0 or not 1 <= limit <= 64:
            raise ValueError("事件分页参数无效")
        await self.get_run(run_id)
        async with self.database.write_lock, self.database.sessions() as session:
            rows = await session.scalars(
                select(RunEventRow)
                .where(RunEventRow.run_id == run_id, RunEventRow.seq >= from_seq)
                .order_by(RunEventRow.seq)
                .limit(limit)
            )
            return [
                StreamFrame(
                    stream_id=row.run_id,
                    seq=row.seq,
                    kind=row.kind,
                    data=row.data,
                    created_at=row.created_at,
                )
                for row in rows
            ]

    async def event_summary(self, run_id: str) -> StreamEventView:
        await self.get_run(run_id)
        async with self.database.write_lock, self.database.sessions() as session:
            counts = (
                await session.execute(
                    select(
                        func.count(RunEventRow.seq),
                        func.min(RunEventRow.seq),
                        func.max(RunEventRow.seq),
                    ).where(RunEventRow.run_id == run_id)
                )
            ).one()
            last_kind = await session.scalar(
                select(RunEventRow.kind)
                .where(RunEventRow.run_id == run_id)
                .order_by(RunEventRow.seq.desc())
                .limit(1)
            )
        return StreamEventView(
            run_id=run_id,
            first_seq=counts[1] if counts[1] is not None else 0,
            last_seq=counts[2] if counts[2] is not None else -1,
            count=counts[0],
            terminal=last_kind in {"end", "error"},
            durable=self.database.is_persistent,
        )

    async def ensure_terminal_event(self, run_id: str) -> list[StreamFrame]:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            run = await session.get(RunRow, run_id)
            if run is None:
                raise ServiceError("run_not_found", "运行不存在", 404)
            rows = await session.scalars(
                select(RunEventRow).where(RunEventRow.run_id == run_id).order_by(RunEventRow.seq)
            )
            events = [
                StreamFrame(
                    stream_id=row.run_id,
                    seq=row.seq,
                    kind=row.kind,
                    data=row.data,
                    created_at=row.created_at,
                )
                for row in rows
            ]
            if (
                not events
                or events[-1].kind in {"end", "error"}
                or run.status not in TERMINAL_STATUSES
            ):
                return events
            frame = StreamFrame(
                stream_id=run.id,
                seq=events[-1].seq + 1,
                kind="end",
                data={"status": run.status, "error": run.error, "code": run.error_code},
            )
            session.add(
                RunEventRow(
                    run_id=run.id,
                    seq=frame.seq,
                    kind=frame.kind,
                    data=frame.data,
                    created_at=frame.created_at,
                )
            )
            return [*events, frame]

    async def set_running(self, run_id: str) -> None:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            await session.execute(
                update(RunRow)
                .where(RunRow.id == run_id)
                .values(status="running", updated_at=utc_now())
            )

    async def request_stop(self, run_id: str) -> None:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            await session.execute(
                update(RunRow)
                .where(RunRow.id == run_id, RunRow.status.not_in(TERMINAL_STATUSES))
                .values(stop_requested=True, updated_at=utc_now())
            )
            await session.execute(
                update(ContextProposalRow)
                .where(ContextProposalRow.run_id == run_id, ContextProposalRow.status == "proposed")
                .values(
                    status="cancelled",
                    revision=ContextProposalRow.revision + 1,
                    updated_at=utc_now(),
                )
            )

    async def finish(
        self,
        run_id: str,
        status: RunStatus,
        error: str | None,
        audits: list[ToolAudit],
        *,
        error_code: str | None = None,
    ) -> None:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            await session.execute(
                update(InteractionRow)
                .where(InteractionRow.run_id == run_id, InteractionRow.status == "pending")
                .values(status="cancelled", updated_at=utc_now())
            )
            await session.execute(
                update(ContextProposalRow)
                .where(ContextProposalRow.run_id == run_id, ContextProposalRow.status == "proposed")
                .values(
                    status="cancelled",
                    revision=ContextProposalRow.revision + 1,
                    updated_at=utc_now(),
                )
            )
            for audit in audits:
                session.add(
                    ToolAuditRow(
                        call_id=audit.call_id, run_id=run_id, payload=audit.model_dump(mode="json")
                    )
                )
            await session.execute(
                update(RunRow)
                .where(RunRow.id == run_id)
                .values(
                    status=status,
                    error=error,
                    error_code=error_code,
                    finished_at=utc_now(),
                    updated_at=utc_now(),
                )
            )

    async def audits(self, run_id: str) -> list[ToolAudit]:
        await self.get_run(run_id)
        async with self.database.write_lock, self.database.sessions() as session:
            rows = await session.scalars(select(ToolAuditRow).where(ToolAuditRow.run_id == run_id))
            return [ToolAudit.model_validate(row.payload) for row in rows]

    async def recover_interrupted(self) -> None:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            rows = list(
                await session.scalars(
                    select(RunRow).where(RunRow.status.in_(("queued", "running")))
                )
            )
            for row in rows:
                await session.execute(
                    update(ContextProposalRow)
                    .where(
                        ContextProposalRow.run_id == row.id, ContextProposalRow.status == "proposed"
                    )
                    .values(
                        status="cancelled",
                        revision=ContextProposalRow.revision + 1,
                        updated_at=utc_now(),
                    )
                )
                await session.execute(
                    update(InteractionRow)
                    .where(InteractionRow.run_id == row.id, InteractionRow.status == "pending")
                    .values(status="cancelled", updated_at=utc_now())
                )
                row.status = "interrupted"
                row.error = "服务重启；未自动重放工具"
                row.error_code = "interrupted"
                row.finished_at = utc_now()
                row.updated_at = utc_now()
                highest = await session.scalar(
                    select(func.max(RunEventRow.seq)).where(RunEventRow.run_id == row.id)
                )
                last_kind = await session.scalar(
                    select(RunEventRow.kind)
                    .where(RunEventRow.run_id == row.id)
                    .order_by(RunEventRow.seq.desc())
                    .limit(1)
                )
                if last_kind == "end":
                    continue
                session.add(
                    RunEventRow(
                        run_id=row.id,
                        seq=(highest + 1) if highest is not None else 0,
                        kind="end",
                        data={"status": "interrupted", "error": row.error, "code": "interrupted"},
                    )
                )
