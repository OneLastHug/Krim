"""Agent 核心五表；Run与Eval各自终态/最终报告同事务。"""

from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    Float,
    ForeignKey,
    ForeignKeyConstraint,
    Integer,
    MetaData,
    String,
    Table,
    UniqueConstraint,
)

metadata = MetaData()
runs = Table(
    "runs",
    metadata,
    Column("id", String(36), primary_key=True),
    Column("request_key", String(128), unique=True, nullable=False),
    Column("request_hash", String(64), nullable=False),
    Column("request", JSON, nullable=False),
    Column("target", JSON, nullable=False),
    Column("status", String(16), nullable=False),
    Column("trace_id", String(32), nullable=False),
    Column("root_span_id", String(16), nullable=False),
    Column("created_at", Float, nullable=False),
    Column("finished_at", Float),
    Column("code", String(64)),
    Column("stop_requested", Boolean, nullable=False),
)
run_events = Table(
    "run_events",
    metadata,
    Column("run_id", String(36), ForeignKey("runs.id"), primary_key=True),
    Column("seq", Integer, primary_key=True),
    Column("event_id", String(64), nullable=False),
    Column("kind", String(32), nullable=False),
    Column("data", JSON, nullable=False),
    Column("at", Float, nullable=False),
    UniqueConstraint("run_id", "event_id"),
)

dataset_revisions = Table(
    "dataset_revisions",
    metadata,
    Column("name", String(64), primary_key=True),
    Column("version", String(64), primary_key=True),
    Column("fingerprint", String(64), nullable=False),
    Column("body", JSON, nullable=False),
    Column("created_at", Float, nullable=False),
)
evaluations = Table(
    "evaluations",
    metadata,
    Column("id", String(36), primary_key=True),
    Column("request_key", String(128), unique=True, nullable=False),
    Column("request_hash", String(64), nullable=False),
    Column("request_json", JSON, nullable=False),
    Column("dataset_name", String(64), nullable=False),
    Column("dataset_version", String(64), nullable=False),
    Column("dataset_fingerprint", String(64), nullable=False),
    Column("target", JSON, nullable=False),
    Column("rules", JSON, nullable=False),
    Column("execution", JSON, nullable=False),
    Column("status", String(16), nullable=False),
    Column("code", String(64)),
    Column("created_at", Float, nullable=False),
    Column("finished_at", Float),
    Column("stop_requested", Boolean, nullable=False),
    Column("revision", Integer, nullable=False),
    Column("planned_count", Integer, nullable=False),
    Column("report", JSON),
    Column("report_hash", String(64)),
    ForeignKeyConstraint(
        ["dataset_name", "dataset_version"], ["dataset_revisions.name", "dataset_revisions.version"]
    ),
)
eval_items = Table(
    "eval_items",
    metadata,
    Column("eval_id", String(36), ForeignKey("evaluations.id"), primary_key=True),
    Column("case_id", String(64), primary_key=True),
    Column("repetition", Integer, primary_key=True),
    Column("case_index", Integer, nullable=False),
    Column("run_request_key", String(128), nullable=False, unique=True),
    Column("run_id", String(36), ForeignKey("runs.id"), unique=True),
    Column("status", String(16), nullable=False),
    Column("code", String(64)),
    Column("created_at", Float, nullable=False),
    Column("finished_at", Float),
    Column("output", JSON),
    Column("scores", JSON),
    Column("quality", String(16)),
    Column("content_hash", String(64)),
)
