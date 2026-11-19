"""单会话执行模式表：模式行、修改请求台账与本会话允许规则；复用核心metadata。"""

from sqlalchemy import (
    JSON,
    Column,
    Float,
    ForeignKey,
    Integer,
    String,
    Table,
    UniqueConstraint,
)

from .models import metadata

# 单会话执行模式 manual/auto/bypass：session_id 唯一（主键），不从项目或账号
# 动态继承；revision 是热切换 CAS 与执行前重验的版本锚点。
session_execution_policies = Table(
    "session_execution_policies",
    metadata,
    Column("session_id", String(36), ForeignKey("sessions.id"), primary_key=True),
    Column("mode", String(16), nullable=False),
    Column("revision", Integer, nullable=False),
    Column("updated_at", Float, nullable=False),
    Column("updated_by", String(64), nullable=False),
)

# 模式修改请求台账：(principal_id, request_key) 全局唯一。相同键重试返回已提交
# 响应而不重写状态；相同键不同内容或跨会话复用拒绝。不是单次批准账本。
session_execution_policy_requests = Table(
    "session_execution_policy_requests",
    metadata,
    Column("principal_id", String(64), primary_key=True),
    Column("request_key", String(128), primary_key=True),
    Column("request_hash", String(64), nullable=False),
    Column("session_id", String(36), ForeignKey("sessions.id"), nullable=False),
    Column("result", JSON, nullable=False),
    Column("created_at", Float, nullable=False),
)

# "本会话允许此类操作"规则：与单次批准分开，仅按 tool_hash+参数digest 严格
# 全等匹配，不存在通配或前缀形态，任何Shell批准都不能泛化成全部Shell操作。
# 规则授予时的项目上下文（project_id+membership revision）一并持久化：换绑
# 项目或ACL变化后旧规则不再匹配，须在新上下文重新授予。
session_allow_rules = Table(
    "session_allow_rules",
    metadata,
    Column("id", String(36), primary_key=True),
    Column("session_id", String(36), ForeignKey("sessions.id"), nullable=False),
    Column("project_id", String(36), nullable=False),
    Column("project_access_revision", Integer, nullable=False),
    Column("tool_name", String(128), nullable=False),
    Column("tool_hash", String(64), nullable=False),
    Column("arguments_hash", String(64), nullable=False),
    Column("created_at", Float, nullable=False),
    UniqueConstraint("session_id", "tool_hash", "arguments_hash"),
)

EXECUTION_POLICY_TABLES = (
    session_execution_policies,
    session_execution_policy_requests,
    session_allow_rules,
)
EXECUTION_POLICY_TABLE_NAMES = frozenset(table.name for table in EXECUTION_POLICY_TABLES)
