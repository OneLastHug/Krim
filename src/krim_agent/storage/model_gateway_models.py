"""模型网关配置域表：当前文档、版本历史与请求幂等台账；复用核心 metadata。"""

from sqlalchemy import JSON, Column, Integer, String, Table

from .models import metadata

# 每账号一份当前文档；secret 只存保险库密文或空映射，绝不存明文密钥。
model_gateway_documents = Table(
    "model_gateway_documents",
    metadata,
    Column("principal_id", String(64), primary_key=True),
    Column("revision", Integer, nullable=False),
    Column("document", JSON, nullable=False),
    Column("secret", JSON, nullable=False),
)

# 每次更新追加一版，保留旧文档与旧密文，使按 revision 重建的会话仍可用。
model_gateway_revisions = Table(
    "model_gateway_revisions",
    metadata,
    Column("principal_id", String(64), primary_key=True),
    Column("revision", Integer, primary_key=True),
    Column("document", JSON, nullable=False),
    Column("secret", JSON, nullable=False),
)

# 更新请求台账：(principal_id, request_key) 唯一；相同键重试返回已提交版本。
model_gateway_requests = Table(
    "model_gateway_requests",
    metadata,
    Column("principal_id", String(64), primary_key=True),
    Column("request_key", String(128), primary_key=True),
    Column("request_hash", String(64), nullable=False),
    Column("revision", Integer, nullable=False),
)

MODEL_GATEWAY_TABLES = (
    model_gateway_documents,
    model_gateway_revisions,
    model_gateway_requests,
)
MODEL_GATEWAY_TABLE_NAMES = frozenset(table.name for table in MODEL_GATEWAY_TABLES)
