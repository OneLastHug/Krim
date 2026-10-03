"""仅借用调用方的事务与连接；不读取环境密钥或建立额外连接。"""

from alembic import context
from sqlalchemy.engine import Connection

connection = context.config.attributes.get("connection")
if not isinstance(connection, Connection):
    raise RuntimeError("迁移必须提供受管理的数据库连接")
context.configure(connection=connection, render_as_batch=connection.dialect.name == "sqlite")
with context.begin_transaction():
    context.run_migrations()
