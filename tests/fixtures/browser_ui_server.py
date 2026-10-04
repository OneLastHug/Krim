"""Playwright 的本机 Python 服务；临时 SQLite、合作式慢 fake 流与测试 Key。"""

from pathlib import Path
from tempfile import TemporaryDirectory

import uvicorn
from pydantic import SecretStr

from krim_agent.api import create_app
from krim_agent.config import Settings
from krim_agent.persistence import Database, KrimRepository
from krim_agent.providers import FakeProvider
from krim_agent.runtime import ChatRuntime
from krim_agent.tools.builtins import builtin_registry

with TemporaryDirectory(prefix="krim-browser-") as directory:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{Path(directory) / 'browser.sqlite'}",
        api_key=SecretStr("browser-test-key"),
    )
    runtime = ChatRuntime(
        KrimRepository(Database(settings.database_url)),
        FakeProvider(delay=0.04),
        builtin_registry(),
        settings,
    )
    uvicorn.run(create_app(runtime=runtime), host="127.0.0.1", port=8876, log_level="error")
