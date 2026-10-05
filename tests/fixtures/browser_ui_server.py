"""Playwright 的本机 Python 服务；临时 SQLite、合作式慢 fake 流与测试 Key。"""

from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Literal

import uvicorn
from pydantic import SecretStr

from krim_agent.api import create_app
from krim_agent.config import Settings
from krim_agent.persistence import Database, KrimRepository
from krim_agent.protocol.models import StrictModel
from krim_agent.providers import FakeProvider
from krim_agent.runtime import ChatRuntime
from krim_agent.tools.builtins import builtin_registry

with TemporaryDirectory(prefix="krim-browser-") as directory:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{Path(directory) / 'browser.sqlite'}",
        api_key=SecretStr("browser-test-key"),
        interactive_tools=True,
        run_timeout_seconds=120,
        context_tokenizer="cl100k_base",
        context_reserved_output_tokens=128,
        model_context_window_tokens=32768,
        model_framing_reserve_tokens=64,
    )
    runtime = ChatRuntime(
        KrimRepository(Database(settings.database_url)),
        FakeProvider(delay=0.04),
        builtin_registry(),
        settings,
    )
    app = create_app(runtime=runtime)

    class TestContextConfig(StrictModel):
        mode: Literal["automatic", "confirmation"] = "automatic"
        max_messages: int | None = None
        ttl_seconds: float = 300

    # 仅测试服务装配的可信配置入口；不进入产品 API 或分发包。
    @app.post("/_test/context-config")
    async def configure_context(payload: TestContextConfig) -> dict[str, str]:
        runtime.settings = Settings.model_validate(
            {
                **settings.model_dump(),
                "context_strategy": "annotated_compact"
                if payload.mode == "confirmation"
                else "sliding_window",
                "context_compact_mode": payload.mode,
                "context_max_messages": payload.max_messages,
                "context_proposal_ttl_seconds": payload.ttl_seconds,
            }
        )
        return {"mode": payload.mode}

    uvicorn.run(app, host="127.0.0.1", port=8876, log_level="error")
