"""Playwright 的本机 Python 服务；临时 SQLite、合作式慢 fake 流与测试 Key。"""

import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Literal

import uvicorn
from pydantic import Field, SecretStr

from krim_agent.agents import AgentDefinition
from krim_agent.api import create_app
from krim_agent.config import Settings
from krim_agent.persistence import Database, KrimRepository
from krim_agent.protocol.domain import ServiceError
from krim_agent.protocol.models import StrictModel
from krim_agent.providers import FakeProvider
from krim_agent.runtime import ChatRuntime
from krim_agent.tools.builtins import builtin_registry
from krim_agent.workspace import WORKSPACE_TOOL_NAMES

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
    workspace_root = Path(directory) / "workspace"
    (workspace_root / "src").mkdir(parents=True)
    (workspace_root / "src" / "main.py").write_text(
        "# 浏览器工作区\nneedle = 42\n", encoding="utf-8"
    )
    (workspace_root / ".env").write_text("hidden", encoding="utf-8")
    (workspace_root / "html.txt").write_text(
        '<img src=x onerror="window.workspaceExecuted=1">', encoding="utf-8"
    )
    app.state.agents.register_workspace("demo", workspace_root, version="browser-v1")
    write_root = Path(directory) / "write-workspace"
    write_root.mkdir()
    app.state.agents.register_workspace(
        "edit", write_root, version="browser-write-v1", writable=True
    )

    class TestWorkspaceControl(StrictModel):
        action: Literal["seed", "fail_save", "large"]
        path: str = Field(default="file.txt", pattern=r"^[A-Za-z0-9_-]+\.txt$")
        content: str = Field(default="old\n", max_length=24000)

    fail_next_save = False
    original_settle = app.state.agents.workspace_edits.settle

    async def controlled_settle(*args, **kwargs):
        global fail_next_save
        if fail_next_save:
            fail_next_save = False
            raise OSError("injected browser receipt save failure")
        return await original_settle(*args, **kwargs)

    app.state.agents.workspace_edits.settle = controlled_settle

    @app.post("/_test/workspace")
    async def configure_workspace(payload: TestWorkspaceControl) -> dict[str, str]:
        global fail_next_save
        if payload.action == "fail_save":
            fail_next_save = True
        else:
            content = "x" * 50000 if payload.action == "large" else payload.content
            await asyncio.to_thread(
                (write_root / payload.path).write_text, content, encoding="utf-8"
            )
        return {"action": payload.action}

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

    class TestAgentInventory(StrictModel):
        action: Literal["create", "register"]

    inventory_definition = AgentDefinition(
        name="restoreworker",
        provider=runtime.provider,
        tools=runtime.tools.select(
            [
                tool.name
                for tool in runtime.tools.definitions
                if tool.name not in WORKSPACE_TOOL_NAMES
            ]
        ),
        persistent=True,
        policy=runtime.tool_policy,
    )

    @app.post("/_test/agent-inventory")
    async def agent_inventory_setup(payload: TestAgentInventory) -> dict[str, str]:
        agents = app.state.agents
        if payload.action == "create":
            handle = await agents.manager.spawn(inventory_definition)
            identifier = handle.agent_id
            await handle.detach()
            try:
                await agents.restore(identifier)
            except ServiceError:
                pass
            return {"agent_id": identifier}
        if not any(item.name == "restoreworker" for item in agents.catalog.descriptors()):
            agents.catalog.register(
                "restoreworker", inventory_definition, description="恢复测试", expose_as_tool=False
            )
        return {"name": "restoreworker"}

    uvicorn.run(app, host="127.0.0.1", port=8876, log_level="error")
