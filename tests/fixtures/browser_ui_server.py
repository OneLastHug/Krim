"""Playwright 的本机 Python 服务；临时 SQLite、合作式慢 fake 流与测试 Key。"""

import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Literal

import httpx
import uvicorn
from cryptography.fernet import Fernet
from mcp.types import CallToolResult, ListToolsResult, Tool
from pydantic import Field, SecretStr
from sqlalchemy import func, select

from krim_agent.agents import AgentDefinition
from krim_agent.api import create_app
from krim_agent.config import Settings
from krim_agent.execution import LinuxSandboxRunner
from krim_agent.execution.binding import COMMAND_TOOL_NAMES
from krim_agent.mcp import (
    MCPOAuthAuth,
    MCPServerConfig,
    OAuthConfig,
    OAuthHTTPRequester,
    PersistentEncryptedStateStore,
    PersistentEncryptedTokenStore,
    TrustedMCPServer,
)
from krim_agent.persistence import Database, KrimRepository
from krim_agent.persistence.database import (
    WorkspaceDirectoryAttemptRow,
    WorkspaceDirectoryProposalRow,
)
from krim_agent.protocol.domain import ServiceError
from krim_agent.protocol.models import StrictModel
from krim_agent.providers import FakeProvider
from krim_agent.runtime import ChatRuntime
from krim_agent.tools.builtins import builtin_registry
from krim_agent.tools.core import ToolContext, ToolDefinition
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
    batch_gates: dict[str, tuple[asyncio.Event, asyncio.Event]] = {}
    checkpoint_gates: dict[str, tuple[str, asyncio.Event, asyncio.Event]] = {}

    async def pause_native_checkpoint(definition: ToolDefinition, context: ToolContext) -> None:
        gate = checkpoint_gates.get(context.agent_id or "")
        if gate is None:
            return
        phase, entered, released = gate
        matches = (
            phase == "capture"
            and definition.name == "workspace_read"
            and context.checkpoint_capture is not None
        ) or (
            phase == "restore"
            and definition.name == "workspace_change"
            and context.checkpoint_restore is not None
        )
        if matches:
            entered.set()
            async with context.human_wait():
                await released.wait()

    async def pause_native_batch(definition: ToolDefinition, context: ToolContext) -> None:
        gate = batch_gates.get(context.agent_id or "")
        if definition.name == "workspace_change" and context.workspace_batch is not None and gate:
            entered, released = gate
            entered.set()
            # 可信测试暂停沿原Registry等待，取消仍由生产代码传播，不伪造文件回执。
            async with context.human_wait():
                await released.wait()

    runtime.tools.add_global_pre(pause_native_batch)
    runtime.tools.add_global_pre(pause_native_checkpoint)
    browser_oauth_key = Fernet.generate_key()
    app = create_app(runtime=runtime, oauth_browser_key=browser_oauth_key)
    mcp_counts: dict[str, int] = {}
    mcp_gates: dict[str, tuple[asyncio.Event, asyncio.Event]] = {}

    class BrowserMCPSession:
        def __init__(self, name: str) -> None:
            self.name = name

        async def initialize(self):
            mcp_counts[self.name] = mcp_counts.get(self.name, 0) + 1
            if self.name == "browser_fail_mcp":
                raise ValueError("private-browser-secret")
            gate = mcp_gates.get(self.name)
            if gate is not None:
                gate[0].set()
                await gate[1].wait()

        async def list_tools(self, *, params=None):
            return ListToolsResult(
                tools=[Tool(name="echo", inputSchema={"type": "object", "properties": {}})]
            )

        async def call_tool(self, name, arguments):
            return CallToolResult(content=[], structuredContent={"ok": True})

    def mcp_factory(name: str):
        async def factory():
            return BrowserMCPSession(name)

        return factory

    for name in ("browser_mcp", "browser_slow_mcp", "browser_fail_mcp"):
        app.state.mcp.register(
            TrustedMCPServer(
                MCPServerConfig(
                    name=name, transport="stdio", command="trusted", readonly_tools={"echo"}
                ),
                "browser-mcp-v1",
                display_name='<img src=x onerror="window.mcpExecuted=1">'
                if name == "browser_mcp"
                else name,
                session_factory=mcp_factory(name),
            )
        )

    oauth_tokens = PersistentEncryptedTokenStore(runtime.repository.database, browser_oauth_key)
    oauth_states = PersistentEncryptedStateStore(runtime.repository.database, browser_oauth_key)
    oauth_calls = 0

    async def browser_token_request(request):
        global oauth_calls
        oauth_calls += 1
        return httpx.Response(
            200,
            json={
                "access_token": "browser-oauth-access",
                "refresh_token": "browser-oauth-refresh",
                "expires_in": 600,
            },
        )

    oauth_requester = OAuthHTTPRequester(
        {"127.0.0.1", "mcp.example"}, transport=httpx.MockTransport(browser_token_request)
    )
    oauth_config = OAuthConfig(
        server="browser_oauth",
        client_id="browser-client",
        resource="https://mcp.example/mcp",
        authorization_endpoint="http://127.0.0.1:8876/_test/oauth/authorize",
        token_endpoint="http://127.0.0.1:8876/_test/oauth/token",
        redirect_uri="http://127.0.0.1:8876/v1/mcp/oauth/callback",
        scopes=["tools"],
        allowed_hosts={"127.0.0.1"},
    )
    browser_auth = MCPOAuthAuth(
        oauth_config.resource,
        requester=oauth_requester,
        tokens=oauth_tokens,
        states=oauth_states,
        subject="browser-subject",
        server="browser_oauth",
        config=oauth_config,
    )
    app.state.mcp.register(
        TrustedMCPServer(
            MCPServerConfig(
                name="browser_oauth",
                transport="streamable_http",
                url=oauth_config.resource,
                allowed_hosts={"mcp.example"},
            ),
            "browser-oauth-v1",
            auth=browser_auth,
        )
    )

    @app.get("/_test/oauth-counts")
    async def browser_oauth_counts() -> dict[str, int]:
        return {"tokens": oauth_calls}

    class TestMCPControl(StrictModel):
        name: Literal["browser_mcp", "browser_slow_mcp", "browser_fail_mcp"]
        action: Literal["block", "release", "status"]

    @app.post("/_test/mcp")
    async def configure_mcp(payload: TestMCPControl) -> dict[str, bool | int]:
        if payload.action == "block":
            mcp_gates[payload.name] = (asyncio.Event(), asyncio.Event())
        elif payload.action == "release":
            gate = mcp_gates.pop(payload.name, None)
            if gate is not None:
                gate[1].set()
        current = mcp_gates.get(payload.name)
        return {
            "entered": current is not None and current[0].is_set(),
            "starts": mcp_counts.get(payload.name, 0),
        }

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
    app.state.agents.register_execution(
        "sandbox", LinuxSandboxRunner(), version="browser-execution-v1", owned=True
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

    class TestDirectoryControl(StrictModel):
        action: Literal["seed", "nonempty", "stat", "fail_save", "counts"]
        path: str = Field(default="empty", pattern=r"^[A-Za-z0-9_-]{1,80}$")
        agent_id: str | None = Field(default=None, pattern=r"^[a-f0-9-]{36}$")

    directory_fail_save = False
    original_directory_settle = app.state.agents.manager.directory_changes.store.settle

    async def controlled_directory_settle(*args, **kwargs):
        global directory_fail_save
        if directory_fail_save:
            directory_fail_save = False
            raise OSError("injected directory receipt save failure")
        return await original_directory_settle(*args, **kwargs)

    app.state.agents.manager.directory_changes.store.settle = controlled_directory_settle

    @app.post("/_test/directory")
    async def configure_directory(payload: TestDirectoryControl) -> dict[str, bool | int | None]:
        # 只在合成Playwright服务存在；不打入产品API或wheel。
        global directory_fail_save
        target = write_root / payload.path
        if payload.action == "fail_save":
            directory_fail_save = True
        elif payload.action == "seed":
            await asyncio.to_thread(target.mkdir, mode=0o750)
        elif payload.action == "nonempty":
            await asyncio.to_thread((target / "child.txt").write_text, "child")
        if payload.action == "counts":
            scope = "session:" + (payload.agent_id or "")
            database = runtime.repository.database
            async with database.write_lock, database.sessions() as session:
                proposals = await session.scalar(
                    select(func.count())
                    .select_from(WorkspaceDirectoryProposalRow)
                    .where(WorkspaceDirectoryProposalRow.scope_id == scope)
                )
                operations = await session.scalar(
                    select(func.count())
                    .select_from(WorkspaceDirectoryAttemptRow)
                    .where(WorkspaceDirectoryAttemptRow.scope_id == scope)
                )
            return {"proposals": proposals, "operations": operations}
        info = await asyncio.to_thread(target.stat) if target.exists() else None
        return {
            "found": info is not None,
            "inode": info.st_ino if info else None,
            "mode": info.st_mode & 0o777 if info else None,
        }

    class TestNativeBatchControl(StrictModel):
        agent_id: str = Field(min_length=36, max_length=36, pattern=r"^[a-f0-9-]{36}$")
        action: Literal["block", "release", "status"]

    @app.post("/_test/native-batch")
    async def configure_native_batch(payload: TestNativeBatchControl) -> dict[str, bool]:
        # 此路由只在Playwright装配存在，不进入产品API或wheel。
        await app.state.agents.session(payload.agent_id)
        previous = batch_gates.get(payload.agent_id)
        if payload.action == "block":
            if previous is not None and not previous[1].is_set():
                raise ServiceError("test_batch_already_blocked", "测试批次已暂停", 409)
            batch_gates[payload.agent_id] = (asyncio.Event(), asyncio.Event())
        elif payload.action == "release" and previous is not None:
            previous[1].set()
            batch_gates.pop(payload.agent_id, None)
        current = batch_gates.get(payload.agent_id)
        return {
            "blocked": current is not None and not current[1].is_set(),
            "entered": current is not None and current[0].is_set(),
        }

    class TestNativeCheckpointControl(StrictModel):
        agent_id: str = Field(min_length=36, max_length=36, pattern=r"^[a-f0-9-]{36}$")
        phase: Literal["capture", "restore"] = "restore"
        action: Literal["block", "release", "status"]

    @app.post("/_test/native-checkpoint")
    async def configure_native_checkpoint(payload: TestNativeCheckpointControl) -> dict[str, bool]:
        await app.state.agents.session(payload.agent_id)
        previous = checkpoint_gates.get(payload.agent_id)
        if payload.action == "block":
            if previous is not None and not previous[2].is_set():
                raise ServiceError("test_checkpoint_already_blocked", "测试检查点已暂停", 409)
            checkpoint_gates[payload.agent_id] = (payload.phase, asyncio.Event(), asyncio.Event())
        elif payload.action == "release" and previous is not None:
            previous[2].set()
            checkpoint_gates.pop(payload.agent_id, None)
        current = checkpoint_gates.get(payload.agent_id)
        return {
            "blocked": current is not None and not current[2].is_set(),
            "entered": current is not None and current[1].is_set(),
        }

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
                if tool.name not in WORKSPACE_TOOL_NAMES | COMMAND_TOOL_NAMES
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
