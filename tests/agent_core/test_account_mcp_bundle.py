"""账号MCP无总量上限与open_bundle会话绑定验收。

覆盖：单连接>8所选工具、单会话>8连接选择、空selected_tools资源连接、
bundle初始scope绑定与跨会话拒绝、policy逐次resolve+scope.check默认ask、
close幂等与失败清理、旧build_runtime保留宿主pipeline、account命名空间
与宿主同名服务器共存。
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import socket
from contextlib import asynccontextmanager
from typing import Any

import pytest
from mcp.server.fastmcp import FastMCP
from pydantic import BaseModel
from uvicorn import Config, Server

from krim_agent.account_mcp.secrets import AuthenticatedSecretVault, MasterKeyring
from krim_agent.account_mcp.service import (
    AccountMCPBundle,
    AccountMCPService,
    ConnectionCreate,
    ConnectionUpdate,
    SessionConnection,
    SessionMCPSelection,
    _config_document,
)
from krim_agent.accounts.service import IdentityService
from krim_agent.config import Settings
from krim_agent.context.types import ContextPolicy
from krim_agent.contracts import RunEvent, ServiceError, encoded
from krim_agent.mcp.types import MCPServerConfig
from krim_agent.security import LOCAL, Membership, PrincipalContext
from krim_agent.sessions.types import SessionCreate, TurnRequest
from krim_agent.storage import sessions as session_sql
from krim_agent.storage.sqlite import SQLiteStore
from krim_agent.tools.core import Invocation, ToolSpec
from krim_agent.tools.executor import ToolContext, ToolExecutor
from krim_agent.tools.mcp_resources import (
    ListResourcesInput,
    MCPResourceBinding,
    MCPResourceService,
    ReadResourceInput,
)
from krim_agent.tools.registry import ToolRegistry
from krim_agent.tools.session import SessionToolScope

CREDENTIAL = "bundle-secret-marker"


class ToolServer:
    """回环streamable_http MCP服务器：N个工具与一个资源，记录工具调用。"""

    def __init__(self, tool_count: int = 1, *, resources: bool = False) -> None:
        self.calls: list[dict] = []
        server = self
        mcp = FastMCP("docs")

        def make_tool(index: int):
            async def tool(query: str) -> dict:
                server.calls.append({"tool": f"tool_{index}", "query": query})
                return {"result": f"found:tool_{index}:{query}"}

            tool.__name__ = f"tool_{index}"
            return tool

        for index in range(tool_count):
            mcp.tool()(make_tool(index))

        if resources:

            @mcp.resource("file://docs/greeting")
            async def greeting() -> str:
                return "hello from docs"

        self.endpoint = mcp.streamable_http_app()
        self._listener: socket.socket | None = None
        self._server: Server | None = None
        self.port = 0

    async def start(self) -> None:
        listener = socket.socket()
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", 0))
        listener.listen(16)
        listener.setblocking(False)
        self.port = listener.getsockname()[1]
        self._listener = listener
        instance = Server(Config(self.endpoint, access_log=False, log_level="error"))
        self._server = instance
        task = asyncio.create_task(instance.serve(sockets=[listener]), name="bundle-fixture")
        async with asyncio.timeout(10):
            while not instance.started:
                if task.done():
                    await task
                await asyncio.sleep(0.005)

    async def stop(self) -> None:
        listener, self._listener = self._listener, None
        if self._server is not None:
            self._server.should_exit = True
            await self._server.shutdown()
        if listener is not None:
            listener.close()


@asynccontextmanager
async def managed(path):
    store = await SQLiteStore.open(path)
    identities = await IdentityService.open(store)
    boot = await identities.bootstrap("admin", request_key="bootstrap")
    admin = identities.authenticate("Bearer " + boot.token)
    try:
        yield store, identities, admin
    finally:
        await identities.close()
        await store.close()


async def create_owner(identities: IdentityService, admin: PrincipalContext, name: str):
    account = await identities.create_account(
        admin, {"display_name": name}, request_key="account-" + name
    )
    issued = await identities.issue_key(
        admin, account.account.principal_id, {}, request_key="key-" + name
    )
    return identities.authenticate("Bearer " + issued.token)


async def create_session(store: SQLiteStore, owner: PrincipalContext, workspace: str):
    return await session_sql.create_session(
        store,
        workspace,
        owner.principal_id,
        SessionCreate(request_key="session"),
        {"key": "demo"},
        ContextPolicy(),
        Settings(),
    )


async def connect_docs(
    service: AccountMCPService,
    owner: PrincipalContext,
    server: ToolServer,
    *,
    name: str = "docs",
    selected: tuple[str, ...] = ("tool_0",),
    readonly: tuple[str, ...] = ("tool_0",),
    credentials: dict | None = None,
    key: str = "create-docs",
):
    return await service.create_connection(
        owner,
        ConnectionCreate(
            request_key=key,
            name=name,
            url=f"http://127.0.0.1:{server.port}/mcp",
            allowed_hosts=("127.0.0.1",),
            selected_tools=selected,
            readonly_tools=readonly,
            credentials=credentials or {},
            timeout_seconds=10.0,
        ),
    )


async def prepare_turn(store, identities, service, owner, *, connections, key="turn"):
    """建session、写selection并提交turn；返回(workspace, session, turn, actor)。"""
    profile = await identities.profile(owner)
    workspace = next(ref.workspace_id for ref in profile.workspaces if ref.personal)
    session = await create_session(store, owner, workspace)
    await service.session_selection(
        owner,
        workspace,
        session.id,
        SessionMCPSelection(
            request_key="select-" + key,
            expected_revision=0,
            connections=tuple(
                SessionConnection(connection_id=item["id"], revision=item["revision"])
                for item in connections
            ),
        ),
    )
    actor = identities.validate(owner)
    turn = await session_sql.submit(
        store,
        session.id,
        TurnRequest(request_key=key, text="bundle", expected_revision=0),
        Settings(),
        actor=actor,
        account_mcp_service=service,
    )
    return workspace, session, turn, actor


def make_scope(
    actor: PrincipalContext,
    workspace: str,
    session: Any,
    turn: Any,
    *,
    run_id: str = "run-bundle",
    session_id: str | None = None,
    check: Any = None,
) -> SessionToolScope:
    return SessionToolScope(
        actor=actor,
        workspace_id=workspace,
        session_id=session_id if session_id is not None else session.id,
        turn_id=turn.id,
        run_id=run_id,
        check=check or (lambda: None),
    )


def sequence_resolver(scopes: list[SessionToolScope], calls: list[str]):
    """按调用顺序返回scope；耗尽后停留在最后一个。"""
    remaining = list(scopes)

    async def resolve(invocation: Invocation) -> SessionToolScope:
        calls.append(invocation.name)
        if len(remaining) > 1:
            return remaining.pop(0)
        return remaining[0]

    return resolve


def invocation_for(spec: ToolSpec, arguments: str = '{"query": "x"}') -> Invocation:
    return Invocation("run-bundle", "call-1", "span-1", 0, spec.name, arguments, spec.fingerprint)


async def call_spec(spec: ToolSpec, query: str = "x"):
    value = await spec.external_executor(
        invocation_for(spec), spec.parameters.model_validate({"query": query})
    )
    return json.loads(value) if isinstance(value, str) else value


@pytest.mark.asyncio
async def test_single_connection_accepts_over_eight_selected_tools(tmp_path):
    server = ToolServer(12)
    await server.start()
    try:
        async with managed(tmp_path / "bundle.sqlite") as (store, identities, admin):
            owner = await create_owner(identities, admin, "bundle-owner")
            vault = AuthenticatedSecretVault(MasterKeyring.create(bytes(range(96, 128))))
            service = AccountMCPService(store, identities, vault)
            created = await connect_docs(
                service,
                owner,
                server,
                selected=tuple(f"tool_{index}" for index in range(12)),
                readonly=tuple(f"tool_{index}" for index in range(6)),
                credentials={"X-Test-Credential": CREDENTIAL},
            )
            workspace, session, turn, actor = await prepare_turn(
                store, identities, service, owner, connections=[created]
            )
            configs = await service.configurations_for_turn(turn.id)
            assert len(configs) == 1
            assert len(configs[0].selected_tools) == 12
            assert configs[0].headers["X-Test-Credential"].get_secret_value() == CREDENTIAL

            scope = make_scope(actor, workspace, session, turn)
            calls: list[str] = []
            bundle = await service.open_bundle(configs, sequence_resolver([scope], calls))
            try:
                assert isinstance(bundle, AccountMCPBundle)
                assert bundle.owner_scope is None  # 延迟绑定：首次resolve才固定
                specs = bundle.bundle.specs
                assert {spec.name for spec in specs} == {
                    f"account__docs__tool_{index}" for index in range(12)
                }
                assert await bundle.bundle.policy(invocation_for(specs[0])) == "ask"
                assert bundle.owner_scope is scope
                value = await call_spec(specs[0])
                assert value == {"result": "found:tool_0:x"}
                assert server.calls == [{"tool": "tool_0", "query": "x"}]
                # policy前置、执行前与执行后各resolve一次（执行后防撤权回读）。
                assert calls == [specs[0].name] * 3
            finally:
                await bundle.close()
            assert all(connection.closed for connection in bundle.connections)
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_more_than_eight_connections_compose_into_one_bundle(tmp_path):
    server = ToolServer(1)
    await server.start()
    try:
        async with managed(tmp_path / "many.sqlite") as (store, identities, admin):
            owner = await create_owner(identities, admin, "many-owner")
            vault = AuthenticatedSecretVault(MasterKeyring.create(bytes(range(128, 160))))
            service = AccountMCPService(store, identities, vault)
            created = [
                await connect_docs(
                    service,
                    owner,
                    server,
                    name=f"docs{index}",
                    key=f"create-docs-{index}",
                )
                for index in range(9)
            ]
            workspace, session, turn, actor = await prepare_turn(
                store, identities, service, owner, connections=created, key="turn-many"
            )
            configs = await service.configurations_for_turn(turn.id)
            assert len(configs) == 9

            scope = make_scope(actor, workspace, session, turn)
            bundle = await service.open_bundle(configs, sequence_resolver([scope], []))
            try:
                assert len(bundle.connections) == 9
                specs = bundle.bundle.specs
                assert len(specs) == 9
                assert len({spec.name for spec in specs}) == 9
                assert await bundle.bundle.policy(invocation_for(specs[8])) == "ask"
            finally:
                await bundle.close()
            assert all(connection.closed for connection in bundle.connections)
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_bundle_binds_initial_scope_and_rejects_cross_session(tmp_path):
    server = ToolServer(1)
    await server.start()
    try:
        async with managed(tmp_path / "scope.sqlite") as (store, identities, admin):
            owner = await create_owner(identities, admin, "scope-owner")
            vault = AuthenticatedSecretVault(MasterKeyring.create(bytes(range(160, 192))))
            service = AccountMCPService(store, identities, vault)
            created = await connect_docs(service, owner, server)
            workspace, session, turn, actor = await prepare_turn(
                store, identities, service, owner, connections=[created], key="turn-scope"
            )
            configs = await service.configurations_for_turn(turn.id)
            first = make_scope(actor, workspace, session, turn, run_id="run-a")
            second = make_scope(
                actor, workspace, session, turn,
                session_id="other-session", run_id="run-b",
            )
            calls: list[str] = []
            bundle = await service.open_bundle(configs, sequence_resolver([first, second], calls))
            specs = bundle.bundle.specs
            assert await bundle.bundle.policy(invocation_for(specs[0])) == "ask"
            assert bundle.owner_scope is first
            with pytest.raises(ServiceError) as mismatch:
                await call_spec(specs[0])
            assert mismatch.value.code == "permission_denied"
            assert server.calls == []
            assert bundle.owner_scope is first
            with pytest.raises(ServiceError, match="permission_denied"):
                await bundle.bundle.policy(invocation_for(specs[0]))
            await bundle.close()

            # 新束独立绑定另一scope：初始范围不跨束共享，绑定期后可正常执行。
            holder = [second, second]
            fresh = await service.open_bundle(configs, sequence_resolver(holder, []))
            try:
                assert fresh.owner_scope is None
                assert await call_spec(fresh.bundle.specs[0], "fresh") == {
                    "result": "found:tool_0:fresh"
                }
                assert fresh.owner_scope is second
            finally:
                await fresh.close()
            assert server.calls[-1] == {"tool": "tool_0", "query": "fresh"}

            # 显式传入scope立即绑定；resolve结果不一致在policy处即被拒绝。
            eager = await service.open_bundle(
                configs, sequence_resolver([second, second], []), scope=first
            )
            try:
                assert eager.owner_scope is first
                with pytest.raises(ServiceError, match="permission_denied"):
                    await eager.bundle.policy(invocation_for(eager.bundle.specs[0]))
            finally:
                await eager.close()
            assert server.calls[-1] == {"tool": "tool_0", "query": "fresh"}
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_bundle_requires_workspace_membership(tmp_path):
    server = ToolServer(1)
    await server.start()
    try:
        async with managed(tmp_path / "member.sqlite") as (store, identities, admin):
            owner = await create_owner(identities, admin, "member-owner")
            vault = AuthenticatedSecretVault(MasterKeyring.create(bytes(range(192, 224))))
            service = AccountMCPService(store, identities, vault)
            created = await connect_docs(service, owner, server, key="create-member")
            workspace, session, turn, _actor = await prepare_turn(
                store, identities, service, owner, connections=[created], key="turn-member"
            )
            configs = await service.configurations_for_turn(turn.id)
            outsider_actor = PrincipalContext(
                "outside", 1, (Membership(workspace_id="elsewhere"),)
            )
            outsider = make_scope(
                outsider_actor, workspace, session, turn, session_id="intrude"
            )
            with pytest.raises(ServiceError, match="not_found"):
                await service.open_bundle(
                    configs, sequence_resolver([outsider], []), scope=outsider
                )
            assert server.calls == []
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_bundle_policy_resolves_and_checks_on_every_call(tmp_path):
    server = ToolServer(1)
    await server.start()
    try:
        async with managed(tmp_path / "checks.sqlite") as (store, identities, admin):
            owner = await create_owner(identities, admin, "check-owner")
            vault = AuthenticatedSecretVault(MasterKeyring.create(bytes(range(224, 256))))
            service = AccountMCPService(store, identities, vault)
            created = await connect_docs(service, owner, server, key="create-checks")
            workspace, session, turn, actor = await prepare_turn(
                store, identities, service, owner, connections=[created], key="turn-checks"
            )
            configs = await service.configurations_for_turn(turn.id)
            checks = {"count": 0}

            def check() -> None:
                checks["count"] += 1

            scope = make_scope(actor, workspace, session, turn, check=check)
            calls: list[str] = []
            bundle = await service.open_bundle(configs, sequence_resolver([scope], calls))
            try:
                spec = bundle.bundle.specs[0]
                for _ in range(2):
                    assert await bundle.bundle.policy(invocation_for(spec)) == "ask"
                assert calls == [spec.name, spec.name]
                # 每次resolve经admit与scope_key各核对一次scope.check。
                assert checks["count"] == 4
                # 非bundle工具直接deny，不触发resolve。
                assert await bundle.bundle.policy(
                    Invocation("run-bundle", "call", "span", 0, "host_tool", "{}", "0" * 64)
                ) == "deny"
                assert calls == [spec.name, spec.name]
                assert checks["count"] == 4
            finally:
                await bundle.close()
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_bundle_close_is_idempotent_and_blocks_further_calls(tmp_path):
    server = ToolServer(1)
    await server.start()
    try:
        async with managed(tmp_path / "close.sqlite") as (store, identities, admin):
            owner = await create_owner(identities, admin, "close-owner")
            vault = AuthenticatedSecretVault(MasterKeyring.create(bytes(range(32, 64))))
            service = AccountMCPService(store, identities, vault)
            created = await connect_docs(service, owner, server, key="create-close")
            workspace, session, turn, actor = await prepare_turn(
                store, identities, service, owner, connections=[created], key="turn-close"
            )
            configs = await service.configurations_for_turn(turn.id)
            scope = make_scope(actor, workspace, session, turn)
            bundle = await service.open_bundle(configs, sequence_resolver([scope], []))
            spec = bundle.bundle.specs[0]
            await bundle.close()
            assert all(connection.closed for connection in bundle.connections)
            await bundle.close()
            with pytest.raises(ServiceError) as blocked:
                await call_spec(spec)
            assert blocked.value.code == "mcp_unavailable"
            assert server.calls == []
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_scoped_executor_rechecks_authority_after_execution(tmp_path):
    server = ToolServer(1)
    await server.start()
    try:
        async with managed(tmp_path / "recheck.sqlite") as (store, identities, admin):
            owner = await create_owner(identities, admin, "recheck-owner")
            vault = AuthenticatedSecretVault(MasterKeyring.create(bytes(range(16, 48))))
            service = AccountMCPService(store, identities, vault)
            created = await connect_docs(service, owner, server, key="create-recheck")
            workspace, session, turn, actor = await prepare_turn(
                store, identities, service, owner, connections=[created], key="turn-recheck"
            )
            configs = await service.configurations_for_turn(turn.id)
            checks = {"count": 0}

            def check() -> None:
                checks["count"] += 1
                if checks["count"] > 2:  # 执行后的第二次resolve即撤权
                    raise ServiceError("session_closed", 409)

            scope = make_scope(actor, workspace, session, turn, check=check)
            bundle = await service.open_bundle(configs, sequence_resolver([scope], []))
            try:
                with pytest.raises(ServiceError) as revoked:
                    await call_spec(bundle.bundle.specs[0], "revoked")
                assert revoked.value.code == "session_closed"
                # 服务器执行了调用，但结果不返回给已撤权的scope。
                assert server.calls == [{"tool": "tool_0", "query": "revoked"}]
            finally:
                await bundle.close()
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_bundle_close_continues_after_caller_cancellation(tmp_path):
    server = ToolServer(1)
    await server.start()
    try:
        async with managed(tmp_path / "cancel.sqlite") as (store, identities, admin):
            owner = await create_owner(identities, admin, "cancel-owner")
            vault = AuthenticatedSecretVault(MasterKeyring.create(bytes(range(48, 80))))
            service = AccountMCPService(store, identities, vault)
            created = await connect_docs(service, owner, server, key="create-cancel")
            workspace, session, turn, actor = await prepare_turn(
                store, identities, service, owner, connections=[created], key="turn-cancel"
            )
            configs = await service.configurations_for_turn(turn.id)
            scope = make_scope(actor, workspace, session, turn)
            bundle = await service.open_bundle(configs, sequence_resolver([scope], []))
            closer = asyncio.create_task(bundle.close())
            await asyncio.sleep(0)  # 关闭任务已启动并停在清理await点
            closer.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await closer
            await bundle.close()  # 继续等待同一关闭任务直至清理完成
            assert all(connection.closed for connection in bundle.connections)
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_open_bundle_failure_closes_earlier_connections(tmp_path):
    good = ToolServer(1)
    await good.start()
    try:
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            dead_port = probe.getsockname()[1]
        good_config = MCPServerConfig(
            name="docs",
            transport="streamable_http",
            url=f"http://127.0.0.1:{good.port}/mcp",
            allowed_hosts=frozenset({"127.0.0.1"}),
            selected_tools=("tool_0",),
        )
        bad_config = MCPServerConfig(
            name="late",
            transport="streamable_http",
            url=f"http://127.0.0.1:{dead_port}/mcp",
            allowed_hosts=frozenset({"127.0.0.1"}),
            selected_tools=("tool_0",),
        )
        service = AccountMCPService(None, None, None)
        scope = SessionToolScope(
            LOCAL, "local", "session", "turn", "run", check=lambda: None
        )
        calls: list[str] = []
        with pytest.raises(ServiceError) as failure:
            await service.open_bundle((good_config, bad_config), sequence_resolver([scope], calls))
        assert failure.value.code == "mcp_connect_failed"
        assert good.calls == []
        assert calls == []
    finally:
        await good.stop()


@pytest.mark.asyncio
async def test_resource_only_connection_opens_bundle_and_binds_resources(tmp_path):
    server = ToolServer(0, resources=True)
    await server.start()
    try:
        async with managed(tmp_path / "resources.sqlite") as (store, identities, admin):
            owner = await create_owner(identities, admin, "resource-owner")
            vault = AuthenticatedSecretVault(MasterKeyring.create(bytes(range(64, 96))))
            service = AccountMCPService(store, identities, vault)
            created = await connect_docs(
                service, owner, server, selected=(), readonly=(), key="create-resources"
            )
            workspace, session, turn, actor = await prepare_turn(
                store, identities, service, owner, connections=[created], key="turn-resources"
            )
            configs = await service.configurations_for_turn(turn.id)
            assert len(configs) == 1 and configs[0].selected_tools == ()

            scope = make_scope(actor, workspace, session, turn)
            calls: list[str] = []
            bundle = await service.open_bundle(configs, sequence_resolver([scope], calls))
            try:
                assert bundle.bundle.specs == ()
                assert bundle.owner_scope is None
                invocation = Invocation(
                    "run-bundle", "call", "span", 0, "list_mcp_resources", "{}", "0" * 64
                )
                bindings = await bundle.resource_bindings(invocation)
                assert bundle.owner_scope is scope
                assert len(bindings) == 1
                binding = bindings[0]
                assert isinstance(binding, MCPResourceBinding)
                assert binding.source_id == "account:docs"
                assert binding.owner_scope is scope
                resources = MCPResourceService(bindings)
                sources = resources.sources(scope)
                assert sources and sources[0]["server"] == "account:docs"
                page = await resources.list(scope, ListResourcesInput(server="account:docs"))
                assert page["server"] == "account:docs"
                assert any(
                    item["uri"] == "file://docs/greeting" for item in page["resources"]
                )
                read = await resources.read(
                    scope,
                    ReadResourceInput(server="account:docs", uri="file://docs/greeting"),
                )
                assert read["contents"][0]["text"] == "hello from docs"
                assert read["contents"][0]["kind"] == "text"
                other = make_scope(
                    actor, workspace, session, turn, session_id="elsewhere", run_id="run-x"
                )
                with pytest.raises(ServiceError, match="not_found"):
                    await resources.list(other, ListResourcesInput(server="account:docs"))
            finally:
                await bundle.close()
    finally:
        await server.stop()


class HostArguments(BaseModel):
    query: str = ""


async def _host_execute(arguments: BaseModel) -> Any:
    return {"host": arguments.model_dump(mode="json")}


def _host_spec(name: str) -> ToolSpec:
    return ToolSpec(
        name, HostArguments, _host_execute, {"type": "object"}, "host/1", "0" * 64
    )


@pytest.mark.asyncio
async def test_build_runtime_preserves_pipeline_and_account_namespace_coexists(tmp_path):
    server = ToolServer(2)
    await server.start()
    try:
        service = AccountMCPService(None, None, None)
        config = MCPServerConfig(
            name="docs",
            transport="streamable_http",
            url=f"http://127.0.0.1:{server.port}/mcp",
            allowed_hosts=frozenset({"127.0.0.1"}),
            selected_tools=("tool_0", "tool_1"),
        )
        base_specs = (_host_spec("host_tool"), _host_spec("docs__tool_0"))
        policy_calls: list[str] = []
        pre_calls: list[str] = []
        post_calls: list[str] = []

        async def base_policy(invocation: Invocation) -> str:
            policy_calls.append(invocation.name)
            return "deny"

        async def pre(invocation: Invocation) -> None:
            pre_calls.append(invocation.name)

        async def post(invocation: Invocation, _observation) -> None:
            post_calls.append(invocation.name)

        async def middleware(invocation: Invocation) -> None:
            return None

        async def wrap(invocation: Invocation, next_once):
            return await next_once()

        base = ToolExecutor(
            ToolRegistry((_host_spec("host_tool"),)), base_policy, pre, post,
            middleware=(middleware,), wrap=wrap,
        )
        unchanged, no_connections = await service.build_runtime((), base)
        assert unchanged is base
        assert no_connections == ()

        # 旧build_runtime沿用原名并保留宿主pipeline：base policy仍管host工具。
        executor, connections = await service.build_runtime((config,), base)
        try:
            assert executor.pre is pre
            assert executor.post is post
            assert executor.middleware == (middleware,)
            assert executor.wrap is wrap
            assert len(executor.registry.schemas()) == 3
            assert await executor.policy(
                Invocation("r", "c", "s", 0, "host_tool", "{}", "0" * 64)
            ) == "deny"
            assert policy_calls == ["host_tool"]
            assert await executor.policy(
                Invocation("r", "c", "s", 0, "docs__tool_1", "{}", "0" * 64)
            ) == "ask"
            assert policy_calls == ["host_tool"]
        finally:
            await asyncio.gather(*(connection.close() for connection in connections))

        scope = SessionToolScope(LOCAL, "local", "session", "turn", "run")
        calls: list[str] = []
        bundle = await service.open_bundle(
            configs=(config,), resolve=sequence_resolver([scope], calls)
        )
        try:
            names = {spec.name for spec in bundle.bundle.specs}
            assert names == {"account__docs__tool_0", "account__docs__tool_1"}
            assert all(spec.source.server == "docs" for spec in bundle.bundle.specs)
            combined = ToolRegistry(base_specs + bundle.bundle.specs)
            assert len(combined.schemas()) == 4

            # 经ToolExecutor完整链路：bundle policy ask → 批准 → scoped执行。
            events: list[tuple[str, str]] = []

            async def emit(kind: str, data: dict) -> RunEvent:
                events.append((kind, str(data.get("name"))))
                return RunEvent(
                    run_id="run-bundle", seq=len(events), event_id=f"e{len(events)}",
                    kind=kind, data=data,
                )

            async def approve(_invocation: Invocation, _timeout: float) -> bool:
                return True

            bundle_executor = ToolExecutor(
                ToolRegistry(bundle.bundle.specs), bundle.bundle.policy, pre, post,
                middleware=(middleware,), wrap=wrap,
            )
            context = ToolContext(emit=emit, approve=approve, timeout=5.0)
            observation = await bundle_executor.execute(
                invocation_for(bundle.bundle.specs[0], '{"query": "roundtrip"}'), context
            )
            assert observation.status == "ok"
            assert observation.decision == "ask"
            assert json.loads(observation.value) == {"result": "found:tool_0:roundtrip"}
            assert pre_calls == ["account__docs__tool_0"]
            assert post_calls == ["account__docs__tool_0"]
            assert server.calls == [{"tool": "tool_0", "query": "roundtrip"}]
        finally:
            await bundle.close()
        assert events[0][0] == "tool_start"
        assert events[-1][0] == "tool_end"
        assert "account__docs__tool_0" in calls
        assert "account__docs__tool_0" in pre_calls
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_connection_config_document_has_no_capacity_limit(tmp_path):
    """约1000个所选工具、配置文档超过8KiB仍可create/update。"""
    tools = tuple(f"tool_{index:04d}" for index in range(1000))
    create = ConnectionCreate(
        request_key="large-config",
        name="docs",
        url="https://docs.example/mcp",
        allowed_hosts=("docs.example",),
        selected_tools=tools,
        readonly_tools=("tool_0000",),
    )
    update = ConnectionUpdate(
        request_key="large-config-update",
        expected_revision=1,
        name="docs",
        url="https://docs.example/mcp",
        allowed_hosts=("docs.example",),
        selected_tools=tools,
        readonly_tools=("tool_0000", "tool_0001"),
    )
    assert len(encoded(_config_document(create)).encode("utf-8")) > 8192
    assert len(encoded(_config_document(update)).encode("utf-8")) > 8192

    async with managed(tmp_path / "large.sqlite") as (store, identities, admin):
        owner = await create_owner(identities, admin, "large-owner")
        service = AccountMCPService(store, identities, None)  # 无凭据不涉及vault
        created = await service.create_connection(owner, create)
        assert created["revision"] == 1
        updated = await service.update_connection(owner, created["id"], update)
        assert updated["revision"] == 2
        listed = await service.list_connections(owner)
        assert len(listed[0]["selected_tools"]) == 1000
        assert listed[0]["revision"] == 2
