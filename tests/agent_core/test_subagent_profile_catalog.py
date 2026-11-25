"""Profile能力目录与只读提示：list_agents目录、spawn/replay回执、worker输入注入。

Service与SQLite存储全真实；仅宿主Caller/session_tools解析边界按service.py
契约用桩替换。验证：目录只含当前family实际注册、无endpoint/密钥/argv/宿主
路径；spawn成功与幂等replay回执都带profile与workspace mode；shared_read_only
的回执与worker输入都带明确只读提示；目录可见不代表权限放宽。
"""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from krim_agent.config import Settings, minimum_limit
from krim_agent.context.types import ContextPolicy
from krim_agent.contracts import RunRequest
from krim_agent.sessions.types import SessionCreate, TurnRequest
from krim_agent.storage import sessions as session_sql
from krim_agent.storage.sqlite import SQLiteStore
from krim_agent.subagents.configuration import (
    SubagentProcessConfiguration,
    SubagentProfileConfiguration,
    SubagentsConfiguration,
)
from krim_agent.subagents.errors import SubagentError
from krim_agent.subagents.providers.base import ProviderCapabilities
from krim_agent.subagents.service import Caller, SubagentService
from krim_agent.subagents.tool_models import ListAgents, SpawnAgent
from krim_agent.subagents.types import ReserveAgent, TreeScope
from krim_agent.tools.core import Invocation

FAMILY = "target-http"
SECRET_ARGV = ("python", "-c", "print('F:/host/only/path')")
DESCRIPTOR_TOOLS = ("read", "list", "find", "search", "write", "edit", "shell")


class QuietProvider:
    version = "fake/1"
    capabilities = ProviderCapabilities()
    descriptor = {
        "system_prompt": "worker prompt",
        "tools": [{"name": name} for name in DESCRIPTOR_TOOLS],
    }

    def __init__(self) -> None:
        self.started: list[str] = []

    async def start(self, task: Any) -> None:
        self.started.append(task.agent_id)

    async def interrupt(self, run_id: str) -> None:
        return None

    async def dispose(self, activation_id: str) -> None:
        return None


async def build_service(tmp_path: Path) -> tuple[SubagentService, SQLiteStore, QuietProvider]:
    store = await SQLiteStore.open(tmp_path / "catalog.sqlite")
    config = SubagentsConfiguration(
        enabled=True,
        default_profile="deepseek",
        profiles={
            "deepseek": SubagentProfileConfiguration(),
            "deepseek-write": SubagentProfileConfiguration(
                workspace="isolated_write",
                tools=("read", "edit", "shell"),
            ),
            # 受限allowlist：隔离工作区但没有可写/可执行工具。
            "reader": SubagentProfileConfiguration(
                workspace="isolated_write",
                tools=("read",),
            ),
            # shared_read_only却声明shell：准入仍拒绝，目录不得声称可用。
            "naive": SubagentProfileConfiguration(tools=("read", "shell")),
            "proc": SubagentProfileConfiguration(provider="process", process="probe"),
        },
        processes={"probe": SubagentProcessConfiguration(argv=SECRET_ARGV)},
    )
    service = SubagentService(store, config)
    provider = QuietProvider()
    for name in ("deepseek", "deepseek-write", "reader", "naive", "proc"):
        service.register_profile(name, provider, config.profiles[name])
    # 其他family的注册不得进入当前调用方的目录。
    service.register_profile(
        "elsewhere", provider, SubagentProfileConfiguration(), family="other-target"
    )
    return service, store, provider


async def seed_caller(service: SubagentService, store: SQLiteStore) -> Caller:
    session = await session_sql.create_session(
        store,
        "local",
        "local",
        SessionCreate(request_key="catalog-session"),
        {"key": "demo"},
        ContextPolicy(),
        Settings(),
    )
    turn = await session_sql.submit(
        store,
        session.id,
        TurnRequest(text="root task", request_key="catalog-root-turn", expected_revision=0),
        Settings(),
    )
    root, _ = await store.begin(
        RunRequest(text="root task", request_key="catalog-root", target=FAMILY),
        {"key": "demo"},
    )
    tree = await service.repository.ensure_tree(TreeScope(
        session_id=session.id,
        turn_id=turn.id,
        run_id=root.id,
        workspace_id="local",
        owner_id="local",
        project_id=None,
        request_key="catalog-tree",
    ))
    reserved = await service.repository.reserve_agent(ReserveAgent(
        tree_id=tree["tree_id"],
        path="lead",
        role="root",
        provider="sdk_session",
        profile="root",
        provider_version="v1",
        profile_version="root/1",
        request_key="catalog-root-member",
    ))
    member = await service.repository.get_agent(reserved["agent"]["agent_id"])
    # _caller桩化后补齐宿主职责：树预算行是spawn真实路径的准入前提。
    await service.budgets.ensure(
        tree["tree_id"],
        minimum_limit(None, service.configuration.max_requests_per_tree),
    )
    caller = Caller(member, tree, SimpleNamespace(check=lambda: None))

    async def host(invocation: Invocation) -> Caller:
        return caller

    service._caller = host  # type: ignore[method-assign]
    return caller


def invocation(run_id: str, call_id: str = "call-1", arguments: str = "{}") -> Invocation:
    return Invocation(
        run_id=run_id,
        call_id=call_id,
        span_id="span",
        step_index=0,
        name="spawn_agent",
        arguments_json=arguments,
        tool_hash="hash",
    )


async def test_catalog_lists_family_profiles_without_sensitive_fields(tmp_path: Path) -> None:
    service, store, _ = await build_service(tmp_path)
    try:
        caller = await seed_caller(service, store)
        result = await service.list_agents(
            invocation(caller.tree["root_run_id"]), ListAgents()
        )
        assert result["default_profile"] == "deepseek"
        catalog = {entry["name"]: entry for entry in result["profiles"]}
        assert set(catalog) == {"deepseek", "deepseek-write", "reader", "naive", "proc"}
        assert catalog["deepseek"]["provider"] == "sdk_session"
        assert catalog["deepseek"]["workspace"] == "shared_read_only"
        assert catalog["deepseek"]["can_run_shell"] is False
        assert catalog["deepseek"]["can_edit_workspace"] is False
        assert catalog["deepseek"]["can_execute_task"] is False
        assert catalog["deepseek"]["fork"] is True
        assert catalog["deepseek-write"]["workspace"] == "isolated_write"
        assert catalog["deepseek-write"]["can_run_shell"] is True
        assert catalog["deepseek-write"]["can_edit_workspace"] is True
        # allowlist把工具收窄到只读：隔离工作区也不能执行或改文件。
        assert catalog["reader"]["can_run_shell"] is False
        assert catalog["reader"]["can_edit_workspace"] is False
        # shared_read_only即使声明shell，准入拒绝，目录不得声称可用。
        assert catalog["naive"]["can_run_shell"] is False
        assert catalog["naive"]["can_edit_workspace"] is False
        # process是预配置一次性命令，不等于任意shell。
        assert catalog["proc"]["provider"] == "process"
        assert catalog["proc"]["can_execute_task"] is True
        assert catalog["proc"]["can_run_shell"] is False
        assert catalog["proc"]["can_edit_workspace"] is False
        for entry in catalog.values():
            assert set(entry) == {
                "name",
                "provider",
                "workspace",
                "can_run_shell",
                "can_edit_workspace",
                "can_execute_task",
                "fork",
                "max_depth",
            }
        encoded = json.dumps(result, ensure_ascii=False)
        assert "elsewhere" not in encoded
        assert SECRET_ARGV[-1] not in encoded
        # 工具名清单不发布，避免把shared_read_only下不可用的名字误传为可用。
        assert "tools" not in encoded
        for forbidden in ("argv", "api_key", "base_url", "endpoint", "system_prompt"):
            assert forbidden not in encoded
    finally:
        await store.close()


async def test_spawn_receipt_and_worker_input_carry_readonly_notice(tmp_path: Path) -> None:
    service, store, provider = await build_service(tmp_path)
    try:
        caller = await seed_caller(service, store)
        receipt = await service.spawn(
            invocation(caller.tree["root_run_id"]), SpawnAgent(task="review the diff")
        )
        assert receipt["accepted"] is True
        assert receipt["profile"] == "deepseek"
        assert receipt["workspace_mode"] == "shared_read_only"
        assert "cannot run shell" in receipt["notice"]
        assert "can_run_shell" in receipt["notice"]
        assert "list_agents" in receipt["notice"]
        task = await service.repository.task(receipt["task_id"])
        text = str(task["input"]["text"])
        assert text.startswith("[host capability notice]")
        assert "profile=deepseek" in text
        assert "workspace=shared_read_only" in text
        assert "review the diff" in text
        assert provider.started == [receipt["agent_id"]]
    finally:
        await store.close()


async def test_spawn_replay_and_recover_keep_profile_fields(tmp_path: Path) -> None:
    service, store, provider = await build_service(tmp_path)
    try:
        caller = await seed_caller(service, store)
        run_id = caller.tree["root_run_id"]
        request = SpawnAgent(task="review the diff")
        first = await service.spawn(invocation(run_id), request)
        replay = await service.spawn(invocation(run_id), request)
        assert replay["created"] is False
        assert replay["agent_id"] == first["agent_id"]
        assert replay["task_id"] == first["task_id"]
        assert replay["profile"] == "deepseek"
        assert replay["workspace_mode"] == "shared_read_only"
        assert "can_run_shell" in replay["notice"]
        assert provider.started == [first["agent_id"]]

        # 回执profile事实来自持久化profile_snapshot：当前配置变更不得让
        # 旧回执声称新权限。
        service._profiles["deepseek"] = SubagentProfileConfiguration(
            workspace="isolated_write",
        )
        replay_after_change = await service.spawn(invocation(run_id), request)
        assert replay_after_change["workspace_mode"] == "shared_read_only"
        assert replay_after_change["notice"] == replay["notice"]

        async def by_run(target_run_id: str, check: Any = None) -> Any:
            return SimpleNamespace(
                session_id=caller.tree["session_id"],
                actor=SimpleNamespace(principal_id=caller.tree["owner_id"]),
            )

        service.session_tools = SimpleNamespace(by_run=by_run)
        recovered = await service.recover_tool(
            invocation(run_id, arguments=request.model_dump_json())
        )
        assert recovered["ok"] is True
        assert recovered["created"] is False
        assert recovered["agent_id"] == first["agent_id"]
        assert recovered["profile"] == "deepseek"
        assert recovered["workspace_mode"] == "shared_read_only"
        assert "can_run_shell" in recovered["notice"]
    finally:
        await store.close()


async def test_catalog_does_not_expand_isolated_write_permissions(tmp_path: Path) -> None:
    service, store, _ = await build_service(tmp_path)
    try:
        caller = await seed_caller(service, store)
        with pytest.raises(SubagentError) as caught:
            await service.spawn(
                invocation(caller.tree["root_run_id"]),
                SpawnAgent(task="run the tests", profile="deepseek-write"),
            )
        assert caught.value.code == "subagent_capability"
        assert "workspace_isolation" in str(caught.value.action)
    finally:
        await store.close()
