"""跨版本持久Agent验证：旧版创建，新版恢复，旧版严格解析新回执。

调用约定：python workspace_effect_compatibility_check.py MODE STATE_DIRECTORY。
MODE为create/reject-tool/reject-resource/restore/parse；parse不打开数据库。
"""

import asyncio
import json
import sys
from dataclasses import replace
from pathlib import Path

from pydantic import ValidationError
from sqlalchemy import text

import krim_agent
from krim_agent.agents import AgentProfile, AgentProfileUpdate
from krim_agent.config import Settings
from krim_agent.protocol.domain import ServiceError
from krim_agent.providers import FakeProvider
from krim_agent.sdk import FileChangeSpec, PythonAI, WorkspaceChangeAction
from krim_agent.tools import ToolRegistry
from krim_agent.tools.builtins import builtin_registry
from krim_agent.workspace_change import FileChangeReceipt, WorkspaceAttemptView

WRITE_TOOLS = [
    "workspace_preview",
    "workspace_change",
    "workspace_reconcile",
    "workspace_finalize",
]


class CountingProvider(FakeProvider):
    def __init__(self):
        super().__init__(delay=0)
        self.calls = 0

    async def stream(self, messages, tools):
        self.calls += 1
        async for part in super().stream(messages, tools):
            yield part


async def allow(_definition, _context):
    return "allow"


def write_json(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")


def schemas():
    return {
        "WorkspaceAttemptView": WorkspaceAttemptView.model_json_schema(),
        "FileChangeReceipt": FileChangeReceipt.model_json_schema(),
    }


async def migration(ai):
    async with ai.runtime.repository.database.engine.connect() as connection:
        return (await connection.execute(text("SELECT version_num FROM alembic_version"))).scalar()


async def snapshot(ai, agent):
    definition = ai.agents().manager._handles[agent.agent_id].definition
    state = await agent.state()
    return {
        "agent_id": agent.agent_id,
        "session_id": agent.session_id,
        "profile_revision": definition.profile_revision,
        "instructions": definition.instructions,
        "definition_hash": ai.agents().manager.definition_hash(definition),
        "tool_info": [item.model_dump(mode="json") for item in definition.tools.list_tools()],
        "resource_version": definition.resource_version,
        "resource_manifest": definition.resource_manifest,
        "workspace_manifest": definition.workspace.manifest(),
        "session_resources": state.permissions["resources"],
        "session_workspace": state.permissions["workspace"],
    }


async def create(ai, state_dir, provider):
    profile = AgentProfile(
        name="frozen-worker",
        description="持久升级兼容验证",
        instructions="保留第一版配置",
        tool_names=["echo", "workspace_read", *WRITE_TOOLS],
        workspace_key="files",
        persistent=True,
    )
    service = ai.agents()
    await service.profile_put(AgentProfileUpdate(expected_revision=0, config=profile))
    agents = {}
    for name in ("default", "frozen-worker"):
        path = f"{name}.txt"
        (state_dir / "files" / path).write_text(f"old:{name}\n", encoding="utf-8")
        agent = await ai.agent(name, persistent=True)
        first = await agent.generate_text(f"/echo created-{name}", idempotency_key="first")
        assert first.run.status == "completed" and f"created-{name}" in first.text
        proposal = await agent.preview_file_change(
            FileChangeSpec(
                path=path,
                operation="write",
                expected_sha256=(await agent.read_file(path)).sha256,
                content=f"created:{name}\n",
            ),
            request_key="old-preview",
        )
        request = WorkspaceChangeAction(
            proposal_id=proposal.proposal_id,
            action="apply",
            expected_revision=0,
            request_key="old-apply",
        )
        applied = await agent.change_file(request)
        assert applied.status == "done"
        assert (await agent.read_file(path)).content == f"created:{name}\n"
        agents[name] = {
            "snapshot": await snapshot(ai, agent),
            "run_id": first.run.id,
            "proposal_id": proposal.proposal_id,
            "operation_id": applied.operation_id,
            "request": request.model_dump(mode="json"),
            "path": path,
        }
    await service.profile_put(
        AgentProfileUpdate(
            expected_revision=1,
            config=profile.model_copy(update={"instructions": "第二版不可替换冻结实例"}),
        )
    )
    record = {
        "package": krim_agent.__file__,
        "migration": await migration(ai),
        "agents": agents,
        "schemas": schemas(),
        "registered_tools": [
            item.model_dump(mode="json") for item in ai.runtime.tools.list_tools()
        ],
        "provider_calls": provider.calls,
    }
    write_json(state_dir / "original.json", record)
    return {
        "agents": len(agents),
        "migration": record["migration"],
        "provider_calls": provider.calls,
    }


async def reject(ai, original, provider, mode):
    service = ai.agents()
    assert await service.restore_persistent() == 0
    inventory = await service.inventory()
    assert len(inventory) == 2
    assert all(item.error_code == "agent_definition_changed" for item in inventory)
    rejected = []
    for name, previous in original["agents"].items():
        agent = ai.agent_session(previous["snapshot"]["agent_id"])
        try:
            await agent.restore()
        except ServiceError as error:
            assert error.code == "agent_definition_changed"
            rejected.append(agent.agent_id)
        else:
            raise AssertionError("变更后的真实定义被错误恢复")
        definition = await service.definition(name, previous["snapshot"]["profile_revision"])
        assert (
            service.manager.definition_hash(definition) != previous["snapshot"]["definition_hash"]
        )
        if mode == "reject-tool":
            assert [
                item.model_dump(mode="json") for item in definition.tools.list_tools()
            ] != previous["snapshot"]["tool_info"]
        else:
            assert definition.resource_version != previous["snapshot"]["resource_version"]
    assert provider.calls == 0
    return {"rejected": rejected, "provider_calls": provider.calls}


async def restore(ai, state_dir, original, provider):
    service = ai.agents()
    assert [item.model_dump(mode="json") for item in ai.runtime.tools.list_tools()] == original[
        "registered_tools"
    ]
    assert schemas() == original["schemas"]
    assert await service.restore_persistent() == 2
    assert provider.calls == 0
    assert (await service.profile_get("frozen-worker")).revision == 2
    attempts, proofs = [], []
    for name, previous in original["agents"].items():
        agent = ai.agent_session(previous["snapshot"]["agent_id"])
        assert await snapshot(ai, agent) == previous["snapshot"]
        assert (await agent.view()).current_run_id == previous["run_id"]
        path = previous["path"]
        # 原请求保持原operation；升级和恢复不重放旧文件动作。
        repeated = await agent.change_file(
            WorkspaceChangeAction.model_validate(previous["request"])
        )
        assert repeated.operation_id == previous["operation_id"] and repeated.status == "done"
        assert (await agent.read_file(path)).content == f"created:{name}\n"
        assert len(await agent.file_change_history(previous["proposal_id"])) == 1
        attempts.append(repeated.model_dump(mode="json"))
        old_proposal = await agent.file_change(previous["proposal_id"])
        undone = await agent.change_file(
            WorkspaceChangeAction(
                proposal_id=old_proposal.proposal_id,
                action="undo",
                expected_revision=old_proposal.revision,
                request_key="upgraded-undo-old",
            )
        )
        assert undone.status == "done"
        assert (await agent.read_file(path)).content == f"old:{name}\n"
        attempts.append(undone.model_dump(mode="json"))
        proposal = await agent.preview_file_change(
            FileChangeSpec(
                path=path,
                operation="write",
                expected_sha256=(await agent.read_file(path)).sha256,
                content=f"upgraded:{name}\n",
            ),
            request_key="upgraded-preview",
        )
        applied = await agent.change_file(
            WorkspaceChangeAction(
                proposal_id=proposal.proposal_id,
                action="apply",
                expected_revision=0,
                request_key="upgraded-apply",
            )
        )
        assert applied.status == "done"
        attempts.append(applied.model_dump(mode="json"))
        successor = await agent.preview_file_change(
            FileChangeSpec(
                path=path,
                operation="write",
                expected_sha256=(await agent.read_file(path)).sha256,
                content=f"successor:{name}\n",
            ),
            request_key="upgraded-successor-preview",
        )
        successor_apply = await agent.change_file(
            WorkspaceChangeAction(
                proposal_id=successor.proposal_id,
                action="apply",
                expected_revision=0,
                request_key="upgraded-successor-apply",
            )
        )
        assert successor_apply.status == "done"
        attempts.append(successor_apply.model_dump(mode="json"))
        proposal = await agent.file_change(proposal.proposal_id)
        stamp = (state_dir / "files" / path).stat()
        kept = await agent.change_file(
            WorkspaceChangeAction(
                proposal_id=proposal.proposal_id,
                action="keep",
                expected_revision=proposal.revision,
                request_key="upgraded-keep",
            )
        )
        assert kept.status == "done" and kept.receipt is not None and not kept.receipt.changed
        assert kept.receipt.before_sha256 == proposal.preview.before_sha256
        assert kept.receipt.after_sha256 == proposal.preview.after_sha256
        assert kept.receipt.after_sha256 != successor.preview.after_sha256
        assert (await agent.read_file(path)).content == f"successor:{name}\n"
        assert (state_dir / "files" / path).stat() == stamp
        proof = await agent.file_change_proof(kept.operation_id)
        assert proof.kind == "successor_chain" and proof.successor_count == 1
        assert proof.tip_proposal_id == successor.proposal_id
        assert proof.tip_operation_id == successor_apply.operation_id
        assert proof.tip_after_sha256 == successor.preview.after_sha256
        proofs.append(proof.model_dump(mode="json"))
        attempts.append(kept.model_dump(mode="json"))
        for item, content in ((successor, f"upgraded:{name}\n"), (proposal, f"old:{name}\n")):
            current = await agent.file_change(item.proposal_id)
            restored = await agent.change_file(
                WorkspaceChangeAction(
                    proposal_id=current.proposal_id,
                    action="undo",
                    expected_revision=current.revision,
                    request_key=f"reverse-undo-{current.proposal_id}",
                )
            )
            assert restored.status == "done"
            assert (await agent.read_file(path)).content == content
            attempts.append(restored.model_dump(mode="json"))
        assert await agent.file_change_proof(kept.operation_id) == proof
        for proposal_id in (previous["proposal_id"], proposal.proposal_id, successor.proposal_id):
            for attempt in await agent.file_change_history(proposal_id):
                cleaned = await agent.finalize_file_change(attempt.operation_id)
                assert not cleaned.cleanup_pending
        second = await agent.generate_text(f"/echo restored-{name}", idempotency_key="second")
        assert second.run.status == "completed" and f"restored-{name}" in second.text
        assert second.run.id != previous["run_id"]
        assert [turn.turn for turn in await agent.turns()] == [1, 2]
    assert not list((state_dir / "files").glob("._krim-tx-*"))
    result = {
        "package": krim_agent.__file__,
        "migration": await migration(ai),
        "original_package": original["package"],
        "original_migration": original["migration"],
        "restored_agents": 2,
        "restore_provider_calls": 0,
        "provider_calls": provider.calls,
        "attempts": attempts,
        "proofs": proofs,
    }
    write_json(state_dir / "upgraded-receipts.json", result)
    return {key: value for key, value in result.items() if key != "attempts"}


def parse(state_dir, original):
    # 此路径供旧wheel执行：只打开JSON，不能让旧迁移器接触已升级数据库。
    assert schemas() == original["schemas"]
    values = json.loads((state_dir / "upgraded-receipts.json").read_text(encoding="utf-8"))
    assert len(values["attempts"]) == 14
    for value in values["attempts"]:
        attempt = WorkspaceAttemptView.model_validate_json(json.dumps(value))
        assert attempt.status == "done" and attempt.receipt is not None
        FileChangeReceipt.model_validate_json(json.dumps(value["receipt"]))
        for model, payload in (
            (WorkspaceAttemptView, value),
            (FileChangeReceipt, value["receipt"]),
        ):
            try:
                model.model_validate_json(json.dumps({**payload, "private_effect_leak": 1}))
            except ValidationError as error:
                assert any(item["type"] == "extra_forbidden" for item in error.errors())
            else:
                raise AssertionError("回执模型意外放宽了额外字段检查")
    return {"parsed_attempts": len(values["attempts"]), "strict_extra_rejected": True}


async def main():
    mode, directory = sys.argv[1:3]
    if mode not in {"create", "restore", "reject-tool", "reject-resource", "parse"}:
        raise ValueError("未知兼容验证模式")
    state_dir = await asyncio.to_thread(Path(directory).resolve)
    state_dir.mkdir(parents=True, exist_ok=True)
    if mode == "create":
        assert not (state_dir / "agents.sqlite").exists(), "create必须使用新的状态目录"
        (state_dir / "files").mkdir()
        original = None
    else:
        original = json.loads((state_dir / "original.json").read_text(encoding="utf-8"))
    if mode == "parse":
        result = parse(state_dir, original)
    else:
        provider = CountingProvider()
        tools = builtin_registry()
        if mode == "reject-tool":
            changed = ToolRegistry()
            for definition in tools.definitions:
                changed.register(
                    replace(definition, description="宿主已修改echo工具合约")
                    if definition.name == "echo"
                    else definition
                )
            tools = changed
        async with await PythonAI.create(
            settings=Settings(database_url=f"sqlite+aiosqlite:///{state_dir / 'agents.sqlite'}"),
            provider=provider,
            tools=tools,
        ) as ai:
            ai.runtime.tool_policy = allow
            ai.agents().register_workspace(
                "files",
                state_dir / "files",
                version="v2" if mode == "reject-resource" else "v1",
                writable=True,
                default=True,
            )
            if mode == "create":
                result = await create(ai, state_dir, provider)
            elif mode == "restore":
                result = await restore(ai, state_dir, original, provider)
            else:
                result = await reject(ai, original, provider, mode)
    print(json.dumps({"mode": mode, "package": krim_agent.__file__, **result}, ensure_ascii=False))


asyncio.run(main())
