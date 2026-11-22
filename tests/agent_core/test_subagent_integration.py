"""root_merge 验收：fake port + 真实 SQLiteStore/repository，8 个场景。

不验证真实容器；整合算法复用 workspace.py，测试只覆盖 helper 的授权、
CAS、重放与 unknown 语义。
"""

from __future__ import annotations

import base64
import hashlib
import time
import uuid
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from sqlalchemy import insert, select, update
from test_subagent_team import write

from krim_agent.config import Settings
from krim_agent.context.types import ContextPolicy
from krim_agent.contracts import RunRequest, ServiceError, fingerprint
from krim_agent.security import Membership, PrincipalContext
from krim_agent.sessions.types import SessionCreate
from krim_agent.storage import sessions as session_sql
from krim_agent.storage.models import run_events
from krim_agent.storage.sqlite import SQLiteStore
from krim_agent.storage.subagent_models import subagent_tasks
from krim_agent.subagents.integration import root_merge
from krim_agent.subagents.service import Caller
from krim_agent.subagents.storage import SubagentStore
from krim_agent.subagents.types import ReserveAgent, TreeScope
from krim_agent.subagents.workspace import WorkspaceIsolation
from krim_agent.tools.core import Invocation

WORKSPACE = "local"
OWNER = "local"
ACTOR = PrincipalContext(WORKSPACE, 1, (Membership(workspace_id=WORKSPACE, role="admin"),))
PROJECT = "proj-0001-1111-2222-333344444444"


class FakePort:
    """内存版 WorkspacePort；write 带 CAS，写计数供重放断言。"""

    def __init__(self, files: dict[str, str]) -> None:
        self._files = {f"{PROJECT}:{path}": text for path, text in files.items()}
        self.writes = 0

    async def read(
        self, project_id: str, path: str, *, offset: int = 0, limit: int = 65536
    ) -> dict[str, Any]:
        content = self._files.get(f"{project_id}:{path}")
        if content is None:
            raise ServiceError("not_found", 404)
        data = content.encode("utf-8")
        chunk = data[offset:offset + limit]
        return {
            "data": base64.b64encode(chunk).decode("ascii"),
            "sha256": hashlib.sha256(data).hexdigest(),
            "size": len(data),
            "eof": offset + len(chunk) >= len(data),
        }

    async def entries(self, project_id: str, path: str, *, offset: int = 0) -> dict[str, Any]:
        prefix = "" if path in ("", ".") else path.rstrip("/") + "/"
        found: set[tuple[str, str]] = set()
        for key in self._files:
            project, _, relative = key.partition(":")
            if project != project_id or not relative.startswith(prefix):
                continue
            rest = relative[len(prefix):]
            if not rest:
                continue
            if "/" in rest:
                found.add(("directory", rest.split("/", 1)[0]))
            else:
                found.add(("file", rest))
        return {
            "entries": [{"name": name, "type": kind} for kind, name in sorted(found)],
            "next_offset": None,
        }

    async def make_directory(self, project_id: str, path: str) -> dict[str, Any]:
        return {"path": path}

    async def write(
        self, project_id: str, path: str, content: str, *, expected_sha256: str | None = None
    ) -> dict[str, Any]:
        key = f"{project_id}:{path}"
        current = self._files.get(key)
        if expected_sha256 is not None:
            digest = (
                hashlib.sha256(current.encode("utf-8")).hexdigest()
                if current is not None
                else None
            )
            if digest != expected_sha256:
                raise ServiceError("input_revision_conflict")
        self._files[key] = content
        self.writes += 1
        return {"path": path}

    async def remove(self, project_id: str, path: str, *, expected_sha256: str) -> dict[str, Any]:
        self._files.pop(f"{project_id}:{path}", None)
        return {"path": path}

    async def git_head(self, project_id: str) -> str | None:
        return None


def invocation(run_id: str, call_id: str = "merge-call") -> Invocation:
    return Invocation(
        run_id=run_id, call_id=call_id, span_id="span", step_index=0,
        name="root_merge_task", arguments_json="{}", tool_hash="tool-hash",
    )


async def scenario(
    tmp_path: Path, *, name: str = "integration.sqlite", event_shape: str = "process"
):
    store = await SQLiteStore.open(tmp_path / name)
    session = await session_sql.create_session(
        store, WORKSPACE, OWNER,
        SessionCreate(request_key=f"merge-{name}"),
        {"key": "demo"}, ContextPolicy(), Settings(),
    )
    repository = SubagentStore(store)
    tree = await repository.ensure_tree(
        TreeScope(session_id=session.id, workspace_id=WORKSPACE, owner_id=OWNER,
                  request_key=f"root:{name}")
    )
    registration_hint = ReserveAgent(
        tree_id=tree["tree_id"], path="lead", role="root",
        provider="route-fake", profile="worker", provider_version="v1",
        profile_version=fingerprint({}), request_key=f"spawn:{name}:root",
    )
    root = await repository.reserve_agent(registration_hint)
    agent = await repository.reserve_agent(
        ReserveAgent(
            tree_id=tree["tree_id"], parent_id=root["agent"]["agent_id"], path="lead/w1",
            role="agent", provider="route-fake", profile="worker", provider_version="v1",
            profile_version=fingerprint({}), request_key=f"spawn:{name}:w1",
        )
    )
    agent_id = agent["agent"]["agent_id"]
    port = FakePort({"app.py": "print('v1')\n"})
    isolation = WorkspaceIsolation(port)
    assignment, baseline = await isolation.assign(
        "isolated_write", source_project_id=PROJECT, agent_id=agent_id, input_version=1
    )
    await port.write(
        PROJECT, f"{assignment.directory}/app.py", "print('v2')\n"
    )
    patch = await isolation.collect(assignment, baseline)
    child, _ = await store.begin(
        RunRequest(request_key=f"child:{name}", text="child run", target="demo"),
        {"key": "demo"},
    )
    root_run, _ = await store.begin(
        RunRequest(request_key=f"rootrun:{name}", text="root run", target="demo"),
        {"key": "demo"},
    )
    task_id = str(uuid.uuid4())
    document = {
        "goal": "isolated work",
        "baseline": baseline.model_dump(mode="json"),
        "workspace": assignment.model_dump(mode="json"),
    }

    async def seed_task(conn: Any) -> None:
        await conn.execute(
            insert(subagent_tasks).values(
                id=task_id, agent_id=agent_id, tree_id=tree["tree_id"], ordinal=1,
                status="completed", run_id=child.id, parent_run_id=root_run.id,
                source_version=1, provider_version="v1", profile_version="v1",
                input=document, input_hash=fingerprint(document),
                request_key=f"seed:{task_id}", request_hash=fingerprint({"t": task_id}),
                revision=2, input_closed=True,
                created_at=time.time(), finished_at=time.time(),
            )
        )

    await write(store, seed_task)
    if event_shape == "dump":
        # WorkspacePatch.model_dump 的真实形状（键 fingerprint、含
        # workspace_project_id），外层 {"patch": …} 包裹一并覆盖。
        payload = {"patch": patch.model_dump(mode="json")}
    else:
        # providers/process.py 当前 emit 的顶层形状。
        payload = {
            "patch_fingerprint": patch.fingerprint,
            "baseline_fingerprint": baseline.fingerprint,
            "source_project_id": PROJECT,
            "directory": assignment.directory,
            "mode": "isolated_write",
            "input_version": 1,
            "truncated": False,
            "diffs_included": True,
            "changes": [],
        }
    await store.append(child.id, "workspace_patch", payload,
                       event_id=f"seed-patch:{name}")
    caller = Caller(
        member={"agent_id": root["agent"]["agent_id"], "role": "root"},
        tree={"tree_id": tree["tree_id"], "owner_id": OWNER, "workspace_id": WORKSPACE,
              "project_id": None, "session_id": session.id},
        scope=SimpleNamespace(run_id=root_run.id, check=lambda: None),
    )
    return SimpleNamespace(
        store=store, ids=tree, port=port, patch=patch, baseline=baseline,
        caller=caller, root_run=root_run.id, task_id=task_id, session=session,
    )


async def test_root_only_caller_may_merge(tmp_path: Path) -> None:
    env = await scenario(tmp_path)
    try:
        agent_caller = Caller(
            member={"agent_id": "agent", "role": "agent"},
            tree=env.caller.tree,
            scope=SimpleNamespace(run_id=env.root_run, check=lambda: None),
        )
        with pytest.raises(ServiceError, match="permission_denied"):
            await root_merge(
                env.store, agent_caller, invocation(env.root_run),
                env.task_id, env.patch.fingerprint, env.port,
            )
        result = await root_merge(
            env.store, env.caller, invocation(env.root_run),
            env.task_id, env.patch.fingerprint, env.port,
        )
        assert result["state"] == "integrated"
        assert result["tests_pending"] is True
        assert result["replayed"] is False
        assert result["post_fingerprint"] is None or len(result["post_fingerprint"]) == 64
    finally:
        await env.store.close()


async def test_cross_tree_task_is_not_visible(tmp_path: Path) -> None:
    env = await scenario(tmp_path)
    try:
        foreign_root_run, _ = await env.store.begin(
            RunRequest(request_key="foreign-root", text="x", target="demo"), {"key": "demo"}
        )
        foreign_caller = Caller(
            member={"agent_id": "root", "role": "root"},
            tree={"tree_id": str(uuid.uuid4()), "owner_id": OWNER, "workspace_id": WORKSPACE,
                  "project_id": None, "session_id": env.session.id},
            scope=SimpleNamespace(run_id=foreign_root_run.id, check=lambda: None),
        )
        with pytest.raises(ServiceError, match="not_found"):
            await root_merge(
                env.store, foreign_caller, invocation(foreign_root_run.id),
                env.task_id, env.patch.fingerprint, env.port,
            )
    finally:
        await env.store.close()


async def test_unsettled_task_is_rejected(tmp_path: Path) -> None:
    env = await scenario(tmp_path)
    try:
        async def reopen(conn: Any) -> None:
            await conn.execute(
                update(subagent_tasks).where(subagent_tasks.c.id == env.task_id)
                .values(status="running")
            )

        await write(env.store, reopen)
        with pytest.raises(ServiceError, match="invalid_input"):
            await root_merge(
                env.store, env.caller, invocation(env.root_run),
                env.task_id, env.patch.fingerprint, env.port,
            )
    finally:
        await env.store.close()


async def test_patch_fingerprint_mismatch_rejected_before_intent(tmp_path: Path) -> None:
    env = await scenario(tmp_path)
    try:
        with pytest.raises(ServiceError, match="input_revision_conflict"):
            await root_merge(
                env.store, env.caller, invocation(env.root_run),
                env.task_id, "0" * 64, env.port,
            )
        digest = fingerprint(
            {"run": env.root_run, "call": "merge-call", "task": env.task_id}
        )

        async def no_intent(conn: Any) -> bool:
            rows = (
                await conn.execute(
                    select(run_events.c.run_id).where(
                        run_events.c.run_id == env.root_run,
                        run_events.c.event_id == f"integrate-intent:{digest}",
                    )
                )
            ).all()
            return not rows

        # 指纹不匹配在任何写入之前拒绝：同 key 的 intent 事件不存在。
        assert await env.store.work_read(no_intent) is True
    finally:
        await env.store.close()


async def test_successful_merge_applies_with_cas_and_replays_receipt(tmp_path: Path) -> None:
    env = await scenario(tmp_path, event_shape="dump")
    try:
        first = await root_merge(
            env.store, env.caller, invocation(env.root_run),
            env.task_id, env.patch.fingerprint, env.port,
        )
        assert first["state"] == "integrated"
        assert first["post_fingerprint"] is not None
        assert first["tests_pending"] is True
        writes_after_first = env.port.writes
        replay = await root_merge(
            env.store, env.caller, invocation(env.root_run, "merge-call"),
            env.task_id, env.patch.fingerprint, env.port,
        )
        assert replay["replayed"] is True
        assert replay["state"] == "integrated"
        assert replay["post_fingerprint"] == first["post_fingerprint"]
        assert env.port.writes == writes_after_first
        content = await env.port.read(PROJECT, "app.py")
        assert base64.b64decode(content["data"]).decode("utf-8") == "print('v2')\n"
    finally:
        await env.store.close()


async def test_baseline_drift_records_outcome_and_replays(tmp_path: Path) -> None:
    env = await scenario(tmp_path)
    try:
        # 基线快照后源文件被用户改动：整合如实报 baseline_changed，不改补丁。
        await env.port.write(PROJECT, "app.py", "user edit\n")
        drifted = await root_merge(
            env.store, env.caller, invocation(env.root_run),
            env.task_id, env.patch.fingerprint, env.port,
        )
        assert drifted["state"] == "baseline_changed"
        assert drifted["post_fingerprint"] is None
        replay = await root_merge(
            env.store, env.caller, invocation(env.root_run),
            env.task_id, env.patch.fingerprint, env.port,
        )
        assert replay["replayed"] is True and replay["state"] == "baseline_changed"
    finally:
        await env.store.close()


async def test_unknown_intent_blocks_retry(tmp_path: Path) -> None:
    env = await scenario(tmp_path)
    try:
        digest = fingerprint(
            {"run": env.root_run, "call": "unknown-call", "task": env.task_id}
        )
        await env.store.append(
            env.root_run, "workspace_integrate_intent",
            {"task_id": env.task_id}, event_id=f"integrate-intent:{digest}",
        )
        with pytest.raises(ServiceError, match="execution_unknown"):
            await root_merge(
                env.store, env.caller, invocation(env.root_run, "unknown-call"),
                env.task_id, env.patch.fingerprint, env.port,
            )
        content = await env.port.read(PROJECT, "app.py")
        assert base64.b64decode(content["data"]).decode("utf-8") == "print('v1')\n"
    finally:
        await env.store.close()
