"""子代理正式UI本机fixture：真实app/存储/域服务，脚本化模型，不接网络。

真实路径：open_application（subagents启用+worker路由）→ SessionService.submit
→ 真实 spawn_agent → 真实child run与RunStore事件 → 真实settle_result。
UI静态资源由create_app默认挂载src/krim_agent/web；不覆写任何正式API。
托管身份经IdentityService.bootstrap签发一次性本地token（非真实用户），
UI在"输入访问密钥"弹窗粘贴登录；无公开免鉴权后门。

启动（WSL venv，在worktree根目录）：
  /home/krim/.venvs/krim-agent/bin/python \
    tests/agent_core/fixtures/subagent_ui_server.py \
    --host 0.0.0.0 --port 5195 --evidence .cache/subagent-validation/ui-bootstrap.json
stdout输出一行JSON：{url, token, workspace, session, database, evidence, agents}。
数据库每次启动重建：/tmp/krim-subagent-ui/fixture-<port>/ui.sqlite（不碰共享数据）。

场景（脚本路由按task内容，非mock fetch，list/detail/control读真实DB/服务）：
  lead/reporter  completed：真实assistant文本事件 + settle_result结果
  lead/runner    running：模型采样阻塞，interrupt后随child run取消收束
  lead/failing   error：模型流抛错，走真实错误结算
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import socket
import sys
import tempfile
from collections.abc import AsyncIterator, Sequence
from contextlib import nullcontext
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "src"))

import uvicorn
from pydantic import BaseModel, JsonValue
from sqlalchemy import func, select

from krim_agent.accounts.service import IdentityService
from krim_agent.api.server import create_app
from krim_agent.composition import open_application
from krim_agent.config import Settings
from krim_agent.contracts import StrictModel
from krim_agent.planning.contracts import PlanStep, PlanUpdate
from krim_agent.providers.types import ModelEvent, ModelText, ModelToolCall
from krim_agent.sessions.types import SessionCreate, TurnRequest
from krim_agent.storage.sqlite import SQLiteStore
from krim_agent.storage.subagent_models import (
    subagent_members,
    subagent_results,
    subagent_tasks,
    subagent_trees,
)
from krim_agent.subagents.configuration import (
    SubagentProfileConfiguration,
    SubagentsConfiguration,
)
from krim_agent.tools.core import ToolSpec
from krim_agent.tools.executor import ToolExecutor
from krim_agent.tools.registry import ToolRegistry

WORKSPACE_REQUEST_KEY = "subagent-ui-fixture"


class RootProvider:
    """根采样脚本：首个user请求派三名成员，其后阻塞保持树开放直到取消。"""

    def __init__(self) -> None:
        self.calls = 0

    async def stream(
        self, messages: Sequence[Any], tools: list[dict[str, JsonValue]]
    ) -> AsyncIterator[ModelEvent]:
        self.calls += 1
        last = messages[-1]
        if last.role == "user" and not any(m.role == "assistant" for m in messages):
            yield ModelToolCall(
                id="spawn-done",
                name="spawn_agent",
                arguments={
                    "task": "fixture-worker-done：汇总基础数据并给出最终结论。",
                    "name": "reporter",
                },
            )
            yield ModelToolCall(
                id="spawn-hold",
                name="spawn_agent",
                arguments={
                    "task": "fixture-worker-running：保持运行，等待后续指令或中断。",
                    "name": "runner",
                },
            )
            yield ModelToolCall(
                id="spawn-error",
                name="spawn_agent",
                arguments={
                    "task": "fixture-worker-error：本成员按脚本触发模型失败。",
                    "name": "failing",
                },
            )
            yield ModelToolCall(
                id="spawn-approval",
                name="spawn_agent",
                arguments={
                    "task": "fixture-worker-approval：等待批准后执行本地检查。",
                    "name": "approval",
                },
            )
            return
        await asyncio.Event().wait()  # 阻塞保持根运行；取消随run任务一起结束
        yield ModelText(text="联调根任务结束。")

    async def close(self) -> None:
        return None


class WorkerProvider:
    """worker路由脚本：按task内容给出completed/running/error三种真实行为。"""

    def __init__(self) -> None:
        self.calls = 0

    async def stream(
        self, messages: Sequence[Any], tools: list[dict[str, JsonValue]]
    ) -> AsyncIterator[ModelEvent]:
        self.calls += 1
        task = messages[-1].content
        if "fixture-worker-approval" in task:
            yield ModelToolCall(id="local-check", name="fixture_check", arguments={})
        elif any(message.role == "tool" for message in messages):
            yield ModelText(text="本地检查已收到审批结果。")
        elif "fixture-worker-running" in task:
            await asyncio.Event().wait()  # 运行中场景：保持采样直到该run被取消
            yield ModelText(text="runner 已停止。")
        elif "fixture-worker-error" in task:
            raise RuntimeError("fixture worker model failure")
        else:
            await asyncio.sleep(0.2)
            yield ModelText(text="reporter 已完成任务：本机fixture数据汇总结论。")

    async def close(self) -> None:
        return None


class FixtureCheck(StrictModel):
    pass


async def fixture_check(_values: BaseModel) -> JsonValue:
    return {"checked": True}


async def fixture_policy(invocation: Any) -> str:
    return "ask" if invocation.name == "fixture_check" else "allow"


class FixtureServer(uvicorn.Server):
    def capture_signals(self):
        return nullcontext()


def save_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def database_path(args: argparse.Namespace) -> Path:
    if args.database:
        return Path(args.database).resolve()
    base = (
        Path("/tmp/krim-subagent-ui")
        if os.name != "nt"
        else Path(tempfile.gettempdir()) / "krim-subagent-ui"
    )
    return base / f"fixture-{args.port}" / "ui.sqlite"


async def seed_snapshot(app: Any, session_id: str) -> dict[str, Any]:
    async def read(conn: Any) -> dict[str, Any]:
        tree_id = await conn.scalar(
            select(subagent_trees.c.id).where(subagent_trees.c.session_id == session_id)
        )
        if tree_id is None:
            return {"members": [], "results": 0}
        rows = (
            (
                await conn.execute(
                    select(
                        subagent_members.c.id,
                        subagent_members.c.path,
                        subagent_members.c.role,
                        subagent_members.c.current_task_id,
                    ).where(subagent_members.c.tree_id == tree_id)
                )
            )
            .mappings()
            .all()
        )
        statuses = {
            row["task_id"]: row["status"]
            for row in (
                await conn.execute(
                    select(subagent_tasks.c.id.label("task_id"), subagent_tasks.c.status).where(
                        subagent_tasks.c.tree_id == tree_id
                    )
                )
            )
            .mappings()
            .all()
        }
        results = int(
            await conn.scalar(
                select(func.count())
                .select_from(subagent_results)
                .join(subagent_tasks, subagent_results.c.task_id == subagent_tasks.c.id)
                .where(subagent_tasks.c.tree_id == tree_id)
            )
        )
        members = [
            {
                "agent_id": row["id"],
                "path": row["path"],
                "role": row["role"],
                "task_status": statuses.get(row["current_task_id"]),
            }
            for row in rows
        ]
        return {"members": members, "results": results}

    return await app.store.work_read(read)


async def wait_seed(app: Any, session_id: str, limit: float = 30.0) -> dict[str, Any]:
    """等待真实spawn落库：root+3成员、reporter/failing已结算、无未启动任务。"""

    async def settled() -> bool:
        snapshot = await seed_snapshot(app, session_id)
        members = snapshot["members"]
        agents = [m for m in members if m["role"] == "agent"]
        return (
            len(agents) == 4
            and snapshot["results"] >= 2
            and all(m["task_status"] not in (None, "accepted") for m in agents)
        )

    try:
        async with asyncio.timeout(limit):
            while not await settled():  # noqa: ASYNC110 - observe durable DB state
                await asyncio.sleep(0.2)
    except TimeoutError:
        raise RuntimeError("seed未在限时内落库：检查subagents配置与worker路由是否生效") from None
    return await seed_snapshot(app, session_id)


async def serve(args: argparse.Namespace) -> None:
    evidence = await asyncio.to_thread(Path(args.evidence).resolve)
    database = database_path(args)
    database.parent.mkdir(parents=True, exist_ok=True)
    # Only reset the exact fixture files; a custom existing database is never overwritten.
    if args.database and database.exists():
        raise RuntimeError("自定义数据库已存在，请使用新的fixture数据库路径")
    if not args.database:
        for suffix in ("", "-wal", "-shm"):
            database.with_name(database.name + suffix).unlink(missing_ok=True)

    store = await SQLiteStore.open(database)
    identity = await IdentityService.open(store)
    try:
        bootstrap = await identity.bootstrap("子代理联调", request_key=WORKSPACE_REQUEST_KEY)
        token = bootstrap.token
    finally:
        await identity.close()
        await store.close()
    if not token:
        raise RuntimeError("fixture bootstrap未签发token")

    root_provider, worker_provider = RootProvider(), WorkerProvider()
    app = None
    listener: socket.socket | None = None
    server_task: asyncio.Task[None] | None = None
    try:
        app = await open_application(
            database,
            settings=Settings(database=database),
            provider=root_provider,
            registry=ToolRegistry(
                (
                    ToolSpec(
                        "fixture_check",
                        FixtureCheck,
                        fixture_check,
                        {"type": "object"},
                        "1",
                        "a" * 64,
                        description="Run a local fixture check without side effects.",
                    ),
                )
            ),
            policy=fixture_policy,
            subagent_configuration=SubagentsConfiguration(
                enabled=True,
                profiles={"worker": SubagentProfileConfiguration(model="worker")},
                default_profile="worker",
            ),
            model_routes={"worker": worker_provider},
        )
        assert app.sessions is not None and app.identities is not None
        open_tools = app.service.run_tool_factory

        async def require_fixture_approval(scope: Any, base: Any) -> Any:
            runtime = await open_tools(scope, base)
            original = runtime.executor

            async def policy(invocation: Any) -> Any:
                decision = await original.policy(invocation) if original.policy else "allow"
                if invocation.name == "fixture_check" and decision == "allow":
                    return "ask"
                return decision

            runtime.executor = ToolExecutor(
                original.registry, policy, original.pre, original.post,
                middleware=original.middleware, wrap=original.wrap,
            )
            return runtime

        app.service.run_tool_factory = require_fixture_approval
        actor = app.identities.authenticate("Bearer " + str(token))
        workspace = actor.memberships[0].workspace_id
        session = await app.sessions.create(
            actor,
            workspace,
            SessionCreate(request_key=WORKSPACE_REQUEST_KEY, title="子代理后台任务联调"),
        )
        await app.sessions.submit(
            actor,
            workspace,
            session.id,
            TurnRequest(
                text="并行派出三个后台成员分别完成、保持运行与触发失败；你保持等待。",
                request_key=WORKSPACE_REQUEST_KEY + "-turn",
                expected_revision=0,
            ),
        )
        await app.planning.update(
            actor,
            workspace,
            session.id,
            PlanUpdate(
                request_key="fixture-plan",
                expected_revision=0,
                steps=(
                    PlanStep(id="summary", text="汇总基础数据", status="completed"),
                    PlanStep(id="review", text="检查后台执行与取消", status="in_progress"),
                    PlanStep(id="approval", text="完成需要批准的本地检查", status="blocked"),
                ),
            ),
        )
        seed = await wait_seed(app, session.id)

        api = create_app(application=app)
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind((args.host, args.port))
        listener.listen(16)
        listener.setblocking(False)
        port = int(listener.getsockname()[1])
        server = FixtureServer(
            uvicorn.Config(
                api,
                host=args.host,
                port=port,
                access_log=False,
                log_level="warning",
                proxy_headers=False,
            )
        )
        server_task = asyncio.create_task(
            server.serve(sockets=[listener]), name="subagent-ui-fixture"
        )
        async with asyncio.timeout(10):
            while not server.started:
                if server_task.done():
                    await server_task
                await asyncio.sleep(0.01)

        payload = {
            "url": f"http://127.0.0.1:{port}/ui/",
            "token": str(token),
            "workspace": workspace,
            "session": session.id,
            "database": str(database),
            "evidence": str(evidence),
            "agents": seed["members"],
            "note": "token为本机fixture一次性凭据，仅用于本机UI登录；数据库每次启动重建",
        }
        save_json(evidence, payload)
        print(json.dumps(payload, ensure_ascii=False), flush=True)
        await server_task
    finally:
        if server_task is not None and not server_task.done():
            await asyncio.shield(server_task)
        if listener is not None:
            listener.close()
        if app is not None:
            await asyncio.wait_for(app.close(), 20)
        await root_provider.close()
        await worker_provider.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="子代理正式UI本机联调fixture")
    parser.add_argument("--host", default="0.0.0.0", help="监听地址（WSL需0.0.0.0）")
    parser.add_argument("--port", type=int, default=5195)
    parser.add_argument("--evidence", default=".cache/subagent-validation/ui-bootstrap.json")
    parser.add_argument("--database", default="", help="覆盖默认 /tmp/krim-subagent-ui 路径")
    asyncio.run(serve(parser.parse_args()))


if __name__ == "__main__":
    main()
