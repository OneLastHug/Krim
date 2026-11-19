"""Local formal-UI verification: real managed sessions/ACL/approval APIs, synthetic tools."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import tempfile
import time
import uuid
from pathlib import Path

import uvicorn
from pydantic import BaseModel
from sqlalchemy import insert, select

from krim_agent.accounts.service import IdentityService
from krim_agent.api.server import create_app
from krim_agent.composition import open_application
from krim_agent.config import Settings
from krim_agent.contracts import StrictModel
from krim_agent.project_access.policy import initialize_project_access, set_project_access
from krim_agent.providers.fake import FakeProvider
from krim_agent.providers.types import ModelText, ModelToolCall
from krim_agent.sessions.types import SessionCreate
from krim_agent.storage.sandbox_models import sandbox_projects, sandbox_session_bindings
from krim_agent.storage.sqlite import SQLiteStore
from krim_agent.tools.builtins import builtin_registry
from krim_agent.tools.core import ToolSource, ToolSpec
from krim_agent.tools.registry import ToolRegistry


class LocalAction(StrictModel):
    text: str = "permission UI fixture"


class PermissionProvider(FakeProvider):
    def __init__(self):
        super().__init__()
        self.executions = 0

    async def stream(self, messages, tools):
        last = max(
            (index for index, item in enumerate(messages) if item.role == "user"), default=-1
        )
        if last < 0 or not messages[last].content.startswith("/write"):
            async for item in super().stream(messages, tools):
                yield item
            return
        self.calls += 1
        for message in messages[last + 1 :]:
            if message.role != "tool":
                continue
            try:
                value = json.loads(message.content)
            except ValueError:
                continue
            nested = value.get("value", {}) if isinstance(value, dict) else {}
            if isinstance(value, dict) and (
                value.get("permission_ui_done")
                or (isinstance(nested, dict) and nested.get("permission_ui_done"))
            ):
                yield ModelText(text="本机测试操作已完成。")
                return
            if isinstance(value, dict) and value.get("code") in {
                "permission_denied",
                "project_readonly",
                "approval_denied",
            }:
                yield ModelText(text="此项目权限不允许执行该操作，未发生写入。")
                return
        names = {item.get("name") for item in tools}
        if "fixture_write" not in names:
            yield ModelToolCall(
                id="discover-" + str(self.calls),
                name="tool_search",
                arguments={"query": "fixture_write"},
            )
        else:
            yield ModelToolCall(
                id="local-action-" + str(self.calls),
                name="fixture_write",
                arguments={"text": "permission UI fixture"},
            )

    def registry(self):
        async def placeholder(_values: BaseModel):
            return {}

        async def execute(_invocation, _values: BaseModel):
            self.executions += 1
            return {"permission_ui_done": True, "executions": self.executions}

        spec = ToolSpec(
            "fixture_write",
            LocalAction,
            placeholder,
            {"type": "object"},
            "1",
            hashlib.sha256(b"permission-ui-fixture/1").hexdigest(),
            risk="write",
            source=ToolSource("mcp", "permission-ui", "fixture_write", "f" * 64),
            external_executor=execute,
            description="Synthetic local action for permission UI verification",
        )
        return ToolRegistry(tuple(item for item, _ in builtin_registry()._frozen) + (spec,))


async def main(args):
    evidence = await asyncio.to_thread(Path(args.evidence).absolute)
    evidence.mkdir(parents=True, exist_ok=True)
    run = evidence / ("run-" + uuid.uuid4().hex[:10])
    run.mkdir()
    # The existing WSL distro lives on F:. SQLite's writer-lock protocol needs
    # its native filesystem rather than a Windows-mounted DrvFS directory.
    database = Path(tempfile.gettempdir()) / "krim-permission-ui" / run.name / "ui.sqlite"
    database.parent.mkdir(parents=True)
    store = await SQLiteStore.open(database)
    identity = await IdentityService.open(store)
    bootstrap = await identity.bootstrap("权限联调账号", request_key="fixture-bootstrap")
    token = bootstrap.token
    await store.close()
    provider = PermissionProvider()
    async with await open_application(
        database,
        settings=Settings(database=database),
        provider=provider,
        registry=provider.registry(),
        policy=allow,
    ) as app:
        actor = app.identities.authenticate("Bearer " + token)
        workspace = actor.memberships[0].workspace_id
        first = await app.sessions.create(
            actor, workspace, SessionCreate(request_key="fixture-a", title="权限联调会话 A")
        )
        async with app.store.lock, app.store.engine.connect() as conn:
            project_id = await conn.scalar(
                select(sandbox_session_bindings.c.project_id).where(
                    sandbox_session_bindings.c.session_id == first.id
                )
            )
            original = dict(
                (
                    await conn.execute(
                        select(sandbox_projects).where(sandbox_projects.c.project_id == project_id)
                    )
                )
                .mappings()
                .one()
            )
        second = await app.sessions.create(
            actor,
            workspace,
            SessionCreate(request_key="fixture-b", title="权限联调会话 B", project_id=project_id),
        )
        readonly_project = str(uuid.uuid4())

        async def seed(conn):
            await conn.execute(
                insert(sandbox_projects).values(
                    **(
                        original
                        | {
                            "project_id": readonly_project,
                            "title": "只读参考项目与较长标题的弹窗排版验证",
                            "created_at": time.time(),
                            "updated_at": time.time(),
                        }
                    )
                )
            )
            await initialize_project_access(conn, readonly_project, actor.principal_id)
            await set_project_access(
                conn, actor, readonly_project, actor.principal_id, "read", expected_revision=1
            )

        await app.store._write(seed)
        readonly = await app.sessions.create(
            actor,
            workspace,
            SessionCreate(
                request_key="fixture-read", title="只读权限验证", project_id=readonly_project
            ),
        )
        api = create_app(application=app)
        (evidence / "ready.json").write_text(
            json.dumps(
                {
                    "fixture_key": token,
                    "workspace": workspace,
                    "session_a": first.id,
                    "session_b": second.id,
                    "readonly_session": readonly.id,
                    "database": str(database),
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        await uvicorn.Server(
            uvicorn.Config(
                api, host="127.0.0.1", port=args.port, access_log=False, log_level="warning"
            )
        ).serve()
    await provider.close()


async def allow(_invocation):
    return "allow"


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=5201)
    parser.add_argument("--evidence", default=".data/permission-ui")
    asyncio.run(main(parser.parse_args()))
