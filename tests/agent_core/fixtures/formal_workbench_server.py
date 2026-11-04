"""正式工作台浏览器验收用的本机真实会话服务。"""

import argparse
import asyncio
import hashlib
import json
import os
import socket
from collections.abc import AsyncIterator, Sequence
from dataclasses import replace
from pathlib import Path
from typing import cast

import uvicorn
from fastapi import Request
from pydantic import BaseModel, JsonValue, SecretStr

from krim_agent import __version__
from krim_agent.api.server import create_app
from krim_agent.composition import Application, open_application
from krim_agent.config import Settings
from krim_agent.contracts import Message, ServiceError
from krim_agent.providers.fake import FakeProvider
from krim_agent.providers.types import ModelEvent, ModelText, ModelToolCall
from krim_agent.security import AccessPolicy, Credential, Membership, PrincipalContext
from krim_agent.sessions.types import SessionCreate
from krim_agent.tools.builtins import builtin_registry
from krim_agent.tools.registry import ToolRegistry

ADMIN_KEY = "formal-workbench-admin-browser-key"
OPERATOR_KEY = "formal-workbench-operator-browser-key"
VIEWER_KEY = "formal-workbench-viewer-browser-key"
MEMBER_KEY = "formal-workbench-member-browser-key"
VIEWER_SESSION_TITLE = "formal-workbench viewer seed"
IMPLEMENTATION_DIGEST = hashlib.sha256(b"formal-workbench-counting-fixture/2").hexdigest()
MARKDOWN_RESPONSE = """# 正式工作台 Markdown 验证

正文支持 **强调**、表格、代码高亮与安全链接。

| 字段 | 值 |
| --- | --- |
| 来源 | 本机 FakeProvider |
| 渲染 | 正式消息组件 |

```python
value = "formal-workbench"
print(value)
```

![外部图片（只显示链接）](https://images.example.invalid/formal-workbench.png)

[不安全协议](javascript:alert(1))

<script>window.__formalMarkdownScriptRan = true</script>
"""


class FormalWorkbenchProvider(FakeProvider):
    """为正式 UI 验收合成少量固定回应，其余请求交给 FakeProvider。"""

    def __init__(self) -> None:
        super().__init__(delay=0.02)
        self.executions: list[dict[str, JsonValue]] = []

    async def stream(
        self, messages: Sequence[Message], tools: list[dict[str, JsonValue]]
    ) -> AsyncIterator[ModelEvent]:
        if not messages or messages[-1].role == "tool":
            async for event in super().stream(messages, tools):
                yield event
            return

        prompt = messages[-1].content.strip()
        errors = {
            "/protocol": "provider_protocol",
            "/tool-limit": "tool_limit",
            "/step-limit": "step_limit",
        }
        if prompt not in {"/markdown", "/two-notes", "/partial", *errors}:
            async for event in super().stream(messages, tools):
                yield event
            return

        self.calls += 1
        self.active += 1
        self.entered.set()
        try:
            await asyncio.sleep(self.delay)
            if prompt in errors:
                yield ModelText(text="已收到请求，正在处理工具调用。")
                raise ServiceError(errors[prompt])
            if prompt == "/markdown":
                yield ModelText(text=MARKDOWN_RESPONSE)
            elif prompt == "/two-notes":
                yield ModelToolCall(
                    id="formal-provider-note-welcome",
                    name="lookup_note",
                    arguments={"key": "welcome"},
                )
                yield ModelToolCall(
                    id="formal-provider-note-agent",
                    name="lookup_note",
                    arguments={"key": "agent"},
                )
            else:
                yield ModelText(text="```python\npartial_value = 42\n")
                await asyncio.Event().wait()
        finally:
            self.active -= 1
            self.streams_closed += 1

    def registry(self) -> ToolRegistry:
        values = []
        for spec, _policy in builtin_registry()._frozen:
            original = spec.executor

            async def counted(
                arguments: BaseModel, *, original=original, name: str = spec.name
            ) -> JsonValue:
                self.executions.append(
                    {
                        "name": name,
                        "arguments": cast(dict[str, JsonValue], arguments.model_dump(mode="json")),
                    }
                )
                if name == "echo" and getattr(arguments, "text", None) == "synthetic-tool-error":
                    raise RuntimeError("Synthetic tool executor failure")
                return await original(arguments)

            values.append(
                replace(
                    spec,
                    executor=counted,
                    implementation_digest=IMPLEMENTATION_DIGEST,
                )
            )
        return ToolRegistry(tuple(values))


def access_policy() -> AccessPolicy:
    def credential(principal_id: str, token: str, role: str, approve: bool) -> Credential:
        return Credential(
            principal_id=principal_id,
            token=SecretStr(token),
            generation=1,
            memberships=(Membership(workspace_id="local", role=role, approve=approve),),
        )

    return AccessPolicy(
        credentials=(
            credential("formal-admin", ADMIN_KEY, "admin", True),
            credential("formal-operator", OPERATOR_KEY, "member", True),
            credential("formal-viewer", VIEWER_KEY, "viewer", True),
            credential("formal-member", MEMBER_KEY, "member", False),
        )
    )


def save_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, path)


async def serve(args: argparse.Namespace) -> None:
    evidence = Path(args.evidence).resolve()
    database = Path(args.database).resolve()
    evidence.mkdir(parents=True, exist_ok=True)
    database.parent.mkdir(parents=True, exist_ok=True)

    provider = FormalWorkbenchProvider()
    app: Application | None = None
    listener: socket.socket | None = None
    server_task: asyncio.Task[None] | None = None
    server: uvicorn.Server | None = None
    port = args.port
    viewer_session_id = ""
    try:
        settings = Settings(
            database=database,
            max_active_runs=1,
            run_timeout=args.run_timeout,
            tool_timeout=args.tool_timeout,
            approval_timeout=args.approval_timeout,
            session_queue_timeout=120,
        )
        app = await open_application(
            database,
            settings=settings,
            provider=provider,
            registry=provider.registry(),
            access_policy=access_policy(),
        )
        assert app.sessions is not None
        # 仅内部种子创建时借用admin成员资格；该会话所有者仍是formal-viewer，HTTP viewer策略不变。
        viewer_session = await app.sessions.create(
            PrincipalContext(
                "formal-viewer",
                1,
                (Membership(workspace_id="local", role="admin", approve=True),),
            ),
            "local",
            SessionCreate(
                request_key="formal-viewer-seed-v1",
                title=VIEWER_SESSION_TITLE,
                target_key="demo",
            ),
        )
        viewer_session_id = viewer_session.id

        api = create_app(application=app)

        async def tool_counts(request: Request) -> dict[str, JsonValue]:
            actor = cast(PrincipalContext, request.state.principal)
            by_name: dict[str, int] = {}
            for item in provider.executions:
                name = str(item["name"])
                by_name[name] = by_name.get(name, 0) + 1
            return {
                "principal_id": actor.principal_id,
                "calls": provider.calls,
                "active": provider.active,
                "executions": cast(list[JsonValue], list(provider.executions)),
                "by_name": by_name,
                "limits": {"run": settings.run_timeout, "tool": settings.tool_timeout,
                           "approval": settings.approval_timeout},
            }

        # 该诊断路由仍经过 create_app 的 LocalBoundary 和 AccessPolicy 鉴权。
        api.add_api_route("/test/tool-counts", tool_counts, methods=["GET"])

        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", args.port))
        listener.listen(16)
        listener.setblocking(False)
        port = int(listener.getsockname()[1])
        server = uvicorn.Server(
            uvicorn.Config(
                api,
                access_log=False,
                host="127.0.0.1",
                log_level="warning",
                proxy_headers=False,
            )
        )
        server_task = asyncio.create_task(server.serve(sockets=[listener]), name="formal-workbench-api")
        async with asyncio.timeout(10):
            while not server.started:
                if server_task.done():
                    await server_task
                await asyncio.sleep(0.01)

        import krim_agent

        save_json(
            evidence / "ready.json",
            {
                "api_port": port,
                "url": f"http://127.0.0.1:{port}",
                "database": str(database),
                "module": krim_agent.__file__,
                "version": __version__,
                "provider": type(provider).__name__,
                "approval_timeout": args.approval_timeout,
                "run_timeout": args.run_timeout,
                "tool_timeout": args.tool_timeout,
                "viewer_seed_session_id": viewer_session_id,
                "viewer_seed_session_title": VIEWER_SESSION_TITLE,
            },
        )
        await server_task
    finally:
        if server_task is not None and not server_task.done():
            assert server is not None
            server.should_exit = True
            await server_task
        if listener is not None:
            listener.close()
        if app is not None:
            await app.close()
        await provider.close()

        closed = {
            "provider_closed": provider.closed,
            "provider_active": provider.active,
            "provider_streams_closed": provider.streams_closed,
            "run_tasks_done": app is None or all(task.done() for task in app.service.tasks.values()),
            "control_tasks_done": app is None or all(task.done() for task in app.sessions.controls.values()),
            "dispatcher_done": app is None or app.sessions._dispatcher.done(),
            "tool_executions": provider.executions,
        }
        save_json(evidence / "closed.json", closed)
        if app is not None:
            assert closed["run_tasks_done"] and closed["control_tasks_done"] and closed["dispatcher_done"]
        assert provider.closed and provider.active == 0
        if port:
            probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                assert probe.connect_ex(("127.0.0.1", port)) != 0, "fixture listener is still open"
            finally:
                probe.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="本机正式工作台 FakeProvider 验收服务")
    parser.add_argument("--database", required=True, help="本次验收专用 SQLite 数据库路径")
    parser.add_argument("--port", type=int, default=8879, help="仅监听 127.0.0.1 的端口")
    parser.add_argument("--evidence", required=True, help="保存 ready/closed 证据的目录")
    parser.add_argument(
        "--approval-timeout", type=float, default=20,
        help="工具审批等待秒数，可用于有限时长的过期用例",
    )
    parser.add_argument("--run-timeout", type=float, default=60)
    parser.add_argument("--tool-timeout", type=float, default=30)
    asyncio.run(serve(parser.parse_args()))


if __name__ == "__main__":
    main()
