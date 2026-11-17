"""End-to-end general session tools flow using real SQLite and scripted models.

No external network or paid model calls. The only network fixture is an explicitly
authorized loopback FastMCP server used for resource discovery/read.
"""

from __future__ import annotations

import asyncio
import json
import socket
import sys
import textwrap
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
from fastapi import FastAPI

from krim_agent.api.interaction import mount_interaction
from krim_agent.composition import open_application
from krim_agent.config import Settings
from krim_agent.contracts import JsonValue, Message
from krim_agent.interaction.contracts import Answer, AnswerRequest
from krim_agent.mcp import MCPServerConfig
from krim_agent.providers.types import ModelEvent, ModelText, ModelToolCall
from krim_agent.security import LOCAL
from krim_agent.sessions.types import SessionCreate, TurnRequest
from krim_agent.tools.runtime import ToolRuntimeConfig

MCP_SERVER_SCRIPT = textwrap.dedent(
    """
    import os
    from mcp.server.fastmcp import FastMCP
    server = FastMCP("docs", host="127.0.0.1",
                     port=int(os.environ["KRIM_FLOW_MCP_PORT"]),
                     streamable_http_path="/mcp")
    @server.tool()
    def doc_read(value: str) -> dict[str, str]:
        return {"doc": value}
    @server.resource("doc://brief")
    def brief() -> str:
        return "项目简报：目标流程集成证据。"
    server.run(transport="streamable-http")
    """
)


@asynccontextmanager
async def loopback_mcp(tmp_path: Path) -> AsyncIterator[str]:
    script = tmp_path / "flow_mcp_server.py"
    script.write_text(MCP_SERVER_SCRIPT, encoding="utf-8")
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        str(script),
        env={"KRIM_FLOW_MCP_PORT": str(port), "PATH": "/usr/local/bin:/usr/bin:/bin"},
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    try:
        deadline = asyncio.get_running_loop().time() + 20
        while True:
            try:
                _reader, writer = await asyncio.open_connection("127.0.0.1", port)
                writer.close()
                await writer.wait_closed()
                break
            except OSError:
                if asyncio.get_running_loop().time() > deadline or process.returncode is not None:
                    raise AssertionError(f"loopback MCP fixture failed to start: {port}") from None
                await asyncio.sleep(0.1)
        yield f"http://127.0.0.1:{port}/mcp"
    finally:
        if process.returncode is None:
            process.terminate()
        try:
            await asyncio.wait_for(process.wait(), timeout=5)
        except TimeoutError:
            process.kill()
            await process.wait()


class FlowProvider:
    mode = "scripted-general-flow"

    def __init__(self) -> None:
        self.requests: list[tuple[list[str], Sequence[Message]]] = []
        self.script: list = []
        self.closed = False

    async def stream(
        self, messages: Sequence[Message], tools: list[dict[str, JsonValue]]
    ) -> AsyncIterator[ModelEvent]:
        names = [item["name"] for item in tools]
        self.requests.append((names, tuple(messages)))
        step = self.script.pop(0) if self.script else None
        if step is None:
            yield ModelText(text="script-finished")
            return
        result = step(messages, names)
        events = result[-1] if isinstance(result, tuple) else result
        for event in events:
            yield event

    async def close(self) -> None:
        self.closed = True


def call(name: str, arguments: dict[str, JsonValue], index: int) -> ModelToolCall:
    return ModelToolCall(id=f"call-{index}", name=name, arguments=arguments)


def require_schemas(
    names: list[str], *, have: set[str] = frozenset(), absent: set[str] = frozenset()
) -> None:
    assert have <= set(names), f"missing schemas {have - set(names)}; actual={names}"
    assert not absent & set(names), f"unexpected schemas {absent & set(names)}"


def require_history(messages: Sequence[Message], *needles: str) -> None:
    # Complete tool results are earlier in history; the tail may be a budget prompt.
    history = "\n".join(str(item.content) for item in messages)
    for needle in needles:
        assert needle in history, f"{needle!r} missing from model history"


def allow_policy(_invocation):
    return "allow"


def app_settings() -> Settings:
    return Settings(
        run_timeout=None, tool_timeout=30, approval_timeout=60, max_request_bytes=262144
    )


async def open_flow_app(provider: FlowProvider, tmp_path: Path, name: str, mcp_url: str):
    config = ToolRuntimeConfig(
        mcp_servers=(
            MCPServerConfig(
                name="docs",
                transport="streamable_http",
                url=mcp_url,
                allowed_hosts=frozenset({"127.0.0.1"}),
                selected_tools=("doc_read",),
            ),
        ),
        web_result_root=tmp_path / "web-results",
    )
    return await open_application(
        tmp_path / name,
        settings=app_settings(),
        provider=provider,
        policy=allow_policy,
        tool_config=config,
    )


async def submit_turn(app, session, text: str, key: str):
    current = await app.sessions.get(LOCAL, "local", session.id)
    return await app.sessions.submit(
        LOCAL,
        "local",
        session.id,
        TurnRequest(text=text, request_key=key, expected_revision=current.revision),
    )


async def wait_turn_run(app, session, turn):
    """Dispatcher assigns run_id after admission; poll for it before reading the run."""
    async with asyncio.timeout(60):
        while True:
            current = await app.sessions.turn(LOCAL, "local", session.id, turn.id)
            if current.run_id:
                return current
            await asyncio.sleep(0.01)


async def wait_run(app, run_id: str, status: str):
    async with asyncio.timeout(60):
        while True:
            view = await app.service.get(run_id)
            if view.status == status:
                return view
            task = app.service.tasks.get(run_id)
            if (
                task is not None
                and task.done()
                and view.status not in {"running", "awaiting", status}
            ):
                events = await app.service.events(run_id)
                recent = [(e.kind, e.data.get("name"), e.data.get("code")) for e in events][-12:]
                raise AssertionError(f"run ended {view.status}/{view.code}; events={recent}")
            await asyncio.sleep(0.01)


async def wait_input_park(app, session, turn):
    async with asyncio.timeout(60):
        while True:
            current = await app.sessions.turn(LOCAL, "local", session.id, turn.id)
            if current.run_id:
                view = await app.service.get(current.run_id)
                if view.status == "awaiting":
                    state = await app.interactions.state(LOCAL, "local", session.id)
                    if state["pending"]:
                        return current.run_id, state["pending"][0]
                task = app.service.tasks.get(current.run_id)
                if task is not None and task.done() and view.status != "awaiting":
                    events = await app.service.events(current.run_id)
                    raise AssertionError(
                        [(e.kind, e.data.get("name"), e.data.get("code")) for e in events]
                    )
            await asyncio.sleep(0.01)


def tool_ends(events):
    return [e.data for e in events if e.kind == "tool_end"]


async def events_of(app, run_id: str):
    return list(await app.service.events(run_id))


async def wait_turn_with_prefix(app, session, prefix: str):
    async with asyncio.timeout(60):
        while True:
            page = await app.sessions.turns(LOCAL, "local", session.id, 0, 50)
            for turn in page:
                if turn.request_key.startswith(prefix):
                    return turn
            await asyncio.sleep(0.05)


async def wait_continuation_consumed(app, session, continue_key: str):
    """The dispatcher settles a finished internal turn on its next scan."""
    async with asyncio.timeout(60):
        while True:
            view = await app.goals.continuation_by_key(LOCAL, "local", session.id, continue_key)
            if view is not None and view.status == "consumed":
                return view
            await asyncio.sleep(0.05)


@asynccontextmanager
async def interaction_client(app):
    api = FastAPI()
    api.state.application = app

    @api.middleware("http")
    async def bind_local(request, call_next):
        request.state.principal = LOCAL
        return await call_next(request)

    mount_interaction(api)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=api), base_url="http://test"
    ) as client:
        yield client


async def test_single_session_chains_plan_memory_discovery_resource_goal(tmp_path):
    async with loopback_mcp(tmp_path) as mcp_url:
        provider = FlowProvider()
        provider.script = [
            lambda messages, names: (
                require_schemas(
                    names,
                    have={
                        "plan_update",
                        "plan_read",
                        "ask_user",
                        "memory_read",
                        "compact_context",
                        "goal_update",
                        "goal_status",
                        "tool_search",
                    },
                    absent={"docs__doc_read"},
                ),
                [
                    call(
                        "plan_update",
                        {
                            "request_key": "plan-1",
                            "expected_revision": 0,
                            "steps": [
                                {"id": "s1", "text": "收集集成证据", "status": "in_progress"},
                                {"id": "s2", "text": "完成目标", "status": "pending"},
                            ],
                        },
                        1,
                    )
                ],
            ),
            lambda messages, names: (
                require_history(messages, "revision", "in_progress"),
                [call("plan_read", {}, 2)],
            ),
            lambda messages, names: (
                require_history(messages, "s1", "收集集成证据"),
                [call("memory_read", {"memory_id": "missing"}, 3)],
            ),
            lambda messages, names: (
                require_history(messages, "disabled"),
                [call("tool_search", {"names": ["docs__doc_read"]}, 4)],
            ),
            lambda messages, names: (
                require_schemas(names, have={"docs__doc_read"}),
                [call("read_mcp_resource", {"server": "host:docs", "uri": "doc://brief"}, 5)],
            ),
            lambda messages, names: (
                require_history(messages, "项目简报：目标流程集成证据。"),
                [call("compact_context", {"reason": "flow-integration"}, 6)],
            ),
            lambda messages, names: (
                require_history(messages, "compact-context", "pending"),
                [
                    call(
                        "goal_update",
                        {
                            "request_key": "goal-create",
                            "expected_revision": 0,
                            "action": "set",
                            "objective": "完成通用工具链集成验证",
                            "completion_criteria": ["集成证据齐全"],
                        },
                        7,
                    )
                ],
            ),
            lambda messages, names: (
                require_history(messages, "active", "goal_id"),
                [
                    call(
                        "goal_update",
                        {
                            "request_key": "goal-complete",
                            "expected_revision": 1,
                            "action": "complete",
                            "completion_criteria": ["集成证据齐全"],
                            "evidence_refs": [{"kind": "test", "ref": "pytest:general-flow"}],
                        },
                        8,
                    )
                ],
            ),
            lambda messages, names: (
                require_history(messages, "completed"),
                [ModelText(text="done")],
            ),
        ]
        app = await open_flow_app(provider, tmp_path, "flow-chain.sqlite", mcp_url)
        try:
            session = await app.sessions.create(LOCAL, "local", SessionCreate(request_key="s"))
            turn = await submit_turn(app, session, "请完成通用工具链集成验证", "turn-1")
            initial = await wait_turn_run(app, session, turn)
            await wait_run(app, initial.run_id, "completed")
            saved = await app.sessions.turn(LOCAL, "local", session.id, turn.id)
            assert saved.status == "completed"
            assert "docs__doc_read" not in provider.requests[0][0]
            assert "docs__doc_read" in provider.requests[4][0]
            assert len(provider.requests) == 9
            ended = tool_ends(await events_of(app, saved.run_id))
            assert [e["name"] for e in ended] == [
                "plan_update",
                "plan_read",
                "memory_read",
                "tool_search",
                "read_mcp_resource",
                "compact_context",
                "goal_update",
                "goal_update",
            ]
            assert all(e["status"] == "ok" for e in ended)
            assert "disabled" in json.dumps(ended[2]["value"])
            assert "项目简报" in json.dumps(ended[4]["value"], ensure_ascii=False)
            plan = await app.planning.snapshot(LOCAL, "local", session.id)
            assert plan.revision == 1 and plan.counts.in_progress == 1
            goal = await app.goals.snapshot(LOCAL, "local", session.id)
            assert goal.status == "completed" and goal.origin_turn_id == turn.id
            assert [r.ref for r in goal.evidence_refs] == ["pytest:general-flow"]
            # The request is registered idempotently; the host boundary may already
            # have consumed it, so assert the durable tool result rather than timing.
            assert "compact-context" in json.dumps(ended[5]["value"])
            await asyncio.sleep(2.5)
            turns = await app.sessions.turns(LOCAL, "local", session.id, 0, 50)
            assert not [item for item in turns if item.request_key.startswith("goal-")]
        finally:
            await app.close()


async def test_ask_park_restart_restores_same_call_then_goal_continues(tmp_path):
    async with loopback_mcp(tmp_path) as mcp_url:
        provider = FlowProvider()
        provider.script = [
            lambda messages, names: (
                require_schemas(names, have={"goal_update", "ask_user"}),
                [
                    call(
                        "goal_update",
                        {
                            "request_key": "create",
                            "expected_revision": 0,
                            "action": "set",
                            "objective": "持续完成用户任务",
                            "completion_criteria": ["用户答复后完成"],
                        },
                        1,
                    )
                ],
            ),
            lambda messages, names: (
                require_history(messages, "goal_id"),
                [
                    call(
                        "ask_user",
                        {
                            "questions": [
                                {
                                    "id": "q1",
                                    "prompt": "选择继续方式",
                                    "mode": "text",
                                    "options": [],
                                    "allow_text": True,
                                }
                            ]
                        },
                        2,
                    )
                ],
            ),
            lambda messages, names: (
                require_schemas(names, have={"goal_update"}),
                require_history(messages, "restart-answer"),
                [
                    call(
                        "goal_update",
                        {
                            "request_key": "record-answer",
                            "expected_revision": 1,
                            "action": "progress",
                            "note": "收到恢复后的答复",
                            "evidence_refs": [{"kind": "user_message", "ref": "restart-answer"}],
                        },
                        3,
                    )
                ],
            ),
            lambda messages, names: (
                require_history(messages, "restart-answer"),
                [ModelText(text="用户答复已记录")],
            ),
            lambda messages, names: (
                require_history(messages, "内部续跑", "goal_id="),
                [
                    call(
                        "goal_update",
                        {
                            "request_key": "finish-goal",
                            "expected_revision": 2,
                            "action": "complete",
                            "completion_criteria": ["用户答复后完成"],
                            "evidence_refs": [{"kind": "test", "ref": "pytest:continued-goal"}],
                        },
                        4,
                    )
                ],
            ),
            lambda messages, names: (
                require_history(messages, "completed"),
                [ModelText(text="continued-done")],
            ),
        ]
        app = await open_flow_app(provider, tmp_path, "flow-restart.sqlite", mcp_url)
        session = await app.sessions.create(LOCAL, "local", SessionCreate(request_key="s"))
        turn = await submit_turn(app, session, "持续完成任务并在需要时询问", "turn-1")
        run_id, request = await wait_input_park(app, session, turn)
        assert len(provider.requests) == 2
        goal_before = await app.goals.snapshot(LOCAL, "local", session.id)
        assert goal_before.status == "active" and goal_before.origin_turn_id == turn.id
        await app.close()

        reopened = await open_flow_app(provider, tmp_path, "flow-restart.sqlite", mcp_url)
        try:
            assert (
                await reopened.goals.snapshot(LOCAL, "local", session.id)
            ).goal_id == goal_before.goal_id
            async with interaction_client(reopened) as client:
                response = await client.post(
                    f"/v2/workspaces/local/sessions/{session.id}/interactions/{request['id']}/answer",
                    json=AnswerRequest(
                        request_key="answer",
                        expected_revision=request["revision"],
                        answers=[Answer(question_id="q1", text="restart-answer")],
                    ).model_dump(mode="json"),
                )
            assert response.status_code == 200
            await wait_run(reopened, run_id, "completed")
            first_run_ends = tool_ends(await events_of(reopened, run_id))
            assert [e["name"] for e in first_run_ends] == ["goal_update", "ask_user", "goal_update"]
            assert len([e for e in first_run_ends if e["name"] == "ask_user"]) == 1
            # 2 initial requests + 2 resumed requests; the parked call was not replayed.
            assert len(provider.requests) == 4

            internal = await wait_turn_with_prefix(reopened, session, "goal-continue:")
            saved = await reopened.sessions.wait(LOCAL, "local", session.id, internal.id)
            assert saved.status == "completed"
            goal = await reopened.goals.snapshot(LOCAL, "local", session.id)
            assert goal.status == "completed" and goal.revision == 3
            consumed = await wait_continuation_consumed(
                reopened, session, internal.request_key.removeprefix("goal-continue:")
            )
            assert consumed.turn_id == internal.id
            assert consumed.outcome == "completed"
            assert consumed.goal_revision == goal.revision
            assert consumed.progressed is True
            settled_goal = await reopened.goals.snapshot(LOCAL, "local", session.id)
            assert settled_goal.continuation is not None
            assert settled_goal.continuation.status == "consumed"
            await asyncio.sleep(2.5)
            assert len(provider.requests) == 6
            turns = await reopened.sessions.turns(LOCAL, "local", session.id, 0, 50)
            assert (
                len([item for item in turns if item.request_key.startswith("goal-continue:")]) == 1
            )
        finally:
            await reopened.close()
