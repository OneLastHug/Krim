"""最小端到端验收：真实 open_application → SessionService → spawn_agent 后台 worker。

不请求付费模型：主/worker provider 均为脚本化 fake（providers.types 协议）。
"""

import asyncio
from pathlib import Path
from typing import Any

from krim_agent.composition import open_application
from krim_agent.config import Settings
from krim_agent.contracts import TERMINAL
from krim_agent.providers.types import ModelText, ModelToolCall
from krim_agent.security import LOCAL
from krim_agent.sessions.types import SessionCreate, TurnRequest
from krim_agent.subagents.configuration import (
    SubagentProfileConfiguration,
    SubagentsConfiguration,
)
from krim_agent.tools.registry import ToolRegistry


class Scripted:
    """Fake ModelProvider: 按采样次序回放脚本；耗尽后默认收尾文本。"""

    def __init__(
        self, script: list[Any], fallback: str = "done", *, seen: asyncio.Event | None = None,
    ) -> None:
        self.script = list(script)
        self.fallback = fallback
        self.seen = seen
        self.requests = 0
        self.histories: list[list[Any]] = []

    async def stream(self, messages: Any, tools: Any) -> Any:
        index = self.requests
        self.requests += 1
        self.histories.append(list(messages))
        if self.seen is not None:
            self.seen.set()
        action = self.script[index] if index < len(self.script) else ModelText(text=self.fallback)
        if isinstance(action, BaseException):
            raise action
        yield action

    async def close(self) -> None:
        return None


async def settle(app: Any, session_id: str, turn_id: str, limit: float = 30.0) -> Any:
    """轮询到 run 进入终态；返回 (turn_view, run_view)。"""
    last: str | None = None
    probes = 0
    async with asyncio.timeout(limit):
        while True:
            current = await app.sessions.turn(LOCAL, "local", session_id, turn_id)
            probes += 1
            if current.run_id:
                view = await app.service.get(current.run_id)
                if view.status != last:
                    last = view.status
                    print(f"ROOT_STATUS -> {last}", flush=True)
                if view.status in TERMINAL:
                    return current, view
            elif probes in (5, 25, 100):
                dispatcher = getattr(app.sessions, "_dispatcher", None)
                print("NO_RUN_PROBE", probes, str(current)[:200],
                      "dispatcher_done=", None if dispatcher is None else dispatcher.done(),
                      "dispatcher_exc=", None if dispatcher is None or not dispatcher.done()
                      else repr(dispatcher.exception()), flush=True)
            await asyncio.sleep(0.1)


async def test_root_spawns_background_worker_end_to_end(tmp_path: Path) -> None:
    database = tmp_path / "subagent-integration.sqlite"
    main = Scripted([ModelToolCall(
        id="spawn-1", name="spawn_agent",
        arguments={"task": "Summarize the numbers.", "profile": "fast"},
    )])
    worker = Scripted([], fallback="worker report: 42", seen=asyncio.Event())
    app = await open_application(
        database,
        settings=Settings(database=database, run_timeout=60, tool_timeout=30),
        provider=main,
        registry=ToolRegistry(()),
        subagent_configuration=SubagentsConfiguration(
            enabled=True,
            profiles={"fast": SubagentProfileConfiguration(model="fast")},
            default_profile="fast",
        ),
        model_routes={"fast": worker},
    )
    print("APP_OPEN subagents=", app.subagents is not None, flush=True)
    try:
        session = await app.sessions.create(LOCAL, "local", SessionCreate(request_key="root-s"))
        turn = await app.sessions.submit(LOCAL, "local", session.id, TurnRequest(
            text="Delegate the summary in the background.",
            request_key="root-t", expected_revision=0,
        ))
        print("TURN_SUBMITTED", turn.id, flush=True)
        async with asyncio.timeout(15):
            await worker.seen.wait()
        assert "Delegate the summary in the background." in str(main.histories[0][-1]), \
            "root request lost the user task"
        assert "Summarize the numbers." in str(worker.histories[0][-1]), \
            "worker request lost the delegated task"

        # 修复后根应持开等待后代：worker 结果附入下一次根采样，根完成后才终态。
        current, view = await settle(app, session.id, turn.id, limit=45.0)
        assert view.status == "completed", f"root settled {view.status}: {view}"
        assert worker.requests >= 1, "worker never sampled"

        events = await app.service.events(current.run_id)
        ends = [event for event in events if event.kind == "tool_end"
                and event.data.get("name") == "spawn_agent"]
        assert ends, f"spawn_agent never executed; status={view.status} kinds=" \
            f"{[e.kind for e in events]}"
        receipt = ends[-1].data
        assert receipt.get("status") == "ok" and receipt.get("value", {}).get("ok") is True, \
            f"spawn rejected: {receipt}"

        # Every subsequent request retains history. Count notification IDs in
        # the final request, rather than counting the same history across calls.
        from krim_agent.contracts import parse_json

        notices = []
        for message in main.histories[-1]:
            if message.role == "user" and message.content.startswith("{"):
                envelope = parse_json(message.content)
                if isinstance(envelope, dict) and envelope.get("source") == "subagent_result":
                    notices.append(envelope)
        assert len(notices) == 1, "worker result must be attached exactly once"
        assert notices[0]["result"]["output"]["text"] == "worker report: 42"
        root_saw_result = True
        print(f"ROOT_STATUS={view.status} MAIN_REQUESTS={main.requests} "
              f"WORKER_REQUESTS={worker.requests} ROOT_SAW_RESULT={root_saw_result}")
    finally:
        # drain/dispose 已修（dispose 单次共享 cleanup Task）；限时防挂起，
        # 失败原样上抛，不断言、不吞。
        await asyncio.wait_for(app.close(), 20)
