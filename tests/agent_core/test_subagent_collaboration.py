"""子代理协作的端到端契约：真实 open_application → SessionService → RunService → SDK。

主/worker 模型均为脚本化 fake（providers.types 协议），不产生付费请求。
并发时序只用 asyncio.Event 门控：fake provider 进入采样时发事件，脚本步可按
事件等待后再产出；测试不靠 sleep 推断交错。所有等待 15s 超时，app.close 20s。
"""

from __future__ import annotations

import asyncio
import hashlib
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast

from pydantic import BaseModel, Field, JsonValue

from krim_agent.composition import open_application
from krim_agent.config import Settings
from krim_agent.contracts import TERMINAL, StrictModel, parse_json
from krim_agent.providers.types import ModelText, ModelToolCall
from krim_agent.security import LOCAL
from krim_agent.sessions.types import SessionCreate, TurnRequest
from krim_agent.subagents.configuration import (
    SubagentProfileConfiguration,
    SubagentsConfiguration,
)
from krim_agent.tools.core import Decision, Invocation, ToolSpec
from krim_agent.tools.registry import ToolRegistry


@dataclass
class Step:
    """一次采样的脚本：进入即置 entered，等待门控事件，再产出或抛错。"""

    deltas: tuple[Any, ...] = ()
    entered: asyncio.Event | None = None
    wait: tuple[asyncio.Event, ...] = ()
    raises: BaseException | None = None
    done: tuple[asyncio.Event, ...] = ()


class Scripted:
    """按请求次序回放；脚本耗尽后用 fallback 文本正常收尾。"""

    def __init__(
        self,
        steps: Sequence[Step] = (),
        *,
        fallback: str = "done",
        route: Callable[[list[Any], int], Step] | None = None,
    ) -> None:
        self.steps = list(steps)
        self.fallback = fallback
        self.route = route
        self.requests = 0
        self.histories: list[list[Any]] = []

    async def stream(self, messages: Any, tools: Any) -> Any:
        index = self.requests
        self.requests += 1
        history = list(messages)
        self.histories.append(history)
        if self.route is not None:
            step = self.route(history, index)
        elif index < len(self.steps):
            step = self.steps[index]
        else:
            step = Step(deltas=(ModelText(text=self.fallback),))
        if step.entered is not None:
            step.entered.set()
        for gate in step.wait:
            async with asyncio.timeout(15):
                await gate.wait()
        for marker in step.done:
            marker.set()
        if step.raises is not None:
            raise step.raises
        for delta in step.deltas:
            yield delta

    async def close(self) -> None:
        return None


class ReadProbe(StrictModel):
    path: str = Field(min_length=1, max_length=256)


@dataclass
class Probe:
    """根侧独立 read 工具的观测：执行时记录协作方门控状态。"""

    executed: asyncio.Event = field(default_factory=asyncio.Event)
    observed: dict[str, Any] = field(default_factory=dict)


def probe_registry(
    probe: Probe,
    enter_flags: dict[str, asyncio.Event],
    release: asyncio.Event,
) -> ToolRegistry:
    async def read_probe(arguments: BaseModel) -> JsonValue:
        args = cast(ReadProbe, arguments)
        probe.observed["path"] = args.path
        probe.observed["workers_entered"] = all(
            event.is_set() for event in enter_flags.values()
        )
        probe.observed["workers_released"] = release.is_set()
        probe.executed.set()
        return {"path": args.path, "content": "probe"}

    digest = hashlib.sha256(b"test-subagent-collaboration-read-probe").hexdigest()
    return ToolRegistry(
        (
            ToolSpec(
                "read_probe",
                ReadProbe,
                read_probe,
                {"type": "object"},
                "1",
                digest,
                description="Read a host-provided probe path without touching the network.",
            ),
        )
    )


async def allow_read_probe(invocation: Invocation) -> Decision:
    return "allow" if invocation.name == "read_probe" else "deny"


def worker_configuration() -> SubagentsConfiguration:
    return SubagentsConfiguration(
        enabled=True,
        profiles={"worker": SubagentProfileConfiguration(model="worker")},
        default_profile="worker",
        max_resident_agents=4,
    )


def worker_routes(provider: Scripted) -> dict[str, Any]:
    return {"worker": provider}


async def settle(app: Any, session_id: str, turn_id: str, limit: float = 15.0) -> Any:
    """轮询到根 run 进入终态；只用于等待已由事件确定的收尾，不推断交错。"""
    async with asyncio.timeout(limit):
        while True:
            current = await app.sessions.turn(LOCAL, "local", session_id, turn_id)
            if current.run_id:
                view = await app.service.get(current.run_id)
                if view.status in TERMINAL:
                    return current, view
            await asyncio.sleep(0.05)


def result_notices(history: Sequence[Any], task_id: str | None = None) -> list[dict[str, Any]]:
    found: list[dict[str, Any]] = []
    for message in history:
        if message.role != "user" or not message.content.startswith("{"):
            continue
        envelope = parse_json(message.content)
        if not isinstance(envelope, dict) or envelope.get("source") != "subagent_result":
            continue
        if task_id is None or envelope.get("task_id") == task_id:
            found.append(envelope)
    return found


async def tool_receipts(app: Any, run_id: str, name: str) -> list[dict[str, Any]]:
    events = await app.service.events(run_id)
    return [
        event.data
        for event in events
        if event.kind == "tool_end" and event.data.get("name") == name
    ]


async def test_two_workers_overlap_root_read_step(tmp_path: Path) -> None:
    """根同一步 spawn 两个 worker；worker 已进入模型采样并阻塞时，根的下一次
    模型请求已进入并完成独立 read 工具；放行后两 worker 才产出结果。"""
    database = tmp_path / "subagent-overlap.sqlite"
    a_entered, b_entered = asyncio.Event(), asyncio.Event()
    release = asyncio.Event()
    probe = Probe()
    root_second = asyncio.Event()

    worker = Scripted(
        [
            Step(entered=a_entered, wait=(release,), deltas=(ModelText(text="worker a report"),)),
            Step(entered=b_entered, wait=(release,), deltas=(ModelText(text="worker b report"),)),
        ]
    )
    main = Scripted(
        [
            Step(
                deltas=(
                    ModelToolCall(
                        id="spawn-a",
                        name="spawn_agent",
                        arguments={"task": "collect alpha", "name": "a"},
                    ),
                    ModelToolCall(
                        id="spawn-b",
                        name="spawn_agent",
                        arguments={"task": "collect beta", "name": "b"},
                    ),
                )
            ),
            Step(
                entered=root_second,
                wait=(a_entered, b_entered),
                deltas=(
                    ModelToolCall(
                        id="read-1", name="read_probe", arguments={"path": "shared-note"}
                    ),
                ),
            ),
            Step(deltas=(ModelText(text="root interim"),)),
        ]
    )
    app = await open_application(
        database,
        settings=Settings(database=database, run_timeout=60, tool_timeout=30),
        provider=main,
        registry=probe_registry(probe, {"a": a_entered, "b": b_entered}, release),
        policy=allow_read_probe,
        subagent_configuration=worker_configuration(),
        model_routes=worker_routes(worker),
    )
    try:
        session = await app.sessions.create(
            LOCAL, "local", SessionCreate(request_key="overlap-session")
        )
        turn = await app.sessions.submit(
            LOCAL,
            "local",
            session.id,
            TurnRequest(
                text="Run both collectors while you keep reading.",
                request_key="overlap-turn",
                expected_revision=0,
            ),
        )
        async with asyncio.timeout(15):
            await probe.executed.wait()
        assert probe.observed == {
            "path": "shared-note",
            "workers_entered": True,
            "workers_released": False,
        }, f"root read ran without confirmed worker overlap: {probe.observed}"
        assert root_second.is_set(), "root's next model request was not in progress"
        assert worker.requests == 2, f"both workers must sample before release: {worker.requests}"

        current = await app.sessions.turn(LOCAL, "local", session.id, turn.id)
        assert current.run_id is not None
        spawns = await tool_receipts(app, current.run_id, "spawn_agent")
        assert len(spawns) == 2
        for item in spawns:
            assert item["status"] == "ok", item
            assert item["value"]["ok"] is True and item["value"]["accepted"] is True, item
        receipts = [item["value"] for item in spawns]
        for receipt in receipts:
            task = await app.subagents.repository.task(receipt["task_id"])
            assert task["status"] == "running", task
            run = await app.service.get(task["run_id"])
            assert run.status == "running", run
            events = await app.service.events(task["run_id"])
            starts = [event for event in events if event.kind == "model_start"]
            assert len(starts) == 1, "worker must have entered its model request"

        release.set()
        current, view = await settle(app, session.id, turn.id)
        assert view.status == "completed", f"root settled {view.status}: {view}"

        agent_ids = {receipt["agent_id"] for receipt in receipts}
        assert len(agent_ids) == 2
        task_ids = {receipt["task_id"] for receipt in receipts}
        final = [envelope for envelope in result_notices(main.histories[-1])
                 if envelope["task_id"] in task_ids]
        assert len(final) == 2, f"root must see both results: {final}"
        assert {envelope["result"]["output"]["text"] for envelope in final} == {
            "worker a report",
            "worker b report",
        }
        for task_id in task_ids:
            result = await app.subagents.repository.result(task_id)
            assert result is not None and result["stop_reason"] == "completed", result
        assert worker.requests == 2, "each worker samples exactly once"
    finally:
        await asyncio.wait_for(app.close(), 20)


async def until(predicate: Any, *, limit: float = 15.0) -> Any:
    """等待一个已定序的持久条件成立；不用于推断模型/工具交错。"""
    async with asyncio.timeout(limit):
        while True:
            value = await predicate()
            if value:
                return value
            await asyncio.sleep(0.05)


def message_inputs(history: Sequence[Any]) -> list[dict[str, Any]]:
    found: list[dict[str, Any]] = []
    for message in history:
        if message.role != "user" or not message.content.startswith("{"):
            continue
        envelope = parse_json(message.content)
        if isinstance(envelope, dict) and envelope.get("source") == "subagent_message":
            found.append(envelope)
    return found


async def open_app(database: Path, main: Scripted, worker: Scripted, **kwargs: Any) -> Any:
    return await open_application(
        database,
        settings=Settings(database=database, run_timeout=60, tool_timeout=30),
        provider=main,
        subagent_configuration=worker_configuration(),
        model_routes=worker_routes(worker),
        **kwargs,
    )


async def first_turn(app: Any, text: str, key: str) -> Any:
    session = await app.sessions.create(LOCAL, "local", SessionCreate(request_key=key + "-s"))
    turn = await app.sessions.submit(
        LOCAL,
        "local",
        session.id,
        TurnRequest(text=text, request_key=key, expected_revision=0),
    )
    return session, turn


async def test_busy_send_waits_for_closed_step(tmp_path: Path) -> None:
    """worker 采样中发送的消息不打断本次采样，只在下一个闭合 step 进入输入；
    根最终结果恰好一次，不重复。"""
    database = tmp_path / "subagent-busy-send.sqlite"
    worker_entered, worker_second = asyncio.Event(), asyncio.Event()
    release, root_after_send = asyncio.Event(), asyncio.Event()
    steer = "keep the collector focused"

    worker = Scripted(
        [
            Step(
                entered=worker_entered,
                wait=(release,),
                deltas=(ModelText(text="worker first answer"),),
            ),
            Step(entered=worker_second, deltas=(ModelText(text="worker final report"),)),
        ]
    )
    main = Scripted(
        [
            Step(
                deltas=(
                    ModelToolCall(
                        id="spawn-1",
                        name="spawn_agent",
                        arguments={"task": "collect metrics", "name": "w1"},
                    ),
                )
            ),
            Step(
                wait=(worker_entered,),
                deltas=(
                    ModelToolCall(
                        id="steer-1",
                        name="send_message",
                        arguments={"target": "lead/w1", "text": steer},
                    ),
                ),
            ),
            Step(entered=root_after_send, deltas=(ModelText(text="root interim"),)),
        ]
    )
    app = await open_app(database, main, worker)
    try:
        session, turn = await first_turn(app, "Delegate collection, then steer it.", "busy")
        async with asyncio.timeout(15):
            await root_after_send.wait()
        # 发送已执行且 worker 首次采样仍未放行：消息不可能已进入第一次采样。
        assert not release.is_set()
        assert worker.histories and steer not in str(worker.histories[0]), \
            "message leaked into the open first sampling"
        current = await app.sessions.turn(LOCAL, "local", session.id, turn.id)
        assert current.run_id is not None
        sends = await tool_receipts(app, current.run_id, "send_message")
        assert len(sends) == 1, sends
        assert sends[0]["status"] == "ok", sends
        receipt = sends[0]["value"]
        assert receipt["ok"] is True and receipt["accepted"] is True
        assert receipt["wake"] == "auto" and receipt["scheduled"] is True

        release.set()
        async with asyncio.timeout(15):
            await worker_second.wait()
        second = worker.histories[1]
        injected = [item for item in message_inputs(second)
                    if item["body"].get("text") == steer]
        assert len(injected) == 1, f"busy send must enter exactly the next closed step: {injected}"
        assert injected[0]["task_id"] == receipt["task_id"]

        current, view = await settle(app, session.id, turn.id)
        assert view.status == "completed", f"root settled {view.status}: {view}"
        notices = result_notices(main.histories[-1], receipt["task_id"])
        assert len(notices) == 1, f"root result duplicated or missing: {notices}"
        assert notices[0]["result"]["output"]["text"] == "worker final report"
        result = await app.subagents.repository.result(receipt["task_id"])
        assert result is not None and result["stop_reason"] == "completed", result
        agent_id = receipt["target"]["agent_id"]
        tasks = await app.subagents.repository.tasks(agent_id)
        assert len(tasks) == 1, "the busy message must not create a second task"
        assert worker.requests == 2
    finally:
        await asyncio.wait_for(app.close(), 20)


async def test_new_task_continuation_and_send_key_replay(tmp_path: Path) -> None:
    """完成后的 worker 用 new_task=True 续派：同一 agent 保留历史；同一 send_key
    重试返回原 message/task，不重复执行。"""
    database = tmp_path / "subagent-new-task.sqlite"
    worker_second = asyncio.Event()

    worker = Scripted(
        [
            Step(deltas=(ModelText(text="worker first answer"),)),
            Step(entered=worker_second, deltas=(ModelText(text="worker second answer"),)),
        ]
    )
    follow_up = {
        "target": "lead/w3",
        "text": "follow up with the numbers",
        "new_task": True,
        "send_key": "follow-up-1",
    }
    main = Scripted(
        [
            Step(
                deltas=(
                    ModelToolCall(
                        id="spawn-1",
                        name="spawn_agent",
                        arguments={"task": "collect the first numbers", "name": "w3"},
                    ),
                )
            ),
            Step(
                deltas=(
                    ModelToolCall(id="wait-1", name="wait_agent", arguments={"seconds": 3}),
                )
            ),
            Step(
                deltas=(ModelToolCall(id="send-1", name="send_message", arguments=follow_up),)
            ),
            Step(
                deltas=(
                    ModelToolCall(id="send-2", name="send_message", arguments=dict(follow_up)),
                )
            ),
            Step(deltas=(ModelText(text="root interim"),)),
        ]
    )
    app = await open_app(database, main, worker)
    try:
        session, turn = await first_turn(app, "Collect once, then continue.", "new-task")
        # spawn → wait_agent → 两次 send → worker 续跑 → 收尾采样序列较长，
        # 根终态预算沿用集成验收的 45s；事件门控等待仍为 15s。
        current, view = await settle(app, session.id, turn.id, limit=45.0)
        assert view.status == "completed", f"root settled {view.status}: {view}"
        async with asyncio.timeout(15):
            await worker_second.wait()

        spawns = await tool_receipts(app, current.run_id, "spawn_agent")
        assert len(spawns) == 1 and spawns[0]["status"] == "ok", spawns
        agent_id = spawns[0]["value"]["agent_id"]
        first_task = spawns[0]["value"]["task_id"]

        sends = await tool_receipts(app, current.run_id, "send_message")
        assert [item["status"] for item in sends] == ["ok", "ok"], sends
        first, retry = (item["value"] for item in sends)
        assert first["accepted"] is True and first["created"] is True
        assert retry["accepted"] is True and retry["created"] is False
        assert retry["message_id"] == first["message_id"]
        assert retry["seq"] == first["seq"]
        assert retry["task_id"] == first["task_id"]
        assert first["task_id"] != first_task

        tasks = await app.subagents.repository.tasks(agent_id)
        assert [task["task_id"] for task in tasks] == [first_task, first["task_id"]], \
            "replay must not create a duplicate task"
        first_task_row = await app.subagents.repository.task(first_task)
        second_task_row = await app.subagents.repository.task(first["task_id"])
        assert first_task_row["status"] == "completed"
        assert second_task_row["created_at"] >= first_task_row["finished_at"]
        assert "worker first answer" in str(second_task_row["input"]["history"]), \
            "continuation must keep the completed worker history"
        assert worker.requests == 2, "replay must not execute a duplicate run"
        second_request = worker.histories[1]
        assert "worker first answer" in str(second_request)
        assert "follow up with the numbers" in str(second_request[-1])

        for task_id, answer in ((first_task, "worker first answer"),
                                (first["task_id"], "worker second answer")):
            result = await app.subagents.repository.result(task_id)
            assert result is not None and result["stop_reason"] == "completed", result
            assert result["output"]["text"] == answer
        notices = result_notices(main.histories[-1], first["task_id"])
        assert len(notices) == 1, f"second task result must arrive once: {notices}"
        assert notices[0]["agent_id"] == agent_id
    finally:
        await asyncio.wait_for(app.close(), 20)


async def test_queue_only_task_does_not_start_or_block_root(tmp_path: Path) -> None:
    """queue_only 新任务不启动 worker，也不延长根任务；根完成时由 drain 以
    member_closed 结算，不作为成功结果进入根输入。"""
    database = tmp_path / "subagent-queue-only.sqlite"
    worker_second = asyncio.Event()
    worker = Scripted(
        [
            Step(deltas=(ModelText(text="worker first answer"),)),
            Step(entered=worker_second, deltas=(ModelText(text="worker queued answer"),)),
        ]
    )
    queued = {
        "target": "lead/w4",
        "text": "queued follow-up",
        "new_task": True,
        "wake": "queue_only",
        "send_key": "queued-1",
    }
    main = Scripted(
        [
            Step(
                deltas=(
                    ModelToolCall(
                        id="spawn-1",
                        name="spawn_agent",
                        arguments={"task": "collect once", "name": "w4"},
                    ),
                )
            ),
            Step(
                deltas=(
                    ModelToolCall(id="wait-1", name="wait_agent", arguments={"seconds": 3}),
                )
            ),
            Step(deltas=(ModelToolCall(id="send-1", name="send_message", arguments=queued),)),
            Step(deltas=(ModelText(text="root final"),)),
        ]
    )
    app = await open_app(database, main, worker)
    try:
        session, turn = await first_turn(app, "Collect once; queue the follow-up.", "queue-only")
        current, view = await settle(app, session.id, turn.id, limit=45.0)
        assert view.status == "completed", f"root must not wait for a queue_only task: {view}"

        spawns = await tool_receipts(app, current.run_id, "spawn_agent")
        sends = await tool_receipts(app, current.run_id, "send_message")
        assert len(spawns) == 1 and len(sends) == 1, (spawns, sends)
        task_id = sends[0]["value"]["task_id"]
        assert sends[0]["status"] == "ok" and sends[0]["value"]["accepted"] is True
        assert sends[0]["value"]["wake"] == "queue_only"
        assert sends[0]["value"]["scheduled"] is False
        assert sends[0]["value"]["action"].startswith("Queued only")
        assert not worker_second.is_set(), "queue_only must not start the worker"
        assert worker.requests == 1

        settled = await until(
            lambda: _terminal_task(app, task_id), limit=15.0
        )
        assert settled["status"] == "aborted", settled
        assert settled["run_id"] is None
        result = await app.subagents.repository.result(task_id)
        assert result is not None
        assert result["stop_reason"] == "aborted", result
        assert result["code"] == "member_closed", result
        assert result["output"]["text"] == ""
        assert result["output"].get("reason") == "member_closed"

        for history in main.histories:
            assert result_notices(history, task_id) == [], \
                "a drained queue_only task must not surface as a successful result"
        agent_id = sends[0]["value"]["target"]["agent_id"]
        tasks = await app.subagents.repository.tasks(agent_id)
        assert [task["task_id"] for task in tasks][-1] == task_id
        assert len(tasks) == 2
    finally:
        await asyncio.wait_for(app.close(), 20)


async def _terminal_task(app: Any, task_id: str) -> dict[str, Any] | None:
    task = await app.subagents.repository.task(task_id)
    return task if task["status"] in TERMINAL_TASK_STATUS else None


TERMINAL_TASK_STATUS = frozenset(
    {"completed", "aborted", "error", "max_tokens", "refusal", "interrupted"}
)


async def test_sibling_failure_does_not_cancel_worker(tmp_path: Path) -> None:
    """一个 worker 模型失败不取消兄弟；根同时看到 error 与 completed 两条结果。"""
    database = tmp_path / "subagent-sibling-failure.sqlite"
    a_failed, b_entered = asyncio.Event(), asyncio.Event()
    failing = Scripted(
        [
            Step(
                entered=asyncio.Event(),
                done=(a_failed,),
                raises=RuntimeError("worker a model exploded"),
            ),
        ]
    )
    surviving = Scripted(
        [
            Step(
                entered=b_entered,
                wait=(a_failed,),
                deltas=(ModelText(text="worker b report"),),
            ),
        ]
    )
    main = Scripted(
        [
            Step(
                deltas=(
                    ModelToolCall(
                        id="spawn-a",
                        name="spawn_agent",
                        arguments={"task": "fragile job", "name": "fa", "profile": "fail"},
                    ),
                    ModelToolCall(
                        id="spawn-b",
                        name="spawn_agent",
                        arguments={"task": "steady job", "name": "ok", "profile": "ok"},
                    ),
                )
            ),
        ]
    )
    configuration = SubagentsConfiguration(
        enabled=True,
        profiles={
            "fail": SubagentProfileConfiguration(model="fail"),
            "ok": SubagentProfileConfiguration(model="ok"),
        },
        default_profile="ok",
        max_resident_agents=4,
    )
    app = await open_application(
        database,
        settings=Settings(database=database, run_timeout=60, tool_timeout=30),
        provider=main,
        subagent_configuration=configuration,
        model_routes={"fail": failing, "ok": surviving},
    )
    try:
        session, turn = await first_turn(app, "Try both jobs.", "siblings")
        current, view = await settle(app, session.id, turn.id, limit=45.0)
        assert view.status == "completed", f"root settled {view.status}: {view}"
        async with asyncio.timeout(15):
            await b_entered.wait()

        spawns = await tool_receipts(app, current.run_id, "spawn_agent")
        assert len(spawns) == 2 and all(item["status"] == "ok" for item in spawns)
        by_name = {item["value"]["path"]: item["value"] for item in spawns}
        broken, steady = by_name["lead/fa"], by_name["lead/ok"]
        assert broken["agent_id"] != steady["agent_id"]

        broken_result = await app.subagents.repository.result(broken["task_id"])
        steady_result = await app.subagents.repository.result(steady["task_id"])
        assert broken_result is not None and broken_result["stop_reason"] == "error", \
            broken_result
        assert steady_result is not None and steady_result["stop_reason"] == "completed", \
            steady_result
        assert steady_result["output"]["text"] == "worker b report"
        assert surviving.requests == 1, "sibling must run despite the failure"

        final = result_notices(main.histories[-1])
        by_task = {envelope["task_id"]: envelope for envelope in final}
        assert set(by_task) == {broken["task_id"], steady["task_id"]}
        assert by_task[broken["task_id"]]["result"]["stop_reason"] == "error"
        assert by_task[steady["task_id"]]["result"]["stop_reason"] == "completed"
        assert by_task[steady["task_id"]]["result"]["output"]["text"] == "worker b report"
        steady_task = await app.subagents.repository.task(steady["task_id"])
        steady_run = await app.service.get(steady_task["run_id"])
        assert steady_run.status == "completed", steady_run
    finally:
        await asyncio.wait_for(app.close(), 20)


async def _released_agent(app: Any, agent_id: str) -> dict[str, Any] | None:
    agent = await app.subagents.repository.get_agent(agent_id)
    activation = agent.get("activation")
    return agent if activation is not None and activation["status"] == "released" else None


async def test_new_user_turn_reuses_tree_and_cold_restores_worker(tmp_path: Path) -> None:
    """显式新用户 turn 在同一 session 复用协作树：list_agents 看到旧 worker，
    send_message(new_task) 冷恢复同一稳定 agent（新 activation 实例、保留历史），
    不新建代理。"""
    database = tmp_path / "subagent-cross-turn.sqlite"
    turn_two_text = "Continue with the same worker."
    worker_sampled = asyncio.Event()
    worker = Scripted(
        [
            Step(deltas=(ModelText(text="turn one answer"),)),
            Step(entered=worker_sampled, deltas=(ModelText(text="turn two answer"),)),
        ]
    )
    state = {"spawned": False, "turn_two": 0}

    def route(history: list[Any], index: int) -> Step:
        last_user = next(
            (message.content for message in reversed(history) if message.role == "user"), ""
        )
        if last_user == turn_two_text:
            state["turn_two"] += 1
            if state["turn_two"] == 1:
                return Step(
                    deltas=(ModelToolCall(id="list-1", name="list_agents", arguments={}),)
                )
            if state["turn_two"] == 2:
                return Step(
                    deltas=(
                        ModelToolCall(
                            id="send-1",
                            name="send_message",
                            arguments={
                                "target": "lead/w6",
                                "text": "second turn follow up",
                                "new_task": True,
                                "send_key": "turn-2",
                            },
                        ),
                    )
                )
            return Step(deltas=(ModelText(text="root turn two done"),))
        if not state["spawned"]:
            state["spawned"] = True
            return Step(
                deltas=(
                    ModelToolCall(
                        id="spawn-1",
                        name="spawn_agent",
                        arguments={"task": "first turn task", "name": "w6"},
                    ),
                )
            )
        return Step(deltas=(ModelText(text="root turn one done"),))

    main = Scripted(route=route)
    app = await open_app(database, main, worker)
    try:
        session, turn1 = await first_turn(app, "Start the first turn.", "turn-one")
        current1, view1 = await settle(app, session.id, turn1.id, limit=45.0)
        assert view1.status == "completed", f"first turn settled {view1.status}: {view1}"
        spawns = await tool_receipts(app, current1.run_id, "spawn_agent")
        assert len(spawns) == 1 and spawns[0]["status"] == "ok", spawns
        agent_id = spawns[0]["value"]["agent_id"]
        first_task = spawns[0]["value"]["task_id"]
        tree = await app.subagents.repository.tree_by_key(
            session.id, "root:" + current1.run_id
        )
        assert tree is not None, "first turn must establish the collaboration tree"

        released = await until(lambda: _released_agent(app, agent_id), limit=15.0)
        first_activation = released["activation"]["activation_id"]
        first_instance = released["activation"]["instance"]
        assert released["resident"] is False

        session_view = await app.sessions.get(LOCAL, "local", session.id)
        turn2 = await app.sessions.submit(
            LOCAL,
            "local",
            session.id,
            TurnRequest(
                text=turn_two_text,
                request_key="turn-two",
                expected_revision=session_view.revision,
            ),
        )
        current2, view2 = await settle(app, session.id, turn2.id, limit=45.0)
        assert view2.status == "completed", f"second turn settled {view2.status}: {view2}"
        async with asyncio.timeout(15):
            await worker_sampled.wait()

        lists = await tool_receipts(app, current2.run_id, "list_agents")
        assert len(lists) == 1 and lists[0]["status"] == "ok", lists
        listed = lists[0]["value"]
        assert listed["tree_id"] == tree["tree_id"]
        members = {item["path"]: item for item in listed["agents"]}
        assert set(members) == {"lead", "lead/w6"}, members
        member = members["lead/w6"]
        assert member["agent_id"] == agent_id
        assert member["task"]["task_id"] == first_task
        assert member["resident"] is False, "worker must be cold at the new turn boundary"

        sends = await tool_receipts(app, current2.run_id, "send_message")
        assert len(sends) == 1 and sends[0]["status"] == "ok", sends
        receipt = sends[0]["value"]
        assert receipt["accepted"] is True and receipt["scheduled"] is True
        assert receipt["target"]["agent_id"] == agent_id
        assert receipt["task_id"] != first_task

        after = await app.subagents.repository.get_agent(agent_id)
        assert after["agent_id"] == agent_id
        assert after["path"] == "lead/w6"
        assert after["tree_id"] == tree["tree_id"]
        assert after["activation"]["activation_id"] != first_activation
        assert after["activation"]["instance"] == first_instance + 1

        rebound = await app.subagents.repository.tree_by_key(
            session.id, "root:" + current1.run_id
        )
        assert rebound["tree_id"] == tree["tree_id"]
        assert rebound["root_run_id"] == current2.run_id

        second_row = await app.subagents.repository.task(receipt["task_id"])
        assert second_row["status"] == "completed"
        assert "turn one answer" in str(second_row["input"]["history"])
        assert "turn one answer" in str(worker.histories[1])
        assert "second turn follow up" in str(worker.histories[1][-1])
        result = await app.subagents.repository.result(receipt["task_id"])
        assert result is not None and result["stop_reason"] == "completed", result
        assert result["output"]["text"] == "turn two answer"
        notices = result_notices(main.histories[-1], receipt["task_id"])
        assert len(notices) == 1, notices

        assert len(await app.subagents.repository.list_agents(tree["tree_id"])) == 2
        tasks = await app.subagents.repository.tasks(agent_id)
        assert len(tasks) == 2
        assert worker.requests == 2
    finally:
        await asyncio.wait_for(app.close(), 20)