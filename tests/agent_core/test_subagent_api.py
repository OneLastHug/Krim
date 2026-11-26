"""子代理会话级API：session鉴权、增量游标与domain control契约（真实app+HTTP）。"""

from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import httpx
import pytest
from pydantic import SecretStr

from krim_agent.api.server import create_app
from krim_agent.composition import open_application
from krim_agent.config import Settings
from krim_agent.configuration import ServerConfiguration, ServiceConfiguration
from krim_agent.contracts import RunRequest
from krim_agent.goals.continuation import ContinuationPolicy
from krim_agent.providers.openai_compatible import OpenAICompatibleConfig
from krim_agent.security import LOCAL
from krim_agent.sessions.types import SessionCreate
from krim_agent.subagents.configuration import SubagentsConfiguration
from krim_agent.subagents.storage import SubagentStore
from krim_agent.subagents.types import (
    AcceptSpawn,
    ReserveAgent,
    SendMessage,
    SettleResult,
    StartTask,
    TreeScope,
)

WORKSPACE = "local"


def configuration_for(path: Path) -> ServiceConfiguration:
    return ServiceConfiguration(
        settings=Settings(database=path, run_timeout=30, max_request_bytes=262144),
        model=OpenAICompatibleConfig(
            base_url="http://127.0.0.1:9", model="test-model", api_key=SecretStr("k"),
        ),
        sandbox=None, development=None, tools=None, access=None,
        continuation_policy=ContinuationPolicy(),
        server=ServerConfiguration(),
        legacy_artifact_root=None, personal_mcp_key_file=None, snapshot={},
        subagents=SubagentsConfiguration(enabled=True),
    )


@asynccontextmanager
async def client_for(path: Path):
    app = await open_application(path, configuration=configuration_for(path))
    api = create_app(application=app)
    async with (
        api.router.lifespan_context(api),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=api), base_url="http://127.0.0.1",
        ) as client,
    ):
        yield app, client


async def seed_tree(
    app: Any, session_id: str, *, with_history: bool = False,
) -> tuple[Any, Any, Any, str]:
    subagents = SubagentStore(app.store)
    tree = await subagents.ensure_tree(TreeScope(
        session_id=session_id, workspace_id=WORKSPACE, owner_id=LOCAL.principal_id,
        request_key="tree-" + session_id[:8],
    ))
    lead = await subagents.reserve_agent(ReserveAgent(
        tree_id=tree["tree_id"], path="lead", role="root", provider="sdk_session",
        profile="coordinator", provider_version="v1", profile_version="p1",
        request_key="spawn-root-" + session_id[:8],
    ))
    reserved = await subagents.reserve_agent(ReserveAgent(
        tree_id=tree["tree_id"], path="research", provider="sdk_session",
        profile="worker", provider_version="v1", profile_version="p1",
        parent_id=lead["agent"]["agent_id"],
        request_key="spawn-worker-" + session_id[:8],
    ))
    task_input: dict[str, Any] = {"text": "seed task"}
    if with_history:
        task_input["history"] = []
    spawned = await subagents.accept_spawn(AcceptSpawn(
        agent_id=reserved["agent"]["agent_id"],
        activation_id=reserved["activation"]["activation_id"],
        input=task_input, parent_run_id=None,
    ))
    return tree, reserved, spawned, lead["agent"]["agent_id"]


@pytest.mark.asyncio
async def test_subagents_list_and_cursors(tmp_path: Path) -> None:
    async with client_for(tmp_path / "api.sqlite") as (app, client):
        session = await app.sessions.create(
            LOCAL, WORKSPACE,
            SessionCreate(request_key="api-session", title="api", target_key="target-http"),
        )
        base = f"/v2/workspaces/{WORKSPACE}/sessions/{session.id}/subagents"
        tree, reserved, spawned, lead_id = await seed_tree(app, session.id)
        response = await client.get(base)
        assert response.status_code == 200, response.text
        body = response.json()
        assert body["next_after_path"] is None
        rows = body["subagents"]
        assert [row["path"] for row in rows] == ["lead", "research"]
        worker = rows[1]
        assert worker["agent_id"] == reserved["agent"]["agent_id"]
        assert worker["parent_path"] == "lead"
        assert worker["profile"] == "worker"
        assert worker["route_key"] is None  # 无run不猜测binding键
        assert worker["tree_status"] == "open"
        assert worker["activation"]["status"] in {"starting", "resident"}
        assert worker["task"]["status"] == "accepted"
        assert worker["task"]["run_id"] is None

        paged = await client.get(base, params={"after_path": "lead", "limit": 1})
        assert paged.status_code == 200
        paged_body = paged.json()
        assert [row["path"] for row in paged_body["subagents"]] == ["research"]
        assert paged_body["next_after_path"] is None

        # 另一session不可见本session的树（不泄露）。
        other = await app.sessions.create(
            LOCAL, WORKSPACE,
            SessionCreate(request_key="api-other", title="other", target_key="target-http"),
        )
        leaked = await client.get(f"/v2/workspaces/{WORKSPACE}/sessions/{other.id}/subagents")
        assert leaked.status_code == 200
        assert leaked.json()["subagents"] == []


@pytest.mark.asyncio
async def test_agent_messages_and_results_increments(tmp_path: Path) -> None:
    async with client_for(tmp_path / "api-msg.sqlite") as (app, client):
        session = await app.sessions.create(
            LOCAL, WORKSPACE,
            SessionCreate(request_key="api-msg", title="msg", target_key="target-http"),
        )
        base = f"/v2/workspaces/{WORKSPACE}/sessions/{session.id}/subagents"
        tree, reserved, spawned, lead_id = await seed_tree(app, session.id)
        agent_id = reserved["agent"]["agent_id"]

        messages = await client.get(f"{base}/{agent_id}/messages")
        assert messages.status_code == 200, messages.text
        body = messages.json()
        assert len(body["messages"]) == 1  # accept_spawn首条任务消息
        assert body["messages"][0]["seq"] == 1
        assert body["messages"][0]["kind"] == "task"
        assert body["next_after_seq"] is None
        more = await client.get(f"{base}/{agent_id}/messages", params={"after_seq": 1})
        assert more.json()["messages"] == []

        empty = await client.get(f"{base}/{agent_id}/results")
        assert empty.status_code == 200
        assert empty.json()["results"] == []

        await SubagentStore(app.store).settle_result(
            SettleResult(
                task_id=spawned["task_id"], stop_reason="completed",
                output={"text": "done"},
            )
        )
        results = await client.get(f"{base}/{agent_id}/results")
        assert results.status_code == 200, results.text
        rows = results.json()["results"]
        assert len(rows) == 1
        assert rows[0]["stop_reason"] == "completed"
        assert rows[0]["producer_path"] == "research"
        assert rows[0]["output"] == {"text": "done"}

        unknown = await client.get(f"{base}/not-an-agent/messages")
        assert unknown.status_code == 404


@pytest.mark.asyncio
async def test_control_endpoints_use_domain_contract(tmp_path: Path) -> None:
    async with client_for(tmp_path / "api-control.sqlite") as (app, client):
        session = await app.sessions.create(
            LOCAL, WORKSPACE,
            SessionCreate(request_key="api-ctl", title="ctl", target_key="target-http"),
        )
        base = f"/v2/workspaces/{WORKSPACE}/sessions/{session.id}/subagents"
        tree, reserved, spawned, lead_id = await seed_tree(app, session.id)
        agent_id = reserved["agent"]["agent_id"]
        activation_id = reserved["activation"]["activation_id"]

        # 根成员拒绝（domain以409 permission_denied拒绝控制根；用户停止根
        # 走既有会话API）。
        root_reply = await client.post(f"{base}/{lead_id}/interrupt", json={})
        assert root_reply.status_code == 409
        assert root_reply.json()["error"]["code"] == "permission_denied"

        # 正确activation句柄：interrupt经domain control返回实际域结果。
        reply = await client.post(
            f"{base}/{agent_id}/interrupt", json={"activation_id": activation_id},
        )
        assert reply.status_code == 200, reply.text
        assert reply.json() == {"agent_id": agent_id, "interrupted": True}

        # 旧/错activation句柄不得影响新activation。
        stale = await client.post(f"{base}/{agent_id}/close", json={"activation_id": "stale"})
        assert stale.status_code == 409
        assert stale.json()["error"]["code"] == "subagent_activation_closed"

        # 未知agent不泄露存在性。
        missing = await client.post(f"{base}/not-an-agent/close", json={})
        assert missing.status_code == 404

        # close返回域结果且成员关闭后历史仍可读。
        closed = await client.post(f"{base}/{agent_id}/close", json={})
        assert closed.status_code == 200, closed.text
        history = await client.get(f"{base}/{agent_id}/messages")
        assert history.status_code == 200
        assert len(history.json()["messages"]) == 1

@pytest.mark.asyncio
async def test_agent_detail_returns_full_history_beyond_legacy_limits(tmp_path: Path) -> None:
    """详情全量返回历史：>200事件、>50消息、>20结果与超长中文均完整不截断。"""
    async with client_for(tmp_path / "api-detail.sqlite") as (app, client):
        session = await app.sessions.create(
            LOCAL, WORKSPACE,
            SessionCreate(request_key="api-detail", title="detail", target_key="target-http"),
        )
        base = f"/v2/workspaces/{WORKSPACE}/sessions/{session.id}/subagents"
        tree, reserved, spawned, lead_id = await seed_tree(app, session.id)
        agent_id = reserved["agent"]["agent_id"]
        subagents = SubagentStore(app.store)
        run = await subagents.start_task(StartTask(
            agent_id=agent_id, task_id=spawned["task_id"],
            activation_id=reserved["activation"]["activation_id"],
            request=RunRequest(text="全量历史", request_key="ac-child.detail-run-key"),
            target={"tree_id": tree["tree_id"]},
        ))
        run_id = run["run_id"]
        long_text = "边界验证：" + "逐字保留超长中文内容，不做任何截断。" * 3000 + "结尾标记甲乙丙"
        for index in range(260):
            data: dict[str, Any] = {"text": f"第{index}段"}
            if index == 259:
                data["tail"] = long_text
            await app.store.append(run_id, "text", data)
        for index in range(60):
            await subagents.accept_message(SendMessage(
                tree_id=tree["tree_id"], target_id=agent_id,
                body={"text": f"消息{index}", "tail": long_text if index == 59 else ""},
                send_key=f"detail-msg-{index:03d}",
            ))
        await subagents.settle_result(SettleResult(
            task_id=spawned["task_id"], stop_reason="completed",
            output={"text": "结果0", "tail": long_text},
        ))
        for index in range(1, 25):
            receipt = await subagents.accept_message(SendMessage(
                tree_id=tree["tree_id"], target_id=agent_id, kind="task",
                body={"text": f"任务{index}"}, send_key=f"detail-task-{index:03d}",
            ))
            await subagents.settle_result(SettleResult(
                task_id=receipt["task_id"], stop_reason="completed",
                output={"text": f"结果{index}"},
            ))

        detail = await client.get(f"{base}/{agent_id}")
        assert detail.status_code == 200, detail.text
        body = detail.json()
        assert body["events_truncated"] is False
        assert body["run_id"] == run_id
        assert len(body["events"]) == 261  # 含宿主自动加入的run_started
        assert body["events"][0]["seq"] == 0
        assert body["events"][-1]["seq"] == 260
        assert body["events"][-1]["data"]["tail"] == long_text
        seqs = [message["seq"] for message in body["messages"]]
        assert seqs == sorted(seqs)
        assert seqs[0] == 1
        assert len(body["messages"]) == 85  # 1条spawn任务消息 + 60条后续消息 + 24条新任务消息
        followups = [message for message in body["messages"] if message["kind"] == "message"]
        assert len(followups) == 60
        assert followups[-1]["seq"] == 61
        assert followups[-1]["body"]["tail"] == long_text
        assert len(body["results"]) == 25
        result_seqs = [result["seq"] for result in body["results"]]
        assert result_seqs == sorted(result_seqs)
        assert body["results"][0]["output"] == {"text": "结果0", "tail": long_text}
        assert body["results"][-1]["output"] == {"text": "结果24"}
