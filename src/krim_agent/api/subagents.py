"""会话级子代理只读/控制端点；鉴权沿sessions/planning，写操作经domain.control。

不伪造Invocation/_PRODUCER：控制面由真实用户principal经domain host入口
执行；查询只读本session的树/成员，绝不泄露其他session/tree。返回实际
Run/域结果，不把控制平面JSON当用户turn，不创建隐藏会话。
"""

from typing import Any, Literal

from fastapi import APIRouter, FastAPI, Query, Request
from pydantic import Field
from sqlalchemy import select

from krim_agent.contracts import ApprovalReply, RunEvent, ServiceError, StrictModel
from krim_agent.storage.models import runs
from krim_agent.storage.subagent_models import (
    subagent_activations,
    subagent_child_runs,
    subagent_members,
    subagent_messages,
    subagent_notifications,
    subagent_results,
    subagent_tasks,
    subagent_trees,
)

from .sessions import application, principal

MEMBER_LIMIT = 50


def _domain(request: Request) -> Any:
    domain = application(request).subagents
    if domain is None:
        raise ServiceError("subagent_capability", 409)
    return domain


def _store(request: Request):
    return application(request).store


async def _authorize_read(request: Request, workspace_id: str, session_id: str) -> None:
    sessions = application(request).sessions
    if sessions is None:
        raise ServiceError("schema_upgrade_required")
    await sessions.get(principal(request), workspace_id, session_id)


async def _agent_session(store: Any, agent_id: str) -> str | None:
    """归属核对：agent必须属于指定session的树，否则一律not_found。"""

    async def read(conn: Any) -> str | None:
        return await conn.scalar(
            select(subagent_trees.c.session_id)
            .select_from(
                subagent_members.join(
                    subagent_trees, subagent_members.c.tree_id == subagent_trees.c.id
                )
            )
            .where(subagent_members.c.id == agent_id)
        )

    return await store.work_read(read)


async def _require_agent_session(store: Any, agent_id: str, session_id: str) -> None:
    found = await _agent_session(store, agent_id)
    if found != session_id:
        raise ServiceError("not_found", 404)


async def _list_members(
    store: Any, session_id: str, after_path: str | None, limit: int
) -> tuple[list[dict[str, Any]], bool]:
    member = subagent_members.alias("member")
    parent = subagent_members.alias("parent")
    tree = subagent_trees.alias("tree")
    after = after_path if after_path is not None else ""

    async def read(conn: Any) -> list[dict[str, Any]]:
        rows = (
            await conn.execute(
                select(
                    member.c.id.label("agent_id"), member.c.tree_id,
                    member.c.path, member.c.role, member.c.status, member.c.depth,
                    member.c.profile, member.c.provider, member.c.workdir,
                    member.c.revision, member.c.current_task_id,
                    parent.c.path.label("parent_path"),
                    tree.c.status.label("tree_status"),
                )
                .select_from(
                    member.join(tree, member.c.tree_id == tree.c.id).join(
                        parent, member.c.parent_id == parent.c.id, isouter=True
                    )
                )
                .where(tree.c.session_id == session_id, member.c.path > after)
                .order_by(member.c.path)
                .limit(limit + 1)
            )
        ).mappings().all()
        out = [await _member_entry(conn, row) for row in rows]
        has_more = len(out) > limit
        return out[:limit], has_more

    rows, has_more = await store.work_read(read)
    return rows, has_more


async def _agent_run_ids(store: Any, agent_id: str) -> list[str]:
    """该成员全部child run（含历史续派），按创建顺序。"""

    async def read(conn: Any) -> list[str]:
        return list(
            (
                await conn.execute(
                    select(subagent_child_runs.c.run_id)
                    .where(subagent_child_runs.c.agent_id == agent_id)
                    .order_by(subagent_child_runs.c.created_at, subagent_child_runs.c.run_id)
                )
            ).scalars()
        )

    return await store.work_read(read)


class AgentControlBody(StrictModel):
    activation_id: str | None = None
    task_id: str | None = None


class AgentMessageBody(StrictModel):
    text: str = Field(min_length=1, max_length=262144)
    wake: Literal["auto", "queue_only"] = "auto"
    new_task: bool = False
    task_id: str | None = Field(default=None, max_length=36)
    request_key: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.:-]+$")


async def _member_entry(conn: Any, row: Any) -> dict[str, Any]:
    """列表/详情共用成员视图：持久记录 + 当前task摘要 + route_key/title。"""
    if row is None:
        raise ServiceError("not_found", 404)
    entry = dict(row)
    if "parent_path" not in entry:
        entry["parent_path"] = await conn.scalar(
            select(subagent_members.c.path).where(
                subagent_members.c.id == entry.get("parent_id")
            )
        ) if entry.get("parent_id") is not None else None
    entry.pop("parent_id", None)
    # route_key只来自持久run的target['key']；无run不猜测，保持None
    # （正式binding键为subagent:<family>:<profile>，由宿主装配决定）。
    entry["route_key"] = None
    activation = (
        await conn.execute(
            select(subagent_activations)
            .where(subagent_activations.c.agent_id == entry["agent_id"])
            .order_by(subagent_activations.c.instance.desc())
            .limit(1)
        )
    ).mappings().first()
    entry["activation"] = (
        {
            "activation_id": activation["id"],
            "instance": activation["instance"],
            "status": activation["status"],
        }
        if activation is not None
        else None
    )
    entry["task"] = None
    entry["title"] = None
    if entry["current_task_id"]:
        task = (
            await conn.execute(
                select(subagent_tasks).where(subagent_tasks.c.id == entry["current_task_id"])
            )
        ).mappings().first()
        if task is not None:
            entry["task"] = {
                "task_id": task["id"], "status": task["status"],
                "run_id": task["run_id"], "ordinal": task["ordinal"],
                "input": task["input"] if isinstance(task["input"], dict) else None,
            }
            if task["run_id"] is not None:
                target = (
                    await conn.execute(select(runs.c.target).where(runs.c.id == task["run_id"]))
                ).scalar_one_or_none()
                if isinstance(target, dict) and isinstance(target.get("key"), str):
                    entry["route_key"] = target["key"]
            first = (
                await conn.execute(
                    select(subagent_messages.c.body).where(
                        subagent_messages.c.target_id == entry["agent_id"],
                        subagent_messages.c.seq == 1,
                    )
                )
            ).scalar_one_or_none()
            entry["title"] = (
                first.get("text")[:160]
                if isinstance(first, dict) and isinstance(first.get("text"), str)
                else None
            )
    return entry


def mount_subagents(app: FastAPI) -> None:
    router = APIRouter()
    base = "/v2/workspaces/{workspace_id}/sessions/{session_id}/subagents"

    @router.get(base)
    async def list_subagents(
        workspace_id: str,
        session_id: str,
        request: Request,
        after_path: str | None = Query(default=None, max_length=256),
        limit: int = Query(default=20, ge=1, le=50),
    ) -> Any:
        await _authorize_read(request, workspace_id, session_id)
        rows, has_more = await _list_members(_store(request), session_id, after_path, limit)
        return {
            "subagents": rows,
            "next_after_path": rows[-1]["path"] if has_more and rows else None,
        }

    @router.get(base + "/{agent_id}")
    async def agent_detail(
        workspace_id: str,
        session_id: str,
        agent_id: str,
        request: Request,
    ) -> Any:
        await _authorize_read(request, workspace_id, session_id)
        store = _store(request)
        await _require_agent_session(store, agent_id, session_id)

        async def read(conn: Any) -> dict[str, Any]:
            row = (
                await conn.execute(
                    select(
                        subagent_members.c.id.label("agent_id"), subagent_members.c.tree_id,
                        subagent_members.c.path, subagent_members.c.role,
                        subagent_members.c.status, subagent_members.c.depth,
                        subagent_members.c.profile, subagent_members.c.provider,
                        subagent_members.c.workdir, subagent_members.c.revision,
                        subagent_members.c.current_task_id, subagent_members.c.parent_id,
                        subagent_trees.c.status.label("tree_status"),
                    )
                    .select_from(
                        subagent_members.join(
                            subagent_trees, subagent_members.c.tree_id == subagent_trees.c.id
                        )
                    )
                    .where(subagent_members.c.id == agent_id)
                )
            ).mappings().first()
            entry = await _member_entry(conn, row)
            run_ids = list(
                (
                    await conn.execute(
                        select(subagent_child_runs.c.run_id)
                        .where(subagent_child_runs.c.agent_id == agent_id)
                        .order_by(subagent_child_runs.c.created_at, subagent_child_runs.c.run_id)
                    )
                ).scalars()
            )
            messages = [
                dict(item)
                for item in (
                    await conn.execute(
                        select(
                            subagent_messages.c.seq, subagent_messages.c.id,
                            subagent_messages.c.kind, subagent_messages.c.wake,
                            subagent_messages.c.body, subagent_messages.c.sender_id,
                            subagent_messages.c.task_id, subagent_messages.c.accepted_at,
                        )
                        .where(subagent_messages.c.target_id == agent_id)
                        .order_by(subagent_messages.c.seq.desc())
                    )
                ).mappings().all()
            ][::-1]
            results = [
                dict(item)
                for item in (
                    await conn.execute(
                        select(
                            subagent_notifications.c.seq,
                            subagent_notifications.c.id.label("notification_id"),
                            subagent_notifications.c.status.label("notification_status"),
                            subagent_notifications.c.result_id,
                            subagent_notifications.c.task_id,
                            subagent_results.c.stop_reason, subagent_results.c.code,
                            subagent_results.c.output, subagent_results.c.diagnostics,
                            subagent_results.c.settled_at,
                        )
                        .select_from(
                            subagent_notifications.join(
                                subagent_results,
                                subagent_notifications.c.result_id == subagent_results.c.id,
                            )
                        )
                        .where(subagent_notifications.c.producer_id == agent_id)
                        .order_by(subagent_notifications.c.seq.desc())
                    )
                ).mappings().all()
            ][::-1]
            for result in results:
                result["producer_path"] = entry["path"]
            return {
                "entry": entry,
                "run_ids": run_ids,
                "messages": messages,
                "results": results,
            }

        data = await store.work_read(read)
        entry, run_ids = data["entry"], data["run_ids"]
        events: list[RunEvent] = []
        run_view: dict[str, Any] | None = None
        approvals: list[dict[str, Any]] = []
        # 只投影该成员自己最新的child run：优先当前task绑定run，否则最近一个；
        # 没有任何run时events/approvals为空，run为null，不伪装主会话run。
        service = application(request).service
        latest_run_id = (
            (entry.get("task") or {}).get("run_id")
            if entry.get("task") is not None
            else None
        ) or (run_ids[-1] if run_ids else None)
        if latest_run_id is not None:
            view = await service.get(latest_run_id)
            run_view = view.model_dump(mode="json")
            # 全量返回执行记录：快照本就载入全部事件，前端完整渲染历史。
            snapshot = await store.snapshot(latest_run_id)
            events = list(snapshot.events)
            pending = await service.approvals(latest_run_id)
            approvals = [dict(record) | {"run_id": latest_run_id} for record in pending]
        return {
            "agent": entry,
            "messages": data["messages"],
            "results": data["results"],
            "events": [event.model_dump(mode="json") for event in events],
            "approvals": approvals,
            "run": run_view,
            "run_id": latest_run_id,
            "events_truncated": False,
        }

    @router.get(base + "/{agent_id}/messages")
    async def agent_messages(
        workspace_id: str,
        session_id: str,
        agent_id: str,
        request: Request,
        after_seq: int = Query(default=0, ge=0),
        limit: int = Query(default=20, ge=1, le=50),
    ) -> Any:
        await _authorize_read(request, workspace_id, session_id)
        store = _store(request)
        await _require_agent_session(store, agent_id, session_id)

        async def read(conn: Any) -> list[dict[str, Any]]:
            rows = (
                await conn.execute(
                    select(
                        subagent_messages.c.seq, subagent_messages.c.id,
                        subagent_messages.c.kind, subagent_messages.c.wake,
                        subagent_messages.c.body, subagent_messages.c.sender_id,
                        subagent_messages.c.task_id, subagent_messages.c.accepted_at,
                    )
                    .where(
                        subagent_messages.c.target_id == agent_id,
                        subagent_messages.c.seq > after_seq,
                    )
                    .order_by(subagent_messages.c.seq)
                    .limit(limit + 1)
                )
            ).mappings().all()
            has_more = len(rows) > limit
            return [dict(row) for row in rows[:limit]], has_more

        messages, has_more = await store.work_read(read)
        return {
            "messages": messages,
            "next_after_seq": messages[-1]["seq"] if has_more and messages else None,
        }

    @router.post(base + "/{agent_id}/messages", status_code=202)
    async def send_agent_message(
        workspace_id: str,
        session_id: str,
        agent_id: str,
        body: AgentMessageBody,
        request: Request,
    ) -> Any:
        """向子代理成员发送用户消息；经真实principal的host入口派发。"""
        user = principal(request)
        sessions = application(request).sessions
        if sessions is None:
            raise ServiceError("schema_upgrade_required")
        # 与既有control端点同口径：要求当前write权限；域内提交前再重验。
        await sessions.get(user, workspace_id, session_id, write=True)
        domain = _domain(request)
        send = getattr(domain, "host_send_message", None)
        if send is None:
            raise ServiceError("subagent_capability", 409)
        return await send(
            user, workspace_id, session_id, agent_id,
            text=body.text, wake=body.wake, new_task=body.new_task,
            task_id=body.task_id, request_key=body.request_key,
        )

    @router.get(base + "/{agent_id}/results")
    async def agent_results(
        workspace_id: str,
        session_id: str,
        agent_id: str,
        request: Request,
        after_seq: int = Query(default=0, ge=0),
        limit: int = Query(default=20, ge=1, le=50),
    ) -> Any:
        await _authorize_read(request, workspace_id, session_id)
        store = _store(request)
        await _require_agent_session(store, agent_id, session_id)

        async def read(conn: Any) -> list[dict[str, Any]]:
            producer = subagent_members.alias("producer")
            rows = (
                await conn.execute(
                    select(
                        subagent_notifications.c.seq,
                        subagent_notifications.c.id.label("notification_id"),
                        subagent_notifications.c.status.label("notification_status"),
                        subagent_notifications.c.result_id,
                        subagent_notifications.c.task_id,
                        producer.c.path.label("producer_path"),
                        subagent_results.c.stop_reason, subagent_results.c.code,
                        subagent_results.c.output, subagent_results.c.diagnostics,
                        subagent_results.c.settled_at,
                    )
                    .select_from(
                        subagent_notifications.join(
                            subagent_results,
                            subagent_notifications.c.result_id == subagent_results.c.id,
                        ).join(
                            producer,
                            subagent_notifications.c.producer_id == producer.c.id,
                        )
                    )
                    .where(
                        subagent_notifications.c.tree_id.in_(
                            select(subagent_trees.c.id).where(
                                subagent_trees.c.session_id == session_id
                            )
                        ),
                        subagent_notifications.c.producer_id == agent_id,
                        subagent_notifications.c.seq > after_seq,
                    )
                    .order_by(subagent_notifications.c.seq)
                    .limit(limit + 1)
                )
            ).mappings().all()
            has_more = len(rows) > limit
            return [dict(row) for row in rows[:limit]], has_more

        results, has_more = await store.work_read(read)
        return {
            "results": results,
            "next_after_seq": results[-1]["seq"] if has_more and results else None,
        }

    @router.post(base + "/{agent_id}/interrupt")
    async def interrupt_agent(
        workspace_id: str, session_id: str, agent_id: str, request: Request,
        body: AgentControlBody | None = None,
    ) -> Any:
        return await _control(
            request, workspace_id, session_id, agent_id, "interrupt", body
        )

    @router.post(base + "/{agent_id}/close")
    async def close_agent(
        workspace_id: str, session_id: str, agent_id: str, request: Request,
        body: AgentControlBody | None = None,
    ) -> Any:
        return await _control(request, workspace_id, session_id, agent_id, "close", body)

    @router.post(base + "/{agent_id}/approvals/{request_id}")
    async def agent_approval_reply(
        workspace_id: str,
        session_id: str,
        agent_id: str,
        request_id: str,
        body: ApprovalReply,
        request: Request,
    ) -> Any:
        """批准/拒绝子代理挂起审批；授权与turn审批同口径（approve权限）。"""
        user = principal(request)
        sessions = application(request).sessions
        if sessions is None:
            raise ServiceError("schema_upgrade_required")
        # 与既有turn审批同口径：approve权限，viewer拒绝；session.read同时
        # 复核workspace成员资格、owner归属与项目访问（复用同一get实现）。
        await sessions.get(user, workspace_id, session_id, approve=True)
        store = _store(request)
        await _require_agent_session(store, agent_id, session_id)
        domain = _domain(request)
        return await domain.approval_reply(
            user, workspace_id, session_id, agent_id, request_id, body
        )

    app.include_router(router)


async def _control(
    request: Request,
    workspace_id: str,
    session_id: str,
    agent_id: str,
    action: Literal["interrupt", "close"],
    body: AgentControlBody | None,
) -> Any:
    user = principal(request)
    sessions = application(request).sessions
    if sessions is None:
        raise ServiceError("schema_upgrade_required")
    # 与既有control端点同口径：控制类操作要求当前write权限；域内control
    # 再经authorize_session(write=True)与tree/activation/项目权限重验。
    await sessions.get(user, workspace_id, session_id, write=True)
    domain = _domain(request)
    control = getattr(domain, "control", None)
    if control is None:
        raise ServiceError("subagent_capability", 409)
    payload = body if body is not None else AgentControlBody()
    return await control(
        user, workspace_id, session_id, agent_id,
        action=action, activation_id=payload.activation_id, task_id=payload.task_id,
    )
