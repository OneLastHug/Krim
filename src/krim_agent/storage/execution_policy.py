"""单会话执行模式持久化：manual/auto/bypass、CAS热切换、请求幂等与本会话规则。

模式只属于单个会话，不从项目或账号继承；运行中可修改，不检查会话空闲。
项目访问能力由 project_access.policy.resolve_project_access 解析，本模块只
决定确认方式：无项目绑定的旧会话可读 manual，但拒绝任何模式升权；read
项目可改模式，bypass 仍不能越过项目只读上限（由执行判定链另行检查）。
会话owner严格核对，不使用workspace admin的owns()例外。本会话允许规则按
tool_hash与参数digest严格全等匹配，不存在通配或前缀形态，并持久化授予
时的项目上下文（绑定项目+owner membership revision）：换绑项目或ACL变化
后旧规则失效。单次批准仍由现有确认账本记录，本模块不建立第二份批准事实。
"""

from __future__ import annotations

import re
import time
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, cast

from sqlalchemy import delete, func, insert, select, update
from sqlalchemy.ext.asyncio import AsyncConnection

from krim_agent.contracts import (
    JsonValue,
    ServiceError,
    encoded,
    fingerprint,
    parse_json,
)
from krim_agent.project_access.policy import ProjectAccessSnapshot, resolve_project_access
from krim_agent.security import PrincipalContext
from krim_agent.sessions.execution_policy import (
    EXECUTION_POLICY_MODES,
    ExecutionPolicyMode,
    SessionExecutionPolicyView,
)

from .development_models import development_environments
from .execution_policy_models import (
    EXECUTION_POLICY_TABLES,
    session_allow_rules,
    session_execution_policies,
    session_execution_policy_requests,
)
from .models import metadata, sessions
from .project_membership_models import project_memberships
from .sandbox_models import sandbox_projects, sandbox_session_bindings
from .sessions import read, session_row

if TYPE_CHECKING:
    from .sqlite import SQLiteStore

EXECUTION_POLICY_SCHEMA = 19  # session_execution_policies 等表自 schema 19 引入
_HEX64 = re.compile(r"^[0-9a-f]{64}$")


def require(store: SQLiteStore) -> None:
    if store.schema_version < EXECUTION_POLICY_SCHEMA:
        raise ServiceError("schema_upgrade_required")


@dataclass(frozen=True)
class SessionModeSnapshot:
    """判定链用的最小版本锚点；执行前应复核 revision 未变化。"""

    mode: ExecutionPolicyMode
    revision: int
    updated_at: float


async def _policy_row(conn: AsyncConnection, session_id: str) -> Any | None:
    return (
        (
            await conn.execute(
                select(session_execution_policies).where(
                    session_execution_policies.c.session_id == session_id
                )
            )
        )
        .mappings()
        .first()
    )


@dataclass(frozen=True)
class _SessionContext:
    project: ProjectAccessSnapshot | None


async def _resolve(
    conn: AsyncConnection, principal: PrincipalContext, workspace: str, session_id: str
) -> _SessionContext:
    # 归属先于一切：不存在/跨工作区/非本人会话统一404，不泄露他人会话存在性。
    session = await session_row(conn, session_id)
    if session["workspace_id"] != workspace or session["owner_id"] != principal.principal_id:
        # 会话owner严格；同项目write或workspace admin都不能替其他会话读写。
        raise ServiceError("not_found", 404)
    binding = (
        await conn.execute(
            select(sandbox_session_bindings.c.project_id).where(
                sandbox_session_bindings.c.session_id == session_id
            )
        )
    ).first()
    project_id = binding[0] if binding is not None else None
    if project_id is None:
        return _SessionContext(project=None)
    # 实际项目只从会话绑定解析；none/项目不存在统一404，read/write都可改模式。
    project = await resolve_project_access(conn, principal, project_id)
    project.require()
    if project.workspace_id != workspace:
        raise ServiceError("facts_corrupt")
    return _SessionContext(project=project)


def _view(
    session_id: str, row: Any | None, project: ProjectAccessSnapshot | None
) -> SessionExecutionPolicyView:
    if row is None:
        # 迁移前缺失的行按"未持久化的manual"呈现：GET可读，PUT以revision 0为CAS基线。
        mode, revision, updated_at, updated_by = "manual", 0, 0.0, ""
    else:
        mode = row["mode"]
        if mode not in EXECUTION_POLICY_MODES:
            raise ServiceError("facts_corrupt")
        revision, updated_at, updated_by = row["revision"], row["updated_at"], row["updated_by"]
        if type(revision) is not int or revision < 0:
            raise ServiceError("facts_corrupt")
    return SessionExecutionPolicyView(
        session_id=session_id,
        mode=cast(ExecutionPolicyMode, mode),
        revision=revision,
        updated_at=updated_at,
        updated_by=updated_by,
        project_id=project.project_id if project is not None else None,
        project_access=(cast(Literal["read", "write"], project.access)
                        if project is not None else None),
        project_access_revision=project.revision if project is not None else 0,
    )


async def get_policy(
    store: SQLiteStore, *, principal: PrincipalContext, workspace: str, session_id: str
) -> SessionExecutionPolicyView:
    require(store)

    async def operation(conn: AsyncConnection) -> SessionExecutionPolicyView:
        context = await _resolve(conn, principal, workspace, session_id)
        return _view(session_id, await _policy_row(conn, session_id), context.project)

    return await read(store, operation)


async def _committed_result(
    store: SQLiteStore,
    *,
    principal: PrincipalContext,
    workspace: str,
    session_id: str,
    request_key: str,
    digest: str,
) -> SessionExecutionPolicyView:
    async def operation(conn: AsyncConnection) -> SessionExecutionPolicyView:
        await _resolve(conn, principal, workspace, session_id)
        old = (
            (
                await conn.execute(
                    select(session_execution_policy_requests).where(
                        session_execution_policy_requests.c.principal_id == principal.principal_id,
                        session_execution_policy_requests.c.request_key == request_key,
                    )
                )
            )
            .mappings()
            .first()
        )
        if old is None or old["session_id"] != session_id or old["request_hash"] != digest:
            raise ServiceError("not_found", 404)
        try:
            return SessionExecutionPolicyView.model_validate(old["result"])
        except ValueError:
            raise ServiceError("facts_corrupt") from None

    return await read(store, operation)


async def set_mode(
    store: SQLiteStore,
    *,
    principal: PrincipalContext,
    workspace: str,
    session_id: str,
    request_key: str,
    expected_revision: int,
    mode: ExecutionPolicyMode,
) -> SessionExecutionPolicyView:
    require(store)
    if mode not in EXECUTION_POLICY_MODES:
        raise ServiceError("invalid_input", 422)
    payload = {
        "principal_id": principal.principal_id,
        "workspace_id": workspace,
        "session_id": session_id,
        "mode": mode,
        "expected_revision": expected_revision,
    }
    digest = fingerprint(payload)

    async def operation(conn: AsyncConnection) -> SessionExecutionPolicyView:
        # 与GET相同顺序：先核归属（404），再查幂等键，避免借回放路径绕过授权。
        context = await _resolve(conn, principal, workspace, session_id)
        old = (
            (
                await conn.execute(
                    select(session_execution_policy_requests).where(
                        session_execution_policy_requests.c.principal_id == principal.principal_id,
                        session_execution_policy_requests.c.request_key == request_key,
                    )
                )
            )
            .mappings()
            .first()
        )
        if old is not None:
            if old["session_id"] != session_id or old["request_hash"] != digest:
                raise ServiceError("key_conflict")
            try:
                # 重放返回已提交响应；不重写当前状态，不降级任何人的切换。
                return SessionExecutionPolicyView.model_validate(old["result"])
            except ValueError:
                raise ServiceError("facts_corrupt") from None
        row = await _policy_row(conn, session_id)
        if context.project is None and mode != "manual":
            # 无项目绑定的旧会话可确认manual现状；不接受任何模式升权。
            raise ServiceError("permission_denied", 403)
        current_revision = row["revision"] if row is not None else 0
        if current_revision != expected_revision:
            raise ServiceError("revision_conflict", 409)
        current_mode = row["mode"] if row is not None else "manual"
        if row is None or mode != current_mode:
            # 首次持久化（含虚拟manual落库）与真实模式变化都推进版本；
            # 确认现状的重复PUT只登记台账，不推进版本。
            revision = current_revision + 1
            updated_at = time.time()
            updated_by = principal.principal_id
            if row is None:
                await conn.execute(
                    insert(session_execution_policies).values(
                        session_id=session_id,
                        mode=mode,
                        revision=revision,
                        updated_at=updated_at,
                        updated_by=updated_by,
                    )
                )
            else:
                await conn.execute(
                    update(session_execution_policies)
                    .where(session_execution_policies.c.session_id == session_id)
                    .values(
                        mode=mode,
                        revision=revision,
                        updated_at=updated_at,
                        updated_by=updated_by,
                    )
                )
        else:
            revision, updated_at, updated_by = (
                row["revision"],
                row["updated_at"],
                row["updated_by"],
            )
        result = _view(
            session_id,
            {
                "session_id": session_id,
                "mode": mode,
                "revision": revision,
                "updated_at": updated_at,
                "updated_by": updated_by,
            },
            context.project,
        )
        await conn.execute(
            insert(session_execution_policy_requests).values(
                principal_id=principal.principal_id,
                request_key=request_key,
                request_hash=digest,
                session_id=session_id,
                result=parse_json(result.model_dump_json()),
                created_at=time.time(),
            )
        )
        return result

    async def confirm() -> SessionExecutionPolicyView | None:
        try:
            return await _committed_result(
                store,
                principal=principal,
                workspace=workspace,
                session_id=session_id,
                request_key=request_key,
                digest=digest,
            )
        except ServiceError as error:
            if error.code == "not_found":
                return None
            raise

    return await store._eval_write(operation, confirm)


async def initialize(
    conn: AsyncConnection, session_id: str, mode: ExecutionPolicyMode = "manual"
) -> bool:
    """会话创建事务内的初始模式写入；已有行幂等跳过，返回是否新插入。

    初始值由创建方按项目访问决定（write项目bypass、read项目manual、旧会话
    迁移manual）；调用方必须在sessions行插入之后调用，否则外键失败。
    """
    if mode not in EXECUTION_POLICY_MODES:
        raise ServiceError("invalid_input", 422)
    existing = (
        await conn.execute(
            select(session_execution_policies.c.session_id).where(
                session_execution_policies.c.session_id == session_id
            )
        )
    ).scalar_one_or_none()
    if existing is not None:
        return False
    await conn.execute(
        insert(session_execution_policies).values(
            session_id=session_id,
            mode=mode,
            revision=1,
            updated_at=time.time(),
            updated_by="",
        )
    )
    return True


async def migrate(conn: AsyncConnection) -> None:
    """schema19升级：幂等建表，并给缺行旧会话保守回填manual；不覆盖已有模式。"""
    await conn.run_sync(lambda sync: metadata.create_all(sync, tables=EXECUTION_POLICY_TABLES))
    present = set((await conn.execute(select(session_execution_policies.c.session_id))).scalars())
    for session_id in (await conn.execute(select(sessions.c.id))).scalars():
        if session_id in present:
            continue
        await conn.execute(
            insert(session_execution_policies).values(
                session_id=session_id,
                mode="manual",
                revision=1,
                updated_at=time.time(),
                updated_by="",
            )
        )


async def get_mode(conn: AsyncConnection, session_id: str) -> ExecutionPolicyMode:
    """判定链读取：无行回退manual（revision视为0）；mode值损坏视为事实损坏。"""
    row = await _policy_row(conn, session_id)
    if row is None:
        return "manual"
    if row["mode"] not in EXECUTION_POLICY_MODES:
        raise ServiceError("facts_corrupt")
    return cast(ExecutionPolicyMode, row["mode"])


async def read_mode_snapshot(conn: AsyncConnection, session_id: str) -> SessionModeSnapshot:
    row = await _policy_row(conn, session_id)
    if row is None:
        return SessionModeSnapshot(mode="manual", revision=0, updated_at=0.0)
    mode, revision = row["mode"], row["revision"]
    if mode not in EXECUTION_POLICY_MODES or type(revision) is not int or revision < 0:
        raise ServiceError("facts_corrupt")
    return SessionModeSnapshot(mode=mode, revision=revision, updated_at=row["updated_at"])


def arguments_digest(arguments_json: str) -> str:
    """参数digest口径与approval_request的arguments_hash一致；JSON非法直接抛错。"""
    return fingerprint(parse_json(arguments_json))


async def _session_project_context(
    conn: AsyncConnection, session_id: str
) -> tuple[str, int] | None:
    """会话当前项目上下文：绑定项目+会话owner的membership revision。

    规则的授予与匹配都以此为准（不接受调用方传入项目）；无绑定、无有效
    访问或访问为none时返回None——不存在可被规则承接的项目能力。
    """
    row = (
        await conn.execute(
            select(sessions.c.owner_id, sandbox_session_bindings.c.project_id)
            .join(sandbox_session_bindings, sandbox_session_bindings.c.session_id == sessions.c.id)
            .where(sessions.c.id == session_id)
        )
    ).first()
    if row is None or row[1] is None:
        return None
    membership = (
        await conn.execute(
            select(project_memberships.c.access, project_memberships.c.revision).where(
                project_memberships.c.project_id == row[1],
                project_memberships.c.principal_id == row[0],
            )
        )
    ).first()
    if membership is None or membership[0] == "none":
        return None
    return (row[1], membership[1])


async def record_allow_rule(
    conn: AsyncConnection,
    session_id: str,
    *,
    tool_name: str,
    tool_hash: str,
    arguments_hash: str,
) -> bool:
    """登记"本会话允许此类操作"规则；同规则已存在返回False。

    项目上下文从会话绑定与owner的membership读取并持久化：换绑项目或ACL
    revision变化后旧规则失效，同规则在新上下文重新授予即刷新上下文。
    只接受完整64位digest的tool_hash+arguments_hash组合：匹配时严格全等，
    没有名称、通配或前缀形态，某次Shell批准不可能扩大成全部Shell操作。
    """
    if (
        not isinstance(tool_name, str)
        or not 1 <= len(tool_name) <= 128
        or any(ord(char) < 32 for char in tool_name)
    ):
        raise ServiceError("invalid_input", 422)
    if _HEX64.fullmatch(tool_hash) is None or _HEX64.fullmatch(arguments_hash) is None:
        raise ServiceError("invalid_input", 422)
    context = await _session_project_context(conn, session_id)
    if context is None:
        # 无项目绑定或无有效访问：规则没有可承接的项目能力，不授予。
        raise ServiceError("permission_denied", 403)
    existing = (
        (
            await conn.execute(
                select(
                    session_allow_rules.c.id,
                    session_allow_rules.c.project_id,
                    session_allow_rules.c.project_access_revision,
                ).where(
                    session_allow_rules.c.session_id == session_id,
                    session_allow_rules.c.tool_hash == tool_hash,
                    session_allow_rules.c.arguments_hash == arguments_hash,
                )
            )
        )
        .mappings()
        .first()
    )
    if existing is not None:
        if (existing["project_id"], existing["project_access_revision"]) == context:
            return False
        await conn.execute(
            update(session_allow_rules)
            .where(session_allow_rules.c.id == existing["id"])
            .values(project_id=context[0], project_access_revision=context[1])
        )
        return True
    await conn.execute(
        insert(session_allow_rules).values(
            id=str(uuid.uuid4()),
            session_id=session_id,
            project_id=context[0],
            project_access_revision=context[1],
            tool_name=tool_name,
            tool_hash=tool_hash,
            arguments_hash=arguments_hash,
            created_at=time.time(),
        )
    )
    return True


async def allow_rule_satisfied(
    conn: AsyncConnection, session_id: str, *, tool_hash: str, arguments_hash: str
) -> bool:
    """严格全等匹配，并要求规则的项目上下文等于会话当前上下文。

    会话换绑项目、owner的membership revision变化（降权或恢复）都会使旧
    规则失效；摘要畸形或上下文缺失一律不满足（宁可再次询问，不放大范围）。
    """
    if _HEX64.fullmatch(tool_hash) is None or _HEX64.fullmatch(arguments_hash) is None:
        return False
    context = await _session_project_context(conn, session_id)
    if context is None:
        return False
    rule = (
        (
            await conn.execute(
                select(
                    session_allow_rules.c.project_id,
                    session_allow_rules.c.project_access_revision,
                ).where(
                    session_allow_rules.c.session_id == session_id,
                    session_allow_rules.c.tool_hash == tool_hash,
                    session_allow_rules.c.arguments_hash == arguments_hash,
                )
            )
        )
        .mappings()
        .first()
    )
    return rule is not None and ((rule["project_id"], rule["project_access_revision"]) == context)


async def clear_allow_rules(conn: AsyncConnection, session_id: str) -> int:
    """清除仅限单个会话的允许规则，返回清除条数；不触碰其他会话与单次批准。"""
    result = await conn.execute(
        delete(session_allow_rules).where(session_allow_rules.c.session_id == session_id)
    )
    return result.rowcount or 0


async def session_allow_rule_context(
    store: SQLiteStore, session_id: str
) -> tuple[str, int] | None:
    """批复链预检：会话当前是否存在可承接session范围规则的项目上下文。

    runtime在落approval_reply前调用；返回None表示该会话没有可被规则承接
    的项目能力（未绑定、无有效访问或access=none），session范围批复应整体
    拒绝而不是降级落账。
    """

    async def operation(conn: AsyncConnection) -> tuple[str, int] | None:
        return await _session_project_context(conn, session_id)

    return await read(store, operation)


async def count_allow_rules(
    store: SQLiteStore, *, principal: PrincipalContext, workspace: str, session_id: str
) -> int:
    """本会话当前允许规则数；owner严格并要求当前项目ACL有效。"""
    require(store)

    async def operation(conn: AsyncConnection) -> int:
        await _resolve(conn, principal, workspace, session_id)
        return (
            await conn.execute(
                select(func.count())
                .select_from(session_allow_rules)
                .where(session_allow_rules.c.session_id == session_id)
            )
        ).scalar_one()

    return await read(store, operation)


def _clear_result(row: Any) -> dict[str, JsonValue]:
    stored = row["result"]
    result = parse_json(stored) if isinstance(stored, str) else stored
    if (
        not isinstance(result, dict)
        or result.get("session_id") != row["session_id"]
        or type(result.get("cleared_count")) is not int
        or type(result.get("rule_count")) is not int
    ):
        raise ServiceError("facts_corrupt")
    return cast(dict[str, JsonValue], result)


async def clear_session_allow_rules(
    store: SQLiteStore,
    *,
    principal: PrincipalContext,
    workspace: str,
    session_id: str,
    request_key: str,
) -> dict[str, JsonValue]:
    """按幂等键清除本会话允许规则；删除与台账同一事务，重放不重复清。

    台账复用session_execution_policy_requests，payload含operation字段，
    与mode PUT同键异内容互为key_conflict；同键重放只返回首次结果，即使
    期间新增了规则也不会被重试误清。
    """
    require(store)
    payload = {
        "principal_id": principal.principal_id,
        "workspace_id": workspace,
        "session_id": session_id,
        "operation": "clear_allow_rules",
    }
    digest = fingerprint(payload)

    async def replay(conn: AsyncConnection) -> dict[str, JsonValue] | None:
        old = (
            (
                await conn.execute(
                    select(session_execution_policy_requests).where(
                        session_execution_policy_requests.c.principal_id == principal.principal_id,
                        session_execution_policy_requests.c.request_key == request_key,
                    )
                )
            )
            .mappings()
            .first()
        )
        if old is None:
            return None
        if old["session_id"] != session_id or old["request_hash"] != digest:
            raise ServiceError("key_conflict")
        return _clear_result(old)

    async def operation(conn: AsyncConnection) -> dict[str, JsonValue]:
        # 与GET相同顺序：先核归属（404），再查幂等键。
        await _resolve(conn, principal, workspace, session_id)
        old = await replay(conn)
        if old is not None:
            return old
        cleared = await clear_allow_rules(conn, session_id)
        remaining = (
            await conn.execute(
                select(func.count())
                .select_from(session_allow_rules)
                .where(session_allow_rules.c.session_id == session_id)
            )
        ).scalar_one()
        result: dict[str, JsonValue] = {
            "session_id": session_id,
            "cleared_count": cleared,
            "rule_count": remaining,
        }
        await conn.execute(
            insert(session_execution_policy_requests).values(
                principal_id=principal.principal_id,
                request_key=request_key,
                request_hash=digest,
                session_id=session_id,
                result=parse_json(encoded(result)),
                created_at=time.time(),
            )
        )
        return result

    async def confirm() -> dict[str, JsonValue] | None:
        async def check(conn: AsyncConnection) -> dict[str, JsonValue] | None:
            await _resolve(conn, principal, workspace, session_id)
            return await replay(conn)

        return await read(store, check)

    return await store._eval_write(operation, confirm)


async def get_session_project(
    store: SQLiteStore, *, principal: PrincipalContext, workspace: str, session_id: str
) -> dict[str, JsonValue]:
    """已批准项目详情最小只读视图：真实项目行+会话owner当前ACL+环境状态。

    只读：不准备环境、不连宿主；owner_id取项目真实创建者（principal_id），
    is_creator对比会话owner；未绑定项目的旧会话404。
    """
    require(store)

    async def operation(conn: AsyncConnection) -> dict[str, JsonValue]:
        context = await _resolve(conn, principal, workspace, session_id)
        project = context.project
        if project is None:
            raise ServiceError("not_found", 404)
        row = (
            (
                await conn.execute(
                    select(sandbox_projects).where(
                        sandbox_projects.c.project_id == project.project_id
                    )
                )
            )
            .mappings()
            .first()
        )
        if row is None:
            raise ServiceError("facts_corrupt")
        owner_id = (
            await conn.execute(
                select(sessions.c.owner_id).where(sessions.c.id == session_id)
            )
        ).scalar_one()
        record = (
            await conn.execute(
                select(development_environments.c.record).where(
                    development_environments.c.project_id == project.project_id
                )
            )
        ).scalar_one_or_none()
        sandbox_status = "unprovisioned"
        if isinstance(record, dict):
            environment = record.get("environment")
            if isinstance(environment, dict) and isinstance(environment.get("state"), str):
                sandbox_status = environment["state"]
        return {
            "session_id": session_id,
            "project_id": project.project_id,
            "title": row["title"],
            "owner_id": row["principal_id"],
            "project_access": project.access,
            "is_creator": row["principal_id"] == owner_id,
            "sandbox_status": sandbox_status,
        }

    return await read(store, operation)
