"""账号管理CLI：离线初始化独占写端，运行中管理走受认证API。

在线操作只经 AsyncRunClient(url, KRIM_AGENT_API_KEY) 调 HTTP 管理端点，
永不直开数据库；离线 bootstrap/import/recover 由可信宿主显式执行，
SQLiteStore 写端 flock 保证服务未停止时以 database_busy 拒绝。
密钥只经环境变量或无回显 stdin 进入，输出只在签发结果中出现一次。
"""

from __future__ import annotations

import argparse
import asyncio
import getpass
import os
import re
import sys
import unicodedata
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlencode

from pydantic import BaseModel, SecretStr
from sqlalchemy import func, select

from krim_agent.client import AsyncRunClient
from krim_agent.contracts import ServiceError
from krim_agent.security import AccessPolicy, Credential, Membership

REQUEST_KEY = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")
ROLES = ("viewer", "member", "admin")
MUTATIONS = frozenset({"create", "update", "issue", "rotate", "revoke", "memberships"})
OFFLINE = frozenset({"bootstrap", "import", "recover"})


def _display_name(value: str) -> str:
    cleaned = value.strip()
    if not 1 <= len(cleaned) <= 64 or any(
        unicodedata.category(char).startswith("C") for char in cleaned
    ):
        raise ServiceError("invalid_input", 422)
    return cleaned


def _request_key(args: argparse.Namespace) -> str:
    key = str(args.request_key)
    if REQUEST_KEY.fullmatch(key) is None:
        raise ServiceError("invalid_input", 422)
    return key


def _membership(raw: str) -> dict[str, Any]:
    parts = raw.split(":")
    if len(parts) not in (2, 3) or not parts[0] or parts[1] not in ROLES:
        raise ServiceError("invalid_input", 422)
    if len(parts) == 2:
        approve = True
    elif parts[2] in {"approve", "no-approve"}:
        approve = parts[2] == "approve"
    else:
        raise ServiceError("invalid_input", 422)
    return {"workspace_id": parts[0], "role": parts[1], "approve": approve}


def _memberships(args: argparse.Namespace) -> list[dict[str, Any]]:
    return [_membership(raw) for raw in getattr(args, "membership", ()) or ()]


def _display_names(args: argparse.Namespace) -> dict[str, str]:
    result: dict[str, str] = {}
    for raw in getattr(args, "display_name", ()) or ():
        principal, separator, name = raw.partition("=")
        if not separator or not principal:
            raise ServiceError("invalid_input", 422)
        result[principal] = _display_name(name)
    return result


def _query(**values: Any) -> str:
    params = {key: value for key, value in values.items() if value is not None}
    return "?" + urlencode(params) if params else ""


async def _call(
    args: argparse.Namespace, method: str, path: str, body: dict[str, Any] | None = None
) -> Any:
    client = AsyncRunClient(args.url, os.environ.get("KRIM_AGENT_API_KEY", ""))
    try:
        return await client.request(method, path, body)
    finally:
        await client.close()


async def execute_online(args: argparse.Namespace) -> Any:
    operation = args.operation
    if operation == "profile":
        return await _call(args, "GET", "/v2/me/profile")
    if operation == "list":
        return await _call(
            args,
            "GET",
            "/v2/admin/accounts"
            + _query(offset=args.offset, limit=args.limit, status=args.status, query=args.query),
        )
    if operation == "get":
        return await _call(args, "GET", "/v2/admin/accounts/" + quote(args.id, safe=""))
    if operation == "events":
        return await _call(
            args,
            "GET",
            "/v2/admin/account-events"
            + _query(
                offset=args.offset,
                limit=args.limit,
                principal_id=args.id,
                request_key=args.request_key,
            ),
        )
    key = _request_key(args)
    if operation == "create":
        body: dict[str, Any] = {
            "display_name": _display_name(args.display_name),
            "platform_role": args.platform_role,
            "memberships": _memberships(args),
            "request_key": key,
        }
        return await _call(args, "POST", "/v2/admin/accounts", body)
    if operation == "update":
        body = {"request_key": key}
        if args.display_name is not None:
            body["display_name"] = _display_name(args.display_name)
        if args.status is not None:
            body["status"] = args.status
        if args.platform_role is not None:
            body["platform_role"] = args.platform_role
        if args.revision is not None:
            body["expected_revision"] = args.revision
        if len(body) == 1:
            raise ServiceError("invalid_input", 422)
        return await _call(args, "PATCH", "/v2/admin/accounts/" + quote(args.id, safe=""), body)
    if operation == "issue":
        return await _call(
            args,
            "POST",
            "/v2/admin/accounts/" + quote(args.id, safe="") + "/key/issue",
            {"request_key": key},
        )
    if operation == "rotate":
        return await _call(args, "POST", "/v2/me/key/rotate", {"request_key": key})
    if operation == "revoke":
        return await _call(
            args,
            "POST",
            "/v2/admin/accounts/" + quote(args.id, safe="") + "/key/revoke",
            {"request_key": key},
        )
    if operation == "memberships":
        body = {"memberships": _memberships(args), "request_key": key}
        if args.generation is not None:
            body["expected_generation"] = args.generation
        return await _call(
            args, "PUT", "/v2/admin/accounts/" + quote(args.id, safe="") + "/memberships", body
        )
    raise ServiceError("invalid_input", 422)


def _read_global_key(args: argparse.Namespace) -> str:
    if args.global_key_env:
        token = os.environ.get("KRIM_AGENT_API_KEY", "")
    elif sys.stdin.isatty():
        token = getpass.getpass("输入原全局密钥：")
    else:
        token = sys.stdin.readline().rstrip("\n")
    if not token or len(token) > 256:
        raise ServiceError("invalid_input", 422)
    return token


def _source_policy(args: argparse.Namespace) -> tuple[AccessPolicy, str]:
    """严格加载旧身份来源；密钥摘要化交给服务，CLI不落盘不明文。"""
    if args.policy is not None:
        return AccessPolicy.load(Path(args.policy)), "policy_file"
    credential = Credential(
        principal_id="local",
        token=SecretStr(_read_global_key(args)),
        memberships=(Membership(workspace_id="local", role="admin"),),
        legacy=True,
    )
    return AccessPolicy(credentials=(credential,)), "global_key"


def _import_plan(args: argparse.Namespace) -> dict[str, Any]:
    policy, source = _source_policy(args)
    if not any(item.principal_id == args.admin_id for item in policy.credentials):
        raise ServiceError("invalid_input", 422)
    return {
        "source": source,
        "principal_count": len(policy.credentials),
        "principals": [
            {
                "principal_id": item.principal_id,
                "generation": item.generation,
                "legacy": item.legacy,
                "workspaces": [
                    {"workspace_id": m.workspace_id, "role": m.role, "approve": m.approve}
                    for m in item.memberships
                ],
            }
            for item in policy.credentials
        ],
        "platform_admin": args.admin_id,
        "display_names": _display_names(args),
    }


async def _database_preflight(path: Path) -> dict[str, Any]:
    """只读预检：验证schema/版本，报告托管状态与历史归属；不升级、不写库、不含密钥。"""
    if not await asyncio.to_thread(path.exists):
        return {"exists": False, "would_initialize": True}
    from krim_agent.storage.account_models import (
        account_meta,
        account_workspaces,
        accounts,
        workspace_profiles,
    )
    from krim_agent.storage.models import sessions as sessions_table
    from krim_agent.storage.sqlite import SQLiteStore

    store = await SQLiteStore.open(path, readonly=True)
    try:
        async with store.engine.connect() as conn:
            tables = set(
                (
                    await conn.exec_driver_sql("SELECT name FROM sqlite_master WHERE type='table'")
                ).scalars()
            )
            report: dict[str, Any] = {
                "exists": True,
                "schema_version": store.schema_version,
                "managed": False,
            }
            if "sessions" in tables:
                owners = (
                    await conn.execute(
                        select(sessions_table.c.owner_id, func.count())
                        .group_by(sessions_table.c.owner_id)
                        .order_by(sessions_table.c.owner_id)
                        .limit(100)
                    )
                ).all()
                report["sessions_total"] = (
                    await conn.execute(select(func.count()).select_from(sessions_table))
                ).scalar_one()
                report["session_owners"] = [
                    {"principal_id": owner, "sessions": count} for owner, count in owners
                ]
                pairs = (
                    await conn.execute(
                        select(sessions_table.c.workspace_id, sessions_table.c.owner_id)
                        .distinct()
                        .limit(5000)
                    )
                ).all()
                grouped: dict[str, set[str]] = {}
                for workspace, owner in pairs:
                    grouped.setdefault(workspace, set()).add(owner)
                report["shared_session_workspaces"] = [
                    {"workspace_id": workspace, "owners": sorted(members)}
                    for workspace, members in sorted(grouped.items())
                    if len(members) > 1
                ][:100]
            if "accounts" in tables:
                bodies = (
                    await conn.execute(select(account_meta.c.body).limit(16))
                ).scalars().all()
                identity = next(
                    (
                        body
                        for body in bodies
                        if isinstance(body, dict) and body.get("mode") == "managed"
                    ),
                    None,
                )
                report["managed"] = identity is not None
                report["identity_source"] = (
                    identity.get("source") if isinstance(identity, dict) else None
                )
                rows = (
                    await conn.execute(
                        select(
                            accounts.c.principal_id,
                            accounts.c.display_name,
                            accounts.c.status,
                            accounts.c.platform_role,
                            accounts.c.legacy,
                        )
                        .order_by(accounts.c.principal_id)
                        .limit(100)
                    )
                ).mappings().all()
                report["account_total"] = (
                    await conn.execute(select(func.count()).select_from(accounts))
                ).scalar_one()
                report["accounts"] = [
                    {
                        "principal_id": row["principal_id"],
                        "display_name": row["display_name"],
                        "status": row["status"],
                        "platform_role": row["platform_role"],
                        "legacy": bool(row["legacy"]),
                    }
                    for row in rows
                ]
                report["legacy_owners"] = (
                    (
                        await conn.execute(
                            select(accounts.c.principal_id)
                            .where(accounts.c.legacy)
                            .order_by(accounts.c.principal_id)
                            .limit(100)
                        )
                    )
                    .scalars()
                    .all()
                )
                memberships = (
                    await conn.execute(
                        select(
                            account_workspaces.c.workspace_id,
                            account_workspaces.c.principal_id,
                        )
                        .order_by(account_workspaces.c.workspace_id)
                        .limit(5000)
                    )
                ).all()
                member_map: dict[str, set[str]] = {}
                for workspace, principal in memberships:
                    member_map.setdefault(workspace, set()).add(principal)
                report["shared_membership_workspaces"] = [
                    {"workspace_id": workspace, "members": sorted(members)}
                    for workspace, members in sorted(member_map.items())
                    if len(members) > 1
                ][:100]
                report["personal_workspaces"] = (
                    await conn.execute(
                        select(func.count()).select_from(workspace_profiles).where(
                            workspace_profiles.c.owner_id.is_not(None)
                        )
                    )
                ).scalar_one()
            return report
    finally:
        await store.close()


async def _preflight(args: argparse.Namespace) -> dict[str, Any]:
    report: dict[str, Any] = {
        "operation": args.operation,
        "dry_run": True,
        "database": await _database_preflight(Path(args.database)),
    }
    if args.operation == "import":
        report.update(_import_plan(args))
    elif args.operation == "bootstrap":
        report["display_name"] = _display_name(args.display_name)
        report["principal_id"] = args.principal_id
        report["claim_local"] = bool(args.claim_local)
    else:
        report["principal_id"] = args.id
    return report


async def execute_offline(args: argparse.Namespace) -> Any:
    from krim_agent.accounts.service import IdentityService
    from krim_agent.storage.sqlite import SQLiteStore

    key = _request_key(args)
    if args.dry_run:
        return await _preflight(args)
    import_policy: AccessPolicy | None = None
    import_names: dict[str, str] = {}
    if args.operation == "import":
        import_policy = _source_policy(args)[0]
        import_names = _display_names(args)
        if not any(item.principal_id == args.admin_id for item in import_policy.credentials):
            raise ServiceError("invalid_input", 422)
    try:
        store = await SQLiteStore.open(Path(args.database))
    except ServiceError as error:
        if error.code == "database_busy":
            print("运行中的服务持有数据库写端；停止服务后重试", file=sys.stderr)
        raise
    result: Any
    try:
        identities = await IdentityService.open(store)
        try:
            if args.operation == "bootstrap":
                result = await identities.bootstrap(
                    _display_name(args.display_name),
                    request_key=key,
                    principal_id=args.principal_id,
                    claim_local=bool(args.claim_local),
                )
            elif args.operation == "import" and import_policy is not None:
                result = await identities.import_policy(
                    import_policy, args.admin_id, request_key=key, display_names=import_names
                )
            else:
                result = await identities.recover(args.id, request_key=key, actor="host")
        finally:
            await identities.close()
    finally:
        await store.close()
    if isinstance(result, BaseModel):
        return result.model_dump(mode="json")
    return result


async def execute_account(args: argparse.Namespace) -> Any:
    if args.operation in OFFLINE:
        return await execute_offline(args)
    return await execute_online(args)


def add_account_parser(sub: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    account = sub.add_parser("account", help="账号管理：在线经API，离线初始化需停止服务")
    operations = account.add_subparsers(dest="operation", required=True)
    for operation in (
        "profile",
        "list",
        "get",
        "events",
        "create",
        "update",
        "issue",
        "rotate",
        "revoke",
        "memberships",
    ):
        child = operations.add_parser(operation)
        child.add_argument("--url", required=True)
        if operation in MUTATIONS:
            child.add_argument("--request-key", required=True)
        if operation in {"get", "update", "issue", "revoke", "memberships"}:
            child.add_argument("--id", required=True)
        if operation in {"list", "events"}:
            child.add_argument("--offset", type=int, default=0)
            child.add_argument("--limit", type=int, default=20)
        if operation == "list":
            child.add_argument("--status", choices=["active", "disabled"])
            child.add_argument("--query")
        if operation == "events":
            child.add_argument("--id")
            child.add_argument("--request-key")
        if operation == "create":
            child.add_argument("--display-name", required=True)
            child.add_argument(
                "--platform-role", choices=["member", "platform_admin"], default="member"
            )
            child.add_argument("--membership", action="append", default=[], metavar="工作区:角色")
        if operation == "update":
            child.add_argument("--display-name")
            child.add_argument("--status", choices=["active", "disabled"])
            child.add_argument("--platform-role", choices=["member", "platform_admin"])
            child.add_argument("--revision", type=int, required=True)
        if operation == "memberships":
            child.add_argument("--membership", action="append", default=[], metavar="工作区:角色")
            child.add_argument("--generation", type=int, required=True)
    for operation in sorted(OFFLINE):
        child = operations.add_parser(operation)
        child.add_argument("--database", required=True)
        child.add_argument("--request-key", required=True)
        child.add_argument("--dry-run", action="store_true")
        if operation == "bootstrap":
            child.add_argument("--display-name", required=True)
            child.add_argument("--principal-id")
            child.add_argument("--claim-local", action="store_true")
        if operation == "import":
            source = child.add_mutually_exclusive_group(required=True)
            source.add_argument("--policy", type=Path)
            source.add_argument("--global-key-env", action="store_true")
            source.add_argument("--global-key-stdin", action="store_true")
            child.add_argument("--admin-id", required=True)
            child.add_argument(
                "--display-name", action="append", default=[], metavar="PRINCIPAL=名称"
            )
        if operation == "recover":
            child.add_argument("--id", required=True)
