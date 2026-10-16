"""缺失基线回退CLI：显式原键/版本/保留路径，不自动重发写动作。"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from pydantic import BaseModel

from krim_agent.client import KrimClient
from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace_missing_directory_rollback import (
    MissingDirectoryRollbackAction,
    MissingDirectoryRollbackAttemptView,
    MissingDirectoryRollbackPreview,
    MissingDirectoryRollbackView,
)
from krim_agent.workspace_missing_directory_rollback_control import MissingDirectoryRollbackQuery


def add_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser(
        "missing-directory-rollbacks",
        help="missing directory rollbacks（缺失基线回退）：完整保留当前树后恢复缺失",
    )
    parser.add_argument("action", choices=["preview", "change", "query", "reconcile", "cancel"])
    parser.add_argument("agent_id", help="原Agent标识")
    parser.add_argument("value", nargs="?", help="reconcile用原operation ID，cancel用原proposal ID")
    parser.add_argument("--request", help="request（请求）：preview/change本地严格JSON文件")
    parser.add_argument(
        "--kind",
        choices=["proposal", "operation", "by_key", "operation_by_key", "history", "proposals"],
        default="proposals",
        help="kind（种类）：query分支",
    )
    parser.add_argument("--identifier", help="identifier（标识）：proposal/operation/history原ID")
    parser.add_argument("--request-key", help="request key（请求键）：by_key/operation_by_key原键")
    parser.add_argument("--after", help="after（后续游标）：列表/历史起点")
    parser.add_argument(
        "--limit", type=int, default=4, help="limit（页容量）：最大4，空页有游标仍继续"
    )
    parser.add_argument(
        "--confirm", action="store_true", help="confirm（确认）：只确认原参数，不覆盖deny"
    )
    parser.add_argument("--url", default="http://127.0.0.1:8765", help="本机服务地址")


def run_command(
    client: KrimClient, args: argparse.Namespace, parser: argparse.ArgumentParser
) -> None:
    if args.action not in {"preview", "change"} and args.request is not None:
        parser.error("--request仅用于缺失回退preview/change")
    if args.action not in {"cancel", "reconcile"} and args.value is not None:
        parser.error("只有cancel/reconcile接受原位置ID")
    if args.action in {"cancel", "reconcile"} and args.value is None:
        parser.error("cancel/reconcile需要原ID")
    if args.action != "query" and (
        args.kind != "proposals"
        or args.identifier is not None
        or args.request_key is not None
        or args.after is not None
        or args.limit != 4
    ):
        parser.error("--kind/--identifier/--request-key/--after/--limit仅用于query")
    result: BaseModel
    if args.action in {"preview", "change"}:
        if not args.request:
            parser.error("preview/change需要--request本地严格JSON")
        try:
            with Path(args.request).open("rb") as source:
                raw = source.read(65537)
            if len(raw) > 65536:
                parser.error("缺失回退请求不能超过64KiB")
            if args.action == "preview":
                preview = MissingDirectoryRollbackPreview.model_validate_json(raw)
                result = client.agent_missing_directory_rollback_preview(
                    args.agent_id, preview, confirmed=args.confirm
                )
            else:
                action = MissingDirectoryRollbackAction.model_validate_json(raw)
                result = client.agent_missing_directory_rollback_change(
                    args.agent_id, action, confirmed=args.confirm
                )
        except (OSError, ValueError, ServiceError):
            parser.error("缺失回退请求文件无效")
    elif args.action == "query":
        try:
            request = MissingDirectoryRollbackQuery(
                kind=args.kind,
                identifier=args.identifier,
                request_key=args.request_key,
                after=args.after,
                limit=args.limit,
            )
        except (ValueError, TypeError):
            parser.error("回退query身份/分页无效")
        result = client.agent_missing_directory_rollback_query(
            args.agent_id, request, confirmed=args.confirm
        )
    elif args.action == "reconcile":
        result = client.agent_missing_directory_rollback_reconcile(
            args.agent_id, args.value, confirmed=args.confirm
        )
    else:
        result = client.agent_missing_directory_rollback_cancel(
            args.agent_id, args.value, confirmed=args.confirm
        )
    print(json.dumps(result.model_dump(mode="json"), ensure_ascii=False, indent=2))
    if (
        isinstance(result, MissingDirectoryRollbackAttemptView)
        and result.operation.status != "done"
    ):
        parser.exit(1)
    if isinstance(result, MissingDirectoryRollbackView) and result.move.status in {
        "rejected",
        "uncertain",
    }:
        parser.exit(1)
