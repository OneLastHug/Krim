"""缺失目录恢复CLI；所有动作通过同一公开Client，不自动重发生产请求。"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from pydantic import BaseModel

from krim_agent.client import KrimClient
from krim_agent.workspace_missing_directory_restore import (
    MissingDirectoryRestoreExecute,
    MissingDirectoryRestorePreview,
    MissingDirectoryRestoreView,
)


def add_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser(
        "missing-directory-restores",
        help="directory restores（缺失目录恢复）：完整预览、发布和原操作核实",
    )
    parser.add_argument(
        "action",
        choices=[
            "preview",
            "start",
            "verify",
            "get",
            "by-key",
            "list",
            "diff",
            "operation",
            "operation-by-key",
            "wait",
            "cancel",
        ],
    )
    parser.add_argument("agent_id", help="原Agent实例标识")
    parser.add_argument(
        "value", nargs="?", help="原计划ID；verify/operation用原操作ID；by-key用原键"
    )
    parser.add_argument("--request", help="request（请求）：preview/start使用的本地严格JSON文件")
    parser.add_argument(
        "--confirm", action="store_true", help="confirm（确认）：仅确认原参数，不覆盖deny"
    )
    parser.add_argument("--offset", type=int, default=0, help="offset（偏移）：历史/差异起始项")
    parser.add_argument("--limit", type=int, help="limit（容量）：历史默认32，差异默认128")
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=30,
        help="timeout seconds（等待秒数）：wait使用0–300",
    )
    parser.add_argument("--url", default="http://127.0.0.1:8765", help="服务地址")


def run_command(
    client: KrimClient, args: argparse.Namespace, parser: argparse.ArgumentParser
) -> None:
    if args.action not in {"preview", "start"} and args.request is not None:
        parser.error("--request仅用于缺失目录恢复preview/start")
    if args.action not in {"preview", "start", "verify"} and args.confirm:
        parser.error("--confirm仅用于缺失目录恢复preview/start/verify")
    if args.action not in {"list", "diff"} and (args.offset != 0 or args.limit is not None):
        parser.error("--offset/--limit仅用于缺失目录恢复list/diff")
    if args.action != "wait" and args.timeout_seconds != 30:
        parser.error("--timeout-seconds仅用于缺失目录恢复wait")
    if args.action in {"preview", "start", "list"} and args.value is not None:
        parser.error("缺失目录恢复preview/start/list不接受位置标识")
    if args.action not in {"preview", "start", "list"} and not args.value:
        parser.error("此缺失目录恢复动作需要原ID或请求键")
    result: BaseModel | list[MissingDirectoryRestoreView] | None
    if args.action in {"preview", "start"}:
        if not args.request:
            parser.error("缺失目录恢复preview/start需要--request本地JSON文件")
        try:
            with Path(args.request).open("rb") as source:
                raw = source.read(65537)
            if len(raw) > 65536:
                parser.error("缺失目录恢复请求不能超过64KiB")
            if args.action == "preview":
                preview = MissingDirectoryRestorePreview.model_validate_json(raw)
                result = client.agent_workspace_preview_missing_directory_restore(
                    args.agent_id, preview, confirmed=args.confirm
                )
            else:
                execution = MissingDirectoryRestoreExecute.model_validate_json(raw)
                result = client.agent_workspace_execute_missing_directory_restore(
                    args.agent_id, execution, confirmed=args.confirm
                )
        except (OSError, ValueError):
            parser.error("缺失目录恢复请求文件无效，须为严格原目标/原版本/请求键JSON")
    elif args.action == "verify":
        result = client.agent_workspace_verify_missing_directory_restore(
            args.agent_id, args.value, confirmed=args.confirm
        )
    elif args.action == "get":
        result = client.agent_workspace_missing_directory_restore(args.agent_id, args.value)
    elif args.action == "by-key":
        result = client.agent_workspace_missing_directory_restore_by_key(args.agent_id, args.value)
    elif args.action == "list":
        result = client.agent_workspace_missing_directory_restores(
            args.agent_id, offset=args.offset, limit=32 if args.limit is None else args.limit
        )
    elif args.action == "diff":
        result = client.agent_workspace_missing_directory_restore_diff(
            args.agent_id,
            args.value,
            offset=args.offset,
            limit=128 if args.limit is None else args.limit,
        )
    elif args.action == "operation":
        result = client.agent_workspace_missing_directory_restore_operation(
            args.agent_id, args.value
        )
    elif args.action == "operation-by-key":
        result = client.agent_workspace_missing_directory_restore_operation_by_key(
            args.agent_id, args.value
        )
    elif args.action == "wait":
        result = client.agent_workspace_wait_missing_directory_restore(
            args.agent_id, args.value, timeout_seconds=args.timeout_seconds
        )
    else:
        result = client.agent_workspace_cancel_missing_directory_restore(args.agent_id, args.value)
    value = (
        [item.model_dump(mode="json") for item in result]
        if isinstance(result, list)
        else result.model_dump(mode="json")
        if result is not None
        else None
    )
    print(json.dumps(value, ensure_ascii=False, indent=2))
    if isinstance(result, MissingDirectoryRestoreView) and (
        args.action in {"preview", "start", "verify", "wait"}
        and result.status not in {"preparing", "ready", "running", "done"}
        or args.action == "cancel"
        and result.status == "uncertain"
    ):
        parser.exit(1)
