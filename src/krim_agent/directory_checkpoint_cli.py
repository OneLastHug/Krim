"""目录检查点CLI；查询原任务只读取持久事实。"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from krim_agent.client import KrimClient
from krim_agent.workspace_directory_checkpoint import (
    DirectoryCheckpointCreate,
    DirectoryCheckpointView,
)


def add_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser(
        "directory-checkpoints", help="directory checkpoints（目录检查点）：完整捕获和历史控制"
    )
    parser.add_argument("action", choices=["create", "get", "by-key", "list", "wait", "cancel"])
    parser.add_argument("agent_id", help="Agent实例标识")
    parser.add_argument("value", nargs="?", help="原检查点ID；by-key使用原请求键")
    parser.add_argument("--request", help="request（请求）：create使用的本地严格JSON文件")
    parser.add_argument(
        "--confirm", action="store_true", help="confirm（确认）：仅确认原捕获参数，不能覆盖deny"
    )
    parser.add_argument("--offset", type=int, default=0, help="offset（偏移）：list起始项")
    parser.add_argument("--limit", type=int, default=32, help="limit（容量）：list每页1–32项")
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
    if args.action != "create" and (args.request is not None or args.confirm):
        parser.error("--request/--confirm仅用于目录检查点create")
    if args.action != "list" and (args.offset != 0 or args.limit != 32):
        parser.error("--offset/--limit仅用于目录检查点list")
    if args.action != "wait" and args.timeout_seconds != 30:
        parser.error("--timeout-seconds仅用于目录检查点wait")
    if args.action in {"create", "list"} and args.value is not None:
        parser.error("目录检查点create/list不接受位置标识")
    if args.action not in {"create", "list"} and not args.value:
        parser.error("目录检查点动作需要原ID或by-key请求键")
    result: DirectoryCheckpointView | list[DirectoryCheckpointView] | None
    if args.action == "create":
        if not args.request:
            parser.error("目录检查点create需要--request本地JSON文件")
        try:
            with Path(args.request).open("rb") as source:
                raw = source.read(65537)
            if len(raw) > 65536:
                parser.error("目录捕获请求不能超过64KiB")
            request = DirectoryCheckpointCreate.model_validate_json(raw)
        except (OSError, ValueError):
            parser.error("目录捕获请求无效；须提供规范root_path与request_key，可选title")
        result = client.agent_workspace_create_directory_checkpoint(
            args.agent_id, request, confirmed=args.confirm
        )
    elif args.action == "list":
        result = client.agent_workspace_directory_checkpoints(
            args.agent_id, offset=args.offset, limit=args.limit
        )
    elif args.action == "by-key":
        result = client.agent_workspace_directory_checkpoint_by_key(args.agent_id, args.value)
    elif args.action == "get":
        result = client.agent_workspace_directory_checkpoint(args.agent_id, args.value)
    elif args.action == "wait":
        result = client.agent_workspace_wait_directory_checkpoint(
            args.agent_id, args.value, timeout_seconds=args.timeout_seconds
        )
    else:
        result = client.agent_workspace_cancel_directory_checkpoint(args.agent_id, args.value)
    value = (
        [item.model_dump(mode="json") for item in result]
        if isinstance(result, list)
        else result.model_dump(mode="json")
        if result is not None
        else None
    )
    print(json.dumps(value, ensure_ascii=False, indent=2))
    if (
        isinstance(result, DirectoryCheckpointView)
        and args.action in {"create", "wait"}
        and result.status not in {"capturing", "available"}
    ):
        parser.exit(1)
