"""显式会话恢复CLI；原key只读找回，不重新生成旧任务或调用模型。"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from krim_agent.client import ClientError, KrimClient
from krim_agent.conversation_recovery import (
    ConversationRecoveryPreview,
    ConversationRecoveryRecord,
    ConversationRecoveryRequest,
)


def add_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser(
        "conversation-recoveries",
        help="conversation recoveries（会话恢复）：保留原工具组后建立恢复分支",
    )
    parser.add_argument("action", choices=["preview", "apply", "by-key"])
    parser.add_argument("agent_id", help="原Agent标识")
    parser.add_argument("value", nargs="?", help="by-key使用原request key（请求键）")
    parser.add_argument("--request", help="request（请求）：apply用本地严格JSON文件，最大64KiB")
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="confirm（确认）：只确认当前源路径/版本，不重发旧工具",
    )
    parser.add_argument("--url", default="http://127.0.0.1:8765", help="本机服务地址")


def run_command(
    client: KrimClient, args: argparse.Namespace, parser: argparse.ArgumentParser
) -> None:
    if args.action != "apply" and (args.request is not None or args.confirm):
        parser.error("--request/--confirm仅用于apply")
    if (args.action == "by-key") != (args.value is not None):
        parser.error("只有by-key接受且必须提供原请求键")
    value: ConversationRecoveryPreview | ConversationRecoveryRecord | None
    try:
        if args.action == "preview":
            value = client.agent_conversation_recovery_preview(args.agent_id)
        elif args.action == "by-key":
            value = client.agent_conversation_recovery_by_key(args.agent_id, args.value)
        else:
            if not args.request:
                parser.error("apply需要--request，先审阅原preview")
            if not args.confirm:
                parser.error("apply需要--confirm明确确认原源路径与版本")
            try:
                with Path(args.request).open("rb") as stream:
                    raw = stream.read(65537)
                if len(raw) > 65536:
                    parser.error("恢复请求超过64KiB")
                request = ConversationRecoveryRequest.model_validate_json(raw)
            except (OSError, ValueError):
                parser.error("恢复请求文件无效")
            value = client.agent_conversation_recovery_apply(
                args.agent_id, request, confirmed=args.confirm
            )
    except ClientError as error:
        if error.status == 422 and error.code == "conversation_recovery_parameters":
            parser.error("会话恢复参数或原身份无效")
        raise
    print(
        json.dumps(
            value.model_dump(mode="json") if value is not None else None,
            ensure_ascii=False,
            indent=2,
        )
    )
