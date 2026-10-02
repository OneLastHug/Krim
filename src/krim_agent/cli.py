"""krim 最小客户端与本地服务入口；命令含义见 docs/user-guide/m1-local.md。"""

from __future__ import annotations

import argparse
import os

import httpx
from pydantic import ValidationError

from . import __version__
from .client import ClientError, KrimClient
from .protocol.domain import TERMINAL_STATUSES


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="krim", description="Krim Agent 离线 M1")
    subparsers = parser.add_subparsers(dest="command", required=True)
    serve = subparsers.add_parser("serve", help="serve（提供服务）：启动 Python HTTP/SSE")
    serve.add_argument("--host", default="127.0.0.1", help="host（主机）：默认只监听本机")
    serve.add_argument("--port", default=8765, type=int, help="port（端口）：默认 8765")
    subparsers.add_parser("version", help="version（版本）：显示当前版本")
    for command, help_text in (
        ("chat", "chat（对话）：发送消息并订阅输出"),
        ("topics", "topics（话题）：列出历史话题"),
        ("messages", "messages（消息）：重新读取持久化历史"),
        ("run", "run（运行）：查看运行状态"),
        ("stop", "stop（停止）：取消并等待持久化"),
    ):
        child = subparsers.add_parser(command, help=help_text)
        child.add_argument("--url", default="http://127.0.0.1:8765", help="服务地址")
        if command == "chat":
            child.add_argument("text", help="文本；支持 /add 2 3、/echo 内容")
            child.add_argument("--topic", help="复用话题 ID，不指定则创建新话题")
            child.add_argument("--idempotency-key", help="idempotency（幂等）键，重试时复用")
        elif command == "messages":
            child.add_argument("topic_id")
        elif command in {"run", "stop"}:
            child.add_argument("run_id")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    if args.command == "version":
        print(f"Krim Agent {__version__}")
        return
    if args.command == "serve":
        import uvicorn

        from .api.boundary import local_host
        from .api.server import create_app
        from .config import Settings

        if not 1 <= args.port <= 65535:
            parser.error("端口必须在 1–65535 之间")
        settings = Settings.from_env()
        if not settings.api_key.get_secret_value() and not local_host(args.host):
            parser.error("无 API Key 时不能监听非本机地址")
        uvicorn.run(create_app(settings), host=args.host, port=args.port, workers=1)
        return
    try:
        with KrimClient(args.url, os.getenv("KRIM_AGENT_API_KEY", "")) as client:
            if args.command == "topics":
                for topic in client.topics():
                    print(f"{topic.id}\t{topic.title}")
            elif args.command == "messages":
                for message in client.messages(args.topic_id):
                    print(f"[{message.sequence} {message.role}] {message.content}")
            elif args.command in {"run", "stop"}:
                run = (
                    client.stop(args.run_id) if args.command == "stop" else client.run(args.run_id)
                )
                print(run.model_dump_json(indent=2))
            elif args.command == "chat":
                topic_id = args.topic or client.create_topic(args.text[:80]).id
                run = client.send(topic_id, args.text, args.idempotency_key)
                print(f"话题 ID: {topic_id}\n运行 ID: {run.id}", flush=True)
                if run.status in TERMINAL_STATUSES:
                    for message in client.messages(topic_id):
                        if (
                            message.run_id == run.id
                            and message.role == "assistant"
                            and message.content
                        ):
                            print(message.content)
                    print(f"终态: {run.status}")
                    return
                for frame in client.events(run.id):
                    if isinstance(frame.data, dict) and frame.data.get("type") == "text_delta":
                        print(frame.data["text"], end="", flush=True)
                    elif frame.kind == "end":
                        print(f"\n终态: {frame.data}")
    except KeyboardInterrupt:
        print("\n仅断开客户端订阅；后台可能继续运行。需要停止请使用 stop 命令。")
    except (ClientError, httpx.HTTPError, ValidationError) as exc:
        parser.exit(1, f"操作失败: {exc}\n")
