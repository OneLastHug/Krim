"""krim 最小客户端与本地服务入口；命令含义见 docs/user-guide/m1-local.md。"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import httpx
from pydantic import ValidationError

from . import __version__
from .budgets import BudgetResolveRequest
from .client import ClientError, KrimClient
from .context_engine import ContextOptions
from .interactions import InteractionReply
from .protocol.domain import TERMINAL_STATUSES
from .workflow import WorkflowGraph


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="krim", description="Krim Agent 本地对话、分支与持久续传")
    subparsers = parser.add_subparsers(dest="command", required=True)
    serve = subparsers.add_parser("serve", help="serve（提供服务）：启动 Python HTTP/SSE")
    serve.add_argument("--host", default="127.0.0.1", help="host（主机）：默认只监听本机")
    serve.add_argument("--port", default=8765, type=int, help="port（端口）：默认 8765")
    subparsers.add_parser("version", help="version（版本）：显示当前版本")
    budget = subparsers.add_parser("budget", help="budget（预算）：读取累计账本和核实未知请求")
    budget.add_argument("action", choices=["accounts", "account", "run", "request", "resolve"])
    budget.add_argument("value", nargs="?", help="account/run/request 标识")
    budget.add_argument("--url", default="http://127.0.0.1:8765", help="服务地址")
    budget.add_argument("--revision", type=int, help="revision（修订号）：匹配当前未知请求")
    budget.add_argument("--receipt", help="receipt（回执）：可信宿主核实依据")
    budget.add_argument("--usage", help="usage（用量）：输入/输出/总量的 JSON 报告")
    interaction = subparsers.add_parser(
        "interaction", help="interaction（用户交互）：读取审批/问题和提交回应"
    )
    interaction.add_argument("action", choices=["list", "approve", "deny", "answer"])
    interaction.add_argument("run_id")
    interaction.add_argument("interaction_id", nargs="?")
    interaction.add_argument("answer", nargs="?")
    interaction.add_argument(
        "--pending", action="store_true", help="pending（待处理）：只列出当前待回应请求"
    )
    interaction.add_argument("--url", default="http://127.0.0.1:8765", help="服务地址")
    agent = subparsers.add_parser("agent", help="agent（Agent）：目录、执行与持久事件")
    agent.add_argument(
        "action",
        choices=[
            "catalog",
            "list",
            "get",
            "spawn",
            "execute",
            "resume",
            "events",
            "stop",
            "destroy",
        ],
    )
    agent.add_argument("value", nargs="?", help="Agent ID 或 spawn 时的目录名称")
    agent.add_argument("prompt", nargs="?", help="execute/resume 的 prompt（提示）")
    agent.add_argument("--url", default="http://127.0.0.1:8765", help="服务地址")
    agent.add_argument("--idempotency-key", help="重试去重键")
    agent.add_argument("--from-seq", type=int, default=0, help="events 起始序号")
    agent.add_argument(
        "--persistent", action="store_true", help="persistent（持久）：spawn 句柄可由后续进程恢复"
    )
    compact = subparsers.add_parser("compact", help="compact（压缩）：预览或确认持久历史摘要")
    compact.add_argument("topic_id")
    compact.add_argument("--url", default="http://127.0.0.1:8765", help="服务地址")
    compact.add_argument(
        "--strategy",
        choices=["summarize_then_truncate", "annotated_compact"],
        default="annotated_compact",
    )
    compact.add_argument("--policy", choices=["never", "always", "auto"], default="auto")
    compact.add_argument("--max-messages", type=int, help="模型输入消息数量预算，包含摘要")
    compact.add_argument("--max-chars", type=int, help="JSON 请求文本字符预算")
    compact.add_argument(
        "--max-tokens", type=int, help="按服务端 tokenizer（词元编码器）计算的预算"
    )
    compact.add_argument(
        "--reserved-output-tokens",
        type=int,
        default=0,
        help="reserved output tokens（预留输出词元）：不能低于宿主预留",
    )
    compact.add_argument("--expected-branch-key", help="确认时匹配预览中的活动路径标识")
    compact_mode = compact.add_mutually_exclusive_group()
    compact_mode.add_argument(
        "--preview", action="store_true", help="preview（预览）：只读提案，默认行为"
    )
    compact_mode.add_argument("--confirm", action="store_true", help="confirm（确认）：保存摘要")
    workflow = subparsers.add_parser("workflow", help="workflow（工作流）：运行与控制持久图")
    workflow.add_argument(
        "action",
        choices=["run", "list", "get", "events", "pause", "resume", "restore", "stop", "rerun"],
    )
    workflow.add_argument("value", nargs="?", help="run 时为 JSON/文件路径，其余动作为 run_id")
    workflow.add_argument("--url", default="http://127.0.0.1:8765", help="服务地址")
    workflow.add_argument("--input", default="null", help="run 输入 JSON")
    workflow.add_argument("--idempotency-key", help="Workflow 请求幂等键")
    workflow.add_argument("--pause-before", action="append", default=[], help="节点前暂停")
    workflow.add_argument("--pause-after", action="append", default=[], help="节点后暂停")
    workflow.add_argument("--from-seq", type=int, default=0, help="事件起始序号")
    workflow.add_argument("--node", help="rerun 节点 ID")
    workflow.add_argument("--override", help="resume/rerun 输入 JSON")
    workflow.add_argument("--allow-side-effects", action="store_true")
    workflow.add_argument("--completed-outputs", help="恢复时已完成 NodeOutput 的 JSON")
    for command, help_text in (
        ("chat", "chat（对话）：发送消息并订阅输出"),
        ("topics", "topics（话题）：列出历史话题"),
        ("messages", "messages（消息）：重新读取持久化历史"),
        ("run", "run（运行）：查看运行状态"),
        ("stop", "stop（停止）：取消并等待持久化"),
        ("resume", "resume（续传）：从持久事件序号继续输出"),
        ("usage", "usage（用量）：读取每步供应商报告和完整覆盖状态"),
        ("tree", "tree（树）：查看全部消息节点与活动路径"),
        ("branch", "branch（分支）：切换活动消息分支"),
        ("edit", "edit（编辑）：创建用户消息的新分支并重新回答"),
        ("regenerate", "regenerate（重新生成）：保留用户消息并生成新回答"),
    ):
        child = subparsers.add_parser(command, help=help_text)
        child.add_argument("--url", default="http://127.0.0.1:8765", help="服务地址")
        if command == "chat":
            child.add_argument("text", help="文本；支持 /add 2 3、/echo 内容")
            child.add_argument("--topic", help="复用话题 ID，不指定则创建新话题")
            child.add_argument("--idempotency-key", help="idempotency（幂等）键，重试时复用")
            child.add_argument("--parent", help="在指定完整消息后创建分支")
        elif command in {"messages", "tree"}:
            child.add_argument("topic_id")
        elif command in {"branch", "edit", "regenerate"}:
            child.add_argument("topic_id")
            child.add_argument("message_id")
            if command == "edit":
                child.add_argument("text")
            if command != "branch":
                child.add_argument("--idempotency-key", help="重试时复用的幂等键")
        elif command in {"run", "stop", "resume", "usage"}:
            child.add_argument("run_id")
            if command == "resume":
                child.add_argument("--from-seq", type=int, default=0, help="起始事件序号")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    if args.command == "version":
        print(f"Krim Agent {__version__}")
        return
    if args.command == "workflow" and args.action == "run":
        if not args.value:
            parser.error("workflow run 需要 JSON 图或文件路径")
        try:
            source = (
                Path(args.value).read_text("utf-8") if Path(args.value).exists() else args.value
            )
            graph = WorkflowGraph.model_validate_json(source)
            input_value = json.loads(args.input)
        except (OSError, ValueError) as exc:
            parser.error(f"Workflow JSON 无效: {exc}")
        with KrimClient(args.url, os.getenv("KRIM_AGENT_API_KEY", "")) as client:
            print(
                client.workflow_run(
                    graph,
                    input_value,
                    idempotency_key=args.idempotency_key,
                    pause_before=args.pause_before,
                    pause_after=args.pause_after,
                ).model_dump_json(indent=2)
            )
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
            if args.command == "budget":
                if args.action == "accounts":
                    print(
                        json.dumps(
                            [value.model_dump(mode="json") for value in client.budget_accounts()],
                            ensure_ascii=False,
                            indent=2,
                        )
                    )
                else:
                    if not args.value:
                        parser.error("budget 动作需要标识")
                    if args.action == "account":
                        print(client.budget_account(args.value).model_dump_json(indent=2))
                    elif args.action == "run":
                        print(
                            json.dumps(
                                [
                                    value.model_dump(mode="json")
                                    for value in client.run_budget(args.value)
                                ],
                                ensure_ascii=False,
                                indent=2,
                            )
                        )
                    elif args.action == "request":
                        print(client.budget_request(args.value).model_dump_json(indent=2))
                    elif args.action == "resolve":
                        if args.revision is None or not args.receipt or not args.usage:
                            parser.error("budget resolve 需要 revision/receipt/usage")
                        request = BudgetResolveRequest.model_validate(
                            {
                                "revision": args.revision,
                                "receipt": args.receipt,
                                "usage": json.loads(args.usage),
                            }
                        )
                        print(client.budget_resolve(args.value, request).model_dump_json(indent=2))
                return
            if args.command == "interaction":
                if args.action == "list":
                    print(
                        json.dumps(
                            [
                                item.model_dump(mode="json")
                                for item in client.interactions(
                                    args.run_id, pending_only=args.pending
                                )
                            ],
                            ensure_ascii=False,
                            indent=2,
                        )
                    )
                else:
                    if not args.interaction_id:
                        parser.error("interaction 回应需要 interaction_id")
                    if args.action == "answer" and not args.answer:
                        parser.error("interaction answer 需要真实答案")
                    print(
                        client.respond_interaction(
                            args.run_id,
                            args.interaction_id,
                            InteractionReply(
                                action=args.action,
                                answer=args.answer if args.action == "answer" else None,
                            ),
                        ).model_dump_json(indent=2)
                    )
                return
            if args.command == "agent":
                if args.action == "catalog":
                    print(
                        json.dumps(
                            [item.model_dump(mode="json") for item in client.agents_catalog()],
                            ensure_ascii=False,
                            indent=2,
                        )
                    )
                elif args.action == "list":
                    print(
                        json.dumps(
                            [item.model_dump(mode="json") for item in client.agents()],
                            ensure_ascii=False,
                            indent=2,
                        )
                    )
                else:
                    if not args.value:
                        parser.error("agent 操作需要 Agent ID 或目录名称")
                    if args.action == "spawn":
                        print(
                            client.agent_spawn(
                                args.value, persistent=args.persistent
                            ).model_dump_json(indent=2)
                        )
                    elif args.action == "get":
                        print(client.agent(args.value).model_dump_json(indent=2))
                    elif args.action in {"execute", "resume"}:
                        if not args.prompt:
                            parser.error("agent execute/resume 需要 prompt")
                        view = (
                            client.agent_execute(args.value, args.prompt, args.idempotency_key)
                            if args.action == "execute"
                            else client.agent_resume(args.value, args.prompt, args.idempotency_key)
                        )
                        print(view.model_dump_json(indent=2))
                    elif args.action == "events":
                        for frame in client.agent_events(args.value, args.from_seq):
                            print(frame.model_dump_json(exclude_none=True))
                    elif args.action == "stop":
                        print(client.agent_stop(args.value).model_dump_json(indent=2))
                    elif args.action == "destroy":
                        print(client.agent_destroy(args.value).model_dump_json(indent=2))
                return
            if args.command == "compact":
                print(
                    client.compact(
                        args.topic_id,
                        ContextOptions(
                            strategy=args.strategy,
                            policy=args.policy,
                            max_messages=args.max_messages,
                            max_chars=args.max_chars,
                            max_tokens=args.max_tokens,
                            reserved_output_tokens=args.reserved_output_tokens,
                        ),
                        confirm=args.confirm,
                        expected_branch_key=args.expected_branch_key,
                    ).model_dump_json(indent=2)
                )
                return
            if args.command == "workflow":
                if args.action == "list":
                    print(
                        json.dumps(
                            [item.model_dump(mode="json") for item in client.workflows()],
                            ensure_ascii=False,
                            indent=2,
                        )
                    )
                    return
                if not args.value:
                    parser.error("workflow 操作需要 run_id")
                run_id = args.value
                if args.action == "get":
                    print(client.workflow(run_id).model_dump_json(indent=2))
                elif args.action == "events":
                    for event in client.workflow_events(run_id, args.from_seq):
                        print(event.model_dump_json(exclude_none=True))
                elif args.action == "pause":
                    print(client.workflow_pause(run_id).model_dump_json(indent=2))
                elif args.action == "stop":
                    print(client.workflow_stop(run_id).model_dump_json(indent=2))
                elif args.action == "resume":
                    value = json.loads(args.override) if args.override is not None else None
                    print(
                        client.workflow_resume(
                            run_id, value, override=args.override is not None
                        ).model_dump_json(indent=2)
                    )
                elif args.action == "restore":
                    outputs = json.loads(args.completed_outputs) if args.completed_outputs else {}
                    print(
                        client.workflow_restore(
                            run_id,
                            replay_side_effects=args.allow_side_effects,
                            completed_outputs=outputs,
                        ).model_dump_json(indent=2)
                    )
                elif args.action == "rerun":
                    if not args.node:
                        parser.error("workflow rerun 需要 --node")
                    value = json.loads(args.override) if args.override is not None else None
                    print(
                        client.workflow_rerun(
                            run_id,
                            args.node,
                            value,
                            override=args.override is not None,
                            allow_side_effects=args.allow_side_effects,
                        ).model_dump_json(indent=2)
                    )
                return
            if args.command == "topics":
                for topic in client.topics():
                    print(f"{topic.id}\t{topic.title}")
            elif args.command == "messages":
                for message in client.messages(args.topic_id):
                    print(f"[{message.sequence} {message.role}] {message.content}")
            elif args.command == "tree":
                print(client.tree(args.topic_id).model_dump_json(indent=2))
            elif args.command == "branch":
                print(
                    client.select_branch(args.topic_id, args.message_id).model_dump_json(indent=2)
                )
            elif args.command in {"edit", "regenerate"}:
                run = (
                    client.edit(args.topic_id, args.message_id, args.text, args.idempotency_key)
                    if args.command == "edit"
                    else client.regenerate(args.topic_id, args.message_id, args.idempotency_key)
                )
                print(run.model_dump_json(indent=2))
            elif args.command in {"run", "stop"}:
                run = (
                    client.stop(args.run_id) if args.command == "stop" else client.run(args.run_id)
                )
                print(run.model_dump_json(indent=2))
            elif args.command == "usage":
                print(client.usage(args.run_id).model_dump_json(indent=2))
            elif args.command == "resume":
                if args.from_seq < 0:
                    parser.error("from-seq 必须是非负整数")
                for frame in client.events(args.run_id, args.from_seq):
                    print(frame.model_dump_json(exclude_none=True))
            elif args.command == "chat":
                topic_id = args.topic or client.create_topic(args.text[:80]).id
                run = (
                    client.send(
                        topic_id, args.text, args.idempotency_key, parent_message_id=args.parent
                    )
                    if args.parent
                    else client.send(topic_id, args.text, args.idempotency_key)
                )
                print(f"话题 ID: {topic_id}\n运行 ID: {run.id}", flush=True)
                if run.status in TERMINAL_STATUSES:
                    for message in client.run_messages(run.id):
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
