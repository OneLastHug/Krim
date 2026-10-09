"""krim 最小客户端与本地服务入口；命令含义见 docs/user-guide/m1-local.md。"""

from __future__ import annotations

import argparse
import json
import os
from collections.abc import Sequence
from pathlib import Path

import httpx
from pydantic import BaseModel, ValidationError

from . import __version__
from .budgets import BudgetResolveRequest
from .client import ClientError, KrimClient
from .context_engine import ContextOptions, ContextProposalDecision
from .interactions import InteractionReply
from .protocol.domain import TERMINAL_STATUSES, ServiceError
from .streams import MultiplexRequest
from .task_state import TaskWriteRequest
from .workflow import WorkflowGraph
from .workspace_change import WorkspaceAttemptView, WorkspaceChangeAction, WorkspacePreviewRequest
from .workspace_checkpoint import WorkspaceCheckpointCreate, WorkspaceCheckpointView
from .workspace_list import NativeBatchRequest, NativeBatchView
from .workspace_restore import (
    CheckpointRestoreExecute,
    CheckpointRestorePreview,
    CheckpointRestoreView,
)

_CHECKPOINT_ACTIONS = (
    "create-checkpoint",
    "checkpoints",
    "checkpoint",
    "checkpoint-by-key",
    "wait-checkpoint",
    "cancel-checkpoint",
    "preview-restore",
    "execute-restore",
    "restores",
    "restore",
    "restore-by-key",
    "wait-restore",
    "cancel-restore",
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="krim", description="Krim Agent 本地对话、分支与持久续传")
    subparsers = parser.add_subparsers(dest="command", required=True)
    serve = subparsers.add_parser("serve", help="serve（提供服务）：启动 Python HTTP/SSE")
    serve.add_argument("--host", default="127.0.0.1", help="host（主机）：默认只监听本机")
    serve.add_argument("--port", default=8765, type=int, help="port（端口）：默认 8765")
    serve.add_argument("--workspace", help="workspace（工作区）：宿主明确授权的根目录，默认只读")
    serve.add_argument(
        "--workspace-version", default="local-v1", help="workspace version（工作区版本）：恢复校验"
    )
    serve.add_argument(
        "--workspace-write",
        action="store_true",
        help="workspace write（工作区写入）：启用受权限控制的文件修改工具",
    )
    subparsers.add_parser("version", help="version（版本）：显示当前版本")
    from .evaluation.cli import add_parser as add_eval_parser

    add_eval_parser(subparsers)
    workspace = subparsers.add_parser(
        "workspace", help="workspace（工作区）：Agent文件、修改提案、操作与异常核实"
    )
    workspace.add_argument(
        "action",
        choices=[
            "info",
            "list",
            "read",
            "search",
            "changes",
            "proposal",
            "history",
            "operation",
            "proof",
            "preview",
            "apply",
            "undo",
            "keep",
            "reconcile",
            "finalize",
            "native-changes",
            "start-batch",
            "native-batch",
            "batch-by-key",
            "batch-history",
            "wait-batch",
            "cancel-batch",
            *_CHECKPOINT_ACTIONS,
        ],
    )
    workspace.add_argument("agent_id", help="Agent实例标识")
    workspace.add_argument(
        "value", nargs="?", help="相对路径、搜索文本、proposal/operation/batch/checkpoint/plan标识"
    )
    workspace.add_argument("--path", default=".", help="path（路径）：列出/搜索的相对目录")
    workspace.add_argument("--limit", type=int, help="limit（上限）：目录项或搜索匹配数")
    workspace.add_argument("--start-line", type=int, default=1, help="start line（起始行）")
    workspace.add_argument("--max-lines", type=int, default=200, help="max lines（最大行数）")
    workspace.add_argument(
        "--request", help="request（请求）：修改/批次/检查点捕获/恢复预览的本地JSON文件"
    )
    workspace.add_argument(
        "--revision", type=int, help="revision（修订号）：文件动作或execute-restore的预期版本"
    )
    workspace.add_argument(
        "--request-key", help="request key（请求键）：文件/恢复执行的幂等键或原请求查询键"
    )
    workspace.add_argument(
        "--filter",
        choices=["active", "history", "all"],
        default="active",
        help="filter（筛选）：原生修改清单筛选范围",
    )
    workspace.add_argument("--cursor", help="cursor（游标）：原生清单快照游标")
    workspace.add_argument("--batch-id", help="batch identifier（批次标识）：查询、等待或取消批次")
    workspace.add_argument(
        "--timeout-seconds",
        type=float,
        default=30,
        help="timeout seconds（等待秒数）：批次/检查点/恢复查询最长等待时间",
    )
    workspace.add_argument(
        "--offset", type=int, default=0, help="offset（偏移）：修改/检查点/恢复列表起始项"
    )
    workspace.add_argument(
        "--confirm", action="store_true", help="confirm（确认）：本次原参数的Host确认，不覆盖deny"
    )
    workspace.add_argument("--url", default="http://127.0.0.1:8765", help="服务地址")
    tasks = subparsers.add_parser("tasks", help="tasks（任务）：读取和修改Agent持久计划/待办")
    tasks.add_argument("action", choices=["get", "history", "update"])
    tasks.add_argument("agent_id", help="Agent实例标识")
    tasks.add_argument(
        "--kind", choices=["plan", "todo"], default="todo", help="plan（计划）或todo（待办）"
    )
    tasks.add_argument("--request", help="update（更新）的本地JSON文件，包含版本和幂等键")
    tasks.add_argument("--url", default="http://127.0.0.1:8765", help="服务地址")
    trace = subparsers.add_parser("trace", help="trace（追踪）：读取脱敏 span 与观测状态")
    trace.add_argument("action", choices=["run", "workflow", "get", "status"])
    trace.add_argument("value", nargs="?")
    trace.add_argument("--url", default="http://127.0.0.1:8765", help="服务地址")
    budget = subparsers.add_parser("budget", help="budget（预算）：读取累计账本和核实未知请求")
    budget.add_argument("action", choices=["accounts", "account", "run", "request", "resolve"])
    budget.add_argument("value", nargs="?", help="account/run/request 标识")
    budget.add_argument("--url", default="http://127.0.0.1:8765", help="服务地址")
    budget.add_argument("--revision", type=int, help="revision（修订号）：匹配当前未知请求")
    budget.add_argument("--receipt", help="receipt（回执）：可信宿主核实依据")
    budget.add_argument("--usage", help="usage（用量）：输入/输出/总量的 JSON 报告")
    multiplex = subparsers.add_parser(
        "multiplex", help="multiplex（多路）：一个 SSE 连接读取多个 Run"
    )
    multiplex.add_argument("--sources", required=True, help="JSON 数组：run_id/topic_id/next_seq")
    multiplex.add_argument("--url", default="http://127.0.0.1:8765", help="服务地址")
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
            "profiles",
            "profile",
            "configure",
            "enable",
            "disable",
            "inventory",
            "resources",
            "restore",
            "session",
            "turns",
            "close",
            "reopen",
            "list",
            "get",
            "spawn",
            "execute",
            "start",
            "wait",
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
    agent.add_argument("--run-id", help="run ID（运行标识）：固定要续读的 Agent 轮次")
    agent.add_argument("--revision", type=int, help="revision（配置版本）：新建选择或更新预期版本")
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
    compact.add_argument("--proposal-id", help="proposal ID（提案标识）：确认已保存的原摘要")
    compact.add_argument(
        "--proposal-revision", type=int, help="proposal revision（提案修订号）：匹配预览"
    )
    compact_mode = compact.add_mutually_exclusive_group()
    compact_mode.add_argument(
        "--preview", action="store_true", help="preview（预览）：保存待确认提案，不改写摘要"
    )
    compact_mode.add_argument("--confirm", action="store_true", help="confirm（确认）：保存摘要")
    proposal = subparsers.add_parser(
        "proposal", help="proposal（提案）：读取或确认/拒绝持久压缩提案"
    )
    proposal.add_argument("action", choices=["list", "get", "confirm", "reject"])
    proposal.add_argument("topic_id")
    proposal.add_argument("proposal_id", nargs="?")
    proposal.add_argument("--revision", type=int, help="revision（修订号）：确认/拒绝的预期版本")
    proposal.add_argument(
        "--pending", action="store_true", help="pending（待确认）：只读取有效待办"
    )
    proposal.add_argument("--url", default="http://127.0.0.1:8765", help="服务地址")
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


def _workspace_checkpoint_command(
    client: KrimClient,
    args: argparse.Namespace,
    parser: argparse.ArgumentParser,
) -> BaseModel | Sequence[BaseModel] | None:
    if args.action in {"create-checkpoint", "preview-restore"}:
        if not args.request:
            parser.error("检查点捕获/恢复预览需要--request本地JSON文件")
        try:
            with Path(args.request).open("rb") as source:
                raw = source.read(65537)
            if len(raw) > 65536:
                parser.error("检查点请求不能超过64KiB")
            payload = (
                WorkspaceCheckpointCreate.model_validate_json(raw)
                if args.action == "create-checkpoint"
                else CheckpointRestorePreview.model_validate_json(raw)
            )
        except (OSError, ValueError):
            parser.error("检查点请求无效；请检查捕获paths或恢复checkpoint_id、request_key等字段")
        if isinstance(payload, WorkspaceCheckpointCreate):
            return client.agent_workspace_create_checkpoint(
                args.agent_id, payload, confirmed=args.confirm
            )
        return client.agent_workspace_preview_restore(
            args.agent_id, payload, confirmed=args.confirm
        )
    if args.action in {"checkpoints", "restores"}:
        limit = args.limit if args.limit is not None else 32
        if args.action == "checkpoints":
            return client.agent_workspace_checkpoints(
                args.agent_id, offset=args.offset, limit=limit
            )
        return client.agent_workspace_restores(args.agent_id, offset=args.offset, limit=limit)
    if args.action in {"checkpoint-by-key", "restore-by-key"}:
        if not args.request_key:
            parser.error("按键查询检查点或恢复计划需要--request-key")
        if args.action == "checkpoint-by-key":
            return client.agent_workspace_checkpoint_by_key(args.agent_id, args.request_key)
        return client.agent_workspace_restore_by_key(args.agent_id, args.request_key)
    if not args.value:
        parser.error("检查点动作需要checkpoint或plan标识")
    if args.action == "checkpoint":
        return client.agent_workspace_checkpoint(args.agent_id, args.value)
    if args.action == "wait-checkpoint":
        return client.agent_workspace_wait_checkpoint(
            args.agent_id, args.value, timeout_seconds=args.timeout_seconds
        )
    if args.action == "cancel-checkpoint":
        return client.agent_workspace_cancel_checkpoint(args.agent_id, args.value)
    if args.action == "restore":
        return client.agent_workspace_restore(args.agent_id, args.value)
    if args.action == "wait-restore":
        return client.agent_workspace_wait_restore(
            args.agent_id, args.value, timeout_seconds=args.timeout_seconds
        )
    if args.action == "cancel-restore":
        return client.agent_workspace_cancel_restore(args.agent_id, args.value)
    if args.revision is None or not args.request_key:
        parser.error("execute-restore需要--revision和--request-key")
    try:
        request = CheckpointRestoreExecute(
            plan_id=args.value, expected_revision=args.revision, request_key=args.request_key
        )
    except ValueError:
        parser.error("恢复执行参数无效；请检查计划标识、revision与request-key")
    return client.agent_workspace_execute_restore(args.agent_id, request, confirmed=args.confirm)


def _workspace_command(
    client: KrimClient, args: argparse.Namespace, parser: argparse.ArgumentParser
) -> None:
    result: BaseModel | Sequence[BaseModel]
    if args.action in {"native-batch", "wait-batch", "cancel-batch"} and args.batch_id:
        if args.value and args.value != args.batch_id:
            parser.error("位置参数与--batch-id的批次标识不一致")
        args.value = args.batch_id
    if args.action in _CHECKPOINT_ACTIONS:
        checkpoint_result = _workspace_checkpoint_command(client, args, parser)
        if checkpoint_result is None:
            print("null")
            return
        result = checkpoint_result
    elif args.action == "info":
        result = client.agent_workspace(args.agent_id)
    elif args.action == "list":
        result = client.agent_list_files(
            args.agent_id, args.path, limit=args.limit if args.limit is not None else 200
        )
    elif args.action == "read":
        if not args.value:
            parser.error("workspace read需要相对文件路径")
        result = client.agent_read_file(
            args.agent_id, args.value, start_line=args.start_line, max_lines=args.max_lines
        )
    elif args.action == "search":
        if not args.value:
            parser.error("workspace search需要搜索文本")
        result = client.agent_search_files(
            args.agent_id,
            args.value,
            path=args.path,
            limit=args.limit if args.limit is not None else 100,
        )
    elif args.action == "changes":
        result = client.agent_workspace_changes(
            args.agent_id, offset=args.offset, limit=args.limit if args.limit is not None else 50
        )
    elif args.action == "native-changes":
        result = client.agent_workspace_native_changes(
            args.agent_id,
            filter=args.filter,
            cursor=args.cursor,
            limit=args.limit if args.limit is not None else 32,
        )
    elif args.action == "batch-history":
        result = client.agent_workspace_native_batch_history(
            args.agent_id, cursor=args.cursor, limit=args.limit if args.limit is not None else 32
        )
    elif args.action == "start-batch":
        if not args.request:
            parser.error("workspace start-batch需要--request本地JSON文件")
        try:
            with Path(args.request).open("rb") as source:
                raw = source.read(65537)
            if len(raw) > 65536:
                parser.error("原生批次请求不能超过64KiB")
            batch_request = NativeBatchRequest.model_validate_json(raw)
        except (OSError, ValueError):
            parser.error("原生批次请求无效；请检查action、snapshot_revision、targets和request_key")
        result = client.agent_workspace_start_native_batch(
            args.agent_id, batch_request, confirmed=args.confirm
        )
    elif args.action == "native-batch":
        if not args.value:
            parser.error("workspace native-batch需要批次标识")
        result = client.agent_workspace_native_batch(args.agent_id, args.value)
    elif args.action == "batch-by-key":
        if not args.request_key:
            parser.error("workspace batch-by-key需要--request-key")
        batch = client.agent_workspace_native_batch_by_key(args.agent_id, args.request_key)
        if batch is None:
            print("null")
            return
        result = batch
    elif args.action == "wait-batch":
        if not args.value:
            parser.error("workspace wait-batch需要批次标识")
        result = client.agent_workspace_wait_native_batch(
            args.agent_id, args.value, timeout_seconds=args.timeout_seconds
        )
    elif args.action == "cancel-batch":
        if not args.value:
            parser.error("workspace cancel-batch需要批次标识")
        result = client.agent_workspace_cancel_native_batch(args.agent_id, args.value)
    elif args.action == "preview":
        if not args.request:
            parser.error("workspace preview需要--request本地JSON文件")
        try:
            with Path(args.request).open("rb") as source:
                raw = source.read(65537)
            if len(raw) > 65536:
                parser.error("文件修改请求不能超过64KiB")
            preview = WorkspacePreviewRequest.model_validate_json(raw)
        except (OSError, ValueError):
            parser.error("文件修改请求无效；请检查本地JSON文件的spec、request_key和ttl_seconds")
        result = client.agent_workspace_preview(args.agent_id, preview, confirmed=args.confirm)
    else:
        if not args.value:
            parser.error("workspace动作需要proposal或operation标识")
        if args.action == "proposal":
            result = client.agent_workspace_proposal(args.agent_id, args.value)
        elif args.action == "history":
            result = client.agent_workspace_history(args.agent_id, args.value)
        elif args.action == "operation":
            result = client.agent_workspace_operation(args.agent_id, args.value)
        elif args.action == "proof":
            result = client.agent_workspace_operation_proof(args.agent_id, args.value)
        elif args.action == "reconcile":
            result = client.agent_workspace_reconcile(
                args.agent_id, args.value, confirmed=args.confirm
            )
        elif args.action == "finalize":
            result = client.agent_workspace_finalize(
                args.agent_id, args.value, confirmed=args.confirm
            )
        else:
            if args.revision is None or not args.request_key:
                parser.error("workspace apply/undo/keep需要--revision和--request-key")
            request = WorkspaceChangeAction(
                proposal_id=args.value,
                action=args.action,
                expected_revision=args.revision,
                request_key=args.request_key,
            )
            result = client.agent_workspace_change(args.agent_id, request, confirmed=args.confirm)
    if isinstance(result, BaseModel):
        print(result.model_dump_json(indent=2))
    else:
        print(
            json.dumps(
                [item.model_dump(mode="json") for item in result], ensure_ascii=False, indent=2
            )
        )
    if (
        args.action in {"apply", "undo", "keep", "reconcile"}
        and isinstance(result, WorkspaceAttemptView)
        and result.status != "done"
    ):
        parser.exit(1)
    if (
        isinstance(result, WorkspaceCheckpointView)
        and args.action in {"create-checkpoint", "wait-checkpoint"}
        and result.status not in {"capturing", "available"}
    ):
        parser.exit(1)
    if (
        isinstance(result, CheckpointRestoreView)
        and args.action in {"preview-restore", "execute-restore", "wait-restore"}
        and result.status not in {"preparing", "ready", "running", "done"}
    ):
        parser.exit(1)
    if (
        isinstance(result, NativeBatchView)
        and args.action in {"start-batch", "wait-batch"}
        and result.status not in {"running", "done"}
    ):
        parser.exit(1)


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    if args.command == "eval":
        from .evaluation.cli import execute as execute_eval

        try:
            status = execute_eval(args)
        except KeyboardInterrupt:
            parser.exit(130, "评估已取消，当前运行已收束。\n")
        except (OSError, ValueError):
            parser.exit(2, "评估数据或配置无效；请检查本地文件与参数。\n")
        parser.exit(status)
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
        try:
            app = create_app(
                settings,
                workspace_root=args.workspace,
                workspace_version=args.workspace_version,
                workspace_writable=args.workspace_write,
            )
        except (ValueError, OSError, ServiceError) as error:
            parser.error(f"工作区配置无效：{error}")
        uvicorn.run(app, host=args.host, port=args.port, workers=1)
        return
    try:
        with KrimClient(args.url, os.getenv("KRIM_AGENT_API_KEY", "")) as client:
            if args.command == "workspace":
                _workspace_command(client, args, parser)
                return
            if args.command == "tasks":
                if args.action == "get":
                    print(client.agent_tasks(args.agent_id, args.kind).model_dump_json(indent=2))
                elif args.action == "history":
                    print(
                        json.dumps(
                            [
                                item.model_dump(mode="json")
                                for item in client.agent_tasks_history(args.agent_id, args.kind)
                            ],
                            ensure_ascii=False,
                            indent=2,
                        )
                    )
                else:
                    if not args.request:
                        parser.error("tasks update需要--request本地JSON文件")
                    raw = Path(args.request).read_bytes()
                    if len(raw) > 65536:
                        parser.error("任务请求不能超过64KiB")
                    task_request = TaskWriteRequest.model_validate_json(raw)
                    print(
                        client.agent_tasks_write(
                            args.agent_id, args.kind, task_request
                        ).model_dump_json(indent=2)
                    )
                return
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
            if args.command == "multiplex":
                multiplex_request = MultiplexRequest.model_validate(
                    {"sources": json.loads(args.sources)}
                )
                for item in client.multiplex_events(multiplex_request):
                    print(
                        "null" if item is None else item.model_dump_json(exclude_none=True),
                        flush=True,
                    )
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
                if args.action == "profiles":
                    print(
                        json.dumps(
                            [item.model_dump(mode="json") for item in client.agent_profiles()],
                            ensure_ascii=False,
                            indent=2,
                        )
                    )
                elif args.action == "profile":
                    if not args.value:
                        parser.error("profile需要Agent配置名")
                    print(client.agent_profile(args.value, args.revision).model_dump_json(indent=2))
                elif args.action == "configure":
                    from krim_agent.agents import AgentProfile, AgentProfileUpdate

                    if not args.value:
                        parser.error("configure需要本地JSON配置路径")
                    raw = Path(args.value).read_bytes()
                    if len(raw) > 65536:
                        parser.error("Agent配置不能超过64KiB")
                    agent_config = AgentProfile.model_validate_json(raw)
                    print(
                        client.agent_profile_put(
                            AgentProfileUpdate(
                                expected_revision=args.revision or 0, config=agent_config
                            )
                        ).model_dump_json(indent=2)
                    )
                elif args.action in {"enable", "disable"}:
                    from krim_agent.agents import AgentProfileUpdate

                    if not args.value or args.revision is None or args.revision < 1:
                        parser.error("enable/disable需要配置名和--revision预期版本")
                    saved = client.agent_profile(args.value, args.revision)
                    print(
                        client.agent_profile_put(
                            AgentProfileUpdate(
                                expected_revision=args.revision,
                                config=saved.config,
                                enabled=args.action == "enable",
                            )
                        ).model_dump_json(indent=2)
                    )
                elif args.action == "inventory":
                    print(
                        json.dumps(
                            [item.model_dump(mode="json") for item in client.agent_inventory()],
                            ensure_ascii=False,
                            indent=2,
                        )
                    )
                elif args.action == "resources":
                    print(
                        json.dumps(
                            [item.model_dump(mode="json") for item in client.agent_resources()],
                            ensure_ascii=False,
                            indent=2,
                        )
                    )
                elif args.action == "catalog":
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
                                args.value,
                                persistent=args.persistent,
                                profile_revision=args.revision,
                            ).model_dump_json(indent=2)
                        )
                    elif args.action == "get":
                        print(client.agent(args.value).model_dump_json(indent=2))
                    elif args.action == "restore":
                        print(client.agent_restore(args.value).model_dump_json(indent=2))
                    elif args.action == "session":
                        print(client.agent_session(args.value).model_dump_json(indent=2))
                    elif args.action == "turns":
                        print(
                            json.dumps(
                                [
                                    item.model_dump(mode="json")
                                    for item in client.agent_turns(args.value)
                                ],
                                ensure_ascii=False,
                                indent=2,
                            )
                        )
                    elif args.action in {"close", "reopen"}:
                        if args.revision is None:
                            parser.error("close/reopen需要--revision会话预期版本")
                        controlled_session = (
                            client.agent_session_close(args.value, args.revision)
                            if args.action == "close"
                            else client.agent_session_reopen(args.value, args.revision)
                        )
                        print(controlled_session.model_dump_json(indent=2))
                    elif args.action in {"execute", "resume", "start"}:
                        if not args.prompt:
                            parser.error("agent execute/resume 需要 prompt")
                        view = (
                            client.agent_execute(args.value, args.prompt, args.idempotency_key)
                            if args.action == "execute"
                            else client.agent_start(args.value, args.prompt, args.idempotency_key)
                            if args.action == "start"
                            else client.agent_resume(args.value, args.prompt, args.idempotency_key)
                        )
                        print(view.model_dump_json(indent=2))
                    elif args.action == "events":
                        for frame in client.agent_events(
                            args.value, args.from_seq, run_id=args.run_id
                        ):
                            print(frame.model_dump_json(exclude_none=True))
                    elif args.action == "stop":
                        print(client.agent_stop(args.value).model_dump_json(indent=2))
                    elif args.action == "wait":
                        print(client.agent_wait(args.value).model_dump_json(indent=2))
                    elif args.action == "destroy":
                        print(client.agent_destroy(args.value).model_dump_json(indent=2))
                return
            if args.command == "trace":
                if args.action == "status":
                    print(client.observability_status().model_dump_json(indent=2))
                elif not args.value:
                    parser.error("trace run/workflow/get 需要标识")
                elif args.action == "get":
                    print(client.trace(args.value).model_dump_json(indent=2))
                else:
                    records = (
                        client.run_trace(args.value)
                        if args.action == "run"
                        else client.workflow_trace(args.value)
                    )
                    print(
                        json.dumps(
                            [item.model_dump(mode="json") for item in records],
                            ensure_ascii=False,
                            indent=2,
                        )
                    )
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
                        proposal_id=args.proposal_id,
                        proposal_revision=args.proposal_revision,
                    ).model_dump_json(indent=2)
                )
                return
            if args.command == "proposal":
                if args.action == "list":
                    print(
                        json.dumps(
                            [
                                item.model_dump(mode="json")
                                for item in client.context_proposals(
                                    args.topic_id, pending_only=args.pending
                                )
                            ],
                            ensure_ascii=False,
                            indent=2,
                        )
                    )
                elif not args.proposal_id:
                    parser.error("proposal get/confirm/reject 需要 proposal_id")
                elif args.action == "get":
                    print(
                        client.context_proposal(args.topic_id, args.proposal_id).model_dump_json(
                            indent=2
                        )
                    )
                else:
                    if args.revision is None:
                        parser.error("proposal confirm/reject 需要 --revision")
                    print(
                        client.decide_context_proposal(
                            args.topic_id,
                            args.proposal_id,
                            ContextProposalDecision(action=args.action, revision=args.revision),
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
