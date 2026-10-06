"""评估任务 CLI 管理；恢复使用冻结定义，不从文件导入 callable。"""

import argparse
import asyncio
import json
import os
from pathlib import Path

from pydantic import BaseModel

from krim_agent.config import Settings
from krim_agent.persistence import Database
from krim_agent.protocol.domain import ServiceError
from krim_agent.sdk import PythonAI

from .conversation import ConversationTarget
from .jobs import EvalResumeRequest
from .remote import SyncEvalClient
from .scorers import (
    ContainsScorer,
    ConversationScorer,
    EvalScorer,
    ExactMatchScorer,
    JsonMatchScorer,
    JsonSchemaScorer,
    SuccessScorer,
    ToolCallsScorer,
)
from .service import EvaluationService
from .targets import GenerationTarget
from .types import EvalScorerInfo


def restore_scorers(infos: list[EvalScorerInfo]) -> list[EvalScorer]:
    result: list[EvalScorer] = []
    for info in infos:
        config = info.config
        if info.name == "exact_match":
            result.append(
                ExactMatchScorer(normalize_whitespace=config.get("normalize_whitespace") is True)
            )
        elif info.name == "contains":
            result.append(ContainsScorer())
        elif info.name == "json_match":
            result.append(JsonMatchScorer())
        elif info.name == "json_schema":
            schema = config.get("schema")
            if not isinstance(schema, (dict, bool)):
                raise ValueError("输出模式无效")
            result.append(JsonSchemaScorer(schema))
        elif info.name == "success":
            result.append(
                SuccessScorer(require_tools_success=config.get("require_tools_success") is True)
            )
        elif info.name == "conversation_turns":
            result.append(ConversationScorer())
        elif info.name == "tool_calls":
            result.append(
                ToolCallsScorer(
                    ordered=config.get("ordered") is True,
                    require_success=config.get("require_success") is True,
                )
            )
        else:
            raise ValueError("自定义评分器需要由可信 Python SDK 恢复")
    return result


def _display(value: BaseModel | list[BaseModel]) -> None:
    if isinstance(value, list):
        print(
            json.dumps(
                [item.model_dump(mode="json") for item in value], ensure_ascii=False, indent=2
            )
        )
    else:
        print(value.model_dump_json(indent=2))


async def _local(args: argparse.Namespace) -> BaseModel | list[BaseModel]:
    path = await asyncio.to_thread(Path(args.database).resolve)
    database = Database(f"sqlite+aiosqlite:///{path.as_posix()}")
    await database.initialize()
    service = EvaluationService(database, scope=args.scope)
    try:
        if args.action == "list":
            return list(await service.store.list_jobs())
        if not args.job_id:
            raise ValueError("任务操作需要 job_id")
        handle = service.handle(args.job_id)
        if args.action == "get":
            return await handle.view()
        if args.action == "items":
            return await handle.items()
        if args.action == "report":
            return await handle.report()
        if args.action == "partial":
            return await handle.partial_report()
        if args.action == "events":
            return list(await service.store.events(args.job_id, args.from_seq, 1000))
        if args.action in {"pause", "cancel"}:
            return await service.stop(args.job_id, args.action)
        if not (await handle.view()).can_resume:
            raise ServiceError("eval_busy_or_terminal", "评估已被持有或不能恢复", 409)
        spec = await service.store.spec(args.job_id)
        if spec.target.kind not in {"pythonai", "conversation"}:
            raise ValueError("此目标须由原 SDK/服务端可信定义恢复")
        async with await PythonAI.create(
            settings=Settings(database_url=str(database.engine.url))
        ) as ai:
            handle = await service.resume(
                args.job_id,
                target=(
                    ConversationTarget if spec.target.kind == "conversation" else GenerationTarget
                )(ai, version=spec.target.version, prompt_prefix=args.prompt_prefix),
                scorers=restore_scorers(spec.scorers),
                request=EvalResumeRequest(
                    retry_uncertain=args.retry_uncertain, skip_uncertain=args.skip_uncertain
                ),
            )
            await handle.wait()
            return await handle.view()
    finally:
        await service.close()
        await database.close()


def execute_jobs(args: argparse.Namespace) -> int:
    if not args.url:
        _display(asyncio.run(_local(args)))
        return 0
    with SyncEvalClient(args.url, os.getenv("KRIM_AGENT_API_KEY", "")) as client:
        if args.action == "list":
            _display(list(client.list()))
            return 0
        if not args.job_id:
            raise ValueError("远端任务操作需要 job_id")
        if args.action == "get":
            _display(client.get(args.job_id))
        elif args.action == "items":
            _display(client.items(args.job_id))
        elif args.action == "report":
            _display(client.report(args.job_id))
        elif args.action == "partial":
            _display(client.partial_report(args.job_id))
        elif args.action == "pause":
            _display(client.pause(args.job_id))
        elif args.action == "cancel":
            _display(client.cancel(args.job_id))
        elif args.action == "resume":
            from .jobs import EvalRemoteResumeRequest

            _display(
                client.resume(
                    args.job_id,
                    EvalRemoteResumeRequest(
                        retry_uncertain=args.retry_uncertain,
                        skip_uncertain=args.skip_uncertain,
                        prompt_prefix=args.prompt_prefix,
                    ),
                )
            )
        elif args.action == "events":
            for event in client.events(args.job_id, args.from_seq):
                _display(event)
        else:
            raise ValueError("远端目录请调用 REST GET /v1/evals")
    return 0
