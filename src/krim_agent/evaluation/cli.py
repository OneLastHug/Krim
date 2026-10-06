"""本地 eval CLI；默认离线模型，数据文件不能指定 Python 插件。"""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

from krim_agent.agents import AgentDefinition, AgentManager
from krim_agent.config import Settings
from krim_agent.context_engine import ContextRunConfig
from krim_agent.sdk import PythonAI
from krim_agent.tools.output import OutputSchema

from .files import load_dataset, load_report, parse_json, save_report
from .reports import compare_reports
from .runner import evaluate, rescore
from .scorers import (
    ContainsScorer,
    EvalScorer,
    ExactMatchScorer,
    JsonMatchScorer,
    JsonSchemaScorer,
    SuccessScorer,
    ToolCallsScorer,
)
from .targets import AgentTarget
from .types import EvalConfig, EvalReport

SCORERS = ("exact-match", "contains", "json-match", "json-schema", "success", "tool-calls")


def add_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    root = subparsers.add_parser(
        "eval", help="eval（evaluation，评估）：本地数据集、评分与回归报告"
    )
    actions = root.add_subparsers(dest="eval_action", required=True)
    validate = actions.add_parser("validate", help="validate（校验）：检查数据集")
    validate.add_argument("dataset", help="JSON 或 JSONL 数据集路径")
    run = actions.add_parser("run", help="run（运行）：批量评估生成或 Agent")
    run.add_argument("dataset", help="JSON 或 JSONL 数据集路径")
    run.add_argument("--output", required=True, help="output（输出）：本地报告路径")
    run.add_argument("--database", default=".data/evals/runs.sqlite", help="独立评估 SQLite 路径")
    run.add_argument(
        "--target", choices=("text", "agent"), default="text", help="target（目标）：生成或 Agent"
    )
    run.add_argument("--version", default="1", help="version（版本）：目标代码/Prompt 版本")
    run.add_argument("--prompt-prefix", default="", help="Prompt 前缀")
    run.add_argument("--concurrency", type=int, default=1, help="concurrency（并发）：最多 16")
    run.add_argument("--repetitions", type=int, default=1, help="repetitions（重复）：最多 20")
    run.add_argument("--timeout", type=float, default=60, help="timeout（超时）：单次运行秒数")
    run.add_argument("--scorer-timeout", type=float, default=10, help="单次评分超时秒数")
    run.add_argument(
        "--threshold",
        type=float,
        default=1,
        help="threshold（阈值）：0–1，通过需要全部评分达到阈值",
    )
    run.add_argument(
        "--configured-provider",
        action="store_true",
        help="显式读取现有 KRIM_AGENT_ 模型配置；默认使用离线 fake",
    )
    _score_arguments(run)
    score = actions.add_parser("rescore", help="rescore（重评分）：复用报告，不再次执行模型/工具")
    score.add_argument("report")
    score.add_argument("--output", required=True)
    score.add_argument("--threshold", type=float)
    _score_arguments(score)
    compare = actions.add_parser("compare", help="compare（比较）：同数据集与配置的回归检查")
    compare.add_argument("baseline", help="baseline（基线）：之前的报告")
    compare.add_argument("candidate", help="candidate（候选）：新版本报告")
    compare.add_argument(
        "--tolerance", type=float, default=0, help="tolerance（容差）：评分降低容许值"
    )


def _score_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--scorer",
        action="append",
        choices=SCORERS,
        help="scorer（评分器）：可重复指定，默认精确匹配",
    )
    parser.add_argument("--schema", help="schema（模式）：json-schema 评分所需的本地 JSON Schema")


def _scorers(args: argparse.Namespace) -> list[EvalScorer]:
    selected: list[EvalScorer] = []
    for name in args.scorer or ["exact-match"]:
        if name == "exact-match":
            selected.append(ExactMatchScorer())
        elif name == "contains":
            selected.append(ContainsScorer())
        elif name == "json-match":
            selected.append(JsonMatchScorer())
        elif name == "json-schema":
            if not args.schema:
                raise ValueError("JSON Schema 评分需要 schema 文件")
            raw = parse_json(Path(args.schema).read_text("utf-8"))
            if not isinstance(raw, (dict, bool)):
                raise ValueError("Schema 必须为 JSON 对象或布尔值")
            schema: OutputSchema = raw
            selected.append(JsonSchemaScorer(schema))
        elif name == "success":
            selected.append(SuccessScorer())
        else:
            selected.append(ToolCallsScorer())
    return selected


async def _run(args: argparse.Namespace) -> EvalReport:
    dataset = load_dataset(args.dataset)
    scorers = _scorers(args)
    options = EvalConfig(
        concurrency=args.concurrency,
        repetitions=args.repetitions,
        timeout_seconds=args.timeout,
        scorer_timeout_seconds=args.scorer_timeout,
        threshold=args.threshold,
    )
    path = await asyncio.to_thread(Path(args.database).resolve)
    await asyncio.to_thread(path.parent.mkdir, parents=True, exist_ok=True)
    base = Settings.from_env() if args.configured_provider else Settings()
    settings = Settings.model_validate(
        {**base.model_dump(), "database_url": f"sqlite+aiosqlite:///{path.as_posix()}"}
    )
    async with await PythonAI.create(settings=settings) as ai:
        if args.target == "text":
            return await ai.evaluate(
                dataset,
                scorers=scorers,
                config=options,
                version=args.version,
                prompt_prefix=args.prompt_prefix,
            )
        manager = AgentManager(
            ai.runtime.repository, max_concurrent=options.concurrency, tracer=ai.runtime.tracer
        )
        definition = AgentDefinition(
            name="eval_agent",
            provider=ai.runtime.provider,
            tools=ai.runtime.tools,
            max_steps=settings.max_steps,
            timeout_seconds=settings.run_timeout_seconds,
            tool_timeout_seconds=settings.tool_timeout_seconds,
            policy=ai.runtime.tool_policy,
            context_config=ContextRunConfig.from_settings(settings),
            usage_budget=settings.agent_usage_budget,
        )
        try:
            return await evaluate(
                dataset,
                AgentTarget(
                    manager, definition, version=args.version, prompt_prefix=args.prompt_prefix
                ),
                scorers=scorers,
                config=options,
            )
        finally:
            await manager.close()


def execute(args: argparse.Namespace) -> int:
    if args.eval_action == "validate":
        data = load_dataset(args.dataset)
        print(
            json.dumps(
                {
                    "name": data.name,
                    "version": data.version,
                    "cases": len(data.cases),
                    "fingerprint": data.fingerprint,
                },
                ensure_ascii=False,
            )
        )
        return 0
    if args.eval_action == "compare":
        comparison = compare_reports(
            load_report(args.baseline), load_report(args.candidate), tolerance=args.tolerance
        )
        print(comparison.model_dump_json(indent=2))
        return 0 if comparison.compatible and not comparison.regressions else 1
    if args.eval_action == "rescore":
        report = asyncio.run(
            rescore(load_report(args.report), scorers=_scorers(args), threshold=args.threshold)
        )
    else:
        report = asyncio.run(_run(args))
    save_report(report, args.output)
    print(
        json.dumps(
            {
                "report_id": report.id,
                "report": str(Path(args.output)),
                "summary": report.summary.model_dump(mode="json"),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0 if report.summary.passed == report.summary.total else 1
