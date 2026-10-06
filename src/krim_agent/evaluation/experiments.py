"""确定性数据选择与可信目标矩阵；复用同一评估/任务服务。"""

from __future__ import annotations

import asyncio
import random
from collections.abc import Mapping, Sequence

from pydantic import JsonValue

from .runner import EvalTarget, evaluate
from .scorers import EvalScorer
from .service import EvalRunHandle, EvaluationService
from .types import EvalConfig, EvalDataset, EvalReport, fingerprint


def select_dataset(
    dataset: EvalDataset,
    *,
    tags: Sequence[str] | None = None,
    case_ids: Sequence[str] | None = None,
    limit: int | None = None,
    seed: int = 0,
) -> EvalDataset:
    snapshot = EvalDataset.model_validate(dataset.model_dump())
    if (
        type(seed) is not int
        or not 0 <= seed <= 9007199254740991
        or (limit is not None and (type(limit) is not int or not 1 <= limit <= 1000))
    ):
        raise ValueError("选择种子须为安全非负整数，limit 为1–1000")
    selected_tags, selected_ids = set(tags or []), set(case_ids or [])
    if selected_ids - {case.id for case in snapshot.cases}:
        raise ValueError("选择包含不存在的样本 ID")
    candidates = [
        case
        for case in snapshot.cases
        if (not selected_tags or selected_tags.intersection(case.tags))
        and (not selected_ids or case.id in selected_ids)
    ]
    if not candidates:
        raise ValueError("筛选没有样本")
    if limit is not None and limit < len(candidates):
        chosen = {case.id for case in random.Random(seed).sample(candidates, limit)}
        candidates = [case for case in candidates if case.id in chosen]
    tags_json: list[JsonValue] = [tag for tag in sorted(selected_tags)]
    ids_json: list[JsonValue] = [identifier for identifier in sorted(selected_ids)]
    return EvalDataset(
        name=snapshot.name,
        version=snapshot.version,
        cases=candidates,
        selection={
            "source_fingerprint": snapshot.fingerprint,
            "tags": tags_json,
            "case_ids": ids_json,
            "limit": limit,
            "seed": seed,
        },
    )


def _targets(targets: Mapping[str, EvalTarget]) -> dict[str, EvalTarget]:
    snapshot = dict(targets)
    if not 1 <= len(snapshot) <= 16 or any(not name or len(name) > 128 for name in snapshot):
        raise ValueError("矩阵需要1–16个有名称的可信目标")
    return snapshot


async def evaluate_matrix(
    dataset: EvalDataset,
    targets: Mapping[str, EvalTarget],
    *,
    scorers: Sequence[EvalScorer] | None = None,
    config: EvalConfig | None = None,
) -> dict[str, EvalReport]:
    snapshot = EvalDataset.model_validate(dataset.model_dump())
    results = {}
    # 顺序执行目标，内部仍按 concurrency；避免候选数乘并发造成无界生产。
    for name, target in _targets(targets).items():
        results[name] = await evaluate(snapshot, target, scorers=scorers, config=config)
    return results


async def start_matrix(
    service: EvaluationService,
    dataset: EvalDataset,
    targets: Mapping[str, EvalTarget],
    *,
    scorers: Sequence[EvalScorer] | None = None,
    config: EvalConfig | None = None,
    request_key: str | None = None,
) -> dict[str, EvalRunHandle]:
    if request_key is not None and (not request_key or len(request_key) > 96):
        raise ValueError("矩阵请求键须为1–96字符")
    snapshot = EvalDataset.model_validate(dataset.model_dump())
    handles: dict[str, EvalRunHandle] = {}
    known_tasks = set(service.tasks)
    try:
        for name, target in _targets(targets).items():
            key = f"{request_key}:matrix:{fingerprint(name)[:16]}" if request_key else None
            handles[name] = await service.start(
                snapshot, target, scorers=scorers, config=config, request_key=key
            )
        return handles
    except BaseException:
        await asyncio.gather(
            *(
                h.pause()
                for h in handles.values()
                if h.id in service.tasks and h.id not in known_tasks
            ),
            return_exceptions=True,
        )
        raise
