"""有限本地 JSON/JSONL 文件与报告原子写入，不检索网络资源。"""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

from pydantic import JsonValue, TypeAdapter

from krim_agent.protocol.models import check_json

from .types import EvalCase, EvalDataset, EvalReport


def parse_json(raw: str) -> JsonValue:
    def unique(pairs: list[tuple[str, JsonValue]]) -> dict[str, JsonValue]:
        result: dict[str, JsonValue] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("JSON 对象不能包含重复键")
            result[key] = value
        return result

    try:
        value: JsonValue = TypeAdapter(JsonValue).validate_python(
            json.loads(raw, object_pairs_hook=unique), strict=True
        )
        check_json(value, max_bytes=32 * 1024 * 1024)
        return value
    except (RecursionError, OverflowError) as error:
        raise ValueError("JSON 嵌套或数值超过限制") from error


def _read(path: str | Path, limit: int) -> str:
    with Path(path).open("rb") as handle:
        raw = handle.read(limit + 1)
    if len(raw) > limit:
        raise ValueError("评估文件超过大小限制")
    return raw.decode("utf-8")


def load_dataset(path: str | Path, *, name: str | None = None, version: str = "1") -> EvalDataset:
    source = Path(path)
    raw = _read(source, 16 * 1024 * 1024)
    if source.suffix.lower() == ".jsonl":
        cases = []
        for index, line in enumerate(raw.splitlines(), 1):
            if not line.strip():
                continue
            try:
                cases.append(EvalCase.model_validate(parse_json(line)))
            except ValueError:
                raise ValueError(f"JSONL 第 {index} 行无效") from None
            if len(cases) > 1000:
                raise ValueError("评估样本超过数量限制")
        return EvalDataset(name=name or source.stem, version=version, cases=cases)
    return EvalDataset.model_validate(parse_json(raw))


def _atomic_json(path: str | Path, value: str) -> None:
    raw = value.encode("utf-8")
    if len(raw) > 32 * 1024 * 1024:
        raise ValueError("评估报告超过大小限制")
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{destination.name}.", dir=destination.parent)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, destination)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def save_dataset(dataset: EvalDataset, path: str | Path) -> None:
    snapshot = EvalDataset.model_validate(dataset.model_dump())
    _atomic_json(path, snapshot.model_dump_json(indent=2))


def save_report(report: EvalReport, path: str | Path) -> None:
    snapshot = EvalReport.model_validate(report.model_dump())
    _atomic_json(path, snapshot.model_dump_json(indent=2))


def load_report(path: str | Path) -> EvalReport:
    raw = _read(path, 32 * 1024 * 1024)
    value = parse_json(raw)
    return EvalReport.model_validate_json(json.dumps(value, ensure_ascii=False))
