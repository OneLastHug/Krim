"""真正的 Python 子进程 CLI、评估 SQLite 与报告重开。"""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from krim_agent.evaluation import load_report

ROOT = Path(__file__).resolve().parents[1]


def cli(tmp_path, *arguments):
    env = {key: value for key, value in os.environ.items() if not key.startswith("KRIM_AGENT_")}
    # 默认离线模式必须忽略环境里的外部模型配置。
    env.update(
        KRIM_AGENT_PROVIDER="openai-compatible",
        KRIM_AGENT_PROVIDER_MODEL="test",
        KRIM_AGENT_PROVIDER_BASE_URL="http://127.0.0.1:1/v1",
        KRIM_AGENT_PROVIDER_API_KEY="PRIVATE-CLI-CANARY",
    )
    return subprocess.run(
        [sys.executable, "-m", "krim_agent", "eval", *map(str, arguments)],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=45,
    )


@pytest.mark.parametrize("target", ["text", "agent"])
def test_cli_executes_offline_targets_and_reopens_saved_report(tmp_path, target):
    path = tmp_path / "report.json"
    result = cli(
        tmp_path,
        "run",
        ROOT / "examples/eval-basic.json",
        "--target",
        target,
        "--scorer",
        "exact-match",
        "--scorer",
        "tool-calls",
        "--concurrency",
        "2",
        "--output",
        path,
        "--database",
        tmp_path / "runs.sqlite",
    )
    assert result.returncode == 0, result.stderr
    report = load_report(path)
    assert report.summary.passed == report.summary.total == 3
    assert report.target.kind == ("pythonai" if target == "text" else "agent")
    assert report.summary.total_tokens is None
    assert "PRIVATE-CLI-CANARY" not in result.stdout + path.read_text("utf-8")
    assert (tmp_path / "runs.sqlite").exists()
    rescored = tmp_path / "rescored.json"
    score_result = cli(
        tmp_path,
        "rescore",
        path,
        "--scorer",
        "exact-match",
        "--scorer",
        "tool-calls",
        "--output",
        rescored,
    )
    assert score_result.returncode == 0, score_result.stderr
    assert load_report(rescored).parent_report_id == report.id
    comparison = cli(tmp_path, "compare", path, rescored)
    assert comparison.returncode == 0, comparison.stderr
    assert json.loads(comparison.stdout)["regressions"] == []


def test_cli_validation_failure_report_and_incompatible_comparison_have_exit_codes(tmp_path):
    source = tmp_path / "data.json"
    source.write_text(
        json.dumps(
            {"name": "failure", "cases": [{"id": "one", "input": "hello", "expected": "wrong"}]}
        ),
        "utf-8",
    )
    validate = cli(tmp_path, "validate", source)
    assert validate.returncode == 0 and json.loads(validate.stdout)["cases"] == 1
    path = tmp_path / "failed.json"
    result = cli(tmp_path, "run", source, "--output", path)
    assert result.returncode == 1 and load_report(path).summary.failed == 1
    changed = tmp_path / "changed.json"
    score = cli(tmp_path, "rescore", path, "--scorer", "success", "--output", changed)
    assert score.returncode == 0
    assert cli(tmp_path, "compare", path, changed).returncode == 1
    source.write_text('{"name":"invalid","cases":[]}', "utf-8")
    assert cli(tmp_path, "validate", source).returncode == 2
    assert (
        cli(
            tmp_path,
            "run",
            ROOT / "examples/eval-basic.json",
            "--output",
            tmp_path / "invalid.json",
            "--concurrency",
            "0",
        ).returncode
        == 2
    )
    assert not (tmp_path / "invalid.json").exists()
