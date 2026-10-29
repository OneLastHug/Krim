"""只求值夹具实际配置表达式，不启动服务或连接任何数据库。"""

import ast
from pathlib import Path
from types import SimpleNamespace

from krim_agent.config import Settings


def test_ui_fixture_ignores_unrelated_evaluation_database(monkeypatch, tmp_path):
    source = Path(__file__).parents[1] / "core/fixtures/ui_server.py"
    tree = ast.parse(source.read_text(encoding="utf-8"))
    expression = next(
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.keyword) and node.arg == "settings"
    )
    unrelated = tmp_path / "unrelated.sqlite"
    unrelated.write_bytes(b"unrelated data")
    database = tmp_path / "owned.sqlite"
    monkeypatch.setenv("KRIM_AGENT_PRODUCT_EVAL_DATABASE", str(unrelated))
    settings = eval(
        compile(ast.Expression(expression), str(source), "eval"),
        {"Settings": Settings, "Path": Path, "args": SimpleNamespace(database=str(database))},
    )
    assert settings.database == database
    assert settings.product_eval_database == database.with_suffix(".eval.sqlite")
    assert unrelated.read_bytes() == b"unrelated data"
