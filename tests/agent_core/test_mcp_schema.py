"""远端 MCP Schema 不得引入 Python re 的无界回溯。"""

import subprocess
import sys

import pytest

from krim_agent.tools.mcp import arguments_model
from krim_agent.tools.mcp_schema import MCPContract


def test_nested_patterns_use_linear_rust_engine_and_keep_original_schema():
    schema = {
        "type": "object",
        "properties": {"text": {"$ref": "#/$defs/text"}},
        "$defs": {
            "text": {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "string",
                "pattern": "(a+)+$",
            }
        },
        "required": ["text"],
    }
    model = arguments_model("Pattern", schema)
    assert model.model_json_schema() == schema
    assert model.model_validate({"text": "aaaa"}).model_dump() == {"text": "aaaa"}
    with pytest.raises(ValueError):
        model.model_validate({"text": "a" * 4096 + "!"})


def test_dialect_stripping_does_not_remove_user_property_named_schema():
    schema = {
        "type": "object",
        "required": ["$schema"],
        "properties": {"$schema": {"type": "string", "pattern": "^ok$"}},
    }
    model = arguments_model("UserProperty", schema)
    with pytest.raises(ValueError):
        model.model_validate({"$schema": "wrong"})
    assert model.model_validate({"$schema": "ok"}).model_dump() == {"$schema": "ok"}


@pytest.mark.parametrize(
    "schema",
    [
        {"type": "string", "pattern": "(?=unsafe)"},
        {"type": "object", "patternProperties": {"(a+)+$": {"type": "string"}}},
        {"type": "object", "$defs": {"x": {"$schema": "http://json-schema.org/draft-07/schema"}}},
    ],
)
def test_unsupported_regex_or_dialect_rejected_at_registration(schema):
    with pytest.raises(ValueError):
        MCPContract(schema)


def test_output_pattern_guard_in_separate_bounded_process():
    script = """
from krim_agent.tools.mcp_schema import MCPContract
schema = {'type':'object','properties':{'text':{'type':'string','pattern':'(a+)+$'}}}
try:
    MCPContract(schema).validate({'text':'a'*4096+'!'})
except ValueError:
    print('rejected')
else:
    raise AssertionError('pattern accepted incorrect value')
"""
    process = subprocess.run(
        [sys.executable, "-I", "-c", script], capture_output=True, text=True, timeout=10
    )
    assert process.returncode == 0 and process.stdout.strip() == "rejected", process.stderr
