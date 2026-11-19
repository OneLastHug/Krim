#!/usr/bin/env python3
"""Run bounded real-provider conversations through Krim's HTTP session API.

Credentials are read from stdin or KRIM_LIVE_API_KEY and never saved. Reports
contain the synthetic conversation, run facts and local exception locations;
upstream response bodies, headers and exception messages are not recorded.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
import time
import uuid
from pathlib import Path
from typing import Any

import httpx
from pydantic import SecretStr

from krim_agent.api.server import create_app
from krim_agent.composition import open_application
from krim_agent.config import Settings
from krim_agent.contracts import ServiceError
from krim_agent.providers.openai_compatible import (
    OpenAICompatibleConfig,
    OpenAICompatibleProvider,
)
from krim_agent.security import LOCAL

PROMPTS = (
    "你好，请用一句简短的中文回答。",
    "你可见的工具有哪些？请按实际收到的工具定义列出名称和用途，不要调用工具。",
    "请实际调用 add 工具计算 19 + 23，再依据工具返回值告诉我结果。不要心算代替工具。",
    "请实际调用 echo 工具，原样返回：协议验收🙂。然后简短确认收到的内容。",
    "刚才加法工具返回了多少？请根据这个会话的历史回答，不要再次调用工具。",
)


def redact(value: Any, secret: str) -> Any:
    if isinstance(value, str):
        return value.replace(secret, "[REDACTED]")
    if isinstance(value, list):
        return [redact(item, secret) for item in value]
    if isinstance(value, dict):
        return {
            redact(str(name), secret): redact(item, secret)
            for name, item in value.items()
        }
    return value


def exception_trace(records: list[dict[str, Any]]) -> Any:
    provider_suffix = "/krim_agent/providers/openai_compatible.py"

    def trace(frame: Any, event: str, argument: Any) -> Any:
        filename = frame.f_code.co_filename.replace("\\", "/")
        if event == "call":
            return trace if filename.endswith(provider_suffix) else None
        if event != "exception" or not filename.endswith(provider_suffix):
            return trace
        error = argument[1]
        if not isinstance(error, ServiceError):
            return trace
        item: dict[str, Any] = {
            "file": "src/krim_agent/providers/openai_compatible.py",
            "line": frame.f_lineno,
            "function": frame.f_code.co_name,
            "exception_type": type(error).__name__,
            "code": error.code,
        }
        delta = frame.f_locals.get("delta")
        if isinstance(delta, dict):
            fields = set(delta)
            allowed = frame.f_locals.get("allowed")
            item["unknown_field_count"] = (
                len(fields - allowed)
                + sum(not isinstance(name, str) for name in fields)
                if isinstance(allowed, set)
                else None
            )
            item["field_types"] = sorted(
                {
                    "role=" + type(delta["role"]).__name__
                    for _ in (0,)
                    if "role" in delta
                }
                | {
                    "reasoning_content=" + type(delta["reasoning_content"]).__name__
                    for _ in (0,)
                    if "reasoning_content" in delta
                }
                | {
                    "content=" + type(delta["content"]).__name__
                    for _ in (0,)
                    if "content" in delta
                }
                | {
                    "tool_calls=" + type(delta["tool_calls"]).__name__
                    for _ in (0,)
                    if "tool_calls" in delta
                }
            )
            item["reasoning_nonempty"] = bool(delta.get("reasoning_content"))
            item["assistant_role"] = delta.get("role", "assistant") == "assistant"
            item["refusal_present"] = bool(delta.get("refusal"))
        if item not in records and len(records) < 32:
            records.append(item)
        return trace

    return trace


async def exercise(args: argparse.Namespace, key: str) -> int:
    root = args.evidence.absolute()
    root.mkdir(parents=True, exist_ok=False)
    report: dict[str, Any] = {
        "model": args.model,
        "thinking": args.thinking,
        "provider": "moerouter.com",
        "identity_mode": "LOCAL loopback",
        "rounds": [],
        "passed": False,
    }
    database = root / "session.sqlite"
    endpoint = "https://moerouter.com/v1"
    settings = Settings(
        database=database,
        provider="http",
        http_mode="agent",
        max_active_runs=1,
        run_timeout=120,
        max_steps=6,
        max_tools_per_step=1,
        max_tool_calls=6,
        system_prompt=(
            "你是 Krim Agent。根据实际工具定义回答，用户要求调用工具时实际调用，"
            "依据工具结果简短回答。不要编造工具或执行结果。"
        ),
    )
    config = OpenAICompatibleConfig(
        base_url=endpoint,
        api_key=SecretStr(key),
        model=args.model,
        max_output_tokens=2048,
        max_tool_calls=1,
        read_timeout_seconds=60,
        thinking=args.thinking,
    )
    try:
        async with await open_application(database, settings=settings, http_config=config) as host:
            app = create_app(application=host)
            async with app.router.lifespan_context(app):
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app),
                    base_url="http://127.0.0.1",
                    trust_env=False,
                    timeout=130,
                ) as client:
                    response = await client.get("/v2/me")
                    response.raise_for_status()
                    workspace = response.json()["workspaces"][0]["workspace_id"]
                    base = f"/v2/workspaces/{workspace}/sessions"
                    tools_response = await client.get("/v2/targets/target-http/tools")
                    tools_response.raise_for_status()
                    catalog = tools_response.json()
                    report["target_tools"] = [item.get("name") for item in catalog]
                    binding = host.service.bindings["target-http"]
                    if not isinstance(binding.loop.provider, OpenAICompatibleProvider):
                        raise ServiceError("provider_binding_mismatch")
                    report["target_identity"] = redact(
                        binding.loop.provider.identity(), key
                    )
                    report["api_key_configured"] = bool(
                        config.api_key.get_secret_value()
                    )
                    required_tools = {"add", "echo", "lookup_note"}
                    if not required_tools.issubset(set(report["target_tools"])):
                        raise ServiceError("provider_tool_catalog_mismatch")
                    response = await client.post(
                        base,
                        json={
                            "request_key": "live." + uuid.uuid4().hex,
                            "title": "真实模型协议验收",
                            "target_key": "target-http",
                        },
                    )
                    response.raise_for_status()
                    session = response.json()
                    report["session_id"] = session["id"]
                    route = base + "/" + session["id"]
                    for index, prompt in enumerate(PROMPTS[: args.turns]):
                        session = (await client.get(route)).json()
                        exceptions: list[dict[str, Any]] = []
                        previous_trace = sys.gettrace()
                        started = time.monotonic()
                        sys.settrace(exception_trace(exceptions))
                        try:
                            response = await client.post(
                                route + "/turns",
                                json={
                                    "request_key": f"turn.{index}",
                                    "text": prompt,
                                    "expected_revision": session["revision"],
                                },
                            )
                            response.raise_for_status()
                            turn = response.json()
                            assert host.sessions is not None
                            async with asyncio.timeout(130):
                                await host.sessions.wait(
                                    LOCAL,
                                    workspace,
                                    session["id"],
                                    turn["id"],
                                )
                            response = await client.get(route + "/turns/" + turn["id"])
                            response.raise_for_status()
                            turn = response.json()
                            response = await client.get(
                                route + "/turns/" + turn["id"] + "/result"
                            )
                            response.raise_for_status()
                            facts = response.json()
                        finally:
                            sys.settrace(previous_trace)
                        row = {
                            "prompt": prompt,
                            "turn_id": turn["id"],
                            "run_id": turn["run_id"],
                            "status": turn["status"],
                            "code": turn["code"],
                            "text": redact(facts["text"], key),
                            "model_steps": facts["model_steps"],
                            "tools": redact(facts["tools"], key),
                            "event_kinds": [event["kind"] for event in facts["events"]],
                            "elapsed_seconds": round(time.monotonic() - started, 2),
                            "local_exceptions": exceptions,
                        }
                        report["rounds"].append(row)
                        print(json.dumps(row, ensure_ascii=False), flush=True)
                        (root / "report.json").write_text(
                            json.dumps(report, ensure_ascii=False, indent=2) + "\n"
                        )
                        if turn["status"] != "completed":
                            break
                    rounds = report["rounds"]
                    add_receipts = (
                        rounds[2]["tools"] if args.turns >= 3 and len(rounds) >= 3 else []
                    )
                    echo_receipts = (
                        rounds[3]["tools"] if args.turns >= 4 and len(rounds) >= 4 else []
                    )
                    add_ok = any(
                        tool.get("name") == "add"
                        and tool.get("status") == "ok"
                        and tool.get("arguments") == {"left": 19, "right": 23}
                        and tool.get("result_available") is True
                        and tool.get("value") == {"sum": 42.0}
                        for tool in add_receipts
                    )
                    echo_ok = any(
                        tool.get("name") == "echo"
                        and tool.get("status") == "ok"
                        and tool.get("arguments") == {"text": "协议验收🙂"}
                        and tool.get("result_available") is True
                        and tool.get("value") == "协议验收🙂"
                        for tool in echo_receipts
                    )
                    history_text = rounds[4]["text"] if len(rounds) >= 5 else ""
                    history_match = re.search(r"(?<!\d)42(?:\.0+)?(?!\d)", history_text)
                    history_ok = (
                        args.turns < 5
                        or history_match is not None
                        and not rounds[4]["tools"]
                    )
                    report["passed"] = (
                        len(rounds) == args.turns
                        and all(row["status"] == "completed" for row in rounds)
                        and (args.turns < 3 or add_ok)
                        and (args.turns < 4 or echo_ok)
                        and history_ok
                    )
    except Exception as error:
        report["error_type"] = type(error).__name__
        if isinstance(error, ServiceError):
            report["error_code"] = error.code
    (root / "report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n"
    )
    print(json.dumps({"passed": report["passed"], "rounds": len(report["rounds"])}))
    return 0 if report["passed"] else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", default="glm-5.3-flash")
    parser.add_argument("--evidence", required=True, type=Path)
    parser.add_argument("--thinking", choices=["disabled"], default=None)
    parser.add_argument("--turns", type=int, choices=range(1, 6), default=5)
    parser.add_argument("--key-stdin", action="store_true")
    args = parser.parse_args()
    key = sys.stdin.read().strip() if args.key_stdin else os.environ.get("KRIM_LIVE_API_KEY", "")
    if not key or len(key) > 2048 or any(not 33 <= ord(c) <= 126 for c in key):
        parser.error("missing or invalid credential")
    try:
        return asyncio.run(exercise(args, key))
    finally:
        key = ""


if __name__ == "__main__":
    raise SystemExit(main())
