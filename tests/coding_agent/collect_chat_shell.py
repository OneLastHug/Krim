"""Read an existing production chat SSE and save its real final facts, no submission."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import httpx

from tests.coding_agent.serve_chat_shell import TEST_WORKSPACE, test_access_token


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--session", required=True)
    parser.add_argument("--turn", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    base = f"/v2/workspaces/{TEST_WORKSPACE}/sessions/{args.session}"
    path = base + "/turns/" + args.turn
    with httpx.Client(
        base_url="http://127.0.0.1:5204",
        headers={
            "Authorization": "Bearer " + test_access_token(),
        },
        timeout=340,
    ) as client:
        with client.stream("GET", path + "/stream") as response:
            response.raise_for_status()
            for line in response.iter_lines():
                if line.startswith("event: terminal"):
                    break
        facts = client.get(path + "/result")
        facts.raise_for_status()
        result = facts.json()
        turn = client.get(path).json()
        session = client.get(base).json()
        binding = client.get(f"/v2/me/sandbox/sessions/{args.session}/project").json()
        artifact = {"session": session, "turn": turn, "binding": binding, "result": result}
        (args.output / "chat.json").write_text(json.dumps(artifact, ensure_ascii=False, indent=2))
        summary = {
            "run_id": result["run"]["id"],
            "status": result["run"]["status"],
            "code": result["run"]["code"],
            "model_steps": result["model_steps"],
            "tool_calls": [
                {
                    k: tool.get(k)
                    for k in (
                        "name",
                        "status",
                        "code",
                        "executor_started",
                        "result_available",
                        "value",
                    )
                }
                for tool in result["tools"]
            ],
            "text": result["text"],
            "prompt_sha256": hashlib.sha256(turn["text"].encode()).hexdigest(),
        }
        (args.output / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2))
        print(json.dumps(summary, ensure_ascii=False))


if __name__ == "__main__":
    main()
