"""普通被测fixture进程，实际改文件/运行unittest；绝不是Codex或Claude CLI。"""

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

SOLUTION = (
    "from decimal import Decimal\n"
    "\n"
    "def reconcile(rows):\n"
    "    seen, totals = {}, {}\n"
    "    for row in rows:\n"
    '        ident, account = row["id"], row["account"]\n'
    '        value = Decimal(row["amount"])\n'
    "        if not value.is_finite() or value * 100 != (value * 100).to_inte"
    "gral_value():\n"
    '            raise ValueError("amount")\n'
    "        current = (account, int(value * 100))\n"
    "        if ident in seen:\n"
    "            if seen[ident] != current:\n"
    '                raise ValueError("duplicate conflict")\n'
    "            continue\n"
    "        seen[ident] = current\n"
    "        totals[account] = totals.get(account, 0) + current[1]\n"
    "    return totals\n"
)
TESTS = (
    "import unittest\n"
    "from ledger.reconcile import reconcile\n"
    "\n"
    "class TestLedger(unittest.TestCase):\n"
    "    def test_sum(self):\n"
    '        self.assertEqual(reconcile([{"id":"1","account":"a","amount":"0'
    '.10"},\n'
    '                                    {"id":"2","account":"a","amount":"0'
    '.20"}]), {"a":30})\n'
    "    def test_refund(self):\n"
    '        self.assertEqual(reconcile([{"id":"1","account":"a","amount":"-'
    '1.00"}]), {"a":-100})\n'
)


def emit(value):
    print(json.dumps(value), flush=True)


async def owner_intent(config):
    # 只测持久准入/intent的独立owner，不重复应用装配和readonly预检。
    values = json.loads(config.read_text())

    def progress(phase):
        with config.with_suffix(".progress.jsonl").open("a") as stream:
            stream.write(json.dumps({"phase": phase, "at": time.monotonic()}) + "\n")

    progress("stdlib_ready")
    sys.path.insert(0, str(Path(values["source_root"]) / "src"))
    from krim_agent.contracts import fingerprint
    from krim_agent.evaluation.adapters.profiles import CampaignStoreBinding, file_digest
    from krim_agent.evaluation.repository import Repository

    progress("product_imported")
    emit(
        {
            "owner_import_ready": True,
            "consumer": sys.modules["krim_agent.evaluation.repository"].__file__,
        },
    )
    if sys.stdin.readline().strip() != "continue":
        raise AssertionError("owner setup not acknowledged")
    binding = CampaignStoreBinding(values["admission"])
    descriptor = values["descriptor"]

    class AdmissionPort:
        # 窄fixture端口只给Store真实冻结数据；不复制Store或原生执行算法。
        execute_calls = 0

        def descriptor(self):
            return descriptor

        def assert_current(self, frozen):
            binding.assert_store(Path(values["database"]))
            if (
                fingerprint(
                    frozen,
                )
                != fingerprint(descriptor)
                or file_digest(Path(__file__).resolve()) != descriptor["implementation_digest"]
            ):
                raise AssertionError("fixture binding changed")

    repo = await Repository.open(Path(values["database"]))
    progress("store_opened")
    adapter = AdmissionPort()
    repo.register(values["key"], adapter)
    snapshot, created = await repo.begin(values["request"])
    assert created
    progress("reserved")
    await repo.intent(repo.plan(snapshot).trials[0])
    progress("intent_committed")
    emit(
        {
            "id": snapshot["id"],
            "resources": repo.resources,
            "consumer": sys.modules["krim_agent.evaluation.repository"].__file__,
            "execute_calls": adapter.execute_calls,
        }
    )
    import asyncio

    await asyncio.Event().wait()


def main():
    if len(sys.argv) == 3 and sys.argv[1] == "--owner-intent":
        import asyncio

        asyncio.run(owner_intent(Path(sys.argv[2])))
        return
    parser = argparse.ArgumentParser()
    parser.add_argument("--family", choices=["codex", "claude"], required=True)
    parser.add_argument("--mode", default="normal")
    args = parser.parse_args()
    codex = args.family == "codex"
    if codex:
        emit({"type": "thread.started", "thread_id": "fixture-only"})
        emit({"type": "turn.started"})
        emit(
            {
                "type": "item.started",
                "item": {"id": "edit-1", "type": "file_change", "status": "in_progress"},
            }
        )
    else:
        emit({"type": "system", "subtype": "init", "session_id": "fixture-only"})
        emit(
            {
                "type": "assistant",
                "message": {
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "edit-1",
                            "name": "Write",
                            "input": {"file_path": "ledger/reconcile.py"},
                        }
                    ]
                },
            }
        )
    Path("ledger/reconcile.py").write_text(SOLUTION)
    Path("tests/test_reconcile.py").write_text(TESTS)
    if codex:
        emit(
            {
                "type": "item.completed",
                "item": {"id": "edit-1", "type": "file_change", "status": "completed"},
            }
        )
        emit(
            {
                "type": "item.started",
                "item": {
                    "id": "cmd-1",
                    "type": "command_execution",
                    "status": "in_progress",
                    "command": "python -B -m unittest",
                },
            },
        )
    else:
        emit(
            {
                "type": "user",
                "message": {
                    "content": [
                        {
                            "type": "tool_result",
                            "tool_use_id": "edit-1",
                            "content": "written",
                            "is_error": False,
                        }
                    ]
                },
            }
        )
        emit(
            {
                "type": "assistant",
                "message": {
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "cmd-1",
                            "name": "Bash",
                            "input": {"command": "python -B -m unittest"},
                        }
                    ]
                },
            }
        )
    process = subprocess.run(
        [sys.executable, "-B", "-m", "unittest", "discover", "-s", "tests"],
        capture_output=True,
        text=True,
        timeout=5,
    )
    if process.returncode:
        sys.stderr.write(process.stderr)
        raise SystemExit(process.returncode)
    import csv
    import runpy

    reconcile = runpy.run_path("ledger/reconcile.py")["reconcile"]
    with Path("transactions.csv").open() as source:
        result = reconcile(list(csv.DictReader(source)))
    Path("outputs/ledger.json").write_text(json.dumps(result))
    Path("outputs/summary.md").write_text(
        f"accounts={len(result)}; total_cents={sum(result.values())}\n"
    )
    if codex:
        emit(
            {
                "type": "item.completed",
                "item": {
                    "id": "cmd-1",
                    "type": "command_execution",
                    "status": "completed",
                    "exit_code": process.returncode,
                    "aggregated_output": process.stderr,
                },
            }
        )
    else:
        emit(
            {
                "type": "user",
                "message": {
                    "content": [
                        {
                            "type": "tool_result",
                            "tool_use_id": "cmd-1",
                            "content": process.stderr,
                            "is_error": False,
                        }
                    ]
                },
            }
        )
    Path(".fixture-ready").write_text("completed edit and unittest")
    if args.mode == "live":
        while not Path(".fixture-release").exists():
            time.sleep(0.005)
    if args.mode == "bad":
        print("{bad", flush=True)
        return
    if codex:
        emit(
            {
                "type": "turn.completed",
                "usage": {"input_tokens": 10, "cached_input_tokens": 3, "output_tokens": 2},
            }
        )
    else:
        emit(
            {
                "type": "result",
                "subtype": "success",
                "is_error": False,
                "usage": {
                    "input_tokens": 7,
                    "cache_read_input_tokens": 3,
                    "cache_creation_input_tokens": 0,
                    "output_tokens": 2,
                },
            }
        )


if __name__ == "__main__":
    main()
