"""真实强退窗口：外部计数只作证据，不进入恢复评分。"""

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path


def prepare(args):
    if args.source_test:
        sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "src"))
    Path(args.evidence).mkdir(parents=True, exist_ok=True)


async def main(args):
    from dataclasses import replace

    from krim_agent.composition import open_application
    from krim_agent.evaluation import scorers, targets
    from krim_agent.evaluation.types import EvalRequest, FreezeRequest, rule
    from krim_agent.providers.eval_script import EvalScriptProvider
    from krim_agent.tools.registry import ToolRegistry

    root = Path(args.evidence)

    def record(kind):
        with (root / "counts.jsonl").open("a") as stream:
            stream.write(json.dumps({"kind": kind}) + "\n")
            stream.flush()
            os.fsync(stream.fileno())

    async def stop_here():
        (root / "ready.json").write_text(json.dumps({"mode": args.mode, "pid": os.getpid()}))
        await asyncio.Event().wait()

    stream_original = EvalScriptProvider.stream

    async def stream(self, messages, tools):
        record("provider_entry")
        source = stream_original(self, messages, tools)
        try:
            async for event in source:
                yield event
        finally:
            await source.aclose()
            record("provider_exit")

    EvalScriptProvider.stream = stream
    score_original = scorers.score_rule

    async def score(*values):
        record("scorer_entry")
        if args.mode == "scorer":
            await stop_here()
        result = await score_original(*values)
        record("scorer_exit")
        return result

    scorers.score_rule = score
    registry_original = targets.builtin_registry

    def tracked(spec):
        async def execute(arguments):
            record("tool_entry")
            try:
                return await spec.executor(arguments)
            finally:
                record("tool_exit")

        return replace(spec, executor=execute)

    targets.builtin_registry = lambda: ToolRegistry(
        tuple(tracked(spec) for spec, _ in registry_original()._frozen)
    )
    async with await open_application(Path(args.database)) as app:
        service = app.evaluations
        assert service is not None
        source = "/slow" if args.mode.startswith("association") else "/add 2 3"
        body = {
            "name": "crash",
            "version": "1",
            "cases": [
                {
                    "case_id": "one",
                    "source_kind": "manual",
                    "input": source,
                    "reason": "强退合成参考",
                    "reference": {
                        "expected": "5",
                        "expected_status": "completed",
                        "expected_code": None,
                        "expected_tools": None,
                        "trajectory": None,
                        "expected_schema": None,
                    },
                }
            ],
        }
        revision = await service.freeze(FreezeRequest.model_validate(body))
        original_attach = app.store.attach_child

        async def attach(*values):
            if args.mode == "association_before":
                await app.service.bindings["target-v2"].loop.provider.entered.wait()
                await stop_here()
            await original_attach(*values)
            if args.mode == "association_after":
                await app.service.bindings["target-v2"].loop.provider.entered.wait()
                await stop_here()

        app.store.attach_child = attach
        original_final = app.store._finalize_eval

        async def final(*values):
            if args.mode == "final":
                await stop_here()
            return await original_final(*values)

        app.store._finalize_eval = final
        request = EvalRequest.model_validate(
            {
                "request_key": "crash-eval",
                "dataset": {k: getattr(revision, k) for k in ["name", "version", "fingerprint"]},
                "target_key": "target-v2",
                "rules": [rule("status_code").model_dump(mode="json")],
                "execution": {
                    "repetitions": 1,
                    "concurrency": 1,
                    "item_timeout_seconds": 12,
                    "scorer_timeout_seconds": 1,
                    "batch_timeout_seconds": 600,
                },
            }
        )
        view = await service.start(request)
        await service.wait(view.id)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", required=True)
    parser.add_argument("--evidence", required=True)
    parser.add_argument(
        "--mode",
        required=True,
        choices=["association_before", "association_after", "scorer", "final"],
    )
    parser.add_argument("--source-test", action="store_true")
    args = parser.parse_args()
    prepare(args)
    asyncio.run(main(args))
