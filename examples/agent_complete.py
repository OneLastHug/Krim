"""项目外-I运行：离线主链＋显式本机HTTP；人工参考不从观测输出自动生成。"""

import argparse
import asyncio
import json
import os
from pathlib import Path
from urllib.parse import urlsplit

from krim_agent.composition import open_application
from krim_agent.evaluation.types import EvalRequest, FreezeRequest, rule
from krim_agent.providers.openai_compatible import OpenAICompatibleConfig
from krim_agent.sdk import EvaluationSDK, RunSDK


def request(dataset, key, target):
    return EvalRequest.model_validate(
        {
            "request_key": key,
            "dataset": {k: getattr(dataset, k) for k in ["name", "version", "fingerprint"]},
            "target_key": target,
            "rules": [rule(n).model_dump(mode="json") for n in ["status_code", "exact_match"]],
            "execution": {
                "repetitions": 1,
                "concurrency": 1,
                "item_timeout_seconds": 12,
                "scorer_timeout_seconds": 1,
                "batch_timeout_seconds": 600,
            },
        }
    )


async def main(args):
    config = None
    if args.http_base_url:
        address = urlsplit(args.http_base_url)
        if (
            address.scheme != "http"
            or address.hostname not in {"127.0.0.1", "::1"}
            or not args.http_model
        ):
            raise ValueError("此演示只接受显式本机HTTP fixture与model")
        config = OpenAICompatibleConfig(
            base_url=args.http_base_url,
            model=args.http_model,
            api_key=os.environ.get("KRIM_AGENT_HTTP_API_KEY", ""),
        )
    elif args.http_model:
        raise ValueError("model须配合显式本机HTTP地址")
    async with await open_application(args.database, http_config=config) as app:
        runs, evaluation = RunSDK(app.service), EvaluationSDK(app.evaluations)
        source = await runs.start("/add 2 3", "complete-source")
        await runs.wait(source.id)
        facts = await runs.result(source.id)
        candidate = await evaluation.candidate(source.id, True)
        # 明确人工核算，而非读取facts.text后自动用作expected。
        reference = {
            "expected": "5",
            "expected_status": "completed",
            "expected_code": None,
            "expected_tools": None,
            "trajectory": None,
            "expected_schema": None,
        }
        dataset = await evaluation.freeze(
            FreezeRequest.model_validate(
                {
                    "name": "complete",
                    "version": "1",
                    "cases": [
                        {
                            "case_id": "sum",
                            "source_kind": "run",
                            "source_run_id": source.id,
                            "candidate_fingerprint": candidate.candidate_fingerprint,
                            "selected_input": "/add 2 3",
                            "reference": reference,
                            "reason": "人工核算2加3等于5",
                        }
                    ],
                }
            )
        )
        reports = {}
        for target in ["target-v1", "target-v2", "target-bad"]:
            view = await evaluation.start(request(dataset, "complete-" + target, target))
            await evaluation.wait(view.id)
            reports[target] = await evaluation.report(view.id)
        output = {
            "source_run_id": source.id,
            "source_facts": facts.model_dump(mode="json"),
            "dataset_fingerprint": dataset.fingerprint,
            "evaluations": {k: v["eval_id"] for k, v in reports.items()},
            "v2_gate": await evaluation.compare(
                reports["target-v1"]["eval_id"], reports["target-v2"]["eval_id"]
            ),
            "bad_gate": await evaluation.compare(
                reports["target-v1"]["eval_id"], reports["target-bad"]["eval_id"]
            ),
        }
        assert output["v2_gate"]["status"] == "pass" and output["bad_gate"]["status"] == "fail"
        if config is not None:
            view = await runs.start("/add 2 3", "complete-http", target="target-http")
            await runs.wait(view.id)
            http_facts = await runs.result(view.id)
            evaluation_view = await evaluation.start(
                request(dataset, "complete-http-eval", "target-http")
            )
            await evaluation.wait(evaluation_view.id)
            report = await evaluation.report(evaluation_view.id)
            output["http"] = {
                "run": http_facts.model_dump(mode="json"),
                "eval_id": evaluation_view.id,
                "target": report["target_descriptor"],
                "quality": report["items"][0]["quality"],
                "provider_calls": app.service.bindings["target-http"].loop.provider.calls,
            }
            assert output["http"]["provider_calls"] == 4 and output["http"]["quality"] == "passed"
    output["cleanup"] = {
        "database_closed": app.store.closed,
        "run_tasks_done": all(t.done() for t in app.service.tasks.values()),
        "eval_tasks_done": all(t.done() for t in app.evaluations.tasks.values()),
        "providers_closed": all(p.closed for p in app.owned_providers),
    }
    print(json.dumps(output, ensure_ascii=False, allow_nan=False))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", required=True, type=Path)
    parser.add_argument("--http-base-url")
    parser.add_argument("--http-model")
    asyncio.run(main(parser.parse_args()))
