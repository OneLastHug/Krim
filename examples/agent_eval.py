"""显式人工参考→冻结→真实版本→规则与Gate；只用离线合成任务。"""

import argparse
import asyncio
import json
from pathlib import Path

from krim_agent.composition import open_application
from krim_agent.evaluation.types import EvalRequest, FreezeRequest, rule
from krim_agent.sdk import EvaluationSDK, RunSDK


async def main(path: Path) -> None:
    async with await open_application(path) as app:
        assert app.evaluations is not None
        runs, evaluation = RunSDK(app.service), EvaluationSDK(app.evaluations)
        source = await runs.start("/add 2 3", "agent-source")
        await runs.wait(source.id)
        candidate = await evaluation.candidate(source.id, True)
        # 参考是明确的人工核算，不能从observed输出自动复制。
        reference = {
            "expected": "5",
            "expected_status": "completed",
            "expected_code": None,
            "expected_tools": None,
            "trajectory": None,
            "expected_schema": None,
        }
        revision = await evaluation.freeze(
            FreezeRequest.model_validate(
                {
                    "name": "agent",
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
        for target in ("target-v1", "target-v2", "target-bad"):
            request = EvalRequest.model_validate(
                {
                    "request_key": "agent-" + target,
                    "dataset": {
                        k: getattr(revision, k) for k in ("name", "version", "fingerprint")
                    },
                    "target_key": target,
                    "rules": [
                        rule(n).model_dump(mode="json") for n in ("status_code", "exact_match")
                    ],
                    "execution": {
                        "repetitions": 1,
                        "concurrency": 1,
                        "item_timeout_seconds": 12,
                        "scorer_timeout_seconds": 1,
                        "batch_timeout_seconds": 600,
                    },
                }
            )
            view = await evaluation.start(request)
            await evaluation.wait(view.id)
            reports[target] = await evaluation.report(view.id)
        print(
            json.dumps(
                {
                    "source_run_id": source.id,
                    "dataset": revision.fingerprint,
                    "evaluations": {key: value["eval_id"] for key, value in reports.items()},
                    "v2": await evaluation.compare(
                        reports["target-v1"]["eval_id"], reports["target-v2"]["eval_id"]
                    ),
                    "bad": await evaluation.compare(
                        reports["target-v1"]["eval_id"], reports["target-bad"]["eval_id"]
                    ),
                },
                ensure_ascii=False,
            )
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", type=Path, required=True)
    asyncio.run(main(parser.parse_args().database))
