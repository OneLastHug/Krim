"""旧v1 HTTP评测子运行事实校验回归。"""

import pytest

from krim_agent.contracts import ServiceError
from krim_agent.evaluation.adapters.krim import LegacyRepository
from krim_agent.evaluation.trace_cases import freeze_revision
from krim_agent.evaluation.types import EvalRequest, FreezeRequest, rule


def _snapshot(item_timeout: int | float = 60) -> dict:
    dataset = freeze_revision(
        FreezeRequest.model_validate(
            {
                "name": "http-child",
                "version": "1",
                "cases": [
                    {
                        "case_id": "manual-case",
                        "source_kind": "manual",
                        "input": "/add 2 3",
                        "reason": "合成回归用例",
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
        ),
        {},
    )
    request = EvalRequest.model_validate(
        {
            "request_key": "http-child-regression",
            "dataset": {
                "name": dataset.name,
                "version": dataset.version,
                "fingerprint": dataset.fingerprint,
            },
            "target_key": "target-http",
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
    return {
        "id": "eval-http-child",
        "request_json": request.model_dump(mode="json"),
        "execution": {
            "effective_timeouts": {
                "schema_version": 1,
                "source": "trusted-http-host/1",
                "item_timeout_seconds": item_timeout,
                "batch_timeout_seconds": 600,
            }
        },
        "target": {"limits": {"run_timeout": 30}},
        "dataset": dataset.model_dump(mode="json"),
        "items": [{"case_id": "manual-case", "repetition": 0}],
    }


def _plan(snapshot: dict):
    # plan只读取冻结事实，不需要存储或Run服务。
    repository = object.__new__(LegacyRepository)
    return repository.plan(snapshot)


def test_legacy_http_plan_accepts_child_timeout_from_short_frozen_run_limit():
    plan = _plan(_snapshot())

    assert plan.item_timeout == 60
    assert plan.batch_timeout == 600


@pytest.mark.parametrize(
    "changes",
    [
        {"item_timeout_seconds": 59},
        {"item_timeout_seconds": 61},
        {"item_timeout_seconds": True},
        {"source": "untrusted-http-host/1"},
        {"batch_timeout_seconds": 599},
        {"schema_version": 2},
    ],
)
def test_legacy_http_plan_rejects_child_timeout_facts_not_matching_frozen_target(changes):
    snapshot = _snapshot()
    snapshot["execution"]["effective_timeouts"].update(changes)

    with pytest.raises(ServiceError, match="facts_corrupt"):
        _plan(snapshot)
