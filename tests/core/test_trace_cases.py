"""M2显式来源/参考负例；只用合成Run。"""

from krim_agent.contracts import RunRequest


def test_registered_run_target_contract():
    request = RunRequest(text="/add 2 3", request_key="m2-target", target="target-v2")
    assert request.target == "target-v2"


def test_shared_eval_contract_vectors():
    import json
    from pathlib import Path

    from krim_agent.contracts import ServiceError, parse_json
    from krim_agent.evaluation.regression import validate_report
    from krim_agent.evaluation.types import DatasetRevision, EvalView, Reference, RuleSpec

    for vector in json.loads((Path(__file__).parent / "eval_contracts.json").read_text()):
        valid = True
        try:
            if vector["kind"] == "json":
                parse_json(vector["text"])
            elif vector["kind"] == "report":
                validate_report(vector["value"])
            else:
                {
                    "reference": Reference,
                    "eval": EvalView,
                    "rule": RuleSpec,
                    "dataset": DatasetRevision,
                }[vector["kind"]].model_validate(vector["value"])
        except (ValueError, TypeError, ServiceError):
            valid = False
        assert valid is vector["valid"], vector["name"]


def test_manual_identity_null_and_revision_conflicts():
    import pytest
    from pydantic import ValidationError

    from krim_agent.evaluation.trace_cases import freeze_revision
    from krim_agent.evaluation.types import FreezeRequest, Reference

    ref = dict(
        expected=None,
        expected_status="completed",
        expected_code=None,
        expected_tools=None,
        trajectory=None,
        expected_schema=None,
    )
    with pytest.raises(ValidationError):
        Reference.model_validate({k: v for k, v in ref.items() if k != "expected"})
    request = FreezeRequest.model_validate(
        dict(
            name="manual",
            version="1",
            cases=[
                dict(
                    case_id="null",
                    source_kind="manual",
                    input="/echo null",
                    reference=ref,
                    reason="人工选择JSON null",
                )
            ],
        )
    )
    revision = freeze_revision(request, {})
    assert revision.cases[0].reference.expected is None
    assert revision.cases[0].source.source_run_id is None
    assert revision.selection() == request
    assert revision.cases[0].source.observed is None
    with pytest.raises(ValidationError):
        FreezeRequest.model_validate(request.model_dump(mode="json") | {"eligible": True})


async def test_candidates_from_real_add_echo(tmp_path):
    from krim_agent.composition import open_application

    async with await open_application(tmp_path / "source.sqlite") as app:
        for index, text in enumerate(["/add 2 3", "/echo sample"]):
            view = await app.service.start(RunRequest(text=text, request_key=str(index)))
            await app.service.wait(view.id)
            before = app.provider.calls
            candidate = await app.evaluations.candidate(view.id)
            assert candidate.eligibility == "eligible" and candidate.selected_input is None
            chosen = await app.evaluations.candidate(view.id, True)
            assert (
                chosen.selected_input == text
                and chosen.candidate_fingerprint == candidate.candidate_fingerprint
            )
            assert app.provider.calls == before == 2 * (index + 1)


async def test_freeze_revalidates_source_and_explicit_reference(tmp_path):
    import pytest

    from krim_agent.composition import open_application
    from krim_agent.contracts import ServiceError
    from krim_agent.evaluation.types import FreezeRequest

    async with await open_application(tmp_path / "freeze.sqlite") as app:
        run = await app.service.start(RunRequest(text="/add 2 3", request_key="source"))
        await app.service.wait(run.id)
        candidate = await app.evaluations.candidate(run.id, True)
        body = {
            "name": "chosen",
            "version": "1",
            "cases": [
                {
                    "case_id": "sum",
                    "source_kind": "run",
                    "source_run_id": run.id,
                    "candidate_fingerprint": candidate.candidate_fingerprint,
                    "selected_input": "/add 2 3",
                    "reason": "人工核算",
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
        request = FreezeRequest.model_validate(body)
        revision = await app.evaluations.freeze(request)
        assert (await app.evaluations.freeze(request)) == revision
        bad = FreezeRequest.model_validate(body | {"version": "2"})
        assert bad.cases[0].reference.expected_tools is None
        source = bad.cases[0].model_copy(update={"candidate_fingerprint": "0" * 64})
        with pytest.raises(ServiceError, match="source_changed"):
            await app.evaluations.freeze(bad.model_copy(update={"cases": [source]}))
        changed = request.cases[0].model_copy(update={"reason": "不同理由"})
        with pytest.raises(ServiceError, match="revision_conflict"):
            await app.evaluations.freeze(request.model_copy(update={"cases": [changed]}))


async def test_source_filter_rejects_live_approval_context_and_unknown(tmp_path):
    from conftest import pending

    from krim_agent.composition import open_application
    from krim_agent.contracts import ApprovalReply

    async with await open_application(tmp_path / "filter.sqlite") as app:
        run = await app.service.start(RunRequest(text="/note welcome", request_key="note"))
        request = await pending(app.service, run.id)
        assert (await app.evaluations.candidate(run.id)).eligibility == "ineligible"
        reply = ApprovalReply(
            request_key="deny",
            allow=False,
            **{k: request[k] for k in ["call_id", "arguments_hash", "tool_hash"]},
        )
        await app.service.reply_approval(run.id, request["request_id"], reply)
        await app.service.wait(run.id)
        assert (await app.evaluations.candidate(run.id)).eligibility == "needs_context"


async def test_revision_read_never_reopens_source(tmp_path):
    from krim_agent.composition import open_application
    from krim_agent.evaluation.types import FreezeRequest

    async with await open_application(tmp_path / "cold.sqlite") as app:
        body = {
            "name": "manual",
            "version": "1",
            "cases": [
                {
                    "case_id": "manual",
                    "source_kind": "manual",
                    "input": "/echo data",
                    "reason": "人工参考",
                    "reference": {
                        "expected": "data",
                        "expected_status": "completed",
                        "expected_code": None,
                        "expected_tools": None,
                        "trajectory": None,
                        "expected_schema": None,
                    },
                }
            ],
        }
        request = FreezeRequest.model_validate(body)
        revision = await app.evaluations.freeze(request)

        async def forbidden(*args):
            raise AssertionError("revision read must not visit source")

        app.store._snapshot = forbidden
        assert (await app.store.dataset("manual", "1")) == revision
        assert (await app.evaluations.freeze(request)) == revision
        assert app.provider.calls == 0
