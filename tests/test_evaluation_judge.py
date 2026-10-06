"""显式离线评委、严格 JSON、预算与评分恢复/取消真实生产。"""

import asyncio
import json

import pytest

from krim_agent.budgets import UsageBudgetConfig
from krim_agent.config import Settings
from krim_agent.evaluation import (
    CallableTarget,
    EvalCase,
    EvalConfig,
    EvalDataset,
    EvalOutput,
    ModelJudgeScorer,
    evaluate,
    rescore,
)
from krim_agent.providers import ModelText, ModelToolCall, ModelUsage
from krim_agent.sdk import PythonAI
from krim_agent.usage import TokenUsage


class OfflineJudge:
    name = "offline-judge"

    def __init__(self, text=None):
        self.text = text or '{"score":1,"reason":"参考一致","evidence":["回答与期望相同"]}'
        self.calls = 0

    async def stream(self, messages, tools):
        self.calls += 1
        assert tools == []
        assert "不可信数据" in messages[-1].content
        yield ModelText(text=self.text)
        yield ModelUsage(usage=TokenUsage(input_tokens=10, output_tokens=5, total_tokens=15))


def dataset():
    return EvalDataset(name="judge", cases=[EvalCase(id="one", input="a", expected="a")])


def echo_target():
    async def target(case, _attempt):
        return EvalOutput(value=case.input)

    return CallableTarget("echo", target)


async def test_explicit_judge_persists_evidence_run_usage_and_is_not_offline_rescore():
    provider = OfflineJudge()
    async with await PythonAI.create(provider=provider) as ai:
        scorer = ModelJudgeScorer(ai, "检查候选回答与参考一致", version="rubric-1")
        handle = await ai.evaluation_service().start(dataset(), echo_target(), scorers=[scorer])
        assert (await handle.wait()).status == "completed"
        report = await handle.report()
        score = report.results[0].scores[0]
        assert report.summary.passed == 1 and provider.calls == 1
        assert score.value == 1 and score.evidence["items"] == ["回答与期望相同"]
        assert score.token_usage.total_tokens == 15 and score.usage_complete
        assert len(score.attempts) == 1 and score.attempts[0].run_id == score.run_id
        assert (await ai.read_result(score.run_id)).text == provider.text
        before = provider.calls
        with pytest.raises(ValueError, match="离线重评分"):
            await rescore(report, scorers=[scorer])
        assert provider.calls == before


@pytest.mark.parametrize(
    "raw",
    [
        '{"score":true,"reason":"x","evidence":["x"]}',
        '{"score":1.2,"reason":"x","evidence":["x"]}',
        '{"score":NaN,"reason":"x","evidence":["x"]}',
        '{"score":1,"score":0,"reason":"x","evidence":["x"]}',
        '{"score":1,"reason":"x","evidence":[]}',
        '{"score":1,"reason":"x","evidence":["x"],"script":"SECRET-CANARY"}',
    ],
)
async def test_judge_invalid_json_retains_real_usage_without_manufacturing_score(raw):
    provider = OfflineJudge(raw)
    async with await PythonAI.create(provider=provider) as ai:
        report = await evaluate(
            dataset(), echo_target(), scorers=[ModelJudgeScorer(ai, "参考一致")]
        )
        score = report.results[0].scores[0]
        assert report.summary.errors == 1 and score.value is None
        assert score.code == "judge_invalid_json" and score.token_usage.total_tokens == 15
        assert "SECRET-CANARY" not in (score.reason or "") + json.dumps(score.evidence)


async def test_judge_budget_blocks_dispatch_and_uses_original_account():
    provider = OfflineJudge()
    settings = Settings(
        database_url="sqlite+aiosqlite:///:memory:",
        context_tokenizer="cl100k_base",
        context_reserved_output_tokens=32,
        usage_budget=UsageBudgetConfig(max_total_tokens=1),
    )
    async with await PythonAI.create(provider=provider, settings=settings) as ai:
        handle = await ai.evaluation_service().start(
            dataset(), echo_target(), scorers=[ModelJudgeScorer(ai, "参考一致")]
        )
        await handle.wait()
        score = (await handle.report()).results[0].scores[0]
        assert score.status == "error" and provider.calls == 0
        assert score.code == "budget_tokens_exceeded"
        accounts = await ai.runtime.budget_store.accounts()
        assert len(accounts) == 1 and accounts[0].spent_tokens == accounts[0].held_tokens == 0


async def test_judge_cannot_invoke_host_tools_and_does_not_close_borrowed_runtime():
    class Malicious(OfflineJudge):
        async def stream(self, _messages, tools):
            self.calls += 1
            assert tools == []
            yield ModelToolCall(name="echo", arguments={"text": "SECRET-CANARY"})

    provider = Malicious()
    async with await PythonAI.create(provider=provider) as ai:
        original = ai.runtime.tools
        report = await evaluate(
            dataset(), echo_target(), scorers=[ModelJudgeScorer(ai, "参考一致")]
        )
        assert report.summary.errors == 1 and provider.calls == 1
        assert ai.runtime.tools is original and not ai.runtime._closed
        score = report.results[0].scores[0]
        assert score.value is None
        assert not any(a.ok for a in await ai.runtime.repository.audits(score.run_id))


@pytest.mark.parametrize("resolution", ["retry", "skip"])
async def test_partial_judge_pause_requires_explicit_resolution_without_target_replay(resolution):
    entered, closed = asyncio.Event(), asyncio.Event()

    class Partial(OfflineJudge):
        async def stream(self, messages, tools):
            if self.calls:
                async for value in super().stream(messages, tools):
                    yield value
                return
            self.calls += 1
            yield ModelText(text='{"score":')
            entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                closed.set()

    target_calls = 0

    async def target(case, _attempt):
        nonlocal target_calls
        target_calls += 1
        return EvalOutput(value=case.input)

    provider = Partial()
    async with await PythonAI.create(provider=provider) as ai:
        selected = ModelJudgeScorer(ai, "参考一致")
        service = ai.evaluation_service()
        handle = await service.start(dataset(), CallableTarget("echo", target), scorers=[selected])
        await asyncio.wait_for(entered.wait(), 5)
        await handle.pause()
        assert closed.is_set() and (await handle.items()).items[0].output.value == "a"
        await handle.resume()
        assert (await handle.wait()).status == "interrupted"
        assert (await handle.items()).items[0].phase == "grading_uncertain"
        assert provider.calls == target_calls == 1
        await handle.resume(
            retry_uncertain=resolution == "retry", skip_uncertain=resolution == "skip"
        )
        assert (await handle.wait()).status == "completed"
        report = await handle.report()
        score = report.results[0].scores[0]
        assert target_calls == 1
        if resolution == "retry":
            assert report.summary.passed == 1 and provider.calls == 2
            assert len(score.attempts) == 2 and score.attempts[0].status == "aborted"
            assert (
                score.token_usage.total_tokens is None
                and score.token_usage.reported["total_tokens"] == 15
            )
            from krim_agent.evaluation import analyze_report

            usage = analyze_report(report).grading
            assert usage.runs == 2 and usage.reported_runs == 1 and usage.total_tokens is None
            assert usage.reported_tokens["total_tokens"] == 15
        else:
            assert report.summary.errors == 1 and provider.calls == 1
            assert score.code == "eval_grading_skipped"


async def test_judge_completed_call_recovers_before_score_commit_with_zero_new_calls(monkeypatch):
    entered = asyncio.Event()
    provider = OfflineJudge()
    async with await PythonAI.create(provider=provider) as ai:
        scorer = ModelJudgeScorer(ai, "参考一致")
        original = scorer._decision

        async def waiting(result, binding):
            entered.set()
            await asyncio.Event().wait()

        monkeypatch.setattr(scorer, "_decision", waiting)
        handle = await ai.evaluation_service().start(dataset(), echo_target(), scorers=[scorer])
        await asyncio.wait_for(entered.wait(), 5)
        await handle.pause()
        assert provider.calls == 1
        monkeypatch.setattr(scorer, "_decision", original)
        await handle.resume()
        assert (await handle.wait()).status == "completed"
        assert (await handle.report()).summary.passed == 1 and provider.calls == 1


async def test_judge_timeout_stops_real_production_and_keeps_partial_usage():
    closed = asyncio.Event()

    class Slow(OfflineJudge):
        async def stream(self, _messages, _tools):
            self.calls += 1
            yield ModelText(text='{"score":')
            yield ModelUsage(
                usage=TokenUsage(input_tokens=7, output_tokens=1, total_tokens=8), final=False
            )
            try:
                await asyncio.Event().wait()
            finally:
                closed.set()

    async with await PythonAI.create(provider=Slow()) as ai:
        report = await evaluate(
            dataset(),
            echo_target(),
            scorers=[ModelJudgeScorer(ai, "参考一致")],
            config=EvalConfig(scorer_timeout_seconds=0.2),
        )
        score = report.results[0].scores[0]
        assert closed.is_set() and report.summary.errors == 1
        assert score.reason == "scorer_timeout" and score.run_id
        assert (await ai.runtime.repository.get_run(score.run_id)).status == "aborted"
        assert score.token_usage.reported["total_tokens"] == 8 and not score.usage_complete


async def test_remote_judge_requires_server_registration(runtime):
    import httpx

    from krim_agent.api import create_app
    from krim_agent.evaluation.jobs import EvalRemoteStartRequest
    from krim_agent.evaluation.remote import AsyncEvalClient

    provider = OfflineJudge()
    runtime.provider = provider
    judge = ModelJudgeScorer(PythonAI(runtime), "参考一致")
    app = create_app(runtime=runtime, eval_scorers={"judge-local": judge})
    async with (
        app.router.lifespan_context(app),
        AsyncEvalClient(transport=httpx.ASGITransport(app=app)) as client,
    ):
        request = EvalRemoteStartRequest(dataset=dataset(), scorers=["judge-local"])
        # 本用例目标也输出评委 JSON；可信目录评分真实响应。
        provider_assertion = provider.stream

        async def route(messages, tools):
            if messages[-1].content == "a":
                yield ModelText(text="a")
            else:
                async for event in provider_assertion(messages, tools):
                    yield event

        provider.stream = route
        view = await client.start(request)
        assert (await client.wait(view.id)).status == "completed"
        assert (await client.report(view.id)).summary.passed == 1
        response = await client.http.post(
            "/v1/evals",
            json={"dataset": dataset().model_dump(mode="json"), "scorers": ["judge-unregistered"]},
        )
        assert response.status_code == 422


async def test_real_judge_process_crash_after_response_resumes_with_zero_model_calls(tmp_path):
    import sys
    from pathlib import Path

    path, marker = tmp_path / "judge.sqlite", tmp_path / "marker.json"
    script = Path(__file__).parent / "fixtures/eval_judge_crash_worker.py"
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        str(script),
        str(path),
        str(marker),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await asyncio.wait_for(process.communicate(), 20)
    assert process.returncode == 73, (stdout + stderr).decode()
    record = json.loads(marker.read_text("utf-8"))
    await asyncio.sleep(10.05)
    provider = OfflineJudge()
    async with await PythonAI.create(
        provider=provider, settings=Settings(database_url=f"sqlite+aiosqlite:///{path}")
    ) as ai:
        handle = await ai.evaluation_service().resume(
            record["job_id"], target=echo_target(), scorers=[ModelJudgeScorer(ai, "参考一致")]
        )
        assert (await handle.wait()).status == "completed"
        report = await handle.report()
        assert report.summary.passed == 1 and provider.calls == 0
        assert report.results[0].scores[0].run_id == record["run_id"]
