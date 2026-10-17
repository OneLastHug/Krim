"""真实目标与有限规则/取消，不预造运行结果。"""

import asyncio

import pytest

from krim_agent.composition import open_application
from krim_agent.contracts import RunRequest


async def one_request(app, text="/add 2 3", key="eval", rules=("status_code",), reference=None):
    from krim_agent.evaluation.types import EvalRequest, FreezeRequest, rule

    revision = await app.evaluations.freeze(
        FreezeRequest.model_validate(
            {
                "name": key,
                "version": "1",
                "cases": [
                    {
                        "case_id": "one",
                        "source_kind": "manual",
                        "input": text,
                        "reason": "显式人工参考",
                        "reference": reference
                        if reference is not None
                        else {
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
        )
    )
    return EvalRequest.model_validate(
        {
            "request_key": key,
            "dataset": {k: getattr(revision, k) for k in ["name", "version", "fingerprint"]},
            "target_key": "target-v2",
            "rules": [rule(n).model_dump(mode="json") for n in rules],
            "execution": {
                "repetitions": 1,
                "concurrency": 1,
                "item_timeout_seconds": 12,
                "scorer_timeout_seconds": 1,
                "batch_timeout_seconds": 600,
            },
        }
    )


@pytest.mark.asyncio
async def test_real_versions_have_distinct_child_runs(tmp_path):
    async with await open_application(tmp_path / "versions.sqlite") as app:
        answers = []
        for target in ["target-v1", "target-v2", "target-bad"]:
            run = await app.service.start(
                RunRequest(text="/add 2.5 0.5", request_key=target, target=target)
            )
            await app.service.wait(run.id)
            facts = await app.service.result(run.id)
            answers.append(facts.text)
            assert facts.model_steps == 2 and len(facts.tools) == 1
            assert facts.tools[0].executor_started is True
        assert answers == ["2", "3", "3"]


@pytest.mark.asyncio
async def test_rule_vectors_and_unknown_facts(tmp_path):
    from krim_agent.evaluation.scorers import score_rule
    from krim_agent.evaluation.trace_cases import observe_snapshot
    from krim_agent.evaluation.types import Reference, rule

    async with await open_application(tmp_path / "rules.sqlite") as app:
        run = await app.service.start(RunRequest(text="/echo null", request_key="null"))
        await app.service.wait(run.id)
        output = observe_snapshot(await app.store.snapshot(run.id))
        reference = Reference(
            expected=None,
            expected_status="completed",
            expected_code=None,
            expected_tools=None,
            trajectory=None,
            expected_schema=None,
        )
        score = await score_rule(rule("json_match"), reference, output)
        assert (score.status, score.value) == ("scored", 1)
        assert output.token_usage is None and output.usage_complete is False
        assert (await score_rule(rule("exact_match"), reference, output)).status == "skipped"
        assert (await score_rule(rule("json_schema"), reference, output)).status == "skipped"
        assert (await score_rule(rule("tool_calls"), reference, output)).status == "skipped"
        assert (await score_rule(rule("trajectory"), reference, output)).status == "skipped"
        for text, expected, passed in [
            ("true", 1, 0),
            ("false", 0, 0),
            ("1", 1.0, 1),
            ('{"x":1,"x":2}', {"x": 2}, 0),
            ("NaN", None, 0),
        ]:
            changed = output.model_copy(update={"text": text})
            ref = reference.model_copy(update={"expected": expected})
            assert (await score_rule(rule("json_match"), ref, changed)).value == passed
        schema = reference.model_copy(update={"expected_schema": {"type": "integer"}})
        assert (await score_rule(rule("json_schema"), schema, output)).value == 0
        assert (
            await score_rule(rule("json_schema"), schema, output.model_copy(update={"text": "1"}))
        ).value == 1
        # 真实策略拒绝仍可符合人工参考：Eval完成、子Run失败、质量通过是不同事实。
        from krim_agent.evaluation.scorers import classify

        refusal = {
            "expected": None,
            "expected_status": "failed",
            "expected_code": "permission_denied",
            "expected_tools": [
                {
                    "name": "lookup_note",
                    "arguments": None,
                    "executor_started": False,
                    "status": "denied",
                }
            ],
            "trajectory": {"exact_steps": 1, "tool_names": ["lookup_note"]},
            "expected_schema": None,
        }
        request = await one_request(
            app,
            "/note welcome",
            "expected-refusal",
            ("status_code", "tool_calls", "trajectory"),
            refusal,
        )
        batch = await app.evaluations.start(request)
        assert (await app.evaluations.wait(batch.id)).status == "completed"
        item = (await app.evaluations.report(batch.id))["items"][0]
        assert item["quality"] == "passed" and item["output"]["status"] == "failed"
        assert item["output"]["tools"][0]["executor_started"] is False
        actual = observe_snapshot(await app.store.snapshot(item["run_id"]))
        ref = Reference.model_validate(refusal)
        unknown = actual.model_copy(
            update={
                "tools": [
                    actual.tools[0].model_copy(
                        update={"executor_started": None, "status": "unknown"}
                    )
                ]
            }
        )
        skipped = await score_rule(rule("tool_calls"), ref, unknown)
        assert skipped.status == "skipped" and skipped.value is None
        assert classify([skipped]) == "unscored"
        assert (
            await score_rule(
                rule("trajectory"), ref, actual.model_copy(update={"model_steps": None})
            )
        ).status == "skipped"


@pytest.mark.asyncio
async def test_shared_admission_with_interactive_runs(tmp_path):
    from krim_agent.providers.fake import FakeProvider

    entered = asyncio.Event()
    count = 0

    class Counted(FakeProvider):
        async def stream(self, messages, tools):
            nonlocal count
            count += 1
            if count == 4:
                entered.set()
            source = super().stream(messages, tools)
            try:
                async for event in source:
                    yield event
            finally:
                await source.aclose()

    provider = Counted()
    async with await open_application(tmp_path / "capacity.sqlite", provider=provider) as app:
        occupied = [
            await app.service.start(RunRequest(text="/slow", request_key="occupied-" + str(i)))
            for i in range(4)
        ]
        await asyncio.wait_for(entered.wait(), 3)
        assert app.provider.calls == 4
        request = await one_request(app, key="full")
        view = await app.evaluations.start(request)
        assert (await app.evaluations.wait(view.id)).code == "capacity"
        assert app.service.bindings["target-v2"].loop.provider.calls == 0
        assert len((await app.store.eval_snapshot(view.id))["items"]) == 1
        await app.service.cancel(occupied[0].id)
        next_view = await app.evaluations.start(
            request.model_copy(update={"request_key": "released"})
        )
        assert (await app.evaluations.wait(next_view.id)).status == "completed"
        assert app.service.bindings["target-v2"].loop.provider.calls == 2
        assert sum(not t.done() for t in app.service.tasks.values()) == 3
    await provider.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["provider", "executor"])
async def test_target_and_tool_binding_after_await(tmp_path, kind):
    async with await open_application(tmp_path / (kind + ".sqlite")) as app:
        entered = asyncio.Event()
        release = asyncio.Event()
        original = app.store.append

        async def block(run_id, event, *args, **kwargs):
            result = await original(run_id, event, *args, **kwargs)
            if event == "tool_intent":
                entered.set()
                await release.wait()
            return result

        app.store.append = block
        binding = app.service.bindings["target-v2"]
        run = await app.service.start(
            RunRequest(text="/add 2 3", request_key="binding", target="target-v2")
        )
        await asyncio.wait_for(entered.wait(), 3)
        if kind == "provider":
            old = binding.loop.provider.stream

            async def changed(*args, **kwargs):
                async for item in old(*args, **kwargs):
                    yield item

            binding.loop.provider.stream = changed
        else:
            spec = binding.loop.executor.registry.get("add")

            async def changed(args):
                raise AssertionError("new executor must not enter")

            object.__setattr__(spec, "executor", changed)
        release.set()
        await app.service.wait(run.id)
        facts = await app.service.result(run.id)
        assert facts.run.code == (
            "target_binding_changed" if kind == "provider" else "tool_binding_changed"
        )
        assert facts.tools[0].executor_started is False


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["cancel", "timeout"])
async def test_scorer_timeout_and_cancel_join_real_cleanup(tmp_path, monkeypatch, action):
    from krim_agent.contracts import ServiceError
    from krim_agent.evaluation import scorers

    entered = asyncio.Event()
    cleaning = asyncio.Event()
    release = asyncio.Event()
    counts = {"enter": 0, "exit": 0}
    original = scorers.score_rule

    async def held(*args):
        counts["enter"] += 1
        entered.set()
        try:
            await asyncio.Event().wait()
            return await original(*args)
        finally:
            cleaning.set()
            await release.wait()
            counts["exit"] += 1

    monkeypatch.setattr(scorers, "score_rule", held)
    path = tmp_path / (action + ".sqlite")
    async with await open_application(path) as app:
        view = await app.evaluations.start(await one_request(app))
        await asyncio.wait_for(entered.wait(), 3)
        waiting = asyncio.create_task(
            app.evaluations.cancel(view.id) if action == "cancel" else app.evaluations.wait(view.id)
        )
        await asyncio.wait_for(cleaning.wait(), 3)
        assert not waiting.done() and not app.store.closed
        assert counts == {"enter": 1, "exit": 0}
        with pytest.raises(ServiceError, match="database_busy"):
            await type(app.store).open(path)
        release.set()
        result = await asyncio.wait_for(waiting, 3)
        assert counts == {"enter": 1, "exit": 1} and app.evaluations.tasks[view.id].done()
        assert result.status == ("aborted" if action == "cancel" else "completed")
        report = await app.evaluations.report(view.id)
        if action == "timeout":
            assert report["items"][0]["scores"][0]["reason"] == "scorer_timeout"
        before = app.service.bindings["target-v2"].loop.provider.calls
        await app.evaluations.report(view.id)
        assert before == app.service.bindings["target-v2"].loop.provider.calls == 2
