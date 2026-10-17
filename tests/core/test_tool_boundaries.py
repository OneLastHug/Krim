import asyncio
import json
import sqlite3

import pytest
from conftest import EventProvider, pending, registry_for

from krim_agent.composition import open_application
from krim_agent.config import Settings
from krim_agent.contracts import ApprovalReply, RunRequest, ServiceError
from krim_agent.providers.types import ModelToolCall
from krim_agent.tools.output import OutputContract


class FalseyDeny:
    def __bool__(self):
        raise AssertionError("合法策略不能被判断真假")

    async def __call__(self, invocation):
        return "deny"


@pytest.mark.asyncio
async def test_falsey_policy_and_post_failure_preserve_truth(tmp_path):
    async with await open_application(tmp_path / "deny.db", policy=FalseyDeny()) as app:
        run = await app.service.start(RunRequest(text="/add 2 3", request_key="deny"))
        await app.service.wait(run.id)
        result = await app.service.result(run.id)
        assert result.run.status == "failed"
        assert result.tools[0].executor_started is False
        assert result.tools[0].code == "permission_denied"
        assert app.provider.calls == 1

    async def broken_post(invocation, outcome):
        raise RuntimeError("SYNTHETIC_SECRET")

    async with await open_application(tmp_path / "post.db", post=broken_post) as app:
        run = await app.service.start(RunRequest(text="/add 2 3", request_key="post"))
        await app.service.wait(run.id)
        result = await app.service.result(run.id)
        assert result.run.status == "failed"
        assert result.tools[0].executor_started is True
        assert result.tools[0].result_available is True
        assert result.tools[0].value == {"sum": 5.0}
        assert result.tools[0].code == "post_hook_error"
        assert app.provider.calls == 1


def make_reply(request, allow=True, key="reply-1"):
    return ApprovalReply(
        request_key=key,
        allow=allow,
        **{k: request[k] for k in ("call_id", "arguments_hash", "tool_hash")},
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mode,code",
    [
        ("no-policy", "permission_denied"),
        ("input", "invalid_input"),
        ("unknown", "unknown_tool"),
        ("pre-forge", "pre_hook_error"),
        ("output", "invalid_output"),
        ("post-forge", "post_hook_error"),
    ],
)
async def test_tool_chain_rejects_and_never_invents_success(tmp_path, mode, code):
    calls = []

    async def execute(args):
        calls.append(args)
        return {"wrong": "shape"} if mode == "output" else "actual"

    async def pre(invocation):
        return {"fake": "success"}

    async def post(invocation, result):
        result.value = "forged"

    provider = EventProvider(
        [
            ModelToolCall(
                id="c",
                name="absent" if mode == "unknown" else "echo",
                arguments={"text": False} if mode == "input" else {"text": "ok"},
            )
        ]
    )
    kwargs = {"provider": provider, "registry": registry_for(execute)}
    if mode == "no-policy":
        kwargs["policy"] = None
    if mode == "pre-forge":
        kwargs["pre"] = pre
    if mode == "post-forge":
        kwargs["post"] = post
    async with await open_application(tmp_path / (mode + ".db"), **kwargs) as app:
        run = await app.service.start(RunRequest(text="request", request_key=mode))
        await app.service.wait(run.id)
        facts = await app.service.result(run.id)
        assert facts.run.status == "failed" and facts.run.code == code
        assert len(calls) == (1 if mode in {"output", "post-forge"} else 0)
        assert provider.calls == 1 and provider.active == 0
        if mode == "post-forge":
            assert facts.tools[0].value == "actual"


@pytest.mark.parametrize(
    "schema", [{"$ref": "https://example.invalid/secret"}, {"type": "not-a-type"}]
)
def test_output_schema_never_fetches_remote_or_accepts_invalid_schema(schema):
    with pytest.raises(ValueError):
        OutputContract(schema)


def test_registry_rejects_write_and_duplicate_names():
    from dataclasses import replace

    from krim_agent.tools.builtins import builtin_registry

    spec = builtin_registry()._frozen[0][0]
    with pytest.raises(ValueError):
        replace(spec, risk="write")
    with pytest.raises(ValueError):
        __import__("krim_agent.tools.registry", fromlist=["ToolRegistry"]).ToolRegistry(
            (spec, spec)
        )


@pytest.mark.asyncio
async def test_frozen_definition_change_during_intent_wait_refuses_executor(tmp_path):
    calls = []

    async def execute(args):
        calls.append(1)
        return "actual"

    registry = registry_for(execute)
    async with await open_application(tmp_path / "freeze.db", registry=registry) as app:
        original = app.store.append

        async def replace_definition(run_id, kind, data, **kwargs):
            result = await original(run_id, kind, data, **kwargs)
            if kind == "tool_intent":
                registry._items = {}
            return result

        app.store.append = replace_definition
        run = await app.service.start(RunRequest(text="/echo ok", request_key="freeze"))
        await app.service.wait(run.id)
        assert (await app.service.get(run.id)).code == "tool_binding_changed"
        assert calls == []


@pytest.mark.asyncio
async def test_approval_bound_once_and_conflicting_or_cross_run_rejected(tmp_path):
    calls = []

    async def execute(args):
        calls.append(1)
        return "real"

    async def ask(invocation):
        return "ask"

    async with await open_application(
        tmp_path / "approval.db", registry=registry_for(execute), policy=ask
    ) as app:
        run = await app.service.start(RunRequest(text="/echo ok", request_key="approval"))
        request = await pending(app.service, run.id)
        wrong = make_reply(request).model_copy(update={"call_id": "different"})
        with pytest.raises(ServiceError):
            await app.service.reply_approval(run.id, request["request_id"], wrong)
        assert calls == []
        reply = make_reply(request)
        await app.service.reply_approval(run.id, request["request_id"], reply)
        await app.service.wait(run.id)
        assert (await app.service.reply_approval(run.id, request["request_id"], reply))[
            "replayed"
        ] is True
        with pytest.raises(ServiceError):
            await app.service.reply_approval(
                run.id, request["request_id"], make_reply(request, False)
            )
        other = await app.service.start(RunRequest(text="/slow", request_key="other"))
        with pytest.raises(ServiceError):
            await app.service.reply_approval(other.id, request["request_id"], reply)
        await app.service.cancel(other.id)
        assert calls == [1]


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["deny", "expire", "cancel"])
async def test_denied_expired_cancelled_approval_has_zero_executor_calls(tmp_path, action):
    calls = []

    async def execute(args):
        calls.append(1)
        return "actual"

    async def ask(invocation):
        return "ask"

    settings = Settings(approval_timeout=0.05 if action == "expire" else 2)
    async with await open_application(
        tmp_path / (action + ".db"), settings=settings, registry=registry_for(execute), policy=ask
    ) as app:
        run = await app.service.start(RunRequest(text="/echo ok", request_key=action))
        if action == "expire":
            await app.service.wait(run.id)
            request = next(
                e.data
                for e in (await app.store.snapshot(run.id)).events
                if e.kind == "approval_request"
            )
        else:
            request = await pending(app.service, run.id)
        if action == "deny":
            await app.service.reply_approval(
                run.id, request["request_id"], make_reply(request, False)
            )
        if action == "cancel":
            await app.service.cancel(run.id)
        await app.service.wait(run.id)
        if action != "deny":
            with pytest.raises(ServiceError):
                await app.service.reply_approval(run.id, request["request_id"], make_reply(request))
        facts = await app.service.result(run.id)
        assert calls == [] and facts.tools[0].executor_started is False
        assert (
            facts.run.code
            == {"deny": "approval_denied", "expire": "approval_expired", "cancel": "cancelled"}[
                action
            ]
        )


@pytest.mark.asyncio
async def test_schema_tamper_is_a_binding_error_before_new_run(tmp_path):
    async def execute(args):
        raise AssertionError("不得调用")

    registry = registry_for(execute)
    async with await open_application(tmp_path / "schema-tamper.db", registry=registry) as app:
        registry._frozen[0][0].output_schema["$ref"] = "https://example.invalid/never-fetch"
        with pytest.raises(ServiceError, match="tool_binding_changed"):
            await app.service.start(RunRequest(text="/echo x", request_key="tamper"))
        assert await app.store.by_key("tamper") is None


@pytest.mark.asyncio
async def test_post_cannot_mutate_nested_original_result(tmp_path):
    async def post(invocation, result):
        result.value["sum"] = -99

    async with await open_application(tmp_path / "nested-post.db", post=post) as app:
        run = await app.service.start(RunRequest(text="/add 2 3", request_key="nested"))
        await app.service.wait(run.id)
        result = await app.service.result(run.id)
        assert result.run.code == "post_hook_error" and result.tools[0].value == {"sum": 5.0}
        assert result.tools[0].executor_started and result.tools[0].result_available


@pytest.mark.asyncio
@pytest.mark.parametrize("allow", [True, False])
async def test_approval_commit_caller_cancellation_delivers_original_decision_once(tmp_path, allow):
    entered, release = asyncio.Event(), asyncio.Event()
    calls = []

    async def execute(args):
        calls.append(args.text)
        return "actual"

    async def ask(invocation):
        return "ask"

    database = tmp_path / ("approval-commit-" + str(allow) + ".db")
    async with await open_application(
        database,
        settings=Settings(approval_timeout=3),
        registry=registry_for(execute),
        policy=ask,
    ) as app:
        run = await app.service.start(RunRequest(text="/echo bound", request_key="commit"))
        request = await pending(app.service, run.id)
        request_id = request["request_id"]
        future = app.service._pending[request_id]
        original = app.store._event

        async def hold_transaction(conn, run_id, kind, *args, **kwargs):
            event = await original(conn, run_id, kind, *args, **kwargs)
            if kind == "approval_reply":
                entered.set()
                await release.wait()
            return event

        app.store._event = hold_transaction
        reply = make_reply(request, allow)
        caller = asyncio.create_task(app.service.reply_approval(run.id, request_id, reply))
        try:
            await asyncio.wait_for(entered.wait(), 2)
            with sqlite3.connect("file:" + str(database) + "?mode=ro", uri=True) as reader:
                assert (
                    reader.execute(
                        "SELECT count(*) FROM run_events WHERE kind='approval_reply'"
                    ).fetchone()[0]
                    == 0
                )
            caller.cancel()
            await asyncio.sleep(0)
            caller.cancel()
            assert not caller.done() and not future.done() and calls == []
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await caller
            replay = await app.service.reply_approval(run.id, request_id, reply)
            assert replay["replayed"] is True
            assert future.done() and not future.cancelled(), "持久决定未交付原等待者"
            assert future.result() is allow
            await asyncio.wait_for(app.service.wait(run.id), 2)
            facts = await app.service.result(run.id)
            assert facts.run.status == ("completed" if allow else "failed")
            assert facts.run.code == (None if allow else "approval_denied")
            assert calls == (["bound"] if allow else [])
            assert sum(e.kind == "approval_reply" for e in facts.events) == 1
            assert request_id not in app.service._pending
            for conflicting in [
                make_reply(request, not allow),
                make_reply(request, allow, "other-key"),
            ]:
                with pytest.raises(ServiceError, match="approval_conflict"):
                    await app.service.reply_approval(run.id, request_id, conflicting)
            assert calls == (["bound"] if allow else [])
            (tmp_path / "approval-observed.json").write_text(
                json.dumps(
                    {
                        "allow": allow,
                        "caller_cancelled": caller.cancelled(),
                        "future_done": future.done(),
                        "future_cancelled": future.cancelled(),
                        "future_result": future.result(),
                        "tools": len(calls),
                        "status": facts.run.status,
                        "code": facts.run.code,
                        "approval_reply_events": sum(
                            e.kind == "approval_reply" for e in facts.events
                        ),
                    }
                )
            )
        finally:
            release.set()
            await asyncio.gather(caller, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["expire", "cancel"])
async def test_approval_commit_never_revives_expired_or_cancelled_waiter(tmp_path, action):
    entered, release, ended = (asyncio.Event() for _ in range(3))
    calls = []

    async def execute(args):
        calls.append(1)
        return "actual"

    async def ask(invocation):
        return "ask"

    async with await open_application(
        tmp_path / ("late-commit-" + action + ".db"),
        registry=registry_for(execute),
        policy=ask,
        settings=Settings(approval_timeout=0.6 if action == "expire" else 3),
    ) as app:
        run = await app.service.start(RunRequest(text="/echo bound", request_key="late"))
        request = await pending(app.service, run.id)
        request_id = request["request_id"]
        future = app.service._pending[request_id]
        future.add_done_callback(lambda _: ended.set())
        original = app.store._event

        async def hold_transaction(conn, run_id, kind, *args, **kwargs):
            event = await original(conn, run_id, kind, *args, **kwargs)
            if kind == "approval_reply":
                entered.set()
                await release.wait()
            return event

        app.store._event = hold_transaction
        reply = make_reply(request)
        caller = asyncio.create_task(app.service.reply_approval(run.id, request_id, reply))
        try:
            await asyncio.wait_for(entered.wait(), 2)
            caller.cancel()
            if action == "cancel":
                # 真实 producer 在事务屏障期间取消；等待其原 future 收束。
                app.service.tasks[run.id].cancel()
            await asyncio.wait_for(ended.wait(), 2)
            assert future.cancelled() and calls == []
            release.set()
            with pytest.raises(ServiceError, match="approval_expired"):
                await caller
            await asyncio.wait_for(app.service.wait(run.id), 2)
            facts = await app.service.result(run.id)
            assert facts.run.status == ("aborted" if action == "cancel" else "failed")
            assert facts.run.code == ("cancelled" if action == "cancel" else "approval_expired")
            assert sum(e.kind == "approval_reply" for e in facts.events) == 1
            assert calls == [] and request_id not in app.service._pending
            # 同键只能读到历史决定，不能重新激活已收束的 future 或工具。
            assert (await app.service.reply_approval(run.id, request_id, reply))["replayed"]
            assert future.cancelled() and calls == []
        finally:
            release.set()
            await asyncio.gather(caller, return_exceptions=True)
