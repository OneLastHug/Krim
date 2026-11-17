"""Durable interaction requests, ownership, and plan-mode policy."""

from contextlib import asynccontextmanager

import pytest
from sqlalchemy import update

from krim_agent.config import Settings
from krim_agent.context.types import ContextPolicy
from krim_agent.contracts import ServiceError, StrictModel, fingerprint
from krim_agent.interaction import InteractionService, RequestPending
from krim_agent.interaction.contracts import (
    Answer,
    AnswerRequest,
    AskUserArgs,
    CancelRequest,
    EnterPlanModeArgs,
    ExitPlanModeArgs,
    Question,
)
from krim_agent.security import Membership, PrincipalContext
from krim_agent.sessions.types import SessionCreate
from krim_agent.storage import sessions as session_sql
from krim_agent.storage.interaction_models import INTERACTION_TABLES
from krim_agent.storage.models import metadata, sessions
from krim_agent.storage.sqlite import SQLiteStore
from krim_agent.tools.core import Invocation, ToolSource, ToolSpec
from krim_agent.tools.output import OutputContract
from krim_agent.tools.session import SessionToolScope


class EmptyArgs(StrictModel):
    pass


@asynccontextmanager
async def interaction_env(path, owner="alice"):
    store = await SQLiteStore.open(path)
    try:
        async with store.engine.begin() as conn:
            await conn.run_sync(
                lambda sync: metadata.create_all(sync, tables=list(INTERACTION_TABLES))
            )
        actor = PrincipalContext(owner, 1, (Membership(workspace_id="alpha"),))
        session = await session_sql.create_session(
            store,
            "alpha",
            owner,
            SessionCreate(request_key="session"),
            {"key": "demo"},
            ContextPolicy(),
            Settings(),
        )
        yield store, InteractionService(store), actor, session
    finally:
        await store.close()


def scope(actor, session, *, turn="turn-1", run="run-1"):
    return SessionToolScope(actor, "alpha", session.id, turn, run)


def text_question():
    return AskUserArgs(questions=[Question(id="q", prompt="Question", mode="text")])


def full_questions():
    return AskUserArgs(
        questions=[
            Question(
                id="single",
                prompt="Choose one",
                mode="single",
                options=[{"id": "a", "label": "A"}, {"id": "b", "label": "B"}],
                allow_text=False,
            ),
            Question(
                id="multiple",
                prompt="Choose any",
                mode="multiple",
                options=[{"id": "x", "label": "X"}, {"id": "y", "label": "Y"}],
            ),
            Question(id="text", prompt="Add a note", mode="text"),
        ]
    )


async def test_restart_replay_and_request_key_conflict(tmp_path):
    path = tmp_path / "interactions.sqlite"
    async with interaction_env(path) as (_, service, actor, session):
        first = await service.ask(scope(actor, session), text_question(), "same-key")
        replay = await service.ask(scope(actor, session), text_question(), "same-key")
        assert replay["id"] == first["id"] and replay["status"] == "pending"
        with pytest.raises(ServiceError, match="key_conflict"):
            await service.ask(
                scope(actor, session),
                AskUserArgs(questions=[Question(id="other", prompt="Other", mode="text")]),
                "same-key",
            )
        request_id = first["id"]

    reopened = await SQLiteStore.open(path)
    try:
        service = InteractionService(reopened)
        saved = await service.read_request(actor, "alpha", session.id, request_id)
        fingerprint(saved)  # SQLAlchemy quoted_name keys must become exact JSON str keys.
        assert all(type(key) is str for key in saved)
        assert saved["status"] == "pending"
        assert saved["payload"] == text_question().model_dump(mode="json")
    finally:
        await reopened.close()


async def test_answer_validation_staleness_and_duplicate_answers(tmp_path):
    async with interaction_env(tmp_path / "answers.sqlite") as (_, service, actor, session):
        request = await service.ask(scope(actor, session), full_questions(), "ask")
        stale = AnswerRequest(
            request_key="stale-revision",
            expected_revision=2,
            answers=[Answer(question_id="single", option_ids=["a"])],
        )
        with pytest.raises(ServiceError, match="input_revision_conflict"):
            await service.answer(actor, "alpha", session.id, request["id"], stale)
        invalid = AnswerRequest(
            request_key="invalid",
            expected_revision=1,
            answers=[
                Answer(question_id="single", option_ids=["a", "b"]),
                Answer(question_id="multiple", option_ids=["x"]),
                Answer(question_id="text", text="note"),
            ],
        )
        with pytest.raises(ServiceError, match="invalid_input"):
            await service.answer(actor, "alpha", session.id, request["id"], invalid)
        missing = AnswerRequest(
            request_key="missing",
            expected_revision=1,
            answers=[Answer(question_id="single", option_ids=["a"])],
        )
        with pytest.raises(ServiceError, match="invalid_input"):
            await service.answer(actor, "alpha", session.id, request["id"], missing)

        valid = AnswerRequest(
            request_key="valid",
            expected_revision=1,
            answers=[
                Answer(question_id="single", option_ids=["a"]),
                Answer(question_id="multiple", option_ids=["x", "y"]),
                Answer(question_id="text", text="A note"),
            ],
        )
        answered = await service.answer(actor, "alpha", session.id, request["id"], valid)
        assert answered["status"] == "answered"
        assert answered["response"]["answers"] == [
            answer.model_dump(mode="json") for answer in valid.answers
        ]
        replay = await service.answer(actor, "alpha", session.id, request["id"], valid)
        assert replay == answered
        with pytest.raises(ServiceError, match="key_conflict"):
            await service.answer(
                actor,
                "alpha",
                session.id,
                request["id"],
                valid.model_copy(update={
                    "answers": [
                        Answer(question_id="single", option_ids=["b"]),
                        *valid.answers[1:],
                    ]
                }),
            )
        with pytest.raises(ServiceError, match="input_revision_conflict"):
            await service.answer(
                actor,
                "alpha",
                session.id,
                request["id"],
                valid.model_copy(update={"request_key": "duplicate-mutation"}),
            )

        duplicate_question = AnswerRequest(
            request_key="duplicate-question",
            expected_revision=1,
            answers=[
                Answer(question_id="single", option_ids=["a"]),
                Answer(question_id="single", option_ids=["b"]),
                Answer(question_id="multiple", option_ids=["x"]),
                Answer(question_id="text", text="note"),
            ],
        )
        another = await service.ask(scope(actor, session, turn="turn-2"), full_questions(), "ask-2")
        with pytest.raises(ServiceError, match="invalid_input"):
            await service.answer(actor, "alpha", session.id, another["id"], duplicate_question)


async def test_cancel_and_supersede_prevent_late_answer(tmp_path):
    async with interaction_env(tmp_path / "settlement.sqlite") as (_, service, actor, session):
        cancelled = await service.ask(scope(actor, session), text_question(), "cancel-me")
        value = await service.cancel(
            actor,
            "alpha",
            session.id,
            cancelled["id"],
            CancelRequest(request_key="cancel", expected_revision=1, reason="not needed"),
        )
        assert value["status"] == "cancelled"
        assert value["response"] == {"reason": "not needed"}

        old = await service.ask(
            scope(actor, session, turn="turn-2"), text_question(), "supersede-me"
        )
        state = await service.supersede(actor, "alpha", session.id, "new-user-turn")
        assert state["pending"] == []
        old_request = await service.read_request(actor, "alpha", session.id, old["id"])
        assert old_request["status"] == "superseded"
        with pytest.raises(ServiceError, match="input_revision_conflict"):
            await service.answer(
                actor,
                "alpha",
                session.id,
                old["id"],
                AnswerRequest(
                    request_key="too-late",
                    expected_revision=1,
                    answers=[Answer(question_id="q", text="late")],
                ),
            )


async def test_plan_confirmation_false_preserves_mode_and_true_clears_it(tmp_path):
    async with interaction_env(tmp_path / "plan.sqlite") as (_, service, actor, session):
        assert await service.state(actor, "alpha", session.id) == {
            "mode": "execute",
            "revision": 0,
            "pending": [],
        }
        entered = await service.enter_plan_mode(
            scope(actor, session), EnterPlanModeArgs(reason="inspect first"), "enter"
        )
        assert entered == {"mode": "plan", "revision": 1, "pending": []}
        proposal = await service.exit_plan_mode(
            scope(actor, session), ExitPlanModeArgs(plan="First proposal"), "proposal-no"
        )
        assert proposal["status"] == "pending"
        rejected = await service.answer(
            actor,
            "alpha",
            session.id,
            proposal["id"],
            AnswerRequest(request_key="reject", expected_revision=1, confirmed=False),
        )
        assert rejected["response"] == {"confirmed": False}
        assert (await service.state(actor, "alpha", session.id))["mode"] == "plan"

        proposal = await service.exit_plan_mode(
            scope(actor, session), ExitPlanModeArgs(plan="Approved proposal"), "proposal-yes"
        )
        accepted = await service.answer(
            actor,
            "alpha",
            session.id,
            proposal["id"],
            AnswerRequest(request_key="confirm", expected_revision=1, confirmed=True),
        )
        assert accepted["response"] == {"confirmed": True}
        state = await service.state(actor, "alpha", session.id)
        assert state["mode"] == "execute" and state["pending"] == []

        with pytest.raises(ServiceError, match="input_revision_conflict"):
            await service.exit_plan_mode(
                scope(actor, session), ExitPlanModeArgs(plan="Not in plan mode"), "bad-exit"
            )


async def test_reads_and_mutations_require_session_owner_not_workspace_admin(tmp_path):
    async with interaction_env(tmp_path / "owner.sqlite") as (_, service, owner, session):
        other = PrincipalContext("bob", 1, (Membership(workspace_id="alpha"),))
        admin = PrincipalContext("carol", 1, (Membership(workspace_id="alpha", role="admin"),))
        request = await service.ask(scope(owner, session), text_question(), "owner-request")
        for actor in (other, admin):
            with pytest.raises(ServiceError, match="not_found"):
                await service.state(actor, "alpha", session.id)
            with pytest.raises(ServiceError, match="not_found"):
                await service.read_request(actor, "alpha", session.id, request["id"])
            with pytest.raises(ServiceError, match="not_found"):
                await service.answer(
                    actor,
                    "alpha",
                    session.id,
                    request["id"],
                    AnswerRequest(
                        request_key="foreign-" + actor.principal_id,
                        expected_revision=1,
                        answers=[Answer(question_id="q", text="not authorized")],
                    ),
                )
        with pytest.raises(ServiceError, match="not_found"):
            await service.read_request(owner, "alpha", "another-session", request["id"])
        with pytest.raises(ServiceError, match="not_found"):
            await service.state(owner, "wrong-workspace", session.id)


async def test_new_request_supersedes_prior_pending_and_resume_reports_terminals(tmp_path):
    async with interaction_env(tmp_path / "resume.sqlite") as (_, service, actor, session):
        pending = await service.ask(scope(actor, session), text_question(), "pending")
        with pytest.raises(RequestPending) as parked:
            await service.resume(actor, "alpha", session.id, pending["id"])
        assert parked.value.request["id"] == pending["id"]

        replacement = await service.ask(
            scope(actor, session, turn="turn-2"), text_question(), "replacement"
        )
        assert (
            await service.read_request(actor, "alpha", session.id, pending["id"])
        )["status"] == "superseded"
        superseded = await service.resume(actor, "alpha", session.id, pending["id"])
        assert superseded["status"] == "superseded"
        with pytest.raises(ServiceError, match="input_revision_conflict"):
            await service.answer(
                actor,
                "alpha",
                session.id,
                pending["id"],
                AnswerRequest(
                    request_key="answer-superseded",
                    expected_revision=1,
                    answers=[Answer(question_id="q", text="late")],
                ),
            )

        answered = await service.answer(
            actor,
            "alpha",
            session.id,
            replacement["id"],
            AnswerRequest(
                request_key="answer-replacement",
                expected_revision=1,
                answers=[Answer(question_id="q", text="done")],
            ),
        )
        assert (await service.resume(actor, "alpha", session.id, replacement["id"])) == answered

        to_cancel = await service.ask(
            scope(actor, session, turn="turn-3"), text_question(), "cancel-request"
        )
        cancelled = await service.cancel(
            actor,
            "alpha",
            session.id,
            to_cancel["id"],
            CancelRequest(request_key="cancel", expected_revision=1, reason="stop"),
        )
        assert await service.resume(actor, "alpha", session.id, to_cancel["id"]) == cancelled


async def test_answered_request_survives_restart_and_ask_replay_returns_answer(tmp_path):
    path = tmp_path / "answered-restart.sqlite"
    async with interaction_env(path) as (_, service, actor, session):
        request = await service.ask(scope(actor, session), text_question(), "ask-key")
        answer_body = AnswerRequest(
            request_key="answer-key",
            expected_revision=1,
            answers=[Answer(question_id="q", text="persisted")],
        )
        answered = await service.answer(actor, "alpha", session.id, request["id"], answer_body)
        assert (await service.ask(scope(actor, session), text_question(), "ask-key")) == answered

    reopened = await SQLiteStore.open(path)
    try:
        service = InteractionService(reopened)
        resumed = await service.resume(actor, "alpha", session.id, request["id"])
        replayed_ask = await service.ask(scope(actor, session), text_question(), "ask-key")
        assert resumed == answered
        assert replayed_ask == answered
    finally:
        await reopened.close()


async def test_reentering_plan_supersedes_old_confirmation(tmp_path):
    async with interaction_env(tmp_path / "reenter-plan.sqlite") as (_, service, actor, session):
        await service.enter_plan_mode(scope(actor, session), EnterPlanModeArgs(), "enter-one")
        old = await service.exit_plan_mode(
            scope(actor, session), ExitPlanModeArgs(plan="First plan"), "proposal-one"
        )
        await service.enter_plan_mode(
            scope(actor, session, turn="turn-2"),
            EnterPlanModeArgs(reason="revised"),
            "enter-two",
        )
        settled = await service.read_request(actor, "alpha", session.id, old["id"])
        assert settled["status"] == "superseded"
        with pytest.raises(ServiceError, match="input_revision_conflict"):
            await service.answer(
                actor,
                "alpha",
                session.id,
                old["id"],
                AnswerRequest(request_key="stale-confirm", expected_revision=1, confirmed=True),
            )
        assert (await service.state(actor, "alpha", session.id))["mode"] == "plan"


async def test_identity_revocation_blocks_read_answer_and_resume(tmp_path):
    class MutableIdentities:
        valid = True

        def validate(self, actor):
            if not self.valid:
                raise ServiceError("unauthorized", 401)
            return actor

    async with interaction_env(tmp_path / "identity-revoke.sqlite") as (store, _, actor, session):
        identities = MutableIdentities()
        service = InteractionService(store, identities)
        request = await service.ask(scope(actor, session), text_question(), "request")
        identities.valid = False
        for operation in (
            service.state(actor, "alpha", session.id),
            service.read_request(actor, "alpha", session.id, request["id"]),
            service.answer(
                actor,
                "alpha",
                session.id,
                request["id"],
                AnswerRequest(
                    request_key="revoked-answer",
                    expected_revision=1,
                    answers=[Answer(question_id="q", text="denied")],
                ),
            ),
            service.resume(actor, "alpha", session.id, request["id"]),
        ):
            with pytest.raises(ServiceError, match="unauthorized"):
                await operation


async def test_answer_notifier_is_best_effort_and_repeats_on_idempotent_replay(tmp_path):
    notifications = []

    async def notify(request):
        saved = await service.read_request(actor, "alpha", session.id, request["id"])
        assert saved == request
        notifications.append(request)
        if len(notifications) == 1:
            raise RuntimeError("wake-up unavailable")

    async with interaction_env(tmp_path / "notify.sqlite") as (store, _, actor, session):
        service = InteractionService(store, on_answer=notify)
        request = await service.ask(scope(actor, session), text_question(), "ask")
        body = AnswerRequest(
            request_key="answer", expected_revision=1,
            answers=[Answer(question_id="q", text="done")],
        )
        result = await service.answer(actor, "alpha", session.id, request["id"], body)
        assert result["status"] == "answered"
        assert len(notifications) == 1 and notifications[0]["id"] == request["id"]
        replay = await service.answer(actor, "alpha", session.id, request["id"], body)
        assert replay == result
        assert len(notifications) == 2
        pending = await service.ask(scope(actor, session), text_question(), "ask-cancel")
        cancel = CancelRequest(request_key="cancel", expected_revision=1)
        cancelled = await service.cancel(actor, "alpha", session.id, pending["id"], cancel)
        assert cancelled["status"] == "cancelled"
        assert notifications[-1] == cancelled
        assert await service.cancel(actor, "alpha", session.id, pending["id"], cancel) == cancelled
        assert len(notifications) == 4


async def test_scope_check_blocks_interaction_write_before_persistence(tmp_path):
    async with interaction_env(tmp_path / "scope-check.sqlite") as (_, service, actor, session):
        def reject():
            raise ServiceError("permission_denied", 403)

        rejected_scope = SessionToolScope(
            actor, "alpha", session.id, "turn", "run", check=reject
        )

        async def resolve(_):
            return rejected_scope

        bundle = service.bundle(resolve)
        spec = next(item for item in bundle.specs if item.name == "ask_user")
        invocation = Invocation(
            run_id="run",
            call_id="call",
            span_id="span",
            step_index=0,
            name="ask_user",
            arguments_json=(
                '{"questions":[{"id":"q","prompt":"Question",'
                '"mode":"text","options":[],"allow_text":true}]}'
            ),
            tool_hash="hash",
        )
        with pytest.raises(ServiceError, match="permission_denied"):
            await spec.scoped_executor(
                invocation,
                AskUserArgs.model_validate_json(invocation.arguments_json),
                None,
            )
        assert (await service.state(actor, "alpha", session.id))["pending"] == []


async def local_executor(_):
    return {}


async def external_executor(_, __):
    return {}


def tool_spec(name, *, risk="read", source=None, capability=None, scoped_executor=None):
    return ToolSpec(
        name=name,
        parameters=EmptyArgs,
        executor=local_executor,
        output_schema=OutputContract({"type": "object"}).schema(),
        version="test/1",
        implementation_digest=fingerprint(name),
        risk=risk,
        source=source,
        external_executor=external_executor if source is not None else None,
        capability=capability,
        scoped_executor=scoped_executor,
    )


async def test_plan_mode_allows_only_host_trusted_mcp_reads(tmp_path):
    async with interaction_env(tmp_path / "trusted-policy.sqlite") as (
        store, service, actor, session
    ):
        current_scope = scope(actor, session)
        await service.enter_plan_mode(current_scope, EnterPlanModeArgs(), "enter-plan")
        trusted = tool_spec(
            "account__docs__read", source=ToolSource("mcp", "fixture", "read", "a" * 64)
        )
        assert not await service.permits(current_scope, trusted)
        assert await service.permits(
            current_scope, trusted, trusted_readonly_names={trusted.name}
        )
        configured = InteractionService(store, trusted_readonly_names={trusted.name})
        assert await configured.permits(current_scope, trusted)
        assert not await configured.permits(current_scope, trusted, trusted_readonly_names=())
        assert not await service.permits(
            current_scope, tool_spec("account__docs__write", risk="write", source=trusted.source),
            trusted_readonly_names={"account__docs__write"},
        )
        resource = tool_spec("read_mcp_resource")
        assert not await service.permits(current_scope, resource)
        assert await service.permits(
            current_scope, resource, trusted_readonly_names={resource.name}
        )


async def test_plan_mode_blocks_mcp_and_side_effects_but_allows_reads_and_questions(tmp_path):
    async with interaction_env(tmp_path / "policy.sqlite") as (_, service, actor, session):
        current_scope = scope(actor, session)
        await service.enter_plan_mode(current_scope, EnterPlanModeArgs(), "enter-plan")

        async def resolve(_):
            return current_scope

        question_spec = next(
            spec for spec in service.bundle(resolve).specs if spec.name == "ask_user"
        )
        read_spec = tool_spec("local_read")
        plan_update_spec = tool_spec(
            "plan_update",
            risk="write",
            capability="session_state",
            scoped_executor=local_executor,
        )
        mcp_read = tool_spec(
            "remote_read", source=ToolSource("mcp", "fixture", "read", "a" * 64)
        )
        mcp_write = tool_spec(
            "remote_write",
            risk="write",
            source=ToolSource("mcp", "fixture", "write", "b" * 64),
        )
        command_spec = tool_spec(
            "sandbox_command_run",
            risk="exec",
            capability="command",
            scoped_executor=local_executor,
        )
        assert await service.permits(current_scope, read_spec)
        assert await service.permits(current_scope, plan_update_spec)
        assert await service.permits(current_scope, question_spec)
        assert not await service.permits(current_scope, mcp_read)
        assert not await service.permits(current_scope, mcp_write)
        assert not await service.permits(current_scope, command_spec)


async def test_confirmed_plan_resume_is_fenced_after_supersede(tmp_path):
    async with interaction_env(tmp_path / "plan-resume-fence.sqlite") as (
        _, service, actor, session
    ):
        current_scope = scope(actor, session)
        await service.enter_plan_mode(current_scope, EnterPlanModeArgs(), "enter-plan")
        proposal = await service.exit_plan_mode(
            current_scope, ExitPlanModeArgs(plan="Execute this plan"), "proposal-key"
        )
        confirmed = await service.answer(
            actor,
            "alpha",
            session.id,
            proposal["id"],
            AnswerRequest(request_key="confirm-key", expected_revision=1, confirmed=True),
        )
        assert await service.resume(actor, "alpha", session.id, proposal["id"]) == confirmed

        await service.supersede(actor, "alpha", session.id, "new-message")
        with pytest.raises(ServiceError, match="input_revision_conflict"):
            await service.resume(actor, "alpha", session.id, proposal["id"])
        with pytest.raises(ServiceError, match="input_revision_conflict"):
            await service.exit_plan_mode(
                current_scope,
                ExitPlanModeArgs(plan="Execute this plan"),
                "proposal-key",
            )


async def test_closed_session_allows_only_exact_answer_replay(tmp_path):
    async with interaction_env(tmp_path / "closed-answer-replay.sqlite") as (
        store, service, actor, session
    ):
        request = await service.ask(scope(actor, session), text_question(), "ask-key")
        answer = AnswerRequest(
            request_key="answer-key",
            expected_revision=1,
            answers=[Answer(question_id="q", text="accepted")],
        )
        result = await service.answer(actor, "alpha", session.id, request["id"], answer)
        async with store.lock, store.engine.begin() as conn:
            await conn.execute(
                update(sessions)
                .where(sessions.c.id == session.id)
                .values(status="closed")
            )

        assert await service.answer(actor, "alpha", session.id, request["id"], answer) == result
        with pytest.raises(ServiceError, match="session_closed"):
            await service.answer(
                actor,
                "alpha",
                session.id,
                request["id"],
                answer.model_copy(update={"request_key": "new-answer-key"}),
            )
        with pytest.raises(ServiceError, match="key_conflict"):
            await service.answer(
                actor,
                "alpha",
                session.id,
                request["id"],
                answer.model_copy(update={
                    "answers": [Answer(question_id="q", text="changed payload")]
                }),
            )


async def test_interaction_bundle_scoped_executor_parks_and_replays_answer(tmp_path):
    async with interaction_env(tmp_path / "bundle.sqlite") as (_, service, actor, session):
        current_scope = scope(actor, session)

        async def resolve(_):
            return current_scope

        bundle = service.bundle(resolve)
        spec = next(item for item in bundle.specs if item.name == "ask_user")
        assert spec.scoped_executor is not None
        invocation = Invocation(
            run_id="run-1",
            call_id="call-1",
            span_id="span-1",
            step_index=0,
            name="ask_user",
            arguments_json=(
                '{"questions":[{"id":"q","prompt":"Type a response",'
                '"mode":"text","options":[],"allow_text":true}]}'
            ),
            tool_hash="tool-hash",
        )
        parsed = AskUserArgs.model_validate_json(invocation.arguments_json)
        with pytest.raises(RequestPending) as pending:
            await spec.scoped_executor(invocation, parsed, None)
        request = pending.value.request
        assert request["status"] == "pending"
        await service.answer(
            actor,
            "alpha",
            session.id,
            request["id"],
            AnswerRequest(
                request_key="answer",
                expected_revision=1,
                answers=[Answer(question_id="q", text="response")],
            ),
        )
        replayed = await spec.scoped_executor(invocation, parsed, None)
        assert replayed["id"] == request["id"] and replayed["status"] == "answered"
