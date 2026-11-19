"""Turn-finish continuation port: one decision per finished turn, queued by the host.

``on_turn_finished`` is the single host-called decision point. It never runs a
model loop and never parks: it inspects the finished turn plus persisted goal
state and either enqueues exactly one internal follow-up turn through the
injected :class:`ContinuationQueue` or stops with a concrete reason. Internal
continuation turns keep the original user-turn binding of the goal; their text
is not a new user authorization and cannot create, replace, resume or cancel a
goal. The continue key is deterministic per (goal, finished turn), so restarts
and replays reuse the same idempotent queue protocol.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal, Protocol

from pydantic import Field

from krim_agent.contracts import StrictModel
from krim_agent.security import PrincipalContext

from .contracts import GoalSnapshot

CONTINUE_TURN_PREFIX = "goal-continue:"
RESUME_TURN_PREFIX = "goal-resume:"
INTERNAL_TURN_PREFIXES = (CONTINUE_TURN_PREFIX, RESUME_TURN_PREFIX)

StopReason = Literal[
    "goal_active",
    "no_goal",
    "goal_paused",
    "goal_blocked",
    "goal_completed",
    "goal_cancelled",
    "session_closed",
    "session_archived",
    "permission_denied",
    "user_stopped",
    "parked",
    "waiting_input",
    "approval_pending",
    "awaiting",
    "user_message_pending",
    "continuation_pending",
    "run_interrupted",
    "repeated_failure",
    "approval_expired",
    "queue_unavailable",
    "enqueue_failed",
    "already_finished",
    "superseded",
]

FAILURE_OUTCOMES = frozenset({"failed"})
TERMINAL_TURN_STATUSES = frozenset({"completed", "failed", "cancelled", "interrupted"})
# Terminal run codes that automatic continuation must never retry: re-enqueueing
# would ask for the same approval again instead of surfacing the decision to the
# owner. ``approval_denied`` is a user decision (plain stop); ``approval_expired``
# blocks the goal so the parked wait cannot silently loop.
PERMANENT_FAILURE_CODES = frozenset({"approval_expired", "approval_denied"})


class ContinuationRequest(StrictModel):
    """Idempotent descriptor for exactly one internal follow-up turn.

    The host submits this verbatim: ``turn_request_key`` is the TurnRequest
    request key (idempotent at SessionService), ``internal`` must be kept
    ``True``, and ``text`` is internal continuation material, never a new user
    authorization. ``origin_turn_id`` keeps the goal's original user binding.
    """

    version: Literal["goals/1"] = "goals/1"
    workspace_id: str
    session_id: str
    owner_id: str
    goal_id: str
    goal_revision: int = Field(ge=1)
    continue_key: str
    turn_request_key: str
    origin_turn_id: str
    origin_run_id: str
    source_turn_id: str | None = None
    source_run_id: str | None = None
    internal: Literal[True] = True
    reason: str
    text: str


class ContinuationDecision(StrictModel):
    version: Literal["goals/1"] = "goals/1"
    action: Literal["continue", "stop"]
    reason: StopReason
    goal_id: str | None = None
    goal_revision: int | None = None
    continue_key: str | None = None
    queued: bool = False
    request: ContinuationRequest | None = None


@dataclass(frozen=True)
class TurnFinishedContext:
    """Host-assembled terminal facts for one finished turn.

    ``actor`` is the live principal that owns the run; ``status``/``code`` come
    from the finished run, ``stop_requested``/``parked``/pending flags from the
    run/session state, and ``check`` revalidates the caller's authorization
    between and after persistence steps, mirroring tool scopes.
    """

    actor: PrincipalContext
    workspace_id: str
    session_id: str
    turn_id: str
    run_id: str
    status: Literal["completed", "failed", "aborted", "interrupted", "awaiting"]
    code: str | None = None
    stop_requested: bool = False
    parked: bool = False
    pending_interaction: bool = False
    approval_pending: bool = False
    check: Callable[[], None] = lambda: None


class ContinuationQueue(Protocol):
    """Host-owned single-slot enqueue; must be idempotent by ``turn_request_key``."""

    async def enqueue(self, request: ContinuationRequest) -> None: ...


def continuation_text(goal: GoalSnapshot) -> str:
    """Internal prompt for the follow-up turn.

    The prompt deliberately does not copy the goal body: the live goal snapshot
    (context projection port) carries the full objective, criteria and evidence
    there is no application-side truncation anywhere. A real model-window limit
    is handled by the context budget/compaction boundary, never by silently
    rewriting the goal in this text.
    """
    return "\n".join(
        [
            "内部续跑（宿主自动触发）：这不是新的用户消息，也不授予新的目标授权。",
            "继续推进当前目标；不要新建、替换、取消或恢复目标，也不要扩大范围。",
            f"当前目标：goal_id={goal.goal_id} revision={goal.revision}",
            "目标全文、完成条件与证据引用以会话目标快照为准；继续实际工作并用工具/测试验证。",
            "有新进展时用 goal_update action=progress 记录证据；完成条件均有证据时用 "
            "goal_update action=complete；无法继续时用 goal_update action=block 给出具体原因。",
        ]
    )
