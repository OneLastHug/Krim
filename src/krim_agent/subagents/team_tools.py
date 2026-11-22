"""Team board tools: host-resolved caller identity, model-visible board changes.

Five fixed tools (team_create_task/team_list_tasks/team_claim_task/
team_update_task/team_reassign_task) call the real :class:`TeamService`. The
bundle resolves the host Caller via ``subagents._caller(invocation)`` and passes
``caller.tree["tree_id"]`` / ``caller.member["agent_id"]`` to the service;
models never supply caller, tree or owner authority. The only target-owner
field is ``target`` on reassign, validated inside the service. CAS and
dependency failures return ``ok=false`` with ``code``/``accepted``/``action``
so the model can correct; ``storage_unavailable``/``cancelled`` propagate.
Board changes never wake workers: these tools only call TeamService.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, cast

from pydantic import BaseModel, Field, JsonValue, field_validator

from krim_agent.contracts import ServiceError, StrictModel, fingerprint
from krim_agent.tools.core import Decision, Invocation, ToolSpec
from krim_agent.tools.session import SessionScopeResolver, ToolBundle

from .errors import SubagentError
from .team import (
    TeamEvidence,
    TeamTaskClaim,
    TeamTaskCreate,
    TeamTaskQuery,
    TeamTaskReassign,
    TeamTaskUpdate,
)
from .types import KEY_PATTERN, MAX_KEY_LENGTH

if TYPE_CHECKING:
    from .service import Caller

TeamStatus = Literal[
    "open", "claimed", "in_progress", "blocked", "completed", "failed", "cancelled",
]
UpdateStatus = Literal["in_progress", "completed", "failed", "cancelled", "open"]


async def _unavailable(parameters: BaseModel) -> JsonValue:
    raise ServiceError("workspace_contract_required")


class TeamCreateTask(StrictModel):
    """Create a board task; the replay key comes from the host invocation."""

    goal: str = Field(min_length=1, max_length=4000)
    dependencies: tuple[str, ...] = Field(default=(), max_length=32)
    source_task_id: str | None = Field(default=None, max_length=36)
    source_run_id: str | None = Field(default=None, max_length=36)

    @field_validator("goal")
    @classmethod
    def meaningful(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("goal is empty")
        value.encode("utf-8")
        return value


class TeamListTasks(StrictModel):
    """List this tree's board; owner accepts a member id or ``me``."""

    owner: str | None = Field(default=None, max_length=64)
    status: TeamStatus | None = None


class TeamClaimTask(StrictModel):
    task_id: str = Field(min_length=1, max_length=36)
    expected_revision: int = Field(ge=1)


class TeamEvidenceItem(StrictModel):
    kind: Literal["artifact", "check"]
    body: dict[str, JsonValue]
    request_key: str | None = Field(
        default=None, min_length=1, max_length=MAX_KEY_LENGTH, pattern=KEY_PATTERN
    )


class TeamUpdateTask(StrictModel):
    """Start/complete/fail/cancel, coordinator re-planning or evidence attach."""

    task_id: str = Field(min_length=1, max_length=36)
    expected_revision: int = Field(ge=1)
    expected_ownership_version: int | None = Field(default=None, ge=1)
    status: UpdateStatus | None = None
    goal: str | None = Field(default=None, min_length=1, max_length=4000)
    dependencies: tuple[str, ...] | None = Field(default=None, max_length=32)
    evidence: tuple[TeamEvidenceItem, ...] | None = Field(default=None, max_length=8)

    @field_validator("goal")
    @classmethod
    def meaningful(cls, value: str | None) -> str | None:
        if value is None:
            return None
        if not value.strip():
            raise ValueError("goal is empty")
        value.encode("utf-8")
        return value


class TeamReassignTask(StrictModel):
    """Coordinator transfer; ``target`` is a member id or path, omitted returns
    the task to the board. Authority (root or owner ancestor) is validated by
    TeamService from the database, never from this request."""

    task_id: str = Field(min_length=1, max_length=36)
    expected_revision: int = Field(ge=1)
    expected_ownership_version: int | None = Field(default=None, ge=1)
    target: str | None = Field(default=None, max_length=256)


def _invocation_key(invocation: Invocation, *parts: object) -> str:
    """Host-derived replay key: the same tool call replays to the same key."""
    return "team:" + fingerprint(
        {"run": invocation.run_id, "call": invocation.call_id, "p": list(parts)}
    )


def bundle(team: Any, subagents: Any, resolve: SessionScopeResolver) -> ToolBundle:
    """Assemble the five board tools over a real TeamService.

    ``team`` is the TeamService; ``subagents`` is the SubagentService used only
    for host caller resolution (``_caller``) and member target lookup
    (``_target``); ``resolve`` is the session scope resolver for admission.
    """
    declarations: tuple[tuple[str, type[BaseModel], str, str], ...] = (
        (
            "team_create_task",
            TeamCreateTask,
            "parallel",
            "Create a task on this tree's shared board with a goal and optional same-tree "
            "dependency task ids. Returns the task with its revision: read the board before "
            "every state change and send that revision as expected_revision. Creation does "
            "not assign work and does not wake anyone; claim it or reassign it explicitly.",
        ),
        (
            "team_list_tasks",
            TeamListTasks,
            "parallel",
            "List this tree's task board with status, owner, dependencies (with their "
            "statuses), revision, ownership_version and evidence. Use owner=me for your own "
            "tasks. Always refresh with this before claiming or updating: expected_revision "
            "from a stale read is rejected. A task whose dependency failed shows blocked.",
        ),
        (
            "team_claim_task",
            TeamClaimTask,
            "parallel",
            "Claim an open task you can run. All dependencies must already be completed. "
            "Send the revision you read as expected_revision; a rejected claim means another "
            "member moved first - re-list and pick different work, do not retry blindly.",
        ),
        (
            "team_update_task",
            TeamUpdateTask,
            "parallel",
            "Change one task: start (claimed to in_progress), complete, fail, or coordinator "
            "actions (cancel, edit goal/dependencies, reopen blocked by replacing "
            "dependencies). Owner-side changes must carry expected_ownership_version from "
            "your last read. Completion requires attached artifact or check evidence and is "
            "never automatic; execution success alone does not complete a task.",
        ),
        (
            "team_reassign_task",
            TeamReassignTask,
            "parallel",
            "Coordinator transfer of a task to target (member id or path), or omit target to "
            "return it to the board. Allowed for the root coordinator or an ancestor of the "
            "current owner; carry expected_ownership_version for an owned task. The old "
            "owner keeps their recorded artifacts but can no longer complete the task.",
        ),
    )
    methods = {name: (parameters, concurrency) for name, parameters, concurrency, _ in declarations}

    async def policy(invocation: Invocation) -> Decision:
        if invocation.name not in methods:
            return "deny"
        scope = await resolve(invocation)
        scope.check()
        return "allow" if scope.run_id == invocation.run_id else "deny"

    async def execute(invocation: Invocation, parsed: BaseModel, context: object) -> JsonValue:
        scope = await resolve(invocation)
        scope.check()
        declaration = methods.get(invocation.name)
        if declaration is None or not isinstance(parsed, declaration[0]):
            raise ServiceError("invalid_input", 422)
        try:
            caller: Caller = await subagents._caller(invocation)
            tree_id = caller.tree["tree_id"]
            caller_id = caller.member["agent_id"]
            if invocation.name == "team_create_task" and isinstance(parsed, TeamCreateTask):
                result = await team.create(
                    TeamTaskCreate(
                        tree_id=tree_id,
                        caller_id=caller_id,
                        goal=parsed.goal,
                        dependencies=parsed.dependencies,
                        source_task_id=parsed.source_task_id,
                        source_run_id=parsed.source_run_id,
                        request_key=_invocation_key(invocation, "create"),
                    )
                )
            elif invocation.name == "team_list_tasks" and isinstance(parsed, TeamListTasks):
                result = {
                    "tasks": await team.list(
                        TeamTaskQuery(
                            tree_id=tree_id,
                            caller_id=caller_id,
                            owner_id=caller_id if parsed.owner == "me" else parsed.owner,
                            status=parsed.status,
                        )
                    )
                }
            elif invocation.name == "team_claim_task" and isinstance(parsed, TeamClaimTask):
                result = await team.claim(
                    TeamTaskClaim(
                        tree_id=tree_id,
                        task_id=parsed.task_id,
                        caller_id=caller_id,
                        expected_revision=parsed.expected_revision,
                    )
                )
            elif invocation.name == "team_update_task" and isinstance(parsed, TeamUpdateTask):
                result = await team.update(
                    TeamTaskUpdate(
                        tree_id=tree_id,
                        task_id=parsed.task_id,
                        caller_id=caller_id,
                        expected_revision=parsed.expected_revision,
                        expected_ownership_version=parsed.expected_ownership_version,
                        status=parsed.status,
                        goal=parsed.goal,
                        dependencies=parsed.dependencies,
                        evidence=(
                            tuple(
                                TeamEvidence(
                                    kind=item.kind,
                                    body=item.body,
                                    request_key=item.request_key
                                    or _invocation_key(invocation, "evidence", index),
                                )
                                for index, item in enumerate(parsed.evidence)
                            )
                        )
                        if parsed.evidence
                        else None,
                    )
                )
            elif invocation.name == "team_reassign_task" and isinstance(parsed, TeamReassignTask):
                target_id = (
                    (await subagents._target(caller, parsed.target))["agent_id"]
                    if parsed.target is not None
                    else None
                )
                result = await team.reassign(
                    TeamTaskReassign(
                        tree_id=tree_id,
                        task_id=parsed.task_id,
                        caller_id=caller_id,
                        expected_revision=parsed.expected_revision,
                        expected_ownership_version=parsed.expected_ownership_version,
                        new_owner_id=target_id,
                    )
                )
            else:
                raise ServiceError("invalid_input", 422)
        except SubagentError as error:
            if error.code == "storage_unavailable":
                raise
            return cast(JsonValue, {"ok": False, **error.payload()})
        except ServiceError as error:
            if error.code in {"storage_unavailable", "cancelled"}:
                raise
            return {"ok": False, "accepted": False, "code": error.code}
        except ValueError:
            return {
                "ok": False,
                "accepted": False,
                "code": "invalid_input",
                "action": "Correct the request fields and retry.",
            }
        return cast(JsonValue, {"ok": True, **result})

    digest = hashlib.sha256()
    root = Path(__file__).parent
    for path in (
        root / "team.py",
        Path(__file__),
        root.parent / "storage" / "subagent_team_models.py",
        root / "types.py",
    ):
        digest.update(path.read_bytes())
    implementation = digest.hexdigest()
    specs = tuple(
        ToolSpec(
            name=name,
            parameters=parameters,
            executor=_unavailable,
            output_schema={"type": "object"},
            version="team/1",
            implementation_digest=implementation,
            scoped_executor=execute,
            capability="session_read",
            risk="read",
            concurrency=cast(Any, concurrency),
            description=description,
        )
        for name, parameters, concurrency, description in declarations
    )
    return ToolBundle(specs=specs, policy=policy)
