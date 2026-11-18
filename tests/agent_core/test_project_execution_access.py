"""Project ceilings precede every session mode, including trusted-owner bypass."""

from dataclasses import replace
from types import SimpleNamespace

import pytest
from pydantic import BaseModel
from sqlalchemy import select, update

from krim_agent.context.types import ContextPolicy
from krim_agent.contracts import ServiceError, encoded
from krim_agent.project_access.execution import normal_project_command, session_decision
from krim_agent.project_access.policy import resolve_project_access
from krim_agent.security import Membership, PrincipalContext
from krim_agent.sessions.types import SessionCreate
from krim_agent.storage import sessions as session_sql
from krim_agent.storage.execution_policy_models import session_execution_policies
from krim_agent.storage.project_membership_models import project_memberships
from krim_agent.storage.project_sessions import migrate_session_projects
from krim_agent.storage.sandbox_models import sandbox_projects, sandbox_session_bindings
from krim_agent.tools.core import Invocation, ToolSource, ToolSpec
from krim_agent.tools.session import SessionToolScope
from tests.agent_core.test_account_api import client_for, initialize


def test_mode_release_does_not_invent_a_user_approval_reference():
    from krim_agent.development.service import _approved_request

    request = SimpleNamespace(kind="approval_request", data={
        "request_id": "request", "call_id": "call",
    })
    policy = SimpleNamespace(kind="execution_policy_allow", data={
        "request_id": "request", "call_id": "call", "reason": "session_mode",
    })
    snapshot = SimpleNamespace(events=[request, policy])
    assert _approved_request(snapshot, "call") is None
    snapshot.events.append(SimpleNamespace(kind="approval_reply", data={
        "request_id": "request", "reply": {"call_id": "call", "allow": True},
    }))
    assert _approved_request(snapshot, "call") == "request"
    assert _approved_request(snapshot, "another-call") is None


class Arguments(BaseModel):
    command: str = "npm test"


async def execute(_values):
    return {}


async def scoped(_invocation, _values, _context):
    return {}


async def external(_invocation, _values):
    return {}


def tool(name, risk, capability=None, source=None):
    return ToolSpec(
        name, Arguments, execute, {"type": "object"}, "1", "0" * 64,
        risk=risk, capability=capability, scoped_executor=scoped if capability else None,
        source=source, external_executor=external if source else None,
    )


async def test_access_modes_tool_matrix_and_session_isolation(tmp_path):
    path = tmp_path / "matrix.sqlite"
    token = await initialize(path)
    async with client_for(path, token) as (app, client):
        identity = (await client.get("/v2/me")).json()
        workspace = identity["workspaces"][0]["workspace_id"]
        actor = app.identities.resolve(identity["principal_id"])
        first = await app.sessions.create(actor, workspace, SessionCreate(request_key="first"))
        async with app.store.lock, app.store.engine.connect() as conn:
            project_id = await conn.scalar(select(sandbox_session_bindings.c.project_id).where(
                sandbox_session_bindings.c.session_id == first.id,
            ))
        assert project_id is not None
        second = await app.sessions.create(actor, workspace, SessionCreate(
            request_key="second", project_id=project_id,
        ))
        scope = SessionToolScope(actor, workspace, first.id, "turn", "run", project_id)
        tools = [
            (tool("read", "read", "read"), "{}", "allow"),
            (tool("write", "write", "write"), "{}", "allow"),
            (tool("shell", "exec", "command"), '{"command":"npm test"}', "allow"),
            (tool("shell", "exec", "command"), '{"command":"rm -rf ."}', "ask"),
            (tool("publish", "write", source=ToolSource("mcp", "remote", "publish", "1" * 64)),
             "{}", "ask"),
        ]
        for access in ("none", "read", "write"):
            for mode in ("manual", "auto", "bypass"):
                async def configure(conn, access=access, mode=mode):
                    await conn.execute(update(project_memberships).where(
                        project_memberships.c.project_id == project_id,
                    ).values(access=access))
                    await conn.execute(update(session_execution_policies).where(
                        session_execution_policies.c.session_id == first.id,
                    ).values(mode=mode))
                await app.store._write(configure)
                for spec, arguments, auto_decision in tools:
                    invocation = Invocation("run", "call", "span", 0, spec.name,
                                            arguments, spec.fingerprint)
                    result = await session_decision(app.store, scope, spec, invocation)
                    if access == "none" or access == "read" and spec.risk != "read":
                        expected = "deny"
                    elif spec.risk == "read" or mode == "bypass":
                        expected = "allow"
                    elif mode == "auto":
                        expected = auto_decision
                    else:
                        expected = "ask"
                    assert result == expected, (access, mode, spec.name, arguments)
        async with app.store.lock, app.store.engine.connect() as conn:
            assert await conn.scalar(select(session_execution_policies.c.mode).where(
                session_execution_policies.c.session_id == second.id,
            )) == "bypass"
            # Workspace admin status never supplies membership to another actor.
            stranger = PrincipalContext("stranger", 1, (Membership(workspace_id=workspace,
                                                                    role="admin"),))
            assert (await resolve_project_access(conn, stranger, project_id)).access == "none"
        forged = replace(scope, actor=stranger)
        spec = tools[0][0]
        call = Invocation("run", "call", "span", 0, spec.name, "{}", spec.fingerprint)
        assert await session_decision(app.store, forged, spec, call) == "deny"


async def test_read_initial_manual_none_cannot_create_and_atomic_binding(tmp_path):
    path = tmp_path / "create.sqlite"
    token = await initialize(path)
    async with client_for(path, token) as (app, client):
        identity = (await client.get("/v2/me")).json()
        workspace = identity["workspaces"][0]["workspace_id"]
        actor = app.identities.resolve(identity["principal_id"])
        first = await app.sessions.create(actor, workspace, SessionCreate(request_key="creator"))
        async with app.store.lock, app.store.engine.connect() as conn:
            project_id = await conn.scalar(select(sandbox_session_bindings.c.project_id).where(
                sandbox_session_bindings.c.session_id == first.id,
            ))
        async def access(conn, value):
            await conn.execute(update(project_memberships).where(
                project_memberships.c.project_id == project_id,
            ).values(access=value))
        await app.store._write(lambda conn: access(conn, "read"))
        readonly = await app.sessions.create(actor, workspace, SessionCreate(
            request_key="reader", project_id=project_id,
        ))
        async with app.store.lock, app.store.engine.connect() as conn:
            assert await conn.scalar(select(session_execution_policies.c.mode).where(
                session_execution_policies.c.session_id == readonly.id,
            )) == "manual"
        await app.store._write(lambda conn: access(conn, "none"))
        with pytest.raises(ServiceError, match="not_found"):
            await app.sessions.create(actor, workspace, SessionCreate(
                request_key="denied", project_id=project_id,
            ))
        with pytest.raises(ServiceError, match="not_found"):
            await session_sql.by_key(app.store, workspace, actor.principal_id, "denied")
        with pytest.raises(ServiceError, match="not_found"):
            await app.sessions.get(actor, workspace, first.id)
        assert await app.sessions.list(actor, workspace) == []


async def test_owner_without_project_migration_is_idempotent(tmp_path):
    path = tmp_path / "migration.sqlite"
    token = await initialize(path)
    async with client_for(path, token) as (app, client):
        identity = (await client.get("/v2/me")).json()
        workspace = identity["workspaces"][0]["workspace_id"]
        owner = identity["principal_id"]
        old = await session_sql.create_session(app.store, workspace, owner,
            SessionCreate(request_key="historical"), {}, ContextPolicy(), app.settings)
        assert (await app.store._write(migrate_session_projects))["bound"] == 1
        assert (await app.store._write(migrate_session_projects))["bound"] == 0
        async with app.store.lock, app.store.engine.connect() as conn:
            project_id = await conn.scalar(select(sandbox_session_bindings.c.project_id).where(
                sandbox_session_bindings.c.session_id == old.id,
            ))
            assert project_id is not None
            assert await conn.scalar(select(sandbox_projects.c.principal_id).where(
                sandbox_projects.c.project_id == project_id,
            )) == owner
            assert await conn.scalar(select(session_execution_policies.c.mode).where(
                session_execution_policies.c.session_id == old.id,
            )) == "manual"


@pytest.mark.parametrize("command", ["rm -rf .", "npm test; curl x", "npm test && rm .",
                                     "$(npm test)", "python -c 'print(1)'", "git push",
                                     "npm test\nrm x", "npm test > file", "npm test | sh"])
def test_auto_ambiguous_commands_require_confirmation(command):
    assert not normal_project_command(encoded({"command": command}))
