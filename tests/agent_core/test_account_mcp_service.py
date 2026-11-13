"""Owner-scoped personal MCP service and immutable turn binding tests."""

from contextlib import asynccontextmanager

import pytest
from sqlalchemy import select

from krim_agent.account_mcp.secrets import AuthenticatedSecretVault, MasterKeyring
from krim_agent.account_mcp.service import (
    AccountMCPService,
    ConnectionCreate,
    ConnectionDelete,
    ConnectionUpdate,
    SessionMCPSelection,
    SessionConnection,
)
from krim_agent.accounts.service import IdentityService
from krim_agent.config import Settings
from krim_agent.context.types import ContextPolicy
from krim_agent.contracts import ServiceError
from krim_agent.security import PrincipalContext
from krim_agent.sessions.types import SessionCreate, TurnRequest
from krim_agent.storage import sessions as session_sql
from krim_agent.storage.mcp_models import mcp_connections, session_mcp, turn_mcp
from krim_agent.storage.sqlite import SQLiteStore


@asynccontextmanager
async def managed(path):
    store = await SQLiteStore.open(path)
    identities = await IdentityService.open(store)
    boot = await identities.bootstrap("admin", request_key="bootstrap")
    admin = identities.authenticate("Bearer " + boot.token)
    try:
        yield store, identities, admin
    finally:
        await identities.close()
        await store.close()


async def create_owner(identities: IdentityService, admin: PrincipalContext, name: str):
    account = await identities.create_account(admin, {"display_name": name}, request_key="account-" + name)
    issued = await identities.issue_key(
        admin, account.account.principal_id, {}, request_key="key-" + name
    )
    return identities.authenticate("Bearer " + issued.token)


async def create_session(store: SQLiteStore, owner: PrincipalContext, workspace: str):
    return await session_sql.create_session(
        store,
        workspace,
        owner.principal_id,
        SessionCreate(request_key="session"),
        {"key": "demo"},
        ContextPolicy(),
        Settings(),
    )


def body(request_key: str = "create", token: str = "secret-marker") -> ConnectionCreate:
    return ConnectionCreate(
        request_key=request_key,
        name="docs",
        url="https://docs.example/mcp",
        allowed_hosts=("docs.example",),
        selected_tools=("search",),
        readonly_tools=("search",),
        credentials={"Authorization": token},
    )


@pytest.mark.asyncio
async def test_crud_is_owner_scoped_and_credentials_never_returned(tmp_path):
    async with managed(tmp_path / "owner.sqlite") as (store, identities, admin):
        owner = await create_owner(identities, admin, "mcp-owner")
        other = await create_owner(identities, admin, "mcp-other")
        vault = AuthenticatedSecretVault(MasterKeyring.create(bytes(range(32))))
        service = AccountMCPService(store, identities, vault)

        created = await service.create_connection(owner, body())
        assert created["credential_configured"] is True
        assert "secret-marker" not in str(created)
        listed = await service.list_connections(owner)
        assert len(listed) == 1 and listed[0]["credential_configured"] is True
        assert "secret-marker" not in str(listed)
        assert await service.list_connections(other) == []

        with pytest.raises(ServiceError, match="not_found"):
            await service.update_connection(
                other,
                created["id"],
                ConnectionUpdate(
                    request_key="foreign-update",
                    expected_revision=1,
                    name="docs",
                    url="https://docs.example/mcp",
                    allowed_hosts=("docs.example",),
                    selected_tools=("search",),
                    readonly_tools=("search",),
                ),
            )
        with pytest.raises(ServiceError, match="key_conflict"):
            await service.create_connection(
                owner, body(request_key="create", token="changed-secret")
            )

        updated = await service.update_connection(
            owner,
            created["id"],
            ConnectionUpdate(
                request_key="rotate-credential",
                expected_revision=1,
                name="docs",
                url="https://docs.example/mcp",
                allowed_hosts=("docs.example",),
                selected_tools=("search",),
                readonly_tools=("search",),
                credentials={"Authorization": "rotated-secret"},
            ),
        )
        assert updated["revision"] == 2
        replayed = await service.update_connection(
            owner,
            created["id"],
            ConnectionUpdate(
                request_key="rotate-credential",
                expected_revision=1,
                name="docs",
                url="https://docs.example/mcp",
                allowed_hosts=("docs.example",),
                selected_tools=("search",),
                readonly_tools=("search",),
                credentials={"Authorization": "rotated-secret"},
            ),
        )
        assert replayed == updated
        assert await service.list_connections(owner)
        with pytest.raises(ServiceError, match="revision_conflict"):
            await service.delete_connection(
                owner,
                created["id"],
                ConnectionDelete(request_key="stale-delete", expected_revision=1),
            )


@pytest.mark.asyncio
async def test_session_selection_is_owner_only_and_turn_snapshot_is_immutable(tmp_path):
    async with managed(tmp_path / "snapshot.sqlite") as (store, identities, admin):
        owner = await create_owner(identities, admin, "snapshot-owner")
        vault = AuthenticatedSecretVault(MasterKeyring.create(bytes(range(32, 64))))
        service = AccountMCPService(store, identities, vault)
        created = await service.create_connection(owner, body())
        personal = await identities.profile(owner)
        workspace = next(ref.workspace_id for ref in personal.workspaces if ref.personal)
        session = await create_session(store, owner, workspace)

        selection = SessionMCPSelection(
            request_key="select",
            expected_revision=0,
            connections=(SessionConnection(connection_id=created["id"], revision=1),),
        )
        selected = await service.session_selection(owner, workspace, session.id, selection)
        assert selected["revision"] == 1

        actor = identities.validate(owner)
        turn = await session_sql.submit(
            store,
            session.id,
            TurnRequest(request_key="turn", text="find docs", expected_revision=1),
            Settings(),
            actor=actor,
            account_mcp_service=service,
        )
        assert turn.status == "queued"
        configs = await service.configurations_for_turn(turn.id)
        assert len(configs) == 1
        assert configs[0].headers["Authorization"].get_secret_value() == "secret-marker"

        await service.update_connection(
            owner,
            created["id"],
            ConnectionUpdate(
                request_key="update-after-admission",
                expected_revision=1,
                name="docs-new",
                url="https://new.example/mcp",
                allowed_hosts=("new.example",),
                selected_tools=("query",),
                readonly_tools=("query",),
                credentials={"Authorization": "new-secret"},
            ),
        )
        frozen = await service.configurations_for_turn(turn.id)
        assert frozen[0].url == "https://docs.example/mcp"
        assert frozen[0].headers["Authorization"].get_secret_value() == "secret-marker"

        async with store.lock, store.engine.begin() as conn:
            persisted = (await conn.execute(
                select(turn_mcp).where(turn_mcp.c.turn_id == turn.id)
            )).mappings().one()
            assert persisted["fingerprint"]
            assert "secret-marker" not in str(persisted["snapshot"])
            row = (await conn.execute(
                select(mcp_connections).where(mcp_connections.c.id == created["id"])
            )).mappings().one()
            assert "secret-marker" not in str(row["credential_envelope"])
            assert (await conn.execute(
                select(session_mcp).where(session_mcp.c.session_id == session.id)
            )).mappings().one()["revision"] == 1
