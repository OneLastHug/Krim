"""Atomic repository grants and their host-only network-policy mapping."""

from __future__ import annotations

import time

from sqlalchemy import insert, select, update
from sqlalchemy.ext.asyncio import AsyncConnection

from krim_agent.contracts import ServiceError, fingerprint
from krim_agent.network.egress_policy import EgressViolation
from krim_agent.project_access.egress import ProjectEgressPolicy
from krim_agent.project_access.git import GitGrant
from krim_agent.project_access.types import NetworkGrant
from krim_agent.security import PrincipalContext
from krim_agent.storage.development_models import project_access_records as records
from krim_agent.storage.project_access import SQLiteAccessRepository
from krim_agent.storage.sandbox_models import sandbox_projects
from krim_agent.storage.sqlite import SQLiteStore


async def put_git_grant(
    store: SQLiteStore,
    actor: PrincipalContext,
    grant: GitGrant,
    network_policy_ref: str | None,
    *,
    expected_revision: int,
) -> int:
    access = SQLiteAccessRepository(store)
    access.require()
    if type(expected_revision) is not int or expected_revision < 0:
        raise ServiceError("revision_conflict")
    key = fingerprint(grant.repository)
    revision = expected_revision + 1
    document = grant.model_dump(mode="json")
    mapping = {
        "repository": grant.repository,
        "policy_id": network_policy_ref,
        "revision": revision,
        "revoked": grant.revoked,
    }

    async def write(conn: AsyncConnection) -> int:
        current_actor = await access._actor(conn, actor.principal_id)
        project = (
            (
                await conn.execute(
                    select(sandbox_projects).where(
                        sandbox_projects.c.project_id == grant.project_id,
                    )
                )
            )
            .mappings()
            .first()
        )
        if (
            current_actor is None
            or current_actor != actor
            or project is None
            or project["principal_id"] != grant.owner_id
            or actor.principal_id != grant.owner_id
        ):
            raise ServiceError("permission_denied", 403)
        current_actor.owns(project["workspace_id"], grant.owner_id, write=True, approve=True)
        if network_policy_ref is not None:
            network = await access._record(
                conn, grant.owner_id, grant.project_id, "network", network_policy_ref
            )
            if (
                not isinstance(network, NetworkGrant)
                or network.revoked
                or network.expires_at <= time.time()
            ):
                raise ServiceError("permission_denied", 403)
            try:
                ProjectEgressPolicy(network).validate_target(grant.repository)
            except EgressViolation:
                raise ServiceError("permission_denied", 403) from None
        elif (
            any(action in {"git.clone", "git.push"} for action in grant.actions)
            and not grant.revoked
        ):
            raise ServiceError("permission_denied", 403)
        for kind, value in (("git", document), ("git_network", mapping)):
            where = (
                records.c.project_id == grant.project_id,
                records.c.kind == kind,
                records.c.resource_id == key,
            )
            old = (await conn.execute(select(records).where(*where))).mappings().first()
            if (
                old is None
                and expected_revision != 0
                or old is not None
                and (old["revision"] != expected_revision or old["owner_id"] != grant.owner_id)
            ):
                raise ServiceError("revision_conflict")
            if old is None:
                await conn.execute(
                    insert(records).values(
                        project_id=grant.project_id,
                        kind=kind,
                        resource_id=key,
                        owner_id=grant.owner_id,
                        revision=revision,
                        document=value,
                        updated_at=time.time(),
                    )
                )
            else:
                await conn.execute(
                    update(records)
                    .where(*where)
                    .values(
                        revision=revision,
                        document=value,
                        updated_at=time.time(),
                    )
                )
        return revision

    async def confirm() -> int | None:
        async with store.lock, store.engine.connect() as conn:
            found = (
                (
                    await conn.execute(
                        select(records).where(
                            records.c.project_id == grant.project_id,
                            records.c.owner_id == grant.owner_id,
                            records.c.resource_id == key,
                            records.c.kind.in_(("git", "git_network")),
                        )
                    )
                )
                .mappings()
                .all()
            )
        if len(found) == 2 and all(row["revision"] == revision for row in found):
            docs = {row["kind"]: row["document"] for row in found}
            if docs == {"git": document, "git_network": mapping}:
                return revision
        return None

    return await store._eval_write(write, confirm)


async def git_network_policy(
    access: SQLiteAccessRepository,
    actor: PrincipalContext,
    project_id: str,
    repository: str,
) -> str | None:
    access.require()
    key = fingerprint(repository)
    async with access.store.lock, access.store.engine.connect() as conn:
        current = await access._actor(conn, actor.principal_id)
        found = (
            (
                await conn.execute(
                    select(records).where(
                        records.c.owner_id == actor.principal_id,
                        records.c.project_id == project_id,
                        records.c.resource_id == key,
                        records.c.kind.in_(("git", "git_network")),
                    )
                )
            )
            .mappings()
            .all()
        )
    if current != actor or len(found) != 2:
        raise ServiceError("permission_denied", 403)
    by_kind = {row["kind"]: row for row in found}
    grant = by_kind["git"]
    mapping = by_kind["git_network"]
    value = mapping["document"]
    if (
        mapping["revision"] != grant["revision"]
        or value.get("revision") != grant["revision"]
        or value.get("repository") != repository
        or value.get("revoked") is not False
        or grant["document"].get("revoked") is not False
        or grant["document"].get("repository") != repository
        or grant["document"].get("expires_at", 0) <= time.time()
    ):
        raise ServiceError("permission_denied", 403)
    policy_id = value.get("policy_id")
    if policy_id is not None and not isinstance(policy_id, str):
        raise ServiceError("facts_corrupt")
    return policy_id
