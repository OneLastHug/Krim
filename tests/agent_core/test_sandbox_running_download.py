"""Authorized active-project binary reads keep page and lease snapshot bindings."""

from __future__ import annotations

import base64
import hashlib
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import update
from test_sandbox_bindings import new_project
from test_sandbox_control import managed

from krim_agent.contracts import ServiceError
from krim_agent.storage.sandbox_models import sandbox_projects


class BinaryBroker:
    def __init__(self, body: bytes):
        self.body = body
        self.digest = hashlib.sha256(body).hexdigest()
        self.token = [1, 2, len(body), 3, 4]
        self.calls = []
        self.change = None
        self.legacy = False
        self.bad_hash = False

    async def call(self, action, **arguments):
        assert action == "read"
        self.calls.append(arguments)
        offset = arguments["offset"]
        data = self.body[offset:offset + arguments["limit"]]
        result = {
            "path": arguments["path"], "offset": offset, "size": len(self.body),
            "eof": offset + len(data) == len(self.body), "sha256": self.digest,
            "data": base64.b64encode(data).decode(),
        }
        if not self.legacy:
            result["stat_token"] = list(self.token)
        if self.bad_hash:
            result["sha256"] = "0" * 64
        if self.change is not None:
            await self.change(len(self.calls))
        return result


async def state(store, project_id, **values):
    async def write(conn):
        await conn.execute(update(sandbox_projects).where(
            sandbox_projects.c.project_id == project_id,
        ).values(**values))
    await store._write(write)


@pytest.mark.asyncio
async def test_running_download_requires_explicit_flag_and_keeps_full_bytes(tmp_path):
    async with managed(tmp_path / "active.sqlite") as (store, _, service, _, actor, *_):
        project = await new_project(service, actor)
        selected = project["project_id"]
        await state(store, selected, status="running", lease_id="active-operation")
        body = bytes(range(256)) * 17000  # >4MiB, several pages, no host/container access
        broker = BinaryBroker(body)
        service.broker = broker
        with pytest.raises(ServiceError, match="workspace_busy"):
            await service.download_file(actor, selected, path="image.png")
        assert not broker.calls
        downloaded, digest = await service.download_file(
            actor, selected, path="image.png", allow_running=True,
        )
        assert downloaded == body and digest == broker.digest
        assert len(broker.calls) > 1
        assert "expected_stat" not in broker.calls[0]
        assert all(call["expected_stat"] == broker.token and call["expected_sha256"] == digest
                   for call in broker.calls[1:])


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["generation", "lease_id", "status"])
async def test_running_download_rejects_project_transition_during_page(tmp_path, changed):
    async with managed(tmp_path / "changed.sqlite") as (store, _, service, _, actor, *_):
        project = await new_project(service, actor)
        selected = project["project_id"]
        await state(store, selected, status="running", lease_id="active-operation")
        broker = BinaryBroker(b"x" * 100000)
        service.broker = broker

        async def change(index):
            if index == 1:
                await state(store, selected, **{changed: {
                    "generation": 1, "lease_id": "other-operation", "status": "ready",
                }[changed]})

        broker.change = change
        with pytest.raises(ServiceError, match="artifact_conflict"):
            await service.download_file(actor, selected, path="image.png", allow_running=True)
        assert len(broker.calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["stat", "legacy", "hash"])
async def test_running_download_rejects_unpinned_or_changed_content(tmp_path, failure):
    async with managed(tmp_path / "content.sqlite") as (store, _, service, _, actor, *_):
        project = await new_project(service, actor)
        selected = project["project_id"]
        await state(store, selected, status="running", lease_id="active-operation")
        broker = BinaryBroker(b"x" * 100000)
        service.broker = broker
        broker.legacy = failure == "legacy"
        broker.bad_hash = failure == "hash"
        if failure == "stat":
            async def change(index):
                if index == 1:
                    broker.token[-1] += 1
            broker.change = change
        with pytest.raises(ServiceError, match="artifact_conflict"):
            await service.download_file(actor, selected, path="image.png", allow_running=True)


@pytest.mark.asyncio
async def test_running_download_rechecks_authorization_before_return(tmp_path, monkeypatch):
    async with managed(tmp_path / "auth.sqlite") as (store, _, service, _, actor, *_):
        project = await new_project(service, actor)
        selected = project["project_id"]
        await state(store, selected, status="running", lease_id="active-operation")
        broker = BinaryBroker(b"image bytes")
        service.broker = broker

        async def revoke(index):
            monkeypatch.setattr(service, "_project_state", AsyncMock(
                side_effect=ServiceError("permission_denied", 403),
            ))
        broker.change = revoke
        with pytest.raises(ServiceError, match="permission_denied"):
            await service.download_file(actor, selected, path="image.png", allow_running=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["blocked", "restoring"])
async def test_running_download_does_not_read_unsettled_recovery_state(tmp_path, status):
    async with managed(tmp_path / "unsettled.sqlite") as (store, _, service, _, actor, *_):
        project = await new_project(service, actor)
        selected = project["project_id"]
        await state(store, selected, status=status, lease_id="operation")
        broker = BinaryBroker(b"x")
        service.broker = broker
        with pytest.raises(ServiceError, match="workspace_busy"):
            await service.download_file(actor, selected, path="image.png", allow_running=True)
        assert not broker.calls
