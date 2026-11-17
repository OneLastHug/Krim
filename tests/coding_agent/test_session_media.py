"""Real SQLite media objects, session projection recovery and SDK MIME wire."""

from __future__ import annotations

import base64
import dataclasses
import hashlib
import shutil
from contextlib import asynccontextmanager

import pytest
from sqlalchemy import select, update

from krim_agent.config import Settings
from krim_agent.context.types import ContextPolicy
from krim_agent.contracts import Message, RunRequest, ServiceError, encoded
from krim_agent.media.artifacts import MEDIA_TABLES, SQLiteMediaArtifacts
from krim_agent.media.ffmpeg import FFmpegImageTransform
from krim_agent.media.formats import inspect_image
from krim_agent.media.references import MediaSource
from krim_agent.media.service import ImageCapabilities
from krim_agent.runtime.session_media import SessionMediaHost
from krim_agent.security import Membership, PrincipalContext
from krim_agent.sessions.types import SessionCreate, TurnRequest
from krim_agent.storage import sessions as session_sql
from krim_agent.storage.models import metadata, run_events, sessions, turns
from krim_agent.storage.sqlite import SQLiteStore
from krim_agent.tools.session import SessionToolScope
from tests.coding_agent.test_development_model_media import _image, _provider
from tests.coding_agent.test_view_image import GIF, PNG, material


class Host:
    def __init__(self, scope):
        self.scope = scope
        self.calls = []

    async def by_run(self, run_id, check=lambda: None):
        self.calls.append(run_id)
        check()
        if run_id != self.scope.run_id:
            raise ServiceError("permission_denied")
        self.scope.check()
        return self.scope

    async def resolve(self, invocation):
        return await self.by_run(invocation.run_id)


@asynccontextmanager
async def environment(tmp_path):
    store = await SQLiteStore.open(tmp_path / "media.sqlite")
    try:
        async with store.lock, store.engine.begin() as conn:
            await conn.run_sync(lambda sync: metadata.create_all(sync, tables=list(MEDIA_TABLES)))
        actor = PrincipalContext("owner", 1, (Membership(workspace_id="media-test"),))
        session = await session_sql.create_session(
            store, "media-test", actor.principal_id, SessionCreate(request_key="session"),
            {"key": "demo"}, ContextPolicy(), Settings(),
        )
        submitted = await session_sql.submit(
            store, session.id, TurnRequest(request_key="turn", text="inspect", expected_revision=0),
            Settings(),
        )
        run, _ = await store.begin(RunRequest(text="inspect", request_key="media-run"), {})
        async with store.lock, store.engine.begin() as conn:
            await conn.execute(
                update(turns).where(turns.c.id == submitted.id).values(run_id=run.id)
            )
        scope = SessionToolScope(actor, "media-test", session.id, submitted.id, run.id)
        host = Host(scope)

        async def capabilities(current):
            return ImageCapabilities(True)

        artifacts = SQLiteMediaArtifacts(store)
        media = SessionMediaHost(store, host, capabilities=capabilities, artifacts=artifacts)
        yield store, host, media, artifacts, scope
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_mcp_authorized_original_persists_and_is_not_a_host_path(tmp_path):
    async with environment(tmp_path) as (store, host, media, artifacts, scope):
        result = await media.media_sink(scope, "server", {
            "server": "server", "uri": "file:///private/provider/image",
            "configuration_digest": "a" * 64,
            "contents": [{"mimeType": "image/gif",
                          "blob": base64.b64encode(GIF).decode()}],
        })
        assert result["status"] == "stored"
        reference = result["images"][0]["reference"]
        value = await artifacts.read(scope, reference)
        assert value.data == GIF and value.reference.project_id is None
        # A new service instance uses the persisted original, not a process cache.
        restored = SQLiteMediaArtifacts(store)
        assert (await restored.read(scope, reference)).data == GIF
        viewed = await media.service.view(scope, MediaSource(reference=reference))
        assert viewed["status"] == "ready" and viewed["image"]["media_type"] == "image/gif"
        other_actor = dataclasses.replace(scope.actor, principal_id="other")
        other = dataclasses.replace(scope, actor=other_actor)
        with pytest.raises(ServiceError, match="permission_denied"):
            await artifacts.read(other, reference)
        assert host.calls


@pytest.mark.asyncio
async def test_missing_authorized_storage_explicitly_unavailable(tmp_path):
    async with environment(tmp_path) as (store, host, media, artifacts, scope):
        disabled = SessionMediaHost(store, host, capabilities=media.service.capabilities)
        result = await disabled.media_sink(scope, "server", {
            "server": "server", "configuration_digest": "a" * 64, "contents": [
                {"mimeType": "image/png", "blob": base64.b64encode(PNG).decode()},
            ],
        })
        assert result == {"status": "unavailable",
                          "reason": "authorized_media_storage_unavailable"}
        with pytest.raises(ServiceError):
            await disabled.read(scope, MediaSource(reference="host-path-is-never-read"))


@pytest.mark.asyncio
async def test_unified_screenshot_and_view_projection_restores_trusted_selection(tmp_path):
    async with environment(tmp_path) as (store, host, media, artifacts, scope):
        host.scope = scope = dataclasses.replace(scope, project_id="project")
        saved = await artifacts.save(scope, GIF, "image/gif")
        screenshot = {"run_id": scope.run_id, "project_id": "project",
                      "reference": "image-screen", "sha256": hashlib.sha256(PNG).hexdigest(),
                      "size": len(PNG)}

        class Screenshots:
            calls = 0

            async def read_browser_artifact(self, actor, project_id, run_id, reference):
                self.calls += 1
                assert project_id == "project" and reference == "image-screen"
                return PNG, {"sha256": screenshot["sha256"], "size": len(PNG)}

        media.screenshots = Screenshots()
        media.include_screenshots = True
        tool_results = [
            ("view-event", "view_image", {"status": "ready", "frame": None,
                                       "media_ref": saved.reference.model_dump(mode="json")}),
            ("screen-event", "browser_screenshot", screenshot),
        ]
        for event_id, name, value in tool_results:
            await store.append(scope.run_id, "tool_end", {
                "name": name, "status": "ok", "result_available": True, "value": value,
                "provider_call_id": event_id + "-message",
            }, event_id=event_id)
        runner = media.for_run(scope.run_id)
        history = [Message(role="user", content="inspect current image context")]
        history.append(Message(role="assistant", content="", tool_calls=[
            {"id": event_id + "-message", "name": name, "arguments": {}}
            for event_id, name, _value in tool_results
        ]))
        history.extend(Message(role="tool", call_id=event_id + "-message", content=encoded(value))
                       for event_id, _name, value in tool_results)
        projected = await runner.project(history, request_id="request", message_id="message")
        assert len(projected[-1].images) == 2
        assert [image.media_type for image in projected[-1].images] == ["image/gif", "image/png"]
        checkpoint = runner.checkpoint()
        assert checkpoint["parts"][0]["source_message_id"] == "view-event-message"
        restarted = media.for_run(scope.run_id)
        assert restarted.checkpoint() is None
        restarted.restore_checkpoint(None)
        restarted.restore_checkpoint(checkpoint)
        assert restarted.checkpoint() == checkpoint
        restored = await restarted(history)
        assert restored[-1].content == projected[-1].content
        assert restored[-1].images[0].data == GIF
        await restarted.accepted()
        await restarted.accepted()
        assert restarted.checkpoint() == checkpoint
        # Acknowledgement does not lose context images on stateless providers.
        again = await restarted(history)
        assert len(again[-1].images) == 2 and media.screenshots.calls == 1
        fake = dict(checkpoint, request_id="not-recorded")
        with pytest.raises(ServiceError, match="permission_denied"):
            await restarted.restore(fake, history)
        staged = media.for_run(scope.run_id)
        staged.restore_checkpoint(fake)  # Synchronous staging grants no authority.
        with pytest.raises(ServiceError, match="permission_denied"):
            await staged(history)
        assert staged.checkpoint() == fake  # Failed restore remains retryable.
        with pytest.raises(ServiceError, match="permission_denied"):
            staged.restore_checkpoint(dict(checkpoint, run_id="other-run"))
        staged.restore_checkpoint(None)
        assert staged.checkpoint() is None
        await staged.accepted()
        async with store.lock, store.engine.connect() as conn:
            events = (await conn.execute(select(run_events.c.data).where(
                run_events.c.kind == "media_selection",
            ))).scalars().all()
        assert any(event["status"] == "accepted" for event in events)
        media.include_screenshots = False
        disabled = media.for_run(scope.run_id)
        disabled.restore_checkpoint(checkpoint)
        filtered = await disabled(history)
        assert len(filtered[-1].images) == 1
        assert filtered[-1].images[0].media_type == "image/gif"
        assert disabled.checkpoint()["request_id"] != checkpoint["request_id"]
        assert media.screenshots.calls == 1


@pytest.mark.asyncio
async def test_current_history_retains_images_but_summary_does_not_revive_old_events(tmp_path):
    async with environment(tmp_path) as (store, host, media, artifacts, scope):
        saved = await artifacts.save(scope, PNG, "image/png")
        value = {"status": "ready", "frame": None,
                 "media_ref": saved.reference.model_dump(mode="json")}
        await store.append(scope.run_id, "tool_end", {
            "name": "view_image", "status": "ok", "result_available": True,
            "provider_call_id": "image-call", "value": value,
        })
        current = [
            Message(role="user", content="inspect"),
            Message(role="assistant", content="", tool_calls=[
                {"id": "image-call", "name": "view_image", "arguments": {}},
            ]),
            Message(role="tool", call_id="image-call", content=encoded(value)),
        ]
        runner = media.for_run(scope.run_id)
        first = await runner(current)
        assert first[-1].images[0].data == PNG
        await runner.accepted()
        # A new persisted turn/run retaining the full group still needs image bytes.
        second = await session_sql.submit(
            store, scope.session_id,
            TurnRequest(request_key="second-turn", text="inspect again", expected_revision=1),
            Settings(),
        )
        second_run, _ = await store.begin(
            RunRequest(text="inspect again", request_key="second-media-run"), {},
        )
        async with store.lock, store.engine.begin() as conn:
            await conn.execute(update(turns).where(turns.c.id == second.id).values(
                run_id=second_run.id,
            ))
        host.scope = scope = dataclasses.replace(scope, run_id=second_run.id, turn_id=second.id)
        runner = media.for_run(scope.run_id)
        later = [*current, Message(role="assistant", content="initial observation"),
                 Message(role="user", content="look at the same image again")]
        repeated = await runner(later)
        assert repeated[-1].images[0].data == PNG
        saved_selection = runner.checkpoint()
        await runner.accepted()
        summary = [Message(role="system", content="Earlier image was summarized as a chart."),
                   Message(role="user", content="continue")]
        assert await runner(summary) is summary
        assert runner.checkpoint() is None
        # A real saved checkpoint cannot resurrect a group absent from current context.
        restored = media.for_run(scope.run_id)
        restored.restore_checkpoint(saved_selection)
        assert await restored(summary) is summary
        assert restored.checkpoint() is None
        # Partial or mismatched tool groups provide no current-context source.
        assert await runner(current[:-1]) == current[:-1]
        changed = [*current[:-1], Message(role="tool", call_id="image-call", content="{}")]
        assert await runner(changed) is changed


@pytest.mark.asyncio
async def test_legacy_missing_provider_identity_matches_only_current_run_complete_group(tmp_path):
    async with environment(tmp_path) as (store, host, media, artifacts, scope):
        saved = await artifacts.save(scope, PNG, "image/png")
        value = {"status": "ready", "frame": None,
                 "media_ref": saved.reference.model_dump(mode="json")}
        await store.append(scope.run_id, "tool_end", {
            "name": "view_image", "status": "ok", "result_available": True, "value": value,
        })
        history = [Message(role="assistant", content="", tool_calls=[
            {"id": "legacy-call", "name": "view_image", "arguments": {}},
        ]), Message(role="tool", call_id="legacy-call", content=encoded(value))]
        selected = await media.select(scope, history)
        assert len(selected) == 1 and selected[0].source_message_id == "legacy-call"
        assert await media.select(scope, history[:1]) == ()
        # The same result in another run cannot retroactively bind an old event.
        records = await media._events(scope)
        records[0]["run_id"] = "past-run-without-provider-identity"

        async def events(current):
            return records

        media._events = events
        assert await media.select(scope, history) == ()


@pytest.mark.asyncio
@pytest.mark.parametrize("enabled,has_port,vision", [
    (False, True, True), (True, False, True), (True, True, False),
])
async def test_disabled_or_unavailable_screenshots_are_ignored_before_read(
    tmp_path, enabled, has_port, vision,
):
    async with environment(tmp_path) as (store, host, media, artifacts, scope):
        class Screenshots:
            async def read_browser_artifact(self, *args):
                pytest.fail("disabled screenshots must never be read")

        async def capabilities(current):
            return ImageCapabilities(vision)

        media = SessionMediaHost(
            store, host, capabilities=capabilities, artifacts=artifacts,
            screenshots=Screenshots() if has_port else None, include_screenshots=enabled,
        )
        assert media.include_screenshots is enabled
        value = {"reference": "screen-should-not-load"}
        await store.append(scope.run_id, "tool_end", {
            "name": "browser_screenshot", "status": "ok", "result_available": True,
            "provider_call_id": "disabled-screen", "value": value,
        })
        history = [Message(role="user", content="continue without screenshot"),
                   Message(role="assistant", content="", tool_calls=[
                       {"id": "disabled-screen", "name": "browser_screenshot", "arguments": {}},
                   ]), Message(role="tool", call_id="disabled-screen", content=encoded(value))]
        runner = media.for_run(scope.run_id)
        assert await runner(history) is history and runner.checkpoint() is None
        saved = await artifacts.save(scope, GIF, "image/gif")
        result = await media.service.view(scope, MediaSource(reference=saved.reference.reference))
        assert result["status"] == ("ready" if vision else "unavailable")


@pytest.mark.asyncio
async def test_project_file_download_hash_checked_and_original_saved(tmp_path):
    async with environment(tmp_path) as (store, host, media, artifacts, scope):
        host.scope = scope = dataclasses.replace(scope, project_id="project")

        class Sandbox:
            async def download_file(self, actor, project_id, *, path, allow_running=False):
                assert project_id == "project" and path == "figures/image.png"
                assert allow_running is True
                return PNG, hashlib.sha256(PNG).hexdigest()

        media.sandbox = Sandbox()
        result = await media.service.view(scope, MediaSource(path="figures/image.png"))
        original = await artifacts.read(scope, result["media_ref"]["reference"])
        assert original.data == PNG and original.reference.path == "figures/image.png"


@pytest.mark.asyncio
async def test_storage_survives_actual_connection_restart_and_session_revocation(tmp_path):
    async with environment(tmp_path) as (store, host, media, artifacts, scope):
        saved = await artifacts.save(scope, PNG, "image/png")
        await store.close()
        reopened = await SQLiteStore.open(tmp_path / "media.sqlite")
        try:
            current = SQLiteMediaArtifacts(reopened)
            assert (await current.read(scope, saved.reference.reference)).data == PNG
            async with reopened.lock, reopened.engine.begin() as conn:
                await conn.execute(update(sessions).where(sessions.c.id == scope.session_id).values(
                    status="closed",
                ))
            with pytest.raises(ServiceError, match="session_closed"):
                await current.read(scope, saved.reference.reference)
        finally:
            await reopened.close()


@pytest.mark.asyncio
async def test_mcp_mime_base64_and_tampered_scope_are_rejected(tmp_path):
    async with environment(tmp_path) as (store, host, media, artifacts, scope):
        for content in [{"mimeType": "image/jpeg", "blob": base64.b64encode(PNG).decode()},
                        {"mimeType": "image/png", "blob": "data:image/png;base64,AA=="}]:
            with pytest.raises(ServiceError, match="invalid_input"):
                await media.media_sink(scope, "server", {
                    "server": "server", "configuration_digest": "a" * 64,
                    "contents": [content],
                })
        fake = dataclasses.replace(scope, session_id="other-session")
        with pytest.raises(ServiceError, match="permission_denied"):
            await media.media_sink(fake, "server", {
                "server": "server", "configuration_digest": "a" * 64, "contents": [],
            })


@pytest.mark.asyncio
@pytest.mark.parametrize("mime", ["image/png", "image/gif", "image/jpeg", "image/webp"])
async def test_true_mime_survives_existing_provider_sdk_serialization(mime):
    payload = GIF if mime == "image/gif" else PNG
    if mime in {"image/jpeg", "image/webp"}:
        executable = shutil.which("ffmpeg")
        if executable is None:
            pytest.skip("existing ffmpeg unavailable")
        source = material()
        converted = await FFmpegImageTransform(executable)(
            source, inspect_image(source.data), ImageCapabilities(True, frozenset({mime})), None,
        )
        payload = converted.data
    provider, captured = await _provider(allow_images=True, max_request_bytes=1_048_576)
    try:
        _ = [event async for event in provider.stream([_image(payload, mime)], [])]
        url = captured[0]["messages"][-1]["content"][1]["image_url"]["url"]
        assert url.startswith(f"data:{mime};base64,")
        assert base64.b64decode(url.partition(",")[2]) == payload
    finally:
        await provider.close()
