"""Session-authorized image IO, MCP ingestion and model request projection.

Host composition supplies the existing sandbox/screenshots and an actual media
artifact port. Without an artifact port this helper cannot claim image storage.
It never opens a model-provided host path or interprets a resource URI as one.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
from collections.abc import Awaitable, Callable, Sequence
from typing import Any, Literal, Protocol, cast

from pydantic import JsonValue
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncConnection

from krim_agent.contracts import Message, ServiceError, fingerprint, parse_json
from krim_agent.media.artifacts import MediaArtifactPort
from krim_agent.media.delivery import MediaDelivery, MediaProjection, MediaRequestSelection
from krim_agent.media.formats import IMAGE_TYPES, inspect_image
from krim_agent.media.references import AuthorizedMedia, AuthorizedMediaRef, MediaSource
from krim_agent.media.service import (
    ImageCapabilities,
    MediaService,
    ResolveCapabilities,
    TransformImage,
)
from krim_agent.security import PrincipalContext
from krim_agent.storage.models import run_events, turns
from krim_agent.storage.sqlite import SQLiteStore
from krim_agent.tools.session import SessionToolScope, ToolBundle

from .session_tools import SessionToolHost


class DownloadMediaPort(Protocol):
    async def download_file(
        self, actor: PrincipalContext, project_id: str, *, path: str,
        allow_running: bool = False,
    ) -> tuple[bytes, str]: ...


class BrowserMediaPort(Protocol):
    async def read_browser_artifact(
        self, actor: PrincipalContext, project_id: str, run_id: str, reference: str,
    ) -> tuple[bytes, dict[str, Any]]: ...


type MediaContextSelector = Callable[
    [SessionToolScope, list[Message], tuple[MediaDelivery, ...]],
    Awaitable[Sequence[MediaDelivery]],
]


class SessionMediaHost:
    def __init__(
        self, store: SQLiteStore, host: SessionToolHost, *,
        capabilities: ResolveCapabilities, sandbox: DownloadMediaPort | None = None,
        screenshots: BrowserMediaPort | None = None, artifacts: MediaArtifactPort | None = None,
        include_screenshots: bool = False, transform: TransformImage | None = None,
        select_context: MediaContextSelector | None = None,
    ) -> None:
        if type(include_screenshots) is not bool:
            raise ValueError("include_screenshots must be a boolean")
        self.include_screenshots = include_screenshots
        self.store, self.host = store, host
        self.sandbox, self.screenshots, self.artifacts = sandbox, screenshots, artifacts
        self.select_context = select_context
        self._capabilities = capabilities
        self.service = MediaService(
            read=self.read, capabilities=self.capabilities, transform=transform,
        )

    async def capabilities(self, scope: SessionToolScope) -> ImageCapabilities:
        current = await self._current(scope)
        value = await self._capabilities(current)
        await self._current(scope)
        return value

    def bundle(self) -> ToolBundle:
        return self.service.bundle(self.host.resolve)

    def for_run(
        self, run_id: str, check: Callable[[], None] = lambda: None,
    ) -> SessionMediaRun:
        return SessionMediaRun(self, run_id, check)

    def _artifacts(self) -> MediaArtifactPort:
        if self.artifacts is None:
            raise ServiceError("artifact_invalid", 503)
        return self.artifacts

    async def _current(self, scope: SessionToolScope) -> SessionToolScope:
        scope.check()
        current = await self.host.by_run(scope.run_id, scope.check)
        if (current.actor.principal_id, current.actor.generation, current.workspace_id,
                current.session_id, current.project_id, current.turn_id) != (
            scope.actor.principal_id, scope.actor.generation, scope.workspace_id,
            scope.session_id, scope.project_id, scope.turn_id,
        ):
            raise ServiceError("permission_denied", 403)
        return current

    async def read(self, scope: SessionToolScope, source: MediaSource) -> AuthorizedMedia:
        current = await self._current(scope)
        artifacts = self._artifacts()
        if source.reference is not None:
            value = await artifacts.read(current, source.reference)
        else:
            if current.project_id is None or self.sandbox is None or source.path is None:
                raise ServiceError("workspace_unavailable", 422)
            data, expected_hash = await self.sandbox.download_file(
                current.actor, current.project_id, path=source.path, allow_running=True,
            )
            await self._current(scope)
            if hashlib.sha256(data).hexdigest() != expected_hash:
                raise ServiceError("source_changed", 409)
            info = inspect_image(data)
            value = await artifacts.save(
                current, data, info.media_type, path=source.path,
                provenance={"kind": "project_file", "run_id": current.run_id},
            )
        await self._current(scope)
        value.validate(current)
        return value

    async def media_sink(
        self, scope: SessionToolScope, server: str, record: dict[str, Any],
    ) -> JsonValue:
        current = await self._current(scope)
        contents = record.get("contents")
        config_digest = record.get("configuration_digest")
        if (not server or record.get("server") != server or not isinstance(contents, list)
                or not isinstance(config_digest, str) or len(config_digest) != 64
                or any(c not in "0123456789abcdef" for c in config_digest)):
            raise ServiceError("invalid_input", 422)
        images: list[JsonValue] = []
        for index, content in enumerate(contents):
            if (not isinstance(content, dict) or not isinstance(content.get("mimeType"), str)
                    or content["mimeType"] not in IMAGE_TYPES):
                continue
            if self.artifacts is None:
                return {"status": "unavailable", "reason": "authorized_media_storage_unavailable"}
            blob = content.get("blob")
            if not isinstance(blob, str):
                continue
            try:
                raw = blob.encode("ascii")
                data = base64.b64decode(raw, validate=True)
                if base64.b64encode(data) != raw:
                    raise ValueError
            except (UnicodeEncodeError, binascii.Error, ValueError):
                raise ServiceError("invalid_input", 422) from None
            media_type = str(content["mimeType"])
            inspect_image(data, media_type)
            await self._current(scope)
            value = await self.artifacts.save(
                current, data, media_type, provenance={
                    "kind": "mcp_resource", "server": server,
                    "uri": cast(JsonValue, record.get("uri")),
                    "configuration_digest": record["configuration_digest"], "index": index,
                },
            )
            await self._current(scope)
            value.validate(current)
            images.append({"reference": value.reference.reference,
                           "media_ref": cast(JsonValue, value.reference.model_dump(mode="json"))})
        return {"status": "stored" if images else "no_images", "images": images}

    async def _events(self, scope: SessionToolScope) -> list[dict[str, Any]]:
        await self._current(scope)

        async def read(conn: AsyncConnection) -> list[dict[str, Any]]:
            rows = (await conn.execute(select(
                run_events.c.run_id, run_events.c.event_id, run_events.c.kind, run_events.c.data,
            ).select_from(run_events.join(turns, turns.c.run_id == run_events.c.run_id)).where(
                turns.c.session_id == scope.session_id,
                turns.c.owner_id == scope.actor.principal_id,
                turns.c.workspace_id == scope.workspace_id,
                run_events.c.kind.in_(("tool_end", "media_source", "media_selection")),
            ).order_by(turns.c.ordinal, run_events.c.seq))).mappings().all()
            return [dict(row) for row in rows]

        rows = await self.store.work_read(read)
        await self._current(scope)
        return rows

    async def _capture_screenshot(
        self, scope: SessionToolScope, event: dict[str, Any], value: dict[str, Any],
    ) -> AuthorizedMediaRef:
        if (not self.include_screenshots or self.screenshots is None or scope.project_id is None
                or value.get("project_id") != scope.project_id
                or value.get("run_id") != event["run_id"]
                or not isinstance(value.get("reference"), str)):
            raise ServiceError("permission_denied", 403)
        data, metadata = await self.screenshots.read_browser_artifact(
            scope.actor, scope.project_id, event["run_id"], value["reference"],
        )
        await self._current(scope)
        digest = hashlib.sha256(data).hexdigest()
        if (metadata.get("sha256") != digest or value.get("sha256") != digest
                or value.get("size") != len(data) or metadata.get("size") != len(data)):
            raise ServiceError("source_changed", 409)
        saved = await self._artifacts().save(scope, data, "image/png", provenance={
            "kind": "browser_screenshot", "source_event_id": event["event_id"],
            "source_run_id": event["run_id"], "reference": value["reference"],
        })
        saved.validate(scope)
        event_key = fingerprint({"source_run": event["run_id"], "source": event["event_id"]})
        await self.store.append(scope.run_id, "media_source", {
            "source_run_id": event["run_id"], "source_event_id": event["event_id"],
            "media_ref": cast(JsonValue, saved.reference.model_dump(mode="json")),
        }, event_id=event_key)
        await self._current(scope)
        return saved.reference

    @staticmethod
    def _context_results(history: list[Message]) -> dict[str, tuple[str, JsonValue]]:
        """Return only results belonging to complete current-history tool groups."""
        complete: dict[str, tuple[str, JsonValue]] = {}
        pending: dict[str, str] = {}
        results: dict[str, tuple[str, JsonValue]] = {}
        for message in history:
            if message.role == "assistant":
                pending, results = {}, {}
                for call in message.tool_calls:
                    identifier, name = call.get("id"), call.get("name")
                    if (not isinstance(identifier, str) or not isinstance(name, str)
                            or identifier in pending):
                        pending = {}
                        break
                    pending[identifier] = name
            elif message.role == "tool" and message.call_id in pending:
                identifier = message.call_id
                if identifier is None or identifier in results:
                    pending, results = {}, {}
                    continue
                try:
                    value = parse_json(message.content)
                except (ValueError, RecursionError):
                    pending, results = {}, {}
                    continue
                results[identifier] = (pending[identifier], value)
                if len(results) == len(pending):
                    complete.update(results)
                    pending, results = {}, {}
            else:
                pending, results = {}, {}
        return complete

    async def select(
        self, scope: SessionToolScope, history: list[Message],
    ) -> Sequence[MediaDelivery]:
        caps = await self.capabilities(scope)
        if not caps.vision:
            return ()
        context = self._context_results(history)
        if not context:
            return ()
        records = await self._events(scope)
        screenshots_enabled = self.include_screenshots and self.screenshots is not None
        captured: dict[tuple[str, str], AuthorizedMediaRef] = {}
        for event in records:
            if screenshots_enabled and event["kind"] == "media_source":
                data = event["data"]
                ref = AuthorizedMediaRef.model_validate(data["media_ref"])
                captured[(data["source_run_id"], data["source_event_id"])] = ref
        candidates = []
        for event in records:
            if event["kind"] != "tool_end":
                continue
            data = event["data"]
            if data.get("status") != "ok" or data.get("result_available") is not True:
                continue
            value = data.get("value")
            if not isinstance(value, dict):
                continue
            source_id = data.get("provider_call_id")
            if isinstance(source_id, str):
                if context.get(source_id) != (data.get("name"), value):
                    continue
            elif event["run_id"] == scope.run_id:
                # Legacy current-run events may lack the provider ID. Match an
                # actual complete tool result rather than scanning all events.
                source_id = next((identifier for identifier, entry in context.items()
                                  if entry == (data.get("name"), value)), None)
                if source_id is None:
                    continue
            else:
                continue
            frame = None
            if data.get("name") == "view_image":
                if value.get("status") != "ready" or not isinstance(value.get("media_ref"), dict):
                    continue
                ref = AuthorizedMediaRef.model_validate(value["media_ref"])
                frame = value.get("frame")
            elif data.get("name") == "browser_screenshot":
                if not screenshots_enabled:
                    continue
                key = (event["run_id"], event["event_id"])
                screenshot_ref = captured.get(key)
                if screenshot_ref is None:
                    screenshot_ref = await self._capture_screenshot(scope, event, value)
                    captured[key] = screenshot_ref
                ref = screenshot_ref
            else:
                continue
            ref.check_scope(scope)
            candidates.append(MediaDelivery(event["event_id"], ref, frame, source_id))
        values = tuple(candidates)
        if self.select_context is None:
            return values
        selected = await self.select_context(scope, history, values)
        await self._current(scope)
        # A selector may narrow/reorder trusted candidates, not create new authority.
        if any(item not in values for item in selected):
            raise ServiceError("permission_denied", 403)
        return selected

    async def record_selection(
        self, scope: SessionToolScope, selection: MediaRequestSelection,
        status: Literal["prepared", "accepted"],
    ) -> None:
        await self._current(scope)
        selection.check_scope(scope)
        key = fingerprint({"request_id": selection.request_id, "status": status})
        await self.store.append(scope.run_id, "media_selection", {
            "status": status, "selection": cast(JsonValue, selection.model_dump(mode="json")),
        }, event_id=key)
        await self._current(scope)


class SessionMediaRun:
    def __init__(
        self, owner: SessionMediaHost, run_id: str, check: Callable[[], None],
    ) -> None:
        self.owner, self.run_id, self.check = owner, run_id, check
        self._pending_checkpoint: MediaRequestSelection | None = None
        self.projection = self._new_projection()

    def _new_projection(self) -> MediaProjection:
        async def resolve() -> SessionToolScope:
            return await self.owner.host.by_run(self.run_id, self.check)

        async def acknowledge(scope: SessionToolScope, ids: tuple[str, ...]) -> None:
            # record_selection(accepted) is the durable request + delivery receipt.
            await self.owner._current(scope)

        return MediaProjection(
            self.owner.service, resolve=resolve, pending=self.owner.select, acknowledge=acknowledge,
            record_selection=self.owner.record_selection,
        )

    async def __call__(self, history: list[Message]) -> list[Message]:
        return await self.project(history)

    async def project(
        self, history: list[Message], *, request_id: str | None = None,
        message_id: str | None = None,
    ) -> list[Message]:
        pending = self._pending_checkpoint
        if pending is not None:
            if (request_id is not None and request_id != pending.request_id
                    or message_id is not None and message_id != pending.message_id):
                raise ServiceError("source_changed", 409)
            result = await self.restore(pending.model_dump(mode="json"), history)
            self._pending_checkpoint = None
            return result
        return await self.projection.project(history, request_id=request_id, message_id=message_id)

    async def acknowledge(self) -> None:
        await self.projection.acknowledge()

    async def accepted(self) -> None:
        """Acknowledge only the most recent successful provider request; idempotent."""
        await self.acknowledge()

    def checkpoint(self) -> dict[str, JsonValue] | None:
        if self._pending_checkpoint is not None:
            return self._pending_checkpoint.model_dump(mode="json")
        return self.projection.checkpoint()

    def restore_checkpoint(self, state: dict[str, Any] | None) -> None:
        """Stage validated refs synchronously; authorize/reload on next async call.

        The SDK restores its checkpoint synchronously. This method does no IO and
        never grants authority. __call__ validates the saved host event and live
        scope before any image is emitted. None clears initial media state.
        """
        selection = MediaRequestSelection.model_validate(state) if state is not None else None
        if selection is not None and selection.run_id != self.run_id:
            raise ServiceError("permission_denied", 403)
        self.projection = self._new_projection()
        self._pending_checkpoint = selection

    async def restore(
        self, selection_json: dict[str, Any], history: list[Message],
    ) -> list[Message]:
        selection = MediaRequestSelection.model_validate(selection_json)
        scope = await self.owner.host.by_run(self.run_id, self.check)
        selection.check_scope(scope)
        events = await self.owner._events(scope)
        if not any(event["run_id"] == self.run_id
                   and event["kind"] == "media_selection"
                   and event["data"].get("selection") == selection.model_dump(mode="json")
                   for event in events):
            raise ServiceError("permission_denied", 403)
        allowed = await self.owner.select(scope, history)
        if any(MediaDelivery(part.delivery_id, part.reference, part.frame, part.source_message_id)
               not in allowed for part in selection.parts):
            # A restored selection is not authority to bypass current policy.
            # Changed screenshot/context policy creates a new request selection.
            return await self.projection(history)
        return await self.projection.reconstruct(history, selection)
