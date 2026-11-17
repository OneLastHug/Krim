"""F-package tests: authorization, original references and real request parts."""

from __future__ import annotations

import base64
import dataclasses
import hashlib
import random
import shutil
import struct
import zlib

import pytest
from ai import messages as sdk
from pydantic import ValidationError

from krim_agent.agent_sdk.host_media import HostImageMessage, validate_host_images
from krim_agent.agent_sdk.media import MediaLimits
from krim_agent.contracts import Message, ServiceError
from krim_agent.media.delivery import MediaDelivery, MediaProjection
from krim_agent.media.ffmpeg import FFmpegImageTransform
from krim_agent.media.formats import inspect_image
from krim_agent.media.references import AuthorizedMedia, AuthorizedMediaRef, MediaSource
from krim_agent.media.service import ImageCapabilities, MediaService
from krim_agent.media.tools import ViewImageInput
from krim_agent.security import Membership, PrincipalContext
from krim_agent.tools.core import Invocation
from krim_agent.tools.session import SessionToolScope


def png(width=2, height=2, *, entropy=False):
    def chunk(kind, payload):
        return (struct.pack(">I", len(payload)) + kind + payload
                + struct.pack(">I", zlib.crc32(kind + payload) & 0xFFFFFFFF))
    rng = random.Random(7)
    rows = b"".join(b"\0" + (rng.randbytes(width * 3) if entropy else b"\xff\0\0" * width)
                    for _ in range(height))
    header = struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0)
    return (b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", header)
            + chunk(b"IDAT", zlib.compress(rows)) + chunk(b"IEND", b""))


PNG = png()
GIF = base64.b64decode("R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7")


def scope(project="project", **changes):
    result = SessionToolScope(
        PrincipalContext("owner", 1, (Membership(workspace_id="workspace"),)),
        "workspace", "session", "turn", "run", project,
    )
    return dataclasses.replace(result, **changes)


def material(data=PNG, *, project="project", path="images/a.png", **changes):
    info = inspect_image(data)
    ref = AuthorizedMediaRef(
        reference="image_original_1", owner_id="owner", workspace_id="workspace",
        session_id="session", project_id=project, path=path,
        sha256=hashlib.sha256(data).hexdigest(), size=len(data), media_type=info.media_type,
    )
    return AuthorizedMedia(ref.model_copy(update=changes), data)


def harness(value=None, caps=None, transform=None):
    value = value or material()
    caps = caps or ImageCapabilities(True)
    reads = []

    async def read(current, source):
        reads.append(source)
        return value

    async def capabilities(current):
        return caps

    return MediaService(read=read, capabilities=capabilities, transform=transform), reads


@pytest.mark.parametrize("path", ["/tmp/a.png", "C:/a.png", "C:\\a.png", "../a.png", "a/../b",
                                   "./a", "a//b", "file:///a", "https://x/a", "a\x00b", "~/.x"])
def test_paths_rejected_before_read(path):
    with pytest.raises(ValidationError):
        ViewImageInput(path=path)


@pytest.mark.parametrize("args", [{}, {"path": "a", "reference": "image"},
                                  {"reference": "https://x/a"}, {"reference": "../a"},
                                  {"path": "a", "owner_id": "other"}])
def test_input_has_one_source_and_no_model_authority(args):
    with pytest.raises(ValidationError):
        ViewImageInput(**args)


@pytest.mark.asyncio
async def test_tool_bundle_resolves_host_identity_and_returns_reference_only():
    service, reads = harness()
    resolved = []

    async def resolve(invocation):
        resolved.append(invocation)
        return scope()

    bundle = service.bundle(resolve)
    tool = bundle.specs[0]
    invocation = Invocation("run", "call", "span", 0, "view_image", "{}", "a" * 64)
    assert tool.capability == "session_read" and tool.risk == "read"
    assert await bundle.policy(invocation) == "allow"
    result = await tool.scoped_executor(invocation, ViewImageInput(path="images/a.png"), None)
    assert result["status"] == "ready"
    assert result["delivery"] == "pending_model_request"
    assert result["media_ref"]["sha256"] == hashlib.sha256(PNG).hexdigest()
    assert "data" not in result and base64.b64encode(PNG).decode() not in str(result)
    assert len(resolved) == 2 and len(reads) == 1


@pytest.mark.asyncio
async def test_no_project_sessions_can_view_authorized_artifacts():
    service, reads = harness(material(project=None, path=None))
    result = await service.view(scope(None), MediaSource(reference="image_original_1"))
    assert result["status"] == "ready"
    with pytest.raises(ServiceError, match="workspace_unavailable"):
        await service.view(scope(None), MediaSource(path="images/a.png"))
    assert len(reads) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("change", [{"owner_id": "other"}, {"workspace_id": "other"},
                                     {"session_id": "other"}, {"project_id": "other"}])
async def test_reference_scope_is_not_a_bearer_capability(change):
    service, _ = harness(material(**change))
    with pytest.raises(ServiceError, match="permission_denied"):
        await service.view(scope(), MediaSource(reference="image_original_1"))


@pytest.mark.asyncio
async def test_source_hash_mime_and_path_are_rechecked():
    for value, code in [(material(sha256="0" * 64), "source_changed"),
                        (material(size=1), "source_changed"),
                        (material(media_type="image/jpeg"), "invalid_input"),
                        (material(path="wrong.png"), "permission_denied")]:
        service, _ = harness(value)
        with pytest.raises(ServiceError, match=code):
            await service.view(scope(), MediaSource(path="images/a.png"))


@pytest.mark.asyncio
async def test_revocation_during_port_read_is_checked_before_return():
    revoked = False

    def check():
        if revoked:
            raise ServiceError("permission_denied")

    async def read(current, source):
        nonlocal revoked
        revoked = True
        return material()

    async def capabilities(current):
        return ImageCapabilities(True)

    service = MediaService(read=read, capabilities=capabilities)
    with pytest.raises(ServiceError, match="permission_denied"):
        await service.view(scope(check=check), MediaSource(path="images/a.png"))


@pytest.mark.asyncio
async def test_nonvisual_model_does_not_read_or_claim_to_see_image():
    service, reads = harness(caps=ImageCapabilities(False))
    assert await service.view(scope(), MediaSource(path="images/a.png")) == {
        "status": "unavailable", "reason": "model_does_not_support_images",
        "delivery": "not_attached",
    }
    assert reads == []


@pytest.mark.asyncio
async def test_unsupported_format_keeps_original_reference_without_fake_delivery():
    caps = ImageCapabilities(True, frozenset({"image/png"}))
    service, _ = harness(material(GIF, path="a.gif"), caps)
    result = await service.view(scope(), MediaSource(path="a.gif"))
    assert result["status"] == "unavailable" and result["delivery"] == "not_attached"
    assert result["reason"] == "image_conversion_unavailable"
    assert result["media_ref"]["media_type"] == "image/gif"


@pytest.mark.parametrize("data", [PNG[:-1], PNG + b"bad", PNG[:-1] + b"\x00",
                                  b"\xff\xd8\xff\xe0fixture", b"GIF89afixture",
                                  b"RIFF\0\0\0\0WEBPfixture"])
def test_real_container_validation_rejects_truncated_or_forged_images(data):
    with pytest.raises(ServiceError, match="invalid_input"):
        validate_host_images([sdk.FilePart(data=data, media_type="image/png")])


def test_default_host_limits_remove_512k_and_eight_image_quotas():
    large = png(600, 600, entropy=True)
    assert len(large) > 1024 * 1024
    parts = [sdk.FilePart(data=large, media_type="image/png") for _ in range(9)]
    assert len(validate_host_images(parts)) == 9
    assert len(HostImageMessage(role="user", content="images", images=parts).images) == 9
    with pytest.raises(ServiceError):
        validate_host_images(parts, MediaLimits(max_images=8))
    with pytest.raises(ServiceError):
        validate_host_images(parts[:1], MediaLimits(max_image_bytes=len(large) - 1))
    with pytest.raises(ServiceError):
        validate_host_images(parts[:2], MediaLimits(max_batch_bytes=len(large)))
    assert MediaLimits(max_images=20, max_batch_bytes=20_000_000).max_images == 20


def test_host_bytes_and_base64_validate_but_urls_never_reach_sdk():
    part = sdk.FilePart(data=base64.b64encode(PNG).decode(), media_type="image/png")
    assert validate_host_images([part]) == [PNG]
    with pytest.raises(ServiceError):
        validate_host_images([sdk.FilePart(data=PNG, media_type="")])
    for data in ["https://example/a.png", "file:///tmp/a.png", "data:image/png;base64,AA=="]:
        with pytest.raises(ServiceError):
            validate_host_images([sdk.FilePart(data=data, media_type="image/png")])


@pytest.mark.asyncio
async def test_projection_survives_compaction_retries_and_only_acknowledges_success():
    value = material()
    service, reads = harness(value)
    receipts = set()

    async def resolve():
        return scope()

    async def pending(current, history):
        return [MediaDelivery("event-1", value.reference)] if "event-1" not in receipts else []

    async def ack(current, ids):
        receipts.update(ids)

    projection = MediaProjection(service, resolve=resolve, pending=pending, acknowledge=ack)
    history = [Message(role="user", content="compacted summary without original tool JSON")]
    first = await projection(history)
    assert isinstance(first[-1], HostImageMessage) and first[-1].images[0].data == PNG
    assert len(history) == 1 and projection.selected == ("event-1",)
    retry = await projection(history)  # failed request, no acknowledgement
    assert isinstance(retry[-1], HostImageMessage) and receipts == set()
    await projection.acknowledge()
    assert receipts == {"event-1"} and await projection(history) is history
    restored = MediaProjection(service, resolve=resolve, pending=pending, acknowledge=ack)
    assert await restored(history) is history
    assert len(reads) == 2


@pytest.mark.asyncio
async def test_projection_reloads_and_compares_full_original_reference():
    value = material()
    service, _ = harness(material(reference="different_reference"))

    async def resolve():
        return scope()

    async def pending(current, history):
        return [MediaDelivery("event-1", value.reference)]

    async def ack(current, ids):
        pytest.fail("must not acknowledge before successful delivery")

    projection = MediaProjection(service, resolve=resolve, pending=pending, acknowledge=ack)
    with pytest.raises(ServiceError, match="source_changed"):
        await projection([])


@pytest.mark.asyncio
async def test_projection_batches_at_provider_count_and_bytes_without_losing_pending():
    values = [material(png(i + 1, 2), reference=f"image_{i}") for i in range(5)]
    limits = MediaLimits(max_images=2, max_batch_bytes=2 * max(len(v.data) for v in values))
    service, _ = harness(caps=ImageCapabilities(True, limits=limits))
    receipts = set()

    async def read(current, source):
        return next(v for v in values if v.reference.reference == source.reference)

    service.read = read

    async def resolve():
        return scope()

    async def pending(current, history):
        return [MediaDelivery(f"event-{i}", value.reference)
                for i, value in enumerate(values) if f"event-{i}" not in receipts]

    async def ack(current, ids):
        receipts.update(ids)

    projection = MediaProjection(service, resolve=resolve, pending=pending, acknowledge=ack)
    for expected in [2, 2, 1]:
        result = await projection([])
        assert len(result[-1].images) == expected
        await projection.acknowledge()
    assert len(receipts) == 5 and await projection([]) == []


@pytest.mark.asyncio
async def test_ffmpeg_existing_decoder_converts_and_preserves_original_hash():
    executable = shutil.which("ffmpeg")
    if executable is None:
        pytest.skip("existing ffmpeg is unavailable; no dependency installation")
    converter = FFmpegImageTransform(executable)
    value = material(png(12, 8))
    for mime in ["image/png", "image/jpeg", "image/webp", "image/gif"]:
        caps = ImageCapabilities(True, frozenset({mime}), max_width=6, max_height=4)
        converted = await converter(value, inspect_image(value.data), caps, None)
        info = inspect_image(converted.data, mime)
        assert info.width <= 6 and info.height <= 4
        assert converted.source_sha256 == value.reference.sha256
        assert converted.frame is None
        validate_host_images([sdk.FilePart(data=converted.data, media_type=mime)])


@pytest.mark.asyncio
async def test_explicit_gif_frame_is_converted_through_injected_port():
    executable = shutil.which("ffmpeg")
    if executable is None:
        pytest.skip("existing ffmpeg is unavailable")
    value = material(GIF, path="a.gif")
    caps = ImageCapabilities(True, frozenset({"image/png"}))
    service, _ = harness(value, caps, FFmpegImageTransform(executable))
    result = await service.view(scope(), MediaSource(path="a.gif"), frame=0)
    assert result["status"] == "ready" and result["image"]["media_type"] == "image/png"
    assert result["media_ref"]["sha256"] == hashlib.sha256(GIF).hexdigest()
    with pytest.raises(ServiceError, match="invalid_input"):
        await service.view(scope(), MediaSource(path="a.gif"), frame=1)


@pytest.mark.asyncio
async def test_failed_delivery_receipt_is_retried_without_losing_image():
    value = material()
    service, _ = harness(value)

    async def resolve():
        return scope()

    async def pending(current, history):
        return [MediaDelivery("event-1", value.reference)]

    async def ack(current, ids):
        raise ServiceError("execution_error")

    projection = MediaProjection(service, resolve=resolve, pending=pending, acknowledge=ack)
    assert isinstance((await projection([]))[-1], HostImageMessage)
    with pytest.raises(ServiceError):
        await projection.acknowledge()
    assert projection.selected == ("event-1",)
    assert isinstance((await projection([]))[-1], HostImageMessage)


@pytest.mark.asyncio
async def test_conversion_applies_real_provider_bytes_and_dimension_capacity():
    executable = shutil.which("ffmpeg")
    if executable is None:
        pytest.skip("existing ffmpeg is unavailable")
    original = material(png(128, 64, entropy=True))
    caps = ImageCapabilities(True, frozenset({"image/png"}),
                             limits=MediaLimits(max_image_bytes=2000), max_width=64)
    converter = FFmpegImageTransform(executable)
    result = await converter(original, inspect_image(original.data), caps, None)
    assert len(result.data) <= 2000 and inspect_image(result.data).width <= 64
    assert original.data != result.data
    assert result.source_sha256 == original.reference.sha256


@pytest.mark.asyncio
async def test_real_animated_gif_selects_requested_frame_preserving_original():
    executable = shutil.which("ffmpeg")
    if executable is None:
        pytest.skip("existing ffmpeg is unavailable")
    import asyncio

    process = await asyncio.create_subprocess_exec(
        executable, "-hide_banner", "-loglevel", "error", "-f", "lavfi",
        "-i", "testsrc=size=4x4:rate=2", "-frames:v", "2", "-threads", "1",
        "-f", "gif", "pipe:1", stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    animated, error = await process.communicate()
    assert process.returncode == 0, error
    assert inspect_image(animated).frames == 2
    original = material(animated, path="animated.gif")
    caps = ImageCapabilities(True, frozenset({"image/png"}))
    converter = FFmpegImageTransform(executable)
    result = await converter(original, inspect_image(animated), caps, 1)
    assert inspect_image(result.data).media_type == "image/png"
    assert result.frame == 1
    assert result.source_sha256 == original.reference.sha256


@pytest.mark.asyncio
async def test_view_result_projects_real_image_through_existing_sdk_wire():
    from tests.coding_agent.test_development_model_media import _provider

    original = material()
    service, _ = harness(original)
    result = await service.view(scope(), MediaSource(path="images/a.png"))
    confirmed_ref = AuthorizedMediaRef.model_validate(result["media_ref"])
    receipts = []

    async def resolve():
        return scope()

    async def pending(current, history):
        return [] if receipts else [MediaDelivery("confirmed-event", confirmed_ref)]

    async def acknowledge(current, ids):
        receipts.extend(ids)

    projection = MediaProjection(
        service, resolve=resolve, pending=pending, acknowledge=acknowledge,
    )
    history = [Message(role="user", content="Inspect the authorized image")]
    provider, captured = await _provider(allow_images=True, max_request_bytes=1_048_576)
    try:
        projected = await projection(history)
        _ = [event async for event in provider.stream(projected, [])]
        content = captured[0]["messages"][-1]["content"]
        assert content[1]["type"] == "image_url"
        data_url = content[1]["image_url"]["url"]
        assert base64.b64decode(data_url.partition(",")[2], validate=True) == original.data
        await projection.acknowledge()
        assert receipts == ["confirmed-event"]
        assert await projection(history) is history
        assert len(history) == 1
    finally:
        await provider.close()
