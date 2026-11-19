"""Context image retention, checkpoint associations and format regressions."""

from __future__ import annotations

import json
import shutil
import struct
import zlib

import pytest

from krim_agent.agent_sdk.host_media import HostImageMessage
from krim_agent.agent_sdk.media import MediaLimits
from krim_agent.contracts import Message, ServiceError
from krim_agent.media.delivery import MediaDelivery, MediaProjection, MediaRequestSelection
from krim_agent.media.ffmpeg import FFmpegImageTransform
from krim_agent.media.formats import inspect_image
from krim_agent.media.references import MediaSource
from krim_agent.media.service import ImageCapabilities
from tests.coding_agent.test_view_image import PNG, harness, material, png, scope


def chunk(kind, data):
    return (struct.pack(">I", len(data)) + kind + data
            + struct.pack(">I", zlib.crc32(kind + data) & 0xFFFFFFFF))


@pytest.mark.asyncio
async def test_stateless_provider_retains_image_after_ack_and_deduplicates_each_request():
    value = material()
    service, _ = harness(value)
    records, receipts = [], []

    async def resolve():
        return scope()

    async def select(current, history):
        # Host policy retains this context image even after provider acceptance.
        return [MediaDelivery("tool-result", value.reference, source_message_id="tool-msg"),
                MediaDelivery("tool-result", value.reference, source_message_id="tool-msg"),
                MediaDelivery("result-copy", value.reference, source_message_id="other-msg")]

    async def ack(current, ids):
        receipts.append(ids)

    async def record(current, selection, status):
        records.append((selection, status))

    projection = MediaProjection(service, resolve=resolve, pending=select, acknowledge=ack,
                                 record_selection=record)
    history = [Message(role="user", content="inspect")]
    first = await projection.project(history, request_id="request-1", message_id="image-msg-1")
    assert isinstance(first[-1], HostImageMessage) and len(first[-1].images) == 1
    assert len(records[0][0].parts) == 2
    assert {part.part_index for part in records[0][0].parts} == {0}
    assert records[0][0].parts[0].source_message_id == "tool-msg"
    assert records[0][1] == "prepared"
    await projection.acknowledge()
    assert records[-1][1] == "accepted"
    assert receipts == [("tool-result", "result-copy")]
    assert projection.checkpoint()["request_id"] == "request-1"
    second = await projection.project(history, request_id="request-2", message_id="image-msg-2")
    assert len(second[-1].images) == 1 and second[-1].images[0].data == PNG
    assert records[-1][0].request_id == "request-2"
    assert len(history) == 1


@pytest.mark.asyncio
async def test_checkpoint_reconstructs_exact_request_selection_after_compaction():
    value = material()
    service, _ = harness(value)

    async def resolve():
        return scope()

    async def pending(current, history):
        return [MediaDelivery("event", value.reference, source_message_id="tool-message")]

    async def ack(current, ids):
        return None

    before = MediaProjection(service, resolve=resolve, pending=pending, acknowledge=ack)
    history = [Message(role="user", content="summary")]
    original = await before.project(history, request_id="request", message_id="image-message")
    saved = json.loads(json.dumps(before.checkpoint()))
    assert "data" not in saved and "images" not in saved
    selection = MediaRequestSelection.model_validate(saved)

    async def no_candidates(current, history):
        pytest.fail("saved selection reconstructs directly without history candidate search")

    after = MediaProjection(service, resolve=resolve, pending=no_candidates, acknowledge=ack)
    restored = await after.reconstruct(history, selection)
    assert restored[-1].content == original[-1].content
    assert restored[-1].images[0].data == PNG
    assert after.checkpoint() == saved
    await after.acknowledge()
    assert after.checkpoint() == saved

    bad = selection.model_copy(update={"session_id": "other"})
    with pytest.raises(ServiceError, match="permission_denied"):
        await after.reconstruct(history, bad)
    parts = [selection.parts[0].model_copy(update={"sha256": "0" * 64})]
    with pytest.raises(ServiceError, match="source_changed"):
        await after.reconstruct(history, selection.model_copy(update={"parts": parts}))


@pytest.mark.asyncio
async def test_single_image_uses_batch_capacity_for_conversion():
    executable = shutil.which("ffmpeg")
    if executable is None:
        pytest.skip("existing ffmpeg unavailable")
    value = material(png(100, 50, entropy=True))
    caps = ImageCapabilities(True, limits=MediaLimits(max_batch_bytes=1000))
    service, _ = harness(value, caps, FFmpegImageTransform(executable))
    prepared = await service.prepare(scope(), value, inspect_image(value.data), caps)
    assert len(prepared.part.data) <= 1000 and prepared.transformed


@pytest.mark.asyncio
async def test_static_frame_zero_reuses_original_without_converter():
    service, _ = harness()
    result = await service.view(scope(), MediaSource(path="images/a.png"), frame=0)
    assert result["status"] == "ready" and result["image"]["transformed"] is False


def test_png_source_inspection_does_not_inflate_before_capacity_selection(monkeypatch):
    header = chunk(b"IHDR", struct.pack(">IIBBBBB", 1, 100_000_000, 1, 0, 0, 0, 0))
    data = PNG[:8] + header + chunk(b"IDAT", b"x") + chunk(b"IEND", b"")

    def forbidden_inflate(*args, **kwargs):
        pytest.fail("source inspection must not inflate untrusted image pixels")

    monkeypatch.setattr(zlib, "decompressobj", forbidden_inflate)
    assert inspect_image(data).height == 100_000_000
    with pytest.raises(ServiceError, match="invalid_input"):
        inspect_image(data, validate_pixels=True, max_pixels=1_000_000)


def test_png_explicit_pixel_validation_requires_capacity_and_rejects_bad_stream():
    assert inspect_image(PNG, validate_pixels=True, max_pixels=100).width == 2
    with pytest.raises(ValueError, match="positive work bound"):
        inspect_image(PNG, validate_pixels=True)
    malformed = PNG[:33] + chunk(b"IDAT", b"not-deflate") + chunk(b"IEND", b"")
    with pytest.raises(ServiceError):
        inspect_image(malformed, validate_pixels=True, max_pixels=100)


def test_apng_declared_frames_require_matching_control_and_data_chunks():
    malformed = PNG[:33] + chunk(b"acTL", struct.pack(">II", 2, 0)) + PNG[33:]
    with pytest.raises(ServiceError, match="invalid_input"):
        inspect_image(malformed)
    control = struct.pack(">IIIIIHHBB", 0, 2, 2, 0, 0, 1, 10, 0, 0)
    second = struct.pack(">IIIIIHHBB", 1, 2, 2, 0, 0, 1, 10, 0, 0)
    raw = zlib.compress(b"\0" + b"\xff\0\0" * 2 + b"\0" + b"\xff\0\0" * 2)
    valid = (PNG[:33] + chunk(b"acTL", struct.pack(">II", 2, 0))
             + chunk(b"fcTL", control) + chunk(b"IDAT", raw)
             + chunk(b"fcTL", second) + chunk(b"fdAT", struct.pack(">I", 2) + raw)
             + chunk(b"IEND", b""))
    assert inspect_image(valid).frames == 2


def test_webp_animation_rejects_empty_nested_pixel_chunks():
    def riff_chunk(kind, payload):
        return kind + struct.pack("<I", len(payload)) + payload + b"\0" * (len(payload) % 2)

    canvas = riff_chunk(b"VP8X", b"\x02" + b"\0" * 9)
    animation = riff_chunk(b"ANIM", b"\0" * 6)
    empty_frame = riff_chunk(b"ANMF", b"\0" * 16 + riff_chunk(b"VP8 ", b""))
    body = b"WEBP" + canvas + animation + empty_frame * 2
    data = b"RIFF" + struct.pack("<I", len(body)) + body
    with pytest.raises(ServiceError, match="invalid_input"):
        inspect_image(data)


def test_jpeg_zero_component_and_unknown_scan_selectors_are_rejected():
    malformed = (b"\xff\xd8\xff\xc0" + struct.pack(">HBHHB", 8, 8, 1, 1, 0)
                 + b"\xff\xda" + struct.pack(">HB", 6, 0) + b"\0\0\0\xff\xd9")
    with pytest.raises(ServiceError, match="invalid_input"):
        inspect_image(malformed)
    bad_selector = (b"\xff\xd8\xff\xc0" + struct.pack(">HBHHB", 11, 8, 1, 1, 1)
                    + b"\x01\x11\0" + b"\xff\xda\0\x08\x01\x02\0\0\x3f\0\xff\xd9")
    with pytest.raises(ServiceError, match="invalid_input"):
        inspect_image(bad_selector)
