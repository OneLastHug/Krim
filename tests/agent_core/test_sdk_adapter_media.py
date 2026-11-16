"""A2 media adapter tests against the installed ai 0.9.0 SDK."""

from __future__ import annotations

import base64
import hashlib
import struct
import zlib

import ai
import pytest
from ai import messages

from krim_agent.agent_sdk.media import (
    MediaLimits,
    authorize_history,
    load_authorized_file,
)
from krim_agent.contracts import ServiceError
from tests.coding_agent.sdk_fixtures import Scripted, done, model, setup, turn


def _png_chunk(kind: bytes, payload: bytes) -> bytes:
    return (
        struct.pack(">I", len(payload))
        + kind
        + payload
        + struct.pack(">I", zlib.crc32(kind + payload) & 0xFFFFFFFF)
    )


PNG = (
    b"\x89PNG\r\n\x1a\n"
    + _png_chunk(b"IHDR", struct.pack(">IIBBBBB", 1, 1, 8, 2, 0, 0, 0))
    + _png_chunk(b"IDAT", zlib.compress(b"\x00\xff\x00\x00"))
    + _png_chunk(b"IEND", b"")
)
JPEG = b"\xff\xd8\xff\xe0fixture"
WEBP = b"RIFF\x00\x00\x00\x00WEBPfixture"
GIF = b"GIF89a" + b"fixture"


async def _no_op(_: messages.FilePart) -> None:
    return None


def _part(data: bytes | str = PNG, media_type: str = "image/png") -> messages.FilePart:
    return messages.FilePart(data=data, media_type=media_type, filename="screen.png")


@pytest.mark.asyncio
async def test_jpeg_base64_leading_slash_is_not_a_filesystem_path() -> None:
    original = _part(JPEG, "image/jpeg")
    persisted = messages.FilePart.model_validate_json(original.model_dump_json())
    assert isinstance(persisted.data, str) and persisted.data.startswith("/9j/")
    history = [messages.Message(role="user", parts=[persisted])]
    authorized = await authorize_history(history, _no_op)
    assert authorized[0].parts[0] is persisted
    assert base64.b64decode(persisted.data, validate=True) == JPEG


@pytest.mark.asyncio
async def test_opaque_loader_returns_the_native_file_part_and_authorizes_it() -> None:
    source = _part()
    loaded: list[str] = []
    authorized: list[messages.FilePart] = []

    async def loader(reference: str) -> messages.FilePart:
        loaded.append(reference)
        return source

    async def authorize(part: messages.FilePart) -> None:
        authorized.append(part)

    result = await load_authorized_file("opaque_screen_1", load=loader, authorize=authorize)

    assert result is source
    assert loaded == ["opaque_screen_1"]
    assert authorized == [source]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("payload", "media_type"),
    [
        (PNG, "image/png"),
        (JPEG, "image/jpeg"),
        (WEBP, "image/webp"),
        (GIF, "image/gif"),
    ],
)
async def test_supported_image_magic_types_are_accepted(
    payload: bytes, media_type: str
) -> None:
    source = _part(payload, media_type)

    async def loader(_: str) -> messages.FilePart:
        return source

    assert await load_authorized_file("opaque", load=loader, authorize=_no_op) is source


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "reference",
    [
        "/tmp/screen.png",
        "C:/screen.png",
        "file:///screen.png",
        "https://x/image.png",
        "data:image/png;base64,AA==",
    ],
)
async def test_loader_rejects_paths_urls_and_data_urls_as_references(reference: str) -> None:
    called = False

    async def loader(_: str) -> messages.FilePart:
        nonlocal called
        called = True
        return _part()

    with pytest.raises(ServiceError):
        await load_authorized_file(reference, load=loader, authorize=_no_op)
    assert not called


@pytest.mark.asyncio
async def test_history_without_authorizer_fails_closed_even_with_metadata() -> None:
    part = messages.FilePart(
        data=PNG,
        media_type="image/png",
        provider_metadata={"authorized": True, "sha256": hashlib.sha256(PNG).hexdigest()},
    )
    history = [messages.Message(role="user", parts=[messages.TextPart(text="look"), part])]

    with pytest.raises(ServiceError, match="unauthorized"):
        await authorize_history(history, None)


@pytest.mark.asyncio
async def test_history_revalidates_every_call_and_keeps_native_part_order() -> None:
    first = _part(PNG, "image/png")
    second = _part(GIF, "image/gif")
    history = [
        messages.Message(
            role="user",
            parts=[
                messages.TextPart(text="before"), first,
                messages.TextPart(text="middle"), second,
            ],
        )
    ]
    authorized: list[messages.FilePart] = []

    async def authorize(part: messages.FilePart) -> None:
        authorized.append(part)

    checked_once = await authorize_history(history, authorize)
    checked_twice = await authorize_history(checked_once, authorize)

    assert checked_once is not history
    assert checked_once[0].parts == history[0].parts
    assert checked_twice[0].parts == history[0].parts
    assert authorized == [first, second, first, second]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("data", "media_type"),
    [
        (b"not an image", "image/png"),
        (PNG, "image/jpeg"),
        ("not-base64", "image/png"),
        (base64.urlsafe_b64encode(PNG).decode("ascii"), "image/png"),
        (f"data:image/jpeg;base64,{base64.b64encode(PNG).decode('ascii')}", "image/png"),
    ],
)
async def test_image_payload_and_mime_validation_rejects_bad_data(
    data: bytes | str, media_type: str
) -> None:
    async def loader(_: str) -> messages.FilePart:
        return _part(data, media_type)

    with pytest.raises(ServiceError):
        await load_authorized_file("screen", load=loader, authorize=_no_op)


@pytest.mark.asyncio
async def test_image_and_batch_limits_are_strict() -> None:
    tiny = MediaLimits(max_image_bytes=len(PNG), max_batch_bytes=len(PNG), max_images=1)

    async def loader(_: str) -> messages.FilePart:
        return _part(PNG)

    with pytest.raises(ServiceError):
        await load_authorized_file(
            "screen", load=loader, authorize=_no_op,
            limits=MediaLimits(max_image_bytes=len(PNG) - 1),
        )

    history = [messages.Message(role="user", parts=[_part(PNG), _part(PNG)])]
    with pytest.raises(ServiceError):
        await authorize_history(history, _no_op, tiny)

    one = [messages.Message(role="user", parts=[_part(PNG)])]
    limits = MediaLimits(
        max_image_bytes=len(PNG), max_batch_bytes=len(PNG) - 1, max_images=1
    )
    with pytest.raises(ServiceError):
        await authorize_history(one, _no_op, limits)


@pytest.mark.asyncio
async def test_data_url_and_base64_roundtrip_preserve_original_sdk_part() -> None:
    encoded = base64.b64encode(PNG).decode("ascii")
    base64_part = _part(encoded)
    data_url_part = _part(f"data:image/png;base64,{encoded}")
    history = [messages.Message(role="user", parts=[base64_part, data_url_part])]
    seen: list[messages.FilePart] = []

    async def authorize(part: messages.FilePart) -> None:
        seen.append(part)

    result = await authorize_history(history, authorize)
    assert result[0].parts == history[0].parts
    assert seen == [base64_part, data_url_part]

    persisted = _part(PNG).model_dump(mode="json")
    assert persisted["data"] == encoded
    restored = messages.FilePart.model_validate(persisted)
    restored_history = await authorize_history(
        [messages.Message(role="user", parts=[restored])], authorize
    )
    assert restored_history[0].parts[0].data == encoded


@pytest.mark.asyncio
async def test_expected_sha256_is_host_supplied_not_provider_metadata() -> None:
    part = _part(PNG)
    digest = hashlib.sha256(PNG).hexdigest()
    calls: list[messages.FilePart] = []

    async def authorize(value: messages.FilePart) -> None:
        calls.append(value)

    async def load(_: str) -> messages.FilePart:
        return part

    assert await load_authorized_file(
        "screen", load=load, authorize=authorize, expected_sha256=digest
    ) is part
    with pytest.raises(ServiceError):
        await load_authorized_file(
            "screen", load=load, authorize=authorize, expected_sha256="0" * 64
        )
    assert calls == [part]


@pytest.mark.asyncio
async def test_sdk_request_receives_true_filepart_bytes() -> None:
    provider = Scripted([done()])
    part = _part(PNG)
    history = await authorize_history(
        [messages.Message(role="user", parts=[part])], _no_op
    )

    async for _ in provider.stream(ai.Model(id="capture", provider=provider), history):
        pass

    captured = provider._seen[0][0].parts[0]
    assert isinstance(captured, messages.FilePart)
    assert captured.data == PNG
    assert isinstance(captured.data, bytes)


@pytest.mark.asyncio
async def test_sdk_adapter_without_media_authorizer_makes_no_provider_request() -> None:
    harness = setup(approval=False)
    provider = Scripted([done()])
    part = _part(PNG)

    with pytest.raises(ServiceError, match="unauthorized"):
        await harness.adapter().run(
            model(provider), [messages.Message(role="user", parts=[part])]
        )
    assert provider._seen == []


@pytest.mark.asyncio
async def test_sdk_adapter_authorizes_media_before_provider_request() -> None:
    harness = setup(approval=False)
    provider = Scripted([done()])
    part = _part(PNG)
    authorized: list[messages.FilePart] = []
    adapter = harness.adapter()

    async def authorize(value: messages.FilePart) -> None:
        authorized.append(value)

    adapter.authorize_media = authorize
    result = await adapter.run(
        model(provider), [messages.Message(role="user", parts=[part])]
    )

    assert result.status == "completed"
    captured = provider._seen[0][0].parts[0]
    assert isinstance(captured, messages.FilePart)
    assert captured.data == PNG
    assert isinstance(captured.data, bytes)
    assert len(authorized) == 2


@pytest.mark.asyncio
async def test_sdk_adapter_reauthorizes_media_on_later_model_turn() -> None:
    harness = setup(approval=False)
    provider = Scripted([turn(("read", "read-1", 1)), done()])
    part = _part(PNG)
    authorized: list[messages.FilePart] = []
    adapter = harness.adapter()

    async def authorize(value: messages.FilePart) -> None:
        authorized.append(value)

    adapter.authorize_media = authorize
    result = await adapter.run(
        model(provider), [messages.Message(role="user", parts=[part])]
    )

    assert result.status == "completed"
    assert len(provider._seen) == 2
    assert all(
        isinstance(history[0].parts[0], messages.FilePart)
        and history[0].parts[0].data == PNG
        for history in provider._seen
    )
    assert len(authorized) >= 3


@pytest.mark.asyncio
async def test_parked_checkpoint_rechecks_revocation_before_provider_or_write() -> None:
    harness = setup()
    provider = Scripted([turn(("write", "write-1", 1)), done()])
    part = _part(PNG)
    revoked = False
    authorization_count = 0
    adapter = harness.adapter()

    async def authorize(_: messages.FilePart) -> None:
        nonlocal authorization_count
        authorization_count += 1
        if revoked:
            raise ServiceError("unauthorized")

    adapter.authorize_media = authorize
    first = await adapter.run(
        model(provider), [messages.Message(role="user", parts=[part])]
    )
    assert first.status == "awaiting"
    assert len(provider._seen) == 1
    assert harness.counts["write"] == 0
    revoked = True
    approval = next(event for event in harness.store.events if event.kind == "approval_request")

    with pytest.raises(ServiceError, match="unauthorized"):
        await adapter.run(
            model(provider), resume=True, replies={approval.data["call_id"]: True}
        )
    assert len(provider._seen) == 1
    assert harness.counts["write"] == 0
    assert authorization_count >= 2

    await harness.store.set_awaiting("run")
    revoked = False
    resumed = await adapter.run(
        model(provider), resume=True, replies={approval.data["call_id"]: True}
    )
    assert resumed.status == "completed"
    assert len(provider._seen) == 2
    assert harness.counts["write"] == 1
    assert authorization_count >= 4
