import asyncio

import httpx
import pytest
from pydantic import ValidationError

from krim_agent.app import LocalApp
from krim_agent.app_sdk import RemoteHostHandle
from krim_agent.link import LinkNavigate, link_origin, normalize_link
from krim_agent.protocol.models import ProtocolEnvelope, StreamFrame


@pytest.mark.parametrize(
    "url",
    [
        "javascript:alert(1)",
        "data:text/html,x",
        "file:///etc/passwd",
        "https:///missing",
        "https://user:pass@example.com/a",
        "https://user@example.com/",
        "https://example.com:bad/",
        "https://example.com:99999/",
        "https://example.com:/",
        "https://exa_mple.com/",
        "https://example.com\\evil",
        "https://example.com/a\n",
        "https://example.com/%0a",
        "https://example.com/%5cpath",
        "https://%65xample.com/",
        "%6aavascript:alert(1)",
    ],
)
def test_unsafe_urls_are_rejected_by_shared_python_boundary(url):
    with pytest.raises(ValueError):
        normalize_link(url)
    with pytest.raises(ValidationError):
        LinkNavigate(url=url)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("HTTPS://EXAMPLE.COM:443/docs?q=1#part", "https://example.com/docs?q=1#part"),
        ("http://example.com", "http://example.com/"),
        ("https://例子.测试/路径", "https://xn--fsqu00a.xn--0zwm56d/%E8%B7%AF%E5%BE%84"),
        ("http://[::1]:8080/a#part", "http://[::1]:8080/a#part"),
    ],
)
def test_web_urls_normalize_host_port_unicode_and_preserve_query_fragment(raw, expected):
    assert normalize_link(raw) == expected
    assert LinkNavigate(url=raw).url == expected


def test_relative_resolution_requires_explicit_base_and_preserves_fragment():
    with pytest.raises(ValueError):
        normalize_link("../docs")
    assert (
        normalize_link("../docs#part", base_url="https://example.com/ui/")
        == "https://example.com/docs#part"
    )
    assert link_origin("https://example.com:443/a") == ("https", "example.com", 443)


async def test_resolve_api_enforces_same_origin_base_and_existing_auth(api):
    resolved = await api.post(
        "/v1/links/resolve", json={"url": "../docs#part", "base_url": "http://127.0.0.1:8765/ui/"}
    )
    assert resolved.status_code == 200 and resolved.json() == {
        "url": "http://127.0.0.1:8765/docs#part",
        "external": False,
    }
    external = await api.post("/v1/links/resolve", json={"url": "https://example.com/a#b"})
    assert external.status_code == 200 and external.json()["external"] is True
    for payload in [
        {"url": "javascript:alert(1)"},
        {"url": "../path", "base_url": "https://evil.example/ui/"},
    ]:
        assert (await api.post("/v1/links/resolve", json=payload)).status_code == 422


async def test_notifications_validate_canonicalize_deduplicate_and_preserve_direction(api):
    sid = (await api.post("/v1/host/sessions")).json()["id"]
    payload = {
        "protocol_version": 2,
        "kind": "notify",
        "id": "link-one",
        "channel": "link/navigate",
        "data": {"url": "HTTPS://EXAMPLE.COM:443/docs#part", "message_id": "m"},
    }
    assert (await api.post(f"/v1/host/sessions/{sid}/notify", json=payload)).status_code == 409
    await api.post(f"/v1/host/sessions/{sid}/ready")
    assert (await api.post(f"/v1/host/sessions/{sid}/notify", json=payload)).status_code == 200
    assert (await api.post(f"/v1/host/sessions/{sid}/notify", json=payload)).status_code == 409
    bad = {**payload, "id": "bad", "data": {"url": "javascript:alert(1)"}}
    assert (await api.post(f"/v1/host/sessions/{sid}/notify", json=bad)).status_code == 422
    assert (
        await api.post(
            f"/v1/host/sessions/{sid}/commands", json={"envelope": {**payload, "kind": "call"}}
        )
    ).status_code == 422
    await api.delete(f"/v1/host/sessions/{sid}")
    assert (
        await api.post(f"/v1/host/sessions/{sid}/notify", json={**payload, "id": "closed"})
    ).status_code == 410


async def test_local_app_typed_link_callback_ignores_result_and_unregisters(runtime):
    local = LocalApp(runtime)
    received, done = [], asyncio.Event()

    def clicked(value):
        received.append(value)
        done.set()
        return object()

    remove = local.handle.on_link(clicked)
    try:
        local.navigate_link(LinkNavigate(url="https://example.com/docs#part", message_id="m"))
        await asyncio.wait_for(done.wait(), 3)
        assert received[0].url == "https://example.com/docs#part"
        remove()
        local.navigate_link(LinkNavigate(url="https://example.com/ignored"))
        await asyncio.sleep(0.02)
        assert len(received) == 1
    finally:
        await local.close()


async def test_remote_host_link_callback_sse_replay_is_consumed_once():
    calls = []
    envelope = ProtocolEnvelope(
        kind="notify",
        id="one",
        channel="link/navigate",
        data={"url": "https://example.com/docs", "message_id": "m"},
    )
    frame = StreamFrame(
        stream_id="session",
        seq=0,
        kind="chunk",
        data=envelope.model_dump(mode="json", exclude={"stream_id", "seq"}),
    )

    def transport(request):
        if request.url.path.endswith("/events"):
            return httpx.Response(
                200,
                text="data: " + frame.model_dump_json() + "\n\n",
                headers={"content-type": "text/event-stream"},
            )
        return httpx.Response(
            200,
            json={
                "id": "session",
                "protocol_version": 2,
                "connected": True,
                "closed": False,
                "expires_at": 9999999999.0,
            },
        )

    async with RemoteHostHandle(
        "http://127.0.0.1", "session", transport=httpx.MockTransport(transport)
    ) as host:
        host.on_link(lambda value: calls.append(value))
        assert len([item async for item in host.events()]) == 1
        assert len([item async for item in host.events()]) == 1
    assert len(calls) == 1 and calls[0].url == "https://example.com/docs"
