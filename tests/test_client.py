import json
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest

from krim_agent.cli import main
from krim_agent.client import ClientError, KrimClient
from krim_agent.protocol.models import StreamFrame


def sse(frame):
    return f"id: {frame.seq}\nevent: {frame.kind}\ndata: {frame.model_dump_json()}\n\n"


def test_client_sse_and_authorization():
    run_id = str(uuid4())
    frames = [
        StreamFrame(stream_id=run_id, seq=0, kind="start"),
        StreamFrame(stream_id=run_id, seq=1, kind="chunk", data={"text": "你好"}),
        StreamFrame(stream_id=run_id, seq=2, kind="end", data={"status": "completed"}),
    ]

    def handler(request):
        assert request.headers["Authorization"] == "Bearer local-test-key"
        assert request.url.params["from_seq"] == "0"
        return httpx.Response(200, text="".join(sse(frame) for frame in frames))

    with KrimClient(api_key="local-test-key", transport=httpx.MockTransport(handler)) as client:
        actual = list(client.events(run_id))
    assert [frame.seq for frame in actual] == [0, 1, 2]
    assert actual[1].data == {"text": "你好"}


@pytest.mark.parametrize("mode", ["wrong_seq", "missing_end", "http_failure"])
def test_client_rejects_invalid_or_incomplete_streams(mode):
    run_id = str(uuid4())
    frame = StreamFrame(stream_id=run_id, seq=1 if mode == "wrong_seq" else 0, kind="start")

    def handler(_request):
        return httpx.Response(401 if mode == "http_failure" else 200, text=sse(frame))

    with KrimClient(transport=httpx.MockTransport(handler)) as client, pytest.raises(ClientError):
        list(client.events(run_id))


def test_cli_version_and_no_key_remote_binding(monkeypatch, capsys):
    monkeypatch.setattr("sys.argv", ["krim", "version"])
    main()
    assert "Krim Agent" in capsys.readouterr().out
    monkeypatch.delenv("KRIM_AGENT_API_KEY", raising=False)
    monkeypatch.setattr("sys.argv", ["krim", "serve", "--host", "0.0.0.0"])
    with pytest.raises(SystemExit) as error:
        main()
    assert error.value.code == 2


def test_client_send_preserves_idempotency_key():
    topic_id = str(uuid4())
    run_id = str(uuid4())

    def handler(request):
        assert json.loads(request.content)["idempotency_key"] == "retry-key"
        return httpx.Response(
            202,
            json={
                "id": run_id,
                "topic_id": topic_id,
                "input_message_id": str(uuid4()),
                "idempotency_key": "retry-key",
                "status": "running",
                "stop_requested": False,
                "error": None,
                "created_at": "2026-10-02T00:00:00Z",
                "updated_at": "2026-10-02T00:00:00Z",
                "finished_at": None,
            },
        )

    with KrimClient(transport=httpx.MockTransport(handler)) as client:
        run = client.send(topic_id, "hello", "retry-key")
    assert run.id == run_id


def test_cli_shows_persisted_reply_if_run_already_finished(monkeypatch, capsys):
    run = SimpleNamespace(id="run-fast", status="completed")

    class CompletedClient:
        def __init__(self, *_args):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            pass

        def send(self, *_args):
            return run

        def messages(self, _topic):
            return [SimpleNamespace(run_id=run.id, role="assistant", content="已保存的回复")]

    monkeypatch.setattr("krim_agent.cli.KrimClient", CompletedClient)
    monkeypatch.setattr("sys.argv", ["krim", "chat", "hello", "--topic", "topic-fast"])
    main()
    assert "已保存的回复" in capsys.readouterr().out
