"""Listener ownership tests; actual TLS/browser transport is verified separately."""

import asyncio
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from pydantic import ValidationError

from krim_agent.contracts import ServiceError
from krim_agent.development import serving
from krim_agent.development.configuration import DevelopmentListenerConfiguration


def listener(tmp_path):
    return DevelopmentListenerConfiguration(
        control_certificate=tmp_path / "control.crt",
        control_private_key=tmp_path / "control.key",
        preview_certificate=tmp_path / "preview.crt",
        preview_private_key=tmp_path / "preview.key",
    )


@pytest.mark.parametrize("change", [
    {"control_host": "0.0.0.0"},
    {"preview_host": "example.com"},
    {"preview_port": 8765},
    {"control_private_key": "relative.key"},
    {"preview_port": True},
])
def test_listener_configuration_rejects_unsafe_binding(tmp_path, change):
    with pytest.raises(ValidationError):
        DevelopmentListenerConfiguration(**{**listener(tmp_path).model_dump(), **change})


def test_tls_private_key_checks(tmp_path):
    path = tmp_path / "key"
    path.write_text("fixture, not a real key")
    path.chmod(0o600)
    serving._tls_file(path, private=True)
    path.chmod(0o640)
    with pytest.raises(ServiceError, match="development_tls_invalid"):
        serving._tls_file(path, private=True)
    link = tmp_path / "link"
    link.symlink_to(path)
    with pytest.raises(ServiceError, match="development_tls_invalid"):
        serving._tls_file(link, private=False)


def test_listener_configs_separate_apps_and_disable_proxy_trust(tmp_path, monkeypatch):
    config = listener(tmp_path)
    for field in ("control_certificate", "control_private_key",
                  "preview_certificate", "preview_private_key"):
        path = getattr(config, field)
        path.write_text("fixture")
        path.chmod(0o600)
    control_app, preview_app = object(), object()
    application = SimpleNamespace(development=SimpleNamespace(preview_app=preview_app))
    monkeypatch.setattr(serving, "create_app", lambda **kwargs: control_app)
    monkeypatch.setattr(serving.uvicorn.Config, "load", lambda self: None)
    control, preview = serving.listener_configs(application, config)
    assert control.app is control_app and preview.app is preview_app
    assert control.ws == "none" and preview.ws == "websockets-sansio"
    for value in (control, preview):
        assert value.is_ssl
        assert value.proxy_headers is False
        assert value.access_log is False
        assert value.timeout_graceful_shutdown == 5
        assert value.log_level == "critical"


@pytest.mark.parametrize("ending", ["stop", "failure", "early_exit", "cancel", "bind_failure"])
async def test_listener_pair_shutdown_and_partial_failure(tmp_path, monkeypatch, ending):
    stop = asyncio.Event()
    ready = asyncio.Event()
    finished = []
    servers = []
    sockets = []
    config = listener(tmp_path)
    application = SimpleNamespace(development=SimpleNamespace(configuration=SimpleNamespace(
        preview=SimpleNamespace(listener=config),
    )))
    monkeypatch.setattr(serving, "listener_configs", lambda *args: ("control", "preview"))

    class Server:
        def __init__(self, name):
            self.name = name
            self.exit = asyncio.Event()
            servers.append(self)

        @property
        def should_exit(self):
            return self.exit.is_set()

        @should_exit.setter
        def should_exit(self, value):
            if value:
                self.exit.set()

        async def serve(self, *, sockets):
            if self.name == "preview":
                ready.set()
            try:
                if self.name == "preview" and ending == "failure":
                    raise SystemExit(3)
                if self.name == "preview" and ending == "early_exit":
                    return
                await self.exit.wait()
            finally:
                finished.append(self.name)

    def bind(name):
        if name == "preview" and ending == "bind_failure":
            raise OSError("fixture occupied port")
        sock = Mock()
        sockets.append(sock)
        return sock

    monkeypatch.setattr(serving, "_Server", Server)
    monkeypatch.setattr(serving, "_bind", bind)
    task = asyncio.create_task(serving.serve_development(application, stop=stop))
    if ending == "bind_failure":
        with pytest.raises(OSError):
            await task
        assert finished == []
    else:
        await asyncio.wait_for(ready.wait(), 2)
        if ending == "stop":
            stop.set()
            await asyncio.wait_for(task, 2)
        elif ending == "cancel":
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        else:
            with pytest.raises(ServiceError, match="development_listener_"):
                await asyncio.wait_for(task, 2)
        assert sorted(finished) == ["control", "preview"]
    assert all(server.should_exit for server in servers)
    for sock in sockets:
        sock.close.assert_called_once()
