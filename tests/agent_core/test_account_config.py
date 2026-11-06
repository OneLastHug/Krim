"""账号部署配置不进入执行目标预算，公开名称不携带私有接入信息。"""

import pytest
from pydantic import ValidationError

from krim_agent.config import SESSION_FIELDS, Settings


def test_account_config_defaults_and_environment(monkeypatch):
    assert Settings().identity_mode == "auto"
    assert Settings().gateway_display_name is None
    monkeypatch.setenv("KRIM_AGENT_IDENTITY_MODE", "managed")
    monkeypatch.setenv("KRIM_AGENT_GATEWAY_DISPLAY_NAME", "  团队网关  ")
    settings = Settings.from_env()
    assert settings.identity_mode == "managed"
    assert settings.gateway_display_name == "团队网关"
    assert {"identity_mode", "gateway_display_name"} <= SESSION_FIELDS


@pytest.mark.parametrize("name", ["", "   ", "a\nb", "a\x00b", "a" + chr(8238) + "b", "x" * 65])
def test_gateway_label_rejects_empty_control_or_oversize(name):
    with pytest.raises(ValidationError):
        Settings(gateway_display_name=name)


def test_unknown_identity_mode_is_not_a_legacy_fallback():
    with pytest.raises(ValidationError):
        Settings(identity_mode="unknown")
