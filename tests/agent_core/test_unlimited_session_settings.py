"""Run totals are optional; model-request capacity remains provider-declared."""

from types import SimpleNamespace

import pytest

from krim_agent.config import Settings


def test_default_session_and_run_totals_have_no_product_quota():
    settings = Settings()
    for name in (
        "max_steps", "max_tool_calls", "max_active_runs", "max_events", "max_output_bytes",
        "max_input_bytes", "max_request_bytes", "run_timeout", "model_timeout", "tool_timeout",
        "session_max_queued", "session_max_open", "session_max_turns",
        "session_max_history_bytes", "session_queue_timeout", "context_max_bytes",
        "context_max_messages",
    ):
        assert getattr(settings, name) is None, name
    assert settings.history_compaction is True
    assert settings.development_history_compaction is True


def test_provider_capacity_is_not_capped_by_an_unrelated_host_default():
    provider = SimpleNamespace(config=SimpleNamespace(max_request_bytes=8_000_000))
    assert Settings().request_capacity(provider) == 8_000_000
    assert Settings(max_request_bytes=4_000_000).request_capacity(provider) == 4_000_000
    assert Settings(max_request_bytes=16_000_000).request_capacity(provider) == 8_000_000


@pytest.mark.parametrize("name", [
    "max_steps", "max_tool_calls", "max_active_runs", "max_events", "max_output_bytes",
    "max_input_bytes", "max_request_bytes", "session_max_open", "session_max_queued",
    "session_max_turns", "session_max_history_bytes", "context_max_bytes",
    "context_max_messages",
])
def test_explicit_large_host_limits_remain_valid(name):
    assert getattr(Settings(**{name: 100_000_000}), name) == 100_000_000


@pytest.mark.parametrize("name", ["run_timeout", "model_timeout", "tool_timeout",
                                  "session_queue_timeout"])
def test_explicit_large_deadlines_remain_valid(name):
    assert getattr(Settings(**{name: 100_000_000}), name) == 100_000_000


def test_large_host_system_prompt_is_not_a_storage_quota():
    prompt = "项目资料" * 10_000
    assert Settings(system_prompt=prompt).system_prompt == prompt
