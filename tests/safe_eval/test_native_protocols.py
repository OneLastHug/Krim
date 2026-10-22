"""仅合成原生JSON向量；不启动CLI，不使用模型、认证或网络。"""

import json

import pytest

from krim_agent.evaluation.adapters.claude_events import ClaudeEvents
from krim_agent.evaluation.adapters.codex_events import CodexEvents


def native_frames(family):
    if family == "codex":
        return [
            {"type": "thread.started", "thread_id": "synthetic-thread"},
            {"type": "turn.started"},
            {
                "type": "item.started",
                "item": {
                    "id": "tool-1",
                    "type": "command_execution",
                    "command": "synthetic unittest",
                    "status": "in_progress",
                },
            },
            {
                "type": "item.updated",
                "item": {
                    "id": "tool-1",
                    "type": "command_execution",
                    "command": "synthetic unittest",
                    "status": "in_progress",
                },
            },
            {
                "type": "item.completed",
                "item": {
                    "id": "tool-1",
                    "type": "command_execution",
                    "command": "synthetic unittest",
                    "status": "completed",
                    "exit_code": 0,
                    "aggregated_output": "ok",
                },
            },
            {
                "type": "turn.completed",
                "usage": {"input_tokens": 10, "cached_input_tokens": 3, "output_tokens": 2},
            },
        ]
    return [
        {"type": "system", "subtype": "init", "session_id": "synthetic-session"},
        {
            "type": "assistant",
            "message": {
                "content": [
                    {
                        "type": "tool_use",
                        "id": "tool-1",
                        "name": "Bash",
                        "input": {"command": "synthetic unittest"},
                    }
                ]
            },
        },
        {
            "type": "user",
            "message": {
                "content": [
                    {
                        "type": "tool_result",
                        "tool_use_id": "tool-1",
                        "content": "ok",
                        "is_error": False,
                    }
                ]
            },
        },
        {
            "type": "result",
            "subtype": "success",
            "is_error": False,
            "usage": {
                "input_tokens": 7,
                "cache_read_input_tokens": 3,
                "cache_creation_input_tokens": 0,
                "output_tokens": 2,
            },
        },
    ]


def encoded_frames(frames):
    return b"".join(json.dumps(frame, ensure_ascii=False).encode() + b"\n" for frame in frames)


@pytest.mark.parametrize("family", ["codex", "claude"])
@pytest.mark.parametrize(
    "case",
    ["valid", "partial", "last_bad", "missing_end", "duplicate_id", "wrong_schema", "oversize"],
)
def test_native_frames_release_only_completed_groups(family, case):
    codec = CodexEvents() if family == "codex" else ClaudeEvents()
    frames = native_frames(family)
    if case == "missing_end":
        frames = frames[:-1]
    if case == "duplicate_id":
        frames.insert(-1, frames[-2])
    if case == "wrong_schema":
        frames[0] = (
            {"type": "thread.started"}
            if family == "codex"
            else {"type": "system", "subtype": "init"}
        )
    data = encoded_frames(frames)
    if case == "last_bad":
        data += b'{"type":bad}\n'
    if case == "partial":
        data = data[:-5]
    if case == "oversize":
        data = b" " * 65537 + b"\n" + data
    try:
        # 分片边界不能改变协议结论。
        for index in range(0, len(data), 17):
            codec.feed(data[index : index + 17])
    except ValueError:
        pass
    result = codec.finish()
    assert result["family"] == family
    assert result["complete"] is (case == "valid")
    assert (result["reason"] is None) is (case == "valid")
    if case == "valid":
        assert len(result["tools"]) == 1
        assert result["tools"][0]["id"] == "tool-1"
        assert result["tools"][0]["exit_code"] == (0 if family == "codex" else None)


@pytest.mark.parametrize("family", ["codex", "claude"])
def test_native_usage_unknown_and_cache_accounting(family):
    codec = CodexEvents() if family == "codex" else ClaudeEvents()
    codec.feed(encoded_frames(native_frames(family)))
    result = codec.finish()
    assert result["usage"]["total_input_tokens"] == 10
    assert result["usage"]["total_tokens"] == 12
    assert result["cost"] is None and result["request_count"] is None
    assert result["usage_complete"] is True
    frames = native_frames(family)
    frames[-1].pop("usage")
    codec = CodexEvents() if family == "codex" else ClaudeEvents()
    codec.feed(encoded_frames(frames))
    result = codec.finish()
    assert result["complete"] is True
    assert result["usage"] is None and result["usage_complete"] is False


@pytest.mark.parametrize("family", ["codex", "claude"])
def test_proposal_does_not_mean_execution(family):
    frames = native_frames(family)[: 3 if family == "codex" else 2]
    codec = CodexEvents() if family == "codex" else ClaudeEvents()
    codec.feed(encoded_frames(frames))
    result = codec.finish()
    assert result["complete"] is False
    assert result["tools"] == []
    assert result["proposals"] == [{"id": "tool-1", "kind": "command"}]


@pytest.mark.parametrize("case", ["split_chunk", "json_escape", "stderr"])
async def test_credentials_redacted_before_persistence(tmp_path, case):
    import os
    import sys

    from krim_agent.evaluation.adapters.process import ProcessOwner, StreamRedactor
    from krim_agent.evaluation.domain import PreparedTrial, TrialSpec

    secret = 'SYNTHETIC-SECRET-中文"VALUE'
    if case == "stderr":
        owner = ProcessOwner()
        lease = object()
        prepared = PreparedTrial(TrialSpec("redaction", "synthetic", 0, {}, {}), str(tmp_path))

        async def observe(job):
            return await owner.observe(job, secrets=(secret,))

        job = await owner.spawn(
            prepared,
            [sys.executable, "-I", "-B", "-c", "import sys;sys.stderr.write(sys.stdin.read())"],
            {k: os.environ[k] for k in ["PATH", "LANG", "LD_LIBRARY_PATH"] if k in os.environ},
            lease,
            {},
            observe,
            stdin_data=secret.encode(),
        )
        try:
            facts = await job.collector
            result = facts["stderr"]
            assert facts["wait_returncode"] == 0
            assert facts["stdout_eof"] and facts["stderr_eof"] and facts["pidfd_closed"]
        finally:
            closed = await owner.close()
            assert closed.closed and job.pidfd == -1
    else:
        source = (
            json.dumps({"value": secret}, ensure_ascii=True).encode()
            if case == "json_escape"
            else secret.encode()
        )
        redactor = StreamRedactor((secret,))
        data = b"".join(redactor.feed(source[i : i + 1]) for i in range(len(source)))
        result = (data + redactor.feed(b"", final=True)).decode()
    path = tmp_path / "persisted-redacted.txt"
    path.write_text(result)
    assert secret not in path.read_text()
    assert json.dumps(secret, ensure_ascii=True)[1:-1] not in path.read_text()
    assert "[REDACTED]" in path.read_text()


def test_fixture_profile_cannot_claim_real_native():
    from pathlib import Path

    from krim_agent.evaluation.adapters.profiles import NativeProfile

    with pytest.raises(ValueError, match="native_not_ready"):
        NativeProfile("codex-cli", "codex", Path("not-read"), {}, {}, {}, None)


def test_old_observation_dump_keeps_exact_keys():
    from krim_agent.contracts import encoded, fingerprint
    from krim_agent.evaluation.domain import Observation, neutral_encoded, neutral_fingerprint

    expected = {
        "execution_ref": "old",
        "exit_code": 0,
        "stdout": "ok",
        "stderr": "",
        "artifacts": {},
        "process": {"known": True},
        "cleanup": "closed",
        "token_usage": None,
        "cost": None,
        "usage_complete": False,
    }
    actual = Observation.model_validate(expected).model_dump(mode="json")
    assert actual == expected
    assert neutral_encoded(actual) == encoded(expected)
    assert neutral_fingerprint(actual) == fingerprint(expected)
