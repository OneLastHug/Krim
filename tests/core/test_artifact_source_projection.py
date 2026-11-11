"""文件来源引用已提交的不可变ArtifactChunk；不读取当下磁盘正文。"""
from krim_agent.contracts import RunEvent
from krim_agent.observation_views import (
    RequestObservation,
    RequestUsageView,
    UsageView,
    _artifact_source,
    project_tools,
    project_usage,
)


def test_unclosed_request_does_not_claim_complete_run_usage():
    request = RequestObservation(id="span", step_index=0, started_at=1.0,
                                 usage=RequestUsageView(usage_final=True, usage_reports=1,
                                                       usage=UsageView(input_tokens=2,
                                                                       output_tokens=1,
                                                                       total_tokens=3)))
    result = project_usage([request])
    assert result.usage_complete is False
    assert result.totals.total_tokens == 3
    assert result.steps_missing_usage == 0
    closed = request.model_copy(update={"ended_at": 2.0, "status": "ok"})
    assert project_usage([closed]).usage_complete is True


def artifact_result():
    return {
        "name": "workspace_read", "status": "ok", "result_available": True,
        "value": {"revision_id": "11111111-1111-4111-8111-111111111111",
                  "path": "src/report.txt", "offset": 2, "length": 4,
                  "total_size": 10, "sha256": "a" * 64, "data": "private encoded body"},
    }


def test_artifact_metadata_requires_workspace_and_successful_known_tool():
    data = artifact_result()
    assert _artifact_source(data, False) is None
    source = _artifact_source(data, True)
    assert source is not None
    assert source.path == "src/report.txt"
    assert source.offset == 2 and source.length == 4
    assert source.sha256 == "a" * 64
    assert "private" not in source.model_dump_json()
    assert "data" not in source.model_dump()
    data["name"] = "echo"
    assert _artifact_source(data, True) is None
    data["name"], data["status"] = "workspace_read", "failed"
    assert _artifact_source(data, True) is None


def test_invalid_file_range_or_absolute_path_is_not_presented_as_provenance():
    data = artifact_result()
    data["value"]["length"] = 20
    assert _artifact_source(data, True) is None
    data = artifact_result()
    data["value"]["path"] = "/current/local/file.txt"
    assert _artifact_source(data, True) is None


def test_missing_start_preserves_zero_based_step_and_immutable_reference():
    data = artifact_result() | {"call_id": "internal", "provider_call_id": "provider",
                                "span_id": "span", "step_index": 0}
    event = RunEvent(run_id="run", seq=0, event_id="event", kind="tool_end", data=data)
    tools = project_tools([event], workspace=True)
    assert len(tools) == 1
    assert tools[0].step_index == 0
    assert tools[0].provider_call_id == "provider"
    assert tools[0].duration_ms is None
    assert tools[0].source.revision_id == data["value"]["revision_id"]
