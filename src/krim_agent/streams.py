"""多路订阅合约；每个 Run 保留原序号和来源，不创建第二份业务日志。"""

from __future__ import annotations

from typing import Literal

from pydantic import Field, model_validator

from krim_agent.protocol.domain import RunView, StreamEventView
from krim_agent.protocol.models import StreamFrame, StrictModel


class RunCursor(StrictModel):
    run_id: str = Field(min_length=1, max_length=128)
    topic_id: str = Field(min_length=1, max_length=128)
    next_seq: int = Field(default=0, ge=0, le=9007199254740991)


class MultiplexRequest(StrictModel):
    sources: list[RunCursor] = Field(min_length=1, max_length=64)

    @model_validator(mode="after")
    def unique_runs(self) -> MultiplexRequest:
        if len({source.run_id for source in self.sources}) != len(self.sources):
            raise ValueError("多路订阅不能重复 Run")
        return self


class RunStreamFrame(StrictModel):
    type: Literal["run-frame"] = "run-frame"
    topic_id: str
    frame: StreamFrame


class ActiveRunView(StrictModel):
    run: RunView
    events: StreamEventView
