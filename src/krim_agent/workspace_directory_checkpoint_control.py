"""目录模型只读回程；原严格CheckpointQuery/Lookup选择器继续复用。"""

from typing import Literal

from pydantic import Field

from krim_agent.protocol.models import StrictModel
from krim_agent.workspace_directory_checkpoint import DirectoryCheckpointView


class DirectoryCheckpointQueryResult(StrictModel):
    kind: Literal["id", "key", "history"]
    checkpoint: DirectoryCheckpointView | None = None
    entries: list[DirectoryCheckpointView] = Field(default_factory=list, max_length=32)
