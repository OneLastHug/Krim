"""Python 生成 SDK；参考 Vercel AI SDK 的语义，运行时全部为 Python。"""

from .text import PythonAI, SDKOutputError, StreamTextResult
from .types import (
    ContextCompactPart,
    FinishReason,
    GenerateResult,
    InteractionPart,
    SDKPart,
    SkillLoadedPart,
    StepFinish,
    StepResult,
    StreamError,
    StreamFinish,
    StreamStart,
    TextDelta,
    ToolCallPart,
    ToolDiscoveryPart,
    ToolResultPart,
)

__all__ = [
    "ContextCompactPart",
    "InteractionPart",
    "ToolDiscoveryPart",
    "SkillLoadedPart",
    "FinishReason",
    "GenerateResult",
    "PythonAI",
    "SDKOutputError",
    "SDKPart",
    "StepFinish",
    "StepResult",
    "StreamError",
    "StreamFinish",
    "StreamStart",
    "StreamTextResult",
    "TextDelta",
    "ToolCallPart",
    "ToolResultPart",
]
