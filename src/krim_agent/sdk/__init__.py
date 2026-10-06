"""Python 生成 SDK；参考 Vercel AI SDK 的语义，运行时全部为 Python。"""

from krim_agent.evaluation import EvalCase, EvalConfig, EvalDataset, EvalReport
from krim_agent.evaluation.remote import AsyncEvalClient, SyncEvalClient
from krim_agent.evaluation.service import EvalRunHandle

from .text import PythonAI, SDKOutputError, StreamTextResult
from .types import (
    ContextBudgetPart,
    ContextCompactPart,
    ContextNoticePart,
    ContextProposalPart,
    FinishReason,
    GenerateResult,
    InteractionPart,
    ModelStepPart,
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
    UsageBudgetPart,
    UsagePart,
)

__all__ = [
    "EvalRunHandle",
    "AsyncEvalClient",
    "SyncEvalClient",
    "EvalCase",
    "EvalConfig",
    "EvalDataset",
    "EvalReport",
    "ContextBudgetPart",
    "ModelStepPart",
    "ContextCompactPart",
    "ContextProposalPart",
    "ContextNoticePart",
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
    "UsagePart",
    "UsageBudgetPart",
]
