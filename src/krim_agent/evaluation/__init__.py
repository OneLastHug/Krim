"""Krim Agent 本地 eval（评估）SDK。"""

from .files import load_dataset, load_report, parse_json, save_dataset, save_report
from .reports import compare_reports
from .runner import EvalAttempt, EvalTarget, EvaluationRunner, evaluate, rescore
from .scorers import (
    CallableScorer,
    ContainsScorer,
    EvalScorer,
    ExactMatchScorer,
    JsonMatchScorer,
    JsonSchemaScorer,
    SuccessScorer,
    ToolCallsScorer,
)
from .targets import AgentTarget, CallableTarget, GenerationTarget, generation_output
from .types import (
    EvalCase,
    EvalCaseResult,
    EvalComparison,
    EvalConfig,
    EvalDataset,
    EvalOutput,
    EvalReport,
    EvalScore,
    EvalScorerInfo,
    EvalSummary,
    EvalTargetInfo,
    EvalToolCall,
)

__all__ = [
    "AgentTarget",
    "CallableTarget",
    "CallableScorer",
    "ContainsScorer",
    "EvalCase",
    "EvalAttempt",
    "EvalCaseResult",
    "EvalComparison",
    "EvalConfig",
    "EvalDataset",
    "EvalOutput",
    "EvalReport",
    "EvalScore",
    "EvalScorer",
    "EvalScorerInfo",
    "EvalSummary",
    "EvalTargetInfo",
    "EvalTarget",
    "EvalToolCall",
    "EvaluationRunner",
    "GenerationTarget",
    "ExactMatchScorer",
    "JsonMatchScorer",
    "JsonSchemaScorer",
    "SuccessScorer",
    "ToolCallsScorer",
    "compare_reports",
    "evaluate",
    "generation_output",
    "load_dataset",
    "load_report",
    "parse_json",
    "rescore",
    "save_dataset",
    "save_report",
]
