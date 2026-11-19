"""纯上下文合约与构建器；导入不创建资源。"""

from .counter import (
    COMPACTION_TRIGGER_TOKENS,
    CONTEXT_WINDOW_TOKENS,
    DEFAULT_COUNTER,
    HeuristicTokenCounter,
    TokenCounter,
    UsageCalibratedCounter,
    counter_for_model,
)
from .types import ContextPolicy, ContextSnapshot, SessionContext, upgrade_to_token_window

__all__ = [
    "COMPACTION_TRIGGER_TOKENS",
    "CONTEXT_WINDOW_TOKENS",
    "DEFAULT_COUNTER",
    "ContextPolicy",
    "ContextSnapshot",
    "HeuristicTokenCounter",
    "SessionContext",
    "TokenCounter",
    "UsageCalibratedCounter",
    "counter_for_model",
    "upgrade_to_token_window",
]
