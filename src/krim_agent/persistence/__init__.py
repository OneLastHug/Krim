"""M1 SQLite 持久化边界。"""

from typing import TYPE_CHECKING

from krim_agent.task_state import TaskItemView, TaskMutation, TaskSnapshot

from .attachments import AttachmentRepository
from .change_lists import ChangeListRepository
from .checkpoints import CheckpointRepository
from .database import (
    ContextSummaryRow,
    Database,
    InteractionRow,
    KrimRepository,
    OAuthTokenRow,
    WorkflowCheckpointRow,
    WorkflowRequestRow,
)
from .eval_models import EvalEventRow, EvalItemRow, EvalJobRow
from .tasks import AgentTaskStore
from .traces import TraceStore
from .workflow import WorkflowCheckpointError, WorkflowCheckpointStore
from .workspace_edits import WorkspaceEditStore

__all__ = [
    "Database",
    "KrimRepository",
    "OAuthTokenRow",
    "WorkflowCheckpointRow",
    "WorkflowRequestRow",
    "ContextSummaryRow",
    "InteractionRow",
    "EvalEventRow",
    "EvalItemRow",
    "EvalJobRow",
    "EvaluationStore",
    "ChangeListRepository",
    "CheckpointRepository",
    "AttachmentRepository",
    "WorkflowCheckpointError",
    "WorkflowCheckpointStore",
    "TraceStore",
    "AgentTaskStore",
    "WorkspaceEditStore",
    "TaskItemView",
    "TaskMutation",
    "TaskSnapshot",
]

if TYPE_CHECKING:
    from .eval_store import EvaluationStore


def __getattr__(name: str) -> object:
    if name == "EvaluationStore":
        from .eval_store import EvaluationStore

        return EvaluationStore
    raise AttributeError(name)
