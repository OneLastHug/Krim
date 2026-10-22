"""只从同一提交快照投影结果；不修复、不初始化、不重放执行。"""

from typing import cast

from pydantic import JsonValue

from krim_agent.contracts import RunFacts, ToolObservation
from krim_agent.usage import TokenUsage

from .contracts import RunSnapshot


def project_facts(snapshot: RunSnapshot) -> RunFacts:
    observations: dict[str, ToolObservation] = {}
    text = ""
    steps = 0
    usages: dict[int, tuple[TokenUsage, bool]] = {}
    ended: set[int] = set()
    for event in snapshot.events:
        data = event.data
        if event.kind == "text":
            text += str(data["text"])
        elif event.kind == "model_start":
            steps += 1
        elif event.kind == "model_end":
            ended.add(cast(int, data["step_index"]))
        elif event.kind == "usage":
            usages[cast(int, data["step_index"])] = (
                TokenUsage.model_validate(data["usage"]),
                data["final"] is True,
            )
        elif event.kind in {"tool_start", "tool_end"}:
            item = ToolObservation.model_validate(data)
            observations[item.call_id] = item
    if snapshot.work is not None:
        # 只合并同事务持久receipt；没有end仍不补span或假成功。
        for event in snapshot.events:
            if event.kind == "workspace_started":
                call_id = cast(str, event.data["call_id"])
                old = observations.get(call_id)
                if old is not None and event.data.get("executor_started") is True:
                    observations[call_id] = old.model_copy(update={"executor_started": True})
                continue
            if event.kind != "workspace_receipt":
                continue
            data = event.data
            receipt = data["receipt"]
            call_id = cast(str, data["call_id"])
            old = observations.get(call_id)
            if old is None or not isinstance(receipt, dict):
                continue
            updates: dict[str, JsonValue] = {}
            if receipt.get("executor_started") is True:
                updates["executor_started"] = True
            if receipt.get("output_complete") is True:
                updates["result_available"] = True
                if old.value is None:
                    updates["value"] = receipt
            observations[call_id] = old.model_copy(update=updates)
    complete = (
        steps > 0
        and len(usages) == steps
        and len(ended) == steps
        and all(
            final
            and all(
                getattr(usage, key) is not None
                for key in ("input_tokens", "output_tokens", "total_tokens")
            )
            for usage, final in usages.values()
        )
    )
    total: dict[str, JsonValue] | None = None
    if usages:
        total = {
            key: sum(cast(int, getattr(usage, key)) for usage, _ in usages.values())
            if complete
            else None
            for key in ("input_tokens", "output_tokens", "total_tokens")
        }
    return RunFacts(
        run=snapshot.run,
        text=text,
        model_steps=steps,
        tools=list(observations.values()),
        events=list(snapshot.events),
        token_usage=total,
        usage_complete=complete,
    )
