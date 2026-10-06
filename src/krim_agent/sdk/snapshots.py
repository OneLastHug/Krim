"""终态生成事实读取：不调用 Provider、不重放工具、不补写事件。"""

from krim_agent.observability import TraceContext
from krim_agent.persistence import KrimRepository
from krim_agent.protocol.domain import TERMINAL_STATUSES, ServiceError
from krim_agent.protocol.models import StreamFrame

from .types import GenerateResult


async def read_generation_result(repository: KrimRepository, run_id: str) -> GenerateResult:
    from .text import _finish, _steps

    run = await repository.get_run(run_id)
    if run.status not in TERMINAL_STATUSES:
        raise ServiceError("result_not_terminal", "运行尚未终态，不能只读结果", 409)
    events = await repository.list_events(run_id)
    # 终态从 Run 账本读取；兼容没有 SSE 历史的旧 Run，仅构造内存投影。
    finish = _finish(
        StreamFrame(
            stream_id=run.id,
            seq=0,
            kind="end",
            data={
                "status": run.status,
                "code": run.error_code,
                "error": run.error,
            },
        )
    )
    trace = None
    if events and events[0].kind == "start" and isinstance(events[0].data, dict):
        value = events[0].data.get("trace_context")
        if value is not None:
            trace = TraceContext.model_validate(value)
    messages = await repository.run_messages(run_id)
    steps = _steps(messages)
    usage = await repository.usage_view(run_id)
    for step in steps:
        observed = next((value for value in usage.steps if value.step == step.step), None)
        if observed is not None:
            step.token_usage = observed.usage
            step.usage_complete = bool(
                observed.completed
                and observed.final
                and observed.usage is not None
                and all(
                    getattr(observed.usage, name) is not None
                    for name in ("input_tokens", "output_tokens", "total_tokens")
                )
            )
    return GenerateResult(
        text=steps[-1].text if steps else "",
        run=run,
        finish_reason=finish.finish_reason,
        steps=steps,
        response_messages=[m for m in messages if m.role != "user"],
        audits=await repository.audits(run_id),
        error=finish.error,
        code=finish.code,
        token_usage=usage.token_usage,
        usage_complete=usage.usage_complete,
        model_steps=usage.steps,
        trace_context=trace,
    )
