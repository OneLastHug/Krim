import type { SessionState } from "../hooks/use-session";
import { useSessionFeatures } from "../hooks/use-session-features";
import { TaskPlan } from "./TaskPlan";
import { UserQuestion } from "./UserQuestion";
import { PlanModeStatus } from "./PlanModeStatus";
import { Button } from "./ui/button";
import "./session-features.css";

export function SessionFeatures({ chat, apiKey, active, fence, backgroundRunning = false }: {
  chat: SessionState; apiKey: string; active: boolean; fence: { current: number }; backgroundRunning?: boolean;
}) {
  const stateResult = [...(chat.events ?? [])].reverse().find(event => {
    const data = event.data as Record<string, unknown> | undefined;
    return event.kind === "tool_end" && data && ["plan_update", "ask_user", "enter_plan_mode", "exit_plan_mode"].includes(String(data.name));
  });
  const refreshKey = JSON.stringify([chat.currentTurn?.id, chat.currentTurn?.status, stateResult?.run_id, stateResult?.seq]);
  const features = useSessionFeatures({
    apiKey, principalId: chat.actor?.principal_id ?? "", generation: chat.actor?.generation ?? 0,
    workspaceId: chat.workspace, sessionId: chat.id,
    enabled: active && chat.ready && !!chat.session && !chat.session.deleted_at,
    running: backgroundRunning || chat.history.some(turn => turn.status === "running" || turn.status === "queued"),
    refreshKey, fence,
  }, chat.refresh);
  const canRespond = chat.canWrite && chat.session?.owner_id === chat.actor?.principal_id
    && chat.session?.status === "open" && !chat.session.archived_at && !chat.session.deleted_at;
  const state = features.interactions;
  const confirmations = state?.pending.filter(item => item.kind === "plan_confirmation") ?? [];
  const questions = state?.pending.filter(item => item.kind === "user_input") ?? [];
  const showMode = state && (state.mode === "plan" || confirmations.length > 0);
  if (!features.plan?.total && !features.planError && !features.interactionError && !showMode && !questions.length) return null;
  return <section className="session-features" aria-label="当前任务状态">
    {features.plan && <TaskPlan snapshot={features.plan} loading={features.paging}
      error={features.planError} onLoadMore={features.loadMore} onRetry={features.reload} />}
    {((!features.plan?.total && features.planError) || features.interactionError) && <div className="session-feature-error" role="alert">
      <span>{[!features.plan?.total && features.planError, features.interactionError].filter(Boolean).join("；")}</span>
      <Button type="button" variant="outline" size="sm" onClick={features.reload}>重新读取任务状态</Button>
    </div>}
    {showMode && (confirmations.length ? confirmations.map(item => <PlanModeStatus key={item.id} state={state} request={item}
      disabled={!canRespond} onConfirm={features.answer} onReject={features.answer} onCancel={features.cancel} />)
      : <PlanModeStatus state={state} request={null} disabled={!canRespond}
        onConfirm={features.answer} onReject={features.answer} onCancel={features.cancel} />)}
    {questions.map(item => <UserQuestion key={item.id} request={item} disabled={!canRespond}
      onAnswer={features.answer} onCancel={features.cancel} />)}
  </section>;
}
