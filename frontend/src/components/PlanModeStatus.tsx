import { useLayoutEffect, useRef, useState } from "react";
import { RequestError } from "../api";
import { ListChecks } from "lucide-react";
import { Button } from "./ui/button";
import type {
  InteractionAnswerBody,
  InteractionCancelBody,
  InteractionState,
  PlanConfirmationRequest,
} from "../lib/interaction-contracts";

type Action = "confirm" | "reject" | "cancel";
type LastAction = { identity: string; action: Action; key: string };
type LocalState = { identity: string; action: Action | null; error: string };

export interface PlanModeStatusProps {
  state: InteractionState;
  request: PlanConfirmationRequest | null;
  onConfirm: (request: PlanConfirmationRequest, body: InteractionAnswerBody) => Promise<unknown>;
  onReject: (request: PlanConfirmationRequest, body: InteractionAnswerBody) => Promise<unknown>;
  onCancel: (request: PlanConfirmationRequest, body: InteractionCancelBody) => Promise<unknown>;
  disabled?: boolean;
}

const terminalLabels: Record<string, string> = {
  answered: "计划确认已处理",
  cancelled: "计划确认已取消",
  superseded: "计划已更新",
};

function actionError(error: unknown, fallback: string): string {
  if (error instanceof RequestError) {
    if (error.code === "input_revision_conflict" || error.code === "revision_conflict") return "计划已更新，请刷新后重试";
    if (error.code === "key_conflict") return "请求状态不一致，请刷新后重试";
    if (error.code === "session_closed") return "对话已关闭";
    if (error.code === "not_found") return "计划请求已结束";
  }
  return fallback;
}

export function PlanModeStatus({ state, request, onConfirm, onReject, onCancel, disabled = false }: PlanModeStatusProps) {
  const uiIdentity = `${request ? request.id : "none"}:${request ? request.revision : "-"}:${state.revision}`;
  const keyIdentity = request ? `${request.id}:${request.revision}` : "";
  const currentIdentity = useRef(uiIdentity);
  currentIdentity.current = uiIdentity;
  const [local, setLocal] = useState<LocalState>({ identity: uiIdentity, action: null, error: "" });
  const inFlight = useRef<string | null>(null);
  const lastAction = useRef<LastAction | null>(null);
  const active = local.identity === uiIdentity ? local : { identity: uiIdentity, action: null, error: "" };
  const terminal = request !== null && request.status !== "pending";
  const stale = request !== null && request.status === "pending"
    && (state.mode !== "plan" || request.mode_revision !== state.revision);
  const blocked = disabled || active.action !== null || terminal;
  const confirmLocked = blocked || stale;

  useLayoutEffect(() => {
    currentIdentity.current = uiIdentity;
    inFlight.current = null;
    setLocal({ identity: uiIdentity, action: null, error: "" });
  }, [uiIdentity]);

  // 同一请求版本且动作相同时复用 request_key；换动作才换新 key。
  function reuseKey(action: Action): string {
    const previous = lastAction.current;
    if (previous && previous.identity === keyIdentity && previous.action === action) return previous.key;
    const key = crypto.randomUUID();
    lastAction.current = { identity: keyIdentity, action, key };
    return key;
  }

  async function perform(action: Action) {
    if (!request || request.status !== "pending" || inFlight.current === uiIdentity) return;
    if (blocked) return;
    if (action !== "cancel" && stale) return;
    inFlight.current = uiIdentity;
    setLocal({ identity: uiIdentity, action, error: "" });
    try {
      if (action === "cancel") {
        await onCancel(request, {
          request_key: reuseKey(action),
          expected_revision: request.revision,
          reason: "user_cancelled",
        });
      } else {
        const body: InteractionAnswerBody = {
          request_key: reuseKey(action),
          expected_revision: request.revision,
          answers: [],
          confirmed: action === "confirm",
        };
        await (action === "confirm" ? onConfirm(request, body) : onReject(request, body));
      }
    } catch (error) {
      if (currentIdentity.current === uiIdentity) {
        setLocal({ identity: uiIdentity, action: null, error: actionError(error, "操作未完成，请重试") });
      }
    } finally {
      if (inFlight.current === uiIdentity) inFlight.current = null;
      if (currentIdentity.current === uiIdentity) {
        setLocal(previous => previous.identity === uiIdentity ? { ...previous, action: null } : previous);
      }
    }
  }

  return <section className="workbench-interaction interaction-card" data-pending-interaction={request?.status === "pending"} aria-label="计划模式状态" data-mode={state.mode}>
    <div className="interaction-head"><ListChecks aria-hidden="true"/>
      <h3>{request?.status === "pending" ? "计划模式确认" : state.mode === "plan" ? "计划模式" : "执行模式"}</h3>
      {request?.status === "pending" && <span className="obs-pill obs-pill-warn">需要确认</span>}
    </div>
    {!request && <p className="interaction-description">{state.mode === "plan" ? "当前仅进行规划与只读操作。" : "当前可按权限执行操作。"}</p>}
    {stale && !terminal ? <p role="status" className="text-sm text-muted-foreground">计划已更新，请等待新的确认请求。</p> : null}
    {request && terminal ? <p role="status" className="text-sm text-muted-foreground">
      {terminalLabels[request.status] ?? "计划请求已结束"}
    </p> : null}
    {request && request.status === "pending" ? <>
      <div className="interaction-body whitespace-pre-wrap">{request.payload.plan}</div>
      {active.error && <p role="alert" className="text-sm text-destructive">{active.error}</p>}
      <div className="interaction-actions">
        <Button type="button" disabled={confirmLocked} onClick={() => void perform("confirm")}>{active.action === "confirm" ? "确认中…" : "按计划执行"}</Button>
        <Button type="button" variant="outline" disabled={confirmLocked} onClick={() => void perform("reject")}>{active.action === "reject" ? "处理中…" : "继续修改计划"}</Button>
        <Button type="button" variant="outline" disabled={blocked} onClick={() => void perform("cancel")}>{active.action === "cancel" ? "取消中…" : "取消请求"}</Button>
      </div>
    </> : null}
  </section>;
}
