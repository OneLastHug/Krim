import { useState } from "react";
import { Ban, CircleCheck, Circle, Clock, ListChecks, LoaderCircle } from "lucide-react";
import type { PlanSnapshot, PlanStatus } from "../lib/planning-contracts";
import { Button } from "./ui/button";
import "./task-plan.css";

const STATUS: Record<PlanStatus, { label: string; Icon: typeof Circle }> = {
  pending: { label: "待开始", Icon: Circle },
  in_progress: { label: "进行中", Icon: Clock },
  completed: { label: "已完成", Icon: CircleCheck },
  blocked: { label: "受阻", Icon: Ban },
};
const STATUS_ORDER: PlanStatus[] = ["pending", "in_progress", "completed", "blocked"];
export interface TaskPlanProps {
  snapshot: PlanSnapshot;
  onLoadMore?: () => Promise<void>;
  loading?: boolean;
  error?: string;
  onRetry?: () => void;
}
function StatusIcon({ status }: { status: PlanStatus }) {
  const { Icon } = STATUS[status];
  return <Icon aria-hidden="true"/>;
}
export function TaskPlan({ snapshot, onLoadMore, loading = false, error = "", onRetry }: TaskPlanProps) {
  const [expanded, setExpanded] = useState(true);
  if (snapshot.total === 0) return null;
  const loaded = snapshot.steps.length;
  const filtered = snapshot.matched_total !== snapshot.total;
  const remaining = Math.max(0, snapshot.matched_total - loaded);
  const more = snapshot.next_offset !== null;
  const retry = onRetry ?? (more ? onLoadMore : undefined);
  function loadMore() { if (onLoadMore && !loading) void onLoadMore().catch(() => undefined); }
  function retryRead() { if (!loading && retry) void Promise.resolve(retry()).catch(() => undefined); }
  return <details className="task-plan task-plan-card" aria-label="会话计划" data-plan-revision={snapshot.revision} open={expanded} onToggle={event => setExpanded(event.currentTarget.open)}>
    <summary><ListChecks aria-hidden="true"/><strong>会话计划进度</strong><span className="task-plan-summary-meta task-plan-counts">已完成 {snapshot.counts.completed} / {snapshot.total}</span></summary>
    <div className="task-plan-body">
      <div className="task-plan-overview sr-only">
        <p className="task-plan-total">计划共 {snapshot.total} 步</p>
        <ul className="task-plan-counts" aria-label="计划状态统计">{STATUS_ORDER.map(status => <li key={status} data-status={status}><StatusIcon status={status}/><span>{STATUS[status].label}</span><strong>{snapshot.counts[status]}</strong></li>)}</ul>
      </div>
      <p className={`task-plan-loaded${!more && !filtered ? " sr-only" : ""}`}>已加载 {loaded} / {snapshot.matched_total} 步{filtered ? `（筛选匹配，计划共 ${snapshot.total} 步）` : ""}</p>
      {snapshot.steps.length > 0 && <ol className="task-plan-steps">{snapshot.steps.map(step => <li className="task-plan-step" key={step.id} data-status={step.status}><span className="task-plan-step-state" title={STATUS[step.status].label}><StatusIcon status={step.status}/><span className="sr-only">{STATUS[step.status].label}</span></span><p className="task-plan-step-text">{step.text}</p></li>)}</ol>}
      {snapshot.matched_total === 0 && <p className="task-plan-empty">当前筛选没有计划步骤。</p>}
      {error && <div className="task-plan-feedback"><p className="task-plan-error" role="alert">{error}</p>{retry && <Button type="button" variant="outline" size="sm" disabled={loading} onClick={retryRead}>重试读取计划</Button>}</div>}
      {loading && <p className="task-plan-loading" role="status"><LoaderCircle className="task-plan-spinner" aria-hidden="true"/>正在读取计划</p>}
      {!error && more && <div className="task-plan-more"><span>{remaining > 0 ? `还有 ${remaining} 步未加载` : "还有计划步骤未加载"}</span>{onLoadMore && <Button type="button" variant="outline" size="sm" disabled={loading} aria-busy={loading} onClick={loadMore}>加载更多</Button>}</div>}
    </div>
  </details>;
}
