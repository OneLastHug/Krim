import { Ban, CheckCircle, Circle, LoaderCircle } from "lucide-react";
import type { PlanSnapshot, PlanStatus } from "../lib/planning-contracts";
import { Button } from "./ui/button";
import "./task-plan.css";

const STATUS: Record<PlanStatus, { label: string; Icon: typeof Circle }> = {
  pending: { label: "待开始", Icon: Circle },
  in_progress: { label: "进行中", Icon: LoaderCircle },
  completed: { label: "已完成", Icon: CheckCircle },
  blocked: { label: "受阻", Icon: Ban },
};
const STATUS_ORDER: PlanStatus[] = ["pending", "in_progress", "completed", "blocked"];

export interface TaskPlanProps {
  /** 服务端单页或调用方合并后的只读展示快照；组件不再按单页 limit 重新解析。 */
  snapshot: PlanSnapshot;
  onLoadMore?: () => Promise<void>;
  loading?: boolean;
  error?: string;
  onRetry?: () => void;
}

function StatusIcon({ status }: { status: PlanStatus }) {
  const { Icon } = STATUS[status];
  return <Icon className={status === "in_progress" ? "task-plan-spinner" : undefined} aria-hidden="true" />;
}

export function TaskPlan({ snapshot, onLoadMore, loading = false, error = "", onRetry }: TaskPlanProps) {
  if (snapshot.total === 0) return null;

  const loaded = snapshot.steps.length;
  const filtered = snapshot.matched_total !== snapshot.total;
  const remaining = Math.max(0, snapshot.matched_total - loaded);
  const more = snapshot.next_offset !== null;
  const retry = onRetry ?? (more ? onLoadMore : undefined);

  function loadMore() {
    if (!onLoadMore || loading) return;
    // 失败由页面通过 error 回传；此处避免遗留未处理的 Promise。
    void onLoadMore().catch(() => undefined);
  }
  function retryRead() {
    if (loading || !retry) return;
    void Promise.resolve(retry()).catch(() => undefined);
  }

  return <details className="workspace-task-area task-plan" aria-label="会话计划" data-plan-revision={snapshot.revision}>
    <summary>
      <strong>计划</strong>
      <span className="task-plan-summary-meta">已完成 {snapshot.counts.completed} / {snapshot.total} 步</span>
    </summary>
    <div className="task-plan-body">
      <div className="task-plan-overview">
        <p className="task-plan-total">计划共 {snapshot.total} 步</p>
        <ul className="task-plan-counts" aria-label="计划状态统计">
          {STATUS_ORDER.map(status => {
            const { label } = STATUS[status];
            return <li key={status} data-status={status}>
              <StatusIcon status={status} />
              <span>{label}</span>
              <strong>{snapshot.counts[status]}</strong>
            </li>;
          })}
        </ul>
      </div>
      <p className="task-plan-loaded">
        已加载 {loaded} / {snapshot.matched_total} 步{filtered ? `（筛选匹配，计划共 ${snapshot.total} 步）` : ""}
      </p>
      {snapshot.steps.length > 0 && <ol className="task-plan-steps">
        {snapshot.steps.map(step => {
          const { label } = STATUS[step.status];
          return <li className="task-plan-step" key={step.id} data-status={step.status}>
            <span className="task-plan-step-state"><StatusIcon status={step.status} />{label}</span>
            <p className="task-plan-step-text">{step.text}</p>
          </li>;
        })}
      </ol>}
      {snapshot.matched_total === 0 && <p className="task-plan-empty">当前筛选没有计划步骤。</p>}
      {error && <div className="task-plan-feedback">
        <p className="task-plan-error" role="alert">{error}</p>
        {retry && <Button type="button" variant="outline" size="sm" disabled={loading} onClick={retryRead}>重试读取计划</Button>}
      </div>}
      {loading && <p className="task-plan-loading" role="status"><LoaderCircle className="task-plan-spinner" aria-hidden="true" />正在读取计划</p>}
      {!error && more && <div className="task-plan-more">
        <span>{remaining > 0 ? `还有 ${remaining} 步未加载` : "还有计划步骤未加载"}</span>
        {onLoadMore && <Button type="button" variant="outline" size="sm" disabled={loading} aria-busy={loading} onClick={loadMore}>加载更多</Button>}
      </div>}
    </div>
  </details>;
}
