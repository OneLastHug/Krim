import { useMemo } from "react";
import { ChevronLeft, ChevronRight, RefreshCw } from "lucide-react";
import type { Actor } from "../lib/session-contracts";
import { traceDuration, traceSpanRows, traceStatuses, traceTargets, type TraceIndexItem, type TraceSpan, type TraceStatus } from "../lib/trace-contracts";
import { useTraceIndex } from "../hooks/use-trace-index";
import { Button } from "./ui/button";
import "./trace-workbench.css";

const statuses: (TraceStatus | "")[] = ["", "queued", "running", "completed", "failed", "aborted", "interrupted"];
function date(value: number) {
  const parsed = new Date(value * 1000);
  return Number.isFinite(parsed.getTime()) ? parsed : null;
}
function timestamp(value: number | null) {
  if (value === null) return "进行中";
  return date(value)?.toLocaleString() ?? String(value);
}
function datetime(value: number) { return date(value)?.toISOString(); }
function attributeText(value: unknown) {
  if (typeof value === "string") return value.length > 512 ? value.slice(0, 509) + "…" : value;
  try {
    const serialized = JSON.stringify(value);
    return serialized.length > 512 ? serialized.slice(0, 509) + "…" : serialized;
  } catch { return String(value); }
}
function SpanAttributes({ span }: { span: TraceSpan }) {
  const attributes = Object.entries(span.attributes).slice(0, 32);
  return <dl className="trace-span-properties">
    <div><dt>Span ID</dt><dd>{span.span_id}</dd></div>
    {span.parent_span_id && <div><dt>父节点</dt><dd>{span.parent_span_id}</dd></div>}
    <div><dt>开始</dt><dd>{timestamp(span.start)}</dd></div>
    <div><dt>结束</dt><dd>{timestamp(span.end)}</dd></div>
    <div><dt>耗时</dt><dd>{traceDuration(span.start, span.end)}</dd></div>
    <div><dt>状态</dt><dd>{span.status}</dd></div>
    {attributes.map(([name, value]) => <div key={name}><dt>{name}</dt><dd>{attributeText(value)}</dd></div>)}
    {Object.keys(span.attributes).length > attributes.length && <p className="trace-property-note">其余属性未展开。</p>}
  </dl>;
}
function TraceRow({ item, selected, onSelect }: { item: TraceIndexItem; selected: boolean; onSelect: () => void }) {
  return <li>
    <Button type="button" variant="ghost" className="trace-index-row" aria-pressed={selected} onClick={onSelect}>
      <span className="trace-index-row-heading"><strong>{item.session_title || "未命名对话"}</strong><span className={`trace-status trace-status-${item.status}`}>{traceStatuses[item.status]}</span></span>
      <span className="trace-index-row-meta"><span>{traceTargets[item.target_key] ?? item.target_key}</span><time dateTime={datetime(item.created_at)}>{timestamp(item.created_at)}</time></span>
    </Button>
  </li>;
}

export function TraceWorkbench({ apiKey, workspace, identity, active, sessionId, turnId, onSelection, onSession, onEvaluate }: {
  apiKey: string;
  workspace: string;
  identity: Actor | null;
  active: boolean;
  sessionId?: string;
  turnId?: string;
  onSelection?: (runId: string) => void;
  onSession?: (sessionId: string) => void;
  onEvaluate?: (workspace: string, runId: string) => void;
}) {
  const trace = useTraceIndex({ apiKey, workspace, identity, active, sessionId, turnId, onSelection, onSession });
  const spans = trace.detail?.spans;
  const spanRows = useMemo(() => spans ? traceSpanRows(spans) : [], [spans]);
  const canEvaluate = active && !!identity?.workspaces.some(value => value.workspace_id === workspace && value.role !== "viewer");
  return <section className="trace-workbench" aria-label="Trace 工作台">
    <section className="trace-index-panel" aria-labelledby="trace-index-heading">
      <header className="trace-index-heading">
        <div><h2 id="trace-index-heading">运行记录</h2><p>来自会话轮次</p></div>
        <Button type="button" variant="ghost" aria-label="刷新运行记录" title="刷新运行记录" disabled={!trace.ready || trace.loading} onClick={trace.refresh}><RefreshCw aria-hidden="true"/></Button>
      </header>
      <label className="trace-status-filter">状态
        <select aria-label="按状态筛选" value={trace.status} onChange={event => trace.filter(event.target.value as TraceStatus | "")}>
          {statuses.map(value => <option key={value || "all"} value={value}>{value ? traceStatuses[value] : "全部状态"}</option>)}
        </select>
      </label>
      {trace.loading && <p className="trace-feedback" role="status">正在读取运行记录…</p>}
      {!trace.loading && trace.error && <p className="trace-feedback trace-error" role="alert">{trace.error}</p>}
      {!trace.loading && !trace.error && trace.ready && trace.page?.items.length === 0 && <p className="trace-feedback">没有符合筛选条件的运行记录。</p>}
      {!trace.ready && <p className="trace-feedback" role="status">连接工作区后读取运行记录。</p>}
      {trace.page && trace.page.items.length > 0 && <ul className="trace-index-list" aria-label="运行记录列表">
        {trace.page.items.map(item => <TraceRow key={item.run_id} item={item} selected={trace.selected?.run_id === item.run_id} onSelect={() => trace.choose(item)} />)}
      </ul>}
      <nav className="trace-pagination" aria-label="运行记录分页">
        <Button type="button" variant="outline" disabled={!trace.ready || trace.loading || !trace.canPrevious} onClick={trace.previous}><ChevronLeft aria-hidden="true"/>上一页</Button>
        <Button type="button" variant="outline" disabled={!trace.ready || trace.loading || !trace.page?.next_cursor} onClick={trace.next}>下一页<ChevronRight aria-hidden="true"/></Button>
      </nav>
    </section>

    {trace.selected && <section className="trace-detail-panel" aria-labelledby="trace-detail-heading">
      <header className="trace-detail-heading">
        <div><p>会话轮次</p><h2 id="trace-detail-heading">{trace.selected.session_title || "未命名对话"}</h2>
          <div className="trace-detail-meta"><span>{traceTargets[trace.selected.target_key] ?? trace.selected.target_key}</span><span className={`trace-status trace-status-${trace.selected.status}`}>{traceStatuses[trace.selected.status]}</span><span>创建于 {timestamp(trace.selected.created_at)}</span></div>
        </div>
        <div className="trace-detail-actions">
          {onEvaluate && canEvaluate && <Button type="button" variant="ghost" className="trace-evaluate-action" onClick={() => {
            const membership = identity?.workspaces.find(value => value.workspace_id === workspace);
            const runId = trace.selected?.run_id;
            if (active && membership && membership.role !== "viewer" && runId) onEvaluate(workspace, runId);
          }}>加入数据集</Button>}
          {onSession && <Button type="button" variant="outline" onClick={() => trace.returnToSession(trace.selected!.session_id)}>返回对话</Button>}
        </div>
      </header>
      <dl className="trace-run-identifiers"><div><dt>Run</dt><dd>{trace.selected.run_id}</dd></div><div><dt>Trace</dt><dd>{trace.selected.trace_id}</dd></div></dl>
      {trace.detailLoading && <p className="trace-feedback" role="status">正在读取节点详情…</p>}
      {!trace.detailLoading && trace.detailError && <p className="trace-feedback trace-error" role="alert">{trace.detailError}</p>}
      {trace.detail && <>
        {!trace.detail.complete && <section className="trace-incomplete" aria-label="记录完整性">
          <strong>记录不完整</strong>
          {trace.detail.reasons.length > 0 && <ul>{trace.detail.reasons.map((reason, index) => <li key={`${index}-${reason}`}>{reason}</li>)}</ul>}
        </section>}
        {spanRows.length === 0 ? <p className="trace-feedback">此运行没有可显示的节点。</p> : <ol className="trace-span-tree" aria-label="Trace 节点">
          {spanRows.map(({ span, depth }) => <li key={span.span_id} className="trace-span-node">
            <details>
              <summary style={{ paddingInlineStart: `${12 + Math.min(depth, 8) * 16}px` }}>
                <span className="trace-span-kind">{span.kind}</span><span className="trace-span-duration">{traceDuration(span.start, span.end)}</span><span className="trace-span-status" title={span.status}>{span.status}</span>
              </summary>
              <SpanAttributes span={span}/>
            </details>
          </li>)}
        </ol>}
      </>}
    </section>}
  </section>;
}
