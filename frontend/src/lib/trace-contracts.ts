import { object } from "../api";

export type TraceStatus = "queued" | "running" | "completed" | "failed" | "aborted" | "interrupted";
export interface TraceIndexItem {
  source_kind: "session_turn";
  run_id: string;
  trace_id: string;
  session_id: string;
  turn_id: string;
  session_title: string;
  target_key: string;
  status: TraceStatus;
  created_at: number;
  finished_at: number | null;
}
export interface TraceIndexPage { items: TraceIndexItem[]; next_cursor: string | null }
export interface TraceSpan {
  run_id: string;
  trace_id: string;
  span_id: string;
  parent_span_id: string | null;
  kind: string;
  start: number;
  end: number | null;
  status: string;
  attributes: Record<string, unknown>;
}
export interface TraceDetail { run_id: string; trace_id: string; complete: boolean; reasons: string[]; spans: TraceSpan[] }
export const traceStatuses: Record<TraceStatus, string> = {
  queued: "排队中", running: "运行中", completed: "已完成", failed: "失败", aborted: "已停止", interrupted: "已中断",
};
const targetKeys = ["demo", "target-v1", "target-v2", "target-bad", "target-http", "target-workspace"];
export const traceTargets: Record<string, string> = {
  demo: "演示目标", "target-v1": "规则目标 v1", "target-v2": "规则目标 v2", "target-bad": "异常目标",
  "target-http": "已配置模型", "target-workspace": "文件任务",
};
function exact(value: Record<string, unknown>, fields: string[]) {
  if (fields.some(key => !(key in value)) || Object.keys(value).some(key => !fields.includes(key))) throw new Error("Trace 响应结构无效");
}
const id = (value: unknown): value is string => typeof value === "string" && value.length > 0 && value.length <= 256 && !/[\u0000-\u001f]/.test(value);
const time = (value: unknown): value is number => typeof value === "number" && Number.isFinite(value);
export function parseTracePage(raw: unknown): TraceIndexPage {
  const page = object(raw); exact(page, ["items", "next_cursor"]);
  if (!Array.isArray(page.items) || page.items.length > 50 || (page.next_cursor !== null && (typeof page.next_cursor !== "string" || !page.next_cursor.length || page.next_cursor.length > 4096 || /[\u0000-\u001f]/.test(page.next_cursor)))) throw new Error("Trace 分页响应无效");
  const seen = new Set<string>();
  for (const rawItem of page.items) {
    const item = object(rawItem);
    exact(item, ["source_kind", "run_id", "trace_id", "session_id", "turn_id", "session_title", "target_key", "status", "created_at", "finished_at"]);
    if (item.source_kind !== "session_turn" || !["run_id", "trace_id", "session_id", "turn_id"].every(key => id(item[key])) || typeof item.session_title !== "string" || item.session_title.length > 128 || !targetKeys.includes(String(item.target_key)) || !Object.hasOwn(traceStatuses, String(item.status)) || !time(item.created_at) || (item.finished_at !== null && !time(item.finished_at))) throw new Error("Trace 索引对象无效");
    if (seen.has(String(item.run_id))) throw new Error("Trace 索引对象重复");
    seen.add(String(item.run_id));
  }
  return page as unknown as TraceIndexPage;
}
export function parseTraceDetail(raw: unknown, item: TraceIndexItem): TraceDetail {
  const envelope = object(raw); exact(envelope, ["session_id", "turn_id", "trace"]);
  if (envelope.session_id !== item.session_id || envelope.turn_id !== item.turn_id) throw new Error("Trace 来源不匹配");
  const detail = object(envelope.trace); exact(detail, ["run_id", "trace_id", "complete", "reasons", "spans"]);
  if (detail.run_id !== item.run_id || detail.trace_id !== item.trace_id || typeof detail.complete !== "boolean" || !Array.isArray(detail.reasons) || detail.reasons.length > 64 || detail.reasons.some(reason => typeof reason !== "string" || reason.length > 256) || !Array.isArray(detail.spans) || detail.spans.length > 4096) throw new Error("Trace 身份或内容无效");
  const seen = new Set<string>();
  for (const rawSpan of detail.spans) {
    const span = object(rawSpan); exact(span, ["run_id", "trace_id", "span_id", "parent_span_id", "kind", "start", "end", "status", "attributes"]);
    if (span.run_id !== item.run_id || span.trace_id !== item.trace_id || !id(span.span_id) || (span.parent_span_id !== null && !id(span.parent_span_id)) || !id(span.kind) || !id(span.status) || !time(span.start) || (span.end !== null && !time(span.end))) throw new Error("Trace 节点无效");
    object(span.attributes);
    if (seen.has(span.span_id)) throw new Error("Trace 节点重复");
    seen.add(span.span_id);
  }
  return detail as unknown as TraceDetail;
}
export function traceSpanRows(spans: TraceSpan[]): { span: TraceSpan; depth: number }[] {
  const ids = new Set(spans.map(span => span.span_id));
  const children = new Map<string | null, TraceSpan[]>();
  for (const span of spans) {
    const parent = span.parent_span_id && ids.has(span.parent_span_id) ? span.parent_span_id : null;
    children.set(parent, [...(children.get(parent) ?? []), span]);
  }
  const result: { span: TraceSpan; depth: number }[] = [], seen = new Set<string>();
  function visit(span: TraceSpan, depth: number) {
    if (seen.has(span.span_id)) return;
    seen.add(span.span_id); result.push({ span, depth: Math.min(depth, 24) });
    if (depth < 32) for (const child of children.get(span.span_id) ?? []) visit(child, depth + 1);
  }
  for (const root of children.get(null) ?? []) visit(root, 0);
  for (const span of spans) if (!seen.has(span.span_id)) visit(span, 0);
  return result;
}
export function traceDuration(start: number, end: number | null): string {
  if (end === null || end < start) return "—";
  const milliseconds = (end - start) * 1000;
  return milliseconds < 1000 ? `${Math.round(milliseconds)} ms` : `${(milliseconds / 1000).toFixed(2)} s`;
}
