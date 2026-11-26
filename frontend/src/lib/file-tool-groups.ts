import type { ToolPresentation, ToolState, TurnPresentation } from "./session-presentation";
import { canonicalToolName } from "./tool-names";

export interface FileToolGroup {
  type: "file-group";
  key: string;
  path: string;
  tools: ToolPresentation[];
}
export type GroupedPresentation = TurnPresentation | FileToolGroup;
export interface FileChangeCount { added: number; removed: number }

const record = (value: unknown): Record<string, unknown> | null =>
  value !== null && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : null;
const natural = (value: unknown): value is number => Number.isSafeInteger(value) && Number(value) >= 0;

/** Only the known project-file tools share a row; unrelated/plugin tools remain separate. */
export function fileToolPath(tool: ToolPresentation): string | null {
  if (tool.approval || tool.state === "approval" || !["read", "write", "edit"].includes(canonicalToolName(tool.name))) return null;
  const args = record(tool.arguments);
  const path = args?.path ?? args?.file_path;
  return typeof path === "string" && path.trim().length > 0 && !/[\u0000-\u001f\u007f]/.test(path) ? path : null;
}

/** Confirmed operation receipts only: never infer changes from model arguments. */
export function fileChangeCount(tool: ToolPresentation): FileChangeCount | null {
  if (!["write", "edit"].includes(canonicalToolName(tool.name)) || tool.state !== "ok" ||
      !tool.resultAvailable || tool.executorStarted === false) return null;
  const result = record(tool.result);
  const value = record(result?.value) ?? result;
  if (!value || result?.ok === false || value.ok === false || !natural(value.lines_added) || !natural(value.lines_removed)) return null;
  return { added: value.lines_added, removed: value.lines_removed };
}

/** Stable first-call keys keep expansion/focus when new live calls join a group. */
export function groupFileTools(items: TurnPresentation[]): GroupedPresentation[] {
  const grouped: GroupedPresentation[] = [];
  for (const item of items) {
    const path = item.type === "tool" ? fileToolPath(item) : null;
    if (item.type !== "tool" || path === null) {
      grouped.push(item);
      continue;
    }
    const previous = grouped.at(-1);
    if (previous?.type === "file-group" && previous.path === path) previous.tools.push(item);
    else grouped.push({ type: "file-group", key: `file-group:${item.providerCallId ?? item.callId ?? item.key}`, path, tools: [item] });
  }
  return grouped;
}

export function fileGroupSummary(tools: ToolPresentation[]) {
  let writes = 0, reads = 0, added = 0, removed = 0, counted = 0, uncounted = 0;
  let failed = 0, denied = 0, cancelled = 0, unknown = 0;
  for (const tool of tools) {
    const name = canonicalToolName(tool.name);
    if (name === "read") reads++; else writes++;
    const count = fileChangeCount(tool);
    if (count) { added += count.added; removed += count.removed; counted++; }
    else if (name !== "read" && tool.state === "ok") uncounted++;
    if (tool.state === "failed") failed++;
    if (tool.state === "denied") denied++;
    if (tool.state === "cancelled") cancelled++;
    if (tool.state === "unknown") unknown++;
  }
  const active = tools.some(tool => tool.state === "preparing" || tool.state === "running");
  const state: ToolState = active ? "running" : failed ? "failed" : denied ? "denied" : unknown ? "unknown" : cancelled ? "cancelled" : "ok";
  const problems = [failed ? `${failed} 次失败` : "", denied ? `${denied} 次拒绝` : "", cancelled ? `${cancelled} 次取消` : "", unknown ? `${unknown} 次结果未确认` : ""].filter(Boolean);
  const status = active ? `${writes ? "修改中" : "读取中"}${problems.length ? ` · ${problems.join("、")}` : ""}` : problems.join("、") || "已完成";
  const operations = [writes ? `修改 ${writes} 次` : "", reads ? `读取 ${reads} 次` : ""].filter(Boolean).join(" · ");
  return { writes, reads, added, removed, counted, uncounted, state, status, operations, active };
}
