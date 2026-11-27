import type { ToolPresentation, ToolState, TurnPresentation } from "./session-presentation";
import { isFileChangeToolName } from "./session-presentation";
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

/** 只认项目文件工具（含 dev_read/dev_write/dev_edit 旧名）；无关/插件工具不并入。 */
const fileGroupName = (name: string): boolean => canonicalToolName(name) === "read" || isFileChangeToolName(name);
const plausiblePath = (value: string): boolean => value.trim().length > 0 && !/[\u0000-\u001f\u007f]/.test(value);

export function fileToolPath(tool: ToolPresentation): string | null {
  if (tool.approval || tool.state === "approval" || !fileGroupName(tool.name)) return null;
  const args = record(tool.arguments);
  const path = args?.path ?? args?.file_path;
  if (typeof path === "string" && plausiblePath(path)) return path;
  // 路径参数可能晚于内容生成：完整参数到达前，实时预览事件是唯一的路径来源。
  if (tool.preview && plausiblePath(tool.preview.path)) return tool.preview.path;
  return tool.previewPath && plausiblePath(tool.previewPath) ? tool.previewPath : null;
}

/** Confirmed operation receipts only: never infer changes from model arguments. */
export function fileChangeCount(tool: ToolPresentation): FileChangeCount | null {
  if (!isFileChangeToolName(tool.name) || tool.state !== "ok" ||
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
    else grouped.push({ type: "file-group", key: `file-group:${item.previewKey ?? item.providerCallId ?? item.callId ?? item.key}`, path, tools: [item] });
  }
  return grouped;
}

/** 进行中的实时预览调用：只有尚未结束的写入/编辑才贡献未确认数字。 */
const livePreview = (tool: ToolPresentation): boolean =>
  tool.preview !== undefined && (tool.state === "preparing" || tool.state === "running");

export function fileGroupSummary(tools: ToolPresentation[]) {
  let writes = 0, reads = 0, added = 0, removed = 0, counted = 0, uncounted = 0;
  let liveAdded: number | null = null, liveRemoved: number | null = null, liveCalls = 0, livePartial = false;
  let failed = 0, denied = 0, cancelled = 0, unknown = 0;
  for (const tool of tools) {
    const name = canonicalToolName(tool.name);
    if (name === "read") reads++; else writes++;
    const count = fileChangeCount(tool);
    if (count) { added += count.added; removed += count.removed; counted++; }
    else if (name !== "read" && tool.state === "ok") uncounted++;
    const preview = tool.preview;
    if (preview && livePreview(tool)) {
      liveCalls++;
      if (preview.linesAdded === null || preview.linesRemoved === null) livePartial = true;
      if (preview.linesAdded !== null) liveAdded = (liveAdded ?? 0) + preview.linesAdded;
      if (preview.linesRemoved !== null) liveRemoved = (liveRemoved ?? 0) + preview.linesRemoved;
    }
    if (tool.state === "failed") failed++;
    if (tool.state === "denied") denied++;
    if (tool.state === "cancelled") cancelled++;
    if (tool.state === "unknown") unknown++;
  }
  const active = tools.some(tool => tool.state === "preparing" || tool.state === "running");
  // 参数尚未完整（仍由预览事件驱动）时是模型生成阶段；执行器接管后回到既有修改中状态。
  const generating = tools.some(tool => livePreview(tool) && tool.arguments === undefined && tool.state !== "running");
  const state: ToolState = active ? "running" : failed ? "failed" : denied ? "denied" : unknown ? "unknown" : cancelled ? "cancelled" : "ok";
  const problems = [failed ? `${failed} 次失败` : "", denied ? `${denied} 次拒绝` : "", cancelled ? `${cancelled} 次取消` : "", unknown ? `${unknown} 次结果未确认` : ""].filter(Boolean);
  const status = generating ? `生成中${problems.length ? ` · ${problems.join("、")}` : ""}`
    : active ? `${writes ? "修改中" : "读取中"}${problems.length ? ` · ${problems.join("、")}` : ""}`
    : problems.join("、") || "已完成";
  const operations = [writes ? `修改 ${writes} 次` : "", reads ? `读取 ${reads} 次` : ""].filter(Boolean).join(" · ");
  return { writes, reads, added, removed, counted, uncounted, liveAdded, liveRemoved, liveCalls, livePartial, generating, state, status, operations, active };
}
