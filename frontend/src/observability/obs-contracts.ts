import { object } from "../api";
import { parseTraceDetail, traceTargets, type TraceDetail, type TraceIndexItem, type TraceSpan } from "../lib/trace-contracts";

/** 观测运行状态：完整 RunStatus（含 awaiting 停靠审批；游标 scope 与 /traces 相同）。 */
export type ObsRunStatus = "queued" | "running" | "awaiting" | "completed" | "failed" | "aborted" | "interrupted";

export const obsRunStatuses: ObsRunStatus[] = ["queued", "running", "awaiting", "completed", "failed", "aborted", "interrupted"];

/** 筛选参数允许的完整子集：observations 支持 awaiting 过滤。 */
export const obsFilterStatuses: ObsRunStatus[] = [...obsRunStatuses];

export const obsStatusNames: Record<ObsRunStatus, string> = {
  queued: "排队中", running: "运行中", awaiting: "待审批", completed: "已完成",
  failed: "失败", aborted: "已停止", interrupted: "已中断",
};

/** 观测只读契约 V1，对齐 docs/plans/observability-api.md。
 * 请求路径：GET /v2/workspaces/{ws}/observations?limit&cursor&status&session_id
 *          GET /v2/workspaces/{ws}/sessions/{sid}/turns/{tid}/observations
 * 图形以 input_bytes 为统一准确分母；estimated_tokens 未知保持未知；
 * cost 恒为 null + cost_status:"unpriced"，前端不推算、不补零。 */

export type ObsCategory = "system" | "tools" | "history" | "tool_result" | "summary" | "unknown" | "files" | "memory";

/** 图例固定顺序，与已验证类别色的相邻序一致（obs-palette.ts）。 */
export const obsCategories: ObsCategory[] = ["system", "tools", "files", "tool_result", "memory", "history", "summary", "unknown"];

export const obsCategoryNames: Record<ObsCategory, string> = {
  system: "系统指令", tools: "工具定义", files: "文件与资料", memory: "记忆",
  tool_result: "工具结果", history: "历史对话", summary: "历史摘要", unknown: "未归属",
};

export interface ObsUsage {
  input_tokens: number | null;
  output_tokens: number | null;
  total_tokens: number | null;
  cached_input_tokens: number | null;
  cache_write_input_tokens: number | null;
  reasoning_output_tokens: number | null;
}

export interface ObsItemUsage {
  usage: ObsUsage | null;
  usage_reports: number;
  usage_final: boolean | null;
}

/** 运行配置白名单投影：展示"这次运行用的是什么"；窗口数值永远来自 manifest。 */
export interface ObsConfiguration {
  key: string | null;
  version: string | null;
  provider_behavior_version: string | null;
  provider_implementation_digest: string | null;
  model: string | null;
  reasoning_effort: string | null;
  thinking: string | null;
  endpoint_fingerprint: string | null;
  prompt_fingerprint: string | null;
  limits_fingerprint: string | null;
  max_output_tokens: number | null;
}

const usageFields = ["input_tokens", "output_tokens", "total_tokens", "cached_input_tokens", "cache_write_input_tokens", "reasoning_output_tokens"] as const;

function numberOrNull(value: unknown): boolean {
  return value === null || typeof value === "number" && Number.isFinite(value);
}

function integerOrNull(value: unknown): boolean {
  return value === null || typeof value === "number" && Number.isSafeInteger(value);
}

/** 类型谓词：在守卫链中收窄 Record 字段的 unknown 类型。 */
function int(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value);
}

function nonNegativeInt(value: unknown): value is number {
  return int(value) && value >= 0;
}

function idText(value: unknown, max = 256): boolean {
  return typeof value === "string" && value.length > 0 && value.length <= max && !/[\u0000-\u001f]/.test(value);
}

function optionalIdText(value: unknown): boolean {
  return value === null || idText(value);
}

function idListOrNull(value: unknown, max = 4096): boolean {
  return value === null || Array.isArray(value) && value.length <= max && value.every(entry => idText(entry));
}

function parseConfiguration(raw: unknown): ObsConfiguration | null {
  if (raw === null) return null;
  const value = object(raw);
  const fields = ["key", "version", "provider_behavior_version", "provider_implementation_digest", "model", "reasoning_effort", "thinking", "endpoint_fingerprint", "prompt_fingerprint", "limits_fingerprint", "max_output_tokens"] as const;
  if (Object.keys(value).length !== fields.length || !fields.every(field => field in value)) throw new Error("运行配置结构无效");
  const textOrNull = (field: typeof fields[number]) => value[field] === null || idText(value[field], 256);
  if (!textOrNull("key") || !textOrNull("version")
    || !textOrNull("provider_behavior_version") || !textOrNull("provider_implementation_digest")
    || !textOrNull("model") || !textOrNull("reasoning_effort") || !textOrNull("thinking")
    || !textOrNull("endpoint_fingerprint") || !textOrNull("prompt_fingerprint") || !textOrNull("limits_fingerprint")
    || (value.max_output_tokens !== null && !nonNegativeInt(value.max_output_tokens))) throw new Error("运行配置字段无效");
  return value as unknown as ObsConfiguration;
}

function parseUsage(raw: unknown): ObsUsage {
  const value = object(raw);
  if (usageFields.some(field => !(field in value)) || Object.keys(value).some(key => !usageFields.includes(key as keyof ObsUsage))) {
    throw new Error("观测 usage 结构无效");
  }
  if (!usageFields.every(field => numberOrNull(value[field]))) throw new Error("观测 usage 数值无效");
  return value as unknown as ObsUsage;
}

export interface ObsManifestItem {
  key: string;
  category: ObsCategory;
  label: string;
  content_hash: string;
  input_bytes: number;
  estimated_tokens: number | null;
  message_index: number | null;
  tool_index: number | null;
  /** Message 协议的 provider 调用 id（定位 tools[].provider_call_id），可能被截断。 */
  call_id: string | null;
  /** 后端追加的显式 provider_call_id；缺省为 null。 */
  provider_call_id: string | null;
  /** 已裁剪为安全字段的来源引用；正文/路径不在 manifest refs 中。 */
  source_refs: { turn_id?: string; run_id?: string | null; last_seq?: number; fingerprint?: string; terminal_seq?: number; transcript_fingerprint?: string }[] | null;
  source_refs_truncated: boolean;
}

export interface ObsCompactionChange {
  pass_index: number;
  summary_step_index: number | null;
  before_input_bytes: number;
  after_input_bytes: number;
  counter_version: string | null;
  source_hash: string | null;
  output_hash: string | null;
}

export interface ObsManifest {
  version: 1;
  counter_version: string;
  input_bytes: number;
  estimated_tokens: number | null;
  items: ObsManifestItem[];
  model_context_window: number | null;
  items_truncated: boolean;
  omitted_items: number | null;
  source_refs_truncated: boolean;
  reserved_output_tokens: number | null;
  input_budget_bytes: number | null;
  /** 后续任务 manifest 记录的真实压缩前后组合上下文字节（同一业务口径，非 summary 请求输入）；旧数据缺省。 */
  compaction_changes: ObsCompactionChange[] | null;
}

function parseSourceRefs(value: unknown): ObsManifestItem["source_refs"] {
  if (value === null) return null;
  if (!Array.isArray(value) || value.length > 256) throw new Error("上下文来源引用无效");
  const allowed = new Set(["turn_id", "run_id", "last_seq", "fingerprint", "terminal_seq", "transcript_fingerprint"]);
  return value.map(raw => {
    const ref = object(raw);
    if (Object.keys(ref).length === 0 || Object.keys(ref).some(key => !allowed.has(key))
      || (ref.turn_id !== undefined && !idText(ref.turn_id))
      || (ref.run_id !== undefined && !(ref.run_id === null || idText(ref.run_id)))
      || (ref.last_seq !== undefined && !nonNegativeInt(ref.last_seq))
      || (ref.terminal_seq !== undefined && !nonNegativeInt(ref.terminal_seq))
      || (ref.fingerprint !== undefined && !idText(ref.fingerprint, 256))
      || (ref.transcript_fingerprint !== undefined && !idText(ref.transcript_fingerprint, 256))) {
      throw new Error("上下文来源引用字段无效");
    }
    return ref as unknown as NonNullable<ObsManifestItem["source_refs"]>[number];
  });
}

const manifestItemFields = ["key", "category", "label", "content_hash", "input_bytes", "estimated_tokens"] as const;
const manifestItemOptionalFields = ["message_index", "tool_index", "call_id", "provider_call_id", "call_id_hash", "call_id_truncated", "source_refs", "source_refs_truncated"] as const;

function parseManifestItem(raw: unknown): ObsManifestItem {
  const value = object(raw);
  if (manifestItemFields.some(field => !(field in value)) || Object.keys(value).some(key => !(manifestItemFields as readonly string[]).includes(key) && !(manifestItemOptionalFields as readonly string[]).includes(key))) {
    throw new Error("上下文清单条目结构无效");
  }
  if (!idText(value.key) || typeof value.label !== "string" || value.label.length > 1024
    || !idText(value.content_hash, 128)
    || !nonNegativeInt(value.input_bytes)
    || !integerOrNull(value.estimated_tokens)
    || (value.message_index !== undefined && value.message_index !== null && !nonNegativeInt(value.message_index))
    || (value.tool_index !== undefined && value.tool_index !== null && !nonNegativeInt(value.tool_index))
    || (value.call_id !== undefined && value.call_id !== null && !idText(value.call_id))
    || (value.provider_call_id !== undefined && !optionalIdText(value.provider_call_id))
    || (value.call_id_hash !== undefined && value.call_id_hash !== null && !idText(value.call_id_hash, 128))
    || (value.call_id_truncated !== undefined && typeof value.call_id_truncated !== "boolean")
    || (value.source_refs !== undefined && value.source_refs !== null && !Array.isArray(value.source_refs))
    || (value.source_refs_truncated !== undefined && typeof value.source_refs_truncated !== "boolean")) {
    throw new Error("上下文清单条目数值无效");
  }
  const item = { ...value } as unknown as ObsManifestItem;
  if (!obsCategories.includes(item.category)) item.category = "unknown";
  item.message_index = typeof value.message_index === "number" ? value.message_index : null;
  item.tool_index = typeof value.tool_index === "number" ? value.tool_index : null;
  item.call_id = typeof value.call_id === "string" ? value.call_id : null;
  item.provider_call_id = typeof value.provider_call_id === "string" ? value.provider_call_id : null;
  item.source_refs = parseSourceRefs(value.source_refs ?? null);
  item.source_refs_truncated = value.source_refs_truncated === true;
  return item;
}

const manifestFields = ["version", "counter_version", "input_bytes", "estimated_tokens", "items", "model_context_window", "items_truncated", "omitted_items", "source_refs_truncated"] as const;
const manifestOptionalFields = ["compaction_changes", "reserved_output_tokens", "input_budget_bytes"] as const;
const compactionChangeFields = ["pass_index", "summary_step_index", "before_input_bytes", "after_input_bytes", "counter_version", "source_hash", "output_hash"] as const;

function parseCompactionChange(raw: unknown): ObsCompactionChange {
  const value = object(raw);
  if (compactionChangeFields.some(field => !(field in value)) || Object.keys(value).length !== compactionChangeFields.length) {
    throw new Error("压缩变化结构无效");
  }
  if (!nonNegativeInt(value.pass_index) || !integerOrNull(value.summary_step_index)
    || !nonNegativeInt(value.before_input_bytes) || !nonNegativeInt(value.after_input_bytes)
    || (value.counter_version !== null && !idText(value.counter_version))
    || (value.source_hash !== null && !idText(value.source_hash, 256))
    || (value.output_hash !== null && !idText(value.output_hash, 256))) {
    throw new Error("压缩变化数值无效");
  }
  return value as unknown as ObsCompactionChange;
}

/** model_start.data.context_manifest 原样投影；窗口未知为 null，不虚构空余。 */
export function parseManifest(raw: unknown): ObsManifest {
  const value = object(raw);
  if (manifestFields.some(field => !(field in value)) || Object.keys(value).some(key => !(manifestFields as readonly string[]).includes(key) && !(manifestOptionalFields as readonly string[]).includes(key))) {
    throw new Error("上下文清单结构无效");
  }
  if (value.version !== 1 || !idText(value.counter_version)
    || !nonNegativeInt(value.input_bytes)
    || !integerOrNull(value.estimated_tokens)
    || value.model_context_window !== null && (typeof value.model_context_window !== "number" || !Number.isSafeInteger(value.model_context_window) || value.model_context_window <= 0)
    || typeof value.items_truncated !== "boolean" || !integerOrNull(value.omitted_items)
    || typeof value.source_refs_truncated !== "boolean"
    || !Array.isArray(value.items)) {
    throw new Error("上下文清单数值无效");
  }
  if (value.compaction_changes !== undefined && value.compaction_changes !== null && !Array.isArray(value.compaction_changes)) {
    throw new Error("压缩变化集合无效");
  }
  const items = value.items.map(parseManifestItem);
  const compactionChanges = Array.isArray(value.compaction_changes) ? value.compaction_changes.map(parseCompactionChange) : null;
  return {
    ...(value as unknown as Omit<ObsManifest, "items" | "compaction_changes" | "reserved_output_tokens" | "input_budget_bytes">),
    items,
    compaction_changes: compactionChanges,
    reserved_output_tokens: typeof value.reserved_output_tokens === "number" ? value.reserved_output_tokens : null,
    input_budget_bytes: typeof value.input_budget_bytes === "number" ? value.input_budget_bytes : null,
  };
}

/** 非截断清单的分项字节合计守恒校验，用于数据状态页的口径提示。 */
export function manifestBytesSum(manifest: ObsManifest): number {
  return manifest.items.reduce((sum, item) => sum + item.input_bytes, 0);
}

export interface ObsRequestRecord {
  id: string;
  step_index: number;
  purpose: string | null;
  status: "ok" | "error" | "cancelled" | "interrupted" | "unknown";
  started_at: number;
  ended_at: number | null;
  duration_ms: number | null;
  duration_source: "mono" | "wall_clock" | null;
  timing: { elapsed_ms: number; first_content_ms: number | null; first_text_ms: number | null; origin: string | null; source: string } | null;
  tool_calls: number;
  usage: ObsItemUsage | null;
  manifest: ObsManifest | null;
  manifest_status: "present" | "invalid" | "missing";
  cost: null;
  cost_status: string;
}

export interface ObsToolRecord {
  id: string;
  call_id: string;
  provider_call_id: string | null;
  step_index: number | null;
  name: string | null;
  status: "ok" | "denied" | "failed" | "cancelled" | "unknown";
  code: string | null;
  decision: string | null;
  executor_started: boolean | null;
  result_available: boolean | null;
  /** 真实读取来源（仅 snapshot.work 的 workspace/artifact 成功读取）：历史 revision、路径与字节范围，无正文。 */
  source: { kind: "artifact_chunk"; revision_id: string; path: string; offset: number; total_size: number; length: number; sha256: string } | null;
  started_at: number;
  ended_at: number | null;
  duration_ms: number | null;
  duration_source: "mono" | "wall_clock" | null;
}

export interface ObsCompactionRecord {
  pass_index: number;
  status: string;
  created_at: number;
  finished_at: number | null;
  summary_step_index: number | null;
  covered_turns: number | null;
  covered_turn_ids: string[] | null;
  source_hash: string | null;
  output_hash: string | null;
  summary_bytes: number | null;
  counter_version: string | null;
  reserved_output_tokens: number | null;
}

export interface ObsCoverage {
  passes: number;
  complete_passes: number;
  covered_turns: number | null;
  history_turns: number | null;
  first_covered_turn_id: string | null;
  last_covered_turn_id: string | null;
  summary_step_index: number | null;
  manifest_status: "present" | "partial" | "missing";
  steps_with_usage: number;
  steps_missing_usage: number;
}

export interface ObsTurnObservations {
  session_id: string;
  turn_id: string;
  run: {
    run_id: string;
    trace_id: string;
    request_key: string;
    status: string;
    code: string | null;
    created_at: number;
    finished_at: number | null;
    duration_ms: number | null;
    duration_source: string | null;
  };
  trace: TraceDetail;
  configuration: ObsConfiguration | null;
  requests: ObsRequestRecord[];
  tools: ObsToolRecord[];
  compactions: ObsCompactionRecord[];
  coverage: ObsCoverage;
  usage: { totals: ObsUsage | null; usage_complete: boolean; steps_with_usage: number; steps_missing_usage: number };
  cost: null;
  cost_status: string;
}

export interface ObservationsIndexItem {
  source_kind: "session_turn";
  run_id: string;
  trace_id: string;
  session_id: string;
  turn_id: string;
  session_title: string;
  target_key: string;
  status: ObsRunStatus;
  created_at: number;
  finished_at: number | null;
  projection_status: "ok" | "unavailable";
  projection_code: string | null;
  duration_ms: number | null;
  duration_source: string | null;
  model_requests: number | null;
  tool_calls: number | null;
  usage: ObsUsage | null;
  usage_complete: boolean | null;
  usage_steps: number | null;
  steps_missing_usage: number | null;
  compaction_passes: number | null;
  manifest_requests: number | null;
  manifest_status: "present" | "partial" | "missing" | null;
  cost: null;
  cost_status: string;
}

export interface ObservationsScope {
  version: 1;
  workspace_id: string;
  role: string;
  status: string | null;
  session_id: string | null;
  limit: number;
  counted: "returned_page";
}

export interface ObservationsPage {
  scope: ObservationsScope;
  items: ObservationsIndexItem[];
  next_cursor: string | null;
}

const indexItemFields = ["source_kind", "run_id", "trace_id", "session_id", "turn_id", "session_title", "target_key", "status", "created_at", "finished_at", "projection_status", "projection_code", "duration_ms", "duration_source", "model_requests", "tool_calls", "usage", "usage_complete", "usage_steps", "steps_missing_usage", "compaction_passes", "manifest_requests", "manifest_status", "cost", "cost_status"] as const;

function timeValue(value: unknown): boolean {
  return typeof value === "number" && Number.isFinite(value);
}

function durationFields(value: unknown): boolean {
  return (value === null || idText(value, 32)) && true;
}

function usageOrNull(value: unknown): boolean {
  return value === null || (typeof value === "object" && value !== null && !Array.isArray(value));
}

function parseIndexItem(raw: unknown): ObservationsIndexItem {
  const value = object(raw);
  if (indexItemFields.some(field => !(field in value)) || Object.keys(value).some(key => !indexItemFields.includes(key as keyof ObservationsIndexItem))) {
    throw new Error("观测索引对象结构无效");
  }
  if (value.source_kind !== "session_turn"
    || !["run_id", "trace_id", "session_id", "turn_id"].every(key => idText(value[key]))
    || typeof value.session_title !== "string" || value.session_title.length > 128
    || !(String(value.target_key) in traceTargets)
    || !obsRunStatuses.includes(value.status as ObsRunStatus)
    || !timeValue(value.created_at) || (value.finished_at !== null && !timeValue(value.finished_at))
    || !["ok", "unavailable"].includes(String(value.projection_status))
    || (value.projection_code !== null && !idText(value.projection_code, 64))
    || !durationMsOrNull(value.duration_ms) || !durationFields(value.duration_source)
    || !integerOrNull(value.model_requests) || !integerOrNull(value.tool_calls)
    || !usageOrNull(value.usage) || (value.usage_complete !== null && typeof value.usage_complete !== "boolean")
    || !integerOrNull(value.usage_steps) || !integerOrNull(value.steps_missing_usage)
    || !integerOrNull(value.compaction_passes) || !integerOrNull(value.manifest_requests)
    || (value.manifest_status !== null && !["present", "partial", "missing"].includes(String(value.manifest_status)))
    || value.cost !== null || !idText(value.cost_status, 32)) {
    throw new Error("观测索引对象数值无效");
  }
  if (value.usage !== null) parseUsage(value.usage);
  return value as unknown as ObservationsIndexItem;
}

const scopeFields = ["version", "workspace_id", "role", "status", "session_id", "limit", "counted"] as const;

export function parseObservationsPage(raw: unknown): ObservationsPage {
  const value = object(raw);
  if (!("scope" in value) || !("items" in value) || !("next_cursor" in value) || Object.keys(value).length !== 3) {
    throw new Error("观测列表结构无效");
  }
  const scope = object(value.scope);
  if (scopeFields.some(field => !(field in scope)) || Object.keys(scope).length !== scopeFields.length
    || scope.version !== 1 || !idText(scope.workspace_id) || !idText(scope.role, 64)
    || (scope.status !== null && !obsRunStatuses.includes(scope.status as ObsRunStatus))
    || (scope.session_id !== null && !idText(scope.session_id))
    || typeof scope.limit !== "number" || !Number.isSafeInteger(scope.limit) || scope.limit < 1 || scope.limit > 50
    || scope.counted !== "returned_page") {
    throw new Error("观测列表 scope 无效");
  }
  if (!Array.isArray(value.items) || value.items.length > 50
    || (value.next_cursor !== null && (!idText(value.next_cursor, 4096) || /[\u0000-\u001f]/.test(String(value.next_cursor))))) {
    throw new Error("观测分页无效");
  }
  const seen = new Set<string>();
  for (const row of value.items) {
    const item = parseIndexItem(row);
    if (seen.has(item.run_id)) throw new Error("观测索引对象重复");
    seen.add(item.run_id);
  }
  return value as unknown as ObservationsPage;
}

const requestFields = ["id", "step_index", "purpose", "status", "started_at", "ended_at", "duration_ms", "duration_source", "timing", "tool_calls", "usage", "manifest", "manifest_status", "cost", "cost_status"] as const;
const requestStatuses = ["ok", "error", "cancelled", "interrupted", "unknown"];
const manifestStatuses = ["present", "invalid", "missing"];
const timingFields = ["elapsed_ms", "first_content_ms", "first_text_ms", "origin", "source"] as const;

function durationMsOrNull(value: unknown): boolean {
  return value === null || typeof value === "number" && Number.isFinite(value) && value >= 0;
}

function parseTiming(raw: unknown): { elapsed_ms: number; first_content_ms: number | null; first_text_ms: number | null; origin: string | null; source: string } | null {
  if (raw === null) return null;
  const value = object(raw);
  if (timingFields.some(field => !(field in value)) || Object.keys(value).length !== timingFields.length
    || typeof value.elapsed_ms !== "number" || !Number.isFinite(value.elapsed_ms) || value.elapsed_ms < 0
    || !durationMsOrNull(value.first_content_ms) || !durationMsOrNull(value.first_text_ms)
    || (value.origin !== null && !idText(value.origin, 32)) || !idText(value.source, 32)) {
    throw new Error("模型计时结构无效");
  }
  return value as unknown as { elapsed_ms: number; first_content_ms: number | null; first_text_ms: number | null; origin: string | null; source: string };
}

function parseItemUsage(raw: unknown): ObsItemUsage | null {
  if (raw === null) return null;
  const value = object(raw);
  if (!("usage" in value) || !("usage_reports" in value) || !("usage_final" in value) || Object.keys(value).length !== 3
    || !integerOrNull(value.usage_reports) || (value.usage_final !== null && typeof value.usage_final !== "boolean")) {
    throw new Error("模型用量包装结构无效");
  }
  return { usage: value.usage === null ? null : parseUsage(value.usage), usage_reports: value.usage_reports as number, usage_final: value.usage_final as boolean };
}

function parseRequestRecord(raw: unknown): ObsRequestRecord {
  const value = object(raw);
  if (requestFields.some(field => !(field in value)) || Object.keys(value).length !== requestFields.length) {
    throw new Error("模型请求对象结构无效");
  }
  if (!idText(value.id) || !nonNegativeInt(value.step_index)
    || (value.purpose !== null && !idText(value.purpose, 64)) || !requestStatuses.includes(String(value.status))
    || !timeValue(value.started_at) || (value.ended_at !== null && !timeValue(value.ended_at))
    || !durationMsOrNull(value.duration_ms) || ![null, "mono", "wall_clock"].includes(value.duration_source as string | null)
    || !nonNegativeInt(value.tool_calls)
    || !manifestStatuses.includes(String(value.manifest_status))
    || value.cost !== null || !idText(value.cost_status, 32)) {
    throw new Error("模型请求对象数值无效");
  }
  return {
    ...(value as unknown as ObsRequestRecord),
    timing: parseTiming(value.timing),
    usage: parseItemUsage(value.usage),
    manifest: value.manifest === null ? null : parseManifest(value.manifest),
  };
}

const toolFields = ["id", "call_id", "provider_call_id", "step_index", "name", "status", "code", "decision", "executor_started", "result_available", "source", "started_at", "ended_at", "duration_ms", "duration_source"] as const;
const toolStatuses = ["ok", "denied", "failed", "cancelled", "unknown"];

function parseToolSource(raw: unknown): ObsToolRecord["source"] {
  if (raw === null) return null;
  const value = object(raw);
  if (Object.keys(value).length !== 7
    || value.kind !== "artifact_chunk"
    || !idText(value.revision_id) || !idText(value.path, 1024)
    || !nonNegativeInt(value.offset) || !nonNegativeInt(value.total_size) || !nonNegativeInt(value.length)
    || !idText(value.sha256, 128)) {
    throw new Error("工具来源结构无效");
  }
  return value as unknown as ObsToolRecord["source"];
}

function parseToolRecord(raw: unknown): ObsToolRecord {
  const value = object(raw);
  if (toolFields.some(field => !(field in value)) || Object.keys(value).length !== toolFields.length) {
    throw new Error("工具对象结构无效");
  }
  if (!idText(value.id) || !idText(value.call_id) || !optionalIdText(value.provider_call_id)
    || !integerOrNull(value.step_index)
    || (value.name !== null && !idText(value.name, 128)) || !toolStatuses.includes(String(value.status))
    || (value.code !== null && !idText(value.code, 64)) || (value.decision !== null && !idText(value.decision, 64))
    || (value.executor_started !== null && typeof value.executor_started !== "boolean")
    || (value.result_available !== null && typeof value.result_available !== "boolean")
    || !timeValue(value.started_at) || (value.ended_at !== null && !timeValue(value.ended_at))
    || !durationMsOrNull(value.duration_ms) || ![null, "mono", "wall_clock"].includes(value.duration_source as string | null)) {
    throw new Error("工具对象数值无效");
  }
  return { ...(value as unknown as ObsToolRecord), source: parseToolSource(value.source) };
}

const compactionFields = ["pass_index", "status", "created_at", "finished_at", "summary_step_index", "covered_turns", "covered_turn_ids", "source_hash", "output_hash", "summary_bytes", "counter_version", "reserved_output_tokens"] as const;

function parseCompactionRecord(raw: unknown): ObsCompactionRecord {
  const value = object(raw);
  if (compactionFields.some(field => !(field in value)) || Object.keys(value).length !== compactionFields.length) {
    throw new Error("压缩记录结构无效");
  }
  if (!nonNegativeInt(value.pass_index)
    || !idText(value.status, 64) || !timeValue(value.created_at) || (value.finished_at !== null && !timeValue(value.finished_at))
    || !integerOrNull(value.summary_step_index) || !integerOrNull(value.covered_turns)
    || !idListOrNull(value.covered_turn_ids)
    || (value.source_hash !== null && !idText(value.source_hash, 256))
    || (value.output_hash !== null && !idText(value.output_hash, 256))
    || !integerOrNull(value.summary_bytes) || (value.counter_version !== null && !idText(value.counter_version))
    || !integerOrNull(value.reserved_output_tokens)) {
    throw new Error("压缩记录数值无效");
  }
  return value as unknown as ObsCompactionRecord;
}

const coverageFields = ["passes", "complete_passes", "covered_turns", "history_turns", "first_covered_turn_id", "last_covered_turn_id", "summary_step_index", "manifest_status", "steps_with_usage", "steps_missing_usage"] as const;

function parseCoverage(raw: unknown): ObsCoverage {
  const value = object(raw);
  if (coverageFields.some(field => !(field in value)) || Object.keys(value).length !== coverageFields.length
    || !nonNegativeInt(value.passes)
    || !nonNegativeInt(value.complete_passes)
    || !integerOrNull(value.covered_turns) || !integerOrNull(value.history_turns)
    || (value.first_covered_turn_id !== null && !idText(value.first_covered_turn_id))
    || (value.last_covered_turn_id !== null && !idText(value.last_covered_turn_id))
    || !integerOrNull(value.summary_step_index)
    || !["present", "partial", "missing"].includes(String(value.manifest_status))
    || !nonNegativeInt(value.steps_with_usage)
    || !nonNegativeInt(value.steps_missing_usage)) {
    throw new Error("覆盖信息结构无效");
  }
  return value as unknown as ObsCoverage;
}

const detailFields = ["session_id", "turn_id", "run", "trace", "configuration", "requests", "tools", "compactions", "coverage", "usage", "cost", "cost_status"] as const;

export function parseTurnObservations(raw: unknown, item: Pick<ObservationsIndexItem, "session_id" | "turn_id"> & Partial<Pick<ObservationsIndexItem, "run_id" | "trace_id">>): ObsTurnObservations {
  const value = object(raw);
  if (detailFields.some(field => !(field in value)) || Object.keys(value).length !== detailFields.length) {
    throw new Error("观测详情结构无效");
  }
  if (value.session_id !== item.session_id || value.turn_id !== item.turn_id) throw new Error("观测详情来源不匹配");
  const run = object(value.run);
  if (item.run_id && run.run_id !== item.run_id || item.trace_id && run.trace_id !== item.trace_id
    || !idText(run.run_id) || !idText(run.trace_id)
    || !idText(run.request_key) || !idText(run.status, 32) || (run.code !== null && !idText(run.code, 64))
    || !timeValue(run.created_at) || (run.finished_at !== null && !timeValue(run.finished_at))
    || !durationMsOrNull(run.duration_ms) || !durationFields(run.duration_source)) {
    throw new Error("运行事实无效");
  }
  const traceIdentity = {
    run_id: run.run_id,
    trace_id: run.trace_id,
    session_id: value.session_id,
    turn_id: value.turn_id,
  } as TraceIndexItem;
  const trace = parseTraceDetail({ session_id: value.session_id, turn_id: value.turn_id, trace: value.trace }, traceIdentity);
  const configuration = parseConfiguration(value.configuration);
  if (!Array.isArray(value.requests) || value.requests.length > 512
    || !Array.isArray(value.tools) || value.tools.length > 2048
    || !Array.isArray(value.compactions) || value.compactions.length > 64
    || value.cost !== null || !idText(value.cost_status, 32)) {
    throw new Error("观测详情集合无效");
  }
  const requests = value.requests.map(parseRequestRecord);
  const tools = value.tools.map(parseToolRecord);
  const compactions = value.compactions.map(parseCompactionRecord);
  const coverage = parseCoverage(value.coverage);
  const usage = object(value.usage);
  if (!("totals" in usage) || !("usage_complete" in usage) || !("steps_with_usage" in usage) || !("steps_missing_usage" in usage) || Object.keys(usage).length !== 4
    || typeof usage.usage_complete !== "boolean"
    || !nonNegativeInt(usage.steps_with_usage)
    || !nonNegativeInt(usage.steps_missing_usage)) {
    throw new Error("用量汇总结构无效");
  }
  const totals = usage.totals === null ? null : parseUsage(usage.totals);
  return {
    session_id: value.session_id,
    turn_id: value.turn_id,
    run: run as unknown as ObsTurnObservations["run"],
    trace,
    configuration,
    requests,
    tools,
    compactions,
    coverage,
    usage: { totals, usage_complete: usage.usage_complete as boolean, steps_with_usage: usage.steps_with_usage as number, steps_missing_usage: usage.steps_missing_usage as number },
    cost: null,
    cost_status: value.cost_status as string,
  };
}

/** 供 hooks 复用的观测选择对象：即列表条目本身。 */
export type ObsSelection = ObservationsIndexItem;

export type { TraceDetail, TraceSpan };

/** 相邻请求差分：契约规定为前端纯计算，以 key 对齐、content_hash 判等、字节比较定量。 */
export type ObsDiffChange = "added" | "removed" | "changed" | "kept";

export interface ObsDiffRow {
  key: string;
  category: ObsCategory;
  label: string;
  left: ObsManifestItem | null;
  right: ObsManifestItem | null;
  left_bytes: number;
  right_bytes: number;
  delta_bytes: number;
  change: ObsDiffChange;
}

export function diffManifests(left: ObsManifest | null, right: ObsManifest): ObsDiffRow[] {
  const leftItems = left?.items ?? [];
  const rows: ObsDiffRow[] = [];
  const byKey = new Map(leftItems.map(item => [item.key, item]));
  const seen = new Set<string>();
  for (const rightItem of right.items) {
    seen.add(rightItem.key);
    const leftItem = byKey.get(rightItem.key) ?? null;
    const change: ObsDiffChange = !leftItem ? "added"
      : leftItem.content_hash !== rightItem.content_hash || leftItem.category !== rightItem.category ? "changed"
        : leftItem.input_bytes === rightItem.input_bytes ? "kept" : "changed";
    rows.push({
      key: rightItem.key,
      category: rightItem.category,
      label: rightItem.label,
      left: leftItem,
      right: rightItem,
      left_bytes: leftItem?.input_bytes ?? 0,
      right_bytes: rightItem.input_bytes,
      delta_bytes: rightItem.input_bytes - (leftItem?.input_bytes ?? 0),
      change,
    });
  }
  for (const leftItem of leftItems) {
    if (!seen.has(leftItem.key)) {
      rows.push({
        key: leftItem.key, category: leftItem.category, label: leftItem.label,
        left: leftItem, right: null,
        left_bytes: leftItem.input_bytes, right_bytes: 0, delta_bytes: -leftItem.input_bytes, change: "removed",
      });
    }
  }
  return rows.sort((a, b) => Math.abs(b.delta_bytes) - Math.abs(a.delta_bytes) || (a.key < b.key ? -1 : 1));
}
