import { object } from "../api.ts";

export interface Actor { principal_id: string; generation: number; legacy: boolean; target_keys?: string[]; workspaces: { workspace_id: string; role: string; approve: boolean }[] }
export interface SessionContextPolicy { max_bytes: number | null; max_messages: number | null; reserved_output_tokens: number; counter_version: string; max_tokens?: number; compaction_trigger_tokens?: number }
export interface Session { id: string; workspace_id: string; owner_id: string; create_key: string; title: string; target_key: string; target_fingerprint: string; context_policy: SessionContextPolicy; revision: number; status: "open" | "closed"; dispatch_paused: boolean; created_at: number; closed_at: number | null; archived_at: number | null; deleted_at: number | null; lifecycle_revision: number }
export interface HistoryMessage { role: string; content: string; call_id: string | null; tool_calls: Record<string, unknown>[] }
export interface Turn { id: string; session_id: string; workspace_id: string; owner_id: string; ordinal: number; request_key: string; text: string; accepted_revision: number; status: "queued" | "running" | "completed" | "failed" | "cancelled" | "interrupted"; run_id: string | null; context_fingerprint: string | null; transcript: HistoryMessage[] | null; code: string | null; created_at: number; started_at: number | null; finished_at: number | null }
const integer = (v: unknown): v is number => typeof v === "number" && Number.isSafeInteger(v) && v >= 0;
const time = (v: unknown) => v === null || (typeof v === "number" && Number.isFinite(v));
const text = (v: unknown): v is string => typeof v === "string";
function exact(v: Record<string, unknown>, fields: string[]) { if (Object.keys(v).some(k => !fields.includes(k)) || fields.some(k => !(k in v))) throw new Error("响应结构无效"); }
export function parseActor(raw: unknown): Actor {
  const value = object(raw);
  if (Object.keys(value).some(k => !["principal_id", "generation", "legacy", "workspaces", "target_keys"].includes(k)) || ["principal_id", "generation", "legacy", "workspaces"].some(k => !(k in value))) throw new Error("身份响应无效");
  if (value.target_keys !== undefined && (!Array.isArray(value.target_keys) || value.target_keys.length > 7 || value.target_keys.some(k => !["demo", "target-v1", "target-v2", "target-bad", "target-http", "target-workspace", "target-development"].includes(String(k))))) throw new Error("目标响应无效");
  if (!text(value.principal_id) || !integer(value.generation) || typeof value.legacy !== "boolean" || !Array.isArray(value.workspaces) || value.workspaces.length > 64) throw new Error("身份响应无效");
  for (const item of value.workspaces) { const w = object(item); exact(w, ["workspace_id", "role", "approve"]); if (!text(w.workspace_id) || !["member", "viewer", "admin"].includes(String(w.role)) || typeof w.approve !== "boolean") throw new Error("工作区响应无效"); }
  return value as unknown as Actor;
}
export function parseSession(raw: unknown): Session {
  const value = object(raw);
  const fields = ["id", "workspace_id", "owner_id", "create_key", "title", "target_key", "target_fingerprint", "context_policy", "revision", "status", "dispatch_paused", "created_at", "closed_at"];
  const lifecycle = ["archived_at", "deleted_at", "lifecycle_revision"];
  if (Object.keys(value).some(key => !fields.includes(key) && !lifecycle.includes(key)) || fields.some(key => !(key in value))) throw new Error("响应结构无效");
  const hasLifecycle = lifecycle.some(key => key in value);
  if (hasLifecycle && lifecycle.some(key => !(key in value))) throw new Error("响应结构无效");
  const normalized = hasLifecycle ? value : { ...value, archived_at: null, deleted_at: null, lifecycle_revision: 0 };
  for (const key of ["id", "workspace_id", "owner_id", "create_key", "title", "target_key"]) if (!text(normalized[key])) throw new Error("对话身份无效");
  const policy = object(normalized.context_policy);
  const basePolicyFields = ["max_bytes", "max_messages", "reserved_output_tokens", "counter_version"];
  const tokenFields = ["max_tokens", "compaction_trigger_tokens"];
  if (basePolicyFields.some(key => !(key in policy)) || Object.keys(policy).some(key => !basePolicyFields.includes(key) && !tokenFields.includes(key))) throw new Error("上下文策略无效");
  const hasTokenWindow = tokenFields.some(key => key in policy);
  if (hasTokenWindow && (tokenFields.some(key => !(key in policy)) || !integer(policy.max_tokens) || policy.max_tokens < 1 || !integer(policy.compaction_trigger_tokens) || policy.compaction_trigger_tokens < 1 || !integer(policy.reserved_output_tokens) || policy.counter_version !== "approx-utf8/1" && policy.compaction_trigger_tokens + policy.reserved_output_tokens > policy.max_tokens)) throw new Error("上下文窗口无效");
  if (typeof policy.counter_version !== "string" || !/^[a-zA-Z][a-zA-Z0-9_.:/-]{0,127}$/.test(policy.counter_version) || !hasTokenWindow && policy.counter_version !== "approx-utf8/1" || policy.max_bytes !== null && (!integer(policy.max_bytes) || policy.max_bytes < 256) || policy.max_bytes === null && policy.counter_version === "approx-utf8/1") throw new Error("上下文计量无效");
  if ((policy.max_messages !== null && (!integer(policy.max_messages) || policy.max_messages < 2)) || !integer(policy.reserved_output_tokens) || policy.reserved_output_tokens < 1 || !integer(normalized.revision) || !["open", "closed"].includes(String(normalized.status)) || typeof normalized.dispatch_paused !== "boolean" || !time(normalized.created_at) || normalized.created_at === null || !time(normalized.closed_at) || !time(normalized.archived_at) || !time(normalized.deleted_at) || !integer(normalized.lifecycle_revision) || (!!normalized.deleted_at && normalized.status !== "closed") || !/^[a-f0-9]{64}$/.test(String(normalized.target_fingerprint))) throw new Error("对话响应无效");
  return normalized as unknown as Session;
}
export function parseTurn(raw: unknown): Turn {
  const value = object(raw); exact(value, ["id", "session_id", "workspace_id", "owner_id", "ordinal", "request_key", "text", "accepted_revision", "status", "run_id", "context_fingerprint", "transcript", "code", "created_at", "started_at", "finished_at"]);
  for (const key of ["id", "session_id", "workspace_id", "owner_id", "request_key", "text"]) if (!text(value[key])) throw new Error("消息身份无效");
  if (!integer(value.ordinal) || !integer(value.accepted_revision) || !["queued", "running", "completed", "failed", "cancelled", "interrupted"].includes(String(value.status)) || !time(value.created_at) || value.created_at === null || !time(value.started_at) || !time(value.finished_at) || (value.run_id !== null && !text(value.run_id)) || (value.context_fingerprint !== null && !/^[a-f0-9]{64}$/.test(String(value.context_fingerprint))) || (value.code !== null && !text(value.code))) throw new Error("消息响应无效");
  if (value.transcript !== null) {
    if (!Array.isArray(value.transcript)) throw new Error("历史响应无效");
    for (const item of value.transcript) { const m = object(item); exact(m, ["role", "content", "call_id", "tool_calls"]); if (!["user", "assistant", "tool"].includes(String(m.role)) || !text(m.content) || (m.call_id !== null && !text(m.call_id)) || !Array.isArray(m.tool_calls)) throw new Error("历史消息无效"); }
  }
  return value as unknown as Turn;
}
export const sessionPath = (workspace: string) => "/v2/workspaces/" + encodeURIComponent(workspace) + "/sessions";
export const turnPath = (workspace: string, session: string, turn: string) => sessionPath(workspace) + "/" + encodeURIComponent(session) + "/turns/" + encodeURIComponent(turn);
