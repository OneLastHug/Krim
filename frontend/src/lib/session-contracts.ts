import { object } from "../api.ts";

export interface Actor { principal_id: string; generation: number; legacy: boolean; target_keys?: string[]; workspaces: { workspace_id: string; role: string; approve: boolean }[] }
export interface Session { id: string; workspace_id: string; owner_id: string; create_key: string; title: string; target_key: string; target_fingerprint: string; context_policy: { max_bytes: number; max_messages: number; reserved_output_tokens: number; counter_version: string }; revision: number; status: "open" | "closed"; dispatch_paused: boolean; created_at: number; closed_at: number | null }
export interface HistoryMessage { role: string; content: string; call_id: string | null; tool_calls: Record<string, unknown>[] }
export interface Turn { id: string; session_id: string; workspace_id: string; owner_id: string; ordinal: number; request_key: string; text: string; accepted_revision: number; status: "queued" | "running" | "completed" | "failed" | "cancelled" | "interrupted"; run_id: string | null; context_fingerprint: string | null; transcript: HistoryMessage[] | null; code: string | null; created_at: number; started_at: number | null; finished_at: number | null }
const integer = (v: unknown): v is number => typeof v === "number" && Number.isSafeInteger(v) && v >= 0;
const time = (v: unknown) => v === null || (typeof v === "number" && Number.isFinite(v));
const text = (v: unknown): v is string => typeof v === "string";
function exact(v: Record<string, unknown>, fields: string[]) { if (Object.keys(v).some(k => !fields.includes(k)) || fields.some(k => !(k in v))) throw new Error("响应结构无效"); }
export function parseActor(raw: unknown): Actor {
  const value = object(raw);
  if (Object.keys(value).some(k => !["principal_id", "generation", "legacy", "workspaces", "target_keys"].includes(k)) || ["principal_id", "generation", "legacy", "workspaces"].some(k => !(k in value))) throw new Error("身份响应无效");
  if (value.target_keys !== undefined && (!Array.isArray(value.target_keys) || value.target_keys.length > 6 || value.target_keys.some(k => !["demo", "target-v1", "target-v2", "target-bad", "target-http", "target-workspace"].includes(String(k))))) throw new Error("目标响应无效");
  if (!text(value.principal_id) || !integer(value.generation) || typeof value.legacy !== "boolean" || !Array.isArray(value.workspaces) || value.workspaces.length > 64) throw new Error("身份响应无效");
  for (const item of value.workspaces) { const w = object(item); exact(w, ["workspace_id", "role", "approve"]); if (!text(w.workspace_id) || !["member", "viewer", "admin"].includes(String(w.role)) || typeof w.approve !== "boolean") throw new Error("工作区响应无效"); }
  return value as unknown as Actor;
}
export function parseSession(raw: unknown): Session {
  const value = object(raw);
  exact(value, ["id", "workspace_id", "owner_id", "create_key", "title", "target_key", "target_fingerprint", "context_policy", "revision", "status", "dispatch_paused", "created_at", "closed_at"]);
  for (const key of ["id", "workspace_id", "owner_id", "create_key", "title", "target_key"]) if (!text(value[key])) throw new Error("对话身份无效");
  const policy = object(value.context_policy); exact(policy, ["max_bytes", "max_messages", "reserved_output_tokens", "counter_version"]);
  if (!integer(policy.max_bytes) || !integer(policy.max_messages) || !integer(policy.reserved_output_tokens) || policy.counter_version !== "approx-utf8/1" || !integer(value.revision) || !["open", "closed"].includes(String(value.status)) || typeof value.dispatch_paused !== "boolean" || !time(value.created_at) || value.created_at === null || !time(value.closed_at) || !/^[a-f0-9]{64}$/.test(String(value.target_fingerprint))) throw new Error("对话响应无效");
  return value as unknown as Session;
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
