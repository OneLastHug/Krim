import { object, request } from "../api.ts";
import { sessionPath } from "./session-contracts.ts";

export type ExecutionMode = "manual" | "auto" | "bypass";
export type ApprovalScope = "once" | "session";
export interface ExecutionPolicy {
  session_id: string; mode: ExecutionMode; revision: number; updated_at: number; updated_by: string;
  project_id: string | null; project_access: "read" | "write" | null; project_access_revision: number;
}
export interface ProjectPermission {
  session_id: string; project_id: string; title: string; owner_id: string;
  project_access: "read" | "write"; is_creator: boolean; sandbox_status: string;
}
export interface ModeUpdate { mode: ExecutionMode; expected_revision: number; request_key: string }
export const modeLabels: Record<ExecutionMode, string> = { manual: "手动确认", auto: "自动执行", bypass: "跳过确认" };
export const modeDescriptions: Record<ExecutionMode, string> = {
  manual: "读取和搜索直接执行，修改文件、运行命令等操作需要确认。",
  auto: "常规项目修改、构建和测试自动执行；不明确或影响范围大的操作仍需确认。",
  bypass: "当前会话的项目工具免确认执行，仍受项目访问权限约束。",
};
const integer = (v: unknown): v is number => typeof v === "number" && Number.isSafeInteger(v) && v >= 0;
export const isExecutionMode = (v: unknown): v is ExecutionMode => v === "manual" || v === "auto" || v === "bypass";
function identity(value: Record<string, unknown>, sessionId: string) {
  if (value.session_id !== sessionId) throw new Error("执行策略会话不匹配");
}
export function parseExecutionPolicy(raw: unknown, sessionId: string): ExecutionPolicy {
  const value = object(raw); identity(value, sessionId);
  if (!isExecutionMode(value.mode) || !integer(value.revision) || typeof value.updated_at !== "number"
    || !Number.isFinite(value.updated_at) || typeof value.updated_by !== "string"
    || !integer(value.project_access_revision)
    || !(value.project_id === null || typeof value.project_id === "string" && !!value.project_id)
    || ![null, "read", "write"].includes(value.project_access as null | string)
    || (value.project_id === null) !== (value.project_access === null)) throw new Error("执行策略响应无效");
  return value as unknown as ExecutionPolicy;
}
export function parseProjectPermission(raw: unknown, sessionId: string): ProjectPermission {
  const value = object(raw); identity(value, sessionId);
  if (typeof value.project_id !== "string" || !value.project_id || typeof value.title !== "string"
    || typeof value.owner_id !== "string" || !value.owner_id || typeof value.is_creator !== "boolean"
    || !["read", "write"].includes(String(value.project_access))
    || typeof value.sandbox_status !== "string" || !value.sandbox_status) throw new Error("项目权限响应无效");
  return value as unknown as ProjectPermission;
}
function parseRuleCount(raw: unknown, sessionId: string): number {
  const value = object(raw); identity(value, sessionId);
  if (!integer(value.rule_count)) throw new Error("会话允许规则响应无效");
  return value.rule_count;
}
export const executionPolicyPath = (workspace: string, sessionId: string) =>
  sessionPath(workspace) + "/" + encodeURIComponent(sessionId) + "/execution-policy";
export async function readExecutionPolicy(key: string, workspace: string, sessionId: string, signal?: AbortSignal) {
  return parseExecutionPolicy(await request(executionPolicyPath(workspace, sessionId), key, undefined, signal), sessionId);
}
export async function updateExecutionMode(key: string, workspace: string, sessionId: string, body: ModeUpdate) {
  return parseExecutionPolicy(await request(executionPolicyPath(workspace, sessionId), key, body, undefined, "PUT"), sessionId);
}
export async function readAllowRuleCount(key: string, workspace: string, sessionId: string, signal?: AbortSignal) {
  return parseRuleCount(await request(executionPolicyPath(workspace, sessionId) + "/allow-rules", key, undefined, signal), sessionId);
}
export async function clearSessionAllowRules(key: string, workspace: string, sessionId: string, body: { request_key: string }) {
  const raw = await request(executionPolicyPath(workspace, sessionId) + "/allow-rules", key, body, undefined, "DELETE");
  const value = object(raw); parseRuleCount(value, sessionId);
  if (!integer(value.cleared_count)) throw new Error("规则清理响应无效");
}
export async function readProjectPermission(key: string, workspace: string, sessionId: string, signal?: AbortSignal) {
  return parseProjectPermission(await request(executionPolicyPath(workspace, sessionId) + "/project", key, undefined, signal), sessionId);
}
