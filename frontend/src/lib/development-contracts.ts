import { object } from "../api.ts";

// 开发信息栏的后端契约；解析与 session-contracts 同等严格，未实现的接口不在此出现。

export interface SandboxProject {
  project_id: string; environment_id: string; principal_id: string; workspace_id: string;
  title: string; generation: number; status: string; created_at: number; updated_at: number;
}
export interface ProjectsPage { items: SandboxProject[]; total: number; offset: number; limit: number }
export interface SessionBinding {
  session_id: string; project_id: string | null; revision: number; session_revision: number; workspace_id: string;
}
export type EnvironmentState = "creating" | "stopped" | "starting" | "ready" | "stopping" | "failed" | "unknown";
export interface EnvironmentView {
  owner_id: string; project_id: string; environment_id: string; generation: number; state: EnvironmentState;
}
export interface ToolEntry {
  name: string; version: string | null; risk: string | null;
  kind: "http" | "mcp" | null; server: string | null; tool: string | null; fingerprint: string | null;
}
export interface McpConnection {
  id: string; name: string; url: string; allowed_hosts: string[]; selected_tools: string[];
  readonly_tools: string[]; timeout_seconds: number; revision: number;
  credential_revision: number; credential_configured: boolean;
}
export interface SecretResult { secret_id: string; revision: number; revoked: boolean }
export interface DirectoryEntry { name: string; type: "directory" | "file" | "blocked"; size: number }
export interface DirectoryPage {
  path: string; entries: DirectoryEntry[]; total: number; next_offset: number | null;
}
export interface FileData {
  path: string; offset: number; size: number; eof: boolean; sha256: string;
  stat_token: number[]; data: string; may_change: boolean;
}
export interface OperationReceipt { request_key: string; kind: string; status: string }

const integer = (v: unknown): v is number => typeof v === "number" && Number.isSafeInteger(v) && v >= 0;
const text = (v: unknown): v is string => typeof v === "string";
function exact(value: Record<string, unknown>, fields: string[]) {
  if (Object.keys(value).some(k => !fields.includes(k)) || fields.some(k => !(k in value))) throw new Error("响应结构无效");
}

export function parseProject(raw: unknown): SandboxProject {
  const value = object(raw);
  exact(value, ["project_id", "environment_id", "principal_id", "workspace_id", "title", "generation", "status", "created_at", "updated_at"]);
  for (const key of ["project_id", "environment_id", "principal_id", "workspace_id", "title", "status"]) if (!text(value[key])) throw new Error("项目响应无效");
  if (!integer(value.generation) || typeof value.created_at !== "number" || !Number.isFinite(value.created_at) || value.created_at < 0 || typeof value.updated_at !== "number" || !Number.isFinite(value.updated_at) || value.updated_at < 0) throw new Error("项目响应无效");
  return value as unknown as SandboxProject;
}

export function parseProjectsPage(raw: unknown): ProjectsPage {
  const value = object(raw);
  exact(value, ["items", "total", "offset", "limit"]);
  if (!Array.isArray(value.items) || !integer(value.total) || !integer(value.offset) || !integer(value.limit)) throw new Error("项目列表无效");
  return { items: value.items.map(parseProject), total: value.total, offset: value.offset, limit: value.limit };
}

export function parseBinding(raw: unknown): SessionBinding {
  const value = object(raw);
  exact(value, ["session_id", "project_id", "revision", "session_revision", "workspace_id"]);
  if (!text(value.session_id) || !text(value.workspace_id)) throw new Error("绑定响应无效");
  if (value.project_id !== null && !text(value.project_id)) throw new Error("绑定响应无效");
  if (!integer(value.revision) || !integer(value.session_revision)) throw new Error("绑定响应无效");
  return value as unknown as SessionBinding;
}

const ENVIRONMENT_STATES = ["creating", "stopped", "starting", "ready", "stopping", "failed", "unknown"];
export function parseEnvironment(raw: unknown): EnvironmentView {
  const value = object(raw);
  exact(value, ["owner_id", "project_id", "environment_id", "generation", "state"]);
  for (const key of ["owner_id", "project_id", "environment_id"]) if (!text(value[key])) throw new Error("环境响应无效");
  if (!integer(value.generation) || !ENVIRONMENT_STATES.includes(String(value.state))) throw new Error("环境响应无效");
  return value as unknown as EnvironmentView;
}

// 工具目录允许内部工具缺少 source；外部工具必须携带完整来源。
export function parseToolCatalog(raw: unknown): ToolEntry[] {
  if (!Array.isArray(raw)) throw new Error("工具目录无效");
  return raw.map((item): ToolEntry => {
    const value = object(item);
    if (!text(value.name) || !value.name) throw new Error("工具目录无效");
    let kind: ToolEntry["kind"] = null, server: string | null = null, tool: string | null = null;
    if (value.source !== undefined) {
      const source = object(value.source);
      if ((source.kind !== "http" && source.kind !== "mcp") || !text(source.server) || !source.server || !text(source.tool) || !source.tool) throw new Error("工具目录无效");
      kind = source.kind; server = source.server; tool = source.tool;
    }
    return {
      name: value.name,
      version: text(value.version) ? value.version : null,
      risk: text(value.risk) ? value.risk : null,
      kind, server, tool,
      fingerprint: text(value.fingerprint) ? value.fingerprint : null,
    };
  });
}

export function parseMcpConnections(raw: unknown): McpConnection[] {
  if (!Array.isArray(raw)) throw new Error("连接列表无效");
  return raw.map((item): McpConnection => {
    const value = object(item);
    exact(value, ["id", "name", "url", "allowed_hosts", "selected_tools", "readonly_tools", "timeout_seconds", "revision", "credential_revision", "credential_configured"]);
    for (const key of ["id", "name", "url"]) if (!text(value[key])) throw new Error("连接响应无效");
    for (const key of ["allowed_hosts", "selected_tools", "readonly_tools"]) {
      if (!Array.isArray(value[key]) || value[key].some(entry => !text(entry))) throw new Error("连接响应无效");
    }
    if (typeof value.timeout_seconds !== "number" || !Number.isFinite(value.timeout_seconds)) throw new Error("连接响应无效");
    if (!integer(value.revision) || !integer(value.credential_revision) || typeof value.credential_configured !== "boolean") throw new Error("连接响应无效");
    return value as unknown as McpConnection;
  });
}

export function parseSecretResult(raw: unknown): SecretResult {
  const value = object(raw);
  exact(value, ["secret_id", "revision", "revoked"]);
  if (!text(value.secret_id) || !integer(value.revision) || typeof value.revoked !== "boolean") throw new Error("凭据响应无效");
  return value as unknown as SecretResult;
}

export function parseDirectory(raw: unknown): DirectoryPage {
  const value = object(raw);
  exact(value, ["path", "entries", "offset", "next_offset", "total", "project_id", "generation", "may_change"]);
  if (!text(value.path) || !Array.isArray(value.entries) || !integer(value.total)) throw new Error("目录响应无效");
  if (value.next_offset !== null && !integer(value.next_offset)) throw new Error("目录响应无效");
  const entries = value.entries.map((item): DirectoryEntry => {
    const entry = object(item);
    exact(entry, ["name", "type", "size"]);
    if (!text(entry.name) || !["directory", "file", "blocked"].includes(String(entry.type)) || !integer(entry.size)) throw new Error("目录项无效");
    return entry as unknown as DirectoryEntry;
  });
  return { path: value.path, entries, total: value.total, next_offset: value.next_offset as number | null };
}

export function parseFileData(raw: unknown): FileData {
  const value = object(raw);
  exact(value, ["path", "offset", "size", "eof", "sha256", "stat_token", "data", "project_id", "generation", "may_change"]);
  if (!text(value.path) || !integer(value.offset) || !integer(value.size) || typeof value.eof !== "boolean") throw new Error("文件响应无效");
  if (!text(value.sha256) || !/^[0-9a-f]{64}$/.test(value.sha256)) throw new Error("文件响应无效");
  if (!text(value.data) || !/^[A-Za-z0-9+/]*={0,2}$/.test(value.data)) throw new Error("文件响应无效");
  const statToken = value.stat_token;
  // Linux nanosecond timestamps exceed JavaScript's safe-integer range; the full-file
  // digest also participates in cross-chunk validation and is checked after assembly.
  if (!Array.isArray(statToken) || statToken.length !== 5 || statToken.some((item, index) => typeof item !== "number" || !Number.isFinite(item) || !Number.isInteger(item) || index < 3 && item < 0)) throw new Error("文件响应无效");
  return {
    path: value.path, offset: value.offset, size: value.size, eof: value.eof, sha256: value.sha256,
    stat_token: statToken, data: value.data, may_change: value.may_change === true,
  };
}

export function parseOperation(raw: unknown): OperationReceipt {
  const value = object(raw);
  exact(value, ["operation_id", "request_key", "kind", "status", "result"]);
  const requestKey = value.request_key, kind = value.kind, status = value.status;
  if (!text(value.operation_id) || !text(requestKey) || !text(kind) || !text(status)) throw new Error("回执响应无效");
  return { request_key: requestKey, kind, status };
}

// 写意图账本：与 use-session 的 controls 同思路，只保存可回放的字段，凭据值绝不入账。
export const DEV_WRITES = "krim.development.writes/1";
export type DevWriteKind = "bind-project" | "prepare-environment";
export interface DevWriteIntent {
  principal_id: string; generation: number; workspace_id: string; session_id: string;
  project_id: string | null; kind: DevWriteKind; request_key: string; target: string;
  body: Record<string, unknown>; rejected: boolean;
}
const WRITE_KINDS = ["bind-project", "prepare-environment"];
const MAX_WRITES = 12;
const bindTarget = (sessionId: string) => `/v2/me/sandbox/sessions/${encodeURIComponent(sessionId)}/project`;
const prepareTarget = (projectId: string) => `/v2/me/development/projects/${encodeURIComponent(projectId)}/prepare`;

// kind 决定 target 与 body 白名单；携带 value/secret 等额外字段的记录直接判损坏。
function validateWriteIntent(intent: DevWriteIntent): void {
  if (intent.kind === "bind-project") {
    if (intent.target !== bindTarget(intent.session_id)) throw new Error("无法读取未确认请求");
    const body = intent.body;
    const fields = Object.keys(body);
    if (fields.length !== 4 || !["request_key", "expected_revision", "expected_session_revision", "project_id"].every(k => fields.includes(k))) throw new Error("无法读取未确认请求");
    if (!integer(body.expected_revision) || !integer(body.expected_session_revision)) throw new Error("无法读取未确认请求");
    if (body.project_id !== intent.project_id || (body.project_id !== null && !text(body.project_id))) throw new Error("无法读取未确认请求");
    return;
  }
  if (intent.project_id === null || intent.target !== prepareTarget(intent.project_id)) throw new Error("无法读取未确认请求");
  const body = intent.body;
  if (Object.keys(body).length !== 1 || body.request_key !== intent.request_key) throw new Error("无法读取未确认请求");
}

export function readDevWrites(store: Storage): DevWriteIntent[] {
  const raw = store.getItem(DEV_WRITES);
  if (!raw) return [];
  const value: unknown = JSON.parse(raw);
  if (!Array.isArray(value)) throw new Error("无法读取未确认请求");
  if (value.length > MAX_WRITES) throw new Error("无法读取未确认请求");
  return value.map((item): DevWriteIntent => {
    const intent = object(item);
    exact(intent, ["principal_id", "generation", "workspace_id", "session_id", "project_id", "kind", "request_key", "target", "body", "rejected"]);
    for (const key of ["principal_id", "workspace_id", "session_id", "request_key", "target"]) if (!text(intent[key])) throw new Error("无法读取未确认请求");
    if (intent.project_id !== null && !text(intent.project_id)) throw new Error("无法读取未确认请求");
    if (!WRITE_KINDS.includes(String(intent.kind)) || !integer(intent.generation) || typeof intent.rejected !== "boolean") throw new Error("无法读取未确认请求");
    const body = object(intent.body);
    if (!text(body.request_key) || body.request_key !== intent.request_key) throw new Error("无法读取未确认请求");
    const result = intent as unknown as DevWriteIntent;
    validateWriteIntent(result);
    return result;
  });
}
export function saveDevWrites(store: Storage, values: DevWriteIntent[]): void {
  if (values.length > MAX_WRITES) throw new Error("未确认请求过多");
  const raw = JSON.stringify(values);
  store.setItem(DEV_WRITES, raw);
  if (store.getItem(DEV_WRITES) !== raw) throw new Error("无法保存请求");
}

// 同 kind + 同 scope 已有未确认记录时禁止用新 request_key 重复提交。
export function findPendingWrite(values: DevWriteIntent[], kind: DevWriteKind, scope: {
  principal_id: string; generation: number; workspace_id: string; session_id: string; project_id?: string | null;
}): DevWriteIntent | null {
  return values.find(item => item.kind === kind && !item.rejected
    && item.principal_id === scope.principal_id && item.generation === scope.generation
    && item.workspace_id === scope.workspace_id && item.session_id === scope.session_id
    && (scope.project_id === undefined || item.project_id === scope.project_id)) ?? null;
}

export const environmentLabels: Record<EnvironmentState, string> = {
  creating: "准备中", starting: "启动中", ready: "已就绪", stopping: "停止中",
  stopped: "已停止", failed: "准备失败", unknown: "状态未知",
};
export const projectStatusLabels: Record<string, string> = {
  unprovisioned: "未准备", ready: "就绪", running: "运行中", blocked: "受限", restoring: "恢复中",
};
export const environmentRowLabel = (state: EnvironmentState | null) =>
  state === null ? "未准备" : environmentLabels[state];

export function fmtBytes(size: number): string {
  if (size < 1024) return `${size} B`;
  if (size < 1024 * 1024) return `${Math.round(size / 102.4) / 10} KB`;
  return `${Math.round(size / (102.4 * 1024)) / 10} MB`;
}
export function fmtCount(count: number): string {
  return count >= 1000 ? `${Math.round(count / 100) / 10}k` : String(count);
}
export function shortHost(url: string): string {
  try { return new URL(url).host; } catch { return url; }
}

const devReasons: Record<string, string> = {
  workspace_busy: "对话或项目正在运行，结束后再试",
  sandbox_management_unavailable: "开发管理服务未启用",
  sandbox_backend_unavailable: "沙箱服务未连接",
  schema_upgrade_required: "账号服务需要升级，暂不可用",
  preview_unavailable: "预览服务未启用",
  storage_unavailable: "暂时无法保存",
  revision_conflict: "状态已更新，本次未执行；请刷新后重试",
  sandbox_operation_unknown: "上一次同键操作尚未生效，请稍后查找结果",
  not_found: "未找到记录，请检查项目与身份",
  invalid_input: "输入无效，请检查后重试",
  unauthorized: "密钥无效，请重新连接",
  permission_denied: "当前身份无权执行此操作",
};
export function devReason(error: unknown): string {
  const code = error instanceof Error && error.message in devReasons ? error.message
    : typeof error === "object" && error !== null && "code" in error && typeof (error as { code: unknown }).code === "string" ? (error as { code: string }).code : null;
  if (code && devReasons[code]) return devReasons[code]!;
  // 身份/完整性类错误原样呈现，不吞成通用失败。
  if (error instanceof Error && ["绑定身份不匹配", "项目身份不匹配", "环境身份不匹配", "文件身份不匹配", "文件响应不连续", "文件在读取期间发生变化", "文件内容校验失败，请重新读取", "未确认请求过多", "无法保存请求", "无法读取未确认请求"].includes(error.message)) return error.message;
  return "操作未完成，请稍后重试";
}
