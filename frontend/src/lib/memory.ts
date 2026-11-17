import { RequestError, object } from "../api.ts";

export const MEMORY_LIMITS = { page: 200, default_page: 50, request_key: 128, source_ref: 256 } as const;
export type MemorySourceKind = "user" | "web" | "mcp" | "task";
export type MemoryKind = "note" | "preference";

export interface Memory {
  id: string;
  body: string;
  revision: number;
  content_hash: string;
  source_ref: string | null;
  workspace_id: string | null;
  project_id: string | null;
  source_kind: MemorySourceKind;
  memory_kind: MemoryKind;
  authorization_turn_id: string | null;
  created_at: number;
  updated_at: number;
  principal_id: string;
}
export interface MemoryPage { items: Memory[]; next_offset: number | null }
export interface MemoryScope { workspace_id: string; project_id: string }
export interface MemoryPreference { enabled: boolean; revision: number }
export interface MemoryReceipt { id: string; revision: number; content_hash: string }
export interface MemoryDeleteReceipt { id: string; removed: true; retention_notice?: string }
export interface MemoryCreateInput {
  request_key: string;
  body: string;
  source_ref?: string;
  source_kind?: MemorySourceKind;
  memory_kind?: MemoryKind;
}
export interface MemoryUpdateInput extends MemoryCreateInput {
  expected_revision: number;
  expected_content_hash: string;
}
export interface MemoryDeleteInput {
  request_key: string;
  expected_revision: number;
  expected_content_hash: string;
}
export interface MemoryPageQuery { offset?: number; limit?: number; query?: string; signal?: AbortSignal }

const REQUEST_KEY = /^[!-~]{1,128}$/;
const HASH = /^[0-9a-f]{64}$/;
const SOURCE_KINDS: readonly string[] = ["user", "web", "mcp", "task"];
const MEMORY_KINDS: readonly string[] = ["note", "preference"];

function exact(value: Record<string, unknown>, fields: string[], name: string): void {
  if (Object.keys(value).length !== fields.length || fields.some(field => !(field in value))) {
    throw new Error(`${name}结构无效`);
  }
}
function nonEmpty(value: unknown, max: number): value is string {
  return typeof value === "string" && value.length > 0 && value.length <= max;
}
function natural(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
}
function positive(value: unknown): value is number {
  return natural(value) && value >= 1;
}
function finite(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value) && value >= 0;
}
function optionalString(value: unknown, max: number, name: string): string | null {
  if (value === null) return null;
  if (typeof value !== "string" || value.length > max) throw new Error(`${name}无效`);
  return value;
}
function checkRequestKey(value: string): void {
  if (typeof value !== "string" || !REQUEST_KEY.test(value)) throw new Error("请求标识无效");
}
function checkScope(scope?: MemoryScope): void {
  if (scope && (!nonEmpty(scope.workspace_id, 128) || !nonEmpty(scope.project_id, 128))) {
    throw new Error("项目记忆范围无效");
  }
}

export function parseMemory(raw: unknown): Memory {
  const value = object(raw);
  exact(value, ["id", "body", "revision", "content_hash", "source_ref", "workspace_id", "project_id",
    "source_kind", "memory_kind", "authorization_turn_id", "created_at", "updated_at", "principal_id"], "记忆");
  if (!nonEmpty(value.id, 128) || typeof value.body !== "string" || !value.body.trim() ||
      !positive(value.revision) || typeof value.content_hash !== "string" || !HASH.test(value.content_hash) ||
      !SOURCE_KINDS.includes(String(value.source_kind)) || !MEMORY_KINDS.includes(String(value.memory_kind)) ||
      !finite(value.created_at) || !finite(value.updated_at) || !nonEmpty(value.principal_id, 64)) {
    throw new Error("记忆响应无效");
  }
  if (value.memory_kind === "preference" && value.source_kind !== "user") throw new Error("用户偏好来源无效");
  return {
    ...(value as unknown as Memory),
    source_ref: optionalString(value.source_ref, MEMORY_LIMITS.source_ref, "记忆来源"),
    workspace_id: optionalString(value.workspace_id, 128, "工作区标识"),
    project_id: optionalString(value.project_id, 128, "项目标识"),
    authorization_turn_id: optionalString(value.authorization_turn_id, 128, "授权回合"),
  };
}

export function parseMemoryList(raw: unknown): Memory[] {
  if (!Array.isArray(raw)) throw new Error("记忆列表无效");
  return raw.map(parseMemory);
}

export function parseMemoryPage(raw: unknown): MemoryPage {
  const value = object(raw);
  exact(value, ["items", "next_offset"], "记忆分页");
  if (!Array.isArray(value.items) || value.items.length > MEMORY_LIMITS.page ||
      (value.next_offset !== null && !natural(value.next_offset))) throw new Error("记忆分页无效");
  const items = value.items.map(parseMemory);
  if (value.next_offset !== null && value.next_offset < items.length) throw new Error("记忆分页游标无效");
  return { items, next_offset: value.next_offset as number | null };
}

export function parseMemoryPreference(raw: unknown): MemoryPreference {
  const value = object(raw);
  exact(value, ["enabled", "revision"], "记忆偏好");
  if (typeof value.enabled !== "boolean" || !natural(value.revision)) throw new Error("记忆偏好响应无效");
  return value as unknown as MemoryPreference;
}

export function parseMemoryReceipt(raw: unknown): MemoryReceipt {
  const value = object(raw);
  exact(value, ["id", "revision", "content_hash"], "记忆回执");
  if (!nonEmpty(value.id, 128) || !positive(value.revision) || typeof value.content_hash !== "string" || !HASH.test(value.content_hash)) {
    throw new Error("记忆回执无效");
  }
  return value as unknown as MemoryReceipt;
}

export function parseMemoryDeleteReceipt(raw: unknown): MemoryDeleteReceipt | MemoryReceipt {
  const value = object(raw);
  if ("removed" in value) {
    const fields = value.retention_notice === undefined ? ["id", "removed"] : ["id", "removed", "retention_notice"];
    if (Object.keys(value).length !== fields.length || fields.some(field => !(field in value)) ||
        !nonEmpty(value.id, 128) || value.removed !== true ||
        (value.retention_notice !== undefined && typeof value.retention_notice !== "string")) {
      throw new Error("记忆删除回执无效");
    }
    return value as unknown as MemoryDeleteReceipt;
  }
  return parseMemoryReceipt(raw);
}

export function newMemoryRequestKey(): string {
  const key = crypto.randomUUID();
  checkRequestKey(key);
  return key;
}

export class MemoryOutcomeUnknownError extends Error {
  readonly status: number | null;
  constructor(message: string, status: number | null = null, options?: { cause?: unknown }) {
    super(message, options);
    this.name = "MemoryOutcomeUnknownError";
    this.status = status;
  }
}

async function memoryRequest(path: string, apiKey: string, method: "GET" | "POST" | "PUT" | "DELETE",
  body?: unknown, signal?: AbortSignal): Promise<unknown> {
  const writing = method !== "GET";
  const headers: Record<string, string> = {};
  if (apiKey) headers.Authorization = "Bearer " + apiKey;
  if (body !== undefined) headers["Content-Type"] = "application/json";
  let response: Response;
  try {
    response = await fetch(path, { method, headers, body: body === undefined ? undefined : JSON.stringify(body),
      redirect: "error", cache: "no-store", signal });
  } catch (cause) {
    if (writing) throw new MemoryOutcomeUnknownError("记忆操作结果未确认", null, { cause });
    throw cause;
  }
  let raw: unknown;
  try {
    raw = await response.json();
  } catch (cause) {
    if (response.ok && writing) throw new MemoryOutcomeUnknownError("记忆响应不完整，操作结果未确认", response.status, { cause });
    if (!response.ok) throw new RequestError(response.status, "invalid_response");
    throw new Error("记忆响应无效");
  }
  if (!response.ok) {
    let code = "invalid_response";
    try {
      const error = object(object(raw).error);
      if (typeof error.code === "string" && /^[a-z_]{1,64}$/.test(error.code)) code = error.code;
    } catch { /* 状态码已经说明请求被拒绝。 */ }
    if (writing && response.status >= 500) throw new MemoryOutcomeUnknownError("记忆操作结果未确认", response.status);
    throw new RequestError(response.status, code);
  }
  return raw;
}

function decided<T>(parse: (raw: unknown) => T, raw: unknown): T {
  try { return parse(raw); }
  catch (cause) { throw new MemoryOutcomeUnknownError("记忆操作已受理，但回执无法确认", null, { cause }); }
}

const projectPath = (scope: MemoryScope) => "/v2/workspaces/" + encodeURIComponent(scope.workspace_id) +
  "/projects/" + encodeURIComponent(scope.project_id) + "/memories";
const memoryPath = (id: string, scope?: MemoryScope) => {
  checkScope(scope);
  if (!nonEmpty(id, 128) || /[\u0000-\u001f\u007f]/.test(id)) throw new Error("记忆标识无效");
  return (scope ? projectPath(scope) : "/v2/me/memories") + "/" + encodeURIComponent(id);
};
function queryString(init: MemoryPageQuery): string {
  const offset = init.offset ?? 0, limit = init.limit ?? MEMORY_LIMITS.default_page;
  if (!natural(offset) || !positive(limit) || limit > MEMORY_LIMITS.page) throw new Error("记忆分页参数无效");
  if (init.query !== undefined && typeof init.query !== "string") throw new Error("记忆搜索条件无效");
  const params = new URLSearchParams({ offset: String(offset), limit: String(limit) });
  if (init.query?.trim()) params.set("query", init.query.trim());
  return "?" + params.toString();
}
function createBody(input: MemoryCreateInput): Record<string, unknown> {
  checkRequestKey(input.request_key);
  if (typeof input.body !== "string" || !input.body.trim() || input.body.includes("\0")) throw new Error("记忆内容无效");
  if (input.source_ref !== undefined && (typeof input.source_ref !== "string" || input.source_ref.length > MEMORY_LIMITS.source_ref))
    throw new Error("记忆来源无效");
  const source_kind = input.source_kind ?? "user", memory_kind = input.memory_kind ?? "note";
  if (!SOURCE_KINDS.includes(source_kind) || !MEMORY_KINDS.includes(memory_kind) ||
      (memory_kind === "preference" && source_kind !== "user")) throw new Error("记忆类型无效");
  return { request_key: input.request_key, body: input.body, ...(input.source_ref === undefined ? {} : { source_ref: input.source_ref }),
    source_kind, memory_kind };
}
function checkRevision(revision: number, hash: string): void {
  if (!positive(revision)) throw new Error("记忆修订号无效");
  if (typeof hash !== "string" || !HASH.test(hash)) throw new Error("记忆内容哈希无效");
}

export const memoriesApi = {
  /** 兼容旧账户列表接口：GET /v2/me/memories 返回数组。 */
  list(apiKey: string, signal?: AbortSignal): Promise<Memory[]> {
    return memoryRequest("/v2/me/memories", apiKey, "GET", undefined, signal).then(parseMemoryList);
  },
  page(apiKey: string, init: MemoryPageQuery = {}, scope?: MemoryScope): Promise<MemoryPage> {
    checkScope(scope);
    const base = scope ? projectPath(scope) : "/v2/me/memories/page";
    return memoryRequest(base + queryString(init), apiKey, "GET", undefined, init.signal).then(parseMemoryPage);
  },
  get(apiKey: string, id: string, scope?: MemoryScope, signal?: AbortSignal): Promise<Memory> {
    return memoryRequest(memoryPath(id, scope), apiKey, "GET", undefined, signal).then(parseMemory);
  },
  create(apiKey: string, input: MemoryCreateInput, scope?: MemoryScope, signal?: AbortSignal): Promise<MemoryReceipt> {
    checkScope(scope);
    const path = scope ? projectPath(scope) : "/v2/me/memories";
    return memoryRequest(path, apiKey, "POST", createBody(input), signal).then(raw => decided(parseMemoryReceipt, raw));
  },
  update(apiKey: string, id: string, input: MemoryUpdateInput, scope?: MemoryScope, signal?: AbortSignal): Promise<MemoryReceipt> {
    checkScope(scope);
    checkRequestKey(input.request_key);
    checkRevision(input.expected_revision, input.expected_content_hash);
    const body = { ...createBody(input), expected_revision: input.expected_revision, expected_content_hash: input.expected_content_hash };
    return memoryRequest(memoryPath(id, scope), apiKey, "PUT", body, signal).then(raw => decided(parseMemoryReceipt, raw));
  },
  remove(apiKey: string, id: string, input: MemoryDeleteInput, scope?: MemoryScope, signal?: AbortSignal): Promise<MemoryDeleteReceipt | MemoryReceipt> {
    checkScope(scope);
    checkRequestKey(input.request_key);
    checkRevision(input.expected_revision, input.expected_content_hash);
    const body = { request_key: input.request_key, expected_revision: input.expected_revision,
      expected_content_hash: input.expected_content_hash };
    return memoryRequest(memoryPath(id, scope), apiKey, "DELETE", body, signal).then(raw => decided(parseMemoryDeleteReceipt, raw));
  },
  preference(apiKey: string, signal?: AbortSignal): Promise<MemoryPreference> {
    return memoryRequest("/v2/me/memory-preference", apiKey, "GET", undefined, signal).then(parseMemoryPreference);
  },
  setPreference(apiKey: string, enabled: boolean, expected_revision: number, request_key: string,
    signal?: AbortSignal): Promise<MemoryPreference> {
    if (typeof enabled !== "boolean" || !natural(expected_revision)) throw new Error("记忆偏好无效");
    checkRequestKey(request_key);
    const body = { enabled, expected_revision, request_key };
    return memoryRequest("/v2/me/memory-preference", apiKey, "PUT", body, signal).then(raw => decided(parseMemoryPreference, raw));
  },
};

const reasons: Record<string, string> = {
  unauthorized: "密钥无效，请重新连接",
  permission_denied: "当前身份没有此操作权限",
  invalid_input: "记忆内容或操作参数无效",
  not_found: "未找到这条记忆",
  capacity: "记忆数量已达上限",
  revision_conflict: "记忆已在其他页面更新，请重新读取后再试",
  key_conflict: "请求标识已用于其他内容，请重新读取后再试",
  storage_unavailable: "暂时无法保存记忆",
};
export function memoryError(error: unknown): string {
  if (error instanceof MemoryOutcomeUnknownError) return "操作结果尚未确认，请重新读取记忆核对后再操作";
  if (error instanceof RequestError) return reasons[error.code] ?? "记忆操作未完成";
  return error instanceof Error && error.message.endsWith("无效") ? error.message : "记忆操作未完成";
}
