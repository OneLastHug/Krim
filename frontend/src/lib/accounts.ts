import { object, RequestError } from "../api.ts";

// 账号管理前后端契约：解析器逐字段校验后端 accounts/models.py 的 JSON 输出。
// 模块不持有任何状态：不持久化、不全局缓存；一次性 token 只存在于返回值里。

export const ACCOUNT_LIMITS = {
  display_name: 64,
  memberships: 64,
  page: 100,
  principal_id: 64,
  request_key: 128,
} as const;

export type AccountStatus = "active" | "disabled";
export type PlatformRole = "member" | "platform_admin";
export type MembershipRole = "viewer" | "member" | "admin";

export interface KeyView {
  key_id: string;
  masked: string;
  credential_version: number;
  status: "active" | "revoked";
  weak: boolean;
  issued_at: number;
  revoked_at: number | null;
  last_used_at: number | null;
}
export interface Membership {
  workspace_id: string;
  role: MembershipRole;
  approve: boolean;
}
export interface AccountView {
  principal_id: string;
  display_name: string;
  status: AccountStatus;
  platform_role: PlatformRole;
  generation: number;
  revision: number;
  legacy: boolean;
  created_at: number;
  updated_at: number;
  has_active_key: boolean;
  key: KeyView | null;
}
export interface WorkspaceRef {
  workspace_id: string;
  display_name: string;
  role: MembershipRole;
  approve: boolean;
  personal: boolean;
}
export interface AccountProfile {
  principal_id: string;
  display_name: string;
  status: AccountStatus;
  platform_role: PlatformRole;
  generation: number;
  revision: number;
  legacy: boolean;
  // 个人资料没有 has_active_key；是否持有有效密钥由 key?.status === "active" 推导。
  key: KeyView | null;
  workspaces: WorkspaceRef[];
}
export interface IssuedKey {
  principal_id: string;
  key: KeyView;
  // 明文密钥只在首次响应出现一次；重放为 null，前端不得存储。
  token: string | null;
  replayed: boolean;
  revoked_key_id: string | null;
}
export interface AccountCreated {
  account: AccountView;
  replayed: boolean;
}
export interface AccountUpdated {
  account: AccountView;
  replayed: boolean;
}
export interface KeyRevoked {
  principal_id: string;
  key: KeyView;
  generation: number;
  replayed: boolean;
}
export interface MembershipsSet {
  principal_id: string;
  memberships: Membership[];
  generation: number;
  replayed: boolean;
}
export interface AccountMemberships {
  principal_id: string;
  generation: number;
  memberships: WorkspaceRef[];
}
export interface AccountEvent {
  id: string;
  action: string;
  actor: string;
  target: string | null;
  request_key: string;
  result: Record<string, unknown>;
  at: number;
}
export interface AccountsPage {
  items: AccountView[];
  offset: number;
  total: number;
}
export interface EventsPage {
  items: AccountEvent[];
  offset: number;
  total: number;
}
export interface ConnectionProfile {
  profile_id: string;
  // 只包含对外名称，不含网关地址等URL。
  display_name: string;
  mode: "api_gateway" | "local";
  target_key: string;
  target_fingerprint: string;
}

const PRINCIPAL = /^[a-zA-Z0-9_.-]{1,64}$/;
const REQUEST_KEY = /^[A-Za-z0-9_.:-]{1,128}$/;
const FINGERPRINT = /^[a-f0-9]{64}$/;
const KEY_ID = /^key_[0-9a-f]{16}$/;
// krim1.key_<16hex>.<32字节的urlsafe base64无padding，共43字符>
const TOKEN = /^krim1\.key_[0-9a-f]{16}\.[A-Za-z0-9_-]{43}$/;

const integer = (v: unknown): v is number => typeof v === "number" && Number.isSafeInteger(v) && v >= 0;
const count = (v: unknown): v is number => typeof v === "number" && Number.isSafeInteger(v) && v >= 1;
const finite = (v: unknown): v is number => typeof v === "number" && Number.isFinite(v);
const flag = (v: unknown): v is boolean => typeof v === "boolean";
const bounded = (v: unknown, max: number): v is string => typeof v === "string" && v.length >= 1 && v.length <= max;
const named = (v: unknown): v is string => bounded(v, ACCOUNT_LIMITS.principal_id) && PRINCIPAL.test(v);

function exact(v: Record<string, unknown>, fields: string[]) {
  if (Object.keys(v).some(k => !fields.includes(k)) || fields.some(k => !(k in v))) throw new Error("账号响应结构无效");
}
function oneOf(v: unknown, values: readonly string[]): boolean {
  return typeof v === "string" && values.includes(v);
}
function jsonValue(v: unknown, depth: number): boolean {
  if (depth > 16) return false;
  if (v === null || typeof v === "boolean" || typeof v === "string") return true;
  if (typeof v === "number") return Number.isFinite(v);
  if (Array.isArray(v)) return v.every(item => jsonValue(item, depth + 1));
  if (typeof v === "object") return Object.values(v).every(item => jsonValue(item, depth + 1));
  return false;
}

export function parseKeyView(raw: unknown): KeyView {
  const v = object(raw);
  exact(v, ["key_id", "masked", "credential_version", "status", "weak", "issued_at", "revoked_at", "last_used_at"]);
  if (!named(v.key_id) || !KEY_ID.test(v.key_id) || !bounded(v.masked, 128) || !count(v.credential_version) ||
      !oneOf(v.status, ["active", "revoked"]) || !flag(v.weak) || !finite(v.issued_at) ||
      (v.revoked_at !== null && !finite(v.revoked_at)) || (v.last_used_at !== null && !finite(v.last_used_at)))
    throw new Error("密钥响应无效");
  return v as unknown as KeyView;
}

function parseMembership(raw: unknown): Membership {
  const v = object(raw);
  exact(v, ["workspace_id", "role", "approve"]);
  if (!named(v.workspace_id) || !oneOf(v.role, ["viewer", "member", "admin"]) || !flag(v.approve)) throw new Error("成员资格响应无效");
  return v as unknown as Membership;
}

export function parseAccount(raw: unknown): AccountView {
  const v = object(raw);
  exact(v, ["principal_id", "display_name", "status", "platform_role", "generation", "revision", "legacy", "created_at", "updated_at", "has_active_key", "key"]);
  if (!named(v.principal_id) || !bounded(v.display_name, ACCOUNT_LIMITS.display_name) ||
      !oneOf(v.status, ["active", "disabled"]) || !oneOf(v.platform_role, ["member", "platform_admin"]) ||
      !count(v.generation) || !count(v.revision) || !flag(v.legacy) || !finite(v.created_at) || !finite(v.updated_at) || !flag(v.has_active_key))
    throw new Error("账号响应无效");
  const key = v.key === null ? null : parseKeyView(v.key);
  const active = key !== null && key.status === "active";
  if (v.has_active_key !== active) throw new Error("账号响应无效");
  return v as unknown as AccountView;
}

function parseWorkspaceRef(raw: unknown): WorkspaceRef {
  const v = object(raw);
  exact(v, ["workspace_id", "display_name", "role", "approve", "personal"]);
  if (!named(v.workspace_id) || !bounded(v.display_name, 128) ||
      !oneOf(v.role, ["viewer", "member", "admin"]) || !flag(v.approve) || !flag(v.personal)) throw new Error("工作区响应无效");
  return v as unknown as WorkspaceRef;
}

export function parseAccountProfile(raw: unknown): AccountProfile {
  const v = object(raw);
  exact(v, ["principal_id", "display_name", "status", "platform_role", "generation", "revision", "legacy", "key", "workspaces"]);
  if (!named(v.principal_id) || !bounded(v.display_name, ACCOUNT_LIMITS.display_name) ||
      !oneOf(v.status, ["active", "disabled"]) || !oneOf(v.platform_role, ["member", "platform_admin"]) ||
      !count(v.generation) || !count(v.revision) || !flag(v.legacy))
    throw new Error("身份响应无效");
  if (v.key !== null) parseKeyView(v.key);
  if (!Array.isArray(v.workspaces) || v.workspaces.length > ACCOUNT_LIMITS.memberships) throw new Error("身份响应无效");
  const workspaces = v.workspaces.map(parseWorkspaceRef);
  return { ...(v as unknown as AccountProfile), workspaces };
}

export function parseIssuedKey(raw: unknown): IssuedKey {
  const v = object(raw);
  exact(v, ["principal_id", "key", "token", "replayed", "revoked_key_id"]);
  if (!named(v.principal_id) || !flag(v.replayed) ||
      (v.revoked_key_id !== null && !(typeof v.revoked_key_id === "string" && KEY_ID.test(v.revoked_key_id))))
    throw new Error("密钥响应无效");
  parseKeyView(v.key);
  if (v.token !== null && !(typeof v.token === "string" && v.token.length <= 128 && TOKEN.test(v.token)))
    throw new Error("密钥响应无效");
  return v as unknown as IssuedKey;
}

export function parseAccountCreated(raw: unknown): AccountCreated {
  const v = object(raw);
  exact(v, ["account", "replayed"]);
  if (!flag(v.replayed)) throw new Error("账号响应无效");
  parseAccount(v.account);
  return v as unknown as AccountCreated;
}

export function parseAccountUpdated(raw: unknown): AccountUpdated {
  const v = object(raw);
  exact(v, ["account", "replayed"]);
  if (!flag(v.replayed)) throw new Error("账号响应无效");
  parseAccount(v.account);
  return v as unknown as AccountUpdated;
}

export function parseKeyRevoked(raw: unknown): KeyRevoked {
  const v = object(raw);
  exact(v, ["principal_id", "key", "generation", "replayed"]);
  if (!named(v.principal_id) || !count(v.generation) || !flag(v.replayed)) throw new Error("密钥响应无效");
  parseKeyView(v.key);
  return v as unknown as KeyRevoked;
}

export function parseMembershipsSet(raw: unknown): MembershipsSet {
  const v = object(raw);
  exact(v, ["principal_id", "memberships", "generation", "replayed"]);
  if (!named(v.principal_id) || !count(v.generation) || !flag(v.replayed) ||
      !Array.isArray(v.memberships) || v.memberships.length > ACCOUNT_LIMITS.memberships)
    throw new Error("成员资格响应无效");
  const memberships = v.memberships.map(parseMembership);
  return { ...(v as unknown as MembershipsSet), memberships };
}

export function parseAccountMemberships(raw: unknown): AccountMemberships {
  const v = object(raw);
  exact(v, ["principal_id", "generation", "memberships"]);
  if (!named(v.principal_id) || !count(v.generation) ||
      !Array.isArray(v.memberships) || v.memberships.length > ACCOUNT_LIMITS.memberships)
    throw new Error("成员资格响应无效");
  const memberships = v.memberships.map(parseWorkspaceRef);
  return { principal_id: v.principal_id as string, generation: v.generation as number, memberships };
}

export function parseAccountEvent(raw: unknown): AccountEvent {
  const v = object(raw);
  exact(v, ["id", "action", "actor", "target", "request_key", "result", "at"]);
  if (!bounded(v.id, 128) || !bounded(v.action, 64) || !named(v.actor) ||
      (v.target !== null && !named(v.target)) ||
      !(typeof v.request_key === "string" && REQUEST_KEY.test(v.request_key)) ||
      !finite(v.at) || typeof v.result !== "object" || v.result === null || Array.isArray(v.result) ||
      !jsonValue(v.result, 0))
    throw new Error("账号事件无效");
  return v as unknown as AccountEvent;
}

export function parseAccountsPage(raw: unknown): AccountsPage {
  const v = object(raw);
  exact(v, ["items", "offset", "total"]);
  if (!integer(v.offset) || !integer(v.total) || !Array.isArray(v.items) || v.items.length > ACCOUNT_LIMITS.page)
    throw new Error("账号列表无效");
  const items = v.items.map(parseAccount);
  return { items, offset: v.offset as number, total: v.total as number };
}

export function parseEventsPage(raw: unknown): EventsPage {
  const v = object(raw);
  exact(v, ["items", "offset", "total"]);
  if (!integer(v.offset) || !integer(v.total) || !Array.isArray(v.items) || v.items.length > ACCOUNT_LIMITS.page)
    throw new Error("账号事件列表无效");
  const items = v.items.map(parseAccountEvent);
  return { items, offset: v.offset as number, total: v.total as number };
}

export function parseConnectionProfile(raw: unknown): ConnectionProfile {
  const v = object(raw);
  exact(v, ["profile_id", "display_name", "mode", "target_key", "target_fingerprint"]);
  if (!bounded(v.display_name, 128) || !oneOf(v.mode, ["api_gateway", "local"]) ||
      !bounded(v.target_key, 128) || !(typeof v.target_fingerprint === "string" && FINGERPRINT.test(v.target_fingerprint)) ||
      !(typeof v.profile_id === "string" && v.profile_id === "connection-" + v.target_key))
    throw new Error("连接配置无效");
  return v as unknown as ConnectionProfile;
}

// ---- 请求层：独立于api.ts的request，支持PATCH/PUT并区分“明确拒绝”与“结果未知”。 ----

export type AccountMethod = "GET" | "POST" | "PATCH" | "PUT";
export interface AccountRequestInit {
  method?: AccountMethod;
  body?: unknown;
  signal?: AbortSignal;
}

// 网络失败、重定向、响应体不完整或无法解析时抛出：mutation 是否已提交无法确认。
export class AccountUnknownError extends Error {
  readonly status: number | null;
  constructor(message: string, status: number | null = null, options?: { cause?: unknown }) {
    super(message, options);
    this.status = status;
  }
}

export function isAccountOutcomeUnknown(error: unknown): boolean {
  if (error instanceof RequestError) return error.status >= 500;
  if (error instanceof AccountUnknownError) return true;
  return false;
}

export async function accountRequest(path: string, apiKey: string, init: AccountRequestInit = {}): Promise<unknown> {
  const headers: Record<string, string> = { Authorization: "Bearer " + apiKey };
  if (init.body !== undefined) headers["Content-Type"] = "application/json";
  let response: Response;
  try {
    response = await fetch(path, {
      method: init.method ?? "GET",
      headers,
      body: init.body === undefined ? undefined : JSON.stringify(init.body),
      redirect: "error",
      cache: "no-store",
      signal: init.signal,
    });
  } catch (cause) {
    throw new AccountUnknownError("账号请求未送达，结果未知", null, { cause });
  }
  let raw: unknown;
  try {
    raw = await response.json();
  } catch (cause) {
    if (response.ok) throw new AccountUnknownError("账号响应不完整，结果未知", response.status, { cause });
    // 非2xx即明确拒绝，即使错误体无法读取。
    throw new RequestError(response.status, "invalid_response");
  }
  if (!response.ok) {
    let code = "execution_error";
    try {
      const err = object((raw as Record<string, unknown>).error);
      code = /^[a-z_]{1,64}$/.test(String(err.code ?? "")) ? String(err.code) : "invalid_response";
    } catch {
      // 错误体无法读取：状态码仍表明是明确拒绝。
      code = "invalid_response";
    }
    throw new RequestError(response.status, code);
  }
  return raw;
}

export function newRequestKey(): string {
  const key = crypto.randomUUID();
  if (!REQUEST_KEY.test(key)) throw new Error("请求标识无效");
  return key;
}

// ---- 中文错误文案：先判结果未知，再按错误码给出具体原因。 ----

const accountReasons: Record<string, string> = {
  unauthorized: "密钥无效，请重新连接",
  permission_denied: "当前身份没有此操作权限",
  invalid_input: "输入不符合要求，请检查后重试",
  invalid_response: "服务返回无法识别",
  revision_conflict: "账号已被其他操作更新，请刷新后重试",
  key_conflict: "请求内容与原提交不一致，请刷新核对",
  not_found: "未找到该账号或记录",
  account_management_unavailable: "账号管理未启用",
  accounts_not_initialized: "账号数据尚未初始化",
  identity_mode_conflict: "身份模式冲突，无法管理账号",
  storage_unavailable: "暂时无法保存，请稍后重试",
  facts_corrupt: "账号记录异常，请刷新或联系管理员",
  execution_error: "账号操作未完成",
};

const clientReasons = ["请求标识无效", "账号身份无效", "显示名称无效", "成员资格无效", "分页参数无效", "修订号无效", "授权代次无效", "平台角色无效", "账号状态无效", "查询条件无效"];

export function accountError(error: unknown): string {
  if (isAccountOutcomeUnknown(error)) return "操作结果未确认，请刷新核对账号状态";
  if (error instanceof RequestError) return accountReasons[error.code] ?? "账号操作未完成";
  return error instanceof Error && clientReasons.includes(error.message) ? error.message : "账号操作未完成";
}

// ---- 类型化方法：每个方法第一个参数都是apiKey；写入都要求调用方提供request_key。 ----

export interface AccountListQuery {
  offset?: number;
  limit?: number;
  status?: AccountStatus;
  query?: string;
}
export interface AccountEventsQuery {
  offset?: number;
  limit?: number;
  principal_id?: string;
  request_key?: string;
}
export interface AccountCreateInput {
  display_name: string;
  platform_role?: PlatformRole;
  memberships?: Membership[];
}
export interface ProfileUpdateInput {
  display_name: string;
  expected_revision: number;
}
export interface AccountUpdateInput {
  display_name?: string;
  status?: AccountStatus;
  platform_role?: PlatformRole;
  expected_revision: number;
}
export interface MembershipsInput {
  memberships: Membership[];
  expected_generation: number;
}

const PROFILE_PATH = "/v2/me/profile";
const EVENTS_PATH = "/v2/admin/account-events";
const accountPath = (principalId: string) => "/v2/admin/accounts/" + encodeURIComponent(principalId);

function requireKey(requestKey: string) {
  if (!(typeof requestKey === "string" && REQUEST_KEY.test(requestKey))) throw new Error("请求标识无效");
}
function requirePrincipal(principalId: string) {
  if (!named(principalId)) throw new Error("账号身份无效");
}
function requirePage(offset: number | undefined, limit: number | undefined) {
  if (offset !== undefined && !integer(offset)) throw new Error("分页参数无效");
  if (limit !== undefined && !(count(limit) && limit <= ACCOUNT_LIMITS.page)) throw new Error("分页参数无效");
}
function query(init: Record<string, string | number | undefined>): string {
  const params = new URLSearchParams();
  for (const [name, value] of Object.entries(init)) {
    if (value !== undefined) params.set(name, String(value));
  }
  const text = params.toString();
  return text ? "?" + text : "";
}
function normalizeMembers(members: Membership[]): Membership[] {
  if (!Array.isArray(members) || members.length > ACCOUNT_LIMITS.memberships ||
      members.some(m => !named(m.workspace_id) || !oneOf(m.role, ["viewer", "member", "admin"]) || !flag(m.approve)))
    throw new Error("成员资格无效");
  return members.map(m => ({ workspace_id: m.workspace_id, role: m.role, approve: m.approve }));
}
function displayName(value: string): string {
  if (typeof value !== "string" || !value.trim() || value.trim().length > ACCOUNT_LIMITS.display_name) throw new Error("显示名称无效");
  return value;
}
// 2xx之后解析失败说明请求已受理但结果读不出来：按“结果未知”处理。
function decided<T>(parse: (raw: unknown) => T, raw: unknown): T {
  try {
    return parse(raw);
  } catch {
    throw new AccountUnknownError("账号响应无法解析，结果未知");
  }
}

export const accountsApi = {
  profile(apiKey: string, signal?: AbortSignal): Promise<AccountProfile> {
    return accountRequest(PROFILE_PATH, apiKey, { signal }).then(parseAccountProfile);
  },
  connection(apiKey: string, targetKey: string, signal?: AbortSignal): Promise<ConnectionProfile> {
    return accountRequest("/v2/targets/" + encodeURIComponent(targetKey) + "/connection-profile", apiKey, { signal }).then(parseConnectionProfile);
  },
  list(apiKey: string, init: AccountListQuery = {}, signal?: AbortSignal): Promise<AccountsPage> {
    requirePage(init.offset, init.limit);
    // 空串查询视同未提供，不发送query参数。
    const search = init.query === undefined || init.query === "" ? undefined : init.query;
    if (search !== undefined && !bounded(search, 64)) throw new Error("查询条件无效");
    return accountRequest("/v2/admin/accounts" + query({
      offset: init.offset, limit: init.limit, status: init.status, query: search,
    }), apiKey, { signal }).then(parseAccountsPage);
  },
  account(apiKey: string, principalId: string, signal?: AbortSignal): Promise<AccountView> {
    requirePrincipal(principalId);
    return accountRequest(accountPath(principalId), apiKey, { signal }).then(parseAccount);
  },
  getMemberships(apiKey: string, principalId: string, signal?: AbortSignal): Promise<AccountMemberships> {
    requirePrincipal(principalId);
    return accountRequest(accountPath(principalId) + "/memberships", apiKey, { signal }).then(parseAccountMemberships);
  },
  updateProfile(apiKey: string, input: ProfileUpdateInput, requestKey: string, signal?: AbortSignal): Promise<AccountUpdated> {
    requireKey(requestKey);
    if (!count(input.expected_revision)) throw new Error("修订号无效");
    const body = { display_name: displayName(input.display_name), expected_revision: input.expected_revision, request_key: requestKey };
    return accountRequest(PROFILE_PATH, apiKey, { method: "PATCH", body, signal }).then(raw => decided(parseAccountUpdated, raw));
  },
  create(apiKey: string, input: AccountCreateInput, requestKey: string, signal?: AbortSignal): Promise<AccountCreated> {
    requireKey(requestKey);
    const body: Record<string, unknown> = { display_name: displayName(input.display_name), request_key: requestKey };
    if (input.platform_role !== undefined) {
      if (!oneOf(input.platform_role, ["member", "platform_admin"])) throw new Error("平台角色无效");
      body.platform_role = input.platform_role;
    }
    if (input.memberships !== undefined) body.memberships = normalizeMembers(input.memberships);
    return accountRequest("/v2/admin/accounts", apiKey, { method: "POST", body, signal }).then(raw => decided(parseAccountCreated, raw));
  },
  update(apiKey: string, principalId: string, input: AccountUpdateInput, requestKey: string, signal?: AbortSignal): Promise<AccountUpdated> {
    requirePrincipal(principalId);
    requireKey(requestKey);
    if (!count(input.expected_revision)) throw new Error("修订号无效");
    const body: Record<string, unknown> = { expected_revision: input.expected_revision, request_key: requestKey };
    if (input.display_name !== undefined) body.display_name = displayName(input.display_name);
    if (input.status !== undefined) {
      if (!oneOf(input.status, ["active", "disabled"])) throw new Error("账号状态无效");
      body.status = input.status;
    }
    if (input.platform_role !== undefined) {
      if (!oneOf(input.platform_role, ["member", "platform_admin"])) throw new Error("平台角色无效");
      body.platform_role = input.platform_role;
    }
    return accountRequest(accountPath(principalId), apiKey, { method: "PATCH", body, signal }).then(raw => decided(parseAccountUpdated, raw));
  },
  rotate(apiKey: string, requestKey: string, signal?: AbortSignal): Promise<IssuedKey> {
    requireKey(requestKey);
    return accountRequest("/v2/me/key/rotate", apiKey, { method: "POST", body: { request_key: requestKey }, signal }).then(raw => decided(parseIssuedKey, raw));
  },
  issue(apiKey: string, principalId: string, requestKey: string, signal?: AbortSignal): Promise<IssuedKey> {
    requirePrincipal(principalId);
    requireKey(requestKey);
    return accountRequest(accountPath(principalId) + "/key/issue", apiKey, { method: "POST", body: { request_key: requestKey }, signal }).then(raw => decided(parseIssuedKey, raw));
  },
  revoke(apiKey: string, principalId: string, requestKey: string, signal?: AbortSignal): Promise<KeyRevoked> {
    requirePrincipal(principalId);
    requireKey(requestKey);
    return accountRequest(accountPath(principalId) + "/key/revoke", apiKey, { method: "POST", body: { request_key: requestKey }, signal }).then(raw => decided(parseKeyRevoked, raw));
  },
  revokeSelf(apiKey: string, requestKey: string, signal?: AbortSignal): Promise<KeyRevoked> {
    requireKey(requestKey);
    return accountRequest("/v2/me/key/revoke", apiKey, { method: "POST", body: { request_key: requestKey }, signal }).then(raw => decided(parseKeyRevoked, raw));
  },
  memberships(apiKey: string, principalId: string, input: MembershipsInput, requestKey: string, signal?: AbortSignal): Promise<MembershipsSet> {
    requirePrincipal(principalId);
    requireKey(requestKey);
    if (!count(input.expected_generation)) throw new Error("授权代次无效");
    const body = { memberships: normalizeMembers(input.memberships), expected_generation: input.expected_generation, request_key: requestKey };
    return accountRequest(accountPath(principalId) + "/memberships", apiKey, { method: "PUT", body, signal }).then(raw => decided(parseMembershipsSet, raw));
  },
  events(apiKey: string, init: AccountEventsQuery = {}, signal?: AbortSignal): Promise<EventsPage> {
    requirePage(init.offset, init.limit);
    if (init.principal_id !== undefined) requirePrincipal(init.principal_id);
    if (init.request_key !== undefined && !REQUEST_KEY.test(init.request_key)) throw new Error("请求标识无效");
    return accountRequest(EVENTS_PATH + query({
      offset: init.offset, limit: init.limit, principal_id: init.principal_id, request_key: init.request_key,
    }), apiKey, { signal }).then(parseEventsPage);
  },
};
