import assert from "node:assert/strict";
import {
  ACCOUNT_LIMITS, AccountUnknownError, accountError, accountRequest, accountsApi,
  isAccountOutcomeUnknown, newRequestKey, parseAccount, parseAccountCreated,
  parseAccountEvent, parseAccountMemberships, parseAccountProfile, parseAccountsPage,
  parseConnectionProfile, parseEventsPage, parseIssuedKey, parseKeyRevoked,
  parseAccountUpdated, parseKeyView, parseMembershipsSet,
} from "../src/lib/accounts.ts";
import { RequestError } from "../src/api.ts";

assert.equal(ACCOUNT_LIMITS.display_name, 64);
let checks = 0;
function ok(condition: unknown, message: string) {
  assert.ok(condition, message);
  checks += 1;
}
async function errorOf(run: () => unknown): Promise<unknown> {
  try {
    const value = run();
    if (value instanceof Promise) await value;
  } catch (error) {
    return error;
  }
  throw new Error("预期抛出未发生");
}

const hex16 = "0123456789abcdef";
const token = "krim1.key_" + hex16 + ".AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
const keyView = (status: "active" | "revoked" = "active") => ({
  key_id: "key_" + hex16,
  masked: "krim1.key_" + hex16 + "…cdef",
  credential_version: 2,
  status,
  weak: false,
  issued_at: 1000.5,
  revoked_at: status === "revoked" ? 2000.25 : null,
  last_used_at: 1001.125,
});
const account = (over: Record<string, unknown> = {}) => ({
  principal_id: "admin.main", display_name: "管理员", status: "active",
  platform_role: "platform_admin", generation: 3, revision: 7, legacy: false,
  created_at: 1, updated_at: 2, has_active_key: true, key: keyView(), ...over,
});
const workspaceRef = (over: Record<string, unknown> = {}) => ({
  workspace_id: "ws_team", display_name: "团队工作区", role: "admin", approve: true, personal: false, ...over,
});
const profile = (over: Record<string, unknown> = {}) => ({
  principal_id: "admin.main", display_name: "管理员", status: "active",
  platform_role: "member", generation: 1, revision: 4, legacy: false,
  key: keyView(), workspaces: [workspaceRef()], ...over,
});
const connection = (over: Record<string, unknown> = {}) => ({
  profile_id: "connection-target-v1", display_name: "API 网关", mode: "api_gateway",
  target_key: "target-v1", target_fingerprint: "a".repeat(64), ...over,
});
const event = (over: Record<string, unknown> = {}) => ({
  id: "3f2a1b4c-5d6e-4f70-8a91-2b3c4d5e6f70", action: "rotate", actor: "admin.main",
  target: "member.one", request_key: "2b7a9c1d-0000-4000-8000-000000000001",
  result: { key_id: "key_" + hex16, revoked_key_id: null }, at: 99.5, ...over,
});

// ---- 解析器：接受后端实际输出，拒绝多余字段、错误枚举与非有限数 ----

const parsedProfile = parseAccountProfile(profile());
ok(parsedProfile.workspaces[0]!.workspace_id === "ws_team" && parsedProfile.generation === 1, "profile解析保留字段");
ok(!("has_active_key" in parsedProfile), "profile不含has_active_key");
ok(await errorOf(() => parseAccountProfile({ ...profile(), has_active_key: true })) instanceof Error, "profile拒绝has_active_key");
ok(await errorOf(() => parseAccountProfile({ ...profile(), token })) instanceof Error, "profile拒绝秘密字段token");
ok(await errorOf(() => parseAccountProfile({ ...profile(), extra: 1 })) instanceof Error, "profile拒绝多余字段");
ok(await errorOf(() => parseAccountProfile({ ...profile(), status: "locked" })) instanceof Error, "profile拒绝状态枚举外取值");
ok(await errorOf(() => parseAccountProfile({ ...profile(), platform_role: "owner" })) instanceof Error, "profile拒绝角色枚举外取值");
ok(await errorOf(() => parseAccountProfile({ ...profile(), revision: 0 })) instanceof Error, "profile拒绝非正代次");
ok(await errorOf(() => parseAccountProfile({ ...profile({ key: { ...keyView(), issued_at: Number.NaN } }) })) instanceof Error, "profile拒绝非有限时间");
ok(await errorOf(() => parseAccountProfile({ ...profile({ key: { ...keyView(), key_id: "other" } }) })) instanceof Error, "profile拒绝非key_格式密钥ID");
ok(await errorOf(() => parseAccountProfile({ ...profile({ workspaces: Array.from({ length: 65 }, () => workspaceRef()) }) })) instanceof Error, "profile拒绝超过64个工作区");
ok(await errorOf(() => parseAccountProfile({ ...profile({ workspaces: [workspaceRef({ role: "owner" })] }) })) instanceof Error, "profile拒绝工作区角色枚举外取值");
ok(await errorOf(() => parseAccountProfile({ ...profile({ workspaces: [workspaceRef({ personal: 1 })] }) })) instanceof Error, "profile拒绝非布尔personal");

const parsedAccount = parseAccount(account());
ok(parsedAccount.has_active_key === true && parsedAccount.key!.status === "active", "account解析保留密钥");
ok(parseAccount(account({ key: keyView("revoked"), has_active_key: false })).has_active_key === false, "account接受撤销密钥");
ok(parseAccount(account({ key: null, has_active_key: false })).key === null, "account接受空密钥");
ok(await errorOf(() => parseAccount(account({ has_active_key: false }))) instanceof Error, "account拒绝has_active_key与key.status矛盾");
ok(parseKeyView(keyView()).credential_version === 2, "keyView解析");
ok(await errorOf(() => parseKeyView({ ...keyView(), status: "pending" })) instanceof Error, "keyView拒绝状态枚举外取值");

const parsedIssued = parseIssuedKey({ principal_id: "member.one", key: keyView(), token, replayed: false, revoked_key_id: "key_fedcba9876543210" });
ok(parsedIssued.token === token && parsedIssued.revoked_key_id === "key_fedcba9876543210", "签发结果保留一次性token");
const parsedReplay = parseIssuedKey({ principal_id: "member.one", key: keyView(), token: null, replayed: true, revoked_key_id: null });
ok(parsedReplay.token === null && parsedReplay.replayed === true, "重放结果token为null");
ok(await errorOf(() => parseIssuedKey({ principal_id: "member.one", key: keyView(), token: "krim1.short", replayed: false, revoked_key_id: null })) instanceof Error, "签发拒绝格式不符token");
ok(parseAccountCreated({ account: account(), replayed: false }).account.display_name === "管理员", "创建结果解析");
ok(parseAccountUpdated({ account: account(), replayed: true }).replayed === true, "更新结果解析");
ok(parseKeyRevoked({ principal_id: "admin.main", key: keyView("revoked"), generation: 4, replayed: false }).generation === 4, "撤销结果解析");
ok(parseMembershipsSet({ principal_id: "member.one", memberships: [{ workspace_id: "ws_team", role: "member", approve: true }], generation: 2, replayed: false }).memberships.length === 1, "成员资格结果解析");
ok(parseAccountMemberships({ principal_id: "member.one", generation: 2, memberships: [workspaceRef()] }).memberships[0]!.personal === false, "成员资格读取解析");

const parsedPage = parseAccountsPage({ items: [account()], offset: 20, total: 21 });
ok(parsedPage.offset === 20 && parsedPage.items.length === 1, "账号列表解析");
ok(await errorOf(() => parseAccountsPage({ items: [], offset: -1, total: 0 })) instanceof Error, "账号列表拒绝负offset");
ok(await errorOf(() => parseAccountsPage({ items: Array.from({ length: 101 }, () => account()), offset: 0, total: 101 })) instanceof Error, "账号列表拒绝超过100条");
const parsedEvents = parseEventsPage({ items: [event()], offset: 0, total: 1 });
ok(parsedEvents.items[0]!.action === "rotate" && parsedEvents.items[0]!.result.key_id === "key_" + hex16, "事件列表解析");
ok(parseAccountEvent(event({ result: { nested: { list: [1, "x", null, true] } } })) instanceof Object || true, "事件接受嵌套JSON结果");
ok(await errorOf(() => parseAccountEvent(event({ result: { bad: Number.POSITIVE_INFINITY } }))) instanceof Error, "事件拒绝非有限数");
ok(await errorOf(() => parseAccountEvent(event({ target: "bad id!" }))) instanceof Error, "事件拒绝非 Principal 字符集target");
ok(await errorOf(() => parseEventsPage({ items: [event({ request_key: "bad key" })], offset: 0, total: 1 })) instanceof Error, "事件拒绝非法request_key");

const parsedConnection = parseConnectionProfile(connection());
ok(parsedConnection.mode === "api_gateway" && parsedConnection.display_name === "API 网关", "连接配置解析只含名称");
ok(await errorOf(() => parseConnectionProfile({ ...connection(), gateway_url: "https://gw.example" })) instanceof Error, "连接配置拒绝URL字段");
ok(await errorOf(() => parseConnectionProfile({ ...connection({ target_fingerprint: "a".repeat(63) }) })) instanceof Error, "连接配置拒绝非64位指纹");
ok(await errorOf(() => parseConnectionProfile({ ...connection({ mode: "proxy" }) })) instanceof Error, "连接配置拒绝模式枚举外取值");
ok(await errorOf(() => parseConnectionProfile({ ...connection({ profile_id: "connection-other" }) })) instanceof Error, "连接配置拒绝profile_id与target_key不符");
ok(parseConnectionProfile(connection({ mode: "local", display_name: "本地模式" })).mode === "local", "本地模式连接解析");

// ---- newRequestKey：字符集与唯一性 ----

const keys = new Set<string>();
for (let i = 0; i < 32; i += 1) {
  const key = newRequestKey();
  ok(/^[A-Za-z0-9_.:-]{1,128}$/.test(key), "request_key字符集合规");
  keys.add(key);
}
ok(keys.size === 32, "request_key不重复");

// ---- accountRequest：传输选项、明确拒绝与结果未知 ----

type Call = { input: string; init: RequestInit };
const calls: Call[] = [];
function stubFetch(respond: (input: string, init: RequestInit) => Response) {
  const original = globalThis.fetch;
  globalThis.fetch = ((input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
    const record = { input: String(input), init: init ?? {} };
    calls.push(record);
    return Promise.resolve(respond(record.input, record.init));
  }) as typeof fetch;
  return () => {
    globalThis.fetch = original;
  };
}
const restore = stubFetch(() => new Response(JSON.stringify({ ok: true }), { status: 200, headers: { "Content-Type": "application/json" } }));
const signal = new AbortController().signal;
const plain = await accountRequest("/v2/me/profile", "secret-key", { signal });
ok((plain as Record<string, unknown>).ok === true, "accountRequest返回已解析JSON");
ok(calls[0]!.init.method === "GET" && calls[0]!.init.redirect === "error" && calls[0]!.init.cache === "no-store", "GET默认方法且禁止跳转缓存");
ok(calls[0]!.init.body === undefined && !(calls[0]!.init.headers as Record<string, string>)["Content-Type"], "GET无请求体");
ok((calls[0]!.init.headers as Record<string, string>).Authorization === "Bearer secret-key", "携带Authorization头");
ok(calls[0]!.init.signal === signal, "GET信号透传");
await accountRequest("/v2/admin/accounts", "k", { method: "POST", body: { request_key: "rk.1" } });
ok(calls[1]!.init.method === "POST" && calls[1]!.init.body === JSON.stringify({ request_key: "rk.1" }), "POST发送JSON请求体");
ok((calls[1]!.init.headers as Record<string, string>)["Content-Type"] === "application/json", "POST携带Content-Type");
await accountRequest("/v2/admin/accounts/x", "k", { method: "PATCH", body: {} });
await accountRequest("/v2/admin/accounts/x/memberships", "k", { method: "PUT", body: {} });
ok(calls[2]!.init.method === "PATCH" && calls[3]!.init.method === "PUT", "PATCH与PUT方法透传");
restore();

const restore422 = stubFetch(() => new Response(JSON.stringify({ error: { code: "invalid_input", message: "x" } }), { status: 422 }));
const rejected = await errorOf(() => accountRequest("/v2/admin/accounts", "k", { method: "POST", body: {} }));
ok(rejected instanceof RequestError && (rejected as RequestError).status === 422 && (rejected as RequestError).code === "invalid_input", "422转为RequestError并保留错误码");
ok(isAccountOutcomeUnknown(rejected) === false, "422为明确拒绝");
restore422();
const restore500 = stubFetch(() => new Response(JSON.stringify({ error: { code: "storage_unavailable" } }), { status: 503 }));
const failed = await errorOf(() => accountRequest("/v2/admin/accounts", "k"));
ok(failed instanceof RequestError && (failed as RequestError).status === 503, "503转为RequestError");
ok(isAccountOutcomeUnknown(failed) === true, "5xx按结果未知处理");
restore500();
const restoreNet = stubFetch(() => {
  throw new TypeError("fetch failed");
});
const network = await errorOf(() => accountRequest("/v2/admin/accounts", "k"));
ok(network instanceof AccountUnknownError && isAccountOutcomeUnknown(network) === true, "网络失败按结果未知处理");
ok(isAccountOutcomeUnknown(new Error("普通错误")) === false, "非传输层错误不算结果未知");
restoreNet();
const restoreBadBody = stubFetch(() => new Response("{broken", { status: 200 }));
const badBody = await errorOf(() => accountRequest("/v2/admin/accounts", "k"));
ok(badBody instanceof AccountUnknownError && isAccountOutcomeUnknown(badBody) === true, "2xx响应体不完整按结果未知处理");
restoreBadBody();
const restoreBadError = stubFetch(() => new Response("{broken", { status: 422 }));
const badError = await errorOf(() => accountRequest("/v2/admin/accounts", "k", { method: "POST", body: {} }));
ok(badError instanceof RequestError && (badError as RequestError).code === "invalid_response" && isAccountOutcomeUnknown(badError) === false, "4xx错误体不可读仍是明确拒绝");
restoreBadError();
const restoreAbort = stubFetch((_input, init) => {
  if (init.signal?.aborted) throw new TypeError("The operation was aborted");
  return new Response("{}", { status: 200 });
});
const controller = new AbortController();
controller.abort();
const aborted = await errorOf(() => accountRequest("/v2/admin/accounts", "k", { signal: controller.signal }));
ok(aborted instanceof AccountUnknownError, "中止请求按结果未知处理");
restoreAbort();

// ---- accountsApi：路径、请求体与解析结果 ----

const restoreApi = stubFetch((input) => {
  if (input === "/v2/me/profile" && calls.at(-1)!.init.method === "PATCH") return new Response(JSON.stringify({ account: account(), replayed: false }), { status: 200 });
  if (input === "/v2/me/profile") return new Response(JSON.stringify(profile()), { status: 200 });
  if (input === "/v2/targets/target-v1/connection-profile") return new Response(JSON.stringify(connection()), { status: 200 });
  if (input.startsWith("/v2/admin/accounts?")) return new Response(JSON.stringify({ items: [account()], offset: 0, total: 1 }), { status: 200 });
  if (input === "/v2/admin/accounts/member.one" && calls.at(-1)!.init.method === "GET") return new Response(JSON.stringify(account({ principal_id: "member.one", platform_role: "member", has_active_key: false, key: null })), { status: 200 });
  if (input === "/v2/admin/accounts/member.one/memberships" && calls.at(-1)!.init.method === "GET") return new Response(JSON.stringify({ principal_id: "member.one", generation: 2, memberships: [workspaceRef()] }), { status: 200 });
  if (input === "/v2/admin/accounts/member.one/memberships" && calls.at(-1)!.init.method === "PUT") return new Response(JSON.stringify({ principal_id: "member.one", memberships: [{ workspace_id: "ws_team", role: "admin", approve: false }], generation: 3, replayed: false }), { status: 200 });
  if (input === "/v2/admin/accounts") return new Response(JSON.stringify({ account: account({ principal_id: "new.member", platform_role: "member", has_active_key: false, key: null }), replayed: false }), { status: 201 });
  if (input === "/v2/admin/accounts/member.one" && calls.at(-1)!.init.method === "PATCH") return new Response(JSON.stringify({ account: account({ principal_id: "member.one", platform_role: "member", has_active_key: false, key: null, revision: 8 }), replayed: false }), { status: 200 });
  if (input === "/v2/me/key/rotate") return new Response(JSON.stringify({ principal_id: "admin.main", key: keyView(), token, replayed: false, revoked_key_id: null }), { status: 200 });
  if (input === "/v2/admin/accounts/member.one/key/issue") return new Response(JSON.stringify({ principal_id: "member.one", key: keyView(), token, replayed: false, revoked_key_id: null }), { status: 200 });
  if (input === "/v2/admin/accounts/member.one/key/revoke") return new Response(JSON.stringify({ principal_id: "member.one", key: keyView("revoked"), generation: 4, replayed: false }), { status: 200 });
  if (input === "/v2/me/key/revoke") return new Response(JSON.stringify({ principal_id: "admin.main", key: keyView("revoked"), generation: 4, replayed: false }), { status: 200 });
  if (input === "/v2/admin/account-events?principal_id=member.one") return new Response(JSON.stringify({ items: [event()], offset: 0, total: 1 }), { status: 200 });
  return new Response(JSON.stringify({ error: { code: "not_found" } }), { status: 404 });
});

const me = await accountsApi.profile("k");
ok(me.display_name === "管理员", "profile方法解析成功");
const conn = await accountsApi.connection("k", "target-v1");
ok(conn.profile_id === "connection-target-v1", "connection方法解析成功");
const page = await accountsApi.list("k", { offset: 0, limit: 50, status: "disabled", query: "张" });
ok(page.total === 1, "list方法解析成功");
ok(calls.at(-1)!.input === "/v2/admin/accounts?offset=0&limit=50&status=disabled&query=" + encodeURIComponent("张"), "list查询参数正确");
const emptyQuery = await errorOf(() => accountsApi.list("k", { query: "" }));
ok(emptyQuery instanceof Error, "空查询不发送query参数");
ok(calls.at(-1)!.input === "/v2/admin/accounts", "空查询仅基础路径");
ok(await accountsApi.account("k", "member.one").then(a => a.principal_id === "member.one"), "account方法解析成功");
ok(await accountsApi.getMemberships("k", "member.one").then(m => m.generation === 2), "getMemberships方法解析成功");

const created = await accountsApi.create("k", { display_name: "新成员", platform_role: "member", memberships: [{ workspace_id: "ws_team", role: "viewer", approve: true }] }, "rk-create-1");
ok(created.account.principal_id === "new.member" && created.replayed === false, "create方法解析成功");
const createCall = [...calls].reverse().find(c => c.input === "/v2/admin/accounts" && c.init.method === "POST")!;
ok(JSON.parse(String(createCall.init.body)).request_key === "rk-create-1", "create请求携带request_key");
ok(JSON.parse(String(createCall.init.body)).memberships[0]!.role === "viewer", "create请求携带成员资格");
ok(!(JSON.parse(String(createCall.init.body)).display_name === undefined), "create请求携带显示名称");

const updated = await accountsApi.update("k", "member.one", { display_name: "改名", status: "disabled", platform_role: "member", expected_revision: 7 }, "rk-update-1");
ok(updated.account.revision === 8, "update方法解析成功");
const updateBody = JSON.parse(String([...calls].reverse().find(c => c.init.method === "PATCH" && c.input.startsWith("/v2/admin/accounts"))!.init.body));
ok(updateBody.expected_revision === 7 && updateBody.request_key === "rk-update-1", "update请求携带修订号与request_key");

const renamed = await accountsApi.updateProfile("k", { display_name: "改名", expected_revision: 8 }, "rk-self-rename-1");
ok(renamed.account.display_name === "管理员", "updateProfile方法解析成功");
const selfPatch = [...calls].reverse().find(c => c.input === "/v2/me/profile" && c.init.method === "PATCH")!;
const selfBody = JSON.parse(String(selfPatch.init.body));
ok(selfBody.display_name === "改名" && selfBody.expected_revision === 8 && selfBody.request_key === "rk-self-rename-1", "updateProfile请求路径与请求体正确");
ok(!("platform_role" in selfBody) && !("status" in selfBody), "updateProfile不携带管理员字段");

const rotated = await accountsApi.rotate("k", "rk-rotate-1");
ok(rotated.token === token, "rotate方法返回一次性token");
const issued = await accountsApi.issue("k", "member.one", "rk-issue-1");
ok(issued.principal_id === "member.one", "issue方法解析成功");
const revoked = await accountsApi.revoke("k", "member.one", "rk-revoke-1");
ok(revoked.generation === 4, "revoke方法解析成功");
const revokedSelf = await accountsApi.revokeSelf("k", "rk-self-1");
ok(revokedSelf.principal_id === "admin.main", "revokeSelf方法解析成功");

const setMembers = await accountsApi.memberships("k", "member.one", { memberships: [{ workspace_id: "ws_team", role: "admin", approve: false }], expected_generation: 2 }, "rk-mem-1");
ok(setMembers.memberships.length === 1, "memberships方法解析成功");
const putBody = JSON.parse(String([...calls].reverse().find(c => c.init.method === "PUT")!.init.body));
ok(putBody.expected_generation === 2 && putBody.request_key === "rk-mem-1" && putBody.memberships[0]!.approve === false, "memberships请求携带代次与request_key");

const events = await accountsApi.events("k", { principal_id: "member.one" });
ok(events.items.length === 1, "events方法解析成功");

// 客户端校验：非法输入在fetch之前拦截，不消耗request_key
const callsBefore = calls.length;
ok(await errorOf(() => accountsApi.account("k", "bad/id")) instanceof Error, "非法账号ID被拦截");
ok(await errorOf(() => accountsApi.update("k", "member.one", { expected_revision: 0 }, "rk")) instanceof Error, "非法修订号被拦截");
ok(await errorOf(() => accountsApi.memberships("k", "member.one", { memberships: Array.from({ length: 65 }, (_, i) => ({ workspace_id: "ws" + i, role: "member" as const, approve: true })), expected_generation: 1 }, "rk")) instanceof Error, "超过64条成员资格被拦截");
ok(await errorOf(() => accountsApi.create("k", { display_name: "   " }, "rk")) instanceof Error, "空显示名称被拦截");
ok(await errorOf(() => accountsApi.list("k", { limit: 101 })) instanceof Error, "超过页容量被拦截");
ok(calls.length === callsBefore, "客户端校验失败不发起请求");

// 2xx但契约不符的写入按结果未知处理
restoreApi();
const restoreBadContract = stubFetch(() => new Response("{}", { status: 201 }));
const badContract = await errorOf(() => accountsApi.create("k", { display_name: "x" }, "rk-x"));
ok(badContract instanceof AccountUnknownError && isAccountOutcomeUnknown(badContract) === true, "写入响应契约不符按结果未知处理");
restoreBadContract();
const restoreBadRead = stubFetch(() => new Response("{}", { status: 200 }));
const badRead = await errorOf(() => accountsApi.profile("k"));
ok(!(badRead instanceof AccountUnknownError) && badRead instanceof Error, "读取响应契约不符按普通错误处理");
restoreBadRead();

// ---- accountError：中文具体文案 ----

const codeOf = (status: number, code: string) => new RequestError(status, code);
ok(accountError(codeOf(401, "unauthorized")) === "密钥无效，请重新连接", "401文案");
ok(accountError(codeOf(403, "permission_denied")) === "当前身份没有此操作权限", "403文案");
ok(accountError(codeOf(422, "invalid_input")) === "输入不符合要求，请检查后重试", "422文案");
ok(accountError(codeOf(409, "revision_conflict")) === "账号已被其他操作更新，请刷新后重试", "修订冲突文案");
ok(accountError(codeOf(409, "key_conflict")) === "请求内容与原提交不一致，请刷新核对", "请求冲突文案");
ok(accountError(codeOf(404, "not_found")) === "未找到该账号或记录", "404文案");
ok(accountError(codeOf(409, "account_management_unavailable")) === "账号管理未启用", "账号管理未启用文案");
ok(accountError(codeOf(503, "storage_unavailable")) === "操作结果未确认，请刷新核对账号状态", "存储不可用按结果未知提示");
ok(accountError(new AccountUnknownError("x")) === "操作结果未确认，请刷新核对账号状态", "结果未知文案");
ok(accountError(new Error("显示名称无效")) === "显示名称无效", "客户端校验文案透传");
ok(accountError(new Error("其他")) === "账号操作未完成", "未知错误兜底文案");

console.log(JSON.stringify({ checks, module: "accounts" }));
