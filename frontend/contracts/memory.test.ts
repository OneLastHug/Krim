import assert from "node:assert/strict";
import {
  MEMORY_LIMITS, MemoryOutcomeUnknownError, memoryError, memoriesApi, newMemoryRequestKey,
  parseMemory, parseMemoryDeleteReceipt, parseMemoryList, parseMemoryPage, parseMemoryPreference,
} from "../src/lib/memory.ts";
import { RequestError } from "../src/api.ts";

let checks = 0;
function ok(value: unknown, message: string) { assert.ok(value, message); checks += 1; }
async function errorOf(run: () => unknown): Promise<unknown> {
  try { const value = run(); if (value instanceof Promise) await value; }
  catch (error) { return error; }
  throw new Error("预期抛出未发生");
}
const memory = (over: Record<string, unknown> = {}) => ({
  id: "51ed345e-8729-47ac-9e3a-9f5a56dc7833", body: "偏好喝无糖茶", revision: 2,
  content_hash: "a".repeat(64), source_ref: "manual-note", workspace_id: null, project_id: null,
  source_kind: "user", memory_kind: "preference", authorization_turn_id: null, created_at: 1761000000, updated_at: 1761000100,
  principal_id: "owner.main", ...over,
});
const json = (body: unknown, status = 200) => new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });
type Call = { path: string; method: string; headers: Headers; body?: Record<string, unknown>; signal?: AbortSignal };
const calls: Call[] = [];
const originalFetch = globalThis.fetch;
function stubFetch(respond: (call: Call) => Response | Promise<Response>) {
  calls.length = 0;
  globalThis.fetch = (async (input: RequestInfo | URL, init: RequestInit = {}) => {
    const call: Call = { path: String(input), method: init.method ?? "GET", headers: new Headers(init.headers),
      body: init.body ? JSON.parse(String(init.body)) as Record<string, unknown> : undefined, signal: init.signal ?? undefined };
    calls.push(call);
    return respond(call);
  }) as typeof fetch;
}
function restoreFetch() { globalThis.fetch = originalFetch; }

assert.equal(MEMORY_LIMITS.page, 200);
assert.equal(MEMORY_LIMITS.default_page, 50);
const parsed = parseMemory(memory());
ok(parsed.memory_kind === "preference" && parsed.revision === 2, "完整记忆字段解析");
ok(parsed.authorization_turn_id === null, "授权来源回合字段保留");
ok(parseMemory(memory({ authorization_turn_id: "8e4b6c03-a33b-4e79-8da1-bab593b15dbb" })).authorization_turn_id === "8e4b6c03-a33b-4e79-8da1-bab593b15dbb", "可解析授权来源回合标识");
ok(await errorOf(() => parseMemory({ ...memory(), leaked: "secret" })) instanceof Error, "拒绝额外记忆字段");
ok(parseMemoryList(Array.from({ length: 1001 }, () => memory())).length === 1001, "旧列表不受客户端条数上限影响");
ok(await errorOf(() => parseMemory({ ...memory(), content_hash: "bad" })) instanceof Error, "拒绝无效内容哈希");
ok(await errorOf(() => parseMemory({ ...memory(), memory_kind: "preference", source_kind: "web" })) instanceof Error, "偏好记忆只能来自用户");
ok(parseMemoryPage({ items: [memory()], next_offset: 1 }).items.length === 1, "记忆分页解析");
ok(await errorOf(() => parseMemoryPage({ items: [], next_offset: -1 })) instanceof Error, "拒绝负分页游标");
ok(parseMemoryPreference({ enabled: false, revision: 0 }).enabled === false, "偏好默认关闭可解析");
ok(await errorOf(() => parseMemoryPreference({ enabled: "false", revision: 0 })) instanceof Error, "偏好拒绝非布尔状态");
const deleted = parseMemoryDeleteReceipt({ id: "memo-1", removed: true, retention_notice: "保留历史记录" });
ok("removed" in deleted && deleted.removed, "兼容旧删除回执");
ok(newMemoryRequestKey().match(/^[!-~]{1,128}$/), "新请求标识符合服务端可见ASCII字符范围");

const workspace = "ws team";
const project = "proj/one";
const scope = { workspace_id: workspace, project_id: project };
stubFetch(call => {
  if (call.method === "GET" && call.path === "/v2/me/memories") return json([memory()]);
  if (call.method === "GET" && call.path.startsWith("/v2/me/memories/page?")) return json({ items: [memory()], next_offset: 50 });
  if (call.method === "GET" && call.path === "/v2/me/memories/memory-1") return json(memory({ id: "memory-1" }));
  if (call.method === "GET" && call.path === "/v2/workspaces/ws%20team/projects/proj%2Fone/memories?offset=0&limit=50") return json({ items: [memory({ workspace_id: workspace, project_id: project })], next_offset: null });
  if (call.method === "POST" && call.path === "/v2/me/memories") return json({ id: "memory-new", revision: 1, content_hash: "b".repeat(64) }, 201);
  if (call.method === "PUT" && call.path === "/v2/workspaces/ws%20team/projects/proj%2Fone/memories/memory-1") return json({ id: "memory-1", revision: 3, content_hash: "c".repeat(64) });
  if (call.method === "DELETE" && call.path === "/v2/me/memories/memory-1") return json({ id: "memory-1", removed: true, retention_notice: "保留历史记录" });
  if (call.path === "/v2/me/memory-preference" && call.method === "GET") return json({ enabled: false, revision: 0 });
  if (call.path === "/v2/me/memory-preference" && call.method === "PUT") return json({ enabled: true, revision: 1 });
  return json({ error: { code: "not_found" } }, 404);
});

ok((await memoriesApi.list("secret")).length === 1, "旧账户记忆列表仍接 list 响应");
const legacyListCall = calls.at(-1)!;
ok(legacyListCall.path === "/v2/me/memories" && legacyListCall.headers.get("Authorization") === "Bearer secret", "旧账户列表路径与授权正确");
const page = await memoriesApi.page("secret", { offset: 0, limit: 50, query: "茶饮" });
ok(page.items[0]!.body === "偏好喝无糖茶", "账户分页返回完整记忆");
ok(calls.at(-1)!.path === "/v2/me/memories/page?offset=0&limit=50&query=%E8%8C%B6%E9%A5%AE", "账户分页及查询编码正确");
await memoriesApi.page("secret", { limit: 200, query: "茶".repeat(129) });
ok(calls.at(-1)!.path.includes("limit=200&query="), "分页允许服务端最大200条且搜索不附加本地长度限制");
ok((await memoriesApi.get("secret", "memory-1")).id === "memory-1", "账户详情路径正确");
ok((await memoriesApi.page("secret", {}, scope)).next_offset === null, "项目分页路径编码正确");
const createReceipt = await memoriesApi.create("secret", { request_key: "create+memory/1", body: "一条新记忆" });
ok(createReceipt.revision === 1, "账户创建回执正确解析");
const createCall = calls.at(-1)!;
ok(createCall.method === "POST" && createCall.path === "/v2/me/memories" && createCall.body?.request_key === "create+memory/1", "账户创建沿用旧写入路径并支持服务端合法请求标识");
await memoriesApi.create("secret", { request_key: "large-body", body: "字".repeat(33000) });
ok(String(calls.at(-1)!.body?.body).length === 33000, "记忆正文不附加本地总长度限制");
const updateReceipt = await memoriesApi.update("secret", "memory-1", { request_key: "update-memory", body: "修改内容",
  source_kind: "user", memory_kind: "preference", expected_revision: 2, expected_content_hash: "a".repeat(64) }, scope);
ok(updateReceipt.revision === 3, "项目更新回执正确解析");
const updateCall = calls.at(-1)!;
ok(updateCall.method === "PUT" && updateCall.body?.expected_revision === 2 && updateCall.body.expected_content_hash === "a".repeat(64), "更新携带乐观锁版本与哈希");
const removal = await memoriesApi.remove("secret", "memory-1", { request_key: "delete-memory", expected_revision: 2, expected_content_hash: "a".repeat(64) });
ok("removed" in removal && removal.retention_notice === "保留历史记录", "删除保留旧接口回执数据");
ok(calls.at(-1)!.method === "DELETE" && calls.at(-1)!.body?.request_key === "delete-memory", "DELETE使用JSON请求体及请求标识");
ok((await memoriesApi.preference("secret")).enabled === false, "偏好GET路径正确且默认关闭");
ok((await memoriesApi.setPreference("secret", true, 0, "pref-enable")).revision === 1, "偏好PUT更新成功");
const preferenceCall = calls.at(-1)!;
ok(preferenceCall.method === "PUT" && preferenceCall.body?.request_key === "pref-enable" && preferenceCall.body.expected_revision === 0, "偏好写入包含请求标识与revision");

const beforeInvalid = calls.length;
ok(await errorOf(() => memoriesApi.page("secret", { limit: 201 })) instanceof Error, "超过服务端最大200条被拦截");
ok(await errorOf(() => memoriesApi.create("secret", { request_key: "bad key", body: "内容" })) instanceof Error, "非法请求标识在发送前拒绝");
ok(await errorOf(() => memoriesApi.update("secret", "x", { request_key: "ok", body: "内容", expected_revision: 0, expected_content_hash: "x" })) instanceof Error, "无效修订号或哈希在发送前拒绝");
ok(calls.length === beforeInvalid, "参数校验失败不发送请求");
restoreFetch();

stubFetch(() => json({ error: { code: "revision_conflict" } }, 409));
const conflict = await errorOf(() => memoriesApi.setPreference("secret", true, 1, "pref-conflict"));
ok(conflict instanceof RequestError && memoryError(conflict).includes("其他页面更新"), "冲突按明确拒绝展示具体提示");
restoreFetch();
stubFetch(() => { throw new TypeError("network down"); });
const unknown = await errorOf(() => memoriesApi.create("secret", { request_key: "create-unknown", body: "内容" }));
ok(unknown instanceof MemoryOutcomeUnknownError && memoryError(unknown).includes("尚未确认"), "写入网络中断视为结果未确认");
restoreFetch();

console.log(JSON.stringify({ checks, module: "memory" }));
