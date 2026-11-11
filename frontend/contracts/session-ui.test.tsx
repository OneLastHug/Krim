import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { StrictMode } from "react";
import { afterEach, beforeEach, expect, test, vi } from "vitest";
import { App } from "../src/App";
import { INTENTS, KNOWN, readIntents } from "../src/lib/session-requests";
import { navigationObservation, navigationObservationPage } from "./observability-navigation-fixture";

let root: Root, container: HTMLDivElement;
let override: ((path: string, options: RequestInit) => Response | Promise<Response> | undefined) | undefined;
const releases: (() => void)[] = [];
const calls: { path: string; method: string; body: unknown }[] = [];
const CONTROLS = "krim.agent-core.controls/1";
const json = (body: unknown, status = 200) => new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });
const session = (id = "A", key = "created") => ({ id, workspace_id: "local", owner_id: "local", create_key: key, title: "演示", target_key: "demo", target_fingerprint: "0".repeat(64), context_policy: { max_bytes: 131072, max_messages: 256, reserved_output_tokens: 2048, counter_version: "approx-utf8/1" }, revision: 0, status: "open", dispatch_paused: false, created_at: 1, closed_at: null });
beforeEach(() => {
  localStorage.clear(); history.replaceState({}, "", "/ui/?view=chat"); calls.length = 0; override = undefined;
  container = document.createElement("div"); document.body.append(container); root = createRoot(container);
  (globalThis as unknown as { IS_REACT_ACT_ENVIRONMENT: boolean }).IS_REACT_ACT_ENVIRONMENT = true;
  vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, options: RequestInit = {}) => {
    const path = String(input), method = options.method ?? "GET", body: unknown = options.body ? JSON.parse(String(options.body)) : null;
    calls.push({ path, method, body });
    const overridden = override?.(path, options); if (overridden !== undefined) return overridden;
    if (path === "/healthz") return json({ http_enabled: false });
    if (path === "/v2/me") return json({ principal_id: "local", generation: 1, legacy: true, workspaces: [{ workspace_id: "local", role: "admin", approve: true }] });
    // 账号接入后的新身份链路：legacy服务器资料返回409 account_management_unavailable，接入信息404。
    if (path === "/v2/me/profile") return json({ error: { code: "account_management_unavailable" } }, 409);
    if (path.startsWith("/v2/targets/")) return json({ error: { code: "not_found" } }, 404);
    if (path.includes("/turns")) return json([]);
    if (method === "POST" && path.endsWith("/sessions")) return json(session("A", (body as { request_key: string }).request_key), 201);
    if (path.endsWith("/sessions/A")) return json(session());
    return json([]);
  }));
});
afterEach(async () => { await act(async () => root.unmount()); await act(async () => { for (const release of releases.splice(0)) release(); await Promise.resolve(); }); container.remove(); vi.unstubAllGlobals(); });
async function until(check: () => void) { let error: unknown; for (let i = 0; i < 60; i++) { try { check(); return; } catch (value) { error = value; await act(async () => { await new Promise(r => setTimeout(r, 0)); }); } } throw error; }
function button(label: string) { const item = [...document.querySelectorAll("button")].find(b => b.getAttribute("aria-label") === label || b.textContent?.trim() === label); expect(item, label).toBeTruthy(); return item!; }
// 侧栏「新建对话」按钮：aria-label 与聊天工具栏同名按钮重复，需容器限定。
function createButton() { const item = document.querySelector<HTMLButtonElement>(".sidebar-buttons [aria-label='新建对话']"); expect(item, "新建对话").toBeTruthy(); return item!; }
function menuItem(label: string) { const item = [...document.querySelectorAll<HTMLButtonElement>("[role='menuitem']")].find(b => b.textContent?.trim() === label); expect(item, label).toBeTruthy(); return item!; }
// 顶栏「更多操作」菜单：刷新对话/关闭对话等由常驻按钮改为菜单项，须先弹出菜单。
async function openMorePopover() { await act(async () => (document.querySelector("header.chat-toolbar [aria-label='更多操作']") as HTMLButtonElement).click()); await until(() => expect(document.querySelector(".popover-menu[aria-label='更多操作']")).toBeTruthy()); }
// 连接入口在侧栏账号菜单：账号菜单 → 「切换账号」→ Dialog（登录先 GET /v2/me 验证再应用）。
async function openAccountMenu() { await act(async () => (document.querySelector('[aria-label="账号菜单"]') as HTMLButtonElement).click()); await until(() => expect(document.querySelector(".popover-menu[aria-label='账号菜单']")).toBeTruthy()); }

test("侧栏会话只展示标题，不再渲染或朗读创建时间", async () => {
  history.replaceState({}, "", "/ui/?view=chat&workspace=local&session=A");
  override = path => path === "/v2/workspaces/local/sessions?limit=50" ? json([session()]) : undefined;
  await act(async () => root.render(<App />));
  await until(() => expect(container.querySelector(".chat-session-row .session-select")).toBeTruthy());
  const row = container.querySelector(".chat-session-row")!;
  expect(row.querySelector(".chat-session-label")?.textContent).toBe("演示");
  expect(row.querySelector("small, time, [id^='session-created-']")).toBeNull();
  expect(row.querySelector(".session-select")?.getAttribute("aria-describedby")).toBeNull();
  expect(calls.filter(value => value.method !== "GET")).toHaveLength(0);
});

test("真实App StrictMode对话入口零自动POST，新建仅由显式操作", async () => {
  await act(async () => root.render(<StrictMode><App /></StrictMode>));
  expect(calls.filter(c => c.method === "POST")).toEqual([]);
  await act(async () => button("导航到对话").click());
  await until(() => expect(createButton()).toBeTruthy());
  expect(calls.filter(c => c.method === "POST")).toEqual([]);
  await act(async () => createButton().click());
  await act(async () => button("创建对话").click());
  await until(() => expect(calls.filter(c => c.method === "POST")).toHaveLength(1));
  await until(() => expect(new URL(location.href).searchParams.get("session")).toBe("A"));
});

test("身份GET失败后刷新只读恢复且零POST", async () => {
  let reject = true;
  override = path => path === "/v2/me" && reject ? json({ error: { code: "execution_error" } }, 503) : undefined;
  await act(async () => root.render(<StrictMode><App /></StrictMode>));
  await until(() => expect(container.textContent).toContain("读取失败，请刷新"));
  reject = false;
  await act(async () => (document.querySelector('[aria-label="刷新对话"]') as HTMLButtonElement).click());
  await until(() => expect(createButton().disabled).toBe(false));
  expect(calls.filter(c => c.method === "POST")).toHaveLength(0);
});

async function type(id: string, value: string) {
  await act(async () => {
    const input = document.getElementById(id) as HTMLInputElement | HTMLTextAreaElement;
    expect(input, id).toBeTruthy();
    Object.getOwnPropertyDescriptor(input.tagName === "TEXTAREA" ? HTMLTextAreaElement.prototype : HTMLInputElement.prototype, "value")!.set!.call(input, value);
    input.dispatchEvent(new Event("input", { bubbles: true }));
  });
}
function deferred() { let release!: () => void; const promise = new Promise<void>(r => { release = r; }); releases.push(release); return { promise, release }; }
function turn(body: { text: string; request_key: string; expected_revision: number }, status: "queued" | "running" | "completed" = "completed") {
  return { id: "A1", session_id: "A", workspace_id: "local", owner_id: "local", ordinal: 1, request_key: body.request_key, text: body.text, accepted_revision: body.expected_revision, status, run_id: "run-A1", context_fingerprint: "1".repeat(64), transcript: [{ role: "user", content: body.text, call_id: null, tool_calls: [] }, { role: "assistant", content: "已完成", call_id: null, tool_calls: [] }], code: null, created_at: 1, started_at: 1, finished_at: 2 };
}
function historyTurn(id: string, ordinal: number, status: "queued" | "running" | "completed" = "completed") {
  return { ...turn({ text: id, request_key: "key-" + id, expected_revision: 0 }, status), id, ordinal, run_id: "run-" + id };
}
function server(barrier: "post" | "get" | "none", hold = deferred()) {
  const base = "/v2/workspaces/local/sessions"; let accepted: ReturnType<typeof turn> | null = null, findCount = 0, rejectFirst: { status: number; code: string } | null = null;
  override = (path, options) => {
    if (path === "/v2/me" && (options.headers as Record<string, string> | undefined)?.Authorization === "Bearer corrected-synthetic") return json({ principal_id: "local", generation: 2, legacy: true, workspaces: [{ workspace_id: "local", role: "admin", approve: true }] });
    if (path === base + "?limit=50") return json([ { ...session("A"), title: "会话 A", revision: accepted ? 1 : 0 }, { ...session("B"), title: "会话 B" } ]);
    if (path === base + "/A/turns" && options.method === "POST") {
      if (rejectFirst) { const value = rejectFirst; rejectFirst = null; return json({ error: { code: value.code } }, value.status); }
      accepted = turn(JSON.parse(String(options.body)) as Parameters<typeof turn>[0]);
      if (barrier === "post") return hold.promise.then(() => json(accepted, 202));
      if (barrier === "get") return Promise.reject(new TypeError("accepted response lost"));
      return json(accepted, 202);
    }
    if (path.startsWith(base + "/A/turns/by-key/")) { findCount++; if (barrier === "get" && findCount === 1) return hold.promise.then(() => json(accepted)); return json(accepted); }
    if (path.startsWith(base + "/A/turns?")) return json(accepted ? [accepted] : []);
    if (path.startsWith(base + "/B/turns?")) return json([]);
    if (path === base + "/A") return json({ ...session("A"), title: "会话 A", revision: accepted ? 1 : 0 });
    if (path === base + "/B") return json({ ...session("B"), title: "会话 B" });
    return undefined;
  };
  return { hold, get accepted() { return accepted; }, get findCount() { return findCount; }, reject(value: { status: number; code: string }) { rejectFirst = value; } };
}
async function mountExisting() {
  history.replaceState({}, "", "/ui/?view=chat&workspace=local&session=A");
  await act(async () => root.render(<StrictMode><App /></StrictMode>));
  await until(() => expect((document.getElementById("chat-prompt") as HTMLTextAreaElement).disabled).toBe(false));
}
test("长历史轮询只重读活动页，并合并终态后的新Turn", async () => {
  const base = "/v2/workspaces/local/sessions", prefix = Array.from({ length: 50 }, (_, index) => historyTurn("old-" + index, index + 1));
  const active = historyTurn("active-50", 51, "running"), added = historyTurn("new-51", 52, "queued");
  const offsets: number[] = []; let activePageReads = 0;
  vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
  override = path => {
    if (path === base + "?limit=50") return json([{ ...session("A"), title: "长历史" }]);
    if (path === base + "/A") return json({ ...session("A"), title: "长历史" });
    if (path.startsWith(base + "/A/turns?offset=")) {
      const offset = Number(new URL("http://local" + path).searchParams.get("offset")); offsets.push(offset);
      if (offset === 0) return json(prefix);
      if (offset === 50) {
        activePageReads++;
        if (activePageReads === 1) return json([active]);
        if (activePageReads === 2) return json([{ ...active, status: "completed" }, added]);
        return json([{ ...active, status: "completed" }, { ...added, status: "completed" }]);
      }
    }
    if (path.includes("/events?") || path.endsWith("/approvals")) return json([]);
    if (path.endsWith("/result")) return json({ run: { id: "run-active-50" }, tools: [] });
    return undefined;
  };
  history.replaceState({}, "", "/ui/?view=chat&workspace=local&session=A");
  try {
    await act(async () => root.render(<App />));
    expect(activePageReads).toBe(1);
    await act(async () => { await vi.advanceTimersByTimeAsync(300); });
    expect(activePageReads).toBe(2);
    expect(document.querySelector('[data-turn-id="new-51"]')).toBeTruthy();
    await act(async () => { await vi.advanceTimersByTimeAsync(300); });
    expect(document.querySelector('[data-turn-id="new-51"]')?.textContent).toContain("已完成");
    expect(offsets).toEqual([0, 50, 50, 50]);
  } finally { vi.useRealTimers(); }
});

test("会话切换时丢弃迟到的历史页响应", async () => {
  server("none"); const fallback = override!, base = "/v2/workspaces/local/sessions";
  const prefix = Array.from({ length: 50 }, (_, index) => historyTurn("old-" + index, index + 1));
  let release!: (response: Response) => void, started = 0;
  const delayed = new Promise<Response>(resolve => { release = resolve; });
  override = (path, options) => {
    if (path === base + "?limit=50") return json([{ ...session("A"), title: "会话 A" }, { ...session("B"), title: "会话 B" }]);
    if (path === base + "/A") return json({ ...session("A"), title: "会话 A" });
    if (path === base + "/B") return json({ ...session("B"), title: "会话 B" });
    if (path === base + "/A/turns?offset=0&limit=50") return json(prefix);
    if (path === base + "/A/turns?offset=50&limit=50") { started++; return delayed; }
    if (path.startsWith(base + "/B/turns?")) return json([]);
    return fallback(path, options);
  };
  history.replaceState({}, "", "/ui/?view=chat&workspace=local&session=A");
  await act(async () => root.render(<App />));
  await until(() => expect(started).toBe(1));
  await until(() => expect(button("会话 B")).toBeTruthy());
  await act(async () => button("会话 B").click());
  await until(() => expect(new URL(location.href).searchParams.get("session")).toBe("B"));
  await until(() => expect((document.getElementById("chat-prompt") as HTMLTextAreaElement).disabled).toBe(false));
  await act(async () => { release(json([historyTurn("late-A", 51, "running")])); await new Promise(resolve => setTimeout(resolve, 0)); });
  expect(new URL(location.href).searchParams.get("session")).toBe("B");
  expect(document.querySelector('[data-turn-id="late-A"]')).toBeNull();
  expect(document.querySelector('[data-turn-id="old-0"]')).toBeNull();
});
function controlServer(action: "close" | "cancel", first: "conflict" | "unknown" = "conflict") {
  server("none");
  const fallback = override!, base = "/v2/workspaces/local/sessions";
  const target = action === "close" ? base + "/A/close" : base + "/A/turns/A1/cancel";
  const posts: { request_key: string; expected_revision: number }[] = [];
  const activeTurn = turn({ text: "正在执行", request_key: "running-turn", expected_revision: 0 }, "running");
  let revision = 0, firstPost = true, closed = false, freshRevisionReads = 0;
  override = (path, options) => {
    const method = options.method ?? "GET";
    if (path === base + "/A" && method === "GET") {
      if (revision === 1) freshRevisionReads++;
      return json({ ...session("A"), revision, status: closed ? "closed" : "open" });
    }
    if (action === "cancel" && path.startsWith(base + "/A/turns?")) return json([activeTurn]);
    if (action === "cancel" && (path.includes("/events?") || path.endsWith("/approvals"))) return json([]);
    if (path === target && method === "POST") {
      const body = JSON.parse(String(options.body)) as { request_key: string; expected_revision: number };
      posts.push(body);
      if (firstPost) {
        firstPost = false;
        if (first === "conflict") { revision = 1; return json({ error: { code: "revision_conflict" } }, 409); }
        return Promise.reject(new TypeError("accepted response lost"));
      }
      if (action === "close") closed = true;
      return json({ session_id: "A", request_key: body.request_key, status: "completed" });
    }
    if (path.startsWith(base + "/A/controls/by-key/")) return json({ error: { code: "not_found" } }, 404);
    return fallback(path, options);
  };
  return { posts, get freshRevisionReads() { return freshRevisionReads; }, target };
}
function controlLedger() { return JSON.parse(localStorage.getItem(CONTROLS) ?? "{}") as Record<string, { body: { request_key: string } }> }

for (const action of ["close", "cancel"] as const) {
  test(`首次${action}遇明确revision_conflict后刷新并显式新操作`, async () => {
    const fixture = controlServer(action); await mountExisting();
    const label = action === "close" ? "关闭对话" : "停止";
    // 「关闭对话」改为顶栏更多操作菜单项（会话已载入时渲染）；「停止」仍是发送区常驻按钮。
    async function trigger() {
      if (action === "close") { await openMorePopover(); await act(async () => menuItem(label).click()); }
      else { await until(() => expect(button(label)).toBeTruthy()); await act(async () => button(label).click()); }
    }
    await trigger();
    await until(() => expect(container.textContent).toContain("请刷新检查状态后重新操作"));
    expect(fixture.posts).toHaveLength(1); expect(controlLedger()).toEqual({});
    expect([...document.querySelectorAll("button")].some(item => item.textContent?.trim() === "查找操作结果")).toBe(false);

    await openMorePopover();
    await act(async () => menuItem("刷新对话").click());
    await until(() => expect(fixture.freshRevisionReads).toBeGreaterThan(0));
    expect(fixture.posts).toHaveLength(1);
    await trigger();
    await until(() => expect(fixture.posts).toHaveLength(2));
    expect(fixture.posts.map(item => item.expected_revision)).toEqual([0, 1]);
    expect(fixture.posts[1]!.request_key).not.toBe(fixture.posts[0]!.request_key);
    await until(() => expect(controlLedger()).toEqual({}));
    expect(calls.filter(item => item.method === "POST" && item.path === fixture.target)).toHaveLength(2);
  });
}

test("未知控制POST只GET查询原key，404仍保留待确认账本", async () => {
  const fixture = controlServer("close", "unknown"); await mountExisting();
  await openMorePopover();
  await act(async () => menuItem("关闭对话").click());
  await until(() => expect(button("查找操作结果")).toBeTruthy());
  const original = controlLedger(), key = Object.values(original)[0]!.body.request_key;
  await act(async () => button("查找操作结果").click());
  await until(() => expect(calls.filter(item => item.path.includes("/controls/by-key/")).length).toBe(1));
  await until(() => expect(container.textContent).toContain("未找到记录"));
  expect(calls.find(item => item.path.includes("/controls/by-key/"))!.path).toContain("/controls/by-key/" + encodeURIComponent(key));
  expect(fixture.posts).toHaveLength(1); expect(controlLedger()).toEqual(original);
});

for (const barrier of ["post", "get"] as const) for (const steering of ["tab", "other", "key", "refresh", "remount"] as const) {
  test(`${barrier}成功回程遇${steering}保留完整intent，显式GET找回且仅一POST`, async () => {
    const fixture = server(barrier); await mountExisting(); await type("chat-prompt", "原始消息不许替换");
    await act(async () => button("发送").click());
    await until(() => expect(fixture.accepted).not.toBeNull());
    const original = JSON.parse(JSON.stringify(readIntents(localStorage)[0])) as unknown;
    if (barrier === "get") { await act(async () => button("查找原请求").click()); await until(() => expect(fixture.findCount).toBe(1)); }
    if (steering === "tab") await act(async () => button("导航到运行记录").click());
    if (steering === "other") await act(async () => button("会话 B").click());
    if (steering === "key") { await openAccountMenu(); await act(async () => menuItem("切换账号").click()); await type("api-key", "corrected-synthetic"); await act(async () => document.getElementById("connect-save")!.click()); }
    if (steering === "refresh") { await openMorePopover(); await act(async () => menuItem("刷新对话").click()); }
    if (steering === "remount") { await act(async () => root.unmount()); root = createRoot(container); await act(async () => root.render(<StrictMode><App /></StrictMode>)); }
    await act(async () => { fixture.hold.release(); await new Promise(r => setTimeout(r, 5)); });
    expect(readIntents(localStorage)).toEqual([original]);
    expect(localStorage.getItem(KNOWN)).toBeNull();
    if (steering === "other") expect(new URL(location.href).searchParams.get("session")).toBe("B");
    expect(calls.filter(c => c.method === "POST")).toHaveLength(1);
    if (steering === "tab") await act(async () => button("导航到对话").click());
    await until(() => expect(button("查找原请求")).toBeTruthy());
    await act(async () => button("查找原请求").click());
    await until(() => expect(readIntents(localStorage)).toEqual([]));
    expect(new URL(location.href).searchParams.get("session")).toBe("A");
    expect(new URL(location.href).searchParams.get("turn")).toBe("A1");
    expect(calls.filter(c => c.method === "POST")).toHaveLength(1);
    expect(JSON.stringify(JSON.parse(localStorage.getItem(KNOWN)!))).toContain("A1");
  });
}

for (const rejection of [{ status: 401, code: "unauthorized" }, { status: 429, code: "capacity" }, { status: 422, code: "input_limit" }]) {
  test(`首次${rejection.status}拒绝保留草稿，纠正后显式提交恰一效果`, async () => {
    const fixture = server("none"); fixture.reject(rejection); await mountExisting(); await type("chat-prompt", "保留草稿");
    await act(async () => button("发送").click());
    await until(() => expect(readIntents(localStorage)).toEqual([]));
    expect(fixture.accepted).toBeNull(); expect((document.getElementById("chat-prompt") as HTMLTextAreaElement).value).toBe("保留草稿");
    await act(async () => button("发送").click());
    await until(() => expect(new URL(location.href).searchParams.get("turn")).toBe("A1"));
    expect(calls.filter(c => c.method === "POST")).toHaveLength(2); expect(fixture.accepted!.text).toBe("保留草稿");
  });
}

test("双击固定原键，GET404不解除未知intent", async () => {
  const fixture = server("get"); await mountExisting(); await type("chat-prompt", "一次效果");
  const send = button("发送"); await act(async () => { send.click(); send.click(); });
  await until(() => expect(readIntents(localStorage)).toHaveLength(1));
  const stored = localStorage.getItem(INTENTS)!;
  const prior = override; let reject = true;
  override = (path, options) => path.includes("/turns/by-key/") && reject ? json({ error: { code: "not_found" } }, 404) : prior?.(path, options);
  await act(async () => button("查找原请求").click());
  await until(() => expect(container.textContent).toContain("未找到记录"));
  expect(localStorage.getItem(INTENTS)).toBe(stored); expect(calls.filter(c => c.method === "POST")).toHaveLength(1);
  reject = false; fixture.hold.release();
  await act(async () => button("查找原请求").click()); await until(() => expect(readIntents(localStorage)).toEqual([]));
});

test("消息更多操作提供可导航Trace路由且保持只读", async () => {
  server("none");
  const fallback = override!, base = "/v2/workspaces/local/sessions";
  override = (path, options) => {
    if (path === base + "/A/turns?offset=0&limit=50") return json([historyTurn("A1", 1)]);
    const observation = navigationObservation("A", "A1", "run-A1", "会话 A");
    if (path.startsWith("/v2/workspaces/local/observations?")) return json(navigationObservationPage("local", [observation.item]));
    if (path === base + "/A/turns/A1/observations") return json(observation.detail);
    return fallback(path, options);
  };
  await mountExisting();
  // 消息级「更多操作」与顶栏同名按钮重名，限定到回答区操作条；展开后「查看 Trace」入口不变。
  const messageMore = () => document.querySelector<HTMLButtonElement>('.chat-answer-actions [aria-label="更多操作"]');
  await until(() => expect(messageMore()).toBeTruthy());
  await act(async () => messageMore()!.click());
  await until(() => expect(button("查看 Trace")).toBeTruthy());
  await act(async () => button("查看 Trace").click());
  await until(() => expect(container.querySelector('section[aria-label="Trace 页面"]')?.hasAttribute("hidden")).toBe(false));
  const url = new URL(location.href);
  expect(url.searchParams.get("view")).toBe("trace");
  expect(url.searchParams.get("workspace")).toBe("local");
  expect(url.searchParams.get("session")).toBe("A");
  expect(url.searchParams.get("turn")).toBe("A1");
  expect(calls.every(item => item.method === "GET")).toBe(true);
  await until(() => expect(button("返回对话")).toBeTruthy());
  await act(async () => button("返回对话").click());
  await until(() => expect(new URL(location.href).searchParams.get("view")).toBe("chat"));
  expect(new URL(location.href).searchParams.get("session")).toBe("A");
  expect(calls.every(item => item.method === "GET")).toBe(true);
});
