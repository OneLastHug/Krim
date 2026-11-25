import { act, StrictMode } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, expect, test, vi } from "vitest";
import { App } from "../src/App";
import { LOGIN_STATE, saveLoginState } from "../src/lib/login-state";
import { request } from "../src/api";

// 登录持久化契约：真实 App + mock fetch，不走真实后端。
// 覆盖 localStorage krim.login/1 的读写边界：自动登录重挂、登录成败、退出、
// 401 清凭据（网络错误保留）、轮换 onKeyPrepared 立即落盘、存储损坏/拒绝不崩溃。
// 轮换走 mock AccountSettings 桩（真实轮换弹窗交互由 account-settings-ui.test.tsx 覆盖），
// 这里只验证 App 的 onKeyPrepared 边界与刷新后的密钥来源。
vi.mock("../src/components/AccountSettings", async () => {
  const { jsx } = await import("react/jsx-runtime");
  return {
    AccountSettings: ({ onKeyPrepared }: { onKeyPrepared: (key: string) => void }) =>
      jsx("button", { id: "rotate-stub", type: "button", onClick: () => onKeyPrepared("rotated-key"), children: "模拟轮换完成" }),
  };
});

let root: Root, container: HTMLDivElement;
let override: ((path: string, options: RequestInit) => Response | Promise<Response> | undefined) | undefined;
const calls: { path: string; method: string; auth: string }[] = [];
const json = (body: unknown, status = 200) => new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });
const actorOf = (legacy: boolean, principal = "local") => ({ principal_id: principal, generation: 1, legacy, target_keys: ["target-http"], workspaces: [{ workspace_id: "local", role: "admin", approve: true }] });
const session = () => ({ id: "A", workspace_id: "local", owner_id: "local", create_key: "created", title: "演示", target_key: "target-http", target_fingerprint: "0".repeat(64), context_policy: { max_bytes: 131072, max_messages: 256, reserved_output_tokens: 2048, counter_version: "approx-utf8/1" }, revision: 0, status: "open", dispatch_paused: false, created_at: 1, closed_at: null });
const profileOf = () => ({
  principal_id: "acct-admin", display_name: "账号甲", status: "active", platform_role: "platform_admin",
  generation: 1, revision: 5, legacy: false, key: null,
  workspaces: [{ workspace_id: "local", display_name: "个人工作区", role: "admin", approve: true, personal: true }],
});
const stored = () => {
  const raw = localStorage.getItem(LOGIN_STATE);
  return raw === null ? null : JSON.parse(raw) as { key: string; loggedOut: boolean };
};
const seed = (key: string, loggedOut: boolean) => localStorage.setItem(LOGIN_STATE, JSON.stringify({ key, loggedOut }));
const authLeaks = () => calls.filter(call => /^Bearer .+/.test(call.auth));

beforeEach(() => {
  localStorage.clear(); sessionStorage.clear(); calls.length = 0; override = undefined;
  history.replaceState({}, "", "/ui/?view=chat");
  container = document.createElement("div"); document.body.append(container); root = createRoot(container);
  (globalThis as unknown as { IS_REACT_ACT_ENVIRONMENT: boolean }).IS_REACT_ACT_ENVIRONMENT = true;
  vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, options: RequestInit = {}) => {
    const path = String(input), method = options.method ?? "GET";
    calls.push({ path, method, auth: new Headers(options.headers).get("Authorization") ?? "" });
    const handled = override?.(path, options);
    if (handled !== undefined) return handled;
    if (path === "/healthz") return json({ http_enabled: false });
    if (path === "/v2/me") return json(actorOf(true));
    if (path === "/v2/me/profile") return json({ error: { code: "account_management_unavailable" } }, 409);
    if (path.startsWith("/v2/targets/")) return json({ error: { code: "not_found" } }, 404);
    if (path === "/v2/workspaces/local/sessions?limit=50") return json([]);
    if (method === "POST" && path.endsWith("/sessions")) return json(session(), 201);
    if (path === "/v2/workspaces/local/sessions/A") return json(session());
    if (path.includes("/turns")) return json([]);
    return json([]);
  }));
});
afterEach(async () => {
  await act(async () => root.unmount());
  container.remove(); vi.restoreAllMocks(); vi.unstubAllGlobals();
});
async function until(check: () => void) {
  let error: unknown;
  for (let attempt = 0; attempt < 80; attempt++) {
    try { check(); return; } catch (value) { error = value; }
    await act(async () => { await new Promise(resolve => setTimeout(resolve, 0)); });
  }
  throw error;
}
async function mountApp(url = "/ui/?view=chat") {
  history.replaceState({}, "", url);
  await act(async () => root.render(<StrictMode><App /></StrictMode>));
}
async function remount(url = "/ui/?view=chat") {
  await act(async () => root.unmount());
  root = createRoot(container);
  await mountApp(url);
}
function createButton() {
  const item = document.querySelector<HTMLButtonElement>(".sidebar-buttons [aria-label='新建对话']");
  expect(item, "新建对话").toBeTruthy();
  return item!;
}
function buttonByText(label: string) {
  const item = [...document.querySelectorAll<HTMLButtonElement>("button")].find(button => button.textContent?.trim() === label);
  expect(item, label).toBeTruthy();
  return item!;
}
function menuItem(label: string) {
  const item = [...document.querySelectorAll<HTMLButtonElement>("[role='menuitem']")].find(button => button.textContent?.trim() === label);
  expect(item, label).toBeTruthy();
  return item!;
}
async function typeInto(id: string, value: string) {
  await act(async () => {
    const input = document.getElementById(id) as HTMLInputElement;
    expect(input, id).toBeTruthy();
    Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, "value")!.set!.call(input, value);
    input.dispatchEvent(new Event("input", { bubbles: true }));
  });
}

test("已保存会话挂载即自动登录，卸载重挂仍携带原密钥的 Authorization", async () => {
  seed("saved-key", false);
  await mountApp();
  await until(() => expect(createButton().disabled).toBe(false));
  expect(calls.filter(call => call.path === "/v2/me").length).toBeGreaterThan(0);
  expect(calls.filter(call => call.path === "/v2/me").every(call => call.auth === "Bearer saved-key")).toBe(true);
  expect(stored()).toEqual({ key: "saved-key", loggedOut: false });
  const before = calls.length;
  await remount();
  await until(() => expect(createButton().disabled).toBe(false));
  const me = calls.slice(before).filter(call => call.path === "/v2/me");
  expect(me.length).toBeGreaterThan(0);
  expect(me.every(call => call.auth === "Bearer saved-key")).toBe(true);
  expect(stored()).toEqual({ key: "saved-key", loggedOut: false });
});

test("登录失败不覆盖已保存状态，成功登录立即持久化新会话", async () => {
  seed("", true);
  await mountApp();
  await until(() => expect(container.textContent).toContain("登录 Krim"));
  await act(async () => buttonByText("输入访问密钥").click());
  await until(() => expect(document.getElementById("api-key")).toBeTruthy());
  override = (path, options) => {
    if (path !== "/v2/me") return undefined;
    const auth = new Headers(options.headers).get("Authorization") ?? "";
    return auth === "Bearer good-key" ? json(actorOf(true)) : json({ error: { code: "unauthorized" } }, 401);
  };
  await typeInto("api-key", "bad-key");
  await act(async () => (document.getElementById("connect-save") as HTMLButtonElement).click());
  await until(() => expect(document.body.textContent).toContain("密钥无效或账号已停用"));
  expect(calls.filter(call => call.path === "/v2/me").every(call => call.auth === "Bearer bad-key")).toBe(true);
  expect(stored()).toEqual({ key: "", loggedOut: true });
  await typeInto("api-key", "good-key");
  await act(async () => (document.getElementById("connect-save") as HTMLButtonElement).click());
  await until(() => expect(stored()).toEqual({ key: "good-key", loggedOut: false }));
  await until(() => expect(document.getElementById("api-key")).toBeNull());
  await until(() => expect(createButton().disabled).toBe(false));
  expect(calls.filter(call => call.path === "/v2/me").some(call => call.auth === "Bearer good-key")).toBe(true);
});

test("退出登录持久化登出状态，重挂后维持登出且不发送 Authorization", async () => {
  seed("saved-key", false);
  await mountApp();
  await until(() => expect(createButton().disabled).toBe(false));
  await act(async () => (document.querySelector("[aria-label='账号菜单']") as HTMLButtonElement).click());
  await until(() => expect(document.querySelector(".popover-menu[aria-label='账号菜单']")).toBeTruthy());
  await act(async () => menuItem("退出登录").click());
  expect(stored()).toEqual({ key: "", loggedOut: true });
  await until(() => expect(container.textContent).toContain("登录 Krim"));
  const before = calls.length;
  await remount();
  await until(() => expect(container.textContent).toContain("登录 Krim"));
  expect(calls.slice(before).filter(call => /^Bearer .+/.test(call.auth))).toEqual([]);
  expect(stored()).toEqual({ key: "", loggedOut: true });
});

test("已保存密钥遇 /v2/me 401 清除持久化凭据，页面不立刻切登录门，重挂保持登出", async () => {
  seed("stale-key", false);
  override = path => path === "/v2/me" ? json({ error: { code: "unauthorized" } }, 401) : undefined;
  await mountApp();
  await until(() => expect(stored()).toEqual({ key: "", loggedOut: true }));
  // 401 清缓存只落存储，App 内存不立刻翻转为登录门（避免旧请求误伤当前页面）。
  expect(calls.filter(call => call.path === "/v2/me").every(call => call.auth === "Bearer stale-key")).toBe(true);
  expect(container.textContent).not.toContain("登录 Krim");
  const before = calls.length;
  await remount();
  await until(() => expect(container.textContent).toContain("登录 Krim"));
  expect(calls.slice(before).filter(call => /^Bearer .+/.test(call.auth))).toEqual([]);
  expect(calls.slice(before).every(call => call.path !== "/v2/me")).toBe(true);
  expect(stored()).toEqual({ key: "", loggedOut: true });
});

test("旧密钥请求迟到的 401 不清除已经轮换的新密钥", async () => {
  seed("old-key", false);
  let complete!: (response: Response) => void;
  override = path => path === "/v2/me" ? new Promise<Response>(resolve => { complete = resolve; }) : undefined;
  const pending = request("/v2/me", "old-key");
  saveLoginState({ key: "rotated-key", loggedOut: false });
  complete(json({ error: { code: "unauthorized" } }, 401));
  await expect(pending).rejects.toMatchObject({ status: 401 });
  expect(stored()).toEqual({ key: "rotated-key", loggedOut: false });
});

test("网络失败保留持久化密钥，恢复后重挂仍自动登录", async () => {
  seed("saved-key", false);
  let networkDown = true;
  override = path => path === "/v2/me" && networkDown ? Promise.reject(new TypeError("identity read lost")) : undefined;
  await mountApp();
  await until(() => expect(container.textContent).toContain("读取失败，请刷新"));
  expect(stored()).toEqual({ key: "saved-key", loggedOut: false });
  expect(container.textContent).not.toContain("登录 Krim");
  networkDown = false;
  const before = calls.length;
  await remount();
  await until(() => expect(createButton().disabled).toBe(false));
  const me = calls.slice(before).filter(call => call.path === "/v2/me");
  expect(me.length).toBeGreaterThan(0);
  expect(me.every(call => call.auth === "Bearer saved-key")).toBe(true);
  expect(stored()).toEqual({ key: "saved-key", loggedOut: false });
});

test("轮换 onKeyPrepared 立即持久化新密钥，弹窗未关闭就刷新也能用新密钥", async () => {
  seed("old-key", false);
  let rotated = false;
  override = (path, options) => {
    if (path === "/v2/me") {
      const auth = new Headers(options.headers).get("Authorization") ?? "";
      if (auth === "Bearer rotated-key") return json(actorOf(false, "acct-admin"));
      if (!rotated && auth === "Bearer old-key") return json(actorOf(false, "acct-admin"));
      return json({ error: { code: "unauthorized" } }, 401);
    }
    if (path === "/v2/me/profile") return json(profileOf());
    return undefined;
  };
  await mountApp("/ui/?view=settings");
  await until(() => expect(document.getElementById("rotate-stub")).toBeTruthy());
  await act(async () => (document.getElementById("rotate-stub") as HTMLButtonElement).click());
  // onKeyPrepared 立即落盘：此时弹窗仍开着（桩不触发 onKeyChange），旧密钥已在服务端作废。
  expect(stored()).toEqual({ key: "rotated-key", loggedOut: false });
  rotated = true;
  const before = calls.length;
  await remount("/ui/?view=settings");
  await until(() => expect(document.getElementById("rotate-stub")).toBeTruthy());
  const me = calls.slice(before).filter(call => call.path === "/v2/me");
  expect(me.length).toBeGreaterThan(0);
  expect(me.every(call => call.auth === "Bearer rotated-key")).toBe(true);
  expect(stored()).toEqual({ key: "rotated-key", loggedOut: false });
});

test("存储内容损坏时按未登录冷启动，页面不崩溃且不带旧凭据", async () => {
  localStorage.setItem(LOGIN_STATE, "{{{不是合法JSON");
  await mountApp();
  await until(() => expect(createButton().disabled).toBe(false));
  expect(authLeaks()).toEqual([]);
  expect(container.textContent).not.toContain("登录 Krim");
  // 冷启动不回写存储，损坏内容保持原样直到下一次显式登录。
  expect(localStorage.getItem(LOGIN_STATE)).toBe("{{{不是合法JSON");
});

test("localStorage 读取被拒绝时按未登录冷启动，页面不崩溃", async () => {
  const realGetItem = Storage.prototype.getItem;
  vi.spyOn(Storage.prototype, "getItem").mockImplementation(function (this: Storage, key: string) {
    if (key === LOGIN_STATE) throw new DOMException("storage denied", "SecurityError");
    return realGetItem.call(this, key);
  });
  await mountApp();
  await until(() => expect(createButton().disabled).toBe(false));
  expect(authLeaks()).toEqual([]);
  expect(container.textContent).not.toContain("登录 Krim");
});

test("localStorage 写入被拒绝时登录仍可用，页面内保持已登录", async () => {
  seed("", true);
  const realSetItem = Storage.prototype.setItem;
  vi.spyOn(Storage.prototype, "setItem").mockImplementation(function (this: Storage, key: string, value: string) {
    if (key === LOGIN_STATE) throw new DOMException("quota exceeded", "QuotaExceededError");
    return realSetItem.call(this, key, value);
  });
  await mountApp();
  await until(() => expect(container.textContent).toContain("登录 Krim"));
  await act(async () => buttonByText("输入访问密钥").click());
  await until(() => expect(document.getElementById("api-key")).toBeTruthy());
  await typeInto("api-key", "good-key");
  await act(async () => (document.getElementById("connect-save") as HTMLButtonElement).click());
  await until(() => expect(createButton().disabled).toBe(false));
  expect(calls.filter(call => call.path === "/v2/me").some(call => call.auth === "Bearer good-key")).toBe(true);
  // 写入被拒：存储保持登出快照，本次页面内登录能力不受影响。
  expect(localStorage.getItem(LOGIN_STATE)).toBe(JSON.stringify({ key: "", loggedOut: true }));
});
