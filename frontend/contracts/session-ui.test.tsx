import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { StrictMode } from "react";
import { afterEach, beforeEach, expect, test, vi } from "vitest";
import { App } from "../src/App";
import { INTENTS, KNOWN, readIntents } from "../src/lib/session-requests";

let root: Root, container: HTMLDivElement;
let override: ((path: string, options: RequestInit) => Response | Promise<Response> | undefined) | undefined;
const releases: (() => void)[] = [];
const calls: { path: string; method: string; body: unknown }[] = [];
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
    if (path.includes("/turns")) return json([]);
    if (method === "POST" && path.endsWith("/sessions")) return json(session("A", (body as { request_key: string }).request_key), 201);
    if (path.endsWith("/sessions/A")) return json(session());
    return json([]);
  }));
});
afterEach(async () => { await act(async () => root.unmount()); await act(async () => { for (const release of releases.splice(0)) release(); await Promise.resolve(); }); container.remove(); vi.unstubAllGlobals(); });
async function until(check: () => void) { let error: unknown; for (let i = 0; i < 60; i++) { try { check(); return; } catch (value) { error = value; await act(async () => { await new Promise(r => setTimeout(r, 0)); }); } } throw error; }
function button(label: string) { const item = [...document.querySelectorAll("button")].find(b => b.textContent?.trim() === label); expect(item, label).toBeTruthy(); return item!; }

test("真实App StrictMode对话入口零自动POST，新建仅由显式操作", async () => {
  await act(async () => root.render(<StrictMode><App /></StrictMode>));
  expect(calls.filter(c => c.method === "POST")).toEqual([]);
  await act(async () => button("对话").click());
  await until(() => expect(button("新建对话")).toBeTruthy());
  expect(calls.filter(c => c.method === "POST")).toEqual([]);
  await act(async () => button("新建对话").click());
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
  await until(() => expect(button("新建对话").disabled).toBe(false));
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
function turn(body: { text: string; request_key: string; expected_revision: number }) {
  return { id: "A1", session_id: "A", workspace_id: "local", owner_id: "local", ordinal: 1, request_key: body.request_key, text: body.text, accepted_revision: body.expected_revision, status: "completed", run_id: "run-A1", context_fingerprint: "1".repeat(64), transcript: [{ role: "user", content: body.text, call_id: null, tool_calls: [] }, { role: "assistant", content: "已完成", call_id: null, tool_calls: [] }], code: null, created_at: 1, started_at: 1, finished_at: 2 };
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

for (const barrier of ["post", "get"] as const) for (const steering of ["tab", "other", "key", "refresh", "remount"] as const) {
  test(`${barrier}成功回程遇${steering}保留完整intent，显式GET找回且仅一POST`, async () => {
    const fixture = server(barrier); await mountExisting(); await type("chat-prompt", "原始消息不许替换");
    await act(async () => button("发送").click());
    await until(() => expect(fixture.accepted).not.toBeNull());
    const original = JSON.parse(JSON.stringify(readIntents(localStorage)[0])) as unknown;
    if (barrier === "get") { await act(async () => button("查找原请求").click()); await until(() => expect(fixture.findCount).toBe(1)); }
    if (steering === "tab") await act(async () => button("任务").click());
    if (steering === "other") await act(async () => button("会话 B").click());
    if (steering === "key") { await act(async () => button("连接").click()); await type("api-key", "corrected-synthetic"); await act(async () => document.getElementById("connect-save")!.click()); }
    if (steering === "refresh") await act(async () => (document.querySelector('[aria-label="刷新对话"]') as HTMLButtonElement).click());
    if (steering === "remount") { await act(async () => root.unmount()); root = createRoot(container); await act(async () => root.render(<StrictMode><App /></StrictMode>)); }
    await act(async () => { fixture.hold.release(); await new Promise(r => setTimeout(r, 5)); });
    expect(readIntents(localStorage)).toEqual([original]);
    expect(localStorage.getItem(KNOWN)).toBeNull();
    if (steering === "other") expect(new URL(location.href).searchParams.get("session")).toBe("B");
    expect(calls.filter(c => c.method === "POST")).toHaveLength(1);
    if (steering === "tab") await act(async () => button("对话").click());
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
