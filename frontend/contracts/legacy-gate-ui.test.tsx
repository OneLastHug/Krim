import { act, StrictMode } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, expect, test, vi } from "vitest";
import { App } from "../src/App";
import { CURRENT, INTENT } from "../src/lib/run-requests";

let root: Root, host: HTMLDivElement;
let handler: (path: string, options: RequestInit) => Response | Promise<Response>;
const calls: { path: string; method: string; key: string }[] = [];
const releases: (() => void)[] = [];
const json = (body: unknown, status = 200) => new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });
const actor = (legacy: boolean) => ({ principal_id: "operator", generation: 1, legacy, workspaces: [{ workspace_id: "local", role: "admin", approve: true }] });
const run = { id: "saved-run", request_key: "saved-key", status: "completed", trace_id: "saved-trace", root_span_id: "saved-span", created_at: 1, finished_at: 2, code: null, stop_requested: false };
function response(path: string) {
  if (path === "/healthz") return json({ http_enabled: false, product_eval_enabled: false });
  if (path === "/v2/me") return json(actor(true));
  if (path === "/v1/runs/saved-run") return json(run);
  if (path === "/v1/runs/saved-run/result") return json({ run, text: "原身份私有结果", tools: [] });
  if (path === "/v1/runs/saved-run/trace") return json({ run_id: run.id, trace_id: run.trace_id, complete: true, reasons: [], spans: [] });
  if (path.startsWith("/v2/workspaces/local/traces?")) return json({ items: [], next_cursor: null });
  return json([]);
}
function hold() {
  let release!: () => void;
  const promise = new Promise<void>(resolve => { release = resolve; });
  releases.push(release);
  return { promise, release };
}
beforeEach(() => {
  localStorage.clear(); history.replaceState(null, "", "/ui/?view=run");
  localStorage.setItem(CURRENT, run.id);
  calls.length = 0;
  host = document.createElement("div"); document.body.append(host); root = createRoot(host);
  (globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean }).IS_REACT_ACT_ENVIRONMENT = true;
  handler = path => response(path);
  vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, options: RequestInit = {}) => {
    const path = String(input);
    calls.push({ path, method: options.method ?? "GET", key: new Headers(options.headers).get("Authorization") ?? "" });
    return handler(path, options);
  }));
});
afterEach(async () => {
  await act(async () => root.unmount());
  await act(async () => { releases.splice(0).forEach(release => release()); await Promise.resolve(); });
  host.remove(); vi.unstubAllGlobals();
});
async function mount() { await act(async () => root.render(<StrictMode><App /></StrictMode>)); }
async function until(check: () => void) {
  let error: unknown;
  for (let attempt = 0; attempt < 80; attempt++) {
    try { check(); return; } catch (value) { error = value; }
    await act(async () => { await new Promise(resolve => setTimeout(resolve, 0)); });
  }
  throw error;
}
async function connect(key: string) {
  const button = [...host.querySelectorAll<HTMLButtonElement>("button")].find(value => value.textContent?.trim() === "连接");
  expect(button).toBeTruthy();
  await act(async () => button!.click());
  await act(async () => {
    const input = document.getElementById("api-key") as HTMLInputElement;
    Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, "value")!.set!.call(input, key);
    input.dispatchEvent(new Event("input", { bubbles: true }));
  });
  await act(async () => document.getElementById("connect-save")!.click());
}

test("保留旧run时等待明确legacy能力才读取v1，零自动POST", async () => {
  const identity = hold();
  handler = path => path === "/v2/me" ? identity.promise.then(() => json(actor(true))) : response(path);
  await mount();
  expect(calls.some(value => value.path.startsWith("/v1/"))).toBe(false);
  expect(host.querySelector("#prompt")).toBeNull();
  await act(async () => identity.release());
  await until(() => expect(host.querySelector("#result")?.textContent).toBe("原身份私有结果"));
  expect(calls.some(value => value.path === "/v1/runs/saved-run")).toBe(true);
  expect(calls.every(value => value.method === "GET")).toBe(true);
});

for (const view of ["run", "chat", "trace", "eval"] as const) {
  test(`非legacy身份保留旧run进入${view}也不请求v1`, async () => {
    history.replaceState(null, "", `/ui/?view=${view}&source_run=unread-source`);
    handler = path => path === "/v2/me" ? json(actor(false)) : response(path);
    await mount();
    await until(() => expect(host.textContent).toContain("已连接"));
    expect(calls.some(value => value.path.startsWith("/v1/"))).toBe(false);
    expect(host.textContent).not.toContain("原身份私有结果");
    expect(localStorage.getItem(CURRENT)).toBe(run.id);
  });
}

for (const status of [401, 503]) {
  test(`身份读取${status}失败不降级v1，未知原请求保留`, async () => {
    const intent = JSON.stringify({ text: "保留输入", request_key: "original-intent", target: "demo" });
    localStorage.setItem(INTENT, intent);
    handler = path => path === "/v2/me" ? json({ error: { code: status === 401 ? "unauthorized" : "execution_error" } }, status) : response(path);
    await mount();
    await until(() => expect(host.textContent).toContain("连接失败"));
    expect(host.querySelector("#prompt")).toBeNull();
    expect(calls.some(value => value.path.startsWith("/v1/"))).toBe(false);
    expect(localStorage.getItem(INTENT)).toBe(intent);
  });
}

test("legacy身份在对话页不后台读取旧run，显式进入运行页后才读取", async () => {
  history.replaceState(null, "", "/ui/?view=chat");
  await mount();
  await until(() => expect(host.textContent).toContain("已连接"));
  expect(calls.some(value => value.path.startsWith("/v1/runs/"))).toBe(false);
  await act(async () => host.querySelector<HTMLButtonElement>('[aria-label="导航到运行记录"]')!.click());
  await until(() => expect(host.querySelector("#result")?.textContent).toBe("原身份私有结果"));
});

test("运行中的轮询离开运行页即停止，旧结果不进入设置页", async () => {
  handler = path => path === "/v1/runs/saved-run" ? json({ ...run, status: "running", finished_at: null }) : response(path);
  await mount();
  await until(() => expect(host.querySelector("#status")?.textContent).toBe("执行中"));
  await act(async () => host.querySelector<HTMLButtonElement>('[aria-label="导航到设置"]')!.click());
  const count = calls.filter(value => value.path.startsWith("/v1/runs/")).length;
  await act(async () => { await new Promise(resolve => setTimeout(resolve, 180)); });
  expect(calls.filter(value => value.path.startsWith("/v1/runs/"))).toHaveLength(count);
  expect(host.querySelector("#result")).toBeNull();
});

test("离页后创建回程保留原intent且不再自动GET确认", async () => {
  const accepted = hold();
  let submitted: Record<string, unknown> | undefined;
  handler = (path, options) => {
    if (path === "/v1/runs" && options.method === "POST") {
      submitted = JSON.parse(String(options.body)) as Record<string, unknown>;
      return accepted.promise.then(() => json({ ...run, request_key: submitted!.request_key }));
    }
    return response(path);
  };
  await mount();
  await until(() => expect(host.querySelector("#prompt")).toBeTruthy());
  await act(async () => {
    const input = host.querySelector<HTMLTextAreaElement>("#prompt")!;
    Object.getOwnPropertyDescriptor(HTMLTextAreaElement.prototype, "value")!.set!.call(input, "保留离页前输入");
    input.dispatchEvent(new Event("input", { bubbles: true }));
  });
  await act(async () => host.querySelector<HTMLButtonElement>("#send")!.click());
  await until(() => expect(submitted).toBeTruthy());
  const original = localStorage.getItem(INTENT);
  await act(async () => host.querySelector<HTMLButtonElement>('[aria-label="导航到设置"]')!.click());
  const count = calls.filter(value => value.path.startsWith("/v1/")).length;
  await act(async () => { accepted.release(); await Promise.resolve(); });
  expect(calls.filter(value => value.path.startsWith("/v1/"))).toHaveLength(count);
  expect(localStorage.getItem(INTENT)).toBe(original);
  expect(calls.filter(value => value.method === "POST")).toHaveLength(1);
});

test("更换为非legacy凭据后中止旧读取，迟到结果不回填也不发新v1请求", async () => {
  const result = hold();
  let oldSignal: AbortSignal | null | undefined;
  handler = (path, options) => {
    const member = new Headers(options.headers).get("Authorization") === "Bearer member-synthetic";
    if (path === "/v2/me") return json(actor(!member));
    if (path === "/v1/runs/saved-run/result" && !member) {
      oldSignal = options.signal;
      return result.promise.then(() => response(path));
    }
    return response(path);
  };
  await mount();
  await until(() => expect(oldSignal).toBeTruthy());
  await connect("member-synthetic");
  await until(() => expect(host.textContent).toContain("运行记录暂不可用"));
  expect(oldSignal?.aborted).toBe(true);
  await act(async () => result.release());
  expect(host.textContent).not.toContain("原身份私有结果");
  expect(calls.some(value => value.path.startsWith("/v1/") && value.key === "Bearer member-synthetic")).toBe(false);
  expect(calls.every(value => value.method === "GET")).toBe(true);
});
