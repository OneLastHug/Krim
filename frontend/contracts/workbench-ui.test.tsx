import { act, StrictMode } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, expect, test, vi } from "vitest";
import { App } from "../src/App";
import { webcrypto } from "node:crypto";

let root: Root, host: HTMLDivElement;
let legacyIdentity = true;
let productEnabled = false;
let override: ((path: string, options: RequestInit) => Response | Promise<Response> | undefined) | undefined;
const releases: (() => void)[] = [];
const calls: { path: string; method: string }[] = [];
const json = (value: unknown, status = 200) => new Response(JSON.stringify(value), { status, headers: { "Content-Type": "application/json" } });
function sampleSession(id: string) {
  return { id, workspace_id: "local", owner_id: "operator", create_key: "create-" + id, title: `会话 ${id}`, target_key: "demo", target_fingerprint: "0".repeat(64), context_policy: { max_bytes: 131072, max_messages: 256, reserved_output_tokens: 2048, counter_version: "approx-utf8/1" }, revision: 0, status: "open", dispatch_paused: false, created_at: 1, closed_at: null };
}
beforeEach(() => {
  Object.defineProperty(window, "innerWidth", { configurable: true, value: 1024 });
  localStorage.clear();
  history.replaceState(null, "", "/ui/");
  legacyIdentity = true; productEnabled = false; override = undefined;
  calls.length = 0;
  host = document.createElement("div");
  document.body.append(host);
  root = createRoot(host);
  (globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean }).IS_REACT_ACT_ENVIRONMENT = true;
  vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, options: RequestInit = {}) => {
    const path = String(input), method = options.method ?? "GET";
    calls.push({ path, method });
    const overridden = override?.(path, options);
    if (overridden !== undefined) return overridden;
    if (path === "/healthz") return json({ http_enabled: false, product_eval_enabled: productEnabled, evaluation_automation_enabled: productEnabled });
    if (path === "/v2/me") return json({ principal_id: "operator", generation: 1, legacy: legacyIdentity, workspaces: [{ workspace_id: "local", role: "admin", approve: true }] });
    if (path.startsWith("/v2/workspaces/local/traces?")) return json({ items: [{ source_kind: "session_turn", run_id: "run-from-trace", trace_id: "trace-from-trace", session_id: "S", turn_id: "T", session_title: "Trace session", target_key: "demo", status: "completed", created_at: 1, finished_at: 2 }], next_cursor: null });
    if (path === "/v2/workspaces/local/sessions/S/turns/T/trace") return json({ session_id: "S", turn_id: "T", trace: { run_id: "run-from-trace", trace_id: "trace-from-trace", complete: true, reasons: [], spans: [] } });
    if (path === "/v2/workspaces/local/sessions?limit=50") return json([sampleSession("A"), sampleSession("B")]);
    if (path.includes("/turns?offset=")) return json([]);
    if (path === "/v2/workspaces/local/sessions/A" || path === "/v2/workspaces/local/sessions/B") return json(sampleSession(path.endsWith("/A") ? "A" : "B"));
    return json([]);
  }));
});
afterEach(async () => {
  await act(async () => root.unmount());
  await act(async () => { for (const release of releases.splice(0)) release(); await Promise.resolve(); });
  host.remove();
  vi.unstubAllGlobals();
});
async function mount() { await act(async () => root.render(<StrictMode><App /></StrictMode>)); }
async function until(check: () => void) {
  let error: unknown;
  for (let i = 0; i < 70; i++) {
    try { check(); return; } catch (value) { error = value; }
    await act(async () => { await new Promise(resolve => setTimeout(resolve, 0)); });
  }
  throw error;
}
function button(label: string) {
  const found = [...document.querySelectorAll("button")].find(item => item.getAttribute("aria-label") === label);
  expect(found, label).toBeTruthy();
  return found!;
}

test("评测与自动化共存时切换页面不复制工作台或复用组件身份", async () => {
  productEnabled = true;
  vi.stubGlobal("crypto", webcrypto);
  history.replaceState(null, "", "/ui/?view=datasets&workspace=local");
  await mount();
  await until(() => expect(host.querySelector("#product-dataset-name")).toBeTruthy());
  for (let attempt = 0; attempt < 2; attempt++) {
    await act(async () => button("导航到评测").click());
    await until(() => expect(host.querySelector("#product-target")).toBeTruthy());
    expect(host.querySelectorAll(".product-eval-workbench")).toHaveLength(1);
    expect(host.querySelectorAll("#product-target")).toHaveLength(1);
    await act(async () => button("导航到数据集").click());
    await until(() => expect(host.querySelector("#product-dataset-name")).toBeTruthy());
    expect(host.querySelectorAll(".product-eval-workbench")).toHaveLength(1);
  }
  expect(calls.every(value => value.method === "GET")).toBe(true);
});


test("首页默认对话，主导航和浏览器回退同步页面", async () => {
  await mount();
  await until(() => expect(host.querySelector('[aria-label="对话工作台"]')).toBeTruthy());
  expect(new URL(location.href).searchParams.get("view")).toBeNull();
  await until(() => expect(host.querySelector('[aria-label="已存对话"]')).toBeTruthy());
  expect(host.querySelectorAll(".primary-navigation")).toHaveLength(1);
  expect(host.querySelectorAll(".chat-sidebar")).toHaveLength(1);
  expect(host.querySelector("#chat-heading")).toBeNull();
  expect(button("导航到运行记录")).toBeTruthy();
  await act(async () => button("导航到设置").click());
  expect(new URL(location.href).searchParams.get("view")).toBe("settings");
  await until(() => expect(host.textContent).toContain("当前身份与工作区"));
  history.replaceState(null, "", "/ui/");
  await act(async () => window.dispatchEvent(new PopStateEvent("popstate")));
  await until(() => expect(host.querySelector('[aria-label="对话工作台"]')).toBeTruthy());
  await until(() => expect(host.querySelector('[aria-label="已存对话"]')).toBeTruthy());
  expect(document.getElementById("chat-prompt")).toBeTruthy();
});

test("会话对象选择写入可回退位置并从旧 query 恢复", async () => {
  history.replaceState(null, "", "/ui/?view=chat&workspace=local&session=A");
  await mount();
  await until(() => expect([...document.querySelectorAll("button")].some(item => item.textContent?.includes("会话 B"))).toBe(true));
  const sessionB = [...document.querySelectorAll("button")].find(item => item.textContent?.includes("会话 B"))!;
  await act(async () => sessionB.click());
  expect(new URL(location.href).searchParams.get("session")).toBe("B");
  history.replaceState(null, "", "/ui/?view=chat&workspace=local&session=A");
  await act(async () => window.dispatchEvent(new PopStateEvent("popstate")));
  await until(() => expect(host.querySelector(".topbar-title h1")?.textContent).toBe("会话 A"));
  expect(new URL(location.href).searchParams.get("session")).toBe("A");
});

test("没有历史偏好时默认暖纸并显示完整主题选项", async () => {
  await mount();
  await until(() => expect(document.documentElement.dataset.theme).toBe("warm"));
  expect(localStorage.getItem("krim.display.theme/2")).toBe("warm");
  await act(async () => button("选择显示主题").click());
  expect(document.querySelectorAll(".theme-options button")).toHaveLength(3);
});

test("三主题选择应用并持久化，旧 light 偏好迁移为亮彩", async () => {
  localStorage.setItem("krim.display.theme/1", "light");
  await mount();
  await until(() => expect(document.documentElement.dataset.theme).toBe("bright"));
  expect(localStorage.getItem("krim.display.theme/2")).toBe("bright");
  expect(button("选择显示主题")).toBeTruthy();
  await act(async () => button("选择显示主题").click());
  const dark = [...document.querySelectorAll<HTMLButtonElement>(".theme-options button")].find(item => item.textContent?.includes("深色"));
  expect(dark).toBeTruthy();
  await act(async () => dark!.click());
  expect(document.documentElement.dataset.theme).toBe("dark");
  expect(document.documentElement.classList.contains("dark")).toBe(true);
  expect(localStorage.getItem("krim.display.theme/2")).toBe("dark");
  expect(localStorage.getItem("krim.display.theme/1")).toBe("dark");
  await act(async () => root.unmount());
  root = createRoot(host);
  await mount();
  expect(document.documentElement.dataset.theme).toBe("dark");
  expect(button("选择显示主题")).toBeTruthy();
});

test("手机导航抽屉可打开并导航后关闭", async () => {
  Object.defineProperty(window, "innerWidth", { configurable: true, value: 375 });
  await mount();
  await act(async () => button("打开导航").click());
  await until(() => expect(document.querySelector(".mobile-drawer-content")).toBeTruthy());
  const setting = document.querySelector<HTMLButtonElement>(".mobile-drawer-content [aria-label='导航到设置']");
  expect(setting).toBeTruthy();
  await act(async () => setting!.click());
  expect(new URL(location.href).searchParams.get("view")).toBe("settings");
  await until(() => expect(document.querySelector(".mobile-drawer-content")).toBeNull());
  await act(async () => button("打开详情面板").click());
  await until(() => expect(document.querySelector(".mobile-inspector-content")).toBeTruthy());
});

test("侧栏Trace入口打开独立运行索引路由", async () => {
  await mount();
  await act(async () => button("导航到Trace").click());
  expect(new URL(location.href).searchParams.get("view")).toBe("trace");
  await until(() => expect(host.querySelector(".topbar-title h1")?.textContent).toBe("Trace"));
});

test("Trace加入数据集入口导航传递原始run且不创建请求", async () => {
  productEnabled = true;
  history.replaceState(null, "", "/ui/?view=trace&workspace=local&session=S&turn=T");
  await mount();
  const addToDataset = () => [...host.querySelectorAll<HTMLButtonElement>("button")].find(item => item.textContent?.trim() === "加入数据集");
  await until(() => expect(addToDataset()).toBeTruthy());
  await act(async () => addToDataset()!.click());
  await until(() => expect(new URL(location.href).searchParams.get("view")).toBe("datasets"));
  expect(new URL(location.href).searchParams.get("source_run")).toBe("run-from-trace");
  expect(calls.every(value => value.method === "GET")).toBe(true);
});

test("工作台切换工作区清空来源和对象深链且只读加载新索引", async () => {
  productEnabled = true;
  const memberships = [{ workspace_id: "local", role: "admin", approve: true }, { workspace_id: "research", role: "viewer", approve: false }];
  override = path => {
    if (path === "/v2/me") return json({ principal_id: "operator", generation: 1, legacy: false, workspaces: memberships });
    if (path.startsWith("/v2/workspaces/research/traces?")) return json({ items: [], next_cursor: null });
    return undefined;
  };
  history.replaceState(null, "", "/ui/?view=trace&workspace=local&session=S&turn=T&eval_id=old-eval&legacy_eval=1&product_eval_id=old-task&product_candidate_id=old-candidate&source_run=old-run");
  await mount();
  await until(() => expect(host.querySelector("#trace-detail-heading")?.textContent).toBe("Trace session"));
  const chooser = host.querySelector<HTMLSelectElement>('select[aria-label="当前工作区"]');
  expect(chooser).toBeTruthy();
  await act(async () => {
    Object.getOwnPropertyDescriptor(HTMLSelectElement.prototype, "value")!.set!.call(chooser, "research");
    chooser!.dispatchEvent(new Event("change", { bubbles: true }));
  });
  await until(() => expect(calls.some(value => value.path.startsWith("/v2/workspaces/research/traces?"))).toBe(true));
  const url = new URL(location.href);
  expect(url.searchParams.get("view")).toBe("trace");
  expect(url.searchParams.get("workspace")).toBe("research");
  for (const field of ["session", "turn", "eval_id", "legacy_eval", "product_eval_id", "product_candidate_id", "source_run"]) expect(url.searchParams.has(field), field).toBe(false);
  expect(host.querySelector("#trace-detail-heading")).toBeNull();
  expect(calls.every(value => value.method === "GET")).toBe(true);
  expect(calls.some(value => value.path.startsWith("/v1/"))).toBe(false);
});

test("更换凭据后旧侧栏与标题不穿透新身份等待期", async () => {
  let release!: () => void, identityReads = 0;
  const waiting = new Promise<void>(resolve => { release = resolve; });
  releases.push(release);
  override = (path, options) => {
    if ((options.headers as Record<string, string> | undefined)?.Authorization !== "Bearer next-synthetic") return undefined;
    if (path === "/v2/me") {
      identityReads++;
      return waiting.then(() => json({ principal_id: "operator", generation: 1, legacy: false, workspaces: [{ workspace_id: "local", role: "admin", approve: true }] }));
    }
    if (path === "/v2/workspaces/local/sessions?limit=50") return json([{ ...sampleSession("A"), title: "新凭据对话" }]);
    if (path === "/v2/workspaces/local/sessions/A") return json({ ...sampleSession("A"), title: "新凭据对话" });
    return undefined;
  };
  history.replaceState(null, "", "/ui/?view=chat&workspace=local&session=A");
  await mount();
  await until(() => expect(host.querySelector(".topbar-title h1")?.textContent).toBe("会话 A"));
  const connect = [...host.querySelectorAll<HTMLButtonElement>("button")].find(value => value.textContent?.trim() === "连接");
  expect(connect).toBeTruthy();
  await act(async () => connect!.click());
  await act(async () => {
    const input = document.getElementById("api-key") as HTMLInputElement;
    Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, "value")!.set!.call(input, "next-synthetic");
    input.dispatchEvent(new Event("input", { bubbles: true }));
  });
  await act(async () => document.getElementById("connect-save")!.click());
  await until(() => expect(identityReads).toBeGreaterThan(0));
  expect(host.querySelector(".topbar-title h1")?.textContent).toBe("对话");
  expect(host.querySelector('[aria-label="会话 A"]')).toBeNull();
  expect(host.querySelector('[aria-label="会话 B"]')).toBeNull();
  await act(async () => release());
  await until(() => expect(host.querySelector(".topbar-title h1")?.textContent).toBe("新凭据对话"));
  expect(host.querySelector('[aria-label="会话 B"]')).toBeNull();
  expect(calls.every(value => value.method === "GET")).toBe(true);
});

test("非 legacy 身份不挂载旧版运行与评测入口", async () => {
  legacyIdentity = false;
  history.replaceState(null, "", "/ui/?view=eval");
  await mount();
  await until(() => expect(host.textContent).toContain("中性评测页面尚未接入"));
  expect(host.textContent).toContain("旧版评测能力不可用");
  expect(host.querySelector("#eval-start")).toBeNull();
  expect(calls.some(item => item.path.startsWith("/v1/eval"))).toBe(false);
  expect(calls.some(item => item.path.startsWith("/v1/runs"))).toBe(false);
});
