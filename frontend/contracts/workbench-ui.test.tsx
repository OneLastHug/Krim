import { act, StrictMode } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, expect, test, vi } from "vitest";
import { App } from "../src/App";
import { webcrypto } from "node:crypto";

let root: Root, host: HTMLDivElement;
let legacyIdentity = true;
let productEnabled = false;
const calls: { path: string; method: string }[] = [];
const json = (value: unknown, status = 200) => new Response(JSON.stringify(value), { status, headers: { "Content-Type": "application/json" } });
function sampleSession(id: string) {
  return { id, workspace_id: "local", owner_id: "operator", create_key: "create-" + id, title: `会话 ${id}`, target_key: "demo", target_fingerprint: "0".repeat(64), context_policy: { max_bytes: 131072, max_messages: 256, reserved_output_tokens: 2048, counter_version: "approx-utf8/1" }, revision: 0, status: "open", dispatch_paused: false, created_at: 1, closed_at: null };
}
beforeEach(() => {
  Object.defineProperty(window, "innerWidth", { configurable: true, value: 1024 });
  localStorage.clear();
  history.replaceState(null, "", "/ui/");
  legacyIdentity = true; productEnabled = false;
  calls.length = 0;
  host = document.createElement("div");
  document.body.append(host);
  root = createRoot(host);
  (globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean }).IS_REACT_ACT_ENVIRONMENT = true;
  vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, options: RequestInit = {}) => {
    const path = String(input), method = options.method ?? "GET";
    calls.push({ path, method });
    if (path === "/healthz") return json({ http_enabled: false, product_eval_enabled: productEnabled, evaluation_automation_enabled: productEnabled });
    if (path === "/v2/me") return json({ principal_id: "operator", generation: 1, legacy: legacyIdentity, workspaces: [{ workspace_id: "local", role: "admin", approve: true }] });
    if (path === "/v2/workspaces/local/sessions?limit=50") return json([sampleSession("A"), sampleSession("B")]);
    if (path.includes("/turns?offset=")) return json([]);
    if (path === "/v2/workspaces/local/sessions/A" || path === "/v2/workspaces/local/sessions/B") return json(sampleSession(path.endsWith("/A") ? "A" : "B"));
    return json([]);
  }));
});
afterEach(async () => {
  await act(async () => root.unmount());
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
  expect(button("导航到运行记录")).toBeTruthy();
  await act(async () => button("导航到设置").click());
  expect(new URL(location.href).searchParams.get("view")).toBe("settings");
  await until(() => expect(host.textContent).toContain("当前身份与工作区"));
  history.replaceState(null, "", "/ui/");
  await act(async () => window.dispatchEvent(new PopStateEvent("popstate")));
  await until(() => expect(host.querySelector('[aria-label="对话工作台"]')).toBeTruthy());
  expect(host.querySelector('[aria-label="已存对话"]')).toBeTruthy();
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
  await until(() => expect(document.getElementById("chat-heading")?.textContent).toBe("会话 A"));
  expect(new URL(location.href).searchParams.get("session")).toBe("A");
});

test("主题切换应用到页面并写入持久偏好", async () => {
  await mount();
  const switchTheme = button("切换到深色主题");
  await act(async () => switchTheme.click());
  expect(document.documentElement.classList.contains("dark")).toBe(true);
  expect(localStorage.getItem("krim.display.theme/1")).toBe("dark");
  await act(async () => root.unmount());
  root = createRoot(host);
  await mount();
  expect(document.documentElement.dataset.theme).toBe("dark");
  expect(button("切换到浅色主题")).toBeTruthy();
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
