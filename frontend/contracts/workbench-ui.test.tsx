import { act, StrictMode } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, expect, test, vi } from "vitest";
import { App } from "../src/App";
import { webcrypto } from "node:crypto";
import { navigationObservation, navigationObservationPage } from "./observability-navigation-fixture";

const traceObservation = navigationObservation("S", "T", "run-from-trace", "Trace session");

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
    // 账号接入后的新身份链路：资料对legacy服务器返回409 account_management_unavailable，接入信息404。
    if (path === "/v2/me/profile") return json({ error: { code: "account_management_unavailable" } }, 409);
    if (path.startsWith("/v2/targets/")) return json({ error: { code: "not_found" } }, 404);
    if (path.startsWith("/v2/workspaces/local/observations?")) return json(navigationObservationPage("local", [traceObservation.item]));
    if (path === "/v2/workspaces/local/sessions/S/turns/T/observations") return json(traceObservation.detail);
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
function menuItem(label: string) {
  const item = [...document.querySelectorAll<HTMLButtonElement>("[role='menuitem']")].find(value => value.textContent?.trim() === label);
  expect(item, label).toBeTruthy();
  return item!;
}
async function clickMenuItem(label: string) {
  await until(() => expect(menuItem(label)).toBeTruthy());
  await act(async () => menuItem(label).click());
}
// 新布局：主题/全部设置都在侧栏齿轮弹出的设置 popover 中，顶栏操作收进「更多操作」菜单。
async function openSettingsPopover() {
  await act(async () => button("设置").click());
  await until(() => expect(document.querySelector(".popover-menu.settings-popover")).toBeTruthy());
}
async function openMorePopover() {
  await act(async () => (host.querySelector("header.chat-toolbar [aria-label='更多操作']") as HTMLButtonElement).click());
  await until(() => expect(document.querySelector(".popover-menu[aria-label='更多操作']")).toBeTruthy());
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
  expect(host.querySelectorAll(".desktop-sidebar .sidebar-buttons")).toHaveLength(1);
  expect(host.querySelectorAll(".sidebar-buttons [aria-label^='导航到']")).toHaveLength(5);
  expect(host.querySelector(".desktop-sidebar .app-sidebar")).toBeTruthy();
  expect(host.querySelectorAll(".chat-sidebar")).toHaveLength(1);
  expect(host.querySelector("#chat-heading")).toBeNull();
  expect(button("导航到运行记录")).toBeTruthy();
  await openSettingsPopover();
  await clickMenuItem("全部设置…");
  expect(new URL(location.href).searchParams.get("view")).toBe("settings");
  await until(() => expect(host.textContent).toContain("当前账号与工作区"));
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
  await until(() => expect(host.querySelector(".chat-toolbar .toolbar-title h1")?.textContent).toBe("会话 A"));
  expect(new URL(location.href).searchParams.get("session")).toBe("A");
});

test("快捷查找筛选当前列表并通过原选择入口打开对话", async () => {
  await mount();
  await until(() => expect(host.querySelector('[aria-label="已存对话"]')).toBeTruthy());
  await act(async () => window.dispatchEvent(new KeyboardEvent("keydown", { key: "k", ctrlKey: true, bubbles: true })));
  const input = document.querySelector<HTMLInputElement>('[aria-label="搜索当前列表"]');
  expect(input).toBeTruthy();
  await act(async () => {
    Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, "value")!.set!.call(input, "会话 B");
    input!.dispatchEvent(new Event("input", { bubbles: true }));
  });
  await until(() => expect(document.querySelectorAll(".session-search-results button")).toHaveLength(1));
  expect(document.querySelector(".session-search-results button")?.textContent).toBe("会话 B");
  await act(async () => input!.dispatchEvent(new KeyboardEvent("keydown", { key: "Enter", bubbles: true })));
  await until(() => expect(new URL(location.href).searchParams.get("session")).toBe("B"));
  await until(() => expect(document.querySelector(".session-search-dialog")).toBeNull());
  expect(calls.every(value => value.method === "GET")).toBe(true);
});

test("其他对话框打开时快捷查找不抢焦点", async () => {
  await mount();
  // 新建对话 Dialog（真实 role=dialog）打开时 Ctrl+K 不得抢焦点。
  await until(() => expect((host.querySelector(".sidebar-buttons [aria-label='新建对话']") as HTMLButtonElement)?.disabled).toBe(false));
  await act(async () => (host.querySelector(".sidebar-buttons [aria-label='新建对话']") as HTMLButtonElement).click());
  await until(() => expect(document.querySelector(".session-create-dialog")).toBeTruthy());
  await act(async () => window.dispatchEvent(new KeyboardEvent("keydown", { key: "k", ctrlKey: true, bubbles: true })));
  expect(document.querySelector(".session-create-dialog")).toBeTruthy();
  expect(document.querySelector(".session-search-dialog")).toBeNull();
  // 行为变化：旧主题 Dialog 已改为 role=menu 的设置 popover；popover 不再触发对话框守卫，Ctrl+K 仍可打开查找。
  await act(async () => [...document.querySelectorAll<HTMLButtonElement>(".session-create-dialog button")].find(item => item.textContent?.trim() === "关闭")!.click());
  await until(() => expect(document.querySelector(".session-create-dialog")).toBeNull());
  await openSettingsPopover();
  await act(async () => window.dispatchEvent(new KeyboardEvent("keydown", { key: "k", ctrlKey: true, bubbles: true })));
  await until(() => expect(document.querySelector(".session-search-dialog")).toBeTruthy());
});

test("没有历史偏好时默认浅色并显示完整主题选项", async () => {
  await mount();
  await until(() => expect(document.documentElement.dataset.theme).toBe("bright"));
  expect(localStorage.getItem("krim.display.theme/2")).toBe("bright");
  await openSettingsPopover();
  expect(document.querySelectorAll(".theme-options button")).toHaveLength(3);
});

test("三主题选择应用并持久化，旧 light 偏好迁移为浅色", async () => {
  localStorage.setItem("krim.display.theme/1", "light");
  await mount();
  await until(() => expect(document.documentElement.dataset.theme).toBe("bright"));
  expect(localStorage.getItem("krim.display.theme/2")).toBe("bright");
  await openSettingsPopover();
  expect(document.querySelector(".theme-options")).toBeTruthy();
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
  await openSettingsPopover();
  expect(document.querySelector(".theme-options")).toBeTruthy();
});

test("手机导航抽屉可打开并导航后关闭", async () => {
  Object.defineProperty(window, "innerWidth", { configurable: true, value: 375 });
  await mount();
  await act(async () => button("打开导航").click());
  await until(() => expect(document.querySelector(".mobile-drawer-content")).toBeTruthy());
  // 抽屉内是同一个 AppSidebar：导航到设置改为齿轮弹出设置 popover 后点「全部设置…」。
  const setting = document.querySelector<HTMLButtonElement>(".mobile-drawer-content [aria-label='设置']");
  expect(setting).toBeTruthy();
  await act(async () => setting!.click());
  await until(() => expect(document.querySelector(".mobile-drawer-content .popover-menu.settings-popover")).toBeTruthy());
  await act(async () => menuItem("全部设置…").click());
  expect(new URL(location.href).searchParams.get("view")).toBe("settings");
  await until(() => expect(document.querySelector(".mobile-drawer-content")).toBeNull());
  // 「打开详情面板」从常驻按钮改为顶栏「更多操作」菜单项。
  await openMorePopover();
  await clickMenuItem("打开详情面板");
  await until(() => expect(document.querySelector(".mobile-inspector-content")).toBeTruthy());
});

test("原侧栏Trace入口承载观测内容，不再追加重复入口", async () => {
  await mount();
  const icons = [...host.querySelectorAll('.sidebar-buttons button')].map(button => button.getAttribute("aria-label"));
  expect(icons).toEqual(["新建对话", "导航到对话", "导航到运行记录", "导航到Trace", "导航到数据集", "导航到评测", "查找对话"]);
  expect(host.querySelector('.sidebar-observability-entry')).toBeNull();
  expect(host.querySelector('[aria-label="导航到观测"]')).toBeNull();
  await act(async () => button("导航到Trace").click());
  expect(new URL(location.href).searchParams.get("view")).toBe("trace");
  await until(() => expect(host.querySelector('.observability-root')).toBeTruthy());
  expect(host.querySelectorAll('[aria-label="观测分析"]')).toHaveLength(1);
  expect(host.querySelectorAll('.sidebar-observe-link')).toHaveLength(7);
  expect(button("导航到Trace").querySelector('.lucide-eye')).toBeTruthy();
  expect(button("导航到Trace").getAttribute("aria-current")).toBe("page");
  expect(host.querySelector(".chat-toolbar .toolbar-title h1")?.textContent).toBe("Trace");
  expect(calls.some(value => value.path.includes("/observations?"))).toBe(true);
  expect(calls.some(value => value.path.includes("/traces?"))).toBe(false);
});

test("已有view=observe链接兼容到原Trace分区并保留子页", async () => {
  history.replaceState(null, "", "/ui/?view=observe&workspace=local&obs=status");
  await mount();
  await until(() => expect(host.querySelector('[data-obs-page="status"]')).toBeTruthy());
  expect(new URL(location.href).searchParams.get("view")).toBe("trace");
  expect(button("导航到Trace").getAttribute("aria-current")).toBe("page");
  expect(host.querySelector('[aria-label="观测页面"]')).toBeNull();
  expect(calls.every(value => value.method === "GET")).toBe(true);
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
    if (path.startsWith("/v2/workspaces/research/observations?")) return json(navigationObservationPage("research", []));
    return undefined;
  };
  history.replaceState(null, "", "/ui/?view=trace&workspace=local&session=S&turn=T&eval_id=old-eval&legacy_eval=1&product_eval_id=old-task&product_candidate_id=old-candidate&source_run=old-run");
  await mount();
  await until(() => expect(host.querySelector(".obs-run-detail h3")?.textContent).toBe("Trace session"));
  const chooser = host.querySelector<HTMLSelectElement>('select[aria-label="当前工作区"]');
  expect(chooser).toBeTruthy();
  await act(async () => {
    Object.getOwnPropertyDescriptor(HTMLSelectElement.prototype, "value")!.set!.call(chooser, "research");
    chooser!.dispatchEvent(new Event("change", { bubbles: true }));
  });
  await until(() => expect(calls.some(value => value.path.startsWith("/v2/workspaces/research/observations?"))).toBe(true));
  const url = new URL(location.href);
  expect(url.searchParams.get("view")).toBe("trace");
  expect(url.searchParams.get("workspace")).toBe("research");
  for (const field of ["session", "turn", "eval_id", "legacy_eval", "product_eval_id", "product_candidate_id", "source_run"]) expect(url.searchParams.has(field), field).toBe(false);
  expect(host.querySelector(".obs-run-detail")).toBeNull();
  expect(calls.every(value => value.method === "GET")).toBe(true);
  expect(calls.some(value => value.path.startsWith("/v1/"))).toBe(false);
});

test("更换凭据后旧侧栏与标题不穿透新身份等待期", async () => {
  let release!: () => void, identityReads = 0, nextReads = 0;
  const waiting = new Promise<void>(resolve => { release = resolve; });
  releases.push(release);
  override = (path, options) => {
    if ((options.headers as Record<string, string> | undefined)?.Authorization !== "Bearer next-synthetic") return undefined;
    if (path === "/v2/me") {
      nextReads++;
      if (nextReads === 1) {
        // 新登录流程：切换账号先 GET /v2/me 验证（立即通过），应用后再读取的请求才保持挂起。
        return json({ principal_id: "operator", generation: 1, legacy: false, workspaces: [{ workspace_id: "local", role: "admin", approve: true }] });
      }
      identityReads++;
      return waiting.then(() => json({ principal_id: "operator", generation: 1, legacy: false, workspaces: [{ workspace_id: "local", role: "admin", approve: true }] }));
    }
    if (path === "/v2/workspaces/local/sessions?limit=50") return json([{ ...sampleSession("A"), title: "新凭据对话" }]);
    if (path === "/v2/workspaces/local/sessions/A") return json({ ...sampleSession("A"), title: "新凭据对话" });
    return undefined;
  };
  history.replaceState(null, "", "/ui/?view=chat&workspace=local&session=A");
  await mount();
  await until(() => expect(host.querySelector(".chat-toolbar .toolbar-title h1")?.textContent).toBe("会话 A"));
  // 连接入口在侧栏账号菜单：账号菜单 → 「切换账号」。
  await act(async () => button("账号菜单").click());
  await clickMenuItem("切换账号");
  await act(async () => {
    const input = document.getElementById("api-key") as HTMLInputElement;
    Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, "value")!.set!.call(input, "next-synthetic");
    input.dispatchEvent(new Event("input", { bubbles: true }));
  });
  await act(async () => document.getElementById("connect-save")!.click());
  await until(() => expect(identityReads).toBeGreaterThan(0));
  expect(host.querySelector(".chat-toolbar .toolbar-title h1")?.textContent).toBe("对话");
  expect(host.querySelector('[aria-label="会话 A"]')).toBeNull();
  expect(host.querySelector('[aria-label="会话 B"]')).toBeNull();
  await act(async () => release());
  // 切换凭据会重置会话选择：新身份列表渲染其会话（新凭据对话），旧会话不穿透，标题回到未选中「对话」。
  await until(() => expect(host.textContent).toContain("新凭据对话"));
  expect(host.querySelector(".chat-toolbar .toolbar-title h1")?.textContent).toBe("对话");
  expect(host.querySelector('[aria-label="会话 A"]')).toBeNull();
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

test("重命名只在会话标题同步后关闭对话框", async () => {
  vi.stubGlobal("crypto", webcrypto);
  let serverTitle = "会话 A";
  override = (path, options) => {
    if (path === "/v2/workspaces/local/sessions?limit=50") return json([{ ...sampleSession("A"), title: serverTitle }, sampleSession("B")]);
    if (path === "/v2/workspaces/local/sessions/A") return json({ ...sampleSession("A"), title: serverTitle });
    if (path === "/v2/workspaces/local/sessions/A/rename" && (options.method ?? "GET") === "POST") {
      const body = JSON.parse(String(options.body)) as { request_key: string };
      return json({ session_id: "A", request_key: body.request_key, status: "completed" });
    }
    return undefined;
  };
  history.replaceState(null, "", "/ui/?view=chat&workspace=local&session=A");
  await mount();
  await until(() => expect(button("重命名当前对话")).toBeTruthy());
  await act(async () => button("重命名当前对话").click());
  await act(async () => {
    const input = document.getElementById("rename-session-title") as HTMLInputElement;
    Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, "value")!.set!.call(input, "更新后的名称");
    input.dispatchEvent(new Event("input", { bubbles: true }));
  });
  await act(async () => document.querySelector<HTMLButtonElement>(".session-rename-dialog button[type=submit]")!.click());
  await until(() => expect(calls.filter(value => value.path.endsWith("/sessions/A/rename") && value.method === "POST")).toHaveLength(1));
  await until(() => expect(document.querySelector(".session-rename-dialog")).toBeTruthy());
  expect(document.querySelector<HTMLButtonElement>(".session-rename-dialog button[type=submit]")?.disabled).toBe(true);
  expect(calls.filter(value => value.path.endsWith("/sessions/A/rename") && value.method === "POST")).toHaveLength(1);

  serverTitle = "更新后的名称";
  await act(async () => [...document.querySelectorAll<HTMLButtonElement>(".session-rename-dialog button")].find(item => item.textContent?.trim() === "重新读取名称")!.click());
  await until(() => expect(document.querySelector(".session-rename-dialog")).toBeNull());
  await until(() => expect(host.querySelector(".chat-toolbar .toolbar-title h1")?.textContent).toBe("更新后的名称"));
});

test("重命名结果未知时保留查询入口且不重复提交", async () => {
  vi.stubGlobal("crypto", webcrypto);
  let requestKey = "";
  override = (path, options) => {
    if (path === "/v2/workspaces/local/sessions/A/rename" && (options.method ?? "GET") === "POST") {
      requestKey = (JSON.parse(String(options.body)) as { request_key: string }).request_key;
      return json({ error: "temporary failure" }, 503);
    }
    if (path.includes("/controls/by-key/")) return json({ session_id: "A", request_key: decodeURIComponent(path.split("/").at(-1) ?? ""), status: "pending" });
    return undefined;
  };
  history.replaceState(null, "", "/ui/?view=chat&workspace=local&session=A");
  await mount();
  await until(() => expect(button("重命名当前对话")).toBeTruthy());
  await act(async () => button("重命名当前对话").click());
  await act(async () => {
    const input = document.getElementById("rename-session-title") as HTMLInputElement;
    Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, "value")!.set!.call(input, "待确认名称");
    input.dispatchEvent(new Event("input", { bubbles: true }));
  });
  await act(async () => document.querySelector<HTMLButtonElement>(".session-rename-dialog button[type=submit]")!.click());
  await until(() => expect(document.querySelector(".session-rename-dialog")?.textContent).toContain("结果尚未确认"));
  expect(requestKey).not.toBe("");
  const lookup = [...document.querySelectorAll<HTMLButtonElement>(".session-rename-dialog button")].find(item => item.textContent?.trim() === "查找操作结果");
  expect(lookup).toBeTruthy();
  await act(async () => lookup!.click());
  await until(() => expect(calls.some(value => value.path.includes("/controls/by-key/") && value.method === "GET")).toBe(true));
  expect(calls.filter(value => value.path.endsWith("/sessions/A/rename") && value.method === "POST")).toHaveLength(1);
  expect(document.querySelector(".session-rename-dialog")).toBeTruthy();
});
