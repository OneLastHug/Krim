import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, expect, test, vi } from "vitest";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { DevelopmentPanel } from "../src/components/DevelopmentPanel";
import type { SessionState } from "../src/hooks/use-session";

// request 按路径分发；RequestError 与 object 保持与真实 api.ts 相同的形状。
vi.mock("../src/api", () => {
  class RequestError extends Error {
    readonly status: number;
    readonly code: string;
    constructor(status: number, code: string) {
      super("请求未完成：" + code);
      this.status = status;
      this.code = code;
    }
  }
  function object(value: unknown): Record<string, unknown> {
    if (typeof value !== "object" || value === null || Array.isArray(value)) throw new Error("响应结构无效");
    return value as Record<string, unknown>;
  }
  return {
    RequestError,
    object,
    request: vi.fn(),
    parseRun: vi.fn(),
    isUnadmittedCreate: vi.fn(),
  };
});
const { request, RequestError } = await import("../src/api");
const readApi = vi.mocked(request);

const bindingUnbound = { session_id: "session-A", project_id: null, revision: 0, session_revision: 1, workspace_id: "local" };
const bindingBound = { session_id: "session-A", project_id: "proj-9", revision: 2, session_revision: 5, workspace_id: "local" };
const project = {
  project_id: "proj-9", environment_id: "env-9", principal_id: "operator", workspace_id: "local",
  title: "图片服务项目", generation: 1, status: "ready", created_at: 1700000000, updated_at: 1700000100,
};
const environment = { owner_id: "operator", project_id: "proj-9", environment_id: "dev-proj-9", generation: 1, state: "ready" };
const tools = [
  {
    name: "docs.search_pages", version: "1", risk: "read",
    input_schema: {}, output_schema: {}, fingerprint: "c".repeat(64),
    source: { kind: "mcp", server: "docs", tool: "search_pages", configuration_digest: "a".repeat(64) },
  },
  { name: "dev_read", version: "1", risk: "read", input_schema: {}, output_schema: {}, fingerprint: "d".repeat(64) },
];
const connections = [{
  id: "conn-1", name: "docs", url: "https://docs.example.com/mcp", allowed_hosts: ["docs.example.com"],
  selected_tools: ["search_pages"], readonly_tools: ["search_pages"], timeout_seconds: 10,
  revision: 2, credential_revision: 1, credential_configured: true,
}];
const projectsPage = { items: [project], total: 1, offset: 0, limit: 100 };

let root: Root;
let host: HTMLDivElement;
let mounted = false;
let previousWidth: number;

function fakeChat(overrides: Record<string, unknown> = {}, sessionOverrides: Record<string, unknown> = {}) {
  return {
    actor: {
      principal_id: "operator", generation: 3, legacy: false,
      workspaces: [{ workspace_id: "local", role: "admin", approve: true }],
      target_keys: ["target-development"],
    },
    ready: true, canWrite: true, canApprove: true, refresh: vi.fn(),
    workspace: "local", id: "session-A", list: [],
    session: {
      id: "session-A", workspace_id: "local", owner_id: "operator", title: "图片服务",
      target_key: "target-development", status: "open", revision: 5,
      context_policy: { max_bytes: 900000, max_messages: 200, reserved_output_tokens: 33000, counter_version: "approx-utf8/1" },
      ...sessionOverrides,
    },
    ...overrides,
  } as unknown as SessionState;
}

const baseProps = { chat: fakeChat(), apiKey: "synthetic-key-A", active: true, fence: { current: 0 } };

function route(path: string, mapping: Record<string, unknown>) {
  const exact = mapping[path];
  if (exact !== undefined) return Promise.resolve(exact);
  return Promise.reject(new Error("意外请求：" + path));
}

async function render(props = baseProps) {
  await act(async () => {
    root.render(<DevelopmentPanel {...props} />);
    for (let i = 0; i < 16; i++) await Promise.resolve();
  });
}

async function flush(times = 16) {
  await act(async () => {
    for (let i = 0; i < times; i++) await Promise.resolve();
  });
}

function typeInput(element: HTMLInputElement, value: string) {
  Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, "value")!.set!.call(element, value);
  element.dispatchEvent(new Event("input", { bubbles: true }));
}

function dialogElement() {
  return document.querySelector('[role="dialog"]') as HTMLElement;
}

function findButton(scope: HTMLElement, label: string) {
  const button = [...scope.querySelectorAll("button")].find(item => item.textContent === label);
  expect(button, "按钮未找到：" + label).toBeTruthy();
  return button as HTMLButtonElement;
}

function findButtonByLabel(scope: HTMLElement, label: string) {
  const button = scope.querySelector(`button[aria-label="${label}"]`);
  expect(button, "按钮未找到：" + label).toBeTruthy();
  return button as HTMLButtonElement;
}

beforeEach(() => {
  readApi.mockReset();
  localStorage.clear();
  host = document.createElement("div");
  document.body.append(host);
  root = createRoot(host);
  mounted = true;
  previousWidth = window.innerWidth;
  window.innerWidth = 1440;
  (globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean }).IS_REACT_ACT_ENVIRONMENT = true;
});

afterEach(async () => {
  if (mounted) await act(async () => root.unmount());
  mounted = false;
  host.remove();
  window.innerWidth = previousWidth;
});

test.each([1600, 1024, 390])("%ipx 下只保留顶栏开关，信息栏关闭后不出现重复竖栏", async width => {
  window.innerWidth = width;
  readApi.mockImplementation((path: string) => route(path, {
    "/v2/me/sandbox/sessions/session-A/project": bindingBound,
    "/v2/me/sandbox/projects/proj-9": project,
    "/v2/me/development/projects/proj-9/environment": environment,
    "/v2/targets/target-development/tools": tools,
    "/v2/me/mcp-connections": connections,
    "/v2/me/sandbox/projects/proj-9/files?path=.&limit=100": {
      path: ".", entries: [], offset: 0, next_offset: null, total: 0,
      project_id: "proj-9", generation: 1, may_change: false,
    },
  }));
  await render();
  const action = async (value: "info" | "files" | "browser") => act(async () => {
    window.dispatchEvent(new CustomEvent("krim:development-action", { detail: {
      principal: "operator", generation: 3, workspace: "local", sessionId: "session-A", action: value,
    } }));
  });
  expect(host.querySelector(".dev-rail")).toBeNull();
  expect(host.querySelector('[aria-label="关闭信息栏"]')).toBeNull();
  if (host.querySelector(".g-info")) await action("info");
  expect(host.querySelector(".g-info")).toBeNull();
  await action("info");
  expect(host.querySelector(".g-info")).not.toBeNull();
  expect(host.querySelector(".g-info-section.environment .g-info-head button")).toBeNull();
  await action("info");
  expect(host.querySelector(".g-info,.dev-rail")).toBeNull();
  expect(host.querySelector(".dev-panel-root")?.getAttribute("data-info")).toBe("false");
  await action("files");
  expect(host.querySelector('[role="tabpanel"]')?.getAttribute("aria-label")).toBe("项目文件");
  expect(host.querySelector(".dev-rail")).toBeNull();
  await action("files");
  expect(host.querySelector(".g-work")).toBeNull();
  await action("browser");
  expect(host.querySelector('[role="tabpanel"]')?.getAttribute("aria-label")).toBe("浏览器预览");
  await action("browser");
  expect(host.querySelector(".g-info,.g-work,.dev-rail")).toBeNull();
});

const compactPath = (id = "session-A") => `/v2/workspaces/local/sessions/${id}/context/compactions`;
function compactReceipt(body: unknown, sessionId = "session-A") {
  return { request_key: (body as { request_key: string }).request_key, session_id: sessionId, revision: 1, reason: null, status: "pending", created_at: 1, created: true };
}

test("Compact session点击现有按钮仅排队，不将接纳称为压缩完成", async () => {
  readApi.mockImplementation((path, _key, body) => path === compactPath()
    ? Promise.resolve(compactReceipt(body)) : Promise.reject(new Error("not provided")));
  await render();
  const compact = findButtonByLabel(host, "Compact session");
  expect(compact.className).toContain("outline");
  await act(async () => { compact.click(); compact.click(); });
  const posts = readApi.mock.calls.filter(([path]) => path === compactPath());
  expect(posts).toHaveLength(1);
  expect(posts[0]![2]).toEqual({ request_key: expect.any(String) });
  expect(host.textContent).toContain("整理请求已排队");
  expect(host.textContent).toContain("下次安全的模型请求边界执行");
  expect(host.textContent).not.toContain("压缩完成");
  expect(document.querySelector('[role="dialog"]')).toBeNull();
});

test("Compact session断网后重挂载仍以原键重试，确认后下次生成新键", async () => {
  let fail = true;
  readApi.mockImplementation((path, _key, body) => {
    if (path !== compactPath()) return Promise.reject(new Error("not provided"));
    return fail ? Promise.reject(new TypeError("offline")) : Promise.resolve(compactReceipt(body));
  });
  await render();
  await act(async () => findButtonByLabel(host, "Compact session").click());
  expect(host.textContent).toContain("重试原请求");
  const original = readApi.mock.calls.find(([path]) => path === compactPath())![2];
  await act(async () => root.unmount());
  root = createRoot(host);
  await render();
  fail = false;
  await act(async () => findButtonByLabel(host, "Compact session").click());
  const retried = readApi.mock.calls.filter(([path]) => path === compactPath());
  expect(retried).toHaveLength(2);
  expect(retried[1]![2]).toEqual(original);
  await act(async () => findButtonByLabel(host, "Compact session").click());
  const fresh = readApi.mock.calls.filter(([path]) => path === compactPath());
  expect(fresh).toHaveLength(3);
  expect(fresh[2]![2]).not.toEqual(original);
});

test("Compact session丢弃旧scope响应，新的会话不串提示和请求键", async () => {
  let release!: (value: unknown) => void;
  readApi.mockImplementation((path, _key, body) => {
    if (path === compactPath()) return new Promise(resolve => { release = resolve; });
    if (path === compactPath("session-B")) return Promise.resolve(compactReceipt(body, "session-B"));
    return Promise.reject(new Error("not provided"));
  });
  await render();
  await act(async () => findButtonByLabel(host, "Compact session").click());
  const original = readApi.mock.calls.find(([path]) => path === compactPath())![2];
  const next = { ...baseProps, chat: fakeChat({ id: "session-B" }, { id: "session-B" }) };
  await render(next);
  expect(findButtonByLabel(host, "Compact session").disabled).toBe(false);
  await act(async () => { release(compactReceipt(original)); await Promise.resolve(); });
  expect(host.textContent).not.toContain("整理请求已排队");
  await act(async () => findButtonByLabel(host, "Compact session").click());
  const fresh = readApi.mock.calls.find(([path]) => path === compactPath("session-B"))![2];
  expect(fresh).not.toEqual(original);
  expect(host.textContent).toContain("整理请求已排队");
});

test("Compact session拒绝错会话回执并保留原键", async () => {
  readApi.mockImplementation((path, _key, body) => path === compactPath()
    ? Promise.resolve(compactReceipt(body, "another-session")) : Promise.reject(new Error("not provided")));
  await render();
  await act(async () => findButtonByLabel(host, "Compact session").click());
  expect(host.textContent).toContain("重试原请求");
  await act(async () => findButtonByLabel(host, "Compact session").click());
  const posts = readApi.mock.calls.filter(([path]) => path === compactPath());
  expect(posts[1]![2]).toEqual(posts[0]![2]);
  expect(host.textContent).not.toContain("整理请求已排队");
});

test("绑定项目后环境行、工具目录与账号连接显示真实数据，无百分比构成", async () => {
  readApi.mockImplementation((path: string) => route(path, {
    "/v2/me/sandbox/sessions/session-A/project": bindingBound,
    "/v2/me/sandbox/projects/proj-9": project,
    "/v2/me/development/projects/proj-9/environment": environment,
    "/v2/targets/target-development/tools": tools,
    "/v2/me/mcp-connections": connections,
  }));

  await render();

  expect(host.textContent).toContain("图片服务项目");
  expect(host.textContent).toContain("已就绪");
  expect(host.textContent).toContain("Secrets 密钥");
  expect(host.querySelector('[aria-label="会话工具目录"]')?.textContent).toContain("docs.search_pages");
  expect(host.querySelector('[aria-label="会话工具目录"]')?.textContent).not.toContain("dev_read");
  expect(host.querySelector('[aria-label="会话工具目录"]')?.textContent).toContain("read");
  await act(async () => (host.querySelector('[title="MCP 连接"]') as HTMLButtonElement).click());
  expect(dialogElement().textContent).toContain("docs");
  expect(dialogElement().textContent).toContain("凭据已配置");
  // 上下文不编造构成百分比；容量来自会话策略，用量缺失如实标注。
  expect(host.textContent).not.toContain("%");
  expect(host.textContent).toContain("878.9 KB");
  expect(host.textContent).toContain("上下文用量未提供");
  expect(host.textContent).toContain("记忆来源未提供");
  expect(host.textContent).toContain("Memory files—");
});

test("null消息上限在信息栏与Context弹窗均显示不设上限", async () => {
  readApi.mockImplementation((path: string) => route(path, {
    "/v2/me/sandbox/sessions/session-A/project": bindingUnbound,
    "/v2/targets/target-development/tools": [],
    "/v2/me/mcp-connections": [],
  }));
  await render({ ...baseProps, chat: fakeChat({}, {
    context_policy: { max_bytes: 131072, max_messages: null, reserved_output_tokens: 2048, counter_version: "approx-utf8/1" },
  }) });
  expect(host.querySelector(".g-context-overview")?.textContent).toContain("不设消息数上限");
  const row = [...host.querySelectorAll(".g-context-table tr")].find(item => item.textContent?.includes("消息上限"))!;
  expect(row.querySelectorAll("td")[1]?.textContent).toBe("不设消息数上限");
  expect(row.textContent).not.toContain("— 条");
  await act(async () => findButtonByLabel(host, "上下文用量说明").click());
  const dialog = dialogElement();
  const label = [...dialog.querySelectorAll("dt")].find(item => item.textContent === "消息上限")!;
  expect(label.nextElementSibling?.textContent).toBe("不设消息数上限");
  expect(dialog.textContent).not.toContain("— 条");
});

test("未绑定会话显示未准备，不显示凭据管理", async () => {
  readApi.mockImplementation((path: string) => route(path, {
    "/v2/me/sandbox/sessions/session-A/project": bindingUnbound,
    "/v2/targets/target-development/tools": tools,
    "/v2/me/mcp-connections": connections,
  }));

  await render();

  expect(host.textContent).toContain("未准备");
  expect(host.textContent).toContain("未绑定");
  expect(host.textContent).not.toContain("图片服务项目");
  const toolsList = host.querySelector('[aria-label="会话工具目录"]');
  expect(toolsList?.textContent).toContain("docs.search_pages");
});

test("Secret 添加：值只出现在一次 PUT 请求体中，界面与账本均不保留", async () => {
  readApi.mockImplementation((path: string, _key: string, body?: unknown, _signal?: AbortSignal, method?: string) => {
    if (path === "/v2/me/development/projects/proj-9/secrets/API_TOKEN" && method === "PUT") {
      expect((body as { value: string }).value).toBe("sk-secret-abc-123");
      return Promise.resolve({ secret_id: "API_TOKEN", revision: 1, revoked: false });
    }
    return route(path, {
      "/v2/me/sandbox/sessions/session-A/project": bindingBound,
      "/v2/me/sandbox/projects/proj-9": project,
      "/v2/me/development/projects/proj-9/environment": environment,
      "/v2/targets/target-development/tools": tools,
      "/v2/me/mcp-connections": connections,
    });
  });

  await render();
  const secretRow = [...host.querySelectorAll(".g-env-row")].find(row => row.textContent?.includes("Secret"));
  await act(async () => (secretRow as HTMLButtonElement).click());

  const dialog = dialogElement;
  expect(dialog().textContent).toContain("不读取也不回显凭据内容");
  const nameInput = dialog().querySelector("#dev-secret-name") as HTMLInputElement;
  const valueInput = dialog().querySelector("#dev-secret-value") as HTMLInputElement;
  await act(async () => {
    typeInput(nameInput, "API_TOKEN");
    typeInput(valueInput, "sk-secret-abc-123");
  });
  await act(async () => findButton(dialog(), "添加引用").click());
  await flush();

  expect(readApi.mock.calls.some(call => call[4] === "PUT")).toBe(true);
  expect(document.body.textContent).not.toContain("sk-secret-abc-123");
  expect(host.textContent).not.toContain("sk-secret-abc-123");
  const ledger = localStorage.getItem("krim.development.writes/1");
  expect(ledger === null || !ledger.includes("sk-secret-abc-123")).toBe(true);
  expect(document.querySelector('[role="dialog"]')?.textContent).toContain("引用已保存");
});

test("Secret 轮换先输入新值并确认，不能点击即重发旧值", async () => {
  const writes: { value: string; expected_revision: number }[] = [];
  readApi.mockImplementation((path: string, _key: string, body?: unknown, _signal?: AbortSignal, method?: string) => {
    if (path.endsWith("/secrets/API_TOKEN") && method === "PUT") { writes.push(body as { value: string; expected_revision: number }); return Promise.resolve({ secret_id: "API_TOKEN", revision: writes.length, revoked: false }); }
    return route(path, { "/v2/me/sandbox/sessions/session-A/project": bindingBound, "/v2/me/sandbox/projects/proj-9": project, "/v2/me/development/projects/proj-9/environment": environment, "/v2/targets/target-development/tools": tools, "/v2/me/mcp-connections": connections });
  });
  await render(); await act(async () => ([...host.querySelectorAll<HTMLButtonElement>(".g-env-row")].find(row => row.textContent?.includes("Secret"))!).click());
  await act(async () => { typeInput(dialogElement().querySelector<HTMLInputElement>("#dev-secret-name")!, "API_TOKEN"); typeInput(dialogElement().querySelector<HTMLInputElement>("#dev-secret-value")!, "initial-test-value"); });
  await act(async () => findButton(dialogElement(), "添加引用").click());
  await act(async () => findButton(dialogElement(), "轮换").click());
  expect(writes).toHaveLength(1); expect(dialogElement().querySelector<HTMLInputElement>("#dev-secret-value")!.value).toBe("");
  expect(findButton(dialogElement(), "确认轮换").disabled).toBe(true);
  await act(async () => typeInput(dialogElement().querySelector<HTMLInputElement>("#dev-secret-value")!, "rotated-test-value"));
  await act(async () => findButton(dialogElement(), "确认轮换").click());
  expect(writes).toEqual([{ expected_revision: 0, expires_at: expect.any(Number), value: "initial-test-value", revoked: false }, { expected_revision: 1, expires_at: expect.any(Number), value: "rotated-test-value", revoked: false }]);
  expect(dialogElement().querySelector<HTMLInputElement>("#dev-secret-value")!.value).toBe("");
  expect(localStorage.getItem("krim.development.writes/1") ?? "").not.toContain("rotated-test-value");
});

test("Secret 撤销需要确认并携带 revoked 标记", async () => {
  const putCalls: unknown[] = [];
  readApi.mockImplementation((path: string, _key: string, body?: unknown, _signal?: AbortSignal, method?: string) => {
    if (path === "/v2/me/development/projects/proj-9/secrets/API_TOKEN" && method === "PUT") {
      putCalls.push(body);
      return Promise.resolve({ secret_id: "API_TOKEN", revision: putCalls.length, revoked: putCalls.length === 2 });
    }
    return route(path, {
      "/v2/me/sandbox/sessions/session-A/project": bindingBound,
      "/v2/me/sandbox/projects/proj-9": project,
      "/v2/me/development/projects/proj-9/environment": environment,
      "/v2/targets/target-development/tools": tools,
      "/v2/me/mcp-connections": connections,
    });
  });

  await render();
  const secretRow = [...host.querySelectorAll(".g-env-row")].find(row => row.textContent?.includes("Secret"));
  await act(async () => (secretRow as HTMLButtonElement).click());
  const dialog = dialogElement;
  await act(async () => {
    typeInput(dialog().querySelector("#dev-secret-name") as HTMLInputElement, "API_TOKEN");
    typeInput(dialog().querySelector("#dev-secret-value") as HTMLInputElement, "sk-secret-abc-123");
  });
  await act(async () => findButton(dialog(), "添加引用").click());
  await flush();
  await act(async () => findButton(dialog(), "撤销").click());
  expect(dialog().textContent).toContain("确认撤销");
  await act(async () => findButton(dialog(), "确认撤销").click());
  await flush();

  expect(putCalls).toHaveLength(2);
  expect((putCalls[1] as { revoked: boolean }).revoked).toBe(true);
});

test("绑定网络断开仍保留原intent，不将未知结果当成拒绝", async () => {
  readApi.mockImplementation((path: string, _key: string, _body?: unknown, _signal?: AbortSignal, method?: string) => {
    if (path.endsWith("/session-A/project") && method === "PUT") return Promise.reject(new TypeError("connection lost"));
    return route(path, { "/v2/me/sandbox/sessions/session-A/project": bindingUnbound, "/v2/me/sandbox/projects?offset=0&limit=100": projectsPage, "/v2/targets/target-development/tools": tools, "/v2/me/mcp-connections": connections });
  });
  await render(); await act(async () => ([...host.querySelectorAll<HTMLButtonElement>(".g-env-row")].find(row => row.textContent?.includes("选择项目"))!).click());
  await act(async () => (dialogElement().querySelector('input[name="dev-project"]') as HTMLInputElement).click());
  await act(async () => findButton(dialogElement(), "绑定项目").click());
  const ledger = JSON.parse(localStorage.getItem("krim.development.writes/1")!);
  expect(ledger).toHaveLength(1); expect(ledger[0].kind).toBe("bind-project"); expect(host.textContent).toContain("绑定结果未知");
});

test("未支持的操作显示敬请期待", async () => {
  readApi.mockImplementation((path: string) => route(path, {
    "/v2/me/sandbox/sessions/session-A/project": bindingBound,
    "/v2/me/sandbox/projects/proj-9": project,
    "/v2/me/development/projects/proj-9/environment": environment,
    "/v2/targets/target-development/tools": tools,
    "/v2/me/mcp-connections": connections,
  }));

  await render();
  const terminalRow = [...host.querySelectorAll(".g-output-row")].find(row => row.textContent?.includes("下载项目文件归档"));
  await act(async () => (terminalRow as HTMLButtonElement).click());

  expect(document.querySelector('[role="dialog"]')?.textContent).toContain("敬请期待");
  expect(document.querySelector('[role="dialog"]')?.textContent).toContain("项目打包下载暂未开放");
});

test("无已注册预览时不建 iframe，并说明真实原因", async () => {
  readApi.mockImplementation((path: string) => route(path, {
    "/v2/me/sandbox/sessions/session-A/project": bindingBound,
    "/v2/me/sandbox/projects/proj-9": project,
    "/v2/me/development/projects/proj-9/environment": environment,
    "/v2/targets/target-development/tools": tools,
    "/v2/me/mcp-connections": connections,
  }));

  await render();
  const outputRow = [...host.querySelectorAll(".g-output-row")].find(row => row.textContent?.includes("打开浏览器实时预览"));
  await act(async () => (outputRow as HTMLButtonElement).click());

  expect(host.querySelector('[aria-label="开发工作区"]')).toBeTruthy();
  expect(host.querySelectorAll("iframe")).toHaveLength(0);
  expect(host.textContent).toContain("凭票据打开");
});

// ---- 面板内嵌预览 ----

const previewHandle = { preview_id: "preview-1", environment: { owner_id: "operator", project_id: "proj-9" }, port: 3000, state: "ready" };
const previewEvents = [
  { run_id: "run-p", seq: 0, kind: "tool_start", data: { call_id: "c1", provider_call_id: "pc1", step_index: 0, name: "dev_preview_register", arguments: {} } },
  { run_id: "run-p", seq: 1, kind: "tool_end", data: { call_id: "c1", provider_call_id: "pc1", step_index: 0, name: "dev_preview_register", arguments: {}, status: "ok", result_available: true, value: previewHandle } },
];
const previewTurn = { id: "turn-p", session_id: "session-A", workspace_id: "local", owner_id: "operator", ordinal: 0, request_key: "rp", text: "启动预览", accepted_revision: 0, status: "running", run_id: "run-p", context_fingerprint: null, transcript: null, code: null, created_at: 1, started_at: 1, finished_at: null };
const previewChat = () => fakeChat({ history: [previewTurn], details: {}, turnEvents: { "turn-p": previewEvents }, turnFacts: {}, events: [] });
const previewBootstrap = (allow: boolean) => ({
  action: `https://preview-${"c".repeat(40)}.example.test/.krim-preview/session`, ticket: "k".repeat(43),
  expires_at: Date.now() / 1000 + 30, allow_embedding: allow,
});
const previewRoutes = {
  "/v2/me/sandbox/sessions/session-A/project": bindingBound,
  "/v2/me/sandbox/projects/proj-9": project,
  "/v2/me/development/projects/proj-9/environment": environment,
  "/v2/targets/target-development/tools": tools,
  "/v2/me/mcp-connections": connections,
};
function stubSubmissions() {
  const submitted: { form: HTMLFormElement; ticket: string | null }[] = [];
  const original = HTMLFormElement.prototype.submit;
  HTMLFormElement.prototype.submit = function (this: HTMLFormElement) {
    const input = this.querySelector('input[name="ticket"]') as HTMLInputElement | null;
    submitted.push({ form: this, ticket: input?.value ?? null });
  } as typeof HTMLFormElement.prototype.submit;
  return { submitted, restore: () => { HTMLFormElement.prototype.submit = original; } };
}
function stubWindowOpen() {
  const opened: { url: string; name: string; close: ReturnType<typeof vi.fn>; opener: unknown }[] = [];
  const original = window.open;
  window.open = ((url: string, name: string) => {
    const tab = { url, name, close: vi.fn(), opener: null as unknown };
    opened.push(tab);
    return tab;
  }) as unknown as typeof window.open;
  return { opened, restore: () => { window.open = original; } };
}
function previewRow() {
  const row = [...host.querySelectorAll(".g-row")].find(item => item.textContent?.includes("端口 3000"));
  expect(row, "预览行未找到").toBeTruthy();
  return row as HTMLElement;
}
async function openBrowserTab() {
  const outputRow = [...host.querySelectorAll(".g-output-row")].find(row => row.textContent?.includes("打开浏览器实时预览"));
  await act(async () => (outputRow as HTMLButtonElement).click());
}

test("服务端授权嵌入后，预览经命名 iframe 在面板内打开，票据只随 POST 正文提交", async () => {
  readApi.mockImplementation((path: string) => route(path, { ...previewRoutes, "/api/development/previews/preview-1/open": previewBootstrap(true) }));
  const submit = stubSubmissions();
  const open = stubWindowOpen();
  try {
    await render({ ...baseProps, chat: previewChat() });
    await openBrowserTab();
    const row = previewRow();
    await act(async () => ([...row.querySelectorAll("button")][0] as HTMLButtonElement).click());
    await flush();

    // 候选窗口同步预开保住 user activation；授权嵌入后立即关闭，不重复导航。
    expect(open.opened).toHaveLength(1);
    expect(open.opened[0]!.url).toBe("about:blank");
    expect(open.opened[0]!.close).toHaveBeenCalledTimes(1);
    const iframe = host.querySelector("iframe");
    expect(iframe).toBeTruthy();
    expect(iframe!.getAttribute("sandbox")).toBe("allow-scripts allow-same-origin allow-forms allow-downloads");
    expect(submit.submitted).toHaveLength(1);
    const { form, ticket } = submit.submitted[0]!;
    expect(form.method.toLowerCase()).toBe("post");
    expect(form.action).toBe(previewBootstrap(true).action);
    expect(form.target).toMatch(/^krim-preview-/);
    expect(form.target).toBe(iframe!.getAttribute("name"));
    expect(form.target).not.toBe(open.opened[0]!.name);
    // 提交瞬间票据在 POST 正文里，提交后立即清空且表单移除，不留在 DOM 或 URL。
    expect(ticket).toBe("k".repeat(43));
    expect((form.querySelector('input[name="ticket"]') as HTMLInputElement).value).toBe("");
    expect(document.body.querySelector("form")).toBeNull();
    expect(iframe!.getAttribute("src")).toBeNull(); // 帧由命名表单 POST 导航，初始不带任何携带票据的地址
    // 嵌入打开后保留行内"新窗口"入口；不渲染任何"成功"字样。
    expect([...row.querySelectorAll("button")].map(button => button.textContent)).toEqual(["打开预览", "新窗口"]);
    expect(host.textContent).not.toContain("已成功");
  } finally { submit.restore(); open.restore(); }
});

test("预览长时间没有完成加载时退出空白帧并显示超时原因", async () => {
  readApi.mockImplementation((path: string) => route(path, { ...previewRoutes, "/api/development/previews/preview-1/open": previewBootstrap(true) }));
  const submit = stubSubmissions();
  const open = stubWindowOpen();
  try {
    await render({ ...baseProps, chat: previewChat() });
    await openBrowserTab();
    vi.useFakeTimers();
    await act(async () => findButton(previewRow(), "打开预览").click());
    await flush();
    expect(host.querySelector("iframe")).not.toBeNull();
    await act(async () => { vi.advanceTimersByTime(30_000); });
    expect(host.textContent).toContain("预览加载超时");
    expect(host.querySelector("iframe")).toBeNull();
    expect(findButton(previewRow(), "打开预览").disabled).toBe(false);
    expect(submit.submitted).toHaveLength(1);
  } finally { vi.useRealTimers(); submit.restore(); open.restore(); }
});

test("初始 about:blank 的 load 事件不能取消预览加载超时", async () => {
  readApi.mockImplementation((path: string) => route(path, { ...previewRoutes, "/api/development/previews/preview-1/open": previewBootstrap(true) }));
  const submit = stubSubmissions();
  const open = stubWindowOpen();
  try {
    await render({ ...baseProps, chat: previewChat() });
    await openBrowserTab();
    vi.useFakeTimers();
    await act(async () => findButton(previewRow(), "打开预览").click());
    await flush();
    const frame = host.querySelector("iframe")!;
    // 浏览器初始 about:blank load 不能当作预览加载完成。
    await act(async () => frame.dispatchEvent(new Event("load")));
    await act(async () => { vi.advanceTimersByTime(30_000); });
    expect(host.querySelector("iframe")).toBeNull();
    expect(host.textContent).toContain("预览加载超时");
  } finally { vi.useRealTimers(); submit.restore(); open.restore(); }
});

test("完成预览文档导航后清理旧超时，重开时重新计时", async () => {
  readApi.mockImplementation((path: string) => route(path, { ...previewRoutes, "/api/development/previews/preview-1/open": previewBootstrap(true) }));
  const submit = stubSubmissions();
  const open = stubWindowOpen();
  try {
    await render({ ...baseProps, chat: previewChat() });
    await openBrowserTab();
    vi.useFakeTimers();
    await act(async () => findButton(previewRow(), "打开预览").click());
    await flush();
    const frame = host.querySelector("iframe")!;
    Object.defineProperty(frame, "contentWindow", { configurable: true, value: {
      get location() { throw new DOMException("Blocked cross-origin access", "SecurityError"); },
    } });
    await act(async () => frame.dispatchEvent(new Event("load")));
    await act(async () => { vi.advanceTimersByTime(30_000); });
    expect(host.querySelector("iframe")).toBe(frame);
    expect(host.textContent).not.toContain("预览加载超时");
    await act(async () => findButton(previewRow(), "打开预览").click());
    await flush();
    expect(host.querySelector("iframe")).not.toBe(frame);
    await act(async () => { vi.advanceTimersByTime(30_000); });
    expect(host.querySelector("iframe")).toBeNull();
    expect(host.textContent).toContain("预览加载超时");
  } finally { vi.useRealTimers(); submit.restore(); open.restore(); }
});

test("允许嵌入时不依赖浏览器允许弹窗", async () => {
  readApi.mockImplementation((path: string) => route(path, { ...previewRoutes, "/api/development/previews/preview-1/open": previewBootstrap(true) }));
  const submit = stubSubmissions();
  const original = window.open;
  window.open = () => null;
  try {
    await render({ ...baseProps, chat: previewChat() });
    await openBrowserTab();
    await act(async () => findButton(previewRow(), "打开预览").click());
    await flush();
    expect(host.querySelector("iframe")).not.toBeNull();
    expect(submit.submitted).toHaveLength(1);
    expect(host.textContent).not.toContain("请允许打开项目预览窗口");
  } finally { window.open = original; submit.restore(); }
});

test("未获嵌入授权时保持新窗口行为", async () => {
  readApi.mockImplementation((path: string) => route(path, { ...previewRoutes, "/api/development/previews/preview-1/open": previewBootstrap(false) }));
  const submit = stubSubmissions();
  const open = stubWindowOpen();
  try {
    await render({ ...baseProps, chat: previewChat() });
    await openBrowserTab();
    const row = previewRow();
    await act(async () => ([...row.querySelectorAll("button")][0] as HTMLButtonElement).click());
    await flush();

    expect(open.opened).toHaveLength(1);
    expect(open.opened[0]!.url).toBe("about:blank");
    expect(open.opened[0]!.name).toMatch(/^krim-preview-/);
    expect(open.opened[0]!.opener).toBeNull();
    expect(submit.submitted).toHaveLength(1);
    expect(submit.submitted[0]!.form.target).toBe(open.opened[0]!.name);
    expect(submit.submitted[0]!.ticket).toBe("k".repeat(43));
    expect(open.opened[0]!.close).not.toHaveBeenCalled(); // 窗口保留，由 POST 导航到预览
    expect(host.querySelectorAll("iframe")).toHaveLength(0);
  } finally { submit.restore(); open.restore(); }
});

test("打开失败如实报错，候选窗口关闭且不进帧不提交", async () => {
  readApi.mockImplementation((path: string) => path === "/api/development/previews/preview-1/open"
    ? Promise.reject(new RequestError(503, "preview_unavailable"))
    : route(path, previewRoutes));
  const submit = stubSubmissions();
  const open = stubWindowOpen();
  try {
    await render({ ...baseProps, chat: previewChat() });
    await openBrowserTab();
    const row = previewRow();
    await act(async () => ([...row.querySelectorAll("button")][0] as HTMLButtonElement).click());
    await flush();

    expect(host.textContent).toContain("预览打开未完成");
    expect(host.querySelectorAll("iframe")).toHaveLength(0);
    expect(open.opened).toHaveLength(1);
    expect(open.opened[0]!.close).toHaveBeenCalledTimes(1);
    expect(submit.submitted).toHaveLength(0);
  } finally { submit.restore(); open.restore(); }
});

test("打开的 iframe 名称每次唯一，重复打开替换旧帧", async () => {
  readApi.mockImplementation((path: string) => route(path, { ...previewRoutes, "/api/development/previews/preview-1/open": previewBootstrap(true) }));
  const submit = stubSubmissions();
  const open = stubWindowOpen();
  try {
    await render({ ...baseProps, chat: previewChat() });
    await openBrowserTab();
    const row = previewRow();
    const reopen = async () => act(async () => ([...row.querySelectorAll("button")][0] as HTMLButtonElement).click());
    await reopen(); await flush();
    const firstFrame = host.querySelector("iframe")!;
    const firstName = firstFrame.getAttribute("name");
    await reopen(); await flush();
    const secondFrame = host.querySelector("iframe")!;
    const secondName = secondFrame.getAttribute("name");
    expect(secondFrame).not.toBe(firstFrame);
    expect(firstFrame.isConnected).toBe(false);

    expect(firstName).toMatch(/^krim-preview-/);
    expect(secondName).toMatch(/^krim-preview-/);
    expect(secondName).not.toBe(firstName);
    expect(host.querySelectorAll("iframe")).toHaveLength(1);
    expect(submit.submitted).toHaveLength(2);
    expect(submit.submitted[1]!.form.target).toBe(secondName);
    expect(submit.submitted.map(item => item.ticket)).toEqual(["k".repeat(43), "k".repeat(43)]);
    expect(open.opened).toHaveLength(2);
    expect(open.opened.map(item => item.close)).toEqual([expect.anything(), expect.anything()]);
    expect(open.opened.every(item => item.close.mock.calls.length === 1)).toBe(true);
  } finally { submit.restore(); open.restore(); }
});

test("切换会话后旧预览 iframe 随卸载移除", async () => {
  readApi.mockImplementation((path: string) => route(path, { ...previewRoutes, "/api/development/previews/preview-1/open": previewBootstrap(true) }));
  const submit = stubSubmissions();
  const open = stubWindowOpen();
  try {
    await render({ ...baseProps, chat: previewChat() });
    await openBrowserTab();
    const row = previewRow();
    await act(async () => ([...row.querySelectorAll("button")][0] as HTMLButtonElement).click());
    await flush();
    expect(host.querySelectorAll("iframe")).toHaveLength(1);
    expect(open.opened[0]!.close).toHaveBeenCalledTimes(1);
    await render({ ...baseProps, chat: fakeChat({ id: "session-B" }, { id: "session-B" }) });

    expect(host.querySelectorAll("iframe")).toHaveLength(0);
    expect(document.querySelectorAll("iframe")).toHaveLength(0);
    expect(submit.submitted).toHaveLength(1);
  } finally { submit.restore(); open.restore(); }
});

test("卸载后迟到的授权回执不建帧、不提交票据", async () => {
  let release!: (value: unknown) => void;
  readApi.mockImplementation((path: string) => path === "/api/development/previews/preview-1/open"
    ? new Promise(resolve => { release = resolve; })
    : route(path, previewRoutes));
  const submit = stubSubmissions();
  const open = stubWindowOpen();
  try {
    await render({ ...baseProps, chat: previewChat() });
    await openBrowserTab();
    const row = previewRow();
    await act(async () => ([...row.querySelectorAll("button")][0] as HTMLButtonElement).click());
    expect(submit.submitted).toHaveLength(0);
    await render({ ...baseProps, chat: fakeChat({ id: "session-B" }, { id: "session-B" }) });
    await act(async () => { release(previewBootstrap(true)); });
    await flush();

    expect(document.querySelectorAll("iframe")).toHaveLength(0);
    expect(submit.submitted).toHaveLength(0);
    expect(open.opened).toHaveLength(1);
    expect(open.opened[0]!.close).toHaveBeenCalledTimes(1);
  } finally { submit.restore(); open.restore(); }
});

test("帧内提交异常时清空预览并如实报错", async () => {
  readApi.mockImplementation((path: string) => route(path, { ...previewRoutes, "/api/development/previews/preview-1/open": previewBootstrap(true) }));
  const open = stubWindowOpen();
  const original = HTMLFormElement.prototype.submit;
  let attempts = 0;
  HTMLFormElement.prototype.submit = function () {
    attempts += 1;
    throw new Error("submit failed");
  } as typeof HTMLFormElement.prototype.submit;
  try {
    await render({ ...baseProps, chat: previewChat() });
    await openBrowserTab();
    const row = previewRow();
    await act(async () => ([...row.querySelectorAll("button")][0] as HTMLButtonElement).click());
    await flush();

    expect(attempts).toBe(1); // 只尝试一次，异常后不重放票据
    expect(host.querySelectorAll("iframe")).toHaveLength(0);
    expect(document.body.querySelector("form")).toBeNull();
    expect(host.textContent).toContain("预览打开未完成");
    expect(open.opened[0]!.close).toHaveBeenCalledTimes(1);
  } finally {
    HTMLFormElement.prototype.submit = original;
    open.restore();
  }
});

test("绑定冲突只提交一次并如实提示", async () => {
  let putCount = 0;
  readApi.mockImplementation((path: string, _key: string, _body: unknown, _signal?: AbortSignal, method?: string) => {
    if (path === "/v2/me/sandbox/sessions/session-A/project" && method === "PUT") {
      putCount += 1;
      return Promise.reject(new RequestError(409, "revision_conflict"));
    }
    if (path === "/v2/me/sandbox/sessions/session-A/project") return Promise.resolve(bindingUnbound);
    if (path === "/v2/me/sandbox/projects?offset=0&limit=100") return Promise.resolve(projectsPage);
    return route(path, {
      "/v2/targets/target-development/tools": tools,
      "/v2/me/mcp-connections": connections,
    });
  });

  await render();
  const projectRow = [...host.querySelectorAll(".g-env-row")].find(row => row.textContent?.includes("选择项目"));
  await act(async () => (projectRow as HTMLButtonElement).click());
  const dialog = () => document.querySelector('[role="dialog"]') as HTMLElement;
  const radio = dialog().querySelector('input[name="dev-project"]') as HTMLInputElement;
  await act(async () => radio.click());
  const bindButton = [...dialog().querySelectorAll("button")].find(button => button.textContent === "绑定项目") as HTMLButtonElement;
  await act(async () => bindButton.click());
  await flush();

  expect(putCount).toBe(1);
  expect(host.textContent).toContain("状态已更新，本次未执行；请刷新后重试");
  expect(localStorage.getItem("krim.development.writes/1")).toBe("[]");
  await act(async () => (projectRow as HTMLButtonElement).click());
  await act(async () => (dialog().querySelector('input[name="dev-project"]') as HTMLInputElement).click());
  await act(async () => findButton(dialog(), "绑定项目").click());
  expect(putCount).toBe(2);
  expect(readApi.mock.calls.some(call => call[0].includes("/operations?"))).toBe(false);
  expect(localStorage.getItem("krim.development.writes/1")).toBe("[]");
});

test("身份切换中止旧读取，迟到响应不进入界面", async () => {
  const pending: { path: string; signal?: AbortSignal; resolve: (value: unknown) => void }[] = [];
  readApi.mockImplementation((path: string, _key: string, _body: unknown, signal?: AbortSignal) => {
    return new Promise(resolve => pending.push({ path, signal, resolve }));
  });

  await render({ ...baseProps });
  expect(pending.length).toBeGreaterThan(0);
  await render({ ...baseProps, apiKey: "synthetic-key-B" });

  expect(pending[0]!.signal?.aborted).toBe(true);
  await act(async () => pending[0]!.resolve(bindingBound));
  await flush();
  expect(host.textContent).not.toContain("图片服务项目");
});

test("CSS 保持已验收的列表高度边界", () => {
  const css = readFileSync(join(process.cwd(), "src", "components", "development-panel.css"), "utf8");
  expect(css).toContain(".g-detail-list { max-height: 150px");
  expect(css).toContain(".g-memory-list { max-height: 178px");
  expect(css).toContain("max-height: 125px");
  expect(css).toContain(".g-info-section.outputs .g-section-body { max-height: 174px");
  expect(css).toContain(".g-env-row > .g-env-label { flex: 1; min-width: 0; overflow: hidden; white-space: nowrap; text-overflow: ellipsis");
});

// ---- 修复回归 ----

const seedBindIntent = {
  principal_id: "operator", generation: 3, workspace_id: "local", session_id: "session-A",
  project_id: "proj-9", kind: "bind-project", request_key: "seed-bind-key",
  target: "/v2/me/sandbox/sessions/session-A/project",
  body: { request_key: "seed-bind-key", expected_revision: 0, expected_session_revision: 1, project_id: "proj-9" },
  rejected: false,
};
const seedPrepareIntent = {
  principal_id: "operator", generation: 3, workspace_id: "local", session_id: "session-A",
  project_id: "proj-9", kind: "prepare-environment", request_key: "seed-prepare-key",
  target: "/v2/me/development/projects/proj-9/prepare",
  body: { request_key: "seed-prepare-key" },
  rejected: false,
};
const operationCompleted = { operation_id: "op-1", request_key: "seed-bind-key", kind: "bind_project", status: "completed", result: {} };

test("空密钥的 legacy/local 模式同样加载数据", async () => {
  readApi.mockImplementation((path: string) => route(path, {
    "/v2/me/sandbox/sessions/session-A/project": bindingBound,
    "/v2/me/sandbox/projects/proj-9": project,
    "/v2/me/development/projects/proj-9/environment": environment,
    "/v2/targets/target-development/tools": tools,
    "/v2/me/mcp-connections": connections,
  }));

  await render({ ...baseProps, apiKey: "" });

  expect(readApi.mock.calls.length).toBeGreaterThan(0);
  expect(readApi.mock.calls.every(call => call[1] === "")).toBe(true);
  expect(host.textContent).toContain("已就绪");
});

test("sandbox 绑定链路失败时工具目录仍独立加载", async () => {
  readApi.mockImplementation((path: string) => {
    if (path === "/v2/me/sandbox/sessions/session-A/project") return Promise.reject(new RequestError(503, "sandbox_management_unavailable"));
    return route(path, {
      "/v2/targets/target-development/tools": tools,
      "/v2/me/mcp-connections": connections,
    });
  });

  await render();

  expect(host.querySelector('[aria-label="会话工具目录"]')?.textContent).toContain("docs.search_pages");
  await act(async () => (host.querySelector('[title="MCP 连接"]') as HTMLButtonElement).click());
  expect(dialogElement().textContent).toContain("docs");
  expect(host.textContent).toContain("开发管理服务未启用");
});

test("存在未确认绑定记录时先查回执，不再用新键重复提交", async () => {
  localStorage.setItem("krim.development.writes/1", JSON.stringify([seedBindIntent]));
  let putCount = 0;
  readApi.mockImplementation((path: string, _key: string, _body: unknown, _signal?: AbortSignal, method?: string) => {
    if (path === "/v2/me/sandbox/operations?request_key=seed-bind-key") return Promise.resolve(operationCompleted);
    if (path === "/v2/me/sandbox/sessions/session-A/project" && method === "PUT") { putCount += 1; return Promise.resolve(bindingBound); }
    if (path === "/v2/me/sandbox/sessions/session-A/project") return Promise.resolve(bindingUnbound);
    if (path === "/v2/me/sandbox/projects?offset=0&limit=100") return Promise.resolve(projectsPage);
    return route(path, {
      "/v2/targets/target-development/tools": tools,
      "/v2/me/mcp-connections": connections,
    });
  });

  await render();
  const projectRow = [...host.querySelectorAll(".g-env-row")].find(row => row.textContent?.includes("选择项目"));
  await act(async () => (projectRow as HTMLButtonElement).click());
  const radio = dialogElement().querySelector('input[name="dev-project"]') as HTMLInputElement;
  await act(async () => radio.click());
  await act(async () => findButton(dialogElement(), "绑定项目").click());
  await flush();

  expect(readApi.mock.calls.some(call => String(call[0]).includes("/operations?request_key=seed-bind-key"))).toBe(true);
  expect(putCount).toBe(0);
  expect(localStorage.getItem("krim.development.writes/1")).toBe("[]");
});

test("回执404后的绑定重放遭明确版本拒绝时清除旧intent", async () => {
  localStorage.setItem("krim.development.writes/1", JSON.stringify([seedBindIntent]));
  let puts = 0;
  readApi.mockImplementation((path: string, _key: string, _body?: unknown, _signal?: AbortSignal, method?: string) => {
    if (path.includes("/operations?request_key=seed-bind-key")) return Promise.reject(new RequestError(404, "not_found"));
    if (path.endsWith("/session-A/project") && method === "PUT") { puts++; return Promise.reject(new RequestError(409, "revision_conflict")); }
    return route(path, { "/v2/me/sandbox/sessions/session-A/project": bindingUnbound, "/v2/me/sandbox/projects?offset=0&limit=100": projectsPage, "/v2/targets/target-development/tools": tools, "/v2/me/mcp-connections": connections });
  });
  await render();
  await act(async () => ([...host.querySelectorAll<HTMLButtonElement>(".dev-pending button")].find(button => button.textContent === "查找结果")!).click());
  expect(puts).toBe(1); expect(localStorage.getItem("krim.development.writes/1")).toBe("[]");
  expect(host.textContent).toContain("状态已更新");
});

test("prepare 恢复以 development 环境记录为准：已存在则采纳，不再重发", async () => {
  localStorage.setItem("krim.development.writes/1", JSON.stringify([seedPrepareIntent]));
  let environmentReads = 0;
  readApi.mockImplementation((path: string, _key: string, _body: unknown, _signal?: AbortSignal) => {
    if (path === "/v2/me/development/projects/proj-9/environment") {
      environmentReads += 1;
      // 载入时环境未注册；恢复查询时发现已有记录 → 采纳。
      return environmentReads === 1 ? Promise.reject(new RequestError(404, "not_found")) : Promise.resolve(environment);
    }
    if (path === "/v2/me/development/projects/proj-9/prepare") throw new Error("不应重发 prepare");
    if (path.startsWith("/v2/me/sandbox/projects/proj-9/files")) {
      return Promise.resolve({ path: ".", entries: [], offset: 0, next_offset: null, total: 0, project_id: "proj-9", generation: 1, may_change: false });
    }
    return route(path, {
      "/v2/me/sandbox/sessions/session-A/project": bindingBound,
      "/v2/me/sandbox/projects/proj-9": project,
      "/v2/targets/target-development/tools": tools,
      "/v2/me/mcp-connections": connections,
    });
  });

  await render();
  const environmentRow = [...host.querySelectorAll(".g-env-row")].find(row => row.textContent?.includes("Linux 容器"));
  await act(async () => (environmentRow as HTMLButtonElement).click());
  await act(async () => findButton(dialogElement(), "准备环境").click());
  await flush();

  expect(readApi.mock.calls.some(call => String(call[0]).endsWith("/prepare") && call[2] !== undefined)).toBe(false);
  expect(readApi.mock.calls.some(call => String(call[0]).includes("/operations?"))).toBe(false);
  expect(localStorage.getItem("krim.development.writes/1")).toBe("[]");
  expect(host.textContent).toContain("环境已就绪");
  expect(host.textContent).toContain("已就绪");
});

test("停止中的环境记录不能被当作准备完成并清掉未确认账本", async () => {
  localStorage.setItem("krim.development.writes/1", JSON.stringify([seedPrepareIntent]));
  let reads = 0;
  readApi.mockImplementation((path: string) => {
    if (path.endsWith("/environment")) return ++reads === 1 ? Promise.reject(new RequestError(404, "not_found")) : Promise.resolve({ ...environment, state: "stopped" });
    return route(path, { "/v2/me/sandbox/sessions/session-A/project": bindingBound, "/v2/me/sandbox/projects/proj-9": project, "/v2/targets/target-development/tools": tools, "/v2/me/mcp-connections": connections });
  });
  await render(); await act(async () => ([...host.querySelectorAll<HTMLButtonElement>(".g-env-row")].find(row => row.textContent?.includes("Linux 容器"))!).click());
  await act(async () => findButton(dialogElement(), "准备环境").click());
  expect(JSON.parse(localStorage.getItem("krim.development.writes/1")!)).toHaveLength(1);
  expect(host.textContent).toContain("环境尚未就绪");
  expect(readApi.mock.calls.filter(call => call[0].endsWith("/prepare") && call[2] !== undefined)).toHaveLength(0);
});

test("环境记录确无时按原键重投 prepare", async () => {
  localStorage.setItem("krim.development.writes/1", JSON.stringify([seedPrepareIntent]));
  readApi.mockImplementation((path: string, _key: string, body?: unknown, _signal?: AbortSignal) => {
    if (path === "/v2/me/development/projects/proj-9/environment") return Promise.reject(new RequestError(404, "not_found"));
    if (path === "/v2/me/development/projects/proj-9/prepare") {
      // 恢复路径不带显式 method 参数（请求体非空即 POST）；断言携带原 request_key。
      expect((body as { request_key: string }).request_key).toBe("seed-prepare-key");
      return Promise.resolve(environment);
    }
    if (path === "/v2/me/sandbox/operations?request_key=seed-prepare-key") return Promise.reject(new RequestError(404, "not_found"));
    return route(path, {
      "/v2/me/sandbox/sessions/session-A/project": bindingBound,
      "/v2/me/sandbox/projects/proj-9": project,
      "/v2/targets/target-development/tools": tools,
      "/v2/me/mcp-connections": connections,
    });
  });

  await render();
  const environmentRow = [...host.querySelectorAll(".g-env-row")].find(row => row.textContent?.includes("Linux 容器"));
  await act(async () => (environmentRow as HTMLButtonElement).click());
  await act(async () => findButton(dialogElement(), "准备环境").click());
  await flush();

  expect(readApi.mock.calls.some(call => String(call[0]).endsWith("/prepare") && call[2] !== undefined)).toBe(true);
  expect(localStorage.getItem("krim.development.writes/1")).toBe("[]");
});

test("损坏账本（多余字段）报无法读取且不进入提交队列", async () => {
  const corrupt = { ...seedBindIntent, body: { ...seedBindIntent.body, value: "should-reject" } };
  localStorage.setItem("krim.development.writes/1", JSON.stringify([corrupt]));
  readApi.mockImplementation((path: string) => route(path, {
    "/v2/me/sandbox/sessions/session-A/project": bindingBound,
    "/v2/me/sandbox/projects/proj-9": project,
    "/v2/me/development/projects/proj-9/environment": environment,
    "/v2/targets/target-development/tools": tools,
    "/v2/me/mcp-connections": connections,
  }));

  await render();

  expect(host.textContent).toContain("无法读取未确认请求");
  expect(host.textContent).not.toContain("查找结果");
});

test("Secret 迟到回执不写入新作用域", async () => {
  let resolvePut: ((value: unknown) => void) | null = null;
  readApi.mockImplementation((path: string, _key: string, _body: unknown, _signal?: AbortSignal, method?: string) => {
    if (path === "/v2/me/development/projects/proj-9/secrets/API_TOKEN" && method === "PUT") {
      return new Promise(resolve => { resolvePut = resolve; });
    }
    return route(path, {
      "/v2/me/sandbox/sessions/session-A/project": bindingBound,
      "/v2/me/sandbox/projects/proj-9": project,
      "/v2/me/development/projects/proj-9/environment": environment,
      "/v2/targets/target-development/tools": tools,
      "/v2/me/mcp-connections": connections,
    });
  });

  await render();
  const secretRow = [...host.querySelectorAll(".g-env-row")].find(row => row.textContent?.includes("Secret"));
  await act(async () => (secretRow as HTMLButtonElement).click());
  await act(async () => {
    typeInput(dialogElement().querySelector("#dev-secret-name") as HTMLInputElement, "API_TOKEN");
    typeInput(dialogElement().querySelector("#dev-secret-value") as HTMLInputElement, "sk-secret-abc-123");
  });
  await act(async () => findButton(dialogElement(), "添加引用").click());
  await flush();
  // 身份切换会卸载对话框并作废在途回执；回到原身份后不应出现跨 scope 的引用记录。
  await render({ ...baseProps, apiKey: "synthetic-key-B" });
  await act(async () => resolvePut?.({ secret_id: "API_TOKEN", revision: 1, revoked: false }));
  await flush();
  await render({ ...baseProps });
  const secretRowAgain = [...host.querySelectorAll(".g-env-row")].find(row => row.textContent?.includes("Secret"));
  await act(async () => (secretRowAgain as HTMLButtonElement).click());
  await flush();

  expect(dialogElement().textContent).not.toContain("本会话写入");
});

test("关闭唯一标签后激活项清空，不留已关闭的 id", async () => {
  readApi.mockImplementation((path: string) => route(path, {
    "/v2/me/sandbox/sessions/session-A/project": bindingBound,
    "/v2/me/sandbox/projects/proj-9": project,
    "/v2/me/development/projects/proj-9/environment": environment,
    "/v2/targets/target-development/tools": tools,
    "/v2/me/mcp-connections": connections,
  }));

  await render();
  const outputRow = [...host.querySelectorAll(".g-output-row")].find(row => row.textContent?.includes("打开浏览器实时预览"));
  await act(async () => (outputRow as HTMLButtonElement).click());
  expect(host.querySelectorAll('[role="tab"]')).toHaveLength(1);
  const closeButton = host.querySelector('[aria-label="关闭 浏览器预览"]') as HTMLButtonElement;
  await act(async () => closeButton.click());

  expect(host.querySelectorAll('[role="tab"]')).toHaveLength(0);
  expect(host.textContent).toContain("打开内容");
});

test("分区可合并并再次拆分", async () => {
  readApi.mockImplementation((path: string) => route(path, {
    "/v2/me/sandbox/sessions/session-A/project": bindingBound,
    "/v2/me/sandbox/projects/proj-9": project,
    "/v2/me/development/projects/proj-9/environment": environment,
    "/v2/targets/target-development/tools": tools,
    "/v2/me/mcp-connections": connections,
    "/v2/me/sandbox/projects/proj-9/files?path=.&limit=100": {
      path: ".", entries: [{ name: "README.md", type: "file", size: 30 }], offset: 0, next_offset: null,
      total: 1, project_id: "proj-9", generation: 1, may_change: false,
    },
  }));

  await render();
  const outputRow = [...host.querySelectorAll(".g-output-row")].find(row => row.textContent?.includes("项目文件"));
  await act(async () => (outputRow as HTMLButtonElement).click());
  expect(host.querySelectorAll(".g-work-slot")).toHaveLength(1);
  await act(async () => findButtonByLabel(host, "上下分区").click());
  expect(host.querySelectorAll(".g-work-slot")).toHaveLength(2);
  await act(async () => findButtonByLabel(host, "合并分区").click());
  expect(host.querySelectorAll(".g-work-slot")).toHaveLength(1);
  await act(async () => findButtonByLabel(host, "上下分区").click());
  expect(host.querySelectorAll(".g-work-slot")).toHaveLength(2);
});

test("侧栏 aux 事件在身份、工作区与会话匹配时打开只读记录", async () => {
  const listed = { ...bindingUnbound, session_id: "session-B" };
  const chatWithList = fakeChat({ list: [{ id: "session-B", workspace_id: "local", title: "侧栏会话", status: "open" }] });
  readApi.mockImplementation((path: string) => route(path, {
    "/v2/me/sandbox/sessions/session-A/project": bindingUnbound,
    "/v2/me/sandbox/sessions/session-B/project": listed,
    "/v2/targets/target-development/tools": tools,
    "/v2/me/mcp-connections": connections,
  }));

  await render({ ...baseProps, chat: chatWithList });
  await act(async () => {
    window.dispatchEvent(new CustomEvent("krim:open-aux", { detail: { principal: "operator", generation: 3, workspace: "local", sessionId: "session-B" } }));
    await Promise.resolve();
  });

  expect(host.querySelector('[aria-label="开发工作区"]')).toBeTruthy();
  expect(host.querySelector('[role="tab"]')?.textContent).toContain("侧栏会话");

  await act(async () => {
    window.dispatchEvent(new CustomEvent("krim:open-aux", { detail: { principal: "operator", generation: 3, workspace: "local", sessionId: "session-404" } }));
    window.dispatchEvent(new CustomEvent("krim:open-aux", { detail: { principal: "stranger", generation: 3, workspace: "local", sessionId: "session-B" } }));
    await Promise.resolve();
  });
  expect(host.querySelectorAll('[role="tab"]')).toHaveLength(1);
});

test("新建项目成功后按真实响应刷新并选中", async () => {
  const created = { ...project, project_id: "proj-new", title: "全新项目" };
  readApi.mockImplementation((path: string, _key: string, body?: unknown, _signal?: AbortSignal, method?: string) => {
    if (path === "/v2/me/sandbox/projects" && method === "POST") {
      expect((body as { title: string }).title).toBe("全新项目");
      return Promise.resolve(created);
    }
    if (path === "/v2/me/sandbox/sessions/session-A/project") return Promise.resolve(bindingUnbound);
    if (path === "/v2/me/sandbox/projects?offset=0&limit=100") return Promise.resolve(projectsPage);
    return route(path, {
      "/v2/targets/target-development/tools": tools,
      "/v2/me/mcp-connections": connections,
    });
  });

  await render();
  const projectRow = [...host.querySelectorAll(".g-env-row")].find(row => row.textContent?.includes("选择项目"));
  await act(async () => (projectRow as HTMLButtonElement).click());
  await act(async () => findButton(dialogElement(), "新建项目").click());
  await act(async () => {
    typeInput(dialogElement().querySelector("#dev-project-title") as HTMLInputElement, "全新项目");
  });
  await act(async () => findButton(dialogElement(), "创建项目").click());
  await flush();

  const options = dialogElement().querySelectorAll(".g-project-option");
  expect(options.length).toBe(2);
  expect(dialogElement().textContent).toContain("全新项目");
  expect((dialogElement().querySelector('input[name="dev-project"]:checked') as HTMLInputElement).value).toBe("proj-new");
});

test("分段读取发现文件变化时报错而非拼接坏数据", async () => {
  readApi.mockImplementation((path: string) => {
    if (path.startsWith("/v2/me/sandbox/projects/proj-9/files?")) {
      return Promise.resolve({
        path: ".", entries: [{ name: "big.txt", type: "file", size: 200000 }], offset: 0, next_offset: null,
        total: 1, project_id: "proj-9", generation: 1, may_change: false,
      });
    }
    if (path.includes("/file?path=big.txt")) {
      const offset = Number(new URLSearchParams(path.split("?")[1] ?? "").get("offset") ?? 0);
      return Promise.resolve({
        path: "big.txt", offset, size: 200000, eof: offset + 65536 >= 200000, sha256: "a".repeat(64),
        stat_token: offset === 0 ? [1, 2, 200000, 3, 4] : [1, 2, 999999, 3, 4],
        data: "QUJD", project_id: "proj-9", generation: 1, may_change: true,
      });
    }
    return route(path, {
      "/v2/me/sandbox/sessions/session-A/project": bindingBound,
      "/v2/me/sandbox/projects/proj-9": project,
      "/v2/me/development/projects/proj-9/environment": environment,
      "/v2/targets/target-development/tools": tools,
      "/v2/me/mcp-connections": connections,
    });
  });

  await render();
  const outputRow = [...host.querySelectorAll(".g-output-row")].find(row => row.textContent?.includes("项目文件"));
  await act(async () => (outputRow as HTMLButtonElement).click());
  const fileRow = [...host.querySelectorAll(".dev-file-row")].find(row => row.textContent?.includes("big.txt"));
  await act(async () => (fileRow as HTMLButtonElement).click());
  await flush();

  expect(host.textContent).toContain("文件在读取期间发生变化");
});

test("信息栏保留样板图标顺序，Outputs 只列文件调用并按路径去重", async () => {
  readApi.mockImplementation((path: string) => route(path, {
    "/v2/me/sandbox/sessions/session-A/project": bindingBound,
    "/v2/me/sandbox/projects/proj-9": project,
    "/v2/me/development/projects/proj-9/environment": environment,
    "/v2/targets/target-development/tools": tools,
    "/v2/me/mcp-connections": connections,
  }));
  const calls = [
    ["read_file", "src/long-file-name.tsx"], ["edit", "src/long-file-name.tsx"],
    ["dev_read", "README.md"], ["write_file", "docs/plan.md"],
    ["grep_content", "src"], ["write", "../outside.txt"],
  ];
  const transcript = calls.flatMap(([name, path], index) => [
    { role: "assistant", content: "", call_id: null, tool_calls: [{ kind: "tool_call", id: `call-${index}`, name, arguments: { path } }] },
    { role: "tool", content: "ok", call_id: `call-${index}`, tool_calls: [] },
  ]);
  await render({ ...baseProps, chat: fakeChat({ history: [{ id: "turn-a", run_id: "run-a", status: "completed", transcript }], turnEvents: {}, details: {}, turnFacts: {} }) });
  expect([...host.querySelectorAll(".g-env-row > svg")].map(icon => icon.getAttribute("class"))).toEqual([
    "lucide lucide-terminal", "lucide lucide-folder", "lucide lucide-shield", "lucide lucide-key-round", "lucide lucide-cpu",
  ]);
  expect([...host.querySelectorAll(".g-output-row[title]")].map(row => row.getAttribute("title"))).toEqual([
    "src/long-file-name.tsx", "README.md", "docs/plan.md",
  ]);
  expect(host.querySelector('.g-output-row[title="src/long-file-name.tsx"] svg')?.classList.contains("lucide-file-code")).toBe(true);
  expect(host.querySelector('.g-output-row[title="README.md"] svg')?.classList.contains("lucide-file-text")).toBe(true);
  expect(host.querySelector(".g-context-end button")?.textContent).toBe("Compact");
});
