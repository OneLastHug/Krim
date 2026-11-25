import { StrictMode, act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, expect, test, vi } from "vitest";
import { ChatWorkbench } from "../src/components/ChatWorkbench";
import { ModelGatewaysProvider } from "../src/hooks/use-model-gateways";
import { parseSession, type Session } from "../src/lib/session-contracts";

let root: Root, container: HTMLDivElement;
let override: ((path: string, options: RequestInit) => Response | Promise<Response> | undefined) | undefined;
const calls: { path: string; method: string; body: unknown }[] = [];
const json = (value: unknown, status = 200) => new Response(JSON.stringify(value), { status, headers: { "Content-Type": "application/json" } });
const session = (): Session => parseSession({
  id: "A", workspace_id: "local", owner_id: "local", create_key: "create-A", title: "演示",
  target_key: "target-http", target_fingerprint: "0".repeat(64),
  context_policy: { max_bytes: 131072, max_messages: 256, reserved_output_tokens: 2048, counter_version: "approx-utf8/1" },
  revision: 2, status: "open", dispatch_paused: false, created_at: 1, closed_at: null,
  archived_at: null, deleted_at: null, lifecycle_revision: 0,
});
const identity = { principal_id: "local", generation: 1, legacy: true, target_keys: ["target-http"], workspaces: [{ workspace_id: "local", role: "admin", approve: true }] };
/** 账号网关配置（wire 格式响应体）：default 指向推理模型，与 host 会话冻结模型不同。 */
const baseDocument = () => ({
  revision: 1,
  gateways: [{
    gateway_id: "gw", name: "团队网关", base_url: "https://gateway.example.com/v1", wire_api: "chat_completions", api_key_set: true, api_key_env: null,
    models: [
      { id: "host-model", display_name: "宿主模型", allow_images: true, max_output_tokens: null, reasoning_levels: {} },
      { id: "custom-model", display_name: "自定义模型", allow_images: false, max_output_tokens: 4096, reasoning_levels: { low: { display_name: "低", effort: "low", thinking: "enabled" } } },
    ],
  }],
  default_selection: { gateway_id: "gw", model_id: "custom-model", reasoning_level: "low" },
});
let gatewayDocument = baseDocument();
beforeEach(() => {
  localStorage.clear();
  history.replaceState({}, "", "/ui/?view=chat&workspace=local");
  calls.length = 0; override = undefined; gatewayDocument = baseDocument();
  container = document.createElement("div"); document.body.append(container); root = createRoot(container);
  (globalThis as unknown as { IS_REACT_ACT_ENVIRONMENT: boolean }).IS_REACT_ACT_ENVIRONMENT = true;
  vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, options: RequestInit = {}) => {
    const path = String(input), method = options.method ?? "GET", body: unknown = options.body ? JSON.parse(String(options.body)) : null;
    calls.push({ path, method, body });
    const overridden = override?.(path, options); if (overridden !== undefined) return overridden;
    if (path === "/healthz") return json({ http_enabled: false });
    if (path === "/v2/me") return json(identity);
    if (path === "/v2/me/profile") return json({ error: { code: "account_management_unavailable" } }, 409);
    if (path.startsWith("/v2/targets/")) return json({ error: { code: "not_found" } }, 404);
    if (path === "/v2/me/model-gateways") {
      if (method === "PUT") {
        const next = JSON.parse(String(options.body));
        gatewayDocument = { ...structuredClone(gatewayDocument), revision: 2, default_selection: next.default_selection ?? null };
        return json(gatewayDocument);
      }
      return json(gatewayDocument);
    }
    if (path === "/v2/workspaces/local/sessions?limit=50") return json([session()]);
    if (path === "/v2/workspaces/local/sessions/A") return json(session());
    if (path === "/v2/workspaces/local/sessions/A/turns?offset=0&limit=50") return json([]);
    if (path === "/v2/workspaces/local/sessions/A/turns/A1/observations") return json({ turn_id: "A1", session_id: "A", workspace_id: "local", requests: [] });
    if (path === "/v2/workspaces/local/sessions/A/turns") return json([]);
    if (path.endsWith("/model-selection")) {
      if (method === "PUT") {
        const parsed = JSON.parse(String(options.body)) as { request_key: string; expected_revision: number; model_selection: unknown };
        return json({ session_id: "A", revision: parsed.expected_revision + 1, model_selection: parsed.model_selection, configuration_revision: 2, model: parsed.model_selection ? (parsed.model_selection as { model_id: string }).model_id : "host-model", reasoning_effort: parsed.model_selection ? "low" : null, thinking: null });
      }
      return json({ session_id: "A", revision: 2, model_selection: null, configuration_revision: null, model: "host-model", reasoning_effort: null, thinking: null });
    }
    return json([]);
  }));
});
afterEach(async () => { await act(async () => root.unmount()); await act(async () => { await Promise.resolve(); }); container.remove(); vi.unstubAllGlobals(); });
async function until(check: () => void) {
  let error: unknown;
  for (let i = 0; i < 70; i++) { try { check(); return; } catch (value) { error = value; } await act(async () => { await new Promise(resolve => setTimeout(resolve, 0)); }); }
  throw error;
}
async function mount(strict = false, sessionId = "A") {
  history.replaceState({}, "", `/ui/?view=chat&workspace=local${sessionId ? `&session=${sessionId}` : ""}`);
  const tree = <ModelGatewaysProvider apiKey="key" identity={identity}><ChatWorkbench apiKey="key" active fence={{ current: 0 }} onLegacy={() => {}} onConnect={() => {}} /></ModelGatewaysProvider>;
  await act(async () => root.render(strict ? <StrictMode>{tree}</StrictMode> : tree));
  await until(() => expect(document.querySelector(".chat-controls")).toBeTruthy());
}
async function remount(strict = false, sessionId = "A") {
  await act(async () => root.unmount());
  container.remove();
  container = document.createElement("div"); document.body.append(container);
  root = createRoot(container);
  await mount(strict, sessionId);
}
async function submitDraft(text: string) {
  const prompt = document.getElementById("chat-prompt") as HTMLTextAreaElement;
  expect(prompt).toBeTruthy();
  await act(async () => {
    Object.getOwnPropertyDescriptor(HTMLTextAreaElement.prototype, "value")!.set!.call(prompt, text);
    prompt.dispatchEvent(new Event("input", { bubbles: true }));
  });
  await act(async () => (document.querySelector<HTMLButtonElement>("[aria-label='发送']")!).click());
}
function chip(kind: "model" | "extra") {
  const item = document.querySelector<HTMLButtonElement>(`.chat-controls-right .control-chip${kind === "model" ? ".is-strong" : ":not(.is-strong)"}`);
  expect(item, kind).toBeTruthy();
  return item!;
}
async function openModelMenu() { await act(async () => chip("model").click()); await until(() => expect(document.querySelector(".gateway-model-menu .gateway-menu-group")).toBeTruthy()); }
function menuChoice(label: string) {
  const item = [...document.querySelectorAll<HTMLButtonElement>(".gateway-model-menu [role='menuitemradio']")].find(button => button.textContent?.includes(label));
  expect(item, label).toBeTruthy();
  return item!;
}

test("宿主会话冻结模型优先于账号默认：GET 显示冻结模型，未选时不静默改默认", async () => {
  override = path => path.endsWith("/model-selection") ? json({ session_id: "A", revision: 2, model_selection: null, configuration_revision: null, model: "host-model", reasoning_effort: "high", thinking: "enabled" }) : undefined;
  await mount();
  // model_selection 为 null：chip 显示冻结模型（wire id），不回退账号默认。
  await until(() => expect(chip("model").textContent).toContain("host-model"));
  expect(chip("extra").textContent).toBe("Extra");
  await openModelMenu();
  expect(menuChoice("自定义模型").getAttribute("aria-checked")).toBe("false");
});

test("已有会话切换模型按 GET revision PUT，回执后 refresh 防旧 revision", async () => {
  let expectedRevision = 0, sentSelection: unknown = null, refreshes = 0;
  override = path => {
    if (!path.endsWith("/model-selection")) return undefined;
    if (calls.at(-1)?.method === "PUT") {
      const body = calls.at(-1)!.body as { request_key: string; expected_revision: number; model_selection: unknown };
      expectedRevision = body.expected_revision; sentSelection = body.model_selection;
      return json({ session_id: "A", revision: body.expected_revision + 1, model_selection: body.model_selection, configuration_revision: 3, model: "custom-model", reasoning_effort: "low", thinking: "enabled" });
    }
    return json({ session_id: "A", revision: 2, model_selection: null, configuration_revision: null, model: "host-model", reasoning_effort: null, thinking: null });
  };
  await mount();
  await until(() => expect(chip("model").textContent).toContain("host-model"));
  await openModelMenu();
  await act(async () => menuChoice("自定义模型").click());
  await until(() => expect(sentSelection).not.toBeNull());
  expect(expectedRevision).toBe(2);
  expect((sentSelection as { model_id: string }).model_id).toBe("custom-model");
  // PUT 推进 revision 后 chat.refresh 被调用，会话重读 revision 不再用旧值。
  await until(() => expect(calls.filter(call => call.path === "/v2/workspaces/local/sessions/A" && call.method === "GET").length).toBeGreaterThan(1));
  await until(() => expect(chip("model").textContent).toContain("自定义模型"));
  // chip 文案固定 Extra；推理档位标签在推理菜单里体现。
  await act(async () => chip("extra").click());
  await until(() => expect([...document.querySelectorAll<HTMLButtonElement>(".gateway-reasoning-menu [role='menuitemradio']")].some(button => button.textContent?.includes("低"))).toBe(true));
});

test("发送后会话 revision 优先于旧模型读取回执", async () => {
  let sentRevision: number | undefined;
  override = (path, options) => {
    if (path === "/v2/workspaces/local/sessions/A") return json({ ...session(), revision: 5 });
    if (!path.endsWith("/model-selection")) return undefined;
    if (options.method === "PUT") {
      const body = JSON.parse(String(options.body)); sentRevision = body.expected_revision;
      return json({ session_id: "A", revision: 6, model_selection: body.model_selection, configuration_revision: 1, model: "custom-model", reasoning_effort: "low", thinking: null });
    }
    return json({ session_id: "A", revision: 2, model_selection: null, configuration_revision: null, model: "host-model", reasoning_effort: null, thinking: null });
  };
  await mount();
  await until(() => expect(chip("model").textContent).toContain("host-model"));
  await openModelMenu();
  await act(async () => menuChoice("自定义模型").click());
  await until(() => expect(sentRevision).toBe(5));
});

test("已有会话切回主配置：菜单提供主配置模型，PUT model_selection null", async () => {
  override = path => {
    if (!path.endsWith("/model-selection")) return undefined;
    if (calls.at(-1)?.method === "PUT") {
      const body = calls.at(-1)!.body as { expected_revision: number; model_selection: unknown };
      return json({ session_id: "A", revision: body.expected_revision + 1, model_selection: body.model_selection, configuration_revision: 4, model: "host-model", reasoning_effort: null, thinking: null });
    }
    return json({ session_id: "A", revision: 2, model_selection: { gateway_id: "gw", model_id: "custom-model", reasoning_level: "low" }, configuration_revision: 2, model: "custom-model", reasoning_effort: "low", thinking: "enabled" });
  };
  await mount();
  await until(() => expect(chip("model").textContent).toContain("自定义模型"));
  await openModelMenu();
  expect(menuChoice("主配置模型").getAttribute("aria-checked")).toBe("false");
  await act(async () => menuChoice("主配置模型").click());
  await until(() => expect(calls.some(call => call.method === "PUT" && call.path.endsWith("/model-selection") && (call.body as { model_selection: unknown }).model_selection === null)).toBe(true));
  await until(() => expect(chip("model").textContent).toContain("host-model"));
});

test("只读身份、运行中与外部 owner 会话禁用切换", async () => {
  override = path => {
    if (path === "/v2/me") return json({ ...identity, workspaces: [{ workspace_id: "local", role: "viewer", approve: false }] });
    return undefined;
  };
  await mount();
  await until(() => expect(chip("model").disabled).toBe(true));
  expect(chip("extra").disabled).toBe(true);
  // 运行中：历史里有 running 轮。
  override = path => {
    if (path === "/v2/workspaces/local/sessions/A/turns?offset=0&limit=50") return json([{ id: "A1", session_id: "A", workspace_id: "local", owner_id: "local", ordinal: 1, request_key: "k", text: "t", accepted_revision: 0, status: "running", run_id: "r1", context_fingerprint: "1".repeat(64), transcript: null, code: null, created_at: 1, started_at: 1, finished_at: null }]);
    return undefined;
  };
  await remount();
  await until(() => expect(chip("model").disabled).toBe(true));
  // 外部 owner。
  override = path => {
    if (path === "/v2/workspaces/local/sessions?limit=50") return json([{ ...session(), owner_id: "someone-else" }]);
    if (path === "/v2/workspaces/local/sessions/A") return json({ ...session(), owner_id: "someone-else" });
    return undefined;
  };
  await remount();
  await until(() => expect(chip("model").disabled).toBe(true));
  expect(chip("extra").disabled).toBe(true);
});

test("新会话显式选择随 create 冻结，切换默认模型后 create 携带 wire selection", async () => {
  let created: { request_key: string; model_selection?: unknown } | null = null;
  override = (path, options) => {
    if (path === "/v2/workspaces/local/sessions" && options.method === "POST") {
      created = JSON.parse(String(options.body)) as { request_key: string; model_selection?: unknown };
      return json({ ...session(), id: "N", create_key: created.request_key, title: "从新会话创建" }, 201);
    }
    if (path === "/v2/workspaces/local/sessions?limit=50") return json(created ? [{ ...session(), id: "N", create_key: (created as { request_key: string }).request_key, title: "从新会话创建" }] : []);
    if (path === "/v2/workspaces/local/sessions/N") return json({ ...session(), id: "N", create_key: "create-N", title: "从新会话创建" });
    if (path === "/v2/workspaces/local/sessions/N/turns?offset=0&limit=50") return json([]);
    return undefined;
  };
  await remount(false, "");
  await until(() => expect(chip("model").textContent).toContain("自定义模型"));
  await openModelMenu();
  await act(async () => menuChoice("宿主模型").click());
  await submitDraft("从新会话创建");
  await until(() => expect(created).not.toBeNull());
  expect((created as { model_selection?: unknown }).model_selection).toEqual({ gateway_id: "gw", model_id: "host-model", reasoning_level: null });
});

test("新会话不选择时 create 不携带 model_selection", async () => {
  let created: { request_key: string; model_selection?: unknown } | null = null;
  override = (path, options) => {
    if (path === "/v2/workspaces/local/sessions" && options.method === "POST") {
      created = JSON.parse(String(options.body)) as { request_key: string; model_selection?: unknown };
      return json({ ...session(), id: "N", create_key: created.request_key, title: "默认模型的新会话" }, 201);
    }
    if (path === "/v2/workspaces/local/sessions?limit=50") return json(created ? [{ ...session(), id: "N", create_key: (created as { request_key: string }).request_key, title: "默认模型的新会话" }] : []);
    if (path === "/v2/workspaces/local/sessions/N") return json({ ...session(), id: "N", create_key: "create-N", title: "默认模型的新会话" });
    if (path === "/v2/workspaces/local/sessions/N/turns?offset=0&limit=50") return json([]);
    return undefined;
  };
  await remount(false, "");
  await until(() => expect(chip("model").textContent).toContain("自定义模型"));
  await submitDraft("默认模型的新会话");
  await until(() => expect(created).not.toBeNull());
  expect((created as { model_selection?: unknown }).model_selection).toBeUndefined();
});

test("新会话菜单使用默认模型清除覆盖：create 不携带 model_selection", async () => {
  let created: { request_key: string; model_selection?: unknown } | null = null;
  override = (path, options) => {
    if (path === "/v2/workspaces/local/sessions" && options.method === "POST") {
      created = JSON.parse(String(options.body)) as { request_key: string; model_selection?: unknown };
      return json({ ...session(), id: "N", create_key: created.request_key, title: "默认模型的新会话" }, 201);
    }
    if (path === "/v2/workspaces/local/sessions?limit=50") return json(created ? [{ ...session(), id: "N", create_key: (created as { request_key: string }).request_key, title: "默认模型的新会话" }] : []);
    if (path === "/v2/workspaces/local/sessions/N") return json({ ...session(), id: "N", create_key: "create-N", title: "默认模型的新会话" });
    if (path === "/v2/workspaces/local/sessions/N/turns?offset=0&limit=50") return json([]);
    return undefined;
  };
  await remount(false, "");
  await until(() => expect(chip("model").textContent).toContain("自定义模型"));
  await openModelMenu();
  await act(async () => menuChoice("宿主模型").click());
  await openModelMenu();
  await act(async () => menuChoice("使用默认模型").click());
  await submitDraft("默认模型的新会话");
  await until(() => expect(created).not.toBeNull());
  expect((created as { model_selection?: unknown }).model_selection).toBeUndefined();
});

test("无 Provider 旧组件测试保留原 info 弹出，不发网关请求", async () => {
  await act(async () => root.render(<ChatWorkbench apiKey="key" active fence={{ current: 0 }} onLegacy={() => {}} onConnect={() => {}} />));
  await until(() => expect(document.querySelector(".chat-controls")).toBeTruthy());
  const modelChip = [...document.querySelectorAll<HTMLButtonElement>(".chat-controls-right .control-chip")].find(button => button.classList.contains("is-strong"));
  expect(modelChip).toBeTruthy();
  await act(async () => modelChip!.click());
  await until(() => expect(document.querySelector(".popover-menu .control-info-content")).toBeTruthy());
  expect(calls.filter(call => call.path === "/v2/me/model-gateways")).toHaveLength(0);
});

test("StrictMode 双挂载只发一次 GET，跨会话 remote 不串显示", async () => {
  override = path => path.endsWith("/model-selection")
    ? json({ session_id: "A", revision: 2, model_selection: null, configuration_revision: null, model: "host-model", reasoning_effort: null, thinking: null })
    : undefined;
  await mount(true);
  await until(() => expect(chip("model").textContent).toContain("host-model"));
  const gets = calls.filter(call => call.path.endsWith("/model-selection") && call.method === "GET");
  expect(gets.length).toBe(1);
  override = path => path.endsWith("/sessions/B/model-selection")
    ? json({ session_id: "B", revision: 5, model_selection: { gateway_id: "gw", model_id: "custom-model", reasoning_level: "low" }, configuration_revision: 5, model: "custom-model", reasoning_effort: "low", thinking: "enabled" })
    : path.endsWith("/model-selection") ? json({ session_id: "A", revision: 2, model_selection: null, configuration_revision: null, model: "host-model", reasoning_effort: null, thinking: null }) : undefined;
  await remount(true, "B");
  await until(() => expect(chip("model").textContent).toContain("自定义模型"));
});
