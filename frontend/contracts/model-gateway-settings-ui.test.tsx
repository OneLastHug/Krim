import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, expect, test, vi } from "vitest";
import { ModelGatewaysProvider } from "../src/hooks/use-model-gateways";
import { ModelGatewaySettings } from "../src/components/ModelGatewaySettings";
import type { Actor } from "../src/lib/session-contracts";

let root: Root, host: HTMLDivElement;
let saved: Record<string, unknown> | null;
let fail = false;
let gatewayDocument: Record<string, unknown>;
let puts: Record<string, unknown>[];
let discoverBodies: Record<string, unknown>[];
let discoverFailure: Error | null;
let discoverPayload: unknown;
const actor: Actor = { principal_id: "owner", generation: 1, legacy: false, target_keys: ["target-http"], workspaces: [{ workspace_id: "local", role: "admin", approve: true }] };
const endpointModels = [
  { id: "vendor/model-alpha", display_name: "模型 Alpha", allow_images: true, max_output_tokens: 8192, reasoning_levels: { low: { display_name: "低", effort: "low", thinking: null } } },
  { id: "vendor/model-beta", display_name: "模型 Beta", allow_images: false, max_output_tokens: null, reasoning_levels: {} },
  { id: "vendor/gamma-pro", display_name: "Gamma Pro", allow_images: false, max_output_tokens: null, reasoning_levels: {} },
];
const json = (value: unknown, status = 200) => new Response(JSON.stringify(value), { status, headers: { "Content-Type": "application/json" } });
function button(text: string) { const result = [...document.querySelectorAll<HTMLButtonElement>("button")].find(b => b.textContent?.trim() === text); expect(result, text).toBeTruthy(); return result!; }
function checkbox(label: string) { const input = document.querySelector<HTMLInputElement>(`input[aria-label='${label}']`); expect(input, label).toBeTruthy(); return input!; }
async function click(element: HTMLElement) { await act(async () => element.click()); }
async function fill(selector: string, value: string) {
  const input = document.querySelector<HTMLInputElement>(selector)!; expect(input, selector).toBeTruthy();
  await act(async () => { Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, "value")!.set!.call(input, value); input.dispatchEvent(new Event("input", { bubbles: true })); });
}
async function choose(selector: string, value: string) {
  const select = document.querySelector<HTMLSelectElement>(selector)!; expect(select, selector).toBeTruthy();
  await act(async () => { Object.getOwnPropertyDescriptor(HTMLSelectElement.prototype, "value")!.set!.call(select, value); select.dispatchEvent(new Event("change", { bubbles: true })); });
}
async function submitDraft() { await act(async () => document.querySelector<HTMLFormElement>(".mp-editor")!.dispatchEvent(new Event("submit", { bubbles: true, cancelable: true }))); }
async function until(check: () => void) { for (let i = 0; i < 80; i++) { try { check(); return; } catch (error) { if (i === 79) throw error; } await act(async () => { await new Promise(resolve => setTimeout(resolve, 0)); }); } }
beforeEach(() => {
  saved = null; fail = false; discoverFailure = null; discoverPayload = endpointModels;
  gatewayDocument = { revision: 0, gateways: [], default_selection: null };
  puts = []; discoverBodies = []; localStorage.clear(); sessionStorage.clear();
  host = document.createElement("div"); document.body.append(host); root = createRoot(host);
  (globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean }).IS_REACT_ACT_ENVIRONMENT = true;
  vi.stubGlobal("ResizeObserver", class { observe() {} disconnect() {} unobserve() {} });
  vi.stubGlobal("fetch", vi.fn(async (_path: string, options: RequestInit = {}) => {
    const method = options.method ?? "GET";
    const body = options.body ? JSON.parse(String(options.body)) : null;
    if (method === "PUT") {
      puts.push(body!);
      if (fail) throw new TypeError("Failed to fetch");
      saved = body;
      return json({ revision: 1, gateways: (saved!.gateways as Record<string, unknown>[]).map(g => { const { api_key: _key, clear_api_key: _clear, ...rest } = g; return { ...rest, api_key_set: true }; }), default_selection: saved!.default_selection });
    }
    if (method === "POST") {
      discoverBodies.push(body!);
      if (discoverFailure) throw discoverFailure;
      return json({ models: discoverPayload });
    }
    return json(gatewayDocument);
  }));
});
afterEach(async () => { await act(async () => root.unmount()); host.remove(); vi.unstubAllGlobals(); });
async function mount(identity = actor) { await act(async () => root.render(<ModelGatewaysProvider apiKey="fixture-account" identity={identity}><ModelGatewaySettings/></ModelGatewaysProvider>)); await until(() => expect(button("添加供应商").disabled).toBe(false)); }
async function createDraft() {
  await mount(); await click(button("添加供应商"));
  await until(() => expect(document.querySelector(".mp-custom-preset")).toBeTruthy());
  await click(document.querySelector<HTMLElement>(".mp-custom-preset")!);
  await fill("#gateway-name", "真实接口供应商"); await fill("#gateway-url", "https://provider.example/v1"); await fill("#gateway-key", "fixture-write-only-secret");
  await click(button("添加模型")); await fill("[data-model-id]", "vendor/model:latest"); await fill("[data-model-name]", "推理模型");
}

test("approved provider editor submits one real PUT containing models and no browser credential storage", async () => {
  await createDraft();
  await act(async () => document.querySelector<HTMLFormElement>(".mp-editor")!.dispatchEvent(new Event("submit", { bubbles: true, cancelable: true })));
  await until(() => expect(document.querySelector(".mp-provider-dialog")).toBeNull());
  expect(saved).not.toBeNull();
  const gateway = (saved!.gateways as Record<string, unknown>[])[0]!;
  expect(gateway.name).toBe("真实接口供应商"); expect(gateway.api_key).toBe("fixture-write-only-secret");
  expect((gateway.models as Record<string, unknown>[])[0]?.id).toBe("vendor/model:latest");
  expect(document.querySelector(".mp-provider-name")?.textContent).toContain("真实接口供应商");
  expect(JSON.stringify(localStorage)).not.toContain("fixture-write-only-secret");
});

test("network failure retains the edit draft and requests explicit reload", async () => {
  await createDraft(); fail = true;
  await act(async () => document.querySelector<HTMLFormElement>(".mp-editor")!.dispatchEvent(new Event("submit", { bubbles: true, cancelable: true })));
  await until(() => expect(document.querySelector(".mp-error")?.textContent).toContain("重新读取"));
  expect(document.querySelector<HTMLInputElement>("#gateway-name")?.value).toBe("真实接口供应商");
  expect(button("保存").disabled).toBe(true);
});

test("discovery fetches endpoint models with a draft POST and never issues a PUT", async () => {
  await createDraft();
  await click(button("获取端点模型"));
  await until(() => expect(document.querySelector(".mp-discovery")).toBeTruthy());
  expect(document.querySelector(".mp-discovery-head")?.textContent).toContain("3");
  expect(puts).toHaveLength(0);
  expect(discoverBodies[0]).toEqual({ base_url: "https://provider.example/v1", wire_api: "chat_completions", expected_revision: 0, api_key: "fixture-write-only-secret", api_key_env: null });
  await submitDraft();
  await until(() => expect(document.querySelector(".mp-provider-dialog")).toBeNull());
  expect((saved!.gateways as Record<string, unknown>[])[0]?.models).toHaveLength(1);
});

test("endpoint models can be searched, selected, added to the draft only, then saved", async () => {
  await createDraft();
  await click(button("获取端点模型"));
  await until(() => expect(document.querySelector(".mp-discovery")).toBeTruthy());
  await click(checkbox("选择模型 vendor/model-alpha"));
  await click(checkbox("选择模型 vendor/model-beta"));
  expect(document.querySelector(".mp-discovery-selection")?.textContent).toContain("已选 2 个");
  await click(button("添加所选（2）"));
  await until(() => expect(document.querySelectorAll(".mp-model-entry")).toHaveLength(3));
  expect(document.querySelector(".mp-discovery-selection")?.textContent).toContain("已选 0 个");
  expect(button("添加所选").disabled).toBe(true);
  expect(puts).toHaveLength(0);
  await fill("input[aria-label='搜索端点模型']", "beta");
  expect(document.querySelectorAll(".mp-discovery-row")).toHaveLength(1);
  await fill("input[aria-label='搜索端点模型']", "没有匹配的词");
  expect(document.querySelector(".mp-discovery-list")?.textContent).toContain("没有匹配的模型");
  await fill("input[aria-label='搜索端点模型']", " ");
  const addedRows = document.querySelectorAll<HTMLElement>(".mp-discovery-row.is-added");
  expect(addedRows).toHaveLength(2);
  for (const row of addedRows) {
    expect(row.textContent).toContain("已添加");
    expect(row.querySelector<HTMLInputElement>("input")!.disabled).toBe(true);
    expect(row.querySelector<HTMLInputElement>("input")!.checked).toBe(true);
  }
  await click(button("选择搜索结果"));
  expect(document.querySelector(".mp-discovery-selection")?.textContent).toContain("已选 1 个");
  await click(button("清空选择"));
  expect(document.querySelector(".mp-discovery-selection")?.textContent).toContain("已选 0 个");
  await submitDraft();
  await until(() => expect(document.querySelector(".mp-provider-dialog")).toBeNull());
  const models = ((saved!.gateways as Record<string, unknown>[])[0]!.models) as Record<string, unknown>[];
  expect(models.map(model => model.id)).toEqual(["vendor/model:latest", "vendor/model-alpha", "vendor/model-beta"]);
  expect(models[1]).toMatchObject({ display_name: "模型 Alpha", allow_images: true, max_output_tokens: 8192, reasoning_levels: { low: { display_name: "低", effort: "low", thinking: null } } });
  expect(models[2]).toMatchObject({ display_name: "模型 Beta", allow_images: false, max_output_tokens: null });
  expect(JSON.stringify(localStorage)).not.toContain("fixture-write-only-secret");
});

test("already-added models stay out of the picker and duplicate IDs are rejected on save", async () => {
  await createDraft();
  await click(button("获取端点模型"));
  await until(() => expect(document.querySelector(".mp-discovery")).toBeTruthy());
  await click(checkbox("选择模型 vendor/model-alpha"));
  await click(button("添加所选（1）"));
  await until(() => expect(document.querySelectorAll(".mp-discovery-row.is-added")).toHaveLength(1));
  await click(button("选择搜索结果"));
  expect(document.querySelector(".mp-discovery-selection")?.textContent).toContain("已选 2 个");
  await click(button("清空选择"));
  await click(button("添加模型"));
  await fill("[data-model-id]", "vendor/model-alpha");
  await submitDraft();
  await until(() => expect(document.querySelector(".mp-error")?.textContent).toContain("此供应商下的模型 ID 重复"));
  expect(document.querySelector(".mp-provider-dialog")).toBeTruthy();
  expect(puts).toHaveLength(0);
  expect(button("保存").disabled).toBe(false);
});

test("a failed discovery shows a retryable error without blocking saving", async () => {
  await createDraft();
  discoverFailure = new TypeError("Failed to fetch");
  await click(button("获取端点模型"));
  await until(() => expect(document.querySelector(".mp-discovery-error")?.textContent).toBe("无法获取模型，请检查网络后重试。"));
  expect(document.querySelector(".mp-editor-notice")).toBeNull();
  expect(button("保存").disabled).toBe(false);
  discoverFailure = null;
  await click(button("获取端点模型"));
  await until(() => expect(document.querySelector(".mp-discovery")).toBeTruthy());
  expect(document.querySelector(".mp-discovery-error")).toBeNull();
  await submitDraft();
  await until(() => expect(document.querySelector(".mp-provider-dialog")).toBeNull());
  expect(puts).toHaveLength(1);
});

test("saving an existing provider without retyping the key reuses the stored credential", async () => {
  gatewayDocument = { revision: 3, gateways: [{ gateway_id: "gw", name: "已有供应商", base_url: "https://gw.example.com/v1", wire_api: "chat_completions", api_key_set: true, api_key_env: "PROVIDER_KEY", models: [{ id: "gw-model", display_name: "网关模型", allow_images: false, max_output_tokens: null, reasoning_levels: {} }] }], default_selection: null };
  await mount();
  await click(document.querySelector<HTMLButtonElement>(".mp-provider-row")!);
  await until(() => expect(document.getElementById("gateway-key")).toBeTruthy());
  await click(button("获取端点模型"));
  await until(() => expect(document.querySelector(".mp-discovery")).toBeTruthy());
  expect(discoverBodies[0]).toEqual({ base_url: "https://gw.example.com/v1", wire_api: "chat_completions", expected_revision: 3, gateway_id: "gw", api_key_env: "PROVIDER_KEY" });
  await submitDraft();
  await until(() => expect(document.querySelector(".mp-provider-dialog")).toBeNull());
  const gateway = (saved!.gateways as Record<string, unknown>[])[0]!;
  expect(gateway.api_key).toBeUndefined();
  expect(gateway.clear_api_key).toBeUndefined();
  expect(gateway.api_key_env).toBe("PROVIDER_KEY");
});

test("Responses hides thinking presets and existing thinking levels must be switched to follow manually", async () => {
  await createDraft();
  await click(document.querySelector<HTMLButtonElement>(".mp-reasoning-toggle")!);
  await until(() => expect(document.querySelector(".mp-reasoning")).toBeTruthy());
  const chatOptions = [...document.querySelectorAll<HTMLSelectElement>("select[aria-label='推理参数预设']")[0]!.options].map(option => option.value);
  expect(chatOptions).toEqual(["common", "complete", "thinking", "thinking-effort"]);
  await choose("select[aria-label='推理参数预设']", "thinking");
  await click(button("添加预设档位"));
  await until(() => expect(document.querySelectorAll(".mp-level-entry")).toHaveLength(2));
  expect(document.querySelector(".mp-level-preview")?.textContent).toBe('{"thinking":{"type":"enabled"}}');
  await click(button("Responses"));
  expect([...document.querySelector<HTMLSelectElement>("select[aria-label='推理参数预设']")!.options].map(option => option.value)).toEqual(["common", "complete"]);
  await submitDraft();
  await until(() => expect(document.querySelector(".mp-error")?.textContent).toContain("Responses 不支持 thinking"));
  await choose("select[aria-label='档位 1 thinking']", "omit");
  await choose("select[aria-label='档位 2 thinking']", "omit");
  const previews = document.querySelectorAll<HTMLElement>(".mp-level-preview");
  expect(previews[0]?.textContent).toBe("不发送额外推理参数");
  expect(previews[1]?.textContent).toBe("不发送额外推理参数");
  await submitDraft();
  await until(() => expect(document.querySelector(".mp-provider-dialog")).toBeNull());
  const gateway = (saved!.gateways as Record<string, unknown>[])[0]!;
  expect(gateway.wire_api).toBe("responses");
  expect(((gateway.models as Record<string, unknown>[])[0]!).reasoning_levels).toEqual({
    "thinking-on": { display_name: "开启思考", effort: null, thinking: null },
    "thinking-off": { display_name: "关闭思考", effort: null, thinking: null },
  });
});
