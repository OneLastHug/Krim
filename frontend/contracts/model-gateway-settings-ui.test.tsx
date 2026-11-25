import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, expect, test, vi } from "vitest";
import { ModelGatewaysProvider } from "../src/hooks/use-model-gateways";
import { ModelGatewaySettings } from "../src/components/ModelGatewaySettings";
import type { Actor } from "../src/lib/session-contracts";

let root: Root, host: HTMLDivElement;
let saved: Record<string, unknown> | null;
let fail = false;
const actor: Actor = { principal_id: "owner", generation: 1, legacy: false, target_keys: ["target-http"], workspaces: [{ workspace_id: "local", role: "admin", approve: true }] };
const json = (value: unknown, status = 200) => new Response(JSON.stringify(value), { status, headers: { "Content-Type": "application/json" } });
function button(text: string) { const result = [...document.querySelectorAll<HTMLButtonElement>("button")].find(b => b.textContent?.trim() === text); expect(result, text).toBeTruthy(); return result!; }
async function click(element: HTMLElement) { await act(async () => element.click()); }
async function fill(selector: string, value: string) {
  const input = document.querySelector<HTMLInputElement>(selector)!; expect(input, selector).toBeTruthy();
  await act(async () => { Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, "value")!.set!.call(input, value); input.dispatchEvent(new Event("input", { bubbles: true })); });
}
async function until(check: () => void) { for (let i = 0; i < 80; i++) { try { check(); return; } catch (error) { if (i === 79) throw error; } await act(async () => { await new Promise(resolve => setTimeout(resolve, 0)); }); } }
beforeEach(() => {
  saved = null; fail = false; localStorage.clear(); sessionStorage.clear();
  host = document.createElement("div"); document.body.append(host); root = createRoot(host);
  (globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean }).IS_REACT_ACT_ENVIRONMENT = true;
  vi.stubGlobal("ResizeObserver", class { observe() {} disconnect() {} unobserve() {} });
  vi.stubGlobal("fetch", vi.fn(async (_path: string, options: RequestInit = {}) => {
    if (options.method === "PUT") {
      if (fail) throw new TypeError("Failed to fetch");
      saved = JSON.parse(String(options.body));
      return json({ revision: 1, gateways: (saved!.gateways as Record<string, unknown>[]).map(g => { const { api_key: _key, clear_api_key: _clear, ...rest } = g; return { ...rest, api_key_set: true }; }), default_selection: saved!.default_selection });
    }
    return json({ revision: 0, gateways: [], default_selection: null });
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
