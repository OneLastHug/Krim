import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, expect, test, vi } from "vitest";
import { ModelGatewaysProvider, useModelGateways, type ModelGatewayContext } from "../src/hooks/use-model-gateways";
import { gatewayUpdate, modelKey, parseGatewayDocument } from "../src/lib/model-gateways";
import type { Actor } from "../src/lib/session-contracts";
import { INTENTS, prepareIntent, readIntents } from "../src/lib/session-requests";

const actor: Actor = { principal_id: "account-a", generation: 1, legacy: false, workspaces: [{ workspace_id: "local", role: "admin", approve: true }], target_keys: ["target-http"] };
const documentOf = (revision = 1) => ({ revision, gateways: [{ gateway_id: "gw", name: "团队网关", base_url: "https://api.example.com/v1", wire_api: "chat_completions", api_key_set: true, api_key_env: "PROVIDER_KEY", models: [{ id: "vendor/model:latest", display_name: "推理模型", allow_images: true, max_output_tokens: 4096, reasoning_levels: { deep: { display_name: "深度", effort: "custom.deep", thinking: "enabled" } } }] }], default_selection: { gateway_id: "gw", model_id: "vendor/model:latest", reasoning_level: "deep" } });
const json = (body: unknown, status = 200) => new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });
let root: Root, host: HTMLDivElement, context: ModelGatewayContext | null;
let calls: { method: string; path: string; body: Record<string, unknown> | null; auth: string }[];
let respond: (method: string, body: Record<string, unknown> | null, auth: string) => Response | Promise<Response>;
function Probe() { context = useModelGateways(); return <span>{context?.revision}</span>; }
async function tick() { await act(async () => { await new Promise(resolve => setTimeout(resolve, 0)); }); }
async function until(check: () => void) { for (let i = 0; i < 80; i++) { try { check(); return; } catch (error) { if (i === 79) throw error; } await tick(); } }
async function mount(key = "account-key", identity = actor) { await act(async () => root.render(<ModelGatewaysProvider apiKey={key} identity={identity}><Probe/></ModelGatewaysProvider>)); await until(() => expect(context?.loading).toBe(false)); }
beforeEach(() => {
  localStorage.clear(); sessionStorage.clear(); context = null; calls = [];
  host = document.createElement("div"); document.body.append(host); root = createRoot(host);
  (globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean }).IS_REACT_ACT_ENVIRONMENT = true;
  respond = () => json(documentOf());
  vi.stubGlobal("fetch", vi.fn(async (path: string, options: RequestInit = {}) => {
    const method = options.method ?? "GET", body = options.body ? JSON.parse(String(options.body)) : null, auth = new Headers(options.headers).get("Authorization") ?? "";
    calls.push({ method, path: String(path), body, auth }); return respond(method, body, auth);
  }));
});
afterEach(async () => { await act(async () => root.unmount()); host.remove(); vi.unstubAllGlobals(); });

test("session creation retries preserve the chosen model and reject credential-shaped selections", () => {
  const scope = { principal_id: "account-a", generation: 1, workspace_id: "local", session_id: null };
  const selection = { gateway_id: "gw", model_id: "vendor/model:latest", reasoning_level: "deep" };
  const first = prepareIntent(localStorage, scope, "session", { title: "任务", target_key: "target-http", model_selection: selection });
  selection.model_id = "changed-after-save";
  const retry = prepareIntent(localStorage, scope, "session", { title: "任务", target_key: "target-http", model_selection: { gateway_id: "other", model_id: "other", reasoning_level: null } });
  expect(retry.first).toBe(false);
  expect(retry.intent.body.model_selection?.model_id).toBe("vendor/model:latest");
  expect(retry.intent.body.request_key).toBe(first.intent.body.request_key);
  const raw = JSON.parse(localStorage.getItem(INTENTS)!);
  raw[0].body.model_selection.api_key = "must-not-be-stored-here";
  localStorage.setItem(INTENTS, JSON.stringify(raw));
  expect(() => readIntents(localStorage)).toThrow("未确认请求");
});

test("maps model identifiers, reasoning labels and defaults without exposing credentials", () => {
  const parsed = parseGatewayDocument(documentOf());
  expect(parsed.models[0]?.id).toBe(modelKey("gw", "vendor/model:latest"));
  expect(parsed.models[0]?.levels[0]).toEqual({ id: "deep", label: "深度", effort: "custom.deep", thinking: "enabled" });
  const body = gatewayUpdate(parsed, "save-key");
  expect(body.default_selection).toEqual(documentOf().default_selection);
  expect(body.gateways[0]?.api_key_env).toBe("PROVIDER_KEY");
  expect(body.gateways[0]).not.toHaveProperty("api_key");
  expect(body.gateways[0]?.models[0]?.reasoning_levels.deep?.display_name).toBe("深度");
});

test("writes a provider and all models atomically, never storing provider keys locally", async () => {
  await mount();
  respond = () => json(documentOf(2));
  await act(async () => context!.saveProvider(context!.gateways[0]!, context!.models, "fixture-write-only-key"));
  expect(calls.filter(c => c.method === "PUT")).toHaveLength(1);
  const call = calls.find(c => c.method === "PUT")!;
  expect(call.path).toBe("/v2/me/model-gateways");
  expect(call.auth).toBe("Bearer account-key");
  expect(call.body?.expected_revision).toBe(1);
  const gateway = (call.body?.gateways as Record<string, unknown>[])[0]!;
  expect(gateway.api_key).toBe("fixture-write-only-key");
  expect(gateway.api_key_env).toBeNull();
  expect(context?.revision).toBe(2);
  expect(localStorage.length).toBe(0);
  expect(sessionStorage.length).toBe(0);
  expect(JSON.stringify(context)).not.toContain("fixture-write-only-key");
});

test("unknown save result blocks duplicate writes until an explicit reload", async () => {
  await mount();
  respond = method => { if (method === "PUT") throw new TypeError("Failed to fetch"); return json(documentOf(2)); };
  await act(async () => { await expect(context!.setDefaultSelection({ modelId: null, levelId: null })).rejects.toThrow(); });
  await act(async () => { await expect(context!.setDefaultSelection({ modelId: null, levelId: null })).rejects.toThrow("重新读取"); });
  expect(calls.filter(c => c.method === "PUT")).toHaveLength(1);
  await act(async () => context!.reload());
  expect(context?.revision).toBe(2);
  respond = () => json({ ...documentOf(3), default_selection: null });
  await act(async () => context!.setDefaultSelection({ modelId: null, levelId: null }));
  expect(calls.filter(c => c.method === "PUT")).toHaveLength(2);
  expect(context?.selection.modelId).toBeNull();
});

test("read-only accounts cannot submit configuration changes", async () => {
  await mount("viewer-key", { ...actor, workspaces: [{ workspace_id: "local", role: "viewer", approve: false }] });
  await expect(context!.setDefaultSelection({ modelId: null, levelId: null })).rejects.toThrow("权限");
  expect(calls.filter(c => c.method === "PUT")).toHaveLength(0);
});

test("switching account cannot publish a previous account's in-flight read", async () => {
  let release!: (value: Response) => void;
  respond = (_method, _body, auth) => auth === "Bearer account-key" ? new Promise(resolve => { release = resolve; }) : json({ revision: 0, gateways: [], default_selection: null });
  await act(async () => root.render(<ModelGatewaysProvider apiKey="account-key" identity={actor}><Probe/></ModelGatewaysProvider>));
  await mount("other-key", { ...actor, principal_id: "account-b" });
  await act(async () => release(json(documentOf(9))));
  expect(context?.revision).toBe(0);
  expect(context?.gateways).toEqual([]);
});
