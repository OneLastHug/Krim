import { act, StrictMode } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, expect, test, vi } from "vitest";
import { App } from "../src/App";
import { webcrypto } from "node:crypto";
import { inputRequestHash, type InputSnapshotRequest, type WorkspaceTurnRequest } from "../src/lib/workspace-contracts";
import { readWorkIntents, WORK_KNOWN, WORK_INTENTS } from "../src/lib/workspace-requests";

let root: Root, container: HTMLDivElement;
const calls: { path: string; method: string; body: unknown }[] = [];
let override: ((path: string, options: RequestInit) => Promise<Response> | Response | undefined) | undefined;
const releases: (() => void)[] = [];
const json = (body: unknown, status = 200) => new Response(JSON.stringify(body), { status,
  headers: { "Content-Type": "application/json" } });
beforeEach(() => {
  localStorage.clear(); history.replaceState({}, "", "/ui/?view=chat"); calls.length = 0; override = undefined;
  vi.stubGlobal("crypto", webcrypto);
  container = document.createElement("div"); document.body.append(container); root = createRoot(container);
  (globalThis as unknown as { IS_REACT_ACT_ENVIRONMENT: boolean }).IS_REACT_ACT_ENVIRONMENT = true;
  vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, options: RequestInit = {}) => {
    const path = String(input), method = options.method ?? "GET";
    calls.push({ path, method, body: options.body ? JSON.parse(String(options.body)) : null });
    const value = override?.(path, options); if (value !== undefined) return value;
    if (path === "/healthz") return json({ http_enabled: false });
    if (path === "/v2/me") return json({ principal_id: "local", generation: 1, legacy: true,
      target_keys: ["demo"], workspaces: [{ workspace_id: "local", role: "admin", approve: true }] });
    return json([]);
  }));
});
afterEach(async () => {
  await act(async () => root.unmount());
  await act(async () => { for (const release of releases.splice(0)) release();
    await new Promise(resolve => setTimeout(resolve, 0)); });
  container.remove(); vi.unstubAllGlobals();
});
async function until(check: () => void) {
  let error: unknown;
  for (let index = 0; index < 60; index++) {
    try { check(); return; } catch (value) { error = value;
      await act(async () => { await new Promise(resolve => setTimeout(resolve, 0)); }); }
  }
  throw error;
}
function button(label: string) {
  const value = [...document.querySelectorAll("button")].find(item => item.getAttribute("aria-label") === label || item.textContent?.trim() === label);
  expect(value).toBeTruthy(); return value!;
}
// 侧栏「新建对话」按钮：aria-label 与聊天工具栏同名按钮重复，需容器限定。
function createButton() {
  const item = document.querySelector<HTMLButtonElement>(".sidebar-buttons [aria-label='新建对话']");
  expect(item, "新建对话").toBeTruthy(); return item!;
}
function menuItem(label: string) {
  const item = [...document.querySelectorAll<HTMLButtonElement>("[role='menuitem']")].find(value => value.textContent?.trim() === label);
  expect(item, label).toBeTruthy(); return item!;
}
// 连接入口移到侧栏账户菜单：密钥身份 → 「连接 / 切换密钥」→ Dialog。
async function openAccountMenu() {
  await act(async () => (document.querySelector('[aria-label="密钥身份"]') as HTMLButtonElement).click());
  await until(() => expect(document.querySelector(".popover-menu[aria-label='密钥身份']")).toBeTruthy());
}

test("workspace_unavailable_is_truthful_and_zero_effect", async () => {
  await act(async () => root.render(<StrictMode><App /></StrictMode>));
  await until(() => expect(createButton().disabled).toBe(false));
  await act(async () => createButton().click());
  await until(() => expect(document.body.textContent).toContain("工作区未配置"));
  const option = document.querySelector('option[value="target-workspace"]') as HTMLOptionElement;
  expect(option?.disabled).toBe(true);
  expect(calls.filter(item => item.method === "POST")).toEqual([]);
  expect(container.textContent).not.toContain("核验通过");
});

const A = "11111111-1111-4111-8111-111111111111", B = "22222222-2222-4222-8222-222222222222";
const revisionId = "33333333-3333-4333-8333-333333333333", turnId = "44444444-4444-4444-8444-444444444444";
const hash = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824";
function session(id: string) {
  return { id, workspace_id: "local", owner_id: "local", create_key: "session", title: id === A ? "工作 A" : "工作 B",
    target_key: "target-workspace", target_fingerprint: "0".repeat(64), context_policy: {
      max_bytes: 131072, max_messages: 256, reserved_output_tokens: 2048, counter_version: "approx-utf8/1" },
    revision: 0, status: "open", dispatch_paused: false, created_at: 1, closed_at: null };
}
function deferred() {
  let release!: () => void; const promise = new Promise<void>(resolve => { release = resolve; });
  releases.push(release); return { promise, release };
}
function workspaceServer(kind: "turn" | "input", barrier: "post" | "get" | "none") {
  const hold = deferred(), v2 = "/v2/workspaces/local/sessions", base = "/v3/workspaces/local/sessions/"+A;
  let accepted: WorkspaceTurnRequest | InputSnapshotRequest | null = null, finds = 0;
  const work = () => ({ schema_version: 1, session_id: A, turn_id: turnId, run_id: null,
    request_key: accepted!.request_key, original_request: accepted, requested_input: {
      mode: "head_at_dispatch", revision_id: null }, resolved_input_revision: null, phase: "queued",
    plan: { revision: 0, steps: [] }, compactions: [], effects: [], verification: null, effective_fingerprint: null });
  const turn = () => ({ id: turnId, session_id: A, workspace_id: "local", owner_id: "local", ordinal: 1,
    request_key: accepted!.request_key, text: (accepted as WorkspaceTurnRequest).text, accepted_revision: 0,
    status: "queued", run_id: null, context_fingerprint: null, transcript: null, code: null,
    created_at: 1, started_at: null, finished_at: null });
  const artifact = async () => ({ revision_id: revisionId, session_id: A, parent_id: A, revision: 1,
    source_kind: "input", request_key: accepted!.request_key,
    request_hash: await inputRequestHash(accepted), manifest: (accepted as InputSnapshotRequest).files.map(file => ({
      path: file.path, size: atob(file.data).length, sha256: file.sha256 })), manifest_hash: hash,
    producer_run_id: null, producer_call_id: null });
  override = (path, options) => {
    if (path === "/v2/me") return json({ principal_id: "local", generation:
      (options.headers as Record<string, string> | undefined)?.Authorization === "Bearer rotated-synthetic" ? 2 : 1, legacy: true,
      target_keys: ["demo", "target-workspace"], workspaces: [{ workspace_id: "local", role: "admin", approve: true }] });
    if (path === v2+"?limit=50") return json([session(A), session(B)]);
    if (path === v2+"/"+A || path === v2+"/"+B) return json(session(path.endsWith(A) ? A : B));
    if (path.startsWith(v2) && path.includes("/turns?")) return json([]);
    if (path.endsWith("/work")) return json({ error: { code: "not_found" } }, 404);
    const operation = kind === "turn" ? "/turns" : "/inputs";
    if (path === base+operation && options.method === "POST") {
      accepted = JSON.parse(String(options.body)) as WorkspaceTurnRequest | InputSnapshotRequest;
      if (barrier === "get") return Promise.reject(new TypeError("accepted response lost"));
      return (async () => { if (barrier === "post") await hold.promise;
        return json(kind === "turn" ? turn() : await artifact(), kind === "turn" ? 202 : 200); })();
    }
    if (path.startsWith(base+operation+"/by-key/")) {
      finds++; return (async () => { if (barrier === "get" && finds === 1) await hold.promise;
        return json(kind === "turn" ? work() : await artifact()); })();
    }
    return undefined;
  };
  return { hold, get accepted() { return accepted; }, get finds() { return finds; } };
}
async function typeElement(element: HTMLInputElement | HTMLTextAreaElement, value: string) {
  await act(async () => {
    const proto = element instanceof HTMLTextAreaElement ? HTMLTextAreaElement.prototype : HTMLInputElement.prototype;
    Object.getOwnPropertyDescriptor(proto, "value")!.set!.call(element, value);
    element.dispatchEvent(new Event("input", { bubbles: true }));
  });
}
async function type(id: string, value: string) {
  const element = document.getElementById(id) as HTMLInputElement | HTMLTextAreaElement | null;
  expect(element).toBeTruthy();
  await typeElement(element!, value);
}
async function fillLabel(scope: ParentNode, labelText: string, value: string) {
  const label = [...scope.querySelectorAll("label")].find(item => item.textContent?.trim() === labelText);
  expect(label).toBeTruthy();
  const element = document.getElementById(label!.htmlFor) as HTMLInputElement | HTMLTextAreaElement | null;
  expect(element).toBeTruthy();
  await typeElement(element!, value);
}
async function addRequiredPath(path: string) {
  await type("workspace-required-path", path);
  await act(async () => button("添加路径").click());
}
async function setTurnChecks(text: string) {
  while (document.querySelector(".workspace-check-list li")) {
    const remove = document.querySelector(".workspace-check-list li button") as HTMLButtonElement;
    await act(async () => remove.click());
  }
  while (document.querySelector(".workspace-condition-list li")) {
    const remove = document.querySelector(".workspace-condition-list li .workspace-condition-title button") as HTMLButtonElement;
    await act(async () => remove.click());
  }
  if (text.startsWith("D4_NEXT")) return;
  await addRequiredPath("data.json");
  await act(async () => button("添加完成条件").click());
  const condition = document.querySelector(".workspace-condition")!;
  await fillLabel(condition, "条件名称", "nested");
  await fillLabel(condition, "检查文件", "data.json");
  await fillLabel(condition, "JSON Pointer", "/value");
  await fillLabel(condition, "预期 JSON 值", JSON.stringify({ list: [false, null, { nested: [1, 2] }] }));
}
async function setInputFile(path: string, content: string) {
  await type("workspace-input-revision", "0");
  await act(async () => button("添加空文件").click());
  const editor = document.querySelector(".workspace-file-editor")!;
  await typeElement(editor.querySelector("input") as HTMLInputElement, path);
  await typeElement(editor.querySelector("textarea") as HTMLTextAreaElement, content);
}
async function mountWorkspace() {
  history.replaceState({}, "", "/ui/?view=chat&workspace=local&session="+A);
  await act(async () => root.render(<StrictMode><App /></StrictMode>));
  await until(() => expect((document.getElementById("workspace-text") as HTMLTextAreaElement)?.disabled).toBe(false));
}
async function send(kind: "turn" | "input", twice = false) {
  if (kind === "turn") {
    await type("workspace-text", "完整工作请求");
    await setTurnChecks("完整工作请求");
  } else {
    await setInputFile("hello.txt", "hello");
  }
  await act(async () => { button(kind === "turn" ? "发送工作请求" : "提交输入快照").click();
    if (twice) button(kind === "turn" ? "发送工作请求" : "提交输入快照").click(); });
}

for (const kind of ["turn", "input"] as const) for (const barrier of ["post", "get"] as const)
  for (const steering of ["other-return", "credential", "unmount"] as const) {
    test(`${kind}_${barrier}_late_${steering}_preserves_full_intent`, async () => {
      const server = workspaceServer(kind, barrier); await mountWorkspace(); await send(kind, true);
      await until(() => expect(server.accepted).not.toBeNull());
      const original = readWorkIntents(localStorage)[0]!;
      if (barrier === "get") {
        await until(() => expect(button("查找原工作请求")).toBeTruthy());
        await act(async () => button("查找原工作请求").click());
        await until(() => expect(server.finds).toBe(1));
      }
      if (steering === "other-return") {
        await act(async () => button("工作 B").click());
        await until(() => expect(new URL(location.href).searchParams.get("session")).toBe(B));
        await act(async () => button("工作 A").click());
      } else if (steering === "credential") {
        await openAccountMenu(); await act(async () => menuItem("连接 / 切换密钥").click()); await type("api-key", "rotated-synthetic");
        await act(async () => document.getElementById("connect-save")!.click());
      } else {
        await act(async () => root.unmount()); root = createRoot(container);
        await act(async () => root.render(<StrictMode><App /></StrictMode>));
      }
      await act(async () => { server.hold.release(); await new Promise(resolve => setTimeout(resolve, 5)); });
      expect(readWorkIntents(localStorage)).toEqual([original]);
      expect(localStorage.getItem(WORK_KNOWN)).toBeNull();
      expect(localStorage.getItem(WORK_INTENTS)).not.toContain("rotated-synthetic");
      expect(calls.filter(value => value.method === "POST")).toHaveLength(1);
      await until(() => expect(button("查找原工作请求")).toBeTruthy());
      await act(async () => button("查找原工作请求").click());
      await until(() => expect(readWorkIntents(localStorage)).toEqual([]));
      expect(JSON.parse(localStorage.getItem(WORK_KNOWN)!)[original.id].identity).toBe(
        kind === "turn" ? turnId : revisionId);
      expect(JSON.parse(localStorage.getItem(WORK_KNOWN)!)[original.id].generation).toBe(original.generation);
      expect(calls.filter(value => value.method === "POST")).toHaveLength(1);
      expect(calls.some(value => value.path.includes("/cancel"))).toBe(false);
    });
  }

for (const kind of ["turn", "input"] as const) test(`${kind}_durable_save_failure_keeps_original`, async () => {
  const server = workspaceServer(kind, "none"); await mountWorkspace();
  const original = Storage.prototype.setItem;
  const fault = vi.spyOn(Storage.prototype, "setItem").mockImplementation(function (this: Storage, key, value) {
    if (key === WORK_KNOWN) throw new DOMException("synthetic storage full");
    return original.call(this, key, value);
  });
  await send(kind, true); await until(() => expect(server.accepted).not.toBeNull());
  await until(() => expect(document.body.textContent).toContain("请求未确认"));
  expect(readWorkIntents(localStorage)).toHaveLength(1);
  expect(localStorage.getItem(WORK_KNOWN)).toBeNull(); fault.mockRestore();
  await act(async () => button("查找原工作请求").click());
  await until(() => expect(readWorkIntents(localStorage)).toEqual([]));
  expect(calls.filter(value => value.method === "POST")).toHaveLength(1);
});


function confirmedServer(kind: "turn" | "input", oldTurn = false) {
  const server = workspaceServer(kind, "none"), previous = override!;
  const base = "/v3/workspaces/local/sessions/"+A;
  override = (path, options) => {
    if (server.accepted && path === base+"/turns/"+turnId+"/work")
      return previous(base+"/turns/by-key/"+server.accepted.request_key, options);
    if (server.accepted && path === base+"/artifacts/"+revisionId)
      return previous(base+"/inputs/by-key/"+server.accepted.request_key, options);
    if (path === base+"/work" && oldTurn) {
      const body: WorkspaceTurnRequest = { schema_version: 1, request_key: "old-turn", text: "旧Turn",
        expected_revision: 0, input: { mode: "head_at_dispatch", revision_id: null },
        completion: { profile: "python-unittest/1", required_paths: [], conditions: [] } };
      return json({ schema_version: 1, session_id: A, turn_id: B, run_id: null,
        request_key: "old-turn", original_request: body, requested_input: body.input,
        resolved_input_revision: null, phase: "settled", plan: { revision: 0, steps: [] },
        compactions: [], effects: [], verification: null, effective_fingerprint: null });
    }
    return previous(path, options);
  };
  return server;
}
for (const mode of ["input-no-turn", "input-old-turn", "turn"] as const)
  for (const steering of ["refresh", "remount"] as const)
    test(`confirmed_${mode}_${steering}_GET_only_original_identity`, async () => {
      const kind = mode === "turn" ? "turn" : "input", server = confirmedServer(kind, mode === "input-old-turn");
      await mountWorkspace(); await send(kind);
      await until(() => expect(readWorkIntents(localStorage)).toEqual([]));
      await until(() => expect(localStorage.getItem(WORK_KNOWN)).not.toBeNull());
      const accepted = JSON.parse(localStorage.getItem(WORK_KNOWN)!);
      const record = Object.values(accepted)[0] as { request_key: string; identity: string; manifest_hash: string | null };
      expect(record.identity).toBe(kind === "turn" ? turnId : revisionId);
      expect(record.request_key).toBe(server.accepted!.request_key);
      const checkpoint = calls.length;
      if (steering === "refresh") await act(async () => button("刷新工作结果").click());
      else {
        await act(async () => root.unmount()); root = createRoot(container);
        await act(async () => root.render(<StrictMode><App /></StrictMode>));
      }
      if (kind === "input") await until(() => expect(document.querySelector('.workspace-artifact')?.textContent).toContain("hello.txt"));
      else await until(() => expect(document.querySelector('.workspace-facts')?.textContent).toContain("queued"));
      const reads = calls.slice(checkpoint);
      expect(reads.some(item => item.path === "/v3/workspaces/local/sessions/"+A+
        (kind === "input" ? "/artifacts/"+revisionId : "/turns/"+turnId+"/work"))).toBe(true);
      expect(reads.some(item => item.path.endsWith("/work") && !item.path.includes("/turns/"))).toBe(false);
      expect(calls.filter(item => item.method === "POST")).toHaveLength(1);
      expect(localStorage.getItem(WORK_KNOWN)).toBe(JSON.stringify(accepted));
      expect(readWorkIntents(localStorage)).toEqual([]);
      expect(calls.some(item => item.path.includes("/cancel"))).toBe(false);
    });


for (const corruption of ["unknown-field", "bad-id", "bad-kind", "over-capacity", "over-bytes"] as const)
  test(`confirmed_ledger_${corruption}_fails_closed_in_App`, async () => {
    confirmedServer("input"); await mountWorkspace(); await send("input");
    await until(() => { expect(localStorage.getItem(WORK_KNOWN)).not.toBeNull();
      expect(readWorkIntents(localStorage)).toEqual([]); });
    const ledger = JSON.parse(localStorage.getItem(WORK_KNOWN)!);
    const id = Object.keys(ledger)[0]!, item = ledger[id];
    if (corruption === "unknown-field") item.credential = "synthetic-never-read";
    if (corruption === "bad-id") item.identity = "../outside";
    if (corruption === "bad-kind") item.kind = "eval";
    if (corruption === "over-capacity") for (let index = 0; index < 129; index++)
      ledger["aaaaaaaa-aaaa-4aaa-8aaa-"+String(index).padStart(12, "0")] = item;
    const raw = corruption === "over-bytes" ? " ".repeat(131073) : JSON.stringify(ledger);
    localStorage.setItem(WORK_KNOWN, raw); const checkpoint = calls.length;
    await act(async () => root.unmount()); root = createRoot(container);
    await act(async () => root.render(<StrictMode><App /></StrictMode>));
    await until(() => expect(container.textContent).toContain("无法核实原工作身份"));
    expect(document.querySelector(".workspace-artifact")).toBeNull();
    expect(calls.slice(checkpoint).some(item => item.path.startsWith("/v3/"))).toBe(false);
    expect(localStorage.getItem(WORK_KNOWN)).toBe(raw);
    expect(calls.filter(item => item.method === "POST")).toHaveLength(1);
  });
for (const field of ["principal_id", "workspace_id", "session_id"] as const)
  test(`confirmed_other_${field}_never_crosses_scope`, async () => {
    confirmedServer("input"); await mountWorkspace(); await send("input");
    await until(() => { expect(localStorage.getItem(WORK_KNOWN)).not.toBeNull();
      expect(readWorkIntents(localStorage)).toEqual([]); });
    const ledger = JSON.parse(localStorage.getItem(WORK_KNOWN)!);
    ledger[Object.keys(ledger)[0]!][field] = field === "session_id" ? B : "other";
    localStorage.setItem(WORK_KNOWN, JSON.stringify(ledger)); const checkpoint = calls.length;
    await act(async () => button("刷新工作结果").click());
    await until(() => expect(container.textContent).toContain("无法核实原工作身份"));
    expect(document.querySelector(".workspace-artifact")).toBeNull();
    expect(document.querySelector(".workspace-known")).toBeNull();
    expect(calls.slice(checkpoint).some(item => item.path.includes("/artifacts/") || item.path.includes("/by-key/"))).toBe(false);
    expect(calls.filter(item => item.method === "POST")).toHaveLength(1);
  });
for (const steering of ["other-return", "credential", "unmount"] as const)
  test(`confirmed_GET_late_${steering}_is_fenced`, async () => {
    confirmedServer("input"); await mountWorkspace(); await send("input");
    await until(() => { expect(localStorage.getItem(WORK_KNOWN)).not.toBeNull();
      expect(readWorkIntents(localStorage)).toEqual([]); });
    const ledger = localStorage.getItem(WORK_KNOWN), hold = deferred(), previous = override!;
    let blocked = false;
    override = (path, options) => {
      if (!blocked && path.endsWith("/artifacts/"+revisionId)) {
        blocked = true;
        return (async () => { await hold.promise; return await previous(path, options)!; })();
      }
      return previous(path, options);
    };
    await act(async () => button("刷新工作结果").click());
    await until(() => expect(blocked).toBe(true));
    if (steering === "other-return") {
      await act(async () => button("工作 B").click());
      await until(() => expect(new URL(location.href).searchParams.get("session")).toBe(B));
      await act(async () => { hold.release(); await new Promise(resolve => setTimeout(resolve, 0)); });
      expect(document.querySelector(".workspace-artifact")).toBeNull();
      await act(async () => button("工作 A").click());
    } else if (steering === "credential") {
      await openAccountMenu(); await act(async () => menuItem("连接 / 切换密钥").click()); await type("api-key", "rotated-synthetic");
      await act(async () => document.getElementById("connect-save")!.click());
    } else {
      await act(async () => root.unmount()); root = createRoot(container);
      await act(async () => root.render(<StrictMode><App /></StrictMode>));
    }
    await act(async () => { hold.release(); await new Promise(resolve => setTimeout(resolve, 0)); });
    await until(() => expect(document.querySelector(".workspace-artifact")?.textContent).toContain("hello.txt"));
    expect(localStorage.getItem(WORK_KNOWN)).toBe(ledger);
    expect(localStorage.getItem(WORK_KNOWN)).not.toContain("rotated-synthetic");
    expect(calls.filter(item => item.method === "POST")).toHaveLength(1);
    expect(calls.some(item => item.path.includes("/cancel"))).toBe(false);
  });
