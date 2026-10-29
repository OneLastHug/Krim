import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, expect, test, vi } from "vitest";
import { webcrypto } from "node:crypto";
import { WorkspacePanel } from "../src/components/WorkspacePanel";
import type { SessionState } from "../src/hooks/use-session";
import { createCompletion, createInputSnapshot, validateTextFiles,
  type CompletionConditionDraft, type TextFileDraft } from "../src/lib/workspace-form";

const mocks = vi.hoisted(() => ({ work: {
  view: null as unknown, artifact: null as unknown, notice: "", pending: [] as unknown[], known: [] as unknown[],
  busy: false, scope: vi.fn(() => ({ principal_id: "local", generation: 1, workspace_id: "workspace-a",
    session_id: "session-a", target: "target-workspace" })), capture: vi.fn(() => () => true),
  recover: vi.fn(), recoverConfirmed: vi.fn(), submit: vi.fn(), input: vi.fn(), refresh: vi.fn(),
} }));
vi.mock("../src/hooks/use-workspace", () => ({ useWorkspace: () => mocks.work }));

let root: Root;
let container: HTMLDivElement;
function chat(id: string, canWrite = true): SessionState {
  return { actor: { principal_id: "local", generation: 1, target_keys: ["target-workspace"] },
    workspace: "workspace-a", id, canWrite,
    session: { id, target_key: "target-workspace", revision: 3, status: "open" },
  } as unknown as SessionState;
}
function render(id = "session-a", canWrite = true, active = true) {
  return root.render(<WorkspacePanel chat={chat(id, canWrite)} apiKey="synthetic-key"
    active={active} fence={{ current: 1 }} />);
}
function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>(done => { resolve = done; });
  return { promise, resolve };
}
function input(id: string) { return document.getElementById(id) as HTMLInputElement; }
function fill(element: HTMLInputElement | HTMLTextAreaElement, value: string) {
  const proto = element instanceof HTMLTextAreaElement ? HTMLTextAreaElement.prototype : HTMLInputElement.prototype;
  Object.getOwnPropertyDescriptor(proto, "value")!.set!.call(element, value);
  element.dispatchEvent(new Event("input", { bubbles: true }));
}
async function until(check: () => void) {
  let error: unknown;
  for (let index = 0; index < 30; index++) {
    try { check(); return; } catch (value) { error = value;
      await act(async () => { await new Promise(resolve => setTimeout(resolve, 0)); }); }
  }
  throw error;
}
function button(label: string) {
  const value = [...container.querySelectorAll("button")].find(item => item.textContent?.trim() === label);
  expect(value).toBeTruthy(); return value!;
}

beforeEach(() => {
  vi.stubGlobal("crypto", webcrypto);
  mocks.work.view = null; mocks.work.artifact = null; mocks.work.notice = "";
  mocks.work.pending = []; mocks.work.known = []; mocks.work.busy = false;
  mocks.work.scope.mockReset().mockReturnValue({ principal_id: "local", generation: 1,
    workspace_id: "workspace-a", session_id: "session-a", target: "target-workspace" });
  mocks.work.capture.mockReset().mockReturnValue(() => true);
  vi.clearAllMocks();
  container = document.createElement("div"); document.body.append(container); root = createRoot(container);
  (globalThis as unknown as { IS_REACT_ACT_ENVIRONMENT: boolean }).IS_REACT_ACT_ENVIRONMENT = true;
});
afterEach(async () => {
  await act(async () => root.unmount());
  container.remove(); vi.unstubAllGlobals();
});

const file = (id: string, path: string, content: string): TextFileDraft => ({ id, path, content });

test("input_snapshot_respects_path_duplicate_file_and_total_limits", async () => {
  expect(() => validateTextFiles([file("a", "../outside.txt", "x")])).toThrow("相对路径无效");
  expect(() => validateTextFiles([file("a", ".env", "x")])).toThrow("相对路径无效");
  expect(() => validateTextFiles([file("a", "folder\\\\secret.txt", "x")])).toThrow("相对路径无效");
  expect(() => validateTextFiles([file("a", "same.txt", ""), file("b", "same.txt", "")]))
    .toThrow("文件路径重复");
  expect(() => validateTextFiles(Array.from({ length: 33 }, (_, index) => file(String(index), `${index}.txt`, ""))))
    .toThrow("最多可提交 32 个文件");
  expect(() => validateTextFiles([file("a", "large.txt", "x".repeat(32769))])).toThrow("单个文件");
  expect(() => validateTextFiles([file("a", "a.txt", "x".repeat(16385)), file("b", "b.txt", "y".repeat(16384))]))
    .toThrow("文件总大小");
  expect((await createInputSnapshot([file("a", "exact.txt", "x".repeat(32768))], 0)).files).toHaveLength(1);
});

test("input_snapshot_encodes_unicode_as_utf8_base64_with_matching_sha", async () => {
  const content = "Krim Agent — 你好 🌏";
  const result = await createInputSnapshot([file("a", "src/说明.txt", content)], 7);
  const bytes = Uint8Array.from(atob(result.files[0]!.data), value => value.charCodeAt(0));
  expect(new TextDecoder().decode(bytes)).toBe(content);
  expect(result.files[0]!.sha256).toBe("d6ba10b6b2736a2e297e96ea19923b5d1fa775896a496da1af409224e5a33f36");
  expect(result.expected_head_revision).toBe(7);
});

test("completion_forms_keep_the_protocol_for_json_and_python_checks", () => {
  const conditions: CompletionConditionDraft[] = [
    { uid: "one", id: "json-result", kind: "json_equal", path: "result.json", pointer: "/status",
      expectedText: '"ok"', symbolsText: "" },
    { uid: "two", id: "python-result", kind: "ast_symbols", path: "src/result.py", pointer: "ignored",
      expectedText: "null", symbolsText: "main\nResult" },
  ];
  const result = createCompletion(["result.json"], conditions);
  expect(result).toEqual({ profile: "python-unittest/1", required_paths: ["result.json"], conditions: [
    { id: "json-result", kind: "json_equal", path: "result.json", pointer: "/status", expected: "ok", symbols: [] },
    { id: "python-result", kind: "ast_symbols", path: "src/result.py", pointer: "", expected: null, symbols: ["main", "Result"] },
  ] });
  expect(() => createCompletion([], Array.from({ length: 17 }, (_, index) => ({ ...conditions[0]!, uid: `${index}`, id: `c${index}` }))))
    .toThrow();
  expect(() => createCompletion(["same.txt", "same.txt"], [])).toThrow();
  expect(() => createCompletion(Array.from({ length: 16 }, (_, index) => `file-${index}.txt`), [conditions[0]!]))
    .toThrow();
});

test("head_read_uses_the_current_artifact_route_and_never_posts", async () => {
  const sessionId = "11111111-1111-4111-8111-111111111111";
  const value = { revision_id: "22222222-2222-4222-8222-222222222222", session_id: sessionId,
    parent_id: "33333333-3333-4333-8333-333333333333", revision: 8, source_kind: "input",
    request_key: "head-eight", request_hash: "a".repeat(64), manifest: [],
    manifest_hash: "b".repeat(64), producer_run_id: null, producer_call_id: null };
  const requests: { path: string; method: string }[] = [];
  mocks.work.scope.mockReturnValue({ principal_id: "local", generation: 1, workspace_id: "workspace-a",
    session_id: sessionId, target: "target-workspace" });
  vi.stubGlobal("fetch", vi.fn(async (path: RequestInfo | URL, init: RequestInit = {}) => {
    requests.push({ path: String(path), method: init.method ?? "GET" });
    return new Response(JSON.stringify(value), { status: 200, headers: { "Content-Type": "application/json" } });
  }));
  await act(async () => { render(sessionId); });
  expect(input("workspace-input-revision").value).toBe("");
  expect(button("读取当前输入版本").disabled).toBe(false);
  await act(async () => { button("读取当前输入版本").click(); });
  await until(() => expect(input("workspace-input-revision").value).toBe("8"));
  expect(requests).toEqual([{ path: "/v3/workspaces/workspace-a/sessions/"+sessionId+
    "/artifacts/head", method: "GET" }]);
  expect(mocks.work.input).not.toHaveBeenCalled();
});

test("late_head_read_after_session_switch_does_not_fill_the_new_identity", async () => {
  const sessionA = "11111111-1111-4111-8111-111111111111";
  const sessionB = "44444444-4444-4444-8444-444444444444";
  const response = deferred<Response>();
  const value = { revision_id: "22222222-2222-4222-8222-222222222222", session_id: sessionA,
    parent_id: "33333333-3333-4333-8333-333333333333", revision: 8, source_kind: "input",
    request_key: "stale-head", request_hash: "a".repeat(64), manifest: [],
    manifest_hash: "b".repeat(64), producer_run_id: null, producer_call_id: null };
  mocks.work.scope.mockReturnValue({ principal_id: "local", generation: 1,
    workspace_id: "workspace-a", session_id: sessionA, target: "target-workspace" });
  vi.stubGlobal("fetch", vi.fn(() => response.promise));
  await act(async () => { render(sessionA); });
  await act(async () => { button("读取当前输入版本").click(); });
  await act(async () => { render(sessionB); });
  response.resolve(new Response(JSON.stringify(value), { status: 200,
    headers: { "Content-Type": "application/json" } }));
  await act(async () => { await response.promise; await new Promise(resolve => setTimeout(resolve, 0)); });
  expect(input("workspace-input-revision").value).toBe("");
  expect(container.textContent).not.toContain("当前文件版本身份不符");
});

test("unreadable_head_never_falls_back_to_zero", async () => {
  vi.stubGlobal("fetch", vi.fn(async () => new Response(JSON.stringify({ error: { code: "not_found" } }),
    { status: 404, headers: { "Content-Type": "application/json" } })));
  await act(async () => { render(); });
  await act(async () => { button("读取当前输入版本").click(); });
  await until(() => expect(container.textContent).toContain("无法读取当前工作区文件版本"));
  expect(input("workspace-input-revision").value).toBe("");
  expect(mocks.work.input).not.toHaveBeenCalled();
});

test("permission_is_checked_by_submit_handlers_not_only_disabled_buttons", async () => {
  await act(async () => { render("session-a", false); });
  expect(input("workspace-file-import").disabled).toBe(true);
  expect(input("workspace-input-revision").disabled).toBe(true);
  expect(button("读取当前输入版本").disabled).toBe(false);
  await act(async () => {
    document.getElementById("workspace-input-form")!.dispatchEvent(new Event("submit", { bubbles: true, cancelable: true }));
    document.getElementById("workspace-turn-form")!.dispatchEvent(new Event("submit", { bubbles: true, cancelable: true }));
    await Promise.resolve();
  });
  expect(mocks.work.input).not.toHaveBeenCalled();
  expect(mocks.work.submit).not.toHaveBeenCalled();
});

test("late_file_read_after_session_switch_never_enters_new_snapshot", async () => {
  await act(async () => { render("session-a"); });
  const read = deferred<string>();
  const imported = new File(["old identity"], "old.txt", { type: "text/plain" });
  Object.defineProperty(imported, "text", { value: () => read.promise });
  const picker = input("workspace-file-import");
  Object.defineProperty(picker, "files", { configurable: true, value: [imported] });
  await act(async () => { picker.dispatchEvent(new Event("change", { bubbles: true })); });
  await act(async () => { mocks.work.artifact = { revision: 1, revision_id: "stale-revision", manifest: [] };
    render("session-b"); });
  await act(async () => { read.resolve("belongs to session A"); await read.promise; await Promise.resolve(); });
  expect(container.textContent).toContain("尚未添加文件");
  expect(container.textContent).toContain("产物版本 1");
  expect(container.textContent).not.toContain("old.txt");
  expect(input("workspace-input-revision").value).toBe("");
  await act(async () => { fill(input("workspace-input-revision"), "0"); });
  await act(async () => {
    document.getElementById("workspace-input-form")!.dispatchEvent(new Event("submit", { bubbles: true, cancelable: true }));
    await Promise.resolve();
  });
  expect(mocks.work.input).toHaveBeenCalledOnce();
  expect(mocks.work.input.mock.calls[0]![0].expected_head_revision).toBe(0);
  expect(mocks.work.input.mock.calls[0]![0].files).toEqual([]);
});

test("late_hash_after_deactivation_is_discarded_without_submitting", async () => {
  const digest = deferred<ArrayBuffer>();
  vi.stubGlobal("crypto", { randomUUID: webcrypto.randomUUID.bind(webcrypto),
    subtle: { digest: vi.fn(() => digest.promise) } });
  await act(async () => { render("session-a"); });
  const addFile = [...container.querySelectorAll("button")].find(button => button.textContent === "添加空文件");
  expect(addFile).toBeTruthy();
  await act(async () => { addFile!.click(); });
  await act(async () => { fill(input("workspace-input-revision"), "0"); });
  await act(async () => {
    document.getElementById("workspace-input-form")!.dispatchEvent(new Event("submit", { bubbles: true, cancelable: true }));
    await Promise.resolve();
  });
  await act(async () => { render("session-a", true, false); });
  digest.resolve(new Uint8Array(32).buffer);
  await act(async () => { await digest.promise; await Promise.resolve(); });
  expect(mocks.work.input).not.toHaveBeenCalled();
});

test("recovery_actions_pass_the_original_intent_and_request_key", async () => {
  const intent = { id: "intent-id", kind: "input", body: { request_key: "original-input-key" } };
  const known = { id: "known-id", kind: "turn", request_key: "original-turn-key", identity: "turn-id" };
  mocks.work.pending = [intent]; mocks.work.known = [known];
  await act(async () => { render(); });
  const find = [...container.querySelectorAll("button")].find(button => button.textContent === "查找原工作请求");
  const recover = [...container.querySelectorAll("button")].find(button => button.textContent === "读取原工作对象");
  expect(find).toBeTruthy(); expect(recover).toBeTruthy();
  await act(async () => { find!.click(); recover!.click(); });
  expect(mocks.work.recover).toHaveBeenCalledWith(intent);
  expect(mocks.work.recoverConfirmed).toHaveBeenCalledWith(known);
  expect(known.request_key).toBe("original-turn-key");
});
