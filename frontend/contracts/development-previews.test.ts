import { expect, test } from "vitest";
import { parsePreviewBootstrap, PREVIEW_FRAME_SANDBOX, previewTargetName, registeredPreviews } from "../src/lib/development-previews";
import type { SessionState } from "../src/hooks/use-session";

test("preview bootstrap accepts only bounded single-use tickets and HTTPS preview actions", () => {
  const valid = { action: `https://p-${"a".repeat(40)}.preview.example.test/.krim-preview/session`, ticket: "t".repeat(43), expires_at: Date.now() / 1000 + 30 };
  expect(parsePreviewBootstrap(valid)).toEqual({ action: valid.action, ticket: valid.ticket, allow_embedding: false });
  const sameSite = { ...valid, action: `https://preview-${"b".repeat(40)}.example.test/.krim-preview/session` };
  expect(parsePreviewBootstrap(sameSite)).toEqual({ action: sameSite.action, ticket: sameSite.ticket, allow_embedding: false });
  // 嵌入能力是服务端逐次授权的布尔位；缺省回退新窗口，类型不对的值拒绝。
  expect(parsePreviewBootstrap({ ...valid, allow_embedding: true })).toEqual({ action: valid.action, ticket: valid.ticket, allow_embedding: true });
  expect(() => parsePreviewBootstrap({ ...valid, allow_embedding: "yes" })).toThrow();
  for (const action of ["javascript:alert(1)", valid.action.replace("https:", "http:"), valid.action + "?ticket=exposed", "https://unrelated.example.test/.krim-preview/session", valid.action.replace("/.krim-preview/session", "/arbitrary"), sameSite.action.replace("b".repeat(40), "b".repeat(39))]) expect(() => parsePreviewBootstrap({ ...valid, action })).toThrow();
  expect(() => parsePreviewBootstrap({ ...valid, expires_at: 1 })).toThrow();
});

test("frame target names are unique and the sandbox stays minimal", () => {
  const names = new Set(Array.from({ length: 32 }, () => previewTargetName()));
  expect(names.size).toBe(32);
  expect([...names].every(name => name.startsWith("krim-preview-"))).toBe(true);
  expect(PREVIEW_FRAME_SANDBOX).toBe("allow-scripts allow-same-origin allow-forms allow-downloads");
});
test("only successful same-project tool outputs become preview entries", () => {
  const handle = { preview_id: "preview-1", environment: { owner_id: "p", project_id: "project" }, port: 3000, state: "ready" };
  const turn = { id: "turn", session_id: "session", workspace_id: "local", owner_id: "p", ordinal: 0, request_key: "r", text: "task", accepted_revision: 0, status: "running", run_id: "run", context_fingerprint: null, transcript: null, code: null, created_at: 1, started_at: 1, finished_at: null };
  const events = [
    { run_id: "run", seq: 0, kind: "tool_start", data: { call_id: "c", provider_call_id: "pc", step_index: 0, name: "dev_preview_register", arguments: {} } },
    { run_id: "run", seq: 1, kind: "tool_end", data: { call_id: "c", provider_call_id: "pc", step_index: 0, name: "dev_preview_register", arguments: {}, status: "ok", result_available: true, value: handle } },
  ];
  const chat = { actor: { principal_id: "p" }, history: [turn], details: {}, turnEvents: { turn: events }, turnFacts: {}, events: [] } as unknown as SessionState;
  expect(registeredPreviews(chat, "project")).toEqual([{ preview_id: "preview-1", project_id: "project", port: 3000, state: "ready" }]);
  expect(registeredPreviews(chat, "other-project")).toEqual([]);
  expect(registeredPreviews({ ...chat, actor: { ...chat.actor!, principal_id: "foreign" } }, "project")).toEqual([]);
});

test("model prose cannot introduce navigation targets", () => {
  const chat = { actor: { principal_id: "p" }, history: [], details: {}, turnEvents: {}, turnFacts: {}, events: [] } as unknown as SessionState;
  expect(registeredPreviews(chat, "project")).toEqual([]);
});

// ---- 按端口收敛：最新注册优先，旧 ID 事件不回切 ----

const baseTurn = { id: "turn", session_id: "session", workspace_id: "local", owner_id: "p", ordinal: 0, request_key: "r", text: "task", accepted_revision: 0, status: "running", run_id: "run", context_fingerprint: null, transcript: null, code: null, created_at: 1, started_at: 1, finished_at: null };
function handle(id: string, port: number, state: string, generation = 1) {
  return { preview_id: id, environment: { owner_id: "p", project_id: "project", environment_id: "env", generation }, service_id: id, port, state, access_path: `/api/development/previews/${id}/open` };
}
function failedStatus(id: string, code: string) {
  return { ok: false, preview_id: id, code, next_action: "Inspect read_output and read the target." };
}
function previewChat(calls: { name: string; value: unknown }[]): SessionState {
  const turnEvents: Record<string, unknown[]> = {};
  const history = calls.map((call, index) => {
    turnEvents[`turn-${index}`] = [
      { run_id: "run", seq: 0, kind: "tool_start", data: { call_id: `c${index}`, provider_call_id: `pc${index}`, step_index: 0, name: call.name, arguments: {} } },
      { run_id: "run", seq: 1, kind: "tool_end", data: { call_id: `c${index}`, provider_call_id: `pc${index}`, step_index: 0, name: call.name, arguments: {}, status: "ok", result_available: true, value: call.value } },
    ];
    return { ...baseTurn, id: `turn-${index}`, ordinal: index };
  });
  return { actor: { principal_id: "p" }, history, details: {}, turnEvents, turnFacts: {}, events: [] } as unknown as SessionState;
}

test("same port keeps only the newest registration; superseded IDs cannot return", () => {
  const chat = previewChat([
    { name: "preview_register", value: handle("preview-old", 3000, "ready") },
    { name: "preview_register", value: handle("preview-new", 3000, "pending") },
    // 旧 ID 迟到的状态与停止事件不得覆盖新注册，也不得把端口切回旧 ID。
    { name: "preview_status", value: handle("preview-old", 3000, "ready") },
    { name: "preview_stop", value: handle("preview-old", 3000, "stopped") },
    // 当前登记 ID 的状态事件正常生效。
    { name: "preview_status", value: handle("preview-new", 3000, "ready") },
  ]);
  expect(registeredPreviews(chat, "project")).toEqual([{ preview_id: "preview-new", project_id: "project", port: 3000, state: "ready" }]);
});

test("different ports stay separate rows in registration order", () => {
  const chat = previewChat([
    { name: "preview_register", value: handle("preview-a", 3000, "ready") },
    { name: "preview_register", value: handle("preview-b", 8000, "pending") },
  ]);
  expect(registeredPreviews(chat, "project")).toEqual([
    { preview_id: "preview-a", project_id: "project", port: 3000, state: "ready" },
    { preview_id: "preview-b", project_id: "project", port: 8000, state: "pending" },
  ]);
});

test("stopping the current preview then re-registering the port shows the new registration", () => {
  const chat = previewChat([
    { name: "preview_register", value: handle("preview-a", 3000, "ready") },
    { name: "preview_stop", value: handle("preview-a", 3000, "stopped") },
    { name: "preview_register", value: handle("preview-b", 3000, "pending") },
  ]);
  expect(registeredPreviews(chat, "project")).toEqual([{ preview_id: "preview-b", project_id: "project", port: 3000, state: "pending" }]);
});

test("failed status probes degrade only the current registration with explicit codes", () => {
  // 记录确已死亡（not_found / preview_inactive）→ stopped。
  expect(registeredPreviews(previewChat([
    { name: "preview_register", value: handle("preview-a", 3000, "ready") },
    { name: "preview_status", value: failedStatus("preview-a", "preview_inactive") },
  ]), "project")).toEqual([{ preview_id: "preview-a", project_id: "project", port: 3000, state: "stopped" }]);
  // 暂时不可判定（路由未就绪/不可用等）→ unknown。
  expect(registeredPreviews(previewChat([
    { name: "preview_register", value: handle("preview-a", 3000, "ready") },
    { name: "preview_status", value: failedStatus("preview-a", "preview_route_unavailable") },
  ]), "project")).toEqual([{ preview_id: "preview-a", project_id: "project", port: 3000, state: "unknown" }]);
  // 未列明的错误码（如授权失败）与未登记 ID 不改动任何条目。
  expect(registeredPreviews(previewChat([
    { name: "preview_register", value: handle("preview-a", 3000, "ready") },
    { name: "preview_status", value: failedStatus("preview-a", "permission_denied") },
    { name: "preview_status", value: failedStatus("preview-ghost", "preview_inactive") },
  ]), "project")).toEqual([{ preview_id: "preview-a", project_id: "project", port: 3000, state: "ready" }]);
  // 失效只降级活跃状态：旧 ID 的失效探测不影响新注册；显式停止后不再被改写。
  expect(registeredPreviews(previewChat([
    { name: "preview_register", value: handle("preview-a", 3000, "ready") },
    { name: "preview_register", value: handle("preview-b", 3000, "pending") },
    { name: "preview_status", value: failedStatus("preview-a", "preview_inactive") },
  ]), "project")).toEqual([{ preview_id: "preview-b", project_id: "project", port: 3000, state: "pending" }]);
  expect(registeredPreviews(previewChat([
    { name: "preview_register", value: handle("preview-b", 3000, "ready") },
    { name: "preview_stop", value: handle("preview-b", 3000, "stopped") },
    { name: "preview_status", value: failedStatus("preview-b", "preview_not_ready") },
  ]), "project")).toEqual([{ preview_id: "preview-b", project_id: "project", port: 3000, state: "stopped" }]);
});

test("a later environment generation takes over the port; other projects never mix in", () => {
  const chat = previewChat([
    { name: "preview_register", value: handle("preview-g1", 3000, "ready", 1) },
    { name: "preview_register", value: handle("preview-g2", 3000, "ready", 2) },
  ]);
  expect(registeredPreviews(chat, "project")).toEqual([{ preview_id: "preview-g2", project_id: "project", port: 3000, state: "ready" }]);
  const mixed = previewChat([
    { name: "preview_register", value: handle("preview-mine", 3000, "ready") },
    { name: "preview_register", value: { ...handle("preview-other", 3001, "ready"), environment: { owner_id: "p", project_id: "elsewhere", environment_id: "env", generation: 1 } } },
  ]);
  expect(registeredPreviews(mixed, "project")).toEqual([{ preview_id: "preview-mine", project_id: "project", port: 3000, state: "ready" }]);
});
