import { expect, test } from "vitest";
import { parsePreviewBootstrap, registeredPreviews } from "../src/lib/development-previews";
import type { SessionState } from "../src/hooks/use-session";

test("preview bootstrap accepts only bounded single-use tickets and HTTPS preview actions", () => {
  const valid = { action: `https://p-${"a".repeat(40)}.preview.example.test/.krim-preview/session`, ticket: "t".repeat(43), expires_at: Date.now() / 1000 + 30 };
  expect(parsePreviewBootstrap(valid)).toEqual({ action: valid.action, ticket: valid.ticket });
  for (const action of ["javascript:alert(1)", valid.action.replace("https:", "http:"), valid.action + "?ticket=exposed", "https://unrelated.example.test/.krim-preview/session", valid.action.replace("/.krim-preview/session", "/arbitrary")]) expect(() => parsePreviewBootstrap({ ...valid, action })).toThrow();
  expect(() => parsePreviewBootstrap({ ...valid, expires_at: 1 })).toThrow();
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
