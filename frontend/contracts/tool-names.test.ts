import { expect, test } from "vitest";
import { KNOWN_TOOL_NAMES, canonicalToolName, displayToolName } from "../src/lib/tool-names";
import { registeredPreviews } from "../src/lib/development-previews";
import type { SessionState } from "../src/hooks/use-session";

test("已知旧名显示为迁移后的新名，新名原样显示", () => {
  for (const [oldName, newName] of Object.entries(KNOWN_TOOL_NAMES)) {
    expect(displayToolName(oldName)).toBe(newName);
    expect(displayToolName(newName)).toBe(newName);
    expect(canonicalToolName(oldName)).toBe(canonicalToolName(newName));
  }
  expect(displayToolName("dev_shell")).toBe("shell");
  expect(displayToolName("dev_cancel")).toBe("stop_process");
  expect(displayToolName("dev_output")).toBe("read_output");
  expect(displayToolName("shell")).toBe("shell");
  expect(displayToolName("process_status")).toBe("process_status");
});

test("表外名称与未知 dev_* 原样显示，不粗暴剥前缀", () => {
  expect(displayToolName("docs.search_pages")).toBe("docs.search_pages");
  expect(displayToolName("dev_unknown_external")).toBe("dev_unknown_external");
  expect(displayToolName("dev_terminal")).toBe("dev_terminal");
  expect(displayToolName("")).toBe("");
  expect(canonicalToolName("dev_read")).toBe("read");
});

test("Object 原型键名不是工具名，原样返回", () => {
  for (const name of ["toString", "constructor", "__proto__", "hasOwnProperty", "valueOf"]) {
    expect(displayToolName(name)).toBe(name);
    expect(canonicalToolName(name)).toBe(name);
  }
});

const handle = { preview_id: "preview-1", environment: { owner_id: "p", project_id: "project" }, port: 3000, state: "ready" };
const turn = { id: "turn", session_id: "session", workspace_id: "local", owner_id: "p", ordinal: 0, request_key: "r", text: "task", accepted_revision: 0, status: "running", run_id: "run", context_fingerprint: null, transcript: null, code: null, created_at: 1, started_at: 1, finished_at: null };

function previewChat(toolName: string): SessionState {
  const events = [
    { run_id: "run", seq: 0, kind: "tool_start", data: { call_id: "c", provider_call_id: "pc", step_index: 0, name: toolName, arguments: {} } },
    { run_id: "run", seq: 1, kind: "tool_end", data: { call_id: "c", provider_call_id: "pc", step_index: 0, name: toolName, arguments: {}, status: "ok", result_available: true, value: handle } },
  ];
  return { actor: { principal_id: "p" }, history: [turn], details: {}, turnEvents: { turn: events }, turnFacts: {}, events: [] } as unknown as SessionState;
}

test("Outputs 预览识别新旧名称，未知名称不产生入口", () => {
  const expected = [{ preview_id: "preview-1", project_id: "project", port: 3000, state: "ready" }];
  expect(registeredPreviews(previewChat("dev_preview_register"), "project")).toEqual(expected);
  expect(registeredPreviews(previewChat("preview_register"), "project")).toEqual(expected);
  expect(registeredPreviews(previewChat("dev_preview_status"), "project")).toEqual(expected);
  expect(registeredPreviews(previewChat("preview_status"), "project")).toEqual(expected);
  expect(registeredPreviews(previewChat("register"), "project")).toEqual([]);
  expect(registeredPreviews(previewChat("dev_status"), "project")).toEqual([]);
});
