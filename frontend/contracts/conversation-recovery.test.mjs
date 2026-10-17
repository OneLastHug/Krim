/** Python原v1主键向量、坏来源/假回执和不能丢失的未知意图。 */
import assert from "node:assert/strict";
import { test } from "node:test";
import { checkedConversationRecord, recoveryMessageId, verifyConversationPreview } from "../../src/krim_agent/web/conversation-recovery-contract.js";
import { readConversationState, saveConversationState } from "../../src/krim_agent/web/conversation-recovery-storage.js";

const identity = { agent_id: "agent", session_id: "agent", topic_id: "topic" };
const source = { ...identity, session_revision: 3, available: true, source_path_hash: "a".repeat(64), source_branch_key: "b".repeat(64), source_run_id: "old-run", source_run_status: "interrupted", anchor_message_id: "prefix", excluded_message_ids: ["assistant", "answer"], pending_calls: [{ call_id: "pending", name: "echo", arguments_hash: "c".repeat(64), selectors: { operation_id: "old-operation", expected_revision: 0 } }], answered_calls: [{ call_id: "answered", message_id: "answer", message_hash: "d".repeat(64) }] };
const request = { source_run_id: source.source_run_id, expected_path_hash: source.source_path_hash, expected_session_revision: source.session_revision, request_key: "original-key" };
const record = { version: 1, kind: "conversation_recovery_branch", status: "context_recovered", recovery_message_id: "2b4aaabf-2200-5ccd-bb7a-5a1dadb69ec2", request, source, recovered_branch_key: "e".repeat(64), session_revision: 4, created_at: "2026-10-16T01:02:03.000001+08:00" };

test("Python规范JSON/UUID5原v1向量，含中文引号与非BMP键", async () => {
  assert.equal(await recoveryMessageId("topic", "original-key"), record.recovery_message_id);
  assert.equal(await recoveryMessageId("topic", '已审阅/键 " ß😀'), "4e6b277e-9de4-55a1-94ac-037fd7621ddb");
  assert.deepEqual(await checkedConversationRecord(record, identity, request, source), record);
});
for (const fault of ["scope", "session", "topic", "revision", "private", "status-array", "pending", "duplicate", "answered", "unsafe-selector", "boolean-revision", "limit", "hash-newline", "id-newline"]) test(`源快照拒绝${fault}`, () => {
  const value = structuredClone(source);
  if (fault === "scope") value.agent_id = "other";
  if (fault === "session") value.session_id = "other";
  if (fault === "topic") value.topic_id = "other";
  if (fault === "revision") value.session_revision = Number.MAX_SAFE_INTEGER + 1;
  if (fault === "private") value.owner_id = "private-owner";
  if (fault === "status-array") value.source_run_status = ["interrupted"];
  if (fault === "pending") value.available = false;
  if (fault === "duplicate") value.pending_calls.push(value.pending_calls[0]);
  if (fault === "answered") value.answered_calls[0].message_id = "not-the-original-answer";
  if (fault === "unsafe-selector") value.pending_calls[0].selectors.text = "不能变为指令";
  if (fault === "boolean-revision") value.pending_calls[0].selectors.expected_revision = true;
  if (fault === "limit") value.pending_calls = Array.from({ length: 65 }, (_, index) => ({ ...value.pending_calls[0], call_id: String(index) }));
  if (fault === "hash-newline") value.source_path_hash += "\n";
  if (fault === "id-newline") value.source_run_id += "\n";
  assert.throws(() => verifyConversationPreview(value, identity));
});
for (const fault of ["identifier", "key", "source-run", "hash", "revision", "done", "private", "date", "timezone", "version", "date-newline"]) test(`公共记录拒绝${fault}`, async () => {
  const value = structuredClone(record);
  if (fault === "identifier") value.recovery_message_id = "00000000-0000-5000-8000-000000000000";
  if (fault === "key") value.request.request_key = "different-key";
  if (fault === "source-run") value.source.source_run_id = "different-run";
  if (fault === "hash") value.request.expected_path_hash = "f".repeat(64);
  if (fault === "revision") value.session_revision = 5;
  if (fault === "done") value.status = "done";
  if (fault === "private") value.tool_result = { done: true };
  if (fault === "date") value.created_at = "2026-02-30T01:02:03Z";
  if (fault === "timezone") value.created_at = "2026-10-16T01:02:03";
  if (fault === "version") value.version = 2;
  if (fault === "date-newline") value.created_at += "\n";
  await assert.rejects(() => checkedConversationRecord(value, identity, request, source));
});
test("完整上下文无源、悬空引用或伪造已答回程", () => {
  const complete = { ...source, available: false, source_run_id: null, source_run_status: null, anchor_message_id: null, excluded_message_ids: [], pending_calls: [], answered_calls: [] };
  verifyConversationPreview(complete, identity);
  assert.throws(() => verifyConversationPreview({ ...complete, answered_calls: source.answered_calls }, identity));
});
test("原意图先持久再读回，静默丢写不可当成可靠记录", () => {
  const memory = new Map(); globalThis.localStorage = { getItem: key => memory.get(key) ?? null, setItem: (key, value) => memory.set(key, value) };
  const state = { version: 1, ...identity, pending: { source, request, rejected: false }, record: null };
  saveConversationState(state, identity); assert.deepEqual(readConversationState(identity), state);
  memory.clear(); globalThis.localStorage.setItem = () => {}; assert.throws(() => saveConversationState(state, identity));
});
for (const fault of ["json", "version", "scope", "topic", "request", "private", "oversize"]) test(`坏存储停写${fault}`, () => {
  const state = { version: 1, ...identity, pending: { source: structuredClone(source), request: structuredClone(request), rejected: false }, record: null };
  if (fault === "version") state.version = 2;
  if (fault === "scope") state.session_id = "other";
  if (fault === "topic") state.topic_id = "other";
  if (fault === "request") state.pending.request.expected_session_revision = 2;
  if (fault === "private") state.owner_id = "private-owner";
  const raw = fault === "json" ? "{bad" : fault === "oversize" ? "x".repeat(131073) : JSON.stringify(state);
  globalThis.localStorage = { getItem: () => raw, setItem: () => {} }; assert.throws(() => readConversationState(identity));
});
