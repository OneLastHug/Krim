import assert from "node:assert/strict";
import { RequestError } from "../src/api.ts";
import { parseTurn } from "../src/lib/session-contracts.ts";
import { INTENTS, KNOWN, performIntent, prepareIntent, readIntents } from "../src/lib/session-requests.ts";

class Memory implements Storage {
  values = new Map<string, string>();
  get length() { return this.values.size; }
  getItem(k: string) { return this.values.get(k) ?? null; }
  setItem(k: string, v: string) { this.values.set(k, v); }
  removeItem(k: string) { this.values.delete(k); }
  clear() { this.values.clear(); }
  key(index: number) { return [...this.values.keys()][index] ?? null; }
}
const scope = { principal_id: "local", generation: 1, workspace_id: "local", session_id: "A" };
function value(key: string, text = "原始输入") { return { id: "T", session_id: "A", workspace_id: "local", owner_id: "local", ordinal: 1, request_key: key, text, accepted_revision: 0, status: "completed", run_id: "R", context_fingerprint: "1".repeat(64), transcript: null, code: null, created_at: 1, started_at: 1, finished_at: 2 }; }
const response = (body: unknown, status = 200) => new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });
const original = globalThis.fetch;
let checks = 0;
try {
  const store = new Memory(), body = { text: "原始输入", expected_revision: 0 };
  const prepared = prepareIntent(store, scope, "turn", body); body.text = "晚改草稿";
  assert.equal(prepared.intent.body.text, "原始输入"); assert(Object.isFrozen(prepared.intent.body));
  const calls: { path: string; body: unknown }[] = [];
  globalThis.fetch = async (path, options = {}) => { calls.push({ path: String(path), body: options.body ? JSON.parse(String(options.body)) : null }); return response(value(prepared.intent.body.request_key)); };
  await performIntent(store, prepared.intent, "", true, () => false);
  assert.equal(readIntents(store).length, 1); assert.equal(store.getItem(KNOWN), null); checks++;
  const found = prepareIntent(store, scope, "turn", { text: "不得换键", expected_revision: 99 });
  assert.equal(found.first, false); assert.deepEqual(found.intent.body, prepared.intent.body);
  await performIntent(store, found.intent, "", false, () => true);
  assert.equal(readIntents(store).length, 0); assert.match(store.getItem(KNOWN)!, /"turn_id":"T"/);
  assert.equal(calls.filter(v => v.body !== null).length, 1); assert.match(calls[1]!.path, /by-key/); checks++;
  for (const [status, code, clears] of [[401, "unauthorized", true], [429, "capacity", true], [422, "invalid_input", true], [422, "input_limit", true], [422, "provider_http_error", false], [409, "revision_conflict", false], [503, "execution_error", false]] as const) {
    const memory = new Memory(), p = prepareIntent(memory, scope, "turn", { text: "原始输入", expected_revision: 0 });
    globalThis.fetch = async () => response({ error: { code } }, status);
    await assert.rejects(() => performIntent(memory, p.intent, "", true, () => true), RequestError);
    assert.equal(readIntents(memory).length, clears ? 0 : 1); checks++;
  }
  for (const status of [401, 404, 503]) {
    const memory = new Memory(), p = prepareIntent(memory, scope, "turn", { text: "原始输入", expected_revision: 0 }), snapshot = memory.getItem(INTENTS);
    globalThis.fetch = async () => response({ error: { code: status === 401 ? "unauthorized" : "not_found" } }, status);
    await assert.rejects(() => performIntent(memory, p.intent, "", false, () => true)); assert.equal(memory.getItem(INTENTS), snapshot); checks++;
  }
  for (const field of ["ordinal", "accepted_revision", "created_at"] as const) { assert.throws(() => parseTurn({ ...value("key"), [field]: false })); checks++; }
  assert.throws(() => parseTurn({ ...value("key"), extra: "不能隐藏接口漂移" })); checks++;
  const broken = new Memory(); broken.setItem = () => { throw new Error("quota"); }; let network = 0;
  globalThis.fetch = async () => { network++; return response(null); };
  assert.throws(() => prepareIntent(broken, scope, "turn", { text: "原始输入", expected_revision: 0 })); assert.equal(network, 0); checks++;
  console.log(JSON.stringify({ checks, automatic_posts: 0, ownership: "explicit acceptance before durable identity acknowledgement" }));
} finally { globalThis.fetch = original; }
