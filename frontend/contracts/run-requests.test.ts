import assert from "node:assert/strict";
import { createOrFind, defaultFormalTarget, INTENT } from "../src/lib/run-requests.ts";

class MemoryStorage {
  values = new Map<string, string>();
  getItem(key: string) { return this.values.get(key) ?? null; }
  setItem(key: string, value: string) { this.values.set(key, String(value)); }
  removeItem(key: string) { this.values.delete(key); }
}
const json = (body: unknown) => new Response(JSON.stringify(body), { headers: { "Content-Type": "application/json" } });
const run = (id: string, request_key: string) => ({ id, request_key, status: "completed", trace_id: "trace-" + id, root_span_id: "span-" + id, created_at: 1, finished_at: 2, code: null, stop_requested: false });
const originalFetch = globalThis.fetch;
const calls: { path: string; method: string; body?: Record<string, unknown> }[] = [];
let activeRun = run("new-run", "");
globalThis.fetch = async (input: RequestInfo | URL, options: RequestInit = {}) => {
  const path = String(input), method = options.method ?? "GET";
  const body = options.body ? JSON.parse(String(options.body)) as Record<string, unknown> : undefined;
  calls.push({ path, method, body });
  if (path === "/v1/runs" && method === "POST") {
    activeRun = run("new-run", String(body?.request_key));
    return json(activeRun);
  }
  if (path.startsWith("/v1/runs/by-key/")) {
    const key = decodeURIComponent(path.split("/").at(-1)!);
    return json({ ...activeRun, request_key: key });
  }
  throw new Error(`unexpected request: ${method} ${path}`);
};

try {
  assert.equal(defaultFormalTarget([]), null);
  assert.equal(defaultFormalTarget(["demo"]), null);
  assert.equal(defaultFormalTarget(["target-http"]), "target-http");
  assert.equal(defaultFormalTarget(["target-http", "target-development"]), "target-development");

  const saved = new MemoryStorage();
  const oldIntent = { text: "old scripted request", request_key: "original-demo-key", target: "demo" };
  const originalRaw = JSON.stringify(oldIntent);
  saved.setItem(INTENT, originalRaw);
  activeRun = run("original-run-id", oldIntent.request_key);
  const recovered = await createOrFind(saved as unknown as Storage, oldIntent.text, "", () => true, "target-http");
  assert.equal(recovered.id, "original-run-id");
  assert.deepEqual(calls.map(call => [call.method, call.path]), [
    ["GET", "/v1/runs/by-key/original-demo-key"],
    ["GET", "/v1/runs/by-key/original-demo-key"],
  ]);
  assert.equal(calls.some(call => call.method === "POST"), false);
  assert.equal(saved.getItem(INTENT), null);

  calls.length = 0;
  const unavailable = new MemoryStorage();
  await assert.rejects(
    () => createOrFind(unavailable as unknown as Storage, "new request", "", () => true, null),
    /target_unavailable/,
  );
  assert.equal(unavailable.getItem(INTENT), null);
  assert.equal(calls.length, 0);

  const fresh = new MemoryStorage();
  activeRun = run("new-run", "");
  const created = await createOrFind(fresh as unknown as Storage, "new request", "", () => true, "target-http");
  assert.equal(created.request_key, activeRun.request_key);
  assert.equal(calls.filter(call => call.method === "POST").length, 1);
  assert.equal(calls.find(call => call.method === "POST")?.body?.target, "target-http");
  assert.equal(fresh.getItem(INTENT), null);
} finally {
  globalThis.fetch = originalFetch;
}

console.log("Formal run target defaults and legacy intent recovery verified");
