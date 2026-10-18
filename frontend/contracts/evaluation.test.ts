import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { parseReference, parseEval, parseReport, parseRule, parseDataset, matchesRevision, strictJson, revisionSelection, type Dataset, type EvalBody } from "../src/lib/eval-contracts.ts";
import { freezeOrFind, evalOrFind, acknowledgeEval, unadmitted, FREEZE_INTENT, EVAL_INTENT, EVAL_CURRENT } from "../src/lib/eval-requests.ts";
import { RequestError } from "../src/api.ts";
const vectors = JSON.parse(readFileSync(new URL("../../tests/core/eval_contracts.json", import.meta.url), "utf8"));
for (const vector of vectors) {
    let valid = true;
    try {
        if (vector.kind === "dataset")
            parseDataset(vector.value);
        else if (vector.kind === "reference")
            parseReference(vector.value);
        else if (vector.kind === "eval")
            parseEval(vector.value);
        else if (vector.kind === "rule")
            parseRule(vector.value);
        else if (vector.kind === "report")
            parseReport(vector.value);
        else
            strictJson(vector.text);
    }
    catch {
        valid = false;
    }
    assert.equal(valid, vector.valid, vector.name);
}
console.log(`${vectors.length} shared eval vectors passed`);
for (const [status, code, method, expected] of [[401, "unauthorized", "POST", true], [401, "unauthorized", "GET", false], [404, "not_found", "GET", false], [500, "invalid_input", "POST", false], [401, "capacity", "POST", false], [422, "invalid_input", "POST", true]] as const) {
    assert.equal(unadmitted(new RequestError(status, code), "freeze", method), expected);
}
class Memory implements Storage {
    values = new Map<string, string>();
    get length() { return this.values.size; }
    clear() { this.values.clear(); }
    getItem(k: string) { return this.values.get(k) ?? null; }
    setItem(k: string, v: string) { this.values.set(k, v); }
    removeItem(k: string) { this.values.delete(k); }
    key(i: number) { return [...this.values.keys()][i] ?? null; }
}
const dataset = vectors.find((v: {
    name: string;
}) => v.name === "real-report").value.dataset as Dataset;
const fresh = { body: revisionSelection(dataset), draftId: "original", editRevision: 1, nonce: "first" };
const store = new Memory(), original = globalThis.fetch;
let posts = 0, gets = 0, phase = "auth";
try {
    globalThis.fetch = async (_path, options) => {
        if (options?.method === "POST") {
            posts++;
            if (phase === "auth")
                return new Response(JSON.stringify({ error: { code: "unauthorized" } }), { status: 401 });
            throw new TypeError("unknown return");
        }
        gets++;
        if (phase === "unknown")
            throw new TypeError("first query unavailable");
        return new Response(JSON.stringify(dataset));
    };
    await assert.rejects(freezeOrFind(store, fresh, "WRONG"));
    assert.equal(store.getItem(FREEZE_INTENT), null);
    assert.deepEqual(fresh.body, revisionSelection(dataset));
    assert.equal(gets, 0);
    phase = "unknown";
    await assert.rejects(freezeOrFind(store, fresh, "correct"));
    assert.ok(store.getItem(FREEZE_INTENT));
    const originalBody = JSON.parse(store.getItem(FREEZE_INTENT)!).body;
    phase = "find";
    const result = await freezeOrFind(store, { ...fresh, body: { ...fresh.body, name: "new-draft" }, draftId: "new", nonce: "second" }, "correct");
    assert.equal(result.intent.draftId, "original");
    assert.deepEqual(result.intent.body, originalBody);
    assert.equal(posts, 2);
    assert.equal(store.getItem(FREEZE_INTENT), null);
}
finally {
    globalThis.fetch = original;
}
console.log("freeze first POST rejection and unknown original-key recovery remain distinct");
const numeric = parseDataset(vectors.find((v: {
    name: string;
}) => v.name === "numeric-revision").value);
assert.equal(await matchesRevision(numeric, revisionSelection(numeric)), true, "Python数值指纹的原请求必须能找回");
assert.equal(await matchesRevision(numeric, { ...revisionSelection(numeric), cases: numeric.cases.map(c => ({ ...revisionSelection(numeric).cases[0]!, reference: { ...c.reference, expected: true } })) }), false, "布尔参考不能当数字");
const conflictStore = new Memory(), savedFetch = globalThis.fetch;
try {
    let conflictGets = 0;
    globalThis.fetch = async (_path, options) => {
        if (options?.method === "POST")
            return new Response(JSON.stringify({ error: { code: "revision_conflict" } }), { status: 409 });
        conflictGets++;
        return new Response(JSON.stringify(numeric));
    };
    await assert.rejects(freezeOrFind(conflictStore, { body: revisionSelection(numeric), draftId: "conflict", editRevision: 0, nonce: "conflict" }, "key"), /修订内容冲突/);
    assert.equal(conflictGets, 1);
    assert.equal(conflictStore.getItem(FREEZE_INTENT), null);
}
finally {
    globalThis.fetch = savedFetch;
}
console.log("numeric references keep original content; confirmed POST conflicts remain conflicts");
const evalStore = new Memory(), previousFetch = globalThis.fetch;
try {
    const view = vectors.find((v: {
        kind: string;
        valid: boolean;
    }) => v.kind === "eval" && v.valid).value;
    const body: EvalBody = { request_key: view.request_key, dataset: { name: numeric.name, version: numeric.version, fingerprint: numeric.fingerprint }, target_key: "target-v2", rules: [{ name: "status_code", version: "1", config: { match_code: true } }], execution: { repetitions: 1, concurrency: 1, item_timeout_seconds: 12, scorer_timeout_seconds: 1, batch_timeout_seconds: 600 } };
    let postCount = 0;
    globalThis.fetch = async (_path, options) => { if (options?.method === "POST")
        postCount++; return new Response(JSON.stringify(view)); };
    const confirmed = await evalOrFind(evalStore, body, "key");
    assert.deepEqual(JSON.parse(evalStore.getItem(EVAL_INTENT)!), body);
    assert.throws(() => acknowledgeEval(evalStore, body, confirmed), /身份/);
    evalStore.setItem(EVAL_CURRENT, "other");
    assert.throws(() => acknowledgeEval(evalStore, body, confirmed), /身份/);
    evalStore.setItem(EVAL_CURRENT, confirmed.id);
    assert.throws(() => acknowledgeEval(evalStore, { ...body, target_key: "target-bad" }, confirmed), /身份/);
    acknowledgeEval(evalStore, body, confirmed);
    assert.equal(evalStore.getItem(EVAL_INTENT), null);
    assert.equal(postCount, 1);
}
finally {
    globalThis.fetch = previousFetch;
}
console.log("Eval confirmation retains intent until owned identity is durably accepted");
