import { act, StrictMode } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, expect, test, vi } from "vitest";
import { App } from "../src/App";
import { readFileSync } from "node:fs";
import { createHash, webcrypto } from "node:crypto";
import { resolve as resolvePath } from "node:path";
import { canonical, revisionSelection, type Dataset, type FreezeBody } from "../src/lib/eval-contracts";
import { FREEZE_INTENT, EVAL_INTENT, EVAL_CURRENT } from "../src/lib/eval-requests";
(globalThis as typeof globalThis & {
    IS_REACT_ACT_ENVIRONMENT: boolean;
}).IS_REACT_ACT_ENVIRONMENT = true;
let root: Root, host: HTMLDivElement;
let calls: {
    path: string;
    method: string;
    body: unknown;
}[] = [];
const json = (v: unknown, status = 200) => new Response(JSON.stringify(v), { status });
const fp = (value: unknown) => createHash("sha256").update(canonical(value)).digest("hex");
const wire = JSON.parse(readFileSync(resolvePath(process.cwd(), "../tests/core/eval_contracts.json"), "utf8"));
const realReport = wire.find((v: {
    name: string;
}) => v.name === "real-report").value;
function revision(body: FreezeBody): Dataset { const cases = body.cases.map(c => { const input = c.input ?? c.selected_input!; const source = { schema_version: 1, filter_version: "self-contained/1", source_kind: "manual" as const, source_run_id: null, eligibility: "eligible", reasons: ["manual_input_only"], input_fingerprint: fp(input), selected_input: null, source_snapshot_fingerprint: fp({ kind: "manual", input_fingerprint: fp(input) }), source_target_key: null, source_target_fingerprint: null, observed: null, evidence: null }; const candidate = { ...source, candidate_fingerprint: fp(Object.fromEntries(Object.entries(source).filter(([k]) => k !== "selected_input"))) }; return { case_id: c.case_id, input, input_fingerprint: fp(input), source: candidate, reference: c.reference, reference_fingerprint: fp(c.reference), reason: c.reason }; }).sort((a, b) => a.case_id < b.case_id ? -1 : 1); const content = { schema_version: 1 as const, name: body.name, version: body.version, cases }; return { ...content, fingerprint: fp(content), created_at: 1 }; }
const evalView = (id: string, revision = 1) => ({ id, request_key: "key-" + id, status: "completed", code: null, created_at: 1, finished_at: 2, stop_requested: false, revision, planned_count: 1, finished_count: 1 });
function report(id: string, revision = 1) { const value = structuredClone(realReport); value.eval_id = id; value.revision = revision; value.report_hash = fp(Object.fromEntries(Object.entries(value).filter(([k]) => k !== "report_hash"))); return value; }
const identity = { principal_id: "local", generation: 1, legacy: true, workspaces: [{ workspace_id: "local", role: "admin", approve: true }] };
let identityRead = false;
function intercept(handler: (path: string, options: RequestInit) => Promise<Response> | Response) { identityRead = false; vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, options: RequestInit = {}) => { const path = String(input); calls.push({ path, method: options.method ?? "GET", body: options.body ? JSON.parse(String(options.body)) : null }); if (path === "/v2/me") { identityRead = true; return json(identity); } return handler(path, options); })); }
function reads(path: string) {
    if (path === "/v1/eval-targets")
        return json([{ key: "target-v1" }, { key: "target-v2" }, { key: "target-bad" }]);
    if (path === "/v1/eval-rules")
        return json(realReport.rules);
    if (path.includes("/report")) {
        const id = path.split("/")[3]!;
        return json(report(id));
    }
    if (path.startsWith("/v1/evals/"))
        return json(evalView(path.split("/")[3]!));
    return json([]);
}
async function mount() { await act(async () => root.render(<StrictMode><App /></StrictMode>)); await until(() => expect(identityRead).toBe(true)); await act(async () => { await new Promise(resolve => setTimeout(resolve, 0)); }); }
function button(label: string) { const value = [...document.querySelectorAll("button")].find(b => b.textContent?.trim() === label); expect(value, label).toBeTruthy(); return value!; }
async function click(label: string) { await act(async () => button(label).click()); }
async function type(id: string, value: string) { await act(async () => { const input = document.getElementById(id) as HTMLInputElement | HTMLTextAreaElement; expect(input, id).toBeTruthy(); Object.getOwnPropertyDescriptor(input.tagName === "TEXTAREA" ? HTMLTextAreaElement.prototype : HTMLInputElement.prototype, "value")!.set!.call(input, value); input.dispatchEvent(new Event("input", { bubbles: true })); }); }
async function until(check: () => void) {
    let error: unknown;
    for (let i = 0; i < 70; i++) {
        try {
            check();
            return;
        }
        catch (e) {
            error = e;
        }
        await act(async () => { await new Promise(r => setTimeout(r, 10)); });
    }
    throw error;
}
async function oneCase() { await type("dataset-name", "unit"); await type("case-input", "/add 2 3"); await type("case-expected", '"5"'); await type("case-reason", "人工核算"); await click("加入样本"); }
async function connect(key: string) { await click("连接"); await type("api-key", key); await click("应用"); }
beforeEach(() => { localStorage.clear(); history.replaceState(null, "", "/ui/?view=eval"); calls = []; host = document.createElement("div"); document.body.append(host); root = createRoot(host); vi.stubGlobal("crypto", webcrypto); intercept(path => reads(path)); });
afterEach(async () => { await act(async () => root.unmount()); host.remove(); vi.unstubAllGlobals(); });
test("zero automatic writes", async () => { await mount(); expect(calls.filter(c => c.method === "POST")).toEqual([]); });
test("真实App提供评测与显式冻结入口", async () => { await mount(); expect(host.textContent).toContain("冻结修订"); expect(document.getElementById("case-expected")).not.toBeNull(); });
test("freeze unauthorized keeps draft for explicit retry and one intent per double click", async () => {
    let stored: Dataset | null = null;
    intercept((path, options) => {
        if (path === "/v1/datasets" && options.method === "POST") {
            if (new Headers(options.headers).get("Authorization") !== "Bearer correct")
                return json({ error: { code: "unauthorized" } }, 401);
            stored = revision(JSON.parse(String(options.body)));
            return json(stored);
        }
        if (path.startsWith("/v1/datasets/"))
            return stored ? json(stored) : json({ error: { code: "not_found" } }, 404);
        return reads(path);
    });
    await mount();
    await oneCase();
    await click("冻结修订");
    expect(stored).toBeNull();
    expect(localStorage.getItem(FREEZE_INTENT)).toBeNull();
    expect((document.getElementById("case-input") as HTMLTextAreaElement).value).toBe("/add 2 3");
    expect(host.textContent).toContain("密钥无效");
    const before = calls.filter(c => c.method === "POST").length;
    await connect("correct");
    expect(calls.filter(c => c.method === "POST")).toHaveLength(before);
    await act(async () => { button("冻结修订").click(); button("冻结修订").click(); });
    await until(() => expect(document.getElementById("dataset-fingerprint")).not.toBeNull());
    expect(calls.filter(c => c.method === "POST")).toHaveLength(2);
    expect(localStorage.getItem(FREEZE_INTENT)).toBeNull();
});
test("unknown post keeps original key and freeze response stays with original draft", async () => {
    let stored: Dataset | null = null;
    let first = true;
    intercept((path, options) => {
        if (path === "/v1/datasets" && options.method === "POST") {
            stored = revision(JSON.parse(String(options.body)));
            throw new TypeError("accepted response lost");
        }
        if (path.startsWith("/v1/datasets/")) {
            if (first) {
                first = false;
                throw new TypeError("first query lost");
            }
            return json(stored);
        }
        return reads(path);
    });
    await mount();
    await oneCase();
    await click("冻结修订");
    await until(() => expect(host.textContent).toContain("冻结结果未确认"));
    const old = JSON.parse(localStorage.getItem(FREEZE_INTENT)!);
    await type("dataset-name", "edited");
    await type("case-input", "new draft");
    await click("查找修订");
    await until(() => expect(localStorage.getItem(FREEZE_INTENT)).toBeNull());
    expect(calls.filter(c => c.method === "POST")).toHaveLength(1);
    expect(calls.filter(c => c.path.startsWith("/v1/datasets/")).every(c => c.path === "/v1/datasets/unit/1")).toBe(true);
    expect(old.body.name).toBe("unit");
    expect((document.getElementById("dataset-name") as HTMLInputElement).value).toBe("edited");
    expect((document.getElementById("case-input") as HTMLTextAreaElement).value).toBe("new draft");
    expect(document.getElementById("dataset-fingerprint")).toBeNull();
});
test("GET401 and GET404 never clear unknown freeze", async () => {
    const body = revisionSelection(realReport.dataset);
    localStorage.setItem(FREEZE_INTENT, JSON.stringify({ body, draftId: "saved", editRevision: 0, nonce: "old" }));
    intercept(path => path.startsWith("/v1/datasets/") ? json({ error: { code: "unauthorized" } }, 401) : reads(path));
    await mount();
    await click("查找修订");
    expect(localStorage.getItem(FREEZE_INTENT)).not.toBeNull();
    expect(calls.filter(c => c.method === "POST")).toEqual([]);
    intercept(path => path.startsWith("/v1/datasets/") ? json({ error: { code: "not_found" } }, 404) : reads(path));
    await click("查找修订");
    expect(JSON.parse(localStorage.getItem(FREEZE_INTENT)!).body).toEqual(body);
    expect(calls.filter(c => c.method === "POST")).toEqual([]);
});
test("source response cannot overwrite edited draft or manufacture a reference", async () => {
    let resolve!: (value: Response) => void;
    const held = new Promise<Response>(r => resolve = r);
    intercept(path => path.includes("/candidate?") ? held : reads(path));
    await mount();
    await type("case-input", "manual draft");
    await type("source-run", "source-A");
    await click("读取来源");
    await type("case-reason", "edited while reading");
    await act(async () => resolve(json({ schema_version: 1, source_kind: "run", source_run_id: "source-A", eligibility: "eligible", reasons: ["eligible"], candidate_fingerprint: "a".repeat(64), selected_input: null, input_fingerprint: "b".repeat(64) })));
    expect((document.getElementById("case-input") as HTMLTextAreaElement).value).toBe("manual draft");
    expect((document.getElementById("case-expected") as HTMLTextAreaElement).value).toBe("");
    expect(document.getElementById("source-eligibility")).toBeNull();
});
test("A B A stale report cannot replace the new A", async () => {
    history.replaceState(null, "", "/ui/?view=eval&eval_id=A");
    let first = true, release!: (value: Response) => void;
    const old = new Promise<Response>(r => release = r);
    intercept(path => {
        if (path === "/v1/evals/A" && first) {
            first = false;
            return old;
        }
        if (path === "/v1/evals/A")
            return json(evalView("A", 2));
        if (path.startsWith("/v1/evals/A/report"))
            return json(report("A", 2));
        return reads(path);
    });
    await mount();
    await until(() => expect(calls.some(c => c.path === "/v1/evals/A")).toBe(true));
    await type("open-eval-id", "B");
    await click("打开评测");
    await until(() => expect(document.getElementById("eval-id")?.textContent).toBe("B"));
    await type("open-eval-id", "A");
    await click("打开评测");
    await until(() => expect(host.querySelector(".report-identity")?.textContent).toContain(report("A", 2).report_hash));
    await act(async () => release(json(evalView("A", 1))));
    expect(host.querySelector(".report-identity")?.textContent).toContain(report("A", 2).report_hash);
    expect(calls.filter(c => c.method === "POST")).toEqual([]);
});
test("gate pass cleared before invalid", async () => {
    intercept(path => path.startsWith("/v1/evals/compare?") ? json(new URL("http://localhost" + path).searchParams.get("candidate") === "B" ? { status: "pass", baseline: { eval_id: "A" }, candidate: { eval_id: "B" }, reasons: [] } : { status: "invalid", reasons: [{ code: "incomplete_coverage" }] }) : reads(path));
    await mount();
    await type("baseline-id", "A");
    await type("candidate-id", "B");
    await click("比较");
    await until(() => expect(document.getElementById("gate-status")?.textContent).toBe("通过"));
    await type("candidate-id", "C");
    expect(document.getElementById("gate-status")).toBeNull();
    await click("比较");
    await until(() => expect(document.getElementById("gate-status")?.textContent).toBe("不可比较"));
});
test("corrected catalogue authentication clears only its own stale notice", async () => {
    intercept((path, options) => {
        if ((path === "/v1/eval-targets" || path === "/v1/eval-rules") && new Headers(options.headers).get("Authorization") !== "Bearer correct")
            return json({ error: { code: "unauthorized" } }, 401);
        return reads(path);
    });
    await mount();
    await until(() => expect(document.querySelector(".evaluation-results>.notice")?.textContent).toBe("密钥无效"));
    await connect("correct");
    await until(() => expect(document.querySelectorAll("#eval-target option")).toHaveLength(3));
    expect(document.querySelector(".evaluation-results>.notice")).toBeNull();
    expect(calls.filter(c => c.method === "POST")).toEqual([]);
});
for (const barrier of ["POST", "confirmation GET"] as const) {
    for (const steering of ["task", "other", "key", "refresh", "remount"] as const) {
        test(`successful Eval ${barrier} preserves original intent after ${steering}`, async () => {
            let entered = false, release!: (value: Response) => void, acceptedKey = "", confirmations = 0;
            const held = new Promise<Response>(resolve => release = resolve);
            intercept((path, options) => {
                if (path === "/v1/datasets" && options.method === "POST")
                    return json(revision(JSON.parse(String(options.body))));
                if (path === "/v1/evals" && options.method === "POST") {
                    acceptedKey = JSON.parse(String(options.body)).request_key;
                    if (barrier === "POST") {
                        entered = true;
                        return held;
                    }
                    return json({ ...evalView("accepted"), request_key: acceptedKey });
                }
                if (path.startsWith("/v1/evals/by-key/")) {
                    confirmations++;
                    if (barrier === "confirmation GET" && confirmations === 1) {
                        entered = true;
                        return held;
                    }
                    return json({ ...evalView("accepted"), request_key: acceptedKey });
                }
                return reads(path);
            });
            await mount();
            await oneCase();
            await click("冻结修订");
            await type("open-eval-id", "A");
            await click("打开评测");
            await until(() => expect(document.getElementById("eval-status")?.textContent).toBe("已完成"));
            await click("开始评测");
            await until(() => expect(entered).toBe(true));
            const original = JSON.parse(localStorage.getItem(EVAL_INTENT)!);
            const confirmationsBefore = confirmations;
            if (steering === "task")
                await click("任务");
            if (steering === "other") {
                await type("open-eval-id", "B");
                await click("打开评测");
            }
            if (steering === "key")
                await connect("next-key");
            if (steering === "refresh")
                await click("刷新");
            if (steering === "remount") {
                await act(async () => root.unmount());
                root = createRoot(host);
                await mount();
            }
            await act(async () => release(json({ ...evalView("accepted"), request_key: acceptedKey })));
            await act(async () => { await new Promise(resolve => setTimeout(resolve, 0)); });
            expect(confirmations).toBe(confirmationsBefore);
            expect(JSON.parse(localStorage.getItem(EVAL_INTENT)!)).toEqual(original);
            expect(localStorage.getItem(EVAL_CURRENT)).toBe(steering === "other" ? "B" : "A");
            expect(document.getElementById("eval-id")?.textContent).not.toBe("accepted");
            if (steering === "task")
                await click("评测");
            await click("查找评测");
            await until(() => expect(document.getElementById("eval-id")?.textContent).toBe("accepted"));
            expect(localStorage.getItem(EVAL_CURRENT)).toBe("accepted");
            expect(new URL(location.href).searchParams.get("eval_id")).toBe("accepted");
            expect(localStorage.getItem(EVAL_INTENT)).toBeNull();
            expect(confirmations).toBeGreaterThan(confirmationsBefore);
            expect(calls.filter(c => c.path === "/v1/evals" && c.method === "POST")).toHaveLength(1);
            expect(calls.filter(c => c.path.endsWith("/cancel"))).toEqual([]);
            expect(calls.filter(c => c.path.startsWith("/v1/evals/by-key/")).every(c => c.path.endsWith(original.request_key))).toBe(true);
        });
    }
}

test("HTTP capability is read only and follows the active key epoch", async () => {
    let release!: (response: Response) => void;
    const delayed = new Promise<Response>(resolve => { release = resolve; });
    intercept((path, options) => {
        if (path === "/healthz") {
            return new Headers(options.headers).get("Authorization") === "Bearer new-key" ? json({ http_enabled: false, provider: "offline-script" }) : delayed.then(() => json({ http_enabled: true, provider: "script+http" }));
        }
        return reads(path);
    });
    await mount();
    expect(host.querySelector(".brand")?.textContent).toContain("Krim Agent");
    await connect("new-key");
    await until(() => expect(host.querySelector(".brand")?.textContent).toContain("离线演示"));
    await act(async () => release(json({ http_enabled: true })));
    expect(host.querySelector(".brand")?.textContent).toContain("离线演示");
    expect(calls.filter(c => c.method === "POST")).toEqual([]);
    expect(calls.filter(c => c.path === "/healthz").length).toBeGreaterThanOrEqual(2);
});

test("HTTP target and historical report keep their own identity", async () => {
    intercept(path => path === "/v1/eval-targets" ? json([{ key: "target-http" }]) : path === "/healthz" ? json({ http_enabled: true }) : reads(path));
    await mount();
    await until(() => expect(document.querySelector("#eval-target option")?.textContent).toBe("HTTP目标 · target-http"));
    expect(host.querySelector(".brand")?.textContent).toContain("Krim Agent");
    await type("open-eval-id", "historical-script");
    await click("打开评测");
    await until(() => expect(host.querySelector(".eval-report small")?.textContent).toBe("脚本 · " + realReport.target_descriptor.key));
    expect(document.querySelector("#eval-target option")?.textContent).toBe("HTTP目标 · target-http");
    expect(calls.filter(c => c.method === "POST")).toEqual([]);
});
