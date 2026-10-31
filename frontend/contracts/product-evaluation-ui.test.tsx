import { act, StrictMode } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, expect, test, vi } from "vitest";
import { webcrypto } from "node:crypto";
import { EvaluationProductWorkbench } from "../src/components/EvaluationProductWorkbench";
import { buildProductDatasetRequest, evaluationStorageKey, normalizeProductTask, type ProductCandidateSummary } from "../src/lib/product-evaluation";

(globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean }).IS_REACT_ACT_ENVIRONMENT = true;
let root: Root;
let host: HTMLDivElement;
let calls: Array<{ path: string; method: string; body: Record<string, unknown> | null }> = [];
let handler: ((path: string, method: string, body: Record<string, unknown> | null) => Promise<Response> | Response | undefined) | undefined;
const json = (value: unknown, status = 200) => new Response(JSON.stringify(value), { status, headers: { "Content-Type": "application/json" } });
const base = "/v4/workspaces/team/eval";
const metrics = ["fact_integrity", "context_coverage", "krim_result", "process_constraints"].map((name, index) => ({
  name, version: "1", implementation_digest: String(index).repeat(64), config: {}, required_evidence: [], hard: index < 3,
}));
const target = { key: "target-script", version: "2", kind: "registered" };
function frozenDataset(body: Record<string, unknown>) {
  return { name: body.name, version: body.version, fingerprint: "a".repeat(64), cases: body.cases };
}
function taskView(body: Record<string, unknown>, id = "task-1") {
  const dataset = body.dataset as Record<string, string>;
  return {
    id, request_key: body.request_key, status: "queued", code: null, revision: 0, mode: "execute", kind: "manual", layout: 2,
    dataset, target_key: body.target_key, metrics: body.metrics, planned_count: 1, finished_count: 0,
    created_at: 1, finished_at: null, report_hash: null,
  };
}
function completedTask(id: string) {
  return {
    id, request_key: `key-${id}`, status: "completed", code: null, revision: 1, mode: "execute", kind: "manual", layout: 2,
    dataset: { name: "suite", version: "v1", fingerprint: "a".repeat(64) }, target_key: "target-script",
    metrics: [metrics[2]!], planned_count: 1, finished_count: 1, created_at: 1, finished_at: 2, report_hash: `${id.charCodeAt(0)}`.repeat(64),
  };
}
function defaultHandler(path: string, method: string, body: Record<string, unknown> | null) {
  if (path === `${base}/targets`) return json([target]);
  if (path.startsWith(`${base}/metrics?`)) return json(metrics);
  if (path.startsWith(`${base}/datasets?`)) return json([]);
  if (path.startsWith(`${base}/tasks?`)) return json([]);
  if (path === `${base}/datasets` && method === "POST") return json(frozenDataset(body!), 201);
  if (path === `${base}/tasks` && method === "POST") return json(taskView(body!), 202);
  return json({ error: { code: "not_found" } }, 404);
}
function mount(props: Partial<Parameters<typeof EvaluationProductWorkbench>[0]> = {}) {
  return act(async () => root.render(<StrictMode><EvaluationProductWorkbench apiKey="access-key" workspace="team" active view="datasets" canWrite canApprove {...props} /></StrictMode>));
}
async function until(check: () => void) {
  let caught: unknown;
  for (let index = 0; index < 80; index++) {
    try { check(); return; } catch (error) { caught = error; }
    await act(async () => { await new Promise(resolve => setTimeout(resolve, 0)); });
  }
  throw caught;
}
function button(name: string) {
  const found = [...host.querySelectorAll("button")].find(item => item.textContent?.trim() === name);
  expect(found, `button ${name}`).toBeTruthy();
  return found!;
}
async function click(name: string) { await act(async () => button(name).click()); }
async function type(id: string, value: string) {
  await act(async () => {
    const element = host.querySelector(`#${id}`) as HTMLInputElement | HTMLTextAreaElement;
    expect(element, id).toBeTruthy();
    const prototype = element.tagName === "TEXTAREA" ? HTMLTextAreaElement.prototype : HTMLInputElement.prototype;
    Object.getOwnPropertyDescriptor(prototype, "value")!.set!.call(element, value);
    element.dispatchEvent(new Event("input", { bubbles: true }));
  });
}
async function select(id: string, value: string) {
  await act(async () => {
    const element = host.querySelector(`#${id}`) as HTMLSelectElement;
    Object.getOwnPropertyDescriptor(HTMLSelectElement.prototype, "value")!.set!.call(element, value);
    element.dispatchEvent(new Event("change", { bubbles: true }));
  });
}
async function freezeManualDataset() {
  await type("product-instruction", "将 2 加 3 并说明结果");
  await type("product-input-file", "请计算 2 + 3");
  await type("product-expected", "5");
  await click("冻结数据集版本");
}
async function switchToEval() {
  await act(async () => root.render(<StrictMode><EvaluationProductWorkbench apiKey="access-key" workspace="team" active view="eval" canWrite canApprove /></StrictMode>));
  await until(() => expect(host.querySelector<HTMLOptionElement>("#product-target option")?.value).toBe("target-script"));
}
const storedDatasetRow = { name: "suite", version: "v1", fingerprint: "a".repeat(64), case_count: 1 };
function storedDataset(envelope?: string) {
  return { ...storedDatasetRow, cases: [{ case_id: "case-1", instruction: "计算", files: { "INPUT.txt": "2+3" }, reference: { expected: "5" }, verifier: "krim-script/1" }], ...(envelope ? { dataset_envelope_fp: envelope } : {}) };
}
function storedDatasetHandler(envelope?: string) {
  return (path: string) => path.startsWith(`${base}/datasets?`) ? json([storedDatasetRow])
    : path === `${base}/datasets/suite/v1` ? json(storedDataset(envelope)) : undefined;
}
async function chooseStoredDataset() {
  await mount({ view: "eval" });
  await until(() => expect(host.querySelector<HTMLOptionElement>("#product-target option")?.value).toBe("target-script"));
  await until(() => expect(host.querySelector('#product-dataset-choice option[value="suite/v1"]')).toBeTruthy());
  await select("product-dataset-choice", "suite/v1");
  await until(() => expect(host.textContent).toContain("已选择 suite · v1"));
}
async function chooseKrimMetric() {
  await until(() => expect(host.querySelectorAll(".product-eval-metric-choice input").length).toBeGreaterThan(0));
  const checkbox = [...host.querySelectorAll<HTMLInputElement>(".product-eval-metric-choice input")].find(item => item.parentElement?.textContent?.includes("Krim 执行结果"));
  expect(checkbox).toBeTruthy();
  await act(async () => { checkbox!.click(); });
}
beforeEach(() => {
  localStorage.clear(); calls = []; handler = undefined;
  vi.stubGlobal("crypto", webcrypto);
  host = document.createElement("div"); document.body.append(host); root = createRoot(host);
  vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, options: RequestInit = {}) => {
    const path = String(input), method = options.method ?? "GET";
    const body = options.body ? JSON.parse(String(options.body)) as Record<string, unknown> : null;
    calls.push({ path, method, body });
    return (await handler?.(path, method, body)) ?? defaultHandler(path, method, body);
  }));
});
afterEach(async () => { await act(async () => root.unmount()); host.remove(); vi.restoreAllMocks(); vi.unstubAllGlobals(); });

test("feedback candidate deep link only reads the exact existing candidate and keeps human fields empty", async () => {
  const candidate = { candidate_id: "feedback-case", revision: 0, status: "pending", observation_id: "obs", observation_hash: "b".repeat(64), eligibility: "needs_context", reasons: [], hash: "c".repeat(64) };
  handler = path => path === `${base}/candidates/feedback-case` ? json(candidate) : undefined;
  await mount({ candidateId: "feedback-case", sourceRun: "feedback-run" });
  await until(() => expect(host.querySelector("#product-candidate-instruction")).toBeTruthy());
  expect((host.querySelector("#product-candidate-expected") as HTMLTextAreaElement).value).toBe("");
  expect(calls.filter(item => item.path === `${base}/candidates/feedback-case` && item.method === "GET")).toHaveLength(1);
  expect(calls.every(item => item.method === "GET")).toBe(true);
});


test("late feedback candidate cannot appear after switching workspace identity", async () => {
  let release!: (response: Response) => void;
  const response = new Promise<Response>(done => { release = done; });
  handler = path => path === `${base}/candidates/old-feedback` ? response : undefined;
  await mount({ candidateId: "old-feedback" });
  await until(() => expect(calls.some(item => item.path === `${base}/candidates/old-feedback`)).toBe(true));
  await mount({ workspace: "other", candidateId: undefined });
  await act(async () => release(json({ candidate_id: "old-feedback", revision: 0, status: "pending", observation_id: "old-obs", observation_hash: "b".repeat(64), eligibility: "needs_context", reasons: [], hash: "c".repeat(64) })));
  expect(host.querySelector("#product-candidate-instruction")).toBeNull();
  expect(calls.every(item => item.method === "GET")).toBe(true);
});


test("slower catalogue refresh cannot erase a completed metric request after reactivation", async () => {
  let holdTasks = false;
  let release!: (value: Response) => void;
  handler = path => {
    if (holdTasks && path.startsWith(`${base}/tasks?`)) return new Promise<Response>(done => { release = done; });
    return storedDatasetHandler()(path);
  };
  await chooseStoredDataset();
  await mount({ view: "eval", active: false });
  holdTasks = true;
  await mount({ view: "eval", active: true });
  await chooseKrimMetric();
  await act(async () => { release(json([])); });
  await until(() => {
    const checkbox = [...host.querySelectorAll<HTMLInputElement>(".product-eval-metric-choice input")]
      .find(item => item.parentElement?.textContent?.includes("Krim 执行结果"));
    expect(checkbox?.checked).toBe(true);
    expect(button("启动评测").disabled).toBe(false);
  });
});


test("mount is read-only and manual Krim Script dataset contains explicit human reference", async () => {
  await mount();
  await until(() => expect(host.textContent).toContain("新建 Krim Script 数据集"));
  expect(calls.filter(item => item.method === "POST")).toEqual([]);
  await freezeManualDataset();
  const freeze = calls.find(item => item.path === `${base}/datasets` && item.method === "POST")!;
  expect(freeze.body).toMatchObject({
    name: "script-suite", version: "v1",
    cases: [{ case_id: "case-1", instruction: "将 2 加 3 并说明结果", files: { "INPUT.txt": "请计算 2 + 3" }, reference: { expected: "5" }, verifier: "krim-script/1" }],
  });
  expect(host.textContent).toContain("已冻结 script-suite · v1");
});

test("manual dataset supports editable sample drafts and freezes every joined sample", async () => {
  await mount();
  await until(() => expect(host.textContent).toContain("新建 Krim Script 数据集"));
  await type("product-instruction", "计算 2+3");
  await type("product-input-file", "2 + 3");
  await type("product-expected", "5");
  await click("加入样本");
  await until(() => expect(host.querySelectorAll('ul[aria-label="样本草稿"] li')).toHaveLength(1));
  await type("product-instruction", "计算 3+3");
  await type("product-input-file", "3 + 3");
  await type("product-expected", "6");
  await click("加入样本");
  await until(() => expect(host.querySelectorAll('ul[aria-label="样本草稿"] li')).toHaveLength(2));
  let rows = host.querySelectorAll('ul[aria-label="样本草稿"] li');
  await act(async () => (rows[0]!.querySelectorAll("button")[0] as HTMLButtonElement).click());
  await type("product-expected", "5（人工复核）");
  await click("保存样本修改");
  await until(() => expect(host.querySelector('ul[aria-label="样本草稿"]')?.textContent).toContain("5（人工复核）"));
  rows = host.querySelectorAll('ul[aria-label="样本草稿"] li');
  await act(async () => (rows[1]!.querySelectorAll("button")[1] as HTMLButtonElement).click());
  await until(() => expect(host.querySelectorAll('ul[aria-label="样本草稿"] li')).toHaveLength(1));
  await type("product-instruction", "计算 4+3");
  await type("product-input-file", "4 + 3");
  await type("product-expected", "7");
  expect(button("冻结 1 个样本").disabled).toBe(true);
  await click("加入样本");
  await until(() => expect(host.querySelectorAll('ul[aria-label="样本草稿"] li')).toHaveLength(2));
  await click("冻结 2 个样本");
  const freeze = calls.find(item => item.path === `${base}/datasets` && item.method === "POST")!;
  const cases = (freeze.body as { cases: Array<{ case_id: string; reference: { expected: string } }> }).cases;
  expect(cases).toHaveLength(2);
  expect(new Set(cases.map(item => item.case_id)).size).toBe(2);
  expect(cases.map(item => item.reference.expected)).toEqual(["5（人工复核）", "7"]);
});

test("dataset request preflight rejects duplicate IDs, over 100 cases, and payloads above 64 KiB", async () => {
  const baseCase = { case_id: "case-a", instruction: "计算", files: { "INPUT.txt": "2+3" }, reference: { expected: "5" }, verifier: "krim-script/1" as const };
  expect(() => buildProductDatasetRequest("suite", "v1", [baseCase, baseCase])).toThrow("样本编号重复");
  expect(() => buildProductDatasetRequest("suite", "v1", Array.from({ length: 101 }, (_, index) => ({ ...baseCase, case_id: `case-${index}` })))).toThrow("100");
  expect(() => buildProductDatasetRequest("suite", "v1", [{ ...baseCase, files: { "INPUT.txt": "x".repeat(65500) } }])).toThrow("64 KiB");
});

test("flat v4 task count stays unknown until the completed report confirms it", async () => {
  const task = { ...completedTask("count-v"), finished_count: null };
  const missingField = { ...task } as Record<string, unknown>;
  delete missingField.finished_count;
  expect(normalizeProductTask(missingField).finished_count).toBeNull();
  handler = (path, _method, _body) => {
    if (path.startsWith(`${base}/tasks?`)) return json([task]);
    if (path === `${base}/tasks/${task.id}`) return json(task);
    if (path.startsWith(`${base}/tasks/${task.id}/report?`)) return json({ eval_id: task.id, revision: 1, report_hash: task.report_hash, kind: "manual", dataset: task.dataset,
      execution: { status: "completed", complete: true, planned_count: 1 }, coverage: { cell_coverage: 1, scored_cells: 1, planned_cells: 1 }, quality: { pass_rate: 1 },
      rows: [{ case_id: "case-1", repetition: 0, status: "finished", quality: "passed", scores: [] }], next_cursor: null, next_offset: null });
    return undefined;
  };
  await mount({ view: "eval" });
  await until(() => expect(host.querySelectorAll(".product-eval-task-row")).toHaveLength(1));
  expect(host.textContent).toContain("—/1");
  await act(async () => host.querySelector<HTMLButtonElement>(".product-eval-task-row")!.click());
  await until(() => expect(host.textContent).toContain("— / 1 项完成"));
  await click("查看报告");
  await until(() => expect(host.textContent).toContain("1 / 1 项完成"));
  expect(host.querySelector(".product-eval-task-row")?.textContent).toContain("1/1");
});

test("oversized manual dataset is rejected before freeze POST", async () => {
  await mount();
  await until(() => expect(host.textContent).toContain("新建 Krim Script 数据集"));
  await type("product-instruction", "保存人工参考样本");
  await type("product-input-file", "x".repeat(65500));
  await type("product-expected", "参考结果");
  await click("冻结数据集版本");
  await until(() => expect(host.querySelector('[role="alert"]')?.textContent).toContain("64 KiB"));
  expect(calls.filter(item => item.path === `${base}/datasets` && item.method === "POST")).toHaveLength(0);
});

test("oversized manual dataset is rejected before the backend request", async () => {
  await mount();
  await until(() => expect(host.textContent).toContain("新建 Krim Script 数据集"));
  await type("product-instruction", "人工样本");
  await type("product-input-file", "x".repeat(65500));
  await type("product-expected", "参考答案");
  await click("冻结数据集版本");
  await until(() => expect(host.querySelector('[role="alert"]')?.textContent).toContain("64 KiB"));
  expect(calls.filter(item => item.path === `${base}/datasets` && item.method === "POST")).toHaveLength(0);
});

test("v4 flat task counts remain unknown until the report confirms completion", async () => {
  const task = { ...completedTask("count-task"), finished_count: null };
  const missingCount = { ...task } as Record<string, unknown>;
  delete missingCount.finished_count;
  expect(normalizeProductTask(missingCount).finished_count).toBeNull();
  handler = (path, _method, _body) => {
    if (path.startsWith(`${base}/tasks?`)) return json([task]);
    if (path === `${base}/tasks/${task.id}`) return json(task);
    if (path.startsWith(`${base}/tasks/${task.id}/report?`)) return json({ eval_id: task.id, revision: 1, report_hash: task.report_hash, kind: "manual", dataset: task.dataset,
      execution: { status: "completed", complete: true, planned_count: 1 }, coverage: { cell_coverage: 1, scored_cells: 1, planned_cells: 1 }, quality: { pass_rate: 1 },
      rows: [{ case_id: "case-1", repetition: 0, status: "finished", quality: "passed", scores: [] }], next_cursor: null, next_offset: null });
    return undefined;
  };
  await mount({ view: "eval" });
  await until(() => expect(host.querySelectorAll(".product-eval-task-row")).toHaveLength(1));
  expect(host.textContent).toContain("—/1");
  expect(host.textContent).not.toContain("0/1");
  await act(async () => host.querySelector<HTMLButtonElement>(".product-eval-task-row")!.click());
  await until(() => expect(host.textContent).toContain("— / 1 项完成"));
  await click("查看报告");
  await until(() => expect(host.textContent).toContain("1 / 1 项完成"));
  expect(host.querySelector(".product-eval-task-row")?.textContent).toContain("1/1");
});

test("viewer can read reports and compare but cannot create or cancel tasks", async () => {
  const baseline = completedTask("baseline-v"), candidate = completedTask("candidate-v");
  const activeTask = { ...completedTask("running-v"), status: "running", finished_count: 0, report_hash: null };
  handler = (path, method, body) => {
    if (path.startsWith(`${base}/datasets?`)) return json([storedDatasetRow]);
    if (path === `${base}/datasets/suite/v1`) return json(storedDataset());
    if (path.startsWith(`${base}/tasks?`)) return json([baseline, candidate, activeTask]);
    if (path === `${base}/tasks/${baseline.id}`) return json(baseline);
    if (path === `${base}/tasks/${candidate.id}`) return json(candidate);
    if (path === `${base}/tasks/${activeTask.id}`) return json(activeTask);
    if (path.startsWith(`${base}/tasks/`) && path.includes("/report?")) return json({ eval_id: baseline.id, revision: 1, report_hash: "f".repeat(64), kind: "manual", dataset: baseline.dataset, execution: { status: "completed" }, coverage: { cell_coverage: 1, scored_cells: 1, planned_cells: 1 }, quality: { pass_rate: 1 }, rows: [{ case_id: "case-1", repetition: 0, status: "finished", quality: "passed", scores: [] }], next_cursor: null, next_offset: null });
    if (path === `${base}/tasks/compare` && method === "POST") return json({ status: "pass", reasons: [] });
    return defaultHandler(path, method, body);
  };
  await mount({ view: "eval", canWrite: false, canApprove: false });
  await until(() => expect(host.querySelectorAll(".product-eval-task-row")).toHaveLength(3));
  await select("product-dataset-choice", "suite/v1");
  await until(() => expect(host.textContent).toContain("已选择 suite · v1"));
  await chooseKrimMetric();
  expect(button("启动评测").disabled).toBe(true);
  await act(async () => host.querySelector<HTMLButtonElement>(".product-eval-task-row")!.click());
  await until(() => expect(button("查看报告").disabled).toBe(false));
  await click("查看报告");
  await until(() => expect(host.textContent).toContain("case-1"));
  await select("product-baseline", baseline.id);
  await select("product-candidate-task", candidate.id);
  await click("比较基线与候选");
  await until(() => expect(host.textContent).toContain("比较通过"));
  expect(calls.filter(item => item.path === `${base}/tasks` && item.method === "POST")).toHaveLength(0);
  expect(calls.filter(item => item.path === `${base}/tasks/compare` && item.method === "POST")).toHaveLength(1);
  const taskRows = host.querySelectorAll<HTMLButtonElement>(".product-eval-task-row");
  await act(async () => taskRows[2]!.click());
  await until(() => expect(button("取消任务").disabled).toBe(true));
  expect(calls.filter(item => item.path.endsWith("/cancel") && item.method === "POST")).toHaveLength(0);
});

test("permission to write without approval cannot confirm or freeze a candidate", async () => {
  const pending = { candidate_id: "pending-a", observation_id: "obs-a", eligibility: "eligible", reasons: [], source: { run_id: "run-a" } };
  const confirmed = { candidate_id: "confirmed-b", observation_id: "obs-b", eligibility: "eligible", reasons: [], source: { run_id: "run-b" } };
  handler = (path, method, body) => {
    if (path === `${base}/sources/runs/import` && method === "POST") return json([pending, confirmed], 201);
    if (path === `${base}/candidates/pending-a`) return json({ candidate_id: "pending-a", revision: 0, status: "pending", observation_id: "obs-a", observation_hash: "b".repeat(64), eligibility: "eligible", reasons: [], hash: "c".repeat(64) });
    if (path === `${base}/candidates/confirmed-b`) return json({ candidate_id: "confirmed-b", revision: 1, status: "confirmed", observation_id: "obs-b", observation_hash: "d".repeat(64), eligibility: "eligible", reasons: [], hash: "e".repeat(64), confirmation_id: "confirmation-b" });
    return defaultHandler(path, method, body);
  };
  await mount({ sourceRun: "run-a", canWrite: true, canApprove: false });
  await until(() => expect(button("导入候选").disabled).toBe(false));
  await click("导入候选");
  await until(() => expect(host.textContent).toContain("已导入 2 个候选"));
  await click("补充参考");
  await until(() => expect(host.querySelector("#product-candidate-expected")).toBeTruthy());
  await type("product-candidate-instruction", "计算 2+3");
  await type("product-candidate-input", "2+3");
  await type("product-candidate-expected", "5");
  expect(button("确认人工参考").disabled).toBe(true);
  const candidateButtons = host.querySelectorAll<HTMLButtonElement>(".product-eval-candidates li button");
  await act(async () => candidateButtons[1]!.click());
  await until(() => expect(button("冻结连续评测版本").disabled).toBe(true));
  expect(calls.filter(item => item.path.includes("/confirm") && item.method === "POST")).toHaveLength(0);
  expect(calls.filter(item => item.path === `${base}/datasets/confirmed` && item.method === "POST")).toHaveLength(0);
});

test("permission failures show scoped raw response and allow an explicit read retry", async () => {
  let denied = true;
  handler = (path, _method, _body) => path === `${base}/targets` && denied
    ? json({ error: { code: "permission_denied", detail: "workspace membership required" } }, 403)
    : undefined;
  await mount();
  await until(() => expect(host.querySelector('[role="alert"]')?.textContent).toContain("当前身份没有此工作区的相应权限"));
  const detail = host.querySelector<HTMLDetailsElement>(".product-eval-error details")!;
  expect(detail.open).toBe(false);
  expect(detail.textContent).toContain("permission_denied");
  expect(calls.filter(item => item.method === "POST")).toEqual([]);
  denied = false;
  await click("刷新");
  await until(() => expect(host.querySelector(".product-eval-error")).toBeNull());
  expect(calls.filter(item => item.method === "POST")).toEqual([]);
});

test("quota failure writing the task intent blocks task POST", async () => {
  handler = storedDatasetHandler();
  await chooseStoredDataset();
  await chooseKrimMetric();
  vi.spyOn(Storage.prototype, "setItem").mockImplementation(() => { throw new DOMException("quota exceeded", "QuotaExceededError"); });
  await click("启动评测");
  await until(() => expect(host.querySelector('[role="alert"]')?.textContent).toContain("本地存储空间不足"));
  expect(calls.filter(item => item.path === `${base}/tasks` && item.method === "POST")).toHaveLength(0);
});

test("malformed ledger is surfaced and does not become an empty writable ledger", async () => {
  const storageKey = await evaluationStorageKey("access-key", "team");
  localStorage.setItem(storageKey, "{not-json");
  handler = storedDatasetHandler();
  await chooseStoredDataset();
  await until(() => expect(host.querySelector('[role="alert"]')?.textContent).toContain("本地评测请求账本内容损坏"));
  expect(localStorage.getItem(storageKey)).toBe("{not-json");
  expect(calls.filter(item => item.method === "POST")).toEqual([]);
  const start = [...host.querySelectorAll("button")].find(item => ["启动评测", "正在处理…"].includes(item.textContent?.trim() ?? ""));
  expect(start?.disabled).toBe(true);
});

test("starts TaskSpec once and recovers by original key with read-only GET", async () => {
  let submitted: Record<string, unknown> | null = null;
  handler = (path, method, body) => {
    if (path === `${base}/datasets` && method === "POST") return json(frozenDataset(body!), 201);
    if (path.startsWith(`${base}/datasets?`)) return json([{ name: "script-suite", version: "v1", fingerprint: "a".repeat(64), case_count: 1 }]);
    if (path === `${base}/datasets/script-suite/v1`) return json({ ...frozenDataset({ name: "script-suite", version: "v1", cases: [] }), cases: [{ verifier: "krim-script/1" }] });
    if (path.startsWith(`${base}/tasks?`)) return json([]);
    if (path === `${base}/tasks` && method === "POST") { submitted = body; throw new TypeError("response disconnected after admission"); }
    if (path.startsWith(`${base}/tasks/by-key/`)) return json(taskView(submitted!, "task-recovered"));
    return defaultHandler(path, method, body);
  };
  await mount();
  await until(() => expect(host.textContent).toContain("新建 Krim Script 数据集"));
  await freezeManualDataset();
  await switchToEval();
  await select("product-dataset-choice", "script-suite/v1");
  await until(() => expect(host.textContent).toContain("已选择 script-suite · v1"));
  const checkbox = [...host.querySelectorAll<HTMLInputElement>(".product-eval-metric-choice input")].find(item => item.parentElement?.textContent?.includes("Krim 执行结果"));
  expect(checkbox).toBeTruthy();
  await act(async () => { checkbox!.click(); });
  await click("启动评测");
  await until(() => expect(host.textContent).toContain("网络连接中断"));
  expect(submitted).not.toBeNull();
  expect(submitted).toMatchObject({
    request_key: expect.stringMatching(/^eval-task-/), target_key: "target-script",
    dataset: { name: "script-suite", version: "v1", fingerprint: "a".repeat(64) },
    execution: { repetitions: 1 },
  });
  expect(submitted).not.toHaveProperty("schema_version");
  expect((submitted!.metrics as Array<Record<string, unknown>>).map(item => item.name)).toEqual(["krim_result"]);
  const key = String(submitted!.request_key);
  expect(localStorage.getItem(await evaluationStorageKey("access-key", "team"))).toContain(key);
  expect(host.textContent).toContain(key);
  await click("只读查找");
  await until(() => expect(host.textContent).toContain("已按原键找到任务 task-recovered"));
  expect(calls.filter(item => item.path === `${base}/tasks` && item.method === "POST")).toHaveLength(1);
  expect(calls.filter(item => item.path === `${base}/tasks/by-key/${encodeURIComponent(key)}` && item.method === "GET")).toHaveLength(1);
});

test("unknown manual freeze keeps the original object identity and checks by GET only", async () => {
  let accepted: Record<string, unknown> | null = null;
  handler = (path, method, body) => {
    if (path === `${base}/datasets` && method === "POST") { accepted = frozenDataset(body!); throw new TypeError("freeze response disconnected after admission"); }
    if (path === `${base}/datasets/script-suite/v1`) return json(accepted ?? frozenDataset({ name: "script-suite", version: "v1", cases: [] }));
    return defaultHandler(path, method, body);
  };
  await mount();
  await until(() => expect(host.textContent).toContain("新建 Krim Script 数据集"));
  await freezeManualDataset();
  await until(() => expect(host.textContent).toContain("网络连接中断"));
  expect(localStorage.getItem(await evaluationStorageKey("access-key", "team"))).toContain("dataset-freeze");
  await click("只读检查");
  await until(() => expect(host.textContent).toContain("已按数据集名称与版本找到原冻结结果"));
  expect(calls.filter(item => item.path === `${base}/datasets` && item.method === "POST")).toHaveLength(1);
  expect(calls.filter(item => item.path === `${base}/datasets/script-suite/v1` && item.method === "GET")).toHaveLength(1);
});

test("twenty unresolved task intents are preserved and block another POST", async () => {
  const storageKey = await evaluationStorageKey("access-key", "team");
  const originalKeys = Array.from({ length: 20 }, (_, index) => `unknown-${index}`);
  localStorage.setItem(storageKey, JSON.stringify({ version: 1, task_intents: originalKeys.map((request_key, index) => ({
    request_key, body: { request_key }, created_at: index, state: "unknown",
  })), operation_intents: [], continuous_datasets: [] }));
  handler = storedDatasetHandler();
  await chooseStoredDataset();
  await chooseKrimMetric();
  await click("启动评测");
  await until(() => expect(host.querySelector('[role="alert"]')?.textContent).toContain("已有 20 个尚未确认的评测请求"));
  expect(calls.filter(item => item.path === `${base}/tasks` && item.method === "POST")).toHaveLength(0);
  const persisted = JSON.parse(localStorage.getItem(storageKey)!);
  expect(persisted.task_intents.map((item: { request_key: string }) => item.request_key)).toEqual(originalKeys);
  expect(persisted.task_intents.every((item: { state: string }) => item.state === "unknown")).toBe(true);
});

test("run import is explicit and manual expected text is sent only after confirmation", async () => {
  const imported: ProductCandidateSummary = {
    candidate_id: "candidate-a", observation_id: "observation-a", eligibility: "eligible", reasons: ["run_complete"], source: { run_id: "run-source" },
  };
  let confirmationBody: Record<string, unknown> | null = null;
  handler = (path, method, body) => {
    if (path === `${base}/sources/runs/import` && method === "POST") return json([imported], 201);
    if (path === `${base}/candidates/candidate-a` && method === "GET") return json(confirmationBody
      ? { candidate_id: "candidate-a", revision: 1, status: "confirmed", observation_id: "observation-a", observation_hash: "b".repeat(64), eligibility: "eligible", reasons: ["run_complete"], hash: "c".repeat(64), confirmation_id: "confirmation-a" }
      : { candidate_id: "candidate-a", revision: 0, status: "pending", observation_id: "observation-a", observation_hash: "b".repeat(64), eligibility: "eligible", reasons: ["run_complete"], hash: "c".repeat(64) });
    if (path === `${base}/candidates/candidate-a/confirm` && method === "POST") { confirmationBody = body; throw new TypeError("confirmation response disconnected after admission"); }
    if (path === `${base}/datasets/confirmed` && method === "POST") return json({ dataset: { name: "script-suite", version: "v1", fingerprint: "a".repeat(64) }, dataset_envelope_fp: "d".repeat(64), lineage: {} }, 201);
    return defaultHandler(path, method, body);
  };
  await mount({ sourceRun: "run-source" });
  await until(() => expect(host.textContent).toContain("来源运行：run-source"));
  expect(calls.filter(item => item.method === "POST")).toEqual([]);
  await click("导入候选");
  await until(() => expect(host.textContent).toContain("run_complete"));
  await click("补充参考");
  await until(() => expect(host.querySelector("#product-candidate-expected")).toBeTruthy());
  await type("product-candidate-instruction", "运行期望计算 2+3");
  await type("product-candidate-input", "2 + 3");
  await type("product-candidate-expected", "5");
  await click("确认人工参考");
  await until(() => expect(host.textContent).toContain("网络连接中断"));
  expect(confirmationBody).toMatchObject({
    action: "confirm", context: "complete", case: { verifier: "krim-script/1", files: { "INPUT.txt": "2 + 3" }, reference: { expected: "5" } },
  });
  const key = await evaluationStorageKey("access-key", "team");
  const saved = JSON.parse(localStorage.getItem(key)!);
  expect(saved.operation_intents.find((item: { kind: string }) => item.kind === "candidate-confirm").request_key).toMatch(/^confirm-case-/);
  expect(calls.filter(item => item.path === `${base}/candidates/candidate-a/confirm` && item.method === "POST")).toHaveLength(1);
  await click("只读检查确认状态");
  await until(() => expect(host.textContent).toContain("已找到原人工确认记录"));
  await click("冻结连续评测版本");
  await until(() => expect(host.textContent).toContain("连续评测版本 script-suite · v1 已冻结"));
  await until(() => expect(calls.some(item => item.path.includes("/metrics?") && new URLSearchParams(item.path.split("?")[1]).get("mode") === "continuous")).toBe(true));
  expect(calls.some(item => item.path.includes("/metrics?") && new URLSearchParams(item.path.split("?")[1]).get("target_key") === "target-script")).toBe(true);
});

test("late initial metric catalog cannot strand a fast dataset and target switch", async () => {
  let releaseInitial!: (response: Response) => void;
  const initial = new Promise<Response>(resolve => { releaseInitial = resolve; });
  let held = false;
  const seenTargets: string[] = [];
  handler = (path, _method, _body) => {
    if (path === `${base}/targets`) return json([{ key: "krim-script-bad", version: "1", kind: "krim-run" }, { key: "krim-script-v2", version: "2", kind: "krim-run" }]);
    if (path.startsWith(`${base}/datasets?`)) return json([storedDatasetRow]);
    if (path === `${base}/datasets/suite/v1`) return json(storedDataset());
    if (path.startsWith(`${base}/metrics?`)) {
      const selected = new URLSearchParams(path.split("?")[1]).get("target_key") ?? "";
      seenTargets.push(selected);
      if (!held) { held = true; return initial; }
      return json(metrics);
    }
    return undefined;
  };
  await mount({ view: "eval" });
  await until(() => expect(host.querySelector<HTMLOptionElement>("#product-target option")?.value).toBe("krim-script-bad"));
  await until(() => expect(seenTargets).toContain("krim-script-bad"));
  await until(() => expect(host.querySelector('#product-dataset-choice option[value="suite/v1"]')).toBeTruthy());
  await select("product-dataset-choice", "suite/v1");
  await until(() => expect(host.textContent).toContain("已选择 suite · v1"));
  await select("product-target", "krim-script-v2");
  await until(() => expect(seenTargets).toContain("krim-script-v2"));
  await until(() => expect(host.textContent).toContain("Krim 执行结果"));
  expect(button("启动评测").disabled).toBe(true);
  await chooseKrimMetric();
  await until(() => expect(button("启动评测").disabled).toBe(false));
  await act(async () => releaseInitial(json([{ name: "stale-metric", version: "1", implementation_digest: "f".repeat(64), config: {}, required_evidence: [], hard: true }])));
  expect(host.textContent).not.toContain("stale-metric");
  expect(host.textContent).toContain("Krim 执行结果");
  expect(calls.filter(item => item.path === `${base}/tasks` && item.method === "POST")).toHaveLength(0);
});

test("dataset GET restores continuous selection in a browser without a local ledger", async () => {
  handler = storedDatasetHandler("d".repeat(64));
  const storageKey = await evaluationStorageKey("access-key", "team");
  expect(localStorage.getItem(storageKey)).toBeNull();
  await chooseStoredDataset();
  await until(() => expect(host.textContent).toContain("已包含人工确认记录，可生成分页报告"));
  await until(() => expect(calls.some(item => item.path.includes("/metrics?") && new URLSearchParams(item.path.split("?")[1]).get("mode") === "continuous")).toBe(true));
  expect(JSON.parse(localStorage.getItem(storageKey)!).continuous_datasets[0].dataset_envelope_fp).toBe("d".repeat(64));
  expect(calls.filter(item => item.method === "POST")).toEqual([]);
});

test("unknown run import keeps its request key and recovers only through the matching GET receipt", async () => {
  const onRun = vi.fn();
  let accepted: Record<string, unknown> | null = null;
  const imported = { candidate_id: "receipt-candidate", observation_id: "receipt-observation", eligibility: "eligible", reasons: [], source: { run_id: "run-unknown" } };
  handler = (path, method, body) => {
    if (path === `${base}/sources/runs/import` && method === "POST") { accepted = body; throw new TypeError("import response disconnected after admission"); }
    if (path.startsWith(`${base}/sources/runs/imports/by-key/`)) return json({ request_key: accepted?.request_key, candidates: [imported] });
    return undefined;
  };
  await mount({ sourceRun: "run-unknown", onRun });
  await until(() => expect(button("导入候选").disabled).toBe(false));
  await click("导入候选");
  await until(() => expect(host.textContent).toContain("网络连接中断"));
  const admitted = accepted as Record<string, unknown> | null;
  expect(admitted?.request_key).toMatch(/^import-run-/);
  const storageKey = await evaluationStorageKey("access-key", "team");
  expect(localStorage.getItem(storageKey)).toContain(String(admitted?.request_key));
  await click("只读检查");
  await until(() => expect(host.textContent).toContain("已按原请求键找回 1 个候选"));
  expect(calls.filter(item => item.path.startsWith(`${base}/sources/runs/imports/by-key/`) && item.method === "GET")).toHaveLength(1);
  expect(calls.filter(item => item.path === `${base}/sources/runs/import` && item.method === "POST")).toHaveLength(1);
  await click("打开运行记录");
  expect(onRun).toHaveBeenCalledWith("run-unknown");
});

test("manual task reads paged report and uses backend strict comparison contract", async () => {
  const baseline = completedTask("baseline-a"), candidate = completedTask("candidate-b");
  let comparisonBody: Record<string, unknown> | null = null;
  handler = (path, method, body) => {
    if (path.startsWith(`${base}/tasks?`)) return json([baseline, candidate]);
    if (path === `${base}/tasks/${baseline.id}`) return json(baseline);
    if (path === `${base}/tasks/${candidate.id}`) return json(candidate);
    if (path.startsWith(`${base}/tasks/`) && path.includes("/report?")) {
      const id = path.includes(baseline.id) ? baseline.id : candidate.id;
      return json({ eval_id: id, revision: 1, report_hash: "e".repeat(64), kind: "manual", subject_fingerprint: null,
        dataset: baseline.dataset, execution: { status: "completed", complete: true, planned_count: 1 },
        coverage: { cell_coverage: 1, scored_cells: 1, planned_cells: 1 }, quality: { pass_rate: 1, passed: 1 },
        rows: [{ case_id: "case-1", repetition: 0, status: "finished", quality: "passed", scores: [{ rule: { name: "krim_result" }, value: 1 }] }], next_cursor: null, next_offset: null });
    }
    if (path === `${base}/tasks/compare` && method === "POST") { comparisonBody = body; return json({ status: "pass", reasons: [] }); }
    return defaultHandler(path, method, body);
  };
  await mount({ view: "eval" });
  await until(() => expect(host.querySelectorAll(".product-eval-task-row")).toHaveLength(2));
  await act(async () => host.querySelector<HTMLButtonElement>(".product-eval-task-row")!.click());
  await until(() => expect(button("查看报告").disabled).toBe(false));
  await click("查看报告");
  await until(() => expect(host.textContent).toContain("case-1"));
  expect(host.textContent).toContain("通过");
  await select("product-baseline", baseline.id);
  await select("product-candidate-task", candidate.id);
  await click("比较基线与候选");
  await until(() => expect(host.textContent).toContain("比较通过"));
  expect(comparisonBody).toMatchObject({ baseline_id: baseline.id, candidate_id: candidate.id, allowed_subjects: [] });
  expect(comparisonBody).not.toHaveProperty("schema_version");
});

test("identity or workspace switch fences late catalog responses and isolates ledger", async () => {
  let release!: (value: Response) => void;
  const waiting = new Promise<Response>(resolve => { release = resolve; });
  let targetRequests = 0;
  handler = (path, _method, _body) => {
    if (path === `${base}/targets`) {
      targetRequests += 1;
      if (targetRequests === 1) return waiting;
      return json([{ key: "fresh-target", version: "3", kind: "registered" }]);
    }
    return undefined;
  };
  await mount({ identity: { principal_id: "alice", generation: 1 } });
  await until(() => expect(calls.some(item => item.path === `${base}/targets`)).toBe(true));
  await act(async () => root.render(<StrictMode><EvaluationProductWorkbench apiKey="access-key" workspace="team" identity={{ principal_id: "bob", generation: 2 }} active view="eval" /></StrictMode>));
  await until(() => expect(host.querySelector<HTMLOptionElement>("#product-target option")?.value).toBe("fresh-target"));
  await act(async () => release(json([{ key: "stale-target", version: "1", kind: "registered" }])));
  expect(host.querySelector("#product-target")?.textContent).not.toContain("stale-target");
  expect(await evaluationStorageKey("access-key", "team", { principal_id: "alice", generation: 1 })).not.toBe(await evaluationStorageKey("access-key", "team", { principal_id: "bob", generation: 2 }));
  expect(await evaluationStorageKey("access-key", "team", { principal_id: "alice", generation: 1 })).not.toBe(await evaluationStorageKey("access-key", "other-team", { principal_id: "alice", generation: 1 }));
});
