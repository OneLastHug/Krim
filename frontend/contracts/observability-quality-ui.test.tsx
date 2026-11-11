import { act, StrictMode } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, expect, test, vi } from "vitest";
import { ObservabilityEvaluations, ObservabilityReview, type ObservabilityQualityProps } from "../src/observability/quality";
import type { Actor } from "../src/lib/session-contracts";

(globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean }).IS_REACT_ACT_ENVIRONMENT = true;
let root: Root;
let host: HTMLDivElement;
let calls: Array<{ path: string; method: string; body: Record<string, unknown> | null; auth?: string; signal?: AbortSignal }> = [];
type Handler = (path: string, method: string, body: Record<string, unknown> | null, signal: AbortSignal | undefined) => Promise<Response> | Response | undefined;
let handler: Handler | undefined;
const json = (value: unknown, status = 200) => new Response(JSON.stringify(value), { status, headers: { "Content-Type": "application/json" } });
const base = "/v4/workspaces/team/eval";
const onNavigate = vi.fn();

function actor(role: string, approve: boolean, workspace = "team"): Actor {
  return { principal_id: "alice", generation: 1, legacy: false, workspaces: [{ workspace_id: workspace, role, approve }] };
}
const memberWriter = actor("member", true);
const datasetRow = { name: "suite", version: "v1", fingerprint: "a".repeat(64), case_count: 2 };
function taskFixture(id: string, extra: Record<string, unknown> = {}) {
  return {
    id, request_key: `key-${id}`, status: "completed", code: null, revision: 1, mode: "execute", kind: "manual", layout: 2,
    dataset: { name: "suite", version: "v1", fingerprint: "a".repeat(64) }, target_key: "target-script",
    metrics: [{ name: "krim_result", version: "1", implementation_digest: "1".repeat(64), config: {}, required_evidence: [], hard: true }],
    planned_count: 3, finished_count: 2, created_at: 1, finished_at: 2, report_hash: "f".repeat(64), ...extra,
  };
}
function reportFixture(taskId: string) {
  return {
    eval_id: taskId, revision: 1, report_hash: "e".repeat(64), kind: "manual", subject_fingerprint: null,
    dataset: { name: "suite", version: "v1", fingerprint: "a".repeat(64) },
    execution: { status: "completed", complete: true, planned_count: 2 },
    coverage: { cell_coverage: 0.5, item_coverage: 0.5, scored_cells: 1, planned_cells: 2 },
    quality: { pass_rate: 0.5, passed: 1 },
    rows: [
      { case_id: "case-a", repetition: 0, status: "finished", quality: "passed", scores: [{ rule: { name: "krim_result" }, value: 1 }] },
      { case_id: "case-b", repetition: 0, status: "finished", quality: "failed", scores: [{ rule: { name: "krim_result" }, value: 0 }] },
    ],
    next_cursor: null, next_offset: null,
  };
}
function candidateFixture(extra: Record<string, unknown> = {}) {
  return {
    candidate_id: "candidate-a", revision: 0, status: "pending", observation_id: "obs-a", observation_hash: "b".repeat(64),
    eligibility: "eligible", reasons: [], hash: "c".repeat(64), ...extra,
  };
}
function evalDefaultHandler(path: string, method: string) {
  if (path === `${base}/tasks?offset=0&limit=20` && method === "GET") return json([]);
  if (path === `${base}/datasets?offset=0&limit=20` && method === "GET") return json([]);
  return json({ error: { code: "not_found" } }, 404);
}
function mountEval(props: Partial<ObservabilityQualityProps> = {}) {
  return act(async () => root.render(<StrictMode>
    <ObservabilityEvaluations apiKey="access-key" workspace="team" identity={memberWriter} onNavigate={onNavigate} {...props} />
  </StrictMode>));
}
function mountReview(props: Partial<ObservabilityQualityProps> = {}) {
  return act(async () => root.render(<StrictMode>
    <ObservabilityReview apiKey="access-key" workspace="team" identity={memberWriter} onNavigate={onNavigate} {...props} />
  </StrictMode>));
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
const taskRows = () => host.querySelectorAll<HTMLButtonElement>(".obs-eval-task-row");

beforeEach(() => {
  calls = []; handler = undefined; onNavigate.mockClear();
  host = document.createElement("div"); document.body.append(host); root = createRoot(host);
  vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, options: RequestInit = {}) => {
    const headers = (options.headers ?? {}) as Record<string, string>;
    const entry = {
      path: String(input), method: options.method ?? "GET",
      body: options.body ? JSON.parse(String(options.body)) as Record<string, unknown> : null,
      auth: headers.Authorization, signal: options.signal ?? undefined,
    };
    calls.push(entry);
    return (await handler?.(entry.path, entry.method, entry.body, entry.signal)) ?? evalDefaultHandler(entry.path, entry.method);
  }));
});
afterEach(async () => { await act(async () => root.unmount()); host.remove(); vi.restoreAllMocks(); vi.unstubAllGlobals(); });

test("评测页按 v4 端点读取任务与数据集，密钥随请求头发送，初始不发送任何写请求", async () => {
  await mountEval({ identity: actor("viewer", false) });
  await until(() => expect(host.textContent).toContain("当前工作区暂无评测任务"));
  const taskCall = calls.find(item => item.path === `${base}/tasks?offset=0&limit=20`);
  const datasetCall = calls.find(item => item.path === `${base}/datasets?offset=0&limit=20`);
  expect(taskCall?.method).toBe("GET");
  expect(taskCall?.auth).toBe("Bearer access-key");
  expect(datasetCall?.method).toBe("GET");
  expect(datasetCall?.auth).toBe("Bearer access-key");
  expect(calls.some(item => item.method === "POST")).toBe(false);
  await click("刷新");
  await until(() => expect(calls.filter(item => item.path === `${base}/tasks?offset=0&limit=20`).length).toBeGreaterThan(1));
  expect(calls.every(item => item.method === "GET")).toBe(true);
});

test("任务行展示真实摘要与逐样本报告；未知计数保持未知，不补 100%", async () => {
  const counted = taskFixture("task-count");
  const unknownCount = taskFixture("task-unknown", { finished_count: null, report_hash: null });
  handler = path => {
    if (path === `${base}/tasks?offset=0&limit=20`) return json([counted, unknownCount]);
    if (path === `${base}/datasets?offset=0&limit=20`) return json([datasetRow]);
    if (path === `${base}/tasks/task-count/report?limit=16`) return json(reportFixture("task-count"));
    return undefined;
  };
  await mountEval();
  await until(() => expect(taskRows()).toHaveLength(2));
  expect(taskRows()[0]!.textContent).toContain("suite · v1");
  expect(taskRows()[0]!.textContent).toContain("target-script");
  expect(taskRows()[0]!.textContent).toContain("2 / 3");
  expect(taskRows()[1]!.textContent).toContain("— / 3");
  expect(host.textContent).toContain("样本 2");
  await act(async () => taskRows()[0]!.click());
  await until(() => expect(host.textContent).toContain("任务详情"));
  expect(host.textContent).toContain("key-task-count");
  await click("查看报告");
  await until(() => expect(host.textContent).toContain("case-a"));
  expect(calls.some(item => item.path === `${base}/tasks/task-count/report?limit=16` && item.method === "GET")).toBe(true);
  expect(host.textContent).toContain("50%");
  expect(host.textContent).toContain("通过");
  expect(host.textContent).toContain("krim_result 1");
  expect(host.textContent).not.toContain("100%");
  expect(host.textContent).toContain("已读 2 项样本");
});

test("切换工作区时旧请求立即中止、旧数据被清除，迟到响应不会渲染", async () => {
  const pending: Array<{ signal?: AbortSignal; release: (value: Response) => void }> = [];
  handler = (path, _method, _body, signal) => {
    if (path === `${base}/tasks?offset=0&limit=20`) {
      return new Promise<Response>((resolve, reject) => {
        const onAbort = () => reject(new DOMException("aborted", "AbortError"));
        signal?.addEventListener("abort", onAbort);
        pending.push({ signal, release: value => { signal?.removeEventListener("abort", onAbort); resolve(value); } });
      });
    }
    return undefined;
  };
  await mountEval();
  await until(() => expect(pending.length).toBeGreaterThan(0));
  await mountEval({ workspace: "other" });
  expect(pending.some(item => item.signal?.aborted)).toBe(true);
  await act(async () => pending.forEach(item => item.release(json([taskFixture("stale-task")]))));
  expect(host.textContent).not.toContain("stale-task");
  expect(calls.some(item => item.path === "/v4/workspaces/other/eval/tasks?offset=0&limit=20")).toBe(true);
});

test("比较复用既有只读 compare 契约，不启动任务", async () => {
  const baseline = taskFixture("task-base"), candidate = taskFixture("task-cand");
  handler = (path, method) => {
    if (path === `${base}/tasks?offset=0&limit=20`) return json([baseline, candidate]);
    if (path === `${base}/tasks?offset=2&limit=20`) return json([]);
    if (path === `${base}/datasets?offset=0&limit=20`) return json([]);
    if (path === `${base}/tasks/compare` && method === "POST") return json({ status: "pass", policy: "strict-krim-regression/1", reasons: [] });
    return undefined;
  };
  await mountEval({ identity: actor("viewer", false) });
  await until(() => expect(taskRows()).toHaveLength(2));
  await select("obs-eval-compare-baseline", "task-base");
  await select("obs-eval-compare-candidate", "task-cand");
  await click("比较基线与候选");
  await until(() => expect(host.textContent).toContain("比较通过"));
  const compare = calls.find(item => item.path === `${base}/tasks/compare` && item.method === "POST");
  expect(compare?.body).toEqual({
    baseline_id: "task-base", candidate_id: "task-cand",
    baseline_hash: "f".repeat(64), candidate_hash: "f".repeat(64), allowed_subjects: [],
  });
  expect(calls.some(item => item.path === `${base}/tasks` && item.method === "POST")).toBe(false);
});

test("viewer 可读取候选但不能进入确认表单，全程只读", async () => {
  handler = path => path === `${base}/candidates/candidate-a` ? json(candidateFixture()) : undefined;
  await mountReview({ identity: actor("viewer", false) });
  await type("obs-review-lookup-id", "candidate-a");
  await click("读取候选");
  await until(() => expect(host.textContent).toContain("候选 candidate-a"));
  expect(calls.some(item => item.path === `${base}/candidates/candidate-a` && item.method === "GET")).toBe(true);
  expect(host.textContent).toContain("待确认");
  expect(host.textContent).toContain("viewer 身份只能读取候选");
  expect(host.querySelector("#obs-review-case")).toBeNull();
  expect(host.querySelector("#obs-review-action")).toBeNull();
  expect(calls.some(item => item.method === "POST")).toBe(false);
});

test("缺上下文的候选默认标记 manual_reconstructed；无 approve 权限不能提交", async () => {
  handler = path => path === `${base}/candidates/candidate-a`
    ? json(candidateFixture({ eligibility: "needs_context", reasons: ["missing_reference"] }))
    : undefined;
  await mountReview({ identity: actor("member", false) });
  await type("obs-review-lookup-id", "candidate-a");
  await click("读取候选");
  await until(() => expect((host.querySelector("#obs-review-context") as HTMLSelectElement).value).toBe("manual_reconstructed"));
  expect(host.textContent).toContain("由人工重建填写");
  expect((host.querySelector("#obs-review-action") as HTMLSelectElement).value).toBe("confirm");
  expect(button("提交确认").disabled).toBe(true);
  expect(calls.some(item => item.method === "POST")).toBe(false);
});

test("确认提交既有 confirm 服务并成功后重读候选", async () => {
  let confirmed = false;
  handler = (path, method) => {
    if (path === `${base}/candidates/candidate-a` && method === "GET") {
      return json(confirmed ? candidateFixture({ revision: 1, status: "confirmed", confirmation_id: "confirmation-9" }) : candidateFixture());
    }
    if (path === `${base}/candidates/candidate-a/confirm` && method === "POST") { confirmed = true; return json({ id: "confirmation-9" }); }
    return undefined;
  };
  await mountReview();
  await type("obs-review-lookup-id", "candidate-a");
  await click("读取候选");
  await until(() => expect(host.querySelector("#obs-review-case")).toBeTruthy());
  expect(button("提交确认").disabled).toBe(true);
  await type("obs-review-case", JSON.stringify({
    case_id: "case-a", instruction: "计算 2+2", files: { "INPUT.txt": "2 + 2" }, reference: { expected: "4" }, verifier: "krim-script/1",
  }));
  await type("obs-review-reason", "人工核对运行输出后补充参考结果");
  await until(() => expect(button("提交确认").disabled).toBe(false));
  await click("提交确认");
  await until(() => expect(host.textContent).toContain("confirmation-9"));
  const confirm = calls.find(item => item.path === `${base}/candidates/candidate-a/confirm` && item.method === "POST");
  expect(confirm?.body).toMatchObject({
    action: "confirm", context: "complete", expected_revision: 0, expected_hash: "c".repeat(64),
    case: { case_id: "case-a", instruction: "计算 2+2", files: { "INPUT.txt": "2 + 2" }, reference: { expected: "4" }, verifier: "krim-script/1" },
    reason: "人工核对运行输出后补充参考结果",
  });
  expect(String(confirm?.body?.request_key)).toMatch(/^confirm-case-/);
  expect(calls.filter(item => item.path === `${base}/candidates/candidate-a` && item.method === "GET")).toHaveLength(2);
  expect(host.textContent).toContain("已确认");
});

test("保存失败保留草稿，重试沿用同一确认键，不换键重发", async () => {
  let attempts = 0;
  let confirmed = false;
  handler = (path, method) => {
    if (path === `${base}/candidates/candidate-a` && method === "GET") {
      return json(confirmed ? candidateFixture({ status: "confirmed", confirmation_id: "confirmation-7" }) : candidateFixture());
    }
    if (path === `${base}/candidates/candidate-a/confirm` && method === "POST") {
      attempts += 1;
      if (attempts === 1) throw new TypeError("response disconnected after admission");
      confirmed = true;
      return json({ id: "confirmation-7" });
    }
    return undefined;
  };
  await mountReview();
  await type("obs-review-lookup-id", "candidate-a");
  await click("读取候选");
  await until(() => expect(host.querySelector("#obs-review-case")).toBeTruthy());
  await type("obs-review-case", JSON.stringify({
    case_id: "case-a", instruction: "计算 2+2（人工复核）", files: { "INPUT.txt": "2 + 2" }, reference: { expected: "4" }, verifier: "krim-script/1",
  }));
  await type("obs-review-reason", "人工核对运行输出后补充参考结果");
  await click("提交确认");
  await until(() => expect(host.textContent).toContain("可能已被受理"));
  expect((host.querySelector("#obs-review-case") as HTMLTextAreaElement).value).toContain("人工复核");
  expect((host.querySelector("#obs-review-reason") as HTMLTextAreaElement).value).toContain("人工核对");
  await click("提交确认");
  await until(() => expect(host.textContent).toContain("confirmation-7"));
  const confirmCalls = calls.filter(item => item.path === `${base}/candidates/candidate-a/confirm` && item.method === "POST");
  expect(confirmCalls).toHaveLength(2);
  expect(confirmCalls[0]!.body?.request_key).toBe(confirmCalls[1]!.body?.request_key);
});

test("样本 JSON 无效或理由缺失时阻止提交", async () => {
  handler = path => path === `${base}/candidates/candidate-a` ? json(candidateFixture()) : undefined;
  await mountReview();
  await type("obs-review-lookup-id", "candidate-a");
  await click("读取候选");
  await until(() => expect(host.querySelector("#obs-review-case")).toBeTruthy());
  await type("obs-review-case", "{ not json");
  await until(() => expect(host.textContent).toContain("样本 JSON 无法解析"));
  expect(button("提交确认").disabled).toBe(true);
  expect(calls.some(item => item.method === "POST")).toBe(false);
});

test("回执键查找复用导入回执端点，来源运行随候选展示", async () => {
  handler = path => {
    if (path === `${base}/sources/runs/imports/by-key/receipt-1`) {
      return json({ request_key: "receipt-1", candidates: [{ candidate_id: "rc-1", observation_id: "obs-r", eligibility: "eligible", reasons: ["run_complete"], source: { run_id: "run-r" } }] });
    }
    if (path === `${base}/candidates/rc-1`) return json(candidateFixture({ candidate_id: "rc-1", observation_id: "obs-r" }));
    return undefined;
  };
  await mountReview();
  await type("obs-review-lookup-id", "receipt-1");
  await click("按回执键查找");
  await until(() => expect(host.textContent).toContain("回执 receipt-1"));
  expect(calls.some(item => item.path === `${base}/sources/runs/imports/by-key/receipt-1` && item.method === "GET")).toBe(true);
  await click("打开");
  await until(() => expect(host.textContent).toContain("候选 rc-1"));
  expect(host.textContent).toContain("run-r");
  expect(calls.some(item => item.path === `${base}/candidates/rc-1` && item.method === "GET")).toBe(true);
});

const confirmCase = (instruction: string) => JSON.stringify({
  case_id: "case-a", instruction, files: { "INPUT.txt": "2 + 2" }, reference: { expected: "4" }, verifier: "krim-script/1",
});
const confirmReason = "人工核对运行输出后补充参考结果";
const confirmPosts = () => calls.filter(item => item.path === `${base}/candidates/candidate-a/confirm` && item.method === "POST");
async function openReadyCandidate() {
  await type("obs-review-lookup-id", "candidate-a");
  await click("读取候选");
  await until(() => expect(host.querySelector("#obs-review-case")).toBeTruthy());
  await type("obs-review-case", confirmCase("计算 2+2"));
  await type("obs-review-reason", confirmReason);
  await until(() => expect(button("提交确认").disabled).toBe(false));
}

test("确定未受理的 422 清除意图，改正后换新键重发成功", async () => {
  let attempts = 0;
  let confirmed = false;
  handler = (path, method) => {
    if (path === `${base}/candidates/candidate-a` && method === "GET") {
      return json(confirmed ? candidateFixture({ status: "confirmed", confirmation_id: "confirmation-5" }) : candidateFixture());
    }
    if (path === `${base}/candidates/candidate-a/confirm` && method === "POST") {
      attempts += 1;
      if (attempts === 1) return json({ error: { code: "invalid_input" } }, 422);
      confirmed = true;
      return json({ id: "confirmation-5" });
    }
    return undefined;
  };
  await mountReview();
  await openReadyCandidate();
  await click("提交确认");
  await until(() => expect(host.textContent).toContain("已确定未受理"));
  await type("obs-review-case", confirmCase("计算 2+2（改正）"));
  await until(() => expect(button("提交确认").disabled).toBe(false));
  await click("提交确认");
  await until(() => expect(host.textContent).toContain("confirmation-5"));
  const posts = confirmPosts();
  expect(posts).toHaveLength(2);
  expect(posts[0]!.body?.request_key).not.toBe(posts[1]!.body?.request_key);
  expect((posts[1]!.body as { case: { instruction: string } }).case.instruction).toBe("计算 2+2（改正）");
});

test("未知结果保留意图：改正内容被阻止，重读候选恢复冻结草稿后同键重试成功", async () => {
  let attempts = 0;
  let confirmed = false;
  handler = (path, method) => {
    if (path === `${base}/candidates/candidate-a` && method === "GET") {
      return json(confirmed ? candidateFixture({ status: "confirmed", confirmation_id: "confirmation-3" }) : candidateFixture());
    }
    if (path === `${base}/candidates/candidate-a/confirm` && method === "POST") {
      attempts += 1;
      if (attempts === 1) throw new TypeError("response disconnected after admission");
      confirmed = true;
      return json({ id: "confirmation-3" });
    }
    return undefined;
  };
  await mountReview();
  await openReadyCandidate();
  await click("提交确认");
  await until(() => expect(host.textContent).toContain("可能已被受理"));
  expect(confirmPosts()).toHaveLength(1);
  await type("obs-review-case", confirmCase("计算 3+3"));
  await until(() => expect(host.textContent).toContain("不能修改重发"));
  expect(button("提交确认").disabled).toBe(true);
  expect(confirmPosts()).toHaveLength(1);
  await click("重新读取候选");
  await until(() => expect((host.querySelector("#obs-review-case") as HTMLTextAreaElement).value).toContain("计算 2+2"));
  await until(() => expect(button("提交确认").disabled).toBe(false));
  await click("提交确认");
  await until(() => expect(host.textContent).toContain("confirmation-3"));
  const posts = confirmPosts();
  expect(posts).toHaveLength(2);
  expect(posts[0]!.body?.request_key).toBe(posts[1]!.body?.request_key);
  expect(posts[1]!.body?.expected_revision).toBe(0);
});

test.each([
  ["confirm", "提交确认"], ["edit", "保存修改"], ["reject", "提交拒绝"],
])("%s 缺少验证器要求的参考字段时不向服务发送无效请求", async (action, label) => {
  handler = (path, method) => {
    if (path === `${base}/candidates/candidate-a` && method === "GET") return json(candidateFixture());
    return undefined;
  };
  await mountReview();
  await openReadyCandidate();
  await act(async () => {
    const select = host.querySelector<HTMLSelectElement>("#obs-review-action")!;
    select.value = action;
    select.dispatchEvent(new Event("change", { bubbles: true }));
  });
  const value = JSON.parse(confirmCase("等待补充参考"));
  value.reference = {};
  await type("obs-review-case", JSON.stringify(value));
  expect(button(label).disabled).toBe(true);
  expect(host.textContent).toContain("reference.expected 必须为字符串");
  expect(confirmPosts()).toHaveLength(0);
});

test.each([
  ["reject", "rejected", "提交拒绝", "已拒绝"],
  ["edit", "pending", "保存修改", "待确认"],
])("%s 已受理但重读失败时保留真实动作状态并等待重读", async (action, status, label, visible) => {
  let readFails = false;
  handler = (path, method) => {
    if (path === `${base}/candidates/candidate-a` && method === "GET") {
      if (readFails) throw new TypeError("re-read disconnected");
      return json(candidateFixture());
    }
    if (path === `${base}/candidates/candidate-a/confirm` && method === "POST") {
      readFails = true;
      return json({ id: "receipt-action", status });
    }
    return undefined;
  };
  await mountReview();
  await openReadyCandidate();
  await act(async () => {
    const select = host.querySelector<HTMLSelectElement>("#obs-review-action")!;
    select.value = action;
    select.dispatchEvent(new Event("change", { bubbles: true }));
  });
  await click(label);
  await until(() => expect(host.textContent).toContain("候选状态重读未完成"));
  expect(host.textContent).toContain(visible);
  expect(button(label).disabled).toBe(true);
  expect(confirmPosts()).toHaveLength(1);
});

test("确认成功但重读失败不再生成新键，恢复读取后显示已确认", async () => {
  let confirmed = false;
  let readFails = false;
  handler = (path, method) => {
    if (path === `${base}/candidates/candidate-a` && method === "GET") {
      if (readFails) throw new TypeError("re-read disconnected");
      return json(confirmed ? candidateFixture({ status: "confirmed", confirmation_id: "confirmation-2" }) : candidateFixture());
    }
    if (path === `${base}/candidates/candidate-a/confirm` && method === "POST") {
      confirmed = true;
      readFails = true;
      return json({ id: "confirmation-2" });
    }
    return undefined;
  };
  await mountReview();
  await openReadyCandidate();
  await click("提交确认");
  await until(() => expect(host.textContent).toContain("候选状态重读未完成"));
  expect(confirmPosts()).toHaveLength(1);
  expect(button("提交确认").disabled).toBe(true);
  readFails = false;
  await click("重新读取候选");
  await until(() => expect(host.textContent).toContain("已确认"));
  expect(confirmPosts()).toHaveLength(1);
});
