import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, expect, test, vi } from "vitest";
import { webcrypto } from "node:crypto";
import { EvaluationAutomationPanel, type EvaluationAutomationPanelProps } from "../src/components/EvaluationAutomationPanel";
import {
  evaluationAutomationStorageKey,
  readAutomationLedger,
  parseScheduleView,
  type AutomationStatus,
  type ScheduleView,
} from "../src/lib/evaluation-automation";

(globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean }).IS_REACT_ACT_ENVIRONMENT = true;

const automationBase = "/v4/workspaces/team/eval/automation";
const evaluationBase = "/v4/workspaces/team/eval";
const identity = { principal_id: "alice", generation: 1 };
const defaultStatus: AutomationStatus = {
  automation: { running: true, last_error: null },
  capture: {
    configured: true, enabled: false, observer_ready: true, pending: 0, claimed: 0, acknowledged: 2,
    counters: { seen: 2, enqueued: 2, dropped: 0, acked: 2 },
    text_capture: false, argument_capture: false, artifact_count: 0,
  },
  schedules: { total: 1, enabled: 1 },
};
const metricNames = ["fact_integrity", "context_coverage", "krim_result", "process_constraints"];
const metrics = metricNames.map((name, index) => ({
  name,
  version: "1",
  implementation_digest: String(index + 1).repeat(64),
  config: {},
  required_evidence: [],
  hard: index < 3,
}));
const datasetRow = { name: "script-suite", version: "v1", fingerprint: "a".repeat(64), case_count: 1 };
const target = { key: "target-script", version: "2", kind: "registered" };
function schedule(id = "nightly-ci", enabled = true): ScheduleView {
  return {
    schedule_id: id, version: "v1", enabled, revision: enabled ? 0 : 1, observed_through: -1, missed_windows: 0,
    dataset: { name: "script-suite", version: "v1", fingerprint: datasetRow.fingerprint },
    target_key: target.key, epoch: 1000, interval_seconds: 3600, max_windows: 2,
    control_receipt: enabled ? null : { key: "disable-key-fixture", decision: "disabled", expected_revision: 0, revision: 1 },
  };
}
function scheduleSpec(id: string, version = "v1") {
  return {
    scope: "local",
    schedule_id: id,
    version,
    template: {
      schema_version: "continuous-task/1", scope: "local", request_key: "schedule-task-fixture", mode: "execute",
      dataset: { name: datasetRow.name, version: datasetRow.version, fingerprint: datasetRow.fingerprint },
      dataset_envelope_fp: "b".repeat(64), target_key: target.key, metrics: metrics.slice(0, 3),
      execution: { repetitions: 1, concurrency: 1, item_timeout_seconds: 20, scorer_timeout_seconds: 1, batch_timeout_seconds: 120 },
      parent_task_id: null, parent_report_hash: null, observations: [], seed_policy: "unseeded/1", schedule: null,
    },
    epoch: 1000, interval_seconds: 3600, max_windows: 2,
  };
}
function publicScheduleSpec(spec: Record<string, unknown>): Record<string, unknown> {
  const copy = structuredClone(spec);
  delete copy.scope;
  const template = copy.template as Record<string, unknown>;
  delete template.scope;
  return copy;
}
function canonicalJson(value: unknown): string {
  if (value === null || typeof value === "string" || typeof value === "boolean" || typeof value === "number") return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  const record = value as Record<string, unknown>;
  return `{${Object.keys(record).sort().map(key => `${JSON.stringify(key)}:${canonicalJson(record[key])}`).join(",")}}`;
}
async function fingerprint(value: unknown): Promise<string> {
  const digest = await webcrypto.subtle.digest("SHA-256", new TextEncoder().encode(canonicalJson(value)));
  return [...new Uint8Array(digest)].map(item => item.toString(16).padStart(2, "0")).join("");
}
async function scheduleDetail(value: ScheduleView): Promise<Record<string, unknown>> {
  const spec = publicScheduleSpec(scheduleSpec(value.schedule_id, value.version));
  return {
    ...value,
    spec,
    spec_fingerprint: await fingerprint(spec),
    control_receipt: value.control_receipt ?? null,
  };
}
function continuousDataset(withEnvelope = true) {
  return {
    ...datasetRow,
    cases: [{ case_id: "case-1", verifier: "krim-script/1", reference: { expected: "ok" } }],
    ...(withEnvelope ? { dataset_envelope_fp: "b".repeat(64) } : {}),
  };
}
function json(value: unknown, status = 200): Response {
  return new Response(JSON.stringify(value), { status, headers: { "Content-Type": "application/json" } });
}

let root: Root;
let host: HTMLDivElement;
let calls: Array<{ path: string; method: string; body: Record<string, unknown> | null }> = [];
let handler: ((path: string, method: string, body: Record<string, unknown> | null) => Promise<Response | undefined> | Response | undefined) | undefined;
let schedules: ScheduleView[];

async function defaultHandler(path: string, method: string, body: Record<string, unknown> | null): Promise<Response> {
  if (path === `${automationBase}/status`) return json(defaultStatus);
  if (path.startsWith(`${automationBase}/schedules?`)) return json(schedules);
  const currentWindowPath = path.match(new RegExp(`^${automationBase}/schedules/([^/?]+)/windows/current$`));
  if (currentWindowPath && method === "GET") {
    const found = schedules.find(item => item.schedule_id === decodeURIComponent(currentWindowPath[1]!));
    return found ? json({ schedule_id: found.schedule_id, version: found.version, window_index: 0, enabled: found.enabled, eligible: found.enabled })
      : json({ error: { code: "not_found" } }, 404);
  }
  const windowPath = path.match(new RegExp(`^${automationBase}/schedules/([^/?]+)/windows/(\\d+)$`));
  if (windowPath && method === "GET") {
    const found = schedules.find(item => item.schedule_id === decodeURIComponent(windowPath[1]!));
    return found ? json({ schedule_id: found.schedule_id, version: found.version, window_index: Number(windowPath[2]), status: "unknown", task: null })
      : json({ error: { code: "not_found" } }, 404);
  }
  const schedulePath = path.match(new RegExp(`^${automationBase}/schedules/([^/?]+)$`));
  if (schedulePath && method === "GET") {
    const found = schedules.find(item => item.schedule_id === decodeURIComponent(schedulePath[1]!));
    return found ? json(await scheduleDetail(found)) : json({ error: { code: "not_found" } }, 404);
  }
  if (path === `${evaluationBase}/targets`) return json([target]);
  if (path.startsWith(`${evaluationBase}/datasets?`)) return json([datasetRow]);
  if (path === `${evaluationBase}/datasets/${datasetRow.name}/${datasetRow.version}`) return json(continuousDataset());
  if (path.startsWith(`${evaluationBase}/metrics?`)) return json(metrics);
  if (path.endsWith("/tick") && method === "POST") return json({
    id: "task-current-window", request_key: "window-key", status: "queued", code: null,
    revision: 0, planned_count: 1, finished_count: 0,
  }, 202);
  if (path.endsWith("/disable") && method === "POST" && body) {
    const id = decodeURIComponent(path.split("/").at(-2) ?? "");
    const current = schedules.find(item => item.schedule_id === id);
    if (!current) return json({ error: { code: "not_found" } }, 404);
    expect(typeof body.key).toBe("string"); expect(typeof body.revision).toBe("number");
    const disabled: ScheduleView = { ...current, enabled: false, revision: current.revision + 1,
      control_receipt: { key: body.key as string, decision: "disabled", expected_revision: body.revision as number, revision: current.revision + 1 } };
    schedules = schedules.map(item => item.schedule_id === id ? disabled : item);
    return json(disabled);
  }
  return json({ error: { code: "not_found" } }, 404);
}

function render(props: Partial<EvaluationAutomationPanelProps> = {}) {
  return act(async () => root.render(<EvaluationAutomationPanel
    apiKey="synthetic-key" workspace="team" identity={identity} active
    canWrite canApprove isAdmin sourceRun="run-source" {...props}
  />));
}
async function until(check: () => void) {
  let caught: unknown;
  for (let index = 0; index < 80; index++) {
    try { check(); return; } catch (error) { caught = error; }
    await act(async () => { await new Promise(resolve => setTimeout(resolve, 0)); });
  }
  throw caught;
}
function button(name: string): HTMLButtonElement {
  const found = [...host.querySelectorAll("button")].find(item => item.textContent?.trim() === name);
  expect(found, `button ${name}`).toBeTruthy();
  return found!;
}
async function click(name: string) { await act(async () => button(name).click()); }
async function fill(id: string, value: string) {
  await act(async () => {
    const element = host.querySelector(`#${id}`) as HTMLInputElement | HTMLSelectElement;
    expect(element, id).toBeTruthy();
    const prototype = element.tagName === "SELECT" ? HTMLSelectElement.prototype : HTMLInputElement.prototype;
    Object.getOwnPropertyDescriptor(prototype, "value")!.set!.call(element, value);
    element.dispatchEvent(new Event(element.tagName === "SELECT" ? "change" : "input", { bubbles: true }));
  });
}

beforeEach(() => {
  calls = [];
  handler = undefined;
  schedules = [schedule()];
  localStorage.clear();
  vi.stubGlobal("crypto", webcrypto);
  host = document.createElement("div");
  document.body.append(host);
  root = createRoot(host);
  vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, options: RequestInit = {}) => {
    const path = String(input);
    const method = options.method ?? "GET";
    const body = options.body ? JSON.parse(String(options.body)) as Record<string, unknown> : null;
    calls.push({ path, method, body });
    return (await handler?.(path, method, body)) ?? defaultHandler(path, method, body);
  }));
});

afterEach(async () => {
  await act(async () => root.unmount());
  host.remove();
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

test("execute public spec accepts the real empty observations field but rejects nonempty observations and unknown fields", async () => {
  const value = await scheduleDetail(schedule());
  expect(parseScheduleView(value).spec?.template.observations).toEqual([]);
  const modified = structuredClone(value);
  const template = (modified.spec as Record<string, unknown>).template as Record<string, unknown>;
  template.observations = [{ observation_id: "unrequested" }];
  expect(() => parseScheduleView(modified)).toThrow("计划任务完整规范响应无效");
  template.observations = [];
  template.unregistered = true;
  expect(() => parseScheduleView(modified)).toThrow("计划任务完整规范响应无效");
});


test("authenticated local admin with an empty API key can persist intents and explicitly enable capture", async () => {
  handler = (path, method, body) => path === `${automationBase}/capture` && method === "POST"
    ? json({ ...defaultStatus, capture: { ...defaultStatus.capture, enabled: body?.enabled === true } }) : undefined;
  await render({ apiKey: "" });
  await until(() => expect(button("启用本地采集").disabled).toBe(false));
  expect(calls.every(item => item.method === "GET")).toBe(true);
  await click("启用本地采集");
  await until(() => expect(host.textContent).toContain("本地采集已启用"));
  const key = await evaluationAutomationStorageKey("", "team", identity);
  expect(readAutomationLedger(key).operations.some(item => item.kind === "capture" && item.state === "found")).toBe(true);
  expect(calls.filter(item => item.path === `${automationBase}/capture` && item.method === "POST")).toHaveLength(1);
});


test("viewer reads state and schedules but cannot submit mutations", async () => {
  await render({ canWrite: false, canApprove: false, isAdmin: false });
  await until(() => expect(host.textContent).toContain("nightly-ci · v1"));
  expect(host.textContent).toContain("当前身份为只读");
  expect(button("提交明确反馈").disabled).toBe(true);
  expect(button("创建回归计划").disabled).toBe(true);
  expect(button("读取当前窗口任务").disabled).toBe(true);
  expect(calls.filter(item => item.method === "POST")).toEqual([]);
});

test("server schedule catalogue is restored and details are read with GET", async () => {
  await render();
  await until(() => expect(host.textContent).toContain("nightly-ci · v1"));
  expect(host.textContent).toContain("尚未观察窗口");
  await click("读取详情");
  await until(() => expect(host.textContent).toContain("计划详情：nightly-ci"));
  expect(calls.some(item => item.path === `${automationBase}/schedules/nightly-ci` && item.method === "GET")).toBe(true);
  expect(calls.filter(item => item.method === "POST")).toEqual([]);
});

test("creating a plan uses only a server-confirmed dataset and persists its original request first", async () => {
  let submitted: Record<string, unknown> | null = null;
  let ledgerWasPersistedBeforePost = false;
  handler = async (path, method, body) => {
    if (path === `${evaluationBase}/datasets/${datasetRow.name}/${datasetRow.version}`) return json(continuousDataset());
    if (path === `${automationBase}/schedules` && method === "POST" && body) {
      submitted = body;
      const storageKey = await evaluationAutomationStorageKey("synthetic-key", "team", identity);
      const intent = readAutomationLedger(storageKey).operations.find(item => item.kind === "schedule-create");
      ledgerWasPersistedBeforePost = !!intent && JSON.stringify(intent.body) === JSON.stringify(body);
      const view = { ...schedule(String(body.schedule_id)), version: String(body.version), dataset: { ...datasetRow }, target_key: target.key,
        epoch: body.epoch as number, interval_seconds: body.interval_seconds as number, max_windows: body.max_windows as number, control_receipt: null };
      schedules = [view];
      const spec = publicScheduleSpec(body);
      return json({ ...view, spec, spec_fingerprint: await fingerprint(spec) }, 201);
    }
    return undefined;
  };
  await render();
  await until(() => expect(host.querySelector("#automation-target option[value='target-script']")).toBeTruthy());
  await fill("automation-dataset", "script-suite/v1");
  await until(() => expect(host.textContent).toContain("服务端已返回连续冻结记录"));
  await until(() => expect(button("创建回归计划").disabled).toBe(false));
  await click("创建回归计划");
  await until(() => expect(host.textContent).toContain("已按完整回执确认回归计划"));
  expect(ledgerWasPersistedBeforePost).toBe(true);
  expect(submitted).toMatchObject({
    scope: "local", schedule_id: expect.stringMatching(/^regression-/), version: "v1", interval_seconds: 3600, max_windows: 1,
    template: {
      schema_version: "continuous-task/1", scope: "local", mode: "execute", target_key: "target-script",
      dataset: { name: "script-suite", version: "v1", fingerprint: datasetRow.fingerprint },
      dataset_envelope_fp: "b".repeat(64),
    },
  });
  const template = submitted!.template as Record<string, unknown>;
  expect((template.metrics as Array<{ name: string }>).map(metric => metric.name)).toEqual(["context_coverage", "fact_integrity", "krim_result"]);
  expect((template.request_key as string)).toMatch(/^schedule-task-/);
  expect(calls.filter(item => item.path === `${automationBase}/schedules` && item.method === "POST")).toHaveLength(1);
});

test("uncertain schedule creation is found only by matching its complete saved specification", async () => {
  let submitted: Record<string, unknown> | null = null;
  let detailReads = 0;
  handler = async (path, method, body) => {
    if (path === `${evaluationBase}/datasets/${datasetRow.name}/${datasetRow.version}`) return json(continuousDataset());
    if (path === `${automationBase}/schedules` && method === "POST" && body) {
      submitted = body;
      throw new TypeError("creation response lost after admission");
    }
    if (path === `${automationBase}/schedules/${String(submitted?.schedule_id)}` && method === "GET" && submitted) {
      detailReads += 1;
      const spec = publicScheduleSpec(submitted);
      if (detailReads === 1) (spec.template as Record<string, unknown>).request_key = "different-original-task-key";
      const view = { ...schedule(String(submitted.schedule_id)), version: String(submitted.version),
        dataset: { ...datasetRow }, target_key: target.key, epoch: submitted.epoch as number,
        interval_seconds: submitted.interval_seconds as number, max_windows: submitted.max_windows as number, control_receipt: null };
      return json({ ...view, spec, spec_fingerprint: await fingerprint(spec) });
    }
    return undefined;
  };
  await render();
  await until(() => expect(host.querySelector("#automation-target option[value='target-script']")).toBeTruthy());
  await fill("automation-dataset", "script-suite/v1");
  await until(() => expect(button("创建回归计划").disabled).toBe(false));
  await click("创建回归计划");
  await until(() => expect(host.textContent).toContain("提交结果未确认"));
  const storageKey = await evaluationAutomationStorageKey("synthetic-key", "team", identity);
  const intent = readAutomationLedger(storageKey).operations.find(item => item.kind === "schedule-create");
  expect(intent?.state).toBe("unknown");
  await click("只读核实");
  await until(() => expect(host.textContent).toContain("完整内容或指纹与原请求不完全相符"));
  expect(readAutomationLedger(storageKey).operations.find(item => item.kind === "schedule-create")?.state).toBe("unknown");
  await click("只读核实");
  await until(() => expect(host.textContent).toContain("找回计划 regression-"));
  expect(readAutomationLedger(storageKey).operations.find(item => item.kind === "schedule-create")).toMatchObject({ state: "found" });
  expect(calls.filter(item => item.path === `${automationBase}/schedules` && item.method === "POST")).toHaveLength(1);
});

test("a plain dataset without server-issued continuous envelope cannot create a plan", async () => {
  handler = (path) => path === `${evaluationBase}/datasets/${datasetRow.name}/${datasetRow.version}` ? json(continuousDataset(false)) : undefined;
  await render();
  await until(() => expect(host.querySelector("#automation-target option[value='target-script']")).toBeTruthy());
  await fill("automation-dataset", "script-suite/v1");
  await until(() => expect(host.textContent).toContain("没有服务端返回的连续评测冻结记录"));
  expect(button("创建回归计划").disabled).toBe(true);
  expect(calls.filter(item => item.path === `${automationBase}/schedules` && item.method === "POST")).toEqual([]);
});

test("uncertain feedback is recovered only by its exact original run, key, and reason", async () => {
  let postedKey = "";
  let receiptReads = 0;
  handler = (path, method, body) => {
    if (path === `${automationBase}/feedback/run-source` && method === "POST") {
      postedKey = String(body?.key ?? "");
      throw new TypeError("response lost after admission");
    }
    if (path === `${automationBase}/feedback/run-source/by-key/${postedKey}` && method === "GET") {
      receiptReads += 1;
      return json({ key: postedKey, revision: 1, reason: receiptReads === 1 ? "unsafe" : "other", candidate_ids: [], delivery_status: "pending" });
    }
    return undefined;
  };
  await render();
  await until(() => expect(button("提交明确反馈").disabled).toBe(false));
  await click("提交明确反馈");
  await until(() => expect(host.textContent).toContain("提交结果未确认"));
  const storageKey = await evaluationAutomationStorageKey("synthetic-key", "team", identity);
  const intent = readAutomationLedger(storageKey).operations.find(item => item.kind === "feedback");
  expect(intent?.state).toBe("unknown");
  expect(intent?.request_key).toMatch(/^feedback-/);
  expect(intent?.body).toMatchObject({ key: intent?.request_key, reason: "other" });
  await click("提交明确反馈");
  await until(() => expect(host.textContent).toContain("此运行的反馈操作尚未核实"));
  expect(calls.filter(item => item.path === `${automationBase}/feedback/run-source` && item.method === "POST")).toHaveLength(1);

  await act(async () => root.render(null));
  calls = [];
  await render();
  await until(() => expect(host.textContent).toContain("来源运行反馈"));
  expect(host.textContent).toContain("来源运行反馈 · 结果未确认");
  expect(host.textContent).toContain(intent!.request_key!);
  await click("只读刷新");
  await until(() => expect(calls.some(item => item.path === `${automationBase}/status` && item.method === "GET")).toBe(true));
  expect(calls.filter(item => item.method === "POST")).toEqual([]);
  await click("只读核实原反馈");
  await until(() => expect(host.textContent).toContain("服务端反馈回执与原运行、原键、原原因或修订不一致"));
  expect(readAutomationLedger(storageKey).operations.find(item => item.kind === "feedback")?.state).toBe("unknown");
  await click("只读核实原反馈");
  await until(() => expect(host.textContent).toContain("找回反馈修订 1"));
  expect(readAutomationLedger(storageKey).operations.find(item => item.kind === "feedback")).toMatchObject({ state: "found", result_id: "revision-1" });
  expect(calls.filter(item => item.path === `${automationBase}/feedback/run-source/by-key/${postedKey}` && item.method === "GET")).toHaveLength(2);
  expect(calls.filter(item => item.path === `${automationBase}/feedback/run-source` && item.method === "POST")).toEqual([]);
});

test("linked feedback candidates stay read-only until the exact candidate is explicitly opened", async () => {
  let feedbackKey = "";
  let receiptReads = 0;
  const onCandidate = vi.fn();
  handler = (path, method, body) => {
    if (path === `${automationBase}/feedback/run-h1` && method === "POST" && body) {
      feedbackKey = String(body.key);
      return json({ key: feedbackKey, revision: 4 }, 201);
    }
    if (path === `${automationBase}/feedback/run-h1/by-key/${feedbackKey}` && method === "GET") {
      receiptReads += 1;
      return json({ key: feedbackKey, revision: 4, reason: "unsafe",
        candidate_ids: receiptReads === 1 ? [] : ["candidate-h1-04"],
        delivery_status: receiptReads === 1 ? "pending" : "linked" });
    }
    return undefined;
  };
  await render({ sourceRun: "run-h1", onCandidate });
  await until(() => expect(button("提交明确反馈").disabled).toBe(false));
  await fill("automation-feedback-reason", "unsafe");
  await click("提交明确反馈");
  await until(() => expect(host.textContent).toContain("已提交明确反馈"));
  expect(onCandidate).not.toHaveBeenCalled();
  await click("读取反馈候选");
  await until(() => expect(host.textContent).toContain("等待后台消费"));
  await click("读取反馈候选");
  await until(() => expect(host.textContent).toContain("candidate-h1-04"));
  expect(onCandidate).not.toHaveBeenCalled();
  expect(calls.filter(item => item.path === `${automationBase}/feedback/run-h1/by-key/${feedbackKey}` && item.method === "GET")).toHaveLength(2);
  expect(calls.filter(item => item.path === `${automationBase}/feedback/run-h1` && item.method === "POST")).toHaveLength(1);
  await click("进入人工确认");
  expect(onCandidate).toHaveBeenCalledTimes(1);
  expect(onCandidate).toHaveBeenCalledWith("run-h1", "candidate-h1-04");
});

test("late POST response after identity switch cannot populate the new identity", async () => {
  let release!: (response: Response) => void;
  const waiting = new Promise<Response>(resolve => { release = resolve; });
  handler = (path, method) => path === `${automationBase}/feedback/run-old` && method === "POST" ? waiting : undefined;
  await render({ sourceRun: "run-old" });
  await until(() => expect(button("提交明确反馈").disabled).toBe(false));
  await click("提交明确反馈");
  await until(() => expect(calls.some(item => item.path === `${automationBase}/feedback/run-old` && item.method === "POST")).toBe(true));
  const original = calls.find(item => item.path === `${automationBase}/feedback/run-old` && item.method === "POST")!.body!;
  await render({ identity: { principal_id: "bob", generation: 2 }, sourceRun: "run-new" });
  await until(() => expect((host.querySelector("#automation-feedback-run") as HTMLInputElement).value).toBe("run-new"));
  release(json({ key: original.key, revision: 1 }));
  await act(async () => { await waiting; await new Promise(resolve => setTimeout(resolve, 0)); });
  expect(host.textContent).not.toContain("run-old");
  expect(host.textContent).not.toContain("已提交明确反馈");
  const bobKey = await evaluationAutomationStorageKey("synthetic-key", "team", { principal_id: "bob", generation: 2 });
  expect(readAutomationLedger(bobKey).operations).toEqual([]);
  const aliceKey = await evaluationAutomationStorageKey("synthetic-key", "team", identity);
  expect(readAutomationLedger(aliceKey).operations[0]).toMatchObject({ object_id: "run-old", state: "pending" });
});

test("tick records the server window index before POST and sends that index as a guard", async () => {
  const onTask = vi.fn();
  let persistedWindowIndex: unknown;
  handler = async (path, method, body) => {
    if (path === `${automationBase}/schedules/nightly-ci/tick` && method === "POST") {
      const storageKey = await evaluationAutomationStorageKey("synthetic-key", "team", identity);
      const intent = readAutomationLedger(storageKey).operations.find(item => item.kind === "schedule-tick");
      persistedWindowIndex = intent?.body?.window_index;
      expect(intent?.body?.expected_window_index).toBe(0);
      expect(body).toEqual({ expected_window_index: 0 });
      return json({ id: "task-current-window", request_key: "window-key", status: "queued", code: null, revision: 0, planned_count: 1, finished_count: 0 }, 202);
    }
    return undefined;
  };
  await render({ onTask });
  await until(() => expect(button("读取当前窗口任务").disabled).toBe(false));
  await click("读取当前窗口任务");
  await until(() => expect(onTask).toHaveBeenCalledWith("task-current-window"));
  expect(persistedWindowIndex).toBe(0);
  expect(calls.some(item => item.path === `${automationBase}/schedules/nightly-ci/windows/current` && item.method === "GET")).toBe(true);
  const currentReadIndex = calls.findIndex(item => item.path === `${automationBase}/schedules/nightly-ci/windows/current` && item.method === "GET");
  const tickPostIndex = calls.findIndex(item => item.path === `${automationBase}/schedules/nightly-ci/tick` && item.method === "POST");
  expect(currentReadIndex).toBeGreaterThanOrEqual(0);
  expect(tickPostIndex).toBeGreaterThan(currentReadIndex);
  expect(calls.filter(item => item.path === `${automationBase}/schedules/nightly-ci/tick` && item.method === "POST")).toHaveLength(1);
});

test("an uncertain tick reads only the persisted original window and recovers its task", async () => {
  const onTask = vi.fn();
  handler = (path, method) => {
    if (path === `${automationBase}/schedules/nightly-ci/tick` && method === "POST") throw new TypeError("response lost after reservation");
    if (path === `${automationBase}/schedules/nightly-ci/windows/0` && method === "GET") return json({
      schedule_id: "nightly-ci", version: "v1", window_index: 0, status: "reserved",
      task: { id: "task-window-zero", request_key: "window-zero-key", status: "queued", code: null, revision: 0, planned_count: 1, finished_count: 0 },
    });
    return undefined;
  };
  await render({ onTask });
  await until(() => expect(button("读取当前窗口任务").disabled).toBe(false));
  await click("读取当前窗口任务");
  await until(() => expect(host.textContent).toContain("提交结果未确认"));
  const storageKey = await evaluationAutomationStorageKey("synthetic-key", "team", identity);
  const intent = readAutomationLedger(storageKey).operations.find(item => item.kind === "schedule-tick");
  expect(intent).toMatchObject({ state: "unknown", body: { schedule_id: "nightly-ci", version: "v1", window_index: 0, expected_window_index: 0 } });
  await click("只读核实原窗口");
  await until(() => expect(onTask).toHaveBeenCalledWith("task-window-zero"));
  expect(calls.filter(item => item.path === `${automationBase}/schedules/nightly-ci/windows/0` && item.method === "GET")).toHaveLength(1);
  expect(calls.filter(item => item.path === `${automationBase}/schedules/nightly-ci/windows/current` && item.method === "GET")).toHaveLength(1);
  expect(calls.filter(item => item.path === `${automationBase}/schedules/nightly-ci/tick` && item.method === "POST")).toHaveLength(1);
  expect(readAutomationLedger(storageKey).operations.find(item => item.kind === "schedule-tick")).toMatchObject({ state: "found", result_id: "task-window-zero" });
});

test("legacy uncertain tick without an original index never reads the current window", async () => {
  const storageKey = await evaluationAutomationStorageKey("synthetic-key", "team", identity);
  localStorage.setItem(storageKey, JSON.stringify({ version: 1, operations: [{
    id: "legacy-tick", kind: "schedule-tick", state: "unknown", created_at: 1, object_id: "nightly-ci",
    body: { schedule_id: "nightly-ci", version: "v1" }, request_key: "legacy-tick-key",
  }] }));
  await render();
  await until(() => expect(host.textContent).toContain("当前窗口任务 · 结果未确认"));
  await click("只读核实原窗口");
  await until(() => expect(host.textContent).toContain("旧操作记录没有原窗口编号"));
  expect(calls.some(item => item.path.endsWith("/windows/current") || /\/windows\/\d+$/.test(item.path))).toBe(false);
  expect(calls.filter(item => item.method === "POST")).toEqual([]);
});

test("a window-index conflict never dispatches the task", async () => {
  const onTask = vi.fn();
  handler = (path, method, body) => {
    if (path === `${automationBase}/schedules/nightly-ci/windows/current` && method === "GET") {
      return json({ schedule_id: "nightly-ci", version: "v1", window_index: 1, enabled: true, eligible: true });
    }
    if (path === `${automationBase}/schedules/nightly-ci/tick` && method === "POST") {
      expect(body).toEqual({ expected_window_index: 1 });
      return json({ error: { code: "snapshot_changed" } }, 409);
    }
    return undefined;
  };
  await render({ onTask });
  await until(() => expect(button("读取当前窗口任务").disabled).toBe(false));
  await click("读取当前窗口任务");
  await until(() => expect(host.querySelector('[role="alert"]')?.textContent).toContain("服务端当前版本冲突"));
  const storageKey = await evaluationAutomationStorageKey("synthetic-key", "team", identity);
  expect(readAutomationLedger(storageKey).operations.find(item => item.kind === "schedule-tick")).toMatchObject({
    state: "rejected", body: { window_index: 1, expected_window_index: 1 },
  });
  expect(onTask).not.toHaveBeenCalled();
  expect(calls.filter(item => item.path === `${automationBase}/schedules/nightly-ci/tick` && item.method === "POST")).toHaveLength(1);
  expect(calls.some(item => item.path === `${automationBase}/schedules/nightly-ci/windows/1` && item.method === "GET")).toBe(false);
});

test("late tick response after workspace switch cannot publish into the new workspace", async () => {
  let release!: (response: Response) => void;
  const waiting = new Promise<Response>(resolve => { release = resolve; });
  const onTask = vi.fn();
  handler = (path, method) => path === `${automationBase}/schedules/nightly-ci/tick` && method === "POST" ? waiting : undefined;
  await render({ onTask });
  await until(() => expect(button("读取当前窗口任务").disabled).toBe(false));
  await click("读取当前窗口任务");
  await until(() => expect(calls.some(item => item.path === `${automationBase}/schedules/nightly-ci/tick` && item.method === "POST")).toBe(true));
  const originalKey = await evaluationAutomationStorageKey("synthetic-key", "team", identity);
  await render({ workspace: "other", onTask });
  release(json({ id: "late-other-workspace-task", request_key: "late-key", status: "queued" }, 202));
  await act(async () => { await waiting; await new Promise(resolve => setTimeout(resolve, 0)); });
  expect(onTask).not.toHaveBeenCalled();
  expect(host.textContent).not.toContain("late-other-workspace-task");
  expect(readAutomationLedger(originalKey).operations.find(item => item.kind === "schedule-tick")).toMatchObject({ state: "pending", body: { window_index: 0 } });
  const otherKey = await evaluationAutomationStorageKey("synthetic-key", "other", identity);
  expect(readAutomationLedger(otherKey).operations).toEqual([]);
});

test("capture toggle is hidden from non-admins and handler remains read-only", async () => {
  await render({ isAdmin: false });
  await until(() => expect(host.textContent).toContain("采集启停仅向管理员提供"));
  expect([...host.querySelectorAll("button")].some(item => item.textContent?.trim() === "启用本地采集")).toBe(false);
  expect([...host.querySelectorAll("button")].some(item => item.textContent?.trim() === "停用本地采集")).toBe(false);
  expect(calls.filter(item => item.path === `${automationBase}/capture` && item.method === "POST")).toEqual([]);
});

test("admin capture enable is explicit and the operation is persisted before POST", async () => {
  let persistedBeforeCapture = false;
  handler = async (path, method, body) => {
    if (path === `${automationBase}/capture` && method === "POST" && body) {
      const storageKey = await evaluationAutomationStorageKey("synthetic-key", "team", identity);
      persistedBeforeCapture = readAutomationLedger(storageKey).operations.some(item =>
        item.kind === "capture" && item.body?.enabled === true && item.state === "pending");
      return json({ ...defaultStatus, capture: { ...defaultStatus.capture, enabled: true } });
    }
    return undefined;
  };
  await render({ isAdmin: true });
  await until(() => expect(button("启用本地采集").disabled).toBe(false));
  expect(calls.filter(item => item.path === `${automationBase}/capture` && item.method === "POST")).toEqual([]);
  await click("启用本地采集");
  await until(() => expect(host.textContent).toContain("本地采集已启用"));
  expect(persistedBeforeCapture).toBe(true);
  expect(calls.filter(item => item.path === `${automationBase}/capture` && item.method === "POST")).toHaveLength(1);
});

test("disable persists the complete plan snapshot and verifies the original control receipt", async () => {
  let savedBody: Record<string, unknown> | null = null;
  handler = async (path, method, body) => {
    if (path === `${automationBase}/schedules/nightly-ci/disable` && method === "POST" && body) {
      const storageKey = await evaluationAutomationStorageKey("synthetic-key", "team", identity);
      const intent = readAutomationLedger(storageKey).operations.find(item => item.kind === "schedule-disable");
      savedBody = intent?.body ?? null;
      expect(body).toMatchObject({ revision: 0, key: expect.stringMatching(/^disable-/) });
      expect(body).not.toHaveProperty("spec");
    }
    return undefined;
  };
  await render();
  await until(() => expect(button("禁用计划").disabled).toBe(false));
  await click("禁用计划");
  await until(() => expect(host.textContent).toContain("已按原禁用键和控制回执停用回归计划 nightly-ci"));
  expect(savedBody).toMatchObject({
    schedule_id: "nightly-ci", version: "v1", revision: 0, key: expect.stringMatching(/^disable-/),
    spec_fingerprint: expect.stringMatching(/^[0-9a-f]{64}$/),
    spec: { schedule_id: "nightly-ci", version: "v1", template: { request_key: "schedule-task-fixture" } },
  });
  expect(calls.some(item => item.path === `${automationBase}/schedules/nightly-ci` && item.method === "GET")).toBe(true);
  expect(calls.filter(item => item.path === `${automationBase}/schedules/nightly-ci/disable` && item.method === "POST")).toHaveLength(1);
});

test("uncertain disable recovery requires the original version, full snapshot, and control key", async () => {
  let controlKey = "";
  let recoveryReads = 0;
  handler = async (path, method, body) => {
    if (path === `${automationBase}/schedules/nightly-ci/disable` && method === "POST" && body) {
      controlKey = String(body.key);
      const current = schedules[0]!;
      schedules = [{ ...current, enabled: false, revision: 1,
        control_receipt: { key: controlKey, decision: "disabled", expected_revision: 0, revision: 1 } }];
      throw new TypeError("disable response lost after admission");
    }
    if (path === `${automationBase}/schedules/nightly-ci` && method === "GET" && !schedules[0]!.enabled) {
      recoveryReads += 1;
      const detail = await scheduleDetail(schedules[0]!);
      const receipt = detail.control_receipt as Record<string, unknown>;
      if (recoveryReads === 1) detail.control_receipt = { ...receipt, key: "different-control-key" };
      return json(detail);
    }
    return undefined;
  };
  await render();
  await until(() => expect(button("禁用计划").disabled).toBe(false));
  await click("禁用计划");
  await until(() => expect(host.textContent).toContain("提交结果未确认"));
  const storageKey = await evaluationAutomationStorageKey("synthetic-key", "team", identity);
  const original = readAutomationLedger(storageKey).operations.find(item => item.kind === "schedule-disable");
  expect(original).toMatchObject({ state: "unknown", body: { version: "v1", revision: 0, key: controlKey } });
  await click("只读核实");
  await until(() => expect(host.textContent).toContain("原禁用键、版本、完整内容或控制回执未全部匹配"));
  expect(readAutomationLedger(storageKey).operations.find(item => item.kind === "schedule-disable")?.state).toBe("unknown");
  await click("只读核实");
  await until(() => expect(host.textContent).toContain("按原计划版本、完整内容和禁用控制键找回计划"));
  expect(readAutomationLedger(storageKey).operations.find(item => item.kind === "schedule-disable")).toMatchObject({ state: "found" });
  expect(calls.filter(item => item.path === `${automationBase}/schedules/nightly-ci/disable` && item.method === "POST")).toHaveLength(1);
});

test("corrupt local operation storage blocks every write but keeps GET reads available", async () => {
  const storageKey = await evaluationAutomationStorageKey("synthetic-key", "team", identity);
  localStorage.setItem(storageKey, "{broken");
  await render();
  await until(() => expect(host.querySelector('[role="alert"]')?.textContent).toContain("本地自动化操作记录损坏"));
  expect(button("提交明确反馈").disabled).toBe(true);
  expect(button("启用本地采集").disabled).toBe(true);
  expect(button("创建回归计划").disabled).toBe(true);
  expect(calls.some(item => item.path === `${automationBase}/status` && item.method === "GET")).toBe(true);
  expect(calls.filter(item => item.method === "POST")).toEqual([]);
});
