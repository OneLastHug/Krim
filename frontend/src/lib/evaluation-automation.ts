import { RequestError, object, request } from "../api";

export interface AutomationIdentity {
  principal_id: string;
  generation: number;
}

export interface AutomationStatus {
  automation: { running: boolean; last_error: string | null };
  capture: {
    configured: boolean;
    enabled: boolean;
    observer_ready: boolean;
    pending: number;
    claimed: number;
    acknowledged: number;
    counters: { seen: number; enqueued: number; dropped: number; acked: number };
    text_capture: boolean;
    argument_capture: boolean;
    artifact_count: number;
  };
  schedules: { total: number; enabled: number };
}

export interface EvaluationTarget {
  key: string;
  version: string;
  kind: string;
}

export interface MetricSpec {
  name: string;
  version: string;
  implementation_digest: string;
  config: Record<string, unknown>;
  required_evidence: string[];
  hard: boolean;
}

export interface DatasetRow {
  name: string;
  version: string;
  fingerprint: string;
  case_count: number;
}

export interface ContinuousDataset {
  dataset: { name: string; version: string; fingerprint: string };
  dataset_envelope_fp: string;
  case_count: number;
  verifiers: string[];
  requires_process_metric: boolean;
}

export interface ScheduleControlReceipt {
  key: string;
  decision: "disabled";
  expected_revision: number;
  revision: number;
}

export interface ContinuousRequest {
  schema_version: "continuous-task/1";
  scope: "local";
  request_key: string;
  mode: "execute";
  dataset: { name: string; version: string; fingerprint: string };
  dataset_envelope_fp: string;
  target_key: string;
  metrics: MetricSpec[];
  execution: {
    repetitions: number;
    concurrency: 1 | 2;
    item_timeout_seconds: number;
    scorer_timeout_seconds: number;
    batch_timeout_seconds: number;
  };
  parent_task_id: null;
  parent_report_hash: null;
  observations: [];
  seed_policy: "unseeded/1";
  schedule: null;
}

export interface ScheduleSpec {
  scope: "local";
  schedule_id: string;
  version: string;
  template: ContinuousRequest;
  epoch: number;
  interval_seconds: number;
  max_windows: number;
}

export type PublicScheduleSpec = Omit<ScheduleSpec, "scope" | "template"> & {
  template: Omit<ContinuousRequest, "scope">;
};

export interface ScheduleView {
  schedule_id: string;
  version: string;
  enabled: boolean;
  revision: number;
  observed_through: number;
  missed_windows: number;
  dataset: { name: string; version: string; fingerprint: string };
  target_key: string;
  epoch: number;
  interval_seconds: number;
  max_windows: number;
  spec?: PublicScheduleSpec;
  spec_fingerprint?: string;
  control_receipt?: ScheduleControlReceipt | null;
}

export interface FeedbackReceipt {
  key: string;
  revision: number;
  reason: FeedbackReason;
  candidate_ids: string[];
  delivery_status: "pending" | "linked" | "not_enqueued";
}

export interface CurrentScheduleWindow {
  schedule_id: string;
  version: string;
  window_index: number;
  enabled: boolean;
  eligible: boolean;
}

export interface ScheduleWindowReceipt {
  schedule_id: string;
  version: string;
  window_index: number;
  status: "unknown" | "missed" | "reserved";
  task: { id: string; request_key: string; status: string } | null;
}

export type OperationState = "pending" | "unknown" | "found" | "rejected";
export interface AutomationOperationIntent {
  id: string;
  kind: string;
  state: OperationState;
  created_at: number;
  object_id: string;
  body?: Record<string, unknown>;
  request_key?: string;
  result_id?: string;
}

export interface AutomationLedger {
  version: 1;
  operations: AutomationOperationIntent[];
}

export class AutomationLedgerError extends Error {
  readonly code: string;
  constructor(code: string, message: string) {
    super(message);
    this.name = "AutomationLedgerError";
    this.code = code;
  }
}

export function evaluationAutomationBase(workspace: string): string {
  return `/v4/workspaces/${encodeURIComponent(workspace)}/eval/automation`;
}

function evaluationBase(workspace: string): string {
  return `/v4/workspaces/${encodeURIComponent(workspace)}/eval`;
}

export const evaluationAutomationApi = {
  status: (workspace: string, key: string, signal?: AbortSignal) =>
    request(`${evaluationAutomationBase(workspace)}/status`, key, undefined, signal),
  schedules: (workspace: string, key: string, offset = 0, limit = 20, signal?: AbortSignal) =>
    request(`${evaluationAutomationBase(workspace)}/schedules?offset=${offset}&limit=${limit}`, key, undefined, signal),
  schedule: (workspace: string, key: string, scheduleId: string, signal?: AbortSignal) =>
    request(`${evaluationAutomationBase(workspace)}/schedules/${encodeURIComponent(scheduleId)}`, key, undefined, signal),
  setCapture: (workspace: string, key: string, enabled: boolean) =>
    request(`${evaluationAutomationBase(workspace)}/capture`, key, { enabled }),
  feedback: (workspace: string, key: string, runId: string, body: { key: string; reason: FeedbackReason }) =>
    request(`${evaluationAutomationBase(workspace)}/feedback/${encodeURIComponent(runId)}`, key, body),
  feedbackByKey: (workspace: string, key: string, runId: string, feedbackKey: string, signal?: AbortSignal) =>
    request(`${evaluationAutomationBase(workspace)}/feedback/${encodeURIComponent(runId)}/by-key/${encodeURIComponent(feedbackKey)}`, key, undefined, signal),
  createSchedule: (workspace: string, key: string, body: ScheduleSpec) =>
    request(`${evaluationAutomationBase(workspace)}/schedules`, key, body),
  currentScheduleWindow: (workspace: string, key: string, scheduleId: string, signal?: AbortSignal) =>
    request(`${evaluationAutomationBase(workspace)}/schedules/${encodeURIComponent(scheduleId)}/windows/current`, key, undefined, signal),
  scheduleWindow: (workspace: string, key: string, scheduleId: string, windowIndex: number, signal?: AbortSignal) =>
    request(`${evaluationAutomationBase(workspace)}/schedules/${encodeURIComponent(scheduleId)}/windows/${windowIndex}`, key, undefined, signal),
  tickSchedule: (workspace: string, key: string, scheduleId: string, expectedWindowIndex: number) =>
    request(`${evaluationAutomationBase(workspace)}/schedules/${encodeURIComponent(scheduleId)}/tick`, key,
      { expected_window_index: expectedWindowIndex }),
  disableSchedule: (workspace: string, key: string, scheduleId: string, body: { revision: number; key: string }) =>
    request(`${evaluationAutomationBase(workspace)}/schedules/${encodeURIComponent(scheduleId)}/disable`, key, body),
};

export const evaluationCatalogApi = {
  targets: (workspace: string, key: string, signal?: AbortSignal) =>
    request(`${evaluationBase(workspace)}/targets`, key, undefined, signal),
  datasets: (workspace: string, key: string, offset = 0, limit = 20, signal?: AbortSignal) =>
    request(`${evaluationBase(workspace)}/datasets?offset=${offset}&limit=${limit}`, key, undefined, signal),
  dataset: (workspace: string, key: string, name: string, version: string, signal?: AbortSignal) =>
    request(`${evaluationBase(workspace)}/datasets/${encodeURIComponent(name)}/${encodeURIComponent(version)}`, key, undefined, signal),
  metrics: (workspace: string, key: string, targetKey: string, includeProcess: boolean, signal?: AbortSignal) => {
    const query = new URLSearchParams({ target_key: targetKey, mode: "continuous", include_process: String(includeProcess) });
    return request(`${evaluationBase(workspace)}/metrics?${query}`, key, undefined, signal);
  },
};

export type FeedbackReason = "incorrect" | "unsafe" | "missing_context" | "other";

function isFeedbackReason(value: unknown): value is FeedbackReason {
  return value === "incorrect" || value === "unsafe" || value === "missing_context" || value === "other";
}

function hasExactKeys(value: Record<string, unknown>, expected: string[]): boolean {
  const actual = Object.keys(value).sort();
  const keys = [...expected].sort();
  return actual.length === keys.length && actual.every((key, index) => key === keys[index]);
}

export function parseFeedbackReceipt(value: unknown): FeedbackReceipt {
  const row = object(value);
  if (!isIdentity(row.key) || !nonNegativeInteger(row.revision) || !isFeedbackReason(row.reason)
    || !Array.isArray(row.candidate_ids) || !row.candidate_ids.every(item => typeof item === "string" && !!item)
    || !["pending", "linked", "not_enqueued"].includes(String(row.delivery_status))) {
    throw new Error("反馈回执响应无效。");
  }
  const candidateIds = row.candidate_ids as string[];
  if (new Set(candidateIds).size !== candidateIds.length
    || (row.delivery_status === "linked") !== (candidateIds.length > 0)) {
    throw new Error("反馈候选关联状态与候选列表不一致。");
  }
  return {
    key: row.key,
    revision: row.revision,
    reason: row.reason,
    candidate_ids: candidateIds,
    delivery_status: row.delivery_status as FeedbackReceipt["delivery_status"],
  };
}

export function parseCurrentScheduleWindow(value: unknown): CurrentScheduleWindow {
  const row = object(value);
  if (!isIdentity(row.schedule_id) || !isIdentity(row.version) || !nonNegativeInteger(row.window_index)
    || typeof row.enabled !== "boolean" || typeof row.eligible !== "boolean") {
    throw new Error("当前窗口状态响应无效。");
  }
  return {
    schedule_id: row.schedule_id,
    version: row.version,
    window_index: row.window_index,
    enabled: row.enabled,
    eligible: row.eligible,
  };
}

export function parseScheduleWindowReceipt(value: unknown): ScheduleWindowReceipt {
  const row = object(value);
  if (!isIdentity(row.schedule_id) || !isIdentity(row.version) || !nonNegativeInteger(row.window_index)
    || !["unknown", "missed", "reserved"].includes(String(row.status))) {
    throw new Error("原窗口回执响应无效。");
  }
  const task = row.task === null ? null : parseTaskReceipt(row.task);
  if ((row.status === "reserved") !== (task !== null)) throw new Error("原窗口状态与任务回执不一致。");
  return {
    schedule_id: row.schedule_id,
    version: row.version,
    window_index: row.window_index,
    status: row.status as ScheduleWindowReceipt["status"],
    task,
  };
}

function parsePublicScheduleSpec(value: unknown, scheduleId: string, version: string): PublicScheduleSpec {
  const row = object(value);
  if (!hasExactKeys(row, ["schedule_id", "version", "template", "epoch", "interval_seconds", "max_windows"])
    || row.schedule_id !== scheduleId || row.version !== version || !nonNegativeInteger(row.epoch)
    || !Number.isInteger(row.interval_seconds) || typeof row.interval_seconds !== "number"
    || row.interval_seconds < 60 || row.interval_seconds > 86400
    || !Number.isInteger(row.max_windows) || typeof row.max_windows !== "number" || row.max_windows < 1 || row.max_windows > 3) {
    throw new Error("计划完整规范响应无效。");
  }
  const template = object(row.template);
  if (!hasExactKeys(template, ["schema_version", "request_key", "mode", "dataset", "dataset_envelope_fp", "target_key", "metrics",
    "execution", "parent_task_id", "parent_report_hash", "observations", "seed_policy", "schedule"])
    || template.schema_version !== "continuous-task/1" || typeof template.request_key !== "string" || !template.request_key
    || template.mode !== "execute" || !isDigest(template.dataset_envelope_fp) || !isIdentity(template.target_key)
    || template.parent_task_id !== null || template.parent_report_hash !== null || template.seed_policy !== "unseeded/1"
    || !Array.isArray(template.observations) || template.observations.length !== 0
    || template.schedule !== null) {
    throw new Error("计划任务完整规范响应无效。");
  }
  const dataset = object(template.dataset);
  if (!hasExactKeys(dataset, ["name", "version", "fingerprint"])
    || !isIdentity(dataset.name) || !isIdentity(dataset.version) || !isDigest(dataset.fingerprint)) {
    throw new Error("计划数据集完整规范响应无效。");
  }
  const execution = object(template.execution);
  if (!hasExactKeys(execution, ["repetitions", "concurrency", "item_timeout_seconds", "scorer_timeout_seconds", "batch_timeout_seconds"])
    || !Number.isInteger(execution.repetitions) || typeof execution.repetitions !== "number" || execution.repetitions < 1 || execution.repetitions > 3
    || (execution.concurrency !== 1 && execution.concurrency !== 2)
    || !Number.isInteger(execution.item_timeout_seconds) || typeof execution.item_timeout_seconds !== "number" || execution.item_timeout_seconds < 1 || execution.item_timeout_seconds > 180
    || !Number.isInteger(execution.scorer_timeout_seconds) || typeof execution.scorer_timeout_seconds !== "number" || execution.scorer_timeout_seconds < 1 || execution.scorer_timeout_seconds > 30
    || !Number.isInteger(execution.batch_timeout_seconds) || typeof execution.batch_timeout_seconds !== "number" || execution.batch_timeout_seconds < 1 || execution.batch_timeout_seconds > 240) {
    throw new Error("计划执行预算完整规范响应无效。");
  }
  const metrics = parseMetricSpecs(template.metrics);
  return {
    schedule_id: row.schedule_id,
    version: row.version,
    epoch: row.epoch,
    interval_seconds: row.interval_seconds,
    max_windows: row.max_windows,
    template: {
      schema_version: "continuous-task/1",
      request_key: template.request_key,
      mode: "execute",
      dataset: { name: dataset.name, version: dataset.version, fingerprint: dataset.fingerprint },
      dataset_envelope_fp: template.dataset_envelope_fp,
      target_key: template.target_key,
      metrics,
      execution: {
        repetitions: execution.repetitions,
        concurrency: execution.concurrency,
        item_timeout_seconds: execution.item_timeout_seconds,
        scorer_timeout_seconds: execution.scorer_timeout_seconds,
        batch_timeout_seconds: execution.batch_timeout_seconds,
      },
      parent_task_id: null,
      parent_report_hash: null,
      observations: [],
      seed_policy: "unseeded/1",
      schedule: null,
    },
  };
}

function parseScheduleControlReceipt(value: unknown): ScheduleControlReceipt | null {
  if (value === null) return null;
  const row = object(value);
  if (!hasExactKeys(row, ["key", "decision", "expected_revision", "revision"])
    || !isIdentity(row.key) || row.decision !== "disabled" || !nonNegativeInteger(row.expected_revision)
    || !nonNegativeInteger(row.revision) || row.revision !== row.expected_revision + 1) {
    throw new Error("计划控制回执响应无效。");
  }
  return {
    key: row.key,
    decision: "disabled",
    expected_revision: row.expected_revision,
    revision: row.revision,
  };
}

export function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function nonNegativeInteger(value: unknown): value is number {
  return typeof value === "number" && Number.isInteger(value) && value >= 0;
}

export function isIdentity(value: unknown): value is string {
  return typeof value === "string" && /^[A-Za-z0-9_.-]{1,64}$/.test(value);
}

export function isDigest(value: unknown): value is string {
  return typeof value === "string" && /^[0-9a-f]{64}$/.test(value);
}

export function parseAutomationStatus(value: unknown): AutomationStatus {
  const root = object(value);
  const automation = object(root.automation);
  const capture = object(root.capture);
  const counters = object(capture.counters);
  const schedules = object(root.schedules);
  const counts = ["pending", "claimed", "acknowledged", "artifact_count"] as const;
  const counterNames = ["seen", "enqueued", "dropped", "acked"] as const;
  if (typeof automation.running !== "boolean" || !(automation.last_error === null || typeof automation.last_error === "string")
    || typeof capture.configured !== "boolean" || typeof capture.enabled !== "boolean" || typeof capture.observer_ready !== "boolean"
    || typeof capture.text_capture !== "boolean" || typeof capture.argument_capture !== "boolean"
    || counts.some(name => !nonNegativeInteger(capture[name]))
    || counterNames.some(name => !nonNegativeInteger(counters[name]))
    || !nonNegativeInteger(schedules.total) || !nonNegativeInteger(schedules.enabled)) {
    throw new Error("自动化状态响应无效。");
  }
  return {
    automation: { running: automation.running, last_error: automation.last_error },
    capture: {
      configured: capture.configured,
      enabled: capture.enabled,
      observer_ready: capture.observer_ready,
      pending: capture.pending as number,
      claimed: capture.claimed as number,
      acknowledged: capture.acknowledged as number,
      counters: {
        seen: counters.seen as number,
        enqueued: counters.enqueued as number,
        dropped: counters.dropped as number,
        acked: counters.acked as number,
      },
      text_capture: capture.text_capture,
      argument_capture: capture.argument_capture,
      artifact_count: capture.artifact_count as number,
    },
    schedules: { total: schedules.total as number, enabled: schedules.enabled as number },
  };
}

export function parseTargets(value: unknown): EvaluationTarget[] {
  if (!Array.isArray(value)) throw new Error("评测目标目录响应无效。");
  return value.map(item => {
    const row = object(item);
    if (typeof row.key !== "string" || !row.key || typeof row.version !== "string" || typeof row.kind !== "string") {
      throw new Error("评测目标目录响应无效。");
    }
    return { key: row.key, version: row.version, kind: row.kind };
  });
}

export function parseDatasetRows(value: unknown): DatasetRow[] {
  if (!Array.isArray(value)) throw new Error("数据集目录响应无效。");
  return value.map(item => {
    const row = object(item);
    if (typeof row.name !== "string" || typeof row.version !== "string" || !isDigest(row.fingerprint)
      || !nonNegativeInteger(row.case_count)) throw new Error("数据集目录响应无效。");
    return { name: row.name, version: row.version, fingerprint: row.fingerprint, case_count: row.case_count };
  });
}

export function parseContinuousDataset(value: unknown, expected: DatasetRow): ContinuousDataset {
  const row = object(value);
  const datasetEnvelopeFp = row.dataset_envelope_fp;
  if (row.name !== expected.name || row.version !== expected.version || row.fingerprint !== expected.fingerprint
    || !isDigest(datasetEnvelopeFp) || !Array.isArray(row.cases)) {
    throw new Error("该版本没有服务端返回的连续评测冻结记录，不能创建回归计划。");
  }
  const verifiers = new Set<string>();
  let requiresProcessMetric = false;
  for (const item of row.cases) {
    const caseValue = object(item);
    if (typeof caseValue.verifier === "string") verifiers.add(caseValue.verifier);
    if (isRecord(caseValue.reference) && "process_constraints" in caseValue.reference) requiresProcessMetric = true;
  }
  return {
    dataset: { name: expected.name, version: expected.version, fingerprint: expected.fingerprint },
    dataset_envelope_fp: datasetEnvelopeFp,
    case_count: row.cases.length,
    verifiers: [...verifiers],
    requires_process_metric: requiresProcessMetric,
  };
}

export function parseMetricSpecs(value: unknown): MetricSpec[] {
  if (!Array.isArray(value)) throw new Error("连续评测指标目录响应无效。");
  return value.map(item => {
    const row = object(item);
    if (typeof row.name !== "string" || !isIdentity(row.name) || typeof row.version !== "string" || !isIdentity(row.version)
      || !isDigest(row.implementation_digest) || !isRecord(row.config)
      || !Array.isArray(row.required_evidence) || !row.required_evidence.every(entry => typeof entry === "string")
      || typeof row.hard !== "boolean") throw new Error("连续评测指标目录响应无效。");
    return {
      name: row.name,
      version: row.version,
      implementation_digest: row.implementation_digest,
      config: row.config,
      required_evidence: row.required_evidence,
      hard: row.hard,
    };
  });
}

export function parseScheduleViews(value: unknown): ScheduleView[] {
  if (!Array.isArray(value)) throw new Error("回归计划目录响应无效。");
  return value.map(parseScheduleView);
}

export function parseScheduleView(value: unknown): ScheduleView {
  const row = object(value);
  const dataset = object(row.dataset);
  if (!isIdentity(row.schedule_id) || !isIdentity(row.version) || typeof row.enabled !== "boolean"
    || !nonNegativeInteger(row.revision) || typeof row.observed_through !== "number"
    || !Number.isInteger(row.observed_through) || row.observed_through < -1 || !nonNegativeInteger(row.missed_windows)
    || typeof dataset.name !== "string" || typeof dataset.version !== "string" || !isDigest(dataset.fingerprint)
    || !isIdentity(row.target_key) || !nonNegativeInteger(row.epoch)
    || typeof row.interval_seconds !== "number" || !Number.isInteger(row.interval_seconds)
    || row.interval_seconds < 60 || row.interval_seconds > 86400
    || typeof row.max_windows !== "number" || !Number.isInteger(row.max_windows) || row.max_windows < 1 || row.max_windows > 3) {
    throw new Error("回归计划详情响应无效。");
  }
  const hasSpec = "spec" in row;
  const hasFingerprint = "spec_fingerprint" in row;
  if (hasSpec !== hasFingerprint) throw new Error("计划完整规范和指纹必须同时返回。");
  const spec = hasSpec ? parsePublicScheduleSpec(row.spec, row.schedule_id, row.version) : undefined;
  const specFingerprint = hasFingerprint && isDigest(row.spec_fingerprint) ? row.spec_fingerprint : undefined;
  if (hasFingerprint && !specFingerprint) throw new Error("计划完整规范指纹无效。");
  if (spec && (spec.epoch !== row.epoch || spec.interval_seconds !== row.interval_seconds || spec.max_windows !== row.max_windows
    || spec.template.target_key !== row.target_key || spec.template.dataset.name !== dataset.name
    || spec.template.dataset.version !== dataset.version || spec.template.dataset.fingerprint !== dataset.fingerprint)) {
    throw new Error("计划完整规范与公开详情不一致。");
  }
  const controlReceipt = "control_receipt" in row ? parseScheduleControlReceipt(row.control_receipt) : undefined;
  if (controlReceipt === null && (row.enabled !== true || row.revision !== 0)
    || controlReceipt && (row.enabled !== false || row.revision !== controlReceipt.revision)) {
    throw new Error("计划控制回执与公开状态不一致。");
  }
  return {
    schedule_id: row.schedule_id,
    version: row.version,
    enabled: row.enabled,
    revision: row.revision,
    observed_through: row.observed_through,
    missed_windows: row.missed_windows,
    dataset: { name: dataset.name, version: dataset.version, fingerprint: dataset.fingerprint },
    target_key: row.target_key,
    epoch: row.epoch,
    interval_seconds: row.interval_seconds,
    max_windows: row.max_windows,
    ...(spec ? { spec } : {}),
    ...(specFingerprint ? { spec_fingerprint: specFingerprint } : {}),
    ...(controlReceipt !== undefined ? { control_receipt: controlReceipt } : {}),
  };
}

export function parseTaskReceipt(value: unknown): { id: string; request_key: string; status: string } {
  const row = object(value);
  if (typeof row.id !== "string" || !row.id || typeof row.request_key !== "string" || !row.request_key || typeof row.status !== "string") {
    throw new Error("当前窗口任务响应无效。");
  }
  return { id: row.id, request_key: row.request_key, status: row.status };
}

export function createAutomationKey(prefix = "automation"): string {
  const token = globalThis.crypto?.randomUUID?.().replaceAll("-", "")
    ?? `${Math.random().toString(36).slice(2)}${Date.now().toString(36)}`;
  return `${prefix}-${token}`.slice(0, 64);
}

/** 密钥只作为摘要输入，不写入本地操作账本的键名。 */
export async function evaluationAutomationStorageKey(apiKey: string, workspace: string, identity: AutomationIdentity): Promise<string> {
  const input = JSON.stringify([workspace, apiKey, identity.principal_id, identity.generation]);
  const digest = await globalThis.crypto.subtle.digest("SHA-256", new TextEncoder().encode(input));
  const hex = [...new Uint8Array(digest)].map(value => value.toString(16).padStart(2, "0")).join("");
  return `krim.evaluation-automation.v1:${hex}`;
}

const emptyLedger = (): AutomationLedger => ({ version: 1, operations: [] });
const MAX_OPERATIONS = 40;
const states: OperationState[] = ["pending", "unknown", "found", "rejected"];

function decodeLedger(raw: string): AutomationLedger {
  let parsed: unknown;
  try { parsed = JSON.parse(raw); }
  catch { throw new AutomationLedgerError("ledger_corrupt", "本地自动化操作记录损坏；为避免重复提交，已阻止新操作。"); }
  if (!isRecord(parsed) || parsed.version !== 1 || !Array.isArray(parsed.operations)
    || !parsed.operations.every(isAutomationOperation)) {
    throw new AutomationLedgerError("ledger_corrupt", "本地自动化操作记录结构无效；为避免重复提交，已阻止新操作。");
  }
  return { version: 1, operations: parsed.operations };
}

function isAutomationOperation(value: unknown): value is AutomationOperationIntent {
  if (!isRecord(value)) return false;
  return typeof value.id === "string" && !!value.id && typeof value.kind === "string" && !!value.kind
    && states.includes(value.state as OperationState) && typeof value.created_at === "number" && Number.isFinite(value.created_at)
    && typeof value.object_id === "string" && !!value.object_id
    && (value.body === undefined || isRecord(value.body))
    && (value.request_key === undefined || typeof value.request_key === "string")
    && (value.result_id === undefined || typeof value.result_id === "string");
}

export function readAutomationLedger(storageKey: string): AutomationLedger {
  let raw: string | null;
  try { raw = localStorage.getItem(storageKey); }
  catch { throw new AutomationLedgerError("ledger_read_failed", "无法读取本地自动化操作记录；为避免重复提交，已阻止新操作。"); }
  return raw === null ? emptyLedger() : decodeLedger(raw);
}

export function writeAutomationLedger(storageKey: string, ledger: AutomationLedger): AutomationLedger {
  const serialized = JSON.stringify(ledger);
  try {
    localStorage.setItem(storageKey, serialized);
    const verified = localStorage.getItem(storageKey);
    if (verified !== serialized) throw new AutomationLedgerError("ledger_verification_failed", "本地操作记录写入后回读不一致；本次操作未发送。");
    return decodeLedger(verified);
  } catch (error) {
    if (error instanceof AutomationLedgerError) throw error;
    throw new AutomationLedgerError("ledger_write_failed", "本地操作记录未能持久化并回读；本次操作未发送。");
  }
}

export function appendAutomationOperation(ledger: AutomationLedger, intent: AutomationOperationIntent): AutomationLedger {
  let operations = [...ledger.operations.filter(item => item.id !== intent.id), intent];
  while (operations.length > MAX_OPERATIONS) {
    const removable = operations.findIndex(item => item.state === "found" || item.state === "rejected");
    if (removable < 0) throw new AutomationLedgerError("ledger_capacity", "已有 40 个未确认的自动化操作；请先只读核实原操作，本次未发送。");
    operations = operations.filter((_item, index) => index !== removable);
  }
  return { version: 1, operations };
}

export function replaceAutomationOperation(ledger: AutomationLedger, intent: AutomationOperationIntent): AutomationLedger {
  if (!ledger.operations.some(item => item.id === intent.id)) {
    throw new AutomationLedgerError("ledger_intent_missing", "找不到已持久化的原自动化操作；没有再次发送请求。");
  }
  return { version: 1, operations: ledger.operations.map(item => item.id === intent.id ? intent : item) };
}

export function isDefiniteRequestRejection(error: unknown): error is RequestError {
  return error instanceof RequestError && error.status >= 400 && error.status < 500;
}
