import { RequestError, object } from "../api";

export interface ProductTarget {
  key: string;
  version: string;
  kind: string;
}

export interface ProductMetric {
  name: string;
  version: string;
  implementation_digest: string;
  config: Record<string, unknown>;
  required_evidence: string[];
  hard: boolean;
}

export interface ProductDatasetRef {
  name: string;
  version: string;
  fingerprint: string;
}

export interface ProductDatasetRow extends ProductDatasetRef {
  case_count: number;
}

export interface ProductTask {
  id: string;
  request_key: string;
  status: string;
  code: string | null;
  revision: number;
  mode: string;
  kind: "manual" | "continuous" | string;
  layout: number;
  dataset: ProductDatasetRef;
  target_key: string;
  metrics: ProductMetric[];
  planned_count: number | null;
  finished_count: number | null;
  created_at: number;
  finished_at: number | null;
  report_hash: string | null;
}

function nonnegativeInteger(value: unknown): number | null {
  return typeof value === "number" && Number.isInteger(value) && value >= 0 ? value : null;
}

/** The v4 task DTO uses flat planned_count/finished_count fields; absent values stay unknown. */
export function normalizeProductTask(value: unknown): ProductTask {
  const raw = object(value);
  return { ...raw, planned_count: nonnegativeInteger(raw.planned_count), finished_count: nonnegativeInteger(raw.finished_count) } as unknown as ProductTask;
}

export function normalizeProductTasks(value: unknown): ProductTask[] {
  if (!Array.isArray(value)) throw new Error("评测任务列表响应无效。");
  return value.map(normalizeProductTask);
}

export interface ProductCandidateSummary {
  candidate_id: string;
  observation_id: string;
  eligibility: "eligible" | "needs_context" | "ineligible" | string;
  reasons: string[];
  source: Record<string, unknown>;
}

export interface ProductImportReceipt {
  request_key: string;
  candidates: ProductCandidateSummary[];
}

export interface ProductCandidate {
  candidate_id: string;
  revision: number;
  status: string;
  observation_id: string;
  observation_hash: string;
  eligibility: string;
  reasons: string[];
  hash: string;
  confirmation_id?: string;
  draft_case?: ProductCaseSpec;
}

export interface ProductCaseSpec {
  case_id: string;
  instruction: string;
  files: Record<string, string>;
  reference: Record<string, unknown>;
  verifier: "krim-script/1";
}

export interface ProductDatasetRequest {
  name: string;
  version: string;
  cases: ProductCaseSpec[];
}

export function buildProductDatasetRequest(name: string, version: string, cases: ProductCaseSpec[]): ProductDatasetRequest {
  if (cases.length < 1 || cases.length > 100) throw new Error("数据集样本数需在 1 到 100 之间。");
  if (new Set(cases.map(item => item.case_id)).size !== cases.length) throw new Error("样本编号重复，请分别保留唯一编号。");
  const body: ProductDatasetRequest = { name, version, cases };
  if (new TextEncoder().encode(JSON.stringify(body)).byteLength > 65536) throw new Error("数据集请求超过 64 KiB，请缩短样本内容。");
  return body;
}

function stableJson(value: unknown): string {
  if (value === null || typeof value !== "object") return JSON.stringify(value) ?? "null";
  if (Array.isArray(value)) return `[${value.map(stableJson).join(",")}]`;
  const record = value as Record<string, unknown>;
  const members = Object.keys(record).filter(key => record[key] !== undefined).sort();
  return `{${members.map(key => `${JSON.stringify(key)}:${stableJson(record[key])}`).join(",")}}`;
}

export async function productDatasetFingerprint(name: string, version: string, cases: unknown[]): Promise<string> {
  const ordered = [...cases].sort((left, right) => {
    const leftId = left && typeof left === "object" && "case_id" in left ? String((left as Record<string, unknown>).case_id) : "";
    const rightId = right && typeof right === "object" && "case_id" in right ? String((right as Record<string, unknown>).case_id) : "";
    return leftId < rightId ? -1 : leftId > rightId ? 1 : 0;
  });
  const digest = await globalThis.crypto.subtle.digest("SHA-256", new TextEncoder().encode(stableJson({ name, version, cases: ordered })));
  return [...new Uint8Array(digest)].map(value => value.toString(16).padStart(2, "0")).join("");
}

export interface ProductFrozenDataset {
  dataset: ProductDatasetRef;
  dataset_envelope_fp: string;
  case_count?: number;
}

export interface ProductTaskIntent {
  request_key: string;
  body: Record<string, unknown>;
  created_at: number;
  task_id?: string;
  state: "pending" | "unknown" | "found" | "rejected";
}

export interface ProductOperationIntent {
  id: string;
  kind: "dataset-freeze" | "run-import" | "candidate-confirm" | "confirmed-freeze";
  state: "pending" | "unknown" | "found" | "rejected";
  created_at: number;
  request_key?: string;
  detail: Record<string, unknown>;
  result_id?: string;
}

export interface ProductIdentity {
  principal_id: string;
  generation: number;
}

export interface ProductEvaluationLedger {
  version: 1;
  task_intents: ProductTaskIntent[];
  operation_intents: ProductOperationIntent[];
  continuous_datasets: ProductFrozenDataset[];
}

export interface ProductReportRow {
  case_id: string;
  repetition: number;
  status: string;
  quality?: string;
  code?: string | null;
  scores?: Array<Record<string, unknown>>;
}

export interface ProductReportPage {
  eval_id: string;
  revision: number | null;
  report_hash: string | null;
  kind: string;
  subject_fingerprint?: string | null;
  dataset: ProductDatasetRef;
  execution: Record<string, unknown>;
  coverage: Record<string, unknown>;
  quality: Record<string, unknown>;
  rows: ProductReportRow[];
  next_cursor: string | null;
  next_offset: number | null;
}

export class ProductEvaluationError extends Error {
  readonly status: number | null;
  readonly code: string;
  readonly payload: unknown;

  constructor(message: string, status: number | null, code: string, payload: unknown) {
    super(message);
    this.name = "ProductEvaluationError";
    this.status = status;
    this.code = code;
    this.payload = payload;
  }
}

export class ProductLedgerError extends Error {
  readonly code: string;

  constructor(code: string, message = "评测请求账本不可用。") {
    super(message);
    this.name = "ProductLedgerError";
    this.code = code;
  }
}

export const PRODUCT_LEDGER_LIMIT = 20;

export function productErrorMessage(error: unknown): string {
  if (error instanceof ProductLedgerError) return error.message;
  const status = error instanceof ProductEvaluationError ? error.status
    : error instanceof RequestError ? error.status : null;
  const code = error instanceof ProductEvaluationError ? error.code
    : error instanceof RequestError ? error.code : "";
  if (status === 401 || code === "unauthorized") return "访问凭据无效，请更新密钥后重试。";
  if (status === 403 || code === "permission_denied") return "当前身份没有此工作区的相应权限。";
  if (status === 404 || code === "not_found") return "没有找到对应的数据或评测任务。";
  if (status === 409 || code === "key_conflict" || code === "revision_conflict" || code === "snapshot_changed") return "数据已发生变化，请刷新后重新选择。";
  if (status === 422 || code === "invalid_input" || code === "source_not_eligible") return "提交内容不符合评测要求，请检查必填项和来源资格。";
  if (status === 503 || code === "workspace_unavailable") return "工作区评测服务暂不可用。";
  if (error instanceof TypeError) return "网络连接中断，已保留本次请求凭据；可只读查找原任务。";
  return "评测请求未完成，请查看错误详情。";
}

export function createRequestKey(prefix = "product-eval"): string {
  const random = globalThis.crypto?.randomUUID?.().replaceAll("-", "")
    ?? Math.random().toString(36).slice(2) + Date.now().toString(36);
  return `${prefix}-${random}`;
}

/** 生成不含密钥原文的工作区账本标识，并隔离主体重绑定代次。 */
export async function evaluationStorageKey(apiKey: string, workspace: string, identity?: ProductIdentity): Promise<string> {
  const digestInput = JSON.stringify([workspace, apiKey, identity?.principal_id ?? "", identity?.generation ?? null]);
  const digest = await globalThis.crypto.subtle.digest("SHA-256", new TextEncoder().encode(digestInput));
  const suffix = [...new Uint8Array(digest)].map(value => value.toString(16).padStart(2, "0")).join("");
  return `krim.product-evaluation.v1:${suffix}`;
}

const emptyLedger = (): ProductEvaluationLedger => ({ version: 1, task_intents: [], operation_intents: [], continuous_datasets: [] });

function decodeLedger(raw: string): ProductEvaluationLedger {
  let parsed: unknown;
  try { parsed = JSON.parse(raw); }
  catch { throw new ProductLedgerError("ledger_corrupt", "本地评测请求账本内容损坏；为避免丢失未确认请求，已阻止新提交。"); }
  let value: Record<string, unknown>;
  try { value = object(parsed); }
  catch { throw new ProductLedgerError("ledger_corrupt", "本地评测请求账本结构损坏；为避免丢失未确认请求，已阻止新提交。"); }
  const taskIntents = value.task_intents;
  const operationIntents = value.operation_intents ?? [];
  const datasets = value.continuous_datasets;
  if (value.version !== 1 || !Array.isArray(taskIntents) || !taskIntents.every(isTaskIntent)
    || !Array.isArray(operationIntents) || !operationIntents.every(isOperationIntent)
    || !Array.isArray(datasets) || !datasets.every(isFrozenDataset)) {
    throw new ProductLedgerError("ledger_corrupt", "本地评测请求账本结构不完整；为避免丢失未确认请求，已阻止新提交。");
  }
  return { version: 1, task_intents: taskIntents, operation_intents: operationIntents, continuous_datasets: datasets };
}

export function readProductLedger(storageKey: string): ProductEvaluationLedger {
  let raw: string | null;
  try { raw = localStorage.getItem(storageKey); }
  catch { throw new ProductLedgerError("ledger_read_failed", "无法读取本地评测请求账本；为避免重复提交，已阻止新操作。"); }
  return raw === null ? emptyLedger() : decodeLedger(raw);
}

function isQuotaExceeded(error: unknown): boolean {
  if (!error || typeof error !== "object") return false;
  const value = error as { name?: unknown; code?: unknown };
  return value.name === "QuotaExceededError" || value.name === "NS_ERROR_DOM_QUOTA_REACHED" || value.code === 22 || value.code === 1014;
}

function compactConfirmedRecords(ledger: ProductEvaluationLedger): ProductEvaluationLedger | null {
  const taskIndex = ledger.task_intents.findIndex(item => item.state === "found" || item.state === "rejected");
  if (taskIndex >= 0) return { ...ledger, task_intents: ledger.task_intents.filter((_, index) => index !== taskIndex) };
  const operationIndex = ledger.operation_intents.findIndex(item => item.state === "found" || item.state === "rejected");
  if (operationIndex >= 0) return { ...ledger, operation_intents: ledger.operation_intents.filter((_, index) => index !== operationIndex) };
  if (ledger.continuous_datasets.length > 0) return { ...ledger, continuous_datasets: ledger.continuous_datasets.slice(1) };
  return null;
}

export function writeProductLedger(storageKey: string, ledger: ProductEvaluationLedger): ProductEvaluationLedger {
  let candidate = ledger;
  for (;;) {
    const serialized = JSON.stringify({
      version: 1,
      task_intents: candidate.task_intents,
      operation_intents: candidate.operation_intents,
      continuous_datasets: candidate.continuous_datasets,
    });
    try {
      localStorage.setItem(storageKey, serialized);
      const verified = localStorage.getItem(storageKey);
      if (verified !== serialized) throw new ProductLedgerError("ledger_verification_failed", "本地评测请求账本写入后回读不一致；本次操作未发送。");
      return decodeLedger(verified);
    } catch (error) {
      if (error instanceof ProductLedgerError) throw error;
      if (!isQuotaExceeded(error)) throw new ProductLedgerError("ledger_write_failed", "本地评测请求账本未能持久化并回读；本次操作未发送。");
      const compacted = compactConfirmedRecords(candidate);
      if (!compacted) throw new ProductLedgerError("ledger_capacity", "本地存储空间不足，且账本中没有可安全清理的已确认记录；本次操作未发送。");
      candidate = compacted;
    }
  }
}

export function upsertTaskIntent(ledger: ProductEvaluationLedger, intent: ProductTaskIntent): ProductEvaluationLedger {
  const intents = [...ledger.task_intents.filter(item => item.request_key !== intent.request_key), intent];
  while (intents.length > PRODUCT_LEDGER_LIMIT) {
    const evictable = intents.findIndex(item => item.state === "found" || item.state === "rejected");
    if (evictable < 0) throw new ProductLedgerError("ledger_capacity", "已有 20 个尚未确认的评测请求；请先只读找回或确认它们，当前任务未发送。");
    intents.splice(evictable, 1);
  }
  return { ...ledger, task_intents: intents };
}

export function upsertOperationIntent(ledger: ProductEvaluationLedger, intent: ProductOperationIntent): ProductEvaluationLedger {
  const intents = [...ledger.operation_intents.filter(item => item.id !== intent.id), intent];
  while (intents.length > PRODUCT_LEDGER_LIMIT) {
    const evictable = intents.findIndex(item => item.state === "found" || item.state === "rejected");
    if (evictable < 0) throw new ProductLedgerError("ledger_capacity", "已有 20 个尚未核实的评测操作；请先只读检查它们，当前操作未发送。");
    intents.splice(evictable, 1);
  }
  return { ...ledger, operation_intents: intents };
}

function isTaskIntent(value: unknown): value is ProductTaskIntent {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const item = value as Record<string, unknown>;
  return typeof item.request_key === "string" && !!item.body && typeof item.body === "object" && !Array.isArray(item.body)
    && typeof item.created_at === "number" && ["pending", "unknown", "found", "rejected"].includes(String(item.state));
}

function isOperationIntent(value: unknown): value is ProductOperationIntent {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const item = value as Record<string, unknown>;
  return typeof item.id === "string" && ["dataset-freeze", "run-import", "candidate-confirm", "confirmed-freeze"].includes(String(item.kind))
    && ["pending", "unknown", "found", "rejected"].includes(String(item.state)) && typeof item.created_at === "number"
    && typeof item.detail === "object" && item.detail !== null && !Array.isArray(item.detail)
    && (item.request_key === undefined || typeof item.request_key === "string");
}

function isFrozenDataset(value: unknown): value is ProductFrozenDataset {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const item = value as Record<string, unknown>;
  const dataset = item.dataset;
  return !!dataset && typeof dataset === "object" && !Array.isArray(dataset)
    && typeof (dataset as Record<string, unknown>).name === "string"
    && typeof (dataset as Record<string, unknown>).version === "string"
    && typeof (dataset as Record<string, unknown>).fingerprint === "string"
    && typeof item.dataset_envelope_fp === "string";
}

async function productRequest<T>(path: string, apiKey: string, body?: unknown, signal?: AbortSignal): Promise<T> {
  const headers: Record<string, string> = {};
  if (apiKey) headers.Authorization = `Bearer ${apiKey}`;
  if (body !== undefined) headers["Content-Type"] = "application/json";
  let response: Response;
  try {
    response = await fetch(path, {
      method: body === undefined ? "GET" : "POST",
      headers,
      body: body === undefined ? undefined : JSON.stringify(body),
      redirect: "error",
      signal: body === undefined ? signal : undefined,
    });
  } catch (error) {
    throw error;
  }
  let value: unknown;
  try {
    value = await response.json();
  } catch {
    value = { error: { code: "invalid_response" } };
  }
  if (!response.ok) {
    let code = "execution_error";
    try {
      const error = object(object(value).error);
      if (/^[a-z_]{1,64}$/.test(String(error.code))) code = String(error.code);
    } catch {
      // 保留原始响应供折叠详情查看。
    }
    throw new ProductEvaluationError(`HTTP ${response.status}: ${code}`, response.status, code, value);
  }
  return value as T;
}

export function evaluationBase(workspace: string): string {
  return `/v4/workspaces/${encodeURIComponent(workspace)}/eval`;
}

export const productEvaluationApi = {
  targets: (base: string, key: string, signal?: AbortSignal) => productRequest<ProductTarget[]>(`${base}/targets`, key, undefined, signal),
  metrics: (base: string, key: string, targetKey: string, mode: "manual" | "continuous", includeProcess = false, signal?: AbortSignal) => {
    const query = new URLSearchParams({ target_key: targetKey, mode, include_process: String(includeProcess) });
    return productRequest<ProductMetric[]>(`${base}/metrics?${query}`, key, undefined, signal);
  },
  datasets: (base: string, key: string, offset = 0, signal?: AbortSignal) => productRequest<ProductDatasetRow[]>(`${base}/datasets?offset=${offset}&limit=20`, key, undefined, signal),
  dataset: (base: string, key: string, name: string, version: string, signal?: AbortSignal) => productRequest<Record<string, unknown>>(`${base}/datasets/${encodeURIComponent(name)}/${encodeURIComponent(version)}`, key, undefined, signal),
  freezeDataset: (base: string, key: string, body: unknown) => productRequest<Record<string, unknown>>(`${base}/datasets`, key, body),
  importRuns: (base: string, key: string, body: unknown) => productRequest<ProductCandidateSummary[]>(`${base}/sources/runs/import`, key, body),
  importReceipt: (base: string, key: string, requestKey: string, signal?: AbortSignal) => productRequest<ProductImportReceipt>(`${base}/sources/runs/imports/by-key/${encodeURIComponent(requestKey)}`, key, undefined, signal),
  candidate: (base: string, key: string, id: string, signal?: AbortSignal) => productRequest<ProductCandidate>(`${base}/candidates/${encodeURIComponent(id)}`, key, undefined, signal),
  confirmCandidate: (base: string, key: string, id: string, body: unknown) => productRequest<Record<string, unknown>>(`${base}/candidates/${encodeURIComponent(id)}/confirm`, key, body),
  freezeConfirmed: (base: string, key: string, body: unknown) => productRequest<Record<string, unknown>>(`${base}/datasets/confirmed`, key, body),
  tasks: async (base: string, key: string, offset = 0, signal?: AbortSignal) => normalizeProductTasks(await productRequest<unknown>(`${base}/tasks?offset=${offset}&limit=20`, key, undefined, signal)),
  startTask: async (base: string, key: string, body: unknown) => normalizeProductTask(await productRequest<unknown>(`${base}/tasks`, key, body)),
  taskByKey: async (base: string, key: string, requestKey: string, signal?: AbortSignal) => normalizeProductTask(await productRequest<unknown>(`${base}/tasks/by-key/${encodeURIComponent(requestKey)}`, key, undefined, signal)),
  task: async (base: string, key: string, id: string, signal?: AbortSignal) => normalizeProductTask(await productRequest<unknown>(`${base}/tasks/${encodeURIComponent(id)}`, key, undefined, signal)),
  cancelTask: async (base: string, key: string, id: string) => normalizeProductTask(await productRequest<unknown>(`${base}/tasks/${encodeURIComponent(id)}/cancel`, key, {})),
  report: (base: string, key: string, id: string, page: { cursor?: string | null; offset?: number }, signal?: AbortSignal) => {
    const query = new URLSearchParams({ limit: "16" });
    if (page.cursor) query.set("cursor", page.cursor);
    if (!page.cursor && page.offset) query.set("offset", String(page.offset));
    return productRequest<ProductReportPage>(`${base}/tasks/${encodeURIComponent(id)}/report?${query}`, key, undefined, signal);
  },
  compare: (base: string, key: string, body: unknown) => productRequest<Record<string, unknown>>(`${base}/tasks/compare`, key, body),
};

export function isActiveProductTask(task: ProductTask | null): boolean {
  return task?.status === "queued" || task?.status === "running";
}
