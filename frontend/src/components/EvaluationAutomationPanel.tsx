import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { RequestError, object } from "../api";
import { Button } from "./ui/button";
import { Input } from "./ui/input";
import { Label } from "./ui/label";
import {
  appendAutomationOperation,
  createAutomationKey,
  evaluationAutomationApi,
  evaluationAutomationStorageKey,
  evaluationCatalogApi,
  isDefiniteRequestRejection,
  isIdentity,
  parseAutomationStatus,
  parseContinuousDataset,
  parseCurrentScheduleWindow,
  parseDatasetRows,
  parseFeedbackReceipt,
  parseMetricSpecs,
  parseScheduleView,
  parseScheduleViews,
  parseScheduleWindowReceipt,
  parseTargets,
  parseTaskReceipt,
  readAutomationLedger,
  replaceAutomationOperation,
  writeAutomationLedger,
  type AutomationIdentity,
  type AutomationLedger,
  type AutomationOperationIntent,
  type AutomationStatus,
  type ContinuousDataset,
  type DatasetRow,
  type EvaluationTarget,
  type FeedbackReason,
  type FeedbackReceipt,
  type MetricSpec,
  type ScheduleSpec,
  type ScheduleView,
} from "../lib/evaluation-automation";
import "./evaluation-automation.css";

export type EvaluationAutomationPanelProps = {
  apiKey: string;
  workspace: string;
  identity: AutomationIdentity;
  active: boolean;
  canWrite: boolean;
  canApprove: boolean;
  isAdmin: boolean;
  sourceRun?: string;
  onTask?: (id: string) => void;
  onCandidate?: (runId: string, candidateId: string) => void;
};

type Scoped<T> = { stamp: string; value?: T; error?: string };
type PanelForm = {
  stamp: string;
  feedbackRun: string;
  feedbackReason: FeedbackReason;
  scheduleId: string;
  scheduleVersion: string;
  datasetId: string;
  targetKey: string;
  metricNames: string[];
  epoch: string;
  interval: string;
  maxWindows: string;
};
type MetricState = { context: string; values: MetricSpec[]; loading: boolean; error?: string };

const feedbackReasons: Array<{ value: FeedbackReason; label: string }> = [
  { value: "incorrect", label: "结果不正确" },
  { value: "unsafe", label: "存在安全问题" },
  { value: "missing_context", label: "缺少必要上下文" },
  { value: "other", label: "其他问题" },
];

function initialForm(stamp: string, sourceRun?: string): PanelForm {
  return {
    stamp,
    feedbackRun: sourceRun ?? "",
    feedbackReason: "other",
    scheduleId: createAutomationKey("regression"),
    scheduleVersion: "v1",
    datasetId: "",
    targetKey: "",
    metricNames: [],
    epoch: String(Math.floor(Date.now() / 1000)),
    interval: "3600",
    maxWindows: "1",
  };
}

function readableError(error: unknown): string {
  if (error instanceof RequestError) {
    if (error.status === 401) return "访问凭据无效，请更新密钥后重试。";
    if (error.status === 403) return "当前身份没有此工作区的相应权限。";
    if (error.status === 404) return "没有找到对应的已捕获来源或回归计划。";
    if (error.status === 409) return "操作与服务端当前版本冲突；请刷新后只读核对。";
    if (error.status === 422) return "提交内容不符合自动化契约，请检查必填项与边界。";
    if (error.status === 503) return "本地自动化服务暂不可用。";
    return `请求未完成（HTTP ${error.status}：${error.code}）。`;
  }
  return error instanceof Error ? error.message : "自动化操作未完成。";
}

function metricLabel(name: string): string {
  const labels: Record<string, string> = {
    fact_integrity: "事实完整性",
    context_coverage: "上下文覆盖",
    krim_result: "Krim 执行结果",
    process_constraints: "进程约束",
  };
  return labels[name] ?? name.replaceAll("_", " ");
}

function datasetId(row: DatasetRow): string {
  return `${row.name}/${row.version}`;
}

function requiredMetrics(dataset: ContinuousDataset): string[] {
  return [
    "fact_integrity",
    "context_coverage",
    ...(dataset.verifiers.includes("krim-script/1") ? ["krim_result"] : []),
    ...(dataset.requires_process_metric ? ["process_constraints"] : []),
  ];
}

function feedbackReason(value: unknown): value is FeedbackReason {
  return value === "incorrect" || value === "unsafe" || value === "missing_context" || value === "other";
}

function operationKey(intent: AutomationOperationIntent): string {
  if (intent.request_key) return intent.request_key;
  if (typeof intent.body?.key === "string") return intent.body.key;
  const template = intent.body?.template;
  if (template && typeof template === "object" && !Array.isArray(template)) {
    const requestKey = (template as Record<string, unknown>).request_key;
    if (typeof requestKey === "string") return requestKey;
  }
  return intent.id;
}

function operationLabel(kind: string): string {
  const labels: Record<string, string> = {
    capture: "本地采集启停",
    feedback: "来源运行反馈",
    "schedule-create": "创建回归计划",
    "schedule-disable": "禁用回归计划",
    "schedule-tick": "当前窗口任务",
  };
  return labels[kind] ?? `未识别操作：${kind}`;
}

function canonicalJson(value: unknown): string {
  if (value === null || typeof value === "string" || typeof value === "boolean" || typeof value === "number") {
    const serialized = JSON.stringify(value);
    if (serialized === undefined) throw new Error("计划规范无法编码。");
    return serialized;
  }
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  if (value && typeof value === "object") {
    const entries = Object.entries(value as Record<string, unknown>).sort(([left], [right]) => left < right ? -1 : left > right ? 1 : 0);
    return `{${entries.map(([key, item]) => `${JSON.stringify(key)}:${canonicalJson(item)}`).join(",")}}`;
  }
  throw new Error("计划规范包含无法编码的值。");
}

async function fingerprintJson(value: unknown): Promise<string> {
  const digest = await globalThis.crypto.subtle.digest("SHA-256", new TextEncoder().encode(canonicalJson(value)));
  return [...new Uint8Array(digest)].map(item => item.toString(16).padStart(2, "0")).join("");
}

function normalizedScheduleSpec(value: unknown): Record<string, unknown> | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  const copy = JSON.parse(JSON.stringify(value)) as Record<string, unknown>;
  if (!copy.template || typeof copy.template !== "object" || Array.isArray(copy.template)) return null;
  delete copy.scope;
  const template = copy.template as Record<string, unknown>;
  delete template.scope;
  if (!("execution" in template)) template.execution = {
    repetitions: 1, concurrency: 1, item_timeout_seconds: 20, scorer_timeout_seconds: 1, batch_timeout_seconds: 120,
  };
  if (!("parent_task_id" in template)) template.parent_task_id = null;
  if (!("parent_report_hash" in template)) template.parent_report_hash = null;
  if (!("observations" in template)) template.observations = [];
  if (!("seed_policy" in template)) template.seed_policy = "unseeded/1";
  if (!("schedule" in template)) template.schedule = null;
  return copy;
}

async function matchesPublicScheduleSpec(view: ScheduleView, expected: unknown, expectedFingerprint?: unknown): Promise<boolean> {
  if (!view.spec || !view.spec_fingerprint) return false;
  const normalized = normalizedScheduleSpec(expected);
  if (!normalized || canonicalJson(view.spec) !== canonicalJson(normalized)) return false;
  const digest = await fingerprintJson(view.spec);
  return digest === view.spec_fingerprint && (expectedFingerprint === undefined || expectedFingerprint === digest);
}

async function matchesCreatedSchedule(view: ScheduleView, body: Record<string, unknown>): Promise<boolean> {
  return view.schedule_id === body.schedule_id && view.version === body.version
    && await matchesPublicScheduleSpec(view, body);
}

function matchesDisableReceipt(view: ScheduleView, intent: AutomationOperationIntent): Promise<boolean> {
  const body = intent.body;
  const receipt = view.control_receipt;
  if (!body || !receipt || typeof body.key !== "string" || typeof body.version !== "string"
    || typeof body.revision !== "number" || typeof body.spec_fingerprint !== "string"
    || view.schedule_id !== intent.object_id || view.version !== body.version || view.enabled
    || view.revision !== body.revision + 1 || receipt.key !== body.key || receipt.decision !== "disabled"
    || receipt.expected_revision !== body.revision || receipt.revision !== body.revision + 1) return Promise.resolve(false);
  return matchesPublicScheduleSpec(view, body.spec, body.spec_fingerprint);
}

export function EvaluationAutomationPanel({
  apiKey,
  workspace,
  identity,
  active,
  canWrite,
  canApprove,
  isAdmin,
  sourceRun,
  onTask,
  onCandidate,
}: EvaluationAutomationPanelProps) {
  const identityStamp = JSON.stringify([workspace, apiKey, identity.principal_id, identity.generation]);
  const identityRef = useRef(identityStamp);
  const identityEpoch = useRef(0);
  if (identityRef.current !== identityStamp) {
    identityRef.current = identityStamp;
    identityEpoch.current += 1;
  }
  const activeRef = useRef(active);
  activeRef.current = active;
  const [formState, setFormState] = useState<PanelForm>(() => initialForm(identityStamp, sourceRun));
  if (formState.stamp !== identityStamp) setFormState(initialForm(identityStamp, sourceRun));
  const form = formState.stamp === identityStamp ? formState : initialForm(identityStamp, sourceRun);
  const [ledgerState, setLedgerState] = useState<{
    stamp: string;
    storageKey: string;
    value: AutomationLedger;
    error?: string;
  } | null>(null);
  const [statusState, setStatusState] = useState<Scoped<AutomationStatus> | null>(null);
  const [scheduleState, setScheduleState] = useState<Scoped<{ values: ScheduleView[]; offset: number }> | null>(null);
  const [catalogState, setCatalogState] = useState<Scoped<{ targets: EvaluationTarget[]; datasets: DatasetRow[]; datasetOffset: number }> | null>(null);
  const [datasetState, setDatasetState] = useState<{ context: string; value?: ContinuousDataset; error?: string } | null>(null);
  const [metricState, setMetricState] = useState<MetricState | null>(null);
  const [selectedSchedule, setSelectedSchedule] = useState<{ stamp: string; value?: ScheduleView; error?: string } | null>(null);
  const [feedbackReceiptStates, setFeedbackReceiptStates] = useState<Record<string, Scoped<FeedbackReceipt>>>({});
  const [busyState, setBusyState] = useState<{ stamp: string; action: string } | null>(null);
  const [noticeState, setNoticeState] = useState<{ stamp: string; message: string } | null>(null);
  const [errorState, setErrorState] = useState<{ stamp: string; message: string } | null>(null);
  const [scheduleOffset, setScheduleOffset] = useState(0);
  const [datasetOffset, setDatasetOffset] = useState(0);
  const [refreshSequence, setRefreshSequence] = useState(0);

  const isCurrent = useCallback((stamp: string, epoch: number) =>
    identityRef.current === stamp && identityEpoch.current === epoch && activeRef.current,
  []);
  const updateForm = useCallback((patch: Partial<Omit<PanelForm, "stamp">>) => {
    setFormState(previous => {
      const current = previous.stamp === identityStamp ? previous : initialForm(identityStamp, sourceRun);
      return { ...current, ...patch, stamp: identityStamp };
    });
  }, [identityStamp, sourceRun]);
  const setNotice = useCallback((message: string, stamp = identityStamp) => setNoticeState({ stamp, message }), [identityStamp]);
  const setError = useCallback((message: string, stamp = identityStamp) => setErrorState({ stamp, message }), [identityStamp]);

  const storageKey = ledgerState?.stamp === identityStamp ? ledgerState.storageKey : "";
  const ledgerError = ledgerState?.stamp === identityStamp ? ledgerState.error : undefined;
  const ledgerReady = !!storageKey && !ledgerError;
  const ledger = ledgerState?.stamp === identityStamp ? ledgerState.value : { version: 1 as const, operations: [] };
  const status = statusState?.stamp === identityStamp ? statusState.value : undefined;
  const schedules = scheduleState?.stamp === identityStamp ? scheduleState.value : undefined;
  const catalog = catalogState?.stamp === identityStamp ? catalogState.value : undefined;
  const currentBusy = busyState?.stamp === identityStamp ? busyState.action : "";
  const currentNotice = noticeState?.stamp === identityStamp ? noticeState.message : "";
  const currentError = errorState?.stamp === identityStamp ? errorState.message : "";
  const selectedDatasetId = form.datasetId;
  const selectedDatasetRow = catalog?.datasets.find(row => datasetId(row) === selectedDatasetId) ?? null;
  const datasetContext = `${identityStamp}|${selectedDatasetId}|${selectedDatasetRow?.fingerprint ?? ""}`;
  const selectedDataset = datasetState?.context === datasetContext ? datasetState.value : undefined;
  const required = selectedDataset ? requiredMetrics(selectedDataset) : [];
  const metricContext = `${datasetContext}|${form.targetKey}|${selectedDataset?.requires_process_metric ?? false}`;
  const currentMetrics = metricState?.context === metricContext ? metricState.values : [];
  const metricsLoading = metricState?.context === metricContext && metricState.loading;
  const metricsError = metricState?.context === metricContext ? metricState.error : undefined;
  const selectedMetrics = useMemo(() => currentMetrics.filter(metric => form.metricNames.includes(metric.name)), [currentMetrics, form.metricNames]);
  const missingMetrics = required.filter(name => !form.metricNames.includes(name) || !currentMetrics.some(metric => metric.name === name));
  const scheduleBusy = !!currentBusy;
  const recoveryItems = ledger.operations.filter(item => item.state === "pending" || item.state === "unknown"
    || (item.kind === "feedback" && item.state === "found"));
  const rowsOnPage = schedules?.values ?? [];

  useEffect(() => {
    let live = true;
    const stamp = identityStamp;
    const epoch = identityEpoch.current;
    if (!active || !workspace || !identity.principal_id || !Number.isInteger(identity.generation)) {
      if (!active) setLedgerState(null);
      return () => { live = false; };
    }
    void evaluationAutomationStorageKey(apiKey, workspace, identity).then(key => {
      if (!live || !isCurrent(stamp, epoch)) return;
      try {
        setLedgerState({ stamp, storageKey: key, value: readAutomationLedger(key) });
      } catch (reason) {
        setLedgerState({
          stamp,
          storageKey: key,
          value: { version: 1, operations: [] },
          error: readableError(reason),
        });
      }
    }).catch(reason => {
      if (live && isCurrent(stamp, epoch)) {
        setLedgerState({ stamp, storageKey: "", value: { version: 1, operations: [] }, error: readableError(reason) });
      }
    });
    return () => { live = false; };
  }, [active, apiKey, workspace, identity.principal_id, identity.generation, identityStamp, isCurrent]);

  useEffect(() => {
    if (!sourceRun || formState.stamp !== identityStamp || formState.feedbackRun === sourceRun) return;
    updateForm({ feedbackRun: sourceRun });
  }, [sourceRun, identityStamp, formState.stamp, formState.feedbackRun, updateForm]);

  useEffect(() => {
    if (!active || !workspace || !identity.principal_id || !Number.isInteger(identity.generation)) return;
    const stamp = identityStamp;
    const epoch = identityEpoch.current;
    const controller = new AbortController();
    const signal = controller.signal;
    const load = async () => {
      const results = await Promise.allSettled([
        evaluationAutomationApi.status(workspace, apiKey, signal),
        evaluationAutomationApi.schedules(workspace, apiKey, scheduleOffset, 20, signal),
        evaluationCatalogApi.targets(workspace, apiKey, signal),
        evaluationCatalogApi.datasets(workspace, apiKey, datasetOffset, 20, signal),
      ]);
      if (signal.aborted || !isCurrent(stamp, epoch)) return;
      const [statusResult, scheduleResult, targetsResult, datasetsResult] = results;
      if (statusResult.status === "fulfilled") {
        try { setStatusState({ stamp, value: parseAutomationStatus(statusResult.value) }); }
        catch (reason) { setStatusState({ stamp, error: readableError(reason) }); }
      } else if (!signal.aborted) setStatusState({ stamp, error: readableError(statusResult.reason) });
      if (scheduleResult.status === "fulfilled") {
        try { setScheduleState({ stamp, value: { values: parseScheduleViews(scheduleResult.value), offset: scheduleOffset } }); }
        catch (reason) { setScheduleState({ stamp, error: readableError(reason) }); }
      } else if (!signal.aborted) setScheduleState({ stamp, error: readableError(scheduleResult.reason) });
      if (targetsResult.status === "fulfilled" && datasetsResult.status === "fulfilled") {
        try {
          setCatalogState({ stamp, value: {
            targets: parseTargets(targetsResult.value),
            datasets: parseDatasetRows(datasetsResult.value),
            datasetOffset,
          } });
        } catch (reason) { setCatalogState({ stamp, error: readableError(reason) }); }
      } else {
        const reason = targetsResult.status === "rejected" ? targetsResult.reason
          : datasetsResult.status === "rejected" ? datasetsResult.reason : new Error("评测目录读取失败。");
        if (!signal.aborted) setCatalogState({ stamp, error: readableError(reason) });
      }
    };
    void load();
    return () => controller.abort();
  }, [active, apiKey, workspace, identityStamp, scheduleOffset, datasetOffset, refreshSequence, isCurrent]);

  useEffect(() => {
    if (!active || !selectedDatasetRow) {
      setDatasetState(null);
      return;
    }
    const context = datasetContext;
    const stamp = identityStamp;
    const epoch = identityEpoch.current;
    const controller = new AbortController();
    void evaluationCatalogApi.dataset(workspace, apiKey, selectedDatasetRow.name, selectedDatasetRow.version, controller.signal)
      .then(result => {
        if (controller.signal.aborted || !isCurrent(stamp, epoch)) return;
        setDatasetState({ context, value: parseContinuousDataset(result, selectedDatasetRow) });
        updateForm({ metricNames: [] });
      })
      .catch(reason => {
        if (!controller.signal.aborted && isCurrent(stamp, epoch)) setDatasetState({ context, error: readableError(reason) });
      });
    return () => controller.abort();
  }, [active, apiKey, workspace, identityStamp, datasetContext, selectedDatasetRow?.name, selectedDatasetRow?.version, selectedDatasetRow?.fingerprint, isCurrent, updateForm]);

  useEffect(() => {
    if (!active || !selectedDataset || !form.targetKey) {
      setMetricState(null);
      return;
    }
    const context = metricContext;
    const stamp = identityStamp;
    const epoch = identityEpoch.current;
    const controller = new AbortController();
    setMetricState({ context, values: [], loading: true });
    void evaluationCatalogApi.metrics(workspace, apiKey, form.targetKey, selectedDataset.requires_process_metric, controller.signal)
      .then(result => {
        if (controller.signal.aborted || !isCurrent(stamp, epoch)) return;
        const values = parseMetricSpecs(result);
        setMetricState({ context, values, loading: false });
        const defaults = required.filter(name => values.some(metric => metric.name === name));
        updateForm({ metricNames: defaults });
      })
      .catch(reason => {
        if (!controller.signal.aborted && isCurrent(stamp, epoch)) setMetricState({ context, values: [], loading: false, error: readableError(reason) });
      });
    return () => controller.abort();
  }, [active, apiKey, workspace, identityStamp, metricContext, selectedDataset, form.targetKey, required.join("|"), isCurrent, updateForm]);

  useEffect(() => {
    if (!active || !catalog || catalog.targets.length === 0 || form.targetKey) return;
    updateForm({ targetKey: catalog.targets[0]!.key });
  }, [active, catalog, form.targetKey, updateForm]);

  const persistIntent = useCallback((intent: AutomationOperationIntent, stamp: string, epoch: number, key: string) => {
    if (!isCurrent(stamp, epoch) || !key) throw new Error("当前身份或工作区已变化；本次操作未发送。");
    const current = readAutomationLedger(key);
    const next = appendAutomationOperation(current, intent);
    const verified = writeAutomationLedger(key, next);
    if (isCurrent(stamp, epoch)) setLedgerState({ stamp, storageKey: key, value: verified });
  }, [isCurrent]);

  const persistIntentState = useCallback((intent: AutomationOperationIntent, state: AutomationOperationIntent["state"], resultId: string | undefined, stamp: string, epoch: number, key: string) => {
    if (!isCurrent(stamp, epoch) || !key) return;
    const current = readAutomationLedger(key);
    const updated = { ...intent, state, ...(resultId ? { result_id: resultId } : {}) };
    const verified = writeAutomationLedger(key, replaceAutomationOperation(current, updated));
    if (isCurrent(stamp, epoch)) setLedgerState({ stamp, storageKey: key, value: verified });
  }, [isCurrent]);

  function startIntent(kind: string, objectId: string, body: Record<string, unknown>, requestKey?: string): AutomationOperationIntent {
    const intent: AutomationOperationIntent = {
      id: createAutomationKey(kind),
      kind,
      state: "pending",
      created_at: Date.now(),
      object_id: objectId,
      body,
      ...(requestKey ? { request_key: requestKey } : {}),
    };
    persistIntent(intent, identityStamp, identityEpoch.current, storageKey);
    return intent;
  }

  function hasUnresolved(kind: string, objectId: string, key = storageKey): boolean {
    const saved = readAutomationLedger(key);
    return saved.operations.some(item => item.kind === kind && item.object_id === objectId
      && (item.state === "pending" || item.state === "unknown"));
  }

  function finishIntent(intent: AutomationOperationIntent, resultId?: string) {
    persistIntentState(intent, "found", resultId, identityStamp, identityEpoch.current, storageKey);
  }

  function failIntent(intent: AutomationOperationIntent, reason: unknown) {
    if (identityRef.current !== identityStamp || !activeRef.current) return;
    const state = isDefiniteRequestRejection(reason) ? "rejected" : "unknown";
    try { persistIntentState(intent, state, undefined, identityStamp, identityEpoch.current, storageKey); }
    catch (persistError) { setError(readableError(persistError)); }
    setError(readableError(reason));
    if (state === "unknown") setNotice("提交结果未确认；原操作已保留，刷新只读取状态，不会自动重发。");
  }

  async function refresh() {
    if (!active) return;
    setErrorState(null);
    setNotice("正在只读刷新状态、计划与评测目录。");
    setRefreshSequence(value => value + 1);
  }

  async function toggleCapture(enabled: boolean) {
    if (!active || !canWrite || !isAdmin || !ledgerReady || currentBusy) return;
    const stamp = identityStamp;
    const epoch = identityEpoch.current;
    const key = storageKey;
    setBusyState({ stamp, action: "capture" }); setErrorState(null); setNoticeState(null);
    let intent: AutomationOperationIntent;
    try {
      if (hasUnresolved("capture", "workspace-capture", key)) throw new Error("采集启停操作尚未核实；请先刷新状态，不会重复提交。");
      intent = startIntent("capture", "workspace-capture", { enabled }, createAutomationKey("capture"));
    } catch (reason) {
      if (isCurrent(stamp, epoch)) { setError(readableError(reason)); setBusyState(null); }
      return;
    }
    try {
      const next = parseAutomationStatus(await evaluationAutomationApi.setCapture(workspace, apiKey, enabled));
      if (!isCurrent(stamp, epoch)) return;
      finishIntent(intent, enabled ? "enabled" : "disabled");
      setStatusState({ stamp, value: next });
      setNotice(enabled ? "本地采集已启用。正文、工具参数和产物仍未采集。" : "本地采集已停用。");
    } catch (reason) {
      if (isCurrent(stamp, epoch)) failIntent(intent, reason);
    } finally {
      if (isCurrent(stamp, epoch)) setBusyState(null);
    }
  }

  async function submitFeedback() {
    const runId = form.feedbackRun.trim();
    if (!active || !canWrite || !ledgerReady || currentBusy || !runId || !feedbackReason(form.feedbackReason)) return;
    const stamp = identityStamp;
    const epoch = identityEpoch.current;
    const key = storageKey;
    const feedbackKey = createAutomationKey("feedback");
    setBusyState({ stamp, action: "feedback" }); setErrorState(null); setNoticeState(null);
    let intent: AutomationOperationIntent;
    try {
      if (hasUnresolved("feedback", runId, key)) throw new Error("此运行的反馈操作尚未核实；原键已保留，不会换键重发。");
      intent = startIntent("feedback", runId, { key: feedbackKey, reason: form.feedbackReason }, feedbackKey);
    } catch (reason) {
      if (isCurrent(stamp, epoch)) { setError(readableError(reason)); setBusyState(null); }
      return;
    }
    try {
      const result = object(await evaluationAutomationApi.feedback(workspace, apiKey, runId, { key: feedbackKey, reason: form.feedbackReason }));
      if (!isCurrent(stamp, epoch)) return;
      if (result.key !== feedbackKey || typeof result.revision !== "number" || !Number.isInteger(result.revision)) throw new Error("反馈响应与原请求键不一致；原操作保留为待核实。");
      finishIntent(intent, `revision-${result.revision}`);
      setNotice(`已提交明确反馈（修订 ${result.revision}）。反馈不会确认候选或作为人工参考答案。`);
    } catch (reason) {
      if (isCurrent(stamp, epoch)) failIntent(intent, reason);
    } finally {
      if (isCurrent(stamp, epoch)) setBusyState(null);
    }
  }

  function buildScheduleSpec(): ScheduleSpec {
    if (!selectedDataset || !form.targetKey) throw new Error("请选择服务端确认的连续数据集和评测目标。");
    if (!isIdentity(form.scheduleId) || !isIdentity(form.scheduleVersion)) throw new Error("计划编号和版本需为 1–64 位字母、数字、下划线、点或短横线。");
    const epoch = Number(form.epoch);
    const intervalSeconds = Number(form.interval);
    const maxWindows = Number(form.maxWindows);
    if (!Number.isInteger(epoch) || epoch < 0) throw new Error("起始时间必须是非负整数秒。");
    if (!Number.isInteger(intervalSeconds) || intervalSeconds < 60 || intervalSeconds > 86400) throw new Error("窗口间隔需为 60 到 86400 秒。");
    if (!Number.isInteger(maxWindows) || maxWindows < 1 || maxWindows > 3) throw new Error("窗口数需为 1 到 3。");
    if (selectedMetrics.length < 1 || selectedMetrics.length > 6 || missingMetrics.length > 0) throw new Error("请选择必需指标；连续计划最多包含 6 项真实目录指标。");
    return {
      scope: "local",
      schedule_id: form.scheduleId,
      version: form.scheduleVersion,
      template: {
        schema_version: "continuous-task/1",
        scope: "local",
        request_key: createAutomationKey("schedule-task"),
        mode: "execute",
        dataset: selectedDataset.dataset,
        dataset_envelope_fp: selectedDataset.dataset_envelope_fp,
        target_key: form.targetKey,
        metrics: [...selectedMetrics].sort((left, right) => left.name.localeCompare(right.name)),
        execution: { repetitions: 1, concurrency: 1, item_timeout_seconds: 20, scorer_timeout_seconds: 1, batch_timeout_seconds: 120 },
        parent_task_id: null,
        parent_report_hash: null,
        observations: [],
        seed_policy: "unseeded/1",
        schedule: null,
      },
      epoch,
      interval_seconds: intervalSeconds,
      max_windows: maxWindows,
    };
  }

  async function createSchedule() {
    if (!active || !canWrite || !canApprove || !ledgerReady || currentBusy) return;
    const stamp = identityStamp;
    const epoch = identityEpoch.current;
    const key = storageKey;
    setBusyState({ stamp, action: "schedule-create" }); setErrorState(null); setNoticeState(null);
    let spec: ScheduleSpec;
    let intent: AutomationOperationIntent;
    try {
      spec = buildScheduleSpec();
      const objectId = `${spec.schedule_id}/${spec.version}`;
      if (hasUnresolved("schedule-create", objectId, key)) throw new Error("此计划版本的创建结果尚未核实；请先读取原计划，不会重新发送。");
      intent = startIntent("schedule-create", objectId, spec as unknown as Record<string, unknown>, spec.template.request_key);
    } catch (reason) {
      if (isCurrent(stamp, epoch)) { setError(readableError(reason)); setBusyState(null); }
      return;
    }
    try {
      const result = parseScheduleView(await evaluationAutomationApi.createSchedule(workspace, apiKey, spec));
      if (!isCurrent(stamp, epoch)) return;
      if (!await matchesCreatedSchedule(result, spec as unknown as Record<string, unknown>)) {
        throw new Error("创建回执未能与原计划完整内容和指纹匹配；原操作保留待核实。");
      }
      finishIntent(intent, spec.schedule_id);
      setNotice(`已按完整回执确认回归计划 ${spec.schedule_id} · ${spec.version}。`);
      setSelectedSchedule({ stamp, error: undefined });
      setRefreshSequence(value => value + 1);
      void readScheduleDetail(spec.schedule_id, stamp, epoch);
    } catch (reason) {
      if (isCurrent(stamp, epoch)) failIntent(intent, reason);
    } finally {
      if (isCurrent(stamp, epoch)) setBusyState(null);
    }
  }

  async function readScheduleDetail(scheduleId: string, stamp = identityStamp, epoch = identityEpoch.current) {
    if (!active || !isCurrent(stamp, epoch)) return;
    try {
      const view = parseScheduleView(await evaluationAutomationApi.schedule(workspace, apiKey, scheduleId));
      if (!isCurrent(stamp, epoch)) return;
      if (view.schedule_id !== scheduleId) throw new Error("计划详情与请求编号不一致。");
      setSelectedSchedule({ stamp, value: view });
    } catch (reason) {
      if (isCurrent(stamp, epoch)) setSelectedSchedule({ stamp, error: readableError(reason) });
    }
  }

  async function tickSchedule(schedule: ScheduleView) {
    if (!active || !canWrite || !canApprove || !ledgerReady || currentBusy || !schedule.enabled) return;
    const stamp = identityStamp;
    const epoch = identityEpoch.current;
    const key = storageKey;
    setBusyState({ stamp, action: `tick:${schedule.schedule_id}` }); setErrorState(null); setNoticeState(null);
    let intent: AutomationOperationIntent;
    let windowIndex: number;
    try {
      if (hasUnresolved("schedule-tick", schedule.schedule_id, key)) throw new Error("此计划已有窗口任务结果待核实；不会再次触发。");
      const current = parseCurrentScheduleWindow(await evaluationAutomationApi.currentScheduleWindow(workspace, apiKey, schedule.schedule_id));
      if (!isCurrent(stamp, epoch)) return;
      if (current.schedule_id !== schedule.schedule_id || current.version !== schedule.version) {
        throw new Error("服务端当前窗口与计划版本不一致；本次未发送任务请求。");
      }
      if (!current.enabled || !current.eligible) {
        setNotice("当前计划窗口不可执行；没有创建本地操作记录或发送任务请求。");
        setBusyState(null);
        return;
      }
      if (current.window_index > 2) throw new Error("服务端当前窗口编号超出计划范围；本次未发送任务请求。");
      windowIndex = current.window_index;
      const operationId = createAutomationKey("tick");
      intent = startIntent("schedule-tick", schedule.schedule_id, {
        schedule_id: schedule.schedule_id,
        version: schedule.version,
        window_index: windowIndex,
        expected_window_index: windowIndex,
      }, operationId);
    } catch (reason) {
      if (isCurrent(stamp, epoch)) { setError(readableError(reason)); setBusyState(null); }
      return;
    }
    try {
      const task = parseTaskReceipt(await evaluationAutomationApi.tickSchedule(workspace, apiKey, schedule.schedule_id, windowIndex));
      if (!isCurrent(stamp, epoch)) return;
      finishIntent(intent, task.id);
      setNotice(`已读取计划 ${schedule.schedule_id} 的原窗口 ${windowIndex} 任务：${task.id}（${task.status}）。`);
      try { onTask?.(task.id); }
      catch (reason) { setError(`任务已读取，但打开报告失败：${readableError(reason)}`); }
      setRefreshSequence(value => value + 1);
    } catch (reason) {
      if (isCurrent(stamp, epoch)) failIntent(intent, reason);
    } finally {
      if (isCurrent(stamp, epoch)) setBusyState(null);
    }
  }

  async function disableSchedule(schedule: ScheduleView) {
    if (!active || !canWrite || !canApprove || !ledgerReady || currentBusy || !schedule.enabled) return;
    const stamp = identityStamp;
    const epoch = identityEpoch.current;
    const key = storageKey;
    const disableKey = createAutomationKey("disable");
    setBusyState({ stamp, action: `disable:${schedule.schedule_id}` }); setErrorState(null); setNoticeState(null);
    let intent: AutomationOperationIntent;
    try {
      if (hasUnresolved("schedule-disable", schedule.schedule_id, key)) throw new Error("此计划的禁用操作尚未核实；请先只读读取计划，不会重发。");
      const detail = parseScheduleView(await evaluationAutomationApi.schedule(workspace, apiKey, schedule.schedule_id));
      if (!isCurrent(stamp, epoch)) return;
      if (detail.schedule_id !== schedule.schedule_id || detail.version !== schedule.version
        || detail.revision !== schedule.revision || !detail.enabled
        || !await matchesPublicScheduleSpec(detail, detail.spec, detail.spec_fingerprint)) {
        throw new Error("计划详情与当前列表或完整规范指纹不一致；本次未发送禁用请求。");
      }
      const intentBody = {
        schedule_id: schedule.schedule_id,
        version: schedule.version,
        revision: schedule.revision,
        key: disableKey,
        spec: detail.spec,
        spec_fingerprint: detail.spec_fingerprint,
      };
      intent = startIntent("schedule-disable", schedule.schedule_id, intentBody, disableKey);
    } catch (reason) {
      if (isCurrent(stamp, epoch)) { setError(readableError(reason)); setBusyState(null); }
      return;
    }
    try {
      const result = parseScheduleView(await evaluationAutomationApi.disableSchedule(workspace, apiKey, schedule.schedule_id,
        { revision: schedule.revision, key: disableKey }));
      if (!isCurrent(stamp, epoch)) return;
      const receipt = result.control_receipt;
      if (result.schedule_id !== schedule.schedule_id || result.version !== schedule.version || result.enabled
        || result.revision !== schedule.revision + 1 || !receipt || receipt.key !== disableKey
        || receipt.decision !== "disabled" || receipt.expected_revision !== schedule.revision
        || receipt.revision !== schedule.revision + 1) {
        throw new Error("禁用回执与原键、版本或修订不一致；原操作保留待核实。");
      }
      finishIntent(intent, schedule.schedule_id);
      setNotice(`已按原禁用键和控制回执停用回归计划 ${schedule.schedule_id}。`);
      setRefreshSequence(value => value + 1);
      void readScheduleDetail(schedule.schedule_id, stamp, epoch);
    } catch (reason) {
      if (isCurrent(stamp, epoch)) failIntent(intent, reason);
    } finally {
      if (isCurrent(stamp, epoch)) setBusyState(null);
    }
  }

  function feedbackIntentDetails(intent: AutomationOperationIntent) {
    const key = intent.request_key;
    const reason = intent.body?.reason;
    if (intent.kind !== "feedback" || !intent.object_id || !key || intent.body?.key !== key || !feedbackReason(reason)) {
      throw new Error("原反馈记录缺少运行、键或原因；没有改用其他键查询。");
    }
    const revisionMatch = intent.result_id?.match(/^revision-(\d+)$/);
    if (intent.result_id && !revisionMatch) throw new Error("原反馈记录的修订回执无效；记录保留。");
    return { runId: intent.object_id, key, reason, expectedRevision: revisionMatch ? Number(revisionMatch[1]) : undefined };
  }

  async function readFeedbackReceipt(intent: AutomationOperationIntent, purpose: "verify" | "candidates") {
    if (!active || !storageKey || !ledgerReady || currentBusy || (purpose === "candidates" && intent.state !== "found")) return;
    const stamp = identityStamp;
    const epoch = identityEpoch.current;
    setBusyState({ stamp, action: `feedback-read:${intent.id}` }); setErrorState(null); setNoticeState(null);
    try {
      const { runId, key, reason, expectedRevision } = feedbackIntentDetails(intent);
      const raw = object(await evaluationAutomationApi.feedbackByKey(workspace, apiKey, runId, key));
      if (!isCurrent(stamp, epoch)) return;
      const receipt = parseFeedbackReceipt(raw);
      if (("run_id" in raw && raw.run_id !== runId) || receipt.key !== key || receipt.reason !== reason
        || (expectedRevision !== undefined && receipt.revision !== expectedRevision)) {
        throw new Error("服务端反馈回执与原运行、原键、原原因或修订不一致；操作仍待核实。");
      }
      setFeedbackReceiptStates(previous => ({ ...previous, [intent.id]: { stamp, value: receipt } }));
      if (purpose === "verify") {
        finishIntent(intent, `revision-${receipt.revision}`);
        setNotice(receipt.delivery_status === "linked"
          ? `已按原运行、原键和原因找回反馈修订 ${receipt.revision}，并读取到 ${receipt.candidate_ids.length} 个关联候选。`
          : `已按原运行、原键和原因找回反馈修订 ${receipt.revision}；候选状态为${receipt.delivery_status === "pending" ? "等待后台消费" : "未进入消费队列"}。没有重新发送 POST。`);
      } else {
        setNotice(receipt.delivery_status === "linked"
          ? `已读取原反馈关联的 ${receipt.candidate_ids.length} 个待审核候选。`
          : `原反馈修订 ${receipt.revision} 尚未关联候选（${receipt.delivery_status === "pending" ? "等待后台消费" : "未进入消费队列"}）；可稍后再次只读读取。`);
      }
    } catch (reason) {
      if (isCurrent(stamp, epoch)) {
        if (reason instanceof RequestError && reason.status === 404) {
          setNotice("暂未读取到原反馈回执；本地记录保留。GET 404 不证明原反馈未执行，也不会触发重发。");
        } else setError(readableError(reason));
      }
    } finally {
      if (isCurrent(stamp, epoch)) setBusyState(null);
    }
  }

  function openFeedbackCandidate(intent: AutomationOperationIntent, candidateId: string) {
    const state = feedbackReceiptStates[intent.id];
    const receipt = state?.stamp === identityStamp ? state.value : undefined;
    if (!isCurrent(identityStamp, identityEpoch.current) || !canApprove || !onCandidate || !receipt || receipt.delivery_status !== "linked"
      || !receipt.candidate_ids.includes(candidateId)) return;
    try { onCandidate(intent.object_id, candidateId); }
    catch (reason) { setError(`打开候选失败：${readableError(reason)}`); }
  }

  async function checkOriginal(intent: AutomationOperationIntent) {
    if (!active || !storageKey || !ledgerReady || currentBusy) return;
    const stamp = identityStamp;
    const epoch = identityEpoch.current;
    setBusyState({ stamp, action: `check:${intent.id}` }); setErrorState(null); setNoticeState(null);
    try {
      if (intent.kind === "capture") {
        const current = parseAutomationStatus(await evaluationAutomationApi.status(workspace, apiKey));
        if (!isCurrent(stamp, epoch)) return;
        setStatusState({ stamp, value: current });
        const expected = intent.body?.enabled;
        if (typeof expected === "boolean" && current.capture.enabled === expected) {
          finishIntent(intent, expected ? "enabled" : "disabled");
          setNotice("已通过只读状态找到原采集操作结果；没有重新发送 POST。");
        } else setNotice("服务端采集状态尚未匹配原操作；记录保留，未重新发送 POST。");
        return;
      }
      if (intent.kind === "schedule-tick") {
        const scheduleId = intent.object_id;
        const version = intent.body?.version;
        const windowIndex = intent.body?.window_index;
        if (typeof version !== "string" || !Number.isInteger(windowIndex) || typeof windowIndex !== "number" || windowIndex < 0 || windowIndex > 2) {
          setNotice("旧操作记录没有原窗口编号；保留待核实，不查询当前窗口，也不重新发送 POST。");
          return;
        }
        const original = parseScheduleWindowReceipt(await evaluationAutomationApi.scheduleWindow(workspace, apiKey, scheduleId, windowIndex));
        if (!isCurrent(stamp, epoch)) return;
        if (original.schedule_id !== scheduleId || original.version !== version || original.window_index !== windowIndex) {
          throw new Error("原窗口回执与本地计划版本或窗口编号不一致；操作仍待核实。");
        }
        if (original.status === "reserved" && original.task) {
          finishIntent(intent, original.task.id);
          setNotice(`已按原窗口 ${windowIndex} 找到任务 ${original.task.id}（${original.task.status}）；没有重新发送 POST。`);
          try { onTask?.(original.task.id); }
          catch (reason) { setError(`任务已找到，但打开报告失败：${readableError(reason)}`); }
        } else {
          setNotice(`已读取原窗口 ${windowIndex} 的状态（${original.status}），尚无可确认任务；记录保留，未重新发送 POST。`);
        }
        return;
      }
      if (intent.kind !== "schedule-create" && intent.kind !== "schedule-disable") {
        setNotice("该操作没有可用的只读核实方式；原记录已保留，未重新发送 POST。");
        return;
      }
      const scheduleId = intent.kind === "schedule-create"
        ? String(intent.body?.schedule_id ?? "") : intent.object_id;
      if (!scheduleId) throw new Error("原操作记录缺少计划编号，已保留且没有重新发送。");
      const view = parseScheduleView(await evaluationAutomationApi.schedule(workspace, apiKey, scheduleId));
      if (!isCurrent(stamp, epoch)) return;
      setSelectedSchedule({ stamp, value: view });
      setRefreshSequence(value => value + 1);
      if (intent.kind === "schedule-create") {
        const matched = await matchesCreatedSchedule(view, intent.body ?? {});
        if (!isCurrent(stamp, epoch)) return;
        if (matched) {
          finishIntent(intent, scheduleId);
          setNotice(`已按原键、版本、完整计划内容和指纹找回计划 ${scheduleId}；没有重新发送 POST。`);
        } else setNotice(`已读取计划 ${scheduleId}，但其完整内容或指纹与原请求不完全相符；记录保留，未重新发送 POST。`);
      } else {
        const hasOriginalSnapshot = typeof intent.body?.version === "string" && !!intent.body.spec
          && typeof intent.body.spec_fingerprint === "string" && typeof intent.body.key === "string"
          && intent.body.key === intent.request_key;
        const matched = hasOriginalSnapshot && await matchesDisableReceipt(view, intent);
        if (!isCurrent(stamp, epoch)) return;
        if (matched) {
          finishIntent(intent, scheduleId);
          setNotice(`已按原计划版本、完整内容和禁用控制键找回计划 ${scheduleId}；没有重新发送 POST。`);
        } else setNotice(hasOriginalSnapshot
          ? `已读取计划 ${scheduleId}，但原禁用键、版本、完整内容或控制回执未全部匹配；记录保留，未重新发送 POST。`
          : `旧禁用记录缺少计划版本或完整内容快照；仅读取了计划，操作仍待核实，没有重新发送 POST。`);
      }
    } catch (reason) {
      if (isCurrent(stamp, epoch)) {
        if (reason instanceof RequestError && reason.status === 404) {
          const message = intent.kind === "schedule-tick"
            ? "暂未读取到原窗口回执；记录保留，GET 404 不证明原操作未执行，也不会重发。"
            : "暂未找到原计划；记录保留，GET 404 不证明原操作未执行，也不会重发。";
          setNotice(message);
        } else setError(readableError(reason));
      }
    } finally {
      if (isCurrent(stamp, epoch)) setBusyState(null);
    }
  }

  const storageMessage = ledgerError
    ? ledgerError
    : !storageKey ? "正在确定本地操作记录范围…" : "";
  const resolvedSchedule = selectedSchedule?.stamp === identityStamp ? selectedSchedule : null;
  const statusError = statusState?.stamp === identityStamp ? statusState.error : undefined;
  const scheduleError = scheduleState?.stamp === identityStamp ? scheduleState.error : undefined;
  const catalogError = catalogState?.stamp === identityStamp ? catalogState.error : undefined;
  const datasetError = datasetState?.context === datasetContext ? datasetState.error : undefined;

  return <section className="evaluation-automation-panel" aria-label="评测自动化" hidden={!active}>
    <header className="automation-heading">
      <div>
        <h1>评测自动化</h1>
        <p>管理本地采集与有限窗口回归计划。</p>
      </div>
      <Button variant="outline" disabled={!active} onClick={() => void refresh()}>只读刷新</Button>
    </header>

    {(currentError || statusError || scheduleError || catalogError || storageMessage) && <div className="automation-alert" role="alert">
      {[currentError, statusError, scheduleError, catalogError, storageMessage].filter(Boolean).join(" ")}
    </div>}
    {currentNotice && <p className="automation-notice" role="status">{currentNotice}</p>}

    {!canWrite && <p className="automation-muted">当前身份为只读；可以查看状态与计划，不能提交反馈或变更自动化。</p>}
    {canWrite && !canApprove && <p className="automation-muted">当前身份没有评测审批能力；计划创建、窗口任务与禁用操作不可用。</p>}

    <div className="automation-columns">
      <div className="automation-column">
        <section className="automation-section" aria-labelledby="automation-status-title">
          <div className="automation-section-heading">
            <div><h2 id="automation-status-title">本地自动化状态</h2><p>状态和计数来自当前工作区服务。</p></div>
          </div>
          {status ? <div className="automation-table-scroll">
            <table className="automation-status-table">
              <tbody>
                <tr><th scope="row">后台服务</th><td>{status.automation.running ? "运行中" : "未运行"}{status.automation.last_error && <span className="automation-inline-code">最近错误：{status.automation.last_error}</span>}</td></tr>
                <tr><th scope="row">终态采集</th><td>{status.capture.enabled ? "已启用" : "已停用"} · 观察者{status.capture.observer_ready ? "就绪" : "未就绪"}</td></tr>
                <tr><th scope="row">队列</th><td>待处理 {status.capture.pending} · 已领取 {status.capture.claimed} · 已确认 {status.capture.acknowledged}</td></tr>
                <tr><th scope="row">累计计数</th><td>观察 {status.capture.counters.seen} · 入队 {status.capture.counters.enqueued} · 丢弃 {status.capture.counters.dropped} · 确认 {status.capture.counters.acked}</td></tr>
                <tr><th scope="row">采集范围</th><td>正文 {status.capture.text_capture ? "开启" : "关闭"} · 工具参数 {status.capture.argument_capture ? "开启" : "关闭"} · 产物路径 {status.capture.artifact_count}</td></tr>
                <tr><th scope="row">回归计划</th><td>共 {status.schedules.total} 个 · 启用 {status.schedules.enabled} 个</td></tr>
              </tbody>
            </table>
          </div> : <p className="automation-muted">{statusError ?? "正在读取自动化状态…"}</p>}
          {isAdmin ? <div className="automation-actions">
            <Button disabled={!active || !canWrite || !ledgerReady || currentBusy === "capture" || !status || status.capture.enabled}
              onClick={() => void toggleCapture(true)}>{currentBusy === "capture" ? "正在提交…" : "启用本地采集"}</Button>
            <Button variant="outline" disabled={!active || !canWrite || !ledgerReady || currentBusy === "capture" || !status || !status.capture.enabled}
              onClick={() => void toggleCapture(false)}>停用本地采集</Button>
          </div> : <p className="automation-muted">采集启停仅向管理员提供。</p>}
        </section>

        <section className="automation-section" aria-labelledby="automation-feedback-title">
          <div className="automation-section-heading">
            <div><h2 id="automation-feedback-title">来源运行反馈</h2><p>服务端只接受已捕获的来源运行；反馈不构成人工参考。</p></div>
          </div>
          <div className="automation-field">
            <Label htmlFor="automation-feedback-run">已捕获来源运行 ID</Label>
            <Input id="automation-feedback-run" value={form.feedbackRun} onChange={event => updateForm({ feedbackRun: event.target.value })} placeholder="填写已捕获的运行 ID" />
          </div>
          <div className="automation-field">
            <Label htmlFor="automation-feedback-reason">反馈原因</Label>
            <select id="automation-feedback-reason" value={form.feedbackReason} onChange={event => {
              const value = event.target.value;
              if (feedbackReason(value)) updateForm({ feedbackReason: value });
            }}>
              {feedbackReasons.map(item => <option key={item.value} value={item.value}>{item.label}</option>)}
            </select>
          </div>
          <Button disabled={!active || !canWrite || !ledgerReady || !!currentBusy || !form.feedbackRun.trim()}
            onClick={() => void submitFeedback()}>{currentBusy === "feedback" ? "正在提交…" : "提交明确反馈"}</Button>
        </section>
      </div>

      <div className="automation-column">
        <section className="automation-section" aria-labelledby="automation-schedule-create-title">
          <div className="automation-section-heading">
            <div><h2 id="automation-schedule-create-title">创建有界回归计划</h2><p>只接受服务端返回的连续冻结版本和真实指标目录。</p></div>
          </div>
          <div className="automation-field">
            <Label htmlFor="automation-dataset">连续评测数据集版本</Label>
            <select id="automation-dataset" value={form.datasetId} disabled={!catalog || catalog.datasets.length === 0}
              onChange={event => {
                const row = catalog?.datasets.find(item => datasetId(item) === event.target.value);
                updateForm({ datasetId: row ? datasetId(row) : "", metricNames: [] });
                setDatasetState(null); setMetricState(null);
              }}>
              <option value="">选择冻结版本</option>
              {catalog?.datasets.map(row => <option key={datasetId(row)} value={datasetId(row)}>{row.name} · {row.version}</option>)}
            </select>
            <div className="automation-pagination">
              <Button size="sm" variant="outline" disabled={!catalog || datasetOffset === 0} onClick={() => setDatasetOffset(value => Math.max(0, value - 20))}>上一页数据集</Button>
              <span>目录偏移 {datasetOffset}</span>
              <Button size="sm" variant="outline" disabled={!catalog || catalog.datasets.length < 20} onClick={() => setDatasetOffset(value => value + 20)}>下一页数据集</Button>
            </div>
            {datasetError && <p className="automation-inline-warning" role="status">{datasetError}</p>}
            {selectedDataset && <p className="automation-muted">服务端已返回连续冻结记录 · {selectedDataset.case_count} 个样本 · 验证器 {selectedDataset.verifiers.join("、") || "未标注"}</p>}
          </div>

          <div className="automation-field">
            <Label htmlFor="automation-target">评测目标</Label>
            <select id="automation-target" value={form.targetKey} disabled={!catalog?.targets.length}
              onChange={event => updateForm({ targetKey: event.target.value, metricNames: [] })}>
              <option value="">选择工作区目标</option>
              {catalog?.targets.map(target => <option key={target.key} value={target.key}>{target.key} · {target.version}</option>)}
            </select>
          </div>

          {selectedDataset && <fieldset className="automation-metrics">
            <legend>连续评测指标</legend>
            {metricsLoading ? <p className="automation-muted">正在读取目标指标目录…</p>
              : metricsError ? <p className="automation-inline-warning" role="alert">{metricsError}</p>
                : currentMetrics.length ? currentMetrics.map(metric => {
                  const isRequired = required.includes(metric.name);
                  return <label key={`${metric.name}/${metric.version}`} className="automation-metric-choice">
                    <input type="checkbox" checked={form.metricNames.includes(metric.name)} disabled={isRequired}
                      onChange={event => updateForm({ metricNames: event.target.checked
                        ? [...new Set([...form.metricNames, metric.name])].slice(0, 6)
                        : form.metricNames.filter(name => name !== metric.name) })} />
                    <span><strong>{metricLabel(metric.name)}{isRequired ? "（必需）" : ""}</strong><small>{metric.version} · {metric.hard ? "质量指标" : "诊断指标"}</small></span>
                  </label>;
                }) : <p className="automation-muted">当前目标没有可用的连续评测指标。</p>}
            {missingMetrics.length > 0 && <p className="automation-inline-warning">缺少必需指标：{missingMetrics.map(metricLabel).join("、")}。</p>}
            {form.metricNames.length > 6 && <p className="automation-inline-warning">指标最多选择 6 项。</p>}
          </fieldset>}

          <div className="automation-form-grid">
            <div className="automation-field"><Label htmlFor="automation-schedule-id">计划编号</Label><Input id="automation-schedule-id" value={form.scheduleId} maxLength={64} onChange={event => updateForm({ scheduleId: event.target.value })} /></div>
            <div className="automation-field"><Label htmlFor="automation-schedule-version">计划版本</Label><Input id="automation-schedule-version" value={form.scheduleVersion} maxLength={64} onChange={event => updateForm({ scheduleVersion: event.target.value })} /></div>
            <div className="automation-field"><Label htmlFor="automation-epoch">窗口起点（Unix 秒）</Label><Input id="automation-epoch" type="number" min={0} step={1} value={form.epoch} onChange={event => updateForm({ epoch: event.target.value })} /></div>
            <div className="automation-field"><Label htmlFor="automation-interval">窗口间隔（秒）</Label><Input id="automation-interval" type="number" min={60} max={86400} step={1} value={form.interval} onChange={event => updateForm({ interval: event.target.value })} /></div>
            <div className="automation-field"><Label htmlFor="automation-max-windows">最多窗口数</Label><Input id="automation-max-windows" type="number" min={1} max={3} step={1} value={form.maxWindows} onChange={event => updateForm({ maxWindows: event.target.value })} /></div>
          </div>
          <p className="automation-muted">计划仅覆盖 1–3 个窗口，间隔为 60–86400 秒；服务器会固定本地工作区范围。</p>
          <Button disabled={!active || !canWrite || !canApprove || !ledgerReady || scheduleBusy || !selectedDataset || !form.targetKey
            || !currentMetrics.length || metricsLoading || missingMetrics.length > 0 || form.metricNames.length > 6}
            onClick={() => void createSchedule()}>{currentBusy === "schedule-create" ? "正在创建…" : "创建回归计划"}</Button>
        </section>

        <section className="automation-section" aria-labelledby="automation-schedule-list-title">
          <div className="automation-section-heading">
            <div><h2 id="automation-schedule-list-title">回归计划</h2><p>{status ? `服务端共 ${status.schedules.total} 个，启用 ${status.schedules.enabled} 个。` : "从服务端读取计划目录。"}</p></div>
          </div>
          {scheduleError ? <p className="automation-inline-warning" role="alert">{scheduleError}</p>
            : rowsOnPage.length ? <ul className="automation-schedule-list">
              {rowsOnPage.map(schedule => <li key={`${schedule.schedule_id}/${schedule.version}`}>
                <div className="automation-schedule-title"><strong>{schedule.schedule_id} · {schedule.version}</strong><span>{schedule.enabled ? "已启用" : "已停用"}</span></div>
                <dl className="automation-facts">
                  <dt>数据集</dt><dd>{schedule.dataset.name} · {schedule.dataset.version}</dd>
                  <dt>评测目标</dt><dd>{schedule.target_key}</dd>
                  <dt>窗口</dt><dd>{schedule.max_windows} 个 · 每 {schedule.interval_seconds} 秒 · 起点 {schedule.epoch}</dd>
                  <dt>进度</dt><dd>修订 {schedule.revision} · {schedule.observed_through < 0 ? "尚未观察窗口" : `已观察到 ${schedule.observed_through}`} · 错过 {schedule.missed_windows}</dd>
                </dl>
                <div className="automation-actions">
                  <Button size="sm" variant="outline" disabled={!active || !!currentBusy} onClick={() => void readScheduleDetail(schedule.schedule_id)}>读取详情</Button>
                  <Button size="sm" disabled={!active || !canWrite || !canApprove || !ledgerReady || !!currentBusy || !schedule.enabled}
                    onClick={() => void tickSchedule(schedule)}>{currentBusy === `tick:${schedule.schedule_id}` ? "正在读取…" : "读取当前窗口任务"}</Button>
                  <Button size="sm" variant="outline" disabled={!active || !canWrite || !canApprove || !ledgerReady || !!currentBusy || !schedule.enabled}
                    onClick={() => void disableSchedule(schedule)}>{currentBusy === `disable:${schedule.schedule_id}` ? "正在禁用…" : "禁用计划"}</Button>
                </div>
              </li>)}
            </ul> : <p className="automation-muted">{schedules ? "当前没有回归计划。" : "正在读取回归计划…"}</p>}
          <div className="automation-pagination">
            <Button size="sm" variant="outline" disabled={!schedules || scheduleOffset === 0} onClick={() => setScheduleOffset(value => Math.max(0, value - 20))}>上一页计划</Button>
            <span>目录偏移 {scheduleOffset}</span>
            <Button size="sm" variant="outline" disabled={!schedules || rowsOnPage.length < 20} onClick={() => setScheduleOffset(value => value + 20)}>下一页计划</Button>
          </div>
          {resolvedSchedule?.value && <section className="automation-detail" aria-label="回归计划详情">
            <h3>计划详情：{resolvedSchedule.value.schedule_id}</h3>
            <p>{resolvedSchedule.value.enabled ? "已启用" : "已停用"} · 修订 {resolvedSchedule.value.revision} · {resolvedSchedule.value.dataset.name} · {resolvedSchedule.value.dataset.version}</p>
          </section>}
          {resolvedSchedule?.error && <p className="automation-inline-warning" role="alert">{resolvedSchedule.error}</p>}
        </section>
      </div>
    </div>

    <section className="automation-section automation-recovery" aria-labelledby="automation-recovery-title">
      <div className="automation-section-heading">
        <div><h2 id="automation-recovery-title">待核实的原操作</h2><p>未知回程只保留原键或计划编号；不会自动重发。</p></div>
      </div>
      {recoveryItems.length ? <ul className="automation-recovery-list">
        {recoveryItems.map(intent => {
          const isFeedback = intent.kind === "feedback";
          const feedbackState = feedbackReceiptStates[intent.id];
          const receipt = feedbackState?.stamp === identityStamp ? feedbackState.value : undefined;
          const isChecking = currentBusy === `feedback-read:${intent.id}`;
          return <li key={intent.id}>
            <div>
              <strong>{operationLabel(intent.kind)} · {intent.state === "found" ? "原操作已找到" : intent.state === "pending" ? "提交状态待确认" : "结果未确认"}</strong>
              <small>对象：{intent.object_id} · 原键/编号：<code>{operationKey(intent)}</code></small>
              {isFeedback && receipt && <div className="automation-feedback-candidates">
                {receipt.delivery_status === "linked" ? <>
                  <span className="automation-muted">已关联待审核候选：</span>
                  {receipt.candidate_ids.map(candidateId => <div key={candidateId} className="automation-candidate-row">
                    <code>{candidateId}</code>
                    {onCandidate ? <Button size="sm" variant="outline" disabled={!active || !canApprove || !!currentBusy}
                      onClick={() => openFeedbackCandidate(intent, candidateId)}>进入人工确认</Button>
                      : <span className="automation-muted">当前页面没有候选确认入口。</span>}
                  </div>)}
                  {!canApprove && <span className="automation-muted">当前身份没有评测审批权限，不能进入人工确认。</span>}
                </> : <span className="automation-muted">反馈修订 {receipt.revision} · {receipt.delivery_status === "pending" ? "等待后台消费" : "未进入消费队列"}，尚无可跳转候选。</span>}
              </div>}
            </div>
            <div className="automation-recovery-actions">
              {isFeedback
                ? <Button size="sm" variant="outline" disabled={!active || !ledgerReady || !!currentBusy}
                    onClick={() => void readFeedbackReceipt(intent, intent.state === "found" ? "candidates" : "verify")}>
                    {isChecking ? "正在只读读取…" : intent.state === "found" ? receipt?.delivery_status === "linked" ? "重新读取反馈候选" : "读取反馈候选" : "只读核实原反馈"}
                  </Button>
                : intent.kind === "schedule-tick"
                  ? <Button size="sm" variant="outline" disabled={!active || !ledgerReady || !!currentBusy} onClick={() => void checkOriginal(intent)}>只读核实原窗口</Button>
                  : (["capture", "schedule-create", "schedule-disable"].includes(intent.kind)
                    ? <Button size="sm" variant="outline" disabled={!active || !ledgerReady || !!currentBusy} onClick={() => void checkOriginal(intent)}>只读核实</Button>
                    : <span className="automation-muted">原记录保留，不会自动重发。</span>)}
            </div>
          </li>;
        })}
      </ul> : <p className="automation-muted">没有待核实的操作。</p>}
    </section>
  </section>;
}
