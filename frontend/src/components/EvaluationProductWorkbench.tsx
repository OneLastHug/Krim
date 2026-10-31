import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Button } from "./ui/button";
import { Input } from "./ui/input";
import { Label } from "./ui/label";
import { Textarea } from "./ui/textarea";
import {
  createRequestKey,
  buildProductDatasetRequest,
  evaluationBase,
  evaluationStorageKey,
  isActiveProductTask,
  productErrorMessage,
  productDatasetFingerprint,
  productEvaluationApi as api,
  readProductLedger,
  upsertOperationIntent,
  upsertTaskIntent,
  writeProductLedger,
  type ProductCandidate,
  type ProductCandidateSummary,
  type ProductCaseSpec,
  type ProductDatasetRef,
  type ProductDatasetRow,
  type ProductEvaluationError,
  type ProductEvaluationLedger,
  type ProductFrozenDataset,
  type ProductIdentity,
  type ProductMetric,
  type ProductOperationIntent,
  type ProductReportPage,
  type ProductTarget,
  type ProductTask,
  type ProductTaskIntent,
} from "../lib/product-evaluation";
import "./product-evaluation.css";

type EvaluationView = "datasets" | "eval";
type Props = {
  apiKey: string;
  workspace: string;
  active: boolean;
  view: EvaluationView;
  sourceRun?: string;
  identity?: ProductIdentity;
  canWrite?: boolean;
  canApprove?: boolean;
  onRun?: (id: string) => void;
  taskId?: string;
  onTaskSelection?: (id: string) => void;
  candidateId?: string;
  onCandidateSelection?: (id: string, runId?: string) => void;
};
type ErrorState = { message: string; payload?: unknown; status?: number | null; code?: string };
type SelectedDataset = ProductDatasetRef & {
  caseCount: number;
  verifier: string;
  requiresProcessMetric: boolean;
  datasetEnvelopeFp?: string;
};
type ManualSampleDraft = { case_id: string; instruction: string; inputText: string; expected: string };
type ReportCursor = { cursor?: string | null; offset: number };

const EMPTY_LEDGER: ProductEvaluationLedger = { version: 1, task_intents: [], operation_intents: [], continuous_datasets: [] };
const taskStateLabel: Record<string, string> = {
  queued: "排队中", running: "运行中", completed: "已完成", failed: "失败", cancelled: "已取消", interrupted: "已中断",
};

function readableError(error: unknown): ErrorState {
  const product = error as Partial<ProductEvaluationError>;
  return {
    message: productErrorMessage(error),
    payload: product.payload,
    status: product.status,
    code: product.code,
  };
}

function targetText(target: ProductTarget): string {
  return `${target.key} · ${target.version}`;
}

function metricLabel(name: string): string {
  const labels: Record<string, string> = {
    fact_integrity: "事实完整性", context_coverage: "上下文覆盖", krim_result: "Krim 执行结果",
    process_constraints: "进程约束", artifact_match: "产物匹配", exit_code: "退出状态",
    native_proof: "原生执行证明", repository_checks: "仓库检查",
  };
  return labels[name] ?? name.replaceAll("_", " ");
}

function metricText(metric: ProductMetric): string {
  return metricLabel(metric.name);
}

function cleanCaseId(value: string): string {
  const result = value.replace(/[^A-Za-z0-9_.-]/g, "-").slice(0, 64);
  return result || "case-1";
}

function fingerprint(value: unknown): string {
  return typeof value === "string" && /^[0-9a-f]{64}$/.test(value) ? value : "";
}

function taskCount(value: unknown): number | null {
  return typeof value === "number" && Number.isInteger(value) && value >= 0 ? value : null;
}

export function EvaluationProductWorkbench({ apiKey, workspace, active, view, sourceRun, identity: actorIdentity, canWrite = false, canApprove = false, onRun, taskId, onTaskSelection, candidateId, onCandidateSelection }: Props) {
  const identityStamp = JSON.stringify([workspace, apiKey, actorIdentity?.principal_id ?? "", actorIdentity?.generation ?? null]);
  const identityRef = useRef(identityStamp);
  const identityEpoch = useRef(0);
  if (identityRef.current !== identityStamp) {
    identityRef.current = identityStamp;
    identityEpoch.current += 1;
  }
  const [scope, setScope] = useState<{ identity: string; storageKey: string } | null>(null);
  const storageKey = scope?.identity === identityStamp ? scope.storageKey : "";
  const base = useMemo(() => evaluationBase(workspace), [workspace]);
  const [loadedScope, setLoadedScope] = useState("");
  const [targets, setTargets] = useState<ProductTarget[]>([]);
  const [metrics, setMetrics] = useState<ProductMetric[]>([]);
  const [metricsLoading, setMetricsLoading] = useState(false);
  const [metricsContext, setMetricsContext] = useState("");
  const [metricsRefresh, setMetricsRefresh] = useState(0);
  const [includeProcess, setIncludeProcess] = useState(false);
  const [datasets, setDatasets] = useState<ProductDatasetRow[]>([]);
  const [tasks, setTasks] = useState<ProductTask[]>([]);
  const [datasetOffset, setDatasetOffset] = useState(0);
  const [taskOffset, setTaskOffset] = useState(0);
  const [ledgerState, setLedgerState] = useState<{ storageKey: string; value: ProductEvaluationLedger; error?: ErrorState } | null>(null);
  const [targetKey, setTargetKey] = useState("");
  const [metricNames, setMetricNames] = useState<string[]>([]);
  const [repetitions, setRepetitions] = useState(1);
  const [selectedDataset, setSelectedDataset] = useState<SelectedDataset | null>(null);
  const [selectedTask, setSelectedTask] = useState<ProductTask | null>(null);
  const [report, setReport] = useState<ProductReportPage | null>(null);
  const [reportPage, setReportPage] = useState<ReportCursor>({ offset: 0 });
  const [reportHistory, setReportHistory] = useState<ReportCursor[]>([]);
  const [reportLoading, setReportLoading] = useState(false);
  const [error, setError] = useState<{ identity: string; value: ErrorState } | null>(null);
  const [catalogLoading, setCatalogLoading] = useState(false);
  const [refreshing, setRefreshing] = useState(false);
  const [saving, setSaving] = useState(false);
  const [actionMessage, setActionMessage] = useState("");
  const requestFence = useRef(0);
  const metricsRequestSequence = useRef(0);
  const importRunRef = useRef(sourceRun ?? "");
  const loadedTaskLink = useRef("");
  const loadedCandidateLink = useRef("");

  const [datasetName, setDatasetName] = useState("script-suite");
  const [datasetVersion, setDatasetVersion] = useState("v1");
  const [instruction, setInstruction] = useState("");
  const [inputText, setInputText] = useState("");
  const [expectedText, setExpectedText] = useState("");
  const [sampleDrafts, setSampleDrafts] = useState<ManualSampleDraft[]>([]);
  const [editingSampleId, setEditingSampleId] = useState("");
  const [importRun, setImportRun] = useState(sourceRun ?? "");
  const [candidates, setCandidates] = useState<ProductCandidateSummary[]>([]);
  const [candidate, setCandidate] = useState<ProductCandidate | null>(null);
  const [candidateInstruction, setCandidateInstruction] = useState("");
  const [candidateInput, setCandidateInput] = useState("");
  const [candidateExpected, setCandidateExpected] = useState("");
  const [candidateReason, setCandidateReason] = useState("人工核对来源并补充参考结果");
  const [candidateBusy, setCandidateBusy] = useState(false);
  const [baselineId, setBaselineId] = useState("");
  const [candidateTaskId, setCandidateTaskId] = useState("");
  const [comparison, setComparison] = useState<Record<string, unknown> | null>(null);
  const [comparisonBusy, setComparisonBusy] = useState(false);

  const captureIsCurrent = useCallback((identity: string, epoch: number, key: string) =>
    identityRef.current === identity && identityEpoch.current === epoch && (!key || scope?.storageKey === key),
  [scope]);

  useEffect(() => {
    let live = true;
    const identity = identityStamp;
    void evaluationStorageKey(apiKey, workspace, actorIdentity).then(key => {
      if (live && identityRef.current === identity) setScope({ identity, storageKey: key });
    }).catch(reason => {
      if (live && identityRef.current === identity) setError({ identity, value: readableError(reason) });
    });
    return () => { live = false; };
  }, [apiKey, workspace, identityStamp]);

  useEffect(() => {
    if (!storageKey) return;
    let ledger = EMPTY_LEDGER;
    let ledgerError: ErrorState | undefined;
    try { ledger = readProductLedger(storageKey); }
    catch (reason) { ledgerError = readableError(reason); }
    setLedgerState({ storageKey, value: ledger, error: ledgerError });
    setLoadedScope("");
    setTargets([]); setMetrics([]); setMetricsContext(""); setDatasets([]); setTasks([]);
    setSelectedDataset(null); setSelectedTask(null); setReport(null); setCandidates([]); setCandidate(null);
    setMetricNames([]); setIncludeProcess(false); setError(null); setActionMessage(""); setDatasetOffset(0); setTaskOffset(0);
    setMetricsLoading(false); setSaving(false); setRefreshing(false); setCandidateBusy(false); setReportLoading(false); setComparisonBusy(false);
    setDatasetName("script-suite"); setDatasetVersion("v1"); setInstruction(""); setInputText(""); setExpectedText("");
    setSampleDrafts([]); setEditingSampleId("");
  }, [storageKey]);

  useEffect(() => {
    if (sourceRun && sourceRun !== importRunRef.current) {
      importRunRef.current = sourceRun;
      setImportRun(sourceRun);
      if (!candidateId) { setCandidate(null); setCandidates([]); }
    }
  }, [sourceRun, candidateId]);

  useEffect(() => {
    if (!active || !storageKey || scope?.identity !== identityStamp) return;
    const identity = identityStamp;
    const epoch = identityEpoch.current;
    const controller = new AbortController();
    const localFence = ++requestFence.current;
    const current = () => captureIsCurrent(identity, epoch, storageKey) && requestFence.current === localFence;
    setCatalogLoading(true);
    setError(null);
    void Promise.all([
      api.targets(base, apiKey, controller.signal),
      api.datasets(base, apiKey, datasetOffset, controller.signal),
      api.tasks(base, apiKey, taskOffset, controller.signal),
    ]).then(([nextTargets, nextDatasets, nextTasks]) => {
      if (!current()) return;
      setTargets(nextTargets);
      setDatasets(nextDatasets);
      setTasks(nextTasks);
      setTargetKey(previous => previous && nextTargets.some(item => item.key === previous) ? previous : nextTargets[0]?.key ?? "");
      setLoadedScope(storageKey);
      setError(null);
    }).catch(reason => {
      if (current() && !controller.signal.aborted) setError({ identity, value: readableError(reason) });
    }).finally(() => {
      if (current()) setCatalogLoading(false);
    });
    return () => { controller.abort(); requestFence.current += 1; };
  }, [active, storageKey, base, apiKey, datasetOffset, taskOffset, identityStamp, scope?.identity, captureIsCurrent]);

  const currentLedger = ledgerState?.storageKey === storageKey ? ledgerState.value : EMPTY_LEDGER;
  const ledgerError = ledgerState?.storageKey === storageKey ? ledgerState.error : undefined;
  const ledgerReady = !!storageKey && ledgerState?.storageKey === storageKey && !ledgerError;
  const ready = !!storageKey && loadedScope === storageKey;
  const currentError = ledgerError ?? (error?.identity === identityStamp ? error.value : null);
  const continuousDatasets = currentLedger.continuous_datasets;
  const isContinuousDataset = (value: SelectedDataset | null) => !!value?.datasetEnvelopeFp;
  const evaluationMode = isContinuousDataset(selectedDataset) ? "continuous" : "manual";
  const metricContext = `${targetKey}|${evaluationMode}|${includeProcess}`;
  const selectedMetrics = useMemo(() => metrics.filter(item => metricNames.includes(item.name)), [metrics, metricNames]);
  const requiredContinuousMetrics = useMemo(() => {
    if (!isContinuousDataset(selectedDataset)) return [];
    const required = ["fact_integrity", "context_coverage"];
    if (selectedDataset?.verifier === "krim-script/1") required.push("krim_result");
    if (selectedDataset?.requiresProcessMetric) required.push("process_constraints");
    return required;
  }, [selectedDataset]);
  const missingContinuousMetrics = requiredContinuousMetrics.filter(name => !metricNames.includes(name));
  const metricsReady = metricsContext === metricContext && !metricsLoading;

  useEffect(() => {
    const requestId = ++metricsRequestSequence.current;
    if (!active || !ready || !targetKey) {
      setMetricsLoading(false); setMetrics([]); setMetricNames([]); setMetricsContext("");
      return;
    }
    const identity = identityStamp, epoch = identityEpoch.current, key = storageKey;
    const controller = new AbortController();
    let live = true;
    const current = () => live && metricsRequestSequence.current === requestId && captureIsCurrent(identity, epoch, key);
    const required = selectedDataset?.datasetEnvelopeFp ? [
      "fact_integrity", "context_coverage",
      ...(selectedDataset.verifier === "krim-script/1" ? ["krim_result"] : []),
      ...(selectedDataset.requiresProcessMetric ? ["process_constraints"] : []),
    ] : [];
    setMetricsLoading(true); setMetrics([]); setMetricNames([]); setMetricsContext("");
    void api.metrics(base, apiKey, targetKey, evaluationMode, includeProcess, controller.signal).then(result => {
      if (!current()) return;
      setMetrics(result);
      setMetricNames(required.filter(name => result.some(metric => metric.name === name)));
      setMetricsContext(metricContext);
    }).catch(reason => {
      if (current() && !controller.signal.aborted) setError({ identity, value: readableError(reason) });
    }).finally(() => {
      if (current()) setMetricsLoading(false);
    });
    return () => {
      live = false;
      controller.abort();
      if (metricsRequestSequence.current === requestId) setMetricsLoading(false);
    };
  }, [active, ready, storageKey, base, apiKey, targetKey, evaluationMode, includeProcess, metricContext, metricsRefresh, selectedDataset?.datasetEnvelopeFp, selectedDataset?.verifier, selectedDataset?.requiresProcessMetric, identityStamp, captureIsCurrent]);

  useEffect(() => {
    if (!active || !ready || !selectedTask || !isActiveProductTask(selectedTask)) return;
    const identity = identityStamp, epoch = identityEpoch.current, key = storageKey;
    const controller = new AbortController();
    let busy = false;
    const poll = async () => {
      if (busy || controller.signal.aborted || !captureIsCurrent(identity, epoch, key)) return;
      busy = true;
      try {
        const next = await api.task(base, apiKey, selectedTask.id, controller.signal);
        if (captureIsCurrent(identity, epoch, key) && !controller.signal.aborted) {
          setSelectedTask(next);
          setTasks(old => old.map(item => item.id === next.id ? next : item));
        }
      } catch (reason) {
        if (!controller.signal.aborted && captureIsCurrent(identity, epoch, key)) setError({ identity, value: readableError(reason) });
      } finally { busy = false; }
    };
    const timer = window.setInterval(() => { void poll(); }, 1800);
    return () => { window.clearInterval(timer); controller.abort(); };
  }, [active, ready, selectedTask?.id, selectedTask?.status, storageKey, base, apiKey, identityStamp, captureIsCurrent]);

  useEffect(() => {
    if (!active || !ready || !taskId) return;
    const marker = `${identityStamp}\u0000${taskId}`;
    if (loadedTaskLink.current === marker) return;
    loadedTaskLink.current = marker;
    if (selectedTask?.id === taskId) return;
    const controller = new AbortController();
    let loaded = false;
    const identity = identityStamp, epoch = identityEpoch.current, key = storageKey;
    void api.task(base, apiKey, taskId, controller.signal).then(value => {
      if (!controller.signal.aborted && captureIsCurrent(identity, epoch, key)) {
        if (value.id !== taskId) throw new Error("评测响应标识不一致。");
        loaded = true;
        setSelectedTask(value); setReport(null); setReportPage({ offset: 0 });
      }
    }).catch(reason => {
      if (!controller.signal.aborted && captureIsCurrent(identity, epoch, key)) setError({ identity, value: readableError(reason) });
    });
    return () => {
      controller.abort();
      if (!loaded && loadedTaskLink.current === marker) loadedTaskLink.current = "";
    };
  }, [active, ready, taskId, identityStamp, storageKey, base, apiKey, captureIsCurrent]);

  useEffect(() => {
    if (active && view === "eval" && selectedTask?.id) onTaskSelection?.(selectedTask.id);
  }, [active, view, selectedTask?.id, onTaskSelection]);

  useEffect(() => {
    if (!active || view !== "datasets" || !ready || !candidateId) return;
    const marker = `${identityStamp}\u0000${candidateId}`;
    if (loadedCandidateLink.current === marker) return;
    loadedCandidateLink.current = marker;
    if (candidate?.candidate_id === candidateId) return;
    const controller = new AbortController();
    let loaded = false;
    const identity = identityStamp, epoch = identityEpoch.current, key = storageKey;
    setCandidateBusy(true); setCandidate(null);
    setCandidateInstruction(""); setCandidateInput(""); setCandidateExpected("");
    void api.candidate(base, apiKey, candidateId, controller.signal).then(value => {
      if (!controller.signal.aborted && captureIsCurrent(identity, epoch, key)) {
        if (value.candidate_id !== candidateId) throw new Error("候选响应标识不一致。");
        loaded = true;
        setCandidate(value); setCandidateInstruction(""); setCandidateInput(""); setCandidateExpected("");
        setCandidateReason("人工核对来源并补充参考结果");
      }
    }).catch(reason => {
      if (!controller.signal.aborted && captureIsCurrent(identity, epoch, key)) setError({ identity, value: readableError(reason) });
    }).finally(() => {
      if (!controller.signal.aborted && captureIsCurrent(identity, epoch, key)) setCandidateBusy(false);
    });
    return () => {
      controller.abort();
      if (!loaded && loadedCandidateLink.current === marker) loadedCandidateLink.current = "";
      if (captureIsCurrent(identity, epoch, key)) setCandidateBusy(false);
    };
  }, [active, view, ready, candidateId, identityStamp, storageKey, base, apiKey, captureIsCurrent]);

  useEffect(() => {
    if (!active || view !== "datasets" || !candidate?.candidate_id) return;
    const runId = candidates.find(item => item.candidate_id === candidate.candidate_id)?.source.run_id;
    onCandidateSelection?.(candidate.candidate_id, typeof runId === "string" ? runId : undefined);
  }, [active, view, candidate?.candidate_id, candidates, onCandidateSelection]);

  function persistLedger(next: ProductEvaluationLedger, key = storageKey): ProductEvaluationLedger {
    if (!key) throw new Error("当前身份账本尚未准备好；本次操作未发送。");
    const verified = writeProductLedger(key, next);
    setLedgerState({ storageKey: key, value: verified });
    return verified;
  }

  async function refreshLists() {
    if (!storageKey || refreshing) return;
    const identity = identityStamp, epoch = identityEpoch.current, key = storageKey;
    const controller = new AbortController();
    setRefreshing(true); setError(null);
    try {
      const [nextTargets, nextDatasets, nextTasks] = await Promise.all([
        api.targets(base, apiKey, controller.signal),
        api.datasets(base, apiKey, datasetOffset, controller.signal), api.tasks(base, apiKey, taskOffset, controller.signal),
      ]);
      if (!captureIsCurrent(identity, epoch, key)) return;
      setTargets(nextTargets); setDatasets(nextDatasets); setTasks(nextTasks); setLoadedScope(key); setMetricsContext(""); setMetricsRefresh(value => value + 1); setActionMessage("已刷新数据集、目标与评测任务。");
      if (!nextTargets.some(item => item.key === targetKey)) setTargetKey(nextTargets[0]?.key ?? "");
    } catch (reason) {
      if (captureIsCurrent(identity, epoch, key)) setError({ identity, value: readableError(reason) });
    } finally {
      if (captureIsCurrent(identity, epoch, key)) setRefreshing(false);
      controller.abort();
    }
  }

  function saveError(reason: unknown) {
    setError({ identity: identityStamp, value: readableError(reason) });
    setActionMessage("");
  }

  function addManualSample() {
    if (!canWrite) return;
    if (!instruction.trim() || !inputText.trim() || !expectedText.trim()) {
      setError({ identity: identityStamp, value: { message: "请先填写完整的任务指令、输入材料和人工参考答案。" } });
      return;
    }
    if (editingSampleId) {
      const index = sampleDrafts.findIndex(item => item.case_id === editingSampleId);
      if (index < 0) {
        setError({ identity: identityStamp, value: { message: "正在编辑的样本已不在草稿列表中；当前内容已保留，请重新加入。" } });
        setEditingSampleId("");
        return;
      }
      const next = [...sampleDrafts];
      next[index] = { case_id: editingSampleId, instruction, inputText, expected: expectedText };
      if (new Set(next.map(item => item.case_id)).size !== next.length) {
        setError({ identity: identityStamp, value: { message: "样本编号重复，无法保存修改。" } });
        return;
      }
      setSampleDrafts(next);
    } else {
      if (sampleDrafts.length >= 100) {
        setError({ identity: identityStamp, value: { message: "每个数据集最多加入 100 个样本。" } });
        return;
      }
      let caseId = `case-${createRequestKey("sample").slice(7)}`;
      while (sampleDrafts.some(item => item.case_id === caseId)) caseId = `case-${createRequestKey("sample").slice(7)}`;
      setSampleDrafts(old => [...old, { case_id: caseId, instruction, inputText, expected: expectedText }]);
    }
    setEditingSampleId(""); setInstruction(""); setInputText(""); setExpectedText(""); setError(null);
  }

  function editManualSample(item: ManualSampleDraft) {
    if (!canWrite) return;
    const formHasContent = !!(instruction || inputText || expectedText);
    if (formHasContent && editingSampleId !== item.case_id) {
      setError({ identity: identityStamp, value: { message: "当前表单内容尚未加入草稿；先加入样本或清空后再编辑。" } });
      return;
    }
    setEditingSampleId(item.case_id); setInstruction(item.instruction); setInputText(item.inputText); setExpectedText(item.expected); setError(null);
  }

  function removeManualSample(caseId: string) {
    if (!canWrite) return;
    setSampleDrafts(items => items.filter(item => item.case_id !== caseId));
    if (editingSampleId === caseId) setEditingSampleId("");
  }

  function cancelManualSampleEdit() {
    setEditingSampleId("");
    setActionMessage("编辑已取消；表单内容仍保留，可继续加入为新样本。");
  }

  async function freezeManualDataset() {
    if (!ready || !ledgerReady || !canWrite || saving) return;
    if (!/^[A-Za-z0-9_.-]{1,38}$/.test(datasetName) || !/^[A-Za-z0-9_.-]{1,64}$/.test(datasetVersion)) {
      setError({ identity: identityStamp, value: { message: "请填写有效的数据集名称和版本。" } });
      return;
    }
    const currentHasContent = !!(instruction || inputText || expectedText);
    let cases: ProductCaseSpec[];
    if (sampleDrafts.length > 0) {
      if (editingSampleId) {
        setError({ identity: identityStamp, value: { message: "请先保存或取消当前样本编辑，再冻结数据集。" } });
        return;
      }
      if (currentHasContent) {
        setError({ identity: identityStamp, value: { message: "当前表单还有未加入的样本内容；请先加入样本或清空表单。" } });
        return;
      }
      cases = sampleDrafts.map(item => ({ case_id: item.case_id, instruction: item.instruction, files: { "INPUT.txt": item.inputText }, reference: { expected: item.expected }, verifier: "krim-script/1" }));
    } else {
      if (!instruction.trim() || !inputText.trim() || !expectedText.trim()) {
        setError({ identity: identityStamp, value: { message: "请填写任务指令、输入材料和人工参考答案。" } });
        return;
      }
      cases = [{ case_id: "case-1", instruction: instruction.trim(), files: { "INPUT.txt": inputText }, reference: { expected: expectedText }, verifier: "krim-script/1" }];
    }
    let body: ReturnType<typeof buildProductDatasetRequest>;
    try { body = buildProductDatasetRequest(datasetName.trim(), datasetVersion.trim(), cases); }
    catch (reason) {
      setError({ identity: identityStamp, value: { message: reason instanceof Error ? reason.message : "数据集内容未通过预检查。" } });
      return;
    }
    const identity = identityStamp, epoch = identityEpoch.current, key = storageKey;
    setSaving(true); setError(null); setActionMessage("");
    let expectedDatasetFingerprint: string;
    try { expectedDatasetFingerprint = await productDatasetFingerprint(body.name, body.version, body.cases); }
    catch (reason) {
      if (captureIsCurrent(identity, epoch, key)) { setError({ identity, value: { message: reason instanceof Error ? reason.message : "无法核验数据集内容。" } }); setSaving(false); }
      return;
    }
    if (!captureIsCurrent(identity, epoch, key)) return;
    const operationIdentity = `${body.name}/${body.version}`;
    if (unresolvedOperation("dataset-freeze", operationIdentity)) {
      setError({ identity, value: { message: "此数据集版本的冻结结果尚未核实；请先只读检查，不会再次发送 POST。" } });
      setSaving(false);
      return;
    }
    let operation: ProductOperationIntent;
    try { operation = beginOperation("dataset-freeze", operationIdentity, { name: body.name, version: body.version, expected_dataset_fingerprint: expectedDatasetFingerprint }); }
    catch (reason) { saveError(reason); setSaving(false); return; }
    let responseReceived = false;
    try {
      const frozen = await api.freezeDataset(base, apiKey, body);
      responseReceived = true;
      if (!captureIsCurrent(identity, epoch, key)) return;
      finishOperation(operation, "found", `${body.name}/${body.version}`);
      const ref = { name: String(frozen.name), version: String(frozen.version), fingerprint: String(frozen.fingerprint) };
      setSelectedDataset({ ...ref, caseCount: cases.length, verifier: "krim-script/1", requiresProcessMetric: false });
      setSampleDrafts([]); setEditingSampleId(""); setInstruction(""); setInputText(""); setExpectedText("");
      setActionMessage(`已冻结 ${ref.name} · ${ref.version}，可以前往评测启动任务。`);
      setDatasetOffset(0);
      const rows = await api.datasets(base, apiKey, 0);
      if (captureIsCurrent(identity, epoch, key)) setDatasets(rows);
    } catch (reason) {
      if (captureIsCurrent(identity, epoch, key)) {
        if (!responseReceived) {
          const status = (reason as { status?: number }).status;
          finishOperation(operation, [401, 403, 409, 422, 429].includes(status ?? 0) ? "rejected" : "unknown");
        }
        saveError(reason);
      }
    } finally { if (captureIsCurrent(identity, epoch, key)) setSaving(false); }
  }

  async function chooseDataset(row: ProductDatasetRow) {
    if (!storageKey) return;
    const identity = identityStamp, epoch = identityEpoch.current, key = storageKey;
    setError(null); setActionMessage(""); setReport(null);
    try {
      const value = await api.dataset(base, apiKey, row.name, row.version);
      if (!captureIsCurrent(identity, epoch, key)) return;
      const cases = Array.isArray(value.cases) ? value.cases as Array<Record<string, unknown>> : [];
      const verifier = typeof cases[0]?.verifier === "string" ? String(cases[0].verifier) : "";
      const requiresProcessMetric = cases.some(item => !!item.reference && typeof item.reference === "object" && !Array.isArray(item.reference) && "process_constraints" in item.reference);
      const lineage = continuousDatasets.find(item => item.dataset.name === row.name && item.dataset.version === row.version && item.dataset.fingerprint === row.fingerprint);
      const serverEnvelope = fingerprint(value.dataset_envelope_fp);
      const envelope = serverEnvelope || lineage?.dataset_envelope_fp;
      setSelectedDataset({ name: row.name, version: row.version, fingerprint: row.fingerprint, caseCount: row.case_count, verifier, requiresProcessMetric, datasetEnvelopeFp: envelope });
      setIncludeProcess(requiresProcessMetric);
      if (serverEnvelope && ledgerReady) {
        const cached: ProductFrozenDataset = { dataset: { name: row.name, version: row.version, fingerprint: row.fingerprint }, dataset_envelope_fp: serverEnvelope, case_count: row.case_count };
        const ledger = readProductLedger(key);
        const nextLedger = { ...ledger, continuous_datasets: [...ledger.continuous_datasets.filter(item => !(item.dataset.name === row.name && item.dataset.version === row.version)), cached] };
        try { persistLedger(nextLedger, key); } catch (reason) { saveError(reason); }
      }
      setActionMessage(`已选择 ${row.name} · ${row.version}。`);
    } catch (reason) { if (captureIsCurrent(identity, epoch, key)) saveError(reason); }
  }

  async function importSourceRun() {
    const runId = importRun.trim();
    if (!ready || !ledgerReady || !canWrite || candidateBusy || !runId) return;
    const identity = identityStamp, epoch = identityEpoch.current, key = storageKey;
    if (unresolvedOperation("run-import", runId)) {
      setError({ identity, value: { message: "此运行的导入结果尚未核实；请查看来源运行，不能用新键重发。" } });
      return;
    }
    const requestKey = createRequestKey("import-run");
    let operation: ProductOperationIntent;
    try { operation = beginOperation("run-import", runId, { run_id: runId }, requestKey); }
    catch (reason) { saveError(reason); return; }
    setCandidateBusy(true); setError(null); setActionMessage(""); setCandidate(null);
    try {
      const imported = await api.importRuns(base, apiKey, { request_key: requestKey, sources: [{ run_id: runId }] });
      if (!captureIsCurrent(identity, epoch, key)) return;
      finishOperation(operation, "found", imported.map(item => item.candidate_id).join(","));
      setCandidates(imported);
      setActionMessage(imported.length ? `已导入 ${imported.length} 个候选；请逐项补充人工参考。` : "来源中没有可导入的候选。");
    } catch (reason) {
      if (!captureIsCurrent(identity, epoch, key)) return;
      const status = (reason as { status?: number }).status;
      finishOperation(operation, [401, 403, 409, 422, 429].includes(status ?? 0) ? "rejected" : "unknown");
      saveError(reason);
    } finally { if (captureIsCurrent(identity, epoch, key)) setCandidateBusy(false); }
  }

  async function openCandidate(item: ProductCandidateSummary) {
    if (!storageKey) return;
    const identity = identityStamp, epoch = identityEpoch.current, key = storageKey;
    setCandidateBusy(true); setCandidate(null); setError(null); setActionMessage("");
    try {
      const value = await api.candidate(base, apiKey, item.candidate_id);
      if (!captureIsCurrent(identity, epoch, key)) return;
      setCandidate(value);
      setCandidateInstruction(""); setCandidateInput(""); setCandidateExpected("");
      setCandidateReason("人工核对来源并补充参考结果");
    } catch (reason) { if (captureIsCurrent(identity, epoch, key)) saveError(reason); }
    finally { if (captureIsCurrent(identity, epoch, key)) setCandidateBusy(false); }
  }

  async function confirmCandidate() {
    if (!candidate || candidate.eligibility === "ineligible" || candidateBusy || !ledgerReady || !canWrite || !canApprove || !candidateInstruction.trim() || !candidateInput.trim() || !candidateExpected.trim() || !candidateReason.trim()) return;
    const identity = identityStamp, epoch = identityEpoch.current, key = storageKey;
    if (unresolvedOperation("candidate-confirm", candidate.candidate_id)) {
      setError({ identity, value: { message: "此候选的确认结果尚未核实；请先只读检查，不会换新键重发。" } });
      return;
    }
    setCandidateBusy(true); setError(null); setActionMessage("");
    const caseSpec = {
      case_id: cleanCaseId(candidate.candidate_id), instruction: candidateInstruction.trim(),
      files: { "INPUT.txt": candidateInput }, reference: { expected: candidateExpected }, verifier: "krim-script/1",
    };
    const requestKey = createRequestKey("confirm-case");
    const body = {
      action: "confirm", request_key: requestKey, expected_revision: candidate.revision, expected_hash: candidate.hash,
      case: caseSpec, context: candidate.eligibility === "needs_context" ? "manual_reconstructed" : "complete", reason: candidateReason.trim(),
    };
    let operation: ProductOperationIntent;
    try { operation = beginOperation("candidate-confirm", candidate.candidate_id, { candidate_id: candidate.candidate_id }, requestKey); }
    catch (reason) { saveError(reason); setCandidateBusy(false); return; }
    let responseReceived = false;
    try {
      const confirmation = await api.confirmCandidate(base, apiKey, candidate.candidate_id, body);
      responseReceived = true;
      if (!captureIsCurrent(identity, epoch, key)) return;
      const confirmationId = typeof confirmation.id === "string" ? confirmation.id : "";
      finishOperation(operation, "found", confirmationId);
      const next = await api.candidate(base, apiKey, candidate.candidate_id);
      if (!captureIsCurrent(identity, epoch, key)) return;
      setCandidate({ ...next, confirmation_id: confirmationId || next.confirmation_id });
      setActionMessage("人工参考已确认。确认记录已保存，下一步可冻结连续评测版本。");
    } catch (reason) {
      if (captureIsCurrent(identity, epoch, key)) {
        if (!responseReceived) {
          const status = (reason as { status?: number }).status;
          finishOperation(operation, [401, 403, 409, 422, 429].includes(status ?? 0) ? "rejected" : "unknown");
        }
        saveError(reason);
      }
    } finally { if (captureIsCurrent(identity, epoch, key)) setCandidateBusy(false); }
  }

  async function checkCandidateConfirmation() {
    if (!candidate || candidateBusy) return;
    const identity = identityStamp, epoch = identityEpoch.current, key = storageKey;
    setCandidateBusy(true); setError(null);
    try {
      const next = await api.candidate(base, apiKey, candidate.candidate_id);
      if (!captureIsCurrent(identity, epoch, key)) return;
      setCandidate(next);
      const intent = unresolvedOperation("candidate-confirm", candidate.candidate_id);
      if (next.confirmation_id && intent) finishOperation(intent, "found", next.confirmation_id);
      if (next.confirmation_id) setActionMessage("已找到原人工确认记录，可继续冻结版本。未重新发送确认请求。");
      else setActionMessage("候选尚未显示确认记录；原请求键仍保留，未重新发送任何请求。");
    } catch (reason) { if (captureIsCurrent(identity, epoch, key)) saveError(reason); }
    finally { if (captureIsCurrent(identity, epoch, key)) setCandidateBusy(false); }
  }

  async function checkOperationIntent(intent: ProductOperationIntent) {
    const identity = identityStamp, epoch = identityEpoch.current, key = storageKey;
    setCandidateBusy(true); setError(null); setActionMessage("正在只读检查原操作…");
    try {
      if (intent.kind === "run-import") {
        const requestKey = intent.request_key ?? "";
        const expectedRun = String(intent.detail.run_id ?? "");
        const receipt = await api.importReceipt(base, apiKey, requestKey);
        if (!captureIsCurrent(identity, epoch, key)) return;
        const candidates = Array.isArray(receipt.candidates) ? receipt.candidates : [];
        const matching = receipt.request_key === requestKey && candidates.every(item =>
          item.source && typeof item.source.run_id === "string" && item.source.run_id === expectedRun);
        if (!matching) {
          setActionMessage("导入回执与原请求键或来源运行不匹配；原意图仍保留，未重新发送 POST。");
          return;
        }
        finishOperation(intent, "found", candidates.map(item => item.candidate_id).join(","));
        setCandidates(candidates);
        setActionMessage(candidates.length ? `已按原请求键找回 ${candidates.length} 个候选；未重新发送 POST。` : "原请求回执确认成功，来源中没有候选；未重新发送 POST。");
        return;
      }
      if (intent.kind === "candidate-confirm") {
        const candidateId = String(intent.detail.candidate_id ?? "");
        const value = await api.candidate(base, apiKey, candidateId);
        if (!captureIsCurrent(identity, epoch, key)) return;
        setCandidate(value);
        if (value.confirmation_id) {
          finishOperation(intent, "found", value.confirmation_id);
          setActionMessage("已找到原人工确认记录；未重新发送确认请求。");
        } else {
          setActionMessage("候选仍未显示确认记录；原请求键仍保留，未重新发送请求。");
        }
        return;
      }
      const name = String(intent.detail.name ?? ""), version = String(intent.detail.version ?? "");
      const value = await api.dataset(base, apiKey, name, version);
      if (!captureIsCurrent(identity, epoch, key)) return;
      const ref = {
        name: String(value.name ?? name), version: String(value.version ?? version),
        fingerprint: String(value.fingerprint ?? ""),
      };
      if (!fingerprint(ref.fingerprint)) throw new Error("只读查询未返回有效的数据集指纹，原操作仍保留且未重新发送。");
      const envelope = fingerprint(value.dataset_envelope_fp);
      if (intent.kind === "confirmed-freeze" && !envelope) {
        setActionMessage("数据集已存在，但服务未返回连续评测 lineage；原请求键仍保留，未重新发送。");
        return;
      }
      finishOperation(intent, "found", `${ref.name}/${ref.version}`);
      if (envelope) {
        const cases = Array.isArray(value.cases) ? value.cases as Array<Record<string, unknown>> : [];
        const verifier = typeof cases[0]?.verifier === "string" ? String(cases[0].verifier) : "";
        const cached: ProductFrozenDataset = { dataset: ref, dataset_envelope_fp: envelope, case_count: cases.length };
        if (ledgerReady) {
          const ledger = readProductLedger(key);
          persistLedger({ ...ledger, continuous_datasets: [...ledger.continuous_datasets.filter(item => !(item.dataset.name === ref.name && item.dataset.version === ref.version)), cached] }, key);
        }
        setSelectedDataset({ ...ref, caseCount: cases.length, verifier, requiresProcessMetric: false, datasetEnvelopeFp: envelope });
      } else {
        const count = typeof value.cases === "object" && Array.isArray(value.cases) ? value.cases.length : 1;
        setSelectedDataset({ ...ref, caseCount: count, verifier: "krim-script/1", requiresProcessMetric: false });
      }
      setActionMessage("已按数据集名称与版本找到原冻结结果；未重新发送 POST。");
    } catch (reason) {
      if (captureIsCurrent(identity, epoch, key)) {
        const status = (reason as { status?: number }).status;
        if (intent.kind === "run-import" && status === 404) {
          setActionMessage("尚未找到原导入回执；原请求键仍保留，未重新发送 POST。");
        } else {
          setActionMessage("只读查询尚未找到可确认的结果；原请求键仍保留，未重新发送 POST。");
          saveError(reason);
        }
      }
    } finally { if (captureIsCurrent(identity, epoch, key)) setCandidateBusy(false); }
  }

  async function freezeConfirmedDataset() {
    if (!candidate?.confirmation_id || candidateBusy || !ready || !ledgerReady || !canWrite || !canApprove) return;
    const identity = identityStamp, epoch = identityEpoch.current, key = storageKey;
    const operationIdentity = `${datasetName.trim()}/${datasetVersion.trim()}`;
    if (unresolvedOperation("confirmed-freeze", operationIdentity)) {
      setError({ identity, value: { message: "此连续评测版本的冻结结果尚未核实；请先只读检查，不会换新键重发。" } });
      return;
    }
    const requestKey = createRequestKey("freeze-confirmed");
    const body = { request_key: requestKey, name: datasetName.trim(), version: datasetVersion.trim(), confirmations: [candidate.confirmation_id] };
    let operation: ProductOperationIntent;
    try { operation = beginOperation("confirmed-freeze", operationIdentity, { name: body.name, version: body.version, candidate_id: candidate.candidate_id }, requestKey); }
    catch (reason) { saveError(reason); return; }
    setCandidateBusy(true); setError(null); setActionMessage("");
    let responseReceived = false;
    try {
      const result = await api.freezeConfirmed(base, apiKey, body);
      responseReceived = true;
      if (!captureIsCurrent(identity, epoch, key)) return;
      const refValue = result.dataset as Record<string, unknown>;
      const ref = { name: String(refValue.name), version: String(refValue.version), fingerprint: String(refValue.fingerprint) };
      const envelope = fingerprint(result.dataset_envelope_fp);
      if (!envelope) {
        finishOperation(operation, "unknown");
        throw new Error("冻结响应缺少有效的 dataset_envelope_fp；请只读检查冻结结果。");
      }
      finishOperation(operation, "found", `${ref.name}/${ref.version}`);
      const frozen: ProductFrozenDataset = { dataset: ref, dataset_envelope_fp: envelope, case_count: 1 };
      const ledger = readProductLedger(key);
      const nextLedger = { ...ledger, continuous_datasets: [...ledger.continuous_datasets.filter(item => !(item.dataset.name === ref.name && item.dataset.version === ref.version)), frozen] };
      persistLedger(nextLedger, key);
      setSelectedDataset({ ...ref, caseCount: 1, verifier: "krim-script/1", requiresProcessMetric: false, datasetEnvelopeFp: envelope });
      setIncludeProcess(false);
      const rows = await api.datasets(base, apiKey, 0);
      if (captureIsCurrent(identity, epoch, key)) setDatasets(rows);
      setDatasetOffset(0);
      setActionMessage(`连续评测版本 ${ref.name} · ${ref.version} 已冻结。`);
    } catch (reason) {
      if (captureIsCurrent(identity, epoch, key)) {
        if (!responseReceived) {
          const status = (reason as { status?: number }).status;
          finishOperation(operation, [401, 403, 409, 422, 429].includes(status ?? 0) ? "rejected" : "unknown");
        }
        saveError(reason);
      }
    } finally { if (captureIsCurrent(identity, epoch, key)) setCandidateBusy(false); }
  }

  function updateIntent(intent: ProductTaskIntent, key = storageKey) {
    const ledger = readProductLedger(key);
    persistLedger(upsertTaskIntent(ledger, intent), key);
  }

  function updateOperation(intent: ProductOperationIntent, key = storageKey) {
    const ledger = readProductLedger(key);
    persistLedger(upsertOperationIntent(ledger, intent), key);
  }

  function unresolvedOperation(kind: ProductOperationIntent["kind"], identity: string): ProductOperationIntent | undefined {
    return currentLedger.operation_intents.find(item => item.kind === kind && item.state !== "found" && item.state !== "rejected"
      && String(item.detail.identity ?? "") === identity);
  }

  function beginOperation(kind: ProductOperationIntent["kind"], identity: string, detail: Record<string, unknown>, requestKey?: string): ProductOperationIntent {
    const intent: ProductOperationIntent = {
      id: requestKey ?? createRequestKey(kind), kind, state: "pending", created_at: Date.now(), request_key: requestKey,
      detail: { ...detail, identity },
    };
    updateOperation(intent);
    return intent;
  }

  function finishOperation(intent: ProductOperationIntent, state: ProductOperationIntent["state"], resultId?: string) {
    try { updateOperation({ ...intent, state, result_id: resultId }); }
    catch (reason) { saveError(reason); }
  }

  async function startEvaluation() {
    if (!ready || !ledgerReady || !canWrite || !selectedDataset || !targetKey || selectedMetrics.length === 0 || saving || metricsLoading || metricsContext !== metricContext || missingContinuousMetrics.length > 0) return;
    if (selectedMetrics.length > 6) {
      setError({ identity: identityStamp, value: { message: "每次任务最多选择 6 项指标。" } });
      return;
    }
    const requestKey = createRequestKey("eval-task");
    const continuous = isContinuousDataset(selectedDataset);
    const body: Record<string, unknown> = continuous ? {
      schema_version: "continuous-task/1", request_key: requestKey, mode: "execute",
      dataset: { name: selectedDataset.name, version: selectedDataset.version, fingerprint: selectedDataset.fingerprint },
      dataset_envelope_fp: selectedDataset.datasetEnvelopeFp, target_key: targetKey,
      metrics: selectedMetrics, execution: { repetitions }, observations: [], seed_policy: "unseeded/1",
    } : {
      request_key: requestKey,
      dataset: { name: selectedDataset.name, version: selectedDataset.version, fingerprint: selectedDataset.fingerprint },
      target_key: targetKey, metrics: selectedMetrics, execution: { repetitions },
    };
    const identity = identityStamp, epoch = identityEpoch.current, key = storageKey;
    const intent: ProductTaskIntent = { request_key: requestKey, body, created_at: Date.now(), state: "pending" };
    try { updateIntent(intent, key); }
    catch (reason) { saveError(reason); return; }
    setSaving(true); setError(null); setActionMessage("正在启动评测任务…");
    try {
      const task = await api.startTask(base, apiKey, body);
      if (!captureIsCurrent(identity, epoch, key)) return;
      try { updateIntent({ ...intent, task_id: task.id, state: "found" }, key); }
      catch (reason) { saveError(reason); }
      setSelectedTask(task); setTasks(old => [task, ...old.filter(item => item.id !== task.id)]);
      setReport(null); setReportHistory([]); setActionMessage(`评测任务已启动：${task.id}`);
    } catch (reason) {
      if (!captureIsCurrent(identity, epoch, key)) return;
      const status = (reason as { status?: number }).status;
      const code = (reason as { code?: string }).code;
      const definitelyRejected = [401, 403, 422, 429].includes(status ?? 0)
        && ["unauthorized", "permission_denied", "invalid_input", "input_limit", "capacity", "source_not_eligible"].includes(code ?? "");
      try { updateIntent({ ...intent, state: definitelyRejected ? "rejected" : "unknown" }, key); }
      catch (ledgerFailure) { saveError(ledgerFailure); return; }
      saveError(reason);
    } finally { if (captureIsCurrent(identity, epoch, key)) setSaving(false); }
  }

  async function findTaskByKey(intent: ProductTaskIntent) {
    if (!storageKey) return;
    const identity = identityStamp, epoch = identityEpoch.current, key = storageKey;
    setSaving(true); setError(null); setActionMessage("正在只读查找原评测请求…");
    try {
      const task = await api.taskByKey(base, apiKey, intent.request_key);
      if (!captureIsCurrent(identity, epoch, key)) return;
      updateIntent({ ...intent, state: "found", task_id: task.id }, key);
      setSelectedTask(task); setTasks(old => [task, ...old.filter(item => item.id !== task.id)]);
      setReport(null); setReportHistory([]); setActionMessage(`已按原键找到任务 ${task.id}。未重新发送创建请求。`);
    } catch (reason) {
      if (!captureIsCurrent(identity, epoch, key)) return;
      setActionMessage("暂时没有找到原任务；请求记录仍保留，且没有重新发送创建请求。");
      saveError(reason);
    } finally { if (captureIsCurrent(identity, epoch, key)) setSaving(false); }
  }

  async function selectTask(task: ProductTask) {
    const identity = identityStamp, epoch = identityEpoch.current, key = storageKey;
    setError(null); setActionMessage(""); setReport(null); setReportHistory([]); setReportPage({ offset: 0 });
    try {
      const detail = await api.task(base, apiKey, task.id);
      if (!captureIsCurrent(identity, epoch, key)) return;
      setSelectedTask(detail); setTasks(old => old.map(item => item.id === detail.id ? detail : item));
    } catch (reason) { if (captureIsCurrent(identity, epoch, key)) saveError(reason); }
  }

  async function cancelSelectedTask() {
    if (!canWrite || !selectedTask || !isActiveProductTask(selectedTask) || saving) return;
    const identity = identityStamp, epoch = identityEpoch.current, key = storageKey;
    setSaving(true); setError(null);
    try {
      const next = await api.cancelTask(base, apiKey, selectedTask.id);
      if (!captureIsCurrent(identity, epoch, key)) return;
      setSelectedTask(next); setTasks(old => old.map(item => item.id === next.id ? next : item));
      setActionMessage("已提交取消，并读取任务当前状态。");
    } catch (reason) { if (captureIsCurrent(identity, epoch, key)) saveError(reason); }
    finally { if (captureIsCurrent(identity, epoch, key)) setSaving(false); }
  }

  async function loadReport(page: ReportCursor = { offset: 0 }) {
    if (!selectedTask) return;
    const identity = identityStamp, epoch = identityEpoch.current, key = storageKey;
    const controller = new AbortController(); setReportLoading(true); setError(null);
    try {
      const result = await api.report(base, apiKey, selectedTask.id, page, controller.signal);
      if (!captureIsCurrent(identity, epoch, key)) return;
      const plannedCount = selectedTask.planned_count ?? taskCount(result.execution.planned_count);
      const reportedFinishedCount = taskCount(result.execution.finished_count);
      const reportComplete = result.execution.complete === true;
      const finishedCount = reportedFinishedCount ?? (reportComplete ? plannedCount : selectedTask.finished_count);
      if (plannedCount !== selectedTask.planned_count || finishedCount !== selectedTask.finished_count) {
        const normalized = { ...selectedTask, planned_count: plannedCount, finished_count: finishedCount };
        setSelectedTask(normalized);
        setTasks(old => old.map(item => item.id === normalized.id ? normalized : item));
      }
      setReport(result); setReportPage(page); setActionMessage("");
    } catch (reason) { if (captureIsCurrent(identity, epoch, key)) saveError(reason); }
    finally { if (captureIsCurrent(identity, epoch, key)) setReportLoading(false); controller.abort(); }
  }

  async function compareTasks() {
    const baseline = tasks.find(item => item.id === baselineId);
    const candidateTask = tasks.find(item => item.id === candidateTaskId);
    if (!baseline?.report_hash || !candidateTask?.report_hash || comparisonBusy) return;
    const identity = identityStamp, epoch = identityEpoch.current, key = storageKey;
    setComparisonBusy(true); setComparison(null); setError(null);
    try {
      let allowedSubjects: string[] = [];
      if (baseline.kind === candidateTask.kind) {
        const [before, after] = await Promise.all([
          api.report(base, apiKey, baseline.id, { offset: 0 }),
          api.report(base, apiKey, candidateTask.id, { offset: 0 }),
        ]);
        if (!captureIsCurrent(identity, epoch, key)) return;
        allowedSubjects = [...new Set([fingerprint(before.subject_fingerprint), fingerprint(after.subject_fingerprint)].filter(Boolean))];
        if (baseline.kind === "continuous" && !allowedSubjects.length) throw new Error("报告缺少目标比较标识，无法核对版本。");
      }
      const result = await api.compare(base, apiKey, {
        baseline_id: baseline.id, candidate_id: candidateTask.id,
        baseline_hash: baseline.report_hash, candidate_hash: candidateTask.report_hash, allowed_subjects: allowedSubjects,
      });
      if (!captureIsCurrent(identity, epoch, key)) return;
      setComparison(result);
    } catch (reason) { if (captureIsCurrent(identity, epoch, key)) saveError(reason); }
    finally { if (captureIsCurrent(identity, epoch, key)) setComparisonBusy(false); }
  }

  const unresolvedIntents = currentLedger.task_intents.filter(item => item.state === "pending" || item.state === "unknown");
  const unresolvedOperations = currentLedger.operation_intents.filter(item => item.state === "pending" || item.state === "unknown");

  return <section className="product-eval-workbench" hidden={!active} aria-label={view === "datasets" ? "评测数据集" : "工作区评测"}>
    <header className="product-eval-header">
      <div><h1>{view === "datasets" ? "数据集" : "评测"}</h1><p>{view === "datasets" ? "准备人工参考，冻结可复用的数据版本。" : "从冻结版本启动任务，查看进度与评测结果。"}</p></div>
      <Button variant="outline" disabled={!storageKey || refreshing} onClick={() => void refreshLists()}>{refreshing ? "正在刷新…" : "刷新"}</Button>
    </header>
    {!storageKey || !ready ? <>
      {currentError && <section className="product-eval-error" role="alert"><p>{currentError.message}</p>{(currentError.status || currentError.code) && <small>{[currentError.status && `HTTP ${currentError.status}`, currentError.code].filter(Boolean).join(" · ")}</small>}{currentError.payload !== undefined && <details><summary>查看原始响应</summary><pre>{JSON.stringify(currentError.payload, null, 2)}</pre></details>}</section>}
      <div className="product-eval-state" role="status">{currentError ? "工作区评测目录读取失败；修正权限后可刷新。" : scope?.identity === identityStamp ? "正在读取工作区评测目录…" : "正在确定当前身份与工作区…"}</div>
    </> : <>
      {currentError && <section className="product-eval-error" role="alert">
        <p>{currentError.message}</p>
        {(currentError.status || currentError.code) && <small>{[currentError.status && `HTTP ${currentError.status}`, currentError.code].filter(Boolean).join(" · ")}</small>}
        {currentError.payload !== undefined && <details><summary>查看原始响应</summary><pre>{JSON.stringify(currentError.payload, null, 2)}</pre></details>}
      </section>}
      {actionMessage && <p className="product-eval-message" role="status">{actionMessage}</p>}
      {view === "datasets" ? <ProductDatasets
        datasets={datasets} offset={datasetOffset} loading={catalogLoading || saving || !ledgerReady}
        operationIntents={unresolvedOperations} canWrite={canWrite && ledgerReady} canApprove={canApprove && ledgerReady} onCheckOperation={intent => void checkOperationIntent(intent)}
        name={datasetName} version={datasetVersion} instruction={instruction} inputText={inputText} expected={expectedText}
        sampleDrafts={sampleDrafts} editingSampleId={editingSampleId}
        importRun={importRun} sourceRun={sourceRun} candidates={candidates} selectedCandidate={candidate}
        candidateInstruction={candidateInstruction} candidateInput={candidateInput} candidateExpected={candidateExpected} candidateReason={candidateReason}
        candidateBusy={candidateBusy} onName={setDatasetName} onVersion={setDatasetVersion} onInstruction={setInstruction} onInput={setInputText} onExpected={setExpectedText}
        onAddSample={() => addManualSample()} onEditSample={editManualSample} onRemoveSample={removeManualSample} onCancelSampleEdit={cancelManualSampleEdit}
        onFreeze={() => void freezeManualDataset()} onOffset={setDatasetOffset} onChooseDataset={row => void chooseDataset(row)}
        onImportRun={setImportRun} onImport={() => void importSourceRun()} onOpenCandidate={item => void openCandidate(item)}
        onCandidateInstruction={setCandidateInstruction} onCandidateInput={setCandidateInput} onCandidateExpected={setCandidateExpected} onCandidateReason={setCandidateReason}
        onConfirm={() => void confirmCandidate()} onCheckConfirmation={() => void checkCandidateConfirmation()} onFreezeConfirmed={() => void freezeConfirmedDataset()}
        onRun={onRun}
      /> : <ProductEvaluation
        datasets={datasets} continuousDatasets={continuousDatasets} tasks={tasks} offsets={{ dataset: datasetOffset, task: taskOffset }}
        targets={targets} metrics={metrics} selectedDataset={selectedDataset} selectedTask={selectedTask} report={report}
        reportPage={reportPage} hasPreviousReport={reportHistory.length > 0} reportLoading={reportLoading}
        targetKey={targetKey} metricNames={metricNames} repetitions={repetitions} loading={catalogLoading || saving || metricsLoading || !ledgerReady} metricsReady={metricsReady} missingContinuousMetrics={missingContinuousMetrics}
        canWrite={canWrite && ledgerReady}
        metricMode={evaluationMode} includeProcess={includeProcess}
        baselineId={baselineId} candidateTaskId={candidateTaskId} comparison={comparison} comparisonBusy={comparisonBusy}
        intents={unresolvedIntents} onDataset={row => void chooseDataset(row)} onTarget={setTargetKey} onMetricNames={setMetricNames} onRepetitions={setRepetitions} onIncludeProcess={setIncludeProcess}
        onStart={() => void startEvaluation()} onFind={intent => void findTaskByKey(intent)} onSelectTask={task => void selectTask(task)} onCancel={() => void cancelSelectedTask()}
        onReport={() => void loadReport({ offset: 0 })} onNextReport={() => { if (report) { setReportHistory(old => [...old, reportPage]); void loadReport({ cursor: report.next_cursor, offset: report.next_offset ?? reportPage.offset + report.rows.length }); } }}
        onPreviousReport={() => { const previous = reportHistory[reportHistory.length - 1]; if (previous) { setReportHistory(old => old.slice(0, -1)); void loadReport(previous); } }}
        onOffsets={{ datasets: setDatasetOffset, tasks: setTaskOffset }}
        onBaseline={setBaselineId} onCandidateTask={setCandidateTaskId} onCompare={() => void compareTasks()}
      />}
    </>}
  </section>;
}

type ProductDatasetsProps = {
  datasets: ProductDatasetRow[]; offset: number; loading: boolean; operationIntents: ProductOperationIntent[]; canWrite: boolean; canApprove: boolean;
  name: string; version: string; instruction: string; inputText: string; expected: string;
  sampleDrafts: ManualSampleDraft[]; editingSampleId: string;
  importRun: string; sourceRun?: string; candidates: ProductCandidateSummary[]; selectedCandidate: ProductCandidate | null;
  candidateInstruction: string; candidateInput: string; candidateExpected: string; candidateReason: string; candidateBusy: boolean;
  onName: (value: string) => void; onVersion: (value: string) => void; onInstruction: (value: string) => void; onInput: (value: string) => void; onExpected: (value: string) => void;
  onAddSample: () => void; onEditSample: (sample: ManualSampleDraft) => void; onRemoveSample: (caseId: string) => void; onCancelSampleEdit: () => void;
  onFreeze: () => void; onOffset: (value: number) => void; onChooseDataset: (row: ProductDatasetRow) => void; onImportRun: (value: string) => void;
  onImport: () => void; onOpenCandidate: (item: ProductCandidateSummary) => void; onCandidateInstruction: (value: string) => void; onCandidateInput: (value: string) => void;
  onCandidateExpected: (value: string) => void; onCandidateReason: (value: string) => void; onConfirm: () => void; onCheckConfirmation: () => void; onFreezeConfirmed: () => void;
  onCheckOperation: (intent: ProductOperationIntent) => void; onRun?: (id: string) => void;
};

function ProductDatasets(props: ProductDatasetsProps) {
  const currentCandidate = props.selectedCandidate;
  const sourceId = typeof currentCandidate?.candidate_id === "string" ? currentCandidate.candidate_id : "";
  const candidateSummary = sourceId ? props.candidates.find(item => item.candidate_id === sourceId) : undefined;
  const candidateSourceRun = candidateSummary?.source && typeof candidateSummary.source.run_id === "string" ? candidateSummary.source.run_id : props.sourceRun;
  const datasetIdentity = `${props.name}/${props.version}`;
  const manualFreezePending = props.operationIntents.some(item => item.kind === "dataset-freeze" && item.detail.identity === datasetIdentity);
  const importPending = props.operationIntents.some(item => item.kind === "run-import" && item.detail.identity === props.importRun.trim());
  const confirmationPending = !!sourceId && props.operationIntents.some(item => item.kind === "candidate-confirm" && item.detail.identity === sourceId);
  const confirmedFreezePending = props.operationIntents.some(item => item.kind === "confirmed-freeze" && item.detail.identity === datasetIdentity);
  const currentFormHasContent = !!(props.instruction || props.inputText || props.expected);
  const currentFormComplete = !!(props.instruction.trim() && props.inputText.trim() && props.expected.trim());
  const hasUnjoinedSample = props.sampleDrafts.length > 0 && (props.editingSampleId !== "" || currentFormHasContent);
  const canFreezeManual = props.sampleDrafts.length > 0 ? !hasUnjoinedSample : currentFormComplete;
  return <div className="product-eval-columns">
    <div className="product-eval-main-column">
      {!props.canWrite ? <p className="product-eval-inline-warning">当前身份只能查看工作区数据，无法修改。</p> : !props.canApprove ? <p className="product-eval-muted">候选确认和连续版本冻结需要审批权限；数据集维护仍可用。</p> : null}
      <section className="product-eval-section" aria-labelledby="product-dataset-list-heading">
        <div className="product-eval-section-heading"><div><h2 id="product-dataset-list-heading">已冻结的数据集</h2><p>选择已有版本可直接用于普通评测。</p></div></div>
        {props.datasets.length ? <div className="product-eval-table-wrap"><table className="product-eval-table">
          <caption className="sr-only">已冻结的数据集版本</caption>
          <thead><tr><th scope="col">名称</th><th scope="col">版本</th><th scope="col">样本</th><th scope="col">操作</th></tr></thead>
          <tbody>{props.datasets.map(row => <tr className="product-eval-list-row" key={`${row.name}/${row.version}`}>
            <td><strong>{row.name}</strong><details className="product-eval-record-meta"><summary>版本指纹</summary><code>{row.fingerprint}</code></details></td>
            <td>{row.version}</td><td>{row.case_count}</td>
            <td><Button variant="outline" size="sm" onClick={() => props.onChooseDataset(row)}>用于评测</Button></td>
          </tr>)}</tbody>
        </table></div> : <div className="product-eval-empty"><strong>还没有冻结版本</strong><p>新建样本，或从已有运行导入候选。</p></div>}
        <ProductPagination offset={props.offset} count={props.datasets.length} limit={20} onChange={props.onOffset} label="数据集" />
      </section>
      <details className="product-eval-editor">
        <summary>新建数据集</summary>
      <section className="product-eval-section" aria-labelledby="product-dataset-create-heading">
        <div className="product-eval-section-heading"><div><h2 id="product-dataset-create-heading">新建 Krim Script 数据集</h2><p>参考答案由你填写；来源输出不会自动成为正确答案。</p></div></div>
        <div className="product-eval-form-grid">
          <div><Label htmlFor="product-dataset-name">名称</Label><Input id="product-dataset-name" value={props.name} maxLength={38} onChange={event => props.onName(event.target.value)} /></div>
          <div><Label htmlFor="product-dataset-version">版本</Label><Input id="product-dataset-version" value={props.version} maxLength={64} onChange={event => props.onVersion(event.target.value)} /></div>
        </div>
        <div className="product-eval-field"><Label htmlFor="product-instruction">任务指令</Label><Textarea id="product-instruction" value={props.instruction} maxLength={4096} rows={3} onChange={event => props.onInstruction(event.target.value)} placeholder="描述脚本应完成的任务" /></div>
        <div className="product-eval-field"><Label htmlFor="product-input-file">输入材料（INPUT.txt）</Label><Textarea id="product-input-file" value={props.inputText} rows={4} onChange={event => props.onInput(event.target.value)} placeholder="写入评测样本的初始文件内容" /></div>
        <div className="product-eval-field"><Label htmlFor="product-expected">人工参考答案</Label><Textarea id="product-expected" value={props.expected} rows={3} onChange={event => props.onExpected(event.target.value)} placeholder="逐字填写人工参考结果" /></div>
        <div className="product-eval-actions">
          <Button variant="outline" disabled={!props.canWrite || props.loading || props.sampleDrafts.length >= 100} onClick={props.onAddSample}>{props.editingSampleId ? "保存样本修改" : "加入样本"}</Button>
          {props.editingSampleId && <Button variant="ghost" onClick={props.onCancelSampleEdit}>取消编辑</Button>}
          <span className="product-eval-muted">样本 {props.sampleDrafts.length}/100</span>
        </div>
        {props.sampleDrafts.length > 0 && <ul className="product-eval-candidates" aria-label="样本草稿">
          {props.sampleDrafts.map((item, index) => {
            const isEditing = props.editingSampleId === item.case_id;
            return <li key={item.case_id}><div><strong>样本 {index + 1}{isEditing ? " · 编辑中" : ""}</strong><small>{item.instruction.slice(0, 80)} · 参考：{item.expected.slice(0, 60)}</small><details><summary>样本编号</summary><code>{item.case_id}</code></details></div>
              <div className="product-eval-actions"><Button size="sm" variant="outline" disabled={!props.canWrite || props.loading || isEditing || props.editingSampleId !== "" || currentFormHasContent} onClick={() => props.onEditSample(item)}>{isEditing ? "正在编辑" : "回填编辑"}</Button><Button size="sm" variant="ghost" disabled={!props.canWrite || props.loading} onClick={() => props.onRemoveSample(item.case_id)}>移除</Button></div>
            </li>;
          })}
        </ul>}
        {hasUnjoinedSample && <p className="product-eval-inline-warning">当前表单还有未加入的样本内容；先加入样本或清空表单，避免冻结时丢失。</p>}
        <div className="product-eval-form-foot"><span>{props.sampleDrafts.length ? "冻结草稿列表中的样本；整个请求上限 64 KiB。" : "未加入草稿时可直接冻结当前单样本；整个请求上限 64 KiB。"}</span><Button disabled={props.loading || !props.canWrite || manualFreezePending || !canFreezeManual} onClick={props.onFreeze}>{props.loading ? "正在保存…" : manualFreezePending ? "等待只读核实" : props.sampleDrafts.length ? `冻结 ${props.sampleDrafts.length} 个样本` : "冻结数据集版本"}</Button></div>
      </section>
      </details>
    </div>
    <div className="product-eval-side-column">
      <details className="product-eval-editor" open={Boolean(props.sourceRun) || props.candidates.length > 0}>
        <summary>导入运行候选</summary>
      <section className="product-eval-section" aria-labelledby="product-source-heading">
        <div className="product-eval-section-heading"><div><h2 id="product-source-heading">从运行记录导入候选</h2><p>先检查资格，再补充人工参考。</p></div></div>
        {props.sourceRun && <p className="product-eval-source-context">来源运行：<code>{props.sourceRun}</code>{props.onRun && <Button variant="link" size="sm" onClick={() => props.onRun?.(props.sourceRun!)}>打开运行记录</Button>}</p>}
        <div className="product-eval-field"><Label htmlFor="product-source-run">运行 ID</Label><Input id="product-source-run" value={props.importRun} onChange={event => props.onImportRun(event.target.value)} placeholder="粘贴 Krim Agent 运行 ID" /></div>
        <Button variant="outline" disabled={props.loading || !props.canWrite || props.candidateBusy || !props.importRun.trim() || importPending} onClick={props.onImport}>{props.candidateBusy ? "处理中…" : importPending ? "等待只读核实" : "导入候选"}</Button>
        {props.candidates.length > 0 && <ul className="product-eval-candidates" aria-label="导入的候选">
          {props.candidates.map(item => <li key={item.candidate_id}>
            <div><strong>{eligibilityLabel(item.eligibility)}</strong><small>{item.reasons.length ? `${item.reasons.length} 条资格说明` : "来源资格已返回"}</small><details><summary>来源详情</summary><pre>{JSON.stringify({ candidate_id: item.candidate_id, reasons: item.reasons, source: item.source }, null, 2)}</pre></details></div>
            <Button variant="outline" size="sm" disabled={props.candidateBusy} onClick={() => props.onOpenCandidate(item)}>补充参考</Button>
          </li>)}
        </ul>}
      </section>
      </details>
      {props.operationIntents.length > 0 && <section className="product-eval-section" aria-labelledby="product-operation-recovery-heading">
        <div className="product-eval-section-heading"><div><h2 id="product-operation-recovery-heading">待核实的原操作</h2><p>未知回程只读取原对象，不会重新发送 POST。</p></div></div>
        <ul className="product-eval-recovery-list">{props.operationIntents.map(item => {
          const runId = typeof item.detail.run_id === "string" ? item.detail.run_id : "";
          const label = item.kind === "dataset-freeze" ? "数据集冻结" : item.kind === "run-import" ? "候选导入" : item.kind === "candidate-confirm" ? "人工参考确认" : "连续版本冻结";
          const key = item.request_key ?? String(item.detail.identity ?? item.id);
          return <li key={item.id}><div><strong>{label} · {item.state === "pending" ? "等待确认" : "结果未确认"}</strong><small>{item.kind === "run-import" ? "按原请求键只读核对候选回执。" : "可通过 GET 检查原对象。"}</small><code>{key}</code></div>
            <Button size="sm" variant="outline" disabled={props.candidateBusy} onClick={() => props.onCheckOperation(item)}>只读检查</Button>
            {item.kind === "run-import" && runId && props.onRun && <Button size="sm" variant="link" onClick={() => props.onRun?.(runId)}>查看来源运行</Button>}
          </li>;
        })}</ul>
      </section>}
      {currentCandidate && <section className="product-eval-section product-eval-candidate-editor" aria-labelledby="product-candidate-heading">
        <div className="product-eval-section-heading"><div><h2 id="product-candidate-heading">人工确认候选</h2><p>候选资格：{eligibilityLabel(currentCandidate.eligibility)}</p></div></div>
        <div className="product-eval-form-grid">
          <div><Label htmlFor="product-confirmed-dataset-name">冻结数据集名称</Label><Input id="product-confirmed-dataset-name" value={props.name} maxLength={38} onChange={event => props.onName(event.target.value)} /></div>
          <div><Label htmlFor="product-confirmed-dataset-version">冻结版本</Label><Input id="product-confirmed-dataset-version" value={props.version} maxLength={64} onChange={event => props.onVersion(event.target.value)} /></div>
        </div>
        <p className="product-eval-candidate-note">评测输出只作为来源线索。任务指令、输入材料和参考答案均由你核对填写。</p>
        {candidateSourceRun && <p className="product-eval-source-context">来源运行：<code>{candidateSourceRun}</code></p>}
        <div className="product-eval-field"><Label htmlFor="product-candidate-instruction">任务指令</Label><Textarea id="product-candidate-instruction" value={props.candidateInstruction} rows={3} onChange={event => props.onCandidateInstruction(event.target.value)} /></div>
        <div className="product-eval-field"><Label htmlFor="product-candidate-input">输入材料（INPUT.txt）</Label><Textarea id="product-candidate-input" value={props.candidateInput} rows={3} onChange={event => props.onCandidateInput(event.target.value)} /></div>
        <div className="product-eval-field"><Label htmlFor="product-candidate-expected">人工参考答案</Label><Textarea id="product-candidate-expected" value={props.candidateExpected} rows={3} onChange={event => props.onCandidateExpected(event.target.value)} /></div>
        <div className="product-eval-field"><Label htmlFor="product-candidate-reason">确认理由</Label><Input id="product-candidate-reason" value={props.candidateReason} maxLength={512} onChange={event => props.onCandidateReason(event.target.value)} /></div>
        <div className="product-eval-actions">
          {!currentCandidate.confirmation_id && <Button disabled={props.candidateBusy || !props.canWrite || !props.canApprove || confirmationPending || currentCandidate.eligibility === "ineligible" || !props.candidateInstruction.trim() || !props.candidateInput.trim() || !props.candidateExpected.trim() || !props.candidateReason.trim()} onClick={props.onConfirm}>{confirmationPending ? "等待只读核实" : "确认人工参考"}</Button>}
          {currentCandidate.confirmation_id && <Button disabled={props.candidateBusy || !props.canWrite || !props.canApprove || confirmedFreezePending || !/^[A-Za-z0-9_.-]{1,38}$/.test(props.name) || !/^[A-Za-z0-9_.-]{1,64}$/.test(props.version)} onClick={props.onFreezeConfirmed}>{confirmedFreezePending ? "等待只读核实" : "冻结连续评测版本"}</Button>}
          <Button variant="outline" disabled={props.candidateBusy} onClick={props.onCheckConfirmation}>只读检查确认状态</Button>
        </div>
        {currentCandidate.eligibility === "ineligible" && <p className="product-eval-inline-warning">该候选不符合确认条件，不能冻结为评测样本。</p>}
      </section>}
    </div>
  </div>;
}

function eligibilityLabel(value: string): string {
  if (value === "eligible") return "可确认";
  if (value === "needs_context") return "需要补全上下文";
  if (value === "ineligible") return "不符合条件";
  return "资格待确认";
}

type ProductEvaluationProps = {
  datasets: ProductDatasetRow[]; continuousDatasets: ProductFrozenDataset[]; tasks: ProductTask[]; offsets: { dataset: number; task: number };
  targets: ProductTarget[]; metrics: ProductMetric[]; selectedDataset: SelectedDataset | null; selectedTask: ProductTask | null; report: ProductReportPage | null;
  reportPage: ReportCursor; hasPreviousReport: boolean; reportLoading: boolean; targetKey: string; metricNames: string[]; repetitions: number; loading: boolean; metricsReady: boolean; missingContinuousMetrics: string[];
  metricMode: "manual" | "continuous"; includeProcess: boolean; canWrite: boolean;
  baselineId: string; candidateTaskId: string; comparison: Record<string, unknown> | null; comparisonBusy: boolean; intents: ProductTaskIntent[];
  onDataset: (row: ProductDatasetRow) => void; onTarget: (key: string) => void; onMetricNames: (names: string[]) => void; onRepetitions: (value: number) => void; onIncludeProcess: (value: boolean) => void;
  onStart: () => void; onFind: (intent: ProductTaskIntent) => void; onSelectTask: (task: ProductTask) => void; onCancel: () => void; onReport: () => void;
  onNextReport: () => void; onPreviousReport: () => void; onOffsets: { datasets: (value: number) => void; tasks: (value: number) => void };
  onBaseline: (id: string) => void; onCandidateTask: (id: string) => void; onCompare: () => void;
};

function ProductEvaluation(props: ProductEvaluationProps) {
  const options = useMemo(() => {
    const saved = props.continuousDatasets.map(item => ({ ...item.dataset, case_count: item.case_count ?? 1, continuous: true }));
    const seen = new Set(saved.map(item => `${item.name}/${item.version}`));
    return [...saved, ...props.datasets.filter(row => !seen.has(`${row.name}/${row.version}`)).map(row => ({ ...row, continuous: false }))];
  }, [props.continuousDatasets, props.datasets]);
  const eligibleTasks = props.tasks.filter(task => !!task.report_hash);
  const canStart = props.canWrite && props.metricsReady && !!props.selectedDataset && !!props.targetKey && props.metricNames.length > 0 && props.metricNames.length <= 6 && !props.loading;
  return <div className="product-eval-columns product-eval-eval-columns">
    <div className="product-eval-main-column">
      {!props.canWrite && <p className="product-eval-inline-warning">当前身份只能查看评测结果；启动和取消任务需要写入权限。</p>}
      <section className="product-eval-section" aria-labelledby="product-task-list-heading">
        <div className="product-eval-section-heading"><div><h2 id="product-task-list-heading">评测任务</h2><p>选择一次运行，查看样本与评分。</p></div></div>
        {props.tasks.length ? <div className="product-eval-table-wrap product-eval-task-list"><table className="product-eval-table">
          <caption className="sr-only">当前工作区评测任务</caption>
          <thead><tr><th scope="col">数据集与版本</th><th scope="col">目标</th><th scope="col">状态</th><th scope="col">完成</th></tr></thead>
          <tbody>{props.tasks.map(task => <tr className={props.selectedTask?.id === task.id ? "is-selected" : undefined} key={task.id}>
            <td><button type="button" className="product-eval-task-row" aria-pressed={props.selectedTask?.id === task.id} onClick={() => props.onSelectTask(task)}>
              <span><strong>{task.dataset.name} · {task.dataset.version}</strong><small>{task.metrics.map(metric => metricLabel(metric.name)).join("、")}</small></span>
              <span className="sr-only">{taskStateLabel[task.status] ?? task.status} {task.finished_count ?? "—"}/{task.planned_count ?? "—"}</span>
            </button></td>
            <td>{task.target_key}</td><td>{taskStateLabel[task.status] ?? task.status}</td><td>{task.finished_count ?? "—"}/{task.planned_count ?? "—"}</td>
          </tr>)}</tbody>
        </table></div> : <div className="product-eval-empty"><strong>还没有评测任务</strong><p>新建评测，选择冻结数据集与目标。</p></div>}
        <ProductPagination offset={props.offsets.task} count={props.tasks.length} limit={20} onChange={props.onOffsets.tasks} label="任务" />
      </section>
      {props.selectedTask && <ProductTaskDetail task={props.selectedTask} canWrite={props.canWrite} onCancel={props.onCancel} onReport={props.onReport} loading={props.loading} reportLoading={props.reportLoading} report={props.report} reportPage={props.reportPage} hasPrevious={props.hasPreviousReport} onPrevious={props.onPreviousReport} onNext={props.onNextReport} />}
      <details className="product-eval-editor">
        <summary>新建评测</summary>
      <section className="product-eval-section" aria-labelledby="product-launch-heading">
        <div className="product-eval-section-heading"><div><h2 id="product-launch-heading">启动评测</h2><p>先选冻结版本，再选择目录中可用的目标与指标。</p></div></div>
        {options.length ? <div className="product-eval-field"><Label htmlFor="product-dataset-choice">数据集版本</Label><select id="product-dataset-choice" value={props.selectedDataset ? `${props.selectedDataset.name}/${props.selectedDataset.version}` : ""} onChange={event => {
          const row = props.datasets.find(item => `${item.name}/${item.version}` === event.target.value);
          const frozen = props.continuousDatasets.find(item => `${item.dataset.name}/${item.dataset.version}` === event.target.value);
          if (row) props.onDataset(row);
          else if (frozen) props.onDataset({ ...frozen.dataset, case_count: frozen.case_count ?? 1 });
        }}><option value="">选择数据集</option>{options.map(item => <option key={`${item.name}/${item.version}`} value={`${item.name}/${item.version}`}>{item.name} · {item.version}{item.continuous ? " · 连续评测" : ""}</option>)}</select>
          {props.selectedDataset && <p className="product-eval-selected-meta">{props.selectedDataset.caseCount} 个样本 · {props.selectedDataset.datasetEnvelopeFp ? "已包含人工确认记录，可生成分页报告" : "普通数据集"}</p>}
        </div> : <div className="product-eval-empty"><strong>还没有可用数据集</strong><p>先到“数据集”页冻结一个版本。</p></div>}
        <div className="product-eval-form-grid">
          <div className="product-eval-field"><Label htmlFor="product-target">评测目标</Label><select id="product-target" value={props.targetKey} onChange={event => props.onTarget(event.target.value)} disabled={!props.targets.length}>{props.targets.map(target => <option key={target.key} value={target.key}>{targetText(target)}</option>)}</select></div>
          <div className="product-eval-field"><Label htmlFor="product-repetitions">重复次数</Label><Input id="product-repetitions" type="number" min={1} max={3} value={props.repetitions} onChange={event => { const next = Number(event.target.value); if (Number.isInteger(next) && next >= 1 && next <= 3) props.onRepetitions(next); }} /></div>
        </div>
        <fieldset className="product-eval-metrics"><legend>指标目录</legend>
          {props.metricMode === "continuous" && <label className="product-eval-metric-choice"><input type="checkbox" checked={props.includeProcess} onChange={event => props.onIncludeProcess(event.target.checked)} /><span><strong>包含进程约束指标</strong><small>按工作区指标目录刷新可选项</small></span></label>}
          {props.metrics.length ? props.metrics.map(metric => <label key={metric.name} className="product-eval-metric-choice">
            <input type="checkbox" checked={props.metricNames.includes(metric.name)} onChange={event => props.onMetricNames(event.target.checked ? [...props.metricNames, metric.name] : props.metricNames.filter(name => name !== metric.name))} />
            <span><strong>{metricText(metric)}</strong><small>{metric.version} · {metric.hard ? "质量指标" : "诊断指标"}</small></span>
          </label>) : <p className="product-eval-muted">当前目录没有可用指标。</p>}
        </fieldset>
        {props.selectedDataset?.datasetEnvelopeFp && <p className="product-eval-inline-warning">连续评测需要：{["fact_integrity", "context_coverage", ...(props.selectedDataset.verifier === "krim-script/1" ? ["krim_result"] : []), ...(props.selectedDataset.requiresProcessMetric ? ["process_constraints"] : [])].map(metricLabel).join("、")}。{props.missingContinuousMetrics.length ? `还需选择 ${props.missingContinuousMetrics.map(metricLabel).join("、")}。` : "必需指标已选。"}</p>}
        <Button disabled={!canStart || props.missingContinuousMetrics.length > 0} onClick={props.onStart}>{props.loading ? "正在处理…" : "启动评测"}</Button>
      </section>
      </details>
      {props.intents.length > 0 && <section className="product-eval-section" aria-labelledby="product-recovery-heading">
        <div className="product-eval-section-heading"><div><h2 id="product-recovery-heading">待查找的原请求</h2><p>仅按原键读取，不会再次提交创建请求。</p></div></div>
        <ul className="product-eval-recovery-list">{props.intents.map(intent => <li key={intent.request_key}><div><code>{intent.request_key}</code><small>{intent.state === "unknown" ? "创建结果未确认" : "尚未找到任务"}</small></div><Button variant="outline" size="sm" disabled={props.loading} onClick={() => props.onFind(intent)}>只读查找</Button></li>)}</ul>
      </section>}
    </div>
    <aside className="product-eval-side-column">
      <section className="product-eval-section" aria-labelledby="product-compare-heading">
        <div className="product-eval-section-heading"><div><h2 id="product-compare-heading">基线比较</h2><p>比较两次评测结果；超大连续报告按分页策略核对。</p></div></div>
        <div className="product-eval-field"><Label htmlFor="product-baseline">基线任务</Label><select id="product-baseline" value={props.baselineId} onChange={event => props.onBaseline(event.target.value)}><option value="">选择基线</option>{eligibleTasks.map(task => <option key={task.id} value={task.id}>{task.dataset.name} · {task.id.slice(0, 12)}</option>)}</select></div>
        <div className="product-eval-field"><Label htmlFor="product-candidate-task">候选任务</Label><select id="product-candidate-task" value={props.candidateTaskId} onChange={event => props.onCandidateTask(event.target.value)}><option value="">选择候选</option>{eligibleTasks.map(task => <option key={task.id} value={task.id}>{task.dataset.name} · {task.id.slice(0, 12)}</option>)}</select></div>
        <p className="product-eval-muted">比较会核对目标与评测条件；不满足比较条件时会显示原因。</p>
        <Button variant="outline" disabled={props.comparisonBusy || !props.baselineId || !props.candidateTaskId || props.baselineId === props.candidateTaskId} onClick={props.onCompare}>{props.comparisonBusy ? "正在比较…" : "比较基线与候选"}</Button>
        {props.comparison && <ComparisonSummary value={props.comparison} />}
      </section>
      <section className="product-eval-section" aria-labelledby="product-catalog-heading">
        <div className="product-eval-section-heading"><div><h2 id="product-catalog-heading">当前目录</h2><p>目标与指标均来自工作区评测 API。</p></div></div>
        <dl className="product-eval-catalog-summary"><dt>可用目标</dt><dd>{props.targets.length}</dd><dt>可用指标</dt><dd>{props.metrics.length}</dd><dt>已选数据集</dt><dd>{props.selectedDataset?.name ?? "未选择"}</dd></dl>
      </section>
    </aside>
  </div>;
}

function ProductTaskDetail({ task, canWrite, onCancel, onReport, loading, reportLoading, report, reportPage, hasPrevious, onPrevious, onNext }: {
  task: ProductTask; canWrite: boolean; onCancel: () => void; onReport: () => void; loading: boolean; reportLoading: boolean; report: ProductReportPage | null;
  reportPage: ReportCursor; hasPrevious: boolean; onPrevious: () => void; onNext: () => void;
}) {
  const complete = task.status === "completed" || task.status === "failed" || task.status === "cancelled" || task.status === "interrupted";
  const progressKnown = task.finished_count !== null && task.planned_count !== null;
  const countText = `${task.finished_count ?? "—"} / ${task.planned_count ?? "—"}`;
  return <section className="product-eval-section" aria-labelledby="product-task-detail-heading">
    <div className="product-eval-section-heading"><div><h2 id="product-task-detail-heading">任务详情</h2><p>{taskStateLabel[task.status] ?? task.status} · {countText} 项完成</p></div></div>
    {progressKnown ? <div className="product-eval-progress" role="progressbar" aria-label="评测任务进度" aria-valuemin={0} aria-valuemax={Math.max(task.planned_count ?? 1, 1)} aria-valuenow={task.finished_count ?? 0}><span style={{ width: `${task.planned_count ? Math.min(100, (task.finished_count ?? 0) / task.planned_count * 100) : 0}%` }} /></div>
      : <p className="product-eval-muted" role="status">任务响应未提供完成计数；可查看报告核对真实完成情况。</p>}
    <dl className="product-eval-task-facts"><dt>任务 ID</dt><dd><code>{task.id}</code></dd><dt>原请求键</dt><dd><code>{task.request_key}</code></dd><dt>数据集版本</dt><dd>{task.dataset.name} · {task.dataset.version}</dd><dt>评测目标</dt><dd>{task.target_key}</dd>{task.code && <><dt>状态代码</dt><dd>{task.code}</dd></>}</dl>
    <div className="product-eval-actions">
      {isActiveProductTask(task) && <Button variant="destructive" disabled={!canWrite || loading} onClick={onCancel}>取消任务</Button>}
      {complete && <Button variant="outline" disabled={reportLoading} onClick={onReport}>{reportLoading ? "正在读取…" : "查看报告"}</Button>}
    </div>
    {report && <ProductReport report={report} reportPage={reportPage} hasPrevious={hasPrevious} loading={reportLoading} onPrevious={onPrevious} onNext={onNext} />}
  </section>;
}

function ProductReport({ report, reportPage, hasPrevious, loading, onPrevious, onNext }: {
  report: ProductReportPage; reportPage: ReportCursor; hasPrevious: boolean; loading: boolean; onPrevious: () => void; onNext: () => void;
}) {
  const rawExecutionStatus = String(report.execution.status ?? "未知");
  const executionStatus = taskStateLabel[rawExecutionStatus] ?? qualityLabel(rawExecutionStatus);
  const quality = report.quality;
  return <section className="product-eval-report" aria-label="评测报告">
    <div className="product-eval-section-heading"><div><h3>评测报告</h3><p>{executionStatus} · 报告修订 {report.revision ?? "—"}</p></div></div>
    <div className="product-eval-report-summary">
      <div><small>覆盖率</small><strong>{formatPercent(report.coverage.cell_coverage)}</strong></div>
      <div><small>通过率</small><strong>{formatPercent(quality.pass_rate)}</strong></div>
      <div><small>已评分</small><strong>{String(report.coverage.scored_cells ?? "—")} / {String(report.coverage.planned_cells ?? "—")}</strong></div>
    </div>
    <div className="product-eval-report-rows">{report.rows.map(row => <article className={`product-eval-report-row${row.quality && row.quality !== "passed" ? " is-failed" : ""}`} key={`${row.case_id}-${row.repetition}`}>
      <header><strong>{row.case_id}</strong><span>第 {row.repetition + 1} 次</span><span>{qualityLabel(row.quality ?? row.status)}</span></header>
      {row.code && <p className="product-eval-inline-warning">{row.code}</p>}
      {row.scores?.length ? <ul>{row.scores.map((score, index) => <li key={`${String(score.rule ?? score.name ?? index)}-${index}`}><span>{scoreName(score)}</span><strong>{scoreValue(score)}</strong>{typeof score.reason === "string" && <small>{score.reason}</small>}</li>)}</ul> : <p className="product-eval-muted">没有返回逐指标评分。</p>}
      <details><summary>查看样本状态 JSON</summary><pre>{JSON.stringify(row, null, 2)}</pre></details>
    </article>)}</div>
    <div className="product-eval-pagination"><Button size="sm" variant="outline" disabled={!hasPrevious || loading} onClick={onPrevious}>上一页</Button><span>偏移 {reportPage.offset}</span><Button size="sm" variant="outline" disabled={!report.next_cursor && report.next_offset === null || loading} onClick={onNext}>下一页</Button></div>
  </section>;
}

function scoreName(score: Record<string, unknown>): string {
  const rule = score.rule && typeof score.rule === "object" ? score.rule as Record<string, unknown> : null;
  return metricLabel(String(rule?.name ?? score.name ?? "评分项"));
}
function scoreValue(score: Record<string, unknown>): string {
  if (typeof score.value === "number") return String(score.value);
  return String(score.status ?? score.state ?? "未评分");
}
function qualityLabel(value: string): string {
  const labels: Record<string, string> = { passed: "通过", failed: "失败", unknown: "未知", skipped: "跳过", partial: "部分完成", scored: "已评分" };
  return labels[value] ?? value;
}
function formatPercent(value: unknown): string {
  return typeof value === "number" && Number.isFinite(value) ? `${Math.round(value * 100)}%` : "—";
}

function ComparisonSummary({ value }: { value: Record<string, unknown> }) {
  const status = String(value.status ?? "unknown");
  const reasons = Array.isArray(value.reasons) ? value.reasons : [];
  return <div className={`product-eval-comparison product-eval-comparison-${status}`} role="status">
    <strong>{status === "pass" ? "比较通过" : status === "invalid" ? "当前不可比较" : status === "regression" ? "发现退化" : status === "fail" ? "比较未通过" : status}</strong>
    {reasons.length > 0 && <ul>{reasons.slice(0, 8).map((item, index) => {
      const reason: Record<string, unknown> = item && typeof item === "object" ? item as Record<string, unknown> : { code: item };
      return <li key={`${String(reason.code ?? "reason")}-${index}`}>{String(reason.code ?? "比较结果")} {reason.case_id ? `· ${String(reason.case_id)}` : ""}{reason.rule ? ` · ${String(reason.rule)}` : ""}</li>;
    })}</ul>}
    <details><summary>查看比较响应</summary><pre>{JSON.stringify(value, null, 2)}</pre></details>
  </div>;
}

function ProductPagination({ offset, count, limit, onChange, label }: { offset: number; count: number; limit: number; onChange: (value: number) => void; label: string }) {
  return <div className="product-eval-pagination" aria-label={`${label}分页`}>
    <Button size="sm" variant="outline" disabled={offset === 0} onClick={() => onChange(Math.max(0, offset - limit))}>上一页</Button>
    <span>{offset + 1}–{offset + count}</span>
    <Button size="sm" variant="outline" disabled={count < limit} onClick={() => onChange(offset + limit)}>下一页</Button>
  </div>;
}
