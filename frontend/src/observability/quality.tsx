import { useEffect, useRef, useState } from "react";
import {
  createRequestKey,
  evaluationBase,
  productErrorMessage,
  productEvaluationApi as api,
  type ProductCandidate,
  type ProductCaseSpec,
  type ProductEvaluationError,
  type ProductImportReceipt,
  type ProductReportPage,
  type ProductReportRow,
  type ProductTask,
} from "../lib/product-evaluation";
import type { Actor } from "../lib/session-contracts";
import type { WorkbenchView } from "../lib/workbench-route";
import { formatCount, formatTime } from "./obs-format";

/** 观测质量页：评测分析与人工复核，读取既有 v4 工作区评测服务。
 * 评测页只读展示任务/数据集/报告，比较复用现有只读 compare 契约（服务只读报告，不调度模型）；
 * 复核页按候选 ID 或导入回执键显式读取既有候选，确认走既有 confirm 服务，
 * 不新建队列存储、不放假评分。所有写入按工作区 membership 授权：viewer 只读，确认需 approve。 */

export interface ObservabilityQualityProps {
  apiKey: string;
  workspace: string;
  identity: Actor | null;
  onNavigate: (view: WorkbenchView) => void;
}

type QualityError = { message: string; payload?: unknown; status?: number | null; code?: string };
type ReportCursor = { cursor?: string | null; offset: number };
type ConfirmAction = "confirm" | "edit" | "reject";
type ConfirmContext = "complete" | "manual_reconstructed";

const hex64 = /^[0-9a-f]{64}$/;
const identityPattern = /^[A-Za-z0-9_.-]{1,64}$/;
const caseVerifiers = ["reconciliation/1", "repository-repair/1", "krim-script/1", "agent-bench/1"];
const CASE_TEXT_BYTES = 65536;
const REASON_BYTES = 512;
const PAGE_SIZE = 20;

/** 工作区 membership 派生：role 决定写入，approve 决定候选确认；无 membership 时按只读处理。 */
export function qualityMembership(identity: Actor | null, workspace: string): { role: string; approve: boolean } | null {
  const found = identity?.workspaces.find(item => item.workspace_id === workspace);
  return found ? { role: found.role, approve: found.approve } : null;
}

function readableError(error: unknown): QualityError {
  const product = error as Partial<ProductEvaluationError>;
  return { message: productErrorMessage(error), payload: product.payload, status: product.status, code: product.code };
}

function shortHash(value: string): string {
  return value ? `${value.slice(0, 12)}…` : "—";
}

function textOrDash(value: unknown): string {
  return typeof value === "string" && value ? value : "—";
}

function countOrDash(value: unknown): string {
  return typeof value === "number" && Number.isFinite(value) ? formatCount(value) : "—";
}

/** 比率（0–1）显示为百分比；缺失或非法一律"——"，不补 100%。 */
function ratioText(value: unknown): string {
  if (typeof value !== "number" || !Number.isFinite(value) || value < 0 || value > 1) return "—";
  return `${Math.round(value * 100)}%`;
}

function taskStatusText(status: string): string {
  const labels: Record<string, string> = { queued: "排队中", running: "运行中", completed: "已完成", failed: "失败", cancelled: "已取消", interrupted: "已中断" };
  return labels[status] ?? status;
}

function candidateStatusText(status: string): string {
  const labels: Record<string, string> = { pending: "待确认", confirmed: "已确认", rejected: "已拒绝" };
  return labels[status] ?? status;
}

function eligibilityText(eligibility: string): string {
  const labels: Record<string, string> = { eligible: "来源完整", needs_context: "缺少上下文", ineligible: "不合格" };
  return labels[eligibility] ?? eligibility;
}

function qualityText(value: unknown): string {
  if (value === "passed") return "通过";
  if (value === "failed") return "未通过";
  return typeof value === "string" && value ? value : "—";
}

function comparisonStatusText(status: unknown): string {
  if (status === "pass") return "比较通过";
  if (status === "fail") return "发现回归";
  if (status === "invalid") return "比较不可用";
  return textOrDash(status);
}

function reasonEntryText(entry: unknown): string {
  if (typeof entry === "string") return entry;
  if (typeof entry === "object" && entry !== null && typeof (entry as Record<string, unknown>).code === "string") {
    return String((entry as Record<string, unknown>).code);
  }
  return JSON.stringify(entry);
}

function scoreCellsText(row: ProductReportRow): string {
  if (!Array.isArray(row.scores)) return "—";
  if (row.scores.length === 0) return "无评分";
  return row.scores.map(score => {
    const rule = score.rule;
    const name = typeof rule === "object" && rule !== null && typeof (rule as Record<string, unknown>).name === "string"
      ? String((rule as Record<string, unknown>).name) : "规则";
    return typeof score.value === "number" ? `${name} ${score.value}` : `${name} —`;
  }).join("，");
}

/** 进度只在两端已知时成立；任一端未知保持"——"，不补 0。 */
function progressText(task: ProductTask): string {
  const planned = typeof task.planned_count === "number" ? task.planned_count : null;
  const finished = typeof task.finished_count === "number" ? task.finished_count : null;
  return `${finished === null ? "—" : finished} / ${planned === null ? "—" : planned}`;
}

function cleanCaseId(value: string): string {
  const cleaned = value.replace(/[^A-Za-z0-9_.-]/g, "-").slice(0, 64);
  return cleaned || "case-1";
}

/** 候确认表单的样本模板：草稿存在时原样给出，否则留空待人工填写；不含任何虚构示例数据。 */
function candidateCaseTemplate(candidate: ProductCandidate): string {
  const draft = candidate.draft_case;
  const spec: ProductCaseSpec = draft ?? {
    case_id: cleanCaseId(candidate.candidate_id),
    instruction: "",
    files: { "INPUT.txt": "" },
    reference: {},
    verifier: "krim-script/1",
  };
  return JSON.stringify(spec, null, 2);
}

/** 有界样本校验，镜像服务端 CaseSpec/请求字节边界；错误逐条可读，不在前端放宽。 */
function validateCaseText(text: string): { errors: string[]; value?: ProductCaseSpec } {
  const errors: string[] = [];
  if (new TextEncoder().encode(text).byteLength > CASE_TEXT_BYTES) return { errors: ["样本 JSON 超过 64 KiB 上限。"] };
  let parsed: unknown;
  try { parsed = JSON.parse(text); }
  catch { return { errors: ["样本 JSON 无法解析。"] }; }
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) return { errors: ["样本需为 JSON 对象。"] };
  const value = parsed as Record<string, unknown>;
  if (typeof value.case_id !== "string" || !identityPattern.test(value.case_id)) errors.push("case_id 需为 1–64 位字母、数字或 ._-。");
  if (typeof value.instruction !== "string" || value.instruction.length < 1 || value.instruction.length > 4096) errors.push("instruction 需为 1–4096 字符。");
  if (typeof value.files !== "object" || value.files === null || Array.isArray(value.files)) errors.push("files 需为文件名到内容的对象。");
  else {
    const entries = Object.entries(value.files);
    if (entries.length < 1 || entries.length > 16) errors.push("files 需含 1–16 个文件。");
    else if (!entries.every(([, content]) => typeof content === "string")) errors.push("files 内容需为字符串。");
  }
  if (typeof value.reference !== "object" || value.reference === null || Array.isArray(value.reference)) errors.push("reference 需为 JSON 对象。");
  else {
    const reference = value.reference as Record<string, unknown>;
    if (value.verifier === "krim-script/1" && typeof reference.expected !== "string") {
      errors.push("krim-script/1 的 reference.expected 必须为字符串；保存、确认与拒绝均需填写。");
    }
    if (value.verifier === "reconciliation/1" && (
      !Number.isSafeInteger(reference.exit_code) || typeof reference.exit_code !== "number"
      || typeof reference.files !== "object" || reference.files === null || Array.isArray(reference.files)
    )) {
      errors.push("reconciliation/1 的 reference 需包含整数 exit_code 和文件对象 files。");
    }
  }
  if (!caseVerifiers.includes(String(value.verifier))) errors.push("verifier 需为服务支持的验证器标识。");
  return errors.length === 0 ? { errors, value: parsed as unknown as ProductCaseSpec } : { errors };
}

/** 保存、确认和拒绝共用后端验证器的参考结构；确认另需人工补齐内容。 */
function requiredFieldErrors(spec: ProductCaseSpec, action: string): string[] {
  if (action !== "confirm") return [];
  const errors: string[] = [];
  if (!spec.instruction.trim()) errors.push("提交确认或修改前需填写任务指令（instruction）。");
  if (!Object.values(spec.files).some(content => content.trim())) errors.push("提交确认或修改前需至少填写一个文件内容（files）。");
  if (Object.keys(spec.reference).length === 0) errors.push("提交确认或修改前需填写参考结果（reference）。");
  return errors;
}

function reasonValid(reason: string): boolean {
  const trimmed = reason.trim();
  return trimmed.length > 0 && new TextEncoder().encode(trimmed).byteLength <= REASON_BYTES;
}

/** 确定性拒绝：4xx 响应已明确说明服务未受理，可清除意图换新键重发。 */
function isDefinitiveRejection(caught: unknown): boolean {
  const status = (caught as { status?: number | null }).status;
  return typeof status === "number" && status >= 400 && status < 500;
}

/** scope 门闩：workspace/密钥/身份变化发生在 render 阶段时立即递增纪元、中止在途请求并清空数据，
 * 迟到响应按纪元丢弃，不泄漏上一 scope。 */
function useQualityFence(stamp: string, reset: () => void) {
  const stampRef = useRef(stamp);
  const epochRef = useRef(0);
  const liveRef = useRef<Set<AbortController>>(new Set());
  if (stampRef.current !== stamp) {
    stampRef.current = stamp;
    epochRef.current += 1;
    for (const controller of liveRef.current) controller.abort();
    liveRef.current.clear();
    reset();
  }
  return { epochRef, liveRef };
}

function ErrorBlock({ error, onRetry, retryLabel }: { error: QualityError; onRetry?: () => void; retryLabel?: string }) {
  return <div className="obs-alert" role="alert">
    <p>{error.message}</p>
    {(error.payload !== undefined || error.code) && <details className="obs-alert-detail">
      <summary>错误详情</summary>
      <pre>{JSON.stringify(error.payload ?? { code: error.code }, null, 2)}</pre>
    </details>}
    {onRetry && <button className="obs-btn" onClick={onRetry}>{retryLabel ?? "刷新"}</button>}
  </div>;
}

export function ObservabilityEvaluations({ apiKey, workspace, identity, onNavigate }: ObservabilityQualityProps) {
  const base = evaluationBase(workspace);
  const stamp = JSON.stringify(["obs-quality-eval", workspace, apiKey, identity?.principal_id ?? "", identity?.generation ?? null]);
  const [tasks, setTasks] = useState<ProductTask[] | null>(null);
  const [datasets, setDatasets] = useState<Awaited<ReturnType<typeof api.datasets>> | null>(null);
  const [taskOffset, setTaskOffset] = useState(0);
  const [datasetOffset, setDatasetOffset] = useState(0);
  const [loading, setLoading] = useState(false);
  const [loadError, setLoadError] = useState<QualityError | null>(null);
  const [refresh, setRefresh] = useState(0);
  const [selectedTaskId, setSelectedTaskId] = useState("");
  const [report, setReport] = useState<ProductReportPage | null>(null);
  const [reportPage, setReportPage] = useState<ReportCursor>({ offset: 0 });
  const [reportHistory, setReportHistory] = useState<ReportCursor[]>([]);
  const [reportLoading, setReportLoading] = useState(false);
  const [reportError, setReportError] = useState<QualityError | null>(null);
  const [baselineId, setBaselineId] = useState("");
  const [candidateTaskId, setCandidateTaskId] = useState("");
  const [comparison, setComparison] = useState<Record<string, unknown> | null>(null);
  const [comparisonBusy, setComparisonBusy] = useState(false);
  const [comparisonError, setComparisonError] = useState<QualityError | null>(null);

  function resetEval() {
    setTasks(null); setDatasets(null); setTaskOffset(0); setDatasetOffset(0);
    setLoading(false); setLoadError(null); setRefresh(0);
    setSelectedTaskId(""); setReport(null); setReportPage({ offset: 0 }); setReportHistory([]);
    setReportLoading(false); setReportError(null);
    setBaselineId(""); setCandidateTaskId(""); setComparison(null);
    setComparisonBusy(false); setComparisonError(null);
  }
  const fence = useQualityFence(stamp, resetEval);

  useEffect(() => {
    const epoch = fence.epochRef.current;
    const controller = new AbortController();
    fence.liveRef.current.add(controller);
    setLoading(true);
    setLoadError(null);
    void Promise.all([
      api.tasks(base, apiKey, 0, controller.signal),
      api.datasets(base, apiKey, 0, controller.signal),
    ]).then(([taskRows, datasetRows]) => {
      if (fence.epochRef.current !== epoch || controller.signal.aborted) return;
      setTasks(taskRows);
      setDatasets(datasetRows);
    }).catch(reason => {
      if (controller.signal.aborted || fence.epochRef.current !== epoch) return;
      setLoadError(readableError(reason));
    }).finally(() => {
      if (fence.epochRef.current === epoch && !controller.signal.aborted) setLoading(false);
      fence.liveRef.current.delete(controller);
    });
    return () => { controller.abort(); fence.liveRef.current.delete(controller); };
  }, [base, apiKey, stamp, refresh, fence.epochRef, fence.liveRef]);

  async function loadMoreTasks() {
    if (!tasks || loading) return;
    const epoch = fence.epochRef.current;
    const controller = new AbortController();
    fence.liveRef.current.add(controller);
    setLoadError(null);
    try {
      const rows = await api.tasks(base, apiKey, taskOffset + tasks.length, controller.signal);
      if (fence.epochRef.current !== epoch || controller.signal.aborted) return;
      setTasks([...tasks, ...rows]);
      setTaskOffset(taskOffset + rows.length);
    } catch (reason) {
      if (!controller.signal.aborted && fence.epochRef.current === epoch) setLoadError(readableError(reason));
    } finally { fence.liveRef.current.delete(controller); }
  }

  async function loadMoreDatasets() {
    if (!datasets || loading) return;
    const epoch = fence.epochRef.current;
    const controller = new AbortController();
    fence.liveRef.current.add(controller);
    setLoadError(null);
    try {
      const rows = await api.datasets(base, apiKey, datasetOffset + datasets.length, controller.signal);
      if (fence.epochRef.current !== epoch || controller.signal.aborted) return;
      setDatasets([...datasets, ...rows]);
      setDatasetOffset(datasetOffset + rows.length);
    } catch (reason) {
      if (!controller.signal.aborted && fence.epochRef.current === epoch) setLoadError(readableError(reason));
    } finally { fence.liveRef.current.delete(controller); }
  }

  function selectTask(task: ProductTask) {
    if (selectedTaskId !== task.id) {
      setReport(null); setReportPage({ offset: 0 }); setReportHistory([]); setReportError(null);
    }
    setSelectedTaskId(task.id);
  }

  const selectedTask = tasks?.find(task => task.id === selectedTaskId) ?? null;

  async function loadReport(page: ReportCursor, history: ReportCursor[]) {
    if (!selectedTask || reportLoading) return;
    const epoch = fence.epochRef.current;
    const controller = new AbortController();
    fence.liveRef.current.add(controller);
    setReportLoading(true);
    setReportError(null);
    try {
      const value = await api.report(base, apiKey, selectedTask.id, page, controller.signal);
      if (fence.epochRef.current !== epoch || controller.signal.aborted) return;
      setReport(value);
      setReportPage(page);
      setReportHistory(history);
    } catch (reason) {
      if (!controller.signal.aborted && fence.epochRef.current === epoch) setReportError(readableError(reason));
    } finally {
      if (fence.epochRef.current === epoch) setReportLoading(false);
      fence.liveRef.current.delete(controller);
    }
  }

  const nextPage: ReportCursor | null = report
    ? report.next_cursor ? { cursor: report.next_cursor, offset: reportPage.offset + report.rows.length }
      : typeof report.next_offset === "number" ? { offset: report.next_offset } : null
    : null;
  const previousPage = reportHistory[reportHistory.length - 1] ?? null;

  const comparableTasks = (tasks ?? []).filter(task => task.status === "completed" && hex64.test(task.report_hash ?? ""));
  const baselineTask = comparableTasks.find(task => task.id === baselineId) ?? null;
  const candidateTask = comparableTasks.find(task => task.id === candidateTaskId) ?? null;

  async function compareTasks() {
    if (!baselineTask?.report_hash || !candidateTask?.report_hash || comparisonBusy || baselineTask.id === candidateTask.id) return;
    const epoch = fence.epochRef.current;
    setComparisonBusy(true);
    setComparisonError(null);
    setComparison(null);
    try {
      const value = await api.compare(base, apiKey, {
        baseline_id: baselineTask.id, candidate_id: candidateTask.id,
        baseline_hash: baselineTask.report_hash!, candidate_hash: candidateTask.report_hash!,
        allowed_subjects: [],
      });
      if (fence.epochRef.current !== epoch) return;
      setComparison(value);
    } catch (reason) {
      if (fence.epochRef.current === epoch) setComparisonError(readableError(reason));
    } finally { if (fence.epochRef.current === epoch) setComparisonBusy(false); }
  }

  const comparisonReasons = comparison && Array.isArray(comparison.reasons) ? comparison.reasons : null;

  return <section className="obs-eval" aria-label="质量评测">
    <header className="obs-page-head">
      <div>
        <h2>评测</h2>
        <p className="obs-page-note">读取工作区的评测任务、数据集版本与报告。</p>
      </div>
      <div className="obs-page-actions">
        <button className="obs-btn" onClick={() => onNavigate("eval")}>打开原评测页</button>
        <button className="obs-btn" onClick={() => setRefresh(value => value + 1)}>刷新</button>
      </div>
    </header>
    {loadError && <ErrorBlock error={loadError} onRetry={() => setRefresh(value => value + 1)} />}
    {loading && <p className="obs-state" role="status">{tasks ? "正在刷新评测任务…" : "正在读取评测任务…"}</p>}
    {tasks !== null && tasks.length === 0 && !loadError && <p className="obs-state">当前工作区暂无评测任务。</p>}
    {tasks !== null && tasks.length > 0 && <ul className="obs-eval-tasks" aria-label="评测任务列表">
      {tasks.map(task => <li key={task.id}>
        <button
          className={task.id === selectedTaskId ? "obs-eval-task-row obs-eval-task-row-active" : "obs-eval-task-row"}
          aria-pressed={task.id === selectedTaskId}
          onClick={() => selectTask(task)}
        >
          <span className="obs-eval-task-id" title={task.id}>{task.id}</span>
          <span className="obs-eval-task-status">{taskStatusText(task.status)}</span>
          <span className="obs-eval-task-dataset">{task.dataset.name} · {task.dataset.version}</span>
          <span className="obs-eval-task-target">{task.target_key}</span>
          <span className="obs-eval-task-progress">{progressText(task)}</span>
          <span className="obs-eval-task-time">{formatTime(task.created_at)}</span>
        </button>
      </li>)}
    </ul>}
    {tasks !== null && taskOffset + tasks.length > 0 && tasks.length % PAGE_SIZE === 0
      && <button className="obs-btn" onClick={() => void loadMoreTasks()}>更多任务</button>}
    <section className="obs-eval-datasets" aria-label="数据集版本">
      <h3>数据集版本</h3>
      {datasets === null && !loadError && <p className="obs-state">正在读取数据集版本…</p>}
      {datasets !== null && datasets.length === 0 && <p className="obs-state">暂无数据集版本。</p>}
      {datasets !== null && datasets.length > 0 && <ul className="obs-eval-dataset-list">
        {datasets.map(row => <li key={`${row.name}/${row.version}`}>
          <span className="obs-eval-dataset-name">{row.name} · {row.version}</span>
          <span className="obs-eval-dataset-count">样本 {countOrDash(row.case_count)}</span>
          <span className="obs-eval-dataset-fp" title={row.fingerprint}>{shortHash(row.fingerprint)}</span>
        </li>)}
      </ul>}
      {datasets !== null && datasets.length % PAGE_SIZE === 0 && datasets.length > 0
        && <button className="obs-btn" onClick={() => void loadMoreDatasets()}>更多数据集</button>}
    </section>
    {selectedTask && <article className="obs-eval-detail" aria-label={`评测任务 ${selectedTask.id}`}>
      <h3>任务详情</h3>
      <dl className="obs-facts">
        <div><dt>任务 ID</dt><dd className="obs-mono">{selectedTask.id}</dd></div>
        <div><dt>请求键</dt><dd className="obs-mono">{selectedTask.request_key}</dd></div>
        <div><dt>状态</dt><dd>{taskStatusText(selectedTask.status)}{selectedTask.code ? `（${selectedTask.code}）` : ""}</dd></div>
        <div><dt>数据集</dt><dd>{selectedTask.dataset.name} · {selectedTask.dataset.version}</dd></div>
        <div><dt>目标</dt><dd className="obs-mono">{selectedTask.target_key}</dd></div>
        <div><dt>指标</dt><dd>{selectedTask.metrics.map(metric => metric.name).join("、") || "—"}</dd></div>
        <div><dt>进度</dt><dd>{progressText(selectedTask)}</dd></div>
        <div><dt>报告指纹</dt><dd className="obs-mono" title={selectedTask.report_hash ?? ""}>{selectedTask.report_hash ? shortHash(selectedTask.report_hash) : "—"}</dd></div>
      </dl>
      {selectedTask.report_hash
        ? <button className="obs-btn" disabled={reportLoading} onClick={() => void loadReport({ offset: 0 }, reportHistory)}>查看报告</button>
        : <p className="obs-note">任务尚无报告指纹；完成后刷新即可读取报告。</p>}
      {reportError && <ErrorBlock error={reportError} onRetry={() => void loadReport(reportPage, reportHistory)} />}
      {report && <div className="obs-eval-report" aria-label="评测报告">
        <h4>报告摘要</h4>
        <dl className="obs-facts">
          <div><dt>类型</dt><dd>{textOrDash(report.kind)}</dd></div>
          <div><dt>修订</dt><dd>{countOrDash(report.revision)}</dd></div>
          <div><dt>报告指纹</dt><dd className="obs-mono" title={report.report_hash ?? ""}>{report.report_hash ? shortHash(report.report_hash) : "—"}</dd></div>
          <div><dt>执行状态</dt><dd>{textOrDash(report.execution.status)}</dd></div>
          <div><dt>质量通过率</dt><dd>{ratioText(report.quality.pass_rate)}</dd></div>
          <div><dt>通过数</dt><dd>{countOrDash(report.quality.passed)}</dd></div>
          <div><dt>评分覆盖</dt><dd>{ratioText(report.coverage.cell_coverage)}</dd></div>
          <div><dt>样本覆盖</dt><dd>{ratioText(report.coverage.item_coverage)}</dd></div>
          <div><dt>评分单元</dt><dd>{countOrDash(report.coverage.scored_cells)} / {countOrDash(report.coverage.planned_cells)}</dd></div>
        </dl>
        <table className="obs-eval-rows">
          <thead><tr><th scope="col">样本</th><th scope="col">重复</th><th scope="col">状态</th><th scope="col">质量</th><th scope="col">评分</th></tr></thead>
          <tbody>
            {report.rows.map((row, index) => <tr key={`${row.case_id}/${row.repetition}/${index}`}>
              <td className="obs-mono">{row.case_id}</td>
              <td>{countOrDash(row.repetition)}</td>
              <td>{textOrDash(row.status)}</td>
              <td>{qualityText(row.quality)}</td>
              <td>{scoreCellsText(row)}</td>
            </tr>)}
          </tbody>
        </table>
        <div className="obs-eval-paging">
          <button className="obs-btn" disabled={!previousPage || reportLoading} onClick={() => {
            if (previousPage) void loadReport(previousPage, reportHistory.slice(0, -1));
          }}>上一页</button>
          <button className="obs-btn" disabled={!nextPage || reportLoading} onClick={() => {
            if (nextPage) void loadReport(nextPage, [...reportHistory, reportPage]);
          }}>下一页</button>
          <span className="obs-note">已读 {formatCount(report.rows.length)} 项样本</span>
        </div>
      </div>}
    </article>}
    <section className="obs-eval-compare" aria-label="任务比较">
      <h3>任务比较</h3>
      <p className="obs-page-note">比较两份已完成的报告；不可比时显示具体原因。</p>
      {comparableTasks.length < 2
        ? <p className="obs-state">需要两个已完成的任务才能比较；当前可比任务 {formatCount(comparableTasks.length)} 个。</p>
        : <div className="obs-eval-compare-form">
          <div className="obs-field">
            <label htmlFor="obs-eval-compare-baseline">基线任务</label>
            <select id="obs-eval-compare-baseline" value={baselineId} onChange={event => { setBaselineId(event.target.value); setComparison(null); setComparisonError(null); }}>
              <option value="">选择基线</option>
              {comparableTasks.map(task => <option key={task.id} value={task.id}>{task.dataset.name} · {task.id.slice(0, 12)}</option>)}
            </select>
          </div>
          <div className="obs-field">
            <label htmlFor="obs-eval-compare-candidate">候选任务</label>
            <select id="obs-eval-compare-candidate" value={candidateTaskId} onChange={event => { setCandidateTaskId(event.target.value); setComparison(null); setComparisonError(null); }}>
              <option value="">选择候选</option>
              {comparableTasks.map(task => <option key={task.id} value={task.id}>{task.dataset.name} · {task.id.slice(0, 12)}</option>)}
            </select>
          </div>
          <button className="obs-btn" disabled={!baselineTask || !candidateTask || baselineTask.id === candidateTask.id || comparisonBusy} onClick={() => void compareTasks()}>比较基线与候选</button>
        </div>}
      {comparisonError && <ErrorBlock error={comparisonError} onRetry={() => void compareTasks()} />}
      {comparison && <div className="obs-eval-comparison">
        <p className={comparison.status === "pass" ? "obs-ok" : "obs-state"}>
          {comparisonStatusText(comparison.status)}{typeof comparison.policy === "string" ? `（${comparison.policy}）` : ""}
        </p>
        {comparisonReasons && comparisonReasons.length > 0 && <ul className="obs-eval-comparison-reasons">
          {comparisonReasons.map((entry, index) => <li key={index}>{reasonEntryText(entry)}</li>)}
        </ul>}
      </div>}
    </section>
  </section>;
}

export function ObservabilityReview({ apiKey, workspace, identity, onNavigate }: ObservabilityQualityProps) {
  const base = evaluationBase(workspace);
  const membership = qualityMembership(identity, workspace);
  const canWrite = membership !== null && membership.role !== "viewer";
  const canApprove = membership !== null && membership.role !== "viewer" && membership.approve;
  const stamp = JSON.stringify(["obs-quality-review", workspace, apiKey, identity?.principal_id ?? "", identity?.generation ?? null]);
  const [lookupId, setLookupId] = useState("");
  const [candidate, setCandidate] = useState<ProductCandidate | null>(null);
  const [receipt, setReceipt] = useState<ProductImportReceipt | null>(null);
  const [loading, setLoading] = useState(false);
  const [loadError, setLoadError] = useState<QualityError | null>(null);
  const [action, setAction] = useState<ConfirmAction>("confirm");
  const [context, setContext] = useState<ConfirmContext>("complete");
  const [caseText, setCaseText] = useState("");
  const [reason, setReason] = useState("");
  const [saving, setSaving] = useState(false);
  const [savedNeedsReload, setSavedNeedsReload] = useState(false);
  const [saveError, setSaveError] = useState<QualityError | null>(null);
  const [actionMessage, setActionMessage] = useState("");
  /** 有界确认意图（仅内存）：绑定确认键与冻结的提交内容（动作/口径/样本/理由）。
   * 同内容重试沿用同键；确定未受理的 4xx 清除意图（改正后换新键）；
   * 网络/5xx 未知结果保留意图并禁止新内容覆盖，原样重试或重读候选核实。 */
  const confirmIntents = useRef(new Map<string, { requestKey: string; payload: string; revision: number; hash: string }>());

  function resetReview() {
    setLookupId(""); setCandidate(null); setReceipt(null);
    setLoading(false); setLoadError(null);
    setAction("confirm"); setContext("complete"); setCaseText(""); setReason("");
    setSaving(false); setSavedNeedsReload(false); setSaveError(null); setActionMessage("");
    confirmIntents.current.clear();
  }
  const fence = useQualityFence(stamp, resetReview);

  /** 冻结可编辑内容；revision/hash 也保存在意图中，重试完整复用原请求。 */
  function parseIntentPayload(payload: string): { action: ConfirmAction; context: ConfirmContext; case: ProductCaseSpec; reason: string } | null {
    try {
      const parsed = JSON.parse(payload) as Record<string, unknown>;
      if (typeof parsed.action !== "string" || !["confirm", "edit", "reject"].includes(parsed.action)) return null;
      if (parsed.context !== "complete" && parsed.context !== "manual_reconstructed") return null;
      if (typeof parsed.reason !== "string") return null;
      const check = validateCaseText(JSON.stringify(parsed.case));
      if (!check.value) return null;
      return { action: parsed.action as ConfirmAction, context: parsed.context as ConfirmContext, case: check.value, reason: parsed.reason };
    } catch { return null; }
  }

  function adoptCandidate(value: ProductCandidate) {
    setCandidate(value);
    setSavedNeedsReload(false);
    setSaveError(null);
    setActionMessage("");
    if (value.status !== "pending") confirmIntents.current.delete(value.candidate_id);
    const frozen = parseIntentPayload(confirmIntents.current.get(value.candidate_id)?.payload ?? "");
    if (frozen) {
      // 仍有待核实的确认意图：恢复冻结的提交内容，不覆盖为空模板。
      setAction(frozen.action);
      setContext(frozen.context);
      setCaseText(JSON.stringify(frozen.case, null, 2));
      setReason(frozen.reason);
    } else {
      setAction("confirm");
      setContext(value.eligibility === "needs_context" ? "manual_reconstructed" : "complete");
      setCaseText(candidateCaseTemplate(value));
      setReason("");
    }
  }

  async function openCandidate(id: string) {
    if (!id || loading) return;
    const epoch = fence.epochRef.current;
    const controller = new AbortController();
    fence.liveRef.current.add(controller);
    setLoading(true);
    setLoadError(null);
    try {
      const value = await api.candidate(base, apiKey, id, controller.signal);
      if (fence.epochRef.current !== epoch || controller.signal.aborted) return;
      adoptCandidate(value);
    } catch (caught) {
      if (!controller.signal.aborted && fence.epochRef.current === epoch) setLoadError(readableError(caught));
    } finally {
      if (fence.epochRef.current === epoch) setLoading(false);
      fence.liveRef.current.delete(controller);
    }
  }

  async function openReceipt(requestKey: string) {
    if (!requestKey || loading) return;
    const epoch = fence.epochRef.current;
    const controller = new AbortController();
    fence.liveRef.current.add(controller);
    setLoading(true);
    setLoadError(null);
    try {
      const value = await api.importReceipt(base, apiKey, requestKey, controller.signal);
      if (fence.epochRef.current !== epoch || controller.signal.aborted) return;
      setReceipt(value);
      setCandidate(null);
      setSaveError(null);
      setActionMessage("");
    } catch (caught) {
      if (!controller.signal.aborted && fence.epochRef.current === epoch) setLoadError(readableError(caught));
    } finally {
      if (fence.epochRef.current === epoch) setLoading(false);
      fence.liveRef.current.delete(controller);
    }
  }

  function receiptSourceRun(candidateId: string): string {
    const summary = receipt?.candidates.find(item => item.candidate_id === candidateId);
    const runId = summary?.source?.run_id;
    return typeof runId === "string" && runId ? runId : "";
  }

  /** 确认成功后的候选重读：带中止与 fence；失败不影响"已保存"结论，返回 null 由调用方提示。 */
  async function reloadCandidate(id: string): Promise<ProductCandidate | null> {
    const epoch = fence.epochRef.current;
    const controller = new AbortController();
    fence.liveRef.current.add(controller);
    try {
      const value = await api.candidate(base, apiKey, id, controller.signal);
      if (fence.epochRef.current !== epoch || controller.signal.aborted) return null;
      return value;
    } catch { return null; }
    finally { fence.liveRef.current.delete(controller); }
  }

  const caseCheck: { errors: string[]; value?: ProductCaseSpec } = caseText.trim()
    ? validateCaseText(caseText)
    : { errors: ["样本 JSON 尚未填写。"] };
  const requiredErrors = caseCheck.value ? requiredFieldErrors(caseCheck.value, action) : [];
  const draftPayload = caseCheck.value && reasonValid(reason)
    ? JSON.stringify({ action, context, case: caseCheck.value, reason: reason.trim() })
    : null;
  const pendingIntent = candidate ? confirmIntents.current.get(candidate.candidate_id) : undefined;
  /** 意图未决时表单内容不得偏离冻结 payload：未知结果下禁止换内容重发。 */
  const intentConflict = pendingIntent !== undefined && draftPayload !== null && pendingIntent.payload !== draftPayload;
  const candidateHashOk = candidate !== null && hex64.test(candidate.hash);
  const submitDisabled = candidate === null || !canApprove || saving || savedNeedsReload
    || candidate.status !== "pending" || candidate.eligibility === "ineligible" || !candidateHashOk
    || caseCheck.errors.length > 0 || requiredErrors.length > 0 || !reasonValid(reason) || intentConflict;

  async function submitConfirmation() {
    if (submitDisabled || !candidate || !caseCheck.value || draftPayload === null) return;
    const intent = confirmIntents.current.get(candidate.candidate_id);
    const requestKey = intent ? intent.requestKey : createRequestKey("confirm-case");
    const body = {
      action,
      context,
      request_key: requestKey,
      expected_revision: intent?.revision ?? candidate.revision,
      expected_hash: intent?.hash ?? candidate.hash,
      case: caseCheck.value,
      reason: reason.trim(),
    };
    const candidateId = candidate.candidate_id;
    const epoch = fence.epochRef.current;
    setSaving(true);
    setSaveError(null);
    setActionMessage("");
    if (!intent) confirmIntents.current.set(candidateId, {
      requestKey, payload: draftPayload, revision: candidate.revision, hash: candidate.hash,
    });
    try {
      const confirmation = await api.confirmCandidate(base, apiKey, candidateId, body);
      if (fence.epochRef.current !== epoch) return;
      confirmIntents.current.delete(candidateId);
      const confirmationId = typeof confirmation.id === "string" ? confirmation.id : "";
      const reloaded = await reloadCandidate(candidateId);
      if (fence.epochRef.current !== epoch) return;
      if (reloaded) {
        setCandidate(reloaded);
        setActionMessage(reloaded.confirmation_id
          ? `已保存；确认记录 ${reloaded.confirmation_id}，候选状态 ${candidateStatusText(reloaded.status)}。`
          : "已保存；候选状态已重新读取。");
      } else {
        // 确认已被服务受理；仅重读未完成，本地按受理结果展示，不再生成新键。
        const acceptedStatus = ["confirmed", "pending", "rejected"].includes(String(confirmation.status))
          ? String(confirmation.status) : action === "reject" ? "rejected" : action === "edit" ? "pending" : "confirmed";
        setCandidate({ ...candidate, status: acceptedStatus, ...(confirmationId ? { confirmation_id: confirmationId } : {}) });
        setSavedNeedsReload(true);
        setActionMessage(`已保存${confirmationId ? `；确认记录 ${confirmationId}` : ""}；候选状态重读未完成，可点按"重新读取候选"核实。`);
      }
    } catch (caught) {
      if (fence.epochRef.current !== epoch) return;
      if (isDefinitiveRejection(caught)) {
        // 服务已明确拒绝，未受理；清除意图，改正后可换新键重发。
        confirmIntents.current.delete(candidateId);
        const detail = readableError(caught);
        setSaveError({ ...detail, message: `已确定未受理：${detail.message}改正后可重发。` });
      } else if (caught instanceof TypeError) {
        setSaveError({ message: "响应中断：确认可能已被受理；请重新读取候选核实，重试将沿用原确认键。" });
      } else {
        setSaveError(readableError(caught));
      }
    } finally {
      if (fence.epochRef.current === epoch) setSaving(false);
    }
  }

  const actionLabels: Record<ConfirmAction, string> = { confirm: "提交确认", edit: "保存修改", reject: "提交拒绝" };

  return <section className="obs-review" aria-label="质量复核">
    <header className="obs-page-head">
      <div>
        <h2>复核</h2>
        <p className="obs-page-note">按候选 ID 或导入回执键打开样本，核对并确认参考结果。</p>
      </div>
      <div className="obs-page-actions">
        <button className="obs-btn" onClick={() => onNavigate("datasets")}>打开数据集管理</button>
      </div>
    </header>
    <div className="obs-review-lookup">
      <div className="obs-field">
        <label htmlFor="obs-review-lookup-id">候选 ID 或导入回执键</label>
        <input id="obs-review-lookup-id" className="obs-input" value={lookupId} onChange={event => setLookupId(event.target.value)} autoComplete="off" />
      </div>
      <button className="obs-btn" disabled={!lookupId.trim() || loading} onClick={() => void openCandidate(lookupId.trim())}>读取候选</button>
      <button className="obs-btn" disabled={!lookupId.trim() || loading} onClick={() => void openReceipt(lookupId.trim())}>按回执键查找</button>
      {loading && <p className="obs-state" role="status">正在读取…</p>}
    </div>
    {loadError && <ErrorBlock error={loadError} onRetry={() => void openCandidate(lookupId.trim())} retryLabel="重新读取" />}
    {receipt && <section className="obs-review-receipt" aria-label="导入回执">
      <h3>回执 {receipt.request_key}</h3>
      {receipt.candidates.length === 0
        ? <p className="obs-state">该回执下没有候选记录。</p>
        : <ul className="obs-review-receipt-list">
          {receipt.candidates.map(item => <li key={item.candidate_id}>
            <span className="obs-mono">{item.candidate_id}</span>
            <span>{eligibilityText(item.eligibility)}</span>
            {item.reasons.length > 0 && <span className="obs-note">{item.reasons.join("、")}</span>}
            <button className="obs-btn" onClick={() => void openCandidate(item.candidate_id)}>打开</button>
          </li>)}
        </ul>}
    </section>}
    {candidate && <section className="obs-review-candidate" aria-label={`候选 ${candidate.candidate_id}`}>
      <h3>候选 {candidate.candidate_id}</h3>
      <dl className="obs-facts">
        <div><dt>状态</dt><dd>{candidateStatusText(candidate.status)}</dd></div>
        <div><dt>修订</dt><dd>{countOrDash(candidate.revision)}</dd></div>
        <div><dt>内容指纹</dt><dd className="obs-mono" title={candidate.hash}>{candidateHashOk ? shortHash(candidate.hash) : "指纹缺失"}</dd></div>
        <div><dt>观测 ID</dt><dd className="obs-mono">{candidate.observation_id || "—"}</dd></div>
        <div><dt>观测指纹</dt><dd className="obs-mono" title={candidate.observation_hash}>{candidate.observation_hash ? shortHash(candidate.observation_hash) : "—"}</dd></div>
        <div><dt>资格</dt><dd>{eligibilityText(candidate.eligibility)}{candidate.reasons.length > 0 ? `：${candidate.reasons.join("、")}` : ""}</dd></div>
        <div><dt>来源</dt><dd className="obs-mono">{receiptSourceRun(candidate.candidate_id) || "—"}</dd></div>
        <div><dt>确认记录</dt><dd className="obs-mono">{candidate.confirmation_id ?? "无"}</dd></div>
      </dl>
      {candidate.status !== "pending" && <p className="obs-note">候选当前状态为「{candidateStatusText(candidate.status)}」，仅待确认候选可提交。</p>}
      {candidate.eligibility === "ineligible" && <p className="obs-note">候选不合格，不能提交确认。</p>}
      {!candidateHashOk && <p className="obs-note">候选内容指纹缺失或格式不符，不能提交确认。</p>}
      {!canWrite && <p className="obs-note">viewer 身份只能读取候选；写入与确认需要工作区成员权限。</p>}
      {canWrite && !canApprove && <p className="obs-note">当前账号没有复核权限。</p>}
      {canWrite && <div className="obs-review-form">
        <div className="obs-field">
          <label htmlFor="obs-review-action">动作</label>
          <select id="obs-review-action" value={action} disabled={!canApprove} onChange={event => setAction(event.target.value as typeof action)}>
            <option value="confirm">确认参考</option>
            <option value="edit">保存修改，继续待确认</option>
            <option value="reject">拒绝此样本</option>
          </select>
        </div>
        <div className="obs-field">
          <label htmlFor="obs-review-context">上下文口径</label>
          <select id="obs-review-context" value={context} disabled={!canApprove} onChange={event => setContext(event.target.value as typeof context)}>
            <option value="complete">来源上下文完整</option>
            <option value="manual_reconstructed">上下文由人工重建</option>
          </select>
          <p className="obs-field-note">{context === "manual_reconstructed"
            ? "任务指令、文件与参考结果由人工重建填写。"
            : undefined}</p>
        </div>
        <div className="obs-field">
          <label htmlFor="obs-review-case">样本 JSON（case_id / instruction / files / reference / verifier）</label>
          <textarea id="obs-review-case" className="obs-input obs-mono" rows={10} value={caseText} spellCheck={false}
            disabled={!canApprove} onChange={event => setCaseText(event.target.value)} />
          {caseCheck.errors.length > 0 && <ul className="obs-field-errors" aria-label="样本校验问题">
            {caseCheck.errors.map(message => <li key={message}>{message}</li>)}
          </ul>}
          {requiredErrors.length > 0 && <ul className="obs-field-errors" aria-label="待补齐字段">
            {requiredErrors.map(message => <li key={message}>{message}</li>)}
          </ul>}
        </div>
        <div className="obs-field">
          <label htmlFor="obs-review-reason">理由（必填，≤512 字节）</label>
          <textarea id="obs-review-reason" className="obs-input" rows={2} value={reason}
            disabled={!canApprove} onChange={event => setReason(event.target.value)} />
        </div>
        <div className="obs-review-actions">
          <button className="obs-btn" disabled={submitDisabled} onClick={() => void submitConfirmation()}>
            {saving ? "正在提交…" : actionLabels[action]}
          </button>
          <button className="obs-btn" disabled={loading || saving} onClick={() => void openCandidate(candidate.candidate_id)}>重新读取候选</button>
        </div>
        {actionMessage && <p className="obs-ok" role="status">{actionMessage}</p>}
        {pendingIntent && <p className="obs-note">
          待核实的确认请求键：<span className="obs-mono">{pendingIntent.requestKey}</span>；可原样重试或重新读取候选核实。
        </p>}
        {intentConflict && <p className="obs-field-errors" role="alert">内容与待核实的确认请求不一致；核实前不能修改重发，可原样重试或重新读取候选。</p>}
        {saveError && <ErrorBlock error={saveError} onRetry={() => void openCandidate(candidate.candidate_id)} retryLabel="重新读取候选" />}
      </div>}
    </section>}
  </section>;
}
