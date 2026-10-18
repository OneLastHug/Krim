import { object } from "../api";
import { evalUi as t, labels, qualityLabels } from "../lib/copy";
import type { EvaluationState } from "../hooks/use-evaluation";
import { Button } from "./ui/button";
const text = (v: unknown) => v === null || v === undefined ? "未知" : String(v);
export function EvalReport({ evaluation: e, onRun }: {
    evaluation: EvaluationState;
    onRun: (id: string) => void;
}) {
    const report = e.report;
    return <section aria-labelledby="evaluation-heading" className="eval-report"><div className="workspace-title"><div><h2 id="evaluation-heading">{t.current}</h2><p id="eval-status" role="status">{e.view ? labels[e.view.status] : e.id ? e.notice ? t.unread : t.reading : e.pending ? "创建结果未确认" : e.busy ? "处理中…" : t.empty}</p></div>{e.id && <div className="eval-actions"><Button variant="ghost" onClick={e.refresh}>刷新</Button><Button id="eval-cancel" variant="outline" disabled={e.stopping || !e.view || !["queued", "running"].includes(e.view.status)} onClick={() => void e.stop()}>{e.stopping ? "停止中…" : "停止"}</Button></div>}</div>
  {e.id && <details className="run-identity"><summary data-ui-data>{e.id.slice(0, 8)}</summary><p id="eval-id" data-ui-data>{e.id}</p></details>}
  {report && <><div className="eval-summary" id="eval-summary"><span>{t.coverage} <strong data-ui-data>{text(report.summary.scored_cells)}/{text(report.summary.planned_cells)}</strong></span><span>{t.passed} <strong data-ui-data>{text(report.summary.passed)}/{text(report.summary.planned)}</strong></span><span>{t.usage} <strong data-ui-data>{report.summary.token_usage === null ? t.unknown : text(object(report.summary.token_usage).total_tokens)}</strong></span><span>{report.complete ? "完整" : "部分"}</span></div>
   <div className="report-table-scroll"><table id="eval-items"><thead><tr><th>样本</th><th>结果</th><th>评分</th><th>执行</th></tr></thead><tbody>{report.items.map(raw => { const item = object(raw), output = item.output === null ? null : object(item.output), scores = Array.isArray(item.scores) ? item.scores.map(object) : []; return <tr key={String(item.case_id) + ":" + String(item.repetition)}><td><span data-ui-data>{text(item.case_id)}</span><small>#{Number(item.repetition) + 1}</small></td><td><span>{qualityLabels[String(item.quality)] ?? "未评分"}</span>{output && <details><summary>答案</summary><pre data-ui-data>{text(output.text)}</pre></details>}</td><td>{scores.map(score => <div key={String(object(score.rule).name)}><span data-ui-data>{text(object(score.rule).name)}</span> <strong>{score.status === "scored" ? text(score.value) : score.status === "skipped" ? "跳过" : "错误"}</strong>{score.reason !== null && <small data-ui-data>{text(score.reason)}</small>}</div>)}</td><td>{typeof item.run_id === "string" && <Button variant="ghost" onClick={() => onRun(item.run_id as string)}>查看任务</Button>}<details><summary>ID</summary><p data-ui-data>{text(item.run_id)}</p><p data-ui-data>{item.source_run_id === null ? "手工样本" : text(item.source_run_id)}</p></details></td></tr>; })}</tbody></table></div>
   <details className="report-identity"><summary>报告详情</summary><p data-ui-data>{report.report_hash}</p><p data-ui-data>{String(report.target_descriptor.key)}</p><p data-ui-data>{report.target_fingerprint}</p></details>
  </>}
 </section>;
}
