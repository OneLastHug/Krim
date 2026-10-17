import { evalUi as t } from "../lib/copy";
import type { DatasetState, Draft } from "../hooks/use-dataset";
import { Button } from "./ui/button";
import { Input } from "./ui/input";
import { Textarea } from "./ui/textarea";
import { Label } from "./ui/label";
const sourceLabels: Record<string, string> = { eligible: "可选入", needs_context: "需要上下文", ineligible: "尚未结束" };
const sourceReasons: Record<string, string> = { not_terminal: "任务尚未结束", source_not_completed: "来源未完成", incomplete_execution_facts: "执行记录不完整", target_identity_incomplete: "目标信息不完整", input_capacity: "输入超出样本上限", context_dependency: "包含上下文依赖" };
export function CaseEditor({ data }: {
    data: DatasetState;
}) {
    function field(id: string, label: string, key: keyof Draft, large = false) { return <div className="eval-field"><Label htmlFor={id}>{label}</Label>{large ? <Textarea id={id} className="eval-input" value={data.draft[key]} readOnly={key === "input" && Boolean(data.source)} onChange={e => data.change({ [key]: e.target.value })}/> : <Input id={id} value={data.draft[key]} onChange={e => data.change({ [key]: e.target.value })}/>}</div>; }
    return <section className="case-editor" aria-labelledby="dataset-heading"><h2 id="dataset-heading">{t.data}</h2>
  <div className="eval-two"><div><Label htmlFor="dataset-name">{t.name}</Label><Input id="dataset-name" value={data.name} onChange={e => data.rename(e.target.value)}/></div><div><Label htmlFor="dataset-version">{t.version}</Label><Input id="dataset-version" value={data.version} onChange={e => data.revise(e.target.value)}/></div></div>
  <div className="source-picker"><Label htmlFor="source-run">{t.source}</Label><div className="eval-inline"><Input id="source-run" value={data.draft.sourceRun} onChange={e => data.change({ sourceRun: e.target.value })}/><Button variant="outline" onClick={() => void data.collect(data.draft.sourceRun)} disabled={!data.draft.sourceRun}>{t.readSource}</Button></div>
   {data.candidate && <div className="source-facts" role="status"><span id="source-eligibility">{sourceLabels[data.candidate.eligibility] ?? data.candidate.eligibility}</span>{data.candidate.eligibility !== "eligible" && data.candidate.reasons.map(r => <p key={r}>{sourceReasons[r] ?? r}</p>)}{data.candidate.eligibility === "eligible" && <Button variant="outline" onClick={() => void data.collect(data.candidate!.source_run_id, true)}>{t.useInput}</Button>}</div>}
   {data.source && <div className="eval-inline"><code data-ui-data>{data.source.source_run_id.slice(0, 8)}</code><Button variant="ghost" onClick={data.manual}>{t.manual}</Button></div>}
  </div>
  {field("case-id", t.caseId, "caseId")}{field("case-input", t.input, "input", true)}{field("case-expected", t.expected, "expected", true)}
  <div className="eval-two"><div><Label htmlFor="case-status">{t.status}</Label><select id="case-status" value={data.draft.status} onChange={e => data.change({ status: e.target.value as "completed" | "failed" })}><option value="completed">已完成</option><option value="failed">失败</option></select></div>{field("case-code", t.code, "code")}</div>
  {field("case-reason", t.reason, "reason")}
  <details className="reference-details"><summary>{t.constraints}</summary>{field("case-tools", t.tools, "tools", true)}{field("case-trajectory", t.trajectory, "trajectory", true)}{field("case-schema", t.schema, "schema", true)}</details>
  <Button variant="outline" onClick={data.add}>{t.add}</Button>
  <ul id="case-list" className="case-list">{data.cases.map(c => <li key={c.case_id}><span data-ui-data>{c.case_id}</span><div><Button variant="ghost" aria-label={`编辑样本 ${c.case_id}`} onClick={() => data.editCase(c)}>编辑</Button><Button variant="ghost" aria-label={`移除样本 ${c.case_id}`} onClick={() => data.remove(c.case_id)}>移除</Button></div></li>)}</ul>
  <div className="eval-actions"><Button id="freeze" onClick={() => void data.freeze()} disabled={data.busy || !data.pending && data.cases.length === 0}>{data.busy ? "处理中…" : data.pending ? t.findRevision : t.freeze}</Button><Button variant="outline" onClick={() => void data.open()} disabled={!data.name}>{t.openRevision}</Button></div>
  {data.frozen && <details className="frozen-revision" open><summary>已冻结 <span data-ui-data>{data.frozen.name} / {data.frozen.version}</span></summary><code id="dataset-fingerprint" data-ui-data>{data.frozen.fingerprint}</code></details>}
  {data.notice && <p className="notice" role="status">{data.notice}</p>}
 </section>;
}
