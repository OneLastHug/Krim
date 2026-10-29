import { useEffect, useState } from "react";
import { evalUi as t, targetLabel } from "../lib/copy";
import { useDataset } from "../hooks/use-dataset";
import { useEvaluation } from "../hooks/use-evaluation";
import { CaseEditor } from "./CaseEditor";
import { EvalReport } from "./EvalReport";
import { EvalCompare } from "./EvalCompare";
import { Button } from "./ui/button";
import { Input } from "./ui/input";
import { Label } from "./ui/label";
export function EvaluationWorkbench({ apiKey, active, sourceRun, onRun, beforeSelect }: {
    apiKey: string;
    active: boolean;
    sourceRun: string;
    onRun: (id: string) => void;
    beforeSelect?: (id: string) => void;
}) {
    const data = useDataset(apiKey, sourceRun), evaluation = useEvaluation(apiKey, active);
    const [openId, setOpenId] = useState("");
    const [editorOpen, setEditorOpen] = useState(false);
    useEffect(() => { if (sourceRun)
        setEditorOpen(true); }, [sourceRun]);
    const focused = Boolean(evaluation.id || evaluation.pending || evaluation.busy);
    return <section hidden={!active} className="evaluation-workbench"><div className={"eval-layout" + (focused ? " eval-focused" : "") + (editorOpen ? " eval-editing" : "")}><div className="eval-editor">{focused && <Button id="eval-editor-toggle" className="eval-editor-toggle" variant="outline" aria-expanded={editorOpen} aria-controls="eval-case-editor" onClick={() => setEditorOpen(open => !open)}>{editorOpen ? "收起编辑" : "编辑数据"}</Button>}<div id="eval-case-editor"><CaseEditor data={data}/></div></div><div className="evaluation-results"><section className="eval-controls" aria-label="评测操作"><div className="eval-two"><div><Label htmlFor="eval-target">{t.target}</Label><select id="eval-target" value={evaluation.target} disabled={evaluation.pending} onChange={e => evaluation.setTarget(e.target.value)}>{evaluation.targets.map(target => <option key={String(target.key)} value={String(target.key)}>{targetLabel(target.key)}</option>)}</select></div><div><Label htmlFor="eval-repetitions">{t.repeat}</Label><Input id="eval-repetitions" type="number" min={1} max={3} value={evaluation.repetitions} disabled={evaluation.pending} onChange={e => {
            const n = Number(e.target.value);
            if (Number.isInteger(n) && n >= 1 && n <= 3)
                evaluation.setRepetitions(n);
        }}/></div></div>
   <fieldset className="rule-options"><legend>{t.rules}</legend>{evaluation.rules.map(rule => <label key={rule.name} htmlFor={"rule-" + rule.name}><input type="checkbox" id={"rule-" + rule.name} checked={evaluation.selected.includes(rule.name)} disabled={evaluation.pending || rule.name === "status_code"} onChange={e => evaluation.setSelected(old => e.target.checked ? [...old, rule.name] : old.filter(n => n !== rule.name))}/>{rule.name}</label>)}</fieldset>
   {!evaluation.pending && !evaluation.busy && <Button id="eval-start" disabled={!data.frozen || evaluation.targets.length === 0} onClick={() => { beforeSelect?.(evaluation.id); void evaluation.start(data.frozen); }}>{t.start}</Button>}
   <div className="open-evaluation"><Label htmlFor="open-eval-id">{t.id}</Label><div className="eval-inline"><Input id="open-eval-id" value={openId} onChange={e => setOpenId(e.target.value)}/><Button variant="outline" disabled={!openId.trim()} onClick={() => { beforeSelect?.(evaluation.id); evaluation.select(openId.trim()); }}>{t.open}</Button></div></div>
  </section>{(evaluation.pending || evaluation.busy) && <section className="eval-recovery" aria-label="待确认评测"><p id="eval-pending-state" role="status">{evaluation.busy ? "处理中…" : "评测创建结果未确认"}</p>{evaluation.pendingKey && <code id="eval-request-key" data-ui-data>{evaluation.pendingKey}</code>}<Button id="eval-start" disabled={evaluation.busy} onClick={() => { beforeSelect?.(evaluation.id); void evaluation.start(data.frozen); }}>{evaluation.busy ? "处理中…" : t.findEval}</Button></section>}<EvalReport evaluation={evaluation} onRun={onRun}/><EvalCompare evaluation={evaluation}/>{evaluation.notice && <p className="notice eval-notice" role="status">{evaluation.notice}</p>}</div></div></section>;
}
