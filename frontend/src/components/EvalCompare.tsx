import { object } from "../api";
import { evalUi as t } from "../lib/copy";
import type { EvaluationState } from "../hooks/use-evaluation";
import { Button } from "./ui/button";
import { Input } from "./ui/input";
import { Label } from "./ui/label";
const codes: Record<string, string> = { passed_to_nonpassed: "样本由通过变为未通过", score_regression: "样本分数下降", candidate_not_all_passed: "候选存在未通过样本", incomplete_coverage: "报告未完整评分", report_invalid: "报告无效", read_failed: "报告读取未完成" };
export function EvalCompare({ evaluation: e }: {
    evaluation: EvaluationState;
}) {
    const status = e.gate ? String(e.gate.status) : "";
    return <section className="eval-compare" aria-labelledby="compare-heading"><h2 id="compare-heading">{t.compare}</h2><div className="eval-two"><div><Label htmlFor="baseline-id">{t.baseline}</Label><Input id="baseline-id" value={e.pair.baseline} onChange={event => e.changePair("baseline", event.target.value)}/></div><div><Label htmlFor="candidate-id">{t.candidate}</Label><Input id="candidate-id" value={e.pair.candidate} onChange={event => e.changePair("candidate", event.target.value)}/></div></div><Button id="compare" onClick={() => void e.compare()} disabled={e.comparing || !e.pair.baseline || !e.pair.candidate}>{e.comparing ? "读取中…" : t.compareAction}</Button>
  {e.gate && <div className="gate-result"><p id="gate-status" role="status">{({ pass: "通过", fail: "退化", invalid: "不可比较" } as Record<string, string>)[status]}</p><ul>{(Array.isArray(e.gate.reasons) ? e.gate.reasons : []).map((raw, index) => { const row = object(raw); return <li key={index}><span>{codes[String(row.code)] ?? "比较配置不一致"}</span>{typeof row.case_id === "string" && <code data-ui-data>{row.case_id} #{Number(row.repetition) + 1}</code>}{typeof row.rule === "string" && <span data-ui-data>{row.rule}</span>}</li>; })}</ul></div>}
 </section>;
}
