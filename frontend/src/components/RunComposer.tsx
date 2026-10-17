import { ui } from "../lib/copy";
import { Button } from "./ui/button";
import { Label } from "./ui/label";
import { Textarea } from "./ui/textarea";
import type { RunState } from "../hooks/use-run";
export function RunComposer({ run, onConnect }: {
    run: RunState;
    onConnect: () => void;
}) {
    const notice = run.notice?.area === "composer" ? run.notice : run.pending && !run.submitting ? { text: ui.unknownCreate, code: null } : null;
    return <section className="composer" aria-labelledby="task-heading"><h2 id="task-heading">{ui.task}</h2>
  <form onSubmit={e => { e.preventDefault(); void run.submit(); }}>
   <Label htmlFor="prompt" className="sr-only">{ui.task}</Label>
   <Textarea id="prompt" placeholder={ui.input} value={run.draft} readOnly={run.pending} aria-invalid={notice?.code === "invalid_input" || notice?.code === "input_limit"} onChange={e => run.setDraft(e.target.value)} className="task-input"/>
   <div className="composer-actions"><Button id="send" type="submit" disabled={run.submitting}>{run.submitting ? ui.processing : run.pending ? ui.find : ui.create}</Button></div>
  </form>
  {notice && <div className="notice" role="status"><p>{notice.text}</p>{notice.code === "unauthorized" && <Button variant="outline" onClick={onConnect}>{ui.editKey}</Button>}{notice.code === "capacity" && <Button variant="outline" onClick={() => void run.submit()} disabled={run.submitting}>{ui.retry}</Button>}</div>}
 </section>;
}
