import { ui } from "../lib/copy";
import { ToolProvenance } from "./ToolProvenance";
import { Button } from "./ui/button";
import type { RunState } from "../hooks/use-run";
export function ApprovalPanel({ run }: {
    run: RunState;
}) {
    if (!run.approvals.length)
        return null;
    return <div id="approval" className="approval-list">{run.approvals.map(approval => <section className="approval" key={String(approval.request_id)} aria-label={ui.pendingCall}>
  <div className="approval-title"><span>{ui.pending}</span><strong data-ui-data>{String(approval.name)}</strong></div>
  <ToolProvenance value={approval}/>
  <pre data-ui-data className="approval-arguments">{JSON.stringify(approval.arguments, null, 2)}</pre>
  <div className="approval-actions"><Button disabled={run.replying} aria-label={"允许 " + String(approval.name) + " 本次调用"} onClick={() => void run.reply(approval, true)}>{ui.allow}</Button><Button variant="outline" disabled={run.replying} aria-label={"拒绝 " + String(approval.name) + " 本次调用"} onClick={() => void run.reply(approval, false)}>{ui.deny}</Button></div>
 </section>)}</div>;
}
