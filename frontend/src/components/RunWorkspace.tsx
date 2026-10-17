import { ui } from "../lib/copy";
import { useState } from "react";
import { RefreshCw, Square } from "lucide-react";
import { object } from "../api";
import { labels, runReason } from "../lib/copy";
import { Button } from "./ui/button";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "./ui/tabs";
import { ApprovalPanel } from "./ApprovalPanel";
import type { RunState } from "../hooks/use-run";
function records(value: unknown) { return Array.isArray(value) ? value.map(object) : []; }
function raw(value: unknown) { return <details className="raw"><summary>{ui.raw}</summary><pre data-ui-data>{JSON.stringify(value, null, 2)}</pre></details>; }
export function RunWorkspace({ run, onCandidate }: {
    run: RunState;
    onCandidate?: (id: string) => void;
}) {
    const [tab, setTab] = useState("result");
    const view = run.view;
    const tools = records(run.facts?.tools), spans = records(run.trace?.spans);
    const status = run.reading ? ui.reading : run.approvals.length ? ui.pending : view ? labels[view.status] : run.id ? ui.unread : "";
    const toolStatus: Record<string, string> = { ok: "已完成", denied: "已拒绝", failed: "失败", cancelled: "已停止", unknown: ui.unknownExecution };
    return <section className="workspace" aria-labelledby="workspace-heading">
  <div className="workspace-title"><div><h2 id="workspace-heading">{ui.current}</h2>{status && <p id="status" className="run-status" role="status">{status}</p>}</div>
   {run.id && <div className="run-actions"><Button variant="ghost" size="icon" aria-label={ui.refresh} onClick={run.refresh}><RefreshCw aria-hidden="true"/></Button><Button id="cancel" variant="outline" disabled={run.reading || run.stopping || !view || !["queued", "running"].includes(view.status)} onClick={() => void run.stop()}><Square aria-hidden="true"/>{run.stopping ? ui.stopping : ui.stop}</Button></div>}
  </div>
  {!run.id ? <div className="empty-result"><p id="result">{ui.empty}</p></div> : <>
   <details className="run-identity"><summary data-ui-data>{run.id.slice(0, 8)}</summary><p id="run-id" data-ui-data>{run.id}</p></details>
   {view && !["queued", "running"].includes(view.status) && onCandidate && <Button variant="ghost" className="choose-source" onClick={() => onCandidate(run.id)}>选作样本</Button>}
   {run.notice && run.notice.area !== "composer" && <p className="notice" role="status">{run.notice.text}</p>}
   {view?.status === "failed" && <p className="notice">{runReason(view.code)}</p>}
   <ApprovalPanel run={run}/>
   <Tabs value={tab} onValueChange={value => setTab(String(value))} className="run-tabs">
    <TabsList variant="line" aria-label={ui.details} className="detail-tabs"><TabsTrigger value="result">{ui.result}</TabsTrigger><TabsTrigger value="tools">{ui.tools}</TabsTrigger><TabsTrigger value="trace">{ui.trace}</TabsTrigger><TabsTrigger value="events">{ui.events}</TabsTrigger></TabsList>
    <TabsContent value="result"><pre id="result" className="result-text" data-ui-data>{run.facts ? String(run.facts.text ?? "") : ""}</pre></TabsContent>
    <TabsContent value="tools"><div id="tools" className="tool-list">{tools.length ? tools.map((tool, index) => <article className="tool" key={String(tool.call_id ?? index)}><header><strong data-ui-data>{String(tool.name)}</strong><span>{toolStatus[String(tool.status)] ?? ui.unknown}</span></header><details><summary>{ui.arguments}</summary><pre data-ui-data>{JSON.stringify(tool.arguments, null, 2)}</pre></details>{tool.result_available === true ? <pre data-ui-data>{typeof tool.value === "string" ? tool.value : JSON.stringify(tool.value, null, 2)}</pre> : <p className="muted">{tool.result_available === false ? ui.noResult : ui.unknownResult}</p>}{typeof tool.code === "string" && <p className="muted" data-ui-data>{String(tool.code)}</p>}</article>) : <p className="detail-empty">{ui.noTools}</p>}</div></TabsContent>
    <TabsContent value="trace"><div id="trace">{run.trace && <p id="trace-state" className="muted">{run.trace.complete === true ? ui.completeRecord : ui.incompleteRecord}</p>}{spans.map(span => <article className="span-row" key={String(span.span_id)}><div><strong>{({ run: ui.task, model: "模型", tool: ui.tools } as Record<string, string>)[String(span.kind)] ?? String(span.kind)}</strong><span data-ui-data className="mono">{String(span.span_id)}</span></div><span>{span.end === null ? ui.notEnded : span.status === "ok" ? "已完成" : String(span.status)}</span></article>)}{run.trace && raw(run.trace)}</div></TabsContent>
    <TabsContent value="events"><ol id="events" className="event-list">{run.events.map(event => <li key={Number(event.seq)} data-ui-data><span className="mono">{String(event.seq).padStart(2, "0")}</span><span>{String(event.kind)}</span></li>)}</ol></TabsContent>
   </Tabs>
   {run.notice?.code && <details className="diagnostic"><summary>{ui.errorDetails}</summary><code>{run.notice.code}</code></details>}
  </>}
 </section>;
}
