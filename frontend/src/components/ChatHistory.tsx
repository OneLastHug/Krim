import { Button } from "./ui/button";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "./ui/tabs";
import { object } from "../api";
import { chatCopy as c, chatReason, turnLabels } from "../lib/copy";
import type { SessionState } from "../hooks/use-session";

function records(raw: unknown) { return Array.isArray(raw) ? raw.map(object) : []; }
export function ChatHistory({ chat }: { chat: SessionState }) {
  return <div className="chat-transcript" aria-label={c.history}>
    {!chat.history.length && <p className="chat-empty">{chat.session ? "尚无消息" : c.empty}</p>}
    {chat.history.map(turn => {
      const answer = turn.transcript?.filter(m => m.role === "assistant").at(-1)?.content ?? "";
      const detail = chat.details[turn.id];
      const current = turn.id === chat.currentTurn?.id;
      const lastStep = Math.max(-1, ...chat.events.filter(e => e.kind === "text").map(e => Number(object(e.data).step_index)));
      const streaming = current && turn.status === "running" ? chat.events.filter(e => e.kind === "text" && Number(object(e.data).step_index) === lastStep).map(e => String(object(e.data).text)).join("") : "";
      return <article className="chat-turn" key={turn.id} data-turn-id={turn.id}>
        <div className="chat-turn-meta"><span>你</span><span data-ui-data className="mono">{turn.id.slice(0, 8)}</span><span role="status">{turnLabels[turn.status]}</span>
          {["queued", "running"].includes(turn.status) && <Button variant="ghost" disabled={chat.controlling || !chat.canWrite} onClick={() => void chat.control(turn, "cancel")}>停止此轮</Button>}
        </div>
        <pre className="chat-user-text" data-ui-data>{turn.text}</pre>
        {current && chat.approvals.map(item => <section className="approval chat-approval" key={String(item.request_id)} aria-label="等待审批"><div className="approval-title"><strong data-ui-data>{String(item.name)}</strong><span>等待审批</span></div><pre data-ui-data className="approval-arguments">{JSON.stringify(item.arguments, null, 2)}</pre><div className="approval-actions"><Button disabled={!chat.canApprove || chat.controlling || chat.pendingControls.some(saved => saved.target.endsWith("/approvals/" + encodeURIComponent(String(item.request_id))))} onClick={() => void chat.control(turn, "approval", item, true)} aria-label={"允许 " + String(item.name) + " 本次调用"}>允许本次调用</Button><Button variant="outline" disabled={!chat.canApprove || chat.controlling || chat.pendingControls.some(saved => saved.target.endsWith("/approvals/" + encodeURIComponent(String(item.request_id))))} onClick={() => void chat.control(turn, "approval", item, false)}>拒绝</Button></div></section>)}
        {(answer || streaming) && <div className="chat-assistant"><span className="chat-speaker">Krim Agent</span><pre className="chat-answer" data-ui-data>{answer || streaming}</pre></div>}
        {turn.code && <p className="notice" data-ui-data>{chatReason({ code: turn.code })}<span className="mono"> {turn.code}</span></p>}
        {turn.run_id && <details className="chat-detail" onToggle={event => { if (event.currentTarget.open) void chat.loadDetails(turn); }}><summary>{c.details}</summary>
          <p className="mono" data-ui-data>{turn.run_id}</p>
          {detail && <Tabs defaultValue="tools"><TabsList variant="line" aria-label="消息详情"><TabsTrigger value="tools">工具</TabsTrigger><TabsTrigger value="trace">Trace</TabsTrigger></TabsList>
            <TabsContent value="tools">{records(detail.facts?.tools).map(tool => <section className="tool" key={String(tool.call_id)}><header><strong data-ui-data>{String(tool.name)}</strong><span>{String(tool.status)}</span></header><pre data-ui-data>{JSON.stringify(tool.arguments, null, 2)}</pre>{tool.result_available === true ? <pre data-ui-data>{JSON.stringify(tool.value, null, 2)}</pre> : <span>结果未知</span>}</section>)}</TabsContent>
            <TabsContent value="trace"><span>{detail.trace.complete === true ? "记录完整" : "记录不完整"}</span>{records(detail.trace.spans).map(span => <div className="span-row" key={String(span.span_id)}><strong data-ui-data>{String(span.kind)}</strong><span className="mono" data-ui-data>{String(span.span_id)}</span><span>{span.end == null ? "未结束" : String(span.status)}</span></div>)}<details><summary>原始记录</summary><pre data-ui-data>{JSON.stringify(detail.trace, null, 2)}</pre></details></TabsContent>
          </Tabs>}
        </details>}
      </article>;
    })}
  </div>;
}
