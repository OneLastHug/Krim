import { useState } from "react";
import { Button } from "./ui/button";
import { object } from "../api";
import { chatCopy as c, chatReason, turnLabels } from "../lib/copy";
import type { SessionState } from "../hooks/use-session";

function records(raw: unknown) { return Array.isArray(raw) ? raw.map(object) : []; }
export function ChatHistory({ chat, onTrace }: { chat: SessionState; onTrace?: (turnId: string) => void }) {
  const [expanded, setExpanded] = useState<Record<string, boolean>>({});
  return <div className="chat-transcript" aria-label={c.history}>
    {!chat.history.length && <p className="chat-empty">{chat.session ? "尚无消息" : c.empty}</p>}
    {chat.history.map(turn => {
      const answer = turn.transcript?.filter(m => m.role === "assistant").at(-1)?.content ?? "";
      const detail = chat.details[turn.id];
      const current = turn.id === chat.currentTurn?.id;
      const lastStep = Math.max(-1, ...chat.events.filter(e => e.kind === "text").map(e => Number(object(e.data).step_index)));
      const streaming = current && turn.status === "running" ? chat.events.filter(e => e.kind === "text" && Number(object(e.data).step_index) === lastStep).map(e => String(object(e.data).text)).join("") : "";
      const moreOpen = expanded[turn.id] === true;
      return <article className="chat-turn" key={turn.id} data-turn-id={turn.id}>
        <div className="chat-turn-meta">
          {turn.status !== "completed" && <span role="status">{turnLabels[turn.status] ?? turn.status}</span>}
          {["queued", "running"].includes(turn.status) && <Button variant="ghost" disabled={chat.controlling || !chat.canWrite} onClick={() => void chat.control(turn, "cancel")}>停止此轮</Button>}
          {turn.run_id && <Button variant="ghost" aria-expanded={moreOpen} onClick={() => {
            setExpanded(value => ({ ...value, [turn.id]: !moreOpen }));
            if (!moreOpen && !detail) void chat.loadDetails(turn);
          }}>更多操作</Button>}
        </div>
        <pre className="chat-user-text" data-ui-data>{turn.text}</pre>
        {current && chat.approvals.map(item => <section className="approval chat-approval" key={String(item.request_id)} aria-label="等待审批"><div className="approval-title"><strong data-ui-data>{String(item.name)}</strong><span>等待审批</span></div><pre data-ui-data className="approval-arguments">{JSON.stringify(item.arguments, null, 2)}</pre><div className="approval-actions"><Button disabled={!chat.canApprove || chat.controlling || chat.pendingControls.some(saved => saved.target.endsWith("/approvals/" + encodeURIComponent(String(item.request_id))))} onClick={() => void chat.control(turn, "approval", item, true)} aria-label={"允许 " + String(item.name) + " 本次调用"}>允许本次调用</Button><Button variant="outline" disabled={!chat.canApprove || chat.controlling || chat.pendingControls.some(saved => saved.target.endsWith("/approvals/" + encodeURIComponent(String(item.request_id))))} onClick={() => void chat.control(turn, "approval", item, false)}>拒绝</Button></div></section>)}
        {(answer || streaming) && <div className="chat-assistant"><span className="chat-speaker">Krim Agent</span><pre className="chat-answer" data-ui-data>{answer || streaming}</pre></div>}
        {turn.code && <p className="notice" data-ui-data>{chatReason({ code: turn.code })}<span className="mono"> {turn.code}</span></p>}
        {moreOpen && turn.run_id && <section className="chat-detail" aria-label="运行详情">
          <p className="chat-run-reference">运行记录可在 Trace 中查看。</p>
          {detail ? <>
            <h3>工具摘要</h3>
            {records(detail.facts?.tools).length ? records(detail.facts?.tools).map(tool => <section className="tool" key={String(tool.call_id)}>
              <header><strong data-ui-data>{String(tool.name)}</strong><span>{String(tool.status)}</span></header>
              <pre data-ui-data>{JSON.stringify(tool.arguments, null, 2)}</pre>
              {tool.result_available === true ? <pre data-ui-data>{JSON.stringify(tool.value, null, 2)}</pre> : <span role="status">结果未知</span>}
            </section>) : <p className="detail-empty">暂无工具调用</p>}
            <p className="detail-empty">{detail.trace.complete === true ? "Trace 记录完整" : "Trace 记录不完整"}</p>
          </> : <p className="detail-empty" role="status">正在读取工具摘要</p>}
          {onTrace && <Button variant="outline" onClick={() => onTrace(turn.id)}>查看 Trace</Button>}
        </section>}
      </article>;
    })}
  </div>;
}
