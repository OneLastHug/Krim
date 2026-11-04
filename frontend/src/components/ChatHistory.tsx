import { useEffect, useLayoutEffect, useRef, useState } from "react";
import { ArrowDown, Copy, MoreHorizontal, Pencil, Square } from "lucide-react";
import { Button } from "./ui/button";
import { chatCopy as c, chatReason, turnLabels } from "../lib/copy";
import type { SessionState } from "../hooks/use-session";
import { useCopyFeedback } from "../hooks/use-copy-feedback";
import { projectTurn } from "../lib/session-presentation";
import { turnPath } from "../lib/session-contracts";
import { MessageMarkdown } from "./messages/MessageMarkdown";
import { CopyFeedback } from "./messages/CopyFeedback";
import { ToolCallCard } from "./ToolCallCard";

const EMPTY_EVENTS: Record<string, unknown>[] = [];

export function ChatHistory({ chat, onTrace }: { chat: SessionState; onTrace?: (turnId: string) => void }) {
  const [expanded, setExpanded] = useState<Record<string, boolean>>({});
  const [showJump, setShowJump] = useState(false);
  const feedback = useCopyFeedback();
  const scroll = useRef<HTMLDivElement>(null);
  const thread = useRef<HTMLDivElement>(null);
  const stickToBottom = useRef(true);
  const selected = useRef(chat.id);
  const refreshedDetails = useRef(new Set<string>());

  function bottom() {
    stickToBottom.current = true;
    setShowJump(false);
    if (scroll.current) scroll.current.scrollTop = scroll.current.scrollHeight;
  }
  function reuse(text: string) {
    chat.edit(text);
    document.getElementById("chat-prompt")?.focus();
  }

  useLayoutEffect(() => {
    if (selected.current !== chat.id) { selected.current = chat.id; stickToBottom.current = true; setExpanded({}); }
    if (stickToBottom.current && scroll.current) scroll.current.scrollTop = scroll.current.scrollHeight;
  }, [chat.id, chat.history, chat.events, chat.turnEvents, chat.details, chat.turnFacts]);

  useEffect(() => {
    if (!chat.ready) { refreshedDetails.current.clear(); setExpanded({}); }
  }, [chat.ready]);

  useEffect(() => {
    for (const turn of chat.history) {
      const detail = chat.details[turn.id];
      const key = `${chat.workspace}:${chat.id}:${turn.id}:${turn.status}`;
      if (expanded[turn.id] && !["queued", "running"].includes(turn.status) && chat.turnFacts?.[turn.id] && detail?.traceLoaded === false && !refreshedDetails.current.has(key)) {
        refreshedDetails.current.add(key);
        void chat.loadDetails(turn);
      }
    }
  }, [chat.history, chat.turnFacts, chat.details, expanded, chat.id, chat.workspace]);

  useEffect(() => {
    const panel = scroll.current, content = thread.current;
    if (!panel || !content || typeof ResizeObserver === "undefined") return;
    const resize = () => {
      if (stickToBottom.current) panel.scrollTop = panel.scrollHeight;
      setShowJump(panel.scrollHeight - panel.clientHeight - panel.scrollTop > 48);
    };
    const observer = new ResizeObserver(resize);
    observer.observe(panel);
    observer.observe(content);
    document.fonts?.addEventListener("loadingdone", resize);
    return () => { observer.disconnect(); document.fonts?.removeEventListener("loadingdone", resize); };
  }, [chat.id]);

  return <div className="chat-history-shell">
    <div className="chat-transcript" aria-label={c.history} ref={scroll} onScroll={() => {
      const panel = scroll.current;
      if (!panel) return;
      const atBottom = panel.scrollHeight - panel.scrollTop - panel.clientHeight < 48;
      stickToBottom.current = atBottom;
      setShowJump(!atBottom);
    }}>
      <div className="chat-thread" ref={thread}>
        {!chat.history.length && <div className="chat-empty"><h2>{chat.session ? "发送第一条消息" : "选择或新建对话"}</h2></div>}
        {chat.history.map(turn => {
          const detail = chat.details[turn.id];
          const events = chat.turnEvents?.[turn.id] ?? (turn.id === chat.currentTurn?.id ? chat.events : EMPTY_EVENTS);
          const items = projectTurn(turn, events, chat.approvals, { events: detail?.events, facts: chat.turnFacts?.[turn.id] ?? detail?.facts });
          const lastAnswer = items.filter(item => item.type === "assistant").at(-1);
          const waitingApproval = items.some(item => item.type === "tool" && item.approval);
          const working = turn.status === "running" || turn.status === "queued";
          const moreOpen = expanded[turn.id] === true;
          const canReuse = chat.canWrite && chat.session?.status === "open" && !chat.turnPending;
          return <article className="chat-turn" key={turn.id} data-turn-id={turn.id}>
            <div className="chat-user-message">
              <div className="chat-user-text" data-ui-data>{turn.text}</div>
              <div className="chat-user-actions">
                <Button variant="ghost" size="icon-sm" className="chat-message-action" aria-label="复制用户消息" title="复制用户消息" onClick={() => void feedback.copy(turn.text)}><Copy aria-hidden="true" /></Button>
                <Button variant="ghost" size="icon-sm" className="chat-message-action" aria-label="复用提问" title="复用提问" disabled={!canReuse} onClick={() => reuse(turn.text)}><Pencil aria-hidden="true" /></Button>
              </div>
            </div>
            <div className="chat-turn-meta">
              {turn.status !== "completed" && <span role="status">{waitingApproval ? "等待审批" : turnLabels[turn.status] ?? turn.status}</span>}
              {working && <Button variant="ghost" size="sm" disabled={chat.controlling || !chat.canWrite} onClick={() => void chat.control(turn, "cancel")}><Square aria-hidden="true" />停止此轮</Button>}
            </div>
            {!working && items.some(item => item.type === "assistant" && item.partial) && <p className="chat-partial-note">以下为已生成部分</p>}
            {items.map(item => {
              if (item.type === "assistant") return <div className="chat-assistant" key={item.key}>
                <div className={item.key === lastAnswer?.key ? "chat-answer" : "chat-step-text"} data-ui-data><MessageMarkdown text={item.text} onCopy={feedback.copy} /></div>
              </div>;
              const approval = item.approval;
              const target = approval ? turnPath(chat.workspace, chat.id, turn.id) + "/approvals/" + encodeURIComponent(approval.request_id) : "";
              const pending = !!target && chat.pendingControls.some(saved => saved.target === target);
              return <ToolCallCard key={item.key} tool={item} canApprove={chat.canApprove} busy={chat.controlling} pending={pending} onInspect={() => { stickToBottom.current = false; void chat.loadDetails(turn); }} onDecision={allow => { if (approval) void chat.control(turn, "approval", approval, allow); }} />;
            })}
            {turn.status === "running" && !items.length && <p className="chat-waiting" role="status">正在处理</p>}
            {turn.code && <p className="notice chat-turn-notice" data-ui-data>{chatReason({ code: turn.code })}<code>{turn.code}</code></p>}
            <div className="chat-answer-actions">
              {lastAnswer && <Button variant="ghost" size="icon-sm" className="chat-message-action" aria-label="复制回答" title="复制回答" onClick={() => void feedback.copy(lastAnswer.text)}><Copy aria-hidden="true" /></Button>}
              {turn.run_id && <Button variant="ghost" size="icon-sm" className="chat-message-action" aria-label="更多操作" title="运行详情" aria-expanded={moreOpen} onClick={() => {
                stickToBottom.current = false;
                setExpanded(value => ({ ...value, [turn.id]: !moreOpen }));
                if (!moreOpen) void chat.loadDetails(turn);
              }}><MoreHorizontal aria-hidden="true" /></Button>}
            </div>
            {moreOpen && turn.run_id && <section className="chat-detail" aria-label="运行详情">
              {detail?.traceLoaded ? <p className="detail-empty">{detail.trace.complete === true ? "Trace 记录完整" : "Trace 记录不完整"}</p> : <Button variant="ghost" onClick={() => void chat.loadDetails(turn)}>读取运行详情</Button>}
              {onTrace && <Button variant="outline" onClick={() => onTrace(turn.id)}>查看 Trace</Button>}
            </section>}
          </article>;
        })}
      </div>
    </div>
    {showJump && <Button variant="outline" size="icon-sm" className="chat-jump" aria-label="回到底部" onClick={bottom}><ArrowDown aria-hidden="true" /></Button>}
    <CopyFeedback {...feedback} />
  </div>;
}
