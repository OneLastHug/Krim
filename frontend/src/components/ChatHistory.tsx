import { useEffect, useLayoutEffect, useRef, useState, type ReactNode } from "react";
import { ArrowDown, Ban, CircleAlert, Clock, Eye, Copy, LoaderCircle, MoreHorizontal, Pencil, Shield, Square } from "lucide-react";
import { Button } from "./ui/button";
import { chatCopy as c, runReason, turnLabels } from "../lib/copy";
import type { SessionState } from "../hooks/use-session";
import { useCopyFeedback } from "../hooks/use-copy-feedback";
import { projectTurn, type ToolPresentation, type TurnPresentation } from "../lib/session-presentation";
import { groupFileTools } from "../lib/file-tool-groups";
import { FileToolGroupCard } from "./FileToolGroupCard";
import { turnPath, type Turn } from "../lib/session-contracts";
import { MessageMarkdown } from "./messages/MessageMarkdown";
import { CopyFeedback } from "./messages/CopyFeedback";
import { ToolCallCard } from "./ToolCallCard";

const EMPTY_EVENTS: Record<string, unknown>[] = [];

export function ChatHistory({ chat, onTrace, onOpenSubagent, children, empty }: { chat: SessionState; onTrace?: (turnId: string) => void; onOpenSubagent?: (agentId: string) => void; children?: ReactNode; empty?: ReactNode }) {
  const [expanded, setExpanded] = useState<Record<string, boolean>>({});
  const [showJump, setShowJump] = useState(false);
  const [pendingCount, setPendingCount] = useState(0);
  const feedback = useCopyFeedback();
  const scroll = useRef<HTMLDivElement>(null);
  const thread = useRef<HTMLDivElement>(null);
  const stickToBottom = useRef(true);
  const lastScrollTop = useRef(0);
  const syncScroll = useRef<(() => void) | null>(null);
  const selected = useRef(chat.id);
  const refreshedDetails = useRef(new Set<string>());
  const projections = useRef(new Map<string, { turn: Turn; events: unknown[]; approvals: unknown[]; detailEvents: unknown[] | undefined; facts: Record<string, unknown> | null | undefined; items: TurnPresentation[] }>());
  useEffect(() => {
    const ids = new Set(chat.history.map(turn => turn.id));
    for (const id of projections.current.keys()) if (!ids.has(id)) projections.current.delete(id);
  }, [chat.history]);

  function bottom() {
    stickToBottom.current = true;
    setShowJump(false);
    if (scroll.current) {
      const panel = scroll.current;
      panel.scrollTop = Math.max(0, panel.scrollHeight - panel.clientHeight);
      lastScrollTop.current = panel.scrollTop;
    }
  }
  function reuse(text: string) {
    chat.edit(text);
    document.getElementById("chat-prompt")?.focus();
  }

  useLayoutEffect(() => {
    if (selected.current !== chat.id) { selected.current = chat.id; stickToBottom.current = true; setExpanded({}); }
    if (stickToBottom.current && scroll.current) {
      const panel = scroll.current;
      panel.scrollTop = Math.max(0, panel.scrollHeight - panel.clientHeight);
      lastScrollTop.current = panel.scrollTop;
    }
  }, [chat.id, chat.history.length]);

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
    if (!panel || !content) return;
    let frame: number | undefined, timer: ReturnType<typeof setTimeout> | undefined;
    const resize = () => {
      const maximum = Math.max(0, panel.scrollHeight - panel.clientHeight);
      if (stickToBottom.current && Math.abs(panel.scrollTop - maximum) > 1) panel.scrollTop = maximum;
      lastScrollTop.current = panel.scrollTop;
      const visible = maximum - panel.scrollTop > 48;
      setShowJump(previous => previous === visible ? previous : visible);
    };
    const cancel = () => { if (frame !== undefined) cancelAnimationFrame(frame); clearTimeout(timer); frame = undefined; timer = undefined; };
    const schedule = () => {
      if (timer !== undefined) return;
      const commit = () => { cancel(); resize(); };
      if (typeof requestAnimationFrame === "function") frame = requestAnimationFrame(commit);
      timer = setTimeout(commit, 50);
    };
    syncScroll.current = schedule;
    const observer = typeof ResizeObserver === "undefined" ? null : new ResizeObserver(resize);
    observer?.observe(panel); observer?.observe(content);
    document.fonts?.addEventListener("loadingdone", schedule);
    return () => { observer?.disconnect(); cancel(); syncScroll.current = null; document.fonts?.removeEventListener("loadingdone", schedule); };
  }, [chat.id]);
  useEffect(() => { syncScroll.current?.(); }, [chat.events, chat.turnEvents, chat.details, chat.turnFacts]);

  useEffect(() => {
    const content = thread.current;
    if (!content) return;
    const selector = '.chat-approval,[data-pending-interaction="true"]';
    const update = () => { const count = content.querySelectorAll(selector).length; setPendingCount(previous => previous === count ? previous : count); };
    const relevant = (node: Node) => node instanceof Element && (node.matches(selector) || node.querySelector(selector) !== null);
    update();
    const observer = new MutationObserver(records => {
      if (records.some(record => record.type === "attributes" || [...record.addedNodes, ...record.removedNodes].some(relevant))) update();
    });
    observer.observe(content, { childList: true, subtree: true, attributes: true, attributeFilter: ["data-pending-interaction"] });
    return () => observer.disconnect();
  }, [chat.id]);
  useEffect(() => {
    const focusTurn = () => {
      if (new URL(location.href).searchParams.get("view") !== "chat") return;
      const id = new URL(location.href).searchParams.get("turn");
      if (!id || !scroll.current) return;
      const target = [...scroll.current.querySelectorAll<HTMLElement>("[data-turn-id]")].find(element => element.dataset.turnId === id);
      if (!target) return;
      const focused = chat.history.find(turn => turn.id === id);
      const latest = chat.history.at(-1);
      if (focused && focused.id === latest?.id && (focused.status === "queued" || focused.status === "running")) {
        // A submit receipt updates ?turn= too; keep following the live answer.
        stickToBottom.current = true; setShowJump(false);
        const panel = scroll.current;
        panel.scrollTop = Math.max(0, panel.scrollHeight - panel.clientHeight);
        lastScrollTop.current = panel.scrollTop;
        return;
      }
      stickToBottom.current = false;
      const panel = scroll.current, row = target.getBoundingClientRect(), viewport = panel.getBoundingClientRect();
      // Limit navigation to the transcript; scrollIntoView also scrolls overflow-hidden ancestors.
      panel.scrollTop += row.top - viewport.top - Math.max(0, (panel.clientHeight - row.height) / 2);
      lastScrollTop.current = panel.scrollTop;
    };
    focusTurn();
    window.addEventListener("krim:focus-turn", focusTurn); window.addEventListener("popstate", focusTurn);
    return () => { window.removeEventListener("krim:focus-turn", focusTurn); window.removeEventListener("popstate", focusTurn); };
  }, [chat.id, chat.history.length]);
  return <div className="chat-history-shell">
    <div className="chat-transcript" aria-label={c.history} ref={scroll} onScroll={() => {
      const panel = scroll.current;
      if (!panel) return;
      const atBottom = panel.scrollHeight - panel.scrollTop - panel.clientHeight < 48;
      if (atBottom) stickToBottom.current = true;
      else if (panel.scrollTop < lastScrollTop.current - 1) stickToBottom.current = false;
      lastScrollTop.current = panel.scrollTop;
      if (stickToBottom.current && !atBottom) syncScroll.current?.();
      else setShowJump(previous => previous === !atBottom ? previous : !atBottom);
    }}>
      <div className="chat-thread" ref={thread}>
        {!chat.history.length && (empty ?? <div className="chat-empty"><h2>{chat.session ? "发送第一条消息" : "选择或新建对话"}</h2></div>)}
        {chat.history.map(turn => {
          const detail = chat.details[turn.id];
          const events = chat.turnEvents?.[turn.id] ?? (turn.id === chat.currentTurn?.id ? chat.events : EMPTY_EVENTS);
          const facts = chat.turnFacts?.[turn.id] ?? detail?.facts;
          const cached = projections.current.get(turn.id);
          const items = cached && cached.turn === turn && cached.events === events && cached.approvals === chat.approvals && cached.detailEvents === detail?.events && cached.facts === facts
            ? cached.items : projectTurn(turn, events, chat.approvals, { events: detail?.events, facts });
          if (items !== cached?.items) projections.current.set(turn.id, { turn, events, approvals: chat.approvals, detailEvents: detail?.events, facts, items });
          const lastAnswer = items.filter(item => item.type === "assistant").at(-1);
          const waitingApproval = items.some(item => item.type === "tool" && item.approval);
          const working = turn.status === "running" || turn.status === "queued";
          const showState = turn.status !== "completed";
          const stateLabel = waitingApproval ? "等待审批" : turnLabels[turn.status] ?? turn.status;
          const StateIcon = waitingApproval ? Shield : turn.status === "running" ? LoaderCircle : turn.status === "queued" ? Clock : turn.status === "failed" ? CircleAlert : Ban;
          const moreOpen = expanded[turn.id] === true;
          const canReuse = chat.canWrite && chat.session?.status === "open" && !chat.turnPending;
          const renderTool = (item: ToolPresentation) => {
            const approval = item.approval;
            const target = approval ? turnPath(chat.workspace, chat.id, turn.id) + "/approvals/" + encodeURIComponent(approval.request_id) : "";
            const pending = !!target && chat.pendingControls.some(saved => saved.target === target);
            return <ToolCallCard key={item.key} tool={item} onOpenSubagent={onOpenSubagent} canApprove={chat.canApprove} canAllowSession={!!chat.execution?.policy?.project_id && chat.execution.policy.project_access === "write"} busy={chat.controlling} pending={pending} onInspect={() => { stickToBottom.current = false; void chat.loadDetails(turn); }} onDecision={(allow, scope = "once") => { if (approval) void chat.control(turn, "approval", approval, allow, undefined, undefined, scope); }} />;
          };
          return <article className="chat-turn" key={turn.id} data-turn-id={turn.id}>
            <div className={`chat-user-wrap${showState ? " has-turn-state" : ""}`}><div className="chat-user-message">
              <div className="chat-user-text" data-ui-data>{turn.text}</div>
              </div><div className="chat-user-actions" role="group" aria-label="本轮消息操作">
                <Button variant="ghost" size="icon-sm" className="chat-message-action" aria-label="复制用户消息" title="复制用户消息" onClick={() => void feedback.copy(turn.text)}><Copy aria-hidden="true" /></Button>
                <Button variant="ghost" size="icon-sm" className="chat-message-action" aria-label="复用提问" title="复用提问" disabled={!canReuse} onClick={() => reuse(turn.text)}><Pencil aria-hidden="true" /></Button>
                {showState && <span className="chat-turn-status" role="status" aria-label={stateLabel} title={stateLabel}><StateIcon aria-hidden="true" className={turn.status === "running" && !waitingApproval ? "motion-safe:animate-spin" : undefined}/><span className="sr-only">{stateLabel}</span></span>}
                {working && <Button type="button" variant="ghost" size="icon-sm" className="chat-message-action" aria-label="停止此轮" title="停止此轮" disabled={chat.controlling || !chat.canWrite} onClick={() => void chat.control(turn, "cancel")}><Square aria-hidden="true" /></Button>}
              </div>
            </div>
            {!working && items.some(item => item.type === "assistant" && item.partial) && <p className="chat-partial-note">以下为已生成部分</p>}
            {groupFileTools(items).map(item => {
              if (item.type === "assistant") return <div className="chat-assistant" key={item.key}>
                <div className={item.key === lastAnswer?.key ? "chat-answer" : "chat-step-text"} data-ui-data><MessageMarkdown text={item.text} onCopy={feedback.copy} streaming={working} /></div>
              </div>;
              if (item.type === "file-group") return <FileToolGroupCard key={item.key} group={item} renderTool={renderTool} />;
              return renderTool(item);
            })}
            {turn.code && <p className="notice chat-turn-notice" data-ui-data>{runReason(turn.code)}<code>{turn.code}</code></p>}
            <div className="chat-answer-actions">
              {onTrace && turn.run_id && <Button variant="ghost" size="icon-sm" className="chat-message-action" aria-label="查看此轮 Trace" title="查看此轮 Trace" onClick={() => onTrace(turn.id)}><Eye aria-hidden="true"/></Button>}
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
        {children}
      </div>
    </div>
    {showJump && <Button variant="outline" size="icon-sm" className="chat-jump" aria-label="回到底部" onClick={bottom}><ArrowDown aria-hidden="true" /></Button>}
    {showJump && pendingCount > 0 && <div className="chat-pending-location" role="status"><span>{pendingCount} 项待处理操作</span><Button variant="ghost" size="sm" onClick={() => thread.current?.querySelector<HTMLElement>('.chat-approval,[data-pending-interaction="true"]')?.scrollIntoView?.({ block: "center" })}>定位处理</Button></div>}
    <CopyFeedback {...feedback} />
  </div>;
}
