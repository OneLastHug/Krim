import { useLayoutEffect, useRef } from "react";
import { CornerDownLeft, Plus, Square } from "lucide-react";
import { Button } from "./ui/button";
import { Textarea } from "./ui/textarea";
import { Label } from "./ui/label";
import { chatCopy as c } from "../lib/copy";
import type { SessionState } from "../hooks/use-session";

export function ChatComposer({ chat, onStart, busy = false }: { chat: SessionState; onStart?: () => void; busy?: boolean }) {
  const composing = useRef(false), input = useRef<HTMLTextAreaElement>(null);
  const value = chat.turnPending?.body.text ?? chat.draft;
  const waiting = chat.history.some(t => t.status === "running" || t.status === "queued");
  const disabled = busy || !chat.ready || (!chat.session ? !onStart || chat.creating || !!chat.newPending : chat.reading || chat.session.status !== "open" || chat.session.dispatch_paused) || !chat.canWrite || !!chat.turnPending || chat.sending;
  const action = chat.sending ? "提交中" : waiting ? c.enqueue : c.send;
  const running = !!chat.currentTurn && ["queued", "running"].includes(chat.currentTurn.status);
  useLayoutEffect(() => {
    const element = input.current;
    if (!element) return;
    const resize = () => {
      const maximum = Math.max(20, Math.min(220, window.innerHeight * .34));
      element.style.height = "0px";
      const height = element.scrollHeight;
      element.style.height = `${Math.max(20, Math.min(height, maximum))}px`;
      element.style.overflowY = height > maximum + 1 ? "auto" : "hidden";
    };
    resize();
    let width = element.clientWidth;
    const observer = typeof ResizeObserver === "undefined" ? null : new ResizeObserver(() => {
      if (element.clientWidth !== width) { width = element.clientWidth; resize(); }
    });
    observer?.observe(element);
    window.addEventListener("resize", resize);
    document.fonts?.addEventListener("loadingdone", resize);
    return () => { observer?.disconnect(); window.removeEventListener("resize", resize); document.fonts?.removeEventListener("loadingdone", resize); };
  }, [chat.id, value]);
  function submit() { if (!disabled && !composing.current && chat.draft.trim()) { if (!chat.session && onStart) onStart(); else void chat.submit(); } }
  return <form className="chat-composer" onSubmit={event => { event.preventDefault(); submit(); }}>
    <Label htmlFor="chat-prompt" className="sr-only">{c.message}</Label>
    <Button variant="ghost" className="composer-add" type="button" aria-label="添加（规划中）" title="添加（规划中）" disabled><Plus aria-hidden="true" /></Button>
    <Textarea id="chat-prompt" ref={input} placeholder={waiting ? "补充下一条消息…" : "给 Krim 一个任务…"} rows={1} value={value} onChange={event => chat.edit(event.target.value)} disabled={disabled} autoComplete="off"
      onCompositionStart={() => { composing.current = true; }} onCompositionEnd={() => { composing.current = false; }}
      onKeyDown={event => {
        if (event.key === "Enter" && !event.shiftKey && !event.nativeEvent.isComposing && !composing.current && event.nativeEvent.keyCode !== 229) { event.preventDefault(); submit(); }
      }} />
    <div className="chat-compose-actions">
      {running
        ? <Button id="chat-stop" className="chat-send" type="button" variant="ghost" aria-label="停止" title="停止" disabled={!chat.canWrite || chat.controlling} onClick={() => { if (chat.currentTurn) void chat.control(chat.currentTurn, "cancel"); }}><Square aria-hidden="true" /></Button>
        : <Button className="chat-send" type="submit" variant="ghost" aria-label={action} title={action} disabled={disabled || !chat.draft.trim()}><CornerDownLeft aria-hidden="true" /><span className="sr-only">{action}</span></Button>}
    </div>
  </form>;
}
