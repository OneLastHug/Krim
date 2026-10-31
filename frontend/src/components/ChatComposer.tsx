import { CornerDownLeft } from "lucide-react";
import { Button } from "./ui/button";
import { Textarea } from "./ui/textarea";
import { Label } from "./ui/label";
import { chatCopy as c } from "../lib/copy";
import type { SessionState } from "../hooks/use-session";

export function ChatComposer({ chat }: { chat: SessionState }) {
  const waiting = chat.history.some(t => t.status === "running" || t.status === "queued");
  const disabled = !chat.session || chat.session.status !== "open" || chat.session.dispatch_paused || !chat.canWrite || !!chat.turnPending || chat.sending;
  const action = chat.sending ? "提交中" : waiting ? c.enqueue : c.send;
  return <form className="chat-composer" onSubmit={e => { e.preventDefault(); void chat.submit(); }}>
    <Label htmlFor="chat-prompt" className="sr-only">{c.message}</Label>
    <Textarea id="chat-prompt" placeholder="发送消息…" rows={1} value={chat.turnPending?.body.text ?? chat.draft} onChange={e => chat.edit(e.target.value)} disabled={disabled} maxLength={32768} autoComplete="off" />
    <div className="chat-compose-actions"><Button className="chat-send" type="submit" variant="ghost" aria-label={action} title={action} disabled={disabled || !chat.draft.trim()}><CornerDownLeft aria-hidden="true"/><span className="sr-only">{action}</span></Button></div>
  </form>;
}
