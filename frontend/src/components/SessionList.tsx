import { useState } from "react";
import { Plus } from "lucide-react";
import { Button } from "./ui/button";
import { Input } from "./ui/input";
import { Label } from "./ui/label";
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogTrigger } from "./ui/dialog";
import { chatCopy as c } from "../lib/copy";
import type { SessionState } from "../hooks/use-session";

function createdLabel(value: number) {
  const date = new Date(value * 1000);
  return Number.isFinite(date.getTime()) ? date.toLocaleString(undefined, { month: "numeric", day: "numeric", hour: "2-digit", minute: "2-digit", second: "2-digit" }) : "时间未记录";
}

export function SessionList({ chat, httpEnabled, beforeSelect }: { chat: SessionState; httpEnabled: boolean; beforeSelect?: (workspace: string, sessionId: string) => void }) {
  const [open, setOpen] = useState(false), [title, setTitle] = useState("新对话"), [target, setTarget] = useState("demo");
  return <section className="chat-sidebar" aria-label="对话列表">
    <div className="chat-session-heading">
      <Dialog open={open} onOpenChange={setOpen}>
        <DialogTrigger render={<Button variant="outline" className="new-conversation-button" aria-label={c.create} title={c.create} disabled={!chat.ready || !chat.canWrite} />}><Plus aria-hidden="true" />{c.create}</DialogTrigger>
        <DialogContent aria-describedby={undefined}><DialogHeader><DialogTitle>{c.create}</DialogTitle></DialogHeader>
          <form onSubmit={e => { e.preventDefault(); beforeSelect?.(chat.workspace, ""); void chat.create(title, target).then(() => setOpen(false)); }}>
            <Label htmlFor="session-title">{c.title}</Label><Input id="session-title" value={title} onChange={e => setTitle(e.target.value)} maxLength={128} required autoComplete="off" />
            <div className="chat-target"><Label htmlFor="chat-target">{c.target}</Label><select id="chat-target" value={target} onChange={e => setTarget(e.target.value)}><option value="demo">{c.defaultTarget}</option>{httpEnabled && chat.actor?.target_keys?.includes("target-http") && <option value="target-http">HTTP</option>}<option value="target-workspace" disabled={!chat.actor?.target_keys?.includes("target-workspace")}>工作区</option></select>{!chat.actor?.target_keys?.includes("target-workspace") && <span role="status">工作区未配置</span>}</div>
            <div className="dialog-actions"><Button type="submit" disabled={chat.creating || !!chat.newPending || !title.trim()}>{chat.creating ? "提交中" : c.createSubmit}</Button></div>
          </form>
        </DialogContent>
      </Dialog>
      <h2>最近对话</h2>
    </div>
    {chat.actor && chat.actor.workspaces.length > 1 && <div className="chat-workspace-select"><Label htmlFor="chat-workspace">{c.workspace}</Label><select id="chat-workspace" value={chat.workspace} onChange={e => { beforeSelect?.(e.target.value, ""); chat.select(e.target.value, ""); }}>{chat.actor.workspaces.map(w => <option key={w.workspace_id} value={w.workspace_id}>{w.workspace_id}</option>)}</select></div>}
    <nav className="chat-session-list" aria-label="已存对话">
      {!chat.list.length && <p className="chat-list-empty">{chat.ready ? c.empty : "连接后显示对话"}</p>}
      {chat.list.map(s => <Button key={s.id} variant="ghost" aria-label={s.title} aria-describedby={`session-created-${s.id}`} aria-current={chat.id === s.id ? "page" : undefined} onClick={() => { if (chat.id !== s.id) beforeSelect?.(chat.workspace, s.id); chat.select(chat.workspace, s.id); }}><span className="chat-session-label" data-ui-data><span>{s.title}</span><small id={`session-created-${s.id}`}>{createdLabel(s.created_at)}</small></span>{s.status === "closed" && <span className="muted">已关闭</span>}</Button>)}
    </nav>
  </section>;
}
