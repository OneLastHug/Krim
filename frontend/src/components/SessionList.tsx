import { useEffect, useState, type FormEvent } from "react";
import { ChevronDown, Pencil, Plus } from "lucide-react";
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
  const [renameOpen, setRenameOpen] = useState(false), [renameText, setRenameText] = useState(""), [submittedTitle, setSubmittedTitle] = useState<string | null>(null);
  const scopeKey = JSON.stringify([chat.actor?.principal_id, chat.actor?.generation, chat.workspace, chat.id]);
  const renameControl = chat.pendingControls.find(value => value.session_id === chat.id && value.target.endsWith("/rename"));
  const canRename = chat.ready && chat.canWrite && chat.session?.id === chat.id && chat.session.status === "open";
  const workspaceConfigured = chat.actor?.target_keys?.includes("target-workspace") === true;

  useEffect(() => {
    setRenameOpen(false);
    setRenameText("");
    setSubmittedTitle(null);
  }, [scopeKey]);
  useEffect(() => {
    if (submittedTitle && chat.session?.id === chat.id && chat.session.title === submittedTitle) {
      setRenameOpen(false);
      setSubmittedTitle(null);
    }
  }, [submittedTitle, chat.id, chat.session?.id, chat.session?.title]);

  function openRename(currentTitle: string) {
    setRenameText(currentTitle);
    setSubmittedTitle(null);
    setRenameOpen(true);
  }
  function updateRenameOpen(next: boolean) {
    if (!next && submittedTitle && chat.session?.title !== submittedTitle) return;
    setRenameOpen(next);
    if (!next) setSubmittedTitle(null);
  }
  function submitRename(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const nextTitle = renameText.trim();
    if (!nextTitle || submittedTitle || renameControl || !canRename) return;
    if (nextTitle === chat.session?.title) {
      setRenameOpen(false);
      return;
    }
    setSubmittedTitle(nextTitle);
    // 只触发真实重命名和重新读取；Dialog 是否关闭由 session.title 更新决定。
    void chat.rename(nextTitle);
  }
  function recoverRename() {
    if (renameControl) void chat.recoverControl(renameControl);
  }

  return <section className="chat-sidebar" aria-label="对话列表">
    <div className="chat-session-heading">
      <Dialog open={open} onOpenChange={setOpen}>
        <DialogTrigger render={<Button variant="outline" className="new-conversation-button" aria-label={c.create} title={c.create} disabled={!chat.ready || !chat.canWrite} />}><Plus aria-hidden="true" />{c.create}</DialogTrigger>
        <DialogContent className="session-create-dialog" aria-describedby={undefined}>
          <DialogHeader><DialogTitle>{c.create}</DialogTitle></DialogHeader>
          <form className="session-create-form" onSubmit={e => { e.preventDefault(); beforeSelect?.(chat.workspace, ""); void chat.create(title, target).then(() => setOpen(false)); }}>
            <div className="session-create-field">
              <Label htmlFor="session-title">{c.title}</Label>
              <Input id="session-title" value={title} onChange={e => setTitle(e.target.value)} maxLength={128} required autoComplete="off" />
            </div>
            <div className="session-create-field chat-target">
              <Label htmlFor="chat-target">{c.target}</Label>
              <div className="session-target-select">
                <select id="chat-target" aria-describedby={workspaceConfigured ? undefined : "session-target-help"} value={target} onChange={e => setTarget(e.target.value)}>
                  <option value="demo">{c.defaultTarget}</option>
                  {httpEnabled && chat.actor?.target_keys?.includes("target-http") && <option value="target-http">HTTP</option>}
                  <option value="target-workspace" disabled={!workspaceConfigured}>工作区</option>
                </select>
                <ChevronDown aria-hidden="true" />
              </div>
              {!workspaceConfigured && <p className="session-target-help" id="session-target-help" role="status">工作区未配置</p>}
            </div>
            <div className="dialog-actions"><Button type="submit" disabled={chat.creating || !!chat.newPending || !title.trim()}>{chat.creating ? "提交中" : c.createSubmit}</Button></div>
          </form>
        </DialogContent>
      </Dialog>
      <h2>最近对话</h2>
    </div>
    {chat.actor && chat.actor.workspaces.length > 1 && <div className="chat-workspace-select"><Label htmlFor="chat-workspace">{c.workspace}</Label><select id="chat-workspace" value={chat.workspace} onChange={e => { beforeSelect?.(e.target.value, ""); chat.select(e.target.value, ""); }}>{chat.actor.workspaces.map(w => <option key={w.workspace_id} value={w.workspace_id}>{w.workspace_id}</option>)}</select></div>}
    <nav className="chat-session-list" aria-label="已存对话">
      {!chat.list.length && <p className="chat-list-empty">{chat.ready ? c.empty : "连接后显示对话"}</p>}
      {chat.list.map(s => <div className="chat-session-row" key={s.id}>
        <Button className="session-select" variant="ghost" aria-label={s.title} aria-describedby={`session-created-${s.id}`} aria-current={chat.id === s.id ? "page" : undefined} onClick={() => { if (chat.id !== s.id) beforeSelect?.(chat.workspace, s.id); chat.select(chat.workspace, s.id); }}><span className="chat-session-label" data-ui-data><span>{s.title}</span><small id={`session-created-${s.id}`}>{createdLabel(s.created_at)}</small></span>{s.status === "closed" && <span className="muted">已关闭</span>}</Button>
        {chat.id === s.id && canRename && <Button className="session-rename-trigger" variant="ghost" aria-label="重命名当前对话" title="重命名当前对话" onClick={() => openRename(chat.session?.title ?? s.title)}><Pencil aria-hidden="true"/></Button>}
        {chat.id === s.id && renameControl && <Button className="session-recovery-trigger" variant="ghost" aria-label="查找重命名结果" title="查找重命名结果" onClick={recoverRename}>恢复</Button>}
      </div>)}
    </nav>
    <Dialog open={renameOpen} onOpenChange={updateRenameOpen}>
      <DialogContent aria-describedby={undefined} className="session-rename-dialog">
        <DialogHeader><DialogTitle>重命名对话</DialogTitle></DialogHeader>
        <form onSubmit={submitRename}>
          <Label htmlFor="rename-session-title">{c.title}</Label>
          <Input id="rename-session-title" value={renameText} onChange={event => setRenameText(event.target.value)} maxLength={128} required autoComplete="off" disabled={!!submittedTitle || !!renameControl}/>
          {(submittedTitle || renameControl) && <p className="rename-status" role="status">{renameControl ? "重命名结果尚未确认。请查找原操作结果。" : "已提交，等待对话名称同步。"}</p>}
          {renameControl && <div className="rename-recovery"><Button type="button" variant="outline" disabled={chat.busy} onClick={recoverRename}>{renameControl.rejected ? "重试原操作" : "查找操作结果"}</Button></div>}
          {submittedTitle && !renameControl && <div className="rename-recovery"><Button type="button" variant="outline" onClick={chat.refresh}>重新读取名称</Button></div>}
          <div className="dialog-actions"><Button type="submit" disabled={!canRename || !renameText.trim() || !!submittedTitle || !!renameControl}>{submittedTitle ? "等待同步" : "保存名称"}</Button></div>
        </form>
      </DialogContent>
    </Dialog>
  </section>;
}
