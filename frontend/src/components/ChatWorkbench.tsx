import { useEffect } from "react";
import { RefreshCw, Square } from "lucide-react";
import type { Actor } from "../lib/session-contracts";
import type { SessionState } from "../hooks/use-session";
import { useSession } from "../hooks/use-session";
import { ChatHistory } from "./ChatHistory";
import { ChatComposer } from "./ChatComposer";
import { Button } from "./ui/button";
import { chatCopy as c } from "../lib/copy";
import { WorkspacePanel } from "./WorkspacePanel";

const EMPTY_SESSION_LIST: SessionState["list"] = [];

export function ChatWorkbench({ apiKey, active, fence, onLegacy, onConnect, onIdentity, onChatState, onTrace, onSelection }: { apiKey: string; active: boolean; fence: { current: number }; onLegacy: (value: boolean) => void; onConnect: () => void; onIdentity?: (actor: Actor | null, error: string, sourceKey: string) => void; onChatState?: (chat: SessionState, sourceKey: string) => void; onTrace?: (workspace: string, sessionId: string, turnId: string) => void; onSelection?: (sessionId: string) => void }) {
  const chat = useSession(apiKey, active, fence, onLegacy);
  const navigationSessions = chat.ready ? chat.list : EMPTY_SESSION_LIST;
  useEffect(() => { if (active) onIdentity?.(chat.actor, chat.actor ? "" : chat.notice ?? "", apiKey); }, [active, apiKey, chat.actor, chat.notice, onIdentity]);
  useEffect(() => { if (active) onSelection?.(chat.session?.id ?? ""); }, [active, chat.session?.id, onSelection]);
  useEffect(() => {
    if (active) onChatState?.(chat, apiKey);
  }, [active, apiKey, chat.actor, chat.workspace, chat.id, navigationSessions, chat.session?.title, chat.session?.status, chat.session?.dispatch_paused, chat.currentTurn?.status, chat.ready, chat.canWrite, chat.creating, chat.newPending, chat.notice, onChatState]);
  const workspaceMode = chat.session?.target_key === "target-workspace";
  return <section hidden={!active} className="chat-workbench" aria-label="对话工作台">
    <section className="chat-main" aria-label="对话">
      {chat.session && <header className="chat-controls">
        <div className="chat-current-actions">
          <Button variant="ghost" aria-label="刷新对话" onClick={chat.refresh}><RefreshCw aria-hidden="true" /></Button>
          {chat.currentTurn && ["queued", "running"].includes(chat.currentTurn.status) && <Button id="chat-stop" variant="outline" disabled={!chat.canWrite || chat.controlling} onClick={() => void chat.control(chat.currentTurn, "cancel")}><Square aria-hidden="true" />停止</Button>}
          {chat.session?.dispatch_paused && <Button variant="outline" disabled={!chat.canWrite || chat.controlling} onClick={() => void chat.control(null, "resume")}>{c.resume}</Button>}
          {chat.session?.status === "open" && <Button variant="ghost" disabled={!chat.canWrite || chat.controlling} onClick={() => void chat.control(null, "close")}>{c.close}</Button>}
        </div>
      </header>}
      {chat.notice && <div className="chat-notice"><p role="status">{chat.notice}</p>{!chat.session && <Button variant="outline" aria-label="刷新对话" onClick={chat.refresh}><RefreshCw aria-hidden="true"/>刷新</Button>}{chat.notice.includes("连接") && <Button variant="outline" onClick={onConnect}>连接</Button>}</div>}
      {chat.pending.length > 0 && <div className="chat-pending" role="status"><strong>{chat.pending.every(chat.posting) ? "提交中" : c.unknown}</strong>{chat.pending.map(intent => <div key={intent.id}><span data-ui-data>{intent.kind === "session" ? intent.body.title : (intent.session_id ?? "").slice(0, 8)}</span><Button variant="outline" onClick={() => void chat.recover(intent)}>{c.find}</Button></div>)}</div>}
      {chat.pendingControls.map(value => <div className="chat-pending" key={String(value.body.request_key)}><span>操作结果未确认</span><Button variant="outline" onClick={() => void chat.recoverControl(value)}>{value.rejected ? "重试原操作" : "查找操作结果"}</Button></div>)}
      <ChatHistory chat={chat} onTrace={turnId => { if (active && chat.session?.id) onTrace?.(chat.workspace, chat.session.id, turnId); }} />
      {workspaceMode ? <details className="workspace-task-area">
        <summary><strong>工作区任务</strong><span>输入文件、工作请求与完成条件</span></summary>
        <WorkspacePanel key={JSON.stringify([chat.actor?.principal_id, chat.actor?.generation, chat.workspace, chat.id])} chat={chat} apiKey={apiKey} active={active} fence={fence} />
      </details> : <ChatComposer chat={chat} />}
    </section>
  </section>;
}
