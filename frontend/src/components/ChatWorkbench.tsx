import { useEffect } from "react";
import { ChevronDown, RefreshCw } from "lucide-react";
import type { Actor } from "../lib/session-contracts";
import type { SessionState } from "../hooks/use-session";
import { useSession } from "../hooks/use-session";
import { ChatHistory } from "./ChatHistory";
import { ToolCatalog } from "./ToolCatalog";
import { ChatComposer } from "./ChatComposer";
import { Button } from "./ui/button";
import { chatCopy as c } from "../lib/copy";
import { WorkspacePanel } from "./WorkspacePanel";
import "../styles/chat-workbench.css";

const EMPTY_SESSION_LIST: SessionState["list"] = [];

export function ChatWorkbench({ apiKey, active, fence, onLegacy, onConnect, onIdentity, onChatState, onTrace, onSelection }: { apiKey: string; active: boolean; fence: { current: number }; onLegacy: (value: boolean) => void; onConnect: () => void; onIdentity?: (actor: Actor | null, error: string, sourceKey: string) => void; onChatState?: (chat: SessionState, sourceKey: string) => void; onTrace?: (workspace: string, sessionId: string, turnId: string) => void; onSelection?: (sessionId: string) => void }) {
  const chat = useSession(apiKey, active, fence, onLegacy);
  const navigationSessions = chat.ready ? chat.list : EMPTY_SESSION_LIST;
  const navigationControls = chat.pendingControls.map(value => `${value.body.request_key}:${value.rejected}`).join("|");
  useEffect(() => { if (active) onIdentity?.(chat.actor, chat.actor ? "" : chat.notice ?? "", apiKey); }, [active, apiKey, chat.actor, chat.notice, onIdentity]);
  useEffect(() => { if (active) onSelection?.(chat.session?.id ?? ""); }, [active, chat.session?.id, onSelection]);
  useEffect(() => {
    if (active) onChatState?.(chat, apiKey);
  }, [active, apiKey, chat.actor, chat.workspace, chat.id, navigationSessions, navigationControls, chat.controlling, chat.session?.title, chat.session?.revision, chat.session?.status, chat.session?.dispatch_paused, chat.currentTurn?.status, chat.ready, chat.canWrite, chat.canApprove, chat.creating, chat.newPending, chat.notice, onChatState]);
  const workspaceMode = chat.session?.target_key === "target-workspace";
  return <section hidden={!active} className="chat-workbench" aria-label="对话工作台">
    <section className="chat-main" aria-label="对话">
      {(chat.notice || chat.pending.length > 0 || chat.pendingControls.length > 0) && <div className="chat-feedback-region">
        {chat.notice && <div className="chat-notice"><p role="status">{chat.notice}</p>{!chat.session && <Button variant="outline" aria-label="刷新对话" onClick={chat.refresh}><RefreshCw aria-hidden="true" />刷新</Button>}{chat.notice.includes("连接") && <Button variant="outline" onClick={onConnect}>连接</Button>}</div>}
        {chat.pending.length > 0 && <div className="chat-pending" role="status"><strong>{chat.pending.every(chat.posting) ? "提交中" : c.unknown}</strong>{chat.pending.map(intent => <div key={intent.id}><span data-ui-data>{intent.kind === "session" ? intent.body.title : chat.list.find(value => value.id === intent.session_id)?.title ?? (intent.session_id ?? "").slice(0, 8)}</span><Button variant="outline" disabled={chat.posting(intent)} onClick={() => void chat.recover(intent)}>{c.find}</Button></div>)}</div>}
        {chat.pendingControls.map(value => <div className="chat-pending" key={String(value.body.request_key)}><span>操作结果未确认</span><Button variant="outline" disabled={value.rejected && (value.target.includes("/approvals/") ? !chat.canApprove : !chat.canWrite)} onClick={() => void chat.recoverControl(value)}>{value.rejected ? "重试原操作" : "查找操作结果"}</Button></div>)}
      </div>}
      {chat.ready && chat.session && chat.actor && <ToolCatalog apiKey={apiKey} targetKey={chat.session.target_key} scope={{ principal_id: chat.actor.principal_id, generation: chat.actor.generation, workspace: chat.workspace, session: chat.session.id, target: chat.session.target_key }} active={active} />}
      <ChatHistory chat={chat} onTrace={turnId => { if (active && chat.session?.id) onTrace?.(chat.workspace, chat.session.id, turnId); }} />
      {workspaceMode ? <details className="workspace-task-area">
        <summary><strong>工作区任务</strong><span>输入文件、工作请求与完成条件</span></summary>
        <WorkspacePanel key={JSON.stringify([chat.actor?.principal_id, chat.actor?.generation, chat.workspace, chat.id])} chat={chat} apiKey={apiKey} active={active} fence={fence} />
      </details> : <ChatComposer chat={chat} />}
      {chat.session && <footer className="chat-controls">
        <button type="button" className="control-chip" disabled title="Agent 模式选择规划中">自动<ChevronDown aria-hidden="true" /></button>
        <div className="chat-controls-right">
          <button type="button" className="control-chip is-strong" disabled title="模型选择规划中">模型</button>
          <button type="button" className="control-chip" disabled title="思考强度选择规划中">思考强度</button>
          <span className="context-meter" title="上下文用量规划中">
            <svg width="14" height="14" viewBox="0 0 16 16" aria-hidden="true">
              <circle cx="8" cy="8" r="6" fill="none" stroke="currentColor" stroke-width="2" opacity=".3"/>
              <path d="M8 2a6 6 0 0 1 5.2 3" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"/>
            </svg>
            —
          </span>
        </div>
      </footer>}
    </section>
  </section>;
}
