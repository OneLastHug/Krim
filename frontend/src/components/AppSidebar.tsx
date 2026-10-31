import { Activity, ChevronDown, Database, Eye, FlaskConical, LayoutGrid, MessageSquare, Settings2 } from "lucide-react";
import type { Actor } from "../lib/session-contracts";
import type { SessionState } from "../hooks/use-session";
import type { WorkbenchView } from "../lib/workbench-route";
import { Button } from "./ui/button";
import { SessionList } from "./SessionList";

const entries: { id: WorkbenchView; label: string; icon: typeof MessageSquare }[] = [
  { id: "chat", label: "对话", icon: MessageSquare },
  { id: "run", label: "任务", icon: Activity },
  { id: "trace", label: "Trace", icon: Eye },
  { id: "datasets", label: "数据集", icon: Database },
  { id: "eval", label: "评测", icon: FlaskConical },
  { id: "settings", label: "设置", icon: Settings2 },
];

export function AppSidebar({ active, identity, identityLoading, identityError, chat, httpEnabled, beforeSessionSelect, onNavigate, onClose, className = "" }: {
  active: WorkbenchView;
  identity: Actor | null;
  identityLoading: boolean;
  identityError: string;
  chat: SessionState | null;
  httpEnabled: boolean;
  beforeSessionSelect?: (workspace: string, sessionId: string) => void;
  onNavigate: (view: WorkbenchView) => void;
  onClose?: () => void;
  className?: string;
}) {
  const workspaces = identity?.workspaces ?? [];
  const chatMode = active === "chat";
  return <aside className={`app-sidebar${chatMode ? " conversation-sidebar" : " observability-sidebar"} ${className}`} aria-label="Krim Agent 工作区导航">
    <div className="sidebar-brand"><span className="brand-mark" aria-hidden="true">K</span><div><strong>Krim Agent</strong></div></div>
    {chatMode && chat && <SessionList chat={chat} httpEnabled={httpEnabled} beforeSelect={(workspace, sessionId) => { beforeSessionSelect?.(workspace, sessionId); onClose?.(); }} />}
    <details className="workspace-navigation" open={!chatMode}>
      <summary aria-label="工作台导航"><LayoutGrid aria-hidden="true"/><span>工作台</span><ChevronDown aria-hidden="true"/></summary>
      <nav className="primary-navigation" aria-label="主导航">
        {entries.map(({ id, label, icon: Icon }) => <Button
          key={id}
          type="button"
          variant="ghost"
          className="navigation-item"
          aria-label={`导航到${id === "run" ? "运行记录" : label}`}
          aria-current={active === id ? "page" : undefined}
          onClick={() => { onNavigate(id); onClose?.(); }}
        ><Icon aria-hidden="true"/><span>{label}</span></Button>)}
      </nav>
    </details>
    <div className="sidebar-footer">
      <span className="workspace-label">{identity ? "当前工作区" : identityLoading ? "正在读取工作区" : "工作区"}</span>
      <span className="workspace-name" title={identityError || workspaces.map(workspace => workspace.workspace_id).join("、") || "未连接"}>
        {workspaces.length ? workspaces.map(workspace => workspace.workspace_id).join("、") : identityError || "未连接"}
      </span>
    </div>
  </aside>;
}
