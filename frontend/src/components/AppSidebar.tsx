import type { Actor } from "../lib/session-contracts";
import type { SessionState } from "../hooks/use-session";
import type { WorkbenchView } from "../lib/workbench-route";
import { SessionList } from "./SessionList";

const sectionLabels: Record<WorkbenchView, string> = {
  chat: "对话",
  run: "运行记录",
  trace: "Trace",
  datasets: "数据集",
  eval: "评测",
  settings: "设置",
};

export function AppSidebar({ active, identity, identityLoading, identityError, chat, httpEnabled, beforeSessionSelect, onClose, className = "" }: {
  active: WorkbenchView;
  identity: Actor | null;
  identityLoading: boolean;
  identityError: string;
  chat: SessionState | null;
  httpEnabled: boolean;
  beforeSessionSelect?: (workspace: string, sessionId: string) => void;
  onClose?: () => void;
  className?: string;
}) {
  const workspaces = identity?.workspaces ?? [];
  const chatMode = active === "chat";
  const identityLabel = identity ? identity.principal_id : identityLoading ? "正在读取身份" : identityError ? "连接失败" : "未连接";
  const identityDetail = identity ? (identityError || "当前身份") : identityError || (identityLoading ? "正在读取工作区" : "连接后显示会话和工作区");
  return <aside className={`app-sidebar ${chatMode ? "conversation-sidebar" : "observability-sidebar"} ${className}`} aria-label="Krim Agent 工作区侧栏">
    <div className="sidebar-brand"><span className="brand-mark" aria-hidden="true">K</span><div><strong>Krim Agent</strong><span>{sectionLabels[active]}</span></div></div>
    <section className="sidebar-context" aria-label="当前身份">
      <span className={`identity-mark${identity ? " is-ready" : ""}`} aria-hidden="true"/>
      <div><strong title={identityLabel}>{identityLabel}</strong><span title={identityDetail}>{identityDetail}</span></div>
    </section>
    {chatMode && chat && <SessionList chat={chat} httpEnabled={httpEnabled} beforeSelect={(workspace, sessionId) => { beforeSessionSelect?.(workspace, sessionId); onClose?.(); }} />}
    {!chatMode && <section className="sidebar-current-section" aria-label="当前分区"><span className="workspace-label">当前分区</span><strong>{sectionLabels[active]}</strong></section>}
    <div className="sidebar-footer">
      <span className="workspace-label">{identity ? "当前工作区" : identityLoading ? "正在读取工作区" : "工作区"}</span>
      <span className="workspace-name" title={identityError || workspaces.map(workspace => workspace.workspace_id).join("、") || "未连接"}>
        {workspaces.length ? workspaces.map(workspace => workspace.workspace_id).join("、") : identityError || "未连接"}
      </span>
    </div>
  </aside>;
}
