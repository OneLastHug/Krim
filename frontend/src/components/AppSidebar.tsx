import { Activity, Database, FlaskConical, MessageSquare, Settings2 } from "lucide-react";
import type { Actor } from "../lib/session-contracts";
import type { WorkbenchView } from "../lib/workbench-route";
import { Button } from "./ui/button";

const entries: { id: WorkbenchView; label: string; icon: typeof MessageSquare }[] = [
  { id: "chat", label: "对话", icon: MessageSquare },
  { id: "run", label: "任务", icon: Activity },
  { id: "datasets", label: "数据集", icon: Database },
  { id: "eval", label: "评测", icon: FlaskConical },
  { id: "settings", label: "设置", icon: Settings2 },
];

export function AppSidebar({ active, identity, identityLoading, identityError, onNavigate, onClose, className = "" }: {
  active: WorkbenchView;
  identity: Actor | null;
  identityLoading: boolean;
  identityError: string;
  onNavigate: (view: WorkbenchView) => void;
  onClose?: () => void;
  className?: string;
}) {
  const workspaces = identity?.workspaces ?? [];
  return <aside className={`app-sidebar ${className}`} aria-label="Krim Agent 工作区导航">
    <div className="sidebar-brand">
      <span className="brand-mark" aria-hidden="true">K</span>
      <div><strong>Krim Agent</strong><span>任务工作台</span></div>
    </div>
    <div className="sidebar-context" aria-live="polite">
      <span className={`identity-mark${identity ? " is-ready" : ""}`} aria-hidden="true" />
      <div>
        <strong>{identity ? (identity.legacy ? "旧版兼容" : "工作区身份") : identityLoading ? "正在读取身份" : identityError ? "身份不可用" : "尚未连接"}</strong>
        <span>{identity ? workspaces.length ? `${workspaces.length} 个可用工作区` : "没有可用工作区" : identityError || "连接后显示可用能力"}</span>
      </div>
    </div>
    <nav className="primary-navigation" aria-label="主导航">
      <p className="navigation-heading">工作区</p>
      {entries.map(({ id, label, icon: Icon }) => <Button
        key={id}
        type="button"
        variant="ghost"
        className="navigation-item"
        aria-label={`导航到${id === "run" ? "运行记录" : label}`}
        aria-current={active === id ? "page" : undefined}
        onClick={() => { onNavigate(id); onClose?.(); }}
      ><Icon aria-hidden="true" /><span>{label}</span></Button>)}
    </nav>
    <div className="sidebar-footer">
      <span className="workspace-label">当前身份</span>
      <span className="workspace-name" title={identity?.principal_id ?? "未连接"}>{identity?.principal_id ?? "未连接"}</span>
      {identity && <span className="workspace-label">{workspaces.map(workspace => workspace.workspace_id).join("、")}</span>}
    </div>
  </aside>;
}
