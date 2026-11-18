import { useEffect, useRef, useState } from "react";
import { ChevronDown, Folder, RefreshCw, Terminal } from "lucide-react";
import type { DisplayTheme } from "../lib/use-display-theme";
import type { Actor } from "../lib/session-contracts";
import type { SandboxProject } from "../lib/development-contracts";
import type { SessionState } from "../hooks/use-session";
import { useSession } from "../hooks/use-session";
import { useSessionContext } from "../hooks/use-session-context";
import { fetchProjectsPage, useDevelopment } from "../hooks/use-development";
import { openDevelopmentPanel } from "../lib/workbench-events";
import { ChatHistory } from "./ChatHistory";
import { SessionFeatures } from "./SessionFeatures";
import { DevelopmentPanel, ProjectDialogBody } from "./DevelopmentPanel";
import { SessionExecutionControl } from "./SessionExecutionControl";
import { ChatComposer } from "./ChatComposer";
import { Button } from "./ui/button";
import { chatCopy as c, chatReason } from "../lib/copy";
import { WorkspacePanel } from "./WorkspacePanel";
import { Dialog, DialogContent } from "./ui/dialog";
import { PopoverMenu } from "./popover-menu";
import { contextCount } from "./ContextSummary";
import "../styles/chat-workbench.css";

const EMPTY_SESSION_LIST: SessionState["list"] = [];

export function ChatWorkbench({ theme, apiKey, active, fence, onLegacy, onConnect, onIdentity, onChatState, onTrace, onSelection }: { theme?: DisplayTheme; apiKey: string; active: boolean; fence: { current: number }; onLegacy: (value: boolean) => void; onConnect: () => void; onIdentity?: (actor: Actor | null, error: string, sourceKey: string) => void; onChatState?: (chat: SessionState, sourceKey: string) => void; onTrace?: (workspace: string, sessionId: string, turnId: string, page?: "runs" | "context") => void; onSelection?: (sessionId: string) => void }) {
  const chat = useSession(apiKey, active, fence, onLegacy);
  const context = useSessionContext(chat, apiKey, active);
  const development = useDevelopment(chat, apiKey, active, fence);
  const navigationSessions = chat.ready ? chat.list : EMPTY_SESSION_LIST;
  const navigationControls = chat.pendingControls.map(value => `${value.body.request_key}:${value.rejected}`).join("|");
  useEffect(() => { if (active) onIdentity?.(chat.actor, chat.actor ? "" : chat.notice ?? "", apiKey); }, [active, apiKey, chat.actor, chat.notice, onIdentity]);
  useEffect(() => { if (active) onSelection?.(chat.session?.id ?? ""); }, [active, chat.session?.id, onSelection]);
  useEffect(() => {
    if (active) onChatState?.(chat, apiKey);
  }, [active, apiKey, chat.actor, chat.workspace, chat.id, navigationSessions, navigationControls, chat.controlling, chat.session?.title, chat.session?.revision, chat.session?.status, chat.session?.dispatch_paused, chat.currentTurn?.status, chat.approvals.length, chat.ready, chat.canWrite, chat.canApprove, chat.creating, chat.newPending, chat.notice, onChatState]);
  const modelRef = useRef<HTMLButtonElement>(null), extraRef = useRef<HTMLButtonElement>(null);
  const [projectPickerOpen, setProjectPickerOpen] = useState(false);
  const [workMaximized, setWorkMaximized] = useState(false);
  const [info, setInfo] = useState<"model" | "extra" | null>(null);
  const [projects, setProjects] = useState<SandboxProject[]>([]), [newProject, setNewProject] = useState(""), [group, setGroup] = useState<string>();
  const [startNotice, setStartNotice] = useState("");
  const canDevelop = chat.actor?.target_keys?.includes("target-development") === true;
  const accountScope = JSON.stringify([apiKey, chat.actor?.principal_id, chat.actor?.generation, chat.workspace]);
  const liveScope = useRef(accountScope); liveScope.current = accountScope;
  const [starting, setStarting] = useState<{ id: string; project: string; text: string; scope: string; stage: "bind" | "wait" } | null>(null);
  const autoFlight = useRef("");
  useEffect(() => { setInfo(null); setProjectPickerOpen(false); setGroup(undefined); setProjects([]); setNewProject(""); setStarting(null); setStartNotice(""); autoFlight.current = ""; }, [accountScope]);
  useEffect(() => {
    if (!active || !chat.ready || chat.id || !canDevelop) return;
    const controller = new AbortController();
    void fetchProjectsPage(apiKey, controller.signal).then(value => { if (!controller.signal.aborted) setProjects(value.items.filter(project => project.workspace_id === chat.workspace)); }).catch(error => { if (!controller.signal.aborted) setStartNotice(chatReason(error)); });
    return () => controller.abort();
  }, [apiKey, accountScope, active, chat.ready, chat.id, canDevelop]);
  useEffect(() => {
    const onNew = (event: Event) => {
      const detail = (event as CustomEvent).detail;
      if (detail?.principal === chat.actor?.principal_id && detail?.generation === chat.actor?.generation && detail?.workspace === chat.workspace) setGroup(typeof detail.group === "string" ? detail.group : undefined);
    };
    window.addEventListener("krim:new-session", onNew);
    return () => window.removeEventListener("krim:new-session", onNew);
  }, [chat.actor?.principal_id, chat.actor?.generation, chat.workspace]);
  async function start() {
    const target = chat.actor?.target_keys?.includes("target-development") ? "target-development" : chat.actor?.target_keys?.includes("target-http") ? "target-http" : "demo";
    const text = chat.draft;
    if (!newProject || target !== "target-development") { await chat.createAndSubmit(text, target, group); return; }
    const created = await chat.create(text.trim().split("\n")[0]!.slice(0, 128), target, group, text);
    if (!created || liveScope.current !== accountScope || new URL(location.href).searchParams.get("session") !== created.id) return;
    chat.edit(text);
    setStarting({ id: created.id, project: newProject, text, scope: accountScope, stage: "bind" });
  }
  useEffect(() => {
    if (!starting || !active || starting.scope !== accountScope || chat.id !== starting.id || chat.session?.id !== starting.id || chat.reading) return;
    if (starting.stage === "bind") {
      const marker = accountScope + starting.id;
      if (autoFlight.current === marker) return;
      autoFlight.current = marker;
      void development.bindProject(starting.project).then(() => setStarting(previous => previous?.id === starting.id && previous.scope === starting.scope ? { ...previous, stage: "wait" } : previous));
    } else if (development.binding?.project_id === starting.project && chat.session.revision >= development.binding.session_revision && !development.busy) {
      setStarting(null); void chat.submit(starting.text);
    } else if (development.notice && development.binding?.project_id !== starting.project && !development.busy) {
      setStarting(null); setStartNotice("首条消息已保留。请确认项目绑定后重新发送。");
    }
  }, [starting, accountScope, active, chat.id, chat.session?.id, chat.session?.revision, chat.reading, development.binding?.project_id, development.busy, development.notice]);
  useEffect(() => { if (active) window.dispatchEvent(new Event("krim:focus-turn")); }, [active]);
  const workspaceMode = chat.session?.target_key === "target-workspace";
  const ratio = context.used !== null && context.limit ? Math.max(0, Math.min(1, context.used / context.limit)) : 0;
  const model = context.data?.configuration?.model ?? "模型信息";
  useEffect(() => {
    const project = development.project;
    if (!chat.session || !project || development.binding?.session_id !== chat.id || development.binding.project_id !== project.project_id) return;
    setNewProject(project.project_id);
    setProjects(previous => [project, ...previous.filter(item => item.project_id !== project.project_id)]);
  }, [accountScope, chat.id, chat.session?.id, development.project?.project_id, development.project?.title, development.binding?.session_id, development.binding?.project_id]);
  const selectedProject = projects.find(project => project.project_id === newProject);
  const emptyGuide = !chat.session && chat.ready ? <div className="chat-empty-guide">
    <div><h2>{selectedProject ? `在 ${selectedProject.title} 中开始新任务` : "开始一个新任务"}</h2><p>{canDevelop ? "确认绑定项目与容器状态后直接输入任务，发送首条消息自动创建会话。" : "直接输入任务开始对话；当前接入未开放开发项目。"}</p></div>
    <div className="empty-setup-grid">
      <button type="button" className="empty-setup-card" data-kind="project" disabled={!chat.canWrite || !canDevelop} onClick={() => setProjectPickerOpen(true)}><span className="empty-setup-icon"><Folder aria-hidden="true"/></span><span className="empty-setup-body"><span>绑定项目（点击切换或新建）</span><strong>{selectedProject?.title ?? "选择或新建项目"}</strong><small>{selectedProject ? "发送首条任务后绑定此项目" : "选择当前任务使用的开发项目"}</small></span></button>
      <button type="button" className="empty-setup-card" data-kind="environment" disabled={!chat.ready || !canDevelop} onClick={() => openDevelopmentPanel(chat, "environment")}><span className="empty-setup-icon"><Terminal aria-hidden="true"/></span><span className="empty-setup-body"><span>沙箱与工具就绪状态</span><strong>Linux 容器{development.environment ? ` · ${development.environment.state === "ready" ? "运行中" : "未就绪"}` : ""}</strong><small>会话创建后载入项目环境与可用工具</small></span></button>
    </div>
    <div className="empty-quick-prompts">{[["排查深色主题对比度", "检查 frontend/src 下深色主题对比度不足的状态徽标并给出修复补丁"], ["运行前端契约测试", "运行前端契约测试并汇总失败用例原因"], ["梳理 Trace 返回路由", "梳理 Observability 模块深链返回对话的路由参数"]].map(([label, text]) => <button type="button" className="empty-prompt-pill" key={label} disabled={!chat.canWrite} onClick={() => { chat.edit(text!); document.getElementById("chat-prompt")?.focus(); }}>{label}</button>)}</div>
  </div> : undefined;
  return <section hidden={!active} className="chat-workbench" aria-label="对话工作台">
    <section className={`chat-main${workMaximized ? " is-hidden-by-max" : ""}`} aria-label="对话">
      {(chat.notice || startNotice || chat.pending.length > 0 || chat.pendingControls.length > 0) && <div className="chat-feedback-region">
        {(chat.notice || startNotice) && <div className="chat-notice"><p role="status">{chat.notice || startNotice}</p>{!chat.session && <Button variant="outline" aria-label="刷新对话" onClick={chat.refresh}><RefreshCw aria-hidden="true" />刷新</Button>}{chat.notice?.includes("连接") && <Button variant="outline" onClick={onConnect}>连接</Button>}</div>}
        {chat.pending.length > 0 && <div className="chat-pending" role="status"><strong>{chat.pending.every(chat.posting) ? "提交中" : c.unknown}</strong>{chat.pending.map(intent => <div key={intent.id}><span data-ui-data>{intent.kind === "session" ? intent.body.title : chat.list.find(value => value.id === intent.session_id)?.title ?? (intent.session_id ?? "").slice(0, 8)}</span><Button variant="outline" disabled={chat.posting(intent)} onClick={() => void chat.recover(intent)}>{c.find}</Button></div>)}</div>}
        {chat.pendingControls.map(value => <div className="chat-pending" key={String(value.body.request_key)}><span>操作结果未确认</span><Button variant="outline" disabled={value.rejected && (value.target.includes("/approvals/") ? !chat.canApprove : !chat.canWrite)} onClick={() => void chat.recoverControl(value)}>{value.rejected ? "重试原操作" : "查找操作结果"}</Button></div>)}
      </div>}
      <ChatHistory chat={chat} onTrace={turnId => { if (active && chat.session?.id) onTrace?.(chat.workspace, chat.session.id, turnId); }} empty={emptyGuide}>
        <SessionFeatures key={JSON.stringify([apiKey, chat.actor?.principal_id, chat.actor?.generation, chat.workspace, chat.id])} chat={chat} apiKey={apiKey} active={active} fence={fence} />
      </ChatHistory>
      {workspaceMode ? <details className="workspace-task-area"><summary><strong>工作区任务</strong><span>输入文件、工作请求与完成条件</span></summary><WorkspacePanel key={JSON.stringify([chat.actor?.principal_id, chat.actor?.generation, chat.workspace, chat.id])} chat={chat} apiKey={apiKey} active={active} fence={fence} /></details> : <ChatComposer chat={chat} busy={!!starting} onStart={starting ? undefined : () => { void start(); }} />}
      <footer className="chat-controls" aria-label="会话控制栏">
        <div className="chat-controls-left"><button type="button" className="control-chip" disabled={!chat.ready} onClick={() => openDevelopmentPanel(chat, "environment")}><Terminal aria-hidden="true"/>Linux 环境</button><button type="button" className="control-chip" disabled={!chat.ready} onClick={() => chat.session ? openDevelopmentPanel(chat, "project") : setProjectPickerOpen(true)}><Folder aria-hidden="true"/>{development.project?.title ?? projects.find(project => project.project_id === newProject)?.title ?? "选择项目"}</button></div>
        <div className="chat-controls-right">
          <button type="button" ref={modelRef} className="control-chip is-strong" aria-haspopup="menu" aria-expanded={info === "model"} onClick={() => setInfo(info === "model" ? null : "model")}><span className="control-model-name" title={model}>{model}</span><ChevronDown aria-hidden="true"/></button>
          <button type="button" ref={extraRef} className="control-chip" aria-haspopup="menu" aria-expanded={info === "extra"} onClick={() => setInfo(info === "extra" ? null : "extra")}>Extra<ChevronDown aria-hidden="true"/></button>
          {!chat.session && <button type="button" className="control-chip" title="创建会话后可切换权限模式" disabled>权限模式</button>}
          {chat.session && <SessionExecutionControl execution={chat.execution} canChange={chat.canWrite && chat.session.status === "open" && !chat.session.archived_at && !chat.session.deleted_at} />}
          <button type="button" className="context-meter" disabled={!chat.ready} aria-label={`上下文用量 ${contextCount(context.used)} / ${contextCount(context.limit)}`} title={`${contextCount(context.used)} / ${contextCount(context.limit)} tokens`} onClick={() => openDevelopmentPanel(chat, "context")}><svg width="14" height="14" viewBox="0 0 16 16" aria-hidden="true"><circle cx="8" cy="8" r="6" fill="none" stroke="currentColor" strokeWidth="2" opacity=".22"/><circle cx="8" cy="8" r="6" fill="none" stroke="currentColor" strokeWidth="2" strokeDasharray={`${ratio * 37.7} 37.7`} transform="rotate(-90 8 8)"/></svg><span>{context.used !== null && context.limit ? `${Math.round(context.used / context.limit * 100)}%` : "—"}</span></button>
        </div>
      </footer>
      <PopoverMenu open={info !== null} anchor={info === "model" ? modelRef.current : extraRef.current} placement="above" onClose={() => setInfo(null)} label={info === "model" ? "模型信息" : "运行配置"}>
        <p className="control-info-content">{info === "model" ? context.data?.configuration?.model ? `当前运行模型：${model}` : context.turnId ? "本轮未提供模型名称。模型接入在账号配置中管理。" : "完成首轮运行后显示实际模型配置。模型接入在账号配置中管理。" : `推理强度：${context.data?.configuration?.reasoning_effort ?? "未提供"}；思考配置：${context.data?.configuration?.thinking ?? "未提供"}`}</p>
      </PopoverMenu>
      <Dialog open={projectPickerOpen} onOpenChange={setProjectPickerOpen}><DialogContent className="new-session-project-dialog" aria-label="选择新会话项目" aria-describedby={undefined}><ProjectDialogBody key={accountScope} dev={development} apiKey={apiKey} projectId={newProject || null} workspace={chat.workspace} canCreate={chat.canWrite} onClose={() => setProjectPickerOpen(false)} onChoose={id => { setNewProject(id); void fetchProjectsPage(apiKey).then(value => { if (liveScope.current === accountScope) setProjects(value.items.filter(project => project.workspace_id === chat.workspace)); }).catch(error => { if (liveScope.current === accountScope) setStartNotice(chatReason(error)); }); }}/></DialogContent></Dialog>
    </section>
    <DevelopmentPanel key={JSON.stringify([chat.actor?.principal_id, chat.actor?.generation, chat.workspace, chat.id])} chat={chat} apiKey={apiKey} active={active} fence={fence} development={development} onMaximizedChange={setWorkMaximized} dark={theme === "dark"} context={context} onContextTrace={() => { if (context.turnId && chat.id) onTrace?.(chat.workspace, chat.id, context.turnId, "context"); }}/>
  </section>;
}
