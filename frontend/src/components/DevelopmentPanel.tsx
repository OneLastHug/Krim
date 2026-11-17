import { useCallback, useEffect, useRef, useState, type KeyboardEvent, type RefObject } from "react";
import {
  Archive, ChevronRight, ChevronUp, CodeXml, Columns2, Download, FileText, Folder, FolderOpen,
  Image, KeyRound, PanelRight, Plus, Shield, Terminal, X,
} from "lucide-react";
import { parseTurn, sessionPath, type Turn } from "../lib/session-contracts.ts";
import {
  devReason, environmentLabels, environmentRowLabel, fmtBytes, fmtCount, parseProject,
  projectStatusLabels, shortHost, type DirectoryPage, type EnvironmentState, type McpConnection,
  type SandboxProject, type ToolEntry,
} from "../lib/development-contracts.ts";
import { fetchDirectory, fetchFileBytes, fetchProjectsPage, useDevelopment, type DevelopmentState } from "../hooks/use-development.ts";
import type { SessionState } from "../hooks/use-session.ts";
import { useSessionCompaction } from "../hooks/use-session-compaction";
import { request } from "../api.ts";
import { parsePreviewBootstrap, registeredPreviews } from "../lib/development-previews";
import { displayToolName } from "../lib/tool-names";
import { Button } from "./ui/button.tsx";
import { Input } from "./ui/input.tsx";
import { Dialog, DialogClose, DialogContent, DialogDescription, DialogHeader, DialogTitle } from "./ui/dialog.tsx";
import { PopoverMenu } from "./popover-menu.tsx";
import "./development-panel.css";

// 开发信息栏与工作区：结构对应已验收的 H 原型（Environment/Context/Outputs + 右侧标签工作区），
// 数据全部来自真实接口；后端尚未提供的能力一律"敬请期待"，不使用模拟数据。

type WorkTab =
  | { id: string; type: "browser"; title: string }
  | { id: string; type: "document"; title: string; path: string | null }
  | { id: string; type: "aux"; title: string; sessionId: string };

type PanelDialog =
  | { kind: "coming"; label: string }
  | { kind: "environment" }
  | { kind: "project" }
  | { kind: "secrets" }
  | { kind: "context" }
  | { kind: "tool"; tool: ToolEntry }
  | { kind: "connection"; connection: McpConnection };

function useMediaQuery(query: string, fallbackWidth: number) {
  const [matches, setMatches] = useState(() =>
    (typeof window !== "undefined" && window.matchMedia?.(query).matches) ?? window.innerWidth <= fallbackWidth);
  useEffect(() => {
    const media = window.matchMedia?.(query);
    if (!media) {
      const update = () => setMatches(window.innerWidth <= fallbackWidth);
      window.addEventListener("resize", update);
      return () => window.removeEventListener("resize", update);
    }
    const update = () => setMatches(media.matches);
    update();
    media.addEventListener?.("change", update);
    return () => media.removeEventListener?.("change", update);
  }, [query, fallbackWidth]);
  return matches;
}

const riskLabel = (risk: string | null) => (risk === "read" ? "只读" : risk === "write" ? "写操作" : risk ? risk : "未标注");
const IMAGE_EXTENSIONS = [".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".bmp", ".ico"];
const isImagePath = (path: string) => IMAGE_EXTENSIONS.some(ext => path.toLowerCase().endsWith(ext));
const lastSegment = (path: string) => path.split("/").at(-1) ?? path;

const EMPTY_TABS: WorkTab[] = [];

export function DevelopmentPanel({ chat, apiKey, active, fence }: {
  chat: SessionState; apiKey: string; active: boolean; fence: { current: number };
}) {
  const dev = useDevelopment(chat, apiKey, active, fence);
  const compaction = useSessionCompaction(chat, apiKey, active, fence);
  const narrow = useMediaQuery("(max-width: 1180px)", 1180);
  const workInline = useMediaQuery("(min-width: 1440px)", 1439);
  const [infoOpen, setInfoOpen] = useState(() => window.innerWidth > 1180);
  const [workOpen, setWorkOpen] = useState(false);
  const [split, setSplit] = useState(false);
  const [activeSlot, setActiveSlot] = useState(0);
  const [slots, setSlots] = useState<WorkTab[][]>([EMPTY_TABS, EMPTY_TABS]);
  const [activeTabs, setActiveTabs] = useState<(string | null)[]>([null, null]);
  const [dialog, setDialog] = useState<PanelDialog | null>(null);
  const [addMenuSlot, setAddMenuSlot] = useState<{ slot: number; anchor: HTMLButtonElement } | null>(null);
  const outputsButton = useRef<HTMLButtonElement | null>(null);
  const identity = `${chat.actor?.principal_id ?? ""}:${chat.actor?.generation ?? ""}:${chat.workspace}:${chat.id}`;
  const previousIdentity = useRef(identity);
  const comingSoon = useCallback((label: string) => setDialog({ kind: "coming", label }), []);

  // 身份或会话变化：清空引用本会话的标签与打开中的对话框，避免旧会话内容或表单泄漏到新会话。
  useEffect(() => {
    if (previousIdentity.current === identity) return;
    previousIdentity.current = identity;
    setSlots([EMPTY_TABS, EMPTY_TABS]); setActiveTabs([null, null]); setSplit(false); setActiveSlot(0);
    setDialog(null); setAddMenuSlot(null);
  }, [identity]);
  const projectId = dev.binding?.project_id ?? null;

  const filesTab = useCallback((): WorkTab => ({ id: "project-files", type: "document", title: "项目文件", path: null }), []);
  function addTab(tab: WorkTab, slot = activeSlot) {
    setSlots(previous => previous.map((list, index) => index === slot && !list.some(item => item.id === tab.id) ? [...list, tab] : list));
    setActiveTabs(previous => previous.map((value, index) => index === slot ? tab.id : value));
    setActiveSlot(slot);
    setWorkOpen(true);
  }
  function closeTab(id: string, slot: number) {
    const previousList = slots[slot] ?? EMPTY_TABS;
    const previousIndex = previousList.findIndex(item => item.id === id);
    const remaining = previousList.filter(item => item.id !== id);
    setSlots(previous => previous.map((list, index) => index === slot ? list.filter(item => item.id !== id) : list));
    setActiveTabs(previous => previous.map((value, index) => {
      if (index !== slot || value !== id) return value;
      // 只在剩余标签中选邻居；列表为空时清空激活项，不留已关闭的 id。
      return remaining[Math.max(0, previousIndex - 1)]?.id ?? remaining[0]?.id ?? null;
    }));
  }
  function toggleSplit() {
    const first = slots[0] ?? EMPTY_TABS, second = slots[1] ?? EMPTY_TABS;
    if (!split && !second.length) {
      const seed = filesTab();
      setSlots([first, [seed]]);
      setActiveTabs([activeTabs[0] ?? null, seed.id]);
      setSplit(true);
      return;
    }
    if (split) {
      const merged = [...first];
      for (const tab of second) if (!merged.some(item => item.id === tab.id)) merged.push(tab);
      setSlots([merged, EMPTY_TABS]);
      setActiveTabs([activeTabs[0] ?? null, null]);
      setActiveSlot(0);
      setSplit(false);
    }
  }
  function toggleWork() {
    if (workOpen) { setWorkOpen(false); return; }
    if (!(slots[0] ?? EMPTY_TABS).length) addTab(filesTab(), 0);
    else setWorkOpen(true);
  }
  // Esc 关闭覆盖态；对话框自身处理 Esc，这里跳过。
  useEffect(() => {
    function onKeyDown(event: globalThis.KeyboardEvent) {
      if (event.key !== "Escape" || document.querySelector('[role="dialog"]')) return;
      if (workOpen && !workInline) { event.stopPropagation(); setWorkOpen(false); }
      else if (infoOpen && narrow) { event.stopPropagation(); setInfoOpen(false); }
    }
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [workOpen, workInline, infoOpen, narrow]);
  // 侧栏"在辅助会话打开"接线：仅在当前身份、代次、工作区一致且会话在列表内时打开只读 AuxView。
  useEffect(() => {
    function onOpenAux(event: Event) {
      const detail = (event as CustomEvent).detail;
      if (!active || !chat.ready || !chat.actor || typeof detail !== "object" || detail === null) return;
      const source = detail as { principal?: unknown; generation?: unknown; workspace?: unknown; sessionId?: unknown };
      if (source.principal !== chat.actor.principal_id || source.generation !== chat.actor.generation
        || source.workspace !== chat.workspace || typeof source.sessionId !== "string") return;
      const matched = chat.list.find(item => item.id === source.sessionId);
      if (!matched) return;
      addTab({ id: "aux:" + matched.id, type: "aux", title: matched.title, sessionId: matched.id });
    }
    window.addEventListener("krim:open-aux", onOpenAux);
    return () => window.removeEventListener("krim:open-aux", onOpenAux);
  }, [active, chat.ready, chat.actor, chat.workspace, chat.list]);

  const environmentState = dev.environment?.state ?? null;
  const policy = chat.session?.context_policy ?? null;
  const overlayWork = workOpen && !workInline;
  const drawerInfo = infoOpen && narrow;

  return <div className="dev-panel-root" data-info={infoOpen ? (drawerInfo ? "drawer" : "true") : "false"} data-work={workOpen ? (workInline ? "inline" : "overlay") : "false"}>
    {drawerInfo && <button type="button" className="g-info-backdrop" aria-label="关闭信息栏背景" onClick={() => setInfoOpen(false)}></button>}
    {overlayWork && <button type="button" className="g-work-backdrop" aria-label="关闭工作区背景" onClick={() => setWorkOpen(false)}></button>}
    {!infoOpen && <div className="dev-rail" role="toolbar" aria-label="开发面板开关" aria-orientation="vertical">
      <Button variant="ghost" className="dev-rail-button" aria-label="显示信息栏" aria-pressed={infoOpen} onClick={() => setInfoOpen(true)}><PanelRight aria-hidden="true" /></Button>
      <Button variant="ghost" className="dev-rail-button" aria-label={workOpen ? "关闭工作区" : "打开工作区"} aria-pressed={workOpen} onClick={toggleWork}><Columns2 aria-hidden="true" /></Button>
    </div>}
    {infoOpen && <section className="g-info" aria-label="开发信息栏">
      {(dev.notice || compaction.notice || dev.writes.length > 0) && <div className="dev-panel-status">
        {compaction.notice && <div className="dev-panel-notice" role="status"><p>{compaction.notice}</p><Button variant="ghost" size="icon-sm" aria-label="关闭整理提示" onClick={compaction.dismiss}><X aria-hidden="true" /></Button></div>}
        {dev.notice && <div className="dev-panel-notice" role="status"><p>{dev.notice}</p><Button variant="ghost" size="icon-sm" aria-label="关闭提示" onClick={dev.dismissNotice}><X aria-hidden="true" /></Button></div>}
        {dev.writes.map(intent => <div className="dev-panel-notice dev-pending" key={intent.request_key} role="status">
          <p>{intent.kind === "bind-project" ? "绑定项目结果未确认" : "准备环境结果未确认"}{intent.rejected ? "；原请求被拒绝" : ""}</p>
          {!intent.rejected && <Button variant="outline" size="sm" disabled={dev.busy} onClick={() => void dev.recoverPending(intent)}>查找结果</Button>}
          <Button variant="ghost" size="sm" onClick={() => dev.discardPending(intent)}>放弃</Button>
        </div>)}
      </div>}
      <section className="g-info-section environment">
        <div className="g-info-head">
          <h2>Environment</h2>
          <Button variant="ghost" size="icon-sm" aria-label={workOpen ? "关闭工作区" : "打开工作区"} aria-pressed={workOpen} onClick={toggleWork}><Columns2 aria-hidden="true" /></Button>
          <Button variant="ghost" size="icon-sm" aria-label="关闭信息栏" onClick={() => setInfoOpen(false)}><X aria-hidden="true" /></Button>
        </div>
        <div className="g-section-body g-environment-body">
          <EnvRow icon={Terminal} label="Linux 项目环境" value={projectId ? environmentRowLabel(environmentState) : "未准备"}
            state={projectId ? environmentState : null} onClick={() => setDialog({ kind: "environment" })} />
          <EnvRow icon={Folder} label={dev.project?.title ?? "选择项目"} value={projectId ? "切换" : "绑定"}
            title={dev.project?.title} onClick={() => setDialog({ kind: "project" })} />
          <EnvRow icon={KeyRound} label="Secret" value={projectId ? "引用管理" : "未绑定"}
            onClick={() => setDialog(projectId ? { kind: "secrets" } : { kind: "project" })} />
          <EnvRow icon={CodeXml} label="终端" value="暂未开放" onClick={() => comingSoon("终端")} />
          <EnvRow icon={Shield} label="项目授权" value={projectId ? "管理" : "未绑定"} onClick={() => comingSoon("项目授权管理")} />
        </div>
      </section>
      <section className="g-info-section context">
        <div className="g-info-head">
          <h2>Context</h2>
          <Button variant="ghost" size="icon-sm" aria-label="上下文用量说明" onClick={() => setDialog({ kind: "context" })}><EllipsisIcon /></Button>
        </div>
        <div className="g-section-body g-context-scroll">
          <div className="g-context-overview">
            <span>{policy ? "会话上下文策略" : "会话载入后显示上下文容量"}</span>
            {policy && <span>{fmtBytes(policy.max_bytes)} · {policy.max_messages === null ? "不设消息数上限" : `${fmtCount(policy.max_messages)} 条消息`}</span>}
          </div>
          {policy && <table className="g-context-table" aria-label="上下文策略容量">
            <tbody>
              <tr><td>总容量</td><td>{fmtBytes(policy.max_bytes)}</td><td>—</td></tr>
              <tr><td>消息上限</td><td>{policy.max_messages === null ? "不设消息数上限" : `${fmtCount(policy.max_messages)} 条`}</td><td>—</td></tr>
              <tr><td>输出预留</td><td>{fmtCount(policy.reserved_output_tokens)} tokens</td><td>—</td></tr>
            </tbody>
          </table>}
          <p className="dev-context-note">上下文用量未提供。</p>
          <details className="g-detail">
            <summary><ChevronRight aria-hidden="true" />MCP tools<span className="g-detail-count">{dev.tools.length}</span></summary>
            <div className="g-detail-list" tabIndex={0} aria-label="会话工具目录">
              {dev.tools.length === 0 && <p className="dev-detail-empty">当前目标没有可用工具</p>}
              {dev.tools.map((tool, index) => <button type="button" className="g-tool-row" key={`${tool.name}:${index}`} title={displayToolName(tool.name)}
                onClick={() => setDialog({ kind: "tool", tool })}>
                <span>{displayToolName(tool.name)}</span><span>{riskLabel(tool.risk)}</span>
              </button>)}
            </div>
          </details>
          <details className="g-detail">
            <summary><ChevronRight aria-hidden="true" />账号 MCP 连接<span className="g-detail-count">{dev.connections === null ? "—" : dev.connections.length}</span></summary>
            <div className="g-detail-list" tabIndex={0} aria-label="账号 MCP 连接">
              {dev.connections === null && <p className="dev-detail-empty">账号 MCP 服务暂不可用</p>}
              {dev.connections?.length === 0 && <p className="dev-detail-empty">还没有账号 MCP 连接</p>}
              {dev.connections?.map(connection => <button type="button" className="g-tool-row" key={connection.id} title={connection.name}
                onClick={() => setDialog({ kind: "connection", connection })}>
                <span>{connection.name}</span><span>{connection.credential_configured ? "凭据已配置" : "未配置凭据"}</span>
              </button>)}
            </div>
          </details>
          <details className="g-detail">
            <summary><ChevronRight aria-hidden="true" />Memory files<span className="g-detail-count">—</span></summary>
            <div className="g-detail-list g-memory-list" tabIndex={0} aria-label="记忆来源列表">
              <p className="dev-detail-empty">记忆来源未提供</p>
            </div>
          </details>
          <div className="g-context-end">
            <span>整理在运行时内自动进行</span>
            <Button variant="outline" size="sm" disabled={compaction.disabled} onClick={() => void compaction.compact()}>Compact session</Button>
          </div>
        </div>
      </section>
      <section className="g-info-section outputs">
        <div className="g-info-head">
          <h2>Outputs</h2>
          <Button variant="ghost" size="icon-sm" aria-label="在分区中打开内容" aria-haspopup="menu"
            ref={outputsButton as RefObject<HTMLButtonElement>} onClick={() => {
              if (outputsButton.current) setAddMenuSlot({ slot: 0, anchor: outputsButton.current });
            }}><Plus aria-hidden="true" /></Button>
        </div>
        <div className="g-section-body g-output-scroll">
          <button type="button" className="g-output-row" onClick={() => addTab(filesTab(), 0)}>
            <FolderOpen aria-hidden="true" /><span>项目文件</span>
          </button>
          <button type="button" className="g-output-row" onClick={() => addTab({ id: "browser", type: "browser", title: "浏览器预览" }, 0)}>
            <PanelRight aria-hidden="true" /><span>浏览器预览</span>
          </button>
          <button type="button" className="g-output-row" onClick={() => comingSoon("项目打包下载")}>
            <Archive aria-hidden="true" /><span>下载项目文件</span>
          </button>
        </div>
      </section>
    </section>}
    {workOpen && <section className={`g-work${workInline ? "" : " is-overlay"}`} aria-label="开发工作区">
      <div className="g-work-topbar">
        <strong>工作区</strong>
        <Button variant="ghost" size="icon-sm" aria-label={split ? "合并分区" : "上下分区"} aria-pressed={split}
          title={split ? "合并分区" : "上下分区"} onClick={toggleSplit}><Columns2 aria-hidden="true" /></Button>
        <Button variant="ghost" size="icon-sm" aria-label="关闭工作区" onClick={() => setWorkOpen(false)}><X aria-hidden="true" /></Button>
      </div>
      <div className={`g-work-stack${split ? " split" : ""}`}>
        {(split ? [0, 1] : [0]).map(slot => <WorkSlot key={slot} slot={slot} label={split ? (slot ? "下方工作区" : "上方工作区") : "工作区内容"}
          tabs={slots[slot] ?? EMPTY_TABS} activeTab={activeTabs[slot] ?? null} split={split}
          onSelect={id => { setActiveTabs(previous => previous.map((value, index) => index === slot ? id : value)); setActiveSlot(slot); }}
          onClose={id => closeTab(id, slot)}
          onAdd={anchor => setAddMenuSlot({ slot, anchor })}
          onCloseSplit={toggleSplit}
          onComing={comingSoon}
          chat={chat} apiKey={apiKey} projectId={projectId} environmentState={environmentState}
          onOpenEnvironment={() => setDialog({ kind: "environment" })}
          onOpenFile={(path, title) => addTab({ id: "file:" + path, type: "document", title, path }, slot)} />)}
      </div>
    </section>}
    <PopoverMenu open={addMenuSlot !== null} anchor={addMenuSlot?.anchor ?? null} onClose={() => setAddMenuSlot(null)} label="在分区中打开内容">
      <button type="button" role="menuitem" className="menu-item" onClick={() => { const target = addMenuSlot?.slot ?? 0; setAddMenuSlot(null); addTab({ id: "browser", type: "browser", title: "浏览器预览" }, target); }}>浏览器预览</button>
      <button type="button" role="menuitem" className="menu-item" onClick={() => { const target = addMenuSlot?.slot ?? 0; setAddMenuSlot(null); addTab(filesTab(), target); }}>项目文件</button>
      <button type="button" role="menuitem" className="menu-item" disabled={!chat.session} onClick={() => {
        const target = addMenuSlot?.slot ?? 0; setAddMenuSlot(null);
        if (chat.session) addTab({ id: "aux:" + chat.session.id, type: "aux", title: chat.session.title, sessionId: chat.session.id }, target);
      }}>辅助会话</button>
    </PopoverMenu>
    <DevelopmentDialogs dialog={dialog} onClose={() => setDialog(null)} dev={dev} chat={chat} apiKey={apiKey}
      projectId={projectId} project={dev.project} environmentState={environmentState} />
  </div>;
}

function EllipsisIcon() {
  return <svg aria-hidden="true" viewBox="0 0 24 24" width="14" height="14" fill="currentColor"><circle cx="5" cy="12" r="1.6" /><circle cx="12" cy="12" r="1.6" /><circle cx="19" cy="12" r="1.6" /></svg>;
}

function EnvRow({ icon: Icon, label, value, state, onClick, title }: {
  icon: typeof Folder; label: string; value: string; state?: EnvironmentState | null; onClick: () => void; title?: string;
}) {
  return <button type="button" className="g-env-row" onClick={onClick} title={title ?? label}>
    <Icon aria-hidden="true" />
    <span className="g-env-label">{label}</span>
    {state && <span className={`g-state-mini${state === "ready" ? " ready" : state === "failed" ? " failed" : ""}`} role="img"
      aria-label={environmentLabels[state]}></span>}
    <span className="g-env-value">{value}</span>
    <ChevronRight aria-hidden="true" />
  </button>;
}

function WorkSlot({ slot, label, tabs, activeTab, split, onSelect, onClose, onAdd, onCloseSplit, onComing, chat, apiKey, projectId, environmentState, onOpenEnvironment, onOpenFile }: {
  slot: number; label: string; tabs: WorkTab[]; activeTab: string | null; split: boolean;
  onSelect: (id: string) => void; onClose: (id: string) => void; onAdd: (anchor: HTMLButtonElement) => void;
  onCloseSplit: () => void; onComing: (label: string) => void;
  chat: SessionState; apiKey: string; projectId: string | null; environmentState: EnvironmentState | null;
  onOpenEnvironment: () => void;
  onOpenFile: (path: string, title: string) => void;
}) {
  const current = tabs.find(item => item.id === activeTab) ?? tabs[0] ?? null;
  const addButton = useRef<HTMLButtonElement | null>(null);
  function onTabKeyDown(event: KeyboardEvent<HTMLDivElement>) {
    const target = event.target as HTMLElement;
    const id = target.dataset.tab;
    if (!id || !["ArrowLeft", "ArrowRight", "Home", "End"].includes(event.key)) return;
    event.preventDefault();
    const index = tabs.findIndex(item => item.id === id);
    if (index < 0) return;
    const next = event.key === "Home" ? 0 : event.key === "End" ? tabs.length - 1
      : (index + (event.key === "ArrowRight" ? 1 : -1) + tabs.length) % tabs.length;
    const nextTab = tabs[next];
    if (nextTab) onSelect(nextTab.id);
  }
  return <section className="g-work-slot" data-slot-index={slot} aria-label={label}>
    <div className="g-work-tabbar">
      <div className="g-work-tabs" role="tablist" aria-label={`${label}标签`} onKeyDown={onTabKeyDown}>
        {tabs.map(tab => <div className="g-work-tab" role="presentation" key={tab.id}>
          <button type="button" role="tab" id={`dev-tab-${slot}-${tab.id.replace(/[^a-zA-Z0-9-]/g, "-")}`}
            aria-selected={current?.id === tab.id} aria-controls={`dev-work-content-${slot}`}
            tabIndex={current?.id === tab.id ? 0 : -1} data-tab={tab.id} title={tab.title}
            onClick={() => onSelect(tab.id)}>{tab.title}</button>
          <button type="button" aria-label={`关闭 ${tab.title}`} onClick={() => onClose(tab.id)}><X aria-hidden="true" /></button>
        </div>)}
      </div>
      <Button variant="ghost" size="icon-sm" aria-label={`在${slot ? "下方" : "上方"}分区打开内容`} aria-haspopup="menu"
        ref={addButton as RefObject<HTMLButtonElement>} onClick={() => { if (addButton.current) onAdd(addButton.current); }}><Plus aria-hidden="true" /></Button>
      {split && slot === 1 && <Button variant="ghost" size="icon-sm" aria-label="关闭下方分区" onClick={onCloseSplit}><X aria-hidden="true" /></Button>}
    </div>
    <div className="g-work-content" id={`dev-work-content-${slot}`} role="tabpanel" aria-label={current?.title ?? "空工作区"} tabIndex={0}>
      {!current && <div className="g-work-empty"><strong>打开内容</strong><span>使用上方 + 选择浏览器预览、项目文件或辅助会话。</span></div>}
      {current?.type === "browser" && <BrowserView chat={chat} apiKey={apiKey} projectId={projectId} environmentState={environmentState} onOpenEnvironment={onOpenEnvironment} />}
      {current?.type === "document" && (current.path === null
        ? <FileBrowser apiKey={apiKey} projectId={projectId} onOpenFile={onOpenFile} />
        : <FileView apiKey={apiKey} projectId={projectId} path={current.path} />)}
      {current?.type === "aux" && <AuxView chat={chat} apiKey={apiKey} sessionId={current.sessionId} onComing={onComing} />}
    </div>
  </section>;
}

function BrowserView({ chat, apiKey, projectId, environmentState, onOpenEnvironment }: {
  chat: SessionState; apiKey: string; projectId: string | null; environmentState: EnvironmentState | null; onOpenEnvironment: () => void;
}) {
  const previews = registeredPreviews(chat, projectId), [busy, setBusy] = useState(false), [error, setError] = useState("");
  const alive = useRef(true);
  useEffect(() => { alive.current = true; return () => { alive.current = false; }; }, []);
  async function openPreview(id: string) {
    if (busy || !chat.ready) return;
    const name = "krim-preview-" + crypto.randomUUID(), tab = window.open("about:blank", name);
    if (!tab) { setError("请允许打开项目预览窗口"); return; }
    tab.opener = null; setBusy(true); setError("");
    try {
      const bootstrap = parsePreviewBootstrap(await request(`/api/development/previews/${encodeURIComponent(id)}/open`, apiKey, {}));
      if (!alive.current) { tab.close(); return; }
      // A one-use ticket travels only in the POST body to the authenticated API's
      // validated HTTPS bootstrap action, never in a URL or persisted browser state.
      const form = document.createElement("form"), ticket = document.createElement("input");
      form.method = "POST"; form.action = bootstrap.action; form.target = name; form.hidden = true;
      ticket.type = "hidden"; ticket.name = "ticket"; ticket.value = bootstrap.ticket; form.append(ticket);
      document.body.append(form); try { form.submit(); } finally { ticket.value = ""; form.remove(); }
    } catch { tab.close(); if (alive.current) setError("预览打开未完成，请检查环境、授权或预览状态"); }
    finally { if (alive.current) setBusy(false); }
  }
  const reason = !projectId ? "绑定项目后可打开开发服务的页面预览。"
    : environmentState !== "ready" ? "项目环境尚未就绪。"
      : "页面预览凭票据打开到独立窗口；运行记录中还没有已注册的预览。";
  return <div className="g-work-empty">
    <strong>浏览器预览</strong>
    {!previews.length ? <span>{reason}</span> : previews.map(preview => <div className="g-row" key={preview.preview_id}><span>端口 {preview.port} · {preview.state === "ready" ? "已就绪" : preview.state === "pending" ? "准备中" : "已关闭或状态未知"}</span><Button size="sm" variant="outline" disabled={busy || !["ready", "pending"].includes(preview.state)} onClick={() => void openPreview(preview.preview_id)}>在新窗口打开</Button></div>)}
    {error && <p role="status">{error}</p>}
    <div className="g-inline-actions"><Button variant="outline" size="sm" onClick={onOpenEnvironment}>项目环境</Button></div>
  </div>;
}

function FileBrowser({ apiKey, projectId, onOpenFile }: {
  apiKey: string; projectId: string | null; onOpenFile: (path: string, title: string) => void;
}) {
  const [path, setPath] = useState("."), [offset, setOffset] = useState(0);
  function navigate(next: string) { setOffset(0); setPath(next); }
  const [page, setPage] = useState<DirectoryPage | null>(null);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);
  const epoch = useRef(0);
  useEffect(() => {
    if (!projectId) return;
    const id = ++epoch.current;
    const controller = new AbortController();
    setLoading(true); setError("");
    void fetchDirectory(apiKey, projectId, path, controller.signal, offset).then(value => {
      if (epoch.current !== id || controller.signal.aborted) return;
      setPage(value);
    }).catch(value => {
      if (epoch.current !== id || controller.signal.aborted) return;
      setPage(null);
      setError(devReason(value));
    }).finally(() => { if (epoch.current === id) setLoading(false); });
    return () => { epoch.current++; controller.abort(); };
  }, [apiKey, projectId, path, offset]);
  if (!projectId) return <div className="g-work-empty"><strong>未绑定项目</strong><span>绑定项目后可以浏览项目文件。</span></div>;
  const parts = path === "." ? [] : path.split("/");
  return <div className="dev-file-browser">
    <div className="g-work-address">
      <button type="button" className="dev-crumb" onClick={() => navigate(".")}>项目根目录</button>
      {parts.map((part, index) => <button type="button" className="dev-crumb" key={index} title={part}
        onClick={() => navigate(parts.slice(0, index + 1).join("/"))}>{part}</button>)}
      {path !== "." && <Button variant="ghost" size="icon-sm" aria-label="返回上一级" onClick={() => navigate(parts.slice(0, -1).join("/") || ".")}><ChevronUp aria-hidden="true" /></Button>}
    </div>
    {loading && <p className="dev-detail-empty" role="status">读取中…</p>}
    {error && <p className="dev-detail-empty dev-detail-error" role="status">{error}</p>}
    {!loading && !error && page && <div className="dev-file-list" role="list" aria-label={`${path === "." ? "项目根目录" : path} 的文件`}>
      {page.entries.length === 0 && <p className="dev-detail-empty">目录为空</p>}
      {page.entries.map(entry => entry.type === "directory"
        ? <button type="button" className="g-tool-row dev-file-row" role="listitem" key={entry.name} title={entry.name}
          onClick={() => navigate(path === "." ? entry.name : `${path}/${entry.name}`)}>
          <Folder aria-hidden="true" /><span>{entry.name}</span><span>{fmtBytes(entry.size)}</span>
        </button>
        : <button type="button" className="g-tool-row dev-file-row" disabled={entry.type === "blocked"} role="listitem" key={entry.name} title={entry.type === "blocked" ? "受限文件" : entry.name}
          onClick={() => onOpenFile(path === "." ? entry.name : `${path}/${entry.name}`, entry.name)}>
          {isImagePath(entry.name) ? <Image aria-hidden="true" /> : <FileText aria-hidden="true" />}
          <span>{entry.name}</span><span>{fmtBytes(entry.size)}</span>
        </button>)}
      {(offset > 0 || page.next_offset !== null) && <div className="g-inline-actions"><Button variant="ghost" size="sm" disabled={offset === 0} onClick={() => setOffset(Math.max(0, offset - 100))}>上一页</Button><span>{offset + 1}–{Math.min(offset + page.entries.length, page.total)} / {page.total}</span><Button variant="ghost" size="sm" disabled={page.next_offset === null} onClick={() => { if (page.next_offset !== null) setOffset(page.next_offset); }}>下一页</Button></div>}
    </div>}
  </div>;
}

function blobType(path: string): string {
  const extension = path.toLowerCase().split(".").at(-1) ?? "";
  const types: Record<string, string> = { png: "image/png", jpg: "image/jpeg", jpeg: "image/jpeg", gif: "image/gif", webp: "image/webp", svg: "image/svg+xml", bmp: "image/bmp", ico: "image/x-icon" };
  return types[extension] ?? "application/octet-stream";
}

async function downloadProjectFile(apiKey: string, projectId: string, path: string) {
  const response = await fetch(`/v2/me/sandbox/projects/${encodeURIComponent(projectId)}/export-file?path=${encodeURIComponent(path)}`,
    { headers: apiKey ? { Authorization: "Bearer " + apiKey } : {}, redirect: "error" });
  if (!response.ok) throw new Error("下载未完成");
  const blob = await response.blob();
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = lastSegment(path);
  document.body.append(link);
  link.click();
  link.remove();
  setTimeout(() => URL.revokeObjectURL(url), 10000);
}

function FileView({ apiKey, projectId, path }: {
  apiKey: string; projectId: string | null; path: string;
}) {
  const [state, setState] = useState<{ url?: string; text?: string; truncated?: boolean; error?: string; loading: boolean }>({ loading: true });
  const objectUrl = useRef<string | null>(null);
  const image = isImagePath(path);
  useEffect(() => {
    if (!projectId) return;
    const controller = new AbortController();
    setState({ loading: true });
    void fetchFileBytes(apiKey, projectId, path, controller.signal).then(value => {
      if (controller.signal.aborted) return;
      const truncated = !value.eof;
      if (image) {
        const blob = new Blob([value.bytes], { type: blobType(path) });
        if (objectUrl.current) URL.revokeObjectURL(objectUrl.current);
        objectUrl.current = URL.createObjectURL(blob);
        setState({ url: objectUrl.current, truncated, loading: false });
      } else {
        setState({ text: new TextDecoder("utf-8", { fatal: false }).decode(value.bytes), truncated, loading: false });
      }
    }).catch(value => {
      if (!controller.signal.aborted) setState({ error: devReason(value), loading: false });
    });
    return () => { controller.abort(); };
  }, [apiKey, projectId, path, image]);
  useEffect(() => () => { if (objectUrl.current) URL.revokeObjectURL(objectUrl.current); }, []);
  if (!projectId) return <div className="g-work-empty"><strong>未绑定项目</strong><span>绑定项目后可以查看项目文件。</span></div>;
  if (state.loading) return <p className="dev-detail-empty" role="status">读取中…</p>;
  if (state.error) return <div className="g-work-empty"><strong>读取未完成</strong><span>{state.error}</span></div>;
  const name = lastSegment(path);
  return image
    ? <figure className="g-picture"><img src={state.url} alt={name} /><figcaption>{name} · {state.truncated ? "仅预览前段" : "完整内容"}</figcaption></figure>
    : <article className="g-document">
      <div className="g-source-line">项目文件 · {path}</div>
      <h1>{name}</h1>
      <pre>{state.text}</pre>
      {state.truncated && <div className="dev-file-truncated">
        <span>文件较大，仅显示前段。</span>
        <Button variant="outline" size="sm" onClick={() => void downloadProjectFile(apiKey, projectId, path).catch(() => undefined)}>
          <Download aria-hidden="true" />下载完整文件
        </Button>
      </div>}
    </article>;
}

function AuxView({ chat, apiKey, sessionId, onComing }: {
  chat: SessionState; apiKey: string; sessionId: string; onComing: (label: string) => void;
}) {
  const [turns, setTurns] = useState<Turn[] | null>(null);
  const [error, setError] = useState("");
  const selected = chat.session?.id === sessionId ? chat.session : chat.list.find(item => item.id === sessionId) ?? null;
  const epoch = useRef(0);
  useEffect(() => {
    const id = ++epoch.current;
    const controller = new AbortController();
    setTurns(null); setError("");
    void request(`${sessionPath(chat.workspace)}/${encodeURIComponent(sessionId)}/turns?offset=0&limit=50`, apiKey, undefined, controller.signal)
      .then(value => {
        if (epoch.current !== id || controller.signal.aborted) return;
        if (!Array.isArray(value)) throw new Error("历史无效");
        const parsed = value.map(parseTurn);
        if (parsed.some(turn => turn.session_id !== sessionId || turn.workspace_id !== chat.workspace)) throw new Error("历史身份不匹配");
        setTurns(parsed);
      })
      .catch(value => { if (epoch.current === id && !controller.signal.aborted) setError(devReason(value)); });
    return () => { epoch.current++; controller.abort(); };
  }, [apiKey, chat.workspace, sessionId]);
  const visible = (turns ?? []).filter(turn => turn.text || (turn.transcript?.length ?? 0) > 0);
  return <article className="g-aux">
    <h2>{selected?.title ?? "会话记录"}</h2>
    {!turns && !error && <p className="dev-detail-empty" role="status">读取中…</p>}
    {error && <p className="dev-detail-empty dev-detail-error" role="status">{error}</p>}
    {turns && visible.length === 0 && <p className="dev-detail-empty">这个会话还没有可显示的对话内容</p>}
    {visible.map(turn => <div key={turn.id} className="dev-aux-turn">
      {turn.text && <p className="g-aux-user">{turn.text}</p>}
      {(turn.transcript ?? []).map((message, index) => message.role === "assistant"
        ? <p key={index}>{message.content}</p>
        : message.role === "tool"
          ? <p key={index} className="dev-aux-tool">工具返回：{message.content.length > 200 ? message.content.slice(0, 200) + "…" : message.content}</p>
          : null)}
    </div>)}
    <div className="g-aux-footer">
      <span className="g-readonly-note">会话记录 · 只读</span>
      <div className="g-inline-actions start"><Button variant="outline" size="sm" onClick={() => onComing("辅助会话继续执行")}>在此继续</Button></div>
    </div>
  </article>;
}

function DevelopmentDialogs({ dialog, onClose, dev, chat, apiKey, projectId, project, environmentState }: {
  dialog: PanelDialog | null; onClose: () => void; dev: DevelopmentState;
  chat: SessionState; apiKey: string; projectId: string | null; project: SandboxProject | null;
  environmentState: EnvironmentState | null;
}) {
  return <Dialog open={dialog !== null} onOpenChange={open => { if (!open) onClose(); }}>
    <DialogContent className="dev-dialog" aria-describedby={undefined}>
      {dialog?.kind === "coming" && <>
        <DialogHeader><DialogTitle>敬请期待</DialogTitle></DialogHeader>
        <p className="g-answer">{dialog.label}暂未开放。</p>
        <div className="dev-dialog-footer"><DialogClose render={<Button variant="ghost" size="sm" />}>知道了</DialogClose></div>
      </>}
      {dialog?.kind === "environment" && <EnvironmentDialogBody dev={dev} project={project} projectId={projectId} environmentState={environmentState} onClose={onClose} />}
      {dialog?.kind === "project" && <ProjectDialogBody dev={dev} apiKey={apiKey} projectId={projectId} workspace={chat.workspace} onClose={onClose} />}
      {dialog?.kind === "secrets" && <SecretsDialogBody dev={dev} projectId={projectId} />}
      {dialog?.kind === "context" && <ContextDialogBody chat={chat} />}
      {dialog?.kind === "tool" && <ToolDialogBody tool={dialog.tool} />}
      {dialog?.kind === "connection" && <ConnectionDialogBody connection={dialog.connection} />}
    </DialogContent>
  </Dialog>;
}

function EnvironmentDialogBody({ dev, project, environmentState, onClose }: {
  dev: DevelopmentState; project: SandboxProject | null; projectId: string | null;
  environmentState: EnvironmentState | null; onClose: () => void;
}) {
  const [confirmStop, setConfirmStop] = useState(false);
  const busy = dev.preparing || dev.stopping;
  const transitional = environmentState !== null && ["creating", "starting", "stopping"].includes(environmentState);
  return <>
    <DialogHeader>
      <DialogTitle>项目环境</DialogTitle>
      <DialogDescription>{project ? project.title : "请先绑定项目。"}</DialogDescription>
    </DialogHeader>
    {project ? <>
      <dl className="g-description-list">
        <dt>状态</dt><dd>{environmentRowLabel(environmentState)}</dd>
        <dt>项目状态</dt><dd>{projectStatusLabels[project.status] ?? project.status}</dd>
        <dt>环境 ID</dt><dd className="g-mono">{dev.environment?.environment_id ?? "未创建"}</dd>
        <dt>项目文件</dt><dd>停止环境后保留</dd>
      </dl>
      {confirmStop && <div className="g-inline-confirm">
        <p>停止环境会结束项目中的进程和预览。文件与已保存的修改会保留。</p>
        <div className="g-inline-actions">
          <Button variant="ghost" size="sm" onClick={() => setConfirmStop(false)}>继续运行</Button>
          <Button variant="destructive" size="sm" disabled={busy} onClick={() => { setConfirmStop(false); void dev.stopEnvironment(); }}>停止环境</Button>
        </div>
      </div>}
    </> : <p className="g-empty">绑定项目后即可准备开发环境。</p>}
    <div className="dev-dialog-footer">
      <Button variant="ghost" size="sm" onClick={onClose}>关闭</Button>
      {project && dev.canWrite && !confirmStop && (environmentState === "ready"
        ? <Button variant="outline" size="sm" disabled={busy} onClick={() => setConfirmStop(true)}>停止环境</Button>
        : transitional
          ? <Button size="sm" disabled>{dev.preparing || dev.stopping ? "处理中…" : environmentRowLabel(environmentState) + "…"}</Button>
          : <Button size="sm" disabled={busy} onClick={() => void dev.prepareEnvironment()}>准备环境</Button>)}
    </div>
  </>;
}

function ProjectDialogBody({ dev, apiKey, projectId, workspace, onClose }: {
  dev: DevelopmentState; apiKey: string; projectId: string | null; workspace: string; onClose: () => void;
}) {
  const [page, setPage] = useState<SandboxProject[] | null>(null);
  const [selected, setSelected] = useState<string | null>(projectId);
  const [error, setError] = useState("");
  const [creating, setCreating] = useState(false);
  const [title, setTitle] = useState("");
  const [creatingBusy, setCreatingBusy] = useState(false);
  const epoch = useRef(0);
  // 对话框可能因身份切换被卸载；晚到响应不得写入新作用域。
  const alive = useRef(true);
  useEffect(() => {
    alive.current = true;
    return () => { alive.current = false; };
  }, []);
  // 创建结果未知时保留同一 request_key，重试由后端按键去重，不换新键盲发。
  const pendingCreate = useRef<{ request_key: string; workspace_id: string; title: string } | null>(null);
  useEffect(() => {
    const id = ++epoch.current;
    const controller = new AbortController();
    void fetchProjectsPage(apiKey, controller.signal).then(value => {
      if (!alive.current || epoch.current !== id || controller.signal.aborted) return;
      setPage(value.items.filter(item => item.workspace_id === workspace));
    }).catch(value => { if (alive.current && epoch.current === id && !controller.signal.aborted) setError(devReason(value)); });
    return () => { epoch.current++; controller.abort(); };
  }, [apiKey, workspace]);
  function createProject() {
    if (!title.trim() || creatingBusy || !dev.canWrite) return;
    setCreatingBusy(true); setError("");
    const body = pendingCreate.current ?? { request_key: crypto.randomUUID(), workspace_id: workspace, title: title.trim() };
    pendingCreate.current = body;
    void request("/v2/me/sandbox/projects", apiKey, body, undefined, "POST")
      .then(value => {
        if (!alive.current) return;
        const created = parseProject(value);
        if (created.workspace_id !== workspace) throw new Error("项目身份不匹配");
        pendingCreate.current = null;
        setCreating(false);
        setTitle("");
        setPage(currentPage => {
          const list = (currentPage ?? []).filter(item => item.project_id !== created.project_id);
          return [...list, created];
        });
        setSelected(created.project_id);
      })
      .catch(value => {
        if (!alive.current) return;
        setError(value instanceof TypeError ? "创建结果未知；已保留原请求，可重试同一请求" : devReason(value));
      })
      .finally(() => { if (alive.current) setCreatingBusy(false); });
  }
  return <>
    <DialogHeader>
      <DialogTitle>项目</DialogTitle>
      <DialogDescription>为这段会话绑定开发项目。</DialogDescription>
    </DialogHeader>
    {error && <p className="g-error" role="status">{error}</p>}
    <div className="g-project-options" role="radiogroup" aria-label="选择项目">
      {(page ?? []).map(item => <label className="g-project-option" key={item.project_id}>
        <input type="radio" name="dev-project" value={item.project_id} checked={selected === item.project_id}
          onChange={() => setSelected(item.project_id)} />
        <span><strong>{item.title}</strong><small>{projectStatusLabels[item.status] ?? item.status} · 更新于 {new Date(item.updated_at * 1000).toLocaleString()}</small></span>
      </label>)}
      {page !== null && page.length === 0 && <p className="dev-detail-empty">当前工作区还没有项目</p>}
      {page === null && !error && <p className="dev-detail-empty" role="status">读取中…</p>}
    </div>
    {creating && <form className="dev-create-form" onSubmit={event => { event.preventDefault(); createProject(); }}>
      <div className="g-field">
        <label htmlFor="dev-project-title">新项目名称</label>
        <Input id="dev-project-title" value={title} onChange={event => setTitle(event.target.value)} maxLength={128} autoComplete="off" />
      </div>
      <div className="g-inline-actions">
        <Button type="button" variant="ghost" size="sm" onClick={() => { setCreating(false); setTitle(""); setError(""); pendingCreate.current = null; }}>取消</Button>
        <Button type="submit" size="sm" disabled={creatingBusy || !title.trim() || !dev.canWrite}>{creatingBusy ? "创建中…" : "创建项目"}</Button>
      </div>
    </form>}
    <div className="dev-dialog-footer">
      {!creating && dev.canWrite && <Button variant="outline" size="sm" onClick={() => { setCreating(true); setError(""); }}>新建项目</Button>}
      <Button variant="ghost" size="sm" onClick={onClose}>关闭</Button>
      <Button size="sm" disabled={dev.busy || selected === projectId || !dev.canWrite}
        onClick={() => { void dev.bindProject(selected); onClose(); }}>{selected === null ? "解除绑定" : "绑定项目"}</Button>
    </div>
  </>;
}

function SecretsDialogBody({ dev, projectId }: {
  dev: DevelopmentState; projectId: string | null;
}) {
  const [name, setName] = useState("");
  const [value, setValue] = useState("");
  const [hours, setHours] = useState("24");
  const [result, setResult] = useState("");
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  const [revokeTarget, setRevokeTarget] = useState<string | null>(null);
  const [rotateTarget, setRotateTarget] = useState<string | null>(null);
  async function submit(mode: "add" | "rotate" | "revoke", secretName: string) {
    if (busy) return;
    setError(""); setResult("");
    setBusy(true);
    try {
      const message = await dev.writeSecret({ secretId: secretName, value: mode === "revoke" ? "revoked" : value, hours: Number(hours), mode });
      setResult(message);
      setValue("");
      setName("");
      setRotateTarget(null);
    } catch (writeError) {
      setError(writeError instanceof Error ? writeError.message : "写入未完成");
    } finally { setBusy(false); }
  }
  return <>
    <DialogHeader>
      <DialogTitle>Secret 引用</DialogTitle>
      <DialogDescription>命令只使用引用名称；面板不读取也不回显凭据内容。</DialogDescription>
    </DialogHeader>
    {!projectId && <p className="g-empty">绑定项目后即可添加 Secret 引用。</p>}
    {projectId && <>
      {result && <p className="dev-secret-result" role="status">{result}</p>}
      {error && <p className="g-error" role="status">{error}</p>}
      <form className="dev-secret-form" onSubmit={event => { event.preventDefault(); if (dev.canApprove) void submit(rotateTarget ? "rotate" : "add", rotateTarget ?? name); }}>
        <div className="g-field">
          <label htmlFor="dev-secret-name">引用名称</label>
          <Input id="dev-secret-name" disabled={!!rotateTarget || busy || !dev.canApprove} value={name} onChange={event => setName(event.target.value)} maxLength={64} autoComplete="off" placeholder="例如 API_TOKEN" />
        </div>
        <div className="g-field">
          <label htmlFor="dev-secret-value">凭据值（仅写入，不会显示或保存到本机）</label>
          <Input id="dev-secret-value" type="password" disabled={busy || !dev.canApprove} value={value} onChange={event => setValue(event.target.value)} maxLength={8192} autoComplete="new-password" />
        </div>
        <div className="g-field">
          <label htmlFor="dev-secret-hours">有效期（小时，1-24）</label>
          <Input id="dev-secret-hours" type="number" min={1} max={24} value={hours} onChange={event => setHours(event.target.value)} />
        </div>
        <div className="g-inline-actions">
          {rotateTarget && <Button type="button" variant="ghost" size="sm" disabled={busy} onClick={() => { setRotateTarget(null); setName(""); setValue(""); }}>取消轮换</Button>}
          <Button type="submit" size="sm" disabled={busy || !dev.canApprove || !name.trim() || !value}>{rotateTarget ? "确认轮换" : "添加引用"}</Button>
        </div>
      </form>
      {dev.secretRefs.length > 0 && <div className="g-divider"></div>}
      {dev.secretRefs.map(ref => <div className="g-row" key={ref.name}>
        <div className="g-row-main">
          <strong>{ref.name}</strong>
          <small className="g-mono">版本 {ref.revision} · 本会话写入</small>
        </div>
        <div className="g-row-actions">
          <Button variant="outline" size="sm" disabled={busy || !dev.canApprove} onClick={() => { setRotateTarget(ref.name); setName(ref.name); setValue(""); setResult(""); setError(""); }}>轮换</Button>
          <Button variant="destructive" size="sm" disabled={busy || !dev.canApprove} onClick={() => setRevokeTarget(ref.name)}>撤销</Button>
        </div>
      </div>)}
      {revokeTarget && <div className="g-inline-confirm" role="alertdialog" aria-label={`撤销 ${revokeTarget}`}>
        <p>撤销后，正在使用旧版本的进程会停止。确认撤销 {revokeTarget}？</p>
        <div className="g-inline-actions">
          <Button variant="ghost" size="sm" onClick={() => setRevokeTarget(null)}>取消</Button>
          <Button variant="destructive" size="sm" disabled={busy} onClick={() => { const target = revokeTarget; setRevokeTarget(null); void submit("revoke", target); }}>确认撤销</Button>
        </div>
      </div>}
      <p className="g-dialog-description">轮换会以新值覆盖同一引用；撤销后，使用旧版本的进程会停止。面板不读取凭据，无法列出项目里已有的引用。</p>
    </>}
  </>;
}

function ContextDialogBody({ chat }: { chat: SessionState }) {
  const policy = chat.session?.context_policy ?? null;
  return <>
    <DialogHeader><DialogTitle>Context</DialogTitle><DialogDescription>当前会话的策略容量</DialogDescription></DialogHeader>
    {policy ? <dl className="g-description-list">
      <dt>总容量</dt><dd>{fmtBytes(policy.max_bytes)}</dd>
      <dt>消息上限</dt><dd>{policy.max_messages === null ? "不设消息数上限" : `${fmtCount(policy.max_messages)} 条`}</dd>
      <dt>输出预留</dt><dd>{fmtCount(policy.reserved_output_tokens)} tokens</dd>
      <dt>计数口径</dt><dd className="g-mono">{policy.counter_version}</dd>
    </dl> : <p className="g-empty">会话载入后显示上下文容量。</p>}
    <p className="g-dialog-description">实时用量未提供。</p>
    <div className="dev-dialog-footer"><DialogClose render={<Button variant="ghost" size="sm" />}>关闭</DialogClose></div>
  </>;
}

function ToolDialogBody({ tool }: { tool: ToolEntry }) {
  return <>
    <DialogHeader><DialogTitle>{displayToolName(tool.name)}</DialogTitle><DialogDescription>会话工具目录</DialogDescription></DialogHeader>
    <dl className="g-source-details">
      <dt>来源</dt><dd>{tool.kind ? `${tool.kind === "mcp" ? "MCP" : "HTTP"} · ${tool.server} / ${tool.tool}` : "内置工具"}</dd>
      {tool.version && <><dt>版本</dt><dd>{tool.version}</dd></>}
      <dt>风险</dt><dd>{riskLabel(tool.risk)}</dd>
      {tool.fingerprint && <><dt>指纹</dt><dd className="g-mono">{tool.fingerprint}</dd></>}
    </dl>
    <div className="dev-dialog-footer"><DialogClose render={<Button variant="ghost" size="sm" />}>关闭</DialogClose></div>
  </>;
}

function ConnectionDialogBody({ connection }: { connection: McpConnection }) {
  return <>
    <DialogHeader><DialogTitle>{connection.name}</DialogTitle><DialogDescription>账号 MCP 连接</DialogDescription></DialogHeader>
    <dl className="g-source-details">
      <dt>地址</dt><dd className="g-mono">{connection.url}</dd>
      <dt>允许主机</dt><dd>{connection.allowed_hosts.map(shortHost).join("、")}</dd>
      <dt>已选工具</dt><dd>{connection.selected_tools.join("、")}</dd>
      {connection.readonly_tools.length > 0 && <><dt>只读工具</dt><dd>{connection.readonly_tools.join("、")}</dd></>}
      <dt>超时</dt><dd>{connection.timeout_seconds}s</dd>
      <dt>配置版本</dt><dd>{connection.revision}</dd>
      <dt>凭据</dt><dd>{connection.credential_configured ? `已配置（版本 ${connection.credential_revision}），内容不可读取` : "未配置"}</dd>
    </dl>
    <div className="dev-dialog-footer"><DialogClose render={<Button variant="ghost" size="sm" />}>关闭</DialogClose></div>
  </>;
}
