import { useCallback, useEffect, useMemo, useRef, useState, type KeyboardEvent, type RefObject } from "react";
import {
  Cpu, Globe, Copy, FileCode, GitFork, ArrowUpRight, Maximize2, Minimize2, ChevronRight, ChevronUp, Columns2, Download, FileText, Folder, FolderOpen,
  Image, KeyRound, Plus, Shield, Terminal, X,
} from "lucide-react";
import { parseTurn, sessionPath, type Turn, type SessionContextPolicy } from "../lib/session-contracts.ts";
import {
  devReason, environmentLabels, environmentRowLabel, fmtBytes, fmtCount, parseProject,
  projectStatusLabels, shortHost, type DirectoryPage, type EnvironmentState, type McpConnection,
  type SandboxProject, type ToolEntry,
} from "../lib/development-contracts.ts";
import { fetchDirectory, fetchFileBytes, fetchProjectsPage, useDevelopment, type DevelopmentState } from "../hooks/use-development.ts";
import type { SessionState } from "../hooks/use-session.ts";
import { useSessionCompaction } from "../hooks/use-session-compaction";
import { request } from "../api.ts";
import { parsePreviewBootstrap, PREVIEW_FRAME_SANDBOX, previewTargetName, registeredPreviews } from "../lib/development-previews";
import { displayToolName } from "../lib/tool-names";
import { modeLabels } from "../lib/execution-policy";
import { contextCount } from "./ContextSummary";
import { Button } from "./ui/button.tsx";
import { Input } from "./ui/input.tsx";
import { Dialog, DialogClose, DialogContent, DialogDescription, DialogHeader, DialogTitle } from "./ui/dialog.tsx";
import { PopoverMenu } from "./popover-menu.tsx";
import "./development-panel.css";
import { MessageMarkdown } from "./messages/MessageMarkdown";
import { useCopyFeedback } from "../hooks/use-copy-feedback";
import { CopyFeedback } from "./messages/CopyFeedback";
import { ContextSummary } from "./ContextSummary";
import type { SessionContextState } from "../hooks/use-session-context";
import { SubagentPanel, type SubagentsState } from "./SubagentPanel";
import type { DevelopmentActionDetail } from "../lib/workbench-events";
import { projectTurn } from "../lib/session-presentation";

// 开发信息栏与工作区：结构对应已验收的 H 原型（Environment/Context/Outputs + 右侧标签工作区），
// 数据全部来自真实接口；后端尚未提供的能力一律"敬请期待"，不使用模拟数据。

function workWidthLimit(element: HTMLElement | null): number {
  const width = window.innerWidth;
  const root = element?.closest(".chat-workbench");
  const info = element?.closest(".dev-panel-root")?.querySelector(".g-info");
  const available = root?.getBoundingClientRect().width ?? width;
  const maximum = width > 1520 ? available - (info?.getBoundingClientRect().width ?? 0) - 480 : width - 24;
  return Math.max(340, Math.min(860, width * .62, maximum));
}

type WorkTab =
  | { id: string; type: "browser"; title: string }
  | { id: string; type: "document"; title: string; path: string | null; rawMarkdown?: boolean }
  | { id: string; type: "aux"; title: string; sessionId: string }
  | { id: string; type: "subagents"; title: string };

type PanelDialog =
  | { kind: "coming"; label: string }
  | { kind: "environment" }
  | { kind: "project" }
  | { kind: "secrets" }
  | { kind: "context" }
  | { kind: "connections" }
  | { kind: "tool"; tool: ToolEntry }
  | { kind: "connection"; connection: McpConnection };

function useMediaQuery(query: string, fallbackWidth: number) {
  const fallback = () => query.includes("min-width") ? window.innerWidth > fallbackWidth : window.innerWidth <= fallbackWidth;
  const [matches, setMatches] = useState(() => typeof window !== "undefined" ? window.matchMedia?.(query).matches ?? fallback() : false);
  useEffect(() => {
    const media = window.matchMedia?.(query);
    const update = () => setMatches(window.matchMedia?.(query).matches ?? fallback());
    update(); window.addEventListener("resize", update); media?.addEventListener?.("change", update);
    return () => { window.removeEventListener("resize", update); media?.removeEventListener?.("change", update); };
  }, [query, fallbackWidth]);
  return matches;
}

const riskLabel = (risk: string | null) => (risk === "read" ? "只读" : risk === "write" ? "写操作" : risk ? risk : "未标注");
const contextWindowLabel = (policy: SessionContextPolicy) => policy.counter_version !== "approx-utf8/1" && policy.max_tokens !== undefined
  ? `${contextCount(policy.max_tokens)} tokens` : policy.max_bytes === null ? "未提供" : fmtBytes(policy.max_bytes);
const IMAGE_EXTENSIONS = [".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".bmp", ".ico"];
const isImagePath = (path: string) => IMAGE_EXTENSIONS.some(ext => path.toLowerCase().endsWith(ext));
const lastSegment = (path: string) => path.split("/").at(-1) ?? path;

const EMPTY_TABS: WorkTab[] = [];

export function DevelopmentPanel({ chat, apiKey, active, fence, development, dark, context, subagents, onContextTrace, onMaximizedChange }: {
  chat: SessionState; apiKey: string; active: boolean; fence: { current: number };
  subagents?: SubagentsState;
  development?: DevelopmentState; dark?: boolean; context?: SessionContextState; onContextTrace?: () => void; onMaximizedChange?: (maximized: boolean) => void;
}) {
  const fallbackDev = useDevelopment(chat, apiKey, active && !development, fence);
  const dev = development ?? fallbackDev;
  const compaction = useSessionCompaction(chat, apiKey, active, fence);
  const narrow = useMediaQuery("(max-width: 1200px)", 1200);
  const wideWorkInline = useMediaQuery("(min-width: 1521px)", 1520);
  const [desktopInfoOpen, setDesktopInfoOpen] = useState(true);
  const [drawerInfoOpen, setDrawerInfoOpen] = useState(false);
  const infoOpen = narrow ? drawerInfoOpen : desktopInfoOpen;
  const setInfoOpen = useCallback((value: boolean | ((previous: boolean) => boolean)) => {
    if (narrow) setDrawerInfoOpen(value); else setDesktopInfoOpen(value);
  }, [narrow]);
  const [workOpen, setWorkOpen] = useState(false);
  const [split, setSplit] = useState(false);
  const [maximized, setMaximized] = useState(false), [workWidth, setWorkWidth] = useState(460), [showAllFiles, setShowAllFiles] = useState(false);
  const [activeSlot, setActiveSlot] = useState(0);
  const [slots, setSlots] = useState<WorkTab[][]>([EMPTY_TABS, EMPTY_TABS]);
  const [activeTabs, setActiveTabs] = useState<(string | null)[]>([null, null]);
  const subagentWork = slots.some((tabs, slot) => (split || slot === 0) && tabs.some(tab => tab.id === activeTabs[slot] && tab.type === "subagents"));
  const workInline = wideWorkInline || (subagentWork && !narrow);
  const [workLimit, setWorkLimit] = useState(() => workWidthLimit(null));
  const workElement = useRef<HTMLElement>(null);
  const resizeStart = useRef<{ x: number; width: number; pointer: number } | null>(null);
  useEffect(() => { workElement.current?.style.setProperty("--work-width", `${workWidth}px`); }, [workWidth, workOpen, maximized]);
  useEffect(() => {
    const move = (event: PointerEvent) => {
      const start = resizeStart.current;
      if (!start || event.pointerId !== start.pointer) return;
      setWorkWidth(Math.max(340, Math.min(workWidthLimit(workElement.current), start.width + start.x - event.clientX)));
    };
    const stop = () => { resizeStart.current = null; };
    window.addEventListener("pointermove", move); window.addEventListener("pointerup", stop); window.addEventListener("pointercancel", stop); window.addEventListener("blur", stop);
    return () => { window.removeEventListener("pointermove", move); window.removeEventListener("pointerup", stop); window.removeEventListener("pointercancel", stop); window.removeEventListener("blur", stop); };
  }, []);
  useEffect(() => {
    const fit = () => { const limit = workWidthLimit(workElement.current); setWorkLimit(limit); setWorkWidth(value => Math.min(value, limit)); };
    fit(); window.addEventListener("resize", fit);
    const root = workElement.current?.closest(".chat-workbench");
    const observer = typeof ResizeObserver === "undefined" ? null : new ResizeObserver(fit);
    if (root) observer?.observe(root);
    return () => { window.removeEventListener("resize", fit); observer?.disconnect(); };
  }, [infoOpen, workInline, workOpen]);
  const [dialog, setDialog] = useState<PanelDialog | null>(null);
  const [addMenuSlot, setAddMenuSlot] = useState<{ slot: number; anchor: HTMLButtonElement } | null>(null);
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
    if (tab.type === "subagents" && !slots.flat().some(item => item.type === "subagents")) setWorkWidth(440);
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
  function toggleFiles() {
    const selected = slots[activeSlot]?.find(tab => tab.id === activeTabs[activeSlot]);
    if (workOpen && selected?.type === "document" && !maximized) setWorkOpen(false);
    else addTab(filesTab(), 0);
  }
  function toggleBrowser() {
    const selected = slots[activeSlot]?.find(tab => tab.id === activeTabs[activeSlot]);
    if (workOpen && selected?.type === "browser" && !maximized) setWorkOpen(false);
    else addTab({ id: "browser", type: "browser", title: "浏览器预览" }, 0);
  }
  // Esc 关闭覆盖态；对话框自身处理 Esc，这里跳过。
  useEffect(() => {
    function onKeyDown(event: globalThis.KeyboardEvent) {
      if (event.key !== "Escape" || document.querySelector('[role="dialog"]')) return;
      if (infoOpen && narrow && !(workOpen && maximized)) { event.stopPropagation(); setInfoOpen(false); }
      else if (workOpen && !workInline) { event.stopPropagation(); setWorkOpen(false); }
    }
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [workOpen, workInline, infoOpen, narrow, maximized, setInfoOpen]);
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

  useEffect(() => {
    const onAction = (event: Event) => {
      const source = (event as CustomEvent<DevelopmentActionDetail>).detail;
      if (!active || !chat.ready || !source || source.principal !== chat.actor?.principal_id || source.generation !== chat.actor?.generation || source.workspace !== chat.workspace || source.sessionId !== chat.id) return;
      if (source.action === "files") toggleFiles();
      else if (source.action === "browser") toggleBrowser();
      else if (source.action === "info") { if (workOpen && maximized) { setMaximized(false); setInfoOpen(true); } else setInfoOpen(value => !value); }
      else if (source.action === "context") { setMaximized(false); setInfoOpen(true); }
      else if (source.action === "project") setDialog({ kind: "project" });
      else if (source.action === "environment") setDialog({ kind: "environment" });
      else if (source.action === "subagents" && subagents?.available) {
        if (source.agentId) subagents.select(source.agentId);
        addTab({ id: "subagents", type: "subagents", title: "后台任务" }, 0);
      }
    };
    window.addEventListener("krim:development-action", onAction);
    return () => window.removeEventListener("krim:development-action", onAction);
  }, [active, chat.ready, chat.actor?.principal_id, chat.actor?.generation, chat.workspace, chat.id, activeSlot, workOpen, maximized, slots, activeTabs, setInfoOpen, subagents]);
  useEffect(() => {
    onMaximizedChange?.(workOpen && maximized);
    return () => onMaximizedChange?.(false);
  }, [workOpen, maximized, onMaximizedChange]);
  useEffect(() => {
    const emit = () => {
      if (!active || !chat.ready || !chat.actor) return;
      const selected = slots[activeSlot]?.find(tab => tab.id === activeTabs[activeSlot]);
      window.dispatchEvent(new CustomEvent("krim:development-state", { detail: {
        principal: chat.actor.principal_id, generation: chat.actor.generation, workspace: chat.workspace, sessionId: chat.id,
        projectTitle: dev.binding && dev.project && dev.binding.session_id === chat.id && dev.binding.project_id === dev.project.project_id ? dev.project.title : null, infoOpen: infoOpen && !(workOpen && maximized),
        filesOpen: workOpen && selected?.type === "document", browserOpen: workOpen && selected?.type === "browser",
      } }));
    };
    const requested = (event: Event) => {
      const value = (event as CustomEvent).detail;
      if (value?.principal === chat.actor?.principal_id && value?.generation === chat.actor?.generation && value?.workspace === chat.workspace && value?.sessionId === chat.id) emit();
    };
    window.addEventListener("krim:development-state-request", requested); emit();
    return () => window.removeEventListener("krim:development-state-request", requested);
  }, [active, chat.ready, chat.actor?.principal_id, chat.actor?.generation, chat.workspace, chat.id, dev.project?.title, infoOpen, maximized, workOpen, slots, activeSlot, activeTabs]);
  const outputFiles = useMemo(() => {
    const outputPaths = new Set<string>();
    for (const turn of chat.history ?? []) {
      const events = chat.turnEvents?.[turn.id] ?? (turn.id === chat.currentTurn?.id ? chat.events : []) ?? [];
      for (const item of projectTurn(turn, events, [], { events: chat.details[turn.id]?.events, facts: chat.turnFacts?.[turn.id] ?? chat.details[turn.id]?.facts })) {
        if (item.type !== "tool" || !["read", "edit", "write", "read_file", "edit_file", "write_file"].includes(displayToolName(item.name)) || !item.arguments || typeof item.arguments !== "object") continue;
        const args = item.arguments as Record<string, unknown>;
        const path = args.path ?? args.file_path;
        if (typeof path === "string" && path.length > 0 && path.length <= 4096 && !path.includes("\0") && !path.split(/[\\/]/).includes("..")) outputPaths.add(path);
      }
    }
    return [...outputPaths];
  }, [chat.history, chat.currentTurn?.id, chat.events, chat.turnEvents, chat.details, chat.turnFacts]);
  const environmentState = dev.environment?.state ?? null;
  const policy = chat.session?.context_policy ?? null;
  const drawerInfo = infoOpen && narrow;

  return <div className={`dev-panel-root${workOpen && subagentWork ? " has-subagent-work" : ""}${workOpen && maximized ? " is-work-maximized" : ""}`} data-info={infoOpen && !(workOpen && maximized) ? (drawerInfo ? "drawer" : "true") : "false"} data-work={workOpen ? (workInline || maximized ? "inline" : "overlay") : "false"}>
    {infoOpen && !(workOpen && maximized) && <section className="g-info" aria-label="开发信息栏">
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
        </div>
        <div className="g-environment-body">
          <EnvRow icon={Terminal} label="Linux 容器" value={projectId ? environmentRowLabel(environmentState) : "未准备"}
            state={projectId ? environmentState : null} onClick={() => setDialog({ kind: "environment" })} />
          <EnvRow icon={Folder} label="项目" value={dev.project?.title ?? "选择项目"} title={dev.project?.title} onClick={() => setDialog({ kind: "project" })} />
          <EnvRow icon={Shield} label="执行策略" value={chat.execution?.policy ? `${chat.execution.policy.project_access === "read" ? "只读 · " : ""}${modeLabels[chat.execution.policy.mode]}` : chat.execution?.loading ? "读取中" : "未提供"} onClick={() => void chat.execution?.openProject()} />
          <EnvRow icon={KeyRound} label="Secrets 密钥" value={!projectId ? "未绑定" : Array.isArray(dev.secretRefs) ? `${dev.secretRefs.length} 项已配置` : "读取中"} onClick={() => setDialog(projectId ? { kind: "secrets" } : { kind: "project" })} />
          <EnvRow icon={Cpu} label="MCP 连接" value={dev.connections === null ? "未提供" : `${dev.connections.length} 个连接`} onClick={() => setDialog({ kind: "connections" })} />
        </div>
      </section>
      <section className="g-info-section context">
        <div className="g-info-head">
          <h2>Context</h2>
          {context ? <button type="button" className="g-info-link" onClick={onContextTrace} disabled={!context.turnId || !onContextTrace} title="在 Trace 页查看来源与请求差分">观测详情<ArrowUpRight aria-hidden="true"/></button> : <Button variant="ghost" size="icon-sm" className="icon-button small" aria-label="上下文用量说明" onClick={() => setDialog({ kind: "context" })}><EllipsisIcon /></Button>}
        </div>
        <div className="g-context-body">
          {context ? <ContextSummary context={context} dark={dark} compactView/> : <>
          <div className="g-context-overview">
            <span>{policy ? "会话上下文策略" : "会话载入后显示上下文容量"}</span>
            {policy && <span>{contextWindowLabel(policy)} · {policy.max_messages === null ? "不设消息数上限" : `${fmtCount(policy.max_messages)} 条消息`}</span>}
          </div>
          {policy && <table className="g-context-table" aria-label="上下文策略容量">
            <tbody>
              <tr><td>总容量</td><td>{contextWindowLabel(policy)}</td><td>—</td></tr>
              <tr><td>消息上限</td><td>{policy.max_messages === null ? "不设消息数上限" : `${fmtCount(policy.max_messages)} 条`}</td><td>—</td></tr>
              <tr><td>输出预留</td><td>{contextCount(policy.reserved_output_tokens)} tokens</td><td>—</td></tr>
            </tbody>
          </table>}
          <p className="dev-context-note">上下文用量未提供。</p></>}
          <div className="g-context-end">
            <span>{context?.used !== null && context?.used !== undefined && context.limit !== null ? `距压缩预估余 ${contextCount(Math.max(0, context.limit - context.used))}` : "整理在运行时内自动进行"}</span>
            <Button variant="outline" size="sm" disabled={compaction.disabled} aria-label="Compact session" onClick={() => void compaction.compact()}>{compaction.busy ? "提交中…" : "Compact"}</Button>
          </div>
          <details className="g-detail">
            <summary><ChevronRight aria-hidden="true" />MCP 与内置工具<span className="g-detail-count">{dev.tools.length}</span></summary>
            <div className="g-detail-list" tabIndex={0} aria-label="会话工具目录">
              {dev.tools.length === 0 && <p className="dev-detail-empty">当前目标没有可用工具</p>}
              {dev.tools.map((tool, index) => <button type="button" className="g-tool-row" key={`${tool.name}:${index}`} title={displayToolName(tool.name)}
                onClick={() => setDialog({ kind: "tool", tool })}>
                <span>{displayToolName(tool.name)}</span><span>{riskLabel(tool.risk)}</span>
              </button>)}
            </div>
          </details>
          <details className="g-detail">
            <summary><ChevronRight aria-hidden="true" />Memory files<span className="g-detail-count">—</span></summary>
            <div className="g-detail-list g-memory-list" tabIndex={0} aria-label="记忆来源列表">
              <p className="dev-detail-empty">记忆来源未提供</p>
            </div>
          </details>
        </div>
      </section>
      <section className="g-info-section outputs">
        <div className="g-info-head"><h2>Outputs</h2>{outputFiles.length > 3 && <button type="button" className="g-info-link" onClick={() => setShowAllFiles(value => !value)}>{showAllFiles ? "收起" : `展开全部 (${outputFiles.length})`}</button>}</div>
        {outputFiles.length > 0 && <div className="g-output-group-label">本会话涉及文件</div>}
        {(showAllFiles ? outputFiles : outputFiles.slice(0, 3)).map(path => <button type="button" className="g-output-row" key={path} title={path} onClick={() => addTab({ id: "file:" + path, type: "document", title: lastSegment(path), path }, 0)}>{/\.(md|markdown)$/i.test(path) ? <FileText aria-hidden="true"/> : <FileCode aria-hidden="true"/>}<span>{path.split("/").slice(-2).join("/")}</span></button>)}
        <div className="g-output-group-label">工作区面板</div>
        <button type="button" className="g-output-row" onClick={() => addTab(filesTab(), 0)}><FolderOpen aria-hidden="true"/><span>浏览全部项目文件</span></button>
        <button type="button" className="g-output-row" onClick={() => addTab({ id: "browser", type: "browser", title: "浏览器预览" }, 0)}><Globe aria-hidden="true"/><span>打开浏览器实时预览</span></button>
        <button type="button" className="g-output-row" onClick={() => comingSoon("项目打包下载")}><Download aria-hidden="true"/><span>下载项目文件归档 (.tar.gz)</span></button>
      </section>
    </section>}
    {workOpen && <section ref={workElement} className={`g-work${workInline || maximized ? "" : " is-overlay"}${maximized ? " is-maximized" : ""}`} aria-label="开发工作区">
      {!maximized && <div className="g-work-resizer" role="separator" aria-label="调整工作区宽度" aria-orientation="vertical" aria-valuenow={Math.round(workWidth)} aria-valuemin={340} aria-valuemax={Math.round(workLimit)} tabIndex={0} onPointerDown={event => { if (event.button !== 0) return; event.preventDefault(); try { event.currentTarget.setPointerCapture?.(event.pointerId); } catch { /* Synthetic pointers have no native capture. */ } resizeStart.current = { x: event.clientX, width: workWidth, pointer: event.pointerId }; }} onKeyDown={event => { if (event.key === "ArrowLeft" || event.key === "ArrowRight") { event.preventDefault(); setWorkWidth(value => Math.max(340, Math.min(workWidthLimit(workElement.current), value + (event.key === "ArrowLeft" ? 24 : -24)))); } }}/>}
      <div className="g-work-topbar">
        <strong>工作区</strong>
        <Button variant="ghost" size="icon-sm" className="icon-button small" aria-label={split ? "合并分区" : "上下分区"} aria-pressed={split} title={split ? "合并单栏" : "上下双栏分屏"} onClick={toggleSplit}><Columns2 aria-hidden="true"/></Button>
        <Button variant="ghost" size="icon-sm" className="icon-button small" aria-label={maximized ? "还原工作区" : "最大化工作区"} onClick={() => setMaximized(value => !value)}>{maximized ? <Minimize2 aria-hidden="true"/> : <Maximize2 aria-hidden="true"/>}</Button>
        <Button variant="ghost" size="icon-sm" className="icon-button small" aria-label="关闭工作区" onClick={() => setWorkOpen(false)}><X aria-hidden="true" /></Button>
      </div>
      <div className={`g-work-stack${split ? " split" : ""}`}>
        {(split ? [0, 1] : [0]).map(slot => <WorkSlot key={slot} slot={slot} label={split ? (slot ? "下方工作区" : "上方工作区") : "工作区内容"}
          tabs={slots[slot] ?? EMPTY_TABS} activeTab={activeTabs[slot] ?? null} split={split}
          onSelect={id => { setActiveTabs(previous => previous.map((value, index) => index === slot ? id : value)); setActiveSlot(slot); }}
          onClose={id => closeTab(id, slot)}
          onAdd={anchor => setAddMenuSlot({ slot, anchor })}
          onCloseSplit={toggleSplit}
          onComing={comingSoon}
          chat={chat} apiKey={apiKey} projectId={projectId} environmentState={environmentState} subagents={subagents}
          onShowApproval={() => { setWorkOpen(false); requestAnimationFrame(() => document.querySelector(".sa-pending-approval")?.scrollIntoView({ block: "center", behavior: "smooth" })); }}
          onOpenEnvironment={() => setDialog({ kind: "environment" })}
          onBrowseFiles={() => addTab(filesTab(), slot)}
          onSetDocumentMode={(id, rawMarkdown) => setSlots(previous => previous.map((items, index) => index === slot ? items.map(tab => tab.id === id && tab.type === "document" ? { ...tab, rawMarkdown } : tab) : items))}
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
      projectId={projectId} project={dev.project} environmentState={environmentState} onInspectConnection={connection => setDialog({ kind: "connection", connection })} />
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
    <span className="g-env-value">{value}</span>
    {state && <span className={`g-state-mini${state === "ready" ? " ready" : state === "failed" ? " failed" : ""}`} role="img"
      aria-label={environmentLabels[state]}></span>}
  </button>;
}

function WorkSlot({ slot, label, tabs, activeTab, split, onSelect, onClose, onAdd, onCloseSplit, onComing, chat, apiKey, projectId, environmentState, subagents, onShowApproval, onOpenEnvironment, onOpenFile, onBrowseFiles, onSetDocumentMode }: {
  slot: number; label: string; tabs: WorkTab[]; activeTab: string | null; split: boolean;
  onSelect: (id: string) => void; onClose: (id: string) => void; onAdd: (anchor: HTMLButtonElement) => void;
  onCloseSplit: () => void; onComing: (label: string) => void;
  chat: SessionState; apiKey: string; projectId: string | null; environmentState: EnvironmentState | null;
  subagents?: SubagentsState; onShowApproval: () => void;
  onOpenEnvironment: () => void;
  onOpenFile: (path: string, title: string) => void;
  onBrowseFiles: () => void; onSetDocumentMode: (id: string, rawMarkdown: boolean) => void;
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
        {tabs.map(tab => <div className={`g-work-tab${current?.id === tab.id ? " is-active" : ""}`} role="presentation" key={tab.id}>
          <button type="button" role="tab" id={`dev-tab-${slot}-${tab.id.replace(/[^a-zA-Z0-9-]/g, "-")}`}
            aria-selected={current?.id === tab.id} aria-controls={`dev-work-content-${slot}`}
            tabIndex={current?.id === tab.id ? 0 : -1} data-tab={tab.id} title={tab.title}
            onClick={() => onSelect(tab.id)}>{tab.type === "browser" ? <Globe aria-hidden="true"/> : (tab.type === "aux" || tab.type === "subagents") ? <GitFork aria-hidden="true"/> : tab.path ? <FileCode aria-hidden="true"/> : <Folder aria-hidden="true"/>}<span>{tab.title}</span></button>
          <button type="button" aria-label={`关闭 ${tab.title}`} onClick={() => onClose(tab.id)}><X aria-hidden="true" /></button>
        </div>)}
      </div>
      <Button variant="ghost" size="icon-sm" className="icon-button small" aria-label={`在${slot ? "下方" : "上方"}分区打开内容`} aria-haspopup="menu"
        ref={addButton as RefObject<HTMLButtonElement>} onClick={() => { if (addButton.current) onAdd(addButton.current); }}><Plus aria-hidden="true" /></Button>
      {split && slot === 1 && <Button variant="ghost" size="icon-sm" className="icon-button small" aria-label="关闭下方分区" onClick={onCloseSplit}><X aria-hidden="true" /></Button>}
    </div>
    <div className="g-work-content" id={`dev-work-content-${slot}`} role="tabpanel" aria-label={current?.title ?? "空工作区"} tabIndex={0}>
      {!current && <div className="g-work-empty"><strong>打开内容</strong><span>使用上方 + 选择浏览器预览、项目文件或辅助会话。</span></div>}
      {current?.type === "browser" && <BrowserView chat={chat} apiKey={apiKey} projectId={projectId} environmentState={environmentState} onOpenEnvironment={onOpenEnvironment} />}
      {current?.type === "document" && (current.path === null
        ? <FileBrowser apiKey={apiKey} projectId={projectId} onOpenFile={onOpenFile} />
        : <FileView apiKey={apiKey} projectId={projectId} path={current.path} rawMarkdown={current.rawMarkdown ?? false} onToggleMarkdown={() => onSetDocumentMode(current.id, !current.rawMarkdown)} onBrowseFiles={onBrowseFiles} />)}
      {current?.type === "subagents" && subagents && <SubagentPanel state={subagents} chat={chat} onOpenFile={onOpenFile} onShowApproval={onShowApproval}/>}
      {current?.type === "aux" && <AuxView chat={chat} apiKey={apiKey} sessionId={current.sessionId} onComing={onComing} />}
    </div>
  </section>;
}

function BrowserView({ chat, apiKey, projectId, environmentState, onOpenEnvironment }: {
  chat: SessionState; apiKey: string; projectId: string | null; environmentState: EnvironmentState | null; onOpenEnvironment: () => void;
}) {
  const previews = registeredPreviews(chat, projectId), [busy, setBusy] = useState(false), [error, setError] = useState("");
  const [frame, setFrame] = useState<{ name: string; action: string; ticket: string } | null>(null);
  const [loadingFrame, setLoadingFrame] = useState<string | null>(null);
  const alive = useRef(true);
  const submitted = useRef<string | null>(null);
  useEffect(() => { alive.current = true; return () => { alive.current = false; }; }, []);
  // 已提交过的帧名不重复提交；StrictMode 双跑 effect 也不会重复消耗一次性票据。
  // 票据只在命名帧挂载后随 POST 正文提交，目标名每次打开重新生成，旧票据进不了新帧。
  useEffect(() => {
    if (!frame || !frame.ticket || submitted.current === frame.name) return;
    submitted.current = frame.name;
    const form = document.createElement("form"), ticket = document.createElement("input");
    form.method = "POST"; form.action = frame.action; form.target = frame.name; form.hidden = true;
    ticket.type = "hidden"; ticket.name = "ticket"; ticket.value = frame.ticket; form.append(ticket);
    document.body.append(form);
    try {
      form.submit();
      // 提交完成即把票据从 state 擦掉，仅保留帧名与地址用于继续使用该帧。
      setFrame(current => current && current.name === frame.name ? { name: current.name, action: current.action, ticket: "" } : current);
    } catch { setFrame(null); setLoadingFrame(null); setError("预览打开未完成，请检查环境、授权或预览状态"); }
    finally { ticket.value = ""; form.remove(); }
  }, [frame]);
  useEffect(() => {
    if (!loadingFrame) return;
    const timer = window.setTimeout(() => {
      setFrame(current => current?.name === loadingFrame ? null : current);
      setLoadingFrame(null);
      setError("预览加载超时，请重新打开预览；仍无法加载时请检查项目服务和网络连接。");
    }, 30_000);
    return () => window.clearTimeout(timer);
  }, [loadingFrame]);
  function frameLoaded(element: HTMLIFrameElement) {
    // 初始空文档也会发 load；只有提交后的文档导航才能结束等待。
    if (!frame || submitted.current !== frame.name) return;
    try {
      const location = element.contentWindow?.location.href;
      if (!location || location === "about:blank") return;
    } catch {
      // 跨 origin 导航后无法读取 location，这是预览隔离的正常结果。
    }
    setLoadingFrame(current => current === frame.name ? null : current);
  }
  async function openPreview(id: string, mode: "auto" | "window") {
    if (busy || !chat.ready) return;
    // 在任何 await 之前同步预开候选窗口，保住 user activation，await 后的 fallback 不被弹窗拦截；
    // 服务端授权嵌入时立即关闭候选窗口，改由命名 iframe 接收 POST。
    const name = previewTargetName(), windowName = name + "-window", tab = window.open("about:blank", windowName);
    if (!tab && mode === "window") { setError("请允许打开项目预览窗口"); return; }
    if (tab) tab.opener = null;
    setBusy(true); setError("");
    try {
      const bootstrap = parsePreviewBootstrap(await request(`/api/development/previews/${encodeURIComponent(id)}/open`, apiKey, {}));
      if (!alive.current) { tab?.close(); return; }
      if (mode === "auto" && bootstrap.allow_embedding) { tab?.close(); setLoadingFrame(name); setFrame({ name, action: bootstrap.action, ticket: bootstrap.ticket }); }
      else if (tab) submitBootstrap(bootstrap, windowName);
      else setError("请允许打开项目预览窗口");
    } catch { tab?.close(); if (alive.current) setError("预览打开未完成，请检查环境、授权或预览状态"); }
    finally { if (alive.current) setBusy(false); }
  }
  // A one-use ticket travels only in the POST body to the authenticated API's
  // validated HTTPS bootstrap action, never in a URL or persisted browser state.
  function submitBootstrap(bootstrap: { action: string; ticket: string }, target: string) {
    const form = document.createElement("form"), ticket = document.createElement("input");
    form.method = "POST"; form.action = bootstrap.action; form.target = target; form.hidden = true;
    ticket.type = "hidden"; ticket.name = "ticket"; ticket.value = bootstrap.ticket; form.append(ticket);
    document.body.append(form); try { form.submit(); } finally { ticket.value = ""; form.remove(); }
  }
  const reason = !projectId ? "绑定项目后可打开开发服务的页面预览。"
    : environmentState !== "ready" ? "项目环境尚未就绪。"
      : "页面预览凭票据打开；运行记录中还没有已注册的预览。";
  return <div className={frame ? "g-browser-preview" : "g-work-empty"}>
    <strong>浏览器预览</strong>
    {!previews.length ? <span>{reason}</span> : previews.map(preview => <div className="g-row" key={preview.preview_id}><span>端口 {preview.port} · {preview.state === "ready" ? "已就绪" : preview.state === "pending" ? "准备中" : "已关闭或状态未知"}</span><span className="g-row-actions">
      <Button size="sm" variant="outline" disabled={busy || !["ready", "pending"].includes(preview.state)} onClick={() => void openPreview(preview.preview_id, "auto")}>打开预览</Button>
      {frame && <Button size="sm" variant="ghost" disabled={busy || !["ready", "pending"].includes(preview.state)} onClick={() => void openPreview(preview.preview_id, "window")}>新窗口</Button>}
    </span></div>)}
    {error && <p role="status">{error}</p>}
    {loadingFrame && <p role="status">正在加载预览…</p>}
    <div className="g-inline-actions"><Button variant="outline" size="sm" onClick={onOpenEnvironment}>项目环境</Button></div>
    {frame && <iframe key={frame.name} name={frame.name} title="浏览器预览" sandbox={PREVIEW_FRAME_SANDBOX} onLoad={event => frameLoaded(event.currentTarget)} />}
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
          {isImagePath(entry.name) ? <Image aria-hidden="true" /> : /\.(md|markdown)$/i.test(entry.name) ? <FileText aria-hidden="true"/> : <FileCode aria-hidden="true"/>}
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

function FileView({ apiKey, projectId, path, rawMarkdown, onToggleMarkdown, onBrowseFiles }: {
  apiKey: string; projectId: string | null; path: string; rawMarkdown: boolean;
  onToggleMarkdown: () => void; onBrowseFiles: () => void;
}) {
  const [state, setState] = useState<{ url?: string; text?: string; truncated?: boolean; error?: string; loading: boolean }>({ loading: true });
  const objectUrl = useRef<string | null>(null), feedback = useCopyFeedback();
  const image = isImagePath(path);
  useEffect(() => {
    if (!projectId) return;
    const controller = new AbortController(); setState({ loading: true });
    void fetchFileBytes(apiKey, projectId, path, controller.signal).then(value => {
      if (controller.signal.aborted) return;
      const truncated = !value.eof;
      if (image) {
        const blob = new Blob([value.bytes], { type: blobType(path) });
        if (objectUrl.current) URL.revokeObjectURL(objectUrl.current);
        objectUrl.current = URL.createObjectURL(blob); setState({ url: objectUrl.current, truncated, loading: false });
      } else setState({ text: new TextDecoder("utf-8", { fatal: false }).decode(value.bytes), truncated, loading: false });
    }).catch(value => { if (!controller.signal.aborted) setState({ error: devReason(value), loading: false }); });
    return () => controller.abort();
  }, [apiKey, projectId, path, image]);
  useEffect(() => () => { if (objectUrl.current) URL.revokeObjectURL(objectUrl.current); }, []);
  if (!projectId) return <div className="g-work-empty"><strong>未绑定项目</strong><span>绑定项目后可以查看项目文件。</span></div>;
  if (state.loading) return <p className="dev-detail-empty" role="status">读取中…</p>;
  if (state.error) return <div className="g-work-empty"><strong>读取未完成</strong><span>{state.error}</span></div>;
  const name = lastSegment(path);
  if (image) return <figure className="g-picture"><img src={state.url} alt={name}/><figcaption>{name} · {state.truncated ? "仅预览前段" : "完整内容"}</figcaption></figure>;
  const markdown = /\.(md|markdown)$/i.test(path), text = state.text ?? "";
  const language = markdown ? "markdown" : ({ ts: "typescript", tsx: "typescript", js: "javascript", jsx: "javascript", py: "python", json: "json", css: "css", html: "html", sh: "bash", yml: "yaml", yaml: "yaml", toml: "toml" } as Record<string, string>)[path.split(".").at(-1) ?? ""] ?? "text";
  const fenceLength = (text.match(/`+/g) ?? []).reduce((maximum, run) => Math.max(maximum, run.length + 1), 3);
  const fence = "`".repeat(fenceLength);
  const renderedText = markdown && !rawMarkdown ? text : `${fence}${language}\n${text}\n${fence}`;
  return <>
    <div className="g-work-address g-file-address"><div className="g-work-crumbs"><button type="button" className="dev-crumb" onClick={onBrowseFiles}><FolderOpen aria-hidden="true"/>全部文件</button><span>/</span><span className="dev-crumb is-current" title={path}>{name}</span></div><div className="g-file-actions">{markdown && <button type="button" className="g-info-link" aria-pressed={rawMarkdown} onClick={onToggleMarkdown}>{rawMarkdown ? "渲染视图" : "Markdown 源码"}</button>}<button type="button" className="g-info-link" onClick={() => void feedback.copy(text)}><Copy aria-hidden="true"/>复制</button></div></div>
    <article className="g-document"><MessageMarkdown text={renderedText} onCopy={feedback.copy}/>{state.truncated && <div className="dev-file-truncated"><span>文件较大，仅显示前段。</span><Button variant="outline" size="sm" onClick={() => void downloadProjectFile(apiKey, projectId, path).catch(() => undefined)}><Download aria-hidden="true"/>下载完整文件</Button></div>}</article>
    <CopyFeedback {...feedback}/>
  </>;
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

function DevelopmentDialogs({ dialog, onClose, dev, chat, apiKey, projectId, project, environmentState, onInspectConnection }: {
  dialog: PanelDialog | null; onClose: () => void; dev: DevelopmentState;
  chat: SessionState; apiKey: string; projectId: string | null; project: SandboxProject | null;
  environmentState: EnvironmentState | null; onInspectConnection: (connection: McpConnection) => void;
}) {
  return <Dialog open={dialog !== null} onOpenChange={open => { if (!open) onClose(); }}>
    <DialogContent className={`dev-dialog${dialog?.kind === "project" || dialog?.kind === "secrets" ? " is-wide" : ""}`} aria-describedby={undefined}>
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
      {dialog?.kind === "connections" && <><DialogHeader><DialogTitle>MCP 连接</DialogTitle><DialogDescription>当前账号配置的服务连接</DialogDescription></DialogHeader><div className="g-mcp-connections">{dev.connections === null ? <p role="status">账号 MCP 服务暂不可用</p> : dev.connections.length === 0 ? <p>还没有账号 MCP 连接</p> : dev.connections.map(connection => <button type="button" className="g-mcp-connection" key={connection.id} onClick={() => onInspectConnection(connection)}><Cpu aria-hidden="true"/><span>{connection.name}</span><small>{connection.credential_configured ? "凭据已配置" : "未配置凭据"}</small><ChevronRight aria-hidden="true"/></button>)}</div></>}
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

export function ProjectDialogBody({ dev, apiKey, projectId, workspace, onClose, onChoose, canCreate = dev.canWrite }: {
  dev: DevelopmentState; apiKey: string; projectId: string | null; workspace: string; onClose: () => void;
  onChoose?: (projectId: string) => void; canCreate?: boolean;
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
    if (!title.trim() || creatingBusy || !canCreate) return;
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
      <DialogDescription>{onChoose ? "选择新会话使用的开发项目。" : "为这段会话绑定开发项目。"}</DialogDescription>
    </DialogHeader>
    {error && <p className="g-error" role="status">{error}</p>}
    <div className="g-project-options question-options" role="radiogroup" aria-label="选择项目">
      {(page ?? []).map(item => <label className="g-project-option question-option" key={item.project_id}>
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
        <Button type="submit" size="sm" disabled={creatingBusy || !title.trim() || !canCreate}>{creatingBusy ? "创建中…" : "创建项目"}</Button>
      </div>
    </form>}
    <div className="dev-dialog-footer">
      {!creating && canCreate && <Button variant="outline" size="sm" onClick={() => { setCreating(true); setError(""); }}>新建项目</Button>}
      <Button variant="ghost" size="sm" onClick={onClose}>关闭</Button>
      <Button size="sm" disabled={dev.busy || (onChoose ? !selected || !canCreate : selected === projectId || !dev.canWrite)} onClick={() => {
        if (onChoose && selected) { onChoose(selected); onClose(); }
        else void dev.bindProject(selected).then(onClose);
      }}>{onChoose ? "使用项目" : "绑定项目"}</Button>
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
      <form className="dev-secret-form" onSubmit={event => { event.preventDefault(); if (dev.canApprove) void submit(rotateTarget ? "rotate" : "add", rotateTarget ?? name); }}>
        <strong className="dev-secret-heading">新增或轮换环境变量</strong>
        <div className="dev-secret-fields">
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
        </div>
        <div className="g-inline-actions">
          {rotateTarget && <Button type="button" variant="ghost" size="sm" disabled={busy} onClick={() => { setRotateTarget(null); setName(""); setValue(""); }}>取消轮换</Button>}
          <Button type="submit" size="sm" disabled={busy || !dev.canApprove || !name.trim() || !value}>{rotateTarget ? "确认轮换" : "添加引用"}</Button>
        </div>
      </form>
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
      <dt>总容量</dt><dd>{contextWindowLabel(policy)}</dd>
      <dt>消息上限</dt><dd>{policy.max_messages === null ? "不设消息数上限" : `${fmtCount(policy.max_messages)} 条`}</dd>
      <dt>输出预留</dt><dd>{contextCount(policy.reserved_output_tokens)} tokens</dd>
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
