import { useEffect, useLayoutEffect, useRef, useState, type CSSProperties, type FormEvent } from "react";
import { Tooltip } from "@base-ui/react/tooltip";
import { Menu } from "@base-ui/react/menu";
import {
  Archive, ArrowDown, ArrowLeft, ArrowRight, ArrowUp, Check, ChevronDown,
  ChevronRight, Code2, Copy, CornerDownLeft, FileCode2, FileText, Folder, FolderPlus,
  Menu as MenuIcon, MoreHorizontal, PanelLeft, PanelRight, Pencil, Pin, Plus, RotateCcw,
  Search, Settings2, Square, X,
} from "lucide-react";
import { Button } from "../components/ui/button";
import { Dialog, DialogContent, DialogTitle } from "../components/ui/dialog";
import { files, type PreviewFile, type PrototypeConversation, type PrototypeTurn } from "./fixtures";
import { MarkdownMessage } from "./MarkdownMessage";
import { ActionMenu, IconButton, KrimMark, MenuAction, TextMenu } from "./primitives";
import { usePrototype, type PrototypeState } from "./use-prototype";

interface SidebarProps {
  chat: PrototypeState;
  open: boolean;
  mobileOpen: boolean;
  onSettings: () => void;
  onSelect: (id: string) => void;
  onNew: (project?: string) => void;
  onRename: (conversation: PrototypeConversation) => void;
}

function Sidebar({ chat, open, mobileOpen, onSettings, onSelect, onNew, onRename }: SidebarProps) {
  const [collapsed, setCollapsed] = useState<string[]>([]);
  const sessions = chat.visible.filter(item => item.id !== "new" || item.turns.length > 0 || item.draft.trim().length > 0);
  const groups = [
    { name: "置顶", entries: sessions.filter(item => item.pinned), pinned: true },
    ...["Krim", "Notes"].map(name => ({ name, entries: sessions.filter(item => !item.pinned && item.project === name), pinned: false })),
  ];
  return <aside className={`p-sidebar ${open ? "" : "is-collapsed"} ${mobileOpen ? "is-mobile-open" : ""}`} aria-label="会话导航">
    <div className="p-sidebar-top"><div className="p-workspace-switch"><span className="p-brand"><KrimMark /><span>Krim Agent</span></span><span className="p-code-mode"><Code2 />Code</span></div></div>
    <nav className="p-primary-nav" aria-label="主要操作">
      <Button variant="ghost" className="p-nav-button p-new-button" onClick={() => onNew()}><Plus /><span>新会话</span><kbd>Ctrl ⇧ N</kbd></Button>
      <Button variant="ghost" className="p-nav-button" onClick={onSettings}><Settings2 /><span>自定义</span></Button>
    </nav>
    <div className="p-sidebar-scroll">
      {groups.filter(group => group.entries.length).map(group => <section className="p-session-group" key={group.name}>
        <div className="p-group-heading"><button className="p-group-toggle" onClick={() => setCollapsed(value => value.includes(group.name) ? value.filter(name => name !== group.name) : [...value, group.name])} aria-expanded={!collapsed.includes(group.name)}><ChevronRight className={collapsed.includes(group.name) ? "" : "is-expanded"} /><span>{group.name}</span></button>{!group.pinned && <IconButton label={`在 ${group.name} 新建会话`} className="p-group-add" onClick={() => onNew(group.name)}><Plus /></IconButton>}</div>
        {!collapsed.includes(group.name) && <ul className="p-session-list">{group.entries.map(conversation => <li className={`p-session ${conversation.id === chat.current.id ? "is-selected" : ""}`} key={conversation.id}>
          <button className="p-session-button" onClick={() => onSelect(conversation.id)} aria-current={conversation.id === chat.current.id ? "page" : undefined} title={conversation.title}><Code2 /><span>{conversation.title}</span>{chat.runningId === conversation.id && <span className="p-running-dot" aria-label="正在生成" />}</button>
          <ActionMenu label={`${conversation.title} 的操作`} trigger={<MoreHorizontal />} className="p-session-menu">
            <MenuAction onClick={() => onRename(conversation)}><Pencil />重命名</MenuAction>
            <MenuAction onClick={() => chat.togglePinned(conversation.id)}><Pin />{conversation.pinned ? "取消置顶" : "置顶"}</MenuAction>
            <Menu.Separator className="p-menu-separator" />
            <MenuAction disabled={chat.runningId === conversation.id} onClick={() => chat.archive(conversation.id)}><Archive />归档会话</MenuAction>
          </ActionMenu>
        </li>)}</ul>}
      </section>)}
      {!groups.some(group => group.entries.length) && <div className="p-sidebar-empty">会话会显示在这里</div>}
    </div>
    <div className="p-sidebar-footer"><span className="p-workspace-avatar">K</span><span>本地样例</span><span className="p-footer-spacer" /><span className="p-sidebar-status" aria-label="不会发送模型请求" /></div>
  </aside>;
}

function MenuChoice({ label, selected, shortcut, onSelect }: { label: string; selected: boolean; shortcut?: string; onSelect: () => void }) {
  return <Menu.Item className="p-menu-item p-choice-item" onClick={onSelect}>
    <span className={`p-choice-check ${selected ? "is-selected" : ""}`}>{selected && <Check />}</span><span>{label}</span>{shortcut && <kbd>{shortcut}</kbd>}
  </Menu.Item>;
}

function HomeOverview({ conversations }: { conversations: PrototypeConversation[] }) {
  const [tab, setTab] = useState<"overview" | "models">("overview");
  const [range, setRange] = useState("all");
  const recorded = conversations.filter(conversation => !conversation.archived && conversation.turns.length > 0);
  const messages = recorded.reduce((total, conversation) => total + conversation.turns.length, 0);
  const metrics = [
    ["Sessions", String(recorded.length)], ["Messages", String(messages)], ["Total tokens", "0"], ["Active days", "0"],
    ["Current streak", "0d"], ["Longest streak", "0d"], ["Peak hour", "—"], ["Favorite model", "—"],
  ];
  return <section className="p-empty-home" aria-label="本地工作区概览">
    <h1><KrimMark /><span>What's up next?</span></h1>
    <section className="p-dashboard" aria-label="本地活动概览">
      <div className="p-dashboard-head">
        <div className="p-dashboard-tabs" role="tablist" aria-label="概览分类">
          <button type="button" role="tab" aria-selected={tab === "overview"} onClick={() => setTab("overview")}>Overview</button>
          <button type="button" role="tab" aria-selected={tab === "models"} onClick={() => setTab("models")}>Models</button>
        </div>
        {tab === "overview" && <div className="p-range-tabs" aria-label="活动时段">{[["all", "All"], ["30d", "30d"], ["7d", "7d"]].map(([key, label]) => <button type="button" key={key} aria-pressed={range === key} onClick={() => setRange(key!)}>{label}</button>)}</div>}
      </div>
      {tab === "overview" ? <>
        <dl className="p-dashboard-metrics">{metrics.map(([label, value]) => <div className="p-metric" key={label}><dt>{label}</dt><dd>{value}</dd></div>)}</dl>
        <div className="p-activity-grid" role="img" aria-label="暂无本地活动时间记录">{Array.from({ length: 196 }, (_, index) => <span key={index} title="没有本地活动记录" />)}</div>
        <p className="sr-only">活动方格仅反映本地记录；当前样例没有活动时间或模型令牌数据。</p>
      </> : <div className="p-model-overview"><span>本地样例</span><strong>0 次模型请求</strong><small>模型选项仅作界面预览，不会发送请求</small></div>}
    </section>
  </section>;
}

function FilePreview({ file, onSelect, onClose }: { file: PreviewFile; onSelect: (file: PreviewFile) => void; onClose: () => void }) {
  return <aside className="p-preview" aria-label="文件预览">
    <div className="p-preview-title"><span>文件</span><IconButton label="关闭文件预览" autoFocus onClick={onClose}><X /></IconButton></div>
    <div className="p-file-list" aria-label="参考文件">{files.map(item => <button key={item.path} className={`p-file-row ${file.path === item.path ? "is-active" : ""}`} onClick={() => onSelect(item)} aria-pressed={file.path === item.path}>{item.language === "Python" ? <FileCode2 /> : <FileText />}<span>{item.name}</span></button>)}</div>
    <div className="p-file-breadcrumb"><Folder /><span>{file.path}</span></div>
    <div className="p-file-body" tabIndex={0} aria-label={`${file.name} 内容`}><pre>{file.content.split("\n").map((line, index) => <span className="p-code-line" key={index}><span className="p-line-number" aria-hidden="true">{index + 1}</span><span>{line || " "}</span></span>)}</pre></div>
    <footer className="p-file-footer"><span>{file.language}</span><span>UTF-8</span><span>只读</span></footer>
  </aside>;
}

function MessageTurn({ turn, busy, onRetry, onFile, onReuse, onCopy }: { turn: PrototypeTurn; busy: boolean; onRetry: () => void; onFile: (name: string) => void; onReuse: () => void; onCopy: (text: string) => void }) {
  return <article className="p-turn" data-turn-id={turn.id} aria-label="一轮对话">
    <div className="p-user-message"><div className="p-user-text">{turn.prompt}</div><div className="p-user-actions"><IconButton label="复制用户消息" onClick={() => onCopy(turn.prompt)}><Copy /></IconButton><IconButton label="复用这条消息" onClick={onReuse}><Pencil /></IconButton></div></div>
    <div className="p-assistant-message">
      {turn.files.length > 0 && <div className="p-reference-files">{turn.files.map(name => <button key={name} onClick={() => onFile(name)}><FileText /><span>{name}</span><ChevronRight /></button>)}</div>}
      {turn.answer ? <MarkdownMessage text={turn.answer} /> : <div className="p-thinking" role="status"><KrimMark /><span>正在整理思路</span><span className="p-thinking-dots">···</span></div>}
      {turn.status === "streaming" && turn.answer && <span className="p-stream-cursor" aria-label="正在生成" />}
      {turn.status === "stopped" && <div className="p-stopped" role="status"><Square />已停止<Button variant="ghost" size="xs" onClick={onRetry} disabled={busy}>重新生成</Button></div>}
      {turn.status === "complete" && <div className="p-answer-actions"><IconButton label="复制回答" onClick={() => onCopy(turn.answer)}><Copy /></IconButton><IconButton label="重新生成回答" onClick={onRetry} disabled={busy}><RotateCcw /></IconButton><ActionMenu label="回答选项" trigger={<MoreHorizontal />}><MenuAction onClick={() => onCopy(turn.answer)}><Copy />复制 Markdown</MenuAction><MenuAction onClick={onReuse}><Pencil />复用提问</MenuAction></ActionMenu></div>}
    </div>
  </article>;
}

export function PrototypeApp() {
  const chat = usePrototype();
  const [sidebarOpen, setSidebarOpen] = useState(true);
  const [mobileOpen, setMobileOpen] = useState(false);
  const [previewFile, setPreviewFile] = useState<PreviewFile | null>(null);
  const [searchOpen, setSearchOpen] = useState(false);
  const [query, setQuery] = useState("");
  const [searchIndex, setSearchIndex] = useState(0);
  const [showArchived, setShowArchived] = useState(false);
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [fontSize, setFontSize] = useState(14);
  const [mode, setMode] = useState("Accept edits");
  const [renameTarget, setRenameTarget] = useState<PrototypeConversation | null>(null);
  const [renameText, setRenameText] = useState("");
  const [toast, setToast] = useState("");
  const [showJump, setShowJump] = useState(false);
  const [narrow, setNarrow] = useState(() => window.matchMedia("(max-width: 700px)").matches);
  const [overlayPreview, setOverlayPreview] = useState(() => window.matchMedia("(max-width: 1100px)").matches);
  const textarea = useRef<HTMLTextAreaElement>(null);
  const scrollArea = useRef<HTMLDivElement>(null);
  const stickToBottom = useRef(true);
  const composing = useRef(false);
  const toastTimer = useRef<number | undefined>(undefined);
  const lastConversation = useRef(chat.current.id);
  const current = chat.current;
  const results = chat.conversations.filter(item => (showArchived || !item.archived) && `${item.title} ${item.turns.map(turn => turn.prompt).join(" ")}`.toLowerCase().includes(query.toLowerCase()));

  function focusInput() { requestAnimationFrame(() => textarea.current?.focus()); }
  function select(id: string) { chat.navigate(id); setMobileOpen(false); setSearchOpen(false); }
  function newConversation(project = "Krim") { chat.createConversation(project); setMobileOpen(false); setPreviewFile(null); stickToBottom.current = true; focusInput(); }
  function openSearch() { setQuery(""); setSearchIndex(0); setSearchOpen(true); setMobileOpen(false); }
  function rename(conversation: PrototypeConversation) { setRenameTarget(conversation); setRenameText(conversation.title); }
  function showFile(name: string) { setPreviewFile(files.find(item => item.name === name) ?? files[0]!); }
  function goToBottom() { stickToBottom.current = true; setShowJump(false); scrollArea.current?.scrollTo({ top: scrollArea.current.scrollHeight, behavior: "instant" }); }
  function send(event?: FormEvent) {
    event?.preventDefault();
    if (composing.current || chat.busy || !current.draft.trim()) return;
    stickToBottom.current = true; chat.send(); focusInput();
  }
  async function copy(text: string) {
    try { await navigator.clipboard.writeText(text); setToast("已复制"); }
    catch { setToast("复制失败，请选择文字后复制"); }
    window.clearTimeout(toastTimer.current);
    toastTimer.current = window.setTimeout(() => setToast(""), 2200);
  }

  useEffect(() => {
    const media = window.matchMedia("(max-width: 700px)");
    const previewMedia = window.matchMedia("(max-width: 1100px)");
    const change = () => { setNarrow(media.matches); setOverlayPreview(previewMedia.matches); };
    media.addEventListener("change", change);
    previewMedia.addEventListener("change", change);
    return () => { media.removeEventListener("change", change); previewMedia.removeEventListener("change", change); window.clearTimeout(toastTimer.current); };
  }, []);

  useEffect(() => {
    const keydown = (event: KeyboardEvent) => {
      if (event.defaultPrevented) return;
      if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === "k") { event.preventDefault(); openSearch(); }
      if ((event.ctrlKey || event.metaKey) && event.shiftKey && event.key.toLowerCase() === "n") { event.preventDefault(); newConversation(); }
      if (event.key === "Escape" && !searchOpen && !settingsOpen && !renameTarget) { setMobileOpen(false); setPreviewFile(null); }
    };
    window.addEventListener("keydown", keydown);
    return () => window.removeEventListener("keydown", keydown);
  });

  useLayoutEffect(() => {
    const element = textarea.current;
    if (!element) return;
    const resize = () => { element.style.height = "0px"; element.style.height = `${Math.max(20, Math.min(element.scrollHeight, 220))}px`; };
    resize();
    let width = element.clientWidth;
    const observer = new ResizeObserver(() => {
      if (element.clientWidth === width) return;
      width = element.clientWidth;
      resize();
    });
    observer.observe(element);
    return () => observer.disconnect();
  }, [current.id, current.draft]);

  useLayoutEffect(() => {
    if (lastConversation.current !== current.id) { stickToBottom.current = true; lastConversation.current = current.id; }
    if (stickToBottom.current && scrollArea.current) {
      scrollArea.current.scrollTop = scrollArea.current.scrollHeight;
      setShowJump(false);
    }
  }, [current.id, current.turns]);

  return <Tooltip.Provider delay={350}><div className="p-app" data-sidebar={sidebarOpen} style={{ "--reading-size": `${fontSize}px` } as CSSProperties}>
    {mobileOpen && <button className="p-mobile-backdrop" aria-label="关闭导航背景" onClick={() => setMobileOpen(false)} />}
    <header className="p-topbar" aria-label="全局导航">
      <TextMenu label="应用菜单" trigger={<MenuIcon />} className="p-app-menu-trigger">
        <div className="p-menu-label">Krim Agent</div>
        <MenuAction onClick={() => newConversation()}><Plus />新会话<kbd>Ctrl ⇧ N</kbd></MenuAction>
        <MenuAction onClick={openSearch}><Search />搜索会话<kbd>Ctrl K</kbd></MenuAction>
        <Menu.Separator className="p-menu-separator" />
        <MenuAction onClick={chat.loadExamples}><Code2 />打开示例</MenuAction>
      </TextMenu>
      <IconButton label={(narrow ? mobileOpen : sidebarOpen) ? "收起侧栏" : "展开侧栏"} aria-expanded={narrow ? mobileOpen : sidebarOpen} onClick={() => narrow ? setMobileOpen(value => !value) : setSidebarOpen(value => !value)}><PanelLeft /></IconButton>
      <IconButton label="搜索会话" onClick={openSearch}><Search /></IconButton>
      <div className="p-history-navigation" aria-label="会话历史">
        <IconButton label="上一个会话" disabled={!chat.canBack} onClick={chat.back}><ArrowLeft /></IconButton>
        <IconButton label="下一个会话" disabled={!chat.canForward} onClick={chat.forward}><ArrowRight /></IconButton>
      </div>
    </header>
    <div className="p-workspace">
      <Sidebar chat={chat} open={sidebarOpen} mobileOpen={mobileOpen} onSettings={() => { setSettingsOpen(true); setMobileOpen(false); }} onSelect={select} onNew={newConversation} onRename={rename} />
      <main className="p-main" inert={narrow && mobileOpen}>
      {current.turns.length > 0 && <header className="p-conversation-header"><h1 title={current.title}>{current.title}</h1><span className="p-header-project">{current.project === "Krim" ? "project" : current.project}</span><div className="p-header-actions"><IconButton label={previewFile ? "关闭文件预览" : "打开文件预览"} aria-pressed={!!previewFile} onClick={() => setPreviewFile(value => value ? null : files[0]!)}><PanelRight /></IconButton></div></header>}
      <div className="p-content">
        <section className="p-chat" aria-label="对话" inert={overlayPreview && !!previewFile}>
          <div className="p-scroll" ref={scrollArea} onScroll={() => { const element = scrollArea.current; if (!element) return; const atBottom = element.scrollHeight - element.scrollTop - element.clientHeight < 48; stickToBottom.current = atBottom; setShowJump(!atBottom); }}>
            {current.turns.length ? <div className="p-thread">{current.turns.map(turn => <MessageTurn key={turn.id} turn={turn} busy={chat.busy} onRetry={() => { stickToBottom.current = true; chat.retry(turn.id); }} onFile={showFile} onReuse={() => { chat.editDraft(turn.prompt); focusInput(); }} onCopy={text => void copy(text)} />)}</div> : <HomeOverview conversations={chat.visible} />}
          </div>
          {showJump && <Button variant="outline" size="icon-sm" className="p-jump" aria-label="回到底部" onClick={goToBottom}><ArrowDown /></Button>}
          <div className="p-composer-wrap">
            {current.turns.length === 0 && <div className="p-composer-context">
              <span className="p-context-chip" title="固定本地样例，不发送模型请求"><span className="p-context-dot" />Local</span>
              <span className="p-context-chip"><Folder />{current.project === "Krim" ? "project" : current.project}</span>
              <IconButton label={previewFile ? "关闭文件预览" : "打开文件预览"} aria-pressed={!!previewFile} onClick={() => setPreviewFile(value => value ? null : files[0]!)}><FolderPlus /></IconButton>
              <span className="p-context-spacer" />
            </div>}
            <form className="p-composer" onSubmit={send}>
              <label className="sr-only" htmlFor="prototype-prompt">发送消息</label>
              <textarea id="prototype-prompt" ref={textarea} rows={1} placeholder="给 Krim 一个任务…" value={current.draft} maxLength={12000} onChange={event => chat.editDraft(event.target.value)} onCompositionStart={() => { composing.current = true; }} onCompositionEnd={() => { composing.current = false; }} onKeyDown={event => {
                if (event.key === "Enter" && !event.shiftKey && !event.nativeEvent.isComposing && !composing.current && event.nativeEvent.keyCode !== 229) { event.preventDefault(); send(); }
              }} />
              {chat.runningId === current.id ? <Button className="p-send p-stop" size="icon-sm" type="button" aria-label="停止生成" onClick={chat.stop}><Square /></Button> : <Button className="p-send" size="icon-sm" type="submit" aria-label="发送消息" disabled={chat.busy || !current.draft.trim()}><CornerDownLeft /></Button>}
            </form>
            <div className="p-composer-footer">
              <div className="p-footer-left">
                <TextMenu label="权限模式" trigger={<><span>{mode}</span><ChevronDown /></>}>
                  <div className="p-menu-label">Mode</div>
                  <MenuChoice label="Ask permissions" selected={mode === "Ask permissions"} shortcut="1" onSelect={() => setMode("Ask permissions")} />
                  <MenuChoice label="Accept edits" selected={mode === "Accept edits"} shortcut="2" onSelect={() => setMode("Accept edits")} />
                  <MenuChoice label="Plan mode" selected={mode === "Plan mode"} shortcut="3" onSelect={() => setMode("Plan mode")} />
                  <MenuChoice label="Bypass permissions" selected={mode === "Bypass permissions"} shortcut="Enable" onSelect={() => setMode("Bypass permissions")} />
                </TextMenu>
                <ActionMenu label="添加文件上下文" trigger={<Plus />}>
                  <div className="p-menu-label">添加到上下文</div>
                  <MenuAction onClick={() => setPreviewFile(value => value ? null : files[0]!)}><PanelRight />打开文件预览</MenuAction>
                  <Menu.Separator className="p-menu-separator" />
                  {files.map(file => <MenuAction key={file.name} onClick={() => { chat.editDraft(`${current.draft}${current.draft ? " " : ""}@${file.name}`); focusInput(); }}><FileText />{file.name}</MenuAction>)}
                </ActionMenu>
              </div>
              <div className="p-footer-right">
                <TextMenu label="选择模型（本地样例，不发起请求）" trigger={<><span>GPT-6 Luna</span><ChevronDown /></>} className="p-model-menu-trigger">
                  <div className="p-menu-label">Models</div>
                  <MenuChoice label="GPT-6 Luna" selected shortcut="1" onSelect={() => undefined} />
                </TextMenu>
                <TextMenu label="更多模型选项" trigger={<span>Extra</span>} className="p-extra-menu-trigger">
                  <div className="p-menu-label">本地样例</div>
                  <MenuAction disabled>不会发起模型请求</MenuAction>
                </TextMenu>
                <span className="p-local-label">本地样例</span>
              </div>
            </div>
          </div>
        </section>
        {previewFile && <FilePreview file={previewFile} onSelect={setPreviewFile} onClose={() => { setPreviewFile(null); focusInput(); }} />}
      </div>
    </main>
    </div>

    <Dialog open={searchOpen} onOpenChange={setSearchOpen}><DialogContent className="p-search-dialog" showCloseButton={false}>
      <DialogTitle className="sr-only">搜索会话</DialogTitle>
      <div className="p-search-input"><Search /><input aria-label="搜索会话" placeholder="搜索会话…" value={query} onChange={event => { setQuery(event.target.value); setSearchIndex(0); }} onKeyDown={event => {
        if (event.nativeEvent.isComposing) return;
        if (event.key === "ArrowDown") { event.preventDefault(); setSearchIndex(value => Math.min(value + 1, results.length - 1)); }
        if (event.key === "ArrowUp") { event.preventDefault(); setSearchIndex(value => Math.max(0, value - 1)); }
        if (event.key === "Enter" && results[searchIndex]) { event.preventDefault(); const result = results[searchIndex]!; if (result.archived) chat.restore(result.id); select(result.id); }
      }} /><kbd>Esc</kbd></div>
      <div className="p-search-results">{results.length ? results.map((item, index) => <button key={item.id} className={index === searchIndex ? "is-highlighted" : ""} onMouseEnter={() => setSearchIndex(index)} onClick={() => { if (item.archived) chat.restore(item.id); select(item.id); }}><Code2 /><span><strong>{item.title}</strong><small>{item.project}{item.archived ? " · 已归档，点击恢复" : ""}</small></span><ArrowUp className="p-enter-arrow" /></button>) : <p className="p-no-results">没有找到相关会话</p>}</div>
      <div className="p-search-footer"><label><input type="checkbox" checked={showArchived} onChange={event => { setShowArchived(event.target.checked); setSearchIndex(0); }} />包含已归档</label><span>↑↓ 选择 <kbd>Enter</kbd> 打开</span></div>
    </DialogContent></Dialog>

    <Dialog open={!!renameTarget} onOpenChange={open => { if (!open) setRenameTarget(null); }}><DialogContent className="p-small-dialog"><DialogTitle>重命名会话</DialogTitle><form onSubmit={event => { event.preventDefault(); if (renameTarget && renameText.trim()) { chat.rename(renameTarget.id, renameText); setRenameTarget(null); } }}><label htmlFor="conversation-name">会话名称</label><input id="conversation-name" value={renameText} maxLength={80} onChange={event => setRenameText(event.target.value)} /><div className="p-dialog-actions"><Button variant="ghost" onClick={() => setRenameTarget(null)} type="button">取消</Button><Button type="submit" disabled={!renameText.trim()}>保存</Button></div></form></DialogContent></Dialog>

    <Dialog open={settingsOpen} onOpenChange={setSettingsOpen}><DialogContent className="p-small-dialog p-settings-dialog"><DialogTitle>阅读设置</DialogTitle><fieldset><legend>消息字号</legend><div className="p-font-options">{[12, 14, 16].map(size => <label key={size} className={fontSize === size ? "is-selected" : ""}><input type="radio" name="reading-size" value={size} checked={fontSize === size} onChange={() => setFontSize(size)} /><span>{size} px</span>{fontSize === size && <Check />}</label>)}</div></fieldset><p className="p-setting-note">调整消息正文，导航保持紧凑。</p><div className="p-settings-divider" /><div className="p-settings-info"><span>工作区</span><strong>本地样例</strong></div><div className="p-settings-info"><span>模型请求</span><strong>未启用</strong></div></DialogContent></Dialog>
    <div className={`p-toast ${toast ? "is-visible" : ""}`} role="status" aria-live="polite">{toast && <><Check />{toast}</>}</div>
  </div></Tooltip.Provider>;
}
