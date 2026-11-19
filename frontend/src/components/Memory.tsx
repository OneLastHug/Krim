import { useCallback, useEffect, useMemo, useRef, useState, type FormEvent } from "react";
import { BookOpen, ChevronDown, ChevronUp, LoaderCircle, Pencil, Plus, Search, Trash2, X } from "lucide-react";
import { Button } from "./ui/button";
import { Input } from "./ui/input";
import { Textarea } from "./ui/textarea";
import {
  memoriesApi, memoryError, newMemoryRequestKey,
  type Memory, type MemoryCreateInput, type MemoryDeleteInput, type MemoryPage,
  type MemoryPageQuery, type MemoryPreference, type MemoryScope, type MemoryUpdateInput,
} from "../lib/memory";
import "./memory.css";

export type MemoryViewScope = { kind: "account" } | { kind: "project"; workspace_id: string; project_id: string };
export interface MemoryAdapter {
  list(query: MemoryPageQuery, scope: MemoryScope | undefined): Promise<MemoryPage>;
  get(id: string, scope: MemoryScope | undefined): Promise<Memory>;
  create(input: MemoryCreateInput, scope: MemoryScope | undefined): Promise<unknown>;
  update(id: string, input: MemoryUpdateInput, scope: MemoryScope | undefined): Promise<unknown>;
  remove(id: string, input: MemoryDeleteInput, scope: MemoryScope | undefined): Promise<unknown>;
  preference?(): Promise<MemoryPreference>;
  setPreference?(enabled: boolean, expectedRevision: number, requestKey: string): Promise<MemoryPreference>;
}
export interface MemoryProps {
  /** 默认通过 memoriesApi 请求；提供 adapter 时所有记忆数据与操作由调用方注入。 */
  apiKey?: string;
  adapter?: MemoryAdapter;
  scope: MemoryViewScope;
  title?: string;
}

const PAGE_SIZE = 50;
const SOURCE_LABEL: Record<Memory["source_kind"], string> = {
  user: "用户添加", web: "网页", mcp: "MCP", task: "任务",
};
const KIND_LABEL: Record<Memory["memory_kind"], string> = { note: "记事", preference: "偏好" };
const timeValue = (seconds: number) => {
  const date = new Date(seconds * 1000);
  return Number.isNaN(date.getTime()) ? "—" : date.toLocaleString("zh-CN", { hour12: false });
};
const isoTime = (seconds: number) => {
  const date = new Date(seconds * 1000);
  return Number.isNaN(date.getTime()) ? undefined : date.toISOString();
};

function scopeValue(scope: MemoryViewScope): MemoryScope | undefined {
  return scope.kind === "project" ? { workspace_id: scope.workspace_id, project_id: scope.project_id } : undefined;
}
function defaultAdapter(apiKey: string): MemoryAdapter {
  return {
    list: (query, scope) => memoriesApi.page(apiKey, query, scope),
    get: (id, scope) => memoriesApi.get(apiKey, id, scope),
    create: (input, scope) => memoriesApi.create(apiKey, input, scope),
    update: (id, input, scope) => memoriesApi.update(apiKey, id, input, scope),
    remove: (id, input, scope) => memoriesApi.remove(apiKey, id, input, scope),
    preference: () => memoriesApi.preference(apiKey),
    setPreference: (enabled, expectedRevision, requestKey) =>
      memoriesApi.setPreference(apiKey, enabled, expectedRevision, requestKey),
  };
}

export function Memory({ apiKey, adapter: injectedAdapter, scope, title }: MemoryProps) {
  const adapter = useMemo(() => injectedAdapter ?? (apiKey ? defaultAdapter(apiKey) : null), [injectedAdapter, apiKey]);
  const requestScope = useMemo(() => scopeValue(scope), [scope.kind, scope.kind === "project" ? scope.workspace_id : "", scope.kind === "project" ? scope.project_id : ""]);
  const identity = `${scope.kind}:${requestScope?.workspace_id ?? ""}:${requestScope?.project_id ?? ""}:${apiKey ?? "injected"}`;
  const [items, setItems] = useState<Memory[]>([]);
  const [displayIdentity, setDisplayIdentity] = useState(identity);
  const [nextOffset, setNextOffset] = useState<number | null>(0);
  const [loaded, setLoaded] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [query, setQuery] = useState("");
  const [appliedQuery, setAppliedQuery] = useState("");
  const [expandedId, setExpandedId] = useState<string | null>(null);
  const [detail, setDetail] = useState<Memory | null>(null);
  const [detailBusy, setDetailBusy] = useState(false);
  const [editing, setEditing] = useState(false);
  const [draft, setDraft] = useState("");
  const [createOpen, setCreateOpen] = useState(false);
  const [createBody, setCreateBody] = useState("");
  const [createKind, setCreateKind] = useState<Memory["memory_kind"]>("note");
  const [confirmDelete, setConfirmDelete] = useState(false);
  const [preference, setPreference] = useState<MemoryPreference | null>(null);
  const [preferenceBusy, setPreferenceBusy] = useState(false);
  const alive = useRef(true);
  const operationRef = useRef(0);
  const detailRequestRef = useRef(0);
  const activeIdentity = useRef(identity);
  activeIdentity.current = identity;
  const accountScope = scope.kind === "account";
  const scopeReady = displayIdentity === identity;
  const visibleItems = scopeReady ? items : [];
  const visibleLoaded = scopeReady && loaded;
  const visibleExpandedId = scopeReady ? expandedId : null;
  const visibleDetail = scopeReady ? detail : null;
  const visiblePreference = scopeReady ? preference : null;
  const visibleError = scopeReady ? error : "";
  const current = (owner: string) => alive.current && activeIdentity.current === owner;

  const loadPage = useCallback(async (offset: number, append: boolean, search = "") => {
    if (!adapter) { setError("请提供 API 密钥或记忆数据适配器。"); return; }
    const owner = identity;
    const operation = ++operationRef.current;
    setBusy(true);
    setError("");
    try {
      const page = await adapter.list({ offset, limit: PAGE_SIZE, query: search || undefined }, requestScope);
      if (!alive.current || activeIdentity.current !== owner || operationRef.current !== operation) return;
      setItems(current => {
        if (!append) return page.items;
        const seen = new Set(current.map(item => item.id));
        return [...current, ...page.items.filter(item => {
          if (seen.has(item.id)) return false;
          seen.add(item.id);
          return true;
        })];
      });
      setNextOffset(page.next_offset);
      setLoaded(true);
    } catch (reason) {
      if (!alive.current || activeIdentity.current !== owner || operationRef.current !== operation) return;
      setError(memoryError(reason));
    } finally {
      if (alive.current && activeIdentity.current === owner && operationRef.current === operation) setBusy(false);
    }
  }, [adapter, identity, requestScope]);

  useEffect(() => {
    alive.current = true;
    operationRef.current += 1;
    setDisplayIdentity(identity);
    setItems([]); setNextOffset(0); setLoaded(false); setBusy(false); setError("");
    setQuery(""); setAppliedQuery(""); setCreateOpen(false); setCreateBody(""); setCreateKind("note");
    setExpandedId(null); setDetail(null); setDetailBusy(false); setEditing(false); setDraft(""); setConfirmDelete(false);
    detailRequestRef.current += 1;
    setPreference(null); setPreferenceBusy(false);
    void loadPage(0, false, "");
    if (scope.kind === "account" && adapter?.preference) {
      void adapter.preference().then(value => {
        if (alive.current && activeIdentity.current === identity) setPreference(value);
      }).catch(reason => {
        if (alive.current && activeIdentity.current === identity) setError(memoryError(reason));
      });
    }
    return () => { alive.current = false; operationRef.current += 1; };
  }, [identity, loadPage, scope.kind, adapter]);

  async function submitSearch(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const normalized = query.trim();
    setAppliedQuery(normalized);
    setItems([]); setNextOffset(0); setLoaded(false); setExpandedId(null); setDetail(null);
    await loadPage(0, false, normalized);
  }
  async function loadMore() {
    if (nextOffset === null || busy) return;
    await loadPage(nextOffset, true, appliedQuery);
  }
  async function openDetail(item: Memory) {
    const detailRequest = ++detailRequestRef.current;
    if (expandedId === item.id) { setExpandedId(null); setDetail(null); setEditing(false); return; }
    if (!adapter) return;
    setExpandedId(item.id); setDetail(null); setEditing(false); setConfirmDelete(false); setDetailBusy(true); setError("");
    const owner = identity;
    try {
      const value = await adapter.get(item.id, requestScope);
      if (current(owner) && detailRequestRef.current === detailRequest) {
        setDetail(value); setDraft(value.body);
      }
    } catch (reason) {
      if (current(owner) && detailRequestRef.current === detailRequest) setError(memoryError(reason));
    } finally {
      if (current(owner) && detailRequestRef.current === detailRequest) setDetailBusy(false);
    }
  }
  async function refreshAfterWrite(id: string | undefined, owner: string) {
    if (!current(owner)) return;
    setEditing(false); setConfirmDelete(false); setCreateOpen(false); setCreateBody("");
    const detailRequest = ++detailRequestRef.current;
    if (id) { setExpandedId(id); setDetail(null); }
    await loadPage(0, false, appliedQuery);
    if (!current(owner) || detailRequestRef.current !== detailRequest) return;
    if (id && adapter) {
      try {
        const value = await adapter.get(id, requestScope);
        if (current(owner) && detailRequestRef.current === detailRequest) { setDetail(value); setDraft(value.body); }
      } catch {
        if (current(owner) && detailRequestRef.current === detailRequest) { setExpandedId(null); setDetail(null); }
      }
    }
  }
  async function saveNew() {
    if (!adapter || busy) return;
    const owner = identity;
    const body = createBody.trim();
    if (!body) { setError("请输入记忆内容。"); return; }
    setBusy(true); setError("");
    try {
      await adapter.create({ request_key: newMemoryRequestKey(), body, memory_kind: createKind, source_kind: "user" }, requestScope);
      if (!current(owner)) return;
      setCreateOpen(false); setCreateBody("");
      await loadPage(0, false, appliedQuery);
    } catch (reason) {
      if (current(owner)) setError(memoryError(reason));
    } finally { if (current(owner)) setBusy(false); }
  }
  async function saveEdit() {
    if (!adapter || !detail || busy) return;
    const owner = identity;
    const body = draft.trim();
    if (!body) { setError("记忆内容不能为空。"); return; }
    setBusy(true); setError("");
    try {
      await adapter.update(detail.id, { request_key: newMemoryRequestKey(), body, source_ref: detail.source_ref ?? undefined,
        source_kind: detail.source_kind, memory_kind: detail.memory_kind, expected_revision: detail.revision,
        expected_content_hash: detail.content_hash }, requestScope);
      if (!current(owner)) return;
      await refreshAfterWrite(detail.id, owner);
    } catch (reason) {
      if (current(owner)) setError(memoryError(reason));
    } finally { if (current(owner)) setBusy(false); }
  }
  async function deleteMemory() {
    if (!adapter || !detail || busy) return;
    const owner = identity;
    setBusy(true); setError("");
    try {
      await adapter.remove(detail.id, { request_key: newMemoryRequestKey(), expected_revision: detail.revision,
        expected_content_hash: detail.content_hash }, requestScope);
      if (!current(owner)) return;
      ++detailRequestRef.current;
      setExpandedId(null); setDetail(null); setConfirmDelete(false);
      await loadPage(0, false, appliedQuery);
    } catch (reason) {
      if (current(owner)) setError(memoryError(reason));
    } finally { if (current(owner)) setBusy(false); }
  }
  async function changePreference(enabled: boolean) {
    if (!current(identity) || !adapter?.setPreference || !preference || preferenceBusy) return;
    const owner = identity;
    setPreferenceBusy(true); setError("");
    try {
      const value = await adapter.setPreference(enabled, preference.revision, newMemoryRequestKey());
      if (current(owner)) setPreference(value);
    } catch (reason) { if (current(owner)) setError(memoryError(reason)); }
    finally { if (current(owner)) setPreferenceBusy(false); }
  }

  const heading = title ?? (scope.kind === "project" ? "项目记忆" : "记忆");
  return <section className="memory-panel" aria-label={heading}>
    <header className="memory-heading">
      <div className="memory-heading-copy">
        <h2>{heading}</h2>
        <p>{scope.kind === "project" ? "仅在此项目中使用的记忆。" : "管理可供 Krim 参考的个人记忆。"}</p>
      </div>
      <Button type="button" disabled={!scopeReady || busy} onClick={() => { setCreateOpen(value => !value); setError(""); }}>
        {scopeReady && createOpen ? <X aria-hidden="true" /> : <Plus aria-hidden="true" />}{scopeReady && createOpen ? "取消新增" : "新增记忆"}
      </Button>
    </header>

    {accountScope && <div className="memory-preference" aria-live="polite">
      <span><strong>模型使用记忆</strong><small>关闭后仍可查看、编辑和删除已有记忆。</small></span>
      {visiblePreference ? <label className="memory-toggle">
        <input type="checkbox" checked={visiblePreference.enabled} disabled={preferenceBusy || !adapter?.setPreference}
          onChange={event => void changePreference(event.target.checked)} aria-label="允许模型使用记忆" />
        <span>{visiblePreference.enabled ? "已开启" : "已关闭"}</span>
      </label> : <span className="memory-state">{scopeReady && preferenceBusy ? "正在保存…" : "读取中"}</span>}
    </div>}

    {scopeReady && createOpen && <div className="memory-editor memory-create">
      <label htmlFor="memory-create-body">记忆内容</label>
      <Textarea id="memory-create-body" value={scopeReady ? createBody : ""} rows={4} disabled={busy}
        onChange={event => setCreateBody(event.target.value)} placeholder="写下希望保留的信息" />
      <div className="memory-editor-actions">
        <label className="memory-kind-select" htmlFor="memory-create-kind">类型
          <select id="memory-create-kind" value={scopeReady ? createKind : "note"} disabled={!scopeReady || busy} onChange={event => setCreateKind(event.target.value as Memory["memory_kind"])}>
            <option value="note">记事</option><option value="preference">偏好</option>
          </select>
        </label>
        <Button type="button" disabled={busy} onClick={() => void saveNew()}>{busy ? "正在保存…" : "保存记忆"}</Button>
      </div>
    </div>}

    <form className="memory-search" onSubmit={event => void submitSearch(event)} role="search">
      <Search aria-hidden="true" />
      <Input aria-label="搜索记忆" type="search" value={scopeReady ? query : ""} autoComplete="off" placeholder="搜索记忆" disabled={!scopeReady || busy}
        onChange={event => setQuery(event.target.value)} />
      <Button variant="outline" type="submit" disabled={!scopeReady || busy}>搜索</Button>
    </form>

    {visibleError && <p className="memory-error" role="alert">{visibleError}</p>}
    {scopeReady && busy && !loaded && <div className="memory-loading" role="status" aria-live="polite"><LoaderCircle aria-hidden="true" />正在读取记忆</div>}
    {scopeReady && !busy && !loaded && visibleError && <div className="memory-retry"><Button variant="outline" type="button" onClick={() => void loadPage(0, false, appliedQuery)}>重新读取</Button></div>}
    {visibleLoaded && visibleItems.length === 0 && <div className="memory-empty"><BookOpen aria-hidden="true" /><strong>{appliedQuery ? "没有匹配的记忆" : "还没有记忆"}</strong><span>{appliedQuery ? "试试其他关键词。" : "新增一条记忆，之后仍可随时修改或删除。"}</span></div>}
    {visibleItems.length > 0 && <ul className="memory-list">
      {visibleItems.map(item => <li className="memory-item" key={item.id}>
        <div className="memory-item-heading">
          <div className="memory-item-labels"><span className="memory-kind">{KIND_LABEL[item.memory_kind]}</span><span>{SOURCE_LABEL[item.source_kind]}</span></div>
          <Button variant="ghost" size="sm" type="button" disabled={busy} aria-expanded={visibleExpandedId === item.id}
            aria-label={`${visibleExpandedId === item.id ? "收起" : "查看"}记忆 ${item.body.slice(0, 24)}`}
            onClick={() => void openDetail(item)}>{visibleExpandedId === item.id ? <ChevronUp aria-hidden="true" /> : <ChevronDown aria-hidden="true" />}{visibleExpandedId === item.id ? "收起" : "查看"}</Button>
        </div>
        <p className="memory-preview">{item.body}</p>
        <div className="memory-item-meta"><span title={item.source_ref ?? undefined}>{item.source_ref ? `来源：${item.source_ref}` : "来源：直接添加"}</span><time dateTime={isoTime(item.updated_at)}>更新于 {timeValue(item.updated_at)}</time></div>
        {scopeReady && visibleExpandedId === item.id && <div className="memory-detail">
          {detailBusy && <p className="memory-detail-loading" role="status">正在读取记忆详情</p>}
          {visibleDetail && visibleDetail.id === item.id && <>
            <div className="memory-detail-meta"><span>创建于 {timeValue(visibleDetail.created_at)}</span><span>修订 {visibleDetail.revision}</span></div>
            {visibleDetail.authorization_turn_id && <div className="memory-detail-meta"><span>授权来源回合 {visibleDetail.authorization_turn_id}</span></div>}
            {editing ? <div className="memory-editor">
              <label htmlFor={`memory-edit-${item.id}`}>记忆内容</label>
              <Textarea id={`memory-edit-${item.id}`} value={draft} rows={5} disabled={busy} onChange={event => setDraft(event.target.value)} />
              <div className="memory-editor-actions"><Button variant="outline" type="button" disabled={busy} onClick={() => { setEditing(false); setDraft(visibleDetail.body); }}>取消</Button>
                <Button type="button" disabled={busy} onClick={() => void saveEdit()}>{busy ? "正在保存…" : "保存更改"}</Button></div>
            </div> : <>
              <p className="memory-full-body">{visibleDetail.body}</p>
              {!confirmDelete ? <div className="memory-detail-actions">
                <Button variant="outline" type="button" disabled={busy} onClick={() => { setDraft(visibleDetail.body); setEditing(true); }}><Pencil aria-hidden="true" />编辑</Button>
                <Button variant="ghost" type="button" className="memory-delete-action" disabled={busy} onClick={() => setConfirmDelete(true)}><Trash2 aria-hidden="true" />删除</Button>
              </div> : <div className="memory-delete-confirm" role="group" aria-label="确认删除记忆">
                <span>删除后，模型不会再使用这条记忆。历史记录按原保留策略保留。</span>
                <Button variant="outline" type="button" disabled={busy} onClick={() => setConfirmDelete(false)}>取消</Button>
                <Button variant="destructive" type="button" disabled={busy} onClick={() => void deleteMemory()}>{busy ? "正在删除…" : "确认删除"}</Button>
              </div>}
            </>}
          </>}
        </div>}
      </li>)}
    </ul>}
    {scopeReady && busy && loaded && <p className="memory-loading-more" role="status">正在读取…</p>}
    {visibleLoaded && nextOffset !== null && <div className="memory-load-more"><Button variant="outline" type="button" disabled={busy} onClick={() => void loadMore()}>加载更多</Button></div>}
  </section>;
}
