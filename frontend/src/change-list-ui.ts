/** 两种来源共用清单视图；文件事实始终由原生账本返回，Host 保持原协议。 */
import {
  ApiError, errorText, requestJson, requestJsonNullable, type AgentView, type NativeBatchHistoryPage,
  type NativeBatchRequest, type NativeBatchView, type NativeChangeItem, type NativeChangePage,
  type NativeListFilter, type WorkspaceView,
} from "./api.js";
import type { HostJson } from "./host.js";
import { localDate, t } from "./i18n.js";

type ChangeItem = { id: string; title: string; description: string; metadata: Record<string, HostJson>; status: "pending" | "executing" | "undone" | "kept" | "uncertain" };
type ChangeList = { topic_id: string; revision: number; items: ChangeItem[]; active_operation_id: string | null };
type ChangeOperation = { execution: { operation_id: string; topic_id: string; action: string }; status: "executing" | "done" | "error" | "uncertain"; receipt: { results: Array<{ item_id: string; status: string; error_code: string | null }> } | null };
type WorkspaceState = {
  agent: AgentView; grant?: WorkspaceView; page: NativeChangePage | null; filter: NativeListFilter;
  cursors: Array<string | null>; historyCursors: Array<string | null>; history: NativeBatchHistoryPage | null;
  batch: NativeBatchView | null; pending: NativeBatchRequest | null; query: string; message: string;
  activeBatchId: string | null; busy: boolean; sending: boolean; stale: boolean; queryVersion: number; loadingVersion: number;
};
type Source = { kind: "host" } | { kind: "workspace"; id: string };
type Row = { id: string; title: string; description: string; status: string; undo: boolean; keep: boolean; click: () => Promise<void> };
const node = (id: string): HTMLElement => document.getElementById(id)!;
const button = (id: string): HTMLButtonElement => node(id) as HTMLButtonElement;
const labels: Record<string, string> = {
  pending: "待处理变更", executing: "变更处理中", undone: "已撤销", kept: "已保留", uncertain: "文件结果待核实",
  applied: "文件已应用", applying: "文件应用中", undoing: "文件撤销中", keeping: "保留确认中",
  running: "文件操作执行中", done: "文件操作已确认", partial: "批次部分完成", failed: "批次失败",
  cancelled: "批次已取消", interrupted: "批次已中断", rejected: "文件操作已拒绝", skipped: "尚未执行",
};
const statusText = (value: string): string => t(labels[value] || value);
const actionText = (action: "keep" | "undo"): string => t(action === "keep" ? "保留变更" : "撤销变更");

export class ChangeListUI {
  private topicId: string | null = null;
  private sessionId: string | null = null;
  private catalog: ChangeList | null = null;
  private operations: ChangeOperation[] = [];
  private pendingTopics = new Set<string>();
  private source: Source = { kind: "host" };
  private workspaces = new Map<string, WorkspaceState>();
  private generation = 0;
  private loadVersion = 0;
  private timer: number | undefined;

  constructor(
    private readonly notice: (message: string, error?: boolean) => void,
    private readonly invokeHost: (data: HostJson) => Promise<HostJson>,
    private readonly openWorkspaceProposal: (agentId: string, proposalId: string, operationId?: string) => Promise<void>,
  ) {
    node("change-refresh").onclick = () => void this.refresh(true);
    node("change-clear").onclick = () => void this.clear();
    node("change-undo-all").onclick = () => void this.invoke("undo", null);
    node("change-keep-all").onclick = () => void this.invoke("keep", null);
    node("change-cancel").onclick = () => void this.cancel();
    (node("change-source") as HTMLSelectElement).onchange = () => {
      const value = (node("change-source") as HTMLSelectElement).value;
      void this.switchSource(value === "host" ? { kind: "host" } : { kind: "workspace", id: value });
    };
    (node("change-filter") as HTMLSelectElement).onchange = () => {
      const state = this.workspace(); if (!state) return;
      state.filter = (node("change-filter") as HTMLSelectElement).value as NativeListFilter;
      state.cursors = [null]; state.page = null; state.stale = true; void this.refresh();
    };
    node("change-prev").onclick = () => void this.page(-1);
    node("change-next").onclick = () => void this.page(1);
    node("change-history-prev").onclick = () => void this.historyPage(-1);
    node("change-history-next").onclick = () => void this.historyPage(1);
    node("change-query-id").onclick = () => void this.query("id");
    node("change-query-key").onclick = () => void this.query("key");
    node("change-wait").onclick = () => void this.query("wait");
    (node("change-batch-query") as HTMLInputElement).oninput = () => {
      const state = this.workspace(); if (state) state.query = (node("change-batch-query") as HTMLInputElement).value;
      this.syncQueryButtons();
    };
    node("change-panel").ontoggle = () => {
      if ((node("change-panel") as HTMLDetailsElement).open) this.schedule(); else this.stopTimer();
    };
    this.render();
  }
  setSession(id: string): void { this.sessionId = id; this.render(); }
  refreshLocale(): void { this.renderSources(); this.render(); }
  setWorkspaceSources(agents: AgentView[]): void {
    const ids = new Set<string>();
    let activeInvalidated = false;
    for (const agent of agents.filter(item => Boolean(item.session.permissions.workspace))) {
      ids.add(agent.agent_id);
      const state = this.workspaces.get(agent.agent_id);
      if (state) {
        const oldWorkspace = state.agent.session.permissions.workspace;
        const nextWorkspace = agent.session.permissions.workspace;
        const identityChanged = state.agent.session_id !== agent.session_id
          || state.agent.session.status !== agent.session.status
          || JSON.stringify(oldWorkspace) !== JSON.stringify(nextWorkspace)
          || String(state.agent.session.permissions.mode) !== String(agent.session.permissions.mode)
          || JSON.stringify(state.agent.session.permissions.tool_names) !== JSON.stringify(agent.session.permissions.tool_names);
        if (identityChanged) {
          // 同一个Agent ID也可能重开/换资源；旧请求不能覆盖新会话页面。
          if (this.source.kind === "workspace" && this.source.id === agent.agent_id) {
            this.generation += 1; this.stopTimer(); activeInvalidated = true;
          }
          const sameSession = state.agent.session_id === agent.session_id;
          // 换对象使原请求只能更新原缓存；同会话保留批次查询键，清除旧页面和未提交动作。
          this.workspaces.set(agent.agent_id, { ...state, agent, grant: undefined, page: null,
            cursors: [null], historyCursors: [null], history: null, stale: true, busy: false, sending: false,
            batch: sameSession ? state.batch : null, pending: null,
            activeBatchId: sameSession ? state.activeBatchId : null, queryVersion: state.queryVersion + 1 });
        } else state.agent = agent;
      } else this.workspaces.set(agent.agent_id, { agent, page: null, filter: "active", cursors: [null], historyCursors: [null], history: null,
        batch: null, pending: null, query: "", message: "", activeBatchId: null, busy: false, sending: false, stale: true, queryVersion: 0, loadingVersion: 0 });
    }
    for (const id of this.workspaces.keys()) if (!ids.has(id)) this.workspaces.delete(id);
    if (this.source.kind === "workspace" && !this.workspaces.has(this.source.id)) void this.switchSource({ kind: "host" });
    this.renderSources(); this.render();
    if (activeInvalidated && (node("change-panel") as HTMLDetailsElement).open) void this.refresh(true);
  }
  async selectWorkspace(id: string): Promise<void> {
    if (!this.workspaces.has(id)) throw new Error(t("工作区资源已停用或关闭"));
    (node("change-panel") as HTMLDetailsElement).open = true;
    await this.switchSource({ kind: "workspace", id });
    node("change-panel").scrollIntoView({ block: "nearest" });
  }
  async selectTopic(id: string): Promise<void> {
    this.beginTopic(id);
    if (this.source.kind === "host") await this.refresh();
  }
  /** 话题切换开始时立即废弃旧 Host 控件，不等消息树读取完成。 */
  beginTopic(id: string): void {
    if (id !== this.topicId) {
      this.catalog = null; this.operations = [];
      if (this.source.kind === "host") { this.generation += 1; this.stopTimer(); }
    }
    this.topicId = id; this.renderSources(); this.render();
  }
  private workspace(): WorkspaceState | undefined { return this.source.kind === "workspace" ? this.workspaces.get(this.source.id) : undefined; }
  private key(): string { return this.source.kind === "host" ? `host:${this.topicId}` : `workspace:${this.source.id}`; }
  private isCurrent(key: string, generation: number): boolean { return key === this.key() && generation === this.generation; }
  private base(id: string): string { return `/v1/topics/${encodeURIComponent(id)}/change-list`; }
  private native(state: WorkspaceState): string { return `/v1/agents/${encodeURIComponent(state.agent.agent_id)}/workspace`; }
  private async switchSource(source: Source): Promise<void> {
    this.stopTimer(); this.generation += 1; this.source = source;
    this.renderSources(); this.render(); await this.refresh();
  }
  private renderSources(): void {
    const select = node("change-source") as HTMLSelectElement;
    select.replaceChildren();
    const host = document.createElement("option"); host.value = "host"; host.textContent = t("Host 当前话题"); select.append(host);
    for (const [id, state] of this.workspaces) {
      const option = document.createElement("option"); option.value = id;
      option.textContent = `${t("工作区")} · ${state.agent.name} · ${id}`; select.append(option);
    }
    select.value = this.source.kind === "host" ? "host" : this.source.id;
  }
  private async refresh(reset = false): Promise<void> {
    const state = this.workspace(), key = this.key(), generation = this.generation, version = ++this.loadVersion;
    if (state) {
      if (reset) { state.cursors = [null]; state.historyCursors = [null]; }
      state.busy = true; state.loadingVersion = version; state.stale = true; this.render();
      try {
        const base = this.native(state);
        const queryVersion = state.queryVersion;
        const [page, history, grant] = await Promise.all([
          this.readPage(state), this.readHistory(state),
          state.agent.session.status === "open" ? requestJson<WorkspaceView>(base).catch(() => undefined) : Promise.resolve(undefined),
        ]);
        if (!this.isCurrent(key, generation) || version !== this.loadVersion) return;
        state.page = page.page;
        if (page.reset) { state.cursors = [null]; state.message = t("清单已变化，已返回第一页，请重新审阅"); }
        state.history = history; state.grant = grant; state.stale = false;
        // 历史始终可发现原生产者；手工选择已结束批次时不强制覆盖审阅对象。
        const latest = history.items.find(item => item.producer_status === "running") || history.items[0];
        if (state.historyCursors.length === 1) state.activeBatchId = history.items.find(item => item.producer_status === "running")?.batch_id || null;
        const id = history.items.find(item => item.request_key === state.pending?.request_key)?.batch_id || state.batch?.batch_id || latest?.batch_id;
        const activeId = state.activeBatchId;
        if (id) {
          const [result, active] = await Promise.all([
            requestJson<NativeBatchView>(`${base}/native-batches/${encodeURIComponent(id)}`),
            activeId && activeId !== id ? requestJson<NativeBatchView>(`${base}/native-batches/${encodeURIComponent(activeId)}`) : Promise.resolve(null),
          ]);
          if (!this.isCurrent(key, generation) || version !== this.loadVersion) return;
          if (queryVersion === state.queryVersion) {
            state.batch = result;
            if (state.pending?.request_key === result.request_key) state.pending = null;
          }
          if (result.producer_status === "running") state.activeBatchId = result.batch_id;
          else if (state.activeBatchId === result.batch_id) state.activeBatchId = null;
          if (active) state.activeBatchId = active.producer_status === "running" ? active.batch_id : null;
        }
      } catch (error) {
        if (this.isCurrent(key, generation) && version === this.loadVersion) { state.message = errorText(error); this.notice(errorText(error), true); }
      } finally {
        if (state.loadingVersion === version) state.busy = false;
        if (this.isCurrent(key, generation) && version === this.loadVersion) { this.render(); this.schedule(); }
      }
    } else {
      const id = this.topicId; if (!id) return;
      try {
        const [catalog, operations] = await Promise.all([
          requestJson<ChangeList>(this.base(id)), requestJson<ChangeOperation[]>(this.base(id) + "/operations"),
        ]);
        if (!this.isCurrent(key, generation) || version !== this.loadVersion) return;
        this.catalog = catalog; this.operations = operations; this.render(); this.schedule();
      } catch (error) { if (this.isCurrent(key, generation)) this.notice(errorText(error), true); }
    }
  }
  private async readPage(state: WorkspaceState): Promise<{ page: NativeChangePage; reset: boolean }> {
    const params = new URLSearchParams({ filter: state.filter, limit: "32" });
    const cursor = state.cursors.at(-1); if (cursor) params.set("cursor", cursor);
    try { return { page: await requestJson<NativeChangePage>(`${this.native(state)}/native-changes?${params}`), reset: false }; }
    catch (error) {
      if (!(error instanceof ApiError) || !["workspace_list_stale", "workspace_list_cursor_invalid"].includes(error.code || "")) throw error;
      params.delete("cursor"); return { page: await requestJson<NativeChangePage>(`${this.native(state)}/native-changes?${params}`), reset: true };
    }
  }
  private readHistory(state: WorkspaceState): Promise<NativeBatchHistoryPage> {
    const params = new URLSearchParams({ limit: "32" });
    const cursor = state.historyCursors.at(-1); if (cursor) params.set("cursor", cursor);
    return requestJson<NativeBatchHistoryPage>(`${this.native(state)}/native-batches/history?${params}`);
  }
  private async page(direction: number): Promise<void> {
    const state = this.workspace(); if (!state || state.busy || state.sending) return;
    if (direction < 0) { if (state.cursors.length <= 1) return; state.cursors.pop(); }
    else { if (!state.page?.next_cursor) return; state.cursors.push(state.page.next_cursor); }
    await this.refresh();
  }
  private async historyPage(direction: number): Promise<void> {
    const state = this.workspace(); if (!state || state.busy) return;
    if (direction < 0) { if (state.historyCursors.length <= 1) return; state.historyCursors.pop(); }
    else { if (!state.history?.next_cursor) return; state.historyCursors.push(state.history.next_cursor); }
    await this.refresh();
  }
  async bridge(channel: string, data: HostJson): Promise<HostJson> {
    if (!data || typeof data !== "object" || Array.isArray(data) || typeof data.topic_id !== "string") throw new Error(t("需要话题 ID"));
    const { topic_id: id, ...payload } = data;
    let result: HostJson;
    if (channel === "changeList/get") result = await requestJson<HostJson>(this.base(id));
    else if (channel === "changeList/sync") result = await requestJson<HostJson>(this.base(id), { method: "PUT", body: JSON.stringify(payload) });
    else if (channel === "changeList/clear") result = await requestJson<HostJson>(this.base(id) + "/clear", { method: "POST", body: JSON.stringify(payload) });
    else throw new Error(t("变更清单信道无效"));
    if (this.source.kind === "host" && id === this.topicId && channel !== "changeList/get") await this.refresh();
    return result;
  }
  private async clear(): Promise<void> {
    if (this.source.kind !== "host" || !this.catalog || !this.topicId || this.catalog.active_operation_id) return;
    const key = this.key(), generation = this.generation;
    try { await this.bridge("changeList/clear", { topic_id: this.topicId, revision: this.catalog.revision }); }
    catch (error) { if (this.isCurrent(key, generation)) { this.notice(errorText(error), true); await this.refresh(); } }
  }
  private async hostClick(item: ChangeItem): Promise<void> {
    if (!this.topicId || !this.catalog || !this.sessionId) return;
    const key = this.key(), generation = this.generation;
    await requestJson(this.base(this.topicId) + "/click", { method: "POST", body: JSON.stringify({ revision: this.catalog.revision, item_id: item.id, host_session_id: this.sessionId }) });
    if (this.isCurrent(key, generation)) this.notice(t("已通知宿主查看变更"));
  }
  private writable(state: WorkspaceState): boolean {
    return state.agent.session.status === "open" && !["ask", "plan"].includes(String(state.agent.session.permissions.mode))
      && state.grant?.available === true && state.grant.writable && state.grant.tool_names.includes("workspace_change");
  }
  private eligible(item: NativeChangeItem, action: "keep" | "undo"): boolean {
    return !item.active_operation_id && (item.status === "applied" || action === "undo" && item.status === "kept");
  }
  private async invoke(action: "keep" | "undo", itemId: string | null): Promise<void> {
    const state = this.workspace(), key = this.key(), generation = this.generation;
    if (state) {
      if (!state.page || state.stale || state.busy || state.sending || state.pending || !this.writable(state)) return;
      const items = state.page.items.filter(item => (itemId === null || item.proposal_id === itemId) && this.eligible(item, action));
      if (!items.length) return;
      if (!window.confirm(t("确认{action}当前页的{count}项修改？", { action: actionText(action), count: items.length }) + `\n${state.agent.name}\n${items.map(item => item.path).join("\n")}`)) return;
      const request: NativeBatchRequest = { action, snapshot_revision: state.page.snapshot_revision,
        targets: items.map(item => ({ proposal_id: item.proposal_id, expected_revision: item.revision })), request_key: crypto.randomUUID() };
      state.pending = request; state.query = request.request_key; state.sending = true; state.stale = true; state.message = ""; this.render();
      try {
        state.batch = await requestJson<NativeBatchView>(`${this.native(state)}/native-batches?confirmed=true`, { method: "POST", body: JSON.stringify(request) });
        state.activeBatchId = state.batch.producer_status === "running" ? state.batch.batch_id : null;
        state.pending = null;
      } catch (error) {
        state.message = errorText(error);
        // 只查询原键，接受响应丢失不会再 POST，也不替换原 snapshot/targets。
        try {
          const found = await requestJsonNullable<NativeBatchView>(`${this.native(state)}/native-batches/by-key?${new URLSearchParams({ request_key: request.request_key })}`);
          if (found) { state.batch = found; state.pending = null; state.activeBatchId = found.producer_status === "running" ? found.batch_id : null; }
          else if (error instanceof ApiError && error.status >= 400 && error.status < 500) state.pending = null;
          else state.message = t("未找到原批次，结果尚未确认，请按原键继续查询");
        } catch { state.message = t("批次响应未确认，请按原键找回"); }
        if (this.isCurrent(key, generation)) this.notice(state.message, true);
      } finally {
        state.sending = false;
        // 切到别的scope再切回同一缓存时，旧POST仍是该缓存的真实幂等动作；
        // 必须解除sending并重新读取账本，不能因为旧generation而让按钮永久锁死。
        const ownsState = this.workspaces.get(state.agent.agent_id) === state;
        if (ownsState && this.workspace() === state) {
          state.cursors = [null]; state.historyCursors = [null]; this.render(); await this.refresh();
        }
      }
      return;
    }
    const id = this.topicId, catalog = this.catalog;
    if (!id || !catalog || !this.sessionId || this.pendingTopics.has(id) || catalog.active_operation_id) return;
    this.pendingTopics.add(id); this.render(); this.schedule();
    try {
      const operation = await this.invokeHost({ topic_id: id, revision: catalog.revision, action: itemId === null ? `${action}-all` : action,
        item_id: itemId, idempotency_key: crypto.randomUUID() }) as unknown as ChangeOperation;
      if (this.isCurrent(key, generation)) this.notice(t(operation.status === "done" ? "变更操作已完成" : operation.status === "uncertain" ? "结果待宿主核实" : "部分变更处理失败"), operation.status !== "done");
    } catch (error) { if (this.isCurrent(key, generation)) this.notice(errorText(error), true); }
    finally { this.pendingTopics.delete(id); if (this.isCurrent(key, generation)) await this.refresh(); }
  }
  private async cancel(): Promise<void> {
    const state = this.workspace(), key = this.key(), generation = this.generation;
    try {
      if (state) {
        if (!state.activeBatchId) return;
        const result = await requestJson<NativeBatchView>(`${this.native(state)}/native-batches/${encodeURIComponent(state.activeBatchId)}/cancel`, { method: "POST" });
        state.batch = result; state.queryVersion += 1;
        state.activeBatchId = result.producer_status === "running" ? result.batch_id : null;
      } else {
        if (!this.topicId || !this.catalog?.active_operation_id) return;
        await requestJson(this.base(this.topicId) + `/operations/${encodeURIComponent(this.catalog.active_operation_id)}/cancel`, { method: "POST" });
      }
      if (this.isCurrent(key, generation)) await this.refresh(true);
    } catch (error) { if (this.isCurrent(key, generation)) this.notice(errorText(error), true); }
  }
  private async query(kind: "id" | "key" | "wait", id?: string): Promise<void> {
    const state = this.workspace(); if (!state) return;
    const value = id || (kind === "wait" ? state.batch?.batch_id : state.query); if (!value) return;
    const key = this.key(), generation = this.generation, version = ++state.queryVersion;
    try {
      const base = `${this.native(state)}/native-batches`;
      const result = kind === "key"
        ? await requestJsonNullable<NativeBatchView>(`${base}/by-key?${new URLSearchParams({ request_key: value })}`)
        : await requestJson<NativeBatchView>(`${base}/${encodeURIComponent(value)}${kind === "wait" ? "/wait?timeout_seconds=2" : ""}`, kind === "wait" ? { method: "POST" } : undefined);
      if (!this.isCurrent(key, generation) || version !== state.queryVersion) return;
      if (result) {
        state.batch = result;
        if (result.producer_status === "running") state.activeBatchId = result.batch_id;
        else if (result.batch_id === state.activeBatchId) state.activeBatchId = null;
        if (state.pending?.request_key === result.request_key) state.pending = null;
        state.message = "";
      } else state.message = t("未找到原批次，结果尚未确认，请按原键继续查询");
      this.render(); this.schedule();
    } catch (error) { if (this.isCurrent(key, generation) && version === state.queryVersion) { state.message = errorText(error); this.render(); this.notice(errorText(error), true); } }
  }
  private stopTimer(): void { window.clearTimeout(this.timer); this.timer = undefined; }
  private schedule(): void {
    this.stopTimer();
    if (!(node("change-panel") as HTMLDetailsElement).open) return;
    const state = this.workspace();
    const active = state ? Boolean(state.activeBatchId) : Boolean(this.operations.some(item => item.execution.operation_id === this.catalog?.active_operation_id && item.status === "executing") || this.topicId && this.pendingTopics.has(this.topicId));
    if (!active) return;
    const key = this.key(), generation = this.generation;
    this.timer = window.setTimeout(() => { if (this.isCurrent(key, generation)) void this.refresh(); }, 700);
  }
  private row(row: Row, disabled: boolean, key: string, generation: number): HTMLElement {
    const card = document.createElement("article"); card.className = "change-card"; card.dataset.changeId = row.id;
    const title = document.createElement("button"); title.type = "button"; title.className = "secondary change-title"; title.textContent = row.title;
    title.disabled = this.source.kind === "host" && !this.sessionId;
    title.onclick = () => { if (this.isCurrent(key, generation)) void row.click().catch(error => { if (this.isCurrent(key, generation)) this.notice(errorText(error), true); }); };
    const description = document.createElement("p"); description.textContent = row.description;
    const status = document.createElement("span"); status.className = "badge";
    status.textContent = row.status === "uncertain" && this.source.kind === "host" ? t("结果待宿主核实") : statusText(row.status);
    status.dataset.changeStatus = row.status;
    const actions = document.createElement("div"); actions.className = "actions";
    for (const action of ["undo", "keep"] as const) {
      const control = document.createElement("button"); control.type = "button"; control.className = "secondary"; control.textContent = actionText(action);
      control.disabled = disabled || !row[action];
      control.onclick = () => { if (this.isCurrent(key, generation)) void this.invoke(action, row.id); }; actions.append(control);
    }
    card.append(title, description, status, actions); return card;
  }
  private render(): void {
    const state = this.workspace(), native = Boolean(state), key = this.key(), generation = this.generation;
    const root = node("change-items"); root.replaceChildren();
    node("change-native-pages").hidden = node("change-native-history").hidden = !native;
    node("change-clear").hidden = native;
    for (const [id, label] of [["change-undo-all", native ? "撤销本页" : "全部撤销"], ["change-keep-all", native ? "保留本页" : "全部保留"]]) {
      node(id!).dataset.i18n = label!; node(id!).textContent = t(label!);
    }
    let rows: Row[], busy: boolean;
    if (state) {
      busy = state.busy || state.sending || state.pending !== null || state.stale || !this.writable(state) || Boolean(state.activeBatchId);
      rows = (state.page?.items || []).map(item => ({ id: item.proposal_id, title: item.path,
        description: `${t("修订版本")} ${item.revision} · ${item.order_known ? `${t("文件效果顺序")} ${item.apply_sequence}` : t("历史顺序未知")}`,
        status: item.status, undo: this.eligible(item, "undo"), keep: this.eligible(item, "keep"),
        click: () => this.openWorkspaceProposal(state.agent.agent_id, item.proposal_id, item.active_operation_id || undefined) }));
      (node("change-filter") as HTMLSelectElement).value = state.filter;
      (node("change-filter") as HTMLSelectElement).disabled = state.busy || state.sending;
      button("change-prev").disabled = state.busy || state.sending || state.cursors.length <= 1;
      button("change-next").disabled = state.busy || state.sending || !state.page?.next_cursor;
      node("change-page-info").textContent = t("第{page}页 · 本页{count}项 · 共{total}项", { page: state.cursors.length, count: state.page?.items.length || 0, total: state.page?.total || 0 });
      node("change-snapshot").textContent = state.page ? `${t("清单快照")} ${state.page.snapshot_revision}` : "";
      node("change-status").textContent = state.message || (state.pending ? t("批次响应未确认，请按原键找回") : state.busy ? t("正在读取工作区") : !this.writable(state) ? t("当前会话只能查询修改历史") : !rows.length ? t("暂无变更") : "");
      button("change-cancel").disabled = !state.activeBatchId;
      (node("change-batch-query") as HTMLInputElement).value = state.query;
      this.renderNativeResults(state);
    } else {
      busy = Boolean(this.catalog?.active_operation_id || this.topicId && this.pendingTopics.has(this.topicId)) || !this.sessionId;
      rows = (this.catalog?.items || []).map(item => ({ ...item, undo: item.status === "pending", keep: item.status === "pending", click: () => this.hostClick(item) }));
      const active = this.operations.find(op => op.execution.operation_id === this.catalog?.active_operation_id);
      node("change-status").textContent = active?.status === "uncertain" ? t("结果待宿主核实") : this.catalog?.active_operation_id || this.topicId && this.pendingTopics.has(this.topicId) ? t("变更处理中") : !this.sessionId ? t("连接宿主后可撤销或保留变更") : !rows.length ? t("暂无变更") : "";
      button("change-clear").disabled = Boolean(this.catalog?.active_operation_id || this.topicId && this.pendingTopics.has(this.topicId)) || !rows.length;
      button("change-cancel").disabled = active?.status !== "executing";
      const history = node("change-results"); history.replaceChildren();
      for (const operation of this.operations.slice(0, 5)) {
        const line = document.createElement("p"); line.className = "muted";
        const label = operation.status === "done" ? "变更操作已完成" : operation.status === "error" ? "部分变更处理失败" : operation.status === "uncertain" ? "结果待宿主核实" : "变更处理中";
        const failed = operation.receipt?.results.filter(result => result.status !== "done").map(result => result.item_id) || [];
        line.textContent = t(label) + (failed.length ? ` · ${failed.join(", ")}` : ""); history.append(line);
      }
    }
    for (const row of rows) root.append(this.row(row, busy, key, generation));
    button("change-undo-all").disabled = busy || !rows.some(row => row.undo);
    button("change-keep-all").disabled = busy || !rows.some(row => row.keep);
    button("change-refresh").disabled = state ? state.busy || state.sending : !this.topicId;
    this.syncQueryButtons();
  }
  private syncQueryButtons(): void {
    const state = this.workspace();
    button("change-query-id").disabled = button("change-query-key").disabled = !state?.query;
    button("change-wait").disabled = !state?.batch || state.batch.producer_status !== "running";
  }
  private renderNativeResults(state: WorkspaceState): void {
    const results = node("change-results"); results.replaceChildren();
    const key = this.key(), generation = this.generation, batch = state.batch;
    if (batch) {
      const heading = document.createElement("p"); heading.className = "native-batch-result"; heading.dataset.batchId = batch.batch_id;
      heading.textContent = `${statusText(batch.status)} · ${batch.batch_id} · ${batch.request_key}${batch.stop_reason ? ` · ${batch.stop_reason}` : ""}`;
      results.append(heading);
      for (const item of batch.items) {
        const line = document.createElement("p"); line.dataset.batchItem = item.proposal_id;
        line.textContent = `${item.path} · ${statusText(item.status)}${item.error_code ? ` · ${item.error_code}` : ""} · ${item.operation?.operation_id || t("尚无关联文件操作")}`;
        const open = document.createElement("button"); open.type = "button"; open.textContent = t("查看文件差异与证明");
        open.onclick = () => { if (this.isCurrent(key, generation)) void this.openWorkspaceProposal(state.agent.agent_id, item.proposal_id, item.operation?.operation_id).catch(error => { if (this.isCurrent(key, generation)) this.notice(errorText(error), true); }); };
        line.append(open); results.append(line);
      }
    }
    const history = node("change-batches"); history.replaceChildren();
    for (const summary of state.history?.items || []) {
      const control = document.createElement("button"); control.type = "button"; control.className = "secondary"; control.dataset.batchId = summary.batch_id;
      control.textContent = `${actionText(summary.action)} · ${statusText(summary.status)} · ${summary.done_count}/${summary.item_count} · ${summary.batch_id} · ${localDate(summary.created_at)}`;
      control.onclick = () => { if (this.isCurrent(key, generation)) void this.query("id", summary.batch_id); }; history.append(control);
    }
    node("change-history-info").textContent = t("批次历史共{total}项", { total: state.history?.total || 0 });
    button("change-history-prev").disabled = state.busy || state.historyCursors.length <= 1;
    button("change-history-next").disabled = state.busy || !state.history?.next_cursor;
  }
}
