/** 双完整树界面消费原REST事实；意图先保存，恢复仅GET原键/ID。 */
import {
  errorText, requestJson, type CompletedTreeOriginView, type CompletedTreeOriginLookupView, type TreeExchangeAction, type TreeExchangeAttemptView,
  type TreeExchangeInspectView, type TreeExchangePreviewRequest, type TreeExchangePreviewView,
  type TreeExchangeQueryKind, type TreeExchangeQueryResult, type WorkspaceAction, type WorkspaceView,
} from "./api.js";
import { currentLocale, localDate, t } from "./i18n.js";

const ID = /^[A-Za-z0-9][A-Za-z0-9_-]{0,35}$/, SHA = /^[0-9a-f]{64}$/, PAGE = 20;
const states: Record<string, string> = {
  proposed: "双树交换待确认", applying: "双树交换执行中", applied: "双树已交换", undoing: "树撤销执行中",
  undone: "双树已撤销交换", keeping: "树保留核实中", kept: "双树交换已保留", cancelled: "双树交换已取消",
  rejected: "树操作已拒绝", uncertain: "树结果待核实", running: "树操作执行中", done: "树操作已确认",
};
const actionNames: Record<WorkspaceAction, string> = { apply: "应用双树交换", undo: "撤销双树交换", keep: "保留双树交换" };
type Pending = { kind: "seal"; plan: string; fingerprint: string } | { kind: "preview"; key: string; digest: string } | { kind: "action"; body: TreeExchangeAction } | { kind: "cancel"; id: string };
type Recovery = { version: 1; selected?: string; pending?: Pending };
type Control = { tool?: string; ready: () => boolean };
function text<K extends keyof HTMLElementTagNameMap>(tag: K, key: string): HTMLElementTagNameMap[K] {
  const node = document.createElement(tag); node.dataset.i18n = key; node.textContent = t(key); return node;
}
function relative(value: unknown): value is string {
  return typeof value === "string" && Boolean(value) && new TextEncoder().encode(value).length <= 512
    && !/[\\:\x00-\x1f\x7f]/.test(value) && value.split("/").length <= 16
    && value.split("/").every(part => part !== "" && part !== "." && part !== "..");
}
function paths(source: string, target: string): boolean {
  return relative(source) && relative(target) && source !== target && !target.startsWith(source + "/") && !source.startsWith(target + "/");
}
function key(value: unknown): value is string { return typeof value === "string" && value.length > 0 && value.length <= 128 && !/[\x00-\x1f\x7f]/.test(value); }
function keys(value: unknown, allowed: string[]): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value) && Object.keys(value).every(name => allowed.includes(name));
}
function integer(value: unknown, maximum = Number.MAX_SAFE_INTEGER): value is number { return typeof value === "number" && Number.isSafeInteger(value) && value >= 0 && value <= maximum; }
async function digest(value: unknown): Promise<string> {
  const result = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(JSON.stringify(value)));
  return Array.from(new Uint8Array(result), byte => byte.toString(16).padStart(2, "0")).join("");
}

export class TreeExchangesUI {
  readonly root = document.createElement("details");
  private editor = document.createElement("fieldset");
  private source = document.createElement("input");
  private target = document.createElement("input");
  private plan = document.createElement("input");
  private originSummary = document.createElement("p");
  private sealed: CompletedTreeOriginView | undefined;
  private rightInspection: TreeExchangeInspectView | undefined;
  private lookup = document.createElement("input");
  private summary = document.createElement("p");
  private notice = document.createElement("p");
  private pendingBox = document.createElement("div");
  private list = document.createElement("ol");
  private detail = document.createElement("div");
  private buttons = new Map<HTMLButtonElement, Control>();
  private inspection: TreeExchangeInspectView | undefined;
  private grant: WorkspaceView | undefined;
  private proposal: TreeExchangePreviewView | undefined;
  private operation: TreeExchangeAttemptView | undefined;
  private items: TreeExchangePreviewView[] = [];
  private history: TreeExchangeAttemptView[] = [];
  private cache: Recovery = { version: 1 };
  private cacheError = false;
  private missing = false;
  private bound = false;
  private available = false;
  private mode = "agent";
  private busy = false;
  private loaded = false;
  private disposed = false;
  private stale = false;
  private locale = currentLocale();
  private after: string | null = null;
  private next: string | null = null;
  private previous: Array<string | null> = [];
  private historyAfter: string | null = null;
  private historyNext: string | null = null;
  private historyPrevious: Array<string | null> = [];
  private stop = document.createElement("button");
  private stopping = false;
  private base: string;
  private storageKey: string;

  constructor(private agentId: string, private report: (message: string, error?: boolean) => void) {
    this.base = `/v1/agents/${encodeURIComponent(agentId)}/workspace`;
    this.storageKey = `krim-tree-exchange-recovery:${agentId}`;
    this.root.className = "tree-exchanges"; this.root.hidden = true;
    this.root.append(text("summary", "双完整目录树交换与历史"));
    this.editor.className = "tree-exchange-editor";
    this.editor.append(text("legend", "准备双完整树交换"));
    this.field("左目录树相对路径", this.source, this.editor);
    this.field("右目录树相对路径", this.target, this.editor);
    this.field("原生成计划标识（可选）", this.plan, this.editor); this.plan.maxLength = 36;
    this.source.maxLength = 512; this.target.maxLength = 512;
    this.summary.className = "tree-exchange-summary"; this.originSummary.className = "tree-exchange-origin"; this.editor.append(this.summary, this.originSummary);
    const preparation = document.createElement("div"); preparation.className = "actions";
    preparation.append(
      this.button("检查左完整目录树", () => this.inspect("left"), "tree_exchange_inspect", () => relative(this.source.value)),
      this.button("检查右完整目录树", () => this.inspect("right"), "tree_exchange_inspect", () => relative(this.target.value)),
      this.button("查询原生成树封存", () => this.readOrigin(), "tree_exchange_seal_query", () => ID.test(this.plan.value) && !this.cache.pending),
      this.button("封存原生成完整树", () => this.seal(), "tree_exchange_seal", () => ID.test(this.plan.value) && this.validRight() && !this.cache.pending && !this.cacheError),
      this.button("预览双树交换", () => this.preview(), "tree_exchange_preview", () => this.valid() && !this.cache.pending && !this.cacheError),
    );
    this.editor.append(preparation);
    this.notice.className = "tree-exchange-status"; this.notice.setAttribute("role", "status"); this.notice.setAttribute("aria-live", "polite");
    const navigation = document.createElement("div"); navigation.className = "actions";
    this.lookup.maxLength = 128; this.field("双树交换原请求键", this.lookup, navigation);
    navigation.append(
      this.button("按键查看树提案", () => this.find("by_key"), "tree_exchange_query", () => key(this.lookup.value) && !this.cache.pending),
      this.button("按键查看树操作", () => this.find("operation_by_key"), "tree_exchange_query", () => key(this.lookup.value) && !this.cache.pending),
      this.button("刷新双树交换历史", () => this.refresh(), "tree_exchange_query"),
      this.button("上一页树提案", () => this.page(-1), "tree_exchange_query", () => this.previous.length > 0),
      this.button("下一页树提案", () => this.page(1), "tree_exchange_query", () => this.next !== null),
    );
    this.pendingBox.className = "tree-exchange-pending"; this.list.className = "tree-exchange-list"; this.detail.className = "tree-exchange-detail";
    this.stop = text("button", "取消进行中的双树交换"); this.stop.type = "button"; this.stop.className = "secondary";
    this.stop.onclick = () => { if (!this.stop.disabled) void this.interrupt(); };
    this.root.append(this.editor, this.notice, this.pendingBox, this.stop, navigation, this.list, this.detail);
    this.readCache();
    this.source.oninput = () => { this.inspection = undefined; this.rightInspection = undefined; this.renderSummary(); this.sync(); };
    this.target.oninput = () => { this.rightInspection = undefined; if (this.sealed?.root_path !== this.target.value) this.sealed = undefined; this.renderSummary(); this.sync(); };
    this.plan.oninput = () => { this.sealed = undefined; this.renderSummary(); this.sync(); }; this.lookup.oninput = () => this.sync();
    this.root.ontoggle = () => { if (this.root.open && this.bound && !this.loaded && !this.busy) void this.work(() => this.refresh()); };
    this.renderPending(); this.sync();
  }

  dispose(): void { this.disposed = true; this.sync(); }
  update(bound: boolean, available: boolean, mode: unknown): void {
    this.bound = bound; this.available = available; this.mode = typeof mode === "string" ? mode : "agent";
    this.root.hidden = !bound;
    if (this.locale !== currentLocale()) { this.locale = currentLocale(); this.renderSummary(); this.renderList(); this.renderDetail(); this.renderPending(); }
    this.sync();
  }
  private field(label: string, input: HTMLInputElement, root: HTMLElement): void {
    const row = document.createElement("label"); row.append(text("span", label), input); root.append(row);
    input.setAttribute("aria-label", t(label)); input.dataset.i18nAriaLabel = label;
  }
  private permitted(tool: string): boolean {
    if (!this.bound || this.disposed || this.mode === "ask" || !this.grant?.tool_names.includes(tool)) return false;
    if (tool === "tree_exchange_query" || tool === "tree_exchange_seal_query") return true;
    if (!this.available || !this.grant.available) return false;
    if (tool === "tree_exchange_inspect") return true;
    return this.grant.writable && this.grant.tool_names.includes("tree_exchange_query") && (["tree_exchange_preview", "tree_exchange_seal"].includes(tool) || this.mode !== "plan");
  }
  private sync(): void {
    this.editor.hidden = !this.permitted("tree_exchange_inspect") && !this.permitted("tree_exchange_preview") && !this.permitted("tree_exchange_seal_query");
    this.editor.disabled = this.busy || this.disposed;
    this.source.disabled = !this.permitted("tree_exchange_inspect"); this.target.disabled = !this.permitted("tree_exchange_inspect");
    this.plan.disabled = !this.permitted("tree_exchange_seal_query") && !this.permitted("tree_exchange_preview");
    for (const [button, control] of this.buttons) button.disabled = this.busy || this.disposed || !this.bound || Boolean(control.tool && !this.permitted(control.tool)) || !control.ready();
    this.stop.hidden = this.cache.pending?.kind !== "action";
    this.stop.disabled = this.stopping || this.cacheError || !this.permitted("tree_exchange_change") || this.cache.pending?.kind !== "action";
    this.root.setAttribute("aria-busy", String(this.busy));
  }
  private button(label: string, action: () => Promise<void>, tool?: string, ready: () => boolean = () => true): HTMLButtonElement {
    const button = text("button", label); button.type = "button"; button.className = "secondary";
    this.buttons.set(button, { tool, ready }); button.onclick = () => { if (!button.disabled) void this.work(action); }; return button;
  }
  private clear(node: HTMLElement): void { for (const button of node.querySelectorAll<HTMLButtonElement>("button")) this.buttons.delete(button); node.replaceChildren(); }
  private async work(action: () => Promise<void>): Promise<void> {
    if (this.busy || this.disposed || !this.bound) return;
    this.busy = true; this.sync();
    try { await action(); }
    catch (error) { if (!this.disposed) { this.stale = true; this.notice.textContent = errorText(error); this.report(errorText(error), true); } }
    finally { this.busy = false; if (!this.disposed) { this.renderList(); this.renderDetail(); this.renderPending(); this.sync(); } }
  }
  private async read<T>(path: string, options?: RequestInit): Promise<T> {
    const result = await requestJson<T>(path, options); if (this.disposed) throw new Error(t("Agent 实例不可用")); return result;
  }
  private verifyProposal(value: TreeExchangePreviewView): void {
    if (!value || !ID.test(value.proposal_id) || value.scope_id !== `session:${this.agentId}` || !paths(value.left_path, value.right_path)
      || !SHA.test(value.left_before_fingerprint) || !SHA.test(value.right_before_fingerprint) || !integer(value.revision)
      || !integer(value.left_entry_count, 16384) || value.left_entry_count === 0 || !integer(value.right_entry_count, 16384) || value.right_entry_count === 0
      || !integer(value.left_byte_size, 134217728) || !integer(value.right_byte_size, 134217728)
      || (value.origin === "existing_trees" ? value.source_plan_id !== null : value.origin !== "completed_tree" || !ID.test(value.source_plan_id || ""))
      || !["proposed", "applying", "applied", "undoing", "undone", "keeping", "kept", "rejected", "uncertain", "cancelled"].includes(value.status)
      || value.workspace_key !== this.grant?.key || value.resource_fingerprint !== this.grant?.fingerprint
      || (value.active_operation_id !== null && !ID.test(value.active_operation_id))) throw new Error(t("双树交换回程与原事实不一致"));
  }
  private verifyOperation(value: TreeExchangeAttemptView): void {
    if (!value || !ID.test(value.operation_id) || !ID.test(value.proposal_id) || value.scope_id !== `session:${this.agentId}`
      || !Object.hasOwn(actionNames, value.action) || !integer(value.expected_revision) || !["running", "done", "rejected", "uncertain"].includes(value.status)
      || typeof value.cancelled !== "boolean" || typeof value.recovery_required !== "boolean") throw new Error(t("双树交换回程与原事实不一致"));
    const receipt = value.receipt;
    if (receipt !== null && (!receipt || receipt.status !== value.status || !paths(receipt.left_path, receipt.right_path)
      || !SHA.test(receipt.left_before_fingerprint) || !SHA.test(receipt.right_before_fingerprint)
      || receipt.left_after_fingerprint !== null && !SHA.test(receipt.left_after_fingerprint)
      || receipt.right_after_fingerprint !== null && !SHA.test(receipt.right_after_fingerprint)
      || typeof receipt.changed !== "boolean" || typeof receipt.recovery_required !== "boolean"
      || receipt.status === "done" && (!receipt.left_after_fingerprint || !receipt.right_after_fingerprint || receipt.error_code !== null || receipt.recovery_required)
      || this.proposal?.proposal_id === value.proposal_id && (receipt.left_path !== this.proposal.left_path || receipt.right_path !== this.proposal.right_path))) throw new Error(t("双树交换回程与原事实不一致"));
  }
  private async query(kind: TreeExchangeQueryKind, options: Record<string, string> = {}): Promise<TreeExchangeQueryResult> {
    const result = await this.read<TreeExchangeQueryResult>(`${this.base}/tree-exchanges/query?${new URLSearchParams({ kind, ...options })}`);
    const expected = { proposal: "proposal", by_key: "proposal", operation: "operation", operation_by_key: "operation", history: "history", proposals: "proposals" }[kind];
    if (!result || result.kind !== kind || ["proposal", "operation", "history", "proposals"].some(field => field !== expected && result[field as "proposal"] != null)) throw new Error(t("双树交换回程与原事实不一致"));
    if (result.proposal) { this.verifyProposal(result.proposal); if (kind === "proposal" && result.proposal.proposal_id !== options.identifier) throw new Error(t("双树交换回程与原事实不一致")); }
    if (result.operation) { this.verifyOperation(result.operation); if (kind === "operation" && result.operation.operation_id !== options.identifier) throw new Error(t("双树交换回程与原事实不一致")); }
    const page = result.history || result.proposals;
    if (["history", "proposals"].includes(kind) && !page) throw new Error(t("双树交换回程与原事实不一致"));
    if (page) {
      if (!Array.isArray(page.items) || page.items.length > PAGE) throw new Error(t("双树交换回程与原事实不一致"));
      const ids: string[] = [];
      for (const item of page.items) {
        if ("operation_id" in item) { this.verifyOperation(item); if (item.proposal_id !== options.identifier) throw new Error(t("双树交换回程与原事实不一致")); ids.push(item.operation_id); }
        else { this.verifyProposal(item); ids.push(item.proposal_id); }
      }
      if (ids.some((id, index) => (index > 0 && id <= ids[index - 1]!) || (options.after && id <= options.after)) || (page.next_after !== null && (ids.length !== PAGE || page.next_after !== ids.at(-1)))) throw new Error(t("双树交换回程与原事实不一致"));
    }
    return result;
  }
  private validRight(): boolean { return relative(this.target.value) && this.rightInspection?.path === this.target.value && this.rightInspection.found && SHA.test(this.rightInspection.fingerprint || ""); }
  private valid(): boolean {
    return Boolean(paths(this.source.value, this.target.value) && this.inspection?.path === this.source.value && this.inspection.found
      && SHA.test(this.inspection.fingerprint || "") && this.validRight() && (!this.plan.value || ID.test(this.plan.value)
      && this.sealed?.plan_id === this.plan.value && this.sealed.root_path === this.target.value && this.sealed.tree_fingerprint === this.rightInspection?.fingerprint));
  }
  private async inspect(side: "left" | "right"): Promise<void> {
    const path = side === "left" ? this.source.value : this.target.value;
    const value = await this.read<TreeExchangeInspectView>(`${this.base}/tree-exchange-state?${new URLSearchParams({ path })}`);
    if (!value || value.path !== path || typeof value.found !== "boolean" || !integer(value.entry_count, 16384) || !integer(value.directory_count, 16384)
      || !integer(value.file_count, 16384) || !integer(value.byte_size, 134217728) || value.entry_count !== value.directory_count + value.file_count
      || (value.found ? !SHA.test(value.fingerprint || "") || value.entry_count === 0 || !integer(value.mode, 511) : value.fingerprint !== null || value.entry_count !== 0 || value.byte_size !== 0 || value.mode !== null)) throw new Error(t("双树交换回程与原事实不一致"));
    if (side === "left") this.inspection = value; else this.rightInspection = value;
    this.renderSummary();
  }
  private renderSummary(): void {
    this.summary.textContent = [this.inspection, this.rightInspection].filter((value): value is TreeExchangeInspectView => Boolean(value)).map(value =>
      !value.found ? `${value.path} · ${t("不存在")}` : `${value.path} · ${t("树摘要：{entries}节点、{files}文件、{bytes}字节", { entries: value.entry_count, files: value.file_count, bytes: value.byte_size })} · ${t("权限模式")} ${value.mode?.toString(8).padStart(4, "0")}\n${value.fingerprint}`).join("\n");
    const origin = this.sealed;
    this.originSummary.textContent = !origin ? "" : `${t("原生成树已封存")} · ${origin.plan_id} · ${origin.root_path}\n${origin.tree_fingerprint} · ${origin.outcome}${origin.accept_failed ? ` · ${t("已明确接受失败来源")}` : ""}`;
  }
  private verifyOrigin(value: CompletedTreeOriginLookupView, plan: string): void {
    const origin = value?.origin;
    if (!value || value.plan_id !== plan || (origin !== null && (!origin || origin.plan_id !== plan || origin.scope_id !== `session:${this.agentId}`
      || origin.workspace_key !== this.grant?.key || origin.resource_fingerprint !== this.grant?.fingerprint || !relative(origin.root_path)
      || !SHA.test(origin.tree_fingerprint) || !SHA.test(origin.proof_hash) || !SHA.test(origin.source_hash) || !SHA.test(origin.snapshot_revision)
      || !ID.test(origin.command_id) || !integer(origin.entry_count, 16384) || origin.entry_count === 0 || !integer(origin.byte_size, 134217728)
      || !["completed", "failed", "output_limit"].includes(origin.outcome) || typeof origin.accept_failed !== "boolean"
      || origin.outcome !== "completed" && !origin.accept_failed))) throw new Error(t("双树交换回程与原事实不一致"));
  }
  private async lookupOrigin(plan: string): Promise<CompletedTreeOriginLookupView> {
    const value = await this.read<CompletedTreeOriginLookupView>(`${this.base}/tree-exchanges/seals/${encodeURIComponent(plan)}`);
    this.verifyOrigin(value, plan); return value;
  }
  private async readOrigin(): Promise<void> {
    const value = await this.lookupOrigin(this.plan.value); this.sealed = value.origin || undefined;
    if (!value.origin) this.notice.textContent = t("未找到原生成树封存"); this.renderSummary();
  }
  private async seal(): Promise<void> {
    if (!ID.test(this.plan.value) || !this.validRight() || this.cache.pending || this.cacheError) return;
    const plan = this.plan.value, fingerprint = this.rightInspection!.fingerprint!;
    if (!window.confirm(`${t("封存原生成完整树")} · ${plan} · ${this.target.value}\n${fingerprint}`)) return;
    this.cache.pending = { kind: "seal", plan, fingerprint }; this.missing = false; this.persist(); this.renderPending(); this.sync();
    const origin = await this.read<CompletedTreeOriginView>(`${this.base}/tree-exchanges/seal?confirmed=true`, { method: "POST", body: JSON.stringify({ plan_id: plan, expected_fingerprint: fingerprint }) });
    this.verifyOrigin({ plan_id: plan, origin }, plan);
    if (origin.tree_fingerprint !== fingerprint || origin.root_path !== this.target.value) throw new Error(t("双树交换回程与原事实不一致"));
    this.sealed = origin; this.cache.pending = undefined; this.persist(); this.renderSummary(); this.notice.textContent = t("原生成树已封存");
  }
  private condition(value?: TreeExchangePreviewView): Omit<TreeExchangePreviewRequest, "request_key"> {
    const result: Omit<TreeExchangePreviewRequest, "request_key"> = { left_path: value?.left_path ?? this.source.value, right_path: value?.right_path ?? this.target.value,
      expected_left: value?.left_before_fingerprint ?? this.inspection!.fingerprint!, expected_right: value?.right_before_fingerprint ?? this.rightInspection!.fingerprint!, ttl_seconds: 1800 };
    const plan = value ? value.source_plan_id : this.plan.value; if (plan) result.source_plan_id = plan; return result;
  }
  private async preview(): Promise<void> {
    if (!this.valid() || this.cache.pending || this.cacheError) return;
    const condition = this.condition();
    const request: TreeExchangePreviewRequest = { ...condition, request_key: crypto.randomUUID() };
    this.cache.pending = { kind: "preview", key: request.request_key, digest: await digest(condition) }; this.missing = false; this.persist();
    const proposal = await this.read<TreeExchangePreviewView>(`${this.base}/tree-exchanges/preview?confirmed=true`, { method: "POST", body: JSON.stringify(request) });
    this.verifyProposal(proposal);
    if (proposal.left_path !== request.left_path || proposal.right_path !== request.right_path || proposal.left_before_fingerprint !== request.expected_left || proposal.right_before_fingerprint !== request.expected_right || proposal.source_plan_id !== (request.source_plan_id || null)) throw new Error(t("双树交换回程与原事实不一致"));
    this.cache.pending = undefined; this.select(proposal.proposal_id); this.proposal = proposal; this.persist();
    this.notice.textContent = t("树提案已保存，请审阅后确认"); this.after = null; this.previous = []; await this.refresh();
  }
  private select(id: string): void { this.cache.selected = id; this.proposal = undefined; this.operation = undefined; this.history = []; this.historyAfter = null; this.historyPrevious = []; this.historyNext = null; }
  private async refresh(): Promise<void> {
    this.grant = await this.read<WorkspaceView>(this.base);
    if (!this.permitted("tree_exchange_query")) { this.loaded = true; this.notice.textContent = t("当前会话未授权双树交换查询"); return; }
    if (this.cache.pending) await this.recover();
    const options: Record<string, string> = { limit: String(PAGE) }; if (this.after) options.after = this.after;
    const page = await this.query("proposals", options); this.items = page.proposals!.items; this.next = page.proposals!.next_after;
    await this.selected(); this.loaded = true; this.stale = false;
  }
  private async selected(): Promise<void> {
    if (!this.cache.selected) return;
    const result = await this.query("proposal", { identifier: this.cache.selected });
    if (!result.proposal) throw new Error(t("双树交换回程与原事实不一致")); this.proposal = result.proposal;
    await this.readHistory();
    if (this.proposal.active_operation_id) { const active = await this.query("operation", { identifier: this.proposal.active_operation_id }); this.operation = active.operation || undefined; }
  }
  private async readHistory(): Promise<void> {
    if (!this.proposal) return;
    const options: Record<string, string> = { identifier: this.proposal.proposal_id, limit: String(PAGE) }; if (this.historyAfter) options.after = this.historyAfter;
    const result = await this.query("history", options); this.history = result.history!.items; this.historyNext = result.history!.next_after;
  }
  private async page(direction: number): Promise<void> {
    if (direction > 0) { if (!this.next) return; this.previous.push(this.after); this.after = this.next; }
    else { if (!this.previous.length) return; this.after = this.previous.pop() || null; }
    await this.refresh();
  }
  private async historyPage(direction: number): Promise<void> {
    if (direction > 0) { if (!this.historyNext) return; this.historyPrevious.push(this.historyAfter); this.historyAfter = this.historyNext; }
    else { if (!this.historyPrevious.length) return; this.historyAfter = this.historyPrevious.pop() || null; }
    await this.readHistory();
  }
  private async find(kind: "by_key" | "operation_by_key"): Promise<void> {
    const result = await this.query(kind, { request_key: this.lookup.value });
    const id = result.proposal?.proposal_id || result.operation?.proposal_id;
    if (!id) { this.notice.textContent = t("未找到原树请求，请保留原键继续查询"); return; }
    this.select(id); this.persist(); await this.selected(); if (result.operation) this.operation = result.operation;
  }
  private async change(action: WorkspaceAction): Promise<void> {
    const proposal = this.proposal; if (!proposal || this.cache.pending || this.cacheError || this.stale) return;
    if (!window.confirm(`${t(actionNames[action])} · ${proposal.left_path} → ${proposal.right_path} · r${proposal.revision}\n${t("确认按原树条件执行此操作？")}`)) return;
    const request: TreeExchangeAction = { proposal_id: proposal.proposal_id, action, expected_revision: proposal.revision, request_key: crypto.randomUUID() };
    this.cache.pending = { kind: "action", body: request }; this.missing = false; this.persist(); this.renderPending(); this.sync();
    const value = await this.read<TreeExchangeAttemptView>(`${this.base}/tree-exchanges/change?confirmed=true`, { method: "POST", body: JSON.stringify(request) });
    this.verifyOperation(value);
    if (value.proposal_id !== request.proposal_id || value.action !== request.action || value.expected_revision !== request.expected_revision) throw new Error(t("双树交换回程与原事实不一致"));
    this.inspection = undefined; this.rightInspection = undefined; this.renderSummary();
    this.cache.pending = undefined; this.operation = value; this.persist(); await this.refresh();
  }
  private async cancel(): Promise<void> {
    if (!this.proposal || !window.confirm(t("确认取消原双树交换？"))) return;
    const id = this.proposal.proposal_id;
    this.cache.pending = { kind: "cancel", id }; this.missing = false; this.persist();
    const value = await this.read<TreeExchangePreviewView>(`${this.base}/tree-exchanges/${encodeURIComponent(id)}/cancel`, { method: "POST" });
    this.verifyProposal(value); if (value.proposal_id !== id) throw new Error(t("双树交换回程与原事实不一致"));
    this.cache.pending = undefined; this.proposal = value; this.persist(); await this.refresh();
  }
  private async interrupt(): Promise<void> {
    const pending = this.cache.pending;
    if (pending?.kind !== "action" || this.stopping || !window.confirm(t("确认取消原双树交换？"))) return;
    this.stopping = true; this.sync();
    try {
      const id = pending.body.proposal_id;
      const value = await this.read<TreeExchangePreviewView>(`${this.base}/tree-exchanges/${encodeURIComponent(id)}/cancel`, { method: "POST" });
      this.verifyProposal(value); if (value.proposal_id !== id) throw new Error(t("双树交换回程与原事实不一致"));
      // 保留正在等待的原动作键；原POST或之后的GET负责读回真实回执。
      this.notice.textContent = t("原树取消已收束，请读取原操作");
      if (!this.busy) await this.work(() => this.refresh());
    } catch (error) { if (!this.disposed) { this.notice.textContent = errorText(error); this.report(errorText(error), true); } }
    finally { this.stopping = false; if (!this.disposed) this.sync(); }
  }
  private async reconcile(): Promise<void> {
    const operation = this.operation; if (!operation || !window.confirm(t("确认核实原树结果？"))) return;
    const value = await this.read<TreeExchangeAttemptView>(`${this.base}/tree-exchange-operations/${encodeURIComponent(operation.operation_id)}/reconcile?confirmed=true`, { method: "POST" });
    this.verifyOperation(value); if (value.operation_id !== operation.operation_id || value.proposal_id !== operation.proposal_id) throw new Error(t("双树交换回程与原事实不一致"));
    this.operation = value; await this.refresh();
  }
  private async recover(): Promise<void> {
    const pending = this.cache.pending; if (!pending) return;
    if (pending.kind === "seal") {
      if (!this.permitted("tree_exchange_seal_query")) throw new Error(t("当前会话未授权原生成树封存查询"));
      const value = await this.lookupOrigin(pending.plan);
      if (!value.origin) { this.missing = true; return; }
      if (value.origin.tree_fingerprint !== pending.fingerprint) throw new Error(t("双树交换回程与原事实不一致"));
      this.plan.value = pending.plan; this.target.value = value.origin.root_path; this.sealed = value.origin;
      this.cache.pending = undefined; this.renderSummary();
    } else if (pending.kind === "preview") {
      const result = await this.query("by_key", { request_key: pending.key });
      if (!result.proposal) { this.missing = true; return; }
      const proposal = result.proposal;
      if (await digest(this.condition(proposal)) !== pending.digest) throw new Error(t("双树交换回程与原事实不一致"));
      this.cache.pending = undefined; this.select(proposal.proposal_id); this.proposal = proposal;
    } else if (pending.kind === "action") {
      const result = await this.query("operation_by_key", { request_key: pending.body.request_key }); const operation = result.operation;
      if (!operation) { this.missing = true; return; }
      if (operation.proposal_id !== pending.body.proposal_id || operation.action !== pending.body.action || operation.expected_revision !== pending.body.expected_revision) throw new Error(t("双树交换回程与原事实不一致"));
      if (operation.status === "running") { this.operation = operation; return; }
      this.inspection = undefined; this.rightInspection = undefined; this.renderSummary();
      this.cache.pending = undefined; this.select(operation.proposal_id); this.operation = operation;
    } else {
      const result = await this.query("proposal", { identifier: pending.id });
      if (!result.proposal) { this.missing = true; return; }
      if (result.proposal.active_operation_id) return;
      this.cache.pending = undefined; this.select(pending.id); this.proposal = result.proposal;
    }
    this.missing = false; this.persist();
  }
  private renderList(): void {
    this.clear(this.list);
    if (!this.items.length) { this.list.append(text("li", "暂无双树交换提案")); return; }
    for (const proposal of this.items) {
      const row = document.createElement("li"); row.dataset.proposalId = proposal.proposal_id;
      const button = this.button("查看双树交换提案", async () => { this.select(proposal.proposal_id); this.persist(); await this.selected(); this.stale = false; }, "tree_exchange_query", () => !this.cache.pending);
      button.removeAttribute("data-i18n"); button.textContent = `${proposal.left_path} → ${proposal.right_path} · ${t(states[proposal.status] || proposal.status)} · r${proposal.revision}`;
      row.append(button); this.list.append(row);
    }
  }
  private renderDetail(): void {
    this.clear(this.detail); const proposal = this.proposal; if (!proposal) return;
    const title = document.createElement("strong"); title.textContent = `${proposal.left_path} → ${proposal.right_path} · ${t(states[proposal.status] || proposal.status)} · r${proposal.revision}`;
    const condition = document.createElement("pre"); condition.className = "tree-exchange-condition";
    condition.textContent = `${t("左目录树相对路径")}: ${proposal.left_path}\n${t("原左树指纹")}: ${proposal.left_before_fingerprint}\n${t("树提案规模：{entries}节点、{bytes}字节", { entries: proposal.left_entry_count, bytes: proposal.left_byte_size })}\n${t("右目录树相对路径")}: ${proposal.right_path}\n${t("原右树指纹")}: ${proposal.right_before_fingerprint}\n${t("树提案规模：{entries}节点、{bytes}字节", { entries: proposal.right_entry_count, bytes: proposal.right_byte_size })}\n${t("两棵完整树交换位置")}\n${t("生成来源")}: ${proposal.source_plan_id || t("授权已有树")}\n${t("原操作标识")}: ${proposal.proposal_id}`;
    const actions = document.createElement("div"); actions.className = "actions";
    const writable = () => !this.cache.pending && !this.cacheError && !this.stale;
    for (const action of ["apply", "keep", "undo"] as const) actions.append(this.button(actionNames[action], () => this.change(action), "tree_exchange_change", () => writable() && (action === "apply" ? proposal.status === "proposed" && proposal.expires_at > Date.now() / 1000 : action === "keep" ? proposal.status === "applied" : ["applied", "kept"].includes(proposal.status))));
    actions.append(this.button("取消双树交换", () => this.cancel(), "tree_exchange_change", () => writable() && ["proposed", "applying", "undoing", "keeping"].includes(proposal.status)));
    const operations = document.createElement("ol"); operations.className = "tree-exchange-history";
    for (const operation of this.history) {
      const row = document.createElement("li"); row.dataset.operationId = operation.operation_id;
      const button = this.button("查看原树操作", async () => { const result = await this.query("operation", { identifier: operation.operation_id }); this.operation = result.operation || undefined; }, "tree_exchange_query");
      button.removeAttribute("data-i18n"); button.textContent = `${t(actionNames[operation.action])} · ${t(states[operation.status] || operation.status)} · ${operation.operation_id} · ${localDate(operation.updated_at)}`;
      row.append(button); operations.append(row);
    }
    const pages = document.createElement("div"); pages.className = "actions";
    pages.append(this.button("上一页树操作", () => this.historyPage(-1), "tree_exchange_query", () => this.historyPrevious.length > 0), this.button("下一页树操作", () => this.historyPage(1), "tree_exchange_query", () => this.historyNext !== null));
    this.detail.append(title, condition, actions, operations, pages);
    if (this.operation) {
      const receipt = document.createElement("p"); receipt.className = "tree-exchange-receipt";
      receipt.textContent = `${t("原操作标识")}: ${this.operation.operation_id} · ${t(actionNames[this.operation.action])} · ${t(states[this.operation.status] || this.operation.status)}${this.operation.receipt ? ` · ${t(this.operation.receipt.changed ? "已产生工作区变化" : "未产生工作区变化")}` : ""}${this.operation.cancelled ? ` · ${t("双树交换已取消")}` : ""}${this.operation.receipt?.error_code ? ` · ${this.operation.receipt.error_code}` : ""}`; this.detail.append(receipt);
      this.detail.append(this.button("核实原树结果", () => this.reconcile(), "tree_exchange_reconcile", () => this.operation?.status === "uncertain" && !this.cache.pending && !this.cacheError));
    }
  }
  private renderPending(): void {
    this.clear(this.pendingBox);
    if (this.cacheError) {
      this.pendingBox.append(text("p", "树恢复记录不可用，请先处理本地记录"));
      this.pendingBox.append(this.button("清除本地树恢复记录", () => this.clearCache(), undefined)); return;
    }
    const pending = this.cache.pending; if (!pending) return;
    const message = text("p", "树响应尚未确认，仅查询原请求");
    const original = document.createElement("code"); original.textContent = pending.kind === "seal" ? pending.plan : pending.kind === "preview" ? pending.key : pending.kind === "action" ? pending.body.request_key : pending.id;
    this.pendingBox.append(message, original, this.button("查询待确认树请求", () => this.refresh(), pending.kind === "seal" ? "tree_exchange_seal_query" : "tree_exchange_query"));
    if (this.missing) this.pendingBox.append(this.button("解除本地树等待状态", async () => { if (!window.confirm(t("保留原键；仅清除本地等待，不取消服务端任务。"))) return; this.cache.pending = undefined; this.missing = false; this.persist(); }, "tree_exchange_query"));
  }
  private async clearCache(): Promise<void> {
    if (!window.confirm(t("保留原键；仅清除本地等待，不取消服务端任务。"))) return;
    try { localStorage.removeItem(this.storageKey); this.cache = { version: 1 }; this.cacheError = false; this.proposal = undefined; this.operation = undefined; this.loaded = false; }
    catch { throw new Error(t("树恢复记录不可用，请先处理本地记录")); }
  }
  private persist(): void {
    try { localStorage.setItem(this.storageKey, JSON.stringify(this.cache)); }
    catch { this.cacheError = true; throw new Error(t("树恢复记录不可用，请先处理本地记录")); }
  }
  private readCache(): void {
    try {
      const raw = localStorage.getItem(this.storageKey); if (!raw) return; if (raw.length > 4096) throw new Error();
      const value: unknown = JSON.parse(raw);
      if (!keys(value, ["version", "selected", "pending"]) || value.version !== 1 || (value.selected !== undefined && (typeof value.selected !== "string" || !ID.test(value.selected)))) throw new Error();
      const pending = value.pending;
      if (pending !== undefined) {
        if (!keys(pending, ["kind", "key", "digest", "body", "id", "plan", "fingerprint"])) throw new Error();
        if (pending.kind === "seal") { if (!keys(pending, ["kind", "plan", "fingerprint"]) || typeof pending.plan !== "string" || !ID.test(pending.plan) || typeof pending.fingerprint !== "string" || !SHA.test(pending.fingerprint)) throw new Error(); }
        else if (pending.kind === "preview") { if (!keys(pending, ["kind", "key", "digest"]) || !key(pending.key) || typeof pending.digest !== "string" || !SHA.test(pending.digest)) throw new Error(); }
        else if (pending.kind === "action") {
          const body = pending.body;
          if (!keys(pending, ["kind", "body"]) || !keys(body, ["proposal_id", "action", "expected_revision", "request_key"]) || typeof body.proposal_id !== "string" || !ID.test(body.proposal_id) || !key(body.request_key) || !integer(body.expected_revision) || typeof body.action !== "string" || !Object.hasOwn(actionNames, body.action) || body.proposal_id !== value.selected) throw new Error();
        } else if (pending.kind === "cancel") { if (!keys(pending, ["kind", "id"]) || typeof pending.id !== "string" || !ID.test(pending.id) || pending.id !== value.selected) throw new Error(); }
        else throw new Error();
      }
      this.cache = value as Recovery;
    } catch { this.cacheError = true; }
  }
}
