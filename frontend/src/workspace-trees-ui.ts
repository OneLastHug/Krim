/** 浏览器消费原目录树事实；恢复仅查询，准备与启动必须由用户分别确认。 */
import {
  errorText, requestJson, requestJsonNullable, type AgentView, type CommandDirectoryChangesPage,
  type TreePlanRequest, type TreePlanView, type TreePlanStepPage, type TreePlanStepView, type WorkspaceView,
} from "./api.js";
import { currentLocale, localDate, t } from "./i18n.js";

type Source = Pick<TreePlanRequest, "command_id" | "source_hash" | "root_path"> & { failed: boolean };
type Pending = { kind: "prepare"; key: string; digest: string } | { kind: "start" | "cancel"; id: string };
type Recovery = { version: 1; selected?: { id: string; key: string }; pending?: Pending };
type Control = { enabled: () => boolean };
const IDENTIFIER = /^[A-Za-z0-9][A-Za-z0-9_-]{0,35}$/;
const SHA = /^[0-9a-f]{64}$/;
const labels: Record<TreePlanView["status"], string> = {
  planned: "目录树待启动", running: "目录树执行中", done: "目录树已确认", partial: "目录树部分完成",
  failed: "目录树执行失败", uncertain: "目录树结果待核实", cancelled: "目录树已取消", interrupted: "目录树已中断",
};
const stepLabels: Record<TreePlanStepView["status"], string> = {
  pending: "步骤待执行", running: "步骤执行中", done: "步骤已确认", rejected: "步骤已拒绝",
  uncertain: "步骤结果待核实", skipped: "步骤已跳过",
};
function text<K extends keyof HTMLElementTagNameMap>(tag: K, key: string): HTMLElementTagNameMap[K] {
  const node = document.createElement(tag); node.dataset.i18n = key; node.textContent = t(key); return node;
}
function object(value: unknown): value is Record<string, unknown> { return typeof value === "object" && value !== null && !Array.isArray(value); }
function keys(value: unknown, allowed: string[]): boolean { return object(value) && Object.keys(value).every(key => allowed.includes(key)); }
function identifier(value: unknown): value is string { return typeof value === "string" && IDENTIFIER.test(value); }
function requestKey(value: unknown): value is string { return typeof value === "string" && [...value].length >= 1 && [...value].length <= 128 && !/[\x00-\x1f\x7f]/.test(value); }
function relative(value: unknown): value is string {
  return typeof value === "string" && value.length > 0 && new TextEncoder().encode(value).length <= 512
    && !/[\\:\x00-\x1f]/.test(value) && value.split("/").length <= 16
    && value.split("/").every(part => part !== "" && part !== "." && part !== "..");
}
function integer(value: unknown, low: number, high: number): value is number { return Number.isSafeInteger(value) && (value as number) >= low && (value as number) <= high; }
async function digest(value: TreePlanRequest): Promise<string> {
  const hash = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(JSON.stringify(value)));
  return Array.from(new Uint8Array(hash), byte => byte.toString(16).padStart(2, "0")).join("");
}

export class WorkspaceTreesUI {
  readonly root = document.createElement("details");
  private base: string;
  private storageKey: string;
  private scope = "";
  private permissions: Record<string, unknown> = {};
  private workspace: WorkspaceView | undefined;
  private bound = false;
  private sessionOpen = false;
  private busy = false;
  private disposed = false;
  private loaded = false;
  private cacheError = false;
  private pendingMissing = false;
  private cache: Recovery = { version: 1 };
  private abort = new AbortController();
  private poll: number | undefined;
  private locale = currentLocale();
  private source: Source | undefined;
  private plan: TreePlanView | undefined;
  private steps: TreePlanStepPage | undefined;
  private after: number | null = null;
  private previous: Array<number | null> = [];
  private controls = new Map<HTMLButtonElement, Control>();
  private editor = document.createElement("fieldset");
  private sourceInfo = document.createElement("pre");
  private key = document.createElement("input");
  private limit = document.createElement("input");
  private accept = document.createElement("input");
  private acceptRow: HTMLLabelElement;
  private query = document.createElement("input");
  private noticeBox = document.createElement("p");
  private pendingBox = document.createElement("div");
  private detail = document.createElement("div");

  constructor(agentId: string, private report: (message: string, error?: boolean) => void,
    private openFile: (id: string, operationId?: string) => Promise<void>,
    private openDirectory: (id: string, operationId?: string) => Promise<void>) {
    this.base = `/v1/agents/${encodeURIComponent(agentId)}/workspace/tree-plans`;
    this.storageKey = `krim-tree-recovery:${agentId}`;
    this.root.className = "workspace-trees"; this.root.hidden = true;
    this.root.append(text("summary", "目录树审阅与控制"));
    this.editor.className = "tree-editor"; this.editor.append(text("legend", "准备目录树计划"));
    this.sourceInfo.className = "tree-source"; this.editor.append(this.sourceInfo);
    this.key.maxLength = 128; this.key.spellcheck = false;
    this.limit.type = "number"; this.limit.min = "1"; this.limit.max = "16384"; this.limit.step = "1"; this.limit.value = "1024";
    this.accept.type = "checkbox";
    this.field(this.editor, "目录树原请求键（可留空自动生成）", this.key);
    this.field(this.editor, "目录树步骤容量", this.limit);
    this.acceptRow = this.field(this.editor, "明确接受失败命令的完整目录树成果", this.accept);
    this.editor.append(text("p", "准备只保存计划，请审阅全部步骤后另行确认启动。"));
    this.editor.append(this.button("准备目录树计划", () => this.prepare(), () => this.canPrepare()));
    this.query.maxLength = 128; this.query.spellcheck = false;
    const queries = document.createElement("div"); queries.className = "tree-query";
    this.field(queries, "目录树标识或原请求键", this.query);
    queries.append(this.button("按标识读取目录树", () => this.find(false), () => this.canRead() && identifier(this.query.value)),
      this.button("按原键找回目录树", () => this.find(true), () => this.canRead() && requestKey(this.query.value)),
      this.button("刷新原目录树", () => this.refresh(), () => this.canRead() && Boolean(this.cache.selected || this.cache.pending)));
    this.noticeBox.className = "tree-status"; this.noticeBox.setAttribute("role", "status"); this.noticeBox.setAttribute("aria-live", "polite");
    this.pendingBox.className = "tree-pending"; this.detail.className = "tree-detail";
    this.root.append(this.editor, queries, this.pendingBox, this.noticeBox, this.detail);
    this.readCache(); this.query.value = this.cache.selected?.key || (this.cache.pending?.kind === "prepare" ? this.cache.pending.key : "");
    this.key.value = this.cache.pending?.kind === "prepare" ? this.cache.pending.key : "";
    for (const field of [this.key, this.limit, this.accept, this.query]) field.oninput = () => this.sync();
    this.root.ontoggle = () => {
      if (!this.root.open) this.clearPoll();
      else if (!this.loaded && this.canRead()) void this.work(() => this.refresh());
      else this.schedulePoll();
    };
    this.renderPending(); this.sync();
  }

  update(agent: AgentView, workspace?: WorkspaceView): void {
    this.scope = `session:${agent.session_id}`; this.permissions = agent.session.permissions;
    this.bound = Boolean(this.permissions.workspace && this.permissions.execution);
    this.sessionOpen = agent.session.status === "open"; if (workspace) this.workspace = workspace;
    this.root.hidden = !this.bound;
    if (this.locale !== currentLocale()) { this.locale = currentLocale(); this.renderSource(); this.renderDetail(); this.renderPending(); }
    this.sync();
    if (this.root.open && this.canRead() && !this.loaded && !this.busy) void this.work(() => this.refresh());
  }

  selectSource(page: CommandDirectoryChangesPage, path: string): void {
    if (!page.complete || !identifier(page.command_id) || !SHA.test(page.source_hash) || !relative(path) || !page.entries.some(item => item.path === path))
      throw new Error(t("目录树回程与原来源不一致"));
    this.source = { command_id: page.command_id, source_hash: page.source_hash, root_path: path, failed: page.outcome !== "completed" };
    this.accept.checked = false; this.root.open = true; this.renderSource(); this.sync();
    this.root.scrollIntoView({ block: "nearest" });
  }

  dispose(): void { this.disposed = true; this.clearPoll(); this.abort.abort(); this.sync(); }
  private canRead(): boolean { return this.bound && !this.disposed && this.permissions.mode !== "ask"; }
  private hasTool(name: string): boolean { return Array.isArray(this.permissions.tool_names) && this.permissions.tool_names.includes(name); }
  private canWrite(): boolean { return this.canRead() && this.sessionOpen && this.workspace?.available === true && this.workspace.writable && !this.cacheError; }
  private canPrepare(): boolean {
    return this.canWrite() && this.hasTool("workspace_tree_prepare") && Boolean(this.source)
      && (!this.key.value || requestKey(this.key.value)) && integer(Number(this.limit.value), 1, 16384)
      && (!this.source?.failed || this.accept.checked)
      && (!this.cache.pending || this.cache.pending.kind === "prepare" && this.pendingMissing);
  }
  private canStart(): boolean {
    return this.canWrite() && this.permissions.mode !== "plan" && !this.cache.pending
      && this.plan?.status === "planned" && this.plan.producer_status === "planned"
      && this.plan.workspace_key === this.workspace?.key && this.plan.resource_fingerprint === this.workspace.fingerprint
      && ["directory_source_preview", "directory_change", "command_propose", "workspace_change"].every(name => this.hasTool(name));
  }
  private canCancel(): boolean { return this.canRead() && this.sessionOpen && !this.cacheError && !this.cache.pending && Boolean(this.plan && ["planned", "running"].includes(this.plan.producer_status)); }

  private field(parent: HTMLElement, key: string, input: HTMLInputElement): HTMLLabelElement {
    const row = document.createElement("label"); row.append(text("span", key), input);
    input.setAttribute("aria-label", t(key)); input.setAttribute("data-i18n-aria-label", key); parent.append(row); return row;
  }
  private button(key: string, action: () => Promise<void>, enabled: () => boolean): HTMLButtonElement {
    const node = text("button", key); node.type = "button"; node.className = "secondary";
    node.disabled = this.busy || !enabled(); this.controls.set(node, { enabled });
    node.onclick = () => { if (!node.disabled) void this.work(action); }; return node;
  }
  private clear(node: HTMLElement): void { for (const button of node.querySelectorAll<HTMLButtonElement>("button")) this.controls.delete(button); node.replaceChildren(); }
  private sync(): void {
    this.editor.hidden = !this.source;
    this.editor.disabled = this.busy || !this.canWrite() || !this.hasTool("workspace_tree_prepare");
    this.key.disabled = this.editor.disabled || Boolean(this.cache.pending);
    this.acceptRow.hidden = !this.source?.failed;
    for (const [node, control] of this.controls) node.disabled = this.busy || this.disposed || !control.enabled();
    this.root.setAttribute("aria-busy", String(this.busy));
  }
  private async read<T>(path: string, init: RequestInit = {}): Promise<T> {
    const value = await requestJson<T>(path, { ...init, signal: this.abort.signal });
    if (this.disposed) throw new DOMException("disposed", "AbortError"); return value;
  }
  private async work(action: () => Promise<void>): Promise<void> {
    if (this.busy || this.disposed || !this.canRead()) return;
    this.busy = true; this.clearPoll(); this.sync();
    try { await action(); }
    catch (error) {
      if (!this.disposed) {
        this.plan = undefined; this.steps = undefined; this.renderDetail();
        this.noticeBox.textContent = errorText(error); this.report(errorText(error), true);
      }
    } finally { this.busy = false; if (!this.disposed) { this.renderPending(); this.sync(); this.schedulePoll(); } }
  }

  private assertPlan(value: TreePlanView, expected: { id?: string; key?: string }, previous?: TreePlanView): void {
    const fail = () => { throw new Error(t("目录树回程与原来源不一致")); };
    if (!object(value) || !identifier(value.plan_id) || !identifier(value.command_id) || value.scope_id !== this.scope
      || expected.id !== undefined && value.plan_id !== expected.id || expected.key !== undefined && value.request_key !== expected.key
      || !requestKey(value.request_key) || !relative(value.root_path) || !SHA.test(value.source_hash) || !SHA.test(value.resource_fingerprint)
      || !SHA.test(value.snapshot_revision) || typeof value.workspace_key !== "string"
      || !Object.hasOwn(labels, value.status) || !["planned", "running", "finished", "cancelled", "interrupted"].includes(value.producer_status)
      || !integer(value.step_count, 1, 16384) || !integer(value.done_count, 0, value.step_count)
      || value.stop_reason !== null && typeof value.stop_reason !== "string"
      || !Number.isFinite(Date.parse(value.created_at)) || !Number.isFinite(Date.parse(value.updated_at))) fail();
    if (previous) for (const field of ["plan_id", "scope_id", "command_id", "source_hash", "root_path", "workspace_key", "resource_fingerprint", "request_key", "snapshot_revision", "step_count", "created_at"] as const)
      if (value[field] !== previous[field]) fail();
  }
  private adopt(value: TreePlanView): void {
    const changed = this.plan?.plan_id !== value.plan_id;
    this.plan = value; this.cache.selected = { id: value.plan_id, key: value.request_key };
    if (changed) { this.after = null; this.previous = []; this.steps = undefined; }
    this.persist(); this.renderDetail();
  }
  private async readPlan(id: string): Promise<TreePlanView> {
    const value = await this.read<TreePlanView>(`${this.base}/${encodeURIComponent(id)}`);
    this.assertPlan(value, { id }, this.plan?.plan_id === id ? this.plan : undefined); return value;
  }
  private async find(byKey: boolean): Promise<void> {
    const query = this.query.value;
    if (this.cache.pending) {
      const pending = this.cache.pending;
      if (!(byKey && pending.kind === "prepare" && query === pending.key || !byKey && pending.kind !== "prepare" && query === pending.id))
        throw new Error(t("目录树响应未确认，请先查询原请求"));
    }
    if (byKey) {
      const value = await requestJsonNullable<TreePlanView>(this.base + "/by-key?" + new URLSearchParams({ request_key: query }), { signal: this.abort.signal });
      if (this.disposed) return;
      if (value === null) {
        this.plan = undefined; this.steps = undefined; this.renderDetail();
        this.pendingMissing = this.cache.pending?.kind === "prepare" && this.cache.pending.key === query;
        this.noticeBox.textContent = t("未找到原目录树；重新选择原来源后仅可明确同键同参数重试"); return;
      }
      this.assertPlan(value, { key: query }, this.plan?.plan_id === value.plan_id ? this.plan : undefined);
      this.cache.pending = undefined; this.pendingMissing = false; this.adopt(value);
    } else {
      const value = await this.readPlan(query);
      this.cache.pending = undefined; this.pendingMissing = false; this.adopt(value);
    }
    await this.readSteps(); this.noticeBox.textContent = t("已读取原目录树，未重新执行");
  }
  private async refresh(): Promise<void> {
    const pending = this.cache.pending;
    if (pending) {
      this.query.value = pending.kind === "prepare" ? pending.key : pending.id; await this.find(pending.kind === "prepare");
    } else if (this.cache.selected) {
      const value = await this.readPlan(this.cache.selected.id); this.assertPlan(value, { key: this.cache.selected.key });
      this.adopt(value); await this.readSteps(); this.noticeBox.textContent = t("已读取原目录树，未重新执行");
    }
    this.loaded = true;
  }

  private async prepare(): Promise<void> {
    if (!this.canPrepare() || !this.source) return;
    const key = this.cache.pending?.kind === "prepare" ? this.cache.pending.key : this.key.value || crypto.randomUUID();
    const body: TreePlanRequest = { command_id: this.source.command_id, source_hash: this.source.source_hash,
      root_path: this.source.root_path, request_key: key, accept_failed: this.source.failed && this.accept.checked, max_entries: Number(this.limit.value) };
    const hash = await digest(body);
    if (this.cache.pending?.kind === "prepare" && this.cache.pending.digest !== hash) throw new Error(t("未确认目录树只能使用原参数与原请求键重试"));
    if (!window.confirm(t("确认保存以下目录树计划？准备后仍需另行确认启动。") + "\n" + JSON.stringify(body, null, 2))) return;
    this.cache.pending = { kind: "prepare", key, digest: hash }; this.pendingMissing = false; this.key.value = key; this.query.value = key;
    this.persist(true); this.renderPending(); this.sync();
    const value = await this.read<TreePlanView>(this.base + "?confirmed=true", { method: "POST", body: JSON.stringify(body) });
    this.assertPlan(value, { key });
    if (value.command_id !== body.command_id || value.source_hash !== body.source_hash || value.root_path !== body.root_path
      || value.workspace_key !== this.workspace?.key || value.resource_fingerprint !== this.workspace.fingerprint) throw new Error(t("目录树回程与原来源不一致"));
    this.cache.pending = undefined; this.key.value = ""; this.adopt(value); await this.readSteps(); this.loaded = true;
    this.noticeBox.textContent = t("目录树计划已保存，请审阅步骤后确认启动");
  }
  private async start(): Promise<void> {
    if (!this.canStart() || !this.plan) return;
    this.adopt(await this.readPlan(this.plan.plan_id));
    if (!this.canStart()) return;
    const value = this.plan!;
    if (!window.confirm(t("确认发布目录树：{path}（共{count}步）？各项独立执行，可能部分成功。", { path: value.root_path, count: value.step_count }) + "\n" + value.plan_id + "\n" + value.request_key)) return;
    await this.control("start");
  }
  private async cancel(): Promise<void> {
    if (!this.canCancel() || !this.plan) return;
    if (!window.confirm(t("确认取消原目录树：{path}？已完成项保留原回执。", { path: this.plan.root_path }))) return;
    await this.control("cancel");
  }
  private async control(kind: "start" | "cancel"): Promise<void> {
    const original = this.plan!;
    this.cache.pending = { kind, id: original.plan_id }; this.persist(true); this.renderPending(); this.sync();
    const value = await this.read<TreePlanView>(`${this.base}/${encodeURIComponent(original.plan_id)}/${kind}${kind === "start" ? "?confirmed=true" : ""}`, { method: "POST" });
    this.assertPlan(value, { id: original.plan_id }, original);
    this.cache.pending = undefined; this.adopt(value); await this.readSteps();
  }
  private async wait(): Promise<void> {
    const original = this.plan; if (!original) return;
    const value = await this.read<TreePlanView>(`${this.base}/${encodeURIComponent(original.plan_id)}/wait?timeout_seconds=1`, { method: "POST" });
    this.assertPlan(value, { id: original.plan_id }, original); this.adopt(value); await this.readSteps();
  }

  private assertSteps(value: TreePlanStepPage): void {
    const plan = this.plan!, start = (this.after ?? -1) + 1;
    const fail = () => { throw new Error(t("目录树步骤回程与原计划不一致")); };
    if (!object(value) || value.plan_id !== plan.plan_id || value.scope_id !== this.scope || !Array.isArray(value.items)
      || value.items.length !== Math.min(32, plan.step_count - start)
      || value.next_after !== (start + value.items.length < plan.step_count ? start + value.items.length - 1 : null)) fail();
    for (const [index, item] of value.items.entries()) {
      if (!object(item) || item.position !== start + index || !relative(item.path) || !["directory", "file"].includes(item.source)
        || !Object.hasOwn(stepLabels, item.status) || !integer(item.mode, 0, 511)
        || item.position === 0 && (item.path !== plan.root_path || item.source !== "directory" || item.parent_position !== null)
        || item.position > 0 && (!integer(item.parent_position, 0, item.position - 1) || !item.path.startsWith(plan.root_path + "/"))
        || item.proposal_id !== null && !identifier(item.proposal_id)
        || item.error_code !== null && typeof item.error_code !== "string") fail();
      if (item.source === "directory" ? item.sha256 !== null || item.byte_size !== null || (item.mode & 448) !== 448
        : typeof item.sha256 !== "string" || !SHA.test(item.sha256) || !integer(item.byte_size, 0, Number.MAX_SAFE_INTEGER)) fail();
      const operation = item.operation;
      if (operation === null) { if (["running", "done", "uncertain"].includes(item.status)) fail(); continue; }
      if (!object(operation) || !identifier(operation.operation_id) || operation.proposal_id !== item.proposal_id
        || operation.scope_id !== this.scope || operation.action !== "apply" || operation.expected_revision !== 0 || operation.status !== item.status) fail();
      const receipt = operation.receipt;
      if (receipt === null) { if (item.status === "done" || item.status === "rejected" || item.status === "uncertain" && item.source !== "directory") fail(); }
      else if (item.status === "running" || !object(receipt) || receipt.path !== item.path || receipt.status !== item.status
        || (item.source === "directory" ? !("after_fingerprint" in receipt) : !("after_sha256" in receipt))) fail();
    }
  }
  private async readSteps(): Promise<void> {
    if (!this.plan) return;
    const query = new URLSearchParams({ limit: "32" }); if (this.after !== null) query.set("after", String(this.after));
    const value = await this.read<TreePlanStepPage>(`${this.base}/${encodeURIComponent(this.plan.plan_id)}/steps?${query}`);
    this.assertSteps(value); this.steps = value; this.renderDetail();
  }
  private async page(direction: number): Promise<void> {
    if (direction > 0) { if (this.steps?.next_after == null) return; this.previous.push(this.after); this.after = this.steps.next_after; }
    else { if (!this.previous.length) return; this.after = this.previous.pop() ?? null; }
    await this.readSteps();
  }
  private renderSource(): void {
    const source = this.source;
    this.sourceInfo.textContent = source ? `${t("目录树根")}: ${source.root_path}\n${t("原命令")}: ${source.command_id}\n${t("原目录来源")}: ${source.source_hash}` : "";
  }
  private renderDetail(): void {
    this.clear(this.detail); const value = this.plan; if (!value) { delete this.detail.dataset.planId; return; }
    this.detail.dataset.planId = value.plan_id;
    const title = document.createElement("strong"); title.textContent = `${t(labels[value.status])} · ${value.root_path}`;
    const metadata = document.createElement("pre"); metadata.className = "tree-meta";
    metadata.textContent = `${value.plan_id}\n${t("原请求键")}: ${value.request_key}\n${t("原命令")}: ${value.command_id}\n${t("原目录来源")}: ${value.source_hash}\n${t("已确认步骤")}: ${value.done_count}/${value.step_count}\n${localDate(value.updated_at)}${value.stop_reason ? "\n" + value.stop_reason : ""}`;
    this.detail.append(title, metadata, text("p", "各项独立执行，可能部分成功"));
    const actions = document.createElement("div"); actions.className = "actions";
    actions.append(this.button("确认启动原目录树", () => this.start(), () => this.canStart()),
      this.button("等待原目录树", () => this.wait(), () => this.canRead() && !this.cache.pending && this.plan?.producer_status === "running"),
      this.button("取消原目录树", () => this.cancel(), () => this.canCancel()));
    this.detail.append(actions);
    if (value.status === "uncertain" || value.status === "interrupted") this.detail.append(text("p", "请导航原操作核实结果；查询不会重新发布目录树。"));
    if (!this.steps) return;
    const info = document.createElement("small"); info.className = "tree-page-info";
    info.textContent = t("目录树步骤页{page} · 本页{count}项", { page: this.previous.length + 1, count: this.steps.items.length });
    const list = document.createElement("ol"); list.className = "tree-steps";
    for (const item of this.steps.items) {
      const row = document.createElement("li"); row.dataset.position = String(item.position); row.dataset.source = item.source; row.dataset.path = item.path;
      const meta = document.createElement("pre"); meta.textContent = `${item.position + 1}. ${item.path} · ${t(item.source === "file" ? "文件" : "目录")} · ${t(stepLabels[item.status])}\n${t("父步骤")}: ${item.parent_position === null ? "—" : item.parent_position + 1} · ${t("文件权限")}: ${item.mode.toString(8).padStart(4, "0")}${item.source === "file" ? `\n${item.byte_size} B · SHA-256 ${item.sha256}` : ""}${item.error_code ? "\n" + item.error_code : ""}`;
      row.append(meta);
      if (item.operation) { const operation = document.createElement("code"); operation.textContent = item.operation.operation_id; row.append(operation); }
      if (item.proposal_id) {
        const id = document.createElement("code"); id.textContent = item.proposal_id; row.append(id);
        row.append(this.button(item.source === "file" ? "查看原文件提案" : "查看原目录提案",
          () => item.source === "file" ? this.openFile(item.proposal_id!, item.operation?.operation_id) : this.openDirectory(item.proposal_id!, item.operation?.operation_id),
          () => this.canRead() && (item.source === "file" || this.hasTool("directory_query"))));
      }
      list.append(row);
    }
    this.detail.append(info, list, this.button("上一页目录树步骤", () => this.page(-1), () => this.canRead() && this.previous.length > 0),
      this.button("下一页目录树步骤", () => this.page(1), () => this.canRead() && this.steps?.next_after != null));
  }
  private renderPending(): void {
    this.clear(this.pendingBox);
    if (this.cacheError) {
      this.pendingBox.append(text("p", "本地目录树恢复记录无效或无法保存，写入已关闭"));
      this.pendingBox.append(this.button("清除无效目录树恢复记录", async () => {
        if (!window.confirm(t("仅清除本地目录树恢复记录，随后请查询原键；不会重发请求？"))) return;
        try { sessionStorage.removeItem(this.storageKey); } catch { throw new Error(t("浏览器无法保存目录树恢复记录，尚未发送新请求")); }
        this.cache = { version: 1 }; if (this.plan) this.cache.selected = { id: this.plan.plan_id, key: this.plan.request_key };
        this.cacheError = false; this.pendingMissing = false; this.persist(true);
      }, () => this.canRead()));
    }
    const pending = this.cache.pending; if (!pending) return;
    this.pendingBox.append(text("p", "目录树响应未确认，请先查询原请求"));
    const key = document.createElement("code"); key.textContent = pending.kind === "prepare" ? pending.key : pending.id; this.pendingBox.append(key);
    this.pendingBox.append(this.button("查询未确认目录树", () => this.refresh(), () => this.canRead()));
    if (pending.kind === "prepare" && this.pendingMissing) this.pendingBox.append(this.button("放弃未找到的本地准备记录", async () => {
      const value = await requestJsonNullable<TreePlanView>(this.base + "/by-key?" + new URLSearchParams({ request_key: pending.key }), { signal: this.abort.signal });
      if (this.disposed) return;
      if (value !== null) { this.assertPlan(value, { key: pending.key }); this.cache.pending = undefined; this.pendingMissing = false; this.adopt(value); await this.readSteps(); return; }
      if (!window.confirm(t("原键尚未找到不证明未执行；仅放弃本地准备记录，不取消远端计划：{key}？", { key: pending.key }))) return;
      this.cache.pending = undefined; this.pendingMissing = false; this.key.value = ""; this.query.value = pending.key; this.persist(true);
      this.noticeBox.textContent = t("已放弃本地准备记录；原键仍可查询：{key}", { key: pending.key });
    }, () => this.canRead() && !this.cacheError && this.pendingMissing));
  }
  private persist(required = false): void {
    if (!this.cacheError) try { sessionStorage.setItem(this.storageKey, JSON.stringify(this.cache)); return; } catch { this.cacheError = true; }
    if (required) throw new Error(t("浏览器无法保存目录树恢复记录，尚未发送新请求"));
  }
  private readCache(): void {
    try {
      const raw = sessionStorage.getItem(this.storageKey); if (!raw) return;
      if (raw.length > 4096) throw new Error();
      const value = JSON.parse(raw) as Recovery;
      if (!keys(value, ["version", "selected", "pending"]) || value.version !== 1) throw new Error();
      if (value.selected !== undefined && (!keys(value.selected, ["id", "key"]) || !identifier(value.selected.id) || !requestKey(value.selected.key))) throw new Error();
      if (value.pending !== undefined) {
        const pending = value.pending;
        if (pending.kind === "prepare") {
          if (!keys(pending, ["kind", "key", "digest"]) || !requestKey(pending.key) || typeof pending.digest !== "string" || !SHA.test(pending.digest)) throw new Error();
        } else if (["start", "cancel"].includes(pending.kind)) {
          if (!keys(pending, ["kind", "id"]) || !identifier(pending.id) || value.selected?.id !== pending.id) throw new Error();
        } else throw new Error();
      }
      this.cache = value;
    } catch { this.cacheError = true; }
  }
  private clearPoll(): void { if (this.poll !== undefined) window.clearTimeout(this.poll); this.poll = undefined; }
  private schedulePoll(): void {
    this.clearPoll();
    if (!this.canRead() || this.busy || !this.root.open || this.cache.pending || this.plan?.producer_status !== "running") return;
    this.poll = window.setTimeout(() => { this.poll = undefined; void this.work(() => this.refresh()); }, 1500);
  }
}
