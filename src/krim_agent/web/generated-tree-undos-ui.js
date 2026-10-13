/** 原生成根独立审阅；副作用前落盘意图，恢复仅GET原键/ID。 */
import { errorText, requestJson, } from "./api.js";
import { currentLocale, localDate, t } from "./i18n.js";
const ID = /^[A-Za-z0-9][A-Za-z0-9_-]{0,35}$/, SHA = /^[0-9a-f]{64}$/, PAGE = 20;
const stateNames = {
    awaiting_approval: "生成根撤销待确认", removing: "生成根移除中", preserved: "生成根已移至保留位置",
    restoring: "生成根还原中", restored: "生成根已还原", keeping: "生成根保留核实中", kept: "生成根保留已确认",
    rejected: "生成根操作已拒绝", uncertain: "生成根结果待核实", cancelled: "生成根操作已取消",
};
const moveStates = { proposed: "awaiting_approval", applying: "removing", applied: "preserved", undoing: "restoring", undone: "restored", keeping: "keeping", kept: "kept", rejected: "rejected", uncertain: "uncertain", cancelled: "cancelled" };
const actionNames = { remove_root: "移除原生成根", restore_root: "还原原生成根", keep: "确认保留生成根" };
const moveActions = { remove_root: "apply", restore_root: "undo", keep: "keep" };
function text(tag, key) {
    const node = document.createElement(tag);
    node.dataset.i18n = key;
    node.textContent = t(key);
    return node;
}
function relative(value) {
    return typeof value === "string" && Boolean(value) && new TextEncoder().encode(value).length <= 512
        && !/[\\:\x00-\x1f\x7f]/.test(value) && value.split("/").length <= 16
        && value.split("/").every(part => part !== "" && part !== "." && part !== "..");
}
function paths(source, target) { return relative(source) && relative(target) && source !== target && !source.startsWith(target + "/") && !target.startsWith(source + "/"); }
function key(value) { return typeof value === "string" && value.length > 0 && value.length <= 128 && !/[\x00-\x1f\x7f]/.test(value); }
function keys(value, allowed) { return typeof value === "object" && value !== null && !Array.isArray(value) && Object.keys(value).every(name => allowed.includes(name)); }
function integer(value, maximum = Number.MAX_SAFE_INTEGER) { return typeof value === "number" && Number.isSafeInteger(value) && value >= 0 && value <= maximum; }
async function digest(value) {
    const result = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(JSON.stringify(value)));
    return Array.from(new Uint8Array(result), byte => byte.toString(16).padStart(2, "0")).join("");
}
export class GeneratedTreeUndosUI {
    agentId;
    report;
    root = document.createElement("details");
    editor = document.createElement("fieldset");
    plan = document.createElement("input");
    preserve = document.createElement("input");
    previousRestore = document.createElement("input");
    lookup = document.createElement("input");
    summary = document.createElement("pre");
    notice = document.createElement("p");
    pendingBox = document.createElement("div");
    list = document.createElement("ol");
    detail = document.createElement("div");
    buttons = new Map();
    grant;
    completed;
    origin;
    inspection;
    predecessor;
    proposal;
    operation;
    items = [];
    history = [];
    cache = { version: 1 };
    cacheError = false;
    missing = false;
    bound = false;
    available = false;
    mode = "agent";
    busy = false;
    loaded = false;
    disposed = false;
    stale = false;
    locale = currentLocale();
    after = null;
    next = null;
    previous = [];
    historyAfter = null;
    historyNext = null;
    historyPrevious = [];
    stop = document.createElement("button");
    stopping = false;
    base;
    storageKey;
    constructor(agentId, report) {
        this.agentId = agentId;
        this.report = report;
        this.base = `/v1/agents/${encodeURIComponent(agentId)}/workspace`;
        this.storageKey = `krim-generated-root-recovery:${agentId}`;
        this.root.className = "generated-tree-undos";
        this.root.hidden = true;
        this.root.append(text("summary", "原生成根撤销与历史"));
        this.editor.className = "generated-root-editor";
        this.editor.append(text("legend", "审阅原生成根与保留位置"));
        this.field("原完成生成计划标识", this.plan, this.editor);
        this.plan.maxLength = 36;
        this.field("生成根保留相对路径", this.preserve, this.editor);
        this.preserve.maxLength = 512;
        this.field("上次实际还原操作标识（再次撤销时填写）", this.previousRestore, this.editor);
        this.previousRestore.maxLength = 36;
        this.summary.className = "generated-root-summary";
        this.editor.append(this.summary);
        const preparation = document.createElement("div");
        preparation.className = "actions";
        preparation.append(this.button("查询原完成生成树", () => this.loadSource(), "workspace_tree_query", () => ID.test(this.plan.value) && this.permitted("tree_exchange_seal_query") && !this.cache.pending), this.button("封存待撤销的完整树", () => this.seal(), "tree_exchange_seal", () => Boolean(this.completed && this.inspection?.found && !this.origin && !this.previousRestore.value) && !this.cache.pending && !this.cacheError && !this.stale), this.button("预览生成根撤销", () => this.preview(), "generated_tree_undo_preview", () => this.valid() && !this.cache.pending && !this.cacheError && !this.stale));
        this.editor.append(preparation);
        this.notice.className = "generated-root-status";
        this.notice.setAttribute("role", "status");
        this.notice.setAttribute("aria-live", "polite");
        const navigation = document.createElement("div");
        navigation.className = "actions";
        this.field("生成根原请求键", this.lookup, navigation);
        this.lookup.maxLength = 128;
        navigation.append(this.button("按键查看生成根提案", () => this.find("by_key"), "generated_tree_undo_query", () => key(this.lookup.value) && !this.cache.pending), this.button("按键查看生成根操作", () => this.find("operation_by_key"), "generated_tree_undo_query", () => key(this.lookup.value) && !this.cache.pending), this.button("刷新生成根历史", () => this.refresh(), "generated_tree_undo_query"), this.button("上一页生成根提案", () => this.page(-1), "generated_tree_undo_query", () => this.previous.length > 0), this.button("下一页生成根提案", () => this.page(1), "generated_tree_undo_query", () => this.next !== null));
        this.pendingBox.className = "generated-root-pending";
        this.list.className = "generated-root-list";
        this.detail.className = "generated-root-detail";
        this.stop = text("button", "取消进行中的生成根操作");
        this.stop.type = "button";
        this.stop.className = "secondary";
        this.stop.onclick = () => { if (!this.stop.disabled)
            void this.interrupt(); };
        this.root.append(this.editor, this.notice, this.pendingBox, this.stop, navigation, this.list, this.detail);
        this.readCache();
        this.plan.oninput = () => { this.completed = undefined; this.origin = undefined; this.inspection = undefined; this.predecessor = undefined; this.renderSummary(); this.sync(); };
        this.previousRestore.oninput = () => { this.predecessor = undefined; this.sync(); };
        this.preserve.oninput = this.lookup.oninput = () => this.sync();
        this.root.ontoggle = () => { if (this.root.open && this.bound && !this.loaded && !this.busy)
            void this.work(() => this.refresh()); };
        this.renderPending();
        this.sync();
    }
    dispose() { this.disposed = true; this.sync(); }
    update(bound, available, mode) {
        this.bound = bound;
        this.available = available;
        this.mode = typeof mode === "string" ? mode : "agent";
        this.root.hidden = !bound;
        if (this.locale !== currentLocale()) {
            this.locale = currentLocale();
            this.renderSummary();
            this.renderList();
            this.renderDetail();
            this.renderPending();
        }
        this.sync();
    }
    field(label, input, root) {
        const row = document.createElement("label");
        row.append(text("span", label), input);
        root.append(row);
        input.dataset.i18nAriaLabel = label;
        input.setAttribute("aria-label", t(label));
    }
    permitted(tool) {
        if (!this.bound || this.disposed || this.mode === "ask" || !this.grant?.tool_names.includes(tool))
            return false;
        if (["generated_tree_undo_query", "workspace_tree_query", "tree_exchange_seal_query"].includes(tool))
            return true;
        if (!this.available || !this.grant.available)
            return false;
        if (tool === "tree_exchange_inspect")
            return true;
        return this.grant.writable && this.grant.tool_names.includes("generated_tree_undo_query") && (["generated_tree_undo_preview", "tree_exchange_seal"].includes(tool) || this.mode !== "plan");
    }
    sync() {
        this.editor.hidden = !this.permitted("workspace_tree_query") || !this.permitted("tree_exchange_seal_query");
        this.editor.disabled = this.busy || this.disposed;
        for (const [button, control] of this.buttons)
            button.disabled = this.busy || this.disposed || !this.bound || Boolean(control.tool && !this.permitted(control.tool)) || !control.ready();
        this.stop.hidden = this.cache.pending?.kind !== "action";
        this.stop.disabled = this.stopping || this.cacheError || !this.permitted("generated_tree_undo_cancel") || this.cache.pending?.kind !== "action";
        this.root.setAttribute("aria-busy", String(this.busy));
    }
    button(label, action, tool, ready = () => true) {
        const button = text("button", label);
        button.type = "button";
        button.className = "secondary";
        this.buttons.set(button, { tool, ready });
        button.onclick = () => { if (!button.disabled)
            void this.work(action); };
        return button;
    }
    clear(node) { for (const button of node.querySelectorAll("button"))
        this.buttons.delete(button); node.replaceChildren(); }
    async work(action) {
        if (this.busy || this.disposed || !this.bound)
            return;
        this.busy = true;
        this.sync();
        try {
            await action();
        }
        catch (error) {
            if (!this.disposed) {
                this.stale = true;
                this.notice.textContent = errorText(error);
                this.report(errorText(error), true);
            }
        }
        finally {
            this.busy = false;
            if (!this.disposed) {
                this.renderSummary();
                this.renderList();
                this.renderDetail();
                this.renderPending();
                this.sync();
            }
        }
    }
    async read(path, options) { const result = await requestJson(path, options); if (this.disposed)
        throw new Error(t("Agent 实例不可用")); return result; }
    check(condition) { if (!condition)
        throw new Error(t("生成根回程与原事实不一致")); }
    verifyProposal(value) {
        this.check(value && keys(value, ["source_plan_id", "source_command_id", "source_hash", "command_outcome", "accept_failed", "snapshot_revision", "sealed_fingerprint", "proof_hash", "previous_restore_operation_id", "generation_status", "root_state", "move"]));
        const move = value.move;
        this.check(move && ID.test(move.proposal_id) && move.scope_id === `session:${this.agentId}` && paths(move.source_path, move.target_path)
            && move.workspace_key === this.grant?.key && move.resource_fingerprint === this.grant?.fingerprint
            && SHA.test(move.before_fingerprint) && integer(move.revision) && integer(move.entry_count, 16384) && move.entry_count > 0 && integer(move.byte_size, 134217728)
            && Number.isFinite(move.expires_at) && Object.hasOwn(moveStates, move.status) && moveStates[move.status] === value.root_state
            && (move.active_operation_id === null || ID.test(move.active_operation_id)) && value.generation_status === "done"
            && ID.test(value.source_plan_id) && ID.test(value.source_command_id) && SHA.test(value.source_hash) && SHA.test(value.snapshot_revision) && SHA.test(value.sealed_fingerprint) && SHA.test(value.proof_hash)
            && ["completed", "failed", "output_limit"].includes(value.command_outcome) && typeof value.accept_failed === "boolean" && (value.command_outcome === "completed" || value.accept_failed)
            && (value.previous_restore_operation_id === null ? move.before_fingerprint === value.sealed_fingerprint : ID.test(value.previous_restore_operation_id)));
        const previous = this.proposal;
        if (previous?.move.proposal_id === move.proposal_id)
            this.check(JSON.stringify(this.condition(value)) === JSON.stringify(this.condition(previous))
                && value.command_outcome === previous.command_outcome && value.accept_failed === previous.accept_failed
                && move.entry_count === previous.move.entry_count && move.byte_size === previous.move.byte_size && move.expires_at === previous.move.expires_at && move.revision >= previous.move.revision);
    }
    verifyOperation(value) {
        this.check(value && keys(value, ["source_plan_id", "action", "operation"]) && ID.test(value.source_plan_id) && Object.hasOwn(actionNames, value.action));
        const operation = value.operation;
        this.check(operation && ID.test(operation.operation_id) && ID.test(operation.proposal_id) && operation.scope_id === `session:${this.agentId}`
            && operation.action === moveActions[value.action] && integer(operation.expected_revision, Number.MAX_SAFE_INTEGER - 2) && ["running", "done", "rejected", "uncertain"].includes(operation.status)
            && typeof operation.cancelled === "boolean" && typeof operation.recovery_required === "boolean");
        const receipt = operation.receipt, proposal = this.proposal;
        if (proposal?.move.proposal_id === operation.proposal_id)
            this.check(value.source_plan_id === proposal.source_plan_id && operation.expected_revision <= proposal.move.revision);
        this.check(receipt === null || receipt && receipt.status === operation.status && paths(receipt.source_path, receipt.target_path)
            && SHA.test(receipt.before_fingerprint) && (receipt.after_fingerprint === null || SHA.test(receipt.after_fingerprint))
            && typeof receipt.changed === "boolean" && typeof receipt.recovery_required === "boolean"
            && (receipt.status !== "done" || receipt.after_fingerprint !== null && receipt.error_code === null && !receipt.recovery_required && !operation.recovery_required)
            && (!proposal || proposal.move.proposal_id !== operation.proposal_id || value.source_plan_id === proposal.source_plan_id
                && receipt.source_path === (value.action === "restore_root" ? proposal.move.target_path : proposal.move.source_path)
                && receipt.target_path === (value.action === "restore_root" ? proposal.move.source_path : proposal.move.target_path)
                && (value.action === "restore_root" || receipt.before_fingerprint === proposal.move.before_fingerprint)));
    }
    async query(kind, options = {}) {
        const result = await this.read(`${this.base}/generated-tree-undos/query?${new URLSearchParams({ kind, ...options })}`);
        const expected = { proposal: "proposal", by_key: "proposal", operation: "operation", operation_by_key: "operation", history: "history", proposals: "proposals" }[kind];
        this.check(result && result.kind === kind && ["proposal", "operation", "history", "proposals"].every(field => field === expected || result[field] == null));
        this.check(["by_key", "operation_by_key"].includes(kind) || result[expected] != null);
        if (result.proposal) {
            this.verifyProposal(result.proposal);
            this.check(kind !== "proposal" || result.proposal.move.proposal_id === options.identifier);
        }
        if (result.operation) {
            this.verifyOperation(result.operation);
            this.check(kind !== "operation" || result.operation.operation.operation_id === options.identifier);
        }
        const page = result.history || result.proposals;
        if (page) {
            this.check(Array.isArray(page.items) && page.items.length <= PAGE);
            const ids = [], plans = [];
            for (const item of page.items) {
                if ("operation" in item) {
                    this.verifyOperation(item);
                    this.check(item.operation.proposal_id === options.identifier);
                    ids.push(item.operation.operation_id);
                    plans.push(item.source_plan_id);
                }
                else {
                    this.verifyProposal(item);
                    ids.push(item.move.proposal_id);
                }
            }
            this.check(new Set(plans).size <= 1 && (!this.proposal || kind !== "history" || plans.every(plan => plan === this.proposal.source_plan_id))
                && ids.every((id, index) => (!index || id > ids[index - 1]) && (!options.after || id > options.after)) && (page.next_after === null || ids.length === PAGE && page.next_after === ids.at(-1)));
        }
        return result;
    }
    verifyOrigin(value, plan) {
        this.check(value && value.plan_id === plan);
        const origin = value.origin, completed = this.completed;
        if (!origin)
            return;
        this.check(origin.plan_id === plan && origin.scope_id === `session:${this.agentId}` && relative(origin.root_path)
            && origin.workspace_key === this.grant?.key && origin.resource_fingerprint === this.grant?.fingerprint && ID.test(origin.command_id)
            && [origin.tree_fingerprint, origin.source_hash, origin.snapshot_revision, origin.proof_hash].every(hash => SHA.test(hash))
            && integer(origin.entry_count, 16384) && origin.entry_count > 0 && integer(origin.byte_size, 134217728)
            && ["completed", "failed", "output_limit"].includes(origin.outcome) && typeof origin.accept_failed === "boolean" && (origin.outcome === "completed" || origin.accept_failed)
            && (!completed || completed.plan_id !== plan || completed.root_path === origin.root_path && completed.command_id === origin.command_id && completed.source_hash === origin.source_hash && completed.snapshot_revision === origin.snapshot_revision));
    }
    async lookupOrigin(plan) { const value = await this.read(`${this.base}/tree-exchanges/seals/${encodeURIComponent(plan)}`); this.verifyOrigin(value, plan); return value; }
    async loadSource() {
        this.completed = undefined;
        this.origin = undefined;
        this.inspection = undefined;
        this.predecessor = undefined;
        const value = await this.read(`${this.base}/tree-plans/${encodeURIComponent(this.plan.value)}`);
        this.check(value && value.plan_id === this.plan.value && value.scope_id === `session:${this.agentId}` && value.status === "done" && value.producer_status === "finished"
            && value.workspace_key === this.grant?.key && value.resource_fingerprint === this.grant?.fingerprint && relative(value.root_path)
            && ID.test(value.command_id) && SHA.test(value.source_hash) && SHA.test(value.snapshot_revision) && integer(value.step_count, 16384) && value.step_count > 0 && value.done_count === value.step_count);
        this.completed = value;
        this.origin = (await this.lookupOrigin(value.plan_id)).origin || undefined;
        if (this.permitted("tree_exchange_inspect")) {
            const inspected = await this.read(`${this.base}/tree-exchange-state?${new URLSearchParams({ path: value.root_path })}`);
            this.check(inspected && inspected.path === value.root_path && typeof inspected.found === "boolean" && integer(inspected.entry_count, 16384)
                && integer(inspected.directory_count, 16384) && integer(inspected.file_count, 16384) && inspected.entry_count === inspected.directory_count + inspected.file_count && integer(inspected.byte_size, 134217728)
                && (inspected.found ? SHA.test(inspected.fingerprint || "") && inspected.entry_count > 0 && integer(inspected.mode, 511) : inspected.fingerprint === null && inspected.mode === null && inspected.entry_count === 0 && inspected.byte_size === 0));
            this.inspection = inspected;
        }
        if (this.previousRestore.value) {
            this.check(ID.test(this.previousRestore.value));
            const result = await this.query("operation", { identifier: this.previousRestore.value }), predecessor = result.operation;
            this.check(predecessor.source_plan_id === value.plan_id && predecessor.action === "restore_root" && predecessor.operation.status === "done"
                && predecessor.operation.receipt?.changed && predecessor.operation.receipt.target_path === value.root_path && predecessor.operation.receipt.after_fingerprint === this.inspection?.fingerprint);
            this.predecessor = predecessor;
        }
        this.stale = false;
        this.notice.textContent = t(this.origin ? "原生成树已封存" : "未找到原生成树封存");
    }
    valid() {
        const completed = this.completed, origin = this.origin, inspected = this.inspection;
        return Boolean(completed?.plan_id === this.plan.value && origin?.plan_id === this.plan.value && inspected?.found && SHA.test(inspected.fingerprint || "") && paths(completed.root_path, this.preserve.value)
            && (this.previousRestore.value ? this.predecessor?.operation.operation_id === this.previousRestore.value && this.predecessor.operation.receipt?.after_fingerprint === inspected.fingerprint : origin.tree_fingerprint === inspected.fingerprint));
    }
    renderSummary() {
        const value = this.completed, origin = this.origin;
        const fingerprint = this.inspection ? this.inspection.fingerprint || t("不存在") : t("当前生成根尚未检查");
        this.summary.textContent = value || origin ? `${t("原完成生成计划标识")}: ${value?.plan_id || origin.plan_id}\n${t("原生成状态")}: ${t("树已完成")}\n${t("原生成根路径")}: ${value?.root_path || origin.root_path}\n${t("原完整指纹")}: ${fingerprint}\n${t("首次封存指纹")}: ${origin?.tree_fingerprint || t("未找到原生成树封存")}\n${t("生成来源")}: ${value?.command_id || origin.command_id}\nSHA-256: ${value?.source_hash || origin.source_hash}\n${t("快照版本")}: ${value?.snapshot_revision || origin.snapshot_revision}${origin ? `\n${t("来源证明")}: ${origin.proof_hash}\n${origin.accept_failed ? t("已明确接受失败来源") : origin.outcome}` : ""}` : "";
    }
    async seal() {
        const plan = this.completed?.plan_id, fingerprint = this.inspection?.fingerprint;
        if (!plan || !fingerprint || this.cache.pending || this.cacheError || this.origin || this.stale)
            return;
        if (!window.confirm(`${t("封存待撤销的完整树")} · ${plan} · ${this.completed.root_path}\n${fingerprint}`))
            return;
        this.cache.pending = { kind: "seal", plan, fingerprint };
        this.persist();
        this.renderPending();
        const origin = await this.read(`${this.base}/tree-exchanges/seal?confirmed=true`, { method: "POST", body: JSON.stringify({ plan_id: plan, expected_fingerprint: fingerprint }) });
        this.verifyOrigin({ plan_id: plan, origin }, plan);
        this.check(origin.tree_fingerprint === fingerprint);
        this.origin = origin;
        this.cache.pending = undefined;
        this.persist();
        this.notice.textContent = t("原生成树已封存");
    }
    condition(value) {
        return value ? [value.source_plan_id, value.move.source_path, value.move.target_path, value.move.before_fingerprint, value.previous_restore_operation_id, value.source_command_id, value.source_hash, value.snapshot_revision, value.sealed_fingerprint, value.proof_hash]
            : [this.plan.value, this.completed.root_path, this.preserve.value, this.inspection.fingerprint, this.previousRestore.value || null, this.origin.command_id, this.origin.source_hash, this.origin.snapshot_revision, this.origin.tree_fingerprint, this.origin.proof_hash];
    }
    async preview() {
        if (!this.valid() || this.cache.pending || this.cacheError || this.stale)
            return;
        const hash = await digest(this.condition());
        const request = { plan_id: this.plan.value, preserve_path: this.preserve.value, expected_fingerprint: this.inspection.fingerprint, request_key: crypto.randomUUID(), previous_restore_operation_id: this.previousRestore.value || null };
        this.cache.pending = { kind: "preview", key: request.request_key, digest: hash };
        this.missing = false;
        this.persist();
        const value = await this.read(`${this.base}/generated-tree-undos/preview?confirmed=true`, { method: "POST", body: JSON.stringify(request) });
        this.verifyProposal(value);
        this.check(await digest(this.condition(value)) === hash);
        this.cache.pending = undefined;
        this.select(value.move.proposal_id);
        this.proposal = value;
        this.persist();
        this.notice.textContent = t("生成根提案已保存，请审阅后确认");
        this.after = null;
        this.previous = [];
        await this.refresh();
    }
    select(id) { this.cache.selected = id; this.proposal = undefined; this.operation = undefined; this.history = []; this.historyAfter = null; this.historyPrevious = []; this.historyNext = null; }
    async refresh() {
        this.grant = await this.read(this.base);
        if (!this.permitted("generated_tree_undo_query")) {
            this.loaded = true;
            this.notice.textContent = t("当前会话未授权生成根查询");
            return;
        }
        if (this.cache.pending)
            await this.recover();
        const options = { limit: String(PAGE) };
        if (this.after)
            options.after = this.after;
        const page = await this.query("proposals", options);
        this.items = page.proposals.items;
        this.next = page.proposals.next_after;
        await this.selected();
        this.loaded = true;
        this.stale = false;
    }
    async selected() {
        if (!this.cache.selected)
            return;
        const result = await this.query("proposal", { identifier: this.cache.selected });
        this.proposal = result.proposal;
        await this.readHistory();
        if (this.proposal.move.active_operation_id) {
            const active = await this.query("operation", { identifier: this.proposal.move.active_operation_id });
            this.check(active.operation?.operation.proposal_id === this.cache.selected && active.operation.source_plan_id === this.proposal.source_plan_id);
            this.operation = active.operation;
        }
    }
    async readHistory() {
        if (!this.proposal)
            return;
        const options = { identifier: this.proposal.move.proposal_id, limit: String(PAGE) };
        if (this.historyAfter)
            options.after = this.historyAfter;
        const result = await this.query("history", options);
        this.history = result.history.items;
        this.historyNext = result.history.next_after;
    }
    async page(direction) {
        if (direction > 0) {
            if (!this.next)
                return;
            this.previous.push(this.after);
            this.after = this.next;
        }
        else {
            if (!this.previous.length)
                return;
            this.after = this.previous.pop() || null;
        }
        await this.refresh();
    }
    async historyPage(direction) {
        if (direction > 0) {
            if (!this.historyNext)
                return;
            this.historyPrevious.push(this.historyAfter);
            this.historyAfter = this.historyNext;
        }
        else {
            if (!this.historyPrevious.length)
                return;
            this.historyAfter = this.historyPrevious.pop() || null;
        }
        await this.readHistory();
    }
    async find(kind) {
        const result = await this.query(kind, { request_key: this.lookup.value }), id = result.proposal?.move.proposal_id || result.operation?.operation.proposal_id;
        if (!id) {
            this.notice.textContent = t("未找到原树请求，请保留原键继续查询");
            return;
        }
        this.select(id);
        this.persist();
        await this.selected();
        if (result.operation) {
            this.check(result.operation.source_plan_id === this.proposal?.source_plan_id);
            this.verifyOperation(result.operation);
            this.operation = result.operation;
        }
        this.stale = false;
    }
    async change(action) {
        const proposal = this.proposal;
        if (!proposal || this.cache.pending || this.cacheError || this.stale)
            return;
        const move = proposal.move;
        if (!window.confirm(`${t(actionNames[action])} · ${proposal.source_plan_id}\n${move.source_path} → ${move.target_path} · r${move.revision}\n${move.before_fingerprint}\n${t("确认按原生成根条件执行此操作？")}`))
            return;
        const body = { proposal_id: move.proposal_id, action, expected_revision: move.revision, request_key: crypto.randomUUID() };
        this.cache.pending = { kind: "action", body, plan: proposal.source_plan_id };
        this.missing = false;
        this.persist();
        this.renderPending();
        this.sync();
        const value = await this.read(`${this.base}/generated-tree-undos/change?confirmed=true`, { method: "POST", body: JSON.stringify(body) });
        this.verifyOperation(value);
        this.check(value.source_plan_id === proposal.source_plan_id && value.action === action && value.operation.proposal_id === body.proposal_id && value.operation.expected_revision === body.expected_revision);
        this.inspection = undefined;
        this.predecessor = undefined;
        this.operation = value;
        if (value.operation.status !== "running")
            this.cache.pending = undefined;
        this.persist();
        await this.refresh();
    }
    async cancel() {
        if (!this.proposal || this.cache.pending || this.cacheError || !window.confirm(t("确认取消原生成根操作？")))
            return;
        const id = this.proposal.move.proposal_id;
        this.cache.pending = { kind: "cancel", id };
        this.persist();
        const value = await this.read(`${this.base}/generated-tree-undos/${encodeURIComponent(id)}/cancel?confirmed=true`, { method: "POST" });
        this.verifyProposal(value);
        this.check(value.move.proposal_id === id && value.source_plan_id === this.proposal.source_plan_id);
        this.proposal = value;
        if (!value.move.active_operation_id)
            this.cache.pending = undefined;
        this.persist();
        await this.refresh();
    }
    async interrupt() {
        const pending = this.cache.pending;
        if (pending?.kind !== "action" || this.stopping || !window.confirm(t("确认取消原生成根操作？")))
            return;
        this.stopping = true;
        this.sync();
        try {
            const value = await this.read(`${this.base}/generated-tree-undos/${encodeURIComponent(pending.body.proposal_id)}/cancel?confirmed=true`, { method: "POST" });
            this.verifyProposal(value);
            this.check(value.move.proposal_id === pending.body.proposal_id && value.source_plan_id === pending.plan);
            // 保留原动作键；原POST或之后GET负责读取同一线程的最终回执。
            this.notice.textContent = t("原生成根取消已请求，请读取原操作");
            if (!this.busy)
                await this.work(() => this.refresh());
        }
        catch (error) {
            if (!this.disposed) {
                this.notice.textContent = errorText(error);
                this.report(errorText(error), true);
            }
        }
        finally {
            this.stopping = false;
            if (!this.disposed)
                this.sync();
        }
    }
    async reconcile() {
        const value = this.operation;
        if (!value || this.cache.pending || this.cacheError || !window.confirm(t("确认核实原生成根结果？")))
            return;
        const original = value.operation;
        this.cache.pending = { kind: "reconcile", id: original.operation_id, proposal: original.proposal_id, plan: value.source_plan_id, action: value.action, revision: original.expected_revision };
        this.persist();
        const verified = await this.read(`${this.base}/generated-tree-undo-operations/${encodeURIComponent(original.operation_id)}/reconcile?confirmed=true`, { method: "POST" });
        this.verifyOperation(verified);
        this.check(verified.source_plan_id === value.source_plan_id && verified.action === value.action && verified.operation.operation_id === original.operation_id && verified.operation.proposal_id === original.proposal_id && verified.operation.expected_revision === original.expected_revision);
        this.cache.pending = undefined;
        this.operation = verified;
        this.persist();
        await this.refresh();
    }
    async recover() {
        const pending = this.cache.pending;
        if (!pending)
            return;
        if (pending.kind === "seal") {
            this.check(this.permitted("tree_exchange_seal_query"));
            const result = await this.lookupOrigin(pending.plan);
            if (!result.origin) {
                this.missing = true;
                return;
            }
            this.check(result.origin.tree_fingerprint === pending.fingerprint);
            this.origin = result.origin;
            this.plan.value = pending.plan;
            this.cache.pending = undefined;
        }
        else if (pending.kind === "preview") {
            const result = await this.query("by_key", { request_key: pending.key });
            if (!result.proposal) {
                this.missing = true;
                return;
            }
            this.check(await digest(this.condition(result.proposal)) === pending.digest);
            this.cache.pending = undefined;
            this.select(result.proposal.move.proposal_id);
            this.proposal = result.proposal;
        }
        else if (pending.kind === "action" || pending.kind === "reconcile") {
            const result = pending.kind === "action" ? await this.query("operation_by_key", { request_key: pending.body.request_key }) : await this.query("operation", { identifier: pending.id });
            const value = result.operation;
            if (!value) {
                this.missing = true;
                return;
            }
            this.check(value.source_plan_id === pending.plan && value.operation.proposal_id === (pending.kind === "action" ? pending.body.proposal_id : pending.proposal) && value.action === (pending.kind === "action" ? pending.body.action : pending.action) && value.operation.expected_revision === (pending.kind === "action" ? pending.body.expected_revision : pending.revision));
            this.operation = value;
            if (value.operation.status === "running")
                return;
            this.inspection = undefined;
            this.predecessor = undefined;
            this.cache.pending = undefined;
            this.select(value.operation.proposal_id);
            this.operation = value;
        }
        else {
            const result = await this.query("proposal", { identifier: pending.id });
            if (result.proposal.move.active_operation_id)
                return;
            this.cache.pending = undefined;
            this.select(pending.id);
            this.proposal = result.proposal;
        }
        this.missing = false;
        this.persist();
    }
    renderList() {
        this.clear(this.list);
        if (!this.items.length) {
            this.list.append(text("li", "暂无生成根撤销提案"));
            return;
        }
        for (const proposal of this.items) {
            const row = document.createElement("li");
            row.dataset.proposalId = proposal.move.proposal_id;
            const button = this.button("查看生成根撤销提案", async () => { this.select(proposal.move.proposal_id); this.persist(); await this.selected(); this.stale = false; }, "generated_tree_undo_query", () => !this.cache.pending);
            button.removeAttribute("data-i18n");
            button.textContent = `${proposal.move.source_path} → ${proposal.move.target_path} · ${t(stateNames[proposal.root_state])} · r${proposal.move.revision}`;
            row.append(button);
            this.list.append(row);
        }
    }
    renderDetail() {
        this.clear(this.detail);
        const proposal = this.proposal;
        if (!proposal)
            return;
        const move = proposal.move, title = document.createElement("strong");
        title.textContent = `${move.source_path} → ${move.target_path} · ${t(stateNames[proposal.root_state])} · r${move.revision}`;
        const condition = document.createElement("pre");
        condition.className = "generated-root-condition";
        condition.textContent = `${t("原生成状态")}: ${t("树已完成")}\n${t("原完成生成计划标识")}: ${proposal.source_plan_id}\n${t("生成来源")}: ${proposal.source_command_id}\nSHA-256: ${proposal.source_hash}\n${t("快照版本")}: ${proposal.snapshot_revision}\n${t("来源证明")}: ${proposal.proof_hash}\n${t("原生成根路径")}: ${move.source_path}\n${t("生成根保留相对路径")}: ${move.target_path}\n${t("原完整指纹")}: ${move.before_fingerprint}\n${t("首次封存指纹")}: ${proposal.sealed_fingerprint}\n${t("上次实际还原操作标识（再次撤销时填写）")}: ${proposal.previous_restore_operation_id || "—"}\n${t("树提案规模：{entries}节点、{bytes}字节", { entries: move.entry_count, bytes: move.byte_size })}\n${t("原操作标识")}: ${move.proposal_id}${proposal.accept_failed ? `\n${t("已明确接受失败来源")}` : ""}`;
        const actions = document.createElement("div");
        actions.className = "actions";
        const writable = () => !this.cache.pending && !this.cacheError && !this.stale;
        for (const action of ["remove_root", "keep", "restore_root"])
            actions.append(this.button(actionNames[action], () => this.change(action), "generated_tree_undo_change", () => writable() && (action === "remove_root" ? proposal.root_state === "awaiting_approval" && move.expires_at > Date.now() / 1000 : action === "keep" ? proposal.root_state === "preserved" : ["preserved", "kept"].includes(proposal.root_state))));
        actions.append(this.button("取消生成根操作", () => this.cancel(), "generated_tree_undo_cancel", () => writable() && ["awaiting_approval", "removing", "restoring", "keeping"].includes(proposal.root_state)));
        const operations = document.createElement("ol");
        operations.className = "generated-root-history";
        for (const value of this.history) {
            const operation = value.operation, row = document.createElement("li");
            row.dataset.operationId = operation.operation_id;
            const button = this.button("查看原生成根操作", async () => { const result = await this.query("operation", { identifier: operation.operation_id }); this.operation = result.operation; }, "generated_tree_undo_query");
            button.removeAttribute("data-i18n");
            button.textContent = `${t(actionNames[value.action])} · ${t(operation.status === "done" ? "树操作已确认" : operation.status === "running" ? "树操作执行中" : operation.status === "uncertain" ? "生成根结果待核实" : "生成根操作已拒绝")} · ${operation.operation_id} · ${localDate(operation.updated_at)}`;
            row.append(button);
            operations.append(row);
        }
        const pages = document.createElement("div");
        pages.className = "actions";
        pages.append(this.button("上一页生成根操作", () => this.historyPage(-1), "generated_tree_undo_query", () => this.historyPrevious.length > 0), this.button("下一页生成根操作", () => this.historyPage(1), "generated_tree_undo_query", () => this.historyNext !== null));
        this.detail.append(title, condition, actions, operations, pages);
        if (this.operation) {
            const operation = this.operation.operation, receipt = document.createElement("pre");
            receipt.className = "generated-root-receipt";
            receipt.textContent = `${t("原操作标识")}: ${operation.operation_id} · ${t(actionNames[this.operation.action])} · ${t(operation.status === "done" ? "树操作已确认" : operation.status === "running" ? "树操作执行中" : operation.status === "uncertain" ? "生成根结果待核实" : "生成根操作已拒绝")}${operation.receipt ? `\n${t(operation.receipt.changed ? "已产生工作区变化" : "未产生工作区变化")}\n${t("结果完整指纹")}: ${operation.receipt.after_fingerprint || "—"}` : ""}${operation.cancelled ? `\n${t("已请求取消生成根操作")}` : ""}${operation.receipt?.error_code ? `\n${operation.receipt.error_code}` : ""}`;
            this.detail.append(receipt);
            this.detail.append(this.button("核实原生成根结果", () => this.reconcile(), "generated_tree_undo_reconcile", () => this.operation?.operation.status === "uncertain" && !this.cache.pending && !this.cacheError));
        }
    }
    renderPending() {
        this.clear(this.pendingBox);
        if (this.cacheError) {
            this.pendingBox.append(text("p", "生成根恢复记录不可用，写操作已停用"));
            return;
        }
        const pending = this.cache.pending;
        if (!pending)
            return;
        const original = document.createElement("code");
        original.textContent = pending.kind === "seal" ? pending.plan : pending.kind === "preview" ? pending.key : pending.kind === "action" ? pending.body.request_key : pending.id;
        this.pendingBox.append(text("p", "生成根响应尚未确认，仅查询原请求"), original, this.button("查询待确认生成根请求", () => this.refresh(), "generated_tree_undo_query"));
        if (this.missing)
            this.pendingBox.append(text("p", "未找到原树请求，请保留原键继续查询"));
    }
    persist() { try {
        localStorage.setItem(this.storageKey, JSON.stringify(this.cache));
    }
    catch {
        this.cacheError = true;
        throw new Error(t("生成根恢复记录不可用，写操作已停用"));
    } }
    readCache() {
        try {
            const raw = localStorage.getItem(this.storageKey);
            if (!raw)
                return;
            if (raw.length > 4096)
                throw new Error();
            const value = JSON.parse(raw);
            if (!keys(value, ["version", "selected", "pending"]) || value.version !== 1 || value.selected !== undefined && (typeof value.selected !== "string" || !ID.test(value.selected)))
                throw new Error();
            const pending = value.pending;
            if (pending !== undefined) {
                if (!keys(pending, ["kind", "plan", "fingerprint", "key", "digest", "body", "id", "proposal", "action", "revision"]))
                    throw new Error();
                if (pending.kind === "seal") {
                    if (!keys(pending, ["kind", "plan", "fingerprint"]) || typeof pending.plan !== "string" || !ID.test(pending.plan) || typeof pending.fingerprint !== "string" || !SHA.test(pending.fingerprint))
                        throw new Error();
                }
                else if (pending.kind === "preview") {
                    if (!keys(pending, ["kind", "key", "digest"]) || !key(pending.key) || typeof pending.digest !== "string" || !SHA.test(pending.digest))
                        throw new Error();
                }
                else if (pending.kind === "action") {
                    const body = pending.body;
                    if (!keys(pending, ["kind", "body", "plan"]) || typeof pending.plan !== "string" || !ID.test(pending.plan) || !keys(body, ["proposal_id", "action", "expected_revision", "request_key"]) || typeof body.proposal_id !== "string" || !ID.test(body.proposal_id) || body.proposal_id !== value.selected || typeof body.action !== "string" || !Object.hasOwn(actionNames, body.action) || !integer(body.expected_revision, Number.MAX_SAFE_INTEGER - 2) || !key(body.request_key))
                        throw new Error();
                }
                else if (pending.kind === "cancel") {
                    if (!keys(pending, ["kind", "id"]) || typeof pending.id !== "string" || !ID.test(pending.id) || pending.id !== value.selected)
                        throw new Error();
                }
                else if (pending.kind === "reconcile") {
                    if (!keys(pending, ["kind", "id", "proposal", "plan", "action", "revision"]) || typeof pending.id !== "string" || !ID.test(pending.id) || typeof pending.plan !== "string" || !ID.test(pending.plan) || typeof pending.proposal !== "string" || !ID.test(pending.proposal) || pending.proposal !== value.selected || typeof pending.action !== "string" || !Object.hasOwn(actionNames, pending.action) || !integer(pending.revision, Number.MAX_SAFE_INTEGER - 2))
                        throw new Error();
                }
                else
                    throw new Error();
            }
            this.cache = value;
        }
        catch {
            this.cacheError = true;
        }
    }
}
