/** 目录面板只消费原REST账本；请求丢失先查原键，不自动再次执行。 */
import { ApiError, errorText, requestJson, requestJsonNullable, } from "./api.js";
import { currentLocale, localDate, t } from "./i18n.js";
const PAGE_SIZE = 20;
const IDENTIFIER = /^[A-Za-z0-9][A-Za-z0-9_-]{0,35}$/;
const labels = {
    proposed: "目录待确认", applying: "目录应用中", applied: "目录已应用", undoing: "目录撤销中",
    undone: "目录已撤销", keeping: "目录保留中", kept: "目录已保留", rejected: "目录操作已拒绝",
    uncertain: "目录结果待核实", running: "目录操作执行中", done: "目录操作已确认",
};
const actionLabels = { apply: "应用目录事务", keep: "保留目录事务", undo: "撤销目录事务" };
function text(tag, key) {
    const node = document.createElement(tag);
    node.dataset.i18n = key;
    node.textContent = t(key);
    return node;
}
function relative(path) {
    return Boolean(path) && new TextEncoder().encode(path).length <= 512 && !/[\\:\x00-\x1f]/.test(path)
        && path.split("/").length <= 16 && path.split("/").every(part => part !== "" && part !== "." && part !== "..");
}
function keys(value, allowed) {
    return typeof value === "object" && value !== null && !Array.isArray(value) && Object.keys(value).every(key => allowed.includes(key));
}
function identifier(value) { return typeof value === "string" && IDENTIFIER.test(value); }
async function digest(value) {
    const data = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(JSON.stringify(value)));
    return Array.from(new Uint8Array(data), byte => byte.toString(16).padStart(2, "0")).join("");
}
export class DirectoryChangesUI {
    report;
    root = document.createElement("details");
    editor = document.createElement("fieldset");
    mode = document.createElement("select");
    path = document.createElement("input");
    directoryMode = document.createElement("input");
    fingerprint = document.createElement("input");
    modeRow;
    fingerprintRow;
    state = document.createElement("p");
    notice = document.createElement("p");
    pendingBox = document.createElement("div");
    list = document.createElement("ol");
    pageInfo = document.createElement("small");
    detail = document.createElement("div");
    buttons = new Map();
    grant;
    bound = false;
    available = false;
    sessionMode = "agent";
    busy = false;
    loaded = false;
    disposed = false;
    stale = false;
    pendingMissing = false;
    locale = currentLocale();
    cache = { version: 1 };
    cacheError = false;
    items = [];
    proposal;
    operation;
    operationId;
    history = [];
    listAfter = null;
    listNext = null;
    listPrevious = [];
    historyAfter = null;
    historyNext = null;
    historyPrevious = [];
    base;
    storageKey;
    constructor(agentId, report) {
        this.report = report;
        this.base = `/v1/agents/${encodeURIComponent(agentId)}/workspace`;
        this.storageKey = `krim-directory-recovery:${agentId}`;
        this.root.className = "directory-changes";
        this.root.hidden = true;
        this.root.append(text("summary", "目录事务与历史"));
        this.editor.className = "directory-editor";
        this.editor.append(text("legend", "准备目录事务"));
        for (const [value, key] of [["create", "新建目录"], ["delete", "删除空目录"]]) {
            const option = text("option", key);
            option.value = value;
            this.mode.append(option);
        }
        this.field("目录操作", this.mode);
        this.field("目录相对路径", this.path);
        this.modeRow = this.field("新目录权限（十进制）", this.directoryMode);
        this.fingerprintRow = this.field("原目录指纹", this.fingerprint);
        this.path.maxLength = 512;
        this.fingerprint.maxLength = 64;
        this.directoryMode.type = "number";
        this.directoryMode.min = "448";
        this.directoryMode.max = "511";
        this.directoryMode.value = "488";
        const controls = document.createElement("div");
        controls.className = "actions";
        controls.append(this.button("检查目录状态", () => this.inspect(), "directory_inspect", () => relative(this.path.value)), this.button("预览目录事务", () => this.preview(), "directory_preview", () => this.valid() && !this.cache.pending && !this.cacheError));
        this.state.className = "directory-state";
        this.editor.append(this.state, controls);
        this.notice.className = "directory-status";
        this.notice.setAttribute("role", "status");
        this.notice.setAttribute("aria-live", "polite");
        const pages = document.createElement("div");
        pages.className = "actions";
        pages.append(this.button("刷新目录历史", () => this.refresh(), undefined, () => this.sessionMode !== "ask"), this.button("上一页", () => this.page(-1), "directory_query", () => this.listPrevious.length > 0), this.button("下一页", () => this.page(1), "directory_query", () => this.listNext !== null));
        this.list.className = "directory-list";
        this.detail.className = "directory-detail";
        this.pendingBox.className = "directory-pending";
        this.root.append(this.editor, this.notice, this.pendingBox, pages, this.pageInfo, this.list, this.detail);
        this.readCache();
        this.path.oninput = () => { this.fingerprint.value = ""; this.state.textContent = ""; this.sync(); };
        for (const field of [this.mode, this.directoryMode, this.fingerprint])
            field.oninput = () => this.sync();
        this.mode.onchange = () => this.sync();
        this.root.ontoggle = () => { if (this.root.open && this.bound && !this.loaded && !this.busy)
            void this.work(() => this.refresh()); };
        this.renderPending();
        this.sync();
    }
    dispose() { this.disposed = true; this.sync(); }
    update(bound, available, mode, grant) {
        this.bound = bound;
        this.available = available;
        this.sessionMode = typeof mode === "string" ? mode : "agent";
        if (grant)
            this.grant = grant;
        const changed = this.locale !== currentLocale();
        this.locale = currentLocale();
        this.root.hidden = !bound;
        if (changed) {
            this.renderList();
            this.renderDetail();
            this.renderPending();
        }
        this.sync();
    }
    field(key, input) {
        const row = document.createElement("label");
        row.append(text("span", key), input);
        input.setAttribute("aria-label", t(key));
        input.setAttribute("data-i18n-aria-label", key);
        this.editor.append(row);
        return row;
    }
    permitted(tool) {
        if (!this.bound || this.disposed || this.sessionMode === "ask" || !this.grant?.tool_names.includes(tool))
            return false;
        if (tool === "directory_query")
            return true;
        if (!this.available || !this.grant.available)
            return false;
        if (tool === "directory_inspect")
            return true;
        if (!this.grant.tool_names.includes("directory_query"))
            return false;
        if (!this.grant.writable)
            return false;
        return tool === "directory_preview" || this.sessionMode !== "plan";
    }
    sync() {
        const live = this.permitted("directory_inspect") || this.permitted("directory_preview");
        this.editor.hidden = !live;
        this.editor.disabled = this.busy || !live || this.disposed;
        this.mode.disabled = !this.permitted("directory_preview");
        this.modeRow.hidden = this.mode.value !== "create";
        this.fingerprintRow.hidden = this.mode.value !== "delete";
        for (const [button, control] of this.buttons) {
            button.disabled = this.busy || !this.bound || this.disposed || Boolean(control.tool && !this.permitted(control.tool)) || Boolean(control.enabled && !control.enabled());
        }
        this.root.setAttribute("aria-busy", String(this.busy));
    }
    button(key, action, tool, enabled) {
        const node = text("button", key);
        node.type = "button";
        node.className = "secondary";
        this.buttons.set(node, { tool, enabled });
        node.disabled = this.busy || Boolean(tool && !this.permitted(tool)) || Boolean(enabled && !enabled());
        node.onclick = () => { if (!node.disabled)
            void this.work(action); };
        return node;
    }
    clear(node) {
        for (const button of node.querySelectorAll("button"))
            this.buttons.delete(button);
        node.replaceChildren();
    }
    async read(path, init) {
        const value = await requestJson(path, init);
        if (this.disposed)
            throw new Error(t("Agent 实例不可用"));
        return value;
    }
    async work(action) {
        if (this.busy || !this.bound || this.disposed)
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
                this.renderList();
                this.renderDetail();
                this.renderPending();
                this.sync();
            }
        }
    }
    spec() {
        return this.mode.value === "create"
            ? { path: this.path.value, operation: "create", mode: Number(this.directoryMode.value) }
            : { path: this.path.value, operation: "delete", expected_fingerprint: this.fingerprint.value };
    }
    valid() {
        if (!relative(this.path.value))
            return false;
        if (this.mode.value === "delete")
            return /^[0-9a-f]{64}$/.test(this.fingerprint.value);
        const mode = Number(this.directoryMode.value);
        return this.mode.value === "create" && Number.isInteger(mode) && mode >= 448 && mode <= 511;
    }
    async inspect() {
        const path = this.path.value;
        const value = await this.read(`${this.base}/directory-state?${new URLSearchParams({ path })}`);
        if (value.path !== path || typeof value.found !== "boolean")
            throw new Error(t("目录检查回程与原路径不一致"));
        this.fingerprint.value = value.fingerprint || "";
        this.state.textContent = value.found ? `${value.path} · ${t(value.empty ? "空目录" : "非空目录")} · ${t("权限模式")} ${value.mode?.toString(8).padStart(4, "0")}` : `${value.path} · ${t("不存在")}`;
    }
    async preview() {
        if (!this.valid() || this.cache.pending || this.cacheError)
            return;
        const spec = this.spec();
        this.cache.pending = { kind: "preview", key: crypto.randomUUID(), digest: await digest(spec) };
        this.pendingMissing = false;
        this.persist();
        await this.sendPreview({ spec, request_key: this.cache.pending.key });
    }
    async sendPreview(body) {
        try {
            const proposal = await this.read(`${this.base}/directory-changes/preview?confirmed=true`, { method: "POST", body: JSON.stringify(body) });
            if (proposal.path !== body.spec.path || !IDENTIFIER.test(proposal.proposal_id))
                throw new Error(t("目录回程与原提案不一致"));
            this.cache.pending = undefined;
            this.select(proposal.proposal_id);
            this.proposal = proposal;
            this.persist();
            this.notice.textContent = t("目录提案已保存，请审阅后确认");
            this.listAfter = null;
            this.listPrevious = [];
            await this.readList();
            await this.readSelected();
            this.loaded = true;
        }
        catch (error) {
            // 明确的HTTP拒绝后先读原键；没有提案才允许修正草稿。网络断线保留原键。
            if (error instanceof ApiError) {
                try {
                    const found = await requestJsonNullable(`${this.base}/directory-changes/by-key?${new URLSearchParams({ request_key: body.request_key })}`);
                    if (!this.disposed && found === null) {
                        this.cache.pending = undefined;
                        this.persist();
                    }
                }
                catch { /* 原键无法查询时保持未知，禁止生成新意图。 */ }
            }
            throw error;
        }
    }
    select(identifier) {
        this.cache.selected = identifier;
        this.operationId = undefined;
        this.operation = undefined;
        this.historyAfter = null;
        this.historyPrevious = [];
        this.historyNext = null;
    }
    async refresh() {
        this.grant = await this.read(this.base);
        if (!this.permitted("directory_query")) {
            this.notice.textContent = t("当前Agent未授权目录历史查询");
            this.loaded = true;
            return;
        }
        await this.recoverPending();
        await this.readList();
        await this.readSelected();
        this.loaded = true;
        this.stale = false;
    }
    async recoverPending() {
        const pending = this.cache.pending;
        if (!pending)
            return;
        const path = pending.kind === "preview" ? "directory-changes" : "directory-operations";
        const key = pending.kind === "preview" ? pending.key : pending.body.request_key;
        const value = await requestJsonNullable(`${this.base}/${path}/by-key?${new URLSearchParams({ request_key: key })}`);
        if (this.disposed)
            return;
        this.pendingMissing = value === null;
        if (value === null) {
            this.notice.textContent = t("尚未找到原目录请求，请查询或明确重试原参数");
            return;
        }
        if (pending.kind === "preview") {
            const proposal = value;
            if (!IDENTIFIER.test(proposal.proposal_id))
                throw new Error(t("目录回程与原提案不一致"));
            this.select(proposal.proposal_id);
            this.proposal = proposal;
        }
        else {
            const operation = value;
            if (operation.proposal_id !== pending.body.proposal_id || operation.action !== pending.body.action)
                throw new Error(t("目录回程与原提案不一致"));
            this.select(operation.proposal_id);
            this.operationId = operation.operation_id;
            this.operation = operation;
        }
        this.cache.pending = undefined;
        this.persist();
        this.notice.textContent = t("已找回原目录请求结果");
    }
    async retryPending() {
        const pending = this.cache.pending;
        if (!pending || !this.pendingMissing)
            return;
        if (pending.kind === "preview") {
            if (!this.valid() || await digest(this.spec()) !== pending.digest)
                throw new Error(t("只能使用原目录参数重试"));
            if (!window.confirm(t("确认重试原目录请求（同键同参数）？")))
                return;
            this.pendingMissing = false;
            await this.sendPreview({ spec: this.spec(), request_key: pending.key });
        }
        else {
            const current = await this.read(`${this.base}/directory-changes/${encodeURIComponent(pending.body.proposal_id)}`);
            if (current.revision !== pending.body.expected_revision)
                throw new Error(t("目录版本已变化，请先查询原历史"));
            if (!window.confirm(t("确认重试原目录请求（同键同参数）？")))
                return;
            this.pendingMissing = false;
            await this.sendAction(pending.body);
        }
    }
    async readList() {
        const params = new URLSearchParams({ limit: String(PAGE_SIZE) });
        if (this.listAfter)
            params.set("after", this.listAfter);
        const page = await this.read(`${this.base}/directory-changes?${params}`);
        if (page.kind !== "proposals" || !Array.isArray(page.proposals) || page.proposals.length > PAGE_SIZE)
            throw new Error(t("目录查询回程类型无效"));
        this.items = page.proposals;
        this.listNext = page.next_after;
        if (!this.cache.selected && page.proposals[0]) {
            this.select(page.proposals[0].proposal_id);
            this.persist();
        }
    }
    async page(direction) {
        if (direction > 0) {
            if (!this.listNext)
                return;
            this.listPrevious.push(this.listAfter);
            this.listAfter = this.listNext;
        }
        else {
            if (!this.listPrevious.length)
                return;
            this.listAfter = this.listPrevious.pop() || null;
        }
        await this.readList();
    }
    async readSelected() {
        if (!this.cache.selected) {
            this.proposal = undefined;
            return;
        }
        const id = this.cache.selected;
        const proposal = await this.read(`${this.base}/directory-changes/${encodeURIComponent(id)}`);
        if (proposal.proposal_id !== id)
            throw new Error(t("目录回程与原提案不一致"));
        this.proposal = proposal;
        await this.readHistory();
        if (this.operationId)
            await this.readOperation(this.operationId);
        else
            this.operation = this.history.at(-1);
        this.stale = false;
    }
    async readHistory() {
        if (!this.proposal)
            return;
        const params = new URLSearchParams({ limit: String(PAGE_SIZE) });
        if (this.historyAfter)
            params.set("after", this.historyAfter);
        const page = await this.read(`${this.base}/directory-changes/${encodeURIComponent(this.proposal.proposal_id)}/history?${params}`);
        if (page.kind !== "history" || page.history.length > PAGE_SIZE || page.history.some(item => item.proposal_id !== this.proposal?.proposal_id))
            throw new Error(t("目录查询回程类型无效"));
        this.history = page.history;
        this.historyNext = page.next_after;
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
    async readOperation(id) {
        const value = await this.read(`${this.base}/directory-operations/${encodeURIComponent(id)}`);
        if (value.operation_id !== id || value.proposal_id !== this.proposal?.proposal_id)
            throw new Error(t("目录回程与原提案不一致"));
        this.operationId = id;
        this.operation = value;
    }
    allowedAction(action, value = this.proposal) {
        if (!value || this.stale || this.cache.pending || this.cacheError)
            return false;
        return action === "apply" ? value.status === "proposed" : action === "keep" ? value.status === "applied" : ["applied", "kept"].includes(value.status);
    }
    async action(action) {
        const visible = this.proposal;
        if (!visible || !this.allowedAction(action))
            return;
        const current = await this.read(`${this.base}/directory-changes/${encodeURIComponent(visible.proposal_id)}`);
        this.proposal = current;
        if (current.revision !== visible.revision || !this.allowedAction(action, current))
            throw new Error(t("目录版本已变化，请重新审阅"));
        if (!window.confirm(t("确认{action}：{path}（修订版本{revision}）？", { action: t(actionLabels[action]), path: current.path, revision: current.revision })))
            return;
        const body = { proposal_id: current.proposal_id, action, expected_revision: current.revision, request_key: crypto.randomUUID() };
        this.cache.pending = { kind: "action", body };
        this.pendingMissing = false;
        this.persist();
        await this.sendAction(body);
    }
    async sendAction(body) {
        const value = await this.read(`${this.base}/directory-changes?confirmed=true`, { method: "POST", body: JSON.stringify(body) });
        if (value.proposal_id !== body.proposal_id || value.action !== body.action)
            throw new Error(t("目录回程与原提案不一致"));
        this.cache.pending = undefined;
        this.cache.selected = body.proposal_id;
        this.operationId = value.operation_id;
        this.operation = value;
        this.persist();
        await this.readList();
        await this.readSelected();
        this.stale = false;
    }
    async operationAction(action) {
        const operation = this.operation;
        if (!operation)
            return;
        const key = action === "reconcile" ? "确认核实原目录结果？" : this.proposal?.operation === "delete" && operation.action !== "undo"
            ? "清理删除目录的恢复材料后无法恢复原目录，是否继续？" : "确认清理原目录操作的恢复材料？";
        if (!window.confirm(t(key)))
            return;
        const value = await this.read(`${this.base}/directory-operations/${encodeURIComponent(operation.operation_id)}/${action}?confirmed=true`, { method: "POST" });
        if (value.operation_id !== operation.operation_id)
            throw new Error(t("目录回程与原提案不一致"));
        this.operation = value;
        this.operationId = value.operation_id;
        await this.readSelected();
    }
    renderList() {
        this.clear(this.list);
        this.pageInfo.textContent = t("目录提案页 {page}", { page: this.listPrevious.length + 1 });
        if (!this.items.length) {
            this.list.append(text("li", "暂无目录提案"));
            return;
        }
        for (const item of this.items) {
            const row = document.createElement("li");
            row.dataset.proposalId = item.proposal_id;
            const button = this.button("查看目录提案", async () => { this.select(item.proposal_id); this.persist(); await this.readSelected(); }, "directory_query");
            button.removeAttribute("data-i18n");
            button.textContent = `${item.path} · ${t(labels[item.status] || item.status)} · r${item.revision}`;
            row.append(button);
            this.list.append(row);
        }
    }
    renderDetail() {
        this.clear(this.detail);
        const proposal = this.proposal;
        if (!proposal)
            return;
        this.detail.dataset.proposalId = proposal.proposal_id;
        const title = document.createElement("strong");
        title.textContent = `${proposal.path} · ${t(labels[proposal.status] || proposal.status)}`;
        const condition = document.createElement("pre");
        condition.className = "directory-condition";
        condition.textContent = `${t(proposal.operation === "create" ? "新建目录" : "删除空目录")}\n${t("修订版本")}: ${proposal.revision}\n${t("原目录指纹")}: ${proposal.before_fingerprint || t("不存在")}\n${t("新目录权限（十进制）")}: ${proposal.mode ?? "—"}`;
        this.detail.append(title, condition);
        const controls = document.createElement("div");
        controls.className = "actions";
        for (const action of ["apply", "keep", "undo"])
            controls.append(this.button(actionLabels[action], () => this.action(action), "directory_change", () => this.allowedAction(action)));
        this.detail.append(controls);
        if (this.operation) {
            const result = document.createElement("div");
            result.className = "directory-operation";
            result.dataset.operationId = this.operation.operation_id;
            const status = document.createElement("strong");
            status.textContent = t(labels[this.operation.status] || this.operation.status);
            result.append(status);
            const metadata = document.createElement("p");
            const receipt = this.operation.receipt;
            metadata.textContent = `${this.operation.operation_id}\n${t("原目录指纹")}: ${receipt?.before_fingerprint || "—"}\n${t("结果目录指纹")}: ${receipt?.after_fingerprint || "—"}${receipt?.error_code ? `\n${receipt.error_code}` : ""}`;
            result.append(metadata);
            if (this.operation.status === "uncertain")
                result.append(this.button("核实目录结果", () => this.operationAction("reconcile"), "directory_reconcile"));
            if (["done", "rejected"].includes(this.operation.status) && this.operation.cleanup_pending)
                result.append(this.button("清理目录恢复材料", () => this.operationAction("finalize"), "directory_finalize"));
            this.detail.append(result);
        }
        const pages = document.createElement("div");
        pages.className = "actions";
        pages.append(this.button("较早目录操作", () => this.historyPage(-1), "directory_query", () => this.historyPrevious.length > 0), this.button("较晚目录操作", () => this.historyPage(1), "directory_query", () => this.historyNext !== null));
        const history = document.createElement("ol");
        history.className = "directory-history";
        for (const item of this.history) {
            const row = document.createElement("li");
            row.dataset.operationId = item.operation_id;
            const meta = document.createElement("span");
            meta.textContent = `${t(actionLabels[item.action])} · ${t(labels[item.status] || item.status)} · ${localDate(new Date(item.created_at * 1000).toISOString())}`;
            row.append(meta, this.button("查看操作回执", () => this.readOperation(item.operation_id), "directory_query"));
            history.append(row);
        }
        this.detail.append(pages, history);
    }
    renderPending() {
        this.clear(this.pendingBox);
        this.pendingBox.hidden = !this.cache.pending && !this.cacheError;
        if (this.cacheError) {
            this.pendingBox.append(text("p", "本地恢复记录无效，请查询目录历史"));
            return;
        }
        const pending = this.cache.pending;
        if (!pending)
            return;
        const key = pending.kind === "preview" ? pending.key : pending.body.request_key;
        this.pendingBox.append(text("p", "目录响应未确认，请查询原请求键"));
        const id = document.createElement("code");
        id.textContent = key;
        this.pendingBox.append(id);
        this.pendingBox.append(this.button("查询原目录请求", async () => { await this.recoverPending(); await this.readList(); await this.readSelected(); }, "directory_query"));
        this.pendingBox.append(this.button("明确重试原目录请求", () => this.retryPending(), pending.kind === "preview" ? "directory_preview" : "directory_change", () => this.pendingMissing));
    }
    persist() {
        try {
            sessionStorage.setItem(this.storageKey, JSON.stringify(this.cache));
        }
        catch {
            this.cacheError = true;
            throw new Error(t("浏览器无法保存目录请求键，尚未发送新请求"));
        }
    }
    readCache() {
        try {
            const raw = sessionStorage.getItem(this.storageKey);
            if (!raw)
                return;
            if (raw.length > 4096)
                throw new Error();
            const value = JSON.parse(raw);
            if (!keys(value, ["version", "selected", "pending"]) || value.version !== 1 || value.selected !== undefined && !identifier(value.selected))
                throw new Error();
            if (value.pending) {
                const pending = value.pending;
                if (pending.kind === "preview") {
                    if (!keys(pending, ["kind", "key", "digest"]) || !identifier(pending.key) || typeof pending.digest !== "string" || !/^[0-9a-f]{64}$/.test(pending.digest))
                        throw new Error();
                }
                else if (pending.kind === "action") {
                    const body = pending.body;
                    if (!keys(pending, ["kind", "body"]) || !keys(body, ["proposal_id", "action", "expected_revision", "request_key"]) || !identifier(body.proposal_id) || !identifier(body.request_key) || !["apply", "undo", "keep"].includes(body.action) || !Number.isSafeInteger(body.expected_revision) || body.expected_revision < 0)
                        throw new Error();
                    value.selected = body.proposal_id;
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
