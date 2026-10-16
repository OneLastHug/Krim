/** 同一Agent的缺失回退；写意图先落盘，POST候选只由原键GET确认。 */
import { ApiError, errorText, requestJson } from "./api.js";
import { currentLocale, t } from "./i18n.js";
import { exact, restoreId, restoreKey, restorePath, verifyRestoreView } from "./missing-directory-restore-contract.js";
import { rollbackFail, rollbackSha, verifyRollbackAttempt, verifyRollbackAttemptTransition, verifyRollbackProposal, verifyRollbackProposalTransition, verifyRollbackQuery } from "./missing-directory-rollback-contract.js";
import { readRollbackRecovery, saveRollbackRecovery } from "./missing-directory-rollback-recovery.js";
const labels = { awaiting_approval: "缺失回退待确认", preserving: "缺失回退保留中", preserved: "当前整树已保留", restoring: "缺失回退还原中", restored: "保留整树已还原", keeping: "缺失回退保留核实中", kept: "缺失回退保留已确认", rejected: "缺失回退已拒绝", uncertain: "缺失回退结果待核实", cancelled: "缺失回退已取消" };
const actionLabels = { return_to_absence: "完整保留并回到缺失", restore_tree: "还原保留的完整树", keep: "确认保留当前整树" };
function text(tag, key) { const element = document.createElement(tag); element.textContent = t(key); element.dataset.i18n = key; return element; }
export class MissingDirectoryRollbackUI {
    report;
    sessionId;
    root = document.createElement("details");
    editor = document.createElement("fieldset");
    plan = document.createElement("input");
    preserve = document.createElement("input");
    lookup = document.createElement("input");
    sourceBox = document.createElement("pre");
    notice = document.createElement("p");
    list = document.createElement("ol");
    detail = document.createElement("div");
    pendingBox = document.createElement("div");
    buttons = new Map();
    grant;
    identity;
    recovery;
    source;
    inspection;
    proposal;
    operation;
    known = new Map();
    knownOperations = new Map();
    items = [];
    history = [];
    after = null;
    next = null;
    back = [];
    historyAfter = null;
    historyNext = null;
    historyBack = [];
    busy = false;
    disposed = false;
    storageError = false;
    stale = true;
    loaded = false;
    bound = false;
    available = false;
    mode = "agent";
    stopping = false;
    locale = currentLocale();
    timer;
    stop = document.createElement("button");
    base;
    expected;
    constructor(agentId, report, sessionId, binding) {
        this.report = report;
        this.sessionId = sessionId;
        if (restoreId(sessionId) && exact(binding, ["key", "version", "fingerprint"]) && typeof binding.key === "string" && rollbackSha(binding.fingerprint))
            this.expected = { scope_id: `session:${sessionId}`, workspace_key: binding.key, resource_fingerprint: binding.fingerprint };
        this.base = `/v1/agents/${encodeURIComponent(agentId)}/workspace`;
        this.root.className = "missing-directory-rollbacks";
        this.root.hidden = true;
        this.root.append(text("summary", "缺失基线回退与历史"));
        this.editor.append(text("legend", "审阅当前目录与完整保留位置"));
        this.field("原缺失恢复计划标识", this.plan, this.editor);
        this.plan.maxLength = 36;
        this.field("当前整树保留相对路径", this.preserve, this.editor);
        this.preserve.maxLength = 512;
        const prepare = document.createElement("div");
        prepare.className = "actions";
        prepare.append(this.button("读取原缺失恢复与当前版本", () => this.loadSource(), "tree_move_inspect", () => restoreId(this.plan.value) && !this.pending()), this.button("预览缺失基线回退", () => this.preview(), "missing_directory_rollback_preview", () => this.canPreview()));
        this.editor.append(this.sourceBox, prepare);
        this.notice.setAttribute("role", "status");
        this.notice.setAttribute("aria-live", "polite");
        const navigation = document.createElement("div");
        navigation.className = "actions";
        this.field("缺失回退原请求键", this.lookup, navigation);
        this.lookup.maxLength = 128;
        navigation.append(this.button("按键查看缺失回退提案", () => this.find("by_key"), "missing_directory_rollback_query", () => restoreKey(this.lookup.value) && !this.pending()), this.button("按键查看缺失回退操作", () => this.find("operation_by_key"), "missing_directory_rollback_query", () => restoreKey(this.lookup.value) && !this.pending()), this.button("刷新缺失回退历史", () => this.refresh(), "missing_directory_rollback_query"), this.button("上一页缺失回退提案", () => this.page(-1), "missing_directory_rollback_query", () => this.back.length > 0), this.button("下一页缺失回退提案", () => this.page(1), "missing_directory_rollback_query", () => this.next !== null));
        this.list.className = "missing-rollback-list";
        this.detail.className = "missing-rollback-detail";
        this.sourceBox.className = "missing-rollback-source";
        this.pendingBox.className = "missing-rollback-pending";
        this.stop = text("button", "取消进行中的缺失回退");
        this.stop.type = "button";
        this.stop.className = "secondary";
        this.stop.onclick = () => void this.interrupt();
        this.root.append(this.editor, this.notice, this.pendingBox, this.stop, navigation, this.list, this.detail);
        this.plan.oninput = () => { this.source = undefined; this.inspection = undefined; this.render(); };
        this.preserve.oninput = this.lookup.oninput = () => this.sync();
        this.root.ontoggle = () => { if (this.root.open && !this.loaded)
            void this.work(() => this.refresh()); if (!this.root.open)
            this.clearTimer(); };
        this.render();
    }
    dispose() { this.disposed = true; this.clearTimer(); this.sync(); }
    update(bound, available, mode) { this.bound = bound; this.available = available; this.mode = String(mode); this.root.hidden = !bound; if (this.locale !== currentLocale()) {
        this.locale = currentLocale();
        this.render();
    } this.sync(); }
    async useSource(planId) { if (!restoreId(planId))
        rollbackFail(); this.root.open = true; await this.work(async () => { if (!this.loaded)
        await this.refresh(); if (this.pending())
        rollbackFail(); this.plan.value = planId; await this.loadSource(); }); this.root.scrollIntoView({ block: "nearest" }); }
    field(label, input, parent) { const row = document.createElement("label"); row.append(text("span", label), input); input.setAttribute("aria-label", t(label)); input.dataset.i18nAriaLabel = label; parent.append(row); }
    pending() { return this.recovery?.pending ?? null; }
    permitted(tool) { if (this.disposed || !this.bound || this.mode === "ask" || !this.grant?.tool_names.includes(tool))
        return false; if (tool === "missing_directory_rollback_query")
        return true; if (!this.available || !this.grant.available)
        return false; if (tool === "tree_move_inspect")
        return true; return this.grant.writable && (tool === "missing_directory_rollback_preview" || this.mode !== "plan"); }
    writable() { return !this.pending() && !this.storageError && !this.stale; }
    canPreview() { return this.writable() && this.source?.plan_id === this.plan.value && this.inspection?.found === true && restorePath(this.preserve.value) && this.preserve.value !== this.source.root_path && !this.preserve.value.startsWith(this.source.root_path + "/") && !this.source.root_path.startsWith(this.preserve.value + "/"); }
    button(label, action, tool, ready = () => true) { const button = text("button", label); button.type = "button"; button.className = "secondary"; this.buttons.set(button, { tool, ready }); button.onclick = () => { if (!button.disabled)
        void this.work(action); }; return button; }
    clear(element) { for (const button of element.querySelectorAll("button"))
        this.buttons.delete(button); element.replaceChildren(); }
    sync() { this.editor.disabled = this.busy || this.disposed; for (const [button, control] of this.buttons)
        button.disabled = this.busy || !this.permitted(control.tool) || !control.ready(); this.stop.hidden = this.pending()?.kind !== "change"; this.stop.disabled = this.stopping || this.storageError || !this.permitted("missing_directory_rollback_cancel"); this.root.setAttribute("aria-busy", String(this.busy)); }
    async work(action) { if (this.busy || this.disposed || !this.bound)
        return; this.busy = true; this.sync(); try {
        await action();
    }
    catch (error) {
        if (!this.disposed) {
            this.stale = true;
            this.notice.textContent = t(errorText(error));
            this.report(t(errorText(error)), true);
        }
    }
    finally {
        this.busy = false;
        if (!this.disposed) {
            this.render();
            this.schedule();
        }
    } }
    async read(path, options) { const result = await requestJson(path, options); if (this.disposed)
        throw new Error(t("Agent 实例不可用")); return result; }
    save() { if (this.storageError || !this.identity || !this.recovery)
        throw new Error(t("缺失基线回退记录不可用，写操作已停用")); try {
        saveRollbackRecovery(this.sessionId, this.recovery, this.identity);
    }
    catch (error) {
        this.storageError = true;
        throw error;
    } }
    acceptProposal(value) { verifyRollbackProposal(value, this.identity); const old = this.known.get(value.move.proposal_id); if (old)
        verifyRollbackProposalTransition(old, value); this.known.set(value.move.proposal_id, structuredClone(value)); }
    acceptOperation(value, proposal) { verifyRollbackAttempt(value, this.identity, proposal ?? this.known.get(value.operation.proposal_id)); const old = this.knownOperations.get(value.operation.operation_id); if (old)
        verifyRollbackAttemptTransition(old, value); this.knownOperations.set(value.operation.operation_id, structuredClone(value)); }
    async query(request) { const params = new URLSearchParams(); for (const [key, value] of Object.entries(request))
        if (value !== undefined)
            params.set(key, String(value)); const value = await this.read(`${this.base}/missing-directory-rollbacks/query?${params}`); verifyRollbackQuery(value, this.identity, request); for (const proposal of [...(value.proposals?.items ?? []), ...(value.proposal ? [value.proposal] : [])])
        this.acceptProposal(proposal); for (const operation of [...(value.history?.items ?? []), ...(value.operation ? [value.operation] : [])])
        this.acceptOperation(operation); return value; }
    async refresh() {
        const grant = await this.read(this.base);
        if (!this.expected || grant.key !== this.expected.workspace_key || grant.fingerprint !== this.expected.resource_fingerprint || typeof grant.available !== "boolean" || typeof grant.writable !== "boolean" || !Array.isArray(grant.tool_names) || grant.tool_names.some(name => typeof name !== "string") || new Set(grant.tool_names).size !== grant.tool_names.length)
            rollbackFail();
        this.grant = grant;
        if (!this.identity) {
            this.identity = { ...this.expected };
            try {
                this.recovery = readRollbackRecovery(this.sessionId, this.identity);
            }
            catch {
                this.storageError = true;
                this.recovery = { version: 1, ...this.identity, selected_id: null, pending: null };
            }
        }
        if (!this.permitted("missing_directory_rollback_query")) {
            this.loaded = true;
            this.notice.textContent = t("当前会话未授权缺失回退查询");
            return;
        }
        if (this.pending())
            await this.recover();
        const page = await this.query({ kind: "proposals", limit: 4, ...(this.after ? { after: this.after } : {}) });
        this.items = page.proposals.items;
        this.next = page.proposals.next_after;
        await this.selected();
        this.loaded = true;
        this.stale = false;
    }
    async loadSource() {
        this.source = undefined;
        this.inspection = undefined;
        const source = await this.read(`${this.base}/missing-directory-restores/${encodeURIComponent(this.plan.value)}`);
        verifyRestoreView(source, this.identity);
        if (source.plan_id !== this.plan.value || source.phase !== "finished" || !source.receipt?.after_fingerprint || !source.baseline)
            rollbackFail();
        const inspected = await this.read(`${this.base}/tree-state?${new URLSearchParams({ path: source.root_path })}`);
        if (inspected.path !== source.root_path || inspected.found !== true || !rollbackSha(inspected.fingerprint) || !Number.isSafeInteger(inspected.entry_count) || inspected.entry_count < 1 || inspected.entry_count > 16384 || !Number.isSafeInteger(inspected.byte_size) || inspected.byte_size < 0 || inspected.byte_size > 134217728 || inspected.entry_count !== inspected.file_count + inspected.directory_count)
            rollbackFail();
        this.source = source;
        this.inspection = inspected;
        this.stale = false;
    }
    select(id) { this.recovery.selected_id = id; this.proposal = undefined; this.operation = undefined; this.history = []; this.historyAfter = null; this.historyNext = null; this.historyBack = []; if (!this.storageError)
        this.save(); }
    async selected() { const id = this.recovery?.selected_id; if (!id)
        return; this.proposal = (await this.query({ kind: "proposal", identifier: id })).proposal; await this.readHistory(); if (this.proposal.move.active_operation_id)
        this.operation = (await this.query({ kind: "operation", identifier: this.proposal.move.active_operation_id })).operation; }
    async readHistory() { if (!this.proposal)
        return; const result = await this.query({ kind: "history", identifier: this.proposal.move.proposal_id, limit: 4, ...(this.historyAfter ? { after: this.historyAfter } : {}) }); for (const value of result.history.items)
        this.acceptOperation(value, this.proposal); this.history = result.history.items; this.historyNext = result.history.next_after; }
    async page(direction) { if (direction > 0) {
        if (this.next === null)
            return;
        this.back.push(this.after);
        this.after = this.next;
    }
    else {
        if (!this.back.length)
            return;
        this.after = this.back.pop();
    } await this.refresh(); }
    async historyPage(direction) { if (direction > 0) {
        if (this.historyNext === null)
            return;
        this.historyBack.push(this.historyAfter);
        this.historyAfter = this.historyNext;
    }
    else {
        if (!this.historyBack.length)
            return;
        this.historyAfter = this.historyBack.pop();
    } await this.readHistory(); }
    async find(kind) { const result = await this.query({ kind, request_key: this.lookup.value }); const id = result.proposal?.move.proposal_id ?? result.operation?.operation.proposal_id; if (!id) {
        this.notice.textContent = t("未找到原缺失回退请求，请保留原键继续查询");
        return;
    } this.select(id); await this.selected(); if (result.operation) {
        this.acceptOperation(result.operation, this.proposal);
        this.operation = result.operation;
    } this.stale = false; }
    async preview() { if (!this.canPreview() || !this.source?.baseline)
        return; await this.send({ kind: "preview", request: { plan_id: this.source.plan_id, preserve_path: this.preserve.value, expected_fingerprint: this.inspection.fingerprint, request_key: crypto.randomUUID(), ttl_seconds: 1800 }, source_operation_id: this.source.operation_id, root_path: this.source.root_path, published_fingerprint: this.source.receipt.after_fingerprint, absence_fingerprint: this.source.baseline.fingerprint, rejected: false }); }
    async change(action) { const proposal = this.proposal; if (!proposal || !this.writable() || !window.confirm(`${t(actionLabels[action])}\n${proposal.move.source_path} → ${proposal.move.target_path} · r${proposal.move.revision}\n${proposal.approved_fingerprint}\n${t("确认按当前审阅版本执行缺失回退？")}`))
        return; await this.send({ kind: "change", request: { proposal_id: proposal.move.proposal_id, action, expected_revision: proposal.move.revision, request_key: crypto.randomUUID() }, proposal, rejected: false }); }
    async control(kind) { if (!this.proposal || !this.writable() || !window.confirm(t(kind === "cancel" ? "确认取消原缺失回退操作？" : "确认核实原缺失回退结果？")))
        return; if (kind === "reconcile") {
        if (!this.operation)
            return;
        await this.send({ kind, proposal: this.proposal, attempt: this.operation, rejected: false });
    }
    else
        await this.send({ kind, proposal: this.proposal, rejected: false }); }
    matchProposal(value, pending) { verifyRollbackProposal(value, this.identity); if (pending.kind === "preview") {
        if (value.source_plan_id !== pending.request.plan_id || value.source_operation_id !== pending.source_operation_id || value.move.source_path !== pending.root_path || value.move.target_path !== pending.request.preserve_path || value.approved_fingerprint !== pending.request.expected_fingerprint || value.published_fingerprint !== pending.published_fingerprint || value.baseline.fingerprint !== pending.absence_fingerprint)
            rollbackFail();
    }
    else
        verifyRollbackProposalTransition(pending.proposal, value); }
    matchAttempt(value, pending) { this.acceptOperation(value, pending.proposal); if (pending.kind === "change") {
        if (value.action !== pending.request.action || value.operation.proposal_id !== pending.request.proposal_id || value.operation.expected_revision !== pending.request.expected_revision)
            rollbackFail();
    }
    else {
        if (value.operation.operation_id !== pending.attempt.operation.operation_id)
            rollbackFail();
        verifyRollbackAttemptTransition(pending.attempt, value);
    } }
    async send(pending) {
        this.recovery.pending = structuredClone(pending);
        this.save();
        this.render();
        let returned = false;
        try {
            const suffix = pending.kind === "preview" ? "/preview" : pending.kind === "change" ? "/change" : pending.kind === "cancel" ? `/${pending.proposal.move.proposal_id}/cancel` : `/operations/${pending.attempt.operation.operation_id}/reconcile`;
            const candidate = await this.read(`${this.base}/missing-directory-rollbacks${suffix}?confirmed=true`, { method: "POST", ...("request" in pending ? { body: JSON.stringify(pending.request) } : {}) });
            returned = true;
            if (pending.kind === "preview" || pending.kind === "cancel")
                this.matchProposal(candidate, pending);
            else
                this.matchAttempt(candidate, pending);
            await this.recover(candidate);
        }
        catch (error) {
            if (!returned && error instanceof ApiError && error.status >= 400 && error.status < 500) {
                this.recovery.pending.rejected = true;
                this.save();
            }
            this.notice.textContent = t("原缺失回退请求尚未确认，请按原键继续查询");
            this.stale = true;
            this.report(t(errorText(error)), true);
        }
        await this.refresh();
    }
    async recover(candidate) {
        const pending = this.pending();
        if (!pending)
            return;
        let proposal = null, attempt = null;
        if (pending.kind === "preview")
            proposal = (await this.query({ kind: "by_key", request_key: pending.request.request_key })).proposal;
        else {
            this.acceptProposal(pending.proposal);
            if (pending.kind === "cancel")
                proposal = (await this.query({ kind: "proposal", identifier: pending.proposal.move.proposal_id })).proposal;
            else
                attempt = (await this.query(pending.kind === "change" ? { kind: "operation_by_key", request_key: pending.request.request_key } : { kind: "operation", identifier: pending.attempt.operation.operation_id })).operation;
        }
        if (!proposal && !attempt) {
            if (pending.rejected) {
                this.recovery.pending = null;
                this.save();
            }
            else
                this.notice.textContent = t("未找到原缺失回退请求，请保留原键继续查询");
            return;
        }
        if (proposal) {
            this.matchProposal(proposal, pending);
            if (candidate)
                verifyRollbackProposalTransition(candidate, proposal);
            if (pending.kind === "cancel" && ["awaiting_approval", "preserving", "restoring", "keeping"].includes(proposal.rollback_state))
                return;
        }
        if (attempt && (pending.kind === "change" || pending.kind === "reconcile")) {
            this.matchAttempt(attempt, pending);
            if (candidate)
                verifyRollbackAttemptTransition(candidate, attempt);
            if (attempt.operation.status === "running")
                return;
            this.inspection = undefined;
        }
        this.recovery.pending = null;
        this.select(proposal?.move.proposal_id ?? attempt.operation.proposal_id);
        this.operation = attempt ?? undefined;
        this.save();
        this.notice.textContent = "";
    }
    async interrupt() { const pending = this.pending(); if (pending?.kind !== "change" || this.stopping || this.stop.disabled || !window.confirm(t("确认取消原缺失回退操作？")))
        return; this.stopping = true; this.sync(); try {
        const value = await this.read(`${this.base}/missing-directory-rollbacks/${pending.request.proposal_id}/cancel?confirmed=true`, { method: "POST" });
        this.matchProposal(value, pending);
        this.notice.textContent = t("原缺失回退取消已请求，请读取原操作");
        if (!this.busy)
            await this.work(() => this.refresh());
    }
    catch (error) {
        if (!this.disposed)
            this.report(t(errorText(error)), true);
    }
    finally {
        this.stopping = false;
        if (!this.disposed)
            this.sync();
    } }
    clearTimer() { if (this.timer !== undefined)
        window.clearTimeout(this.timer); this.timer = undefined; }
    schedule() { this.clearTimer(); if (!this.disposed && this.root.open && (this.pending() || this.proposal?.move.active_operation_id))
        this.timer = window.setTimeout(() => void this.work(() => this.refresh()), 1000); }
    render() {
        this.sourceBox.textContent = this.source ? `${t("原缺失恢复已完成")}: ${this.source.plan_id}\n${t("原恢复操作")}: ${this.source.operation_id}\n${t("原发布指纹")}: ${this.source.receipt.after_fingerprint}\n${t("当前批准指纹")}: ${this.inspection?.fingerprint ?? "—"}\n${this.source.root_path} · ${this.inspection?.entry_count ?? 0} · ${this.inspection?.byte_size ?? 0}` : "";
        this.clear(this.list);
        for (const proposal of this.items) {
            const item = document.createElement("li");
            const button = this.button("查看缺失回退提案", async () => { this.select(proposal.move.proposal_id); await this.selected(); this.stale = false; }, "missing_directory_rollback_query", () => !this.pending());
            button.removeAttribute("data-i18n");
            button.textContent = `${proposal.move.source_path} → ${proposal.move.target_path} · ${t(labels[proposal.rollback_state])}`;
            item.dataset.proposalId = proposal.move.proposal_id;
            item.append(button);
            this.list.append(item);
        }
        this.clear(this.detail);
        delete this.detail.dataset.proposalId;
        delete this.detail.dataset.rollbackState;
        const proposal = this.proposal;
        if (proposal) {
            this.detail.dataset.proposalId = proposal.move.proposal_id;
            this.detail.dataset.rollbackState = proposal.rollback_state;
            const summary = document.createElement("pre");
            summary.textContent = `${t(labels[proposal.rollback_state])} · r${proposal.move.revision}\n${t("原缺失恢复已完成")}: ${proposal.source_plan_id}\n${t("原恢复操作")}: ${proposal.source_operation_id}\nSHA-256: ${proposal.source_hash}\n${t("原缺失基线")}: absent · ${proposal.baseline.fingerprint}\n${t("原发布指纹")}: ${proposal.published_fingerprint}\n${t("当前批准指纹")}: ${proposal.approved_fingerprint}\n${proposal.move.source_path} → ${proposal.move.target_path}\n${t("树提案规模：{entries}节点、{bytes}字节", { entries: proposal.move.entry_count, bytes: proposal.move.byte_size })}`;
            this.detail.append(summary, text("p", "当前目录的新增文件和二进制变化将完整保留"));
            for (const action of ["return_to_absence", "keep", "restore_tree"])
                this.detail.append(this.button(actionLabels[action], () => this.change(action), "missing_directory_rollback_change", () => this.writable() && (action === "return_to_absence" ? proposal.rollback_state === "awaiting_approval" && proposal.move.expires_at > Date.now() / 1000 : action === "keep" ? proposal.rollback_state === "preserved" : ["preserved", "kept"].includes(proposal.rollback_state))));
            this.detail.append(this.button("取消缺失回退操作", () => this.control("cancel"), "missing_directory_rollback_cancel", () => this.writable() && ["awaiting_approval", "preserving", "restoring", "keeping"].includes(proposal.rollback_state)));
            const history = document.createElement("ol");
            history.className = "missing-rollback-history";
            for (const attempt of this.history) {
                const item = document.createElement("li");
                item.dataset.operationId = attempt.operation.operation_id;
                const open = this.button("查看缺失回退操作", async () => { const result = await this.query({ kind: "operation", identifier: attempt.operation.operation_id }); this.acceptOperation(result.operation, this.proposal); this.operation = result.operation; }, "missing_directory_rollback_query");
                open.removeAttribute("data-i18n");
                open.textContent = `${attempt.operation.operation_id} · ${t(actionLabels[attempt.action])} · ${attempt.operation.status}`;
                item.append(open);
                history.append(item);
            }
            this.detail.append(history, this.button("上一页缺失回退操作", () => this.historyPage(-1), "missing_directory_rollback_query", () => this.historyBack.length > 0), this.button("下一页缺失回退操作", () => this.historyPage(1), "missing_directory_rollback_query", () => this.historyNext !== null));
            if (this.operation) {
                const receipt = document.createElement("pre");
                receipt.className = "missing-rollback-receipt";
                receipt.textContent = `${this.operation.operation.operation_id} · ${t(actionLabels[this.operation.action])} · ${this.operation.operation.status}\n${this.operation.operation.receipt?.after_fingerprint ?? "—"}\n${this.operation.operation.receipt?.error_code ?? ""}`;
                this.detail.append(receipt, this.button("核实原缺失回退结果", () => this.control("reconcile"), "missing_directory_rollback_reconcile", () => this.writable() && this.operation?.operation.status === "uncertain"));
            }
        }
        this.clear(this.pendingBox);
        if (this.storageError)
            this.pendingBox.append(text("p", "缺失基线回退记录不可用，写操作已停用"));
        const pending = this.pending();
        if (pending) {
            const original = document.createElement("code");
            original.textContent = "request" in pending ? pending.request.request_key : pending.kind === "reconcile" ? pending.attempt.operation.operation_id : pending.proposal.move.proposal_id;
            this.pendingBox.append(text("p", "原缺失回退请求尚未确认，请按原键继续查询"), original, this.button("查询待确认缺失回退请求", () => this.refresh(), "missing_directory_rollback_query"));
        }
        this.sync();
    }
}
