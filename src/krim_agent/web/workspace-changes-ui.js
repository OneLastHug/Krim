/** 文件编辑只产生受控提案；Diff、revision和回执均读取后端账本。 */
import { errorText, requestJson, } from "./api.js";
import { currentLocale, localDate, t } from "./i18n.js";
const PAGE_SIZE = 20;
const labels = {
    proposed: "待确认修改", applying: "文件应用中", applied: "文件已应用", undoing: "文件撤销中",
    undone: "文件已撤销", keeping: "保留确认中", kept: "文件已保留", rejected: "文件操作已拒绝",
    uncertain: "文件结果待核实", running: "文件操作执行中", done: "文件操作已确认",
    apply: "应用修改", undo: "撤销修改", keep: "保留修改",
};
function text(tag, key) {
    const element = document.createElement(tag);
    element.dataset.i18n = key;
    element.textContent = t(key);
    return element;
}
function aria(element, key) {
    element.setAttribute("aria-label", t(key));
    element.setAttribute("data-i18n-aria-label", key);
}
function statusLabel(value) { return t(Object.hasOwn(labels, value) ? labels[value] : value); }
export class WorkspaceChangesUI {
    notice;
    root = document.createElement("details");
    editor = document.createElement("fieldset");
    mode = document.createElement("select");
    path = document.createElement("input");
    sha = document.createElement("input");
    content = document.createElement("textarea");
    oldText = document.createElement("textarea");
    newText = document.createElement("textarea");
    occurrences = document.createElement("input");
    sourceNote = document.createElement("p");
    noticeBox = document.createElement("p");
    listing = document.createElement("div");
    detail = document.createElement("div");
    pageInfo = document.createElement("small");
    fieldRows = new Map();
    buttons = [];
    grant;
    bound = false;
    available = false;
    sessionMode = "agent";
    busy = false;
    offset = 0;
    itemCount = 0;
    items = [];
    selected;
    history = [];
    operation;
    operationSelection;
    proof;
    proofError = "";
    historyLimit = PAGE_SIZE;
    stale = false;
    loaded = false;
    source;
    draftVersion = 0;
    pendingPreview;
    pendingAction;
    base;
    locale = currentLocale();
    constructor(agentId, notice) {
        this.notice = notice;
        this.base = `/v1/agents/${encodeURIComponent(agentId)}/workspace`;
        this.root.className = "workspace-changes";
        this.root.append(text("summary", "文件修改与历史"));
        this.editor.className = "workspace-editor";
        this.editor.append(text("legend", "准备文件修改"));
        for (const [value, label] of [["create", "新建文件"], ["write", "完整写入"], ["replace", "精确文本替换"], ["delete", "删除文件"]]) {
            const option = text("option", label);
            option.value = value;
            this.mode.append(option);
        }
        this.path.maxLength = 512;
        this.sha.maxLength = 64;
        this.sha.spellcheck = false;
        this.content.rows = 6;
        this.content.maxLength = 24000;
        this.oldText.rows = this.newText.rows = 3;
        this.oldText.maxLength = this.newText.maxLength = 8192;
        this.occurrences.type = "number";
        this.occurrences.min = "1";
        this.occurrences.max = "1000";
        this.occurrences.value = "1";
        for (const [key, element] of [
            ["修改方式", this.mode], ["修改文件路径", this.path], ["原文件SHA-256", this.sha],
            ["完整新内容", this.content], ["待替换原文", this.oldText], ["替换为", this.newText],
            ["预期出现次数", this.occurrences],
        ])
            this.field(key, element);
        const controls = document.createElement("div");
        controls.className = "actions";
        controls.append(this.button("载入原文件", () => this.loadSource(), "workspace_read", () => this.mode.value !== "create"), this.button("预览文件修改", () => this.preview(), "workspace_preview", () => this.validDraft()));
        this.editor.append(this.sourceNote, controls);
        this.noticeBox.setAttribute("role", "status");
        this.noticeBox.className = "workspace-change-status";
        const pages = document.createElement("div");
        pages.className = "actions";
        pages.append(this.button("刷新修改历史", () => this.refresh()), this.button("上一页", () => this.changePage(-1), undefined, () => this.offset > 0), this.button("下一页", () => this.changePage(1), undefined, () => this.itemCount === PAGE_SIZE));
        this.listing.className = "workspace-change-list";
        this.detail.className = "workspace-change-detail";
        this.root.append(this.editor, this.noticeBox, pages, this.pageInfo, this.listing, this.detail);
        for (const input of [this.path, this.sha, this.content, this.oldText, this.newText, this.occurrences])
            input.oninput = () => {
                this.draftVersion += 1;
                if (input === this.path) {
                    this.sha.value = "";
                    this.source = undefined;
                    this.sourceNote.textContent = "";
                }
                this.sync();
            };
        this.mode.onchange = () => { this.draftVersion += 1; this.sync(); };
        this.root.ontoggle = () => { if (this.root.open && !this.loaded && this.bound)
            void this.work(() => this.refresh()); };
        this.sync();
    }
    field(key, element) {
        const row = document.createElement("label");
        row.append(text("span", key), element);
        aria(element, key);
        this.fieldRows.set(key, row);
        this.editor.append(row);
    }
    update(bound, available, mode, grant) {
        const reopened = !this.available && available;
        this.bound = bound;
        this.available = available;
        this.root.hidden = !bound;
        this.sessionMode = typeof mode === "string" ? mode : "agent";
        if (grant)
            this.grant = grant;
        if (this.locale !== currentLocale()) {
            this.locale = currentLocale();
            this.renderList();
            this.renderDetail();
        }
        this.sync();
        if (reopened && this.root.open && this.bound && !this.busy)
            void this.work(() => this.refresh());
    }
    setGrant(grant) { this.grant = grant; this.sync(); }
    /** 清单导航复用当前详情，保留编辑器及其未提交草稿。 */
    async focusProposal(proposalId, operationId) {
        if (this.busy)
            throw new Error(t("文件操作处理中，请稍后打开差异"));
        if (!this.bound)
            throw new Error(t("工作区资源已停用或关闭"));
        this.root.open = true;
        await this.work(async () => {
            if (this.available) {
                try {
                    this.grant = await requestJson(this.base);
                }
                catch (error) {
                    this.grant = undefined;
                    this.noticeBox.textContent = errorText(error);
                }
            }
            const proposal = await requestJson(`${this.base}/changes/${encodeURIComponent(proposalId)}`);
            this.selected = proposal;
            this.operationSelection = operationId;
            this.operation = undefined;
            this.history = [];
            this.historyLimit = PAGE_SIZE;
            await this.readSelected();
            this.loaded = true;
            this.detail.scrollIntoView({ block: "nearest" });
        });
    }
    permitted(tool) {
        return this.available && this.grant?.available === true && this.grant.writable === true
            && this.grant.tool_names.includes(tool) && this.sessionMode !== "ask"
            && (tool === "workspace_preview" || tool === "workspace_read" || this.sessionMode !== "plan");
    }
    sync() {
        const mode = this.mode.value;
        this.editor.hidden = !this.available || !this.grant?.writable || !this.grant.tool_names.includes("workspace_preview");
        this.editor.disabled = this.busy || !this.permitted("workspace_preview");
        this.fieldRows.get("原文件SHA-256").hidden = mode === "create";
        this.fieldRows.get("完整新内容").hidden = mode !== "create" && mode !== "write";
        for (const key of ["待替换原文", "替换为", "预期出现次数"])
            this.fieldRows.get(key).hidden = mode !== "replace";
        for (const control of this.buttons)
            control.button.disabled = this.buttonDisabled(control.tool, control.allowed);
        this.root.setAttribute("aria-busy", String(this.busy));
    }
    buttonDisabled(tool, allowed) {
        return this.busy || !this.bound || Boolean(tool && !this.permitted(tool)) || Boolean(allowed && !allowed());
    }
    button(key, action, tool, allowed) {
        const button = text("button", key);
        button.type = "button";
        button.className = "secondary";
        // 读取尚未收束时重建的按钮也立即遵守同一状态，避免看似可点但被work忽略。
        button.disabled = this.buttonDisabled(tool, allowed);
        button.onclick = () => { if (!button.disabled)
            void this.work(action); };
        this.buttons.push({ button, tool, allowed });
        return button;
    }
    async work(action) {
        if (this.busy || !this.bound)
            return;
        this.busy = true;
        this.sync();
        try {
            await action();
        }
        catch (error) {
            this.noticeBox.textContent = errorText(error);
            this.notice(errorText(error), true);
        }
        finally {
            this.busy = false;
            this.sync();
        }
    }
    validDraft() {
        const mode = this.mode.value;
        if (!this.path.value || this.path.value === ".")
            return false;
        if (mode !== "create" && !/^[0-9a-f]{64}$/.test(this.sha.value))
            return false;
        if (mode === "write" && this.source?.path === this.path.value && this.source.sha256 === this.sha.value && this.source.truncated)
            return false;
        if (mode === "replace" && (!this.oldText.value || !Number.isInteger(Number(this.occurrences.value)) || Number(this.occurrences.value) < 1 || Number(this.occurrences.value) > 1000))
            return false;
        return true;
    }
    async loadSource() {
        if (!this.path.value || this.mode.value === "create")
            return;
        if (this.mode.value === "write" && this.content.value && !window.confirm(t("载入原文件将替换当前内容草稿，是否继续？")))
            return;
        const path = this.path.value, version = this.draftVersion;
        const source = await requestJson(`${this.base}/read?${new URLSearchParams({ path, max_lines: "1000" })}`);
        if (version !== this.draftVersion || path !== this.path.value)
            return;
        this.source = source;
        this.sha.value = source.sha256;
        if (this.mode.value === "write" && !source.truncated)
            this.content.value = source.content;
        this.sourceNote.textContent = source.truncated
            ? t("原文件展示不完整，请用精确替换或其他完整读取入口")
            : t("已载入原文件版本，应用前仍会核对文件是否变化");
    }
    async preview() {
        if (!this.validDraft())
            return;
        const mode = this.mode.value;
        const spec = { path: this.path.value, operation: mode === "create" ? "write" : mode,
            expected_sha256: mode === "create" ? null : this.sha.value };
        if (mode === "create" || mode === "write")
            spec.content = this.content.value;
        if (mode === "replace")
            spec.replacements = [{ old_text: this.oldText.value, new_text: this.newText.value, occurrences: Number(this.occurrences.value) }];
        const signature = JSON.stringify(spec);
        if (new TextEncoder().encode(signature).length > 32768)
            throw new Error(t("修改规格超过大小限制，请缩小修改内容"));
        if (this.pendingPreview?.signature !== signature)
            this.pendingPreview = { signature, request_key: crypto.randomUUID() };
        const proposal = await requestJson(`${this.base}/changes/preview?confirmed=true`, {
            method: "POST", body: JSON.stringify({ spec, request_key: this.pendingPreview.request_key }),
        });
        this.pendingPreview = undefined;
        this.selected = proposal;
        this.history = [];
        this.operation = undefined;
        this.operationSelection = undefined;
        this.stale = false;
        this.offset = 0;
        this.noticeBox.textContent = t("提案已保存，请审阅真实差异后确认");
        this.renderDetail();
        await this.readList();
        await this.readSelected();
        this.detail.scrollIntoView({ block: "nearest" });
    }
    async refresh() {
        if (this.available) {
            try {
                this.grant = await requestJson(this.base);
            }
            catch (error) {
                this.grant = undefined;
                this.noticeBox.textContent = errorText(error);
            }
        }
        await this.readList();
        await this.readSelected();
        this.loaded = true;
    }
    async changePage(direction) {
        this.offset = Math.max(0, this.offset + direction * PAGE_SIZE);
        this.selected = undefined;
        this.history = [];
        this.operation = undefined;
        this.operationSelection = undefined;
        await this.readList();
        await this.readSelected();
    }
    async readList() {
        this.items = await requestJson(`${this.base}/changes?${new URLSearchParams({ offset: String(this.offset), limit: String(PAGE_SIZE) })}`);
        this.itemCount = this.items.length;
        if (!this.selected)
            this.selected = this.items[0];
        this.renderList();
    }
    renderList() {
        this.buttons = this.buttons.filter(control => !this.listing.contains(control.button));
        this.listing.replaceChildren();
        for (const proposal of this.items) {
            const button = this.button("查看文件差异", async () => {
                this.selected = proposal;
                this.operation = undefined;
                this.operationSelection = undefined;
                this.history = [];
                this.historyLimit = PAGE_SIZE;
                await this.readSelected();
            });
            button.removeAttribute("data-i18n");
            button.textContent = `${proposal.preview.path} · ${statusLabel(proposal.status)}`;
            button.dataset.proposalId = proposal.proposal_id;
            this.listing.append(button);
        }
        this.pageInfo.textContent = this.items.length ? `${this.offset + 1}–${this.offset + this.items.length}` : t("暂无文件修改提案");
    }
    async readSelected() {
        if (!this.selected) {
            this.renderDetail();
            return;
        }
        const id = this.selected.proposal_id;
        this.stale = true;
        const [proposal, history] = await Promise.all([
            requestJson(`${this.base}/changes/${encodeURIComponent(id)}`),
            requestJson(`${this.base}/changes/${encodeURIComponent(id)}/history`),
        ]);
        if (this.selected?.proposal_id !== id)
            return;
        this.selected = proposal;
        this.history = history;
        this.operation = history.find(item => item.operation_id === this.operationSelection)
            || history.find(item => item.operation_id === proposal.active_operation_id) || history.at(-1);
        await this.readProof();
        this.stale = false;
        this.renderDetail();
    }
    async readProof() {
        this.proof = undefined;
        this.proofError = "";
        const operation = this.operation;
        if (!operation)
            return;
        try {
            this.proof = await requestJson(`${this.base}/operations/${encodeURIComponent(operation.operation_id)}/proof`);
        }
        catch (error) {
            this.proofError = errorText(error);
        }
    }
    renderDetail() {
        this.buttons = this.buttons.filter(control => !this.detail.contains(control.button));
        this.detail.replaceChildren();
        const proposal = this.selected;
        if (!proposal)
            return;
        this.detail.dataset.proposalId = proposal.proposal_id;
        const heading = document.createElement("strong");
        heading.textContent = `${proposal.preview.path} · ${statusLabel(proposal.status)}`;
        const metadata = document.createElement("p");
        metadata.className = "workspace-change-meta";
        metadata.textContent = `${t("修订版本")} ${proposal.revision} · ${localDate(proposal.created_at)}\n${t("原版本")} ${proposal.preview.before_sha256 || t("不存在")} (${proposal.preview.before_bytes} B)\n${t("新版本")} ${proposal.preview.after_sha256 || t("删除文件")} (${proposal.preview.after_bytes} B)`;
        const diff = document.createElement("pre");
        diff.className = "workspace-diff";
        diff.textContent = proposal.preview.diff || t("内容没有差异");
        this.detail.append(heading, metadata, diff);
        if (proposal.preview.truncated)
            this.detail.append(text("p", "差异已截断，请结合完整内容审阅"));
        const actions = document.createElement("div");
        actions.className = "actions";
        for (const action of ["apply", "keep", "undo"])
            actions.append(this.button(labels[action], () => this.act(action), "workspace_change", () => this.allowedAction(action)));
        this.detail.append(actions);
        if (this.operation)
            this.renderOperation(this.operation);
        const history = document.createElement("ol");
        history.className = "workspace-operation-history";
        for (const operation of this.history.slice(-this.historyLimit)) {
            const item = document.createElement("li");
            item.textContent = `${statusLabel(operation.action)} · ${statusLabel(operation.status)} · ${localDate(operation.created_at)} · ${operation.operation_id}`;
            item.dataset.operationId = operation.operation_id;
            item.append(this.button("查看操作回执", async () => {
                this.operation = operation;
                this.operationSelection = operation.operation_id;
                await this.readProof();
                this.renderDetail();
            }));
            history.append(item);
        }
        this.detail.append(history);
        if (this.history.length > this.historyLimit)
            this.detail.append(this.button("显示更早操作", async () => { this.historyLimit += PAGE_SIZE; this.renderDetail(); }));
    }
    allowedAction(action) {
        if (!this.selected || this.stale)
            return false;
        const status = this.selected.status;
        return action === "apply" ? status === "proposed" && this.selected.expires_at > Date.now() / 1000
            : action === "keep" ? status === "applied" : status === "applied" || status === "kept";
    }
    async act(action) {
        const proposal = this.selected;
        if (!proposal || !this.allowedAction(action) || !this.permitted("workspace_change"))
            return;
        if (!window.confirm(t("确认{action}：{path}（修订版本{revision}）？", { action: statusLabel(action), path: proposal.preview.path, revision: proposal.revision })))
            return;
        const signature = JSON.stringify([proposal.proposal_id, action, proposal.revision]);
        if (this.pendingAction?.signature !== signature)
            this.pendingAction = { signature, body: {
                    proposal_id: proposal.proposal_id, action, expected_revision: proposal.revision, request_key: crypto.randomUUID(),
                } };
        this.stale = true;
        try {
            this.operation = await requestJson(`${this.base}/changes?confirmed=true`, { method: "POST", body: JSON.stringify(this.pendingAction.body) });
            this.operationSelection = this.operation.operation_id;
            this.pendingAction = undefined;
            this.noticeBox.textContent = statusLabel(this.operation.status);
            this.renderDetail();
            await this.readList();
            await this.readSelected();
        }
        catch (error) {
            // 网络或刷新失败不把文件当作未改变，也不自动刷新版本后重新发布。
            this.renderDetail();
            throw error;
        }
    }
    renderOperation(operation) {
        const result = document.createElement("p");
        result.className = "workspace-operation-result";
        result.textContent = `${statusLabel(operation.status)} · ${operation.operation_id}${operation.receipt?.error_code ? ` · ${operation.receipt.error_code}` : ""}`;
        this.detail.append(result);
        if (this.proof?.operation_id === operation.operation_id) {
            const proof = document.createElement("p");
            proof.className = "workspace-operation-proof";
            const labels = { not_applicable: "此操作不适用保留证明", exact_after: "原目标保留证明", successor_chain: "连续后继保留证明" };
            proof.textContent = `${t(labels[this.proof.kind])} · ${statusLabel(this.proof.status)}`;
            if (this.proof.kind !== "not_applicable") {
                proof.textContent += `\n${t("证明链尾操作")} ${this.proof.tip_operation_id || "—"}\n${t("保留时核实的版本")} ${this.proof.tip_after_sha256 || t("删除文件")}\n${t("文件效果顺序")} ${this.proof.effect_sequence ?? "—"} · ${t("后继数量")} ${this.proof.successor_count}`;
                if (this.proof.status !== "done")
                    proof.textContent += `\n${t("该证明尚未确认保留成功")}`;
            }
            this.detail.append(proof);
        }
        else if (this.proofError) {
            const error = document.createElement("p");
            error.className = "workspace-operation-proof";
            error.textContent = this.proofError;
            this.detail.append(error);
        }
        if (operation.status === "running")
            this.detail.append(text("p", "请等待原操作完成，刷新读取结果"));
        if (operation.status === "uncertain")
            this.detail.append(this.button("核实文件结果", () => this.operationAction("reconcile"), "workspace_reconcile"));
        if (operation.cleanup_pending && ["done", "rejected"].includes(operation.status))
            this.detail.append(this.button("清理恢复材料", () => this.operationAction("finalize"), "workspace_finalize"));
    }
    async operationAction(action) {
        const operation = this.operation;
        if (!operation || !this.permitted(`workspace_${action}`))
            return;
        const label = action === "reconcile" ? "核实文件结果" : "清理恢复材料";
        if (!window.confirm(`${t(label)} · ${operation.operation_id}？`))
            return;
        this.operation = await requestJson(`${this.base}/operations/${encodeURIComponent(operation.operation_id)}/${action}?confirmed=true`, { method: "POST" });
        this.noticeBox.textContent = statusLabel(this.operation.status);
        this.renderDetail();
        await this.readList();
        await this.readSelected();
    }
}
