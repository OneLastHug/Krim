/** 原命令事实只读消费；启动、取消与文件提案各自经过现有权限链。 */
import { errorText, requestJson, requestJsonNullable, } from "./api.js";
import { currentLocale, localDate, t } from "./i18n.js";
import { WorkspaceTreesUI } from "./workspace-trees-ui.js";
const labels = {
    preparing: "命令准备中", dispatched: "命令已派发", running: "命令运行中", finished: "命令已结束",
    interrupted: "命令已中断", completed: "命令已完成", failed: "命令失败", timed_out: "命令超时",
    cancelled: "命令已取消", output_limit: "命令输出达到上限", setup_failed: "命令准备失败", uncertain: "命令结果待核实",
    added: "新增文件", modified: "修改文件", deleted: "删除文件", unchanged: "文件未变化",
};
const terminal = (value) => ["finished", "interrupted"].includes(value.phase);
const keyValid = (key) => /^[A-Za-z0-9_.:-]{1,128}$/.test(key);
function status(value) { return t(value && labels[value] || value || "—"); }
function text(tag, key) {
    const element = document.createElement(tag);
    element.textContent = t(key);
    element.dataset.i18n = key;
    return element;
}
function paths(value) {
    const result = value.split("\n").map(item => item.trim()).filter(Boolean);
    if (result.length > 32 || new Set(result).size !== result.length)
        throw new Error(t("请输入最多32个不重复的相对路径"));
    for (const path of result) {
        if (path.length > 512 || path.startsWith("/") || path.includes("\\") || path.split("/").some(part => !part || part === "." || part === "..") || /[\x00-\x1f]/.test(path))
            throw new Error(t("文件和目录须使用工作区相对路径"));
    }
    return result;
}
export class CommandsUI {
    notice;
    openProposal;
    openDirectoryProposal;
    root = document.createElement("details");
    base;
    storageKey;
    editor = document.createElement("fieldset");
    argv = document.createElement("textarea");
    cwd = document.createElement("input");
    inputs = document.createElement("textarea");
    directories = document.createElement("textarea");
    key = document.createElement("input");
    lookupKind = document.createElement("select");
    lookup = document.createElement("input");
    filter = document.createElement("select");
    noticeBox = document.createElement("p");
    pendingBox = document.createElement("p");
    listing = document.createElement("div");
    detail = document.createElement("div");
    pageInfo = document.createElement("small");
    controls = [];
    permissions = {};
    workspace;
    bound = false;
    sessionOpen = false;
    busy = false;
    disposed = false;
    loaded = false;
    abort = new AbortController();
    locale = currentLocale();
    history = [];
    pages = [null];
    pageIndex = 0;
    nextAfter = null;
    selected;
    summary;
    inspected;
    resources;
    changes;
    directoryChanges;
    directoryPages = [null];
    directoryPageIndex = 0;
    changePages = [null];
    changePageIndex = 0;
    slice;
    sliceOffsets = [0];
    sliceIndex = 0;
    pending;
    proposal;
    pendingProposals = new Map();
    poll;
    trees;
    agent;
    constructor(agentId, notice, openProposal, openDirectoryProposal) {
        this.notice = notice;
        this.openProposal = openProposal;
        this.openDirectoryProposal = openDirectoryProposal;
        this.base = "/v1/agents/" + encodeURIComponent(agentId) + "/commands";
        this.storageKey = "krim-command-pending:" + agentId;
        this.trees = new WorkspaceTreesUI(agentId, notice, openProposal, openDirectoryProposal);
        try {
            const key = sessionStorage.getItem(this.storageKey);
            if (key && keyValid(key))
                this.pending = { key, retry: false };
        }
        catch { /* 禁用浏览器存储时仍保留当前页面的原键。 */ }
        this.root.className = "agent-commands";
        this.root.append(text("summary", "命令执行与回执"));
        this.editor.className = "command-editor";
        this.editor.append(text("legend", "准备受控命令"));
        this.argv.rows = 5;
        this.argv.maxLength = 32768;
        this.cwd.value = ".";
        this.cwd.maxLength = 512;
        this.inputs.rows = this.directories.rows = 2;
        this.inputs.maxLength = this.directories.maxLength = 16384;
        this.key.maxLength = 128;
        this.key.value = this.pending?.key || "";
        this.key.spellcheck = false;
        this.field(this.editor, "命令参数（JSON字符串数组）", this.argv);
        this.field(this.editor, "工作区相对工作目录", this.cwd);
        this.field(this.editor, "完整输入文件（每行一个）", this.inputs);
        this.field(this.editor, "副本目录（每行一个）", this.directories);
        this.field(this.editor, "命令请求键（可留空自动生成）", this.key);
        this.editor.append(text("p", "命令修改隔离副本，文件产物须另行审阅和批准。"));
        this.editor.append(this.button("执行受控命令", () => this.start(), "command_execute", () => this.sessionOpen && (!this.pending || this.pending.retry) && this.validDraft()));
        this.noticeBox.className = "command-status";
        this.noticeBox.setAttribute("role", "status");
        this.pendingBox.className = "command-pending";
        this.pendingBox.setAttribute("role", "status");
        const query = document.createElement("div");
        query.className = "command-query";
        for (const [value, key] of [["key", "原请求键"], ["id", "命令标识"]]) {
            const option = text("option", key);
            option.value = value;
            this.lookupKind.append(option);
        }
        this.lookup.maxLength = 128;
        this.lookup.value = this.pending?.key || "";
        this.field(query, "命令查询类型", this.lookupKind);
        this.field(query, "命令标识或原请求键", this.lookup);
        query.append(this.button("查询原命令", () => this.find(), "command_get", () => Boolean(this.lookup.value)));
        query.append(this.button("查询未确认命令", () => this.findPending(), "command_get", () => Boolean(this.pending)));
        const toolbar = document.createElement("div");
        toolbar.className = "actions";
        toolbar.append(this.button("刷新命令历史", () => this.refresh(), "command_history"), this.button("上一页命令", () => this.historyPage(-1), "command_history", () => this.pageIndex > 0), this.button("下一页命令", () => this.historyPage(1), "command_history", () => this.nextAfter !== null));
        for (const [value, key] of [["all", "本页全部命令"], ["active", "本页运行中"], ["finished", "本页已结束"], ["uncertain", "本页待核实"]]) {
            const option = text("option", key);
            option.value = value;
            this.filter.append(option);
        }
        const filterRow = document.createElement("div");
        this.field(filterRow, "命令当前页筛选", this.filter);
        this.filter.onchange = () => { this.renderList(); this.sync(); };
        this.listing.className = "command-list";
        this.detail.className = "command-detail";
        this.root.append(this.editor, this.pendingBox, this.noticeBox, query, toolbar, filterRow, this.pageInfo, this.listing, this.detail, this.trees.root);
        for (const element of [this.argv, this.cwd, this.inputs, this.directories, this.key, this.lookup])
            element.oninput = () => this.sync();
        this.root.ontoggle = () => {
            if (!this.root.open) {
                this.clearPoll();
                this.trees.root.open = false;
            }
            else if (!this.loaded && this.bound)
                void this.work(() => this.refresh());
            else
                this.schedulePoll();
        };
        this.sync();
    }
    update(agent) {
        this.agent = agent;
        const reopened = !this.sessionOpen && agent.session.status === "open";
        this.permissions = agent.session.permissions;
        this.bound = Boolean(this.permissions.execution);
        this.sessionOpen = agent.session.status === "open";
        this.trees.update(agent, this.workspace);
        this.root.hidden = !this.bound;
        if (this.locale !== currentLocale()) {
            this.locale = currentLocale();
            this.renderList();
            this.renderDetail();
        }
        this.sync();
        if (this.bound && this.root.open && (!this.loaded || reopened) && !this.busy)
            void this.work(() => this.refresh());
    }
    dispose() { this.disposed = true; this.clearPoll(); this.abort.abort(); this.trees.dispose(); }
    field(parent, key, element) {
        const row = document.createElement("label");
        row.append(text("span", key), element);
        element.setAttribute("aria-label", t(key));
        element.setAttribute("data-i18n-aria-label", key);
        parent.append(row);
    }
    permitted(tool) {
        const names = this.permissions.tool_names, mode = this.permissions.mode;
        if (!this.bound || !Array.isArray(names) || !names.includes(tool) || mode === "ask")
            return false;
        if (tool === "command_execute" || tool === "command_cancel")
            return this.sessionOpen && mode !== "plan";
        if (tool === "command_propose" || tool === "directory_source_preview" || tool === "workspace_tree_prepare") {
            // Session permissions只保存工作区身份指纹，不把writable复制进冻结清单；
            // UI读取当前WorkspaceView，后端WorkspaceChange/来源校验仍最终裁决。
            return this.sessionOpen && this.workspace?.available === true && this.workspace.writable;
        }
        return true;
    }
    validDraft() { try {
        this.draft("validation");
        return !this.key.value || keyValid(this.key.value);
    }
    catch {
        return false;
    } }
    draft(key) {
        const argv = JSON.parse(this.argv.value);
        if (!Array.isArray(argv) || argv.length < 1 || argv.length > 128
            || argv.some((value, index) => typeof value !== "string" || (index === 0 && !value) || value.length > 8192 || value.includes("\0")))
            throw new Error(t("命令参数须为1至128个字符串的JSON数组"));
        const cwd = this.cwd.value.trim();
        if (!cwd || cwd.length > 512 || (cwd !== "." && !paths(cwd).length))
            throw new Error(t("文件和目录须使用工作区相对路径"));
        const body = { request_key: key, command: { argv: argv, cwd }, input_paths: paths(this.inputs.value), directories: paths(this.directories.value) };
        if (new TextEncoder().encode(JSON.stringify(body.command)).length > 32768)
            throw new Error(t("命令参数超过大小限制"));
        return body;
    }
    button(key, action, tool, allowed) {
        const button = text("button", key);
        button.type = "button";
        button.className = "secondary";
        button.disabled = this.disabled(tool, allowed);
        button.onclick = () => { if (!button.disabled)
            void this.work(action); };
        this.controls.push({ button, tool, allowed });
        return button;
    }
    disabled(tool, allowed) {
        return this.busy || this.disposed || !this.bound || Boolean(tool && !this.permitted(tool)) || Boolean(allowed && !allowed());
    }
    sync() {
        this.controls = this.controls.filter(control => this.root.contains(control.button));
        this.editor.disabled = this.busy || !this.permitted("command_execute");
        this.key.disabled = this.editor.disabled || Boolean(this.pending);
        for (const control of this.controls)
            control.button.disabled = this.disabled(control.tool, control.allowed);
        this.pendingBox.textContent = this.pending ? t("命令请求结果尚未确认，请查询原请求键：{key}", { key: this.pending.key }) : "";
        this.root.setAttribute("aria-busy", String(this.busy));
    }
    async request(path, init = {}) {
        const result = await requestJson(path, { ...init, signal: this.abort.signal });
        if (this.disposed)
            throw new DOMException("disposed", "AbortError");
        return result;
    }
    async work(action) {
        if (this.busy || this.disposed || !this.bound)
            return;
        this.busy = true;
        this.clearPoll();
        this.sync();
        try {
            await action();
        }
        catch (error) {
            if (!this.disposed) {
                this.noticeBox.textContent = errorText(error);
                this.notice(errorText(error), true);
            }
        }
        finally {
            this.busy = false;
            if (!this.disposed) {
                this.sync();
                this.schedulePoll();
            }
        }
    }
    remember() {
        try {
            if (this.pending)
                sessionStorage.setItem(this.storageKey, this.pending.key);
            else
                sessionStorage.removeItem(this.storageKey);
        }
        catch { /* 原键仍可从页面状态复制查询。 */ }
    }
    async start() {
        if (!this.permitted("command_execute") || (this.pending && !this.pending.retry))
            return;
        const key = this.pending?.key || this.key.value || crypto.randomUUID();
        const body = this.draft(key);
        if (this.pending?.body && JSON.stringify(this.pending.body) !== JSON.stringify(body))
            throw new Error(t("未确认命令只能使用原参数与原请求键重试"));
        if (!window.confirm(t("确认执行以下命令及完整输入？") + "\n" + JSON.stringify(body, null, 2)))
            return;
        this.pending = { key, body, retry: false };
        this.key.value = key;
        this.lookupKind.value = "key";
        this.lookup.value = key;
        this.remember();
        this.sync();
        const accepted = await this.request(this.base + "?confirmed=true", { method: "POST", body: JSON.stringify(body) });
        this.selected = accepted.command_id;
        this.pending = undefined;
        this.remember();
        this.key.value = "";
        this.noticeBox.textContent = t("命令已接受，继续查询原命令结果");
        this.clearSelected();
        await this.readSelected();
        if (this.permitted("command_history")) {
            this.pages = [null];
            this.pageIndex = 0;
            await this.readHistory();
        }
    }
    async findPending() {
        if (!this.pending)
            return;
        this.lookupKind.value = "key";
        this.lookup.value = this.pending.key;
        await this.find();
    }
    async find() {
        const value = this.lookup.value.trim();
        if (this.lookupKind.value === "key") {
            if (!keyValid(value))
                throw new Error(t("命令请求键格式无效"));
            const summary = await requestJsonNullable(this.base + "/by-key?" + new URLSearchParams({ request_key: value }), { signal: this.abort.signal });
            if (this.disposed)
                return;
            if (!summary) {
                if (this.pending?.key === value)
                    this.pending.retry = true;
                this.noticeBox.textContent = t("尚未找到原命令；可以明确使用原键重试，不能推断命令从未执行");
                return;
            }
            this.selected = summary.command_id;
            this.clearSelected();
            this.summary = summary;
            if (this.pending?.key === value) {
                this.pending = undefined;
                this.remember();
                this.key.value = "";
            }
        }
        else {
            if (!/^[A-Za-z0-9][A-Za-z0-9_-]{0,35}$/.test(value))
                throw new Error(t("命令标识格式无效"));
            this.selected = value;
            this.clearSelected();
        }
        await this.readSelected();
        this.noticeBox.textContent = t("已读取原命令，未重新执行");
    }
    async refresh() {
        if (this.permissions.workspace && this.sessionOpen) {
            this.workspace = await this.request(this.base.replace(/\/commands$/, "/workspace"));
            if (this.agent)
                this.trees.update(this.agent, this.workspace);
        }
        if (this.permitted("command_history"))
            await this.readHistory();
        if (this.pending && this.permitted("command_get"))
            await this.findPending();
        else if (this.selected)
            await this.readSelected();
        this.loaded = true;
    }
    async historyPage(direction) {
        if (direction > 0) {
            if (!this.nextAfter)
                return;
            this.pages = this.pages.slice(0, this.pageIndex + 1);
            this.pages.push(this.nextAfter);
        }
        this.pageIndex += direction;
        await this.readHistory();
    }
    async readHistory() {
        const after = this.pages[this.pageIndex], query = new URLSearchParams({ limit: "32" });
        if (after)
            query.set("after", after);
        const page = await this.request(this.base + "?" + query);
        this.history = page.entries;
        this.nextAfter = page.next_after;
        this.renderList();
    }
    renderList() {
        this.controls = this.controls.filter(control => !this.listing.contains(control.button));
        this.listing.replaceChildren();
        const filter = this.filter.value;
        const entries = this.history.filter(item => filter === "active" ? !terminal(item)
            : filter === "finished" ? terminal(item) : filter === "uncertain" ? item.outcome === "uncertain" || item.phase === "interrupted" : true);
        this.pageInfo.textContent = t("第{page}页 · 本页{count}项", { page: this.pageIndex + 1, count: this.history.length });
        if (!entries.length)
            this.listing.append(text("p", "当前页没有匹配的命令"));
        for (const item of entries) {
            const button = this.button("查看原命令", async () => {
                this.selected = item.command_id;
                this.clearSelected();
                await this.readSelected();
            }, undefined, () => this.permitted("command_get") || this.permitted("command_inspect"));
            button.removeAttribute("data-i18n");
            button.dataset.commandId = item.command_id;
            button.textContent = item.command_preview + " · " + status(item.outcome || item.phase) + " · " + localDate(item.created_at);
            this.listing.append(button);
        }
    }
    clearSelected() {
        this.summary = undefined;
        this.inspected = undefined;
        this.slice = undefined;
        this.changes = undefined;
        this.proposal = undefined;
        this.resources = undefined;
        this.sliceOffsets = [0];
        this.sliceIndex = 0;
        this.changePages = [null];
        this.changePageIndex = 0;
        this.directoryChanges = undefined;
        this.directoryPages = [null];
        this.directoryPageIndex = 0;
    }
    async readSelected() {
        if (!this.selected)
            return;
        if (this.permitted("command_get"))
            this.summary = await this.request(this.commandPath());
        if (this.permitted("command_inspect"))
            this.inspected = await this.request(this.commandPath() + "/inspect");
        if (this.permitted("command_resources")) {
            const value = await this.request(this.commandPath() + "/resources");
            if (value.command_id !== this.selected || value.scope_id !== `session:${this.agent?.agent_id}` || value.topic_id !== this.agent?.topic_id
                || value.usage && (value.usage.scope !== "process_tree" || value.usage.backend !== "cgroup_v2" || !value.limits.tree
                    || value.usage.limits.cpu_seconds !== value.limits.tree.cpu_seconds || value.usage.limits.memory_bytes !== value.limits.tree.memory_bytes || value.usage.limits.process_count !== value.limits.tree.process_count
                    || value.processes_reaped && (value.usage.populated !== false || value.usage.leader_reaped !== true)))
                throw new Error(t("命令资源回程与原事实不一致"));
            this.resources = value;
        }
        this.renderDetail();
        // 详情轮询收束后同步列表，避免同一持久命令在详情与历史中出现不同终态。
        if (this.state && terminal(this.state) && this.permitted("command_history"))
            await this.readHistory();
    }
    commandPath() { return this.base + "/" + encodeURIComponent(this.selected); }
    get state() { return this.summary || this.inspected; }
    renderDetail() {
        this.controls = this.controls.filter(control => !this.detail.contains(control.button));
        this.detail.replaceChildren();
        const view = this.state;
        if (!view)
            return;
        this.detail.dataset.commandId = view.command_id;
        const heading = document.createElement("strong");
        heading.textContent = status(view.outcome || view.phase) + " · " + view.command_id;
        const metadata = document.createElement("p");
        metadata.className = "command-meta";
        metadata.textContent = t("命令阶段") + " " + status(view.phase) + " · " + t("命令退出码") + " " + (view.exit_code ?? "—")
            + "\n" + t("命令已启动") + " " + t(view.started ? "是" : "否") + " · " + t("命令进程已回收") + " " + t(view.processes_reaped ? "是" : "否")
            + "\n" + t("完整输入数量") + " " + view.input_files + " · " + t("产物数量") + " " + view.artifact_files;
        this.detail.append(heading, metadata);
        if (this.resources) {
            const section = document.createElement("section");
            section.className = "command-resources";
            section.append(text("strong", "命令资源限额与用量"));
            const limits = this.resources.limits.tree, usage = this.resources.usage;
            if (!limits)
                section.append(text("p", "原资源仅有逐进程与UID限额"));
            else {
                const settings = document.createElement("p");
                settings.className = "command-meta";
                settings.textContent = `${t("整树累计CPU预算（秒）")}: ${limits.cpu_seconds}\n${t("整树内存上限（字节）")}: ${limits.memory_bytes}\n${t("整树进程线程上限")}: ${limits.process_count}`;
                section.append(settings);
                section.append(text("p", "包含启动准备、监督器和全部后代；CPU预算按内核合计用量采样收束。"));
                if (!usage)
                    section.append(text("p", "整树用量尚未收束"));
                else {
                    const value = (data) => data === null ? t("用量未确认") : String(data);
                    const actual = document.createElement("p");
                    actual.className = "command-meta";
                    actual.textContent = `${t("整树累计CPU用量（微秒）")}: ${value(usage.cpu_microseconds)}\n${t("整树内存峰值（字节）")}: ${value(usage.memory_peak_bytes)}\n${t("整树进程线程峰值")}: ${value(usage.tasks_peak)}\n${t("整树内存终止计数")}: ${value(usage.oom_kills)}\n${t("整树进程限额拒绝计数")}: ${value(usage.task_limit_hits)}\n${t("整树回收已确认")}: ${t(usage.populated === false && usage.leader_reaped === true ? "是" : "否")}`;
                    section.append(actual);
                }
            }
            this.detail.append(section);
        }
        if (view.error_code) {
            const error = document.createElement("p");
            error.textContent = view.error_code;
            this.detail.append(error);
        }
        if (view.outcome === "uncertain" || view.phase === "interrupted")
            this.detail.append(text("p", "保留原命令事实；禁止自动重跑或使用不完整产物。"));
        if (this.inspected) {
            const request = document.createElement("pre");
            request.className = "command-request";
            request.textContent = JSON.stringify(this.inspected.request, null, 2);
            this.detail.append(request);
        }
        const actions = document.createElement("div");
        actions.className = "actions";
        actions.append(this.button("刷新原命令", () => this.readSelected()), this.button("等待原命令", () => this.wait(), "command_get", () => !terminal(view)), this.button("取消原命令", () => this.cancel(), "command_cancel", () => !terminal(view)), this.button("读取标准输出", () => this.readOutput("stdout"), "command_read"), this.button("读取错误输出", () => this.readOutput("stderr"), "command_read"), this.button("查看命令文件变化", () => this.readChanges(), "command_changes", () => terminal(view) && view.artifacts_complete && view.processes_reaped), this.button("查看命令目录成果", () => this.readDirectoryChanges(), "directory_source_query", () => terminal(view) && view.artifacts_complete && view.processes_reaped));
        this.detail.append(actions);
        if (view.output_truncated)
            this.detail.append(text("p", "命令输出已截断"));
        if (this.summary?.artifacts_truncated)
            this.detail.append(text("p", "产物摘要已截断，请使用文件变化分页读取"));
        if (this.slice)
            this.renderOutput();
        if (this.changes)
            this.renderChanges();
        if (this.directoryChanges)
            this.renderDirectoryChanges();
        if (this.proposal) {
            const section = document.createElement("section");
            section.className = "command-proposal";
            section.dataset.proposalId = this.proposal.proposal.proposal_id;
            const note = document.createElement("p");
            note.textContent = t("产物提案已保存，请独立审阅文件差异后批准") + " · " + this.proposal.change.path;
            const id = this.proposal.proposal.proposal_id;
            section.append(note, this.button("审阅产物文件提案", () => this.openProposal(id)));
            this.detail.append(section);
        }
    }
    async wait() {
        this.summary = await this.request(this.commandPath() + "/wait?timeout_seconds=1", { method: "POST" });
        this.renderDetail();
    }
    async cancel() {
        if (!this.permitted("command_cancel") || !window.confirm(t("确认取消原命令：{id}？", { id: this.selected })))
            return;
        this.summary = await this.request(this.commandPath() + "/cancel?confirmed=true", { method: "POST" });
        this.renderDetail();
        if (this.permitted("command_history"))
            await this.readHistory();
    }
    async readOutput(source, path, offset = 0) {
        const query = new URLSearchParams({ source, offset: String(offset), limit: "4096" });
        if (path)
            query.set("path", path);
        const slice = await this.request(this.commandPath() + "/read?" + query);
        if (offset === 0) {
            this.sliceOffsets = [0];
            this.sliceIndex = 0;
        }
        this.slice = slice;
        this.renderDetail();
    }
    renderOutput() {
        const slice = this.slice, section = document.createElement("section");
        section.className = "command-output";
        section.dataset.source = slice.source;
        const metadata = document.createElement("p");
        metadata.textContent = (slice.path || slice.source) + " · " + slice.offset + "–" + slice.next_offset + " / " + slice.byte_size + " B";
        const output = document.createElement("pre");
        output.className = "command-output-text";
        output.textContent = slice.text;
        const raw = document.createElement("details"), rawText = document.createElement("pre");
        rawText.textContent = slice.data_base64;
        raw.className = "command-output-raw";
        raw.append(text("summary", "原始字节（Base64）"), rawText);
        section.append(metadata, output, raw, this.button("较早输出片段", async () => {
            const index = this.sliceIndex - 1, offset = this.sliceOffsets[index];
            await this.readOutput(slice.source, slice.path || undefined, offset);
            this.sliceIndex = index;
        }, "command_read", () => this.sliceIndex > 0), this.button("较晚输出片段", async () => {
            const offsets = this.sliceOffsets.slice(0, this.sliceIndex + 1), index = this.sliceIndex + 1;
            offsets.push(slice.next_offset);
            await this.readOutput(slice.source, slice.path || undefined, slice.next_offset);
            this.sliceOffsets = offsets;
            this.sliceIndex = index;
        }, "command_read", () => slice.next_offset > slice.offset && slice.next_offset < slice.byte_size && slice.next_offset <= 8388608));
        this.detail.append(section);
    }
    async readChanges(direction = 0) {
        if (direction > 0 && this.changes?.next_after) {
            this.changePages = this.changePages.slice(0, this.changePageIndex + 1);
            this.changePages.push(this.changes.next_after);
        }
        this.changePageIndex += direction;
        const query = new URLSearchParams({ limit: "32" }), after = this.changePages[this.changePageIndex];
        if (after)
            query.set("after", after);
        this.changes = await this.request(this.commandPath() + "/changes?" + query);
        this.renderDetail();
    }
    renderChanges() {
        const changes = this.changes, section = document.createElement("section");
        section.className = "command-changes";
        section.append(text("h3", "命令文件变化"));
        if (changes.requires_failed_acceptance)
            section.append(text("p", "失败命令的完整产物需要另行明确接受"));
        if (!changes.entries.length)
            section.append(text("p", "没有文件变化"));
        for (const entry of changes.entries) {
            const row = document.createElement("div");
            row.className = "command-change-row";
            row.dataset.path = entry.path;
            const info = document.createElement("p");
            info.textContent = entry.path + " · " + status(entry.kind) + " · " + (entry.before_bytes ?? 0) + " → " + (entry.after_bytes ?? 0) + " B"
                + "\n" + (entry.before_sha256 || "—") + " → " + (entry.after_sha256 || "—")
                + "\n" + t("文件权限") + " " + (entry.before_mode?.toString(8) || "—") + " → " + (entry.after_mode?.toString(8) || "—");
            row.append(info, this.button("读取原产物", () => this.readOutput("artifact", entry.path), "command_read", () => entry.kind !== "deleted"), this.button("生成产物文件提案", () => this.propose(entry), "command_propose", () => entry.kind !== "unchanged"));
            section.append(row);
        }
        section.append(this.button("较早文件变化", () => this.readChanges(-1), "command_changes", () => this.changePageIndex > 0), this.button("较晚文件变化", () => this.readChanges(1), "command_changes", () => changes.next_after !== null));
        this.detail.append(section);
    }
    async readDirectoryChanges(direction = 0) {
        if (!this.selected || !this.permitted("directory_source_query"))
            return;
        if (direction > 0 && this.directoryChanges?.next_after) {
            this.directoryPages = this.directoryPages.slice(0, this.directoryPageIndex + 1);
            this.directoryPages.push(this.directoryChanges.next_after);
        }
        this.directoryPageIndex += direction;
        const query = new URLSearchParams({ limit: "32" });
        const after = this.directoryPages[this.directoryPageIndex];
        if (after)
            query.set("after", after);
        this.directoryChanges = await this.request(this.commandPath() + "/directory-changes?" + query);
        this.renderDetail();
    }
    async proposeDirectory(entry) {
        if (!this.selected || !this.permitted("directory_source_preview"))
            return;
        if (!window.confirm(t("确认从原命令目录生成容器提案：{path}？", { path: entry.path })))
            return;
        const body = {
            command_id: this.selected, path: entry.path, request_key: crypto.randomUUID(),
            accept_failed: this.state?.outcome !== "completed",
        };
        const proposal = await this.request(this.commandPath() + "/directory-proposals?confirmed=true", { method: "POST", body: JSON.stringify(body) });
        this.noticeBox.textContent = t("命令目录提案已保存，请审阅目录事务");
        await this.readDirectoryChanges();
        await this.openDirectoryProposal(proposal.proposal.proposal_id);
    }
    renderDirectoryChanges() {
        const changes = this.directoryChanges;
        if (!changes)
            return;
        const section = document.createElement("section");
        section.className = "command-directory-changes";
        section.append(text("h3", "命令目录成果"));
        const meta = document.createElement("p");
        meta.textContent = `${t("原目录来源")}: ${changes.source_hash} · ${status(changes.outcome)} · ${changes.complete ? t("来源完整") : t("来源不完整")}`;
        section.append(meta);
        if (!changes.entries.length)
            section.append(text("p", "没有目录成果"));
        for (const entry of changes.entries) {
            const row = document.createElement("div");
            row.className = "command-directory-row";
            row.dataset.path = entry.path;
            const info = document.createElement("p");
            info.textContent = `${entry.path} · mode ${entry.mode.toString(8).padStart(4, "0")} · ${t(entry.empty ? "空目录" : "非空目录")}`;
            row.append(info);
            if (entry.proposal_id)
                row.append(this.button("审阅目录提案", () => this.openDirectoryProposal(entry.proposal_id), "directory_query"));
            else
                row.append(this.button("生成目录提案", () => this.proposeDirectory(entry), "directory_source_preview", () => entry.creatable && this.sessionOpen));
            row.append(this.button("审阅目录树成果", async () => { this.trees.selectSource(changes, entry.path); }, "workspace_tree_prepare", () => changes.complete && entry.creatable && this.sessionOpen));
            section.append(row);
        }
        section.append(this.button("较早目录成果", () => this.readDirectoryChanges(-1), "directory_source_query", () => this.directoryPageIndex > 0), this.button("较晚目录成果", () => this.readDirectoryChanges(1), "directory_source_query", () => changes.next_after !== null));
        this.detail.append(section);
    }
    async propose(entry) {
        if (!this.permitted("command_propose"))
            return;
        const signature = this.selected + ":" + entry.path, failed = this.changes.requires_failed_acceptance;
        if (!window.confirm(t(failed ? "明确接受失败命令的完整产物并生成提案：{path}？" : "生成原命令产物的文件提案：{path}？", { path: entry.path })))
            return;
        let pending = this.pendingProposals.get(signature);
        if (!pending) {
            pending = { key: crypto.randomUUID(), accept_failed: failed };
            this.pendingProposals.set(signature, pending);
        }
        const proposal = await this.request(this.commandPath() + "/proposals?confirmed=true", {
            method: "POST", body: JSON.stringify({ command_id: this.selected, path: entry.path, request_key: pending.key, accept_failed: pending.accept_failed }),
        });
        this.pendingProposals.delete(signature);
        this.proposal = proposal;
        this.renderDetail();
    }
    clearPoll() { if (this.poll !== undefined)
        window.clearTimeout(this.poll); this.poll = undefined; }
    schedulePoll() {
        this.clearPoll();
        if (this.disposed || this.busy || !this.bound || !this.root.open || !this.state || terminal(this.state)
            || (!this.permitted("command_get") && !this.permitted("command_inspect")))
            return;
        this.poll = window.setTimeout(() => {
            this.poll = undefined;
            void this.work(async () => {
                try {
                    await this.readSelected();
                }
                catch (error) {
                    this.clearSelected();
                    throw error;
                }
            });
        }, 1500);
    }
}
