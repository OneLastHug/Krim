/** 两种来源共用清单视图；文件事实始终由原生账本返回，Host 保持原协议。 */
import { ApiError, errorText, requestJson, requestJsonNullable, } from "./api.js";
import { localDate, t } from "./i18n.js";
const node = (id) => document.getElementById(id);
const button = (id) => node(id);
const labels = {
    pending: "待处理变更", executing: "变更处理中", undone: "已撤销", kept: "已保留", uncertain: "文件结果待核实",
    applied: "文件已应用", applying: "文件应用中", undoing: "文件撤销中", keeping: "保留确认中",
    running: "文件操作执行中", done: "文件操作已确认", partial: "批次部分完成", failed: "批次失败",
    cancelled: "批次已取消", interrupted: "批次已中断", rejected: "文件操作已拒绝", skipped: "尚未执行",
};
const statusText = (value) => t(labels[value] || value);
const actionText = (action) => t(action === "keep" ? "保留变更" : "撤销变更");
const mixedStatusText = (value) => t({ running: "批次执行中", done: "批次已确认", rejected: "操作已拒绝", uncertain: "批次结果待核实" }[value] || labels[value] || value);
const candidateKey = (item) => `${item.source}:${item.proposal_id}`;
const identifier = (value) => typeof value === "string" && /^[A-Za-z0-9][A-Za-z0-9_-]{0,35}$/.test(value);
export class ChangeListUI {
    notice;
    invokeHost;
    openWorkspaceProposal;
    openDirectoryProposal;
    topicId = null;
    sessionId = null;
    catalog = null;
    operations = [];
    pendingTopics = new Set();
    source = { kind: "host" };
    workspaces = new Map();
    generation = 0;
    loadVersion = 0;
    timer;
    constructor(notice, invokeHost, openWorkspaceProposal, openDirectoryProposal) {
        this.notice = notice;
        this.invokeHost = invokeHost;
        this.openWorkspaceProposal = openWorkspaceProposal;
        this.openDirectoryProposal = openDirectoryProposal;
        node("change-refresh").onclick = () => void this.refresh(true);
        node("change-clear").onclick = () => void this.clear();
        node("change-undo-all").onclick = () => void this.invoke("undo", null);
        node("change-keep-all").onclick = () => void this.invoke("keep", null);
        node("change-cancel").onclick = () => void this.cancel();
        node("change-source").onchange = () => {
            const value = node("change-source").value;
            void this.switchSource(value === "host" ? { kind: "host" } : { kind: "workspace", id: value });
        };
        node("change-filter").onchange = () => {
            const state = this.workspace();
            if (!state)
                return;
            state.filter = node("change-filter").value;
            state.cursors = [null];
            state.page = null;
            state.stale = true;
            void this.refresh();
        };
        node("change-prev").onclick = () => void this.page(-1);
        node("change-next").onclick = () => void this.page(1);
        node("change-history-prev").onclick = () => void this.historyPage(-1);
        node("change-history-next").onclick = () => void this.historyPage(1);
        node("change-query-id").onclick = () => void this.query("id");
        node("change-query-key").onclick = () => void this.query("key");
        node("change-wait").onclick = () => void this.query("wait");
        node("change-mixed-prev").onclick = () => void this.mixedPage(-1);
        node("change-mixed-next").onclick = () => void this.mixedPage(1);
        node("change-mixed-query-id").onclick = () => void this.queryMixed("id");
        node("change-mixed-query-key").onclick = () => void this.queryMixed("key");
        node("change-mixed-wait").onclick = () => void this.queryMixed("wait");
        node("change-mixed-cancel").onclick = () => void this.cancelMixed();
        node("change-mixed-options-refresh").onclick = () => void this.readMixedCandidates();
        node("change-mixed-files-prev").onclick = () => void this.candidatePage("file", -1);
        node("change-mixed-files-next").onclick = () => void this.candidatePage("file", 1);
        node("change-mixed-directories-prev").onclick = () => void this.candidatePage("directory", -1);
        node("change-mixed-directories-next").onclick = () => void this.candidatePage("directory", 1);
        node("change-mixed-apply").onclick = () => void this.startMixed("apply");
        node("change-mixed-keep").onclick = () => void this.startMixed("keep");
        node("change-mixed-undo").onclick = () => void this.startMixed("undo");
        node("change-mixed-clear-selection").onclick = () => { const state = this.workspace(); if (state && !state.mixedPending && !state.mixedSending) {
            state.mixedSelected.clear();
            this.render();
        } };
        node("change-mixed-recover").onclick = () => void this.recoverMixedPending();
        node("change-mixed-retry").onclick = () => void this.startMixed("apply", true);
        node("change-mixed-clear-cache").onclick = () => {
            const state = this.workspace();
            if (!state?.mixedCacheInvalid || !window.confirm(t("仅清除无效本地记录，不会重发批次？")))
                return;
            state.mixedCacheInvalid = false;
            state.mixedPending = null;
            state.mixedMessage = "";
            this.persistMixed(state);
            this.render();
        };
        node("change-mixed-query").oninput = () => {
            const state = this.workspace();
            if (state)
                state.mixedQuery = node("change-mixed-query").value;
            this.syncQueryButtons();
        };
        node("change-batch-query").oninput = () => {
            const state = this.workspace();
            if (state)
                state.query = node("change-batch-query").value;
            this.syncQueryButtons();
        };
        node("change-panel").ontoggle = () => {
            if (node("change-panel").open)
                this.schedule();
            else
                this.stopTimer();
        };
        this.render();
    }
    setSession(id) { this.sessionId = id; this.render(); }
    refreshLocale() { this.renderSources(); this.render(); }
    setWorkspaceSources(agents) {
        const ids = new Set();
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
                        this.generation += 1;
                        this.stopTimer();
                        activeInvalidated = true;
                    }
                    const sameSession = state.agent.session_id === agent.session_id;
                    // 换对象使原请求只能更新原缓存；同会话保留批次查询键，清除旧页面和未提交动作。
                    this.workspaces.set(agent.agent_id, { ...state, agent, grant: undefined, page: null,
                        cursors: [null], historyCursors: [null], history: null, stale: true, busy: false, sending: false,
                        batch: sameSession ? state.batch : null, pending: null,
                        activeBatchId: sameSession ? state.activeBatchId : null, queryVersion: state.queryVersion + 1 });
                    const current = this.workspaces.get(agent.agent_id);
                    current.mixedCursors = [null];
                    current.mixedHistory = null;
                    current.mixedBatch = sameSession ? state.mixedBatch : null;
                    current.mixedVersion += 1;
                    current.mixedFiles = [];
                    current.mixedDirectories = [];
                    current.mixedCandidatesLoaded = false;
                    current.mixedSending = false;
                    if (!sameSession || JSON.stringify(oldWorkspace) !== JSON.stringify(nextWorkspace)) {
                        current.mixedSelected = new Map();
                        current.mixedPending = null;
                        current.mixedFingerprint = null;
                    }
                }
                else
                    state.agent = agent;
            }
            else
                this.workspaces.set(agent.agent_id, { agent, page: null, filter: "active", cursors: [null], historyCursors: [null], history: null,
                    batch: null, pending: null, query: "", message: "", activeBatchId: null, busy: false, sending: false, stale: true, queryVersion: 0, loadingVersion: 0,
                    mixedCursors: [null], mixedHistory: null, mixedBatch: null, mixedQuery: "", mixedVersion: 0, mixedMessage: "",
                    mixedFiles: [], mixedDirectories: [], mixedFileOffset: 0, mixedDirectoryAfter: [null], mixedDirectoryNext: null,
                    mixedCandidatesLoaded: false, mixedSelected: new Map(), mixedPending: null, mixedMissing: false, mixedSending: false,
                    mixedFingerprint: null, mixedCacheInvalid: false });
            const current = this.workspaces.get(agent.agent_id);
            if (!state)
                this.loadMixedPending(current);
        }
        for (const id of this.workspaces.keys())
            if (!ids.has(id))
                this.workspaces.delete(id);
        if (this.source.kind === "workspace" && !this.workspaces.has(this.source.id))
            void this.switchSource({ kind: "host" });
        this.renderSources();
        this.render();
        if (activeInvalidated && node("change-panel").open)
            void this.refresh(true);
    }
    async selectWorkspace(id) {
        if (!this.workspaces.has(id))
            throw new Error(t("工作区资源已停用或关闭"));
        node("change-panel").open = true;
        await this.switchSource({ kind: "workspace", id });
        node("change-panel").scrollIntoView({ block: "nearest" });
    }
    async selectTopic(id) {
        this.beginTopic(id);
        if (this.source.kind === "host")
            await this.refresh();
    }
    /** 话题切换开始时立即废弃旧 Host 控件，不等消息树读取完成。 */
    beginTopic(id) {
        if (id !== this.topicId) {
            this.catalog = null;
            this.operations = [];
            if (this.source.kind === "host") {
                this.generation += 1;
                this.stopTimer();
            }
        }
        this.topicId = id;
        this.renderSources();
        this.render();
    }
    workspace() { return this.source.kind === "workspace" ? this.workspaces.get(this.source.id) : undefined; }
    key() { return this.source.kind === "host" ? `host:${this.topicId}` : `workspace:${this.source.id}`; }
    isCurrent(key, generation) { return key === this.key() && generation === this.generation; }
    base(id) { return `/v1/topics/${encodeURIComponent(id)}/change-list`; }
    native(state) { return `/v1/agents/${encodeURIComponent(state.agent.agent_id)}/workspace`; }
    async switchSource(source) {
        this.stopTimer();
        this.generation += 1;
        this.source = source;
        this.renderSources();
        this.render();
        await this.refresh();
    }
    renderSources() {
        const select = node("change-source");
        select.replaceChildren();
        const host = document.createElement("option");
        host.value = "host";
        host.textContent = t("Host 当前话题");
        select.append(host);
        for (const [id, state] of this.workspaces) {
            const option = document.createElement("option");
            option.value = id;
            option.textContent = `${t("工作区")} · ${state.agent.name} · ${id}`;
            select.append(option);
        }
        select.value = this.source.kind === "host" ? "host" : this.source.id;
    }
    async refresh(reset = false) {
        const state = this.workspace(), key = this.key(), generation = this.generation, version = ++this.loadVersion;
        if (state) {
            if (reset) {
                state.cursors = [null];
                state.historyCursors = [null];
                state.mixedCursors = [null];
            }
            state.busy = true;
            state.loadingVersion = version;
            state.stale = true;
            this.render();
            try {
                const base = this.native(state);
                const queryVersion = state.queryVersion, mixedVersion = state.mixedVersion;
                const [page, history, mixedHistory, grant] = await Promise.all([
                    this.readPage(state), this.readHistory(state), this.readMixedHistory(state),
                    state.agent.session.status === "open" ? requestJson(base).catch(() => undefined) : Promise.resolve(undefined),
                ]);
                if (!this.isCurrent(key, generation) || version !== this.loadVersion)
                    return;
                state.page = page.page;
                if (page.reset) {
                    state.cursors = [null];
                    state.message = t("清单已变化，已返回第一页，请重新审阅");
                }
                state.history = history;
                state.mixedHistory = mixedHistory;
                state.grant = grant;
                state.stale = false;
                if (state.mixedPending) {
                    const original = await requestJsonNullable(`${base}/mixed-batches/by-key?${new URLSearchParams({ request_key: state.mixedPending.request_key })}`);
                    if (!this.isCurrent(key, generation) || version !== this.loadVersion)
                        return;
                    if (original)
                        this.acceptMixed(state, original, state.mixedPending);
                    else
                        state.mixedMissing = true;
                }
                const mixedId = state.mixedBatch?.batch_id || mixedHistory.items.find(item => item.producer_status === "running")?.batch_id || mixedHistory.items[0]?.batch_id;
                if (mixedId) {
                    const result = await this.readMixedBatch(state, mixedId);
                    if (!this.isCurrent(key, generation) || version !== this.loadVersion)
                        return;
                    if (mixedVersion === state.mixedVersion)
                        state.mixedBatch = result;
                }
                // 历史始终可发现原生产者；手工选择已结束批次时不强制覆盖审阅对象。
                const latest = history.items.find(item => item.producer_status === "running") || history.items[0];
                if (state.historyCursors.length === 1)
                    state.activeBatchId = history.items.find(item => item.producer_status === "running")?.batch_id || null;
                const id = history.items.find(item => item.request_key === state.pending?.request_key)?.batch_id || state.batch?.batch_id || latest?.batch_id;
                const activeId = state.activeBatchId;
                if (id) {
                    const [result, active] = await Promise.all([
                        requestJson(`${base}/native-batches/${encodeURIComponent(id)}`),
                        activeId && activeId !== id ? requestJson(`${base}/native-batches/${encodeURIComponent(activeId)}`) : Promise.resolve(null),
                    ]);
                    if (!this.isCurrent(key, generation) || version !== this.loadVersion)
                        return;
                    if (queryVersion === state.queryVersion) {
                        state.batch = result;
                        if (state.pending?.request_key === result.request_key)
                            state.pending = null;
                    }
                    if (result.producer_status === "running")
                        state.activeBatchId = result.batch_id;
                    else if (state.activeBatchId === result.batch_id)
                        state.activeBatchId = null;
                    if (active)
                        state.activeBatchId = active.producer_status === "running" ? active.batch_id : null;
                }
            }
            catch (error) {
                if (this.isCurrent(key, generation) && version === this.loadVersion) {
                    state.message = errorText(error);
                    this.notice(errorText(error), true);
                }
            }
            finally {
                if (state.loadingVersion === version)
                    state.busy = false;
                if (this.isCurrent(key, generation) && version === this.loadVersion) {
                    this.render();
                    this.schedule();
                }
            }
        }
        else {
            const id = this.topicId;
            if (!id)
                return;
            try {
                const [catalog, operations] = await Promise.all([
                    requestJson(this.base(id)), requestJson(this.base(id) + "/operations"),
                ]);
                if (!this.isCurrent(key, generation) || version !== this.loadVersion)
                    return;
                this.catalog = catalog;
                this.operations = operations;
                this.render();
                this.schedule();
            }
            catch (error) {
                if (this.isCurrent(key, generation))
                    this.notice(errorText(error), true);
            }
        }
    }
    async readPage(state) {
        const params = new URLSearchParams({ filter: state.filter, limit: "32" });
        const cursor = state.cursors.at(-1);
        if (cursor)
            params.set("cursor", cursor);
        try {
            return { page: await requestJson(`${this.native(state)}/native-changes?${params}`), reset: false };
        }
        catch (error) {
            if (!(error instanceof ApiError) || !["workspace_list_stale", "workspace_list_cursor_invalid"].includes(error.code || ""))
                throw error;
            params.delete("cursor");
            return { page: await requestJson(`${this.native(state)}/native-changes?${params}`), reset: true };
        }
    }
    readHistory(state) {
        const params = new URLSearchParams({ limit: "32" });
        const cursor = state.historyCursors.at(-1);
        if (cursor)
            params.set("cursor", cursor);
        return requestJson(`${this.native(state)}/native-batches/history?${params}`);
    }
    async readMixedHistory(state) {
        const params = new URLSearchParams({ limit: "32" });
        const cursor = state.mixedCursors.at(-1);
        if (cursor)
            params.set("cursor", cursor);
        const value = await requestJson(`${this.native(state)}/mixed-batches/history?${params}`);
        if (value.scope_id !== `session:${state.agent.session_id}`)
            throw new Error(t("混合批次回程与当前会话不一致"));
        return value;
    }
    validateMixed(state, value, id, requestKey) {
        if (value.scope_id !== `session:${state.agent.session_id}` || id && value.batch_id !== id || requestKey && value.request_key !== requestKey
            || !Array.isArray(value.items) || value.items.length < 1 || value.items.length > 32
            || value.items.some((item, index) => item.position !== index || !["file", "directory"].includes(item.source)
                || item.operation && (item.operation.scope_id !== value.scope_id || item.operation.action !== value.action
                    || item.operation.proposal_id !== item.proposal_id || item.operation.expected_revision !== item.expected_revision || item.operation.status !== item.status)
                || !item.operation && !["pending", "rejected", "skipped"].includes(item.status)))
            throw new Error(t("混合批次回程与当前会话不一致"));
        const done = value.items.filter(item => item.status === "done").length;
        const expected = value.producer_status === "running" ? "running"
            : value.items.some(item => item.status === "running" || item.status === "uncertain") ? "uncertain"
                : done === value.items.length ? "done"
                    : value.producer_status === "cancelled" || value.producer_status === "interrupted" ? value.producer_status
                        : done ? "partial" : "failed";
        if (value.status !== expected || new Set(value.items.map(item => `${item.source}:${item.proposal_id}`)).size !== value.items.length
            || value.items.some(item => item.source === "file" && item.target_path != null
                || item.operation && typeof item.operation.created_at !== (item.source === "file" ? "string" : "number")))
            throw new Error(t("混合批次回程与原提案不一致"));
        return value;
    }
    async readMixedBatch(state, id) {
        return this.validateMixed(state, await requestJson(`${this.native(state)}/mixed-batches/${encodeURIComponent(id)}`), id);
    }
    storageKey(state) { return `krim:mixed-pending:${state.agent.session_id}`; }
    persistMixed(state) {
        if (this.workspaces.get(state.agent.agent_id) !== state)
            return;
        if (state.mixedPending)
            sessionStorage.setItem(this.storageKey(state), JSON.stringify({ version: 1,
                request: state.mixedPending, fingerprint: state.mixedFingerprint }));
        else
            sessionStorage.removeItem(this.storageKey(state));
    }
    loadMixedPending(state) {
        try {
            const raw = sessionStorage.getItem(this.storageKey(state));
            if (!raw)
                return;
            if (new TextEncoder().encode(raw).length > 17000)
                throw new Error();
            const value = JSON.parse(raw);
            const request = value?.request;
            if (Object.keys(value).sort().join() !== "fingerprint,request,version" || value.version !== 1
                || typeof value.fingerprint !== "string" || !/^[0-9a-f]{64}$/.test(value.fingerprint)
                || !request || Object.keys(request).sort().join() !== "action,request_key,targets"
                || !["apply", "keep", "undo"].includes(request.action)
                || typeof request.request_key !== "string" || !request.request_key || request.request_key.length > 128 || /[\x00-\x1f\x7f]/.test(request.request_key)
                || !Array.isArray(request.targets) || !request.targets.length || request.targets.length > 32
                || request.targets.some((item) => Object.keys(item).sort().join() !== "expected_revision,proposal_id,source"
                    || !["file", "directory"].includes(item.source) || !identifier(item.proposal_id) || !Number.isSafeInteger(item.expected_revision) || item.expected_revision < 0)
                || new Set(request.targets.map(candidateKey)).size !== request.targets.length)
                throw new Error();
            state.mixedPending = request;
            state.mixedFingerprint = value.fingerprint;
            state.mixedQuery = request.request_key;
        }
        catch {
            state.mixedCacheInvalid = true;
            state.mixedMessage = t("本地混合恢复记录无效，请查询历史");
        }
    }
    acceptMixed(state, value, request) {
        if (this.workspaces.get(state.agent.agent_id) !== state)
            return;
        this.validateMixed(state, value, undefined, request.request_key);
        if (value.action !== request.action || value.items.length !== request.targets.length
            || value.items.some((item, index) => item.source !== request.targets[index].source || item.proposal_id !== request.targets[index].proposal_id || item.expected_revision !== request.targets[index].expected_revision))
            throw new Error(t("混合批次回程与原提案不一致"));
        state.mixedBatch = value;
        state.mixedPending = null;
        state.mixedMissing = false;
        state.mixedSelected.clear();
        state.mixedMessage = "";
        state.mixedCandidatesLoaded = false;
        this.persistMixed(state);
    }
    asCandidate(source, value, state) {
        if (value.scope_id !== `session:${state.agent.session_id}` || !identifier(value.proposal_id) || !Number.isSafeInteger(value.revision)
            || value.revision < 0 || value.workspace_key !== state.grant?.key || value.resource_fingerprint !== state.grant?.fingerprint)
            throw new Error(t("混合批次回程与原提案不一致"));
        if (source === "file") {
            const file = value;
            return { ...file, source, path: file.preview.path, description: `${file.preview.operation} · ${file.preview.before_sha256 || "∅"} → ${file.preview.after_sha256 || "∅"} · ${file.preview.before_bytes} → ${file.preview.after_bytes} bytes`, diff: file.preview.diff, truncated: file.preview.truncated };
        }
        const directory = value;
        return { ...directory, source, description: `${directory.operation} · ${directory.before_fingerprint || "∅"} · mode ${directory.mode?.toString(8) || "—"}` };
    }
    async readMixedCandidates() {
        const state = this.workspace();
        if (!state || state.mixedSending || state.busy)
            return;
        const key = this.key(), generation = this.generation;
        state.busy = true;
        this.render();
        try {
            const grant = await requestJson(this.native(state));
            const params = new URLSearchParams({ limit: "20" });
            const after = state.mixedDirectoryAfter.at(-1);
            if (after)
                params.set("after", after);
            const [files, directories] = await Promise.all([
                requestJson(`${this.native(state)}/changes?offset=${state.mixedFileOffset}&limit=20`),
                grant.tool_names.includes("directory_query") ? requestJson(`${this.native(state)}/directory-changes?${params}`) : Promise.resolve(null),
            ]);
            if (!this.isCurrent(key, generation))
                return;
            state.grant = grant;
            if (!Array.isArray(files) || files.length > 20 || directories && (directories.kind !== "proposals" || !Array.isArray(directories.proposals) || directories.proposals.length > 20))
                throw new Error(t("混合批次回程与原提案不一致"));
            files.forEach(item => this.asCandidate("file", item, state));
            directories?.proposals.forEach(item => this.asCandidate("directory", item, state));
            state.mixedFiles = files;
            state.mixedDirectories = directories?.proposals || [];
            state.mixedDirectoryNext = directories?.next_after || null;
            state.mixedCandidatesLoaded = true;
            state.mixedMessage = "";
        }
        catch (error) {
            if (this.isCurrent(key, generation))
                state.mixedMessage = errorText(error);
        }
        finally {
            state.busy = false;
            if (this.isCurrent(key, generation))
                this.render();
        }
    }
    async candidatePage(source, direction) {
        const state = this.workspace();
        if (!state || state.busy)
            return;
        if (source === "file")
            state.mixedFileOffset = Math.max(0, state.mixedFileOffset + direction * 20);
        else if (direction < 0) {
            if (state.mixedDirectoryAfter.length <= 1)
                return;
            state.mixedDirectoryAfter.pop();
        }
        else {
            if (!state.mixedDirectoryNext)
                return;
            state.mixedDirectoryAfter.push(state.mixedDirectoryNext);
        }
        await this.readMixedCandidates();
    }
    mixedWritable(state) {
        return state.agent.session.status === "open" && !["ask", "plan"].includes(String(state.agent.session.permissions.mode))
            && state.grant?.available === true && state.grant.writable;
    }
    candidateEligible(state, item, action) {
        return this.mixedWritable(state) && state.grant.tool_names.includes(item.source === "file" ? "workspace_change" : "directory_change")
            && (action === "apply" ? item.status === "proposed" && item.expires_at > Date.now() / 1000
                : action === "keep" ? item.status === "applied" : item.status === "applied" || item.status === "kept");
    }
    async recoverMixedPending() {
        const state = this.workspace();
        if (!state?.mixedPending)
            return;
        await this.queryMixed("key", state.mixedPending.request_key);
    }
    async startMixed(action, retry = false) {
        const state = this.workspace();
        if (!state || state.busy || state.mixedSending || state.mixedCacheInvalid || !this.mixedWritable(state))
            return;
        const key = this.key(), generation = this.generation;
        const request = retry ? state.mixedPending : { action, request_key: crypto.randomUUID(),
            targets: Array.from(state.mixedSelected.values(), item => ({ source: item.source, proposal_id: item.proposal_id, expected_revision: item.revision })) };
        if (!request || !request.targets.length || request.targets.length > 32 || retry && !state.mixedMissing || !retry && state.mixedPending)
            return;
        if (state.mixedHistory?.items.some(item => item.producer_status === "running") || state.mixedBatch?.producer_status === "running")
            return;
        state.mixedSending = true;
        this.render();
        try {
            const current = await Promise.all(request.targets.map(async (target) => {
                const path = target.source === "file" ? "changes" : "directory-changes";
                const value = await requestJson(`${this.native(state)}/${path}/${encodeURIComponent(target.proposal_id)}`);
                const item = this.asCandidate(target.source, value, state);
                if (item.proposal_id !== target.proposal_id || item.revision !== target.expected_revision || !this.candidateEligible(state, item, request.action))
                    throw new Error(t("混合提案版本或状态已变化，请重新审阅"));
                return item;
            }));
            if (!this.isCurrent(key, generation))
                return;
            if (retry && state.mixedFingerprint !== state.grant?.fingerprint)
                throw new Error(t("工作区资源版本已变化，请查询原历史"));
            const label = t({ apply: "应用混合批次", keep: "保留混合批次", undo: "撤销混合批次" }[request.action]);
            if (!window.confirm(`${t("确认所选混合提案？")}\n${label} · ${state.agent.name}\n${current.map(item => `${item.source} · ${item.path}${item.target_path ? ` → ${item.target_path}` : ""} · r${item.revision}\n${item.description}`).join("\n")}\n${t("各项独立执行，可能部分成功")}`))
                return;
            state.mixedPending = request;
            state.mixedFingerprint = state.grant.fingerprint;
            state.mixedMissing = false;
            state.mixedQuery = request.request_key;
            this.persistMixed(state);
            const value = await requestJson(`${this.native(state)}/mixed-batches?confirmed=true`, { method: "POST", body: JSON.stringify(request) });
            this.acceptMixed(state, value, request);
        }
        catch (error) {
            state.mixedMessage = errorText(error);
            if (state.mixedPending) {
                try {
                    const original = await requestJsonNullable(`${this.native(state)}/mixed-batches/by-key?${new URLSearchParams({ request_key: request.request_key })}`);
                    if (original)
                        this.acceptMixed(state, original, request);
                    else if (error instanceof ApiError) {
                        state.mixedPending = null;
                        state.mixedMissing = false;
                        this.persistMixed(state);
                    }
                    else
                        state.mixedMissing = true;
                }
                catch { /* 未知结果保留原键，只允许用户主动查询。 */ }
            }
        }
        finally {
            state.mixedSending = false;
            if (this.isCurrent(key, generation)) {
                this.render();
                this.schedule();
            }
        }
    }
    async mixedPage(direction) {
        const state = this.workspace();
        if (!state || state.busy)
            return;
        if (direction < 0) {
            if (state.mixedCursors.length <= 1)
                return;
            state.mixedCursors.pop();
        }
        else {
            if (!state.mixedHistory?.next_cursor)
                return;
            state.mixedCursors.push(state.mixedHistory.next_cursor);
        }
        await this.refresh();
    }
    async page(direction) {
        const state = this.workspace();
        if (!state || state.busy || state.sending)
            return;
        if (direction < 0) {
            if (state.cursors.length <= 1)
                return;
            state.cursors.pop();
        }
        else {
            if (!state.page?.next_cursor)
                return;
            state.cursors.push(state.page.next_cursor);
        }
        await this.refresh();
    }
    async historyPage(direction) {
        const state = this.workspace();
        if (!state || state.busy)
            return;
        if (direction < 0) {
            if (state.historyCursors.length <= 1)
                return;
            state.historyCursors.pop();
        }
        else {
            if (!state.history?.next_cursor)
                return;
            state.historyCursors.push(state.history.next_cursor);
        }
        await this.refresh();
    }
    async bridge(channel, data) {
        if (!data || typeof data !== "object" || Array.isArray(data) || typeof data.topic_id !== "string")
            throw new Error(t("需要话题 ID"));
        const { topic_id: id, ...payload } = data;
        let result;
        if (channel === "changeList/get")
            result = await requestJson(this.base(id));
        else if (channel === "changeList/sync")
            result = await requestJson(this.base(id), { method: "PUT", body: JSON.stringify(payload) });
        else if (channel === "changeList/clear")
            result = await requestJson(this.base(id) + "/clear", { method: "POST", body: JSON.stringify(payload) });
        else
            throw new Error(t("变更清单信道无效"));
        if (this.source.kind === "host" && id === this.topicId && channel !== "changeList/get")
            await this.refresh();
        return result;
    }
    async clear() {
        if (this.source.kind !== "host" || !this.catalog || !this.topicId || this.catalog.active_operation_id)
            return;
        const key = this.key(), generation = this.generation;
        try {
            await this.bridge("changeList/clear", { topic_id: this.topicId, revision: this.catalog.revision });
        }
        catch (error) {
            if (this.isCurrent(key, generation)) {
                this.notice(errorText(error), true);
                await this.refresh();
            }
        }
    }
    async hostClick(item) {
        if (!this.topicId || !this.catalog || !this.sessionId)
            return;
        const key = this.key(), generation = this.generation;
        await requestJson(this.base(this.topicId) + "/click", { method: "POST", body: JSON.stringify({ revision: this.catalog.revision, item_id: item.id, host_session_id: this.sessionId }) });
        if (this.isCurrent(key, generation))
            this.notice(t("已通知宿主查看变更"));
    }
    writable(state) {
        return state.agent.session.status === "open" && !["ask", "plan"].includes(String(state.agent.session.permissions.mode))
            && state.grant?.available === true && state.grant.writable && state.grant.tool_names.includes("workspace_change");
    }
    eligible(item, action) {
        return !item.active_operation_id && (item.status === "applied" || action === "undo" && item.status === "kept");
    }
    async invoke(action, itemId) {
        const state = this.workspace(), key = this.key(), generation = this.generation;
        if (state) {
            if (!state.page || state.stale || state.busy || state.sending || state.pending || !this.writable(state))
                return;
            const items = state.page.items.filter(item => (itemId === null || item.proposal_id === itemId) && this.eligible(item, action));
            if (!items.length)
                return;
            if (!window.confirm(t("确认{action}当前页的{count}项修改？", { action: actionText(action), count: items.length }) + `\n${state.agent.name}\n${items.map(item => item.path).join("\n")}`))
                return;
            const request = { action, snapshot_revision: state.page.snapshot_revision,
                targets: items.map(item => ({ proposal_id: item.proposal_id, expected_revision: item.revision })), request_key: crypto.randomUUID() };
            state.pending = request;
            state.query = request.request_key;
            state.sending = true;
            state.stale = true;
            state.message = "";
            this.render();
            try {
                state.batch = await requestJson(`${this.native(state)}/native-batches?confirmed=true`, { method: "POST", body: JSON.stringify(request) });
                state.activeBatchId = state.batch.producer_status === "running" ? state.batch.batch_id : null;
                state.pending = null;
            }
            catch (error) {
                state.message = errorText(error);
                // 只查询原键，接受响应丢失不会再 POST，也不替换原 snapshot/targets。
                try {
                    const found = await requestJsonNullable(`${this.native(state)}/native-batches/by-key?${new URLSearchParams({ request_key: request.request_key })}`);
                    if (found) {
                        state.batch = found;
                        state.pending = null;
                        state.activeBatchId = found.producer_status === "running" ? found.batch_id : null;
                    }
                    else if (error instanceof ApiError && error.status >= 400 && error.status < 500)
                        state.pending = null;
                    else
                        state.message = t("未找到原批次，结果尚未确认，请按原键继续查询");
                }
                catch {
                    state.message = t("批次响应未确认，请按原键找回");
                }
                if (this.isCurrent(key, generation))
                    this.notice(state.message, true);
            }
            finally {
                state.sending = false;
                // 切到别的scope再切回同一缓存时，旧POST仍是该缓存的真实幂等动作；
                // 必须解除sending并重新读取账本，不能因为旧generation而让按钮永久锁死。
                const ownsState = this.workspaces.get(state.agent.agent_id) === state;
                if (ownsState && this.workspace() === state) {
                    state.cursors = [null];
                    state.historyCursors = [null];
                    this.render();
                    await this.refresh();
                }
            }
            return;
        }
        const id = this.topicId, catalog = this.catalog;
        if (!id || !catalog || !this.sessionId || this.pendingTopics.has(id) || catalog.active_operation_id)
            return;
        this.pendingTopics.add(id);
        this.render();
        this.schedule();
        try {
            const operation = await this.invokeHost({ topic_id: id, revision: catalog.revision, action: itemId === null ? `${action}-all` : action,
                item_id: itemId, idempotency_key: crypto.randomUUID() });
            if (this.isCurrent(key, generation))
                this.notice(t(operation.status === "done" ? "变更操作已完成" : operation.status === "uncertain" ? "结果待宿主核实" : "部分变更处理失败"), operation.status !== "done");
        }
        catch (error) {
            if (this.isCurrent(key, generation))
                this.notice(errorText(error), true);
        }
        finally {
            this.pendingTopics.delete(id);
            if (this.isCurrent(key, generation))
                await this.refresh();
        }
    }
    async cancel() {
        const state = this.workspace(), key = this.key(), generation = this.generation;
        try {
            if (state) {
                if (!state.activeBatchId)
                    return;
                const result = await requestJson(`${this.native(state)}/native-batches/${encodeURIComponent(state.activeBatchId)}/cancel`, { method: "POST" });
                state.batch = result;
                state.queryVersion += 1;
                state.activeBatchId = result.producer_status === "running" ? result.batch_id : null;
            }
            else {
                if (!this.topicId || !this.catalog?.active_operation_id)
                    return;
                await requestJson(this.base(this.topicId) + `/operations/${encodeURIComponent(this.catalog.active_operation_id)}/cancel`, { method: "POST" });
            }
            if (this.isCurrent(key, generation))
                await this.refresh(true);
        }
        catch (error) {
            if (this.isCurrent(key, generation))
                this.notice(errorText(error), true);
        }
    }
    async query(kind, id) {
        const state = this.workspace();
        if (!state)
            return;
        const value = id || (kind === "wait" ? state.batch?.batch_id : state.query);
        if (!value)
            return;
        const key = this.key(), generation = this.generation, version = ++state.queryVersion;
        try {
            const base = `${this.native(state)}/native-batches`;
            const result = kind === "key"
                ? await requestJsonNullable(`${base}/by-key?${new URLSearchParams({ request_key: value })}`)
                : await requestJson(`${base}/${encodeURIComponent(value)}${kind === "wait" ? "/wait?timeout_seconds=2" : ""}`, kind === "wait" ? { method: "POST" } : undefined);
            if (!this.isCurrent(key, generation) || version !== state.queryVersion)
                return;
            if (result) {
                state.batch = result;
                if (result.producer_status === "running")
                    state.activeBatchId = result.batch_id;
                else if (result.batch_id === state.activeBatchId)
                    state.activeBatchId = null;
                if (state.pending?.request_key === result.request_key)
                    state.pending = null;
                state.message = "";
            }
            else
                state.message = t("未找到原批次，结果尚未确认，请按原键继续查询");
            this.render();
            this.schedule();
        }
        catch (error) {
            if (this.isCurrent(key, generation) && version === state.queryVersion) {
                state.message = errorText(error);
                this.render();
                this.notice(errorText(error), true);
            }
        }
    }
    stopTimer() { window.clearTimeout(this.timer); this.timer = undefined; }
    async queryMixed(kind, id) {
        const state = this.workspace();
        if (!state)
            return;
        const value = id || (kind === "wait" ? state.mixedBatch?.batch_id : state.mixedQuery);
        if (!value)
            return;
        const key = this.key(), generation = this.generation, version = ++state.mixedVersion;
        try {
            const base = `${this.native(state)}/mixed-batches`;
            const result = kind === "key"
                ? await requestJsonNullable(`${base}/by-key?${new URLSearchParams({ request_key: value })}`)
                : await requestJson(`${base}/${encodeURIComponent(value)}${kind === "wait" ? "/wait?timeout_seconds=2" : ""}`, kind === "wait" ? { method: "POST" } : undefined);
            if (!this.isCurrent(key, generation) || version !== state.mixedVersion)
                return;
            if (result) {
                state.mixedBatch = this.validateMixed(state, result, kind === "key" ? undefined : value, kind === "key" ? value : undefined);
                if (state.mixedPending?.request_key === result.request_key)
                    this.acceptMixed(state, result, state.mixedPending);
                state.mixedMessage = "";
            }
            else {
                state.mixedMessage = t("未找到原批次，结果尚未确认，请按原键继续查询");
                if (state.mixedPending?.request_key === value)
                    state.mixedMissing = true;
            }
            this.render();
            this.schedule();
        }
        catch (error) {
            if (this.isCurrent(key, generation) && version === state.mixedVersion) {
                state.mixedMessage = errorText(error);
                this.render();
                this.notice(errorText(error), true);
            }
        }
    }
    async cancelMixed() {
        const state = this.workspace(), key = this.key(), generation = this.generation;
        if (!state?.mixedBatch || state.mixedBatch.producer_status !== "running" || !window.confirm(t("确认取消原混合批次？")))
            return;
        const id = state.mixedBatch.batch_id, version = ++state.mixedVersion;
        try {
            const result = await requestJson(`${this.native(state)}/mixed-batches/${encodeURIComponent(id)}/cancel`, { method: "POST" });
            if (!this.isCurrent(key, generation) || version !== state.mixedVersion)
                return;
            state.mixedBatch = this.validateMixed(state, result, id);
            state.mixedMessage = "";
            this.render();
            await this.refresh();
        }
        catch (error) {
            if (this.isCurrent(key, generation) && version === state.mixedVersion) {
                state.mixedMessage = errorText(error);
                this.render();
                await this.queryMixed("id", id);
            }
        }
    }
    schedule() {
        this.stopTimer();
        if (!node("change-panel").open)
            return;
        const state = this.workspace();
        const active = state ? Boolean(state.activeBatchId || state.mixedBatch?.producer_status === "running") : Boolean(this.operations.some(item => item.execution.operation_id === this.catalog?.active_operation_id && item.status === "executing") || this.topicId && this.pendingTopics.has(this.topicId));
        if (!active)
            return;
        const key = this.key(), generation = this.generation;
        this.timer = window.setTimeout(() => { if (this.isCurrent(key, generation))
            void this.refresh(); }, 700);
    }
    row(row, disabled, key, generation) {
        const card = document.createElement("article");
        card.className = "change-card";
        card.dataset.changeId = row.id;
        const title = document.createElement("button");
        title.type = "button";
        title.className = "secondary change-title";
        title.textContent = row.title;
        title.disabled = this.source.kind === "host" && !this.sessionId;
        title.onclick = () => { if (this.isCurrent(key, generation))
            void row.click().catch(error => { if (this.isCurrent(key, generation))
                this.notice(errorText(error), true); }); };
        const description = document.createElement("p");
        description.textContent = row.description;
        const status = document.createElement("span");
        status.className = "badge";
        status.textContent = row.status === "uncertain" && this.source.kind === "host" ? t("结果待宿主核实") : statusText(row.status);
        status.dataset.changeStatus = row.status;
        const actions = document.createElement("div");
        actions.className = "actions";
        for (const action of ["undo", "keep"]) {
            const control = document.createElement("button");
            control.type = "button";
            control.className = "secondary";
            control.textContent = actionText(action);
            control.disabled = disabled || !row[action];
            control.onclick = () => { if (this.isCurrent(key, generation))
                void this.invoke(action, row.id); };
            actions.append(control);
        }
        card.append(title, description, status, actions);
        return card;
    }
    render() {
        const state = this.workspace(), native = Boolean(state), key = this.key(), generation = this.generation;
        const root = node("change-items");
        root.replaceChildren();
        node("change-native-pages").hidden = node("change-native-history").hidden = node("change-mixed-history").hidden = node("change-mixed-review").hidden = !native;
        node("change-clear").hidden = native;
        for (const [id, label] of [["change-undo-all", native ? "撤销本页" : "全部撤销"], ["change-keep-all", native ? "保留本页" : "全部保留"]]) {
            node(id).dataset.i18n = label;
            node(id).textContent = t(label);
        }
        let rows, busy;
        if (state) {
            busy = state.busy || state.sending || state.pending !== null || state.stale || !this.writable(state) || Boolean(state.activeBatchId);
            rows = (state.page?.items || []).map(item => ({ id: item.proposal_id, title: item.path,
                description: `${t("修订版本")} ${item.revision} · ${item.order_known ? `${t("文件效果顺序")} ${item.apply_sequence}` : t("历史顺序未知")}`,
                status: item.status, undo: this.eligible(item, "undo"), keep: this.eligible(item, "keep"),
                click: () => this.openWorkspaceProposal(state.agent.agent_id, item.proposal_id, item.active_operation_id || undefined) }));
            node("change-filter").value = state.filter;
            node("change-filter").disabled = state.busy || state.sending;
            button("change-prev").disabled = state.busy || state.sending || state.cursors.length <= 1;
            button("change-next").disabled = state.busy || state.sending || !state.page?.next_cursor;
            node("change-page-info").textContent = t("第{page}页 · 本页{count}项 · 共{total}项", { page: state.cursors.length, count: state.page?.items.length || 0, total: state.page?.total || 0 });
            node("change-snapshot").textContent = state.page ? `${t("清单快照")} ${state.page.snapshot_revision}` : "";
            node("change-status").textContent = state.message || (state.pending ? t("批次响应未确认，请按原键找回") : state.busy ? t("正在读取工作区") : !this.writable(state) ? t("当前会话只能查询修改历史") : !rows.length ? t("暂无变更") : "");
            button("change-cancel").disabled = !state.activeBatchId;
            node("change-batch-query").value = state.query;
            this.renderNativeResults(state);
            this.renderMixedResults(state);
        }
        else {
            busy = Boolean(this.catalog?.active_operation_id || this.topicId && this.pendingTopics.has(this.topicId)) || !this.sessionId;
            rows = (this.catalog?.items || []).map(item => ({ ...item, undo: item.status === "pending", keep: item.status === "pending", click: () => this.hostClick(item) }));
            const active = this.operations.find(op => op.execution.operation_id === this.catalog?.active_operation_id);
            node("change-status").textContent = active?.status === "uncertain" ? t("结果待宿主核实") : this.catalog?.active_operation_id || this.topicId && this.pendingTopics.has(this.topicId) ? t("变更处理中") : !this.sessionId ? t("连接宿主后可撤销或保留变更") : !rows.length ? t("暂无变更") : "";
            button("change-clear").disabled = Boolean(this.catalog?.active_operation_id || this.topicId && this.pendingTopics.has(this.topicId)) || !rows.length;
            button("change-cancel").disabled = active?.status !== "executing";
            const history = node("change-results");
            history.replaceChildren();
            for (const operation of this.operations.slice(0, 5)) {
                const line = document.createElement("p");
                line.className = "muted";
                const label = operation.status === "done" ? "变更操作已完成" : operation.status === "error" ? "部分变更处理失败" : operation.status === "uncertain" ? "结果待宿主核实" : "变更处理中";
                const failed = operation.receipt?.results.filter(result => result.status !== "done").map(result => result.item_id) || [];
                line.textContent = t(label) + (failed.length ? ` · ${failed.join(", ")}` : "");
                history.append(line);
            }
        }
        for (const row of rows)
            root.append(this.row(row, busy, key, generation));
        button("change-undo-all").disabled = busy || !rows.some(row => row.undo);
        button("change-keep-all").disabled = busy || !rows.some(row => row.keep);
        button("change-refresh").disabled = state ? state.busy || state.sending : !this.topicId;
        this.syncQueryButtons();
    }
    syncQueryButtons() {
        const state = this.workspace();
        button("change-query-id").disabled = button("change-query-key").disabled = !state?.query;
        button("change-wait").disabled = !state?.batch || state.batch.producer_status !== "running";
        button("change-mixed-query-id").disabled = button("change-mixed-query-key").disabled = !state?.mixedQuery;
        button("change-mixed-wait").disabled = button("change-mixed-cancel").disabled = !state?.mixedBatch || state.mixedBatch.producer_status !== "running";
    }
    renderNativeResults(state) {
        const results = node("change-results");
        results.replaceChildren();
        const key = this.key(), generation = this.generation, batch = state.batch;
        if (batch) {
            const heading = document.createElement("p");
            heading.className = "native-batch-result";
            heading.dataset.batchId = batch.batch_id;
            heading.textContent = `${statusText(batch.status)} · ${batch.batch_id} · ${batch.request_key}${batch.stop_reason ? ` · ${batch.stop_reason}` : ""}`;
            results.append(heading);
            for (const item of batch.items) {
                const line = document.createElement("p");
                line.dataset.batchItem = item.proposal_id;
                line.textContent = `${item.path} · ${statusText(item.status)}${item.error_code ? ` · ${item.error_code}` : ""} · ${item.operation?.operation_id || t("尚无关联文件操作")}`;
                const open = document.createElement("button");
                open.type = "button";
                open.textContent = t("查看文件差异与证明");
                open.onclick = () => { if (this.isCurrent(key, generation))
                    void this.openWorkspaceProposal(state.agent.agent_id, item.proposal_id, item.operation?.operation_id).catch(error => { if (this.isCurrent(key, generation))
                        this.notice(errorText(error), true); }); };
                line.append(open);
                results.append(line);
            }
        }
        const history = node("change-batches");
        history.replaceChildren();
        for (const summary of state.history?.items || []) {
            const control = document.createElement("button");
            control.type = "button";
            control.className = "secondary";
            control.dataset.batchId = summary.batch_id;
            control.textContent = `${actionText(summary.action)} · ${statusText(summary.status)} · ${summary.done_count}/${summary.item_count} · ${summary.batch_id} · ${localDate(summary.created_at)}`;
            control.onclick = () => { if (this.isCurrent(key, generation))
                void this.query("id", summary.batch_id); };
            history.append(control);
        }
        node("change-history-info").textContent = t("批次历史共{total}项", { total: state.history?.total || 0 });
        button("change-history-prev").disabled = state.busy || state.historyCursors.length <= 1;
        button("change-history-next").disabled = state.busy || !state.history?.next_cursor;
    }
    renderMixedResults(state) {
        const results = node("change-mixed-results");
        results.replaceChildren();
        const history = node("change-mixed-batches");
        history.replaceChildren();
        const key = this.key(), generation = this.generation, batch = state.mixedBatch;
        this.renderMixedCandidates(state);
        node("change-mixed-query").value = state.mixedQuery;
        node("change-mixed-info").textContent = t("批次历史共{total}项", { total: state.mixedHistory?.total || 0 });
        node("change-mixed-status").textContent = state.mixedMessage;
        const batchAction = (action) => t({ apply: "应用混合批次", keep: "保留混合批次", undo: "撤销混合批次" }[action]);
        for (const summary of state.mixedHistory?.items || []) {
            const control = document.createElement("button");
            control.type = "button";
            control.className = "secondary";
            control.dataset.mixedBatchId = summary.batch_id;
            control.textContent = `${batchAction(summary.action)} · ${mixedStatusText(summary.status)} · ${summary.done_count}/${summary.item_count} · ${summary.batch_id} · ${localDate(summary.created_at)}`;
            control.onclick = () => { if (this.isCurrent(key, generation))
                void this.queryMixed("id", summary.batch_id); };
            history.append(control);
        }
        if (batch) {
            const heading = document.createElement("p");
            heading.className = "mixed-batch-result";
            heading.dataset.mixedBatchId = batch.batch_id;
            heading.textContent = `${batchAction(batch.action)} · ${mixedStatusText(batch.status)} · ${batch.batch_id} · ${batch.request_key}${batch.stop_reason ? ` · ${batch.stop_reason}` : ""}`;
            results.append(heading);
            for (const item of batch.items) {
                const line = document.createElement("p");
                line.dataset.mixedBatchItem = item.proposal_id;
                line.dataset.source = item.source;
                line.textContent = `${t(item.source === "file" ? "文件" : "目录")} · ${item.path}${item.target_path ? ` → ${item.target_path}` : ""} · ${mixedStatusText(item.status)} · ${item.operation?.operation_id || t("尚无关联原操作")}${item.error_code ? ` · ${item.error_code}` : ""}`;
                const open = document.createElement("button");
                open.type = "button";
                open.textContent = t(item.source === "file" ? "查看文件差异与证明" : "查看原目录提案");
                open.onclick = () => { if (this.isCurrent(key, generation))
                    void (item.source === "file" ? this.openWorkspaceProposal(state.agent.agent_id, item.proposal_id, item.operation?.operation_id) : this.openDirectoryProposal(state.agent.agent_id, item.proposal_id)).catch(error => { if (this.isCurrent(key, generation))
                        this.notice(errorText(error), true); }); };
                line.append(open);
                results.append(line);
            }
        }
        button("change-mixed-prev").disabled = state.busy || state.mixedCursors.length <= 1;
        button("change-mixed-next").disabled = state.busy || !state.mixedHistory?.next_cursor;
    }
    renderMixedCandidates(state) {
        const container = node("change-mixed-options");
        container.replaceChildren();
        const key = this.key(), generation = this.generation;
        for (const [source, values] of [["file", state.grant ? state.mixedFiles : []], ["directory", state.grant ? state.mixedDirectories : []]]) {
            for (const value of values) {
                const item = this.asCandidate(source, value, state), id = candidateKey(item);
                const row = document.createElement("article");
                row.className = "change-card";
                row.dataset.mixedCandidate = id;
                const label = document.createElement("label"), choose = document.createElement("input");
                choose.type = "checkbox";
                choose.checked = state.mixedSelected.has(id);
                choose.disabled = state.busy || state.mixedSending || Boolean(state.mixedPending) || !this.mixedWritable(state)
                    || !["proposed", "applied", "kept"].includes(item.status) || !state.grant?.tool_names.includes(source === "file" ? "workspace_change" : "directory_change");
                choose.onchange = () => { if (!this.isCurrent(key, generation))
                    return; if (choose.checked) {
                    if (state.mixedSelected.size >= 32) {
                        choose.checked = false;
                        state.mixedMessage = t("最多选择32项混合提案");
                    }
                    else
                        state.mixedSelected.set(id, item);
                }
                else
                    state.mixedSelected.delete(id); this.render(); };
                const name = document.createElement("span");
                name.textContent = `${t(source === "file" ? "文件" : "目录")} · ${item.path}${item.target_path ? ` → ${item.target_path}` : ""} · r${item.revision} · ${item.status}`;
                label.append(choose, name);
                row.append(label);
                const condition = document.createElement("p");
                condition.textContent = item.description;
                row.append(condition);
                if (item.diff !== undefined) {
                    const details = document.createElement("details"), summary = document.createElement("summary"), diff = document.createElement("pre");
                    summary.textContent = t("查看所选文件差异");
                    diff.textContent = item.diff;
                    details.append(summary, diff);
                    row.append(details);
                    if (item.truncated) {
                        const note = document.createElement("p");
                        note.textContent = t("差异摘要已截断，执行使用原完整提案");
                        row.append(note);
                    }
                }
                const open = document.createElement("button");
                open.type = "button";
                open.textContent = t(source === "file" ? "查看文件差异与证明" : "查看原目录提案");
                open.onclick = () => { if (this.isCurrent(key, generation))
                    void (source === "file" ? this.openWorkspaceProposal(state.agent.agent_id, item.proposal_id) : this.openDirectoryProposal(state.agent.agent_id, item.proposal_id)).catch(error => this.notice(errorText(error), true)); };
                row.append(open);
                container.append(row);
            }
        }
        node("change-mixed-selected").textContent = `${t("已选择{count}项混合提案", { count: state.mixedSelected.size })}\n${Array.from(state.mixedSelected.values(), item => `${item.source} · ${item.path} · r${item.revision}`).join("\n")}`;
        for (const action of ["apply", "keep", "undo"])
            button(`change-mixed-${action}`).disabled = state.busy || state.mixedSending || Boolean(state.mixedPending) || state.mixedCacheInvalid || !state.mixedSelected.size
                || !Array.from(state.mixedSelected.values()).every(item => this.candidateEligible(state, item, action)) || state.mixedBatch?.producer_status === "running";
        button("change-mixed-options-refresh").disabled = state.busy || state.mixedSending || state.agent.session.status !== "open";
        button("change-mixed-clear-selection").disabled = !state.mixedSelected.size || state.mixedSending || Boolean(state.mixedPending);
        button("change-mixed-files-prev").disabled = state.busy || state.mixedFileOffset === 0;
        button("change-mixed-files-next").disabled = state.busy || state.mixedFiles.length < 20;
        button("change-mixed-directories-prev").disabled = state.busy || state.mixedDirectoryAfter.length <= 1;
        button("change-mixed-directories-next").disabled = state.busy || !state.mixedDirectoryNext;
        node("change-mixed-pending").hidden = !state.mixedPending && !state.mixedCacheInvalid;
        node("change-mixed-pending-key").textContent = state.mixedPending?.request_key || t("本地混合恢复记录无效，请查询历史");
        button("change-mixed-recover").disabled = !state.mixedPending;
        button("change-mixed-retry").disabled = !state.mixedPending || !state.mixedMissing || state.mixedSending || !this.mixedWritable(state);
        button("change-mixed-clear-cache").hidden = !state.mixedCacheInvalid;
    }
}
