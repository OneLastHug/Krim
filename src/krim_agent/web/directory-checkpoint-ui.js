/** 原目录捕获事实；写前保存原意图，所有恢复/轮询只查询原任务。 */
import { ApiError, errorText, requestJson, requestJsonNullable } from "./api.js";
import { localDate, t } from "./i18n.js";
import { DirectoryRestoreUI } from "./directory-restore-ui.js";
import { MissingDirectoryRestoreUI } from "./missing-directory-restore-ui.js";
const node = (id) => document.getElementById(id);
const input = (id) => node(id);
const button = (id) => node(id);
const ID = /^[A-Za-z0-9][A-Za-z0-9_-]{0,35}$/, SHA = /^[a-f0-9]{64}$/, PAGE = 8;
const labels = {
    capturing: "目录捕获中", available: "目录检查点可用", failed: "目录捕获失败",
    cancelled: "目录捕获已取消", interrupted: "目录捕获已中断",
};
function object(value) { return typeof value === "object" && value !== null && !Array.isArray(value); }
function keys(value, allowed) { return object(value) && Object.keys(value).length === allowed.length && Object.keys(value).every(key => allowed.includes(key)); }
function key(value) { return typeof value === "string" && value.length > 0 && value.length <= 128 && !/[\x00-\x1f\x7f]/.test(value); }
function relative(value) {
    return typeof value === "string" && new TextEncoder().encode(value).length <= 512 && value.length > 0
        && !/[\\:\x00-\x1f\x7f]/.test(value) && value.split("/").every(part => !["", ".", ".."].includes(part));
}
function integer(value, low, high) { return typeof value === "number" && Number.isSafeInteger(value) && value >= low && value <= high; }
function date(value) { return typeof value === "string" && /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+-]\d{2}:\d{2})$/.test(value) && Number.isFinite(Date.parse(value)); }
function manifest(state) {
    const value = state.agent.session.permissions.workspace;
    if (!object(value) || typeof value.key !== "string" || typeof value.fingerprint !== "string" || !SHA.test(value.fingerprint))
        throw new Error(t("工作区资源授权无效"));
    return { key: value.key, fingerprint: value.fingerprint };
}
function verifyRequest(value) {
    if (!keys(value, ["title", "root_path", "request_key"]) || typeof value.title !== "string" || !value.title.trim() || value.title.length > 120 || !relative(value.root_path) || !key(value.request_key))
        throw new Error(t("目录捕获请求无效"));
}
function verify(state, value) {
    const grant = manifest(state);
    if (!keys(value, ["source", "checkpoint_id", "scope_id", "title", "root_path", "request_key", "workspace_key", "resource_fingerprint", "status", "snapshot", "error_code", "created_at", "updated_at"])
        || value.source !== "workspace_directory" || value.scope_id !== `session:${state.agent.session_id}`
        || typeof value.checkpoint_id !== "string" || !ID.test(value.checkpoint_id) || typeof value.title !== "string" || !value.title.trim() || value.title.length > 120
        || !key(value.request_key) || !relative(value.root_path) || value.workspace_key !== grant.key || value.resource_fingerprint !== grant.fingerprint
        || !["capturing", "available", "failed", "cancelled", "interrupted"].includes(String(value.status)) || !date(value.created_at) || !date(value.updated_at) || Date.parse(value.updated_at) < Date.parse(value.created_at)
        || (["capturing", "available"].includes(String(value.status)) ? value.error_code !== null : typeof value.error_code !== "string" || !value.error_code || value.error_code.length > 128))
        throw new Error(t("目录检查点回程与原事实不一致"));
    const snapshot = value.snapshot;
    if (snapshot !== null && (!keys(snapshot, ["snapshot_fingerprint", "entry_count", "file_count", "directory_count", "byte_size"])
        || typeof snapshot.snapshot_fingerprint !== "string" || !SHA.test(snapshot.snapshot_fingerprint)
        || !integer(snapshot.entry_count, 1, 1024) || !integer(snapshot.file_count, 0, 1023) || !integer(snapshot.directory_count, 1, 1024)
        || snapshot.file_count + snapshot.directory_count !== snapshot.entry_count || !integer(snapshot.byte_size, 0, 33554432)
        || snapshot.file_count === 0 && snapshot.byte_size !== 0) || value.status === "available" && snapshot === null)
        throw new Error(t("目录检查点回程与原事实不一致"));
    const known = state.known.get(value.checkpoint_id);
    if (known)
        verifyTransition(known, value);
}
/** 已见身份和材料不可改写；仅捕获中的任务可按时间向终态推进。 */
function verifyTransition(before, after) {
    const fields = ["checkpoint_id", "scope_id", "source", "title", "root_path", "request_key", "workspace_key", "resource_fingerprint", "created_at"];
    if (fields.some(field => before[field] !== after[field]) || Date.parse(after.updated_at) < Date.parse(before.updated_at)
        || before.status !== "capturing" && (before.status !== after.status || before.error_code !== after.error_code)
        || before.status !== "capturing" && before.snapshot === null && after.snapshot !== null
        || before.snapshot !== null && (after.snapshot === null || Object.keys(before.snapshot).some(field => before.snapshot[field] !== after.snapshot[field])))
        throw new Error(t("目录检查点回程与原事实不一致"));
}
function line(parent, label, value) { const p = document.createElement("p"); p.textContent = `${t(label)}: ${value}`; parent.append(p); }
export class DirectoryCheckpointUI {
    notice;
    states = new Map();
    activeId = null;
    generation = 0;
    timer;
    restores;
    missingRestores;
    constructor(notice) {
        this.notice = notice;
        this.restores = new DirectoryRestoreUI(notice);
        this.missingRestores = new MissingDirectoryRestoreUI(notice);
        node("directory-restore-missing-backup").onclick = () => {
            const state = this.current(), value = this.restores.missingBaseline();
            if (state && value)
                void this.missingRestores.useBaseline(state.agent.agent_id, value).catch(error => this.notice(t(errorText(error)), true));
        };
        for (const [id, field] of [["directory-checkpoint-title", "title"], ["directory-checkpoint-path", "path"], ["directory-checkpoint-query", "query"]]) {
            input(id).oninput = () => { const state = this.current(); if (state)
                state[field] = input(id).value; this.controls(); };
        }
        node("directory-checkpoint-create").onclick = () => void this.create();
        node("directory-checkpoint-refresh").onclick = () => void this.refresh(true);
        node("directory-checkpoint-by-id").onclick = () => void this.query(false);
        node("directory-checkpoint-by-key").onclick = () => void this.query(true);
        node("directory-checkpoint-pending").onclick = () => void this.queryPending();
        node("directory-checkpoint-wait").onclick = () => { const state = this.current(); if (state?.selected)
            void this.read(state.selected.checkpoint_id, false, true); };
        node("directory-checkpoint-cancel").onclick = () => void this.cancel();
        for (const [id, delta] of [["directory-checkpoint-prev", -PAGE], ["directory-checkpoint-next", PAGE]])
            node(id).onclick = () => { const state = this.current(); if (state && !state.loading) {
                state.offset = Math.max(0, state.offset + delta);
                void this.refresh();
            } };
    }
    setSources(agents) {
        this.restores.setSources(agents);
        this.missingRestores.setSources(agents);
        const ids = new Set(agents.map(agent => agent.agent_id));
        let changed = false;
        for (const agent of agents) {
            const old = this.states.get(agent.agent_id);
            if (!old || old.agent.session_id !== agent.session_id || old.agent.session.status !== agent.session.status || JSON.stringify(old.agent.session.permissions) !== JSON.stringify(agent.session.permissions)) {
                const state = { agent, grant: null, title: t("目录检查点"), path: "", query: "", entries: [], known: new Map(), selected: null, selectedId: null, pending: null, storageError: false, message: "", stale: true, offset: 0, more: false, loading: false, querying: false, sending: false, cancelling: false, loadVersion: 0, queryVersion: 0 };
                this.restore(state);
                this.states.set(agent.agent_id, state);
                if (this.activeId === agent.agent_id) {
                    this.generation++;
                    changed = true;
                }
            }
            else
                old.agent = agent;
        }
        for (const id of this.states.keys())
            if (!ids.has(id))
                this.states.delete(id);
        if (this.activeId && !this.states.has(this.activeId))
            this.deactivate();
        if (changed) {
            this.stopTimer();
            this.render();
            if (this.expanded())
                void this.refresh();
        }
    }
    async select(id) { this.stopTimer(); this.generation++; this.activeId = id; const state = this.current(); if (state) {
        state.loading = false;
        state.querying = false;
        state.loadVersion++;
        state.queryVersion++;
    } this.render(); await Promise.all([this.refresh(), this.restores.select(id), this.missingRestores.select(id)]); }
    deactivate() { this.stopTimer(); this.generation++; this.activeId = null; this.restores.deactivate(); this.missingRestores.deactivate(); }
    refreshLocale() { this.render(); this.restores.refreshLocale(); this.missingRestores.refreshLocale(); }
    toggled() { this.restores.toggled(); this.missingRestores.toggled(); if (this.expanded()) {
        const state = this.current();
        if (state?.stale)
            void this.refresh();
        else
            this.schedule();
    }
    else
        this.stopTimer(); }
    current() { return this.activeId ? this.states.get(this.activeId) : undefined; }
    valid(state, generation) { return this.current() === state && this.generation === generation; }
    expanded() { return node("checkpoint-panel").open && !node("checkpoint-directory").hidden; }
    base(state) { return `/v1/agents/${encodeURIComponent(state.agent.agent_id)}/workspace/directory-checkpoints`; }
    storageKey(state) { return `krim-directory-checkpoint-recovery:${state.agent.session_id}`; }
    save(state) {
        if (state.storageError)
            throw new Error(t("目录捕获恢复记录不可用，新建已停用"));
        const grant = manifest(state);
        try {
            const value = JSON.stringify({ version: 1, scope_id: `session:${state.agent.session_id}`, workspace_key: grant.key, resource_fingerprint: grant.fingerprint, pending: state.pending, selected_id: state.selectedId });
            if (new TextEncoder().encode(value).length > 24576)
                throw new Error();
            localStorage.setItem(this.storageKey(state), value);
            if (localStorage.getItem(this.storageKey(state)) !== value)
                throw new Error();
        }
        catch {
            state.storageError = true;
            throw new Error(t("目录捕获恢复记录不可用，新建已停用"));
        }
    }
    restore(state) {
        try {
            const raw = localStorage.getItem(this.storageKey(state));
            if (raw === null)
                return;
            if (new TextEncoder().encode(raw).length > 24576)
                throw new Error();
            const value = JSON.parse(raw), grant = manifest(state);
            if (!keys(value, ["version", "scope_id", "workspace_key", "resource_fingerprint", "pending", "selected_id"]) || value.version !== 1 || value.scope_id !== `session:${state.agent.session_id}` || value.workspace_key !== grant.key || value.resource_fingerprint !== grant.fingerprint
                || value.selected_id !== null && (typeof value.selected_id !== "string" || !ID.test(value.selected_id)))
                throw new Error();
            if (value.pending !== null) {
                if (!keys(value.pending, ["request", "rejected"]) || typeof value.pending.rejected !== "boolean")
                    throw new Error();
                verifyRequest(value.pending.request);
                state.pending = { request: value.pending.request, rejected: value.pending.rejected };
                state.title = state.pending.request.title;
                state.path = state.pending.request.root_path;
                state.query = state.pending.request.request_key;
            }
            state.selectedId = value.selected_id;
        }
        catch {
            state.storageError = true;
            state.message = t("目录捕获恢复记录不可用，新建已停用");
        }
    }
    accept(state, value, select) {
        verify(state, value);
        const pending = state.pending;
        if (pending && value.request_key === pending.request.request_key && (value.title !== pending.request.title || value.root_path !== pending.request.root_path))
            throw new Error(t("目录检查点回程与原事实不一致"));
        state.known.set(value.checkpoint_id, value);
        if (select) {
            state.selected = value;
            state.selectedId = value.checkpoint_id;
        }
        if (pending && value.request_key === pending.request.request_key) {
            state.pending = null;
            try {
                this.save(state);
            }
            catch (error) {
                state.pending = pending;
                throw error;
            }
        }
        else if (select && !state.storageError)
            this.save(state);
    }
    async pending(state, generation, queryVersion) {
        const pending = state.pending;
        if (!pending)
            return;
        const value = await requestJsonNullable(`${this.base(state)}/by-key?${new URLSearchParams({ request_key: pending.request.request_key })}`);
        if (!this.valid(state, generation) || state.pending !== pending)
            return;
        if (value) {
            if (value.request_key !== pending.request.request_key)
                throw new Error(t("目录检查点回程与原事实不一致"));
            this.accept(state, value, queryVersion === state.queryVersion);
        }
        else if (pending.rejected) {
            state.pending = null;
            try {
                this.save(state);
            }
            catch (error) {
                state.pending = pending;
                throw error;
            }
            state.message = t("原目录捕获请求未被接受");
        }
        else
            state.message = t("原目录捕获请求尚未确认，请按原键继续查询");
    }
    async refresh(reset = false) {
        const state = this.current();
        if (!state || state.loading)
            return;
        if (reset)
            state.offset = 0;
        const generation = this.generation, version = ++state.loadVersion, queryVersion = state.queryVersion, selectedId = state.selectedId;
        state.loading = true;
        this.controls();
        try {
            const [entries, grant, selected] = await Promise.all([
                requestJson(`${this.base(state)}?offset=${state.offset}&limit=${PAGE + 1}`),
                requestJson(`/v1/agents/${encodeURIComponent(state.agent.agent_id)}/workspace`),
                selectedId ? requestJson(`${this.base(state)}/${encodeURIComponent(selectedId)}`) : Promise.resolve(null),
            ]);
            if (!this.valid(state, generation) || version !== state.loadVersion)
                return;
            const frozen = manifest(state);
            if (!grant || grant.key !== frozen.key || grant.fingerprint !== frozen.fingerprint || typeof grant.available !== "boolean" || typeof grant.writable !== "boolean" || !Array.isArray(grant.tool_names) || !grant.tool_names.every(name => typeof name === "string")
                || !Array.isArray(entries) || entries.length > PAGE + 1)
                throw new Error(t("目录检查点回程与原事实不一致"));
            for (const item of entries)
                verify(state, item);
            if (new Set(entries.map(item => item.checkpoint_id)).size !== entries.length)
                throw new Error(t("目录检查点回程与原事实不一致"));
            if (selected) {
                verify(state, selected);
                if (selected.checkpoint_id !== selectedId)
                    throw new Error(t("目录检查点回程与原事实不一致"));
            }
            for (const item of entries)
                state.known.set(item.checkpoint_id, item);
            state.grant = grant;
            state.entries = entries.slice(0, PAGE);
            state.more = entries.length > PAGE;
            state.stale = false;
            if (!state.storageError)
                state.message = "";
            if (queryVersion === state.queryVersion && selected)
                this.accept(state, selected, true);
            await this.pending(state, generation, queryVersion);
        }
        catch (error) {
            if (this.valid(state, generation) && version === state.loadVersion) {
                state.stale = true;
                state.message = errorText(error);
                this.notice(state.message, true);
            }
        }
        finally {
            if (version === state.loadVersion)
                state.loading = false;
            if (this.valid(state, generation)) {
                this.render();
                this.schedule();
            }
        }
    }
    async queryPending() {
        const state = this.current();
        if (!state?.pending || state.querying)
            return;
        const generation = this.generation, version = ++state.queryVersion;
        state.querying = true;
        try {
            await this.pending(state, generation, version);
        }
        catch (error) {
            if (this.valid(state, generation)) {
                state.message = errorText(error);
                this.notice(state.message, true);
            }
        }
        finally {
            if (version === state.queryVersion)
                state.querying = false;
            if (this.valid(state, generation)) {
                this.render();
                this.schedule();
            }
        }
    }
    async query(byKey) { const state = this.current(); if (state?.query)
        await this.read(state.query, byKey); }
    async read(value, byKey, wait = false) {
        const state = this.current();
        if (!state || state.querying)
            return;
        if (byKey ? !key(value) : !ID.test(value)) {
            this.notice(t("检查点或计划标识无效"), true);
            return;
        }
        const generation = this.generation, version = ++state.queryVersion;
        state.querying = true;
        try {
            const suffix = byKey ? `/by-key?${new URLSearchParams({ request_key: value })}` : `/${encodeURIComponent(value)}${wait ? "/wait?timeout_seconds=1" : ""}`;
            const result = await requestJsonNullable(this.base(state) + suffix, wait ? { method: "POST" } : undefined);
            if (!this.valid(state, generation) || version !== state.queryVersion)
                return;
            if (result) {
                verify(state, result);
                if (byKey ? result.request_key !== value : result.checkpoint_id !== value)
                    throw new Error(t("目录检查点回程与原事实不一致"));
                this.accept(state, result, true);
                state.message = "";
            }
            else
                state.message = t("未找到目录检查点");
        }
        catch (error) {
            if (this.valid(state, generation) && version === state.queryVersion) {
                state.message = errorText(error);
                this.notice(state.message, true);
            }
        }
        finally {
            if (version === state.queryVersion)
                state.querying = false;
            if (this.valid(state, generation)) {
                this.render();
                this.schedule();
            }
        }
    }
    canCreate(state) {
        return !state.stale && !state.storageError && !state.loading && !state.querying && !state.sending && !state.cancelling && !state.pending
            && state.agent.session.status === "open" && state.agent.session.permissions.mode !== "ask" && state.grant?.available === true && state.grant.writable
            && state.grant.tool_names.includes("workspace_directory_checkpoint_capture") && !state.entries.some(item => item.status === "capturing") && state.selected?.status !== "capturing";
    }
    async create() {
        const state = this.current();
        if (!state || !this.canCreate(state))
            return;
        const request = { title: state.title.trim() || t("目录检查点"), root_path: state.path, request_key: crypto.randomUUID() };
        try {
            verifyRequest(request);
        }
        catch (error) {
            this.notice(errorText(error), true);
            return;
        }
        const generation = this.generation, version = ++state.queryVersion;
        state.pending = { request, rejected: false };
        state.sending = true;
        state.query = request.request_key;
        state.message = "";
        try {
            this.save(state);
        }
        catch (error) {
            state.sending = false;
            state.message = errorText(error);
            this.notice(state.message, true);
            this.render();
            return;
        }
        this.render();
        const controller = new AbortController(), timer = window.setTimeout(() => controller.abort(), 15_000);
        let postReturned = false;
        try {
            const value = await requestJson(`${this.base(state)}?confirmed=true`, { method: "POST", body: JSON.stringify(request), signal: controller.signal });
            postReturned = true;
            if (!this.valid(state, generation))
                return;
            verify(state, value);
            if (value.request_key !== request.request_key || value.root_path !== request.root_path || value.title !== request.title)
                throw new Error(t("目录检查点回程与原事实不一致"));
            // 创建响应只是候选；原键读回核实前不能显示或释放持久意图。
            const truth = await requestJsonNullable(`${this.base(state)}/by-key?${new URLSearchParams({ request_key: request.request_key })}`, { signal: controller.signal });
            if (!this.valid(state, generation))
                return;
            verify(state, truth);
            verifyTransition(value, truth);
            this.accept(state, truth, version === state.queryVersion);
            state.offset = 0;
        }
        catch (error) {
            if (!this.valid(state, generation))
                return;
            // 原键确认GET失败不能把已返回的POST改写成明确拒绝。
            if (!postReturned && state.pending && error instanceof ApiError && error.status >= 400 && error.status < 500) {
                state.pending.rejected = true;
                try {
                    this.save(state);
                }
                catch { /* 保持原意图及只读恢复。 */ }
            }
            state.message = t("原目录捕获请求尚未确认，请按原键继续查询");
            this.notice(state.message, true);
        }
        finally {
            window.clearTimeout(timer);
            state.sending = false;
            if (this.valid(state, generation)) {
                this.render();
                await this.refresh();
            }
        }
    }
    async cancel() {
        const state = this.current(), selected = state?.selected;
        if (!state || !selected || selected.status !== "capturing" || state.cancelling)
            return;
        const generation = this.generation, version = ++state.queryVersion;
        state.cancelling = true;
        this.controls();
        try {
            const value = await requestJson(`${this.base(state)}/${encodeURIComponent(selected.checkpoint_id)}/cancel`, { method: "POST" });
            if (!this.valid(state, generation) || version !== state.queryVersion)
                return;
            verify(state, value);
            if (value.checkpoint_id !== selected.checkpoint_id || value.request_key !== selected.request_key || value.root_path !== selected.root_path)
                throw new Error(t("目录检查点回程与原事实不一致"));
            this.accept(state, value, true);
            state.message = "";
        }
        catch (error) {
            if (this.valid(state, generation)) {
                state.message = errorText(error);
                this.notice(state.message, true);
            }
        }
        finally {
            state.cancelling = false;
            if (this.valid(state, generation)) {
                this.render();
                await this.refresh();
            }
        }
    }
    stopTimer() { if (this.timer !== undefined)
        window.clearTimeout(this.timer); this.timer = undefined; }
    schedule() { this.stopTimer(); const state = this.current(); if (state && this.expanded() && !state.storageError && (state.pending || state.selected?.status === "capturing" || state.entries.some(item => item.status === "capturing")))
        this.timer = window.setTimeout(() => void this.refresh(), 1000); }
    controls() {
        const state = this.current();
        if (!state)
            return;
        button("directory-checkpoint-create").disabled = !this.canCreate(state) || !relative(state.path);
        for (const id of ["directory-checkpoint-title", "directory-checkpoint-path"])
            input(id).disabled = state.sending || !!state.pending;
        button("directory-checkpoint-refresh").disabled = state.loading;
        for (const id of ["directory-checkpoint-by-id", "directory-checkpoint-by-key"])
            button(id).disabled = state.querying || !state.query;
        button("directory-checkpoint-pending").disabled = state.querying || !state.pending;
        button("directory-checkpoint-wait").disabled = state.querying || state.selected?.status !== "capturing";
        button("directory-checkpoint-cancel").disabled = state.cancelling || state.selected?.status !== "capturing";
        button("directory-checkpoint-prev").disabled = state.loading || state.offset === 0;
        button("directory-checkpoint-next").disabled = state.loading || !state.more;
    }
    render() {
        const state = this.current();
        if (!state)
            return;
        for (const [id, value] of [["directory-checkpoint-title", state.title], ["directory-checkpoint-path", state.path], ["directory-checkpoint-query", state.query]])
            if (document.activeElement !== input(id))
                input(id).value = value;
        node("directory-checkpoint-status").textContent = state.message || (state.stale ? t("正在读取目录检查点历史") : !state.grant?.available || state.agent.session.status !== "open" || !state.grant.tool_names.includes("workspace_directory_checkpoint_capture") ? t("当前会话只能查询目录检查点历史") : "");
        if (state.pending)
            node("directory-checkpoint-status").textContent += `\n${t("未确认请求键")}: ${state.pending.request.request_key}`;
        node("directory-checkpoint-page").textContent = t("检查点历史第{page}页", { page: Math.floor(state.offset / PAGE) + 1 });
        const list = node("directory-checkpoint-items");
        list.replaceChildren();
        for (const value of state.entries) {
            const card = document.createElement("article");
            card.className = "checkpoint-card";
            card.dataset.directoryCheckpointId = value.checkpoint_id;
            const title = document.createElement("strong");
            title.textContent = value.title;
            const summary = document.createElement("p");
            summary.textContent = `${value.root_path} · ${t(labels[value.status])}`;
            const open = document.createElement("button");
            open.type = "button";
            open.textContent = t("查看目录检查点");
            open.disabled = state.querying;
            open.onclick = () => { if (this.current() === state)
                void this.read(value.checkpoint_id, false); };
            card.append(title, summary, open);
            list.append(card);
        }
        const detail = node("directory-checkpoint-detail");
        detail.replaceChildren();
        delete detail.dataset.checkpointId;
        delete detail.dataset.checkpointStatus;
        const value = state.selected;
        if (value) {
            detail.dataset.checkpointId = value.checkpoint_id;
            detail.dataset.checkpointStatus = value.status;
            const title = document.createElement("h3");
            title.textContent = `${value.title} · ${t(labels[value.status])}`;
            detail.append(title);
            line(detail, "检查点标识", value.checkpoint_id);
            line(detail, "原请求键", value.request_key);
            line(detail, "目录路径", value.root_path);
            line(detail, "会话作用域", value.scope_id);
            line(detail, "工作区标识", value.workspace_key);
            line(detail, "工作区资源指纹", value.resource_fingerprint);
            line(detail, "创建时间", localDate(value.created_at));
            line(detail, "更新时间", localDate(value.updated_at));
            if (value.error_code)
                line(detail, "错误代码", value.error_code);
            if (value.snapshot) {
                line(detail, "完整目录摘要", value.snapshot.snapshot_fingerprint);
                line(detail, "目录节点数", String(value.snapshot.entry_count));
                line(detail, "目录数量", String(value.snapshot.directory_count));
                line(detail, "文件数量", String(value.snapshot.file_count));
                line(detail, "文件总字节", String(value.snapshot.byte_size));
                if (value.status !== "available")
                    line(detail, "捕获材料状态", t("已保存材料不表示捕获成功"));
            }
            if (value.status === "available" && value.snapshot) {
                const restore = document.createElement("button");
                restore.type = "button";
                restore.textContent = t("恢复完整目录");
                restore.onclick = () => { if (this.current() === state)
                    void this.restores.useCheckpoint(state.agent.agent_id, value).catch(error => this.notice(t(errorText(error)), true)); };
                detail.append(restore);
                const missing = document.createElement("button");
                missing.type = "button";
                missing.textContent = t("恢复缺失目录");
                missing.onclick = () => { if (this.current() === state)
                    void this.missingRestores.useCheckpoint(state.agent.agent_id, value).catch(error => this.notice(t(errorText(error)), true)); };
                detail.append(missing);
            }
        }
        this.controls();
    }
}
