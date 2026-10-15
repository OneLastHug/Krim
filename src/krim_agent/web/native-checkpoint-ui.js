/** 原生检查点消费：只提交公开请求，保留原键与服务端逐文件事实。 */
import { ApiError, errorText, requestJson, requestJsonNullable, } from "./api.js";
import { localDate, t } from "./i18n.js";
const node = (id) => document.getElementById(id);
const button = (id) => node(id);
const PAGE_SIZE = 8;
const ID = /^[A-Za-z0-9][A-Za-z0-9_-]{0,35}$/, SHA = /^[0-9a-f]{64}$/;
function relative(value) {
    return typeof value === "string" && value.length > 0 && new TextEncoder().encode(value).length <= 512
        && !/[\\:\x00-\x1f\x7f]/.test(value) && value.split("/").every(part => part !== "" && part !== "." && part !== "..");
}
function requestKey(value) { return typeof value === "string" && value.length > 0 && value.length <= 128 && !/[\x00-\x1f\x7f]/.test(value); }
function keys(value, allowed) { return typeof value === "object" && value !== null && !Array.isArray(value) && Object.keys(value).every(name => allowed.includes(name)); }
const labels = {
    capturing: "文件捕获中", available: "检查点可用", failed: "检查点或恢复失败", cancelled: "检查点或恢复已取消",
    interrupted: "检查点或恢复已中断", preparing: "正在准备恢复", ready: "恢复待审阅", executing: "正在恢复文件",
    running: "正在恢复文件", done: "文件恢复已确认", partial: "文件恢复部分完成", uncertain: "文件结果待核实",
    expired: "恢复计划已过期", pending: "尚未执行", skipped: "尚未执行", rejected: "文件操作已拒绝", finished: "恢复编排已结束",
};
const statusText = (status) => t(labels[status] || status);
const activePlan = (plan) => !!plan && ["preparing", "executing"].includes(plan.phase);
const modeText = (mode) => mode === null ? t("文件不存在") : `0${mode.toString(8)}`;
function line(parent, label, value) {
    const item = document.createElement("p");
    item.textContent = `${t(label)}: ${value}`;
    parent.append(item);
}
export class NativeCheckpointUI {
    notice;
    openProposal;
    states = new Map();
    activeId = null;
    generation = 0;
    timer;
    signatures = new Map();
    constructor(notice, openProposal) {
        this.notice = notice;
        this.openProposal = openProposal;
        for (const [id, field] of [["native-checkpoint-title", "title"], ["native-checkpoint-paths", "paths"], ["native-checkpoint-query", "query"]]) {
            node(id).oninput = () => { const state = this.current(); if (state)
                state[field] = node(id).value; this.controls(); };
        }
        node("native-checkpoint-query-kind").onchange = () => {
            const state = this.current();
            if (state)
                state.queryKind = node("native-checkpoint-query-kind").value;
        };
        node("native-checkpoint-format").onchange = () => { const state = this.current(); if (state)
            state.captureFormat = node("native-checkpoint-format").value === "bytes" ? "bytes" : "text"; this.controls(); };
        node("native-checkpoint-create").onclick = () => void this.capture();
        node("native-checkpoint-preview").onclick = () => void this.preview();
        node("native-restore-execute").onclick = () => void this.execute();
        node("native-checkpoint-refresh").onclick = () => void this.refresh(true);
        node("native-checkpoint-query-id").onclick = () => void this.query(false);
        node("native-checkpoint-query-key").onclick = () => void this.query(true);
        node("native-checkpoint-query-pending").onclick = () => void this.queryPending();
        for (const kind of ["checkpoint", "restore"]) {
            node(`native-${kind}-prev`).onclick = () => void this.page(kind, -1);
            node(`native-${kind}-next`).onclick = () => void this.page(kind, 1);
            node(`native-${kind}-wait`).onclick = () => void this.wait(kind);
            node(`native-${kind}-cancel`).onclick = () => void this.cancel(kind);
        }
    }
    setSources(agents) {
        const ids = new Set(agents.map(agent => agent.agent_id));
        let changed = false;
        for (const agent of agents) {
            const old = this.states.get(agent.agent_id);
            if (!old) {
                const state = { agent, title: "", paths: "", query: "", queryKind: "checkpoint", captures: [], plans: [], capture: null, plan: null, captureFormat: "text", recoveryError: false,
                    captureOffset: 0, planOffset: 0, moreCaptures: false, morePlans: false, pending: null, message: "", stale: true, loading: false, sending: false, stopping: false, querying: false, loadVersion: 0, queryVersion: 0, querySerial: 0 };
                this.readRecovery(state);
                this.states.set(agent.agent_id, state);
            }
            else if (old.agent.session_id !== agent.session_id || old.agent.session.status !== agent.session.status
                || JSON.stringify(old.agent.session.permissions) !== JSON.stringify(agent.session.permissions)) {
                const sameSession = old.agent.session_id === agent.session_id;
                this.states.set(agent.agent_id, { ...old, agent, grant: undefined, captures: [], plans: [],
                    capture: sameSession ? old.capture : null, plan: sameSession ? old.plan : null, pending: sameSession ? old.pending : null,
                    stale: true, loading: false, sending: false, stopping: false, querying: false, captureOffset: 0, planOffset: 0, loadVersion: 0, queryVersion: 0, querySerial: 0 });
                if (this.activeId === agent.agent_id) {
                    this.generation += 1;
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
            this.signatures.clear();
            this.stopTimer();
            this.render();
            if (this.expanded())
                void this.refresh();
        }
    }
    async select(id) {
        this.stopTimer();
        this.generation += 1;
        this.activeId = id;
        const state = this.current();
        if (state) {
            state.loadVersion += 1;
            state.queryVersion += 1;
            state.querySerial += 1;
            state.loading = false;
            state.querying = false;
        }
        this.signatures.clear();
        this.render();
        await this.refresh();
    }
    deactivate() { this.stopTimer(); this.generation += 1; this.activeId = null; }
    refreshLocale() { this.signatures.clear(); this.render(); }
    toggled() { if (this.expanded()) {
        const state = this.current();
        if (state?.stale)
            void this.refresh();
        else
            this.schedule();
    }
    else
        this.stopTimer(); }
    expanded() { return node("checkpoint-panel").open; }
    current() { return this.activeId ? this.states.get(this.activeId) : undefined; }
    valid(state, generation) { return this.current() === state && this.generation === generation; }
    base(state) { return `/v1/agents/${encodeURIComponent(state.agent.agent_id)}/workspace`; }
    resource(state, kind) { return `${this.base(state)}/${kind === "checkpoint" ? "native-checkpoints" : "native-restores"}`; }
    readable(state) {
        return !state.stale && state.agent.session.status === "open" && state.agent.session.permissions.mode !== "ask"
            && state.grant?.available === true && state.grant.writable && state.grant.tool_names.some(name => name === "workspace_read" || name === "workspace_checkpoint_capture");
    }
    writable(state) {
        return this.readable(state) && state.agent.session.permissions.mode !== "plan" && state.grant.tool_names.includes("workspace_change");
    }
    producing(state) {
        return state.capture?.status === "capturing" || activePlan(state.plan)
            || state.captures.some(item => item.status === "capturing") || state.plans.some(item => activePlan(item));
    }
    canStart(state) { return !state.recoveryError && !state.loading && !state.sending && !state.stopping && !state.querying && !state.pending && !this.producing(state); }
    acceptCapture(state, value, select = true) {
        this.verifyCapture(state, value);
        if (state.pending?.kind === "capture" && state.pending.request.request_key === value.request_key) {
            const request = state.pending.request;
            if (value.title !== request.title || (value.capture_format || "text") !== (request.capture_format || "text") || JSON.stringify(value.files.map(file => file.path)) !== JSON.stringify(request.paths))
                throw new Error(t("检查点回程与原事实不一致"));
            state.pending = null;
            this.persistRecovery(state);
        }
        if (select)
            state.capture = value;
    }
    acceptPlan(state, value, select = true) {
        if (select)
            state.plan = value;
        if (state.pending?.kind === "preview" && state.pending.request.request_key === value.request_key
            || state.pending?.kind === "execute" && state.pending.request.plan_id === value.plan_id
                && (value.execute_key !== null || !["preparing", "ready"].includes(value.phase)))
            state.pending = null;
    }
    verifyCapture(state, value) {
        const format = value?.capture_format || "text";
        if (!value || value.source !== "workspace" || value.scope_id !== `session:${state.agent.agent_id}` || !ID.test(value.checkpoint_id) || !requestKey(value.request_key)
            || !["text", "bytes"].includes(format) || !["capturing", "available", "failed", "cancelled", "interrupted"].includes(value.status)
            || !Array.isArray(value.files) || value.files.length < 1 || value.files.length > 32 || new Set(value.files.map(file => file.path)).size !== value.files.length
            || state.grant && (value.workspace_key !== state.grant.key || value.resource_fingerprint !== state.grant.fingerprint))
            throw new Error(t("检查点回程与原事实不一致"));
        for (const file of value.files) {
            if (!relative(file.path) || typeof file.captured !== "boolean"
                || !file.captured && [file.exists, file.sha256, file.byte_size, file.mode, file.content_kind ?? null].some(field => field !== null)
                || file.captured && (file.exists === true ? !SHA.test(file.sha256 || "") || !Number.isSafeInteger(file.byte_size) || file.byte_size < 0 || file.byte_size > 8388608 || !Number.isSafeInteger(file.mode) || file.mode < 0 || file.mode > 4095 : file.exists !== false || file.sha256 !== null || file.byte_size !== 0 || file.mode !== null)
                || file.captured && (format === "bytes" ? !["text", "binary", "absent"].includes(file.content_kind || "") || (file.content_kind === "absent") !== (file.exists === false) : file.content_kind !== undefined)
                || value.status === "available" && !file.captured)
                throw new Error(t("检查点回程与原事实不一致"));
        }
    }
    storageKey(state) { return `krim-native-capture-recovery:${state.agent.session_id}`; }
    persistRecovery(state) {
        if (state.recoveryError)
            throw new Error(t("检查点恢复记录不可用，写操作已停用"));
        try {
            if (state.pending?.kind === "capture")
                localStorage.setItem(this.storageKey(state), JSON.stringify({ version: 1, pending: state.pending }));
            else
                localStorage.removeItem(this.storageKey(state));
        }
        catch {
            state.recoveryError = true;
            throw new Error(t("检查点恢复记录不可用，写操作已停用"));
        }
    }
    readRecovery(state) {
        try {
            const raw = localStorage.getItem(this.storageKey(state));
            if (!raw)
                return;
            if (raw.length > 24576)
                throw new Error();
            const value = JSON.parse(raw);
            if (!keys(value, ["version", "pending"]) || value.version !== 1 || !keys(value.pending, ["kind", "request"]) || value.pending.kind !== "capture")
                throw new Error();
            const request = value.pending.request;
            if (!keys(request, ["title", "paths", "request_key", "capture_format"]) || typeof request.title !== "string" || !request.title.trim() || request.title.length > 120 || !requestKey(request.request_key)
                || request.capture_format !== undefined && !["text", "bytes"].includes(String(request.capture_format)) || !Array.isArray(request.paths) || request.paths.length < 1 || request.paths.length > 32 || !request.paths.every(relative) || new Set(request.paths).size !== request.paths.length)
                throw new Error();
            state.pending = value.pending;
            state.query = request.request_key;
            state.title = request.title;
            state.paths = request.paths.join("\n");
            state.captureFormat = request.capture_format === "bytes" ? "bytes" : "text";
        }
        catch {
            state.recoveryError = true;
            state.message = t("检查点恢复记录不可用，写操作已停用");
        }
    }
    async refresh(reset = false) {
        const state = this.current();
        if (!state || state.loading)
            return;
        if (reset) {
            state.captureOffset = 0;
            state.planOffset = 0;
        }
        const generation = this.generation, version = ++state.loadVersion, queryVersion = state.queryVersion;
        state.loading = true;
        this.controls();
        try {
            const [captures, plans, grant] = await Promise.all([
                requestJson(`${this.resource(state, "checkpoint")}?offset=${state.captureOffset}&limit=${PAGE_SIZE + 1}`),
                requestJson(`${this.resource(state, "restore")}?offset=${state.planOffset}&limit=${PAGE_SIZE + 1}`),
                state.agent.session.status === "open" ? requestJson(this.base(state)).catch(() => undefined) : Promise.resolve(undefined),
            ]);
            if (!this.valid(state, generation) || state.loadVersion !== version)
                return;
            state.grant = grant;
            state.captures = captures.slice(0, PAGE_SIZE);
            state.plans = plans.slice(0, PAGE_SIZE);
            for (const capture of state.captures)
                this.verifyCapture(state, capture);
            state.moreCaptures = captures.length > PAGE_SIZE;
            state.morePlans = plans.length > PAGE_SIZE;
            state.grant = grant;
            const captureId = state.capture?.checkpoint_id || captures[0]?.checkpoint_id, planId = state.plan?.plan_id || plans[0]?.plan_id;
            const [capture, plan] = await Promise.all([
                captureId ? requestJson(`${this.resource(state, "checkpoint")}/${encodeURIComponent(captureId)}`) : Promise.resolve(null),
                planId ? requestJson(`${this.resource(state, "restore")}/${encodeURIComponent(planId)}`) : Promise.resolve(null),
            ]);
            if (!this.valid(state, generation) || state.loadVersion !== version)
                return;
            if (capture)
                this.acceptCapture(state, capture, queryVersion === state.queryVersion);
            if (plan)
                this.acceptPlan(state, plan, queryVersion === state.queryVersion);
            state.stale = false;
            if (state.pending)
                await this.resolvePending(state, generation, queryVersion);
        }
        catch (error) {
            if (this.valid(state, generation)) {
                state.stale = true;
                state.message = errorText(error);
                this.notice(state.message, true);
            }
        }
        finally {
            if (state.loadVersion === version)
                state.loading = false;
            if (this.valid(state, generation)) {
                this.render();
                this.schedule();
            }
        }
    }
    async page(kind, direction) {
        const state = this.current();
        if (!state || state.loading || state.sending)
            return;
        if (kind === "checkpoint")
            state.captureOffset = Math.max(0, state.captureOffset + direction * PAGE_SIZE);
        else
            state.planOffset = Math.max(0, state.planOffset + direction * PAGE_SIZE);
        await this.refresh();
    }
    async resolvePending(state, generation, queryVersion) {
        const pending = state.pending;
        if (!pending)
            return;
        if (pending.kind === "capture") {
            const value = await requestJsonNullable(`${this.resource(state, "checkpoint")}/by-key?${new URLSearchParams({ request_key: pending.request.request_key })}`);
            if (this.valid(state, generation) && state.pending === pending && value)
                this.acceptCapture(state, value, queryVersion === state.queryVersion);
        }
        else {
            const path = pending.kind === "preview" ? `/by-key?${new URLSearchParams({ request_key: pending.request.request_key })}` : `/${encodeURIComponent(pending.request.plan_id)}`;
            const value = await requestJsonNullable(`${this.resource(state, "restore")}${path}`);
            if (this.valid(state, generation) && state.pending === pending && value)
                this.acceptPlan(state, value, queryVersion === state.queryVersion);
        }
        if (this.valid(state, generation)) {
            if (!state.pending && queryVersion === state.queryVersion)
                state.queryVersion += 1;
            state.message = state.pending ? t("请求结果尚未确认，请继续查询原请求") : "";
        }
    }
    async queryPending() {
        const state = this.current();
        if (!state?.pending || state.querying)
            return;
        const generation = this.generation, queryVersion = ++state.queryVersion, serial = ++state.querySerial;
        state.querying = true;
        this.controls();
        try {
            await this.resolvePending(state, generation, queryVersion);
        }
        catch (error) {
            if (this.valid(state, generation)) {
                state.message = errorText(error);
                this.notice(state.message, true);
            }
        }
        finally {
            if (state.querySerial === serial)
                state.querying = false;
            if (this.valid(state, generation)) {
                this.render();
                this.schedule();
            }
        }
    }
    async query(byKey) {
        const state = this.current();
        if (!state || !state.query || state.querying)
            return;
        await this.read(state.queryKind, state.query, byKey);
    }
    async read(kind, value, byKey = false, wait = false) {
        const state = this.current();
        if (!state || state.querying)
            return;
        if (!byKey && !/^[A-Za-z0-9][A-Za-z0-9_-]{0,35}$/.test(value)) {
            this.notice(t("检查点或计划标识无效"), true);
            return;
        }
        const generation = this.generation, queryVersion = ++state.queryVersion, serial = ++state.querySerial;
        state.querying = true;
        state.message = "";
        if (!wait) {
            if (kind === "checkpoint")
                state.capture = null;
            else
                state.plan = null;
        }
        this.render();
        const suffix = byKey ? `/by-key?${new URLSearchParams({ request_key: value })}` : `/${encodeURIComponent(value)}${wait ? "/wait?timeout_seconds=1" : ""}`;
        try {
            const result = await requestJsonNullable(`${this.resource(state, kind)}${suffix}`, wait ? { method: "POST" } : undefined);
            if (!this.valid(state, generation) || state.queryVersion !== queryVersion)
                return;
            if (!result)
                state.message = t("未找到检查点或恢复计划");
            else if (kind === "checkpoint")
                this.acceptCapture(state, result);
            else
                this.acceptPlan(state, result);
            state.queryVersion += 1;
        }
        catch (error) {
            if (this.valid(state, generation) && state.queryVersion === queryVersion) {
                state.message = errorText(error);
                this.notice(state.message, true);
            }
        }
        finally {
            if (state.querySerial === serial)
                state.querying = false;
            if (this.valid(state, generation)) {
                this.render();
                this.schedule();
            }
        }
    }
    async wait(kind) {
        const state = this.current();
        if (!state)
            return;
        const id = kind === "checkpoint" ? state.capture?.checkpoint_id : state.plan?.plan_id;
        if (id)
            await this.read(kind, id, false, true);
    }
    async capture() {
        const state = this.current();
        if (!state || !this.readable(state) || !this.canStart(state))
            return;
        if (!state.grant.tool_names.includes(state.captureFormat === "bytes" ? "workspace_checkpoint_capture" : "workspace_read"))
            return;
        const paths = state.paths.split(/\r?\n/).filter(value => value.length > 0);
        if (!paths.length || paths.length > 32 || new Set(paths).size !== paths.length) {
            this.notice(t("请填写1至32个不重复的文件路径"), true);
            return;
        }
        await this.send({ kind: "capture", request: { title: state.title.trim() || t("文件检查点"), paths, request_key: crypto.randomUUID(), ...(state.captureFormat === "bytes" ? { capture_format: "bytes" } : {}) } });
    }
    async preview() {
        const state = this.current();
        if (!state || !this.readable(state) || !this.canStart(state) || state.capture?.status !== "available")
            return;
        await this.send({ kind: "preview", request: { checkpoint_id: state.capture.checkpoint_id, request_key: crypto.randomUUID() } });
    }
    async execute() {
        const state = this.current();
        if (!state || !this.writable(state) || !this.canStart(state) || state.plan?.phase !== "ready")
            return;
        const plan = state.plan;
        if (plan.expires_at <= Date.now() / 1000) {
            this.notice(t("恢复计划已过期"), true);
            await this.refresh();
            return;
        }
        const generation = this.generation;
        if (!window.confirm(`${t("确认恢复已审阅的{count}个文件（计划版本{revision}）？", { count: plan.items.length, revision: plan.revision })}\n${plan.items.map(item => item.path).join("\n")}`))
            return;
        if (!this.valid(state, generation) || state.plan !== plan)
            return;
        await this.send({ kind: "execute", request: { plan_id: plan.plan_id, expected_revision: plan.revision, request_key: crypto.randomUUID() } });
    }
    async send(pending) {
        const state = this.current();
        if (!state || state.pending || state.sending)
            return;
        const generation = this.generation;
        state.pending = pending;
        state.sending = true;
        state.queryVersion += 1;
        state.message = "";
        const queryVersion = state.queryVersion;
        // 恢复by-key查的是预览键；执行键仅用于同一计划的执行幂等，不能互换。
        state.query = pending.kind === "execute" ? state.plan.request_key : pending.request.request_key;
        state.queryKind = pending.kind === "capture" ? "checkpoint" : "restore";
        if (pending.kind === "capture") {
            try {
                this.persistRecovery(state);
            }
            catch (error) {
                state.sending = false;
                state.message = errorText(error);
                this.notice(state.message, true);
                this.render();
                return;
            }
        }
        this.render();
        const path = pending.kind === "capture" ? this.resource(state, "checkpoint") : pending.kind === "preview" ? `${this.resource(state, "restore")}/preview` : `${this.resource(state, "restore")}/${encodeURIComponent(pending.request.plan_id)}/execute`;
        const controller = new AbortController(), timer = window.setTimeout(() => controller.abort(), 15_000);
        try {
            const value = await requestJson(`${path}?confirmed=true`, { method: "POST", body: JSON.stringify(pending.request), signal: controller.signal });
            if (!this.valid(state, generation))
                return;
            const select = state.queryVersion === queryVersion;
            if (select)
                state.queryVersion += 1;
            if (pending.kind === "capture")
                this.acceptCapture(state, value, select);
            else
                this.acceptPlan(state, value, select);
            state.captureOffset = 0;
            state.planOffset = 0;
        }
        catch (error) {
            if (!this.valid(state, generation))
                return;
            // 明确拒绝请求没有接受生产者；网络/5xx继续按原键查找，绝不自动POST。
            if (error instanceof ApiError && error.status >= 400 && error.status < 500) {
                state.pending = null;
                if (pending.kind === "capture")
                    this.persistRecovery(state);
            }
            state.message = state.pending ? t("请求结果尚未确认，请继续查询原请求") : errorText(error);
            this.notice(state.message, true);
        }
        finally {
            window.clearTimeout(timer);
            state.sending = false;
            if (this.current() === state) {
                this.render();
                await this.refresh();
            }
        }
    }
    async cancel(kind) {
        const state = this.current();
        if (!state || state.stopping)
            return;
        const id = kind === "checkpoint" ? state.capture?.checkpoint_id : state.plan?.plan_id;
        if (!id || (kind === "checkpoint" ? state.capture?.status !== "capturing" : !state.plan || !["preparing", "ready", "executing"].includes(state.plan.phase)))
            return;
        const generation = this.generation;
        state.stopping = true;
        state.queryVersion += 1;
        this.controls();
        try {
            const value = await requestJson(`${this.resource(state, kind)}/${encodeURIComponent(id)}/cancel`, { method: "POST" });
            if (!this.valid(state, generation))
                return;
            state.queryVersion += 1;
            if (kind === "checkpoint")
                this.acceptCapture(state, value, state.capture?.checkpoint_id === id);
            else
                this.acceptPlan(state, value, state.plan?.plan_id === id);
        }
        catch (error) {
            if (this.valid(state, generation)) {
                state.message = errorText(error);
                this.notice(state.message, true);
            }
        }
        finally {
            state.stopping = false;
            if (this.current() === state) {
                this.render();
                await this.refresh();
            }
        }
    }
    stopTimer() { if (this.timer !== undefined)
        window.clearTimeout(this.timer); this.timer = undefined; }
    schedule() {
        this.stopTimer();
        const state = this.current();
        if (state && this.expanded() && (this.producing(state) || state.pending))
            this.timer = window.setTimeout(() => void this.refresh(), 1000);
    }
    controls() {
        const state = this.current();
        if (!state)
            return;
        const ready = this.canStart(state), readable = this.readable(state), plan = state.plan;
        button("native-checkpoint-create").disabled = !ready || !readable || !state.paths.replace(/\r?\n/g, "") || !state.grant?.tool_names.includes(state.captureFormat === "bytes" ? "workspace_checkpoint_capture" : "workspace_read");
        node("native-checkpoint-format").disabled = !ready || !readable;
        button("native-checkpoint-preview").disabled = !ready || !readable || !state.grant?.tool_names.includes("workspace_read") || state.capture?.status !== "available";
        button("native-restore-execute").disabled = !ready || !this.writable(state) || plan?.phase !== "ready" || plan.expires_at <= Date.now() / 1000;
        button("native-checkpoint-refresh").disabled = state.loading;
        for (const id of ["native-checkpoint-query-id", "native-checkpoint-query-key"])
            button(id).disabled = state.querying || !state.query;
        button("native-checkpoint-query-pending").disabled = state.querying || !state.pending;
        button("native-checkpoint-wait").disabled = state.querying || state.capture?.status !== "capturing";
        button("native-restore-wait").disabled = state.querying || !activePlan(plan);
        button("native-checkpoint-cancel").disabled = state.stopping || state.capture?.status !== "capturing";
        button("native-restore-cancel").disabled = state.stopping || !plan || !["preparing", "ready", "executing"].includes(plan.phase);
        button("native-checkpoint-prev").disabled = state.loading || state.sending || state.captureOffset === 0;
        button("native-checkpoint-next").disabled = state.loading || state.sending || !state.moreCaptures;
        button("native-restore-prev").disabled = state.loading || state.sending || state.planOffset === 0;
        button("native-restore-next").disabled = state.loading || state.sending || !state.morePlans;
        node("checkpoint-native").querySelectorAll("[data-native-query]").forEach(item => { item.disabled = state.querying; });
    }
    replace(id, signature, render) {
        const value = JSON.stringify(signature);
        if (this.signatures.get(id) === value)
            return;
        this.signatures.set(id, value);
        const root = node(id);
        root.replaceChildren();
        for (const key of Object.keys(root.dataset))
            delete root.dataset[key];
        render(root);
    }
    render() {
        const state = this.current();
        if (!state)
            return;
        for (const [id, value] of [["native-checkpoint-title", state.title], ["native-checkpoint-paths", state.paths], ["native-checkpoint-query", state.query], ["native-checkpoint-query-kind", state.queryKind], ["native-checkpoint-format", state.captureFormat]]) {
            const input = node(id);
            if (input.value !== value)
                input.value = value;
        }
        node("native-checkpoint-status").textContent = state.message || (state.stale ? t("正在读取检查点与恢复历史") : !this.readable(state) ? t("当前会话只能查询文件检查点历史") : "");
        if (state.pending && !state.recoveryError)
            node("native-checkpoint-status").textContent += `\n${t("请求结果尚未确认，请继续查询原请求")}\n${t("未确认请求键")}: ${state.pending.request.request_key}`;
        for (const [kind, offset, count] of [["checkpoint", state.captureOffset, state.captures.length], ["restore", state.planOffset, state.plans.length]]) {
            node(`native-${kind}-page`).textContent = t("第{page}页 · 本页{count}项", { page: offset / PAGE_SIZE + 1, count });
        }
        this.replace("native-checkpoint-items", [state.agent.agent_id, state.captures, state.capture?.checkpoint_id], root => {
            for (const item of state.captures) {
                const row = document.createElement("article");
                row.className = "checkpoint-card";
                row.dataset.checkpointId = item.checkpoint_id;
                const title = document.createElement("strong");
                title.textContent = item.title;
                const meta = document.createElement("p");
                meta.textContent = `${statusText(item.status)} · ${localDate(item.created_at)}`;
                const select = document.createElement("button");
                select.type = "button";
                select.textContent = t("查看文件检查点");
                select.dataset.nativeQuery = "checkpoint";
                select.onclick = () => { if (this.current() === state)
                    void this.read("checkpoint", item.checkpoint_id); };
                row.append(title, meta, select);
                root.append(row);
            }
        });
        this.replace("native-restore-items", [state.agent.agent_id, state.plans.map(item => [item.plan_id, item.status, item.phase, item.updated_at]), state.plan?.plan_id], root => {
            for (const item of state.plans) {
                const row = document.createElement("article");
                row.className = "checkpoint-card";
                row.dataset.planId = item.plan_id;
                const title = document.createElement("strong");
                title.textContent = `${statusText(item.status)} · ${localDate(item.created_at)}`;
                const id = document.createElement("small");
                id.textContent = item.plan_id;
                const select = document.createElement("button");
                select.type = "button";
                select.textContent = t("审阅恢复计划");
                select.dataset.nativeQuery = "restore";
                select.onclick = () => { if (this.current() === state)
                    void this.read("restore", item.plan_id); };
                row.append(title, id, select);
                root.append(row);
            }
        });
        this.replace("native-checkpoint-detail", [state.agent.agent_id, state.capture], root => {
            const value = state.capture;
            if (!value)
                return;
            root.dataset.checkpointId = value.checkpoint_id;
            root.dataset.checkpointStatus = value.status;
            const title = document.createElement("h3");
            title.textContent = `${value.title} · ${statusText(value.status)}`;
            root.append(title);
            line(root, "检查点标识", value.checkpoint_id);
            line(root, "原请求键", value.request_key);
            line(root, "检查点捕获格式", t(value.capture_format === "bytes" ? "完整文件字节（含二进制）" : "UTF-8文本（原默认）"));
            if (value.error_code)
                line(root, "错误代码", value.error_code);
            for (const file of value.files) {
                const row = document.createElement("p");
                row.textContent = `${file.path} · ${t(!file.captured ? "尚未捕获" : file.exists ? "文件存在" : "文件不存在")}${file.content_kind === "binary" ? ` · ${t("二进制字节")}` : ""} · ${file.byte_size ?? "—"} B · ${file.captured ? modeText(file.mode) : "—"}\n${file.sha256 || "—"}`;
                root.append(row);
            }
        });
        this.replace("native-restore-detail", [state.agent.agent_id, state.plan], root => {
            const value = state.plan;
            if (!value)
                return;
            root.dataset.planId = value.plan_id;
            root.dataset.restoreStatus = value.status;
            root.dataset.restorePhase = value.phase;
            const title = document.createElement("h3");
            title.textContent = `${t("恢复计划")} · ${statusText(value.status)}`;
            root.append(title);
            line(root, "恢复计划标识", value.plan_id);
            line(root, "目标检查点", value.checkpoint_id);
            line(root, "原请求键", value.request_key);
            line(root, "恢复执行请求键", value.execute_key || "—");
            line(root, "计划版本", String(value.revision));
            line(root, "有效期", localDate(new Date(value.expires_at * 1000).toISOString()));
            if (value.baseline_checkpoint_id) {
                line(root, "恢复前备份", value.baseline_checkpoint_id);
                const backup = document.createElement("button");
                backup.type = "button";
                backup.textContent = t("查看恢复前备份");
                backup.dataset.nativeQuery = "checkpoint";
                backup.onclick = () => { if (this.current() === state)
                    void this.read("checkpoint", value.baseline_checkpoint_id); };
                root.append(backup);
            }
            if (value.error_code)
                line(root, "错误代码", value.error_code);
            if (["cancelled", "expired", "interrupted"].includes(value.phase))
                line(root, "恢复编排状态", statusText(value.phase));
            for (const item of value.items) {
                const row = document.createElement("article");
                row.className = "checkpoint-restore-file";
                row.dataset.restorePath = item.path;
                const title = document.createElement("strong");
                title.textContent = `${item.path} · ${statusText(item.status)}`;
                row.append(title);
                line(row, "文件权限", `${item.proposal ? modeText(item.current_mode) : t("尚未捕获")} → ${modeText(item.target_mode)}`);
                if (item.proposal) {
                    line(row, "原文件SHA-256", item.proposal.preview.before_sha256 || t("文件不存在"));
                    line(row, "目标文件SHA-256", item.proposal.preview.after_sha256 || t("文件不存在"));
                    const diff = document.createElement("pre");
                    diff.className = "workspace-diff";
                    diff.textContent = item.proposal.preview.diff || t("文件内容无差异");
                    row.append(diff);
                    if (item.proposal.preview.truncated) {
                        const warning = document.createElement("p");
                        warning.textContent = t("差异已截断；恢复使用检查点的完整文件版本");
                        row.append(warning);
                    }
                }
                if (item.error_code)
                    line(row, "错误代码", item.error_code);
                if (item.operation) {
                    line(row, "文件操作标识", item.operation.operation_id);
                    const open = document.createElement("button");
                    open.type = "button";
                    open.textContent = t("查看文件差异与操作");
                    open.onclick = () => { if (this.current() !== state)
                        return; void this.openProposal(state.agent.agent_id, item.operation.proposal_id, item.operation.operation_id).catch(error => this.notice(errorText(error), true)); };
                    row.append(open);
                }
                root.append(row);
            }
        });
        this.controls();
    }
}
