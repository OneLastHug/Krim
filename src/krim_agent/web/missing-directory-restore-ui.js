/** 同一缺失目录恢复账本；写前保存原意图，恢复和轮询只GET原键/操作。 */
import { ApiError, errorText, requestJson, requestJsonNullable } from "./api.js";
import { localDate, t } from "./i18n.js";
import { exact, restoreId, restoreKey, restorePath, sameRestoreValue, verifyRestoreDiff, verifyRestoreOperation, verifyRestoreTransition, verifyRestoreView, } from "./missing-directory-restore-contract.js";
import { readRestoreRecovery, saveRestoreRecovery, } from "./missing-directory-restore-recovery.js";
const PAGE = 8, SHA = /^[a-f0-9]{64}$/;
const node = (id) => document.getElementById(id);
const button = (id) => node(id);
const input = (id) => node(id);
const labels = { preparing: "缺失目录恢复预览中", ready: "缺失目录恢复可审阅", executing: "缺失目录恢复发布中", finished: "缺失目录恢复已完成", failed: "缺失目录恢复失败", cancelled: "缺失目录恢复已取消", interrupted: "缺失目录恢复已中断", expired: "缺失目录恢复已过期", uncertain: "缺失目录恢复结果待核实" };
function identity(agent) {
    const value = agent.session.permissions.workspace;
    if (!exact(value, ["key", "version", "fingerprint"]) || typeof value.key !== "string" || !value.key || typeof value.version !== "string" || !value.version || typeof value.fingerprint !== "string" || !SHA.test(value.fingerprint))
        throw new Error(t("工作区资源授权无效"));
    return { scope_id: `session:${agent.session_id}`, workspace_key: value.key, resource_fingerprint: value.fingerprint };
}
function line(parent, label, value) { const p = document.createElement("p"); p.textContent = `${t(label)}: ${value}`; parent.append(p); }
function mismatch() { throw new Error(t("缺失目录恢复回程与原事实不一致")); }
function reviewedAbsence(plan) { return !!plan?.baseline && (plan.phase === "ready" || plan.execute_key !== null); }
export class MissingDirectoryRestoreUI {
    notice;
    states = new Map();
    activeId = null;
    generation = 0;
    timer;
    constructor(notice) {
        this.notice = notice;
        input("missing-directory-restore-query").oninput = () => { const state = this.current(); if (state)
            state.query = input("missing-directory-restore-query").value; this.controls(); };
        input("missing-directory-restore-confirm").onchange = () => this.controls();
        node("missing-directory-restore-preview").onclick = () => void this.preview();
        node("missing-directory-restore-start").onclick = () => void this.start();
        node("missing-directory-restore-refresh").onclick = () => void this.refresh(true);
        node("missing-directory-restore-by-id").onclick = () => void this.readQuery("id");
        node("missing-directory-restore-by-key").onclick = () => void this.readQuery("key");
        node("missing-directory-restore-by-operation-key").onclick = () => void this.readQuery("operation");
        node("missing-directory-restore-pending").onclick = () => void this.findPending();
        node("missing-directory-restore-wait").onclick = () => void this.control("wait");
        node("missing-directory-restore-cancel").onclick = () => void this.control("cancel");
        node("missing-directory-restore-verify").onclick = () => void this.control("verify");
        node("missing-directory-restore-diff-refresh").onclick = () => void this.loadDiff();
        for (const [id, delta] of [["missing-directory-restore-prev", -PAGE], ["missing-directory-restore-next", PAGE]])
            node(id).onclick = () => { const state = this.current(); if (state && !state.loading) {
                state.offset = Math.max(0, state.offset + delta);
                void this.refresh();
            } };
        node("missing-directory-restore-diff-prev").onclick = () => { const state = this.current(); if (state && !state.diffLoading && state.diffBack.length) {
            state.diffOffset = state.diffBack.pop();
            void this.loadDiff();
        } };
        node("missing-directory-restore-diff-next").onclick = () => { const state = this.current(); if (state && !state.diffLoading && state.diff?.next_offset !== null && state.diff?.next_offset !== undefined) {
            state.diffBack.push(state.diffOffset);
            state.diffOffset = state.diff.next_offset;
            void this.loadDiff();
        } };
    }
    setSources(agents) {
        const ids = new Set(agents.map(agent => agent.agent_id));
        let changed = false;
        for (const agent of agents) {
            const old = this.states.get(agent.agent_id);
            if (!old || old.agent.session_id !== agent.session_id || old.agent.session.status !== agent.session.status || !sameRestoreValue(old.agent.session.permissions, agent.session.permissions)) {
                const frozen = identity(agent);
                const state = { agent, identity: frozen, grant: null, recovery: { version: 1, ...frozen, selected_id: null, pending: null }, target: null, selected: null, known: new Map(), entries: [], query: "", diff: null, diffOffset: 0, diffBack: [], diffMore: false, diffAttempted: false, reviewedRevision: null, offset: 0, more: false, stale: true, storageError: false, message: "", loading: false, querying: false, sending: false, controlling: false, diffLoading: false, loadVersion: 0, queryVersion: 0, diffVersion: 0 };
                try {
                    state.recovery = readRestoreRecovery(agent.session_id, frozen);
                }
                catch {
                    state.storageError = true;
                    state.message = t("缺失目录恢复记录不可用，新预览和发布已停用");
                }
                this.states.set(agent.agent_id, state);
                if (agent.agent_id === this.activeId) {
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
    async select(id) {
        this.stopTimer();
        this.generation++;
        this.activeId = id;
        const state = this.current();
        if (state) {
            state.loading = false;
            state.querying = false;
            state.diffLoading = false;
            state.loadVersion++;
            state.queryVersion++;
            state.diffVersion++;
        }
        this.render();
        await this.refresh();
    }
    async useCheckpoint(agentId, checkpoint) {
        if (this.activeId !== agentId)
            await this.select(agentId);
        const state = this.current();
        if (!state)
            return;
        if (checkpoint.scope_id !== state.identity.scope_id || checkpoint.workspace_key !== state.identity.workspace_key || checkpoint.resource_fingerprint !== state.identity.resource_fingerprint
            || !restoreId(checkpoint.checkpoint_id) || !restorePath(checkpoint.root_path) || checkpoint.status !== "available" || !checkpoint.snapshot || !SHA.test(checkpoint.snapshot.snapshot_fingerprint))
            mismatch();
        state.target = { target: { kind: "checkpoint", identifier: checkpoint.checkpoint_id }, root_path: checkpoint.root_path, target_fingerprint: checkpoint.snapshot.snapshot_fingerprint };
        this.render();
        node("missing-directory-restore-panel").scrollIntoView({ block: "nearest" });
    }
    async useBaseline(agentId, value) {
        if (this.activeId !== agentId)
            await this.select(agentId);
        const state = this.current();
        if (!state)
            return;
        if (state.agent.agent_id !== agentId || value.scope_id !== state.identity.scope_id || value.workspace_key !== state.identity.workspace_key || value.resource_fingerprint !== state.identity.resource_fingerprint
            || value.target.kind !== "baseline" || !restoreId(value.target.identifier) || !restorePath(value.root_path) || !SHA.test(value.target_fingerprint))
            mismatch();
        state.target = { target: { ...value.target }, root_path: value.root_path, target_fingerprint: value.target_fingerprint };
        this.render();
        node("missing-directory-restore-panel").scrollIntoView({ block: "nearest" });
    }
    deactivate() { this.stopTimer(); this.generation++; this.activeId = null; }
    refreshLocale() { this.render(); }
    toggled() { if (this.expanded()) {
        if (this.current()?.stale)
            void this.refresh();
        else
            this.schedule();
    }
    else
        this.stopTimer(); }
    current() { return this.activeId ? this.states.get(this.activeId) : undefined; }
    valid(state, generation) { return this.current() === state && this.generation === generation; }
    expanded() { return this.activeId !== null && node("checkpoint-panel").open && !node("checkpoint-directory").hidden; }
    base(state) { return `/v1/agents/${encodeURIComponent(state.agent.agent_id)}/workspace/missing-directory-restores`; }
    save(state) {
        if (state.storageError)
            throw new Error(t("缺失目录恢复记录不可用，新预览和发布已停用"));
        try {
            saveRestoreRecovery(state.agent.session_id, state.recovery, state.identity);
        }
        catch {
            state.storageError = true;
            throw new Error(t("缺失目录恢复记录不可用，新预览和发布已停用"));
        }
    }
    accept(state, value, select) {
        verifyRestoreView(value, state.identity);
        const old = state.known.get(value.plan_id);
        if (old)
            verifyRestoreTransition(old, value);
        state.known.set(value.plan_id, structuredClone(value));
        if (select) {
            if (state.selected?.plan_id !== value.plan_id || state.selected.revision !== value.revision) {
                state.diff = null;
                state.diffOffset = 0;
                state.diffBack = [];
                state.diffAttempted = false;
                state.reviewedRevision = null;
                input("missing-directory-restore-confirm").checked = false;
                state.diffVersion++;
            }
            state.selected = value;
            state.recovery.selected_id = value.plan_id;
            if (!state.storageError)
                this.save(state);
        }
        return value;
    }
    matchPending(state, value, pending) {
        verifyRestoreView(value, state.identity);
        if (value.root_path !== pending.root_path || value.target_snapshot.snapshot_fingerprint !== pending.target_fingerprint)
            mismatch();
        if (pending.kind === "preview") {
            if (value.request_key !== pending.request.request_key || !sameRestoreValue(value.target, pending.request.target))
                mismatch();
        }
        else if (value.plan_id !== pending.request.plan_id || value.execute_key !== pending.request.request_key || value.revision <= pending.request.expected_revision || value.baseline?.fingerprint !== pending.absence_fingerprint)
            mismatch();
    }
    releasePending(state, pending) {
        if (state.recovery.pending !== pending)
            return;
        state.recovery.pending = null;
        try {
            this.save(state);
        }
        catch (error) {
            state.recovery.pending = pending;
            throw error;
        }
    }
    async resolvePending(state, generation, queryVersion, candidate) {
        const pending = state.recovery.pending;
        if (!pending)
            return;
        const encoded = new URLSearchParams({ request_key: pending.request.request_key });
        let value = null;
        if (pending.kind === "preview")
            value = await requestJsonNullable(`${this.base(state)}/by-key?${encoded}`);
        else {
            const operation = await requestJsonNullable(`${this.base(state)}/operations/by-key?${encoded}`);
            if (!this.valid(state, generation) || state.recovery.pending !== pending)
                return;
            if (operation !== null) {
                verifyRestoreOperation(operation, state.identity.scope_id);
                if (operation.kind !== "execute" || operation.plan_id !== pending.request.plan_id || operation.request_key !== pending.request.request_key)
                    mismatch();
                value = await requestJson(`${this.base(state)}/${encodeURIComponent(pending.request.plan_id)}`);
                if (value.operation_id !== operation.operation_id)
                    mismatch();
            }
        }
        if (!this.valid(state, generation) || state.recovery.pending !== pending)
            return;
        if (value !== null) {
            this.matchPending(state, value, pending);
            if (candidate)
                verifyRestoreTransition(candidate, value);
            this.accept(state, value, queryVersion === state.queryVersion);
            this.releasePending(state, pending);
            state.message = "";
        }
        else if (pending.rejected) {
            this.releasePending(state, pending);
            state.message = t("原缺失目录恢复请求未被接受");
        }
        else
            state.message = t("原缺失目录恢复请求尚未确认，请按原键继续查询");
    }
    async refresh(reset = false) {
        const state = this.current();
        if (!state || state.loading)
            return;
        if (reset)
            state.offset = 0;
        const generation = this.generation, version = ++state.loadVersion, queryVersion = state.queryVersion, selectedId = state.recovery.selected_id;
        state.loading = true;
        this.controls();
        try {
            const [items, grant, selected] = await Promise.all([
                requestJson(`${this.base(state)}?offset=${state.offset}&limit=${PAGE + 1}`),
                requestJson(`/v1/agents/${encodeURIComponent(state.agent.agent_id)}/workspace`),
                selectedId ? requestJson(`${this.base(state)}/${encodeURIComponent(selectedId)}`) : Promise.resolve(null),
            ]);
            if (!this.valid(state, generation) || state.loadVersion !== version)
                return;
            if (!grant || grant.key !== state.identity.workspace_key || grant.fingerprint !== state.identity.resource_fingerprint || typeof grant.available !== "boolean" || typeof grant.writable !== "boolean"
                || !Array.isArray(grant.tool_names) || !grant.tool_names.every(name => typeof name === "string") || !Array.isArray(items) || items.length > PAGE + 1)
                mismatch();
            for (const item of items) {
                verifyRestoreView(item, state.identity);
                const before = state.known.get(item.plan_id);
                if (before)
                    verifyRestoreTransition(before, item);
            }
            if (new Set(items.map(item => item.plan_id)).size !== items.length)
                mismatch();
            if (selected) {
                verifyRestoreView(selected, state.identity);
                if (selected.plan_id !== selectedId)
                    mismatch();
                const old = state.known.get(selected.plan_id);
                if (old)
                    verifyRestoreTransition(old, selected);
                const listed = items.find(item => item.plan_id === selected.plan_id);
                if (listed)
                    verifyRestoreTransition(listed, selected);
            }
            state.grant = grant;
            state.entries = items.slice(0, PAGE);
            state.more = items.length > PAGE;
            state.stale = false;
            for (const item of items)
                state.known.set(item.plan_id, structuredClone(item));
            if (!state.storageError)
                state.message = "";
            if (selected && queryVersion === state.queryVersion)
                this.accept(state, selected, true);
            await this.resolvePending(state, generation, queryVersion);
        }
        catch (error) {
            if (this.valid(state, generation) && version === state.loadVersion) {
                state.stale = true;
                state.message = t(errorText(error));
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
    async findPending() {
        const state = this.current();
        if (!state?.recovery.pending || state.querying)
            return;
        const generation = this.generation, version = ++state.queryVersion;
        state.querying = true;
        this.controls();
        try {
            await this.resolvePending(state, generation, version);
        }
        catch (error) {
            if (this.valid(state, generation)) {
                state.message = t(errorText(error));
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
    async readQuery(kind) {
        const state = this.current();
        if (!state || state.querying || !(kind === "id" ? restoreId(state.query) : restoreKey(state.query)))
            return;
        const generation = this.generation, version = ++state.queryVersion, query = state.query;
        state.querying = true;
        this.controls();
        try {
            let value;
            if (kind === "operation") {
                const operation = await requestJsonNullable(`${this.base(state)}/operations/by-key?${new URLSearchParams({ request_key: query })}`);
                if (operation === null)
                    value = null;
                else {
                    verifyRestoreOperation(operation, state.identity.scope_id);
                    if (operation.kind !== "execute" || operation.request_key !== query)
                        mismatch();
                    value = await requestJson(`${this.base(state)}/${encodeURIComponent(operation.plan_id)}`);
                    if (value.plan_id !== operation.plan_id || value.operation_id !== operation.operation_id || value.execute_key !== query)
                        mismatch();
                }
            }
            else
                value = await requestJsonNullable(this.base(state) + (kind === "id" ? `/${encodeURIComponent(query)}` : `/by-key?${new URLSearchParams({ request_key: query })}`));
            if (!this.valid(state, generation) || version !== state.queryVersion)
                return;
            if (value) {
                if (kind === "id" ? value.plan_id !== query : kind === "key" && value.request_key !== query)
                    mismatch();
                this.accept(state, value, true);
                state.message = "";
            }
            else
                state.message = t("未找到缺失目录恢复计划或操作");
        }
        catch (error) {
            if (this.valid(state, generation) && version === state.queryVersion) {
                state.message = t(errorText(error));
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
    activeGrant(state, leaf) { return !state.stale && state.agent.session.status === "open" && state.agent.session.permissions.mode !== "ask" && state.grant?.available === true && state.grant.writable && state.grant.tool_names.includes(leaf); }
    idle(state) { return !state.loading && !state.querying && !state.sending && !state.controlling && !state.diffLoading; }
    canPreview(state) { return this.idle(state) && !state.storageError && !state.recovery.pending && !!state.target && this.activeGrant(state, "workspace_missing_directory_restore_prepare"); }
    canStart(state) { return this.idle(state) && !state.storageError && !state.recovery.pending && state.selected?.phase === "ready" && state.reviewedRevision === state.selected.revision && state.diff !== null && input("missing-directory-restore-confirm").checked && state.agent.session.permissions.mode !== "plan" && this.activeGrant(state, "workspace_missing_directory_restore_apply"); }
    async preview() {
        const state = this.current();
        if (!state || !this.canPreview(state) || !state.target)
            return;
        const target = state.target;
        await this.send(state, { kind: "preview", request: { target: target.target, request_key: crypto.randomUUID(), ttl_seconds: 3600 }, root_path: target.root_path, target_fingerprint: target.target_fingerprint, rejected: false });
    }
    async start() {
        const state = this.current(), plan = state?.selected;
        if (!state || !plan?.baseline || !this.canStart(state))
            return;
        if (!window.confirm(t("确认按当前审阅版本发布缺失目录恢复？")))
            return;
        await this.send(state, { kind: "execute", request: { plan_id: plan.plan_id, expected_revision: plan.revision, request_key: crypto.randomUUID() }, root_path: plan.root_path, target_fingerprint: plan.target_snapshot.snapshot_fingerprint, absence_fingerprint: plan.baseline.fingerprint, rejected: false });
    }
    async send(state, pending) {
        const generation = this.generation, version = ++state.queryVersion;
        state.recovery.pending = pending;
        state.sending = true;
        state.message = "";
        try {
            this.save(state);
        }
        catch (error) {
            state.sending = false;
            state.message = t(errorText(error));
            this.notice(state.message, true);
            this.render();
            return;
        }
        this.render();
        const controller = new AbortController(), timer = window.setTimeout(() => controller.abort(), 15_000);
        let postReturned = false;
        try {
            const candidate = await requestJson(`${this.base(state)}/${pending.kind === "preview" ? "preview" : "start"}?confirmed=true`, { method: "POST", body: JSON.stringify(pending.request), signal: controller.signal });
            postReturned = true;
            if (!this.valid(state, generation))
                return;
            this.matchPending(state, candidate, pending);
            verifyRestoreView(candidate, state.identity);
            // POST只作候选，原键核实前不显示、不释放持久意图。
            await this.resolvePending(state, generation, version, candidate);
        }
        catch (error) {
            if (!this.valid(state, generation))
                return;
            // 确认GET失败不代表已返回的POST被拒绝，必须保留原意图。
            if (!postReturned && state.recovery.pending === pending && error instanceof ApiError && error.status >= 400 && error.status < 500) {
                pending.rejected = true;
                try {
                    this.save(state);
                }
                catch { /* 保留原意图并继续只读核实。 */ }
            }
            state.message = t("原缺失目录恢复请求尚未确认，请按原键继续查询");
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
    async loadDiff() {
        const state = this.current(), plan = state?.selected;
        if (!state || !plan?.baseline || !reviewedAbsence(plan) || state.diffLoading)
            return;
        const generation = this.generation, version = ++state.diffVersion, offset = state.diffOffset;
        state.diffLoading = true;
        state.diffAttempted = true;
        this.controls();
        try {
            const value = await requestJson(`${this.base(state)}/${encodeURIComponent(plan.plan_id)}/diff?offset=${offset}&limit=${PAGE}`);
            if (!this.valid(state, generation) || version !== state.diffVersion || state.selected?.plan_id !== plan.plan_id)
                return;
            verifyRestoreDiff(value, plan, offset, PAGE);
            state.diff = value;
            state.diffMore = value.next_offset !== null;
            if (state.selected.phase === "ready")
                state.reviewedRevision = state.selected.revision;
        }
        catch (error) {
            if (this.valid(state, generation) && version === state.diffVersion) {
                state.diff = null;
                state.diffMore = false;
                state.reviewedRevision = null;
                state.message = t(errorText(error));
                this.notice(state.message, true);
            }
        }
        finally {
            if (version === state.diffVersion)
                state.diffLoading = false;
            if (this.valid(state, generation))
                this.render();
        }
    }
    async control(action) {
        const state = this.current(), plan = state?.selected;
        if (!state || !plan || state.controlling || state.sending)
            return;
        if (action === "verify" && (plan.phase !== "uncertain" || !this.activeGrant(state, "workspace_missing_directory_restore_verify")))
            return;
        const generation = this.generation, version = ++state.queryVersion;
        state.controlling = true;
        this.controls();
        try {
            const suffix = action === "verify" ? `/operations/${encodeURIComponent(plan.operation_id)}/verify?confirmed=true` : `/${encodeURIComponent(plan.plan_id)}/${action}${action === "wait" ? "?timeout_seconds=1" : ""}`;
            const value = await requestJson(this.base(state) + suffix, { method: "POST" });
            if (!this.valid(state, generation) || version !== state.queryVersion)
                return;
            verifyRestoreView(value, state.identity);
            if (value.plan_id !== plan.plan_id || value.request_key !== plan.request_key || plan.execute_key !== null && value.operation_id !== plan.operation_id)
                mismatch();
            this.accept(state, value, true);
            state.message = "";
        }
        catch (error) {
            if (this.valid(state, generation)) {
                state.message = t(errorText(error));
                this.notice(state.message, true);
            }
        }
        finally {
            state.controlling = false;
            if (this.valid(state, generation)) {
                this.render();
                await this.refresh();
            }
        }
    }
    stopTimer() { if (this.timer !== undefined)
        window.clearTimeout(this.timer); this.timer = undefined; }
    schedule() { this.stopTimer(); const state = this.current(); if (state && this.expanded() && (state.recovery.pending || ["preparing", "executing"].includes(state.selected?.phase || "")))
        this.timer = window.setTimeout(() => void this.refresh(), 1000); }
    controls() {
        const state = this.current();
        if (!state)
            return;
        const plan = state.selected;
        button("missing-directory-restore-preview").disabled = !this.canPreview(state);
        button("missing-directory-restore-start").disabled = !this.canStart(state);
        button("missing-directory-restore-refresh").disabled = state.loading;
        for (const id of ["missing-directory-restore-by-id", "missing-directory-restore-by-key", "missing-directory-restore-by-operation-key"])
            button(id).disabled = state.querying || !state.query;
        button("missing-directory-restore-pending").disabled = state.querying || !state.recovery.pending;
        button("missing-directory-restore-wait").disabled = state.controlling || !plan || !["preparing", "executing"].includes(plan.phase);
        button("missing-directory-restore-cancel").disabled = state.controlling || state.sending || !plan || !["preparing", "ready", "executing", "uncertain"].includes(plan.phase);
        button("missing-directory-restore-verify").disabled = state.controlling || plan?.phase !== "uncertain" || !this.activeGrant(state, "workspace_missing_directory_restore_verify");
        button("missing-directory-restore-prev").disabled = state.loading || state.offset === 0;
        button("missing-directory-restore-next").disabled = state.loading || !state.more;
        button("missing-directory-restore-diff-prev").disabled = state.diffLoading || state.diffBack.length === 0;
        button("missing-directory-restore-diff-next").disabled = state.diffLoading || !state.diffMore;
        button("missing-directory-restore-diff-refresh").disabled = state.diffLoading || !reviewedAbsence(plan);
        input("missing-directory-restore-confirm").disabled = plan?.phase !== "ready" || state.reviewedRevision !== plan.revision || state.sending;
    }
    render() {
        const state = this.current();
        if (!state)
            return;
        if (document.activeElement !== input("missing-directory-restore-query"))
            input("missing-directory-restore-query").value = state.query;
        node("missing-directory-restore-target").textContent = state.target ? `${t("缺失目录恢复目标")}: ${state.target.root_path} · ${state.target.target.kind} · ${state.target.target.identifier}` : t("请从可用目录检查点或原计划完整备份选择目标");
        node("missing-directory-restore-message").textContent = state.message || (state.stale ? t("正在读取缺失目录恢复历史") : !state.grant?.available || state.agent.session.status !== "open" ? t("当前会话只能查询缺失目录恢复历史") : "");
        if (state.recovery.pending)
            node("missing-directory-restore-message").textContent += `\n${t("未确认请求键")}: ${state.recovery.pending.request.request_key}`;
        node("missing-directory-restore-page").textContent = t("缺失目录恢复历史第{page}页", { page: Math.floor(state.offset / PAGE) + 1 });
        const items = node("missing-directory-restore-items");
        items.replaceChildren();
        for (const value of state.entries) {
            const article = document.createElement("article");
            article.className = "checkpoint-card";
            article.dataset.directoryRestoreId = value.plan_id;
            const title = document.createElement("strong");
            title.textContent = `${value.root_path} · ${t(labels[value.phase])}`;
            const open = document.createElement("button");
            open.type = "button";
            open.textContent = t("查看缺失目录恢复计划");
            open.disabled = state.querying;
            open.onclick = () => { if (this.current() === state) {
                state.query = value.plan_id;
                void this.readQuery("id");
            } };
            article.append(title, open);
            items.append(article);
        }
        const detail = node("missing-directory-restore-detail");
        detail.replaceChildren();
        delete detail.dataset.planId;
        delete detail.dataset.restorePhase;
        const plan = state.selected;
        if (plan) {
            detail.dataset.planId = plan.plan_id;
            detail.dataset.restorePhase = plan.phase;
            const title = document.createElement("h3");
            title.textContent = `${plan.root_path} · ${t(labels[plan.phase])}`;
            detail.append(title);
            for (const [label, value] of [["恢复计划标识", plan.plan_id], ["目录路径", plan.root_path], ["原预览键", plan.request_key], ["审阅版本", String(plan.revision)], ["会话作用域", plan.scope_id], ["工作区标识", plan.workspace_key], ["工作区资源指纹", plan.resource_fingerprint], ["目标完整目录摘要", plan.target_snapshot.snapshot_fingerprint], ["原操作标识", plan.operation_id], ["创建时间", localDate(plan.created_at)], ["更新时间", localDate(plan.updated_at)]])
                line(detail, label, value);
            for (const [label, count] of [["目录节点数", plan.target_snapshot.entry_count], ["目录数量", plan.target_snapshot.directory_count], ["文件数量", plan.target_snapshot.file_count], ["文件总字节", plan.target_snapshot.byte_size]])
                line(detail, label, String(count));
            if (plan.baseline) {
                line(detail, "缺失目录基线", t("目录确实缺失"));
                line(detail, "缺失基线指纹", plan.baseline.fingerprint);
                if (!reviewedAbsence(plan))
                    line(detail, "基线核实状态", t("已保存观察不表示预览已通过"));
            }
            if (plan.execute_key)
                line(detail, "原执行键", plan.execute_key);
            if (plan.error_code)
                line(detail, "错误代码", plan.error_code);
            if (plan.cancel_requested)
                line(detail, "取消请求", t("原操作已请求取消"));
            if (plan.cleanup_pending)
                line(detail, "保留材料", t("原操作私有暂存材料仍被保留"));
            if (plan.receipt)
                line(detail, "实际发布状态", plan.receipt.status);
        }
        const diff = node("missing-directory-restore-diff");
        diff.replaceChildren();
        node("missing-directory-restore-diff-page").textContent = t("缺失目录恢复差异第{page}页", { page: state.diffBack.length + 1 });
        for (const entry of state.diff?.entries || []) {
            const article = document.createElement("article");
            article.className = "checkpoint-card";
            line(article, "目录差异路径", entry.path);
            line(article, "目录差异动作", entry.operation);
            line(article, "恢复前", t("目录确实缺失"));
            for (const [label, value] of [["恢复后", entry.after]])
                if (value)
                    line(article, label, `${value.kind} · ${value.byte_size} ${t("字节")} · ${value.mode.toString(8)} · ${value.sha256 || ""}`);
            diff.append(article);
        }
        this.controls();
        if (reviewedAbsence(plan) && !state.diff && !state.diffLoading && !state.diffAttempted)
            void this.loadDiff();
    }
}
