/** 宿主资源 Checkpoint 面板；创建/回滚都经 Python callback 持久账本。 */
import { errorText, requestJson } from "./api.js";
import { t } from "./i18n.js";
const node = (id) => document.getElementById(id);
const statusText = { creating: "快照创建中", available: "可回滚", rolling_back: "回滚中", rolled_back: "已回滚", failed: "快照失败", uncertain: "结果待宿主核实" };
export class CheckpointUI {
    notice;
    request;
    topicId = null;
    sessionId = null;
    catalog = null;
    busy = false;
    loadVersion = 0;
    constructor(notice, request) {
        this.notice = notice;
        this.request = request;
        node("checkpoint-refresh").onclick = () => void this.refresh();
        node("checkpoint-create").onclick = () => void this.create();
        this.render();
    }
    setSession(id) { this.sessionId = id; this.render(); }
    refreshLocale() { this.render(); }
    async selectTopic(id) { this.topicId = id; this.catalog = null; this.render(); await this.load(); }
    base(id) { return `/v1/topics/${encodeURIComponent(id)}/checkpoints`; }
    async load() {
        const id = this.topicId;
        if (!id)
            return;
        const version = ++this.loadVersion;
        try {
            const catalog = await requestJson(this.base(id));
            if (id === this.topicId && version === this.loadVersion) {
                this.catalog = catalog;
                this.render();
            }
        }
        catch (error) {
            this.notice(errorText(error), true);
        }
    }
    async refresh() { await this.load(); }
    async create() {
        if (!this.topicId || !this.catalog || !this.sessionId || this.busy || this.catalog.active_operation_id)
            return;
        const topicId = this.topicId, revision = this.catalog.revision;
        this.busy = true;
        this.render();
        try {
            const title = node("checkpoint-title").value.trim() || t("资源快照");
            const result = await this.request("checkpoint/create", { topic_id: topicId, revision, title, metadata: { source: "AgentApp" }, idempotency_key: crypto.randomUUID() });
            if (topicId === this.topicId)
                this.notice(result.status === "done" ? "快照已创建" : "快照结果待宿主核实", result.status !== "done");
        }
        catch (error) {
            this.notice(errorText(error), true);
        }
        finally {
            this.busy = false;
            await this.load();
        }
    }
    async rollback(checkpoint) {
        if (!this.topicId || !this.catalog || !this.sessionId || this.busy || this.catalog.active_operation_id || !["available", "rolled_back"].includes(checkpoint.status))
            return;
        const topicId = this.topicId, revision = this.catalog.revision;
        this.busy = true;
        this.render();
        try {
            const checked = await this.request("checkpoint/canRollback", { topic_id: topicId, revision, checkpoint_id: checkpoint.id, metadata: { source: "AgentApp" } });
            if (!checked.can_rollback) {
                if (topicId === this.topicId)
                    this.notice("无法确认当前快照可回滚", true);
                return;
            }
            const result = await this.request("checkpoint/rollback", { topic_id: topicId, revision, checkpoint_id: checkpoint.id, metadata: { source: "AgentApp" }, idempotency_key: crypto.randomUUID() });
            if (topicId === this.topicId)
                this.notice(result.status === "done" ? "资源已回滚" : "回滚结果待宿主核实", result.status !== "done");
        }
        catch (error) {
            this.notice(errorText(error), true);
        }
        finally {
            this.busy = false;
            await this.load();
        }
    }
    render() {
        const root = node("checkpoint-items");
        root.replaceChildren();
        const busy = this.busy || !!this.catalog?.active_operation_id;
        node("checkpoint-status").textContent = this.catalog?.checkpoints.some((item) => item.status === "uncertain") ? t("结果待宿主核实") : this.catalog?.active_operation_id ? t("Checkpoint 操作处理中") : !this.sessionId ? t("连接宿主后可创建或回滚快照") : !this.catalog?.checkpoints.length ? t("暂无资源快照") : "";
        node("checkpoint-create").disabled = busy || !this.sessionId || !this.catalog;
        node("checkpoint-refresh").disabled = !this.topicId;
        for (const checkpoint of this.catalog?.checkpoints ?? []) {
            const card = document.createElement("article");
            card.className = "checkpoint-card";
            card.dataset.checkpointId = checkpoint.id;
            const title = document.createElement("strong");
            title.textContent = checkpoint.title;
            const state = document.createElement("span");
            state.className = "badge";
            state.dataset.checkpointStatus = checkpoint.status;
            state.textContent = t(statusText[checkpoint.status]);
            const metadata = document.createElement("p");
            metadata.textContent = typeof checkpoint.metadata.description === "string" ? checkpoint.metadata.description : "";
            const rollback = document.createElement("button");
            rollback.type = "button";
            rollback.className = "secondary";
            rollback.textContent = t("回滚到此快照");
            rollback.disabled = busy || !this.sessionId || !["available", "rolled_back"].includes(checkpoint.status);
            rollback.onclick = () => void this.rollback(checkpoint);
            card.append(title, state, metadata, rollback);
            root.append(card);
        }
    }
}
