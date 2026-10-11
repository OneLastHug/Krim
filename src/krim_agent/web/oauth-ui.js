/** OAuth只保存公开资源名称找回原intent；Cookie/code/token不进入JS存储。 */
import { errorText, requestJson } from "./api.js";
import { t } from "./i18n.js";
const pendingKey = "krim-oauth-pending";
const phases = { pending: "等待浏览器授权", exchanging: "授权码交换中", complete: "浏览器授权完成", failed: "浏览器授权失败", cancelled: "浏览器授权已取消", expired: "浏览器授权已过期" };
export class OAuthUI {
    root;
    notice;
    version = 0;
    disposed = false;
    timer;
    cards = new Map();
    busy = new Set();
    pending = new Set();
    constructor(root, notice) {
        this.root = root;
        this.notice = notice;
        try {
            const names = JSON.parse(sessionStorage.getItem(pendingKey) || "[]");
            if (Array.isArray(names))
                for (const value of names.slice(0, 32))
                    if (typeof value === "string" && /^[a-z][a-z0-9_-]{0,31}$/.test(value))
                        this.pending.add(value);
        }
        catch { /* 只读找回原事实。 */ }
    }
    persist() { try {
        sessionStorage.setItem(pendingKey, JSON.stringify([...this.pending]));
    }
    catch { /* 不保存凭证。 */ } }
    dispose() { this.disposed = true; this.version += 1; if (this.timer !== undefined)
        clearTimeout(this.timer); }
    refreshLocale() { for (const card of this.cards.values())
        this.render(card); }
    schedule() { if (this.disposed)
        return; if (this.timer !== undefined)
        clearTimeout(this.timer); this.timer = window.setTimeout(() => void this.load(), 700); }
    async load() {
        if (this.disposed)
            return;
        const version = ++this.version;
        try {
            const capabilities = await requestJson("/v1/mcp/oauth/capabilities");
            const resources = capabilities.enabled ? await requestJson("/v1/mcp/servers") : [];
            const visible = resources.filter(value => capabilities.resources.includes(value.name));
            const attempts = await Promise.all(visible.map(value => requestJson(`/v1/mcp/servers/${encodeURIComponent(value.name)}/oauth/attempts`)));
            if (this.disposed || version !== this.version)
                return;
            this.root.hidden = !visible.length;
            const names = new Set(visible.map(value => value.name));
            for (const [name, card] of this.cards)
                if (!names.has(name)) {
                    card.root.remove();
                    this.cards.delete(name);
                }
            visible.forEach((resource, index) => {
                let card = this.cards.get(resource.name);
                if (!card) {
                    card = this.create(resource);
                    this.cards.set(resource.name, card);
                    this.root.append(card.root);
                }
                card.resource = resource;
                card.current = attempts[index]?.[0] || null;
                if (!this.busy.has(resource.name))
                    this.pending.delete(resource.name);
                this.render(card);
            });
            this.persist();
            if ([...this.cards.values()].some(card => ["pending", "exchanging"].includes(card.current?.phase || "")))
                this.schedule();
        }
        catch (error) {
            if (!this.disposed && version === this.version)
                this.notice(errorText(error), true);
        }
    }
    create(resource) {
        const root = document.createElement("article");
        root.className = "agent-card oauth-resource";
        root.dataset.name = resource.name;
        const title = document.createElement("strong"), status = document.createElement("p");
        status.className = "oauth-status";
        status.setAttribute("role", "status");
        const begin = document.createElement("button"), cancel = document.createElement("button"), read = document.createElement("button");
        for (const button of [begin, cancel, read]) {
            button.type = "button";
            button.className = "secondary";
        }
        const link = document.createElement("a");
        link.className = "oauth-link";
        link.target = "_blank";
        link.rel = "noopener noreferrer";
        const actions = document.createElement("div");
        actions.className = "actions";
        actions.append(begin, cancel, read, link);
        root.append(title, status, actions);
        const card = { root, resource, current: null, title, status, begin, cancel, read, link };
        begin.onclick = () => void this.begin(card);
        cancel.onclick = () => void this.cancel(card);
        read.onclick = () => void this.load();
        return card;
    }
    render(card) {
        const resource = card.resource, busy = this.busy.has(resource.name), unknown = this.pending.has(resource.name), active = ["pending", "exchanging"].includes(card.current?.phase || "");
        card.title.textContent = `${resource.description.display_name} · ${t("MCP浏览器授权")}`;
        card.status.textContent = unknown ? t("浏览器授权响应未确认，请查询原记录") : card.current ? `${t(phases[card.current.phase] || card.current.phase)}${card.current.error_code ? " · " + card.current.error_code : ""}` : t("尚无浏览器授权记录");
        card.begin.textContent = t("发起MCP浏览器授权");
        card.cancel.textContent = t("取消浏览器授权");
        card.read.textContent = t("查询原授权记录");
        card.link.textContent = t("打开MCP授权页面");
        card.begin.disabled = busy || unknown || active || !resource.available || !resource.profile?.enabled || !resource.resource_hash || resource.profile.resource_hash !== resource.resource_hash;
        card.cancel.disabled = busy || unknown || card.current?.phase !== "pending";
        card.read.disabled = busy;
        card.link.hidden = true;
        card.link.removeAttribute("href");
        if (card.current?.authorization_url) {
            const url = new URL(card.current.authorization_url);
            if (["http:", "https:"].includes(url.protocol) && !url.username && !url.password && !url.hash) {
                card.link.href = url.href;
                card.link.hidden = false;
            }
        }
    }
    async begin(card) {
        const resource = card.resource;
        if (this.disposed || this.busy.has(resource.name) || !resource.profile || !resource.resource_hash)
            return;
        if (!confirm(`${t("确认MCP浏览器授权")} ${resource.name}\n${resource.version || "—"}\n${resource.resource_hash}`))
            return;
        this.version += 1;
        this.busy.add(resource.name);
        this.pending.add(resource.name);
        this.persist();
        this.render(card);
        try {
            const attempt = await requestJson(`/v1/mcp/servers/${encodeURIComponent(resource.name)}/oauth/begin`, { method: "POST", body: JSON.stringify({ expected_revision: resource.profile.revision, resource_hash: resource.resource_hash }) });
            if (this.disposed)
                return;
            if (attempt.name !== resource.name)
                throw new Error(t("MCP返回名称不匹配"));
            card.current = attempt;
            this.pending.delete(resource.name);
            this.persist();
        }
        catch (error) {
            if (!this.disposed)
                this.notice(`${errorText(error)} · ${t("浏览器授权响应未确认，请查询原记录")}`, true);
        }
        finally {
            this.busy.delete(resource.name);
            if (!this.disposed) {
                this.render(card);
                if (!this.pending.has(resource.name))
                    this.schedule();
            }
        }
    }
    async cancel(card) {
        const attempt = card.current;
        if (!attempt || this.disposed || this.busy.has(card.resource.name))
            return;
        this.version += 1;
        this.busy.add(card.resource.name);
        this.render(card);
        try {
            const updated = await requestJson(`/v1/mcp/servers/${encodeURIComponent(card.resource.name)}/oauth/attempts/${encodeURIComponent(attempt.attempt_id)}/cancel`, { method: "POST" });
            if (!this.disposed)
                card.current = updated;
        }
        catch (error) {
            if (!this.disposed)
                this.notice(errorText(error), true);
        }
        finally {
            this.busy.delete(card.resource.name);
            if (!this.disposed) {
                this.render(card);
                this.schedule();
            }
        }
    }
}
