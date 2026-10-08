/** 工作区面板沿同一服务端工具权限读取；纯文本展示并保留输入草稿。 */
import { errorText, requestJson } from "./api.js";
import { t } from "./i18n.js";
export class WorkspaceUI {
    notice;
    root = document.createElement("details");
    path = document.createElement("input");
    query = document.createElement("input");
    meta = document.createElement("small");
    status = document.createElement("p");
    entries = document.createElement("ul");
    content = document.createElement("pre");
    buttons = [];
    version = 0;
    loading = false;
    available = false;
    grant;
    base;
    constructor(agentId, notice) {
        this.notice = notice;
        this.base = `/v1/agents/${encodeURIComponent(agentId)}/workspace`;
        this.root.className = "agent-workspace";
        const summary = document.createElement("summary");
        summary.textContent = t("只读工作区");
        this.path.value = ".";
        this.path.maxLength = 512;
        this.path.setAttribute("aria-label", t("工作区相对路径"));
        this.query.maxLength = 200;
        this.query.setAttribute("aria-label", t("工作区搜索文本"));
        this.query.placeholder = t("输入要搜索的文本");
        const controls = document.createElement("div");
        controls.className = "actions";
        for (const [label, action] of [["列出目录", "list"], ["读取文件", "read"], ["搜索文本", "search"]]) {
            const button = document.createElement("button");
            button.type = "button";
            button.className = "secondary";
            button.textContent = t(label);
            button.dataset.action = action;
            button.onclick = () => void this.load(action);
            this.buttons.push(button);
            controls.append(button);
        }
        this.status.setAttribute("role", "status");
        this.content.className = "workspace-content";
        this.root.append(summary, this.meta, this.path, this.query, controls, this.status, this.entries, this.content);
        this.root.ontoggle = () => { if (this.root.open && !this.grant && this.available)
            void this.load("info"); };
    }
    update(bound, available) {
        this.root.hidden = !bound;
        this.available = available;
        this.syncControls();
    }
    syncControls() {
        this.path.disabled = !this.available;
        this.query.disabled = !this.available;
        for (const button of this.buttons)
            button.disabled = this.loading || !this.available || (this.grant !== undefined && (!this.grant.available || !this.grant.tool_names.includes(`workspace_${button.dataset.action}`)));
        for (const button of this.entries.querySelectorAll("button"))
            button.disabled = this.loading || !this.available || this.grant?.available === false;
        if (!this.available)
            this.status.textContent = t("会话关闭后不能访问工作区");
    }
    async load(action) {
        if (this.loading || !this.available)
            return;
        const version = ++this.version;
        const path = this.path.value.trim() || ".", query = this.query.value;
        this.loading = true;
        this.syncControls();
        this.status.textContent = t("正在读取工作区");
        try {
            this.grant = await requestJson(this.base);
            this.meta.textContent = `${this.grant.key} · ${this.grant.version} · ${t("只读")} · ${this.grant.max_file_bytes} B`;
            if (!this.grant.available)
                throw new Error(t("工作区资源已停用或关闭"));
            if (action === "info") {
                this.status.textContent = t("选择相对目录或文件后读取");
                return;
            }
            if (action === "search") {
                if (!query)
                    throw new Error(t("请先输入搜索文本"));
                const result = await requestJson(`${this.base}/search`, { method: "POST", body: JSON.stringify({ path, query, limit: 100 }) });
                if (version !== this.version || !this.available)
                    return;
                this.entries.replaceChildren();
                this.content.textContent = "";
                for (const match of result.matches) {
                    const item = document.createElement("li"), button = document.createElement("button");
                    button.type = "button";
                    button.textContent = `${match.path}:${match.line}`;
                    button.onclick = () => { this.path.value = match.path; void this.load("read"); };
                    const text = document.createElement("span");
                    text.textContent = ` ${match.text}`;
                    item.append(button, text);
                    this.entries.append(item);
                }
                this.status.textContent = `${t("匹配")} ${result.matches.length} · ${t("已扫描")} ${result.files_scanned} · ${t("已跳过")} ${result.files_skipped}${result.truncated ? ` · ${t("结果已截断")}` : ""}`;
            }
            else if (action === "list") {
                const result = await requestJson(`${this.base}/list?${new URLSearchParams({ path })}`);
                if (version !== this.version || !this.available)
                    return;
                this.entries.replaceChildren();
                this.content.textContent = "";
                for (const entry of result.entries) {
                    const item = document.createElement("li"), button = document.createElement("button");
                    button.type = "button";
                    button.textContent = `${entry.path}${entry.kind === "directory" ? "/" : ""}`;
                    button.onclick = () => { this.path.value = entry.path; void this.load(entry.kind === "directory" ? "list" : "read"); };
                    item.append(button);
                    this.entries.append(item);
                }
                this.status.textContent = `${result.path} · ${result.entries.length}${result.truncated ? ` · ${t("结果已截断")}` : ""}`;
            }
            else {
                const file = await requestJson(`${this.base}/read?${new URLSearchParams({ path })}`);
                if (version !== this.version || !this.available)
                    return;
                this.entries.replaceChildren();
                this.content.textContent = file.content.split(/\r?\n/).map((line, index) => `${file.start_line + index}  ${line}`).join("\n");
                this.status.textContent = `${file.path} · SHA-256 ${file.sha256} · ${file.byte_size} B${file.truncated ? ` · ${t("结果已截断")}` : ""}`;
            }
        }
        catch (error) {
            this.status.textContent = errorText(error);
            this.notice(errorText(error), true);
        }
        finally {
            this.loading = false;
            this.syncControls();
        }
    }
}
