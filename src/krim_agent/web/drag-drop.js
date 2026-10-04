/** 原生拖放只在 drop 后消费明确文本/File Blob；预览不读取正文或发送消息。 */
import { requestJson, errorText } from "./api.js";
import { t } from "./i18n.js";
const node = (id) => document.getElementById(id);
const MAX_FILE_BYTES = 32768;
export class DragDropUI {
    accept;
    notice;
    topicId = null;
    busy = false;
    dropping = false;
    preview = null;
    generation = 0;
    constructor(accept, notice) {
        this.accept = accept;
        this.notice = notice;
        const area = node("composer");
        area.addEventListener("dragenter", (event) => {
            const drag = event;
            if (!drag.dataTransfer || !this.topicId || this.busy || this.dropping)
                return;
            drag.preventDefault();
            if (!this.preview)
                void this.enterNative(drag.dataTransfer);
        });
        area.addEventListener("dragover", (event) => {
            const drag = event;
            if (!drag.dataTransfer)
                return;
            drag.preventDefault();
            drag.dataTransfer.dropEffect = this.topicId && !this.busy && !this.dropping ? "copy" : "none";
        });
        area.addEventListener("dragleave", (event) => { const related = event.relatedTarget; if (!(related instanceof Node) || !area.contains(related))
            this.clear(); });
        area.addEventListener("drop", (event) => {
            const drag = event;
            drag.preventDefault();
            this.clear();
            if (!drag.dataTransfer || !this.topicId || this.busy || this.dropping)
                return;
            const id = this.topicId;
            this.dropping = true;
            void this.consume(drag.dataTransfer, id).catch((error) => this.notice(errorText(error), true)).finally(() => { this.dropping = false; this.clear(); });
        });
    }
    selectTopic(id) { if (this.topicId !== id)
        this.clear(); this.topicId = id; }
    setBusy(value) { this.busy = value; if (value)
        this.clear(); }
    refreshLocale() { this.render(); }
    render() {
        const preview = node("drag-preview");
        preview.hidden = this.preview === null;
        preview.textContent = this.preview ? t("松开以添加引用：{label}", { label: this.preview.label }) : "";
        node("composer").classList.toggle("drag-active", this.preview !== null);
    }
    clear() { this.generation += 1; this.preview = null; this.render(); }
    async enterNative(data) {
        const label = data.types.includes("Files") ? t("文本文件") : data.types.includes("text/uri-list") ? t("拖入链接") : t("拖入文本");
        try {
            await this.bridge("drag/enter", { topic_id: this.topicId, kind: data.types.includes("Files") ? "file" : "text", label });
        }
        catch (error) {
            this.notice(errorText(error), true);
        }
    }
    async consume(data, id) {
        let payload;
        if (data.files.length) {
            if (data.files.length !== 1)
                throw new Error(t("一次只能拖入一个文本文件"));
            const file = data.files[0];
            const textType = file.type.startsWith("text/") || ["application/json", "application/xml"].includes(file.type) || (!file.type && /\.(?:txt|md|json|csv|py|ts|js|yaml|yml|toml|xml|log|rst)$/i.test(file.name));
            if (!textType || file.size > MAX_FILE_BYTES)
                throw new Error(t("只接受有限 UTF-8 文本文件"));
            let content;
            try {
                content = new TextDecoder("utf-8", { fatal: true }).decode(await file.arrayBuffer());
            }
            catch {
                throw new Error(t("文件不是有效 UTF-8 文本"));
            }
            if (content.includes("\0") || !content.trim() || Array.from(content).length > 8192)
                throw new Error(t("文件内容为空或超过文本边界"));
            payload = { kind: "file", label: file.name, content, metadata: { filename: file.name, size: file.size, media_type: file.type } };
        }
        else if (data.types.includes("application/vnd.krim-agent.mention+json")) {
            const raw = data.getData("application/vnd.krim-agent.mention+json");
            if (new TextEncoder().encode(raw).length > 32768)
                throw new Error(t("拖入引用超过大小边界"));
            const value = JSON.parse(raw);
            if (!value || typeof value !== "object" || Array.isArray(value))
                throw new Error(t("拖入引用需要对象"));
            payload = value;
        }
        else if (data.types.includes("text/uri-list")) {
            const values = data.getData("text/uri-list").split(/\r?\n/).filter((line) => line && !line.startsWith("#"));
            if (values.length !== 1)
                throw new Error(t("一次只能拖入一个链接"));
            const resolved = await requestJson("/v1/links/resolve", { method: "POST", body: JSON.stringify({ url: values[0], base_url: location.href }) });
            payload = { kind: "resource", label: t("拖入链接"), content: resolved.url, metadata: { url: resolved.url } };
        }
        else if (data.types.includes("text/plain")) {
            const content = data.getData("text/plain");
            if (new TextEncoder().encode(content).length > MAX_FILE_BYTES || Array.from(content).length > 8192 || !content.trim())
                throw new Error(t("拖入文本为空或超过边界"));
            payload = { kind: "text", label: t("拖入文本"), content };
        }
        else
            throw new Error(t("拖放需要明确文本或文本文件"));
        if (this.topicId !== id || this.busy)
            return;
        await this.bridge("drag/drop", { ...payload, topic_id: id });
    }
    async bridge(channel, data) {
        if (!data || typeof data !== "object" || Array.isArray(data) || typeof data.topic_id !== "string")
            throw new Error(t("需要话题 ID"));
        const { topic_id: id, ...payload } = data;
        if (id !== this.topicId || this.busy)
            throw new Error(t("当前话题不可拖放"));
        const base = `/v1/topics/${encodeURIComponent(id)}/drag`;
        if (channel === "drag/enter") {
            const version = ++this.generation;
            const result = await requestJson(base + "/enter", { method: "POST", body: JSON.stringify(payload) });
            if (version === this.generation && id === this.topicId && !this.busy) {
                this.preview = result.preview;
                this.render();
            }
            return result;
        }
        if (channel === "drag/leave") {
            this.clear();
            const result = await requestJson(base + "/leave", { method: "POST" });
            return result;
        }
        if (channel !== "drag/drop")
            throw new Error(t("拖放信道无效"));
        this.clear();
        const reference = await requestJson(base + "/drop", { method: "POST", body: JSON.stringify(payload) });
        if (id === this.topicId && !this.busy)
            await this.accept(id, reference);
        return reference;
    }
}
