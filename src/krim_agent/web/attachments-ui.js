/** 附件面板；文件只在用户明确操作后上传到话题隔离 API。 */
import { errorText, requestJson } from "./api.js";
import { t } from "./i18n.js";
const MAX_BYTES = 2 * 1024 * 1024;
const allowed = (type) => type.startsWith("text/") || ["application/pdf", "application/zip", "image/png", "image/jpeg", "image/gif", "image/webp"].includes(type);
const node = (id) => document.getElementById(id);
const base64 = (bytes) => { let value = ""; const chunk = 0x8000; for (let index = 0; index < bytes.length; index += chunk)
    value += String.fromCharCode(...bytes.subarray(index, index + chunk)); return btoa(value); };
export class AttachmentsUI {
    insert;
    notice;
    topicId = null;
    attachments = [];
    busy = false;
    uploading = false;
    constructor(insert, notice) {
        this.insert = insert;
        this.notice = notice;
        const file = node("attachment-file");
        node("attachment-upload").onclick = () => { const selected = file.files?.[0]; if (selected)
            void this.uploadFile(selected); };
        node("attachment-refresh").onclick = () => void this.load();
        this.render();
    }
    refreshLocale() { this.render(); }
    setBusy(value) { this.busy = value; this.render(); }
    async selectTopic(id) { this.topicId = id; this.attachments = []; this.render(); await this.load(); }
    async uploadFile(file, expectedTopicId = this.topicId) {
        const topic = this.topicId;
        if (!topic || topic !== expectedTopicId || this.busy || this.uploading)
            return null;
        if (file.size <= 0 || file.size > MAX_BYTES || !allowed(file.type || "application/octet-stream")) {
            this.notice("附件为空、过大或媒体类型不允许", true);
            return null;
        }
        this.uploading = true;
        this.render();
        try {
            const bytes = new Uint8Array(await file.arrayBuffer());
            const saved = await requestJson(`/v1/topics/${encodeURIComponent(topic)}/attachments`, { method: "POST", body: JSON.stringify({ filename: file.name, media_type: file.type || "application/octet-stream", content_base64: base64(bytes), idempotency_key: crypto.randomUUID() }) });
            if (topic !== this.topicId)
                return null;
            this.attachments = [...this.attachments.filter((item) => item.id !== saved.id), saved];
            this.render();
            const reference = await requestJson(`/v1/topics/${encodeURIComponent(topic)}/mentions`, { method: "POST", body: JSON.stringify({ kind: "file", label: saved.filename, content: `[附件 ${saved.filename}：仅提供描述，未解析正文]`, metadata: { attachment_id: saved.id, filename: saved.filename, media_type: saved.media_type, size: saved.byte_size, sha256: saved.sha256 } }) });
            await this.insert(topic, reference);
            return reference;
        }
        catch (error) {
            this.notice(errorText(error), true);
            return null;
        }
        finally {
            this.uploading = false;
            this.render();
        }
    }
    async load() { if (!this.topicId)
        return; const id = this.topicId; try {
        const value = await requestJson(`/v1/topics/${encodeURIComponent(id)}/attachments`);
        if (id === this.topicId) {
            this.attachments = value;
            this.render();
        }
    }
    catch (error) {
        this.notice(errorText(error), true);
    } }
    async remove(item) { if (!this.topicId || this.busy)
        return; try {
        await requestJson(`/v1/topics/${encodeURIComponent(this.topicId)}/attachments/${encodeURIComponent(item.id)}`, { method: "DELETE" });
        this.attachments = this.attachments.filter((value) => value.id !== item.id);
        this.render();
    }
    catch (error) {
        this.notice(errorText(error), true);
    } }
    async download(item) {
        try {
            const payload = await requestJson(`/v1/topics/${encodeURIComponent(item.topic_id)}/attachments/${encodeURIComponent(item.id)}/content`);
            const binary = atob(payload.content_base64), bytes = Uint8Array.from(binary, (character) => character.charCodeAt(0));
            const url = URL.createObjectURL(new Blob([bytes], { type: item.media_type }));
            const link = document.createElement("a");
            link.href = url;
            link.download = item.filename;
            link.click();
            window.setTimeout(() => URL.revokeObjectURL(url), 1000);
        }
        catch (error) {
            this.notice(errorText(error), true);
        }
    }
    render() {
        const root = node("attachment-list");
        root.replaceChildren();
        for (const item of this.attachments) {
            const card = document.createElement("div");
            card.className = "attachment-card";
            const label = document.createElement("strong");
            label.textContent = item.filename;
            const details = document.createElement("span");
            details.className = "muted";
            details.textContent = `${item.media_type} · ${item.byte_size} B`;
            const download = document.createElement("button");
            download.type = "button";
            download.className = "secondary";
            download.textContent = t("下载附件");
            download.onclick = () => void this.download(item);
            const remove = document.createElement("button");
            remove.type = "button";
            remove.className = "secondary";
            remove.textContent = t("删除附件");
            remove.disabled = this.busy;
            remove.onclick = () => void this.remove(item);
            card.append(label, details, download, remove);
            root.append(card);
        }
        node("attachment-upload").disabled = this.busy || this.uploading || !this.topicId;
        node("attachment-refresh").disabled = this.busy || this.uploading || !this.topicId;
    }
}
