/** 引用目录与草稿 chip UI；目录、ID、位置和快照由 Python API 复核。 */
import { requestJson, errorText } from "./api.js";
import { insertMention, shiftOccurrences } from "./prompt-context.js";
import { t } from "./i18n.js";
const node = (id) => document.getElementById(id);
const input = (id) => node(id);
export class PromptContextUI {
    notice;
    topicId = null;
    context = { mentions: [], context_tags: [] };
    selected = [];
    previous = "";
    busy = false;
    constructor(notice) {
        this.notice = notice;
        input("prompt").addEventListener("input", () => this.update(input("prompt").value));
        node("mention-add").addEventListener("click", () => void this.change("mention/add", { label: input("mention-label").value, content: input("mention-content").value }));
        node("mention-clear").addEventListener("click", () => void this.change("mention/clear"));
        node("context-tag-add").addEventListener("click", () => void this.change("context/addTag", { name: input("context-tag-name").value, content: input("context-tag-content").value }));
        node("context-tag-clear").addEventListener("click", () => void this.change("context/clearTags"));
        this.render();
    }
    refreshLocale() { this.render(); }
    get occurrences() { return this.selected.map((value) => ({ ...value })); }
    setBusy(value) { this.busy = value; this.render(); }
    async selectTopic(id) {
        if (this.topicId !== id)
            this.clearDraft();
        this.topicId = id;
        await this.load();
    }
    clearDraft() { this.selected = []; this.previous = input("prompt").value; this.render(); }
    setText(value, reset = false) {
        if (reset)
            this.selected = [];
        this.update(value);
        input("prompt").value = value;
    }
    update(value) { this.selected = shiftOccurrences(this.previous, value, this.selected); this.previous = value; this.render(); }
    insert(reference) {
        if (this.busy)
            return;
        const prompt = input("prompt");
        const result = insertMention(prompt.value, prompt.selectionStart, prompt.selectionEnd, reference, this.selected);
        if (result.value.length > 8192) {
            this.notice("加入引用后正文超过上限", true);
            return;
        }
        prompt.value = result.value;
        this.previous = result.value;
        this.selected = result.occurrences;
        prompt.focus();
        prompt.setSelectionRange(result.cursor, result.cursor);
        this.render();
    }
    async acceptDrop(topicId, reference) {
        if (topicId !== this.topicId || this.busy)
            return false;
        await this.load();
        if (topicId !== this.topicId || this.busy)
            return false;
        this.insert(reference);
        return true;
    }
    async load() {
        const id = this.topicId;
        if (!id)
            return;
        const value = await requestJson(`/v1/topics/${encodeURIComponent(id)}/prompt-context`);
        if (this.topicId !== id)
            return;
        this.context = value;
        this.selected = this.selected.filter((occurrence) => value.mentions.some((reference) => reference.id === occurrence.mention_id));
        this.render();
    }
    async change(channel, payload = {}) {
        if (!this.topicId || this.busy)
            return;
        try {
            await this.bridge(channel, { topic_id: this.topicId, ...payload });
            if (channel === "mention/add") {
                input("mention-label").value = "";
                input("mention-content").value = "";
            }
            if (channel === "context/addTag") {
                input("context-tag-name").value = "";
                input("context-tag-content").value = "";
            }
        }
        catch (error) {
            this.notice(errorText(error), true);
        }
    }
    async bridge(channel, data) {
        if (!data || typeof data !== "object" || Array.isArray(data))
            throw new Error(t("上下文参数需要对象"));
        const { topic_id: id, ...payload } = data;
        if (typeof id !== "string" || !id)
            throw new Error(t("需要话题 ID"));
        const base = `/v1/topics/${encodeURIComponent(id)}`;
        let value;
        if (channel === "mention/get" || channel === "context/getTags")
            value = await requestJson(base + "/prompt-context");
        else if (channel === "mention/add")
            value = await requestJson(base + "/mentions", { method: "POST", body: JSON.stringify(payload) });
        else if (channel === "mention/remove") {
            if (typeof payload.mention_id !== "string")
                throw new Error(t("需要引用 ID"));
            value = await requestJson(base + "/mentions/" + encodeURIComponent(payload.mention_id), { method: "DELETE" });
        }
        else if (channel === "mention/clear")
            value = await requestJson(base + "/mentions", { method: "DELETE" });
        else if (channel === "context/addTag")
            value = await requestJson(base + "/context-tags", { method: "POST", body: JSON.stringify(payload) });
        else if (channel === "context/removeTag") {
            if (typeof payload.name !== "string")
                throw new Error(t("需要背景标签名称"));
            value = await requestJson(base + "/context-tags?name=" + encodeURIComponent(payload.name), { method: "DELETE" });
        }
        else if (channel === "context/clearTags")
            value = await requestJson(base + "/context-tags", { method: "DELETE" });
        else
            throw new Error(t("上下文信道无效"));
        if (id === this.topicId)
            await this.load();
        return value;
    }
    render() {
        const references = node("mention-list");
        references.replaceChildren();
        for (const reference of this.context.mentions) {
            const chip = document.createElement("div");
            chip.className = "prompt-chip";
            const insert = document.createElement("button");
            insert.type = "button";
            insert.className = "secondary";
            insert.textContent = t("插入 @{label}", { label: reference.label });
            insert.disabled = this.busy;
            insert.onclick = () => this.insert(reference);
            const remove = document.createElement("button");
            remove.type = "button";
            remove.className = "secondary";
            remove.textContent = t("删除引用");
            remove.disabled = this.busy;
            remove.onclick = () => void this.change("mention/remove", { mention_id: reference.id });
            chip.append(insert, remove);
            references.append(chip);
        }
        const tags = node("context-tag-list");
        tags.replaceChildren();
        for (const tag of this.context.context_tags) {
            const chip = document.createElement("div");
            chip.className = "prompt-chip";
            const title = document.createElement("span");
            title.textContent = tag.name;
            const remove = document.createElement("button");
            remove.type = "button";
            remove.className = "secondary";
            remove.textContent = t("删除背景标签");
            remove.disabled = this.busy;
            remove.onclick = () => void this.change("context/removeTag", { name: tag.name });
            chip.append(title, remove);
            tags.append(chip);
        }
        const draft = node("mention-draft");
        draft.replaceChildren();
        for (const occurrence of this.selected) {
            const reference = this.context.mentions.find((item) => item.id === occurrence.mention_id);
            if (!reference)
                continue;
            const chip = document.createElement("span");
            chip.className = "prompt-chip";
            chip.textContent = t("本次引用：{label}", { label: reference.label });
            draft.append(chip);
        }
        for (const id of ["mention-add", "mention-clear", "context-tag-add", "context-tag-clear"])
            node(id).disabled = this.busy || !this.topicId;
    }
}
