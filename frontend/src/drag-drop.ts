/** 原生拖放只在 drop 后消费明确文本/File Blob；预览不读取正文或发送消息。 */
import { requestJson, errorText, type MentionRef } from "./api.js";
import type { HostJson } from "./host.js";
import { t } from "./i18n.js";
type Preview = { kind: "text" | "file" | "resource"; label: string; metadata?: Record<string, HostJson> };
type DragState = { active: boolean; preview: Preview | null };
const node = (id: string): HTMLElement => document.getElementById(id)!;
const MAX_FILE_BYTES = 32768;
export class DragDropUI {
  private topicId: string | null = null;
  private busy = false;
  private dropping = false;
  private preview: Preview | null = null;
  private generation = 0;
  constructor(private readonly accept: (topicId: string, ref: MentionRef) => Promise<boolean>, private readonly notice: (message: string, error?: boolean) => void, private readonly upload?: (topicId: string, file: File) => Promise<MentionRef | null>) {
    const area = node("composer");
    area.addEventListener("dragenter", (event) => {
      const drag = event as DragEvent;
      if (!drag.dataTransfer || !this.topicId || this.busy || this.dropping) return;
      drag.preventDefault();
      if (!this.preview) void this.enterNative(drag.dataTransfer);
    });
    area.addEventListener("dragover", (event) => {
      const drag = event as DragEvent; if (!drag.dataTransfer) return;
      drag.preventDefault(); drag.dataTransfer.dropEffect = this.topicId && !this.busy && !this.dropping ? "copy" : "none";
    });
    area.addEventListener("dragleave", (event) => { const related = (event as DragEvent).relatedTarget; if (!(related instanceof Node) || !area.contains(related)) this.clear(); });
    area.addEventListener("drop", (event) => {
      const drag = event as DragEvent; drag.preventDefault(); this.clear();
      if (!drag.dataTransfer || !this.topicId || this.busy || this.dropping) return;
      const id = this.topicId; this.dropping = true;
      void this.consume(drag.dataTransfer, id).catch((error: unknown) => this.notice(errorText(error), true)).finally(() => { this.dropping = false; this.clear(); });
    });
  }
  selectTopic(id: string): void { if (this.topicId !== id) this.clear(); this.topicId = id; }
  setBusy(value: boolean): void { this.busy = value; if (value) this.clear(); }
  refreshLocale(): void { this.render(); }
  private render(): void {
    const preview = node("drag-preview"); preview.hidden = this.preview === null;
    preview.textContent = this.preview ? t("松开以添加引用：{label}", { label: this.preview.label }) : "";
    node("composer").classList.toggle("drag-active", this.preview !== null);
  }
  private clear(): void { this.generation += 1; this.preview = null; this.render(); }
  private async enterNative(data: DataTransfer): Promise<void> {
    const label = data.types.includes("Files") ? t("文本文件") : data.types.includes("text/uri-list") ? t("拖入链接") : t("拖入文本");
    try { await this.bridge("drag/enter", { topic_id: this.topicId!, kind: data.types.includes("Files") ? "file" : "text", label }); }
    catch (error) { this.notice(errorText(error), true); }
  }
  private async consume(data: DataTransfer, id: string): Promise<void> {
    let payload: Record<string, HostJson>;
    if (data.files.length) {
      if (data.files.length !== 1) throw new Error(t("一次只能拖入一个文本文件"));
      const file = data.files[0]!;
      const textType = file.type.startsWith("text/") || ["application/json", "application/xml"].includes(file.type) || (!file.type && /\.(?:txt|md|json|csv|py|ts|js|yaml|yml|toml|xml|log|rst)$/i.test(file.name));
      if (!textType || file.size > MAX_FILE_BYTES) {
        if (!this.upload) throw new Error(t("只接受有限 UTF-8 文本文件"));
        await this.upload(id, file); return;
      }
      let content: string;
      try { content = new TextDecoder("utf-8", { fatal: true }).decode(await file.arrayBuffer()); } catch { throw new Error(t("文件不是有效 UTF-8 文本")); }
      if (content.includes("\0") || !content.trim() || Array.from(content).length > 8192) throw new Error(t("文件内容为空或超过文本边界"));
      payload = { kind: "file", label: file.name, content, metadata: { filename: file.name, size: file.size, media_type: file.type } };
    } else if (data.types.includes("application/vnd.krim-agent.mention+json")) {
      const raw = data.getData("application/vnd.krim-agent.mention+json");
      if (new TextEncoder().encode(raw).length > 32768) throw new Error(t("拖入引用超过大小边界"));
      const value: HostJson = JSON.parse(raw);
      if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error(t("拖入引用需要对象"));
      payload = value;
    } else if (data.types.includes("text/uri-list")) {
      const values = data.getData("text/uri-list").split(/\r?\n/).filter((line) => line && !line.startsWith("#"));
      if (values.length !== 1) throw new Error(t("一次只能拖入一个链接"));
      const resolved = await requestJson<{ url: string }>("/v1/links/resolve", { method: "POST", body: JSON.stringify({ url: values[0], base_url: location.href }) });
      payload = { kind: "resource", label: t("拖入链接"), content: resolved.url, metadata: { url: resolved.url } };
    } else if (data.types.includes("text/plain")) {
      const content = data.getData("text/plain");
      if (new TextEncoder().encode(content).length > MAX_FILE_BYTES || Array.from(content).length > 8192 || !content.trim()) throw new Error(t("拖入文本为空或超过边界"));
      payload = { kind: "text", label: t("拖入文本"), content };
    } else throw new Error(t("拖放需要明确文本或文本文件"));
    if (this.topicId !== id || this.busy) return;
    await this.bridge("drag/drop", { ...payload, topic_id: id });
  }
  async bridge(channel: string, data: HostJson): Promise<HostJson> {
    if (!data || typeof data !== "object" || Array.isArray(data) || typeof data.topic_id !== "string") throw new Error(t("需要话题 ID"));
    const { topic_id: id, ...payload } = data;
    if (id !== this.topicId || this.busy) throw new Error(t("当前话题不可拖放"));
    const base = `/v1/topics/${encodeURIComponent(id)}/drag`;
    if (channel === "drag/enter") {
      const version = ++this.generation;
      const result = await requestJson<DragState>(base + "/enter", { method: "POST", body: JSON.stringify(payload) });
      if (version === this.generation && id === this.topicId && !this.busy) { this.preview = result.preview; this.render(); }
      return result as unknown as HostJson;
    }
    if (channel === "drag/leave") { this.clear(); const result = await requestJson<DragState>(base + "/leave", { method: "POST" }); return result as unknown as HostJson; }
    if (channel !== "drag/drop") throw new Error(t("拖放信道无效"));
    this.clear();
    const reference = await requestJson<MentionRef>(base + "/drop", { method: "POST", body: JSON.stringify(payload) });
    if (id === this.topicId && !this.busy) await this.accept(id, reference);
    return reference as unknown as HostJson;
  }
}
