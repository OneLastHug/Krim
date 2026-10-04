/** 清单 UI 只发送选择；Python 持久服务负责执行绑定和真实宿主回执。 */
import { errorText, requestJson } from "./api.js";
import type { HostJson } from "./host.js";
import { t } from "./i18n.js";

type ChangeItem = { id: string; title: string; description: string; metadata: Record<string, HostJson>; status: "pending" | "executing" | "undone" | "kept" | "uncertain" };
type ChangeList = { topic_id: string; revision: number; items: ChangeItem[]; active_operation_id: string | null };
type ChangeOperation = { execution: { operation_id: string; topic_id: string; action: string }; status: "executing" | "done" | "error" | "uncertain"; receipt: { results: Array<{ item_id: string; status: string; error_code: string | null }> } | null };
const node = (id: string): HTMLElement => document.getElementById(id)!;
const labels = { pending: "待处理变更", executing: "变更处理中", undone: "已撤销", kept: "已保留", uncertain: "结果待宿主核实" };

export class ChangeListUI {
  private topicId: string | null = null;
  private sessionId: string | null = null;
  private catalog: ChangeList | null = null;
  private operations: ChangeOperation[] = [];
  private pendingTopic: string | null = null;
  private loadVersion = 0;
  constructor(private readonly notice: (message: string, error?: boolean) => void, private readonly invokeHost: (data: HostJson) => Promise<HostJson>) {
    node("change-refresh").onclick = () => void this.refresh();
    node("change-clear").onclick = () => void this.clear();
    node("change-undo-all").onclick = () => void this.invoke("undo-all", null);
    node("change-keep-all").onclick = () => void this.invoke("keep-all", null);
    node("change-cancel").onclick = () => void this.cancel();
    this.render();
  }
  setSession(id: string): void { this.sessionId = id; this.render(); }
  refreshLocale(): void { this.render(); }
  async selectTopic(id: string): Promise<void> {
    if (id !== this.topicId) { this.catalog = null; this.operations = []; }
    this.topicId = id; this.render(); await this.load();
  }
  private base(id: string): string { return `/v1/topics/${encodeURIComponent(id)}/change-list`; }
  private async load(): Promise<void> {
    const id = this.topicId; if (!id) return;
    const version = ++this.loadVersion;
    const [catalog, operations] = await Promise.all([
      requestJson<ChangeList>(this.base(id)), requestJson<ChangeOperation[]>(this.base(id) + "/operations"),
    ]);
    if (id !== this.topicId || version !== this.loadVersion) return;
    this.catalog = catalog; this.operations = operations; this.render();
  }
  private async refresh(): Promise<void> {
    try { await this.load(); } catch (error) { this.notice(errorText(error), true); }
  }
  async bridge(channel: string, data: HostJson): Promise<HostJson> {
    if (!data || typeof data !== "object" || Array.isArray(data) || typeof data.topic_id !== "string") throw new Error(t("需要话题 ID"));
    const { topic_id: id, ...payload } = data;
    let result: HostJson;
    if (channel === "changeList/get") result = await requestJson<HostJson>(this.base(id));
    else if (channel === "changeList/sync") result = await requestJson<HostJson>(this.base(id), { method: "PUT", body: JSON.stringify(payload) });
    else if (channel === "changeList/clear") result = await requestJson<HostJson>(this.base(id) + "/clear", { method: "POST", body: JSON.stringify(payload) });
    else throw new Error(t("变更清单信道无效"));
    if (id === this.topicId && channel !== "changeList/get") await this.load();
    return result;
  }
  private async clear(): Promise<void> {
    if (!this.catalog || !this.topicId || this.catalog.active_operation_id) return;
    const id = this.topicId;
    try { await this.bridge("changeList/clear", { topic_id: id, revision: this.catalog.revision }); }
    catch (error) { this.notice(errorText(error), true); await this.refresh(); }
  }
  private async click(item: ChangeItem): Promise<void> {
    if (!this.topicId || !this.catalog || !this.sessionId) return;
    try {
      await requestJson(this.base(this.topicId) + "/click", { method: "POST", body: JSON.stringify({ revision: this.catalog.revision, item_id: item.id, host_session_id: this.sessionId }) });
      this.notice("已通知宿主查看变更");
    } catch (error) { this.notice(errorText(error), true); await this.refresh(); }
  }
  private async invoke(action: string, itemId: string | null): Promise<void> {
    const id = this.topicId, catalog = this.catalog;
    if (!id || !catalog || !this.sessionId || this.pendingTopic === id || catalog.active_operation_id) return;
    this.pendingTopic = id; this.render();
    const timer = window.setInterval(() => { if (this.topicId === id) void this.refresh(); }, 500);
    try {
      const operation = await this.invokeHost({ topic_id: id, revision: catalog.revision, action, item_id: itemId, idempotency_key: crypto.randomUUID() }) as unknown as ChangeOperation;
      if (this.topicId === id) this.notice(operation.status === "done" ? "变更操作已完成" : operation.status === "uncertain" ? "结果待宿主核实" : "部分变更处理失败", operation.status !== "done");
    } catch (error) { this.notice(errorText(error), true); }
    finally { window.clearInterval(timer); if (this.pendingTopic === id) this.pendingTopic = null; if (this.topicId === id) await this.refresh(); }
  }
  private async cancel(): Promise<void> {
    if (!this.topicId || !this.catalog?.active_operation_id) return;
    try { await requestJson(this.base(this.topicId) + `/operations/${encodeURIComponent(this.catalog.active_operation_id)}/cancel`, { method: "POST" }); await this.load(); }
    catch (error) { this.notice(errorText(error), true); }
  }
  private render(): void {
    const root = node("change-items"); root.replaceChildren();
    const busy = this.catalog?.active_operation_id !== null && this.catalog?.active_operation_id !== undefined || this.pendingTopic === this.topicId && this.topicId !== null;
    const pending = this.catalog?.items.some((item) => item.status === "pending") ?? false;
    for (const item of this.catalog?.items ?? []) {
      const card = document.createElement("article"); card.className = "change-card"; card.dataset.changeId = item.id;
      const title = document.createElement("button"); title.type = "button"; title.className = "secondary change-title"; title.textContent = item.title; title.disabled = !this.sessionId; title.onclick = () => void this.click(item);
      const description = document.createElement("p"); description.textContent = item.description;
      const status = document.createElement("span"); status.className = "badge"; status.textContent = t(labels[item.status]); status.dataset.changeStatus = item.status;
      const actions = document.createElement("div"); actions.className = "actions";
      for (const [action, label] of [["undo", "撤销变更"], ["keep", "保留变更"]]) {
        const button = document.createElement("button"); button.type = "button"; button.className = "secondary"; button.textContent = t(label!); button.disabled = busy || !this.sessionId || item.status !== "pending"; button.onclick = () => void this.invoke(action!, item.id); actions.append(button);
      }
      card.append(title, description, status, actions); root.append(card);
    }
    const active = this.operations.find((op) => op.execution.operation_id === this.catalog?.active_operation_id);
    node("change-status").textContent = active?.status === "uncertain" ? t("结果待宿主核实") : busy ? t("变更处理中") : !this.sessionId ? t("连接宿主后可撤销或保留变更") : !this.catalog?.items.length ? t("暂无变更") : "";
    for (const id of ["change-undo-all", "change-keep-all"]) (node(id) as HTMLButtonElement).disabled = busy || !this.sessionId || !pending;
    (node("change-clear") as HTMLButtonElement).disabled = busy || !this.catalog?.items.length;
    (node("change-refresh") as HTMLButtonElement).disabled = !this.topicId;
    (node("change-cancel") as HTMLButtonElement).disabled = active?.status !== "executing";
    const history = node("change-results"); history.replaceChildren();
    for (const operation of this.operations.slice(0, 5)) {
      const line = document.createElement("p"); line.className = "muted";
      const label = operation.status === "done" ? "变更操作已完成" : operation.status === "error" ? "部分变更处理失败" : operation.status === "uncertain" ? "结果待宿主核实" : "变更处理中";
      const failed = operation.receipt?.results.filter((result) => result.status !== "done").map((result) => result.item_id) ?? [];
      line.textContent = t(label) + (failed.length ? ` · ${failed.join(", ")}` : ""); history.append(line);
    }
  }
}
