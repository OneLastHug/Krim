/** 统一检查点来源入口；Host回程和原生工作区请求分别沿原业务服务。 */
import { errorText, requestJson, type AgentView } from "./api.js";
import type { HostJson } from "./host.js";
import { t } from "./i18n.js";
import { NativeCheckpointUI } from "./native-checkpoint-ui.js";

type Checkpoint = { id: string; title: string; metadata: Record<string, HostJson>; snapshot_ref: HostJson; status: "creating" | "available" | "rolling_back" | "rolled_back" | "failed" | "uncertain" };
type Catalog = { topic_id: string; revision: number; active_operation_id: string | null; checkpoints: Checkpoint[] };
type Operation = { status: "executing" | "done" | "error" | "uncertain"; execution: { operation_id: string; checkpoint_id: string } };
const node = (id: string): HTMLElement => document.getElementById(id)!;
const statusText: Record<Checkpoint["status"], string> = { creating: "快照创建中", available: "可回滚", rolling_back: "回滚中", rolled_back: "已回滚", failed: "快照失败", uncertain: "结果待宿主核实" };

export class CheckpointUI {
  private topicId: string | null = null;
  private sessionId: string | null = null;
  private catalog: Catalog | null = null;
  private pendingTopics = new Set<string>();
  private loadVersion = 0;
  private generation = 0;
  private source = "host";
  private sources: AgentView[] = [];
  private sourceSignature = "";
  private native: NativeCheckpointUI;
  constructor(
    private readonly notice: (message: string, error?: boolean) => void,
    private readonly request: (channel: string, data: HostJson) => Promise<HostJson>,
    openProposal: (agentId: string, proposalId: string, operationId?: string) => Promise<void>,
  ) {
    this.native = new NativeCheckpointUI(notice, openProposal);
    node("checkpoint-refresh").onclick = () => void this.load();
    node("checkpoint-create").onclick = () => void this.create();
    (node("checkpoint-source") as HTMLSelectElement).onchange = () => void this.switchSource((node("checkpoint-source") as HTMLSelectElement).value);
    node("checkpoint-panel").ontoggle = () => this.native.toggled();
    this.renderSources(); this.render();
  }
  setSession(id: string): void { this.sessionId = id; this.render(); }
  refreshLocale(): void { this.sourceSignature = ""; this.renderSources(); this.render(); this.native.refreshLocale(); }
  setWorkspaceSources(agents: AgentView[]): void {
    this.sources = agents.filter(agent => Boolean(agent.session.permissions.workspace));
    this.native.setSources(this.sources);
    if (this.source !== "host" && !this.sources.some(agent => agent.agent_id === this.source)) void this.switchSource("host");
    this.renderSources();
  }
  async selectWorkspace(id: string): Promise<void> {
    if (!this.sources.some(agent => agent.agent_id === id)) throw new Error(t("工作区资源已停用或关闭"));
    (node("checkpoint-panel") as HTMLDetailsElement).open = true;
    await this.switchSource(id); node("checkpoint-panel").scrollIntoView({ block: "nearest" });
  }
  beginTopic(id: string): void {
    if (this.topicId !== id) { this.catalog = null; if (this.source === "host") this.generation += 1; }
    this.topicId = id; this.render();
  }
  async selectTopic(id: string): Promise<void> { this.beginTopic(id); if (this.source === "host") await this.load(); }
  private async switchSource(source: string): Promise<void> {
    this.generation += 1; this.source = source; this.renderSources(); this.render();
    if (source === "host") { this.native.deactivate(); await this.load(); }
    else await this.native.select(source);
  }
  private renderSources(): void {
    const select = node("checkpoint-source") as HTMLSelectElement;
    const signature = JSON.stringify(this.sources.map(agent => [agent.agent_id, agent.name]));
    if (signature !== this.sourceSignature) {
      this.sourceSignature = signature; select.replaceChildren();
      const host = document.createElement("option"); host.value = "host"; host.textContent = t("Host 当前话题"); select.append(host);
      for (const agent of this.sources) {
        const option = document.createElement("option"); option.value = agent.agent_id; option.textContent = `${t("工作区")} · ${agent.name} · ${agent.agent_id}`; select.append(option);
      }
    }
    select.value = this.source;
  }
  private current(id: string, generation: number): boolean { return this.source === "host" && id === this.topicId && generation === this.generation; }
  private base(id: string): string { return `/v1/topics/${encodeURIComponent(id)}/checkpoints`; }
  private async load(): Promise<void> {
    const id = this.topicId; if (!id || this.source !== "host") return;
    const version = ++this.loadVersion, generation = this.generation;
    try {
      const catalog = await requestJson<Catalog>(this.base(id));
      if (this.current(id, generation) && version === this.loadVersion) { this.catalog = catalog; this.render(); }
    } catch (error) { if (this.current(id, generation) && version === this.loadVersion) this.notice(errorText(error), true); }
  }
  private async create(): Promise<void> {
    if (this.source !== "host" || !this.topicId || !this.catalog || !this.sessionId || this.pendingTopics.has(this.topicId) || this.catalog.active_operation_id) return;
    const topicId = this.topicId, revision = this.catalog.revision, generation = this.generation;
    this.pendingTopics.add(topicId); this.render();
    try {
      const title = (node("checkpoint-title") as HTMLInputElement).value.trim() || t("资源快照");
      const result = await this.request("checkpoint/create", { topic_id: topicId, revision, title, metadata: { source: "AgentApp" }, idempotency_key: crypto.randomUUID() }) as unknown as Operation;
      if (this.current(topicId, generation)) this.notice(result.status === "done" ? "快照已创建" : "快照结果待宿主核实", result.status !== "done");
    } catch (error) { if (this.current(topicId, generation)) this.notice(errorText(error), true); }
    finally { this.pendingTopics.delete(topicId); this.render(); if (this.current(topicId, generation)) await this.load(); }
  }
  private async rollback(checkpoint: Checkpoint): Promise<void> {
    if (this.source !== "host" || !this.topicId || !this.catalog || !this.sessionId || this.pendingTopics.has(this.topicId) || this.catalog.active_operation_id || !["available", "rolled_back"].includes(checkpoint.status)) return;
    const topicId = this.topicId, revision = this.catalog.revision, generation = this.generation;
    this.pendingTopics.add(topicId); this.render();
    try {
      const checked = await this.request("checkpoint/canRollback", { topic_id: topicId, revision, checkpoint_id: checkpoint.id, metadata: { source: "AgentApp" } }) as { can_rollback: boolean; reason: string };
      // canRollback返回前来源已切换时不再发起原话题回滚。
      if (!this.current(topicId, generation)) return;
      if (!checked.can_rollback) { this.notice("无法确认当前快照可回滚", true); return; }
      const result = await this.request("checkpoint/rollback", { topic_id: topicId, revision, checkpoint_id: checkpoint.id, metadata: { source: "AgentApp" }, idempotency_key: crypto.randomUUID() }) as unknown as Operation;
      if (this.current(topicId, generation)) this.notice(result.status === "done" ? "资源已回滚" : "回滚结果待宿主核实", result.status !== "done");
    } catch (error) { if (this.current(topicId, generation)) this.notice(errorText(error), true); }
    finally { this.pendingTopics.delete(topicId); this.render(); if (this.current(topicId, generation)) await this.load(); }
  }
  private render(): void {
    node("checkpoint-host").hidden = this.source !== "host"; node("checkpoint-native").hidden = this.source === "host";
    const root = node("checkpoint-items"); root.replaceChildren();
    const busy = this.pendingTopics.has(this.topicId || "") || !!this.catalog?.active_operation_id;
    node("checkpoint-status").textContent = this.catalog?.checkpoints.some((item) => item.status === "uncertain") ? t("结果待宿主核实") : this.catalog?.active_operation_id ? t("Checkpoint 操作处理中") : !this.sessionId ? t("连接宿主后可创建或回滚快照") : !this.catalog?.checkpoints.length ? t("暂无资源快照") : "";
    (node("checkpoint-create") as HTMLButtonElement).disabled = busy || !this.sessionId || !this.catalog;
    (node("checkpoint-refresh") as HTMLButtonElement).disabled = !this.topicId;
    for (const checkpoint of this.catalog?.checkpoints ?? []) {
      const card = document.createElement("article"); card.className = "checkpoint-card"; card.dataset.checkpointId = checkpoint.id;
      const title = document.createElement("strong"); title.textContent = checkpoint.title;
      const state = document.createElement("span"); state.className = "badge"; state.dataset.checkpointStatus = checkpoint.status; state.textContent = t(statusText[checkpoint.status]);
      const metadata = document.createElement("p"); metadata.textContent = typeof checkpoint.metadata.description === "string" ? checkpoint.metadata.description : "";
      const rollback = document.createElement("button"); rollback.type = "button"; rollback.className = "secondary"; rollback.textContent = t("回滚到此快照"); rollback.disabled = busy || !this.sessionId || !["available", "rolled_back"].includes(checkpoint.status); rollback.onclick = () => void this.rollback(checkpoint);
      card.append(title, state, metadata, rollback); root.append(card);
    }
  }
}
