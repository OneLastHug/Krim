/** 原工具组的审阅/显式分支；刷新、丢回程和历史读取只查询原请求。 */
import { ApiError, requestJson, requestJsonNullable, type AgentView } from "./api.js";
import { t } from "./i18n.js";
import { checkedConversationRecord, recoveryFail, recoveryId, recoveryInteger, recoveryKey, recoveryObject, sameRecoveryValue,
  verifyConversationIdentity, verifyConversationPreview, type ConversationIdentity, type ConversationRecoveryPreview, type ConversationRecoveryRecord } from "./conversation-recovery-contract.js";
import { readConversationState, saveConversationState, type ConversationRecoveryState } from "./conversation-recovery-storage.js";

function element<K extends keyof HTMLElementTagNameMap>(tag: K, key: string, className = ""): HTMLElementTagNameMap[K] {
  const node = document.createElement(tag); node.dataset.i18n = key; node.textContent = t(key); node.className = className; return node;
}
const runLabels = { interrupted: "原运行中断", failed: "原运行失败", aborted: "原运行已停止" };
export class ConversationRecoveryUI {
  readonly root = document.createElement("details");
  private identity: ConversationIdentity;
  private current: AgentView;
  private state?: ConversationRecoveryState;
  private preview?: ConversationRecoveryPreview;
  private record?: ConversationRecoveryRecord;
  private busy = false;
  private disposed = false;
  private storageBroken = false;
  private identityBroken = false;
  private error = "";
  private message = "";
  private sourceBox = document.createElement("div");
  private recordBox = document.createElement("div");
  private pendingBox = document.createElement("div");
  private notice = document.createElement("p");
  private readButton: HTMLButtonElement;
  private applyButton: HTMLButtonElement;
  private pendingButton: HTMLButtonElement;
  private historyButton: HTMLButtonElement;
  private historyKey = document.createElement("input");

  constructor(agent: AgentView, private report: (message: string, error?: boolean) => void, private changed: () => void) {
    this.identity = { agent_id: agent.agent_id, session_id: agent.session_id, topic_id: agent.topic_id }; this.current = agent;
    this.root.className = "conversation-recovery";
    this.sourceBox.className = "conversation-recovery-preview"; this.recordBox.className = "conversation-recovery-record";
    this.pendingBox.className = "conversation-recovery-pending"; this.notice.className = "conversation-recovery-notice"; this.notice.setAttribute("role", "status");
    this.readButton = this.button("读取会话恢复快照", () => this.refresh());
    this.applyButton = this.button("确认恢复上下文", () => this.apply());
    this.pendingButton = this.button("查询原会话恢复请求", () => this.refresh());
    this.historyButton = this.button("查询会话恢复历史", () => this.history());
    const label = document.createElement("label"); this.historyKey.type = "text"; this.historyKey.maxLength = 256;
    this.historyKey.setAttribute("aria-label", t("会话恢复请求键")); this.historyKey.dataset.i18nAriaLabel = "会话恢复请求键";
    label.append(element("span", "会话恢复请求键"), this.historyKey);
    this.root.append(element("summary", "会话上下文恢复"), element("p", "原工具组保留在历史中；恢复上下文不会确认工作区效果，也不会重发原工具"), this.readButton,
      this.sourceBox, this.applyButton, this.pendingBox, this.pendingButton, label, this.historyButton, this.recordBox, this.notice);
    try { verifyConversationIdentity(this.identity); this.state = readConversationState(this.identity); }
    catch { this.storageBroken = true; }
    this.root.ontoggle = () => { if (this.root.open) void this.work(() => this.refresh()); };
    this.render();
  }
  private get base(): string { return `/v1/agents/${encodeURIComponent(this.identity.agent_id)}/conversation-recovery`; }
  blocksInput(): boolean { return this.busy || this.storageBroken || this.identityBroken || Boolean(this.state?.pending); }
  dispose(): void { this.disposed = true; this.root.ontoggle = null; }
  update(agent: AgentView): void {
    if (agent.agent_id !== this.identity.agent_id || agent.session_id !== this.identity.session_id || agent.topic_id !== this.identity.topic_id || agent.session.session_id !== this.identity.session_id) {
      this.identityBroken = true; this.preview = undefined;
    } else if (agent.session.revision >= this.current.session.revision) {
      if (agent.session.revision !== this.current.session.revision) this.preview = undefined;
      this.current = agent;
    }
    this.render(); this.changed();
  }
  private button(key: string, action: () => Promise<void>): HTMLButtonElement {
    const node = element("button", key, "secondary"); node.type = "button"; node.onclick = () => void this.work(action); return node;
  }
  private async work(action: () => Promise<void>): Promise<void> {
    if (this.busy || this.disposed) return;
    this.busy = true; this.error = ""; this.render(); this.changed();
    try { await action(); }
    catch (error) { if (!this.disposed) { this.error = error instanceof Error ? error.message : "会话恢复回程与原事实不一致"; this.report(t(this.error), true); } }
    finally { this.busy = false; if (!this.disposed) { this.render(); this.changed(); } }
  }
  private async read(path: string, init?: RequestInit, nullable = false): Promise<unknown> {
    const value = nullable ? await requestJsonNullable<unknown>(path, init) : await requestJson<unknown>(path, init);
    if (this.disposed || this.identityBroken) recoveryFail();
    return value;
  }
  private save(next: ConversationRecoveryState): void {
    if (this.disposed || this.storageBroken || this.identityBroken) throw new Error("会话恢复记录不可用，写操作已停用");
    try {
      if (!sameRecoveryValue(readConversationState(this.identity), this.state)) throw new Error("会话恢复记录不可用，写操作已停用");
      this.state = next; saveConversationState(next, this.identity);
    }
    catch (error) { this.storageBroken = true; throw error; }
  }
  private async agent(): Promise<AgentView> {
    const value = await this.read(`/v1/agents/${encodeURIComponent(this.identity.agent_id)}`);
    if (!recoveryObject(value) || value.agent_id !== this.identity.agent_id || value.session_id !== this.identity.session_id || value.topic_id !== this.identity.topic_id
      || !recoveryObject(value.session) || value.session.session_id !== this.identity.session_id || !recoveryInteger(value.session.revision)
      || typeof value.session.status !== "string" || !["open", "closed", "expired", "destroyed"].includes(value.session.status) || typeof value.status !== "string"
      || value.current_run_id !== null && !recoveryId(value.current_run_id) || value.session.revision < this.current.session.revision) recoveryFail();
    this.current = value as unknown as AgentView; return this.current;
  }
  private idle(): boolean { return this.current.session.status === "open" && !["queued", "running"].includes(this.current.status); }
  private async refresh(): Promise<void> {
    if (this.identityBroken) recoveryFail();
    this.preview = undefined;
    await this.agent();
    if (!this.storageBroken && this.state?.pending) await this.recover();
    else if (!this.storageBroken && this.state?.record) {
      const old = this.state.record;
      const value = await this.read(`${this.base}/by-key?request_key=${encodeURIComponent(old.request.request_key)}`, undefined, true);
      if (value === null) recoveryFail();
      const record = await checkedConversationRecord(value, this.identity, old.request, old.source);
      if (!sameRecoveryValue(record, old) || this.disposed) recoveryFail();
      this.record = record;
    }
    if (this.state?.pending || !this.idle()) { this.preview = undefined; return; }
    const value = await this.read(`${this.base}/preview`); verifyConversationPreview(value, this.identity);
    if (value.session_revision !== this.current.session.revision) recoveryFail();
    this.preview = value;
  }
  private canApply(): boolean {
    return !this.storageBroken && !this.identityBroken && Boolean(this.state && !this.state.pending && this.preview?.available
      && this.preview.session_revision === this.current.session.revision && this.idle());
  }
  private async apply(): Promise<void> {
    const source = this.preview;
    if (!source || !this.canApply() || !window.confirm(`${t("确认保留原工具组并恢复当前审阅的上下文？")}\n${source.source_run_id}\n${source.source_path_hash}\nr${source.session_revision}\n${t("工作区效果仍需按原操作核实；新任务须另行发起")}`)) return;
    const pending = { request: { source_run_id: source.source_run_id!, expected_path_hash: source.source_path_hash, expected_session_revision: source.session_revision, request_key: crypto.randomUUID() }, source: structuredClone(source), rejected: false };
    this.record = undefined;
    this.save({ version: 1, ...this.identity, pending, record: null }); this.render(); this.changed();
    let postReturned = false;
    try {
      const value = await this.read(`${this.base}?confirmed=true`, { method: "POST", body: JSON.stringify(pending.request) }); postReturned = true;
      const candidate = await checkedConversationRecord(value, this.identity, pending.request, pending.source);
      if (this.disposed) recoveryFail();
      await this.recover(candidate);
      this.preview = undefined;
    } catch (error) {
      if (!this.disposed && !postReturned && error instanceof ApiError && error.code !== null && [400, 401, 403, 404, 409, 410, 422, 429].includes(error.status)) {
        this.save({ version: 1, ...this.identity, pending: { ...pending, rejected: true }, record: null });
      }
      this.message = "原会话恢复请求尚未确认，请保留原键继续查询"; throw error;
    }
  }
  private async recover(candidate?: ConversationRecoveryRecord): Promise<void> {
    const pending = this.state?.pending; if (!pending) return;
    const value = await this.read(`${this.base}/by-key?request_key=${encodeURIComponent(pending.request.request_key)}`, undefined, true);
    if (value === null) {
      if (pending.rejected) { this.save({ version: 1, ...this.identity, pending: null, record: null }); this.message = "原会话恢复请求已拒绝，请重新审阅当前快照"; }
      else this.message = "原会话恢复请求尚未确认，请保留原键继续查询";
      return;
    }
    const record = await checkedConversationRecord(value, this.identity, pending.request, pending.source);
    if (candidate && !sameRecoveryValue(record, candidate) || this.disposed) recoveryFail();
    this.save({ version: 1, ...this.identity, pending: null, record }); this.record = record; this.message = "";
  }
  private async history(): Promise<void> {
    if (this.state?.pending || this.identityBroken || this.storageBroken) return;
    if (!recoveryKey(this.historyKey.value)) throw new Error("会话恢复请求键无效");
    const key = this.historyKey.value; this.record = undefined;
    const value = await this.read(`${this.base}/by-key?request_key=${encodeURIComponent(key)}`, undefined, true);
    if (value === null) { this.message = "未找到原会话恢复请求，请保留原键继续查询"; return; }
    const record = await checkedConversationRecord(value, this.identity);
    if (record.request.request_key !== key || this.disposed || this.state?.record?.request.request_key === key && !sameRecoveryValue(this.state.record, record)) recoveryFail();
    this.save({ version: 1, ...this.identity, pending: null, record }); this.record = record; this.message = "";
  }
  private render(): void {
    this.sourceBox.replaceChildren(); this.recordBox.replaceChildren(); this.pendingBox.replaceChildren();
    if (this.storageBroken) this.pendingBox.append(element("p", "会话恢复记录不可用，写操作已停用"));
    if (this.identityBroken) this.pendingBox.append(element("p", "会话恢复回程与原事实不一致"));
    const source = this.preview;
    if (source) {
      this.sourceBox.dataset.available = String(source.available);
      if (!source.available) this.sourceBox.append(element("p", "当前上下文完整，无需恢复"));
      else {
        const summary = document.createElement("pre"); summary.textContent = `${t(runLabels[source.source_run_status!])}: ${source.source_run_id}\n${t("源路径指纹")}: ${source.source_path_hash}\n${t("源分支指纹")}: ${source.source_branch_key}\n${t("审阅会话版本")}: ${source.session_revision}\n${t("保留的原消息组")}: ${source.excluded_message_ids.join(", ")}\n${t("完整前缀锚点")}: ${source.anchor_message_id ?? "—"}`;
        this.sourceBox.append(summary, element("p", "未闭合调用"));
        const pending = document.createElement("ol"); pending.className = "conversation-pending-calls";
        for (const call of source.pending_calls) { const item = document.createElement("li"); item.textContent = `${call.call_id} · ${call.name}\nSHA-256: ${call.arguments_hash}\n${JSON.stringify(call.selectors)}`; pending.append(item); }
        const answered = document.createElement("ol"); answered.className = "conversation-answered-calls";
        for (const call of source.answered_calls) { const item = document.createElement("li"); item.textContent = `${call.call_id} · ${call.message_id}\nSHA-256: ${call.message_hash}`; answered.append(item); }
        this.sourceBox.append(pending, element("p", "已收到的原工具回程"), answered);
      }
    } else { delete this.sourceBox.dataset.available; if (!this.idle()) this.sourceBox.append(element("p", "Agent正在执行、排队或会话已关闭；可按原键查询历史")); }
    if (this.state?.pending) {
      this.pendingBox.append(element("p", "原会话恢复请求尚未确认，请保留原键继续查询"));
      const key = document.createElement("code"); key.textContent = this.state.pending.request.request_key; this.pendingBox.append(key);
    }
    this.root.dataset.pending = String(Boolean(this.state?.pending));
    if (this.record) {
      this.recordBox.dataset.messageId = this.record.recovery_message_id;
      const summary = document.createElement("pre"); summary.textContent = `${t("上下文已恢复")}: ${this.record.recovery_message_id}\n${t("原请求键")}: ${this.record.request.request_key}\n${t(runLabels[this.record.source.source_run_status!])}: ${this.record.source.source_run_id}\n${t("恢复会话版本")}: ${this.record.session_revision}\n${this.record.created_at}`;
      this.recordBox.append(summary, element("p", "工作区效果仍需按原操作核实；新任务须另行发起"));
    } else delete this.recordBox.dataset.messageId;
    this.notice.textContent = [this.message, this.error].filter(Boolean).map(key => t(key)).join("\n");
    this.readButton.disabled = this.busy || this.identityBroken;
    this.applyButton.disabled = this.busy || !this.canApply();
    this.pendingButton.hidden = !this.state?.pending; this.pendingButton.disabled = this.busy || this.identityBroken || this.storageBroken;
    this.historyButton.disabled = this.busy || this.identityBroken || this.storageBroken || Boolean(this.state?.pending);
  }
}
