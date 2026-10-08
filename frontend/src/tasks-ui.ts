/** 会话任务账本界面：只读获取、CAS写入和保留网络重试键。 */
import { requestJson } from "./api.js";
import { t } from "./i18n.js";

type Kind = "plan" | "todo";
type Snapshot = { revision: number; kind: Kind; items: Array<{ id: string; content: string; completed: boolean; position: number }> };

export class TasksUI {
  readonly root = document.createElement("details");
  private select = document.createElement("select");
  private input = document.createElement("input");
  private add = document.createElement("button");
  private revision = document.createElement("small");
  private list = document.createElement("ul");
  private snapshot: Snapshot | null = null;
  private fetching = false;
  private writing = false;
  private fetchedAt = 0;
  private pending: { signature: string; kind: Kind; body: Record<string, unknown> } | null = null;

  constructor(private agentId: string, private status: () => string, private notice: (message: string, error?: boolean) => void) {
    this.root.className = "agent-tasks";
    const summary = document.createElement("summary"); summary.textContent = t("计划与待办");
    this.select.setAttribute("aria-label", t("任务类型"));
    for (const [value, label] of [["todo", "待办"], ["plan", "计划"]]) { const option = document.createElement("option"); option.value = value!; option.textContent = t(label!); this.select.append(option); }
    this.input.maxLength = 200; this.input.setAttribute("aria-label", t("新增任务内容")); this.input.placeholder = t("输入要记录的任务");
    this.add.type = "button"; this.add.textContent = t("新增条目");
    const refresh = document.createElement("button"); refresh.type = "button"; refresh.textContent = t("刷新任务");
    const actions = document.createElement("div"); actions.className = "actions"; actions.append(this.add, refresh);
    this.root.append(summary, this.select, this.revision, this.list, this.input, actions);
    this.root.ontoggle = () => { if (this.root.open) void this.refresh().catch(error => this.notice(String(error), true)); };
    this.select.onchange = () => { this.snapshot = null; this.pending = null; void this.refresh().catch(error => this.notice(String(error), true)); };
    refresh.onclick = () => void this.refresh().catch(error => this.notice(String(error), true));
    this.add.onclick = () => { const content = this.input.value.trim(); if (content) void this.write({ operation: "add", content }).then(() => { if (this.input.value.trim() === content) this.input.value = ""; }).catch(error => this.notice(String(error), true)); };
  }

  update(): void {
    this.add.disabled = this.status() !== "open" || this.writing || !this.snapshot;
    this.input.disabled = this.status() !== "open" || this.writing;
    for (const button of this.list.querySelectorAll<HTMLButtonElement>("button")) button.disabled = this.status() !== "open" || this.writing;
    if (this.root.open && !this.writing && Date.now() - this.fetchedAt > 1000) void this.refresh().catch(error => this.notice(String(error), true));
  }

  private path(kind = this.select.value as Kind): string { return `/v1/agents/${encodeURIComponent(this.agentId)}/tasks/${kind}`; }

  private async refresh(): Promise<void> {
    if (this.fetching || this.writing) return;
    this.fetching = true;
    const kind = this.select.value as Kind;
    try {
      const snapshot = await requestJson<Snapshot>(this.path(kind));
      if (kind === this.select.value) { this.snapshot = snapshot; this.render(); }
      this.fetchedAt = Date.now();
    } finally { this.fetching = false; }
  }

  private render(): void {
    this.revision.textContent = `${t("任务版本")} ${this.snapshot?.revision ?? 0}`;
    this.list.replaceChildren();
    for (const item of this.snapshot?.items || []) {
      const row = document.createElement("li"); row.dataset.itemId = item.id;
      const content = document.createElement("span"); content.textContent = `${item.completed ? "✓ " : "○ "}${item.content}`;
      const complete = document.createElement("button"); complete.type = "button"; complete.textContent = t(item.completed ? "重新打开" : "标记完成");
      const remove = document.createElement("button"); remove.type = "button"; remove.textContent = t("删除条目");
      complete.onclick = () => void this.write({ operation: "update", item_id: item.id, completed: !item.completed }).catch(error => this.notice(String(error), true));
      remove.onclick = () => void this.write({ operation: "remove", item_id: item.id }).catch(error => this.notice(String(error), true));
      row.append(content, complete, remove); this.list.append(row);
    }
    this.update();
  }

  private async write(change: Record<string, unknown>): Promise<void> {
    if (!this.snapshot || this.writing || this.status() !== "open") return;
    const kind = this.select.value as Kind, signature = JSON.stringify(change);
    const body = this.pending?.signature === signature && this.pending.kind === kind ? this.pending.body : { ...change, expected_revision: this.snapshot.revision, request_key: crypto.randomUUID() };
    this.pending = { signature, kind, body }; this.writing = true; this.update();
    try {
      this.snapshot = await requestJson<Snapshot>(this.path(kind), { method: "POST", body: JSON.stringify(body) });
      this.pending = null; this.fetchedAt = Date.now(); this.render();
    } finally { this.writing = false; this.update(); }
  }
}
