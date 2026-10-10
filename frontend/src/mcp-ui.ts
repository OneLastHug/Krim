/** MCP界面只消费公开引用；控制丢响应后查询事实，不能自动重放写请求。 */
import { errorText, requestJson } from "./api.js";
import { t } from "./i18n.js";

export interface MCPResourceView {
  name: string; available: boolean; version: string | null; resource_hash: string | null;
  description: { display_name: string; transport: string; namespace: string; readonly_tools: string[] };
  profile: { revision: number; enabled: boolean; resource_hash: string; resource_version: string } | null;
  connection: { status: string; tools: string[]; error: string | null; error_code: string | null };
}
type Card = { root: HTMLElement; view: MCPResourceView; title: HTMLElement; meta: HTMLElement; status: HTMLElement;
  pending: HTMLElement; toggle: HTMLButtonElement; connect: HTMLButtonElement; disconnect: HTMLButtonElement;
  query: HTMLButtonElement; wait: HTMLButtonElement };
const pendingKey = "krim-mcp-pending";
const statusLabels: Record<string, string> = { disabled: "MCP未启用", connecting: "MCP连接中", ready: "MCP已连接", failed: "MCP连接失败", closing: "MCP关闭中", closed: "MCP未连接" };

export class MCPUI {
  private disposed = false; private version = 0; private timer: number | undefined;
  private cards = new Map<string, Card>(); private busy = new Set<string>(); private pending = new Set<string>();
  constructor(private root: HTMLElement, private notice: (message: string, error?: boolean) => void,
    private changed: () => Promise<void>) {
    try { const values: unknown = JSON.parse(sessionStorage.getItem(pendingKey) || "[]");
      if (Array.isArray(values)) for (const name of values.slice(0, 256)) if (typeof name === "string" && /^[a-z][a-z0-9_-]{0,31}$/.test(name)) this.pending.add(name);
    } catch { /* 浏览器存储不可用时仍只读取服务端事实。 */ }
  }
  async load(): Promise<void> {
    const version = ++this.version;
    try { const resources = await requestJson<MCPResourceView[]>("/v1/mcp/servers");
      if (this.disposed || version !== this.version) return;
      const names = new Set(resources.map(value => value.name));
      if (resources.length && !this.cards.size) this.root.replaceChildren();
      for (const [name, card] of this.cards) if (!names.has(name)) { card.root.remove(); this.cards.delete(name); }
      for (const resource of resources) { if (!this.busy.has(resource.name)) this.pending.delete(resource.name); let card = this.cards.get(resource.name);
        if (!card) { card = this.create(resource); this.cards.set(resource.name, card); this.root.append(card.root); }
        card.view = resource; this.render(card); }
      this.persist(); if (!resources.length) this.root.textContent = t("暂无MCP资源");
      if (resources.some(value => ["connecting", "closing"].includes(value.connection.status))) this.schedule();
    } catch (error) { if (!this.disposed && version === this.version) this.notice(errorText(error), true); }
  }
  refreshLocale(): void { for (const card of this.cards.values()) this.render(card); }
  dispose(): void { this.disposed = true; this.version += 1; if (this.timer !== undefined) clearTimeout(this.timer); }
  private persist(): void { try { sessionStorage.setItem(pendingKey, JSON.stringify([...this.pending])); } catch { /* 只保存公开名称。 */ } }
  private schedule(): void { if (this.disposed) return; if (this.timer !== undefined) clearTimeout(this.timer); this.timer = window.setTimeout(() => void this.load(), 400); }
  private create(view: MCPResourceView): Card {
    const root = document.createElement("article"); root.className = "agent-card mcp-resource"; root.dataset.name = view.name;
    const title = document.createElement("strong"), meta = document.createElement("small"), status = document.createElement("p"), pending = document.createElement("p");
    meta.className = "muted"; status.className = "mcp-resource-status"; pending.className = "mcp-pending"; pending.setAttribute("role", "status");
    const actions = document.createElement("div"); actions.className = "actions";
    const toggle = document.createElement("button"), connect = document.createElement("button"), disconnect = document.createElement("button"), query = document.createElement("button"), wait = document.createElement("button");
    for (const button of [toggle, connect, disconnect, query, wait]) { button.type = "button"; button.className = "secondary"; actions.append(button); }
    root.append(title, meta, status, pending, actions); const card = { root, view, title, meta, status, pending, toggle, connect, disconnect, query, wait };
    toggle.onclick = () => void this.control(card, card.view.profile?.enabled ? "disable" : "enable"); connect.onclick = () => void this.control(card, "connect"); disconnect.onclick = () => void this.control(card, "disconnect"); query.onclick = () => void this.load(); wait.onclick = () => void this.readWait(card); return card;
  }
  private render(card: Card): void {
    const view = card.view, enabled = view.profile?.enabled === true, busy = this.busy.has(view.name), unknown = this.pending.has(view.name), currentHash = view.resource_hash || view.profile?.resource_hash;
    card.title.textContent = `${view.description.display_name} · ${t(statusLabels[view.connection.status] || view.connection.status)}`;
    card.meta.textContent = `${view.name} · ${view.description.transport} · ${view.version || "—"} · ${t("配置版本")} ${view.profile?.revision || 0}`; card.meta.title = currentHash || "";
    card.status.textContent = view.connection.error || (view.connection.tools.length ? `${t("工具")}: ${view.connection.tools.join(", ")}` : t("尚未连接"));
    if (view.profile && view.resource_hash && view.profile.resource_hash !== view.resource_hash) card.status.textContent = t("MCP资源版本已变化，需重新审阅");
    card.pending.hidden = !unknown; card.pending.textContent = t("MCP状态未确认，请只读查询"); card.toggle.textContent = t(enabled ? "停用MCP资源" : "启用MCP资源"); card.connect.textContent = t("连接MCP资源"); card.disconnect.textContent = t("断开MCP连接"); card.query.textContent = t("查询原MCP状态"); card.wait.textContent = t("等待MCP连接");
    card.toggle.disabled = busy || unknown || (!enabled && !view.available) || !currentHash || view.connection.status === "closing";
    card.connect.disabled = busy || unknown || !enabled || !view.available || !view.resource_hash || view.profile?.resource_hash !== view.resource_hash || ["ready", "connecting", "closing"].includes(view.connection.status);
    card.disconnect.disabled = busy || unknown || !enabled || !view.available || !["ready", "connecting", "failed"].includes(view.connection.status); card.query.disabled = busy; card.wait.disabled = busy || unknown || !["connecting", "closing"].includes(view.connection.status);
  }
  private async control(card: Card, action: "enable" | "disable" | "connect" | "disconnect"): Promise<void> {
    const view = card.view, resourceHash = view.resource_hash || view.profile?.resource_hash; if (this.busy.has(view.name) || this.pending.has(view.name) || !resourceHash || this.disposed) return;
    const payload = { expected_revision: view.profile?.revision || 0, resource_hash: resourceHash };
    if (!confirm(`${t("确认MCP资源操作")} ${action}\n${view.name} · ${view.version || "—"}\n${resourceHash}\n${t("只读工具")}: ${view.description.readonly_tools.join(", ")}`)) return;
    this.busy.add(view.name); this.pending.add(view.name); this.persist(); this.version += 1; this.render(card);
    try { const selection = action === "enable" || action === "disable"; const updated = await requestJson<MCPResourceView>(`/v1/mcp/servers/${encodeURIComponent(view.name)}${selection ? "" : "/" + action}`, { method: selection ? "PUT" : "POST", body: JSON.stringify(selection ? { ...payload, enabled: action === "enable" } : payload) }); if (this.disposed) return; if (updated.name !== view.name) throw new Error(t("MCP返回名称不匹配")); card.view = updated; this.pending.delete(view.name); this.persist(); await this.changed();
    } catch (error) { if (!this.disposed) this.notice(`${errorText(error)} · ${t("MCP状态未确认，请只读查询")}`, true); } finally { this.busy.delete(view.name); if (!this.disposed) { this.render(card); if (!this.pending.has(view.name)) this.schedule(); } }
  }
  private async readWait(card: Card): Promise<void> { const version = ++this.version; try { const updated = await requestJson<MCPResourceView>(`/v1/mcp/servers/${encodeURIComponent(card.view.name)}/wait?timeout_seconds=1`, { method: "POST" }); if (this.disposed || version !== this.version) return; card.view = updated; this.render(card); this.schedule(); } catch (error) { if (!this.disposed) this.notice(errorText(error), true); } }
}
