/** 技能目录、发送选择和按需预览；数据与 ID 由 Python 复核。 */
import { requestJson, errorText, type SkillCatalog, type SkillReadResult } from "./api.js";
import type { HostJson } from "./host.js";
import { t } from "./i18n.js";
const node = (id: string): HTMLElement => document.getElementById(id)!;
const input = (id: string): HTMLInputElement | HTMLTextAreaElement => node(id) as HTMLInputElement;

export class SkillsUI {
  private topicId: string | null = null;
  private catalog: SkillCatalog = { skills: [] };
  private selected = new Set<string>();
  private busy = false;
  private resources: Array<{ path: string; content: string }> = [];
  private loaded: Array<{ name: string; resource: string | null }> = [];
  constructor(private readonly notice: (message: string, error?: boolean) => void) {
    node("skill-save").addEventListener("click", () => void this.save());
    node("skill-clear").addEventListener("click", () => void this.change("skill/remove", { name: null }));
    node("skill-resource-add").addEventListener("click", () => {
      if (this.busy) return;
      const path = input("skill-resource-path").value, content = input("skill-resource-content").value;
      if (!path || !content) { this.notice("请填写资源名称和内容", true); return; }
      this.resources = [...this.resources.filter((item) => item.path !== path), { path, content }];
      input("skill-resource-path").value = ""; input("skill-resource-content").value = ""; this.render();
    });
    this.render();
  }
  get selectedNames(): string[] { return [...this.selected].sort(); }
  clearSelection(): void { this.selected.clear(); this.loaded = []; this.render(); }
  refreshLocale(): void { this.render(); }
  setBusy(value: boolean): void { this.busy = value; this.render(); }
  async selectTopic(id: string): Promise<void> {
    if (this.topicId !== id) { this.selected.clear(); this.catalog = { skills: [] }; this.loaded = []; node("skill-preview").textContent = ""; }
    this.topicId = id; await this.load();
  }
  loadedSkill(name: string, resource: string | null): void {
    if (!this.loaded.some((item) => item.name === name && item.resource === resource)) this.loaded.push({ name, resource });
    this.render();
  }
  private async load(): Promise<void> {
    const id = this.topicId; if (!id) return;
    const value = await requestJson<SkillCatalog>(`/v1/topics/${encodeURIComponent(id)}/skills`);
    if (id !== this.topicId) return;
    this.catalog = value;
    this.selected = new Set([...this.selected].filter((name) => value.skills.some((skill) => skill.name === name)));
    this.render();
  }
  private async save(): Promise<void> {
    const skill = { name: input("skill-name").value, description: input("skill-description").value, content: input("skill-content").value, loading: (node("skill-loading") as HTMLSelectElement).value, license: input("skill-license").value || null, compatibility: input("skill-compatibility").value || null, resources: this.resources };
    if (await this.change("skill/inject", { skills: [skill] })) {
      for (const id of ["skill-name", "skill-description", "skill-content", "skill-license", "skill-compatibility"]) input(id).value = "";
      this.resources = []; this.render();
    }
  }
  private async change(channel: string, payload: HostJson): Promise<boolean> {
    if (!this.topicId || this.busy) return false;
    try { await this.bridge(channel, { topic_id: this.topicId, ...(payload as Record<string, HostJson>) }); return true; }
    catch (error) { this.notice(errorText(error), true); return false; }
  }
  async bridge(channel: string, data: HostJson): Promise<HostJson> {
    if (!data || typeof data !== "object" || Array.isArray(data) || typeof data.topic_id !== "string") throw new Error(t("需要话题 ID"));
    const { topic_id: id, ...payload } = data;
    const base = `/v1/topics/${encodeURIComponent(id)}/skills`;
    let result: HostJson;
    if (channel === "skill/list") result = await requestJson<HostJson>(base);
    else if (channel === "skill/read") {
      if (typeof payload.name !== "string" || (payload.resource !== null && payload.resource !== undefined && typeof payload.resource !== "string")) throw new Error(t("需要技能名称"));
      result = await requestJson<HostJson>(base + "/" + encodeURIComponent(payload.name) + (typeof payload.resource === "string" ? "?resource=" + encodeURIComponent(payload.resource) : ""));
    }
    else if (channel === "skill/inject") result = await requestJson<HostJson>(base, { method: "POST", body: JSON.stringify(payload) });
    else if (channel === "skill/remove") {
      const name = payload.name;
      if (name !== null && typeof name !== "string") throw new Error(t("需要技能名称"));
      result = await requestJson<HostJson>(base + (typeof name === "string" ? "/" + encodeURIComponent(name) : ""), { method: "DELETE" });
    } else throw new Error(t("技能信道无效"));
    if (id === this.topicId && channel !== "skill/list" && channel !== "skill/read") { node("skill-preview").textContent = ""; await this.load(); }
    return result;
  }
  private async preview(name: string, resource: string | null): Promise<void> {
    const id = this.topicId; if (!id) return;
    const digest = this.catalog.skills.find((skill) => skill.name === name)?.digest;
    try {
      const result = await requestJson<SkillReadResult>(`/v1/topics/${encodeURIComponent(id)}/skills/${encodeURIComponent(name)}` + (resource ? `?resource=${encodeURIComponent(resource)}` : ""));
      if (id !== this.topicId || this.catalog.skills.find((skill) => skill.name === name)?.digest !== digest) return;
      node("skill-preview").textContent = result.content;
    } catch (error) { this.notice(errorText(error), true); }
  }
  private render(): void {
    const root = node("skill-list"); root.replaceChildren();
    for (const skill of this.catalog.skills) {
      const card = document.createElement("div"); card.className = "skill-card"; card.dataset.skillName = skill.name;
      const title = document.createElement("strong"); title.textContent = skill.name;
      const description = document.createElement("p"); description.textContent = skill.description;
      const choice = document.createElement("label"), check = document.createElement("input"); check.type = "checkbox"; check.checked = this.selected.has(skill.name); check.disabled = this.busy;
      check.setAttribute("aria-label", t("本次使用技能 {name}", { name: skill.name }));
      check.onchange = () => { if (check.checked && this.selected.size >= 8) { check.checked = false; this.notice("一次最多选择八个技能", true); return; } if (check.checked) this.selected.add(skill.name); else this.selected.delete(skill.name); this.render(); };
      choice.append(check, document.createTextNode(t("本次使用")));
      const actions = document.createElement("div"); actions.className = "actions";
      for (const [label, resource] of [[t("预览技能正文"), null], ...skill.resource_paths.map((path) => [path, path])] as Array<[string, string | null]>) {
        const button = document.createElement("button"); button.type = "button"; button.className = "secondary"; button.textContent = label; button.onclick = () => void this.preview(skill.name, resource); actions.append(button);
      }
      const remove = document.createElement("button"); remove.type = "button"; remove.className = "secondary"; remove.textContent = t("删除技能"); remove.disabled = this.busy; remove.onclick = () => void this.change("skill/remove", { name: skill.name }); actions.append(remove);
      card.append(title, description, choice, actions); root.append(card);
    }
    node("skill-selection").textContent = this.selected.size ? t("已选技能：{names}", { names: this.selectedNames.join(", ") }) : t("未选择发送技能");
    node("skill-selection").hidden = this.selected.size === 0;
    const resources = node("skill-resource-draft"); resources.replaceChildren();
    for (const item of this.resources) {
      const chip = document.createElement("button"); chip.type = "button"; chip.className = "secondary"; chip.textContent = t("移除资源 {path}", { path: item.path }); chip.disabled = this.busy;
      chip.onclick = () => { this.resources = this.resources.filter((value) => value !== item); this.render(); }; resources.append(chip);
    }
    node("skill-load-state").textContent = this.loaded.map((item) => t("已加载技能：{name}{resource}", { name: item.name, resource: item.resource ? " · " + item.resource : "" })).join("\n");
    for (const id of ["skill-save", "skill-clear", "skill-resource-add"]) (node(id) as HTMLButtonElement).disabled = this.busy || !this.topicId;
  }
}
