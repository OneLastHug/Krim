import { expect, test, type Page, type Locator } from "@playwright/test";
import { destroyAgent, ensureChineseUi } from "./session-fixtures.js";
const headers = { Authorization: "Bearer browser-test-key" };
async function actor(page: Page, name: string, options: Record<string, unknown> = {}) { const response = await page.request.post("/_test/missing-rollback-agent", { headers, data: { name, ...options } }); expect(response.ok(), await response.text()).toBeTruthy(); return response.json(); }
async function connect(page: Page, value: any) { await page.goto(`/ui/?topic=${value.agent.topic_id}`); await page.getByLabel("服务访问密钥", { exact: true }).fill("browser-test-key"); await page.getByRole("button", { name: "连接", exact: true }).click(); await ensureChineseUi(page); const panel = page.locator(`.agent-card[data-agent-id="${value.agent.agent_id}"] .missing-directory-rollbacks`); await panel.getByText("缺失基线回退与历史", { exact: true }).click(); await expect(panel.getByRole("button", { name: "刷新缺失回退历史", exact: true })).toBeEnabled(); return panel; }
async function source(panel: Locator, value: any) { await panel.getByLabel("原缺失恢复计划标识", { exact: true }).fill(value.source.plan_id); await panel.getByLabel("当前整树保留相对路径", { exact: true }).fill("preserved"); await panel.getByRole("button", { name: "读取原缺失恢复与当前版本", exact: true }).click(); await expect(panel.locator(".missing-rollback-source")).toContainText(value.source.plan_id); }
async function preview(panel: Locator, value: any) { await source(panel, value); await panel.getByRole("button", { name: "预览缺失基线回退", exact: true }).click(); await expect(panel.locator(".missing-rollback-detail")).toHaveAttribute("data-rollback-state", "awaiting_approval"); }
async function proof(page: Page, id: string) { const response = await page.request.get(`/_test/missing-rollback-proof/${id}`, { headers }); expect(response.ok(), await response.text()).toBeTruthy(); return response.json(); }

for (const fault of ["json", "version", "scope", "private", "oversize", "read", "silent-write"]) test(`可靠存储${fault}拒绝新写且原目录不变`, async ({ page }) => {
  const value = await actor(page, "rb-storage-" + fault);
  try {
    await page.addInitScript(({ id, fault, workspace }) => {
      const key = `krim-missing-directory-rollback-recovery:${id}`;
      if (fault === "read") { const get = Storage.prototype.getItem; Storage.prototype.getItem = function(name) { if (name === key) throw new Error("synthetic"); return get.call(this, name); }; }
      else if (fault === "silent-write") { const set = Storage.prototype.setItem; Storage.prototype.setItem = function(name, value) { if (name !== key) set.call(this, name, value); }; }
      else { const record: any = { version: 1, scope_id: `session:${id}`, workspace_key: workspace.key, resource_fingerprint: workspace.fingerprint, selected_id: null, pending: null }; if (fault === "version") record.version = 2; if (fault === "scope") record.scope_id = "session:other"; if (fault === "private") record.token = "private"; localStorage.setItem(key, fault === "json" ? "{bad" : fault === "oversize" ? "x".repeat(24577) : JSON.stringify(record)); }
    }, { id: value.agent.session_id, fault, workspace: value.agent.session.permissions.workspace });
    const panel = await connect(page, value); const writes: string[] = []; page.on("request", request => { if (request.method() === "POST" && request.url().includes("/missing-directory-rollbacks")) writes.push(request.url()); });
    await source(panel, value); if (fault === "silent-write") await panel.getByRole("button", { name: "预览缺失基线回退", exact: true }).click();
    await expect(panel.locator(".missing-rollback-pending")).toContainText("记录不可用"); await expect(panel.getByRole("button", { name: "预览缺失基线回退", exact: true })).toBeDisabled();
    expect(writes).toEqual([]); expect(await proof(page, value.agent.agent_id)).toMatchObject({ tree: true, preserved: false, applies: 0, attempts: 0 });
  } finally { await destroyAgent(page, value.agent.agent_id); }
});

for (const option of ["readonly", "inspect"]) test(`原冻结${option}限制不因面板暗授检查或写入`, async ({ page }) => {
  const value = await actor(page, "rb-permission-" + option, option === "readonly" ? { readonly: true } : { inspect: false });
  try { const panel = await connect(page, value); await panel.getByLabel("原缺失恢复计划标识", { exact: true }).fill(value.source?.plan_id ?? "missing"); await expect(panel.getByRole("button", { name: "读取原缺失恢复与当前版本", exact: true })).toBeDisabled(); await expect(panel.getByRole("button", { name: "预览缺失基线回退", exact: true })).toBeDisabled(); expect(await proof(page, value.agent.agent_id)).toMatchObject({ tree: true, preserved: false, applies: 0, attempts: 0 }); } finally { await destroyAgent(page, value.agent.agent_id); }
});

test("原deny不被confirm覆盖，原目录和操作数保持", async ({ page }) => {
  const value = await actor(page, "rb-deny", { deny_write: true });
  try { const panel = await connect(page, value); await preview(panel, value); page.once("dialog", dialog => void dialog.accept()); const denied = page.waitForResponse(response => response.url().includes("/missing-directory-rollbacks/change") && response.status() === 403); await panel.getByRole("button", { name: "完整保留并回到缺失", exact: true }).click(); await denied; await expect(panel).toHaveAttribute("aria-busy", "false"); expect(await proof(page, value.agent.agent_id)).toMatchObject({ tree: true, preserved: false, applies: 0, attempts: 0 }); } finally { await destroyAgent(page, value.agent.agent_id); }
});

test("POST候选后的坏GET不被标成POST拒绝，原意图刷新后只GET恢复", async ({ page }) => {
  const value = await actor(page, "rb-confirm-get");
  try {
    let panel = await connect(page, value); await preview(panel, value); let posts = 0, blocked = true;
    page.on("request", request => { if (request.method() === "POST" && request.url().includes("/missing-directory-rollbacks/change")) posts++; });
    await page.route("**/missing-directory-rollbacks/query?*", async route => { const url = new URL(route.request().url()); if (blocked && url.searchParams.get("kind") === "operation_by_key") await route.abort("failed"); else await route.continue(); });
    page.once("dialog", dialog => void dialog.accept()); await panel.getByRole("button", { name: "完整保留并回到缺失", exact: true }).click(); await expect(panel).toHaveAttribute("aria-busy", "false"); await expect(panel.locator(".missing-rollback-pending")).toContainText("尚未确认");
    const saved = await page.evaluate(id => JSON.parse(localStorage.getItem(`krim-missing-directory-rollback-recovery:${id}`)!), value.agent.session_id); expect(saved.pending.kind).toBe("change"); expect(saved.pending.rejected).toBe(false);
    blocked = false; panel = await connect(page, value); await expect(panel.locator(".missing-rollback-detail")).toHaveAttribute("data-rollback-state", "preserved"); expect(posts).toBe(1); expect(await proof(page, value.agent.agent_id)).toMatchObject({ applies: 1, attempts: 1 });
  } finally { await destroyAgent(page, value.agent.agent_id); }
});

test("真实提案短过滤页为空仍按原next_after前进和返回", async ({ page }) => {
  const value = await actor(page, "rb-empty-cursor");
  try {
    const base = `/v1/agents/${value.agent.agent_id}/workspace`;
    const state = await page.request.get(base + "/tree-state?path=tree", { headers }); const inspected = await state.json();
    for (let index = 0; index < 6; index++) { const response = await page.request.post(base + "/missing-directory-rollbacks/preview", { headers, data: { plan_id: value.source.plan_id, preserve_path: `preserved-${index}`, expected_fingerprint: inspected.fingerprint, request_key: `cursor-${index}` } }); expect(response.ok(), await response.text()).toBeTruthy(); }
    let cursor = ""; const seen: string[] = [];
    await page.route("**/missing-directory-rollbacks/query?*", async route => { const url = new URL(route.request().url()); if (url.searchParams.get("kind") !== "proposals") { await route.continue(); return; } const response = await route.fetch(); const body = await response.json(); seen.push(url.searchParams.get("after") ?? ""); if (!url.searchParams.has("after")) { cursor = body.proposals.next_after; expect(cursor).toBeTruthy(); body.proposals.items = []; } await route.fulfill({ response, json: body }); });
    const panel = await connect(page, value); await expect(panel.locator(".missing-rollback-list li")).toHaveCount(0); await expect(panel.getByRole("button", { name: "下一页缺失回退提案", exact: true })).toBeEnabled(); await panel.getByRole("button", { name: "下一页缺失回退提案", exact: true }).click(); await expect(panel.locator(".missing-rollback-list li")).toHaveCount(2); await panel.getByRole("button", { name: "上一页缺失回退提案", exact: true }).click(); await expect(panel.locator(".missing-rollback-list li")).toHaveCount(0); expect(seen).toEqual(["", cursor, ""]); expect(await proof(page, value.agent.agent_id)).toMatchObject({ applies: 0, attempts: 0 });
  } finally { await destroyAgent(page, value.agent.agent_id); }
});

test("原模型结果的浏览器历史不再调用Provider或Writer", async ({ page }) => {
  const value = await actor(page, "rb-model", { model: true });
  try { const before = await proof(page, value.agent.agent_id); expect(before).toMatchObject({ provider_calls: 4, applies: 1, attempts: 1 }); const panel = await connect(page, value); await panel.locator(".missing-rollback-list li button").click(); await expect(panel.locator(".missing-rollback-detail")).toHaveAttribute("data-rollback-state", "preserved"); await expect(panel.locator(".missing-rollback-detail")).toContainText(value.source.operation_id); expect(await proof(page, value.agent.agent_id)).toEqual(before); } finally { await destroyAgent(page, value.agent.agent_id); }
});

test("原finished面板桥只读取同Scope来源和当前版本", async ({ page }) => {
  const value = await actor(page, "rb-bridge");
  try { const panel = await connect(page, value); await page.locator(`.agent-card[data-agent-id="${value.agent.agent_id}"]`).getByRole("button", { name: "查看目录检查点", exact: true }).click(); await page.locator("#missing-directory-restore-query").fill(value.source.plan_id); await page.locator("#missing-directory-restore-by-id").click(); await expect(page.locator("#missing-directory-restore-detail")).toHaveAttribute("data-restore-phase", "finished"); await page.locator("#missing-directory-restore-detail").getByRole("button", { name: "审阅完整保留并回到缺失", exact: true }).click(); await expect(panel.locator(".missing-rollback-source")).toContainText(value.source.operation_id); expect(await proof(page, value.agent.agent_id)).toMatchObject({ applies: 0, attempts: 0 }); } finally { await destroyAgent(page, value.agent.agent_id); }
});

test("英文420像素可读，返回路径按纯文本显示", async ({ page }) => {
  const value = await actor(page, "rb-english");
  try { await page.setViewportSize({ width: 420, height: 900 }); const panel = await connect(page, value); await source(panel, value); const marker = "<img src=x onerror=window.rollbackXss=1>"; await panel.getByLabel("当前整树保留相对路径", { exact: true }).fill(marker); await panel.getByRole("button", { name: "预览缺失基线回退", exact: true }).click(); await expect(panel.locator(".missing-rollback-detail")).toHaveAttribute("data-rollback-state", "awaiting_approval"); await expect(panel.locator(".missing-rollback-detail")).toContainText(marker); await expect(panel.locator("img")).toHaveCount(0); expect(await page.evaluate(() => (window as any).rollbackXss)).toBeUndefined(); await page.locator("#locale-select").selectOption("en-US"); await expect(panel.getByText("Missing baseline rollback and history", { exact: true })).toBeVisible(); await expect(panel.locator(".missing-rollback-detail")).toContainText("Current approved fingerprint"); expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true); expect(await proof(page, value.agent.agent_id)).toMatchObject({ applies: 0, attempts: 0 }); await test.info().attach("english-420", { body: await panel.screenshot(), contentType: "image/png" }); } finally { await destroyAgent(page, value.agent.agent_id); }
});

test("首次workspace回程须匹配原Agent冻结资源，坏身份不能建立新锚点", async ({ page }) => {
  const value = await actor(page, "rb-grant-anchor");
  try { await page.route(`**/v1/agents/${value.agent.agent_id}/workspace`, async route => { const response = await route.fetch(); const body = await response.json(); body.fingerprint = "f".repeat(64); await route.fulfill({ response, json: body }); }); await page.goto(`/ui/?topic=${value.agent.topic_id}`); await page.getByLabel("服务访问密钥", { exact: true }).fill("browser-test-key"); await page.getByRole("button", { name: "连接", exact: true }).click(); await ensureChineseUi(page); const panel = page.locator(`.agent-card[data-agent-id="${value.agent.agent_id}"] .missing-directory-rollbacks`); await panel.getByText("缺失基线回退与历史", { exact: true }).click(); await expect(panel.locator('[role="status"]')).toContainText("回程与原事实不一致"); await expect(panel.getByRole("button", { name: "预览缺失基线回退", exact: true })).toBeDisabled(); expect(await proof(page, value.agent.agent_id)).toMatchObject({ applies: 0, attempts: 0 }); } finally { await destroyAgent(page, value.agent.agent_id); }
});

test("会话关闭后写按钮停用，原提案仍只读可查", async ({ page }) => {
  const value = await actor(page, "rb-closed");
  try { const panel = await connect(page, value); await preview(panel, value); const response = await page.request.post(`/v1/agents/${value.agent.agent_id}/session/close`, { headers, data: { expected_revision: value.agent.session.revision } }); expect(response.ok(), await response.text()).toBeTruthy(); await panel.getByRole("button", { name: "刷新缺失回退历史", exact: true }).click(); await expect(panel.getByRole("button", { name: "完整保留并回到缺失", exact: true })).toBeDisabled(); await expect(panel.locator(".missing-rollback-detail")).toHaveAttribute("data-rollback-state", "awaiting_approval"); expect(await proof(page, value.agent.agent_id)).toMatchObject({ applies: 0, attempts: 0 }); } finally { await destroyAgent(page, value.agent.agent_id); }
});
