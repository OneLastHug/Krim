import { expect, test, type Page, type Locator } from "@playwright/test";
import { destroyAgent, ensureChineseUi } from "./session-fixtures.js";
const headers = { Authorization: "Bearer browser-test-key" };
async function actor(page: Page, name: string, options: Record<string, unknown> = {}) { const response = await page.request.post("/_test/missing-rollback-agent", { headers, data: { name, ...options } }); expect(response.ok(), await response.text()).toBeTruthy(); return response.json(); }
async function connect(page: Page, value: any) { await page.goto(`/ui/?topic=${value.agent.topic_id}`); await page.getByLabel("服务访问密钥", { exact: true }).fill("browser-test-key"); await page.getByRole("button", { name: "连接", exact: true }).click(); await ensureChineseUi(page); const panel = page.locator(`.agent-card[data-agent-id="${value.agent.agent_id}"] .missing-directory-rollbacks`); await panel.getByText("缺失基线回退与历史", { exact: true }).click(); await expect(panel.getByRole("button", { name: "刷新缺失回退历史", exact: true })).toBeEnabled(); return panel; }
async function preview(panel: Locator, source: any) { await panel.getByLabel("原缺失恢复计划标识", { exact: true }).fill(source.plan_id); await panel.getByLabel("当前整树保留相对路径", { exact: true }).fill("preserved"); await panel.getByRole("button", { name: "读取原缺失恢复与当前版本", exact: true }).click(); await expect(panel.locator(".missing-rollback-source")).toContainText("当前批准指纹"); await panel.getByRole("button", { name: "预览缺失基线回退", exact: true }).click(); await expect(panel.locator(".missing-rollback-detail")).toHaveAttribute("data-rollback-state", "awaiting_approval"); }
async function change(page: Page, panel: Locator, name: string, state: string) { page.once("dialog", dialog => void dialog.accept()); await panel.getByRole("button", { name, exact: true }).click(); await expect(panel.locator(".missing-rollback-detail")).toHaveAttribute("data-rollback-state", state); }
async function proof(page: Page, id: string) { const response = await page.request.get(`/_test/missing-rollback-proof/${id}`, { headers }); expect(response.ok(), await response.text()).toBeTruthy(); return response.json(); }
async function control(page: Page, id: string, action: string) { const response = await page.request.post("/_test/missing-rollback-control", { headers, data: { agent_id: id, action } }); expect(response.ok(), await response.text()).toBeTruthy(); }

test("真实当前版本完整保留-keep-反向-刷新同原来源和字节且0重放", async ({ page }) => {
  const value = await actor(page, "rb-complete");
  try {
    let panel = await connect(page, value); await preview(panel, value.source);
    await expect(panel.locator(".missing-rollback-detail")).toContainText(value.source.plan_id); await expect(panel.locator(".missing-rollback-detail")).toContainText("absent");
    const source = await panel.locator(".missing-rollback-source").textContent(); expect(source).toContain(value.source.receipt.after_fingerprint);
    await change(page, panel, "完整保留并回到缺失", "preserved");
    expect(await proof(page, value.agent.agent_id)).toMatchObject({ tree: false, preserved: true, binary_hex: "75736572006d6f646966696564ff", extra: "preserve me", mode: 0o640, applies: 1, attempts: 1 });
    await change(page, panel, "确认保留当前整树", "kept"); await change(page, panel, "还原保留的完整树", "restored");
    const before = await proof(page, value.agent.agent_id); expect(before).toMatchObject({ tree: true, preserved: false, applies: 2, attempts: 3 });
    const writes: string[] = []; page.on("request", request => { if (request.method() === "POST" && request.url().includes("/missing-directory-rollbacks")) writes.push(request.url()); });
    panel = await connect(page, value); await expect(panel.locator(".missing-rollback-detail")).toHaveAttribute("data-rollback-state", "restored"); await expect(panel.locator(".missing-rollback-history li")).toHaveCount(3);
    expect(writes).toEqual([]); expect(await proof(page, value.agent.agent_id)).toEqual(before);
  } finally { await destroyAgent(page, value.agent.agent_id); }
});

test("原效果回程unknown只核实同operation，原树不再次移动", async ({ page }) => {
  const value = await actor(page, "rb-unknown");
  try {
    const panel = await connect(page, value); await preview(panel, value.source); await control(page, value.agent.agent_id, "unknown"); await change(page, panel, "完整保留并回到缺失", "uncertain");
    const original = await panel.locator(".missing-rollback-receipt").textContent();
    await change(page, panel, "核实原缺失回退结果", "preserved"); expect(await panel.locator(".missing-rollback-receipt").textContent()).toContain(original!.split(" · ")[0]!);
    expect(await proof(page, value.agent.agent_id)).toMatchObject({ applies: 1, attempts: 1, tree: false, preserved: true });
  } finally { await destroyAgent(page, value.agent.agent_id); }
});

test("POST回程丢失后刷新只原键GET确认，不再POST", async ({ page }) => {
  const value = await actor(page, "rb-lost");
  try {
    let panel = await connect(page, value); await preview(panel, value.source); let posts = 0;
    await page.route("**/missing-directory-rollbacks/change?*", async route => { posts++; await route.fetch(); await route.abort("failed"); });
    await change(page, panel, "完整保留并回到缺失", "preserved");
    panel = await connect(page, value); await expect(panel.locator(".missing-rollback-detail")).toHaveAttribute("data-rollback-state", "preserved");
    expect(posts).toBe(1); expect(await proof(page, value.agent.agent_id)).toMatchObject({ applies: 1, attempts: 1 });
  } finally { await destroyAgent(page, value.agent.agent_id); }
});

test("外部取消必须等原5秒物理线程并保留同动作键", async ({ page }) => {
  const value = await actor(page, "rb-cancel-thread");
  try {
    const panel = await connect(page, value); await preview(panel, value.source); await control(page, value.agent.agent_id, "block");
    page.once("dialog", dialog => void dialog.accept()); await panel.getByRole("button", { name: "完整保留并回到缺失", exact: true }).click();
    await expect.poll(async () => (await proof(page, value.agent.agent_id)).blocked).toBe(true);
    page.once("dialog", dialog => void dialog.accept()); await panel.getByRole("button", { name: "取消进行中的缺失回退", exact: true }).click();
    await control(page, value.agent.agent_id, "release"); await expect(panel.locator(".missing-rollback-detail")).toHaveAttribute("data-rollback-state", "cancelled");
    expect(await proof(page, value.agent.agent_id)).toMatchObject({ tree: true, preserved: false, applies: 1, attempts: 1 });
  } finally { await destroyAgent(page, value.agent.agent_id); }
});
