import { expect, test, type Locator, type Page } from "@playwright/test";

const headers = { Authorization: "Bearer browser-test-key" };
const tools = ["directory_inspect", "directory_preview", "directory_change", "directory_query", "directory_reconcile", "directory_finalize"];

async function connect(page: Page, topic: string): Promise<void> {
  await page.goto(`/ui/?topic=${encodeURIComponent(topic)}`);
  await page.getByLabel("服务访问密钥").fill("browser-test-key");
  await page.getByRole("button", { name: "连接", exact: true }).click();
  await expect(page.locator("#connection")).toContainText("本地服务已连接");
}

async function agent(page: Page, name: string) {
  const configured = await page.request.put(`/v1/agents/profiles/${name}`, { headers,
    data: { expected_revision: 0, config: { name, description: "目录移动浏览器验证", workspace_key: "edit", tool_names: tools } } });
  expect(configured.ok()).toBeTruthy();
  const view = await (await page.request.post("/v1/agents", { headers, data: { name } })).json();
  await connect(page, view.topic_id);
  const panel = page.locator(`article.agent-card[data-agent-id="${view.agent_id}"] .directory-changes`);
  await panel.getByText("目录事务与历史", { exact: true }).click();
  return { id: view.agent_id as string, topic: view.topic_id as string, panel };
}

async function confirmed(page: Page, panel: Locator, key: string, accept = true): Promise<void> {
  let settled!: () => void;
  const handled = new Promise<void>(resolve => { settled = resolve; });
  page.once("dialog", dialog => {
    const action = accept ? dialog.accept() : dialog.dismiss();
    void action.finally(settled);
  });
  await panel.getByRole("button", { name: key, exact: true }).click();
  await handled;
}

async function control(page: Page, action: string, path = "empty", agent_id?: string) {
  const response = await page.request.post("/_test/directory", { headers, data: { action, path, agent_id } });
  expect(response.ok()).toBeTruthy(); return response.json();
}

test("移动目标审阅、同路径拒绝、取消确认、keep及原inode撤销", async ({ page }) => {
  const source = "directory-move-source", target = "directory-move-target";
  const before = await control(page, "seed", source);
  const { id, panel } = await agent(page, "browser-directory-move");
  try {
    await panel.getByLabel("目录操作", { exact: true }).selectOption("move");
    await panel.getByLabel("目录相对路径", { exact: true }).fill(source);
    await panel.getByRole("button", { name: "检查目录状态", exact: true }).click();
    await panel.getByLabel("目标目录相对路径", { exact: true }).fill(source);
    await expect(panel.getByRole("button", { name: "预览目录事务", exact: true })).toBeDisabled();
    await panel.getByLabel("目标目录相对路径", { exact: true }).fill("../escape");
    await expect(panel.getByRole("button", { name: "预览目录事务", exact: true })).toBeDisabled();
    await panel.getByLabel("目标目录相对路径", { exact: true }).fill(target);
    await page.locator("#locale-select").selectOption("en-US");
    await expect(panel.getByLabel("Target directory relative path", { exact: true })).toHaveValue(target);
    await expect(panel.getByRole("option", { name: "Move empty directory", exact: true })).toBeAttached();
    await page.locator("#locale-select").selectOption("zh-CN");
    await panel.getByRole("button", { name: "预览目录事务", exact: true }).click();
    await expect(panel.locator(".directory-condition")).toContainText(`目标目录相对路径: ${target}`);
    await expect(panel.locator(".directory-condition")).toContainText("目标必须不存在");
    await expect(panel.locator(".directory-detail > strong")).toContainText(`${source} → ${target}`);
    await confirmed(page, panel, "应用目录事务", false);
    expect((await control(page, "counts", "empty", id)).operations).toBe(0);
    let confirmation = "";
    let settled!: () => void;
    const handled = new Promise<void>(resolve => { settled = resolve; });
    page.once("dialog", dialog => { confirmation = dialog.message(); void dialog.accept().finally(settled); });
    await panel.getByRole("button", { name: "应用目录事务", exact: true }).click();
    await handled;
    expect(confirmation).toContain(`${source} → ${target}`);
    await expect(panel.locator(".directory-detail > strong")).toContainText("目录已应用");
    expect((await control(page, "stat", target)).inode).toBe(before.inode);
    expect((await control(page, "stat", source)).found).toBe(false);
    await confirmed(page, panel, "保留目录事务");
    await expect(panel.locator(".directory-detail > strong")).toContainText("目录已保留");
    await confirmed(page, panel, "撤销目录事务");
    await expect(panel.locator(".directory-detail > strong")).toContainText("目录已撤销");
    expect((await control(page, "stat", source)).inode).toBe(before.inode);
    expect((await control(page, "stat", target)).found).toBe(false);
    expect((await control(page, "counts", "empty", id))).toEqual({ proposals: 1, operations: 3 });
    await panel.screenshot({ path: test.info().outputPath("directory-move.png") });
  } finally { await page.request.delete(`/v1/agents/${id}`, { headers }); }
});

test("移动丢响应重载找回双路径、原键零重放及目标纯文本", async ({ page }) => {
  const source = "directory-move-lost-source", target = "<img onerror=alert(1)>";
  const before = await control(page, "seed", source);
  const { id, topic, panel } = await agent(page, "browser-directory-move-lost");
  let previews = 0, actions = 0;
  try {
    await panel.getByLabel("目录操作", { exact: true }).selectOption("move");
    await panel.getByLabel("目录相对路径", { exact: true }).fill(source);
    await panel.getByLabel("目标目录相对路径", { exact: true }).fill(target);
    await panel.getByRole("button", { name: "检查目录状态", exact: true }).click();
    await page.route(`**/v1/agents/${id}/workspace/directory-changes/preview?confirmed=true`, async route => {
      previews += 1; await route.fetch(); await route.abort("failed");
    });
    await panel.getByRole("button", { name: "预览目录事务", exact: true }).click();
    await expect(panel.locator(".directory-pending")).toContainText("目录响应未确认");
    const stored = await page.evaluate(key => sessionStorage.getItem(key), `krim-directory-recovery:${id}`);
    expect(stored).not.toContain(source); expect(stored).not.toContain(target);
    await connect(page, topic);
    let restored = page.locator(`article.agent-card[data-agent-id="${id}"] .directory-changes`);
    await restored.getByText("目录事务与历史", { exact: true }).click();
    await expect(restored.locator(".directory-detail > strong")).toContainText(`${source} → ${target}`);
    await expect(restored.locator(".directory-detail img")).toHaveCount(0);
    expect(previews).toBe(1);
    await page.route(`**/v1/agents/${id}/workspace/directory-changes?confirmed=true`, async route => {
      actions += 1; await route.fetch(); await route.abort("failed");
    });
    await confirmed(page, restored, "应用目录事务");
    await expect(restored.locator(".directory-pending")).toContainText("目录响应未确认");
    await connect(page, topic);
    restored = page.locator(`article.agent-card[data-agent-id="${id}"] .directory-changes`);
    await restored.getByText("目录事务与历史", { exact: true }).click();
    await expect(restored.locator(".directory-detail > strong")).toContainText("目录已应用");
    expect(actions).toBe(1);
    expect((await control(page, "counts", "empty", id))).toEqual({ proposals: 1, operations: 1 });
    await page.unroute(`**/v1/agents/${id}/workspace/directory-changes?confirmed=true`);
    await confirmed(page, restored, "撤销目录事务");
    await expect(restored.locator(".directory-detail > strong")).toContainText("目录已撤销");
    expect((await control(page, "stat", source)).inode).toBe(before.inode);
  } finally { await page.request.delete(`/v1/agents/${id}`, { headers }); }
});

test("移动目标竞争不覆盖、unknown核实只读原动作", async ({ page }) => {
  const source = "directory-move-race-source", target = "directory-move-race-target";
  const before = await control(page, "seed", source);
  const { id, panel } = await agent(page, "browser-directory-move-race");
  try {
    await panel.getByLabel("目录操作", { exact: true }).selectOption("move");
    await panel.getByLabel("目录相对路径", { exact: true }).fill(source);
    await panel.getByLabel("目标目录相对路径", { exact: true }).fill(target);
    await panel.getByRole("button", { name: "检查目录状态", exact: true }).click();
    await panel.getByRole("button", { name: "预览目录事务", exact: true }).click();
    await expect(panel.locator(".directory-detail > strong")).toContainText("目录待确认");
    const external = await control(page, "seed", target);
    await confirmed(page, panel, "应用目录事务");
    await expect(panel.locator(".directory-operation > strong")).toContainText("目录操作已拒绝");
    expect((await control(page, "stat", source)).inode).toBe(before.inode);
    expect((await control(page, "stat", target)).inode).toBe(external.inode);
    await panel.getByLabel("目标目录相对路径", { exact: true }).fill("directory-move-unknown-target");
    await panel.getByRole("button", { name: "预览目录事务", exact: true }).click();
    await expect(panel.locator(".directory-detail > strong")).toContainText("目录待确认");
    await control(page, "fail_save");
    await confirmed(page, panel, "应用目录事务");
    await expect(panel.locator(".directory-operation > strong")).toContainText("目录结果待核实");
    expect((await control(page, "stat", "directory-move-unknown-target")).inode).toBe(before.inode);
    await confirmed(page, panel, "核实目录结果");
    await expect(panel.locator(".directory-operation > strong")).toContainText("目录操作已确认");
    expect((await control(page, "counts", "empty", id))).toEqual({ proposals: 2, operations: 2 });
    await confirmed(page, panel, "撤销目录事务");
    await expect(panel.locator(".directory-detail > strong")).toContainText("目录已撤销");
    expect((await control(page, "stat", source)).inode).toBe(before.inode);
  } finally { await page.request.delete(`/v1/agents/${id}`, { headers }); }
});
