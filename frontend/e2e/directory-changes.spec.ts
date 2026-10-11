import { expect, test, type Locator, type Page } from "@playwright/test";

const headers = { Authorization: "Bearer browser-test-key" };
const tools = ["directory_inspect", "directory_preview", "directory_change", "directory_query", "directory_reconcile", "directory_finalize"];

async function connect(page: Page, topic?: string): Promise<void> {
  await page.goto(topic ? `/ui/?topic=${encodeURIComponent(topic)}` : "/ui/");
  await page.getByLabel("服务访问密钥").fill("browser-test-key");
  await page.getByRole("button", { name: "连接", exact: true }).click();
  await expect(page.locator("#connection")).toContainText("本地服务已连接");
}

async function agent(page: Page, name: string, mode = "agent", selected = tools, workspace = "edit") {
  const configured = await page.request.put(`/v1/agents/profiles/${name}`, { headers,
    data: { expected_revision: 0, config: { name, description: "目录浏览器验证", workspace_key: workspace, tool_names: selected, mode } } });
  expect(configured.ok()).toBeTruthy();
  const view = await (await page.request.post("/v1/agents", { headers, data: { name } })).json();
  await connect(page, view.topic_id);
  const card = page.locator(`article.agent-card[data-agent-id="${view.agent_id}"]`);
  const panel = card.locator(".directory-changes");
  await panel.getByText("目录事务与历史", { exact: true }).click();
  return { id: view.agent_id as string, topic: view.topic_id as string, card, panel };
}

async function confirmed(page: Page, panel: Locator, key: string, accept = true): Promise<void> {
  page.once("dialog", dialog => accept ? dialog.accept() : dialog.dismiss());
  await panel.getByRole("button", { name: key, exact: true }).click();
}

async function control(page: Page, action: string, path = "empty", agent_id?: string) {
  const response = await page.request.post("/_test/directory", { headers, data: { action, path, agent_id } });
  expect(response.ok()).toBeTruthy(); return response.json();
}

test("目录创建、确认取消、keep、undo、清理与关闭后历史", async ({ page }) => {
  const { id, topic, card, panel } = await agent(page, "browser-directory-life");
  try {
    await panel.getByLabel("目录相对路径", { exact: true }).fill("directory-life");
    await panel.getByRole("button", { name: "预览目录事务", exact: true }).click();
    await expect(panel.locator(".directory-detail > strong")).toContainText("目录待确认");
    expect((await control(page, "stat", "directory-life")).found).toBe(false);
    await confirmed(page, panel, "应用目录事务", false);
    expect((await control(page, "counts", "empty", id)).operations).toBe(0);
    await confirmed(page, panel, "应用目录事务");
    await expect(panel.locator(".directory-detail > strong")).toContainText("目录已应用");
    expect((await control(page, "stat", "directory-life")).mode).toBe(0o750);
    await confirmed(page, panel, "保留目录事务");
    await expect(panel.locator(".directory-detail > strong")).toContainText("目录已保留");
    await confirmed(page, panel, "撤销目录事务");
    await expect(panel.locator(".directory-detail > strong")).toContainText("目录已撤销");
    expect((await control(page, "stat", "directory-life")).found).toBe(false);
    await confirmed(page, panel, "清理目录恢复材料");
    await expect(panel.getByRole("button", { name: "清理目录恢复材料", exact: true })).toHaveCount(0);
    await card.getByRole("button", { name: "关闭会话", exact: true }).click();
    await expect(panel.locator(".directory-editor")).toBeHidden();
    await panel.getByRole("button", { name: "刷新目录历史", exact: true }).click();
    await expect(panel.locator(".directory-history li")).toHaveCount(3);
    await connect(page, topic);
    const restored = page.locator(`article.agent-card[data-agent-id="${id}"] .directory-changes`);
    await restored.getByText("目录事务与历史", { exact: true }).click();
    await expect(restored.locator(".directory-detail > strong")).toContainText("目录已撤销");
    await expect(restored.getByRole("button", { name: "应用目录事务", exact: true })).toBeDisabled();
  } finally { await page.request.delete(`/v1/agents/${id}`, { headers }); }
});

test("已有空目录检查、删除、恢复原inode及非空拒绝", async ({ page }) => {
  const before = await control(page, "seed", "directory-existing");
  const { id, panel } = await agent(page, "browser-directory-existing");
  try {
    await panel.getByLabel("目录操作", { exact: true }).selectOption("delete");
    await panel.getByLabel("目录相对路径", { exact: true }).fill("directory-existing");
    await panel.getByRole("button", { name: "检查目录状态", exact: true }).click();
    await expect(panel.getByLabel("原目录指纹", { exact: true })).toHaveValue(/^[0-9a-f]{64}$/);
    await panel.getByRole("button", { name: "预览目录事务", exact: true }).click();
    await expect(panel.locator(".directory-condition")).toContainText("删除空目录");
    await confirmed(page, panel, "应用目录事务");
    await expect(panel.locator(".directory-detail > strong")).toContainText("目录已应用");
    expect((await control(page, "stat", "directory-existing")).found).toBe(false);
    await confirmed(page, panel, "撤销目录事务");
    await expect(panel.locator(".directory-detail > strong")).toContainText("目录已撤销");
    expect((await control(page, "stat", "directory-existing")).inode).toBe(before.inode);
    await control(page, "nonempty", "directory-existing");
    await panel.getByRole("button", { name: "检查目录状态", exact: true }).click();
    await expect(panel.locator(".directory-state")).toContainText("非空目录");
    await panel.getByRole("button", { name: "预览目录事务", exact: true }).click();
    await expect(panel.locator(".directory-status")).toContainText("只允许单项删除空目录");
    expect((await control(page, "counts", "empty", id)).proposals).toBe(1);
    await panel.screenshot({ path: test.info().outputPath("directory-existing.png") });
  } finally { await page.request.delete(`/v1/agents/${id}`, { headers }); }
});

test("预览与动作响应丢失、重载按原键零POST重放和无原文落盘", async ({ page }) => {
  const { id, topic, panel } = await agent(page, "browser-directory-lost");
  let previews = 0, actions = 0;
  try {
    await panel.getByLabel("目录相对路径", { exact: true }).fill("directory-secret-draft");
    await page.route(`**/v1/agents/${id}/workspace/directory-changes/preview?confirmed=true`, async route => {
      previews += 1; await route.fetch(); await route.abort("failed");
    });
    await panel.getByRole("button", { name: "预览目录事务", exact: true }).click();
    await expect(panel.locator(".directory-pending")).toContainText("目录响应未确认");
    const stored = await page.evaluate(key => sessionStorage.getItem(key), `krim-directory-recovery:${id}`);
    expect(stored).not.toContain("directory-secret-draft"); expect(stored).not.toContain("browser-test-key");
    await connect(page, topic);
    let restored = page.locator(`article.agent-card[data-agent-id="${id}"] .directory-changes`);
    await restored.getByText("目录事务与历史", { exact: true }).click();
    await expect(restored.locator(".directory-detail > strong")).toContainText("目录待确认");
    expect(previews).toBe(1);
    await page.route(`**/v1/agents/${id}/workspace/directory-changes?confirmed=true`, async route => {
      actions += 1; await route.fetch(); await route.abort("failed");
    });
    await confirmed(page, restored, "应用目录事务");
    await expect(restored.locator(".directory-pending")).toContainText("目录响应未确认");
    await expect(restored.getByRole("button", { name: "应用目录事务", exact: true })).toBeDisabled();
    await connect(page, topic);
    restored = page.locator(`article.agent-card[data-agent-id="${id}"] .directory-changes`);
    await restored.getByText("目录事务与历史", { exact: true }).click();
    await expect(restored.locator(".directory-detail > strong")).toContainText("目录已应用");
    expect(actions).toBe(1); expect((await control(page, "counts", "empty", id))).toEqual({ proposals: 1, operations: 1 });
  } finally { await page.request.delete(`/v1/agents/${id}`, { headers }); }
});

test("真实回执保存失败、原操作核实与清理", async ({ page }) => {
  const { id, panel } = await agent(page, "browser-directory-unknown");
  try {
    await panel.getByLabel("目录相对路径", { exact: true }).fill("directory-unknown");
    await panel.getByRole("button", { name: "预览目录事务", exact: true }).click();
    await expect(panel.locator(".directory-detail > strong")).toContainText("目录待确认");
    await control(page, "fail_save");
    await confirmed(page, panel, "应用目录事务");
    await expect(panel.locator(".directory-operation > strong")).toContainText("目录结果待核实");
    expect((await control(page, "stat", "directory-unknown")).found).toBe(true);
    await expect(panel.getByRole("button", { name: "应用目录事务", exact: true })).toBeDisabled();
    await confirmed(page, panel, "核实目录结果");
    await expect(panel.locator(".directory-operation > strong")).toContainText("目录操作已确认");
    await confirmed(page, panel, "清理目录恢复材料");
    await expect(panel.getByRole("button", { name: "清理目录恢复材料", exact: true })).toHaveCount(0);
    expect((await control(page, "counts", "empty", id))).toEqual({ proposals: 1, operations: 1 });
  } finally { await page.request.delete(`/v1/agents/${id}`, { headers }); }
});

test("plan/只读/工具子集、拒绝路径与纯文本目录路径", async ({ page }) => {
  const planned = await agent(page, "browser-directory-plan", "plan");
  try {
    await planned.panel.getByLabel("目录相对路径", { exact: true }).fill("directory-plan");
    await planned.panel.getByRole("button", { name: "预览目录事务", exact: true }).click();
    await expect(planned.panel.locator(".directory-detail > strong")).toContainText("目录待确认");
    await expect(planned.panel.getByRole("button", { name: "应用目录事务", exact: true })).toBeDisabled();
    expect((await control(page, "counts", "empty", planned.id)).operations).toBe(0);
  } finally { await page.request.delete(`/v1/agents/${planned.id}`, { headers }); }
  const readonly = await agent(page, "browser-directory-readonly", "agent", ["directory_inspect"], "demo");
  try {
    await readonly.panel.getByLabel("目录相对路径", { exact: true }).fill("src");
    await readonly.panel.getByRole("button", { name: "检查目录状态", exact: true }).click();
    await expect(readonly.panel.locator(".directory-state")).toContainText("非空目录");
    await expect(readonly.panel.getByRole("button", { name: "预览目录事务", exact: true })).toBeDisabled();
  } finally { await page.request.delete(`/v1/agents/${readonly.id}`, { headers }); }
  const { id, panel } = await agent(page, "browser-directory-html");
  try {
    await panel.getByLabel("目录相对路径", { exact: true }).fill("../escape");
    await expect(panel.getByRole("button", { name: "预览目录事务", exact: true })).toBeDisabled();
    const name = "<img onerror=alert(1)>";
    await panel.getByLabel("目录相对路径", { exact: true }).fill(name);
    await panel.getByRole("button", { name: "预览目录事务", exact: true }).click();
    await expect(panel.locator(".directory-detail > strong")).toContainText(name);
    await expect(panel.locator(".directory-detail img")).toHaveCount(0);
    await panel.getByRole("button", { name: "刷新目录历史", exact: true }).click();
    await expect(panel.getByLabel("目录相对路径", { exact: true })).toHaveValue(name);
  } finally { await page.request.delete(`/v1/agents/${id}`, { headers }); }
});

test("启动前断线只能明确复用原预览键和原参数", async ({ page }) => {
  const { id, panel } = await agent(page, "browser-directory-prestart");
  const keys: string[] = [];
  try {
    await panel.getByLabel("目录相对路径", { exact: true }).fill("directory-prestart");
    await page.route(`**/v1/agents/${id}/workspace/directory-changes/preview?confirmed=true`, async route => {
      keys.push(route.request().postDataJSON().request_key);
      if (keys.length === 1) await route.abort("failed"); else await route.continue();
    });
    await panel.getByRole("button", { name: "预览目录事务", exact: true }).click();
    await expect(panel.locator(".directory-pending")).toContainText("目录响应未确认");
    await panel.getByRole("button", { name: "查询原目录请求", exact: true }).click();
    await expect(panel.locator(".directory-status")).toContainText("尚未找到原目录请求");
    await panel.getByLabel("目录相对路径", { exact: true }).fill("changed-directory");
    await panel.getByRole("button", { name: "明确重试原目录请求", exact: true }).click();
    await expect(panel.locator(".directory-status")).toContainText("只能使用原目录参数");
    expect(keys).toHaveLength(1);
    await panel.getByLabel("目录相对路径", { exact: true }).fill("directory-prestart");
    await confirmed(page, panel, "明确重试原目录请求");
    await expect(panel.locator(".directory-detail > strong")).toContainText("目录待确认");
    expect(keys).toHaveLength(2); expect(keys[0]).toBe(keys[1]);
    expect((await control(page, "counts", "empty", id))).toEqual({ proposals: 1, operations: 0 });
  } finally { await page.request.delete(`/v1/agents/${id}`, { headers }); }
});

test("目录列表与原操作历史真实分页，中英文切换保留草稿", async ({ page }) => {
  test.setTimeout(60_000);
  const { id, panel } = await agent(page, "browser-directory-pages");
  try {
    for (let index = 0; index < 23; index++) {
      const response = await page.request.post(`/v1/agents/${id}/workspace/directory-changes/preview`, { headers,
        data: { spec: { path: `directory-page-${index}`, operation: "create", mode: 488 }, request_key: `page-${index}` } });
      expect(response.ok()).toBeTruthy();
    }
    await panel.getByRole("button", { name: "刷新目录历史", exact: true }).click();
    await expect(panel.locator(".directory-list > li")).toHaveCount(20);
    await panel.getByRole("button", { name: "下一页", exact: true }).click();
    await expect(panel.locator(".directory-list > li")).toHaveCount(3);
    await panel.getByRole("button", { name: "上一页", exact: true }).click();
    await expect(panel.locator(".directory-list > li")).toHaveCount(20);
    const proposalId = await panel.locator(".directory-detail").getAttribute("data-proposal-id");
    const proposal = await (await page.request.get(`/v1/agents/${id}/workspace/directory-changes/${proposalId}`, { headers })).json();
    await control(page, "seed", proposal.path);
    for (let index = 0; index < 22; index++) {
      const current = await (await page.request.get(`/v1/agents/${id}/workspace/directory-changes/${proposalId}`, { headers })).json();
      const response = await page.request.post(`/v1/agents/${id}/workspace/directory-changes?confirmed=true`, { headers,
        data: { proposal_id: proposalId, action: "apply", expected_revision: current.revision, request_key: `rejected-${index}` } });
      expect(response.ok()).toBeTruthy(); expect((await response.json()).status).toBe("rejected");
    }
    await panel.getByRole("button", { name: "刷新目录历史", exact: true }).click();
    await expect(panel.locator(".directory-history > li")).toHaveCount(20);
    await panel.getByRole("button", { name: "较晚目录操作", exact: true }).click();
    await expect(panel.locator(".directory-history > li")).toHaveCount(2);
    await panel.getByRole("button", { name: "较早目录操作", exact: true }).click();
    await expect(panel.locator(".directory-history > li")).toHaveCount(20);
    await panel.getByLabel("目录相对路径", { exact: true }).fill("preserved-directory-draft");
    await page.locator("#locale-select").selectOption("en-US");
    await expect(panel.getByLabel("Directory relative path", { exact: true })).toHaveValue("preserved-directory-draft");
    await expect(panel.getByRole("button", { name: "Refresh directory history", exact: true })).toBeVisible();
    await panel.screenshot({ path: test.info().outputPath("directory-pages-en.png") });
  } finally { await page.request.delete(`/v1/agents/${id}`, { headers }); }
});
