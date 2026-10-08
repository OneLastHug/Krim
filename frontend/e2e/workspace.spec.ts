import { expect, test } from "@playwright/test";

const headers = { Authorization: "Bearer browser-test-key" };

async function connect(page: import("@playwright/test").Page): Promise<void> {
  await page.goto("/ui/");
  await page.getByLabel("服务访问密钥").fill("browser-test-key");
  await page.getByRole("button", { name: "连接", exact: true }).click();
  await expect(page.locator("#connection")).toContainText("本地服务已连接");
}

test("工作区目录、文件、检索、敏感拒绝与会话关闭在浏览器实际生效", async ({ page }) => {
  const name = "browser-reader";
  const saved = await page.request.put(`/v1/agents/profiles/${name}`, { headers, data: { expected_revision: 0, config: { name, description: "浏览器读取", workspace_key: "demo", tool_names: ["workspace_read", "workspace_list", "workspace_search"] } } });
  expect(saved.ok()).toBeTruthy();
  const agent = await (await page.request.post("/v1/agents", { headers, data: { name } })).json();
  try {
    await connect(page);
    const card = page.locator(`article.agent-card[data-agent-id="${agent.agent_id}"]`);
    await card.getByText("只读工作区", { exact: true }).click();
    await card.getByRole("button", { name: "列出目录", exact: true }).click();
    await expect(card.locator(".agent-workspace li")).toHaveCount(2);
    await card.getByRole("button", { name: "src/", exact: true }).click();
    await card.getByRole("button", { name: "src/main.py", exact: true }).click();
    await expect(card.locator(".workspace-content")).toContainText("2  needle = 42");
    await expect(card.locator(".agent-workspace [role=status]")).toContainText("SHA-256");
    await card.getByLabel("工作区相对路径").fill(".");
    await card.getByLabel("工作区搜索文本").fill("needle");
    await card.getByRole("button", { name: "搜索文本", exact: true }).click();
    await expect(card.getByRole("button", { name: "src/main.py:2", exact: true })).toBeVisible();
    await card.getByLabel("工作区相对路径").fill(".env");
    await card.getByRole("button", { name: "读取文件", exact: true }).click();
    await expect(card.locator(".agent-workspace [role=status]")).toContainText("不可读取");
    await card.getByLabel("工作区相对路径").fill("html.txt");
    await card.getByRole("button", { name: "读取文件", exact: true }).click();
    await expect(card.locator(".workspace-content")).toContainText("<img");
    expect(await page.evaluate(() => (window as unknown as { workspaceExecuted?: number }).workspaceExecuted)).toBeUndefined();
    await card.getByRole("button", { name: "关闭会话", exact: true }).click();
    await expect(card.getByRole("button", { name: "读取文件", exact: true })).toBeDisabled();
    await expect(card.getByLabel("工作区相对路径")).toBeDisabled();
    await connect(page);
    await page.locator(`article.agent-card[data-agent-id="${agent.agent_id}"]`).getByText("只读工作区", { exact: true }).click();
    await expect(page.locator(`article.agent-card[data-agent-id="${agent.agent_id}"]`).getByRole("button", { name: "搜索文本", exact: true })).toBeDisabled();
  } finally { await page.request.delete(`/v1/agents/${agent.agent_id}`, { headers }); }
});

test("配置界面保存工作区标识并保留其他配置", async ({ page }) => {
  await connect(page);
  await page.getByText("Agent 配置", { exact: true }).click();
  await page.getByLabel("配置名称", { exact: true }).fill("ui-reader");
  await page.getByLabel("任务说明", { exact: true }).fill("工作区只读");
  await page.getByLabel("允许工具（逗号分隔）", { exact: true }).fill("workspace_read");
  await page.getByLabel("工作区资源（留空不授权）", { exact: true }).fill("demo");
  await page.getByRole("button", { name: "保存 Agent 配置", exact: true }).click();
  await expect(page.locator("#notice")).toContainText("配置已保存");
  const profile = await (await page.request.get("/v1/agents/profiles/ui-reader", { headers })).json();
  expect(profile.config.workspace_key).toBe("demo");
  await page.getByLabel("工作区资源（留空不授权）", { exact: true }).fill("");
  await page.getByRole("button", { name: "读取 Agent 配置", exact: true }).click();
  await expect(page.getByLabel("工作区资源（留空不授权）", { exact: true })).toHaveValue("demo");
});
