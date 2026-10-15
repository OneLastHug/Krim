import { ensureChineseUi } from "./session-fixtures.js";
import { expect, test } from "@playwright/test";

const headers = { Authorization: "Bearer browser-test-key" };
async function connect(page: import("@playwright/test").Page): Promise<void> {
  await page.goto("/ui/");
  await page.getByLabel("服务访问密钥").fill("browser-test-key");
  await page.getByRole("button", { name: "连接", exact: true }).click();
  await ensureChineseUi(page);
}

test("持久任务在刷新后保留，各Agent隔离且关闭后只读", async ({ page }) => {
  const first = await (await page.request.post("/v1/agents", { headers, data: { name: "default", persistent: true } })).json();
  const second = await (await page.request.post("/v1/agents", { headers, data: { name: "default" } })).json();
  try {
    await connect(page);
    const card = page.locator(`article.agent-card[data-agent-id="${first.agent_id}"]`), other = page.locator(`article.agent-card[data-agent-id="${second.agent_id}"]`);
    await card.getByText("计划与待办", { exact: true }).click();
    await card.getByLabel("新增任务内容").fill("完成任务模块");
    await card.getByRole("button", { name: "新增条目", exact: true }).click();
    await expect(card.locator(".agent-tasks li")).toContainText("完成任务模块");
    await other.getByText("计划与待办", { exact: true }).click();
    await expect(other.locator(".agent-tasks li")).toHaveCount(0);
    await card.getByRole("button", { name: "标记完成", exact: true }).click();
    await expect(card.getByRole("button", { name: "重新打开", exact: true })).toBeVisible();
    await connect(page);
    const restored = page.locator(`article.agent-card[data-agent-id="${first.agent_id}"]`);
    await restored.getByText("计划与待办", { exact: true }).click();
    await expect(restored.locator(".agent-tasks li")).toContainText("✓ 完成任务模块");
    await restored.getByRole("button", { name: "关闭会话" }).click();
    await expect(restored.getByLabel("新增任务内容")).toBeDisabled();
    await expect(restored.getByRole("button", { name: "删除条目" })).toBeDisabled();
    await restored.getByRole("button", { name: "重开会话" }).click();
    await restored.getByRole("button", { name: "删除条目" }).click();
    await expect(restored.locator(".agent-tasks li")).toHaveCount(0);
  } finally { for (const agent of [first, second]) await page.request.delete(`/v1/agents/${agent.agent_id}`, { headers }); }
});
