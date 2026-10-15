/** 测试显式选择界面语言，并验证原Agent销毁接口真实收束。 */
import { expect, type Page } from "@playwright/test";

const headers = { Authorization: "Bearer browser-test-key" };

export async function ensureChineseUi(page: Page): Promise<void> {
  await expect(page.locator("#connection")).toHaveClass(/ready/);
  if (await page.locator("#locale-select").inputValue() !== "zh-CN") {
    await page.locator("#locale-select").selectOption("zh-CN");
  }
  await expect(page.locator("#connection")).toContainText("本地服务已连接");
}

export async function destroyAgent(page: Page, id: string): Promise<void> {
  const response = await page.request.delete(`/v1/agents/${encodeURIComponent(id)}`, { headers });
  expect(response.status(), await response.text()).toBe(200);
  const result = await response.json();
  expect(result.agent_id).toBe(id);
  expect(result.status).toBe("destroyed");
}
