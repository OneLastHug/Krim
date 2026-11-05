import { expect, test } from "@playwright/test";

// 本机fixture的错误从真实Runtime进入已保存终态；不拦截UI/API响应。
test("正式聊天区区分模型协议、工具和步骤预算失败", async ({ page }, info) => {
  const errors: string[] = [];
  page.on("pageerror", error => errors.push(error.message));
  await page.goto("/ui/");
  await page.getByRole("button", { name: "密钥身份" }).click();
  await page.getByRole("menuitem", { name: "连接 / 切换密钥" }).click();
  await page.locator("#api-key").fill("formal-workbench-admin-browser-key");
  await page.locator("#connect-save").click();
  await page.locator(".sidebar-buttons").getByRole("button", { name: "新建对话", exact: true }).click();
  await page.locator("#session-title").fill("模型响应与调用预算");
  await page.getByRole("dialog").locator("button[type=submit]").click();
  for (const [prompt, message, code] of [
    ["/protocol", "模型响应无效", "provider_protocol"],
    ["/tool-limit", "工具调用次数达到本轮上限", "tool_limit"],
    ["/step-limit", "模型步骤达到本轮上限", "step_limit"],
  ]) {
    await expect(page.locator("#chat-prompt")).toBeEnabled();
    await page.locator("#chat-prompt").fill(prompt!);
    await page.locator(".chat-send").click();
    const last = page.locator(".chat-turn").last();
    await expect(last.locator(".chat-user-text")).toHaveText(prompt!);
    await expect(last.locator(".chat-turn-notice")).toContainText(message!);
    await expect(last.locator(".chat-turn-notice code")).toHaveText(code!);
    await expect(last).not.toContainText("读取失败，请刷新");
  }
  await expect(page.locator(".chat-turn")).toHaveCount(3);
  expect(errors).toEqual([]);
  await page.evaluate(() => document.fonts.ready);
  await page.screenshot({ path: info.outputPath("model-budget-errors.png"), fullPage: true });
});
