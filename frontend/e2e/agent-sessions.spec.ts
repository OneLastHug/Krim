import { expect, test } from "@playwright/test";

const headers = { Authorization: "Bearer browser-test-key" };
async function connect(page: import("@playwright/test").Page): Promise<void> {
  await page.goto("/ui/");
  await page.getByLabel("服务访问密钥").fill("browser-test-key");
  await page.getByRole("button", { name: "连接", exact: true }).click();
  await expect(page.locator("#connection")).toContainText("本地服务已连接");
}

test("会话关闭重开保留草稿和历史轮次，专属话题只读", async ({ page }) => {
  await connect(page);
  const created = await page.request.post("/v1/agents", { headers, data: { name: "default" } });
  const agent = await created.json();
  try {
    await page.request.post(`/v1/agents/${agent.agent_id}/execute`, { headers, data: { prompt: "/add 2 3", idempotency_key: "first" } });
    await page.locator("#refresh").click();
    const card = page.locator(`article.agent-card[data-agent-id="${agent.agent_id}"]`);
    await expect(card.locator(".agent-result")).toContainText('"sum": 5.0');
    const input = card.locator("textarea");
    await input.fill("保留草稿");
    await card.getByRole("button", { name: "关闭会话" }).click();
    await expect(card).toContainText("会话关闭");
    await expect(input).toHaveValue("保留草稿");
    await expect(card.getByRole("button", { name: "继续任务" })).toBeDisabled();
    await card.getByRole("button", { name: "重开会话" }).click();
    await expect(card).toContainText("会话开启");
    await expect(input).toHaveValue("保留草稿");
    await input.fill("/echo session-second");
    await card.getByRole("button", { name: "继续任务" }).click();
    await expect(card.locator(".agent-result")).toContainText("session-second");
    await card.getByText("轮次历史", { exact: true }).click();
    await expect(card.locator("select option")).toHaveCount(2);
    const turns = await (await page.request.get(`/v1/agents/${agent.agent_id}/turns`, { headers })).json();
    await card.locator("select").selectOption(turns[0].run_id);
    await expect(card.locator(".agent-history")).toContainText('"sum": 5.0');
    await expect(card.locator(".agent-history")).toHaveAttribute("data-run-id", turns[0].run_id);
    await card.getByRole("button", { name: "查看会话历史" }).click();
    await expect(page.locator("#prompt")).toBeDisabled();
    await expect(page.locator(".message-controls")).toHaveCount(0);
  } finally { await page.request.delete(`/v1/agents/${agent.agent_id}`, { headers }); }
});

test("多个后台Agent独立显示部分文本和停止，刷新保留原Run", async ({ page }) => {
  await connect(page);
  const one = await (await page.request.post("/v1/agents", { headers, data: { name: "default", persistent: true } })).json();
  const two = await (await page.request.post("/v1/agents", { headers, data: { name: "default", persistent: true } })).json();
  try {
    for (const [agent, text] of [[one, "甲"], [two, "乙"]] as const) {
      const accepted = await page.request.post(`/v1/agents/${agent.agent_id}/start`, { headers, data: { prompt: text.repeat(1200), idempotency_key: text } });
      expect(accepted.status()).toBe(202);
    }
    await page.locator("#refresh").click();
    const first = page.locator(`article.agent-card[data-agent-id="${one.agent_id}"]`), second = page.locator(`article.agent-card[data-agent-id="${two.agent_id}"]`);
    await expect(first.locator(".agent-draft")).toContainText("甲");
    await expect(second.locator(".agent-draft")).toContainText("乙");
    await expect(first.locator(".agent-draft")).not.toContainText("乙");
    await first.locator("textarea").fill("下一轮草稿");
    await first.getByRole("button", { name: "停止", exact: true }).click();
    await expect(first.locator("strong")).toContainText("已停止");
    await expect(first.locator("textarea")).toHaveValue("下一轮草稿");
    const before = await (await page.request.get(`/v1/agents/${two.agent_id}`, { headers })).json();
    expect(["running", "completed"]).toContain(before.status);
    await connect(page);
    const restored = page.locator(`article.agent-card[data-agent-id="${two.agent_id}"]`);
    await expect(restored.locator(".agent-result")).toContainText("乙");
    await restored.getByText("执行步骤与工具", { exact: true }).click();
    await expect(restored.locator("ol")).toContainText("模型生成");
    const after = await (await page.request.get(`/v1/agents/${two.agent_id}`, { headers })).json();
    expect(after.current_run_id).toBe(before.current_run_id); expect(after.turn).toBe(1);
  } finally {
    for (const agent of [one, two]) await page.request.delete(`/v1/agents/${agent.agent_id}`, { headers });
  }
});

test("持久清单解释不可恢复原因，资源补齐后恢复同一实例", async ({ page }) => {
  const created = await page.request.post("/_test/agent-inventory", { headers, data: { action: "create" } });
  const agent = await created.json();
  try {
    await connect(page);
    const unavailable = page.locator(`#agent-unavailable [data-agent-id="${agent.agent_id}"]`);
    await expect(unavailable).toContainText("宿主尚未登记此 Agent");
    await page.request.post("/_test/agent-inventory", { headers, data: { action: "register" } });
    await unavailable.getByRole("button", { name: "尝试恢复" }).click();
    await expect(unavailable).toHaveCount(0);
    const restored = page.locator(`article.agent-card[data-agent-id="${agent.agent_id}"]`);
    await expect(restored).toContainText("待执行");
    await restored.locator("textarea").fill("/echo restored-agent");
    await restored.getByRole("button", { name: "执行任务" }).click();
    await expect(restored.locator(".agent-result")).toContainText("restored-agent");
  } finally { await page.request.delete(`/v1/agents/${agent.agent_id}`, { headers }); }
});
