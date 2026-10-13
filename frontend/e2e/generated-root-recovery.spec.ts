import { expect, test } from "@playwright/test";

test("原Model效果后SIGKILL、新进程浏览器同operation核实重载零重放", async ({ page }) => {
  const headers = { Authorization: "Bearer browser-test-key" };
  const before = await page.request.get("/_test/root-recovery", { headers }); expect(before.ok(), await before.text()).toBeTruthy();
  const original = await before.json(); expect(original.operation_status).toBe("uncertain"); expect(original.provider_calls).toBe(2);
  async function connect() {
    await page.goto(`/ui/?topic=${original.topic_id}`);
    await page.getByLabel("服务访问密钥", { exact: true }).fill("browser-test-key"); await page.getByRole("button", { name: "连接", exact: true }).click();
    await expect(page.locator("#connection")).toHaveClass(/ready/);
    const panel = page.locator(`article.agent-card[data-agent-id="${original.agent_id}"] .generated-tree-undos`);
    await panel.getByText("原生成根撤销与历史", { exact: true }).click(); return panel;
  }
  const panel = await connect(); await expect(panel.locator(".generated-root-list li")).toHaveCount(1);
  await panel.locator(".generated-root-list li button").click(); await expect(panel.locator(".generated-root-detail > strong")).toContainText("生成根结果待核实");
  await expect(panel.locator(".generated-root-condition")).toContainText(original.plan_id);
  await expect(panel.locator(".generated-root-condition")).toContainText("原生成状态: 树已完成");
  await expect(panel.locator(".generated-root-receipt")).toContainText(original.operation_id);
  page.once("dialog", dialog => void dialog.accept()); await panel.getByRole("button", { name: "核实原生成根结果", exact: true }).click();
  await expect(panel.locator(".generated-root-detail > strong")).toContainText("生成根已移至保留位置");
  const reloaded = await connect(); await expect(reloaded.locator(".generated-root-detail > strong")).toContainText("生成根已移至保留位置");
  await expect(reloaded.locator(".generated-root-history li")).toHaveCount(1);
  await reloaded.locator(".generated-root-history li button").click(); await expect(reloaded.locator(".generated-root-receipt")).toContainText(original.operation_id);
  const after = await page.request.get("/_test/root-recovery", { headers }); const actual = await after.json();
  expect(actual).toMatchObject({ operation_id: original.operation_id, plan_id: original.plan_id, root_inode: original.root_inode, file_inode: original.file_inode, generation_status: "done", root_state: "preserved", operation_status: "done", replay_calls: 0, old_owner_fenced: true, binary_hex: "00ff", counts: [1, 1, 1, 0, 2] });
  await test.info().attach("actual-root-recovery-facts", { body: JSON.stringify(actual, null, 2), contentType: "application/json" });
});
