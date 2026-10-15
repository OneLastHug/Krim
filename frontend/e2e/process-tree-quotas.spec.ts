import { destroyAgent } from "./session-fixtures.js";
import { expect, test, type Page, type Locator } from "@playwright/test";

const headers = { Authorization: "Bearer browser-test-key" }, tools = ["workspace_read", "command_execute", "command_get", "command_inspect", "command_resources", "command_history", "command_read", "command_cancel"];
test.setTimeout(90_000);
async function actor(page: Page, name: string) {
  const saved = await page.request.put(`/v1/agents/profiles/${name}`, { headers, data: { expected_revision: 0, config: { name, description: "真实整树配额", workspace_key: "edit", execution_key: "tree", tool_names: tools } } }); expect(saved.ok(), await saved.text()).toBeTruthy();
  const response = await page.request.post("/v1/agents", { headers, data: { name } }); expect(response.ok()).toBeTruthy(); const view = await response.json();
  await page.goto(`/ui/?topic=${view.topic_id}`); await page.getByLabel("服务访问密钥", { exact: true }).fill("browser-test-key"); await page.getByRole("button", { name: "连接", exact: true }).click(); await expect(page.locator("#connection")).toHaveClass(/ready/);
  const panel = page.locator(`.agent-card[data-agent-id="${view.agent_id}"] .agent-commands`); await panel.getByText("命令执行与回执", { exact: true }).click();
  return { id: view.agent_id as string, panel, topic: view.topic_id as string };
}
async function start(page: Page, panel: Locator, code: string) {
  await panel.getByLabel("命令参数（JSON字符串数组）", { exact: true }).fill(JSON.stringify(["/usr/bin/python3", "-c", code]));
  page.once("dialog", dialog => dialog.accept()); await panel.getByRole("button", { name: "执行受控命令", exact: true }).click();
}

test("多子进程CPU合计限额、真实回执与资源统计跨刷新Locale保持且不重跑", async ({ page }) => {
  const { id, panel, topic } = await actor(page, "tree-quota-cpu"); let starts = 0;
  try {
    await page.route("**/commands?**", async route => { if (route.request().method() === "POST") starts++; await route.continue(); });
    await start(page, panel, "import os,time\nfor i in range(2):\n if os.fork()==0:\n  start=time.process_time()\n  while time.process_time()-start<2: pass\n  os._exit(0)\nfor i in range(2): os.wait()");
    await expect(panel.locator(".command-resources")).toContainText("整树累计CPU用量（微秒）", { timeout: 30_000 }); await expect(panel.locator(".command-detail")).toContainText("execution_tree_cpu_limit");
    const commandId = await panel.locator(".command-detail").getAttribute("data-command-id"); const url = `/v1/agents/${id}/commands/${commandId}/resources`;
    const value = await (await page.request.get(url, { headers })).json(); expect(value.usage.scope).toBe("process_tree"); expect(value.usage.cpu_microseconds).toBeGreaterThanOrEqual(1_000_000); expect(value.usage.populated).toBe(false); expect(value.usage.leader_reaped).toBe(true);
    await page.reload(); await page.getByLabel("服务访问密钥", { exact: true }).fill("browser-test-key"); await page.getByRole("button", { name: "连接", exact: true }).click(); await expect(page.locator("#connection")).toHaveClass(/ready/);
    const fresh = page.locator(`.agent-card[data-agent-id="${id}"] .agent-commands`); await fresh.getByText("命令执行与回执", { exact: true }).click(); await fresh.locator(`.command-list [data-command-id="${commandId}"]`).click();
    await expect(fresh.locator(".command-resources")).toContainText(String(value.usage.cpu_microseconds)); expect(starts).toBe(1); expect(await (await page.request.get(url, { headers })).json()).toEqual(value);
    await page.locator("#locale-select").selectOption("en-US"); await expect(fresh.locator(".command-resources")).toContainText("Process tree CPU usage");
    await page.setViewportSize({ width: 420, height: 860 }); await fresh.locator(".command-resources").scrollIntoViewIfNeeded(); await page.screenshot({ path: test.info().outputPath("tree-usage-narrow.png") });
    expect(await fresh.evaluate(node => node.scrollWidth <= node.clientWidth + 2)).toBe(true); expect(topic).toBeTruthy();
  } finally { await destroyAgent(page, id); }
});

test("内核整树PID上限拒绝和全部回收显示同一原计数", async ({ page }) => {
  const { id, panel } = await actor(page, "tree-quota-pids");
  try {
    await start(page, panel, "import os,time\nfor i in range(64):\n try: pid=os.fork()\n except OSError: break\n if pid==0:\n  time.sleep(5)\n  os._exit(0)\ntime.sleep(5)");
    await expect(panel.locator(".command-resources")).toContainText("整树进程限额拒绝计数", { timeout: 30_000 }); await expect(panel.locator(".command-detail")).toContainText("execution_tree_process_limit");
    const commandId = await panel.locator(".command-detail").getAttribute("data-command-id"); const value = await (await page.request.get(`/v1/agents/${id}/commands/${commandId}/resources`, { headers })).json();
    expect(value.usage.task_limit_hits).toBeGreaterThan(0); expect(value.usage.tasks_peak).toBeLessThanOrEqual(8); expect(value.processes_reaped).toBe(true);
  } finally { await destroyAgent(page, id); }
});

test("合法结构但错作用域的资源回复拒绝显示，原命令不受影响", async ({ page }) => {
  const { id, panel } = await actor(page, "tree-quota-response");
  try {
    await page.route("**/commands/*/resources**", async route => { const response = await route.fetch(), value = await response.json(); value.scope_id = "session:another"; await route.fulfill({ response, json: value }); });
    await start(page, panel, "print('safe')"); await expect(panel).toContainText("命令资源回程与原事实不一致", { timeout: 30_000 }); expect(await panel.locator(".command-resources").count()).toBe(0);
  } finally { await page.unrouteAll({ behavior: "wait" }); await destroyAgent(page, id); }
});
