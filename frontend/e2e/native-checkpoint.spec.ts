import { expect, test, type Locator, type Page } from "@playwright/test";
import { createHash } from "node:crypto";

const headers = { Authorization: "Bearer browser-test-key" };
const tools = ["workspace_read", "workspace_list", "workspace_preview", "workspace_change", "workspace_reconcile", "workspace_finalize"];
const panel = (page: Page): Locator => page.locator("#checkpoint-native");
const detail = (page: Page): Locator => page.locator("#native-restore-detail");

async function connect(page: Page, topic: string): Promise<void> {
  await page.goto(`/ui/?topic=${encodeURIComponent(topic)}`);
  await page.getByLabel("服务访问密钥").fill("browser-test-key");
  await page.getByRole("button", { name: "连接", exact: true }).click();
  await expect(page.locator("#connection")).toContainText(/本地服务已连接|Connected to local service/);
}
async function agent(page: Page, name: string, mode = "agent", selectedTools = tools) {
  expect((await page.request.put(`/v1/agents/profiles/${name}`, { headers, data: { expected_revision: 0,
    config: { name, description: "文件检查点浏览器验证", workspace_key: "edit", tool_names: selectedTools, mode },
  } })).ok()).toBeTruthy();
  const view = await (await page.request.post("/v1/agents", { headers, data: { name } })).json();
  await connect(page, view.topic_id);
  const card = page.locator(`.agent-card[data-agent-id="${view.agent_id}"]`);
  await card.getByRole("button", { name: "查看文件检查点", exact: true }).click();
  await expect(panel(page)).toBeVisible();
  await expect(page.locator("#checkpoint-source")).toHaveValue(view.agent_id);
  return { id: view.agent_id as string, topic: view.topic_id as string, card };
}
async function seed(page: Page, path: string, content: string, action = "seed"): Promise<void> {
  expect((await page.request.post("/_test/workspace", { headers, data: { action, path, content } })).ok()).toBeTruthy();
}
async function file(page: Page, id: string, path: string) {
  const response = await page.request.get(`/v1/agents/${id}/workspace/read?${new URLSearchParams({ path })}`, { headers });
  expect(response.ok()).toBeTruthy(); return response.json();
}
async function capture(page: Page, paths: string, title = "保存的文件版本"): Promise<string> {
  await page.locator("#native-checkpoint-title").fill(title);
  await page.locator("#native-checkpoint-paths").fill(paths);
  await expect(page.locator("#native-checkpoint-create")).toBeEnabled();
  await page.locator("#native-checkpoint-create").click();
  // 异步捕获不是10秒完成SLA；观察原记录，不能把尚在进行等同失败或另起一次捕获。
  await expect(page.locator("#native-checkpoint-detail")).toHaveAttribute("data-checkpoint-status", "available", { timeout: 30_000 });
  return (await page.locator("#native-checkpoint-detail").getAttribute("data-checkpoint-id"))!;
}
async function preview(page: Page): Promise<string> {
  await expect(page.locator("#native-checkpoint-preview")).toBeEnabled();
  await page.locator("#native-checkpoint-preview").click();
  try { await expect(detail(page)).toHaveAttribute("data-restore-phase", "ready"); }
  catch (error) {
    const id = await detail(page).getAttribute("data-plan-id"), agentId = await page.locator("#checkpoint-source").inputValue();
    if (id) {
      const result = await page.request.get(`/v1/agents/${agentId}/workspace/native-restores/${id}`, { headers });
      await test.info().attach("restore-original-fact-after-timeout", { body: await result.body(), contentType: "application/json" });
    }
    throw error;
  }
  return (await detail(page).getAttribute("data-plan-id"))!;
}
async function execute(page: Page, approve = true): Promise<void> {
  await expect(page.locator("#native-restore-execute")).toBeEnabled();
  page.once("dialog", dialog => approve ? dialog.accept() : dialog.dismiss());
  await page.locator("#native-restore-execute").click();
}
async function gate(page: Page, id: string, action: string, phase = "restore") {
  const response = await page.request.post("/_test/native-checkpoint", { headers, data: { agent_id: id, action, phase } });
  expect(response.ok()).toBeTruthy(); return response.json();
}

test("原生检查点捕获、恢复前备份、真实Diff、撤销与再次恢复，并从刷新历史找回", async ({ page }) => {
  test.setTimeout(90_000);
  await seed(page, "checkpoint-cycle.txt", "版本A\n");
  const { id, topic, card } = await agent(page, "checkpoint-cycle");
  try {
    const checkpointId = await capture(page, "checkpoint-cycle.txt", "审阅版本A");
    await seed(page, "checkpoint-cycle.txt", "版本B\n");
    const first = await preview(page);
    await expect(detail(page).locator(".workspace-diff")).toContainText("-版本B");
    await expect(detail(page).locator(".workspace-diff")).toContainText("+版本A");
    const before = await (await page.request.get(`/v1/agents/${id}/workspace/native-restores/${first}`, { headers })).json();
    const backup = await (await page.request.get(`/v1/agents/${id}/workspace/native-checkpoints/${before.baseline_checkpoint_id}`, { headers })).json();
    expect(backup.files[0].sha256).toBe(createHash("sha256").update("版本B\n").digest("hex"));
    await expect(detail(page)).toContainText(before.baseline_checkpoint_id);
    await execute(page, false);
    expect((await file(page, id, "checkpoint-cycle.txt")).content).toBe("版本B\n");
    await execute(page);
    await expect(detail(page)).toHaveAttribute("data-restore-status", "done");
    expect((await file(page, id, "checkpoint-cycle.txt")).content).toBe("版本A\n");
    await detail(page).getByRole("button", { name: "查看文件差异与操作", exact: true }).click();
    const changes = card.locator(".workspace-changes");
    await expect(changes.locator(".workspace-change-detail strong")).toContainText("文件已应用");
    page.once("dialog", dialog => dialog.accept());
    await changes.getByRole("button", { name: "撤销修改", exact: true }).click();
    await expect(changes.locator(".workspace-change-detail strong")).toContainText("文件已撤销");
    expect((await file(page, id, "checkpoint-cycle.txt")).content).toBe("版本B\n");
    await seed(page, "checkpoint-cycle.txt", "版本C\n");
    const second = await preview(page); expect(second).not.toBe(first);
    await expect(detail(page).locator(".workspace-diff")).toContainText("-版本C");
    await execute(page); await expect(detail(page)).toHaveAttribute("data-restore-status", "done");
    expect((await file(page, id, "checkpoint-cycle.txt")).content).toBe("版本A\n");
    await page.locator("#locale-select").selectOption("en-US");
    await expect(detail(page).locator("h3")).toContainText("File restore confirmed");
    await expect(panel(page).getByRole("button", { name: "Execute reviewed restore", exact: true })).toBeDisabled();
    await expect(page.locator("#native-checkpoint-title")).toHaveValue("审阅版本A");
    await page.locator("#native-restore-execute").scrollIntoViewIfNeeded();
    await page.locator("#checkpoint-panel").screenshot({ path: test.info().outputPath("native-checkpoint.png") });
    await connect(page, topic);
    await page.locator(`.agent-card[data-agent-id="${id}"]`).getByRole("button", { name: "View file checkpoints", exact: true }).click();
    await expect(detail(page)).toHaveAttribute("data-plan-id", second);
    await expect(detail(page)).toHaveAttribute("data-restore-status", "done");
    expect((await (await page.request.get(`/v1/agents/${id}/workspace/native-checkpoints/${checkpointId}`, { headers })).json()).status).toBe("available");
  } finally { await page.request.delete(`/v1/agents/${id}`, { headers }); }
});

test("HTML以文本显示，大文件截断提示保留且缺失文件可以恢复为缺失", async ({ page }) => {
  test.setTimeout(60_000);
  const html = '<img src=x onerror="window.checkpointExecuted=1">';
  await seed(page, "checkpoint-large.txt", "", "large");
  const { id } = await agent(page, "checkpoint-content");
  try {
    await capture(page, "checkpoint-large.txt\ncheckpoint-added.txt", html);
    await expect(page.locator("#native-checkpoint-items strong")).toHaveText(html);
    await seed(page, "checkpoint-large.txt", html);
    await seed(page, "checkpoint-added.txt", "新建内容");
    await preview(page);
    await expect(detail(page)).toContainText("差异已截断；恢复使用检查点的完整文件版本");
    await expect(detail(page).locator('[data-restore-path="checkpoint-large.txt"] pre')).toContainText(html);
    await expect(panel(page).locator("img")).toHaveCount(0);
    expect(await page.evaluate(() => (window as unknown as { checkpointExecuted?: number }).checkpointExecuted)).toBeUndefined();
    await execute(page); await expect(detail(page)).toHaveAttribute("data-restore-status", "done");
    expect((await file(page, id, "checkpoint-large.txt")).byte_size).toBe(50000);
    expect((await page.request.get(`/v1/agents/${id}/workspace/read?path=checkpoint-added.txt`, { headers })).status()).toBe(404);
  } finally { await page.request.delete(`/v1/agents/${id}`, { headers }); }
});

test("取消捕获与恢复阻止后续写入，等待不代替取消", async ({ page }) => {
  test.setTimeout(60_000);
  await seed(page, "checkpoint-cancel.txt", "A");
  const { id } = await agent(page, "checkpoint-cancel");
  try {
    await gate(page, id, "block", "capture");
    await page.locator("#native-checkpoint-paths").fill("checkpoint-cancel.txt");
    await expect(page.locator("#native-checkpoint-create")).toBeEnabled();
    await page.locator("#native-checkpoint-create").click();
    await expect.poll(async () => (await gate(page, id, "status", "capture")).entered).toBe(true);
    await expect(page.locator("#native-checkpoint-detail")).toHaveAttribute("data-checkpoint-status", "capturing");
    await expect(page.locator("#native-checkpoint-detail")).not.toContainText("文件不存在");
    await page.locator("#native-checkpoint-wait").click();
    await page.locator("#native-checkpoint-cancel").click();
    await expect(page.locator("#native-checkpoint-detail")).toHaveAttribute("data-checkpoint-status", "cancelled");
    await gate(page, id, "release", "capture");
    await capture(page, "checkpoint-cancel.txt");
    await seed(page, "checkpoint-cancel.txt", "B");
    await preview(page);
    await gate(page, id, "block");
    await execute(page);
    await expect.poll(async () => (await gate(page, id, "status")).entered).toBe(true);
    await expect(detail(page)).toHaveAttribute("data-restore-phase", "executing");
    await page.locator("#native-restore-cancel").click();
    await expect(detail(page)).toHaveAttribute("data-restore-phase", "cancelled");
    expect((await file(page, id, "checkpoint-cancel.txt")).content).toBe("B");
  } finally { await gate(page, id, "release"); await page.request.delete(`/v1/agents/${id}`, { headers }); }
});

test("响应丢失按原键找回，恢复失败与未知结果保留真实文件回执", async ({ page }) => {
  test.setTimeout(70_000);
  await seed(page, "checkpoint-unknown.txt", "A");
  const { id, card } = await agent(page, "checkpoint-unknown");
  const base = `/v1/agents/${id}/workspace`;
  let requests = 0;
  try {
    await capture(page, "checkpoint-unknown.txt");
    await seed(page, "checkpoint-unknown.txt", "B");
    const planId = await preview(page);
    await seed(page, "checkpoint-unknown.txt", "", "fail_save");
    await page.route(`**${base}/native-restores/${planId}/execute?*`, async route => {
      requests += 1; await route.fetch(); await route.abort("failed");
    });
    await execute(page);
    await expect(detail(page)).toHaveAttribute("data-restore-status", "uncertain");
    expect(requests).toBe(1);
    expect((await file(page, id, "checkpoint-unknown.txt")).content).toBe("A");
    const original = await (await page.request.get(`${base}/native-restores/${planId}`, { headers })).json();
    await expect(page.locator("#native-checkpoint-query")).toHaveValue(original.request_key);
    await page.locator("#native-checkpoint-query-kind").selectOption("restore");
    await page.locator("#native-checkpoint-query").fill(original.request_key);
    await page.locator("#native-checkpoint-query-key").click();
    await expect(detail(page)).toHaveAttribute("data-plan-id", planId);
    await expect(detail(page)).toContainText("workspace_receipt_pending");
    await detail(page).getByRole("button", { name: "查看文件差异与操作", exact: true }).click();
    const changes = card.locator(".workspace-changes");
    page.once("dialog", dialog => dialog.accept());
    await changes.getByRole("button", { name: "核实文件结果", exact: true }).click();
    await expect(changes.locator(".workspace-change-detail strong")).toContainText("文件已应用");
    await page.locator("#native-checkpoint-refresh").click();
    await expect(detail(page)).toHaveAttribute("data-restore-status", "done");
    expect((await (await page.request.get(`${base}/changes/${original.items[0].proposal.proposal_id}/history`, { headers })).json())).toHaveLength(1);
    await seed(page, "checkpoint-unknown.txt", "C");
    await preview(page);
    await seed(page, "checkpoint-unknown.txt", "外部变化");
    await execute(page); await expect(detail(page)).toHaveAttribute("data-restore-status", "failed");
    expect((await file(page, id, "checkpoint-unknown.txt")).content).toBe("外部变化");
  } finally { await page.unrouteAll({ behavior: "ignoreErrors" }); await page.request.delete(`/v1/agents/${id}`, { headers }); }
});

test("来源切换废弃迟到读取、保留草稿，关闭会话后只能查看原历史", async ({ page }) => {
  test.setTimeout(60_000);
  await seed(page, "checkpoint-source.txt", "A");
  const first = await agent(page, "checkpoint-source-a");
  let release: (() => void) | undefined;
  let secondId: string | undefined;
  try {
    await capture(page, "checkpoint-source.txt", "第一来源");
    await page.locator("#native-checkpoint-paths").fill("保留的路径草稿.txt");
    const response = await page.request.post("/v1/agents", { headers, data: { name: "checkpoint-source-a" } });
    secondId = (await response.json()).agent_id;
    await page.locator("#refresh").click();
    await expect(page.locator(`#checkpoint-source option[value="${secondId}"]`)).toHaveCount(1);
    let entered: (() => void) | undefined; const waiting = new Promise<void>(resolve => { entered = resolve; });
    let heldOnce = false;
    await page.route(`**/v1/agents/${first.id}/workspace/native-checkpoints?*`, async route => {
      if (heldOnce) { await route.continue(); return; } heldOnce = true;
      const result = await route.fetch(); entered!(); await new Promise<void>(resolve => { release = resolve; }); await route.fulfill({ response: result });
    });
    await page.locator("#native-checkpoint-refresh").click(); await waiting;
    await page.locator("#checkpoint-source").selectOption(secondId!);
    await expect(page.locator("#native-checkpoint-paths")).toHaveValue("");
    await page.locator("#native-checkpoint-paths").fill("第二来源草稿.txt");
    await page.locator("#checkpoint-source").selectOption(first.id);
    await expect(page.locator("#native-checkpoint-paths")).toHaveValue("保留的路径草稿.txt");
    await expect(page.locator("#native-checkpoint-create")).toBeEnabled();
    release!();
    await expect(page.locator("#native-checkpoint-items strong")).toHaveText("第一来源");
    await first.card.getByRole("button", { name: "关闭会话", exact: true }).click();
    await expect(page.locator("#native-checkpoint-create")).toBeDisabled();
    await page.locator("#native-checkpoint-refresh").click();
    await expect(page.locator("#native-checkpoint-detail")).toHaveAttribute("data-checkpoint-status", "available");
    await page.locator("#checkpoint-source").selectOption("host");
    await expect(page.locator("#checkpoint-host")).toBeVisible(); await expect(panel(page)).toBeHidden();
    await page.locator("#checkpoint-source").selectOption(first.id);
    await expect(page.locator("#native-checkpoint-create")).toBeDisabled();
    await expect(page.locator("#native-checkpoint-status")).toContainText("当前会话只能查询文件检查点历史");
  } finally { release?.(); await page.unrouteAll({ behavior: "ignoreErrors" }); await page.request.delete(`/v1/agents/${first.id}`, { headers }); if (secondId) await page.request.delete(`/v1/agents/${secondId}`, { headers }); }
});

test("计划模式可以审阅但不能执行，历史分页可以找到较早检查点", async ({ page }) => {
  test.setTimeout(70_000);
  await seed(page, "checkpoint-plan.txt", "A");
  const { id } = await agent(page, "checkpoint-plan", "plan");
  try {
    const firstId = await capture(page, "checkpoint-plan.txt", "最早版本");
    await seed(page, "checkpoint-plan.txt", "B");
    await preview(page); await expect(page.locator("#native-restore-execute")).toBeDisabled();
    const base = `/v1/agents/${id}/workspace/native-checkpoints`;
    for (let i = 0; i < 8; i += 1) {
      const created = await (await page.request.post(base, { headers, data: { title: `历史${i}`, paths: ["checkpoint-plan.txt"], request_key: `history-${i}` } })).json();
      const ready = await (await page.request.post(`${base}/${created.checkpoint_id}/wait`, { headers })).json();
      expect(ready.status).toBe("available");
    }
    await page.locator("#native-checkpoint-refresh").click();
    await expect(page.locator("#native-checkpoint-items .checkpoint-card")).toHaveCount(8);
    await expect(page.locator("#native-checkpoint-next")).toBeEnabled();
    await page.locator("#native-checkpoint-next").click();
    await expect(page.locator(`#native-checkpoint-items [data-checkpoint-id="${firstId}"]`)).toBeVisible();
    await page.locator(`#native-checkpoint-items [data-checkpoint-id="${firstId}"] button`).click();
    await expect(page.locator("#native-checkpoint-detail")).toContainText("最早版本");
    await expect(page.locator("#native-restore-execute")).toBeDisabled();
    expect((await file(page, id, "checkpoint-plan.txt")).content).toBe("B");
  } finally { await page.request.delete(`/v1/agents/${id}`, { headers }); }
});
