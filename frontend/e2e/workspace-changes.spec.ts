import { expect, test, type Locator, type Page } from "@playwright/test";

const headers = { Authorization: "Bearer browser-test-key" };
const tools = ["workspace_read", "workspace_list", "workspace_preview", "workspace_change", "workspace_reconcile", "workspace_finalize"];

async function connect(page: Page, topicId = new URL(page.url()).searchParams.get("topic")): Promise<void> {
  // 共用测试服务的其他话题可能已切换语言；固定本用例的作用域。
  await page.goto(topicId ? `/ui/?topic=${encodeURIComponent(topicId)}` : "/ui/");
  await page.getByLabel("服务访问密钥").fill("browser-test-key");
  await page.getByRole("button", { name: "连接", exact: true }).click();
  await expect(page.locator("#connection")).toContainText("本地服务已连接");
}

async function agent(page: Page, name: string, mode = "agent", selectedTools = tools): Promise<{ id: string; card: Locator; panel: Locator }> {
  const saved = await page.request.put(`/v1/agents/profiles/${name}`, { headers, data: { expected_revision: 0,
    config: { name, description: "浏览器文件修改", workspace_key: "edit", tool_names: selectedTools, mode },
  } });
  expect(saved.ok()).toBeTruthy();
  const view = await (await page.request.post("/v1/agents", { headers, data: { name } })).json();
  await connect(page, view.topic_id);
  const card = page.locator(`article.agent-card[data-agent-id="${view.agent_id}"]`), panel = card.locator(".workspace-changes");
  await panel.getByText("文件修改与历史", { exact: true }).click();
  await expect(panel.getByLabel("修改方式", { exact: true })).toBeVisible();
  return { id: view.agent_id, card, panel };
}

async function seed(page: Page, path: string, content: string, action = "seed"): Promise<void> {
  expect((await page.request.post("/_test/workspace", { headers, data: { action, path, content } })).ok()).toBeTruthy();
}

async function file(page: Page, id: string, path: string): Promise<string> {
  const result = await page.request.get(`/v1/agents/${id}/workspace/read?${new URLSearchParams({ path })}`, { headers });
  expect(result.ok()).toBeTruthy(); return (await result.json()).content;
}

async function confirmed(panel: Locator, page: Page, label: string, approve = true): Promise<void> {
  page.once("dialog", dialog => approve ? dialog.accept() : dialog.dismiss());
  await panel.getByRole("button", { name: label, exact: true }).click();
}

test("真实Diff确认、应用、保留、撤销、清理与刷新历史", async ({ page }) => {
  await seed(page, "lifecycle.txt", "old\n");
  const { id, card, panel } = await agent(page, "browser-writer");
  try {
    await panel.getByLabel("修改方式", { exact: true }).selectOption("write");
    await panel.getByLabel("修改文件路径", { exact: true }).fill("lifecycle.txt");
    await panel.getByRole("button", { name: "载入原文件", exact: true }).click();
    await expect(panel.getByLabel("完整新内容", { exact: true })).toHaveValue("old\n");
    await panel.getByLabel("完整新内容", { exact: true }).fill("new\n");
    await panel.getByRole("button", { name: "预览文件修改", exact: true }).click();
    await expect(panel.locator(".workspace-diff")).toContainText("-old");
    await expect(panel.locator(".workspace-diff")).toContainText("+new");
    await panel.locator(".workspace-change-detail").screenshot({ path: test.info().outputPath("workspace-diff.png") });
    expect(await file(page, id, "lifecycle.txt")).toBe("old\n");
    await confirmed(panel, page, "应用修改", false);
    expect(await file(page, id, "lifecycle.txt")).toBe("old\n");
    await confirmed(panel, page, "应用修改");
    await expect(panel.locator(".workspace-change-detail strong")).toContainText("文件已应用");
    expect(await file(page, id, "lifecycle.txt")).toBe("new\n");
    await confirmed(panel, page, "保留修改");
    await expect(panel.locator(".workspace-change-detail strong")).toContainText("文件已保留");
    const proposalId = await panel.locator(".workspace-change-detail").getAttribute("data-proposal-id");
    const historyAfterKeep = await (await page.request.get(`/v1/agents/${id}/workspace/changes/${proposalId}/history`, { headers })).json();
    const appliedOperation = historyAfterKeep.find((item: { action: string }) => item.action === "apply");
    expect(appliedOperation).toBeTruthy();
    await panel.locator(`[data-operation-id="${appliedOperation.operation_id}"]`).getByRole("button", { name: "查看操作回执", exact: true }).click();
    await confirmed(panel, page, "清理恢复材料");
    await expect(panel.getByRole("button", { name: "清理恢复材料", exact: true })).toHaveCount(0);
    expect((await (await page.request.get(`/v1/agents/${id}/workspace/operations/${appliedOperation.operation_id}`, { headers })).json()).cleanup_pending).toBe(false);
    await confirmed(panel, page, "撤销修改");
    await expect(panel.locator(".workspace-change-detail strong")).toContainText("文件已撤销");
    expect(await file(page, id, "lifecycle.txt")).toBe("old\n");
    await confirmed(panel, page, "清理恢复材料");
    await card.getByRole("button", { name: "关闭会话", exact: true }).click();
    await expect(panel.locator(".workspace-editor")).toBeHidden();
    await panel.getByRole("button", { name: "刷新修改历史", exact: true }).click();
    await expect(panel.locator(".workspace-operation-history li")).toHaveCount(3);
    await connect(page);
    const restored = page.locator(`article.agent-card[data-agent-id="${id}"] .workspace-changes`);
    await restored.getByText("文件修改与历史", { exact: true }).click();
    await expect(restored.locator(".workspace-change-detail strong")).toContainText("文件已撤销");
    await expect(restored.getByRole("button", { name: "应用修改", exact: true })).toBeDisabled();
  } finally { await page.request.delete(`/v1/agents/${id}`, { headers }); }
});

test("新建和删除版本保护、HTML纯文本展示与草稿保留", async ({ page }) => {
  const { id, card, panel } = await agent(page, "browser-create");
  const html = '<img src=x onerror="window.browserFileExecuted=1">';
  let releaseRead: (() => void) | undefined;
  try {
    await panel.getByLabel("修改文件路径", { exact: true }).fill("created.txt");
    await panel.getByLabel("完整新内容", { exact: true }).fill(html);
    await panel.getByRole("button", { name: "刷新修改历史", exact: true }).click();
    await expect(panel.getByLabel("完整新内容", { exact: true })).toHaveValue(html);
    // 预览已有Diff但原历史仍在读取时，不能展示可点却会被busy忽略的动作。
    await expect(panel.getByRole("button", { name: "预览文件修改", exact: true })).toBeEnabled();
    let observedRead: (() => void) | undefined;
    const entered = new Promise<void>(resolve => { observedRead = resolve; });
    const paused = new Promise<void>(resolve => { releaseRead = resolve; });
    let armed = true;
    await page.route(`**/v1/agents/${id}/workspace/changes?offset=0&limit=20`, async route => {
      if (!armed) { await route.continue(); return; }
      armed = false;
      const response = await route.fetch(); observedRead!(); await paused;
      await route.fulfill({ response });
    });
    await panel.getByRole("button", { name: "预览文件修改", exact: true }).click();
    await entered;
    await expect(panel.locator(".workspace-diff")).toContainText(html);
    await expect(panel.locator(".workspace-diff img")).toHaveCount(0);
    await expect(panel.getByRole("button", { name: "应用修改", exact: true })).toBeDisabled();
    releaseRead!();
    await expect(panel.getByRole("button", { name: "应用修改", exact: true })).toBeEnabled();
    await confirmed(panel, page, "应用修改");
    await expect(panel.locator(".workspace-change-detail strong")).toContainText("文件已应用");
    expect(await file(page, id, "created.txt")).toBe(html);
    expect(await page.evaluate(() => (window as unknown as { browserFileExecuted?: number }).browserFileExecuted)).toBeUndefined();
    await panel.getByLabel("修改方式", { exact: true }).selectOption("delete");
    await panel.getByRole("button", { name: "载入原文件", exact: true }).click();
    await panel.getByRole("button", { name: "预览文件修改", exact: true }).click();
    await expect(panel.locator(".workspace-diff")).toContainText(`-${html}`);
    await confirmed(panel, page, "应用修改");
    await expect(panel.locator(".workspace-change-detail strong")).toContainText("文件已应用");
    expect((await page.request.get(`/v1/agents/${id}/workspace/read?path=created.txt`, { headers })).status()).toBe(404);
    await confirmed(panel, page, "撤销修改");
    await expect(panel.locator(".workspace-change-detail strong")).toContainText("文件已撤销");
    expect(await file(page, id, "created.txt")).toBe(html);
    await card.getByRole("button", { name: "关闭会话", exact: true }).click();
  } finally { releaseRead?.(); await page.request.delete(`/v1/agents/${id}`, { headers }); }
});

test("截断原文不能完整写回，精确替换可以预览并恢复", async ({ page }) => {
  await seed(page, "large.txt", "", "large");
  const { id, panel } = await agent(page, "browser-replace");
  try {
    await panel.getByLabel("修改方式", { exact: true }).selectOption("write");
    await panel.getByLabel("修改文件路径", { exact: true }).fill("large.txt");
    await panel.getByRole("button", { name: "载入原文件", exact: true }).click();
    await expect(panel).toContainText("原文件展示不完整");
    await expect(panel.getByRole("button", { name: "预览文件修改", exact: true })).toBeDisabled();
    await panel.getByLabel("修改方式", { exact: true }).selectOption("replace");
    await panel.getByLabel("待替换原文", { exact: true }).fill("xxxx");
    await panel.getByLabel("替换为", { exact: true }).fill("yyyy");
    await panel.getByLabel("预期出现次数", { exact: true }).fill("12500");
    await expect(panel.getByRole("button", { name: "预览文件修改", exact: true })).toBeDisabled();
    await seed(page, "replace.txt", "one old two\n");
    await panel.getByLabel("修改文件路径", { exact: true }).fill("replace.txt");
    await panel.getByRole("button", { name: "载入原文件", exact: true }).click();
    await panel.getByLabel("待替换原文", { exact: true }).fill("old");
    await panel.getByLabel("替换为", { exact: true }).fill("new");
    await panel.getByLabel("预期出现次数", { exact: true }).fill("1");
    await panel.getByRole("button", { name: "预览文件修改", exact: true }).click();
    await confirmed(panel, page, "应用修改");
    await expect(panel.locator(".workspace-change-detail strong")).toContainText("文件已应用");
    expect(await file(page, id, "replace.txt")).toBe("one new two\n");
  } finally { await page.request.delete(`/v1/agents/${id}`, { headers }); }
});

test("回执保存失败显示待核实，浏览器沿原操作核实与清理", async ({ page }) => {
  await seed(page, "uncertain.txt", "old\n");
  const { id, panel } = await agent(page, "browser-verify");
  try {
    await panel.getByLabel("修改方式", { exact: true }).selectOption("write");
    await panel.getByLabel("修改文件路径", { exact: true }).fill("uncertain.txt");
    await panel.getByRole("button", { name: "载入原文件", exact: true }).click();
    await panel.getByLabel("完整新内容", { exact: true }).fill("new\n");
    await panel.getByRole("button", { name: "预览文件修改", exact: true }).click();
    expect((await page.request.post("/_test/workspace", { headers, data: { action: "fail_save" } })).ok()).toBeTruthy();
    await confirmed(panel, page, "应用修改");
    await expect(panel.locator(".workspace-operation-result")).toContainText("文件结果待核实");
    await expect(panel.getByRole("button", { name: "应用修改", exact: true })).toBeDisabled();
    expect(await file(page, id, "uncertain.txt")).toBe("new\n");
    await confirmed(panel, page, "核实文件结果");
    await expect(panel.locator(".workspace-operation-result")).toContainText("文件操作已确认");
    await confirmed(panel, page, "清理恢复材料");
    await expect(panel.getByRole("button", { name: "清理恢复材料", exact: true })).toHaveCount(0);
    await expect(panel.locator(".workspace-operation-history li")).toHaveCount(1);
  } finally { await page.request.delete(`/v1/agents/${id}`, { headers }); }
});

test("丢失预览响应复用原键，丢失动作响应刷新查询原回执", async ({ page }) => {
  await seed(page, "lost.txt", "old\n");
  const { id, panel } = await agent(page, "browser-lost-response");
  try {
    await panel.getByLabel("修改方式", { exact: true }).selectOption("write");
    await panel.getByLabel("修改文件路径", { exact: true }).fill("lost.txt");
    await panel.getByRole("button", { name: "载入原文件", exact: true }).click();
    await expect(panel.getByLabel("原文件SHA-256", { exact: true })).not.toHaveValue("");
    await panel.getByLabel("完整新内容", { exact: true }).fill("new\n");
    const keys: string[] = []; let previews = 0;
    await page.route(`**/v1/agents/${id}/workspace/changes/preview?confirmed=true`, async route => {
      keys.push((route.request().postDataJSON() as { request_key: string }).request_key);
      const response = await route.fetch(); previews += 1;
      if (previews === 1) await route.abort("failed"); else await route.fulfill({ response });
    });
    await panel.getByRole("button", { name: "预览文件修改", exact: true }).click();
    await expect(panel.locator(".workspace-change-status")).toContainText("fetch");
    await panel.getByRole("button", { name: "预览文件修改", exact: true }).click();
    await expect(panel.locator(".workspace-diff")).toContainText("+new");
    expect(keys).toHaveLength(2); expect(keys[0]).toBe(keys[1]);
    let actions = 0;
    await page.route(`**/v1/agents/${id}/workspace/changes?confirmed=true`, async route => {
      await route.fetch(); actions += 1; await route.abort("failed");
    });
    await confirmed(panel, page, "应用修改");
    await expect(panel.getByRole("button", { name: "应用修改", exact: true })).toBeDisabled();
    await panel.getByRole("button", { name: "刷新修改历史", exact: true }).click();
    await expect(panel.locator(".workspace-change-detail strong")).toContainText("文件已应用");
    await expect(panel.locator(".workspace-operation-history li")).toHaveCount(1);
    expect(actions).toBe(1); expect(await file(page, id, "lost.txt")).toBe("new\n");
  } finally { await page.request.delete(`/v1/agents/${id}`, { headers }); }
});

test("旧修订号与外部文件变化不自动覆盖原审阅决定", async ({ page }) => {
  await seed(page, "conflict.txt", "old\n");
  const { id, panel } = await agent(page, "browser-stale");
  try {
    await panel.getByLabel("修改方式", { exact: true }).selectOption("write");
    await panel.getByLabel("修改文件路径", { exact: true }).fill("conflict.txt");
    await panel.getByRole("button", { name: "载入原文件", exact: true }).click();
    await expect(panel.getByLabel("原文件SHA-256", { exact: true })).not.toHaveValue("");
    await panel.getByLabel("完整新内容", { exact: true }).fill("mine\n");
    await panel.getByRole("button", { name: "预览文件修改", exact: true }).click();
    await expect(panel.locator(".workspace-diff")).toContainText("+mine");
    await seed(page, "conflict.txt", "external\n");
    await confirmed(panel, page, "应用修改");
    await expect(panel.locator(".workspace-operation-result")).toContainText("文件操作已拒绝");
    expect(await file(page, id, "conflict.txt")).toBe("external\n");
    await expect(panel.getByRole("button", { name: "应用修改", exact: true })).toBeDisabled();
  } finally { await page.request.delete(`/v1/agents/${id}`, { headers }); }
});

test("会话工具子集和plan模式不能从浏览器扩大写权限", async ({ page }) => {
  const { id, panel } = await agent(page, "browser-plan", "plan");
  try {
    await panel.getByLabel("修改文件路径", { exact: true }).fill("plan.txt");
    await panel.getByLabel("完整新内容", { exact: true }).fill("planned\n");
    await panel.getByRole("button", { name: "预览文件修改", exact: true }).click();
    await expect(panel.locator(".workspace-diff")).toContainText("+planned");
    await expect(panel.getByRole("button", { name: "应用修改", exact: true })).toBeDisabled();
    const proposal = (await (await page.request.get(`/v1/agents/${id}/workspace/changes`, { headers })).json())[0];
    const forced = await page.request.post(`/v1/agents/${id}/workspace/changes?confirmed=true`, { headers, data: { proposal_id: proposal.proposal_id, action: "apply", expected_revision: proposal.revision, request_key: "denied" } });
    expect(forced.status()).toBe(403);
    expect((await page.request.get(`/v1/agents/${id}/workspace/read?path=plan.txt`, { headers })).status()).toBe(404);
  } finally { await page.request.delete(`/v1/agents/${id}`, { headers }); }
});

test("语言切换与Agent后台刷新保留编辑草稿，关闭后重开恢复控件", async ({ page }) => {
  const { id, card, panel } = await agent(page, "browser-editor-draft");
  try {
    await panel.getByLabel("修改文件路径", { exact: true }).fill("draft.txt");
    await panel.getByLabel("完整新内容", { exact: true }).fill("preserved\n");
    await page.locator("#locale-select").selectOption("en-US");
    await expect(panel.getByLabel("Complete new content", { exact: true })).toHaveValue("preserved\n");
    await expect(panel.getByLabel("File path", { exact: true })).toHaveValue("draft.txt");
    await panel.getByRole("button", { name: "Preview file change", exact: true }).click();
    await expect(panel.locator(".workspace-diff")).toContainText("+preserved");
    await expect(panel.locator(".workspace-change-detail strong")).toContainText("Change awaiting confirmation");
    await card.getByRole("button", { name: "Close session", exact: true }).click();
    await expect(panel.locator(".workspace-editor")).toBeHidden();
    await card.getByRole("button", { name: "Reopen session", exact: true }).click();
    await expect(panel.getByLabel("Complete new content", { exact: true })).toBeEnabled();
    await expect(panel.getByLabel("Complete new content", { exact: true })).toHaveValue("preserved\n");
    await page.locator("#locale-select").selectOption("zh-CN");
    await expect(panel.getByLabel("完整新内容", { exact: true })).toHaveValue("preserved\n");
  } finally { await page.request.delete(`/v1/agents/${id}`, { headers }); }
});
