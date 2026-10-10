import { expect, test, type Locator, type Page } from "@playwright/test";

const headers = { Authorization: "Bearer browser-test-key" };
const tools = ["workspace_read", "workspace_preview", "workspace_change", "workspace_reconcile", "workspace_finalize",
  "command_execute", "command_get", "command_read", "command_cancel", "command_history", "command_inspect", "command_changes", "command_propose"];
test.setTimeout(60_000);

async function connect(page: Page, topic: string): Promise<void> {
  await page.goto(`/ui/?topic=${topic}`);
  await page.getByLabel("服务访问密钥", { exact: true }).fill("browser-test-key");
  await page.getByRole("button", { name: "连接", exact: true }).click();
  await expect(page.locator("#connection")).toContainText("本地服务已连接");
}
async function agent(page: Page, name: string, mode = "agent", selectedTools = tools): Promise<{ id: string; topic: string; card: Locator; panel: Locator }> {
  const saved = await page.request.put(`/v1/agents/profiles/${name}`, { headers, data: {
    expected_revision: 0, config: { name, description: "命令浏览器实际进程", workspace_key: "edit", execution_key: "sandbox", tool_names: selectedTools, mode },
  } });
  expect(saved.ok()).toBeTruthy();
  const spawned = await page.request.post("/v1/agents", { headers, data: { name } });
  expect(spawned.ok()).toBeTruthy(); const view = await spawned.json();
  await connect(page, view.topic_id);
  const card = page.locator(`article.agent-card[data-agent-id="${view.agent_id}"]`), panel = card.locator(".agent-commands");
  await panel.getByText("命令执行与回执", { exact: true }).click();
  return { id: view.agent_id, topic: view.topic_id, card, panel };
}
async function draft(panel: Locator, code: string, input?: string): Promise<void> {
  await panel.getByLabel("命令参数（JSON字符串数组）", { exact: true }).fill(JSON.stringify(["/usr/bin/python3", "-I", "-c", code]));
  if (input) await panel.getByLabel("完整输入文件（每行一个）", { exact: true }).fill(input);
  await expect(panel.getByRole("button", { name: "执行受控命令", exact: true })).toBeEnabled();
}
async function confirm(page: Page, scope: Locator, label: string, accept = true): Promise<void> {
  page.once("dialog", dialog => accept ? dialog.accept() : dialog.dismiss());
  await scope.getByRole("button", { name: label, exact: true }).click();
}
async function seed(page: Page, path: string, content: string): Promise<void> {
  expect((await page.request.post("/_test/workspace", { headers, data: { action: "seed", path, content } })).ok()).toBeTruthy();
}

test("真实命令、输出分页与纯文本、产物审阅及独立批准和撤销", async ({ page }) => {
  const { id, card, panel } = await agent(page, "browser-command-flow");
  try {
    await seed(page, "command-flow.txt", "before\n");
    const html = '<img src=x onerror="window.commandExecuted=1">';
    await draft(panel, `open('command-flow.txt','w').write('after\\n');print(${JSON.stringify(html)});print('x'*6000)`, "command-flow.txt");
    await confirm(page, panel, "执行受控命令", false);
    expect((await (await page.request.get(`/v1/agents/${id}/commands`, { headers })).json()).entries).toHaveLength(0);
    let confirmation = "";
    page.once("dialog", dialog => { confirmation = dialog.message(); return dialog.accept(); });
    await panel.getByRole("button", { name: "执行受控命令", exact: true }).click();
    expect(confirmation).toContain('"input_paths"'); expect(confirmation).toContain("command-flow.txt");
    await expect(panel.locator(".command-detail > strong")).toContainText("命令已完成", { timeout: 25_000 });
    await panel.getByRole("button", { name: "读取标准输出", exact: true }).click();
    await expect(panel.locator(".command-output-text")).toContainText(html);
    expect(await panel.locator(".command-output-text img").count()).toBe(0);
    expect(await page.evaluate(() => (window as unknown as { commandExecuted?: number }).commandExecuted)).toBeUndefined();
    await panel.getByRole("button", { name: "较晚输出片段", exact: true }).click();
    await expect(panel.locator(".command-output > p")).toContainText("4096–");
    await panel.getByRole("button", { name: "较早输出片段", exact: true }).click();
    await expect(panel.locator(".command-output-text")).toContainText(html);
    await panel.getByRole("button", { name: "查看命令文件变化", exact: true }).click();
    const row = panel.locator('.command-change-row[data-path="command-flow.txt"]');
    await expect(row).toContainText("修改文件");
    expect((await (await page.request.get(`/v1/agents/${id}/workspace/read?path=command-flow.txt`, { headers })).json()).content).toBe("before\n");
    await confirm(page, row, "生成产物文件提案");
    await panel.getByRole("button", { name: "审阅产物文件提案", exact: true }).click();
    const files = card.locator(".workspace-changes");
    await expect(files.locator(".workspace-diff")).toContainText("-before");
    await expect(files.locator(".workspace-diff")).toContainText("+after");
    await confirm(page, files, "应用修改");
    await expect(files.locator(".workspace-change-detail strong")).toContainText("文件已应用");
    expect((await (await page.request.get(`/v1/agents/${id}/workspace/read?path=command-flow.txt`, { headers })).json()).content).toBe("after\n");
    await confirm(page, files, "撤销修改");
    await expect(files.locator(".workspace-change-detail strong")).toContainText("文件已撤销");
    expect((await (await page.request.get(`/v1/agents/${id}/workspace/read?path=command-flow.txt`, { headers })).json()).content).toBe("before\n");
    await panel.screenshot({ path: test.info().outputPath("command-panel.png") });
  } finally { await page.request.delete(`/v1/agents/${id}`, { headers }); }
});

test("丢失启动响应后重载按原键读取，没有重复POST或秘密落盘", async ({ page }) => {
  const { id, topic, panel } = await agent(page, "browser-command-lost");
  let posts = 0, originalKey = "";
  try {
    await draft(panel, "print('only once')");
    await page.route(`**/v1/agents/${id}/commands?confirmed=true`, async route => {
      posts += 1; originalKey = route.request().postDataJSON().request_key;
      await route.fetch(); await route.abort("failed");
    });
    await confirm(page, panel, "执行受控命令");
    await expect(panel.locator(".command-pending")).toContainText("请查询原请求键");
    await expect(panel.getByRole("button", { name: "执行受控命令", exact: true })).toBeDisabled();
    const stored = await page.evaluate(key => sessionStorage.getItem(key), `krim-command-pending:${id}`);
    expect(stored).toBe(originalKey); expect(stored).not.toContain("python");
    await connect(page, topic);
    const restored = page.locator(`article.agent-card[data-agent-id="${id}"] .agent-commands`);
    await restored.getByText("命令执行与回执", { exact: true }).click();
    await expect(restored.locator(".command-detail > strong")).toContainText("命令已完成", { timeout: 25_000 });
    await expect(restored.getByLabel("命令参数（JSON字符串数组）", { exact: true })).toHaveValue("");
    expect(posts).toBe(1);
    const history = await (await page.request.get(`/v1/agents/${id}/commands`, { headers })).json();
    expect(history.entries).toHaveLength(1); expect(history.entries[0].request_key).toBe(originalKey);
    await restored.getByRole("button", { name: "读取标准输出", exact: true }).click();
    await expect(restored.locator(".command-output-text")).toHaveText("only once\n");
  } finally { await page.request.delete(`/v1/agents/${id}`, { headers }); }
});

test("启动前断线，查询为空后明确复用同键同参数；未知时禁止新命令", async ({ page }) => {
  const { id, panel } = await agent(page, "browser-command-retry");
  const keys: string[] = [];
  try {
    await draft(panel, "print('retry original')");
    await page.route(`**/v1/agents/${id}/commands?confirmed=true`, async route => {
      keys.push(route.request().postDataJSON().request_key);
      if (keys.length === 1) await route.abort("failed"); else await route.continue();
    });
    await confirm(page, panel, "执行受控命令");
    await expect(panel.getByRole("button", { name: "执行受控命令", exact: true })).toBeDisabled();
    await panel.getByRole("button", { name: "查询未确认命令", exact: true }).click();
    await expect(panel.locator(".command-status")).toContainText("尚未找到原命令");
    await draft(panel, "print('changed')");
    await panel.getByRole("button", { name: "执行受控命令", exact: true }).click();
    await expect(panel.locator(".command-status")).toContainText("只能使用原参数");
    expect(keys).toHaveLength(1);
    await draft(panel, "print('retry original')");
    await confirm(page, panel, "执行受控命令");
    await expect(panel.locator(".command-detail > strong")).toContainText("命令已完成", { timeout: 25_000 });
    expect(keys).toHaveLength(2); expect(keys[0]).toBe(keys[1]);
    expect((await (await page.request.get(`/v1/agents/${id}/commands`, { headers })).json()).entries).toHaveLength(1);
  } finally { await page.request.delete(`/v1/agents/${id}`, { headers }); }
});

test("真实运行取消、等待读取与关闭会话后历史保持，plan拒绝执行", async ({ page }) => {
  const { id, card, panel } = await agent(page, "browser-command-stop");
  try {
    await draft(panel, "import time;print('waiting',flush=True);time.sleep(30)");
    await confirm(page, panel, "执行受控命令");
    await expect(panel.locator(".command-detail")).toHaveAttribute("data-command-id", /.+/, { timeout: 10_000 });
    const commandId = await panel.locator(".command-detail").getAttribute("data-command-id");
    expect(commandId).toBeTruthy();
    let phase = "";
    for (let attempt = 0; attempt < 80; attempt++) {
      const inspected = await (await page.request.get(`/v1/agents/${id}/commands/${commandId}/inspect`, { headers })).json();
      phase = inspected.phase;
      if (phase === "running") break;
      if (["finished", "interrupted"].includes(phase)) break;
      await page.waitForTimeout(100);
    }
    expect(phase).toBe("running");
    await expect(panel.locator(".command-meta")).toContainText("命令已启动 是", { timeout: 10_000 });
    await panel.getByRole("button", { name: "等待原命令", exact: true }).click();
    await confirm(page, panel, "取消原命令");
    await expect(panel.locator(".command-detail > strong")).toContainText("命令已取消", { timeout: 25_000 });
    await expect(panel.locator(".command-meta")).toContainText("命令进程已回收 是");
    await card.getByRole("button", { name: "关闭会话", exact: true }).click();
    await expect(panel.getByLabel("命令参数（JSON字符串数组）", { exact: true })).toBeDisabled();
    await panel.getByRole("button", { name: "刷新命令历史", exact: true }).click();
    await expect(panel.locator(".command-detail > strong")).toContainText("命令已取消");
    await panel.getByRole("button", { name: "读取标准输出", exact: true }).click();
    await expect(panel.locator(".command-output-text")).toContainText("waiting");
  } finally { await page.request.delete(`/v1/agents/${id}`, { headers }); }
  const planned = await agent(page, "browser-command-plan", "plan");
  try {
    await expect(planned.panel.getByRole("button", { name: "执行受控命令", exact: true })).toBeDisabled();
    const forced = await page.request.post(`/v1/agents/${planned.id}/commands?confirmed=true`, { headers,
      data: { request_key: "forbidden-plan", command: { argv: ["/usr/bin/true"] } } });
    expect(forced.status()).toBe(403);
    expect((await (await page.request.get(`/v1/agents/${planned.id}/commands`, { headers })).json()).entries).toHaveLength(0);
  } finally { await page.request.delete(`/v1/agents/${planned.id}`, { headers }); }
});

test("失败产物明确接受、产物分页、原字节保留，二进制沿私有提案发布", async ({ page }) => {
  const { id, card, panel } = await agent(page, "browser-command-artifacts");
  try {
    await draft(panel, "import sys;[open('artifact-%02d.txt'%i,'w').write('file %d'%i) for i in range(40)];open('binary.dat','wb').write(bytes([0,255,1]));sys.exit(7)");
    await confirm(page, panel, "执行受控命令");
    await expect(panel.locator(".command-detail > strong")).toContainText("命令失败", { timeout: 25_000 });
    await panel.getByRole("button", { name: "查看命令文件变化", exact: true }).click();
    await expect(panel.locator(".command-change-row")).toHaveCount(32);
    await panel.getByRole("button", { name: "较晚文件变化", exact: true }).click();
    await expect(panel.locator(".command-change-row")).toHaveCount(9);
    const binary = panel.locator('.command-change-row[data-path="binary.dat"]');
    await binary.getByRole("button", { name: "读取原产物", exact: true }).click();
    await panel.locator(".command-output-raw summary").click();
    await expect(panel.locator(".command-output-raw pre")).toHaveText("AP8B");
    await confirm(page, binary, "生成产物文件提案");
    await expect(panel.locator(".command-proposal")).toContainText("产物提案已保存");
    await panel.getByRole("button", { name: "审阅产物文件提案", exact: true }).click();
    const files = card.locator(".workspace-changes");
    await expect(files.locator(".workspace-diff")).toContainText("binary bytes=3");
    await confirm(page, files, "应用修改");
    await expect(files.locator(".workspace-change-detail strong")).toContainText("文件已应用");
    expect((await page.request.get(`/v1/agents/${id}/workspace/read?path=binary.dat`, { headers })).status()).toBe(422);
    await panel.getByRole("button", { name: "较早文件变化", exact: true }).click();
    const first = panel.locator('.command-change-row[data-path="artifact-00.txt"]');
    await confirm(page, first, "生成产物文件提案", false);
    expect((await (await page.request.get(`/v1/agents/${id}/workspace/changes`, { headers })).json())).toHaveLength(1);
    const bodies: Array<{ accept_failed: boolean }> = [];
    await page.route(`**/v1/agents/${id}/commands/*/proposals?confirmed=true`, async route => {
      bodies.push(route.request().postDataJSON()); await route.continue();
    });
    await confirm(page, first, "生成产物文件提案");
    await expect(panel.locator(".command-proposal")).toContainText("产物提案已保存");
    expect(bodies).toHaveLength(1); expect(bodies[0]!.accept_failed).toBe(true);
  } finally { await page.request.delete(`/v1/agents/${id}`, { headers }); }
});

test("只读工具子集、查询缺失、JSON与路径输入边界、中英文草稿保留", async ({ page }) => {
  const full = await agent(page, "browser-command-draft");
  try {
    await full.panel.getByLabel("命令参数（JSON字符串数组）", { exact: true }).fill("python -c pass");
    await expect(full.panel.getByRole("button", { name: "执行受控命令", exact: true })).toBeDisabled();
    await draft(full.panel, "print('preserved')");
    await full.panel.getByLabel("完整输入文件（每行一个）", { exact: true }).fill("../escape.txt");
    await expect(full.panel.getByRole("button", { name: "执行受控命令", exact: true })).toBeDisabled();
    await full.panel.getByLabel("完整输入文件（每行一个）", { exact: true }).fill("");
    await page.locator("#locale-select").selectOption("en-US");
    await expect(full.panel.getByLabel("Command arguments (JSON string array)", { exact: true })).toHaveValue(/preserved/);
    await expect(full.panel.getByRole("button", { name: "Execute controlled command", exact: true })).toBeEnabled();
    await page.locator("#locale-select").selectOption("zh-CN");
    await full.panel.getByLabel("命令标识或原请求键", { exact: true }).fill("not-yet-found");
    await full.panel.getByRole("button", { name: "查询原命令", exact: true }).click();
    await expect(full.panel.locator(".command-status")).toContainText("尚未找到原命令");
  } finally { await page.request.delete(`/v1/agents/${full.id}`, { headers }); }
  const readonly = await agent(page, "browser-command-subset", "agent", ["command_get", "command_history"]);
  try {
    await expect(readonly.panel.getByRole("button", { name: "执行受控命令", exact: true })).toBeDisabled();
    await expect(readonly.panel.getByRole("button", { name: "刷新命令历史", exact: true })).toBeEnabled();
    const forced = await page.request.post(`/v1/agents/${readonly.id}/commands?confirmed=true`, { headers,
      data: { request_key: "no-write-tool", command: { argv: ["/usr/bin/true"] } } });
    expect(forced.status()).toBe(403);
  } finally { await page.request.delete(`/v1/agents/${readonly.id}`, { headers }); }
});

test("运行终态同步命令列表、真实历史游标与销毁后的迟到读取", async ({ page }) => {
  const { id, card, panel } = await agent(page, "browser-command-history");
  let deleted = false, release: (() => void) | undefined;
  try {
    await draft(panel, "import time;time.sleep(2);print('list-ended')");
    await confirm(page, panel, "执行受控命令");
    await expect(panel.locator(".command-detail > strong")).toContainText("命令已完成", { timeout: 25_000 });
    // 详情的终态不能与同一命令的列表状态矛盾。
    await expect(panel.locator(".command-list > button")).toContainText("命令已完成");
    const first = await panel.locator(".command-detail").getAttribute("data-command-id");
    const ids = [first!];
    for (let index = 0; index < 2; index++) {
      const accepted = await page.request.post(`/v1/agents/${id}/commands?confirmed=true`, { headers,
        data: { request_key: `history-${index}`, command: { argv: ["/usr/bin/true"] } } });
      expect(accepted.status()).toBe(202);
      const commandId = (await accepted.json()).command_id;
      const done = await page.request.post(`/v1/agents/${id}/commands/${commandId}/wait`, { headers });
      expect((await done.json()).outcome).toBe("completed"); ids.push(commandId);
    }
    // 降低本测试的服务器页容量，响应仍是实际三个命令的原REST结果。
    await page.route(`**/v1/agents/${id}/commands?limit=32*`, async route => {
      const url = new URL(route.request().url()); url.searchParams.set("limit", "1");
      await route.fulfill({ response: await route.fetch({ url: url.href }) });
    });
    await panel.getByRole("button", { name: "刷新命令历史", exact: true }).click();
    await expect(panel.locator(`.command-list > button[data-command-id="${ids[0]}"]`)).toHaveCount(1);
    await panel.getByRole("button", { name: "下一页命令", exact: true }).click();
    await expect(panel.locator(`.command-list > button[data-command-id="${ids[1]}"]`)).toHaveCount(1);
    await panel.getByRole("button", { name: "下一页命令", exact: true }).click();
    await expect(panel.locator(`.command-list > button[data-command-id="${ids[2]}"]`)).toHaveCount(1);
    await panel.getByRole("button", { name: "上一页命令", exact: true }).click();
    await expect(panel.locator(`.command-list > button[data-command-id="${ids[1]}"]`)).toHaveCount(1);
    await panel.getByLabel("命令当前页筛选", { exact: true }).selectOption("active");
    await expect(panel.locator(".command-list")).toContainText("当前页没有匹配的命令");
    await panel.getByLabel("命令当前页筛选", { exact: true }).selectOption("all");
    let entered: (() => void) | undefined;
    const observed = new Promise<void>(resolve => { entered = resolve; });
    const blocked = new Promise<void>(resolve => { release = resolve; });
    await page.route(`**/v1/agents/${id}/commands/${first}/inspect`, async route => {
      const response = await route.fetch(); entered!(); await blocked; await route.fulfill({ response });
    });
    await panel.getByRole("button", { name: "刷新原命令", exact: true }).click();
    await observed;
    await card.getByRole("button", { name: "销毁", exact: true }).click();
    await expect(card).toHaveCount(0); deleted = true;
    release!(); await expect(card).toHaveCount(0);
  } finally {
    release?.();
    if (!deleted) await page.request.delete(`/v1/agents/${id}`, { headers });
  }
});

test("配置界面显示已登记执行资源并冻结execution_key到新Agent", async ({ page }) => {
  await page.goto("/ui/");
  await page.getByLabel("服务访问密钥", { exact: true }).fill("browser-test-key");
  await page.getByRole("button", { name: "连接", exact: true }).click();
  await expect(page.locator("#connection")).toContainText("本地服务已连接");
  await page.locator("#agent-config-panel summary").click();
  await expect(page.locator("#agent-execution-resources")).toContainText("sandbox");
  const name = `browser-execution-${Date.now()}`;
  await page.getByLabel("配置名称", { exact: true }).fill(name);
  await page.getByLabel("任务说明", { exact: true }).fill("浏览器明确授权命令资源");
  await page.getByLabel("允许工具（逗号分隔）", { exact: true }).fill("workspace_read,command_execute,command_get,command_read,command_history,command_inspect");
  await page.getByLabel("工作区资源（留空不授权）", { exact: true }).fill("edit");
  await page.getByLabel("执行资源（留空不授权）", { exact: true }).fill("sandbox");
  await page.getByRole("button", { name: "保存 Agent 配置", exact: true }).click();
  await expect(page.locator("#agent-config-revision")).toHaveValue("1");
  const saved = await (await page.request.get(`/v1/agents/profiles/${name}`, { headers })).json();
  expect(saved.config.execution_key).toBe("sandbox");
  await page.getByLabel("执行资源（留空不授权）", { exact: true }).fill("");
  await page.getByRole("button", { name: "读取 Agent 配置", exact: true }).click();
  await expect(page.getByLabel("执行资源（留空不授权）", { exact: true })).toHaveValue("sandbox");
  await page.locator("#agent-kind").selectOption(name);
  await page.getByLabel("Agent 任务", { exact: true }).fill("/echo configured");
  await page.getByRole("button", { name: "创建 Agent", exact: true }).click();
  const card = page.locator(".agent-card", { hasText: name }).last();
  await expect(card.locator(".agent-commands")).toBeVisible();
  const agentId = await card.getAttribute("data-agent-id");
  expect(agentId).toBeTruthy();
  let secondId: string | undefined;
  try {
    const bound = page.locator(`article.agent-card[data-agent-id="${agentId}"]`), panel = bound.locator(".agent-commands");
    await panel.getByText("命令执行与回执", { exact: true }).click();
    await draft(panel, "print('configured in browser')");
    await confirm(page, panel, "执行受控命令");
    await expect(panel.locator(".command-detail > strong")).toContainText("命令已完成", { timeout: 25_000 });
    const frozen = await (await page.request.get(`/v1/agents/${agentId}`, { headers })).json();
    expect(frozen.session.permissions.execution.key).toBe("sandbox");
    // 未展示字段与enabled由原版本合并保留，显示的执行资源可明确移除。
    const enriched = await page.request.put(`/v1/agents/profiles/${name}`, { headers, data: {
      expected_revision: 1, enabled: false, config: { ...saved.config, max_turns: 4, timeout_seconds: 21 },
    } });
    expect(enriched.ok()).toBeTruthy();
    await page.getByRole("button", { name: "读取 Agent 配置", exact: true }).click();
    await expect(page.locator("#agent-config-revision")).toHaveValue("2");
    await page.getByLabel("执行资源（留空不授权）", { exact: true }).fill("missing-executor");
    await page.getByRole("button", { name: "保存 Agent 配置", exact: true }).click();
    await expect(page.locator("#notice")).toContainText("执行资源未登记或已停用");
    await expect(page.locator("#agent-config-revision")).toHaveValue("2");
    await page.getByLabel("执行资源（留空不授权）", { exact: true }).fill("");
    await page.getByRole("button", { name: "保存 Agent 配置", exact: true }).click();
    await expect(page.locator("#notice")).toContainText("命令工具需要显式执行资源授权");
    await expect(page.locator("#agent-config-revision")).toHaveValue("2");
    await page.getByLabel("允许工具（逗号分隔）", { exact: true }).fill("echo");
    await page.getByRole("button", { name: "保存 Agent 配置", exact: true }).click();
    await expect(page.locator("#agent-config-revision")).toHaveValue("3");
    const revised = await (await page.request.get(`/v1/agents/profiles/${name}`, { headers })).json();
    expect(revised.config.execution_key).toBeUndefined(); expect(revised.enabled).toBe(false);
    expect(revised.config.max_turns).toBe(4); expect(revised.config.timeout_seconds).toBe(21);
    expect((await (await page.request.get(`/v1/agents/${agentId}`, { headers })).json()).session.permissions.execution.key).toBe("sandbox");
    const enabled = await page.request.put(`/v1/agents/profiles/${name}`, { headers, data: {
      expected_revision: 3, enabled: true, config: revised.config,
    } });
    expect(enabled.ok()).toBeTruthy();
    const second = await page.request.post("/v1/agents", { headers, data: { name } });
    expect(second.status()).toBe(201); secondId = (await second.json()).agent_id;
    const secondView = await (await page.request.get(`/v1/agents/${secondId}`, { headers })).json();
    expect(secondView.session.permissions.execution).toBeUndefined();
    await page.getByRole("button", { name: "读取 Agent 配置", exact: true }).click();
    await expect(page.getByLabel("执行资源（留空不授权）", { exact: true })).toHaveValue("");
    await page.getByLabel("执行资源（留空不授权）", { exact: true }).fill("draft-resource");
    await page.locator("#locale-select").selectOption("en-US");
    await expect(page.getByLabel("Execution resource (leave blank for no grant)", { exact: true })).toHaveValue("draft-resource");
    await page.locator("#locale-select").selectOption("zh-CN");
  } finally {
    await page.request.delete(`/v1/agents/${agentId}`, { headers });
    if (secondId) await page.request.delete(`/v1/agents/${secondId}`, { headers });
  }
});
