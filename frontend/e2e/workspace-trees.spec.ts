import { expect, test, type Locator, type Page } from "@playwright/test";
import { createHash } from "node:crypto";

const headers = { Authorization: "Bearer browser-test-key" };
const tools = ["workspace_read", "workspace_preview", "workspace_change", "workspace_reconcile", "workspace_finalize",
  "command_execute", "command_get", "command_read", "command_cancel", "command_history", "command_inspect", "command_changes", "command_propose",
  "directory_source_preview", "directory_source_query", "directory_query", "directory_change", "directory_reconcile", "directory_finalize", "workspace_tree_prepare"];
type Agent = { agent_id: string; topic_id: string; session_id: string };
type Plan = { plan_id: string; request_key: string; root_path: string; status: string; producer_status: string; done_count: number };
type Step = { position: number; source: string; path: string; status: string; proposal_id: string | null;
  operation: { operation_id: string; receipt: { after_sha256?: string } | null } | null };
test.setTimeout(90_000);

async function create(page: Page, name: string, mode = "agent", names = tools): Promise<Agent> {
  const saved = await page.request.put(`/v1/agents/profiles/${name}`, { headers, data: {
    expected_revision: 0, config: { name, description: "目录树真实浏览器门禁", workspace_key: "edit", execution_key: "sandbox", tool_names: names, mode },
  } });
  expect(saved.ok()).toBeTruthy();
  const spawned = await page.request.post("/v1/agents", { headers, data: { name } }); expect(spawned.ok()).toBeTruthy(); return spawned.json();
}
async function connect(page: Page, value: Agent): Promise<{ card: Locator; commands: Locator; tree: Locator }> {
  await page.goto(`/ui/?topic=${value.topic_id}`);
  await page.getByLabel("服务访问密钥", { exact: true }).fill("browser-test-key");
  await page.getByRole("button", { name: "连接", exact: true }).click();
  await expect(page.locator("#connection")).toHaveClass(/ready/);
  const card = page.locator(`article.agent-card[data-agent-id="${value.agent_id}"]`), commands = card.locator(".agent-commands");
  await commands.getByText("命令执行与回执", { exact: true }).click();
  return { card, commands, tree: commands.locator(".workspace-trees") };
}
async function confirm(page: Page, scope: Locator, name: string, accept = true): Promise<void> {
  const handled = page.waitForEvent("dialog").then(dialog => accept ? dialog.accept() : dialog.dismiss());
  await scope.getByRole("button", { name, exact: true }).click(); await handled;
}
function base(value: Agent): string { return `/v1/agents/${value.agent_id}/workspace/tree-plans`; }
function code(root: string, failed = false): string {
  return `import os;os.makedirs(${JSON.stringify(root + "/sub")});os.chmod(${JSON.stringify(root)},0o750);os.chmod(${JSON.stringify(root + "/sub")},0o700);open(${JSON.stringify(root + "/sub/data.txt")},'w').write('browser tree data\\n');open(${JSON.stringify(root + "/sub/data.bin")},'wb').write(bytes([0,255]))${failed ? ";raise SystemExit(3)" : ""}`;
}
async function source(page: Page, value: Agent, root: string, failed = false, customCode?: string): Promise<void> {
  const accepted = await page.request.post(`/v1/agents/${value.agent_id}/commands?confirmed=true`, { headers, data: {
    request_key: "source", command: { argv: ["/usr/bin/python3", "-I", "-c", customCode || code(root, failed)] },
  } }); expect(accepted.ok()).toBeTruthy();
  const done = await page.request.post(`/v1/agents/${value.agent_id}/commands/${(await accepted.json()).command_id}/wait?timeout_seconds=30`, { headers });
  expect(done.ok()).toBeTruthy(); expect((await done.json()).outcome).toBe(failed ? "failed" : "completed");
}
async function choose(page: Page, value: Agent, root: string): Promise<{ card: Locator; commands: Locator; tree: Locator }> {
  const view = await connect(page, value);
  await view.commands.locator(".command-list button").first().click();
  await view.commands.getByRole("button", { name: "查看命令目录成果", exact: true }).click();
  await view.commands.locator(".command-directory-row").filter({ has: page.locator(`p`, { hasText: root + " · mode" }) }).first()
    .getByRole("button", { name: "审阅目录树成果", exact: true }).click();
  await expect(view.tree.locator(".tree-source")).toContainText(root); return view;
}
async function prepare(page: Page, tree: Locator, key?: string): Promise<string> {
  if (key) await tree.getByLabel("目录树原请求键（可留空自动生成）", { exact: true }).fill(key);
  await confirm(page, tree, "准备目录树计划");
  await expect(tree.locator(".tree-detail > strong")).toContainText("目录树待启动");
  const id = await tree.locator(".tree-detail").getAttribute("data-plan-id"); expect(id).toBeTruthy(); return id!;
}
async function get(page: Page, value: Agent, id: string): Promise<Plan> {
  const response = await page.request.get(`${base(value)}/${id}`, { headers }); expect(response.ok()).toBeTruthy(); return response.json();
}
async function steps(page: Page, value: Agent, id: string): Promise<Step[]> {
  const response = await page.request.get(`${base(value)}/${id}/steps`, { headers }); expect(response.ok()).toBeTruthy(); return (await response.json()).items;
}
async function stat(page: Page, root: string) {
  const result = await page.request.post("/_test/directory", { headers, data: { action: "stat", path: root } }); expect(result.ok()).toBeTruthy(); return result.json();
}
async function destroy(page: Page, value: Agent): Promise<void> { await page.request.delete(`/v1/agents/${value.agent_id}`, { headers }); }

test("真实浏览器准备零效果、两来源发布、原操作导航与重载零重放", async ({ page }) => {
  const value = await create(page, "tree-browser-main"), root = "tree-browser-main";
  let prepares = 0, starts = 0;
  page.on("request", item => { if (item.method() === "POST" && item.url().includes(base(value))) {
    if (/tree-plans\?/.test(item.url())) prepares++; if (/\/start\?/.test(item.url())) starts++;
  } });
  try {
    const view = await connect(page, value);
    await view.commands.getByLabel("命令参数（JSON字符串数组）", { exact: true }).fill(JSON.stringify(["/usr/bin/python3", "-I", "-c", code(root)]));
    await confirm(page, view.commands, "执行受控命令");
    await expect(view.commands.locator(".command-detail > strong")).toContainText("命令已完成", { timeout: 25_000 });
    await view.commands.getByRole("button", { name: "查看命令目录成果", exact: true }).click();
    await view.commands.locator(`.command-directory-row[data-path="${root}"]`).getByRole("button", { name: "审阅目录树成果", exact: true }).click();
    const key = '<img src=x onerror="window.treeExecuted=1">中文?/#%';
    const id = await prepare(page, view.tree, key);
    expect((await stat(page, root)).found).toBe(false);
    const pending = await steps(page, value, id); expect(pending).toHaveLength(4);
    expect(pending.every(item => item.operation === null && item.proposal_id === null && item.status === "pending")).toBe(true);
    await expect(view.tree.locator(".tree-steps img")).toHaveCount(0); await expect(view.tree.locator(".tree-meta img")).toHaveCount(0);
    expect(await page.evaluate(() => (window as unknown as { treeExecuted?: number }).treeExecuted)).toBeUndefined();
    await confirm(page, view.tree, "确认启动原目录树", false);
    expect((await get(page, value, id)).status).toBe("planned"); expect(starts).toBe(0);
    await confirm(page, view.tree, "确认启动原目录树");
    await expect(view.tree.locator(".tree-detail > strong")).toContainText("目录树已确认", { timeout: 30_000 });
    expect((await stat(page, root)).mode).toBe(0o750);
    const done = await steps(page, value, id); expect(done.every(item => item.status === "done" && item.operation !== null)).toBe(true);
    expect(done.find(item => item.path.endsWith("data.bin"))!.operation!.receipt!.after_sha256).toBe(createHash("sha256").update(Buffer.from([0, 255])).digest("hex"));
    const file = await page.request.get(`/v1/agents/${value.agent_id}/workspace/read?${new URLSearchParams({ path: root + "/sub/data.txt" })}`, { headers });
    expect((await file.json()).content).toBe("browser tree data\n");
    const originalFile = done.find(item => item.path.endsWith("data.txt"))!;
    await view.tree.locator(`.tree-steps [data-position="${originalFile.position}"]`).getByRole("button", { name: "查看原文件提案", exact: true }).click();
    await expect(view.card.locator(".workspace-operation-result")).toContainText(originalFile.operation!.operation_id);
    await expect(view.card.locator(".workspace-diff")).toContainText("browser tree data");
    const originalRoot = done[0]!;
    await view.tree.locator('.tree-steps [data-position="0"]').getByRole("button", { name: "查看原目录提案", exact: true }).click();
    await expect(view.card.locator(".directory-operation")).toContainText(originalRoot.operation!.operation_id);
    await expect(view.tree.getByRole("button", { name: "确认启动原目录树", exact: true })).toBeDisabled();
    const restored = await connect(page, value);
    await restored.tree.getByText("目录树审阅与控制", { exact: true }).click();
    await expect(restored.tree.locator(".tree-detail > strong")).toContainText("目录树已确认");
    await expect(restored.tree.locator(".tree-meta")).toContainText(key);
    expect((await steps(page, value, id)).map(item => item.operation!.operation_id)).toEqual(done.map(item => item.operation!.operation_id));
    expect(prepares).toBe(1); expect(starts).toBe(1);
    await restored.tree.locator(".tree-detail > strong").scrollIntoViewIfNeeded();
    await page.screenshot({ path: test.info().outputPath("tree-review-page.png"), fullPage: true });
  } finally { await destroy(page, value); }
});

test("准备响应丢失后原键冷恢复，存储仅有摘要且无第二准备或启动", async ({ page }) => {
  const value = await create(page, "tree-browser-lost-prepare"), root = "tree-lost-prepare";
  let posts = 0, key = "";
  try {
    await source(page, value, root); const view = await choose(page, value, root);
    await page.route(`**${base(value)}?confirmed=true`, async route => {
      posts++; key = route.request().postDataJSON().request_key; await route.fetch(); await route.abort("failed");
    });
    await confirm(page, view.tree, "准备目录树计划");
    await expect(view.tree.locator(".tree-pending")).toContainText("目录树响应未确认");
    const saved = await page.evaluate(k => sessionStorage.getItem(k), `krim-tree-recovery:${value.agent_id}`);
    expect(JSON.parse(saved!).pending).toEqual({ kind: "prepare", key, digest: expect.stringMatching(/^[0-9a-f]{64}$/) });
    expect(saved).not.toContain("source_hash"); expect(saved).not.toContain("browser-test-key"); expect(saved).not.toContain("python");
    const restored = await connect(page, value); await restored.tree.getByText("目录树审阅与控制", { exact: true }).click();
    await expect(restored.tree.locator(".tree-detail > strong")).toContainText("目录树待启动");
    await expect(restored.tree.locator(".tree-meta")).toContainText(key); expect(posts).toBe(1);
    const id = (await restored.tree.locator(".tree-detail").getAttribute("data-plan-id"))!;
    expect((await steps(page, value, id)).every(item => item.operation === null)).toBe(true); expect((await stat(page, root)).found).toBe(false);
  } finally { await destroy(page, value); }
});

test("准备前断线查询为空后仅显式同键同参数重试，改容量拒绝", async ({ page }) => {
  const value = await create(page, "tree-browser-original-retry"), root = "tree-original-retry";
  const bodies: unknown[] = [];
  try {
    await source(page, value, root); const view = await choose(page, value, root);
    await page.route(`**${base(value)}?confirmed=true`, async route => {
      bodies.push(route.request().postDataJSON()); if (bodies.length === 1) await route.abort("failed"); else await route.continue();
    });
    await confirm(page, view.tree, "准备目录树计划");
    await expect(view.tree.locator(".tree-pending")).toContainText("目录树响应未确认");
    await view.tree.getByRole("button", { name: "查询未确认目录树", exact: true }).click();
    await expect(view.tree.locator(".tree-status")).toContainText("未找到原目录树");
    await view.tree.getByLabel("目录树步骤容量", { exact: true }).fill("16");
    await view.tree.getByRole("button", { name: "准备目录树计划", exact: true }).click();
    await expect(view.tree.locator(".tree-status")).toContainText("只能使用原参数与原请求键"); expect(bodies).toHaveLength(1);
    await view.tree.getByLabel("目录树步骤容量", { exact: true }).fill("1024");
    const id = await prepare(page, view.tree); expect(bodies).toHaveLength(2); expect(bodies[0]).toEqual(bodies[1]);
    expect((await get(page, value, id)).status).toBe("planned"); expect((await stat(page, root)).found).toBe(false);
  } finally { await destroy(page, value); }
});

test("启动响应丢失后只读wait和重载，同一生产者取消且后代零效果", async ({ page }) => {
  const value = await create(page, "tree-browser-lost-start"), root = "tree-lost-start";
  let starts = 0;
  try {
    await source(page, value, root); const view = await choose(page, value, root); const id = await prepare(page, view.tree);
    expect((await page.request.post("/_test/native-batch", { headers, data: { agent_id: value.agent_id, action: "block" } })).ok()).toBeTruthy();
    await page.route(`**${base(value)}/${id}/start?confirmed=true`, async route => { starts++; await route.fetch(); await route.abort("failed"); });
    await confirm(page, view.tree, "确认启动原目录树");
    await expect(view.tree.locator(".tree-pending")).toContainText("目录树响应未确认");
    await expect.poll(async () => (await (await page.request.post("/_test/native-batch", { headers, data: { agent_id: value.agent_id, action: "status" } })).json()).entered).toBe(true);
    const restored = await connect(page, value); await restored.tree.getByText("目录树审阅与控制", { exact: true }).click();
    await expect(restored.tree.locator(".tree-detail > strong")).toContainText("目录树执行中");
    await restored.tree.getByRole("button", { name: "等待原目录树", exact: true }).click();
    await expect(restored.tree.locator(".tree-detail > strong")).toContainText("目录树执行中"); expect(starts).toBe(1);
    await confirm(page, restored.tree, "取消原目录树", false); expect((await get(page, value, id)).producer_status).toBe("running");
    await confirm(page, restored.tree, "取消原目录树");
    await expect(restored.tree.locator(".tree-detail > strong")).toContainText("目录树已取消");
    const final = await steps(page, value, id); expect(final.every(item => item.operation === null && item.status === "skipped")).toBe(true);
    expect((await stat(page, root)).found).toBe(false); expect(starts).toBe(1);
    await expect(restored.tree.getByRole("button", { name: "确认启动原目录树", exact: true })).toBeDisabled();
  } finally { await page.request.post("/_test/native-batch", { headers, data: { agent_id: value.agent_id, action: "release" } }); await destroy(page, value); }
});

test("unknown导航原目录核实、原operation保持、关闭树不复活", async ({ page }) => {
  const value = await create(page, "tree-browser-unknown"), root = "tree-browser-unknown";
  let starts = 0;
  page.on("request", item => { if (item.method() === "POST" && item.url().includes(base(value) + "/") && /\/start\?/.test(item.url())) starts++; });
  try {
    await source(page, value, root); const view = await choose(page, value, root); const id = await prepare(page, view.tree);
    expect((await page.request.post("/_test/directory", { headers, data: { action: "fail_save" } })).ok()).toBeTruthy();
    await confirm(page, view.tree, "确认启动原目录树");
    await expect(view.tree.locator(".tree-detail > strong")).toContainText("目录树结果待核实", { timeout: 30_000 });
    const original = (await steps(page, value, id))[0]!; expect(original.status).toBe("uncertain"); expect(original.operation).not.toBeNull();
    expect((await stat(page, root)).found).toBe(true);
    await view.tree.locator('.tree-steps [data-position="0"]').getByRole("button", { name: "查看原目录提案", exact: true }).click();
    const directories = view.card.locator(".directory-changes");
    await expect(directories.locator(".directory-operation")).toContainText(original.operation!.operation_id);
    await confirm(page, directories, "核实目录结果");
    await expect(directories.locator(".directory-operation > strong")).toContainText("目录操作已确认");
    await view.tree.getByRole("button", { name: "刷新原目录树", exact: true }).click();
    await expect(view.tree.locator(".tree-detail > strong")).toContainText("目录树部分完成");
    const cold = await steps(page, value, id); expect(cold[0]!.operation!.operation_id).toBe(original.operation!.operation_id);
    expect(cold[0]!.status).toBe("done"); expect(cold.slice(1).every(item => item.operation === null)).toBe(true);
    await expect(view.tree.getByRole("button", { name: "确认启动原目录树", exact: true })).toBeDisabled(); expect(starts).toBe(1);
  } finally { await destroy(page, value); }
});

test("失败成果须明确接受，34文件步骤分页与Locale草稿、关闭会话冷读", async ({ page }) => {
  const value = await create(page, "tree-browser-pages"), root = "tree-browser-pages";
  try {
    const html = '<img src=x onerror="window.treePathExecuted=1">.txt';
    const script = `import os;os.mkdir(${JSON.stringify(root)});[(open(${JSON.stringify(root)}+'/f%02d.txt'%i,'w').write(str(i))) for i in range(33)];open(${JSON.stringify(root + "/" + html)},'w').write('html path');raise SystemExit(3)`;
    await source(page, value, root, true, script); const view = await choose(page, value, root);
    await expect(view.tree.getByRole("button", { name: "准备目录树计划", exact: true })).toBeDisabled();
    await view.tree.getByLabel("明确接受失败命令的完整目录树成果", { exact: true }).check();
    const id = await prepare(page, view.tree);
    await expect(view.tree.locator(".tree-steps li")).toHaveCount(32);
    await expect(view.tree.locator(".tree-steps")).toContainText(html); await expect(view.tree.locator(".tree-steps img")).toHaveCount(0);
    expect(await page.evaluate(() => (window as unknown as { treePathExecuted?: number }).treePathExecuted)).toBeUndefined();
    await view.tree.getByRole("button", { name: "下一页目录树步骤", exact: true }).click();
    await expect(view.tree.locator(".tree-steps li")).toHaveCount(3); await expect(view.tree.locator(".tree-page-info")).toContainText("页2");
    await view.tree.getByRole("button", { name: "上一页目录树步骤", exact: true }).click(); await expect(view.tree.locator(".tree-steps li")).toHaveCount(32);
    await view.tree.getByLabel("目录树标识或原请求键", { exact: true }).fill("query draft 中文");
    await view.tree.getByLabel("目录树步骤容量", { exact: true }).fill("128");
    await page.locator("#locale-select").selectOption("en-US");
    await expect(view.tree.getByLabel("Tree ID or original request key", { exact: true })).toHaveValue("query draft 中文");
    await expect(view.tree.getByLabel("Tree step capacity", { exact: true })).toHaveValue("128");
    await expect(view.tree.locator(".tree-detail > strong")).toContainText("Directory tree planned");
    await view.card.getByRole("button", { name: "Close session", exact: true }).click();
    await expect(view.card).toContainText("Session closed");
    await view.tree.getByRole("button", { name: "Refresh original tree", exact: true }).click();
    await expect(view.tree.locator(".tree-detail > strong")).toContainText("Directory tree planned");
    await expect(view.tree.getByRole("button", { name: "Confirm original tree execution", exact: true })).toBeDisabled();
    expect((await get(page, value, id)).producer_status).toBe("planned"); expect((await stat(page, root)).found).toBe(false);
  } finally { await destroy(page, value); }
});

test("跨scope实际拒绝、伪回程清空可写详情与无效恢复记录关闭写入", async ({ page }) => {
  const value = await create(page, "tree-browser-boundary"), other = await create(page, "tree-browser-other"), root = "tree-browser-boundary";
  try {
    await source(page, value, root); const view = await choose(page, value, root); const id = await prepare(page, view.tree, "tree-boundary-key");
    expect((await page.request.get(`${base(other)}/${id}`, { headers })).status()).toBe(404);
    await view.tree.getByLabel("目录树标识或原请求键", { exact: true }).fill("tree-boundary-key");
    await page.route(`**${base(value)}/by-key?*`, async route => {
      const response = await route.fetch(); const body = await response.json(); body.scope_id = "session:" + other.session_id;
      await route.fulfill({ response, json: body });
    });
    await view.tree.getByRole("button", { name: "按原键找回目录树", exact: true }).click();
    await expect(view.tree.locator(".tree-status")).toContainText("目录树回程与原来源不一致"); await expect(view.tree.locator(".tree-detail > strong")).toHaveCount(0);
    await page.unroute(`**${base(value)}/by-key?*`);
    await page.route(`**${base(value)}/${id}/steps?*`, async route => {
      const response = await route.fetch(); const body = await response.json(); body.items[1].parent_position = body.items[1].position;
      await route.fulfill({ response, json: body });
    });
    await view.tree.getByRole("button", { name: "刷新原目录树", exact: true }).click();
    await expect(view.tree.locator(".tree-status")).toContainText("目录树步骤回程与原计划不一致"); await expect(view.tree.locator(".tree-detail > strong")).toHaveCount(0);
    await page.unroute(`**${base(value)}/${id}/steps?*`);
    await page.evaluate(k => sessionStorage.setItem(k, JSON.stringify({ version: 1, owner: "forged", selected: { id: "foreign", key: "foreign" } })), `krim-tree-recovery:${value.agent_id}`);
    const restored = await connect(page, value); await restored.tree.getByText("目录树审阅与控制", { exact: true }).click();
    await expect(restored.tree.locator(".tree-pending")).toContainText("写入已关闭");
    await restored.tree.getByLabel("目录树标识或原请求键", { exact: true }).fill("tree-boundary-key");
    await restored.tree.getByRole("button", { name: "按原键找回目录树", exact: true }).click();
    await expect(restored.tree.locator(".tree-detail > strong")).toContainText("目录树待启动");
    await expect(restored.tree.getByRole("button", { name: "确认启动原目录树", exact: true })).toBeDisabled();
    await confirm(page, restored.tree, "清除无效目录树恢复记录");
    await expect(restored.tree.getByRole("button", { name: "确认启动原目录树", exact: true })).toBeEnabled();
    const cache = await page.evaluate(k => JSON.parse(sessionStorage.getItem(k)!), `krim-tree-recovery:${value.agent_id}`);
    expect(cache.selected).toEqual({ id, key: "tree-boundary-key" });
    expect((await get(page, value, id)).status).toBe("planned"); expect((await stat(page, root)).found).toBe(false);
  } finally { await destroy(page, other); await destroy(page, value); }
});

test("原工具未授权与plan/ask模式保持写入拒绝", async ({ page }) => {
  const value = await create(page, "tree-browser-no-tool", "agent", tools.filter(name => name !== "workspace_tree_prepare"));
  try {
    await source(page, value, "tree-browser-no-tool"); const view = await connect(page, value);
    await view.commands.locator(".command-list button").first().click(); await view.commands.getByRole("button", { name: "查看命令目录成果", exact: true }).click();
    for (const button of await view.commands.getByRole("button", { name: "审阅目录树成果", exact: true }).all()) await expect(button).toBeDisabled();
    await view.tree.getByText("目录树审阅与控制", { exact: true }).click();
    await view.tree.getByLabel("目录树标识或原请求键", { exact: true }).fill("missing-original");
    await view.tree.getByRole("button", { name: "按原键找回目录树", exact: true }).click();
    await expect(view.tree.locator(".tree-status")).toContainText("未找到原目录树");
    const history = (await (await page.request.get(`/v1/agents/${value.agent_id}/commands`, { headers })).json()).entries;
    const catalog = await (await page.request.get(`/v1/agents/${value.agent_id}/commands/${history[0].command_id}/directory-changes`, { headers })).json();
    const denied = await page.request.post(`${base(value)}?confirmed=true`, { headers, data: {
      command_id: history[0].command_id, source_hash: catalog.source_hash, root_path: "tree-browser-no-tool", request_key: "denied-original",
    } }); expect(denied.status()).toBe(403); expect((await stat(page, "tree-browser-no-tool")).found).toBe(false);
  } finally { await destroy(page, value); }
  for (const mode of ["plan", "ask"]) {
    const restricted = await create(page, "tree-browser-mode-" + mode, mode);
    try {
      const view = await connect(page, restricted); await view.tree.getByText("目录树审阅与控制", { exact: true }).click();
      await view.tree.getByLabel("目录树标识或原请求键", { exact: true }).fill("missing-original");
      if (mode === "ask") await expect(view.tree.getByRole("button", { name: "按原键找回目录树", exact: true })).toBeDisabled();
      else {
        await view.tree.getByRole("button", { name: "按原键找回目录树", exact: true }).click();
        await expect(view.tree.locator(".tree-status")).toContainText("未找到原目录树");
      }
      await expect(view.commands.getByRole("button", { name: "执行受控命令", exact: true })).toBeDisabled();
    } finally { await destroy(page, restricted); }
  }
});

test("容量真实拒绝后原键空查询与明确放弃仅解锁本地，更正容量须新确认", async ({ page }) => {
  const value = await create(page, "tree-browser-capacity"), root = "tree-browser-capacity";
  let posts = 0;
  page.on("request", item => { if (item.method() === "POST" && item.url().includes(base(value) + "?")) posts++; });
  try {
    await source(page, value, root); const view = await choose(page, value, root);
    await view.tree.getByLabel("目录树原请求键（可留空自动生成）", { exact: true }).fill("capacity-rejected");
    await view.tree.getByLabel("目录树步骤容量", { exact: true }).fill("1");
    await confirm(page, view.tree, "准备目录树计划");
    await expect(view.tree.locator(".tree-pending")).toContainText("目录树响应未确认"); expect(posts).toBe(1);
    await view.tree.getByRole("button", { name: "查询未确认目录树", exact: true }).click();
    await expect(view.tree.locator(".tree-status")).toContainText("未找到原目录树");
    await confirm(page, view.tree, "放弃未找到的本地准备记录", false);
    await expect(view.tree.locator(".tree-pending")).toContainText("目录树响应未确认");
    await confirm(page, view.tree, "放弃未找到的本地准备记录");
    await expect(view.tree.locator(".tree-status")).toContainText("原键仍可查询：capacity-rejected"); expect(posts).toBe(1);
    expect((await (await page.request.get(`${base(value)}/by-key?request_key=capacity-rejected`, { headers })).json())).toBeNull();
    await view.tree.getByLabel("目录树步骤容量", { exact: true }).fill("8");
    const id = await prepare(page, view.tree, "capacity-corrected"); expect(posts).toBe(2);
    expect((await get(page, value, id)).status).toBe("planned"); expect((await stat(page, root)).found).toBe(false);
  } finally { await destroy(page, value); }
});
