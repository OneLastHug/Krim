import { destroyAgent } from "./session-fixtures.js";
import { expect, test, type Locator, type Page } from "@playwright/test";

const headers = { Authorization: "Bearer browser-test-key" };
const tools = ["tree_exchange_inspect", "tree_exchange_seal", "tree_exchange_seal_query", "tree_exchange_preview", "tree_exchange_query", "tree_exchange_change", "tree_exchange_reconcile"];
const generation = ["workspace_read", "workspace_preview", "workspace_change", "workspace_reconcile", "workspace_finalize", "command_execute", "command_get", "command_changes", "command_propose", "directory_source_preview", "directory_source_query", "directory_query", "directory_change", "directory_reconcile", "directory_finalize", "workspace_tree_prepare", "workspace_tree_model_prepare", "workspace_tree_start", "workspace_tree_query"];
type Agent = { id: string; topic: string; panel: Locator };
test.setTimeout(90_000);

async function control(page: Page, action: string, path = "tree", id?: string, right = "right", version = "old") {
  const response = await page.request.post("/_test/tree-exchange", { headers, data: { action, path, agent_id: id, right_path: right, version } });
  expect(response.ok()).toBeTruthy(); return response.json();
}
async function connect(page: Page, topic: string): Promise<void> {
  await page.goto(`/ui/?topic=${encodeURIComponent(topic)}`);
  await page.getByLabel("服务访问密钥", { exact: true }).fill("browser-test-key");
  await page.getByRole("button", { name: "连接", exact: true }).click();
  await expect(page.locator("#connection")).toHaveClass(/ready/);
}
async function actor(page: Page, name: string, mode = "agent", selectedTools = tools, workspace = "edit"): Promise<Agent> {
  const response = await page.request.put(`/v1/agents/profiles/${name}`, { headers, data: { expected_revision: 0, config: {
    name, description: "实际双完整树审阅", workspace_key: workspace, tool_names: selectedTools, mode,
    max_steps: 16, tool_timeout_seconds: 30, timeout_seconds: 120,
    ...(selectedTools.includes("command_execute") ? { execution_key: "sandbox" } : {}),
    context_config: { tokenizer: "cl100k_base", reserved_output_tokens: 128 },
  } } }); expect(response.ok()).toBeTruthy();
  const spawned = await page.request.post("/v1/agents", { headers, data: { name } }); expect(spawned.ok()).toBeTruthy();
  const view = await spawned.json(); await connect(page, view.topic_id);
  const panel = page.locator(`article.agent-card[data-agent-id="${view.agent_id}"] .tree-exchanges`);
  await panel.getByText("双完整目录树交换与历史", { exact: true }).click();
  return { id: view.agent_id, topic: view.topic_id, panel };
}
async function destroy(page: Page, id: string) { await destroyAgent(page, id); }
async function inspect(panel: Locator, left: string, right: string) {
  await panel.getByLabel("左目录树相对路径", { exact: true }).fill(left);
  await panel.getByRole("button", { name: "检查左完整目录树", exact: true }).click();
  await expect(panel.locator(".tree-exchange-summary")).toContainText(left);
  await panel.getByLabel("右目录树相对路径", { exact: true }).fill(right);
  await panel.getByRole("button", { name: "检查右完整目录树", exact: true }).click();
  await expect(panel.locator(".tree-exchange-summary")).toContainText(right);
}
async function prepare(panel: Locator, left: string, right: string) {
  await inspect(panel, left, right);
  await panel.getByRole("button", { name: "预览双树交换", exact: true }).click();
  await expect(panel.locator(".tree-exchange-detail > strong")).toContainText("双树交换待确认");
}
async function confirm(page: Page, panel: Locator, label: string, accept = true): Promise<string> {
  let message = ""; let done!: () => void;
  const completed = new Promise<void>(resolve => { done = resolve; });
  page.once("dialog", dialog => { message = dialog.message(); void (accept ? dialog.accept() : dialog.dismiss()).finally(done); });
  await panel.getByRole("button", { name: label, exact: true }).click(); await completed; return message;
}
async function generate(page: Page, id: string, root: string) {
  const base = `/v1/agents/${id}`;
  const command = await page.request.post(`${base}/commands?confirmed=true`, { headers, data: {
    request_key: `source-${root}`, command: { argv: ["/usr/bin/python3", "-I", "-c", `import os;os.makedirs('${root}/sub');os.chmod('${root}',0o750);os.chmod('${root}/sub',0o700);open('${root}/sub/data.bin','wb').write(bytes([0,255]))`] },
  } }); expect(command.ok()).toBeTruthy(); const original = await command.json();
  const waited = await page.request.post(`${base}/commands/${original.command_id}/wait?timeout_seconds=30`, { headers }); expect(waited.ok()).toBeTruthy(); expect((await waited.json()).outcome).toBe("completed");
  const catalog = await page.request.get(`${base}/commands/${original.command_id}/directory-changes`, { headers }); expect(catalog.ok()).toBeTruthy();
  const prepared = await page.request.post(`${base}/workspace/tree-plans?confirmed=true`, { headers, data: {
    command_id: original.command_id, source_hash: (await catalog.json()).source_hash, root_path: root, request_key: `plan-${root}`,
  } }); expect(prepared.ok()).toBeTruthy(); const plan = await prepared.json();
  const started = await page.request.post(`${base}/workspace/tree-plans/${plan.plan_id}/start?confirmed=true`, { headers }); expect(started.ok()).toBeTruthy();
  const finished = await page.request.post(`${base}/workspace/tree-plans/${plan.plan_id}/wait?timeout_seconds=30`, { headers }); expect(finished.ok()).toBeTruthy(); expect((await finished.json()).status).toBe("done");
  return plan.plan_id as string;
}

test("双完整树双指纹/二进制原inode、确认保留撤销和Locale", async ({ page }) => {
  const left = "exchange-normal-left", right = "exchange-normal-right";
  const old = await control(page, "seed", left), fresh = await control(page, "seed", right, undefined, "right", "new");
  const { id, panel } = await actor(page, "exchange-normal");
  try {
    await prepare(panel, left, right);
    await expect(panel.locator(".tree-exchange-condition")).toContainText("原左树指纹");
    await expect(panel.locator(".tree-exchange-condition")).toContainText("原右树指纹");
    await confirm(page, panel, "应用双树交换", false);
    expect(await control(page, "counts", "tree", id)).toMatchObject({ proposals: 1, operations: 0 });
    expect(await confirm(page, panel, "应用双树交换")).toContain(`${left} → ${right}`);
    await expect(panel.locator(".tree-exchange-detail > strong")).toContainText("双树已交换");
    expect(await control(page, "stat", left)).toMatchObject({ inode: fresh.inode, binary_inode: fresh.binary_inode, binary_hex: "01fe00" });
    expect(await control(page, "stat", right)).toMatchObject({ inode: old.inode, binary_inode: old.binary_inode, binary_hex: "00ff80" });
    await confirm(page, panel, "保留双树交换"); await expect(panel.locator(".tree-exchange-detail > strong")).toContainText("双树交换已保留");
    await expect(panel.locator(".tree-exchange-receipt")).toContainText("未产生工作区变化");
    await page.locator("#locale-select").selectOption("en-US");
    await expect(panel.getByLabel("Left tree relative path", { exact: true })).toHaveValue(left);
    await expect(panel.getByRole("button", { name: "Undo tree exchange", exact: true })).toBeEnabled();
    await page.locator("#locale-select").selectOption("zh-CN");
    await confirm(page, panel, "撤销双树交换"); await expect(panel.locator(".tree-exchange-detail > strong")).toContainText("双树已撤销交换");
    expect(await control(page, "stat", left)).toMatchObject({ inode: old.inode, binary_inode: old.binary_inode, binary_hex: "00ff80" });
    expect(await control(page, "stat", right)).toMatchObject({ inode: fresh.inode, binary_inode: fresh.binary_inode, binary_hex: "01fe00" });
    expect(await control(page, "counts", "tree", id)).toMatchObject({ proposals: 1, operations: 3 });
    await expect(panel.locator(".tree-exchange-history li")).toHaveCount(3);
    await panel.screenshot({ path: test.info().outputPath("complete-tree-exchange.png") });
    await page.setViewportSize({ width: 420, height: 860 });
    await panel.locator(".tree-exchange-detail > strong").scrollIntoViewIfNeeded();
    await page.screenshot({ path: test.info().outputPath("tree-exchange-narrow.png") });
    expect(await panel.evaluate(node => node.scrollWidth <= node.clientWidth + 2)).toBe(true);
  } finally { await destroy(page, id); }
});

test("非法路径/祖孙双树、plan写拒绝及未加载工具", async ({ page }) => {
  const left = "exchange-plan-left", right = "exchange-plan-right";
  await control(page, "seed", left); await control(page, "seed", right, undefined, "right", "new");
  const { id, panel } = await actor(page, "exchange-plan", "plan");
  try {
    await panel.getByLabel("左目录树相对路径", { exact: true }).fill("../escape");
    await expect(panel.getByRole("button", { name: "检查左完整目录树", exact: true })).toBeDisabled();
    await inspect(panel, left, right);
    for (const path of [left, `${left}/inside`, "../escape"]) {
      await panel.getByLabel("右目录树相对路径", { exact: true }).fill(path);
      await expect(panel.getByRole("button", { name: "预览双树交换", exact: true })).toBeDisabled();
    }
    await prepare(panel, left, right);
    await expect(panel.getByRole("button", { name: "应用双树交换", exact: true })).toBeDisabled();
    expect(await control(page, "counts", "tree", id)).toMatchObject({ operations: 0 });
  } finally { await destroy(page, id); }
  const restricted = await actor(page, "exchange-restricted", "agent", ["tree_exchange_inspect"]);
  try {
    await expect(restricted.panel.locator(".tree-exchange-status")).toContainText("当前会话未授权双树交换查询");
    await expect(restricted.panel.getByRole("button", { name: "刷新双树交换历史", exact: true })).toBeDisabled();
  } finally { await destroy(page, restricted.id); }
});

test("丢预览与动作回程、刷新仅GET原键且0第二操作", async ({ page }) => {
  const left = "exchange-lost-left", right = "exchange-lost-right";
  await control(page, "seed", left); await control(page, "seed", right, undefined, "right", "new");
  const { id, topic, panel } = await actor(page, "exchange-lost");
  let previews = 0, changes = 0;
  try {
    await page.route("**/workspace/tree-exchanges/preview?**", async route => { previews++; await route.fetch(); await route.abort("failed"); });
    await inspect(panel, left, right); await panel.getByRole("button", { name: "预览双树交换", exact: true }).click();
    await expect(panel.locator(".tree-exchange-pending")).toContainText("树响应尚未确认");
    await panel.getByRole("button", { name: "查询待确认树请求", exact: true }).click();
    await expect(panel.locator(".tree-exchange-detail > strong")).toContainText("双树交换待确认");
    await page.unroute("**/workspace/tree-exchanges/preview?**");
    await page.route("**/workspace/tree-exchanges/change?**", async route => { changes++; await route.fetch(); await route.abort("failed"); });
    await confirm(page, panel, "应用双树交换");
    await expect(panel.locator(".tree-exchange-pending")).toContainText("树响应尚未确认");
    await connect(page, topic); const restored = page.locator(`article.agent-card[data-agent-id="${id}"] .tree-exchanges`);
    await restored.getByText("双完整目录树交换与历史", { exact: true }).click();
    await expect(restored.locator(".tree-exchange-detail > strong")).toContainText("双树已交换");
    expect(previews).toBe(1); expect(changes).toBe(1);
    expect(await control(page, "counts", "tree", id)).toMatchObject({ proposals: 1, operations: 1 });
  } finally { await destroy(page, id); }
});

test("真实生成源seal、丢封存回程后仅GET原plan恢复和完整来源交换", async ({ page }) => {
  const left = "exchange-source-left", right = "exchange-source-right"; await control(page, "seed", left);
  const { id, topic, panel } = await actor(page, "exchange-source", "agent", [...tools, ...generation]);
  let seals = 0;
  try {
    const plan = await generate(page, id, right);
    await inspect(panel, left, right);
    await panel.getByLabel("原生成计划标识（可选）", { exact: true }).fill(plan);
    await expect(panel.getByRole("button", { name: "预览双树交换", exact: true })).toBeDisabled();
    await page.route("**/workspace/tree-exchanges/seal?**", async route => { seals++; await route.fetch(); await route.abort("failed"); });
    await confirm(page, panel, "封存原生成完整树");
    await expect(panel.locator(".tree-exchange-pending")).toContainText("树响应尚未确认");
    await connect(page, topic); const restored = page.locator(`article.agent-card[data-agent-id="${id}"] .tree-exchanges`);
    await restored.getByText("双完整目录树交换与历史", { exact: true }).click();
    await expect(restored.locator(".tree-exchange-origin")).toContainText(plan);
    expect(seals).toBe(1); expect(await control(page, "counts", "tree", id)).toMatchObject({ seals: 1, proposals: 0 });
    await page.unroute("**/workspace/tree-exchanges/seal?**");
    await prepare(restored, left, right);
    await expect(restored.locator(".tree-exchange-condition")).toContainText(plan);
    await confirm(page, restored, "应用双树交换"); await expect(restored.locator(".tree-exchange-detail > strong")).toContainText("双树已交换");
    expect((await control(page, "stat", left)).binary_hex).toBe("00ff");
    expect(await control(page, "counts", "tree", id)).toMatchObject({ seals: 1, origins: 1, proposals: 1, operations: 1 });
  } finally { await destroy(page, id); }
});

test("回执保存失败unknown仅核实原operation", async ({ page }) => {
  const left = "exchange-unknown-left", right = "exchange-unknown-right";
  await control(page, "seed", left); await control(page, "seed", right, undefined, "right", "new");
  const { id, panel } = await actor(page, "exchange-unknown");
  try {
    await prepare(panel, left, right); await control(page, "fail_save");
    await confirm(page, panel, "应用双树交换"); await expect(panel.locator(".tree-exchange-detail > strong")).toContainText("树结果待核实");
    expect((await control(page, "stat", left)).binary_hex).toBe("01fe00");
    await confirm(page, panel, "核实原树结果"); await expect(panel.locator(".tree-exchange-detail > strong")).toContainText("双树已交换");
    expect(await control(page, "counts", "tree", id)).toMatchObject({ proposals: 1, operations: 1 });
  } finally { await destroy(page, id); }
});

test("等待原写回程时取消、同线程收束保存真实拒绝", async ({ page }) => {
  const left = "exchange-cancel-left", right = "exchange-cancel-right";
  await control(page, "seed", left); await control(page, "seed", right, undefined, "right", "new");
  const { id, panel } = await actor(page, "exchange-cancel");
  try {
    await prepare(panel, left, right); await control(page, "pause", "tree", id);
    await confirm(page, panel, "应用双树交换");
    await expect.poll(async () => (await control(page, "gate", "tree", id)).entered).toBe(true);
    await expect(panel.getByRole("button", { name: "取消进行中的双树交换", exact: true })).toBeEnabled();
    await confirm(page, panel, "取消进行中的双树交换");
    await expect.poll(async () => (await control(page, "counts", "tree", id)).cancelled).toBe(1);
    await control(page, "release", "tree", id);
    await expect(panel.locator(".tree-exchange-detail > strong")).toContainText("双树交换已取消");
    expect((await control(page, "stat", left)).binary_hex).toBe("00ff80");
    expect(await control(page, "counts", "tree", id)).toMatchObject({ proposals: 1, operations: 1 });
  } finally { await control(page, "release", "tree", id); await destroy(page, id); }
});

test("右树外部版本变化拒绝原批准、不交换", async ({ page }) => {
  const left = "exchange-conflict-left", right = "exchange-conflict-right";
  const old = await control(page, "seed", left); await control(page, "seed", right, undefined, "right", "new");
  const { id, panel } = await actor(page, "exchange-conflict");
  try {
    await prepare(panel, left, right); await control(page, "mutate", right);
    await confirm(page, panel, "应用双树交换");
    await expect(panel.locator(".tree-exchange-receipt")).toContainText("树操作已拒绝");
    expect(await control(page, "stat", left)).toMatchObject({ inode: old.inode, binary_hex: "00ff80" });
    expect((await control(page, "stat", right)).binary_hex).toBe("6368616e676564");
  } finally { await destroy(page, id); }
});

test("历史稳定分页、HTML路径纯文本与无注入", async ({ page }) => {
  const html = await control(page, "html", "tree"), right = "exchange-html-right"; await control(page, "seed", right, undefined, "right", "new");
  const { id, panel } = await actor(page, "exchange-html");
  try {
    await prepare(panel, html.path, right);
    await expect(panel.locator(".tree-exchange-condition")).toContainText("<img src=x");
    await expect(panel.locator("img")).toHaveCount(0); expect(await page.evaluate(() => (window as unknown as { treeInjected?: number }).treeInjected)).toBeUndefined();
    const left = "exchange-pages-left", target = "exchange-pages-right";
    await control(page, "seed", left); await control(page, "seed", target, undefined, "right", "new");
    await control(page, "catalog", left, id, target);
    await panel.getByRole("button", { name: "刷新双树交换历史", exact: true }).click();
    await expect(panel.locator(".tree-exchange-list li")).toHaveCount(20);
    await panel.getByRole("button", { name: "下一页树提案", exact: true }).click();
    await expect(panel.locator(".tree-exchange-list li")).toHaveCount(2);
    await panel.getByRole("button", { name: "上一页树提案", exact: true }).click();
    await expect(panel.locator(".tree-exchange-list li")).toHaveCount(20);
  } finally { await destroy(page, id); }
});

test("模型原Run/call事实在界面原键/重载冷读0Provider和Writer", async ({ page }) => {
  const left = "exchange-model-left", right = "exchange-model-right";
  await control(page, "seed", left); await control(page, "seed", right, undefined, "right", "new");
  const { id, topic, panel } = await actor(page, "exchange-model");
  try {
    const response = await page.request.post("/_test/model-tree-exchange", { headers, data: { agent_id: id, left_path: left, right_path: right } });
    expect(response.ok()).toBeTruthy(); const original = await response.json(); expect(original.provider_calls).toBe(6);
    await panel.getByLabel("双树交换原请求键", { exact: true }).fill(original.preview_key);
    await panel.getByRole("button", { name: "按键查看树提案", exact: true }).click();
    await expect(panel.locator(".tree-exchange-detail > strong")).toContainText("双树已交换");
    await connect(page, topic); const restored = page.locator(`article.agent-card[data-agent-id="${id}"] .tree-exchanges`);
    await restored.getByText("双完整目录树交换与历史", { exact: true }).click();
    await expect(restored.locator(".tree-exchange-detail > strong")).toContainText("双树已交换");
    expect(await control(page, "counts", "tree", id)).toMatchObject({ proposals: 1, operations: 1, sources: 2, provider_calls: 6 });
  } finally { await destroy(page, id); }
});

test("合法Schema伪Scope回程拒绝、保留原key仅GET恢复", async ({ page }) => {
  const left = "exchange-forged-left", right = "exchange-forged-right";
  await control(page, "seed", left); await control(page, "seed", right, undefined, "right", "new");
  const { id, panel } = await actor(page, "exchange-forged"); let writes = 0;
  try {
    await page.route("**/workspace/tree-exchanges/preview?**", async route => {
      writes++; const response = await route.fetch(); const value = await response.json();
      await route.fulfill({ response, json: { ...value, scope_id: "session:other" } });
    });
    await inspect(panel, left, right); await panel.getByRole("button", { name: "预览双树交换", exact: true }).click();
    await expect(panel.locator(".tree-exchange-status")).toContainText("双树交换回程与原事实不一致");
    await expect(panel.locator(".tree-exchange-pending")).toContainText("树响应尚未确认");
    await panel.getByRole("button", { name: "查询待确认树请求", exact: true }).click();
    await expect(panel.locator(".tree-exchange-detail > strong")).toContainText("双树交换待确认");
    expect(writes).toBe(1); expect(await control(page, "counts", "tree", id)).toMatchObject({ proposals: 1, operations: 0 });
  } finally { await destroy(page, id); }
});

test("本地意图保存失败关闭写入、0服务端提案或操作", async ({ page }) => {
  await page.addInitScript(() => {
    const original = Storage.prototype.setItem;
    Storage.prototype.setItem = function(key: string, value: string) {
      if (key.startsWith("krim-tree-exchange-recovery:")) throw new DOMException("synthetic blocked recovery storage", "QuotaExceededError");
      return original.call(this, key, value);
    };
  });
  const left = "exchange-storage-left", right = "exchange-storage-right";
  await control(page, "seed", left); await control(page, "seed", right, undefined, "right", "new");
  const { id, panel } = await actor(page, "exchange-storage");
  try {
    await inspect(panel, left, right); await panel.getByRole("button", { name: "预览双树交换", exact: true }).click();
    await expect(panel.locator(".tree-exchange-pending")).toContainText("树恢复记录不可用");
    await expect(panel.getByRole("button", { name: "预览双树交换", exact: true })).toBeDisabled();
    expect(await control(page, "counts", "tree", id)).toMatchObject({ proposals: 0, operations: 0, seals: 0 });
  } finally { await destroy(page, id); }
});

test("只读工作区只检查/查原封存、不获得seal或交换预览", async ({ page }) => {
  const { id, panel } = await actor(page, "exchange-readonly", "agent", ["tree_exchange_inspect", "tree_exchange_query", "tree_exchange_seal_query"], "demo");
  try {
    await panel.getByLabel("左目录树相对路径", { exact: true }).fill("src");
    await panel.getByRole("button", { name: "检查左完整目录树", exact: true }).click();
    await expect(panel.locator(".tree-exchange-summary")).toContainText("src");
    await panel.getByLabel("原生成计划标识（可选）", { exact: true }).fill("missing");
    await panel.getByRole("button", { name: "查询原生成树封存", exact: true }).click();
    await expect(panel.locator(".tree-exchange-status")).toContainText("未找到原生成树封存");
    await expect(panel.getByRole("button", { name: "封存原生成完整树", exact: true })).toBeDisabled();
    await expect(panel.getByRole("button", { name: "预览双树交换", exact: true })).toBeDisabled();
  } finally { await destroy(page, id); }
});
