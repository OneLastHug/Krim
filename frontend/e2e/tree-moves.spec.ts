import { destroyAgent } from "./session-fixtures.js";
import { expect, test, type Locator, type Page } from "@playwright/test";

const headers = { Authorization: "Bearer browser-test-key" };
const tools = ["tree_move_inspect", "tree_move_preview", "tree_move_query", "tree_move_change", "tree_move_reconcile"];

async function control(page: Page, action: string, path = "tree", agent_id?: string) {
  const response = await page.request.post("/_test/tree-move", { headers, data: { action, path, agent_id } });
  expect(response.ok()).toBeTruthy(); return response.json();
}
async function connect(page: Page, topic: string): Promise<void> {
  await page.goto(`/ui/?topic=${encodeURIComponent(topic)}`);
  await page.getByLabel("服务访问密钥").fill("browser-test-key");
  await page.getByRole("button", { name: "连接", exact: true }).click();
  await expect(page.locator("#connection")).toContainText("本地服务已连接");
}
async function agent(page: Page, name: string, mode = "agent", selectedTools = tools) {
  const response = await page.request.put(`/v1/agents/profiles/${name}`, { headers,
    data: { expected_revision: 0, config: { name, description: "真实完整树浏览器", workspace_key: "edit", tool_names: selectedTools, mode, tool_timeout_seconds: 30 } } });
  expect(response.ok()).toBeTruthy();
  const spawned = await page.request.post("/v1/agents", { headers, data: { name } });
  expect(spawned.ok()).toBeTruthy(); const view = await spawned.json();
  await connect(page, view.topic_id);
  const panel = page.locator(`article.agent-card[data-agent-id="${view.agent_id}"] .tree-moves`);
  await panel.getByText("完整目录树移动与历史", { exact: true }).click();
  return { id: view.agent_id as string, topic: view.topic_id as string, panel };
}
async function destroy(page: Page, id: string) { await destroyAgent(page, id); }
async function prepare(panel: Locator, source: string, target: string): Promise<void> {
  await panel.getByLabel("源目录树相对路径", { exact: true }).fill(source);
  await panel.getByRole("button", { name: "检查完整目录树", exact: true }).click();
  await expect(panel.locator(".tree-move-summary")).toContainText("4节点");
  await panel.getByLabel("目标目录树相对路径", { exact: true }).fill(target);
  await panel.getByRole("button", { name: "预览树移动", exact: true }).click();
  await expect(panel.locator(".tree-move-detail > strong")).toContainText("树移动待确认");
}
async function confirm(page: Page, panel: Locator, label: string, accept = true): Promise<string> {
  let message = ""; let done!: () => void;
  const completed = new Promise<void>(resolve => { done = resolve; });
  page.once("dialog", dialog => { message = dialog.message(); void (accept ? dialog.accept() : dialog.dismiss()).finally(done); });
  await panel.getByRole("button", { name: label, exact: true }).click(); await completed; return message;
}

test("完整树指纹/二进制原inode、明确确认、保留撤销与Locale", async ({ page }) => {
  const source = "browser-full-tree", target = "browser-full-target";
  const original = await control(page, "seed", source);
  const { id, panel } = await agent(page, "browser-tree-move-normal");
  try {
    await prepare(panel, source, target);
    await expect(panel.locator(".tree-move-condition")).toContainText(`${source}\n`);
    await expect(panel.locator(".tree-move-condition")).toContainText("目标必须不存在");
    await confirm(page, panel, "应用树移动", false);
    expect(await control(page, "counts", "tree", id)).toMatchObject({ proposals: 1, operations: 0 });
    const message = await confirm(page, panel, "应用树移动"); expect(message).toContain(`${source} → ${target}`);
    await expect(panel.locator(".tree-move-detail > strong")).toContainText("树已移动");
    const moved = await control(page, "stat", target);
    expect(moved.binary_hex).toBe("00ff80"); expect(moved.binary_inode).toBe(original.binary_inode); expect(moved.inode).toBe(original.inode);
    expect((await control(page, "stat", source)).found).toBe(false);
    await confirm(page, panel, "保留树移动"); await expect(panel.locator(".tree-move-detail > strong")).toContainText("树移动已保留");
    await expect(panel.locator(".tree-move-receipt")).toContainText("未产生工作区变化");
    await page.locator("#locale-select").selectOption("en-US");
    await expect(panel.getByLabel("Source tree relative path", { exact: true })).toHaveValue(source);
    await expect(panel.getByRole("button", { name: "Undo tree move", exact: true })).toBeEnabled();
    await page.locator("#locale-select").selectOption("zh-CN");
    await confirm(page, panel, "撤销树移动"); await expect(panel.locator(".tree-move-detail > strong")).toContainText("树已撤销移动");
    const restored = await control(page, "stat", source); expect(restored.binary_inode).toBe(original.binary_inode); expect(restored.binary_hex).toBe("00ff80");
    expect((await control(page, "stat", target)).found).toBe(false);
    expect(await control(page, "counts", "tree", id)).toMatchObject({ proposals: 1, operations: 3, sources: 0 });
    await expect(panel.locator(".tree-move-history li")).toHaveCount(3);
    await panel.screenshot({ path: test.info().outputPath("complete-tree-move.png") });
    await panel.locator(".tree-move-detail > strong").scrollIntoViewIfNeeded();
    await page.screenshot({ path: test.info().outputPath("tree-move-viewport.png") });
  } finally { await destroy(page, id); }
});

test("非法路径与祖孙目标、plan写拒绝和未加载工具不发送", async ({ page }) => {
  const source = "browser-plan-tree"; await control(page, "seed", source);
  const planned = await agent(page, "browser-tree-move-plan", "plan");
  try {
    await planned.panel.getByLabel("源目录树相对路径", { exact: true }).fill("../escape");
    await expect(planned.panel.getByRole("button", { name: "检查完整目录树", exact: true })).toBeDisabled();
    await planned.panel.getByLabel("源目录树相对路径", { exact: true }).fill(source);
    await planned.panel.getByRole("button", { name: "检查完整目录树", exact: true }).click();
    await expect(planned.panel.locator(".tree-move-summary")).toContainText("4节点");
    for (const target of [source, `${source}/inside`, "../escape", "missing/../escape"]) {
      await planned.panel.getByLabel("目标目录树相对路径", { exact: true }).fill(target);
      await expect(planned.panel.getByRole("button", { name: "预览树移动", exact: true })).toBeDisabled();
    }
    await planned.panel.getByLabel("目标目录树相对路径", { exact: true }).fill("browser-plan-target");
    await planned.panel.getByRole("button", { name: "预览树移动", exact: true }).click();
    await expect(planned.panel.locator(".tree-move-detail > strong")).toContainText("树移动待确认");
    await expect(planned.panel.getByRole("button", { name: "应用树移动", exact: true })).toBeDisabled();
    expect(await control(page, "counts", "tree", planned.id)).toMatchObject({ operations: 0 });
  } finally { await destroy(page, planned.id); }
  const restricted = await agent(page, "browser-tree-move-restricted", "agent", ["tree_move_inspect"]);
  try {
    await expect(restricted.panel.locator(".tree-move-status")).toContainText("当前会话未授权树移动查询");
    await expect(restricted.panel.getByRole("button", { name: "刷新树移动历史", exact: true })).toBeDisabled();
    expect(await control(page, "counts", "tree", restricted.id)).toMatchObject({ operations: 0, proposals: 0 });
  } finally { await destroy(page, restricted.id); }
});

test("丢预览和动作响应、刷新仅原键GET与零第二操作", async ({ page }) => {
  const source = "browser-lost-tree", target = "browser-lost-target"; await control(page, "seed", source);
  const { id, topic, panel } = await agent(page, "browser-tree-move-lost");
  try {
    let previews = 0, changes = 0;
    await page.route("**/workspace/tree-moves/preview?**", async route => { previews += 1; await route.fetch(); await route.abort("failed"); });
    await panel.getByLabel("源目录树相对路径", { exact: true }).fill(source);
    await panel.getByRole("button", { name: "检查完整目录树", exact: true }).click();
    await expect(panel.locator(".tree-move-summary")).toContainText("4节点");
    await panel.getByLabel("目标目录树相对路径", { exact: true }).fill(target);
    await panel.getByRole("button", { name: "预览树移动", exact: true }).click();
    await expect(panel.locator(".tree-move-pending")).toContainText("树响应尚未确认");
    await panel.getByRole("button", { name: "查询待确认树请求", exact: true }).click();
    await expect(panel.locator(".tree-move-detail > strong")).toContainText("树移动待确认");
    await page.unroute("**/workspace/tree-moves/preview?**");
    await page.route("**/workspace/tree-moves/change?**", async route => { changes += 1; await route.fetch(); await route.abort("failed"); });
    await confirm(page, panel, "应用树移动");
    await expect(panel.locator(".tree-move-pending")).toContainText("树响应尚未确认");
    await connect(page, topic); const restored = page.locator(`article.agent-card[data-agent-id="${id}"] .tree-moves`);
    await restored.getByText("完整目录树移动与历史", { exact: true }).click();
    await expect(restored.locator(".tree-move-detail > strong")).toContainText("树已移动");
    await expect(restored.getByRole("button", { name: "应用树移动", exact: true })).toBeDisabled();
    expect(previews).toBe(1); expect(changes).toBe(1);
    expect(await control(page, "counts", "tree", id)).toMatchObject({ proposals: 1, operations: 1 });
    expect((await control(page, "stat", target)).binary_hex).toBe("00ff80");
  } finally { await destroy(page, id); }
});

test("实际原回执存储失败、unknown只核实同operation", async ({ page }) => {
  const source = "browser-unknown-tree", target = "browser-unknown-target"; await control(page, "seed", source);
  const { id, panel } = await agent(page, "browser-tree-move-unknown");
  try {
    await prepare(panel, source, target); await control(page, "fail_save");
    await confirm(page, panel, "应用树移动"); await expect(panel.locator(".tree-move-detail > strong")).toContainText("树结果待核实");
    await expect(panel.getByRole("button", { name: "核实原树结果", exact: true })).toBeEnabled();
    expect((await control(page, "stat", target)).binary_hex).toBe("00ff80");
    await confirm(page, panel, "核实原树结果"); await expect(panel.locator(".tree-move-detail > strong")).toContainText("树已移动");
    expect(await control(page, "counts", "tree", id)).toMatchObject({ proposals: 1, operations: 1 });
  } finally { await destroy(page, id); }
});

test("写请求等待期间可取消，等原线程收束后保存真实拒绝", async ({ page }) => {
  const source = "browser-cancel-tree", target = "browser-cancel-target"; await control(page, "seed", source);
  const { id, panel } = await agent(page, "browser-tree-move-cancel");
  try {
    await prepare(panel, source, target); await control(page, "pause", "tree", id);
    await confirm(page, panel, "应用树移动");
    await expect.poll(async () => (await control(page, "gate", "tree", id)).entered).toBe(true);
    await expect(panel.getByRole("button", { name: "取消进行中的树移动", exact: true })).toBeEnabled();
    await confirm(page, panel, "取消进行中的树移动");
    await expect.poll(async () => (await control(page, "counts", "tree", id)).cancelled).toBe(1);
    await control(page, "release", "tree", id);
    await expect(panel.locator(".tree-move-detail > strong")).toContainText("树移动已取消");
    await expect(panel.locator(".tree-move-receipt")).toContainText("树操作已拒绝");
    expect((await control(page, "stat", source)).binary_hex).toBe("00ff80"); expect((await control(page, "stat", target)).found).toBe(false);
    expect(await control(page, "counts", "tree", id)).toMatchObject({ proposals: 1, operations: 1, cancelled: 1 });
  } finally { await control(page, "release", "tree", id); await destroy(page, id); }
});

test("原模型Run/call事实浏览器原键读取、重载0Provider/Writer", async ({ page }) => {
  const source = "browser-model-move-tree", target = "browser-model-move-target"; await control(page, "seed", source);
  const { id, topic, panel } = await agent(page, "browser-tree-move-model");
  try {
    const response = await page.request.post("/_test/model-tree-move", { headers, data: { agent_id: id, source_path: source, target_path: target } });
    expect(response.ok()).toBeTruthy(); const original = await response.json(); expect(original.provider_calls).toBe(5);
    await panel.getByLabel("树移动原请求键", { exact: true }).fill(original.change_key);
    await panel.getByRole("button", { name: "按键查看树操作", exact: true }).click();
    await expect(panel.locator(".tree-move-detail > strong")).toContainText("树已移动");
    await expect(panel.locator(".tree-move-receipt")).toContainText("树操作已确认");
    const before = await control(page, "counts", "tree", id); expect(before).toMatchObject({ proposals: 1, operations: 1, sources: 2, provider_calls: 5 });
    await connect(page, topic); const restored = page.locator(`article.agent-card[data-agent-id="${id}"] .tree-moves`);
    await restored.getByText("完整目录树移动与历史", { exact: true }).click();
    await expect(restored.locator(".tree-move-detail > strong")).toContainText("树已移动");
    expect(await control(page, "counts", "tree", id)).toEqual(before);
  } finally { await destroy(page, id); }
});

test("无效恢复/存储失败关闭写入，异作用域回程不放行", async ({ page }) => {
  const source = "browser-invalid-cache-tree"; await control(page, "seed", source);
  const { id, topic, panel } = await agent(page, "browser-tree-move-cache");
  try {
    await page.evaluate(id => localStorage.setItem(`krim-tree-move-recovery:${id}`, JSON.stringify({ version: 9, selected: "bad", owner_id: "forged" })), id);
    await connect(page, topic);
    await panel.getByText("完整目录树移动与历史", { exact: true }).click();
    await expect(panel.locator(".tree-move-pending")).toContainText("树恢复记录不可用");
    await confirm(page, panel, "清除本地树恢复记录");
    await panel.getByRole("button", { name: "刷新树移动历史", exact: true }).click();
    await prepare(panel, source, "browser-invalid-cache-target");
    await page.route("**/workspace/tree-moves/query?**", async route => {
      const response = await route.fetch(); const body = await response.json();
      if (body.proposal) body.proposal.scope_id = "session:other";
      await route.fulfill({ response, json: body });
    });
    await panel.getByRole("button", { name: "刷新树移动历史", exact: true }).click();
    await expect(panel.locator(".tree-move-status")).toContainText("树移动回程与原事实不一致");
    await expect(panel.getByRole("button", { name: "应用树移动", exact: true })).toBeDisabled();
    expect(await control(page, "counts", "tree", id)).toMatchObject({ operations: 0 });
  } finally { await destroy(page, id); }
});

test("意图无法可靠保存时不发送预览或动作", async ({ page }) => {
  const source = "browser-storage-blocked-tree"; await control(page, "seed", source);
  const { id, panel } = await agent(page, "browser-tree-move-storage");
  try {
    await panel.getByLabel("源目录树相对路径", { exact: true }).fill(source);
    await panel.getByRole("button", { name: "检查完整目录树", exact: true }).click();
    await expect(panel.locator(".tree-move-summary")).toContainText("4节点");
    await panel.getByLabel("目标目录树相对路径", { exact: true }).fill("browser-storage-blocked-target");
    await page.evaluate(() => {
      const original = Storage.prototype.setItem;
      Storage.prototype.setItem = function (name: string, value: string): void {
        if (name.startsWith("krim-tree-move-recovery:")) throw new DOMException("synthetic storage quota", "QuotaExceededError");
        original.call(this, name, value);
      };
    });
    await panel.getByRole("button", { name: "预览树移动", exact: true }).click();
    await expect(panel.locator(".tree-move-pending")).toContainText("树恢复记录不可用");
    await expect(panel.getByRole("button", { name: "预览树移动", exact: true })).toBeDisabled();
    expect(await control(page, "counts", "tree", id)).toMatchObject({ proposals: 0, operations: 0 });
  } finally { await destroy(page, id); }
});

test("原21提案分页与原键查找不创建第二事实", async ({ page }) => {
  const source = "browser-paged-tree"; await control(page, "seed", source);
  const { id, panel } = await agent(page, "browser-tree-move-pages");
  try {
    const catalog = await control(page, "catalog", source, id);
    await panel.getByRole("button", { name: "刷新树移动历史", exact: true }).click();
    await expect(panel.locator(".tree-move-list li")).toHaveCount(20);
    const first = await panel.locator(".tree-move-list li").evaluateAll(rows => rows.map(row => (row as HTMLElement).dataset.proposalId));
    await panel.getByRole("button", { name: "下一页树提案", exact: true }).click();
    await expect(panel.locator(".tree-move-list li")).toHaveCount(1);
    const last = await panel.locator(".tree-move-list li").getAttribute("data-proposal-id");
    expect(new Set([...first, last])).toEqual(new Set(catalog.proposal_ids));
    await expect(panel.getByRole("button", { name: "下一页树提案", exact: true })).toBeDisabled();
    await panel.getByRole("button", { name: "上一页树提案", exact: true }).click();
    await expect(panel.locator(".tree-move-list li")).toHaveCount(20);
    expect(await control(page, "counts", "tree", id)).toMatchObject({ proposals: 21, operations: 0, provider_calls: 0 });
  } finally { await destroy(page, id); }
});

test("目录名中的HTML只作文本审阅与真实移动", async ({ page }) => {
  const seeded = await control(page, "html");
  const { id, panel } = await agent(page, "browser-tree-move-text");
  try {
    await prepare(panel, seeded.path, "browser-html-tree-target");
    await expect(panel.locator(".tree-move-detail > strong")).toContainText(seeded.path);
    await expect(panel.locator("img")).toHaveCount(0);
    expect(await page.evaluate(() => Object.hasOwn(window, "treeInjected"))).toBe(false);
    await confirm(page, panel, "应用树移动");
    await expect(panel.locator(".tree-move-detail > strong")).toContainText("树已移动");
    expect((await control(page, "stat", "browser-html-tree-target")).binary_hex).toBe("00ff80");
    expect(await page.evaluate(() => Object.hasOwn(window, "treeInjected"))).toBe(false);
  } finally { await destroy(page, id); }
});
