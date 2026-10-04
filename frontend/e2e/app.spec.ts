import { test as base, expect } from "@playwright/test";

const test = base.extend({
  request: async ({ playwright, baseURL }, use) => {
    const context = await playwright.request.newContext({ baseURL, extraHTTPHeaders: { Authorization: "Bearer browser-test-key" } });
    await use(context);
    await context.dispose();
  },
});
test.beforeEach(async ({ page }) => {
  await page.goto("/ui/");
  await page.getByLabel("服务访问密钥").fill("browser-test-key");
  await page.getByRole("button", { name: "连接", exact: true }).click();
  await expect(page.locator("#connection")).toContainText("本地服务已连接");
});

test("创建话题并完成真实工具 SSE 对话", async ({ page }) => {
  await expect(page.getByRole("status").filter({ hasText: "本地服务已连接" })).toBeVisible();
  const title = `浏览器测试-${Date.now()}`;
  await page.getByPlaceholder("新话题标题").fill(title);
  await page.getByRole("button", { name: "新建话题" }).click();
  await expect(page.getByRole("heading", { name: title })).toBeVisible();
  await page.getByPlaceholder("输入消息，或试试 /add 2 3").fill("/add 2 3");
  await page.getByRole("button", { name: "发送" }).click();
  await expect(page.locator(".message.tool").last()).toContainText('"sum": 5.0');
  await expect(page.locator("#notice")).toContainText("已完成");
});

test("通过 Agent 目录创建并执行默认 Agent", async ({ page }) => {
  await expect(page.getByRole("status").filter({ hasText: "本地服务已连接" })).toBeVisible();
  await page.getByLabel("Agent 任务").fill("/echo 浏览器 Agent");
  await page.getByRole("button", { name: "创建 Agent" }).click();
  await expect(page.locator(".agent-card").last()).toContainText("已完成");
  await expect(page.locator("#notice")).toContainText("Agent 已完成");
  const card = page.locator(".agent-card").last();
  await page.getByLabel("Agent 任务").fill("/echo 继续任务");
  await card.getByRole("button", { name: "继续任务" }).click();
  await expect(card).toContainText("继续任务");
  const previousCount = await page.locator(".agent-card").count();
  await card.getByRole("button", { name: "销毁" }).click();
  await expect(page.locator(".agent-card")).toHaveCount(previousCount - 1);
});

test("编辑、活动分支和重新生成保留原始消息树", async ({ page, request }) => {
  await expect(page.locator("#connection")).toContainText("本地服务已连接");
  const title = `分支测试-${Date.now()}`;
  await page.getByPlaceholder("新话题标题").fill(title);
  const topicResponse = page.waitForResponse((response) => response.url().endsWith("/v1/topics") && response.request().method() === "POST");
  await page.getByRole("button", { name: "新建话题" }).click();
  const topic = await (await topicResponse).json();
  await expect(page.getByRole("heading", { name: title })).toBeVisible();
  const originalResponse = page.waitForResponse((response) => response.url().endsWith(`/v1/topics/${topic.id}/messages`) && response.request().method() === "POST");
  await page.locator("#prompt").fill("/echo 原分支");
  await page.getByRole("button", { name: "发送", exact: true }).click();
  const original = await (await originalResponse).json();
  await expect(page.locator(".message.assistant").last()).toContainText("原分支");
  await expect(page.locator("#prompt")).toBeEnabled();
  await page.locator(".message.user").first().getByRole("button", { name: "编辑", exact: true }).click();
  await page.locator("#prompt").fill("/echo 新分支");
  await page.getByRole("button", { name: "发送", exact: true }).click();
  await expect(page.locator(".message.assistant").last()).toContainText("新分支");
  await expect(page.locator("#branch-select")).toBeEnabled();
  await page.locator("#branch-select").selectOption(original.input_message_id);
  await expect(page.locator(".message.assistant").last()).toContainText("原分支");
  const treeBefore = await (await request.get(`/v1/topics/${topic.id}/tree`)).json();
  const regeneratedResponse = page.waitForResponse((response) => response.url().endsWith("/regenerate") && response.request().method() === "POST");
  await page.locator(".message.assistant").last().getByRole("button", { name: "重新生成" }).click();
  const regenerated = await (await regeneratedResponse).json();
  expect(regenerated.input_message_id).toBe(original.input_message_id);
  await expect.poll(async () => (await (await request.get(`/v1/topics/${topic.id}/tree`)).json()).messages.length).toBeGreaterThan(treeBefore.messages.length);
  await expect(page.locator("#prompt")).toBeEnabled();
});

test("压缩先预览后确认并保留完整原始历史", async ({ page, request }) => {
  const title = `压缩测试-${Date.now()}`;
  const topic = await (await request.post("/v1/topics", { data: { title } })).json();
  for (let turn = 0; turn < 4; turn++) {
    const run = await (await request.post(`/v1/topics/${topic.id}/messages`, { data: { text: `历史 ${turn}`, idempotency_key: `browser-${turn}` } })).json();
    await request.get(`/v1/runs/${run.id}/events`);
  }
  await page.getByRole("button", { name: "刷新", exact: true }).click();
  await expect(page.locator("#connection")).toContainText("本地服务已连接");
  await page.locator(".topic-button").filter({ hasText: title }).click();
  await expect(page.getByRole("heading", { name: title })).toBeVisible();
  await page.locator("#compact-budget").fill("3");
  await expect(page.getByRole("button", { name: "确认压缩" })).toBeDisabled();
  await page.getByRole("button", { name: "预览压缩" }).click();
  await expect(page.locator("#compact-title")).toContainText("压缩预览");
  await expect(page.getByRole("button", { name: "确认压缩" })).toBeEnabled();
  await page.getByRole("button", { name: "确认压缩" }).click();
  await expect(page.locator("#compact-title")).toContainText("摘要已保存");
  await expect(page.locator("#compact-sources")).toContainText("已保存");
  expect((await (await request.get(`/v1/topics/${topic.id}/messages`)).json()).messages).toHaveLength(8);
  await page.screenshot({ path: "../.cache/browser-agentapp.png", fullPage: true });
});

test("生成中可以停止并等待持久化终态", async ({ page, request }) => {
  const title = `停止测试-${Date.now()}`;
  await page.getByPlaceholder("新话题标题").fill(title);
  await page.getByRole("button", { name: "新建话题" }).click();
  await expect(page.getByRole("heading", { name: title })).toBeVisible();
  const accepted = page.waitForResponse((response) => response.url().endsWith("/messages") && response.request().method() === "POST");
  await page.locator("#prompt").fill("停止流输出 ".repeat(150));
  await page.getByRole("button", { name: "发送", exact: true }).click();
  const run = await (await accepted).json();
  await expect(page.locator(".message.streaming")).toContainText("[fake]");
  await page.getByRole("button", { name: "停止", exact: true }).first().click();
  await expect(page.locator("#notice")).toContainText("已停止");
  expect((await (await request.get(`/v1/runs/${run.id}`)).json()).status).toBe("aborted");
  await expect(page.locator("#prompt")).toBeEnabled();
});

test("页面刷新后重新输入密钥并续读后台运行", async ({ page, request }) => {
  const title = `续读测试-${Date.now()}`;
  const topic = await (await request.post("/v1/topics", { data: { title } })).json();
  const run = await (await request.post(`/v1/topics/${topic.id}/messages`, { data: { text: "后台运行 ".repeat(120), idempotency_key: "background" } })).json();
  await page.reload();
  await expect(page.getByLabel("服务访问密钥")).toBeVisible();
  expect(await page.evaluate(() => Object.values(localStorage).join(" "))).not.toContain("browser-test-key");
  expect(page.url()).not.toContain("browser-test-key");
  await page.getByLabel("服务访问密钥").fill("browser-test-key");
  await page.getByRole("button", { name: "连接", exact: true }).click();
  await expect(page.locator("#connection")).toContainText("本地服务已连接");
  await page.locator(".topic-button").filter({ hasText: title }).click();
  await page.getByRole("button", { name: "继续读取" }).click();
  await expect(page.locator("#notice")).toContainText("已完成");
  expect((await (await request.get(`/v1/runs/${run.id}`)).json()).status).toBe("completed");
});

test("移动端消息与任务界面可操作且不会执行输入 HTML", async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  const title = `移动测试-${Date.now()}`;
  await page.getByPlaceholder("新话题标题").fill(title);
  await page.getByRole("button", { name: "新建话题" }).click();
  await expect(page.getByRole("heading", { name: title })).toBeVisible();
  await page.locator("#prompt").fill('<img id="injected" src="x" onerror="window.attacked=true">');
  await page.getByRole("button", { name: "发送", exact: true }).click();
  await expect(page.locator("#notice")).toContainText("已完成");
  await expect(page.locator("#injected")).toHaveCount(0);
  expect(await page.evaluate(() => Reflect.get(window, "attacked"))).toBeUndefined();
});

test("Workflow 图节点/边编辑、持久运行和重新读取", async ({ page }) => {
  await page.getByText("Workflow 运行面板").click();
  await expect(page.locator("#workflow-svg .workflow-node")).toHaveCount(2);
  await page.locator('[data-workflow-node="start"]').click();
  await page.getByLabel("节点配置 JSON").fill('{"value":3}');
  await page.getByRole("button", { name: "保存节点" }).click();
  await expect(page.locator('[data-workflow-node="start"]')).toContainText("constant");
  await page.getByRole("button", { name: "运行 Workflow" }).click();
  await expect(page.locator("#workflow-status")).toContainText("completed");
  await expect(page.locator("#workflow-status")).toContainText("输出 3");
  await expect(page.locator("#workflow-events")).toContainText("node_end");
  const options = await page.locator("#workflow-run-list option").count();
  expect(options).toBeGreaterThan(0);
  await page.getByRole("button", { name: "读取所选运行" }).click();
  await expect(page.locator("#workflow-svg .workflow-node")).toHaveCount(2);
});

test("Workflow 图支持增删节点和命名端口连接", async ({ page }) => {
  await page.getByText("Workflow 运行面板").click();
  await page.getByRole("button", { name: "添加常量节点" }).click();
  await expect(page.locator("#workflow-svg .workflow-node")).toHaveCount(3);
  await page.locator('[data-workflow-node="start"]').click();
  await page.locator('[data-workflow-node="constant_1"]').click();
  await page.locator("#workflow-edge-source-port").fill("out");
  await page.locator("#workflow-edge-target-port").fill("in");
  await page.getByRole("button", { name: "连接选中节点" }).click();
  await expect(page.locator("#workflow-svg .workflow-edge")).toHaveCount(2);
  await page.locator("#workflow-edge-list").selectOption("1");
  await page.getByRole("button", { name: "删除边" }).click();
  await expect(page.locator("#workflow-svg .workflow-edge")).toHaveCount(1);
  await page.locator('[data-workflow-node="start"]').click();
  await page.getByRole("button", { name: "删除节点" }).click();
  await expect(page.locator("#workflow-svg .workflow-node")).toHaveCount(2);
});

test("Workflow 暂停、覆盖输入、继续与重跑", async ({ page, request }) => {
  await page.getByText("Workflow 运行面板").click();
  const graph = { nodes: [{ id: "start", kind: "input", config: {} }, { id: "finish", kind: "output", config: {} }], edges: [{ source: "start", target: "finish" }], output_node: "finish" };
  await page.locator("#workflow-json").fill(JSON.stringify(graph));
  await page.locator("#workflow-input").fill("1");
  await page.locator("#workflow-pause-before").fill("finish");
  await page.getByRole("button", { name: "运行 Workflow" }).click();
  await expect(page.locator("#workflow-status")).toContainText("paused");
  await page.locator("#workflow-override").fill("9");
  await page.locator("#workflow-has-override").check();
  await page.locator("#workflow-resume").click();
  await expect(page.locator("#workflow-status")).toContainText("completed");
  await expect(page.locator("#workflow-status")).toContainText("输出 9");
  const runId = await page.locator("#workflow-run-list").inputValue();
  const before = await (await request.get(`/v1/workflows/runs/${runId}`)).json();
  await page.locator('[data-workflow-node="finish"]').click();
  const rerunResponse = page.waitForResponse((response) => response.url().endsWith(`/v1/workflows/runs/${runId}/rerun`) && response.request().method() === "POST");
  await page.locator("#workflow-rerun").click();
  expect((await (await rerunResponse).json()).epoch).toBeGreaterThan(before.epoch);
  await expect(page.locator("#workflow-status")).toContainText("completed");
  const finished = await (await request.get(`/v1/workflows/runs/${runId}`)).json();
  expect(finished.status).toBe("completed");
  expect(finished.epoch).toBeGreaterThan(before.epoch);
});
