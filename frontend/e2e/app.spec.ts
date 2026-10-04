import { test as base, expect } from "@playwright/test";
import { execFile } from "node:child_process";
import { promisify } from "node:util";

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
  await expect(page.locator("#workflow-status")).toContainText("已完成");
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
  await expect(page.locator("#workflow-status")).toContainText("已暂停");
  await page.locator("#workflow-override").fill("9");
  await page.locator("#workflow-has-override").check();
  await page.locator("#workflow-resume").click();
  await expect(page.locator("#workflow-status")).toContainText("已完成");
  await expect(page.locator("#workflow-status")).toContainText("输出 9");
  const runId = await page.locator("#workflow-run-list").inputValue();
  const before = await (await request.get(`/v1/workflows/runs/${runId}`)).json();
  await page.locator('[data-workflow-node="finish"]').click();
  const rerunResponse = page.waitForResponse((response) => response.url().endsWith(`/v1/workflows/runs/${runId}/rerun`) && response.request().method() === "POST");
  await page.locator("#workflow-rerun").click();
  expect((await (await rerunResponse).json()).epoch).toBeGreaterThan(before.epoch);
  await expect(page.locator("#workflow-status")).toContainText("已完成");
  const finished = await (await request.get(`/v1/workflows/runs/${runId}`)).json();
  expect(finished.status).toBe("completed");
  expect(finished.epoch).toBeGreaterThan(before.epoch);
});

test("question 先批准具体工具，再提交真实答案到模型", async ({ page, request }) => {
  const title = `问答测试-${Date.now()}`;
  await page.getByPlaceholder("新话题标题").fill(title);
  await page.getByRole("button", { name: "新建话题" }).click();
  await expect(page.getByRole("heading", { name: title })).toBeVisible();
  const accepted = page.waitForResponse((response) => response.url().endsWith("/messages") && response.request().method() === "POST");
  await page.locator("#prompt").fill("/question 选择发布环境");
  await page.getByRole("button", { name: "发送", exact: true }).click();
  const run = await (await accepted).json();
  await expect(page.locator(".interaction-card")).toContainText("工具执行需要批准：question");
  const before = await (await request.get(`/v1/runs/${run.id}/interactions`)).json();
  expect(before).toHaveLength(1);
  expect(before[0].status).toBe("pending");
  await page.getByRole("button", { name: "批准此次调用" }).click();
  await expect(page.locator(".interaction-card")).toContainText("选择发布环境");
  await page.getByRole("textbox", { name: "用户答案" }).fill("只在本机演练");
  await page.getByRole("button", { name: "提交答案" }).click();
  await expect(page.locator(".message.assistant").last()).toContainText("只在本机演练");
  await expect(page.locator("#prompt")).toBeEnabled();
  const history = await (await request.get(`/v1/runs/${run.id}/interactions`)).json();
  expect(history.map((item: { status: string }) => item.status)).toEqual(["approved", "answered"]);
});

test("拒绝审批和停止等待都保存终态并移除待处理卡片", async ({ page, request }) => {
  const title = `拒绝与停止-${Date.now()}`;
  await page.getByPlaceholder("新话题标题").fill(title);
  await page.getByRole("button", { name: "新建话题" }).click();
  await expect(page.getByRole("heading", { name: title })).toBeVisible();
  const accepted = page.waitForResponse((response) => response.url().endsWith("/messages") && response.request().method() === "POST");
  await page.locator("#prompt").fill("/question 不允许执行");
  await page.getByRole("button", { name: "发送", exact: true }).click();
  const deniedRun = await (await accepted).json();
  await page.getByRole("button", { name: "拒绝此次调用" }).click();
  await expect(page.locator("#prompt")).toBeEnabled();
  await expect(page.locator(".interaction-card")).toHaveCount(0);
  const denied = await (await request.get(`/v1/runs/${deniedRun.id}/interactions`)).json();
  expect(denied.map((item: { status: string }) => item.status)).toEqual(["denied"]);

  const next = page.waitForResponse((response) => response.url().endsWith("/messages") && response.request().method() === "POST");
  await page.locator("#prompt").fill("/question 停止等待");
  await page.getByRole("button", { name: "发送", exact: true }).click();
  const stoppedRun = await (await next).json();
  await expect(page.locator(".interaction-card")).toContainText("工具执行需要批准");
  await page.locator("#run-stop").click();
  await expect(page.locator("#prompt")).toBeEnabled();
  await expect(page.locator(".interaction-card")).toHaveCount(0);
  expect((await (await request.get(`/v1/runs/${stoppedRun.id}`)).json()).status).toBe("aborted");
  const cancelled = await (await request.get(`/v1/runs/${stoppedRun.id}/interactions`)).json();
  expect(cancelled.map((item: { status: string }) => item.status)).toEqual(["cancelled"]);
});

test("刷新重连读取实际交互状态，历史审批帧不会重新成为待办", async ({ page, request }) => {
  const title = `交互恢复-${Date.now()}`;
  await page.getByPlaceholder("新话题标题").fill(title);
  await page.getByRole("button", { name: "新建话题" }).click();
  await expect(page.getByRole("heading", { name: title })).toBeVisible();
  const accepted = page.waitForResponse((response) => response.url().endsWith("/messages") && response.request().method() === "POST");
  await page.locator("#prompt").fill("/question 刷新后继续");
  await page.getByRole("button", { name: "发送", exact: true }).click();
  const run = await (await accepted).json();
  await expect(page.locator(".interaction-card")).toContainText("工具执行需要批准");
  const pending = await (await request.get(`/v1/runs/${run.id}/interactions?pending_only=true`)).json();
  expect((await request.post(`/v1/runs/${run.id}/interactions/${pending[0].id}/respond`, { data: { action: "approve" } })).status()).toBe(200);
  await expect.poll(async () => (await (await request.get(`/v1/runs/${run.id}/interactions?pending_only=true`)).json())[0]?.kind).toBe("question");
  await page.reload();
  await page.getByLabel("服务访问密钥").fill("browser-test-key");
  await page.getByRole("button", { name: "连接", exact: true }).click();
  await page.getByRole("button", { name: new RegExp(title) }).click();
  await expect(page.getByRole("heading", { name: title })).toBeVisible();
  await expect(page.locator(".interaction-card")).toContainText("请回答问题");
  await page.locator("#run-resume").click();
  await expect(page.getByRole("textbox", { name: "用户答案" })).toBeVisible();
  await expect(page.getByRole("button", { name: "批准此次调用" })).toHaveCount(0);
  await page.getByRole("button", { name: "提交答案" }).click();
  await expect(page.locator("#notice")).toContainText("请输入真实答案");
  await page.getByRole("textbox", { name: "用户答案" }).fill("刷新后的真实答案");
  await page.getByRole("button", { name: "提交答案" }).click();
  await expect(page.locator(".message.assistant").last()).toContainText("刷新后的真实答案");
  await expect(page.locator("#prompt")).toBeEnabled();
  const replay = await request.get(`/v1/runs/${run.id}/events`);
  expect(await replay.text()).toContain("interaction_request");
  await page.getByRole("button", { name: "刷新", exact: true }).click();
  await expect(page.locator(".interaction-card")).toHaveCount(0);
  const history = await (await request.get(`/v1/runs/${run.id}/interactions`)).json();
  expect(history.map((item: { status: string }) => item.status)).toEqual(["approved", "answered"]);
});

test("Agent 卡片执行时使用同一审批与问答回程", async ({ page, request }) => {
  const created = page.waitForResponse((response) => response.url().endsWith("/v1/agents") && response.request().method() === "POST");
  await page.getByLabel("Agent 任务").fill("/question Agent 如何执行");
  await page.getByRole("button", { name: "创建 Agent" }).click();
  const agent = await (await created).json();
  await page.getByRole("button", { name: "批准此次调用" }).click();
  await expect(page.getByRole("textbox", { name: "用户答案" })).toBeVisible();
  await page.getByRole("textbox", { name: "用户答案" }).fill("Agent 的真实答案");
  await page.getByRole("button", { name: "提交答案" }).click();
  const card = page.locator(".agent-card").filter({ hasText: agent.agent_id });
  await expect(card).toContainText("Agent 的真实答案");
  const finished = await (await request.get(`/v1/agents/${agent.agent_id}`)).json();
  expect(finished.status).toBe("completed");
  const history = await (await request.get(`/v1/runs/${finished.current_run_id}/interactions`)).json();
  expect(history.map((item: { status: string }) => item.status)).toEqual(["approved", "answered"]);
  await card.getByRole("button", { name: "销毁" }).click();
  await expect(card).toHaveCount(0);
});

test("同源 iframe Host SDK 完成握手、Key、Prompt、状态、主题和宽度回程", async ({ page }) => {
  await page.goto("/ui/host.html");
  const frame = page.frameLocator("#agent-frame");
  await page.locator("#host-key").fill("browser-test-key");
  await page.getByRole("button", { name: "连接 iframe" }).click();
  await expect(page.locator("#host-status")).toContainText("iframe 已连接");
  await expect(frame.locator("#connection")).toContainText("本地服务已连接");
  await page.getByRole("button", { name: "读取状态" }).click();
  await expect(page.locator("#host-output")).toContainText('"iframe": true');
  await page.locator("#host-prompt").fill("由父窗口插入");
  await page.getByRole("button", { name: "替换 Prompt" }).click();
  await expect(frame.locator("#prompt")).toHaveValue("由父窗口插入");
  await page.getByRole("button", { name: "深色主题" }).click();
  await expect(frame.locator("html")).toHaveAttribute("data-theme", "dark");
  await page.getByRole("button", { name: "调整宽度" }).click();
  await expect(page.locator("#host-output")).toContainText('"width": 960');
});

test("Python Host SDK 经真实 HTTP/SSE 和 iframe 回执完成控制", async ({ page, baseURL }) => {
  await page.goto("/ui/host.html");
  await page.locator("#host-key").fill("browser-test-key");
  await page.getByRole("button", { name: "连接 iframe" }).click();
  await expect(page.locator("#host-status")).toContainText("iframe 已连接");
  await expect(page.locator("#host-session")).not.toBeEmpty();
  const sessionId = await page.locator("#host-session").textContent();
  const execute = promisify(execFile);
  const result = await execute("../.venv/bin/python", ["../tests/fixtures/host_sdk_client.py", baseURL!, sessionId!], { env: { ...process.env, KRIM_AGENT_API_KEY: "browser-test-key" }, timeout: 20_000 });
  expect(JSON.parse(result.stdout)).toEqual({ connected: true, width: 840, theme: "dark" });
  const frame = page.frameLocator("#agent-frame");
  await expect(frame.locator("#prompt")).toHaveValue("由 Python Host SDK 插入");
  await expect(frame.locator("#notice")).toContainText("Python 回程完成");
  await expect(page.locator("#host-panel")).toBeVisible();
  await page.getByRole("button", { name: "销毁 iframe" }).click();
  await expect(page.locator("#agent-frame")).toHaveCount(0);
});

test("iframe 来源/版本/大小校验拒绝伪造调用，隐藏后状态仍存活", async ({ page }) => {
  await page.goto("/ui/host.html");
  await page.locator("#host-key").fill("browser-test-key");
  await page.getByRole("button", { name: "连接 iframe" }).click();
  await expect(page.locator("#host-status")).toContainText("iframe 已连接");
  const frame = page.frameLocator("#agent-frame");
  await page.locator("#host-prompt").fill("保留当前 Prompt");
  await page.getByRole("button", { name: "替换 Prompt" }).click();
  await expect(frame.locator("#prompt")).toHaveValue("保留当前 Prompt");
  await frame.locator("#prompt").evaluate(() => {
    window.postMessage({ protocol_version: 2, kind: "call", id: "spoof-source", channel: "host/prompt", data: { value: "伪造来源" } }, location.origin);
  });
  await page.evaluate(() => {
    const target = (document.getElementById("agent-frame") as HTMLIFrameElement).contentWindow!;
    target.postMessage({ protocol_version: 3, kind: "call", id: "spoof-version", channel: "host/prompt", data: { value: "版本错误" } }, location.origin);
    target.postMessage({ protocol_version: 2, kind: "call", id: "spoof-size", channel: "host/prompt", data: { value: "x".repeat(65537) } }, location.origin);
  });
  await page.getByRole("button", { name: "隐藏侧栏" }).click();
  await expect(page.locator("#host-panel")).toBeHidden();
  await expect(page.locator("#agent-frame")).toHaveCount(1);
  await page.getByRole("button", { name: "打开侧栏" }).click();
  await expect(frame.locator("#prompt")).toHaveValue("保留当前 Prompt");
  expect(await page.evaluate(() => ({ local: localStorage.length, session: sessionStorage.length }))).toEqual({ local: 0, session: 0 });
});

test("引用 chip 与背景标签发送真实 Unicode 位置快照并在刷新后保留目录", async ({ page, request }) => {
  const title = `引用上下文-${Date.now()}`;
  await page.getByPlaceholder("新话题标题").fill(title);
  const created = page.waitForResponse((response) => response.url().endsWith("/v1/topics") && response.request().method() === "POST");
  await page.getByRole("button", { name: "新建话题" }).click();
  const topic = await (await created).json();
  await expect(page.getByRole("heading", { name: title })).toBeVisible();
  await page.getByText("引用与背景上下文", { exact: true }).click();
  await page.getByLabel("引用名称").fill("设计文档");
  await page.getByLabel("引用内容").fill("引用中的真值");
  const added = page.waitForResponse((response) => response.url().endsWith("/mentions") && response.request().method() === "POST");
  await page.getByRole("button", { name: "添加引用", exact: true }).click();
  const reference = await (await added).json();
  await page.getByLabel("背景标签名称").fill("工作区");
  await page.getByLabel("背景标签内容").fill("背景中的真值");
  await page.getByRole("button", { name: "保存背景标签" }).click();
  await expect(page.locator("#context-tag-list")).toContainText("工作区");
  await page.locator("#prompt").fill("😀 ");
  await page.getByRole("button", { name: "插入 @设计文档" }).click();
  await expect(page.locator("#prompt")).toHaveValue("😀 @设计文档");
  await expect(page.locator("#mention-draft")).toContainText("本次引用：设计文档");
  const accepted = page.waitForResponse((response) => response.url().endsWith("/messages") && response.request().method() === "POST");
  await page.getByRole("button", { name: "发送", exact: true }).click();
  const run = await (await accepted).json();
  await expect(page.locator(".message.assistant").last()).toContainText("引用中的真值");
  await expect(page.locator(".message.assistant").last()).toContainText("背景中的真值");
  const history = (await (await request.get(`/v1/runs/${run.id}/messages`)).json()).messages;
  expect(history[0].content).toBe("😀 @设计文档");
  expect(history[0].details.prompt_context.mentions[0]).toMatchObject({ id: reference.id, start: 2, end: 7 });
  await page.reload();
  await page.getByLabel("服务访问密钥").fill("browser-test-key");
  await page.getByRole("button", { name: "连接", exact: true }).click();
  await page.getByRole("button", { name: new RegExp(title) }).click();
  await page.getByText("引用与背景上下文", { exact: true }).click();
  await expect(page.getByRole("button", { name: "插入 @设计文档" })).toBeVisible();
  await page.getByRole("button", { name: "清空引用目录" }).click();
  await expect(page.locator("#mention-list")).toBeEmpty();
  const saved = (await (await request.get(`/v1/runs/${run.id}/messages`)).json()).messages;
  expect(saved[0].details.prompt_context.mentions[0].content).toBe("引用中的真值");
});

test("编辑正文保持引用位置，修改引用 token 后取消绑定", async ({ page, request }) => {
  const title = `引用位置-${Date.now()}`;
  const topic = await (await request.post("/v1/topics", { data: { title } })).json();
  await request.post(`/v1/topics/${topic.id}/mentions`, { data: { label: "doc", content: "value" } });
  await page.getByRole("button", { name: "刷新", exact: true }).click();
  await page.getByRole("button", { name: new RegExp(title) }).click();
  await page.getByText("引用与背景上下文", { exact: true }).click();
  await page.locator("#prompt").fill("😀 ");
  await page.getByRole("button", { name: "插入 @doc" }).click();
  await page.locator("#prompt").press("Home");
  await page.keyboard.insertText("前缀 ");
  const accepted = page.waitForResponse((response) => response.url().endsWith("/messages") && response.request().method() === "POST");
  await page.getByRole("button", { name: "发送", exact: true }).click();
  const run = await (await accepted).json();
  await expect(page.locator("#prompt")).toBeEnabled();
  const saved = (await (await request.get(`/v1/runs/${run.id}/messages`)).json()).messages;
  expect(saved[0].details.prompt_context.mentions[0]).toMatchObject({ start: 5, end: 9 });
  await page.locator("#prompt").fill("");
  await page.getByRole("button", { name: "插入 @doc" }).click();
  await page.locator("#prompt").press("ArrowLeft");
  await page.keyboard.insertText("x");
  await expect(page.locator("#mention-draft")).toBeEmpty();
});

test("Python Host 通过 iframe 回程保存引用和背景目录", async ({ page, request, baseURL }) => {
  await request.post("/v1/topics", { data: { title: `Host 上下文-${Date.now()}` } });
  await page.goto("/ui/host.html");
  await page.locator("#host-key").fill("browser-test-key");
  await page.getByRole("button", { name: "连接 iframe" }).click();
  await expect(page.locator("#host-status")).toContainText("iframe 已连接");
  await expect(page.locator("#host-session")).not.toBeEmpty();
  const sessionId = await page.locator("#host-session").textContent();
  const result = await promisify(execFile)("../.venv/bin/python", ["../tests/fixtures/host_sdk_client.py", baseURL!, sessionId!, "prompt-context"], { env: { ...process.env, KRIM_AGENT_API_KEY: "browser-test-key" }, timeout: 20_000 });
  const value = JSON.parse(result.stdout);
  const context = await (await request.get(`/v1/topics/${value.topic_id}/prompt-context`)).json();
  expect(context.mentions.find((item: { id: string }) => item.id === value.mention_id).content).toBe("Python 提供的引用");
  expect(context.context_tags.find((item: { name: string }) => item.name === "宿主背景").content).toBe("Python 提供的背景");
  const frame = page.frameLocator("#agent-frame");
  await frame.getByText("引用与背景上下文", { exact: true }).click();
  await expect(frame.getByRole("button", { name: "插入 @宿主引用" })).toBeVisible();
  await expect(frame.locator("#context-tag-list")).toContainText("宿主背景");
});
