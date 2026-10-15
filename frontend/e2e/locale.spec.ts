import { pythonExecutable, pythonFixtureArgs } from "./python-client.js";
import { test as base, expect } from "@playwright/test";
import { execFile } from "node:child_process";
import { promisify } from "node:util";

const test = base.extend({
  request: async ({ playwright, baseURL }, use) => {
    const context = await playwright.request.newContext({ baseURL, extraHTTPHeaders: { Authorization: "Bearer browser-test-key" } });
    await use(context); await context.dispose();
  },
});
test.beforeEach(async ({ page }) => {
  await page.goto("/ui/");
  await page.locator("#api-key").fill("browser-test-key");
  await page.locator("#key-form button").click();
  await expect(page.locator("#connection")).toHaveClass(/ready/);
  await page.locator("#locale-select").selectOption("zh-CN");
  await expect(page.locator("html")).toHaveAttribute("lang", "zh-CN");
});

test("Locale 真实翻译界面、日期和动态角色，刷新持久设置且保留原文与草稿", async ({ page, request }) => {
  const title = `Locale 用户标题-${Date.now()}`;
  await page.locator("#topic-title").fill(title);
  const created = page.waitForResponse((response) => response.url().endsWith("/v1/topics") && response.request().method() === "POST");
  await page.locator("#new-topic").click();
  const topic = await (await created).json();
  await expect(page.locator("#topic-heading")).toHaveText(title);
  await page.locator("#prompt").fill("/echo 用户中文 <script>literal</script>");
  await page.locator("#send").click();
  await expect(page.locator("#send")).toBeEnabled();
  const before = (await (await request.get(`/v1/topics/${topic.id}/messages`)).json()).messages;
  await page.locator(".workflow-panel summary").click();
  await page.locator('[data-workflow-node="start"]').click();
  await page.locator("#workflow-node-config").fill('{"value":"未保存配置"}');
  await page.locator(".workflow-panel summary").click();
  await page.locator(".prompt-context-panel summary").click();
  await page.locator("#mention-label").fill("中文引用");
  await page.locator("#mention-content").fill("用户提供的原始背景");
  await page.locator("#mention-add").click();
  await expect(page.locator("#mention-list")).toContainText("中文引用");
  await page.locator("#prompt").fill("😀 ");
  await page.locator("#mention-list button").first().click();
  await page.locator("#locale-select").selectOption("en-US");
  await expect(page.locator("html")).toHaveAttribute("lang", "en-US");
  await expect(page.getByRole("button", { name: "Send", exact: true })).toBeVisible();
  await expect(page.getByLabel("Send message")).toHaveValue("😀 @中文引用");
  await expect(page.locator(".message.user .role").first()).toHaveText("User");
  await expect(page.locator(".message.assistant .role").first()).toHaveText("Assistant");
  await expect(page.locator("#mention-draft")).toHaveText("Selected reference: 中文引用");
  await expect(page.getByRole("button", { name: "Insert @中文引用" })).toBeVisible();
  await expect(page.getByLabel("Node configuration JSON")).toHaveValue('{"value":"未保存配置"}');
  await expect(page.locator("#workflow-selection")).toContainText("Selected: start");
  await expect(page.locator("#notice")).toContainText("Completed");
  const shownDate = await page.locator(".topic-button.active small").textContent();
  const savedTopics = (await (await request.get("/v1/topics")).json()).topics;
  expect(shownDate).toBe(await page.evaluate((date: string) => new Date(date).toLocaleString("en-US"), savedTopics.find((item: { id: string }) => item.id === topic.id).updated_at));
  await page.screenshot({ path: "../.cache/browser-locale-en.png", fullPage: true });
  await page.reload();
  await page.locator("#api-key").fill("browser-test-key");
  await page.locator("#key-form button").click();
  await expect(page.locator("#connection")).toHaveClass(/ready/);
  await expect(page.locator("html")).toHaveAttribute("lang", "en-US");
  await expect(page.locator("#topic-heading")).toHaveText(title);
  await expect(page.locator(".message.user").first()).toContainText("用户中文 <script>literal</script>");
  expect((await (await request.get(`/v1/topics/${topic.id}/messages`)).json()).messages).toEqual(before);
  await page.locator("#locale-select").selectOption("zh-CN");
  await expect(page.getByRole("button", { name: "发送", exact: true })).toBeVisible();
});

test("话题切换恢复各自的 Locale，新话题继承当前生效语言", async ({ page, request }) => {
  const zhTitle = `中文话题-${Date.now()}`, enTitle = `English topic-${Date.now()}`;
  await request.post("/v1/topics", { data: { title: zhTitle, locale: "zh-CN" } });
  await request.post("/v1/topics", { data: { title: enTitle, locale: "en-GB" } });
  await page.locator("#refresh").click();
  await page.locator(".topic-button").filter({ hasText: enTitle }).click();
  await expect(page.locator("html")).toHaveAttribute("lang", "en-US");
  await expect(page.locator("#locale-select")).toHaveValue("en-US");
  const created = page.waitForResponse((response) => response.url().endsWith("/v1/topics") && response.request().method() === "POST");
  await page.locator("#new-topic").click();
  const topic = await (await created).json();
  expect(topic.locale).toBe("en-US"); expect(topic.title).toBe("New topic");
  await expect(page.locator("#topic-heading")).toHaveText("New topic");
  await page.locator(".topic-button").filter({ hasText: zhTitle }).click();
  await expect(page.locator("html")).toHaveAttribute("lang", "zh-CN");
  await expect(page.locator("#send")).toHaveText("发送");
});

test("Host en-GB 请求返回 Python 的 en-US 回落并在 iframe 刷新后保持", async ({ page, request }) => {
  const topic = await (await request.post("/v1/topics", { data: { title: `Host Locale-${Date.now()}` } })).json();
  await page.goto("/ui/host.html");
  await page.locator("#host-key").fill("browser-test-key"); await page.locator("#host-connect").click();
  await expect(page.locator("#host-status")).toContainText("iframe 已连接");
  const frame = page.frameLocator("#agent-frame");
  await frame.getByRole("button", { name: new RegExp(topic.title) }).click();
  await page.locator("#host-locale").selectOption("en-GB"); await page.locator("#host-locale-apply").click();
  await expect(page.locator("#host-output")).toContainText('"effective": "en-US"');
  await expect(frame.locator("html")).toHaveAttribute("lang", "en-US");
  await expect(frame.getByRole("button", { name: "Send", exact: true })).toBeVisible();
  expect((await (await request.get(`/v1/topics/${topic.id}/tree`)).json()).topic.locale).toBe("en-US");
  await page.locator("#agent-frame").evaluate((node) => { (node as HTMLIFrameElement).contentWindow!.location.reload(); });
  await expect(frame.locator("#key-form")).toBeVisible();
  await frame.locator("#api-key").fill("browser-test-key"); await frame.locator("#key-form button").click();
  await expect(frame.locator("html")).toHaveAttribute("lang", "en-US");
  await page.locator("#host-locale").selectOption("zh-CN"); await page.locator("#host-locale-apply").click();
  await expect(frame.locator("html")).toHaveAttribute("lang", "zh-CN");
});

test("Python RemoteHostHandle 控制真实 iframe 并校验生效语言回执", async ({ page, request, baseURL }) => {
  await request.post("/v1/topics", { data: { title: `Python Locale-${Date.now()}` } });
  await page.goto("/ui/host.html");
  await page.locator("#host-key").fill("browser-test-key"); await page.locator("#host-connect").click();
  await expect(page.locator("#host-status")).toContainText("iframe 已连接");
  const sessionId = await page.locator("#host-session").textContent();
  const run = promisify(execFile);
  const result = await run(pythonExecutable, [...pythonFixtureArgs("host_locale_client.py"), baseURL!, sessionId!, "en-GB"], { env: { ...process.env, KRIM_AGENT_API_KEY: "browser-test-key" }, timeout: 20_000 });
  expect(JSON.parse(result.stdout)).toMatchObject({ requested: "en-GB", effective: "en-US" });
  const frame = page.frameLocator("#agent-frame");
  await expect(frame.locator("#send")).toHaveText("Send");
  await page.locator("#host-locale").selectOption("zh-CN"); await page.locator("#host-locale-apply").click();
  await expect(frame.locator("html")).toHaveAttribute("lang", "zh-CN");
});

test("切换语言保留等待问答的用户输入和实际审批状态", async ({ page, request }) => {
  await page.locator("#topic-title").fill(`Locale 问答-${Date.now()}`); await page.locator("#new-topic").click();
  await expect(page.locator("#prompt")).toBeEnabled();
  const accepted = page.waitForResponse((response) => response.url().endsWith("/messages") && response.request().method() === "POST");
  await page.locator("#prompt").fill("/question 用户原始问题"); await page.locator("#send").click();
  const run = await (await accepted).json();
  await expect(page.getByRole("button", { name: "批准此次调用" })).toBeVisible();
  await page.locator("#locale-select").selectOption("en-US");
  await expect(page.getByRole("button", { name: "Approve this call" })).toBeVisible();
  await page.getByRole("button", { name: "Approve this call" }).click();
  await expect(page.getByRole("textbox", { name: "User answer" })).toBeVisible();
  await page.getByRole("textbox", { name: "User answer" }).fill("未提交的中文答案");
  await page.locator("#locale-select").selectOption("zh-CN");
  await expect(page.getByRole("textbox", { name: "用户答案" })).toHaveValue("未提交的中文答案");
  const pending = await (await request.get(`/v1/runs/${run.id}/interactions?pending_only=true`)).json();
  expect(pending).toHaveLength(1); expect(pending[0].kind).toBe("question");
  await page.getByRole("button", { name: "提交答案" }).click();
  await expect(page.locator(".message.assistant").last()).toContainText("未提交的中文答案");
  await expect(page.locator("#send")).toBeEnabled();
});
