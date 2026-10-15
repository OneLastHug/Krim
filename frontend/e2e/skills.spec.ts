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
test.beforeEach(async ({ page, request }) => {
  const topic = await (await request.post("/v1/topics", { data: { title: `Skills-${Date.now()}` } })).json();
  await page.goto(`/ui/?topic=${topic.id}`);
  await page.locator("#api-key").fill("browser-test-key"); await page.locator("#key-form button").click();
  await expect(page.locator("#connection")).toHaveClass(/ready/);
  await page.locator(".skill-panel summary").click();
});

test("技能目录合并/正文预览/资源读取和模型加载事件是真实持久数据", async ({ page, request }) => {
  await page.getByLabel("技能名称").fill("review"); await page.getByLabel("技能用途").fill("检查业务输入");
  await page.getByLabel("技能正文", { exact: true }).fill("浏览器技能原始正文 <img id='skill-injected' src='x'>");
  await page.getByLabel("文本资源名称").fill("references/guide.md"); await page.getByLabel("文本资源内容").fill("浏览器资源清单");
  await page.getByRole("button", { name: "添加文本资源", exact: true }).click();
  await page.getByRole("button", { name: "保存技能", exact: true }).click();
  const card = page.locator('[data-skill-name="review"]'); await expect(card).toContainText("检查业务输入");
  await card.getByRole("button", { name: "预览技能正文" }).click();
  await expect(page.locator("#skill-preview")).toContainText("浏览器技能原始正文"); await expect(page.locator("#skill-injected")).toHaveCount(0);
  await card.getByRole("button", { name: "references/guide.md", exact: true }).click();
  await expect(page.locator("#skill-preview")).toHaveText("浏览器资源清单");
  await expect(page.locator("#skill-load-state")).toBeEmpty();
  await page.locator(".skill-panel summary").click();
  const accepted = page.waitForResponse((response) => response.url().endsWith("/messages") && response.request().method() === "POST");
  await page.locator("#prompt").fill("/skill review references/guide.md"); await page.locator("#send").click();
  const run = await (await accepted).json();
  await expect(page.locator("#send")).toBeEnabled();
  await page.locator(".skill-panel summary").click();
  await expect(page.locator("#skill-load-state")).toContainText("已加载技能：review · references/guide.md");
  const history = (await (await request.get(`/v1/runs/${run.id}/messages`)).json()).messages;
  expect(history[0].content).toBe("/skill review references/guide.md");
  expect(history.find((item: { role: string }) => item.role === "tool").details.data.content).toBe("浏览器资源清单");
  await page.reload(); await page.locator("#api-key").fill("browser-test-key"); await page.locator("#key-form button").click();
  await expect(page.locator("#connection")).toHaveClass(/ready/); await page.locator(".skill-panel summary").click();
  await expect(page.locator('[data-skill-name="review"]')).toContainText("检查业务输入");
  await page.getByLabel("技能名称").fill("review"); await page.getByLabel("技能用途").fill("更新用途"); await page.getByLabel("技能正文", { exact: true }).fill("更新正文");
  await page.getByRole("button", { name: "保存技能", exact: true }).click();
  await expect(page.locator(".skill-card")).toHaveCount(1); await expect(card).toContainText("更新用途");
  await card.getByRole("button", { name: "删除技能" }).click(); await expect(page.locator(".skill-card")).toHaveCount(0);
  expect((await (await request.get(`/v1/runs/${run.id}/messages`)).json()).messages).toEqual(history);
});

test("选择发送技能与中英文文案切换保持用户正文和技能快照", async ({ page, request }) => {
  await page.getByLabel("技能名称").fill("review"); await page.getByLabel("技能用途").fill("选择检查技能"); await page.getByLabel("技能正文", { exact: true }).fill("当前发送明确选择的技能正文");
  await page.getByRole("button", { name: "保存技能", exact: true }).click();
  await page.getByRole("checkbox", { name: "本次使用技能 review" }).check();
  await page.locator("#locale-select").selectOption("en-US");
  await expect(page.getByRole("checkbox", { name: "Use skill review for this message" })).toBeChecked();
  await expect(page.locator("#skill-selection")).toHaveText("Selected skills: review");
  await page.locator(".skill-panel summary").click();
  const accepted = page.waitForResponse((response) => response.url().endsWith("/messages") && response.request().method() === "POST");
  await page.locator("#prompt").fill("用户原始问题"); await page.locator("#send").click(); const run = await (await accepted).json();
  await expect(page.locator("#send")).toBeEnabled();
  const history = (await (await request.get(`/v1/runs/${run.id}/messages`)).json()).messages;
  expect(history[0].content).toBe("用户原始问题"); expect(history[0].details.skills_snapshot.selected_names).toEqual(["review"]);
  expect(history[0].details.prompt_nodes).toEqual([{ type: "text", text: "用户原始问题" }, { type: "skill", name: "review" }]);
  await expect(page.locator(".message.assistant").last()).toContainText("当前发送明确选择的技能正文");
  await page.locator("#locale-select").selectOption("zh-CN"); await expect(page.locator("html")).toHaveAttribute("lang", "zh-CN");
});

test("Python Host 经 HTTP/SSE/iframe 注入查询并读取技能资源", async ({ page, request, baseURL }) => {
  const topicId = new URL(page.url()).searchParams.get("topic")!;
  await page.goto("/ui/host.html"); await page.locator("#host-key").fill("browser-test-key"); await page.locator("#host-connect").click();
  await expect(page.locator("#host-status")).toContainText("iframe 已连接");
  const frame = page.frameLocator("#agent-frame");
  const title = (await (await request.get(`/v1/topics/${topicId}/tree`)).json()).topic.title;
  await frame.locator(".topic-button").filter({ hasText: title }).click();
  await expect(frame.locator("#topic-heading")).toHaveText(title);
  const sid = await page.locator("#host-session").textContent();
  const result = await promisify(execFile)(pythonExecutable, [...pythonFixtureArgs("host_skills_client.py"), baseURL!, sid!], { env: { ...process.env, KRIM_AGENT_API_KEY: "browser-test-key" }, timeout: 20_000 });
  expect(JSON.parse(result.stdout)).toMatchObject({ topic_id: topicId, name: "host-review", count: 1 });
  await frame.locator(".skill-panel summary").click(); await expect(frame.locator('[data-skill-name="host-review"]')).toContainText("Python 宿主注入的检查技能");
  expect((await (await request.get(`/v1/topics/${topicId}/skills/host-review`)).json()).content).toBe("Python 技能正文");
});
