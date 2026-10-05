import { test as base, expect } from "@playwright/test";

const test = base.extend({
  request: async ({ playwright, baseURL }, use) => {
    const context = await playwright.request.newContext({ baseURL, extraHTTPHeaders: { Authorization: "Bearer browser-test-key" } });
    await use(context); await context.dispose();
  },
});

async function connect(page: import("@playwright/test").Page): Promise<void> {
  await page.getByLabel("服务访问密钥").fill("browser-test-key");
  await page.getByRole("button", { name: "连接", exact: true }).click();
  await expect(page.locator("#connection")).toContainText("本地服务已连接");
}

test.beforeEach(async ({ page, request }) => {
  await request.post("/_test/context-config", { data: {} });
  await page.goto("/ui/"); await connect(page);
});
test.afterEach(async ({ request }) => { await request.post("/_test/context-config", { data: {} }); });

async function seed(request: import("@playwright/test").APIRequestContext) {
  const topic = await (await request.post("/v1/topics", { data: { title: `持久提案-${Date.now()}` } })).json();
  for (let index = 0; index < 4; index++) {
    const run = await (await request.post(`/v1/topics/${topic.id}/messages`, { data: { text: `历史 ${index}`, idempotency_key: `history-${index}` } })).json();
    await request.get(`/v1/runs/${run.id}/events`);
  }
  return topic;
}

test("预览刷新后恢复同一份摘要并确认原提案", async ({ page, request }) => {
  const topic = await seed(request);
  await page.getByRole("button", { name: "刷新", exact: true }).click();
  await page.locator(".topic-button").filter({ hasText: topic.title }).click();
  await page.locator("#compact-budget").fill("3");
  await page.getByRole("button", { name: "预览压缩" }).click();
  await expect(page.locator("#compact-confirm")).toBeEnabled();
  const summary = await page.locator("#compact-summary").textContent();
  const proposals = await (await request.get(`/v1/topics/${topic.id}/compact-proposals?pending_only=true`)).json();
  expect(proposals).toHaveLength(1);
  await page.reload(); await connect(page);
  await expect(page.locator("#compact-title")).toContainText("压缩预览");
  await expect(page.locator("#compact-summary")).toHaveText(summary!);
  await expect(page.locator("#compact-expiry")).toContainText("有效期至");
  const decision = page.waitForResponse((response) => response.url().endsWith(`/${proposals[0].id}/decision`));
  await page.locator("#compact-confirm").click();
  expect((await (await decision).json()).result.summary).toBe(proposals[0].result.summary);
  await expect(page.locator("#compact-title")).toContainText("摘要已保存");
  expect((await (await request.get(`/v1/topics/${topic.id}/messages`)).json()).messages).toHaveLength(8);
});

test("拒绝和过期提案刷新后不会恢复成待办", async ({ page, request }) => {
  const topic = await seed(request);
  await page.getByRole("button", { name: "刷新", exact: true }).click();
  await page.locator(".topic-button").filter({ hasText: topic.title }).click();
  await page.locator("#compact-budget").fill("3");
  await page.locator("#compact-preview").click();
  await expect(page.locator("#compact-reject")).toBeEnabled();
  await page.locator("#compact-reject").click();
  await expect(page.locator("#compact-title")).toContainText("压缩已拒绝");
  await page.reload(); await connect(page);
  await expect(page.locator("#compact-confirm")).toBeDisabled();
  expect(await (await request.get(`/v1/topics/${topic.id}/compact-proposals?pending_only=true`)).json()).toEqual([]);
  await request.post("/_test/context-config", { data: { ttl_seconds: 2 } });
  await page.locator("#compact-budget").fill("3");
  await page.locator("#compact-preview").click();
  await expect(page.locator("#compact-confirm")).toBeEnabled();
  await expect.poll(async () => (await (await request.get(`/v1/topics/${topic.id}/compact-proposals`)).json())[0].status).toBe("expired");
  await page.getByRole("button", { name: "刷新", exact: true }).click();
  await expect(page.locator("#compact-title")).toContainText("压缩提案已过期");
  await expect(page.locator("#compact-confirm")).toBeDisabled();
});

test("运行中提案刷新后可在生成状态确认并继续同一运行", async ({ page, request }) => {
  const topic = await seed(request);
  await page.getByRole("button", { name: "刷新", exact: true }).click();
  await page.locator(".topic-button").filter({ hasText: topic.title }).click();
  await request.post("/_test/context-config", { data: { mode: "confirmation", max_messages: 3 } });
  const accepted = page.waitForResponse((response) => response.url().endsWith(`/v1/topics/${topic.id}/messages`) && response.request().method() === "POST");
  await page.locator("#prompt").fill("确认之后的回答");
  await page.getByRole("button", { name: "发送", exact: true }).click();
  const run = await (await accepted).json();
  await expect(page.locator("#compact-confirm")).toBeEnabled();
  await expect(page.locator("#prompt")).toBeDisabled();
  const original = await page.locator("#compact-summary").textContent();
  expect((await (await request.get(`/v1/runs/${run.id}`)).json()).status).toBe("running");
  await page.reload(); await connect(page);
  await expect(page.locator("#compact-summary")).toHaveText(original!);
  await page.getByRole("button", { name: "继续读取", exact: true }).click();
  await expect(page.locator("#prompt")).toBeDisabled();
  await expect(page.locator("#compact-confirm")).toBeEnabled();
  await page.locator("#compact-confirm").click();
  await expect(page.locator(".message.assistant").last()).toContainText("确认之后的回答");
  await expect.poll(async () => (await (await request.get(`/v1/runs/${run.id}`)).json()).status).toBe("completed");
  expect(await (await request.get(`/v1/topics/${topic.id}/compact-proposals?pending_only=true`)).json()).toEqual([]);
  await page.screenshot({ path: "../.cache/browser-context-proposals.png", fullPage: true });
});
