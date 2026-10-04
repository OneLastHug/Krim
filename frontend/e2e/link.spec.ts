import { test as base, expect } from "@playwright/test";
import { spawn } from "node:child_process";

const test = base.extend({
  request: async ({ playwright, baseURL }, use) => {
    const context = await playwright.request.newContext({ baseURL, extraHTTPHeaders: { Authorization: "Bearer browser-test-key" } });
    await use(context); await context.dispose();
  },
});

test("正文只把安全 HTTP/HTTPS URL 转为通知链接，危险 scheme 保持纯文本", async ({ page, request }) => {
  const topic = await (await request.post("/v1/topics", { data: { title: `Link-${Date.now()}` } })).json();
  await page.goto(`/ui/?topic=${topic.id}`); await page.locator("#api-key").fill("browser-test-key"); await page.locator("#key-form button").click();
  await expect(page.locator("#connection")).toHaveClass(/ready/);
  await page.locator("#prompt").fill("查看 https://example.com/docs?q=1#part"); await page.locator("#send").click(); await expect(page.locator("#send")).toBeEnabled();
  const safe = page.locator(".message.assistant a"); await expect(safe).toHaveCount(1); await expect(safe).toHaveAttribute("href", "https://example.com/docs?q=1#part");
  await page.locator("#prompt").fill("不要执行 javascript:alert(1)"); await page.locator("#send").click(); await expect(page.locator("#send")).toBeEnabled();
  await expect(page.locator(".message.assistant").last().locator("a")).toHaveCount(0); await expect(page.locator(".message.assistant").last()).toContainText("javascript:alert(1)");
  expect((await (await request.get(`/v1/topics/${topic.id}/messages`)).json()).messages.at(-1).content).toContain("不要执行");
});

test("同源 iframe 点击安全 Link 后通过 Host/Python 控制会话通知父容器", async ({ page, request }) => {
  const topic = await (await request.post("/v1/topics", { data: { title: `Host Link-${Date.now()}` } })).json();
  await page.goto("/ui/host.html"); await page.locator("#host-key").fill("browser-test-key"); await page.locator("#host-connect").click(); await expect(page.locator("#host-status")).toContainText("iframe 已连接");
  const frame = page.frameLocator("#agent-frame"); await frame.locator(".topic-button").filter({ hasText: topic.title }).click();
  await frame.locator("#prompt").fill("打开 https://example.com/host-link#section"); await frame.locator("#send").click(); await expect(frame.locator("#send")).toBeEnabled();
  await frame.locator(".message.assistant a").click(); await expect(page.locator("#host-output")).toContainText("https://example.com/host-link#section");
  await expect(page.locator("#host-status")).toContainText("收到链接通知");
});

test("真实 Python Host 从 SSE 消费一次 iframe 导航点击和消息关联", async ({ page, request, baseURL }) => {
  const topic = await (await request.post("/v1/topics", { data: { title: `Python Link-${Date.now()}` } })).json();
  await page.goto("/ui/host.html"); await page.locator("#host-key").fill("browser-test-key"); await page.locator("#host-connect").click();
  await expect(page.locator("#host-status")).toContainText("iframe 已连接");
  const frame = page.frameLocator("#agent-frame"); await frame.locator(".topic-button").filter({ hasText: topic.title }).click();
  await frame.locator("#prompt").fill("链接 http://example.invalid/docs?x=1#section"); await frame.locator("#send").click(); await expect(frame.locator("#send")).toBeEnabled();
  const session = await page.locator("#host-session").textContent();
  const child = spawn("../.venv/bin/python", ["../tests/fixtures/host_link_listener.py", baseURL!, session!], { env: { ...process.env, KRIM_AGENT_API_KEY: "browser-test-key" } });
  let stdout = "", stderr = "";
  child.stdout.on("data", (value: Buffer) => { stdout += value.toString(); }); child.stderr.on("data", (value: Buffer) => { stderr += value.toString(); });
  const closed = new Promise<number | null>((resolve, reject) => { child.on("error", reject); child.on("close", resolve); });
  try {
    await expect.poll(() => stdout).toContain("ready");
    await frame.locator(".message.assistant a").click();
    expect(await closed, stderr).toBe(0);
    const value = JSON.parse(stdout.trim().split("\n").at(-1)!);
    expect(value).toMatchObject({ url: "http://example.invalid/docs?x=1#section", topic_id: topic.id });
    expect(value.message_id).toBeTruthy();
    await expect(page.locator("#host-status")).toContainText("收到链接通知");
  } finally { if (child.exitCode === null) child.kill(); }
});
