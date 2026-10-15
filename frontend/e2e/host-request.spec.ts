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

test("iframe App 请求经父容器和 HTTP/SSE 执行 Python callback 后返回实际回执", async ({ page, baseURL }) => {
  await page.goto("/ui/host.html"); await page.locator("#host-key").fill("browser-test-key"); await page.locator("#host-connect").click();
  await expect(page.locator("#host-status")).toContainText("iframe 已连接");
  const sid = await page.locator("#host-session").textContent();
  const result = await promisify(execFile)(pythonExecutable, [...pythonFixtureArgs("host_callback_client.py"), baseURL!, sid!], { env: { ...process.env, KRIM_AGENT_API_KEY: "browser-test-key" }, timeout: 20_000 });
  expect(JSON.parse(result.stdout)).toEqual({ status: "done", value: "before", metadata: { source: "test" } });
});
