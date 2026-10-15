import { pythonExecutable, pythonFixtureArgs } from "./python-client.js";
import { test, expect } from "@playwright/test";
import { execFile } from "node:child_process";
import { promisify } from "node:util";

test("Python Host 经真实 HTTP/SSE/iframe 查询并确认持久提案", async ({ page, playwright, baseURL }) => {
  const request = await playwright.request.newContext({ baseURL, extraHTTPHeaders: { Authorization: "Bearer browser-test-key" } });
  try {
    const topic = await (await request.post("/v1/topics", { data: { title: `宿主提案-${Date.now()}` } })).json();
    for (let index = 0; index < 4; index++) {
      const run = await (await request.post(`/v1/topics/${topic.id}/messages`, { data: { text: `历史 ${index}`, idempotency_key: `host-history-${index}` } })).json();
      await request.get(`/v1/runs/${run.id}/events`);
    }
    const preview = await (await request.post(`/v1/topics/${topic.id}/compact`, { data: { max_messages: 3 } })).json();
    expect(preview.status).toBe("proposed");
    await page.goto("/ui/host.html");
    await page.locator("#host-key").fill("browser-test-key");
    await page.getByRole("button", { name: "连接 iframe" }).click();
    await expect(page.locator("#host-session")).not.toBeEmpty();
    await expect(page.locator("#host-status")).toContainText("iframe 已连接");
    const frame = page.frameLocator("#agent-frame");
    await frame.locator(".topic-button").filter({ hasText: topic.title }).click();
    await expect(frame.locator("#topic-heading")).toHaveText(topic.title);
    const sessionId = await page.locator("#host-session").textContent();
    const result = await promisify(execFile)(pythonExecutable, [...pythonFixtureArgs("host_context_proposal_client.py"), baseURL!, sessionId!, topic.id, preview.proposal_id], { env: { ...process.env, KRIM_AGENT_API_KEY: "browser-test-key" }, timeout: 20_000 });
    expect(JSON.parse(result.stdout)).toEqual({ topic_id: topic.id, proposal_id: preview.proposal_id, status: "completed" });
    await expect(frame.locator("#compact-title")).toContainText("摘要已保存");
    await expect(frame.locator("#compact-summary")).toHaveText(preview.summary);
  } finally { await request.dispose(); }
});
