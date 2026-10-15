import { pythonExecutable, pythonFixtureArgs } from "./python-client.js";
import { test as base, expect } from "@playwright/test";
import { spawn } from "node:child_process";
import { readFile, writeFile } from "node:fs/promises";

const test = base.extend({
  request: async ({ playwright, baseURL }, use) => {
    const context = await playwright.request.newContext({ baseURL, extraHTTPHeaders: { Authorization: "Bearer browser-test-key" } });
    await use(context); await context.dispose();
  },
});

test("宿主可回滚检查尚未返回时切换话题，不继续回滚旧话题", async ({ page, request, baseURL }) => {
  const original = await (await request.post("/v1/topics", { data: { title: `Checkpoint-old-${Date.now()}` } })).json();
  const next = await (await request.post("/v1/topics", { data: { title: `Checkpoint-next-${Date.now()}` } })).json();
  await page.goto("/ui/host.html"); await page.locator("#host-key").fill("browser-test-key"); await page.locator("#host-connect").click();
  await expect(page.locator("#host-status")).toContainText("iframe 已连接");
  await expect(page.locator("#host-session")).not.toBeEmpty();
  const frame = page.frameLocator("#agent-frame");
  await frame.locator(".topic-button").filter({ hasText: original.title }).click();
  const host = pythonHost(baseURL!, (await page.locator("#host-session").textContent())!, original.id, "check-paused");
  try {
    const ready = await host.ready;
    await frame.getByText("资源快照", { exact: true }).click(); await frame.locator("#checkpoint-create").click();
    await expect(frame.locator('[data-checkpoint-status="available"]')).toHaveCount(1);
    await writeFile(`${ready.directory}/resource.txt`, "新值");
    await frame.locator(".checkpoint-card").getByRole("button", { name: "回滚到此快照", exact: true }).click();
    await expect.poll(async () => readFile(`${ready.directory}/checking`, "utf8").then(() => true).catch(() => false)).toBe(true);
    await frame.locator(".topic-button").filter({ hasText: next.title }).click();
    await expect(frame.locator("#topic-heading")).toHaveText(next.title);
    await writeFile(`${ready.directory}/release`, "go");
    await expect.poll(async () => readFile(`${ready.directory}/checked`, "utf8").then(() => true).catch(() => false)).toBe(true);
    // 后续查询保持新话题；原检查只读返回不应触发第二个文件动作。
    await frame.locator("#checkpoint-refresh").click();
    await expect(frame.locator("#checkpoint-items .checkpoint-card")).toHaveCount(0);
    expect((await (await request.get(`/v1/topics/${original.id}/checkpoints/operations`)).json())).toHaveLength(1);
    expect(await readFile(`${ready.directory}/resource.txt`, "utf8")).toBe("新值");
  } finally { await host.close(); }
});
function pythonHost(baseURL: string, sessionId: string, topicId: string, mode = "success") {
  if (!sessionId) throw new Error("Checkpoint Host 需要已就绪的控制会话");
  const process = spawn(pythonExecutable, [...pythonFixtureArgs("browser_checkpoint_host.py"), baseURL, sessionId, topicId, mode], { env: { ...globalThis.process.env, KRIM_AGENT_API_KEY: "browser-test-key" } });
  let output = "", errors = "";
  process.stdout.on("data", (data) => { output += data.toString(); }); process.stderr.on("data", (data) => { errors += data.toString(); });
  const ready = new Promise<{ directory: string }>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`Python Checkpoint Host 未就绪: ${errors}`)), 12_000);
    process.stdout.on("data", () => { for (const line of output.split("\n")) { if (!line) continue; const value = JSON.parse(line); if (value.event === "ready") { clearTimeout(timer); resolve(value); } } });
    process.on("error", (error) => { clearTimeout(timer); reject(error); }); process.on("exit", (code) => { clearTimeout(timer); if (code !== 0) reject(new Error(`Python Host 退出 ${code}: ${errors}`)); });
  });
  return { ready, close: async () => { if (process.exitCode === null) { process.kill("SIGTERM"); await new Promise((resolve) => process.once("exit", resolve)); } } };
}

test("iframe 创建宿主快照，修改真实文件后检查可回滚并恢复原值", async ({ page, request, baseURL }) => {
  const topic = await (await request.post("/v1/topics", { data: { title: `Checkpoint-${Date.now()}` } })).json();
  await page.goto("/ui/host.html"); await page.locator("#host-key").fill("browser-test-key"); await page.locator("#host-connect").click(); await expect(page.locator("#host-status")).toContainText("iframe 已连接");
  await expect(page.locator("#host-session")).not.toBeEmpty();
  const frame = page.frameLocator("#agent-frame"); await frame.locator(".topic-button").filter({ hasText: topic.title }).click();
  const sid = await page.locator("#host-session").textContent(); const host = pythonHost(baseURL!, sid!, topic.id);
  try {
    const ready = await host.ready; await frame.getByText("资源快照", { exact: true }).click(); await frame.locator("#checkpoint-title").fill("文件原值"); await frame.locator("#checkpoint-create").click();
    await expect(frame.locator('[data-checkpoint-status="available"]')).toHaveCount(1); expect(await readFile(`${ready.directory}/resource.txt`, "utf8")).toBe("原值");
    await writeFile(`${ready.directory}/resource.txt`, "新值");
    await frame.locator(".checkpoint-card").getByRole("button", { name: "回滚到此快照", exact: true }).click(); await expect(frame.locator('[data-checkpoint-status="rolled_back"]')).toHaveCount(1); expect(await readFile(`${ready.directory}/resource.txt`, "utf8")).toBe("原值");
    await frame.locator("#locale-select").selectOption("en-US"); await expect(frame.locator(".checkpoint-card").getByRole("button", { name: "Rollback to this checkpoint", exact: true })).toBeEnabled();
    await expect(frame.locator('[data-checkpoint-status="rolled_back"]')).toHaveText("Rolled back"); await expect(frame.locator(".checkpoint-card strong")).toHaveText("文件原值");
    await page.screenshot({ path: test.info().outputPath("checkpoint.png"), fullPage: true });
    const catalog = await (await request.get(`/v1/topics/${topic.id}/checkpoints`)).json(); expect(catalog.checkpoints[0].metadata).toMatchObject({ source: "AgentApp" });
    expect((await (await request.get(`/v1/topics/${topic.id}/checkpoints/operations`)).json()).every((item: { status: string }) => item.status === "done")).toBe(true);
  } finally { await host.close(); }
});

test("宿主拒绝 canRollback 时不执行回滚，文件仍保持修改值", async ({ page, request, baseURL }) => {
  const topic = await (await request.post("/v1/topics", { data: { title: `Checkpoint-deny-${Date.now()}` } })).json();
  await page.goto("/ui/host.html"); await page.locator("#host-key").fill("browser-test-key"); await page.locator("#host-connect").click();
  await expect(page.locator("#host-status")).toContainText("iframe 已连接");
  await expect(page.locator("#host-session")).not.toBeEmpty();
  const frame = page.frameLocator("#agent-frame"); await frame.locator(".topic-button").filter({ hasText: topic.title }).click(); const sid = await page.locator("#host-session").textContent(); const host = pythonHost(baseURL!, sid!, topic.id, "deny");
  try {
    const ready = await host.ready; await frame.getByText("资源快照", { exact: true }).click(); await frame.locator("#checkpoint-create").click(); await expect(frame.locator('[data-checkpoint-status="available"]')).toHaveCount(1); await writeFile(`${ready.directory}/resource.txt`, "新值");
    await frame.locator(".checkpoint-card").getByRole("button", { name: "回滚到此快照", exact: true }).click(); await expect(frame.locator("#notice")).toHaveClass(/error/); expect(await readFile(`${ready.directory}/resource.txt`, "utf8")).toBe("新值");
    expect((await (await request.get(`/v1/topics/${topic.id}/checkpoints/operations`)).json())).toHaveLength(1);
  } finally { await host.close(); }
});

test("回滚写入资源后 callback 抛错会保留 uncertain，刷新不自动重放", async ({ page, request, baseURL }) => {
  const topic = await (await request.post("/v1/topics", { data: { title: `Checkpoint-uncertain-${Date.now()}` } })).json();
  await page.goto("/ui/host.html"); await page.locator("#host-key").fill("browser-test-key"); await page.locator("#host-connect").click();
  await expect(page.locator("#host-status")).toContainText("iframe 已连接");
  await expect(page.locator("#host-session")).not.toBeEmpty();
  const frame = page.frameLocator("#agent-frame"); await frame.locator(".topic-button").filter({ hasText: topic.title }).click(); const sid = await page.locator("#host-session").textContent(); const host = pythonHost(baseURL!, sid!, topic.id, "rollback-error");
  try {
    const ready = await host.ready; await frame.getByText("资源快照", { exact: true }).click(); await frame.locator("#checkpoint-create").click(); await expect(frame.locator('[data-checkpoint-status="available"]')).toHaveCount(1); await writeFile(`${ready.directory}/resource.txt`, "新值"); await frame.locator(".checkpoint-card").getByRole("button", { name: "回滚到此快照", exact: true }).click();
    await expect(frame.locator('[data-checkpoint-status="uncertain"]')).toHaveCount(1); expect(await readFile(`${ready.directory}/resource.txt`, "utf8")).toBe("原值"); await frame.locator("#checkpoint-refresh").click(); await expect(frame.locator('[data-checkpoint-status="uncertain"]')).toHaveCount(1);
    const operation = (await (await request.get(`/v1/topics/${topic.id}/checkpoints/operations`)).json()).find((item: { status: string }) => item.execution.action === "rollback"); expect(operation.status).toBe("uncertain"); expect(JSON.stringify(operation)).not.toContain("private rollback failure");
    const resolved = await request.post(`/v1/topics/${topic.id}/checkpoints/operations/${operation.execution.operation_id}/resolve`, { data: { operation_id: operation.execution.operation_id, topic_id: topic.id, checkpoint_id: operation.execution.checkpoint_id, revision: operation.execution.revision, action: "rollback", status: "done", snapshot_ref: operation.execution.snapshot_ref } }); expect(resolved.ok()).toBe(true);
  } finally { await host.close(); }
});
