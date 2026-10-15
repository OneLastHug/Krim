import { pythonExecutable, pythonFixtureArgs } from "./python-client.js";
import { test as base, expect } from "@playwright/test";
import { spawn } from "node:child_process";
import { readFile } from "node:fs/promises";

const test = base.extend({
  request: async ({ playwright, baseURL }, use) => {
    const context = await playwright.request.newContext({ baseURL, extraHTTPHeaders: { Authorization: "Bearer browser-test-key" } });
    await use(context); await context.dispose();
  },
});
function pythonHost(baseURL: string, sessionId: string, topicId: string, mode = "success") {
  const process = spawn(pythonExecutable, [...pythonFixtureArgs("browser_change_host.py"), baseURL, sessionId, topicId, mode], { env: { ...globalThis.process.env, KRIM_AGENT_API_KEY: "browser-test-key" } });
  let output = "", errors = "";
  process.stdout.on("data", (data) => { output += data.toString(); }); process.stderr.on("data", (data) => { errors += data.toString(); });
  const ready = new Promise<{ directory: string; revision: number }>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`Python Host 未就绪: ${errors}`)), 12_000);
    process.stdout.on("data", () => { for (const line of output.split("\n")) { if (!line) continue; const value = JSON.parse(line); if (value.event === "ready") { clearTimeout(timer); resolve(value); } } });
    process.on("error", (error) => { clearTimeout(timer); reject(error); });
    process.on("exit", (code) => { clearTimeout(timer); if (code !== 0) reject(new Error(`Python Host 退出 ${code}: ${errors}`)); });
  });
  return { ready, output: () => output, close: async () => { if (process.exitCode === null) { process.kill("SIGTERM"); await new Promise((resolve) => process.once("exit", resolve)); } } };
}

test("Python Host 同步清单，iframe 单条 undo/keep 真实修改文件并发送点击通知", async ({ page, request, baseURL }) => {
  const topic = await (await request.post("/v1/topics", { data: { title: `Changes-${Date.now()}` } })).json();
  await page.goto("/ui/host.html"); await page.locator("#host-key").fill("browser-test-key"); await page.locator("#host-connect").click(); await expect(page.locator("#host-status")).toContainText("iframe 已连接");
  const frame = page.frameLocator("#agent-frame"); await frame.locator(".topic-button").filter({ hasText: topic.title }).click(); await expect(frame.locator("#topic-heading")).toHaveText(topic.title);
  const sid = await page.locator("#host-session").textContent(); const host = pythonHost(baseURL!, sid!, topic.id);
  try {
    const ready = await host.ready;
    await frame.getByText("变更清单", { exact: true }).click();
    await expect(frame.locator(".change-card")).toHaveCount(2); await expect(frame.locator("#change-html")).toHaveCount(0);
    const one = frame.locator('[data-change-id="one"]'), two = frame.locator('[data-change-id="two"]');
    await one.locator(".change-title").click(); await expect.poll(host.output).toContain('"event": "click"');
    await one.getByRole("button", { name: "撤销变更", exact: true }).click(); await expect(one.locator('[data-change-status="undone"]')).toBeVisible();
    expect(await readFile(`${ready.directory}/one.txt`, "utf8")).toBe("旧值"); expect(await readFile(`${ready.directory}/two.txt`, "utf8")).toBe("新值");
    await two.getByRole("button", { name: "保留变更", exact: true }).click(); await expect(two.locator('[data-change-status="kept"]')).toBeVisible();
    expect(JSON.parse(await readFile(`${ready.directory}/kept-two.json`, "utf8"))).toEqual({ decision: "keep", resource: "two" });
    await page.screenshot({ path: test.info().outputPath("change-list.png"), fullPage: true });
    const operations = await (await request.get(`/v1/topics/${topic.id}/change-list/operations`)).json(); expect(operations).toHaveLength(2); expect(operations.every((op: { status: string }) => op.status === "done")).toBe(true);
    await frame.locator("#locale-select").selectOption("en-US"); await expect(one.getByRole("button", { name: "Undo change", exact: true })).toBeDisabled();
    await frame.getByRole("button", { name: "Clear list", exact: true }).click(); await expect(frame.locator(".change-card")).toHaveCount(0);
    expect(await readFile(`${ready.directory}/one.txt`, "utf8")).toBe("旧值");
    expect((await (await request.get(`/v1/topics/${topic.id}/change-list/operations`)).json())).toHaveLength(2);
  } finally { await host.close(); }
});

for (const [action, label] of [["undo-all", "全部撤销"], ["keep-all", "全部保留"]]) {
  test(`iframe ${action} 对完整绑定目标执行 Python callback`, async ({ page, request, baseURL }) => {
    const topic = await (await request.post("/v1/topics", { data: { title: `Batch-${action}-${Date.now()}` } })).json();
    await page.goto("/ui/host.html"); await page.locator("#host-key").fill("browser-test-key"); await page.locator("#host-connect").click(); await expect(page.locator("#host-status")).toContainText("iframe 已连接");
    const frame = page.frameLocator("#agent-frame"); await frame.locator(".topic-button").filter({ hasText: topic.title }).click(); await expect(frame.locator("#topic-heading")).toHaveText(topic.title);
    const sid = await page.locator("#host-session").textContent(); const host = pythonHost(baseURL!, sid!, topic.id);
    try {
      const ready = await host.ready; await frame.getByText("变更清单", { exact: true }).click();
      await frame.getByRole("button", { name: label!, exact: true }).click();
      await expect(frame.locator(`[data-change-status="${action === "undo-all" ? "undone" : "kept"}"]`)).toHaveCount(2);
      for (const name of ["one", "two"]) {
        if (action === "undo-all") expect(await readFile(`${ready.directory}/${name}.txt`, "utf8")).toBe("旧值");
        else expect(JSON.parse(await readFile(`${ready.directory}/kept-${name}.json`, "utf8"))).toMatchObject({ decision: "keep", resource: name });
      }
      const current = await (await request.get(`/v1/topics/${topic.id}/change-list`)).json(); expect(current.revision).toBe(3);
      const operation = (await (await request.get(`/v1/topics/${topic.id}/change-list/operations`)).json())[0];
      const repeat = await request.post(`/v1/topics/${topic.id}/change-list/invoke`, { data: { revision: 1, action, idempotency_key: operation.idempotency_key, host_session_id: sid } });
      expect(repeat.ok()).toBe(true); expect((await repeat.json()).execution.operation_id).toBe(operation.execution.operation_id);
      expect(host.output().split('"event": "applied"')).toHaveLength(2);
    } finally { await host.close(); }
  });
}

test("文件已改但 Python callback 抛错时显示待核实，刷新保持状态且不重放", async ({ page, request, baseURL }) => {
  const topic = await (await request.post("/v1/topics", { data: { title: `Uncertain-${Date.now()}` } })).json();
  await page.goto("/ui/host.html"); await page.locator("#host-key").fill("browser-test-key"); await page.locator("#host-connect").click(); await expect(page.locator("#host-status")).toContainText("iframe 已连接");
  const frame = page.frameLocator("#agent-frame"); await frame.locator(".topic-button").filter({ hasText: topic.title }).click(); await expect(frame.locator("#topic-heading")).toHaveText(topic.title);
  const sid = await page.locator("#host-session").textContent(); const host = pythonHost(baseURL!, sid!, topic.id, "error");
  try {
    const ready = await host.ready; await frame.getByText("变更清单", { exact: true }).click();
    await frame.locator('[data-change-id="one"]').getByRole("button", { name: "撤销变更", exact: true }).click();
    await expect(frame.locator('[data-change-status="uncertain"]')).toHaveCount(1); expect(await readFile(`${ready.directory}/one.txt`, "utf8")).toBe("旧值");
    await expect(frame.locator("#change-clear")).toBeDisabled(); await expect(frame.locator("#change-keep-all")).toBeDisabled();
    await frame.locator("#change-refresh").click(); await expect(frame.locator("#change-status")).toHaveText("结果待宿主核实");
    const operation = (await (await request.get(`/v1/topics/${topic.id}/change-list/operations`)).json())[0]; expect(operation.status).toBe("uncertain"); expect(JSON.stringify(operation)).not.toContain("private executor failure");
    const resolved = await request.post(`/v1/topics/${topic.id}/change-list/operations/${operation.execution.operation_id}/resolve`, { data: { operation_id: operation.execution.operation_id, topic_id: topic.id, revision: 1, action: "undo", results: [{ item_id: "one", status: "done" }] } });
    expect(resolved.ok()).toBe(true); await frame.locator("#change-refresh").click(); await expect(frame.locator('[data-change-status="undone"]')).toHaveCount(1); await expect(frame.locator("#change-clear")).toBeEnabled();
  } finally { await host.close(); }
});
