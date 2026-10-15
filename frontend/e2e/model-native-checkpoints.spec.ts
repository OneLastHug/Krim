import { destroyAgent } from "./session-fixtures.js";
import { expect, test, type Page } from "@playwright/test";
import { createHash } from "node:crypto";

const headers = { Authorization: "Bearer browser-test-key" };
const hash = createHash("sha256").update(Buffer.from([0, 255])).digest("hex");
test.setTimeout(90_000);
async function connect(page: Page, topic: string) {
  await page.goto(`/ui/?topic=${topic}`);
  await page.getByLabel("服务访问密钥", { exact: true }).fill("browser-test-key");
  await page.getByRole("button", { name: "连接", exact: true }).click();
  await expect(page.locator("#connection")).toHaveClass(/ready/);
}
async function detail(page: Page, id: string, checkpoint: string) {
  await page.locator(`.agent-card[data-agent-id="${id}"]`).getByRole("button", { name: "查看文件检查点", exact: true }).click();
  await expect(page.locator(`#native-checkpoint-items [data-checkpoint-id="${checkpoint}"]`)).toBeVisible();
  await page.locator(`#native-checkpoint-items [data-checkpoint-id="${checkpoint}"] button`).click();
  await expect(page.locator("#native-checkpoint-detail")).toHaveAttribute("data-checkpoint-status", "available");
}

for (const format of ["text", "bytes"]) test(`原模型${format}创建与查询，浏览器消费同原来源，刷新只GET不重跑`, async ({ page }) => {
  const response = await page.request.post("/_test/model-checkpoint-agent", { headers, data: { name: `model-native-${format}`, capture_format: format } });
  expect(response.ok(), await response.text()).toBeTruthy(); const actor = await response.json();
  await connect(page, actor.topic_id); let posts = 0;
  try {
    await page.route("**/workspace/native-checkpoints**", async route => { if (route.request().method() === "POST") posts++; await route.continue(); });
    let done = false;
    const execution = page.request.post(`/v1/agents/${actor.agent_id}/execute`, { headers, data: { prompt: "创建并查询原模型检查点", idempotency_key: "browser-original" } });
    void execution.then(() => { done = true; });
    for (let index = 0; index < 240 && !done; index++) {
      const current = await (await page.request.get(`/v1/agents/${actor.agent_id}`, { headers })).json();
      if (current.current_run_id) {
        const pending = await (await page.request.get(`/v1/runs/${current.current_run_id}/interactions?pending_only=true`, { headers })).json();
        for (const item of pending) {
          const approved = await page.request.post(`/v1/runs/${current.current_run_id}/interactions/${item.id}/respond`, { headers, data: { action: "approve" } });
          expect(approved.ok(), await approved.text()).toBeTruthy();
        }
      }
      await page.waitForTimeout(50);
    }
    const result = await execution; expect(result.ok(), await result.text()).toBeTruthy(); const completed = await result.json(); expect(completed.result.status).toBe("completed");
    const proof = await (await page.request.get(`/_test/model-checkpoint-proof/${actor.agent_id}`, { headers })).json();
    expect(proof.calls).toBe(3); expect(proof.sources).toHaveLength(1); expect(proof.sources[0]).toMatchObject({ checkpoint_id: proof.checkpoint_id, run_id: completed.result.run_id, call_id: "browser-model-create", model_bound: true });
    await detail(page, actor.agent_id, proof.checkpoint_id);
    if (format === "bytes") { await expect(page.locator("#native-checkpoint-detail")).toContainText("二进制字节"); await expect(page.locator("#native-checkpoint-detail")).toContainText(hash); }
    const url = `/v1/agents/${actor.agent_id}/workspace/native-checkpoints/${proof.checkpoint_id}`;
    const original = await (await page.request.get(url, { headers })).json(); expect(original.capture_format || "text").toBe(format);
    await page.reload(); await page.getByLabel("服务访问密钥", { exact: true }).fill("browser-test-key"); await page.getByRole("button", { name: "连接", exact: true }).click(); await expect(page.locator("#connection")).toHaveClass(/ready/);
    await detail(page, actor.agent_id, proof.checkpoint_id); await page.locator("#native-checkpoint-refresh").click();
    expect(await (await page.request.get(url, { headers })).json()).toEqual(original);
    expect(await (await page.request.get(`/_test/model-checkpoint-proof/${actor.agent_id}`, { headers })).json()).toEqual(proof); expect(posts).toBe(0);
    await page.locator("#locale-select").selectOption("en-US"); await page.setViewportSize({ width: 420, height: 860 }); await page.locator("#native-checkpoint-detail").scrollIntoViewIfNeeded();
    await page.screenshot({ path: test.info().outputPath(`model-${format}-narrow.png`) }); expect(await page.locator("#checkpoint-native").evaluate(node => node.scrollWidth <= node.clientWidth + 2)).toBe(true);
  } finally { await destroyAgent(page, actor.agent_id); }
});
