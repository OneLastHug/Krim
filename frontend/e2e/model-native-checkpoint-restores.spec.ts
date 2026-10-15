import { expect, test, type Page } from "@playwright/test";
import { createHash } from "node:crypto";

const headers = { Authorization: "Bearer browser-test-key" };
test.setTimeout(120_000);
async function connect(page: Page, topic: string) {
  await page.goto(`/ui/?topic=${topic}`); await page.getByLabel("服务访问密钥", { exact: true }).fill("browser-test-key");
  await page.getByRole("button", { name: "连接", exact: true }).click(); await expect(page.locator("#connection")).toHaveClass(/ready/);
}
async function detail(page: Page, agent: string, plan: string) {
  await page.locator(`.agent-card[data-agent-id="${agent}"]`).getByRole("button", { name: "查看文件检查点", exact: true }).click();
  const row = page.locator(`#native-restore-items [data-plan-id="${plan}"]`); await expect(row).toBeVisible(); await row.locator("button").click();
  await expect(page.locator("#native-restore-detail")).toHaveAttribute("data-restore-status", "done");
}

for (const format of ["text", "bytes"]) test(`原模型${format}恢复与基线，浏览器原来源同事实及刷新零重放`, async ({ page }) => {
  const created = await page.request.post("/_test/model-restore-agent", { headers, data: { name: `model-restore-${format}`, capture_format: format } });
  expect(created.ok(), await created.text()).toBeTruthy(); const actor = await created.json(); let posts = 0;
  await connect(page, actor.topic_id);
  try {
    await page.route("**/workspace/native-**", async route => { if (route.request().method() === "POST") posts++; await route.continue(); });
    let done = false;
    const execution = page.request.post(`/v1/agents/${actor.agent_id}/execute`, { headers, data: { prompt: "原模型捕获及恢复", idempotency_key: "browser-original" } });
    void execution.then(() => { done = true; });
    for (let index = 0; index < 800 && !done; index++) {
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
    const response = await execution; expect(response.ok(), await response.text()).toBeTruthy(); const completed = await response.json(); expect(completed.result.status).toBe("completed");
    const proof = await (await page.request.get(`/_test/model-restore-proof/${actor.agent_id}`, { headers })).json();
    expect(proof.calls).toBe(5); expect(proof.sources).toEqual(expect.arrayContaining([
      { phase: "preview", run_id: completed.result.run_id, call_id: "browser-model-preview" },
      { phase: "execute", run_id: completed.result.run_id, call_id: "browser-model-start" },
    ])); expect(proof.sources).toHaveLength(2); expect(proof.baseline_source).toEqual({ run_id: completed.result.run_id, call_id: "browser-model-preview", restore_plan_id: proof.plan.plan_id });
    expect(proof.files[`model-restore-${format}-plain`]).toEqual({ sha256: createHash("sha256").update("target\n").digest("hex"), mode: 416 });
    expect(proof.files[`model-restore-${format}-absent`]).toBeNull();
    await detail(page, actor.agent_id, proof.plan.plan_id); const root = page.locator("#native-restore-detail");
    await expect(root).toContainText(proof.plan.baseline_checkpoint_id); if (format === "bytes") await expect(root).toContainText(createHash("sha256").update(Buffer.from([0, 255])).digest("hex"));
    await connect(page, actor.topic_id); await detail(page, actor.agent_id, proof.plan.plan_id); await page.locator("#native-checkpoint-refresh").click();
    const current = await (await page.request.get(`/v1/agents/${actor.agent_id}/workspace/native-restores/${proof.plan.plan_id}`, { headers })).json(); expect(current).toEqual(proof.plan);
    expect(await (await page.request.get(`/_test/model-restore-proof/${actor.agent_id}`, { headers })).json()).toEqual(proof); expect(posts).toBe(0);
    await page.locator("#locale-select").selectOption("en-US"); await expect(root).toContainText("Backup capture format before restore"); await page.setViewportSize({ width: 420, height: 860 });
    await root.scrollIntoViewIfNeeded(); await page.screenshot({ path: test.info().outputPath(`model-restore-${format}-narrow.png`) }); expect(await page.locator("#checkpoint-native").evaluate(node => node.scrollWidth <= node.clientWidth + 2)).toBe(true);
  } finally { await page.unrouteAll({ behavior: "wait" }); await page.request.post(`/v1/agents/${actor.agent_id}/destroy`, { headers }); }
});
