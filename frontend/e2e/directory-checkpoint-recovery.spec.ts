import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { expect, test } from "@playwright/test";
import { pythonExecutable } from "./python-client.js";
import { ensureChineseUi } from "./session-fixtures.js";

test("原模型目录SQL后强杀，新服务浏览器SDK和CLI同任务零重放", async ({ page, baseURL }) => {
  const headers = { Authorization: "Bearer browser-test-key" };
  const response = await page.request.get("/_test/directory-recovery", { headers }); expect(response.ok(), await response.text()).toBeTruthy();
  const before = await response.json(), checkpoint = before.checkpoint; let posts = 0;
  page.on("request", request => { if (request.method() === "POST" && request.url().includes("directory-checkpoints")) posts++; });
  async function connect() {
    await page.goto(`/ui/?topic=${before.topic_id}`); await page.getByLabel("服务访问密钥", { exact: true }).fill("browser-test-key");
    await page.getByRole("button", { name: "连接", exact: true }).click(); await ensureChineseUi(page);
    await page.locator(`.agent-card[data-agent-id="${before.agent_id}"]`).getByRole("button", { name: "查看目录检查点", exact: true }).click();
  }
  await connect(); await page.locator("#directory-checkpoint-query").fill(checkpoint.request_key); await page.locator("#directory-checkpoint-by-key").click();
  await expect(page.locator("#directory-checkpoint-detail")).toHaveAttribute("data-checkpoint-id", checkpoint.checkpoint_id);
  await expect(page.locator("#directory-checkpoint-detail")).toHaveAttribute("data-checkpoint-status", checkpoint.status);
  await expect(page.locator("#directory-checkpoint-detail")).toContainText(checkpoint.snapshot.snapshot_fingerprint);
  if (checkpoint.status === "interrupted") await expect(page.locator("#directory-checkpoint-detail")).toContainText("已保存材料不表示捕获成功");
  await connect(); await expect(page.locator("#directory-checkpoint-detail")).toHaveAttribute("data-checkpoint-id", checkpoint.checkpoint_id);
  const cli = await promisify(execFile)(pythonExecutable, ["-I", "-m", "krim_agent", "directory-checkpoints", "get", before.agent_id, checkpoint.checkpoint_id, "--url", baseURL!], {
    env: { ...process.env, KRIM_AGENT_API_KEY: "browser-test-key" }, timeout: 15000,
  });
  expect(JSON.parse(cli.stdout)).toEqual(checkpoint);
  const after = await (await page.request.get("/_test/directory-recovery", { headers })).json();
  expect(after).toEqual(before); expect(after).toMatchObject({ binary_hex: "00ff", mode: 0o640, counts: [1, 1, 1], replays: 0, old_owner_fenced: true, producer_exit: -9 }); expect(posts).toBe(0);
  await test.info().attach("directory-recovery-facts", { body: JSON.stringify(after, null, 2), contentType: "application/json" });
});
