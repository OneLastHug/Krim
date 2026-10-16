import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { expect, test } from "@playwright/test";
import { pythonExecutable } from "./python-client.js";
import { ensureChineseUi } from "./session-fixtures.js";

test("原模型目录恢复强杀后新服务浏览器SDK与CLI同原计划和操作零重放", async ({ page, baseURL }) => {
  const headers = { Authorization: "Bearer browser-test-key" };
  const response = await page.request.get("/_test/directory-restore-recovery", { headers }); expect(response.ok(), await response.text()).toBeTruthy();
  const before = await response.json(); let posts = 0;
  page.on("request", request => { if (request.method() === "POST" && new URL(request.url()).pathname.includes("directory-restores")) posts++; });
  async function connect() {
    await page.goto(`/ui/?topic=${before.topic_id}`); await page.getByLabel("服务访问密钥", { exact: true }).fill("browser-test-key"); await page.getByRole("button", { name: "连接", exact: true }).click(); await ensureChineseUi(page);
    await page.locator(`.agent-card[data-agent-id="${before.agent_id}"]`).getByRole("button", { name: "查看目录检查点", exact: true }).click();
  }
  await connect(); await page.locator("#directory-restore-query").fill("model-preview"); await page.locator("#directory-restore-by-key").click();
  await expect(page.locator("#directory-restore-detail")).toHaveAttribute("data-plan-id", before.plan.plan_id);
  await expect(page.locator("#directory-restore-detail")).toHaveAttribute("data-restore-phase", before.plan.phase);
  await expect(page.locator("#directory-restore-detail")).toContainText(before.plan.backup.snapshot_fingerprint);
  if (before.point === "effect") { await page.locator("#directory-restore-verify").click(); await expect(page.locator("#directory-restore-detail")).toHaveAttribute("data-restore-phase", "finished"); }
  else await expect(page.locator("#directory-restore-start")).toBeDisabled();
  const current = await (await page.request.get("/_test/directory-restore-recovery", { headers })).json();
  await connect(); await expect(page.locator("#directory-restore-detail")).toHaveAttribute("data-plan-id", current.plan.plan_id);
  const cli = await promisify(execFile)(pythonExecutable, ["-I", "-m", "krim_agent", "directory-restores", "get", before.agent_id, current.plan.plan_id, "--url", baseURL!], { env: { ...process.env, KRIM_AGENT_API_KEY: "browser-test-key" }, timeout: 15000 });
  expect(JSON.parse(cli.stdout)).toEqual(current.plan);
  const after = await (await page.request.get("/_test/directory-restore-recovery", { headers })).json();
  expect(after).toEqual(current); expect(after.plan.operation_id).toBe(before.plan.operation_id); expect(after.source).toEqual(before.source); expect(after.root_inode).toBe(before.root_inode);
  expect(after).toMatchObject({ replays: 0, old_owner_fenced: true, producer_exit: -9, restore_lease_natural: true, synthetic_handle_expired: true });
  expect(after.binary_hex).toBe(before.point === "backup" ? "63757272656e748000" : "00ff"); expect(posts).toBe(before.point === "backup" ? 0 : 1);
  await test.info().attach("directory-restore-recovery-facts", { body: JSON.stringify(after, null, 2), contentType: "application/json" });
});
