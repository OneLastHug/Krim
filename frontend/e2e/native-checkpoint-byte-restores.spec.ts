import { destroyAgent } from "./session-fixtures.js";
import { expect, test, type Page } from "@playwright/test";
import { createHash } from "node:crypto";

const headers = { Authorization: "Bearer browser-test-key" }, detail = (page: Page) => page.locator("#native-restore-detail");
const bytesHash = createHash("sha256").update(Buffer.from([0, 255])).digest("hex");
test.setTimeout(120_000);
async function connect(page: Page, topic: string) {
  await page.goto(`/ui/?topic=${topic}`); await page.getByLabel("服务访问密钥", { exact: true }).fill("browser-test-key");
  await page.getByRole("button", { name: "连接", exact: true }).click(); await expect(page.locator("#connection")).toHaveClass(/ready/);
}
async function open(page: Page, id: string) { await page.locator(`.agent-card[data-agent-id="${id}"]`).getByRole("button", { name: "查看文件检查点", exact: true }).click(); await expect(page.locator("#native-restore-format")).toBeEnabled(); }
async function actor(page: Page, name: string) {
  const profile = await page.request.put(`/v1/agents/profiles/${name}`, { headers, data: { expected_revision: 0, config: { name, description: "真实字节恢复", workspace_key: "edit", tool_names: ["workspace_read", "workspace_checkpoint_capture", "workspace_change", "workspace_reconcile"] } } }); expect(profile.ok()).toBeTruthy();
  const response = await page.request.post("/v1/agents", { headers, data: { name } }); expect(response.ok()).toBeTruthy(); const value = await response.json();
  await connect(page, value.topic_id); await open(page, value.agent_id); return { id: value.agent_id as string, topic: value.topic_id as string };
}
async function seed(page: Page, path: string, content = "current\n", action = "seed") {
  const response = await page.request.post("/_test/workspace", { headers, data: { path, content, action } }); expect(response.ok()).toBeTruthy();
}
async function inspect(page: Page, path: string) { const response = await page.request.post("/_test/workspace", { headers, data: { path, action: "inspect" } }); expect(response.ok()).toBeTruthy(); return response.json(); }
async function capture(page: Page, path: string, format = "bytes") {
  await page.locator("#native-checkpoint-paths").fill(path); await page.locator("#native-checkpoint-format").selectOption(format);
  await page.locator("#native-checkpoint-create").click(); await expect(page.locator("#native-checkpoint-detail")).toHaveAttribute("data-checkpoint-status", "available", { timeout: 30_000 });
}
async function preview(page: Page) { await page.locator("#native-checkpoint-preview").click(); await expect(detail(page)).toHaveAttribute("data-restore-phase", "ready", { timeout: 30_000 }); }
async function execute(page: Page) { page.once("dialog", dialog => dialog.accept()); await page.locator("#native-restore-execute").click(); await expect(detail(page)).toHaveAttribute("data-restore-status", "done", { timeout: 30_000 }); }

test("原bytes目标自动保全混合当前字节，真实备份整组回退与Locale窄视口", async ({ page }) => {
  const first = "restore-byte-a.txt", second = "restore-byte-b.txt";
  await seed(page, first, "", "bytes"); await seed(page, second, "target\n");
  const { id } = await actor(page, "byte-restore-mixed");
  try {
    await capture(page, `${first}\n${second}`); await seed(page, first); await seed(page, second, "", "bytes");
    await preview(page); await expect(detail(page)).toContainText("完整文件字节（含二进制）"); await expect(detail(page)).toContainText(bytesHash);
    await execute(page); expect((await inspect(page, first)).sha256).toBe(bytesHash); expect((await inspect(page, second)).sha256).toBe(createHash("sha256").update("target\n").digest("hex"));
    await detail(page).getByRole("button", { name: "查看恢复前备份", exact: true }).click(); await expect(page.locator("#native-checkpoint-detail")).toContainText("恢复前备份");
    await preview(page); await execute(page); expect((await inspect(page, first)).sha256).toBe(createHash("sha256").update("current\n").digest("hex")); expect((await inspect(page, second)).sha256).toBe(bytesHash);
    await page.locator("#locale-select").selectOption("en-US"); await expect(detail(page)).toContainText("Backup capture format before restore");
    await page.setViewportSize({ width: 420, height: 860 }); await detail(page).scrollIntoViewIfNeeded(); await page.screenshot({ path: test.info().outputPath("byte-restore-narrow.png") });
    expect(await page.locator("#checkpoint-native").evaluate(node => node.scrollWidth <= node.clientWidth + 2)).toBe(true);
  } finally { await destroyAgent(page, id); }
});

test("旧text目标到当前binary须明确bytes基线，完整备份可恢复原字节", async ({ page }) => {
  const path = "restore-byte-text.txt"; await seed(page, path, "target\n"); const { id } = await actor(page, "byte-restore-text");
  try {
    await capture(page, path, "text"); await seed(page, path, "", "bytes");
    await page.locator("#native-checkpoint-preview").click(); await expect(detail(page)).toHaveAttribute("data-restore-phase", "failed"); expect((await inspect(page, path)).sha256).toBe(bytesHash);
    await page.locator("#native-restore-format").selectOption("bytes"); await preview(page); await execute(page);
    expect((await inspect(page, path)).sha256).toBe(createHash("sha256").update("target\n").digest("hex"));
    await detail(page).getByRole("button", { name: "查看恢复前备份", exact: true }).click(); await expect(page.locator("#native-checkpoint-detail")).toContainText("恢复前备份");
    await preview(page); await execute(page); expect((await inspect(page, path)).sha256).toBe(bytesHash);
  } finally { await destroyAgent(page, id); }
});

test("预览及执行丢POST回复后刷新仅GET原预览键，不再次捕获或执行", async ({ page }) => {
  const path = "restore-byte-lost.txt"; await seed(page, path, "", "bytes"); const { id, topic } = await actor(page, "byte-restore-lost");
  let blocked = false, previews = 0, executions = 0, completed = 0;
  try {
    await capture(page, path); await seed(page, path);
    await page.route("**/workspace/native-restores**", async route => {
      const pathname = new URL(route.request().url()).pathname;
      if (route.request().method() === "POST" && (pathname.endsWith("/preview") || pathname.endsWith("/execute"))) {
        if (pathname.endsWith("/preview")) previews++; else executions++;
        await route.fetch(); blocked = true; await route.abort("failed"); completed++;
      } else if (blocked) await route.abort("failed"); else await route.continue();
    });
    await page.locator("#native-checkpoint-preview").click(); await expect(page.locator("#native-checkpoint-status")).toContainText("请求结果尚未确认"); await expect.poll(() => completed).toBe(1);
    const raw = await page.evaluate(id => localStorage.getItem(`krim-native-capture-recovery:${id}`), id), saved = JSON.parse(raw!); expect(saved.version).toBe(2); expect(saved.pending.kind).toBe("preview");
    await connect(page, topic); blocked = false; await open(page, id); await expect(detail(page)).toHaveAttribute("data-restore-phase", "ready", { timeout: 30_000 });
    const planId = await detail(page).getAttribute("data-plan-id"); page.once("dialog", dialog => dialog.accept()); await page.locator("#native-restore-execute").click();
    await expect(page.locator("#native-checkpoint-status")).toContainText("请求结果尚未确认"); await expect.poll(() => completed).toBe(2);
    const executeRaw = await page.evaluate(id => localStorage.getItem(`krim-native-capture-recovery:${id}`), id), intent = JSON.parse(executeRaw!); expect(intent.pending.kind).toBe("execute"); expect(intent.pending.expected.preview_key).toBe(saved.pending.request.request_key);
    await connect(page, topic); blocked = false; await open(page, id); await expect(detail(page)).toHaveAttribute("data-restore-status", "done", { timeout: 30_000 });
    expect(await detail(page).getAttribute("data-plan-id")).toBe(planId); expect(previews).toBe(1); expect(executions).toBe(1); expect((await inspect(page, path)).sha256).toBe(bytesHash);
    expect(await page.evaluate(id => localStorage.getItem(`krim-native-capture-recovery:${id}`), id)).toBeNull();
  } finally { await page.unrouteAll({ behavior: "wait" }); await destroyAgent(page, id); }
});

test("错误基线回程保留原意图，恢复存储失败在POST前停用写", async ({ page }) => {
  const path = "restore-byte-response.txt"; await seed(page, path, "", "bytes"); const first = await actor(page, "byte-restore-response"); let posts = 0, replied = false;
  try {
    await capture(page, path); await seed(page, path);
    await page.route("**/workspace/native-restores**", async route => {
      if (route.request().method() === "POST" && new URL(route.request().url()).pathname.endsWith("/preview")) { posts++; const response = await route.fetch(), value = await response.json(); value.baseline_capture_format = "text"; await route.fulfill({ response, json: value }); replied = true; }
      else await route.abort("failed");
    });
    await page.locator("#native-checkpoint-preview").click(); await expect(page.locator("#native-checkpoint-status")).toContainText("请求结果尚未确认"); await expect.poll(() => replied).toBe(true);
    expect(posts).toBe(1); expect(await page.evaluate(id => localStorage.getItem(`krim-native-capture-recovery:${id}`), first.id)).not.toBeNull();
    await expect(page.locator("#native-restore-execute")).toBeDisabled();
  } finally { await page.unrouteAll({ behavior: "wait" }); await destroyAgent(page, first.id); }
  const second = await actor(page, "byte-restore-storage"); posts = 0;
  try {
    await capture(page, path); await page.route("**/workspace/native-restores/preview?**", async route => { posts++; await route.continue(); });
    await page.evaluate(() => { Storage.prototype.setItem = function() { throw new DOMException("synthetic storage failure", "QuotaExceededError"); }; });
    await page.locator("#native-checkpoint-preview").click(); await expect(page.locator("#native-checkpoint-status")).toContainText("检查点恢复记录不可用"); expect(posts).toBe(0); await expect(page.locator("#native-checkpoint-preview")).toBeDisabled();
  } finally { await destroyAgent(page, second.id); }
});
