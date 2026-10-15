import { expect, test, type Page } from "@playwright/test";
import { createHash } from "node:crypto";

const headers = { Authorization: "Bearer browser-test-key" }, paths = "bytes-nul.txt\nbytes-utf.txt\nbytes-empty.txt\nbytes-missing.txt";
const hash = createHash("sha256").update(Buffer.from([0, 255])).digest("hex");
test.setTimeout(60_000);
async function connect(page: Page, topic: string) {
  await page.goto(`/ui/?topic=${topic}`); await page.getByLabel("服务访问密钥", { exact: true }).fill("browser-test-key");
  await page.getByRole("button", { name: "连接", exact: true }).click(); await expect(page.locator("#connection")).toHaveClass(/ready/);
}
async function open(page: Page, id: string) { await page.locator(`.agent-card[data-agent-id="${id}"]`).getByRole("button", { name: "查看文件检查点", exact: true }).click(); await expect(page.locator("#native-checkpoint-format")).toBeEnabled(); }
async function actor(page: Page, name: string, selected = ["workspace_read", "workspace_checkpoint_capture"]) {
  const profile = await page.request.put(`/v1/agents/profiles/${name}`, { headers, data: { expected_revision: 0, config: { name, description: "实际bytes检查点", workspace_key: "edit", tool_names: selected } } }); expect(profile.ok(), await profile.text()).toBeTruthy();
  const response = await page.request.post("/v1/agents", { headers, data: { name } }); expect(response.ok()).toBeTruthy(); const view = await response.json();
  await connect(page, view.topic_id); await open(page, view.agent_id); return { id: view.agent_id as string, topic: view.topic_id as string };
}
async function seed(page: Page, path: string, action = "seed", content = "hello\n") { const response = await page.request.post("/_test/workspace", { headers, data: { path, action, content } }); expect(response.ok()).toBeTruthy(); }
async function fill(page: Page) {
  await page.locator("#native-checkpoint-title").fill("混合字节快照"); await page.locator("#native-checkpoint-paths").fill(paths); await page.locator("#native-checkpoint-format").selectOption("bytes");
}
async function original(page: Page, id: string) { const response = await page.request.get(`/v1/agents/${id}/workspace/native-checkpoints`, { headers }); expect(response.ok()).toBeTruthy(); return response.json(); }

test("真实混合bytes种类摘要、Locale与窄视口，刷新不再次捕获", async ({ page }) => {
  await seed(page, "bytes-nul.txt", "bytes"); await seed(page, "bytes-utf.txt"); await seed(page, "bytes-empty.txt", "seed", "");
  const { id } = await actor(page, "byte-ui-normal"); let creates = 0;
  try {
    await page.route("**/workspace/native-checkpoints?**", async route => { if (route.request().method() === "POST") creates++; await route.continue(); });
    await fill(page); await page.locator("#native-checkpoint-create").click(); await expect(page.locator("#native-checkpoint-detail")).toHaveAttribute("data-checkpoint-status", "available");
    await expect(page.locator("#native-checkpoint-detail")).toContainText("二进制字节"); await expect(page.locator("#native-checkpoint-detail")).toContainText(hash);
    const rows = await original(page, id); expect(rows).toHaveLength(1); expect(rows[0].capture_format).toBe("bytes"); expect(rows[0].files.map((file: { content_kind: string }) => file.content_kind)).toEqual(["binary", "text", "text", "absent"]);
    await page.locator("#native-checkpoint-refresh").click(); expect(creates).toBe(1);
    await page.locator("#locale-select").selectOption("en-US"); await expect(page.locator("#native-checkpoint-detail")).toContainText("Binary bytes"); await expect(page.locator("#native-checkpoint-format")).toHaveValue("bytes");
    await page.setViewportSize({ width: 420, height: 860 }); await page.locator("#native-checkpoint-detail").scrollIntoViewIfNeeded(); await page.screenshot({ path: test.info().outputPath("native-bytes-narrow.png") });
    expect(await page.locator("#checkpoint-native").evaluate(node => node.scrollWidth <= node.clientWidth + 2)).toBe(true);
  } finally { await page.request.post(`/v1/agents/${id}/destroy`, { headers }); }
});

test("捕获POST丢回复与网络中断后重载只GET原key，模式与原结果保持", async ({ page }) => {
  await seed(page, "bytes-nul.txt", "bytes"); await seed(page, "bytes-utf.txt"); await seed(page, "bytes-empty.txt", "seed", "");
  const { id, topic } = await actor(page, "byte-ui-lost"); let offline = false, creates = 0;
  try {
    await page.route("**/workspace/native-checkpoints**", async route => {
      if (route.request().method() === "POST" && new URL(route.request().url()).pathname.endsWith("/native-checkpoints")) { creates++; await route.fetch(); offline = true; await route.abort("failed"); }
      else if (offline) await route.abort("failed"); else await route.continue();
    });
    await fill(page); await page.locator("#native-checkpoint-create").click(); await expect(page.locator("#native-checkpoint-status")).toContainText("请求结果尚未确认");
    const raw = await page.evaluate(id => localStorage.getItem(`krim-native-capture-recovery:${id}`), id); expect(raw).not.toBeNull(); const key = JSON.parse(raw!).pending.request.request_key;
    await connect(page, topic); offline = false; await open(page, id);
    await expect(page.locator("#native-checkpoint-detail")).toHaveAttribute("data-checkpoint-status", "available"); await expect(page.locator("#native-checkpoint-detail")).toContainText(key);
    await expect(page.locator("#native-checkpoint-format")).toHaveValue("bytes"); expect(creates).toBe(1); expect(await original(page, id)).toHaveLength(1);
  } finally { await page.request.post(`/v1/agents/${id}/destroy`, { headers }); }
});

test("未冻结新工具与存储故障均停止byte POST", async ({ page }) => {
  const denied = await actor(page, "byte-ui-missing", ["workspace_read"]);
  try {
    await fill(page); await expect(page.locator("#native-checkpoint-create")).toBeDisabled(); expect(await original(page, denied.id)).toHaveLength(0);
  } finally { await page.request.post(`/v1/agents/${denied.id}/destroy`, { headers }); }
  const { id } = await actor(page, "byte-ui-storage"); let creates = 0;
  try {
    await page.route("**/workspace/native-checkpoints?**", async route => { if (route.request().method() === "POST") creates++; await route.continue(); });
    await fill(page);
    await page.evaluate(() => { const before = Storage.prototype.setItem; Storage.prototype.setItem = function(key, value) { if (key.startsWith("krim-native-capture-recovery:")) throw new DOMException("synthetic quota", "QuotaExceededError"); before.call(this, key, value); }; });
    await page.locator("#native-checkpoint-create").click(); await expect(page.locator("#native-checkpoint-status")).toContainText("检查点恢复记录不可用"); expect(creates).toBe(0);
    await expect(page.locator("#native-checkpoint-create")).toBeDisabled(); expect(await original(page, id)).toHaveLength(0);
  } finally { await page.request.post(`/v1/agents/${id}/destroy`, { headers }); }
});

test("合法结构的错误Scope或捕获格式回复保留原意图，不接受假结果", async ({ page }) => {
  await seed(page, "bytes-nul.txt", "bytes"); await seed(page, "bytes-utf.txt"); await seed(page, "bytes-empty.txt", "seed", "");
  const { id } = await actor(page, "byte-ui-response"); let creates = 0, responseFinished = false;
  try {
    await page.route("**/workspace/native-checkpoints**", async route => {
      if (route.request().method() === "POST" && new URL(route.request().url()).pathname.endsWith("/native-checkpoints")) { creates++; const response = await route.fetch(), value = await response.json(); value.scope_id = "session:another"; await route.fulfill({ response, json: value }); responseFinished = true; }
      else await route.abort("failed");
    });
    await fill(page); await page.locator("#native-checkpoint-create").click(); await expect(page.locator("#native-checkpoint-status")).toContainText("请求结果尚未确认");
    await expect.poll(() => responseFinished).toBe(true);
    await expect(page.locator("#native-checkpoint-detail")).not.toHaveAttribute("data-checkpoint-status", "available"); expect(creates).toBe(1);
    expect(await page.evaluate(id => localStorage.getItem(`krim-native-capture-recovery:${id}`), id)).not.toBeNull();
  } finally { await page.request.post(`/v1/agents/${id}/destroy`, { headers }); }
});
