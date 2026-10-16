import { expect, test, type Page } from "@playwright/test";
import { destroyAgent, ensureChineseUi } from "./session-fixtures.js";

const headers = { Authorization: "Bearer browser-test-key" };
const endpoint = (id: string) => `/v1/agents/${id}/workspace/directory-restores`;
async function actor(page: Page, name: string, options: Record<string, unknown> = {}) {
  const response = await page.request.post("/_test/directory-restore-agent", { headers, data: { name, ...options } });
  expect(response.ok(), await response.text()).toBeTruthy(); return response.json();
}
async function connect(page: Page, topic: string) {
  await page.goto(`/ui/?topic=${topic}`); await page.getByLabel("服务访问密钥", { exact: true }).fill("browser-test-key");
  await page.getByRole("button", { name: "连接", exact: true }).click(); await ensureChineseUi(page);
}
async function open(page: Page, id: string) {
  await page.locator(`.agent-card[data-agent-id="${id}"]`).getByRole("button", { name: "查看目录检查点", exact: true }).click();
  await expect(page.locator("#directory-restore-panel")).toBeVisible();
}
async function target(page: Page, value: Awaited<ReturnType<typeof actor>>) {
  const id = value.agent.agent_id;
  await connect(page, value.agent.topic_id); await open(page, id);
  await page.locator("#directory-checkpoint-query").fill(value.checkpoint.checkpoint_id); await page.locator("#directory-checkpoint-by-id").click();
  await expect(page.locator("#directory-checkpoint-detail")).toHaveAttribute("data-checkpoint-status", "available");
  await page.locator("#directory-checkpoint-detail").getByRole("button", { name: "恢复完整目录", exact: true }).click();
  await expect(page.locator("#directory-restore-preview")).toBeEnabled();
}
async function preview(page: Page) {
  await page.locator("#directory-restore-preview").click();
  await expect(page.locator("#directory-restore-detail")).toHaveAttribute("data-restore-phase", "ready");
  await expect(page.locator("#directory-restore-diff")).not.toBeEmpty();
  return page.locator("#directory-restore-detail").getAttribute("data-plan-id");
}
async function publish(page: Page, status = "finished") {
  await page.locator("#directory-restore-confirm").check();
  await expect(page.locator("#directory-restore-start")).toBeEnabled();
  page.once("dialog", dialog => void dialog.accept()); await page.locator("#directory-restore-start").click();
  await expect(page.locator("#directory-restore-detail")).toHaveAttribute("data-restore-phase", status);
}
async function proof(page: Page, id: string) { const response = await page.request.get(`/_test/directory-restore-proof/${id}`, { headers }); expect(response.ok(), await response.text()).toBeTruthy(); return response.json(); }
async function saved(page: Page, session: string) { return page.evaluate(id => JSON.parse(localStorage.getItem(`krim-directory-restore-recovery:${id}`) || "null"), session); }
async function originalPreview(page: Page, value: Awaited<ReturnType<typeof actor>>, key = "host-preview") {
  const base = endpoint(value.agent.agent_id);
  const response = await page.request.post(base + "/preview", { headers, data: { target: { kind: "checkpoint", identifier: value.checkpoint.checkpoint_id }, request_key: key } });
  expect(response.ok(), await response.text()).toBeTruthy(); const plan = await response.json();
  const ready = await page.request.post(`${base}/${plan.plan_id}/wait?timeout_seconds=10`, { headers }); expect(ready.ok(), await ready.text()).toBeTruthy();
  const result = await ready.json(); expect(result.phase).toBe("ready"); return result;
}

test("完整真实目录预览、审阅发布、完整备份回退与刷新只读", async ({ page }) => {
  const value = await actor(page, "restore-browser-real"), id = value.agent.agent_id;
  try {
    await target(page, value); const plan = await preview(page);
    await expect(page.locator("#directory-restore-diff")).toContainText("sub/raw.bin");
    await expect(page.locator("#directory-restore-start")).toBeDisabled();
    await publish(page);
    const first = await proof(page, id);
    expect(first.files["restore-browser-real/sub/raw.bin"].hex).toBe("00ff"); expect(first.files["restore-browser-real/sub/raw.bin"].mode).toBe(0o640);
    expect(first.files["restore-browser-real/extra.txt"]).toBeUndefined(); expect(first.counts.prepare).toBe(1); expect(first.counts.apply).toBe(1);
    const snapshot = await (await page.request.get(`${endpoint(id)}/${plan}`, { headers })).json();
    expect(snapshot.backup.file_count).toBe(4); expect(snapshot.target_snapshot.file_count).toBe(3);
    await page.locator("#directory-restore-backup").click(); await preview(page); await publish(page);
    const rolled = await proof(page, id); expect(rolled.files["restore-browser-real/sub/raw.bin"].hex).toBe("63757272656e748000"); expect(rolled.files["restore-browser-real/extra.txt"].hex).toBe(Buffer.from("complete backup extra\n").toString("hex"));
    expect((await (await page.request.get(`${endpoint(id)}/${plan}`, { headers })).json())).toEqual(snapshot);
    await connect(page, value.agent.topic_id); await open(page, id);
    await expect(page.locator("#directory-restore-detail")).toHaveAttribute("data-restore-phase", "finished");
    expect(await proof(page, id)).toEqual(rolled); expect((await saved(page, value.agent.session_id)).pending).toBeNull();
  } finally { await destroyAgent(page, id); }
});

test("真实发布unknown按原operation核实且不再次构树或发布", async ({ page }) => {
  const value = await actor(page, "restore-browser-unknown"), id = value.agent.agent_id;
  try {
    await target(page, value); const plan = await preview(page);
    expect((await page.request.post("/_test/directory-restore-control", { headers, data: { agent_id: id, action: "unknown" } })).ok()).toBeTruthy();
    await publish(page, "uncertain"); const before = await proof(page, id);
    const operation = before.plans[0].operation_id;
    await page.locator("#directory-restore-verify").click(); await expect(page.locator("#directory-restore-detail")).toHaveAttribute("data-restore-phase", "finished");
    const after = await proof(page, id); expect(after.plans[0].plan_id).toBe(plan); expect(after.plans[0].operation_id).toBe(operation);
    expect(after.counts.prepare).toBe(before.counts.prepare); expect(after.counts.apply).toBe(before.counts.apply); expect(after.counts.rename).toBe(before.counts.rename); expect(after.counts.verify).toBe(1);
    expect(after.root_inode).toBe(before.root_inode); expect(after.files).toEqual(before.files);
  } finally { await destroyAgent(page, id); }
});

test("start已接受但回程丢失，原execute键null跨刷新保留后只读找回", async ({ page }) => {
  const value = await actor(page, "restore-browser-lost"), id = value.agent.agent_id; let posts = 0;
  try {
    await target(page, value); const plan = await preview(page);
    await page.route(`**${endpoint(id)}/start?confirmed=true`, async route => { posts++; await route.fetch(); await route.abort("connectionfailed"); });
    const operationKey = `**${endpoint(id)}/operations/by-key?*`;
    await page.route(operationKey, route => route.fulfill({ json: null }));
    await page.locator("#directory-restore-confirm").check(); page.once("dialog", dialog => void dialog.accept()); await page.locator("#directory-restore-start").click();
    await expect(page.locator("#directory-restore-message")).toContainText("原目录恢复请求尚未确认");
    const pending = (await saved(page, value.agent.session_id)).pending; expect(pending.kind).toBe("execute");
    await connect(page, value.agent.topic_id); await open(page, id); await expect(page.locator("#directory-restore-preview")).toBeDisabled();
    expect((await saved(page, value.agent.session_id)).pending).toEqual(pending);
    const before = await proof(page, id); expect(before.plans).toHaveLength(1);
    await page.unroute(operationKey); await page.locator("#directory-restore-pending").click();
    await expect.poll(async () => (await saved(page, value.agent.session_id)).pending).toBeNull();
    await expect(page.locator("#directory-restore-detail")).toHaveAttribute("data-plan-id", plan!);
    expect(posts).toBe(1); expect(await proof(page, id)).toEqual(before);
  } finally { await destroyAgent(page, id); }
});

for (const fault of ["empty", "invalid-json", "scope", "resource", "extra", "write-denied", "silent-write"] as const) {
  test(`恢复存储${fault}阻止新写但可读取真实原计划`, async ({ page }) => {
    const value = await actor(page, `restore-storage-${fault}`), id = value.agent.agent_id; let posts = 0;
    try {
      const plan = await originalPreview(page, value);
      await connect(page, value.agent.topic_id);
      if (fault === "write-denied" || fault === "silent-write") {
        await page.addInitScript(kind => {
          const original = Storage.prototype.setItem;
          Storage.prototype.setItem = function (key, value) { if (key.startsWith("krim-directory-restore-recovery:")) { if (kind === "write-denied") throw new DOMException("synthetic recovery write denied", "QuotaExceededError"); return; } return original.call(this, key, value); };
        }, fault);
      } else {
        await page.evaluate(({ session, workspace, kind }) => {
          const key = `krim-directory-restore-recovery:${session}`;
          if (kind === "empty" || kind === "invalid-json") localStorage.setItem(key, kind === "empty" ? "" : "{");
          else { const stored: Record<string, unknown> = { version: 1, scope_id: `session:${session}`, workspace_key: workspace.key, resource_fingerprint: workspace.fingerprint, selected_id: null, pending: null }; if (kind === "scope") stored.scope_id = "session:other"; if (kind === "resource") stored.resource_fingerprint = "a".repeat(64); if (kind === "extra") stored.private_token = "synthetic"; localStorage.setItem(key, JSON.stringify(stored)); }
        }, { session: value.agent.session_id, workspace: value.agent.session.permissions.workspace, kind: fault });
      }
      await connect(page, value.agent.topic_id); await open(page, id);
      page.on("request", request => { if (request.method() === "POST" && [endpoint(id) + "/preview", endpoint(id) + "/start"].includes(new URL(request.url()).pathname)) posts++; });
      if (fault === "write-denied" || fault === "silent-write") {
        await page.locator("#directory-checkpoint-query").fill(value.checkpoint.checkpoint_id); await page.locator("#directory-checkpoint-by-id").click();
        await page.locator("#directory-checkpoint-detail").getByRole("button", { name: "恢复完整目录", exact: true }).click();
        await page.locator("#directory-restore-preview").click();
      }
      await expect(page.locator("#directory-restore-message")).toContainText("目录恢复记录不可用"); await expect(page.locator("#directory-restore-preview")).toBeDisabled();
      await page.locator("#directory-restore-query").fill(plan.plan_id); await page.locator("#directory-restore-by-id").click();
      await expect(page.locator("#directory-restore-detail")).toHaveAttribute("data-plan-id", plan.plan_id);
      expect(posts).toBe(0); expect((await proof(page, id)).plans).toHaveLength(1);
    } finally { await destroyAgent(page, id); }
  });
}

for (const fault of ["scope", "resource", "id", "backup", "private", "date"] as const) {
  test(`计划回程${fault}拒绝假事实且不进入详情`, async ({ page }) => {
    const value = await actor(page, `restore-response-${fault}`), id = value.agent.agent_id;
    try {
      const plan = await originalPreview(page, value);
      await connect(page, value.agent.topic_id); await open(page, id);
      const forged = structuredClone(plan);
      if (fault === "scope") forged.scope_id = "session:other";
      if (fault === "resource") forged.resource_fingerprint = "a".repeat(64);
      if (fault === "id") forged.plan_id = "00000000-0000-4000-8000-000000000001";
      if (fault === "backup") forged.backup.file_count = 1024;
      if (fault === "private") forged.token = "forged";
      if (fault === "date") forged.created_at = forged.updated_at = "2026-02-31T00:00:00Z";
      await page.route(`**${endpoint(id)}/${plan.plan_id}`, route => route.fulfill({ json: forged }));
      await page.locator("#directory-restore-query").fill(plan.plan_id); await page.locator("#directory-restore-by-id").click();
      await expect(page.locator("#directory-restore-message")).toContainText("目录恢复回程与原事实不一致");
      await expect(page.locator("#directory-restore-detail")).not.toHaveAttribute("data-plan-id");
      expect((await proof(page, id)).counts.prepare || 0).toBe(0);
    } finally { await destroyAgent(page, id); }
  });
}

test("错误差异越根路径拒绝且不能批准发布", async ({ page }) => {
  const value = await actor(page, "restore-bad-diff"), id = value.agent.agent_id;
  try {
    const plan = await originalPreview(page, value);
    await connect(page, value.agent.topic_id); await open(page, id);
    await page.route(`**${endpoint(id)}/${plan.plan_id}/diff?*`, async route => { const response = await route.fetch(); const diff = await response.json(); diff.entries[0].path = "../outside"; await route.fulfill({ json: diff }); });
    await page.locator("#directory-restore-query").fill(plan.plan_id); await page.locator("#directory-restore-by-id").click();
    await expect(page.locator("#directory-restore-message")).toContainText("目录恢复回程与原事实不一致");
    await expect(page.locator("#directory-restore-confirm")).toBeDisabled(); await expect(page.locator("#directory-restore-start")).toBeDisabled();
    expect((await proof(page, id)).counts.prepare || 0).toBe(0);
  } finally { await destroyAgent(page, id); }
});

test("原模型恢复由浏览器读同计划来源且刷新不再调用模型或Writer", async ({ page }) => {
  const value = await actor(page, "restore-original-model", { model: true }), id = value.agent.agent_id; let posts = 0;
  try {
    let done = false; const pending = page.request.post(`/v1/agents/${id}/execute`, { headers, data: { prompt: "原模型完整预览发布", idempotency_key: "browser-model-original" } });
    void pending.then(() => { done = true; });
    for (let index = 0; index < 150 && !done; index++) {
      const state = await (await page.request.get(`/v1/agents/${id}`, { headers })).json();
      if (state.current_run_id) {
        const interactions = await (await page.request.get(`/v1/runs/${state.current_run_id}/interactions?pending_only=true`, { headers })).json();
        for (const item of interactions) expect((await page.request.post(`/v1/runs/${state.current_run_id}/interactions/${item.id}/respond`, { headers, data: { action: "approve" } })).ok()).toBeTruthy();
      }
      await page.waitForTimeout(50);
    }
    const response = await pending; expect(response.ok(), await response.text()).toBeTruthy(); const result = (await response.json()).result; expect(result.status).toBe("completed");
    const before = await proof(page, id); expect(before.provider_calls).toBe(3); expect(before.plans).toHaveLength(1);
    expect(before.sources[0].preview).toEqual({ run_id: result.run_id, call_id: "browser-restore-preview" }); expect(before.sources[0].execute).toEqual({ run_id: result.run_id, call_id: "browser-restore-start" });
    page.on("request", request => { if (request.method() === "POST" && new URL(request.url()).pathname.startsWith(endpoint(id))) posts++; });
    await connect(page, value.agent.topic_id); await open(page, id); await page.locator("#directory-restore-query").fill("browser-model-preview"); await page.locator("#directory-restore-by-key").click();
    await expect(page.locator("#directory-restore-detail")).toHaveAttribute("data-plan-id", before.plans[0].plan_id);
    await connect(page, value.agent.topic_id); await open(page, id); await expect(page.locator("#directory-restore-detail")).toHaveAttribute("data-restore-phase", "finished");
    expect(await proof(page, id)).toEqual(before); expect(posts).toBe(0);
  } finally { await destroyAgent(page, id); }
});

for (const kind of ["plan", "ask", "no-leaf", "readonly", "disabled", "closed", "deny"] as const) {
  test(`原${kind}权限与冻结能力控制预览或发布`, async ({ page }) => {
    const value = await actor(page, `restore-permission-${kind}`, { mode: kind === "plan" || kind === "ask" ? kind : "agent", leaf: kind !== "no-leaf", writable: kind !== "readonly", deny_write: kind === "deny", isolated_workspace: kind === "disabled" }), id = value.agent.agent_id;
    try {
      if (kind === "disabled") expect((await page.request.post("/_test/directory-restore-control", { headers, data: { agent_id: id, action: "disable" } })).ok()).toBeTruthy();
      if (kind === "closed") expect((await page.request.post(`/v1/agents/${id}/session/close`, { headers, data: { expected_revision: value.agent.session.revision } })).ok()).toBeTruthy();
      await connect(page, value.agent.topic_id); await open(page, id);
      if (value.checkpoint) {
        await page.locator("#directory-checkpoint-query").fill(value.checkpoint.checkpoint_id); await page.locator("#directory-checkpoint-by-id").click();
        await page.locator("#directory-checkpoint-detail").getByRole("button", { name: "恢复完整目录", exact: true }).click();
      }
      if (kind === "plan" || kind === "deny") {
        await expect(page.locator("#directory-restore-preview")).toBeEnabled(); await preview(page); await page.locator("#directory-restore-confirm").check();
        if (kind === "plan") await expect(page.locator("#directory-restore-start")).toBeDisabled();
        else await publish(page, "failed");
      } else await expect(page.locator("#directory-restore-preview")).toBeDisabled();
      const facts = await proof(page, id); expect(facts.counts.prepare || 0).toBe(0); expect(facts.counts.apply || 0).toBe(0);
    } finally { await destroyAgent(page, id); }
  });
}

test("原真实preparing可等待取消，刷新同ID且没有Reader生产", async ({ page }) => {
  const value = await actor(page, "restore-cancel-preview"), id = value.agent.agent_id;
  try {
    await target(page, value);
    expect((await page.request.post("/_test/directory-restore-control", { headers, data: { agent_id: id, action: "block_preview" } })).ok()).toBeTruthy();
    await page.locator("#directory-restore-preview").click(); await expect(page.locator("#directory-restore-detail")).toHaveAttribute("data-restore-phase", "preparing");
    const plan = await page.locator("#directory-restore-detail").getAttribute("data-plan-id");
    await page.locator("#directory-restore-wait").click(); await expect(page.locator("#directory-restore-wait")).toBeEnabled();
    await page.locator("#directory-restore-cancel").click(); await expect(page.locator("#directory-restore-detail")).toHaveAttribute("data-restore-phase", "cancelled");
    const before = await proof(page, id); expect(before.counts.capture || 0).toBe(0); expect(before.plans).toHaveLength(1);
    await connect(page, value.agent.topic_id); await open(page, id); await expect(page.locator("#directory-restore-detail")).toHaveAttribute("data-plan-id", plan!);
    expect(await proof(page, id)).toEqual(before);
  } finally { await destroyAgent(page, id); }
});

test("真实历史分页、原键纯文本、英文与窄屏仍只读", async ({ page }) => {
  const value = await actor(page, "restore-pages-locale"), id = value.agent.agent_id;
  try {
    const key = '<img src=x onerror="window.restoreXss=1">';
    const first = await originalPreview(page, value, key);
    for (let index = 0; index < 8; index++) await originalPreview(page, value, `restore-page-${index}`);
    const before = await proof(page, id);
    await connect(page, value.agent.topic_id); await open(page, id);
    await expect(page.locator("#directory-restore-items article")).toHaveCount(8); await page.locator("#directory-restore-next").click(); await expect(page.locator("#directory-restore-items article")).toHaveCount(1);
    await page.locator("#directory-restore-prev").click(); await expect(page.locator("#directory-restore-items article")).toHaveCount(8);
    await page.locator("#directory-restore-query").fill(key); await page.locator("#directory-restore-by-key").click();
    await expect(page.locator("#directory-restore-detail")).toHaveAttribute("data-plan-id", first.plan_id);
    await expect(page.locator("#directory-restore-detail")).toContainText(key); await expect(page.locator("#directory-restore-detail img")).toHaveCount(0);
    expect(await page.evaluate(() => (window as unknown as Record<string, unknown>).restoreXss)).toBeUndefined();
    await page.locator("#locale-select").selectOption("en-US"); await expect(page.locator("#directory-restore-panel")).toContainText("Complete directory restore");
    await page.setViewportSize({ width: 420, height: 860 }); await page.locator("#directory-restore-panel").scrollIntoViewIfNeeded();
    await page.screenshot({ path: test.info().outputPath("directory-restore-narrow.png") });
    expect(await page.locator("#directory-restore-panel").evaluate(node => node.scrollWidth <= node.clientWidth + 2)).toBe(true);
    expect(await proof(page, id)).toEqual(before);
  } finally { await destroyAgent(page, id); }
});

test("跨Agent晚回程不覆盖新计划或改动原恢复记录", async ({ page }) => {
  const a = await actor(page, "restore-late-a"), b = await actor(page, "restore-late-b"); let release: (() => void) | undefined;
  try {
    const first = await originalPreview(page, a), second = await originalPreview(page, b);
    await connect(page, a.agent.topic_id); await open(page, a.agent.agent_id);
    let entered = false;
    await page.route(`**${endpoint(a.agent.agent_id)}/${first.plan_id}`, async route => { entered = true; await new Promise<void>(resolve => { release = resolve; }); await route.fulfill({ json: first }); });
    await page.locator("#directory-restore-query").fill(first.plan_id); await page.locator("#directory-restore-by-id").click(); await expect.poll(() => entered).toBe(true);
    await open(page, b.agent.agent_id); await page.locator("#directory-restore-query").fill(second.plan_id); await page.locator("#directory-restore-by-id").click();
    await expect(page.locator("#directory-restore-detail")).toHaveAttribute("data-plan-id", second.plan_id);
    const response = page.waitForResponse(value => new URL(value.url()).pathname === `${endpoint(a.agent.agent_id)}/${first.plan_id}`); release!(); await response;
    await expect(page.locator("#directory-restore-detail")).toHaveAttribute("data-plan-id", second.plan_id); expect((await saved(page, b.agent.session_id)).selected_id).toBe(second.plan_id);
    expect(await saved(page, a.agent.session_id)).toBeNull();
  } finally { release?.(); await destroyAgent(page, a.agent.agent_id); await destroyAgent(page, b.agent.agent_id); }
});

for (const fault of ["id", "backup", "digest"] as const) {
  test(`POST候选${fault}在原键未确认时不能显示或解除意图`, async ({ page }) => {
    const value = await actor(page, `restore-candidate-${fault}`), id = value.agent.agent_id;
    let posts = 0, actual: Record<string, any> | undefined;
    try {
      await target(page, value);
      await page.route(`**${endpoint(id)}/by-key?*`, route => route.fulfill({ json: null }));
      await page.route(`**${endpoint(id)}/preview?confirmed=true`, async route => {
        posts++;
        const response = await route.fetch(), accepted = await response.json();
        const waited = await page.request.post(`${endpoint(id)}/${accepted.plan_id}/wait?timeout_seconds=10`, { headers });
        expect(waited.ok(), await waited.text()).toBeTruthy(); actual = await waited.json();
        const forged = structuredClone(actual!);
        if (fault === "id") forged.plan_id = forged.operation_id = "00000000-0000-4000-8000-000000000001";
        if (fault === "backup") forged.backup.snapshot_fingerprint = "a".repeat(64);
        if (fault === "digest") forged.target_snapshot.snapshot_fingerprint = "b".repeat(64);
        await route.fulfill({ json: forged });
      });
      await page.locator("#directory-restore-preview").click();
      await expect(page.locator("#directory-restore-message")).toContainText("原目录恢复请求尚未确认");
      const pending = (await saved(page, value.agent.session_id)).pending;
      expect(pending.kind).toBe("preview");
      await expect(page.locator("#directory-restore-detail")).not.toHaveAttribute("data-plan-id");
      await expect(page.locator("#directory-restore-start")).toBeDisabled();
      const before = await proof(page, id); expect(before.plans).toHaveLength(1);
      await page.unroute(`**${endpoint(id)}/by-key?*`);
      await expect.poll(async () => (await saved(page, value.agent.session_id)).pending).toBeNull();
      await expect(page.locator("#directory-restore-detail")).toHaveAttribute("data-plan-id", actual!.plan_id);
      await expect(page.locator("#directory-restore-detail")).toContainText(actual!.backup.snapshot_fingerprint);
      expect(posts).toBe(1); expect(await proof(page, id)).toEqual(before);
    } finally { await destroyAgent(page, id); }
  });
}

test("preview已接受丢回程，刷新仅按原键找回且不重复捕获备份", async ({ page }) => {
  const value = await actor(page, "restore-preview-lost"), id = value.agent.agent_id; let posts = 0;
  try {
    await target(page, value);
    await page.route(`**${endpoint(id)}/preview?confirmed=true`, async route => { posts++; await route.fetch(); await route.abort("connectionfailed"); });
    await page.route(`**${endpoint(id)}/by-key?*`, route => route.fulfill({ json: null }));
    await page.locator("#directory-restore-preview").click();
    await expect(page.locator("#directory-restore-message")).toContainText("原目录恢复请求尚未确认");
    const pending = (await saved(page, value.agent.session_id)).pending; expect(pending.kind).toBe("preview");
    await connect(page, value.agent.topic_id); await open(page, id);
    expect((await saved(page, value.agent.session_id)).pending).toEqual(pending);
    const before = await proof(page, id); expect(before.plans).toHaveLength(1);
    await page.unroute(`**${endpoint(id)}/by-key?*`);
    await expect.poll(async () => (await saved(page, value.agent.session_id)).pending).toBeNull();
    await expect(page.locator("#directory-restore-detail")).toHaveAttribute("data-plan-id", before.plans[0].plan_id);
    expect(posts).toBe(1); expect(await proof(page, id)).toEqual(before);
  } finally { await destroyAgent(page, id); }
});

test("preview明确拒绝仍核实原键null才释放且没有生产操作", async ({ page }) => {
  const value = await actor(page, "restore-preview-rejected"), id = value.agent.agent_id;
  let posts = 0, reads = 0;
  try {
    await target(page, value); const before = await proof(page, id);
    await page.route(`**${endpoint(id)}/preview?confirmed=true`, route => { posts++; return route.fulfill({ status: 403, json: { error: { code: "synthetic_preview_denied", message: "synthetic denied" } } }); });
    await page.route(`**${endpoint(id)}/by-key?*`, async route => { reads++; await route.continue(); });
    await page.locator("#directory-restore-preview").click();
    await expect(page.locator("#directory-restore-message")).toContainText("原目录恢复请求未被接受");
    expect((await saved(page, value.agent.session_id)).pending).toBeNull();
    await expect(page.locator("#directory-restore-preview")).toBeEnabled();
    expect(posts).toBe(1); expect(reads).toBeGreaterThan(0); expect(await proof(page, id)).toEqual(before);
  } finally { await destroyAgent(page, id); }
});

for (const fault of ["key", "plan", "operation"] as const) {
  test(`execute原操作回程${fault}错误保留原发布意图并且零重放`, async ({ page }) => {
    const value = await actor(page, `restore-operation-${fault}`), id = value.agent.agent_id; let posts = 0, reads = 0;
    try {
      await target(page, value); const plan = await preview(page);
      page.on("request", request => { if (request.method() === "POST" && new URL(request.url()).pathname === endpoint(id) + "/start") posts++; });
      const pattern = `**${endpoint(id)}/operations/by-key?*`;
      await page.route(pattern, async route => {
        const response = await route.fetch(), forged = await response.json(); reads++;
        if (forged) {
          if (fault === "key") forged.request_key = "different-operation-key";
          if (fault === "plan") forged.plan_id = "00000000-0000-4000-8000-000000000001";
          if (fault === "operation") forged.operation_id = "00000000-0000-4000-8000-000000000001";
        }
        await route.fulfill({ json: forged });
      });
      await page.locator("#directory-restore-confirm").check(); page.once("dialog", dialog => void dialog.accept());
      await page.locator("#directory-restore-start").click();
      await expect.poll(() => reads).toBeGreaterThan(0);
      await expect(page.locator("#directory-restore-message")).toContainText("目录恢复回程与原事实不一致");
      const pending = (await saved(page, value.agent.session_id)).pending;
      expect(pending.kind).toBe("execute"); expect(pending.request.plan_id).toBe(plan);
      await expect(page.locator("#directory-restore-start")).toBeDisabled();
      // 原start是后台执行；先等原发布终态，再记录零重放基线。
      const waited = await page.request.post(`${endpoint(id)}/${plan}/wait?timeout_seconds=10`, { headers });
      expect(waited.ok(), await waited.text()).toBeTruthy(); expect((await waited.json()).phase).toBe("finished");
      expect((await saved(page, value.agent.session_id)).pending).toEqual(pending);
      const before = await proof(page, id);
      await page.unrouteAll({ behavior: "wait" });
      await expect.poll(async () => (await saved(page, value.agent.session_id)).pending).toBeNull();
      expect(posts).toBe(1); expect(await proof(page, id)).toEqual(before);
    } finally { await destroyAgent(page, id); }
  });
}

test("真实非BMP差异沿Python顺序、128码点原键与颠倒页拒绝", async ({ page }) => {
  const value = await actor(page, "restore-unicode", { unicode_paths: true }), id = value.agent.agent_id;
  try {
    const key = "😀".repeat(128), plan = await originalPreview(page, value, key);
    await connect(page, value.agent.topic_id); await open(page, id);
    await page.locator("#directory-restore-query").fill(key); await page.locator("#directory-restore-by-key").click();
    await expect(page.locator("#directory-restore-detail")).toHaveAttribute("data-plan-id", plan.plan_id);
    await expect(page.locator("#directory-restore-diff")).toContainText("𐀀.bin");
    const paths = await page.locator("#directory-restore-diff article p:first-child").allTextContents();
    expect(paths.findIndex(path => path.includes("\ue000.bin"))).toBeLessThan(paths.findIndex(path => path.includes("𐀀.bin")));
    await expect(page.locator("#directory-restore-confirm")).toBeEnabled();
    await page.route(`**${endpoint(id)}/${plan.plan_id}/diff?*`, async route => {
      const response = await route.fetch(), forged = await response.json();
      forged.entries.sort((left: { path: string }, right: { path: string }) => left.path < right.path ? -1 : left.path > right.path ? 1 : 0);
      await route.fulfill({ json: forged });
    });
    await page.locator("#directory-restore-diff-refresh").click();
    await expect(page.locator("#directory-restore-message")).toContainText("目录恢复回程与原事实不一致");
    await expect(page.locator("#directory-restore-confirm")).toBeDisabled();
    expect((await proof(page, id)).counts.prepare || 0).toBe(0);
  } finally { await destroyAgent(page, id); }
});

test("同revision微秒时间回退被拒绝并保留已见原计划", async ({ page }) => {
  const value = await actor(page, "restore-microsecond"), id = value.agent.agent_id;
  try {
    const plan = await originalPreview(page, value);
    await connect(page, value.agent.topic_id); await open(page, id);
    const second = new Date(Date.parse(plan.updated_at) + 1000).toISOString().replace(/\.\d{3}Z$/, "");
    let reads = 0;
    await page.route(`**${endpoint(id)}/${plan.plan_id}`, route => {
      reads++; return route.fulfill({ json: { ...plan, updated_at: second + (reads === 1 ? ".000002Z" : ".000001Z") } });
    });
    await page.locator("#directory-restore-query").fill(plan.plan_id); await page.locator("#directory-restore-by-id").click();
    await expect(page.locator("#directory-restore-detail")).toHaveAttribute("data-plan-id", plan.plan_id);
    await page.locator("#directory-restore-by-id").click();
    await expect(page.locator("#directory-restore-message")).toContainText("目录恢复回程与原事实不一致");
    expect(reads).toBe(2); expect((await saved(page, value.agent.session_id)).selected_id).toBe(plan.plan_id);
    expect((await proof(page, id)).counts.prepare || 0).toBe(0);
  } finally { await destroyAgent(page, id); }
});

for (const kind of ["preview", "execute"] as const) {
  test(`${kind}确认查询4xx不能冒充原POST拒绝并解除已接受意图`, async ({ page }) => {
    const value = await actor(page, `restore-read-error-${kind}`), id = value.agent.agent_id;
    let posts = 0, reads = 0;
    try {
      await target(page, value);
      if (kind === "execute") await preview(page);
      const writePath = endpoint(id) + (kind === "preview" ? "/preview" : "/start");
      page.on("request", request => { if (request.method() === "POST" && new URL(request.url()).pathname === writePath) posts++; });
      const pattern = `**${endpoint(id)}/${kind === "preview" ? "by-key" : "operations/by-key"}?*`;
      await page.route(pattern, route => {
        reads++;
        return reads === 1
          ? route.fulfill({ status: 404, json: { error: { code: "synthetic_read_failure", message: "synthetic lookup failed" } } })
          : route.fulfill({ json: null });
      });
      if (kind === "preview") await page.locator("#directory-restore-preview").click();
      else { await page.locator("#directory-restore-confirm").check(); page.once("dialog", dialog => void dialog.accept()); await page.locator("#directory-restore-start").click(); }
      await expect.poll(() => reads).toBeGreaterThanOrEqual(2);
      await expect(page.locator("#directory-restore-message")).toContainText("原目录恢复请求尚未确认");
      const pending = (await saved(page, value.agent.session_id)).pending;
      expect(pending).not.toBeNull(); expect(pending.kind).toBe(kind); expect(pending.rejected).toBe(false);
      await expect(page.locator("#directory-restore-preview")).toBeDisabled();
      await connect(page, value.agent.topic_id); await open(page, id);
      expect((await saved(page, value.agent.session_id)).pending).toEqual(pending);
      await page.unroute(pattern);
      await expect.poll(async () => (await saved(page, value.agent.session_id)).pending).toBeNull();
      expect(posts).toBe(1); expect((await proof(page, id)).plans).toHaveLength(1);
    } finally { await destroyAgent(page, id); }
  });
}

test("执行键历史查询拒绝原operation指向的计划回程换ID", async ({ page }) => {
  const value = await actor(page, "restore-lookup-plan"), id = value.agent.agent_id;
  try {
    await target(page, value); const plan = await preview(page); await publish(page);
    const original = await (await page.request.get(`${endpoint(id)}/${plan}`, { headers })).json();
    await page.locator("#directory-restore-query").fill(original.execute_key);
    await page.locator("#directory-restore-by-operation-key").click();
    await expect(page.locator("#directory-restore-detail")).toHaveAttribute("data-plan-id", plan!);
    await expect(page.locator("#directory-restore-by-operation-key")).toBeEnabled();
    let reads = 0;
    await page.route(`**${endpoint(id)}/${plan}`, route => {
      reads++; return route.fulfill({ json: { ...original, plan_id: "00000000-0000-4000-8000-000000000001" } });
    });
    await page.locator("#directory-restore-by-operation-key").click();
    await expect.poll(() => reads).toBeGreaterThan(0);
    await expect(page.locator("#directory-restore-message")).toContainText("目录恢复回程与原事实不一致");
    await expect(page.locator("#directory-restore-detail")).toHaveAttribute("data-plan-id", plan!);
    expect((await saved(page, value.agent.session_id)).selected_id).toBe(plan);
    expect((await proof(page, id)).plans).toHaveLength(1);
  } finally { await destroyAgent(page, id); }
});
