import { expect, test, type Page } from "@playwright/test";
import { destroyAgent, ensureChineseUi } from "./session-fixtures.js";
import type { DirectoryCheckpointView } from "../src/api.js";

const headers = { Authorization: "Bearer browser-test-key" };
const endpoint = (id: string) => `/v1/agents/${id}/workspace/directory-checkpoints`;
async function actor(page: Page, name: string, options: Record<string, unknown> = {}) {
  const response = await page.request.post("/_test/directory-checkpoint-agent", { headers, data: { name, root_path: name, ...options } });
  expect(response.ok(), await response.text()).toBeTruthy(); return response.json();
}
async function connect(page: Page, topic: string) {
  await page.goto(`/ui/?topic=${topic}`); await page.getByLabel("服务访问密钥", { exact: true }).fill("browser-test-key");
  await page.getByRole("button", { name: "连接", exact: true }).click(); await ensureChineseUi(page);
}
async function open(page: Page, id: string) {
  await page.locator(`.agent-card[data-agent-id="${id}"]`).getByRole("button", { name: "查看目录检查点", exact: true }).click();
  await expect(page.locator("#checkpoint-directory")).toBeVisible();
}
async function proof(page: Page, id: string) {
  const response = await page.request.get(`/_test/directory-checkpoint-proof/${id}`, { headers });
  expect(response.ok(), await response.text()).toBeTruthy(); return response.json();
}
async function capture(page: Page, id: string, root: string, title = "完整目录快照") {
  await open(page, id); await page.locator("#directory-checkpoint-title").fill(title); await page.locator("#directory-checkpoint-path").fill(root);
  await expect(page.locator("#directory-checkpoint-create")).toBeEnabled(); await page.locator("#directory-checkpoint-create").click();
  await expect(page.locator("#directory-checkpoint-detail")).toHaveAttribute("data-checkpoint-status", "available");
  return page.locator("#directory-checkpoint-detail").getAttribute("data-checkpoint-id");
}
async function saved(page: Page, session: string) {
  return page.evaluate(id => JSON.parse(localStorage.getItem(`krim-directory-checkpoint-recovery:${id}`) || "null"), session);
}
async function seedPending(page: Page, value: { agent_id: string; session_id: string }, requestKey: string, root: string) {
  const agent = await (await page.request.get(`/v1/agents/${value.agent_id}`, { headers })).json();
  const workspace = agent.session.permissions.workspace;
  await page.evaluate(data => localStorage.setItem(`krim-directory-checkpoint-recovery:${data.session}`, JSON.stringify({
    version: 1, scope_id: `session:${data.session}`, workspace_key: data.workspace.key, resource_fingerprint: data.workspace.fingerprint,
    pending: { request: { title: "原未确认请求", root_path: data.root, request_key: data.key }, rejected: false }, selected_id: null,
  })), { session: value.session_id, workspace, root, key: requestKey });
}
async function original(page: Page, id: string, root: string, requestKey: string): Promise<DirectoryCheckpointView> {
  const response = await page.request.post(`${endpoint(id)}?confirmed=true`, { headers, data: { title: "原未确认请求", root_path: root, request_key: requestKey } });
  expect(response.ok(), await response.text()).toBeTruthy(); const value = await response.json();
  const waited = await page.request.post(`${endpoint(id)}/${value.checkpoint_id}/wait?timeout_seconds=1`, { headers });
  expect(waited.ok()).toBeTruthy(); return waited.json();
}

test("已接受捕获的确认GET失败不能冒充原POST拒绝，刷新保持原意图", async ({ page }) => {
  const name = "directory-confirm-read-error", value = await actor(page, name);
  let posts = 0, queries = 0, checkpointId: string | undefined;
  try {
    await connect(page, value.topic_id); await open(page, value.agent_id);
    await page.route(`**${endpoint(value.agent_id)}?confirmed=true`, async route => {
      posts++; const response = await route.fetch(); checkpointId = (await response.json()).checkpoint_id;
      await route.fulfill({ response });
    });
    await page.route(`**${endpoint(value.agent_id)}/by-key?*`, route => {
      queries++;
      return queries === 1
        ? route.fulfill({ status: 404, json: { error: { code: "synthetic_lookup_failure", message: "synthetic lookup failure" } } })
        : route.fulfill({ json: null });
    });
    await page.locator("#directory-checkpoint-path").fill(name); await page.locator("#directory-checkpoint-create").click();
    await expect.poll(() => queries).toBeGreaterThanOrEqual(2);
    await expect(page.locator("#directory-checkpoint-status")).toContainText("原目录捕获请求尚未确认");
    const pending = (await saved(page, value.session_id)).pending;
    expect(pending).not.toBeNull(); expect(pending.rejected).toBe(false); expect(pending.request.root_path).toBe(name);
    await connect(page, value.topic_id); await open(page, value.agent_id);
    expect((await saved(page, value.session_id)).pending).toEqual(pending);
    const waited = await page.request.post(`${endpoint(value.agent_id)}/${checkpointId}/wait?timeout_seconds=10`, { headers });
    expect(waited.ok(), await waited.text()).toBeTruthy(); expect((await waited.json()).status).toBe("available");
    const before = await proof(page, value.agent_id); expect(before.reads).toBe(1);
    await page.unrouteAll({ behavior: "wait" });
    await expect.poll(async () => (await saved(page, value.session_id)).pending).toBeNull();
    await expect(page.locator("#directory-checkpoint-detail")).toHaveAttribute("data-checkpoint-id", checkpointId!);
    expect(posts).toBe(1); expect(await proof(page, value.agent_id)).toEqual(before);
  } finally { await destroyAgent(page, value.agent_id); }
});

test("未确认查询拒绝同Scope的其他原请求键且保留原意图", async ({ page }) => {
  const value = await actor(page, "directory-wrong-key");
  try {
    const other = await original(page, value.agent_id, "directory-wrong-key", "other-original-key");
    await connect(page, value.topic_id); await seedPending(page, value, "missing-original-key", "directory-wrong-key");
    await page.route(`**${endpoint(value.agent_id)}/by-key?request_key=missing-original-key`, route => route.fulfill({ json: other }));
    await connect(page, value.topic_id); await open(page, value.agent_id);
    await expect(page.locator("#directory-checkpoint-status")).toContainText("目录检查点回程与原事实不一致");
    await expect(page.locator("#directory-checkpoint-detail")).not.toHaveAttribute("data-checkpoint-id");
    expect((await saved(page, value.session_id)).pending.request.request_key).toBe("missing-original-key");
    expect((await proof(page, value.agent_id)).entries).toHaveLength(1);
  } finally { await destroyAgent(page, value.agent_id); }
});

test("创建合法错ID不能抢先显示或解除原意图", async ({ page }) => {
  const value = await actor(page, "directory-wrong-id"); let posts = 0;
  try {
    await connect(page, value.topic_id); await open(page, value.agent_id);
    await page.route(`**${endpoint(value.agent_id)}?confirmed=true`, async route => {
      posts++; const response = await route.fetch(); const result = await response.json();
      await route.fulfill({ json: { ...result, checkpoint_id: "00000000-0000-4000-8000-000000000001" } });
    });
    await page.route(`**${endpoint(value.agent_id)}/by-key?*`, route => route.fulfill({ json: null }));
    await page.locator("#directory-checkpoint-path").fill("directory-wrong-id"); await page.locator("#directory-checkpoint-create").click();
    await expect(page.locator("#directory-checkpoint-status")).toContainText("原目录捕获请求尚未确认");
    await expect(page.locator("#directory-checkpoint-detail")).not.toHaveAttribute("data-checkpoint-id");
    expect((await saved(page, value.session_id)).pending).not.toBeNull();
    expect(posts).toBe(1); expect((await proof(page, value.agent_id)).entries).toHaveLength(1);
  } finally { await destroyAgent(page, value.agent_id); }
});

test("创建合法错摘要从未显示且只读原键找回真实结果", async ({ page }) => {
  const value = await actor(page, "directory-wrong-digest"); let posts = 0;
  try {
    await connect(page, value.topic_id); await open(page, value.agent_id);
    await page.evaluate(() => {
      const seen: string[] = []; (window as unknown as Record<string, unknown>).directorySeen = seen;
      new MutationObserver(() => seen.push(document.getElementById("directory-checkpoint-detail")!.textContent!)).observe(document.getElementById("directory-checkpoint-detail")!, { childList: true, subtree: true });
    });
    await page.route(`**${endpoint(value.agent_id)}?confirmed=true`, async route => {
      posts++; const response = await route.fetch(); const result = await response.json();
      const truth = await (await page.request.post(`${endpoint(value.agent_id)}/${result.checkpoint_id}/wait?timeout_seconds=1`, { headers })).json();
      expect(truth.status).toBe("available");
      await route.fulfill({ json: { ...truth, snapshot: { ...truth.snapshot, snapshot_fingerprint: "a".repeat(64) } } });
    });
    await page.locator("#directory-checkpoint-path").fill("directory-wrong-digest"); await page.locator("#directory-checkpoint-create").click();
    await expect(page.locator("#directory-checkpoint-detail")).toHaveAttribute("data-checkpoint-status", "available");
    expect(await page.evaluate(() => (window as unknown as Record<string, string[]>).directorySeen.some(text => text.includes("a".repeat(64))))).toBe(false);
    const result = await proof(page, value.agent_id); expect(result.reads).toBe(1); expect(result.entries).toHaveLength(1); expect(posts).toBe(1);
  } finally { await destroyAgent(page, value.agent_id); }
});

test("真实POST丢回程与原键null在刷新后保留意图，只读找回零重放", async ({ page }) => {
  const value = await actor(page, "directory-lost-response"); let posts = 0;
  try {
    await connect(page, value.topic_id); await open(page, value.agent_id);
    await page.route(`**${endpoint(value.agent_id)}?confirmed=true`, async route => { posts++; await route.fetch(); await route.abort("connectionfailed"); });
    const byKey = `**${endpoint(value.agent_id)}/by-key?*`;
    await page.route(byKey, route => route.fulfill({ json: null }));
    await page.locator("#directory-checkpoint-path").fill("directory-lost-response"); await page.locator("#directory-checkpoint-create").click();
    await expect(page.locator("#directory-checkpoint-status")).toContainText("原目录捕获请求尚未确认");
    const pending = (await saved(page, value.session_id)).pending;
    await connect(page, value.topic_id); await open(page, value.agent_id);
    await expect(page.locator("#directory-checkpoint-create")).toBeDisabled();
    expect((await saved(page, value.session_id)).pending).toEqual(pending);
    const before = await proof(page, value.agent_id); expect(before.entries).toHaveLength(1); expect(before.reads).toBe(1);
    await page.unroute(byKey); await page.locator("#directory-checkpoint-pending").click();
    await expect(page.locator("#directory-checkpoint-detail")).toHaveAttribute("data-checkpoint-id", before.entries[0].checkpoint_id);
    expect((await saved(page, value.session_id)).pending).toBeNull(); expect(posts).toBe(1); expect(await proof(page, value.agent_id)).toEqual(before);
  } finally { await destroyAgent(page, value.agent_id); }
});

test("明确拒绝也先核实原键null才释放意图，不自动再POST", async ({ page }) => {
  const value = await actor(page, "directory-rejected"); let posts = 0, queries = 0;
  try {
    await connect(page, value.topic_id); await open(page, value.agent_id);
    await page.route(`**${endpoint(value.agent_id)}?confirmed=true`, async route => { posts++; await route.fulfill({ status: 403, json: { error: { code: "test_denied", message: "合成明确拒绝" } } }); });
    await page.route(`**${endpoint(value.agent_id)}/by-key?*`, async route => { queries++; await route.continue(); });
    await page.locator("#directory-checkpoint-path").fill("directory-rejected"); await page.locator("#directory-checkpoint-create").click();
    await expect(page.locator("#directory-checkpoint-status")).toContainText("原目录捕获请求未被接受");
    await expect(page.locator("#directory-checkpoint-create")).toBeEnabled();
    expect((await saved(page, value.session_id)).pending).toBeNull(); expect(posts).toBe(1); expect(queries).toBeGreaterThan(0); expect((await proof(page, value.agent_id)).reads).toBe(0);
  } finally { await destroyAgent(page, value.agent_id); }
});

for (const fault of ["invalid-json", "empty", "wrong-scope", "wrong-resource", "unknown-field", "write-denied", "silent-write"] as const) {
  test(`恢复存储${fault}阻止新建但允许真实历史查询`, async ({ page }) => {
    const value = await actor(page, `directory-storage-${fault}`); let posts = 0;
    try {
      const truth = await original(page, value.agent_id, `directory-storage-${fault}`, `storage-${fault}`);
      await connect(page, value.topic_id);
      if (fault === "write-denied" || fault === "silent-write") {
        await page.addInitScript(kind => {
          const set = Storage.prototype.setItem;
          Storage.prototype.setItem = function (key, value) {
            if (key.startsWith("krim-directory-checkpoint-recovery:")) { if (kind === "write-denied") throw new DOMException("合成存储拒绝", "QuotaExceededError"); return; }
            return set.call(this, key, value);
          };
        }, fault);
      } else {
        await seedPending(page, value, "storage-original-pending", `directory-storage-${fault}`);
        await page.evaluate(({ id, fault }) => {
          const key = `krim-directory-checkpoint-recovery:${id}`;
          if (fault === "invalid-json" || fault === "empty") localStorage.setItem(key, fault === "empty" ? "" : "{");
          else { const value = JSON.parse(localStorage.getItem(key)!); if (fault === "wrong-scope") value.scope_id = "session:other"; if (fault === "wrong-resource") value.resource_fingerprint = "a".repeat(64); if (fault === "unknown-field") value.private_token = "test"; localStorage.setItem(key, JSON.stringify(value)); }
        }, { id: value.session_id, fault });
      }
      await connect(page, value.topic_id); await open(page, value.agent_id);
      page.on("request", request => { if (request.method() === "POST" && new URL(request.url()).pathname === endpoint(value.agent_id)) posts++; });
      if (fault === "write-denied" || fault === "silent-write") { await page.locator("#directory-checkpoint-path").fill(`directory-storage-${fault}`); await page.locator("#directory-checkpoint-create").click(); }
      await expect(page.locator("#directory-checkpoint-create")).toBeDisabled();
      await expect(page.locator("#directory-checkpoint-status")).toContainText("目录捕获恢复记录不可用");
      await page.locator("#directory-checkpoint-query").fill(truth.checkpoint_id); await page.locator("#directory-checkpoint-by-id").click();
      await expect(page.locator("#directory-checkpoint-detail")).toHaveAttribute("data-checkpoint-id", truth.checkpoint_id);
      expect(posts).toBe(0); expect((await proof(page, value.agent_id)).reads).toBe(1);
    } finally { await destroyAgent(page, value.agent_id); }
  });
}

for (const fault of ["scope", "resource", "key", "id", "source", "digest", "count", "extra", "date", "status"] as const) {
  test(`历史回程${fault}拒绝且不展示假成功`, async ({ page }) => {
    const value = await actor(page, `directory-history-${fault}`);
    try {
      const truth = await original(page, value.agent_id, `directory-history-${fault}`, `history-${fault}`);
      await connect(page, value.topic_id); await open(page, value.agent_id);
      await page.locator("#directory-checkpoint-query").fill(truth.checkpoint_id); await page.locator("#directory-checkpoint-by-id").click();
      await expect(page.locator("#directory-checkpoint-detail")).toHaveAttribute("data-checkpoint-status", "available");
      const fake = structuredClone(truth) as unknown as Record<string, unknown>;
      if (fault === "scope") fake.scope_id = "session:other";
      if (fault === "resource") fake.resource_fingerprint = "b".repeat(64);
      if (fault === "key") fake.request_key = "another-key";
      if (fault === "id") fake.checkpoint_id = "00000000-0000-4000-8000-000000000002";
      if (fault === "source") fake.source = "workspace";
      if (fault === "digest") (fake.snapshot as Record<string, unknown>).snapshot_fingerprint = "a".repeat(64);
      if (fault === "count") (fake.snapshot as Record<string, unknown>).entry_count = 999;
      if (fault === "extra") fake.private_token = "forbidden";
      if (fault === "date") fake.created_at = "1";
      if (fault === "status") { fake.status = "capturing"; fake.snapshot = null; }
      await page.route(`**${endpoint(value.agent_id)}/${truth.checkpoint_id}`, route => route.fulfill({ json: fake }));
      await page.locator("#directory-checkpoint-by-id").click();
      await expect(page.locator("#directory-checkpoint-status")).toContainText("目录检查点回程与原事实不一致");
      await expect(page.locator("#directory-checkpoint-detail")).toHaveAttribute("data-checkpoint-id", truth.checkpoint_id);
      await expect(page.locator("#directory-checkpoint-detail")).toHaveAttribute("data-checkpoint-status", "available");
      await expect(page.locator("#directory-checkpoint-detail")).toContainText(truth.snapshot!.snapshot_fingerprint);
      expect((await proof(page, value.agent_id)).reads).toBe(1);
    } finally { await destroyAgent(page, value.agent_id); }
  });
}

test("真实完整目录字节与摘要、原任务刷新零重放、纯文本和英文窄视口", async ({ page }) => {
  const value = await actor(page, "directory-normal"); let posts = 0;
  page.on("request", request => { if (request.method() === "POST" && new URL(request.url()).pathname === endpoint(value.agent_id)) posts++; });
  try {
    await connect(page, value.topic_id);
    const title = '<img src=x onerror="window.directoryXss=1">';
    const id = await capture(page, value.agent_id, "directory-normal", title);
    expect(id).toBeTruthy(); const original = await (await page.request.get(`${endpoint(value.agent_id)}/${id}`, { headers })).json();
    expect(original).toMatchObject({ source: "workspace_directory", status: "available", root_path: "directory-normal", snapshot: { entry_count: 6, file_count: 3, directory_count: 3, byte_size: 17 } });
    const before = await proof(page, value.agent_id); expect(before.reads).toBe(1); expect(before.entries).toHaveLength(1);
    expect(before.entries[0].files["directory-normal/sub/raw.bin"]).toEqual({ hex: "00ff", mode: 0o640 });
    await expect(page.locator("#directory-checkpoint-detail")).toContainText(title); await expect(page.locator("#directory-checkpoint-detail img")).toHaveCount(0);
    expect(await page.evaluate(() => (window as unknown as Record<string, unknown>).directoryXss)).toBeUndefined();
    expect(JSON.stringify(original)).not.toContain("directory text"); expect(JSON.stringify(original)).not.toContain("token");
    await connect(page, value.topic_id); await open(page, value.agent_id);
    await expect(page.locator("#directory-checkpoint-detail")).toHaveAttribute("data-checkpoint-id", id!);
    await page.locator("#directory-checkpoint-refresh").click();
    expect(await proof(page, value.agent_id)).toEqual(before); expect(posts).toBe(1);
    await page.locator("#locale-select").selectOption("en-US"); await expect(page.locator("#checkpoint-directory")).toContainText("Complete directory fingerprint");
    await page.setViewportSize({ width: 420, height: 860 }); await page.locator("#checkpoint-directory").scrollIntoViewIfNeeded();
    await page.screenshot({ path: test.info().outputPath("directory-narrow.png") });
    await test.info().attach("directory-layout.json", { body: JSON.stringify(await page.locator("#checkpoint-directory").evaluate(node => [node, ...node.querySelectorAll("*")].map(element => {
      const rect = element.getBoundingClientRect(), css = getComputedStyle(element);
      return { tag: element.tagName, id: element.id, class: element.className, width: rect.width, left: rect.left, right: rect.right, scroll: element.scrollWidth, client: element.clientWidth, grid: css.gridTemplateColumns, minWidth: css.minWidth, overflow: css.overflowWrap, text: element.textContent?.slice(0, 100) };
    })), null, 2), contentType: "application/json" });
    expect(await page.locator("#checkpoint-directory").evaluate(node => node.scrollWidth <= node.clientWidth + 2)).toBe(true);
    expect(await (await page.request.get(`${endpoint(value.agent_id)}/${id}`, { headers })).json()).toEqual(original);
  } finally { await destroyAgent(page, value.agent_id); }
});

test("原模型目录创建与查询由浏览器读取同来源，重载不再次调用模型或Reader", async ({ page }) => {
  const value = await actor(page, "directory-model", { model: true }); let posts = 0;
  page.on("request", request => { if (request.method() === "POST" && new URL(request.url()).pathname === endpoint(value.agent_id)) posts++; });
  try {
    let completed = false;
    const executing = page.request.post(`/v1/agents/${value.agent_id}/execute`, { headers, data: { prompt: "完整捕获目录并查询", idempotency_key: "directory-model-original" } });
    void executing.then(() => { completed = true; });
    for (let index = 0; index < 150 && !completed; index++) {
      const state = await (await page.request.get(`/v1/agents/${value.agent_id}`, { headers })).json();
      if (state.current_run_id) {
        const pending = await (await page.request.get(`/v1/runs/${state.current_run_id}/interactions?pending_only=true`, { headers })).json();
        for (const item of pending) expect((await page.request.post(`/v1/runs/${state.current_run_id}/interactions/${item.id}/respond`, { headers, data: { action: "approve" } })).ok()).toBeTruthy();
      }
      await page.waitForTimeout(50);
    }
    const response = await executing; expect(response.ok(), await response.text()).toBeTruthy(); const result = (await response.json()).result; expect(result.status).toBe("completed");
    const before = await proof(page, value.agent_id); expect(before.reads).toBe(1); expect(before.provider_calls).toBe(3); expect(before.entries).toHaveLength(1);
    expect(before.entries[0].source).toEqual({ run_id: result.run_id, call_id: "browser-directory-create" });
    await connect(page, value.topic_id); await open(page, value.agent_id);
    await page.locator(`article[data-directory-checkpoint-id="${before.entries[0].checkpoint_id}"] button`).click();
    await expect(page.locator("#directory-checkpoint-detail")).toHaveAttribute("data-checkpoint-status", "available");
    await connect(page, value.topic_id); await open(page, value.agent_id);
    await expect(page.locator("#directory-checkpoint-detail")).toHaveAttribute("data-checkpoint-id", before.entries[0].checkpoint_id);
    expect(await proof(page, value.agent_id)).toEqual(before); expect(posts).toBe(0);
  } finally { await destroyAgent(page, value.agent_id); }
});

test("暂停中的真实目录任务可等待取消并保留原ID和零Reader", async ({ page }) => {
  const value = await actor(page, "directory-cancel");
  try {
    const blocked = await page.request.post("/_test/directory-checkpoint-control", { headers, data: { agent_id: value.agent_id, action: "block" } }); expect(blocked.ok()).toBeTruthy();
    await connect(page, value.topic_id); await open(page, value.agent_id); await page.locator("#directory-checkpoint-path").fill("directory-cancel");
    await page.locator("#directory-checkpoint-create").click(); await expect(page.locator("#directory-checkpoint-detail")).toHaveAttribute("data-checkpoint-status", "capturing");
    const id = await page.locator("#directory-checkpoint-detail").getAttribute("data-checkpoint-id");
    await page.locator("#directory-checkpoint-wait").click(); await expect(page.locator("#directory-checkpoint-wait")).toBeEnabled();
    await page.locator("#directory-checkpoint-cancel").click(); await expect(page.locator("#directory-checkpoint-detail")).toHaveAttribute("data-checkpoint-status", "cancelled");
    const saved = await proof(page, value.agent_id); expect(saved.reads).toBe(0); expect(saved.entries).toHaveLength(1); expect(saved.entries[0].checkpoint_id).toBe(id);
    await connect(page, value.topic_id); await open(page, value.agent_id); await expect(page.locator("#directory-checkpoint-detail")).toHaveAttribute("data-checkpoint-status", "cancelled");
    expect(await proof(page, value.agent_id)).toEqual(saved);
  } finally { await destroyAgent(page, value.agent_id); }
});

test("真实历史分页与按ID或原键读取同任务不再捕获", async ({ page }) => {
  const value = await actor(page, "directory-pages");
  try {
    const items = [];
    for (let index = 0; index < 9; index++) items.push(await original(page, value.agent_id, "directory-pages", `page-${index}`));
    await connect(page, value.topic_id); await open(page, value.agent_id);
    await expect(page.locator("#directory-checkpoint-items article")).toHaveCount(8); await page.locator("#directory-checkpoint-next").click();
    await expect(page.locator("#directory-checkpoint-items article")).toHaveCount(1); await expect(page.locator("#directory-checkpoint-next")).toBeDisabled();
    await page.locator("#directory-checkpoint-prev").click(); await expect(page.locator("#directory-checkpoint-items article")).toHaveCount(8);
    const truth = items[0]!; await page.locator("#directory-checkpoint-query").fill(truth.request_key); await page.locator("#directory-checkpoint-by-key").click();
    await expect(page.locator("#directory-checkpoint-detail")).toHaveAttribute("data-checkpoint-id", truth.checkpoint_id);
    await page.locator("#directory-checkpoint-query").fill(truth.checkpoint_id); await page.locator("#directory-checkpoint-by-id").click();
    await expect(page.locator("#directory-checkpoint-detail")).toContainText(truth.snapshot!.snapshot_fingerprint);
    const facts = await proof(page, value.agent_id); expect(facts.entries).toHaveLength(9); expect(facts.reads).toBe(9);
  } finally { await destroyAgent(page, value.agent_id); }
});

for (const kind of ["ask", "plan", "no-leaf", "readonly", "disabled", "closed"] as const) {
  test(`真实${kind}会话沿原授权控制目录新建`, async ({ page }) => {
    const name = `directory-mode-${kind}`;
    const value = await actor(page, name, { mode: kind === "ask" || kind === "plan" ? kind : "agent", leaf: kind !== "no-leaf", writable: kind !== "readonly", isolated_workspace: kind === "disabled" });
    try {
      if (kind === "disabled") expect((await page.request.post("/_test/directory-checkpoint-control", { headers, data: { agent_id: value.agent_id, action: "disable" } })).ok()).toBeTruthy();
      if (kind === "closed") expect((await page.request.post(`/v1/agents/${value.agent_id}/session/close`, { headers, data: { expected_revision: value.session.revision } })).ok()).toBeTruthy();
      await connect(page, value.topic_id); await open(page, value.agent_id); await page.locator("#directory-checkpoint-path").fill(name);
      if (kind === "plan") {
        await expect(page.locator("#directory-checkpoint-create")).toBeEnabled(); await page.locator("#directory-checkpoint-create").click();
        await expect(page.locator("#directory-checkpoint-detail")).toHaveAttribute("data-checkpoint-status", "available"); expect((await proof(page, value.agent_id)).reads).toBe(1);
      } else { await expect(page.locator("#directory-checkpoint-create")).toBeDisabled(); expect((await proof(page, value.agent_id)).reads).toBe(0); }
    } finally {
      await destroyAgent(page, value.agent_id);
    }
  });
}

test("跨Agent晚回程不能覆盖新选择或改变原持久意图", async ({ page }) => {
  const a = await actor(page, "directory-late-a"), b = await actor(page, "directory-late-b");
  let release: (() => void) | undefined;
  try {
    const first = await original(page, a.agent_id, "directory-late-a", "late-a"), second = await original(page, b.agent_id, "directory-late-b", "late-b");
    await connect(page, a.topic_id); await open(page, a.agent_id);
    let entered = false;
    await page.route(`**${endpoint(a.agent_id)}/${first.checkpoint_id}`, async route => { entered = true; await new Promise<void>(resolve => { release = resolve; }); await route.fulfill({ json: first }); });
    await page.locator("#directory-checkpoint-query").fill(first.checkpoint_id); await page.locator("#directory-checkpoint-by-id").click(); await expect.poll(() => entered).toBe(true);
    await open(page, b.agent_id); await page.locator("#directory-checkpoint-query").fill(second.request_key); await page.locator("#directory-checkpoint-by-key").click();
    await expect(page.locator("#directory-checkpoint-detail")).toHaveAttribute("data-checkpoint-id", second.checkpoint_id);
    const returned = page.waitForResponse(response => new URL(response.url()).pathname === `${endpoint(a.agent_id)}/${first.checkpoint_id}`); release!(); await returned;
    await expect(page.locator("#directory-checkpoint-detail")).toHaveAttribute("data-checkpoint-id", second.checkpoint_id);
    expect((await saved(page, b.session_id)).selected_id).toBe(second.checkpoint_id); expect(await saved(page, a.session_id)).toBeNull();
    expect((await proof(page, a.agent_id)).reads).toBe(1); expect((await proof(page, b.agent_id)).reads).toBe(1);
  } finally { release?.(); await destroyAgent(page, a.agent_id); await destroyAgent(page, b.agent_id); }
});

test("真实关闭会话使旧回程失效，冷读仍保留原任务", async ({ page }) => {
  const value = await actor(page, "directory-late-close"); let release: (() => void) | undefined;
  try {
    const truth = await original(page, value.agent_id, "directory-late-close", "late-close"); await connect(page, value.topic_id); await open(page, value.agent_id);
    let entered = false;
    const path = `**${endpoint(value.agent_id)}/${truth.checkpoint_id}`;
    await page.route(path, async route => { entered = true; await new Promise<void>(resolve => { release = resolve; }); await route.fulfill({ json: truth }); });
    await page.locator("#directory-checkpoint-query").fill(truth.checkpoint_id); await page.locator("#directory-checkpoint-by-id").click(); await expect.poll(() => entered).toBe(true);
    expect((await page.request.post(`/v1/agents/${value.agent_id}/session/close`, { headers, data: { expected_revision: value.session.revision } })).ok()).toBeTruthy();
    await page.locator("#refresh").click(); await expect(page.locator("#directory-checkpoint-status")).toContainText("当前会话只能查询目录检查点历史");
    const returned = page.waitForResponse(response => new URL(response.url()).pathname === `${endpoint(value.agent_id)}/${truth.checkpoint_id}`); release!(); await returned;
    await expect(page.locator("#directory-checkpoint-detail")).not.toHaveAttribute("data-checkpoint-id");
    await page.unroute(path); await page.locator("#directory-checkpoint-query").fill(truth.checkpoint_id); await page.locator("#directory-checkpoint-by-id").click();
    await expect(page.locator("#directory-checkpoint-detail")).toHaveAttribute("data-checkpoint-id", truth.checkpoint_id); await expect(page.locator("#directory-checkpoint-create")).toBeDisabled(); expect((await proof(page, value.agent_id)).reads).toBe(1);
  } finally { release?.(); await destroyAgent(page, value.agent_id); }
});
