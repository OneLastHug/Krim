import { expect, test as base, type APIRequestContext, type Page } from "@playwright/test";

const headers = { Authorization: "Bearer browser-test-key" };
const tools = ["workspace_read", "workspace_list", "workspace_preview", "workspace_change", "workspace_reconcile", "workspace_finalize", "directory_inspect", "directory_preview", "directory_change", "directory_query", "directory_reconcile", "directory_finalize"];
const test = base.extend({ request: async ({ playwright, baseURL }, use) => {
  const context = await playwright.request.newContext({ baseURL, extraHTTPHeaders: headers });
  await use(context); await context.dispose();
} });
type Agent = { agent_id: string; session_id: string; topic_id: string; name: string };
type Batch = { batch_id: string; request_key: string; scope_id: string; items: Array<{ source: string; proposal_id: string; operation: { operation_id: string } | null }> };

async function agent(api: APIRequestContext, name: string): Promise<Agent> {
  const configured = await api.put(`/v1/agents/profiles/${name}`, { data: { expected_revision: 0,
    config: { name, description: "混合批次浏览器验证", workspace_key: "edit", tool_names: tools, mode: "agent" } } });
  expect(configured.ok()).toBeTruthy();
  const result = await api.post("/v1/agents", { data: { name } }); expect(result.ok()).toBeTruthy(); return result.json();
}
async function preview(api: APIRequestContext, value: Agent, suffix = "", path?: string) {
  const base = `/v1/agents/${value.agent_id}/workspace`;
  const file = await api.post(`${base}/changes/preview`, { data: { request_key: `file${suffix}`, spec: {
    path: `${value.name}${suffix}.txt`, operation: "write", expected_sha256: null, content: "mixed data\n" } } });
  const directory = await api.post(`${base}/directory-changes/preview`, { data: { request_key: `dir${suffix}`, spec: {
    path: path || `${value.name}${suffix}-dir`, operation: "create", mode: 0o750 } } });
  expect(file.ok()).toBeTruthy(); expect(directory.ok()).toBeTruthy();
  return [{ source: "file", proposal_id: (await file.json()).proposal_id, expected_revision: 0 },
    { source: "directory", proposal_id: (await directory.json()).proposal_id, expected_revision: 0 }];
}
async function apply(api: APIRequestContext, value: Agent, key = "mixed-review", path?: string): Promise<Batch> {
  const targets = await preview(api, value, "", path), base = `/v1/agents/${value.agent_id}/workspace/mixed-batches`;
  const started = await api.post(`${base}?confirmed=true`, { data: { action: "apply", request_key: key, targets } });
  expect(started.ok()).toBeTruthy(); const id = (await started.json()).batch_id;
  const done = await api.post(`${base}/${id}/wait?timeout_seconds=30`); expect(done.ok()).toBeTruthy();
  const result = await done.json(); expect(result.status).toBe("done"); return result;
}
async function connect(page: Page, value: Agent): Promise<void> {
  await page.goto(`/ui/?topic=${value.topic_id}`);
  await page.getByLabel("服务访问密钥").fill("browser-test-key");
  await page.getByRole("button", { name: "连接", exact: true }).click();
  await expect(page.locator("#connection")).toHaveClass(/ready/);
  await page.getByText("变更清单", { exact: true }).click();
  await page.locator("#change-source").selectOption(value.agent_id);
  await expect(page.locator("#change-mixed-history")).toBeVisible();
}
async function destroy(api: APIRequestContext, value: Agent) { await api.delete(`/v1/agents/${value.agent_id}`); }

test("混合历史真实两来源、原操作导航、刷新和重载零新批次", async ({ page, request }) => {
  const value = await agent(request, "mixed-history-basic");
  try {
    const batch = await apply(request, value);
    let starts = 0;
    page.on("request", item => { if (item.method() === "POST" && /\/mixed-batches(?:\?|$)/.test(item.url())) starts++; });
    await connect(page, value);
    await expect(page.locator(".mixed-batch-result")).toContainText("批次已确认");
    await expect(page.locator("#change-mixed-results [data-mixed-batch-item]")).toHaveCount(2);
    for (const item of batch.items) await expect(page.locator(`#change-mixed-results [data-mixed-batch-item="${item.proposal_id}"]`)).toContainText(item.operation!.operation_id);
    await page.locator('#change-mixed-results [data-source="file"] button').click();
    await expect(page.locator(`article[data-agent-id="${value.agent_id}"] .workspace-change-detail .workspace-diff`)).toContainText("mixed data");
    await page.locator('#change-mixed-results [data-source="directory"] button').click();
    await expect(page.locator(`article[data-agent-id="${value.agent_id}"] .directory-detail > strong`)).toContainText("目录已应用");
    await page.locator("#change-refresh").click();
    await expect(page.locator(".mixed-batch-result")).toContainText(batch.batch_id);
    await connect(page, value);
    await expect(page.locator(".mixed-batch-result")).toContainText(batch.batch_id);
    expect(starts).toBe(0);
    expect((await (await request.get(`/v1/agents/${value.agent_id}/workspace/mixed-batches/history`)).json()).total).toBe(1);
    await page.locator("#change-mixed-history").screenshot({ path: test.info().outputPath("mixed-history.png") });
  } finally { await destroy(request, value); }
});

test("模型创建的混合批次由变更清单刷新和历史读取", async ({ page, request }) => {
  const value = await agent(request, "mixed-history-model");
  try {
    const targets = await preview(request, value);
    const created = await request.post("/_test/model-mixed", {
      data: { agent_id: value.agent_id, request: { action: "apply", request_key: "browser-model", targets } },
    });
    expect(created.ok()).toBeTruthy();
    const batch = await created.json();
    expect(batch.status).toBe("done");
    expect(created.headers()["content-type"]).toContain("application/json");
    expect(JSON.stringify(batch)).not.toContain("browser-model-call");
    let starts = 0;
    page.on("request", item => { if (item.method() === "POST" && /\/mixed-batches(?:\?|$)/.test(item.url())) starts++; });
    await connect(page, value);
    await expect(page.locator(".mixed-batch-result")).toContainText(batch.batch_id);
    await expect(page.locator("#change-mixed-results [data-mixed-batch-item]")).toHaveCount(2);
    await page.reload();
    await connect(page, value);
    await expect(page.locator(".mixed-batch-result")).toContainText(batch.batch_id);
    expect(starts).toBe(0);
    const history = await (await request.get(`/v1/agents/${value.agent_id}/workspace/mixed-batches/history`)).json();
    expect(history.items[0].batch_id).toBe(batch.batch_id);
  } finally { await destroy(request, value); }
});

test("混合原键缺失、UTF8纯文本、Locale草稿与关闭会话历史", async ({ page, request }) => {
  const value = await agent(request, "mixed-history-keys");
  const key = '<img src=x onerror="window.mixedExecuted=1">中文?/#%';
  try {
    const batch = await apply(request, value, key, "<img onerror=alert(1)>");
    await connect(page, value);
    await expect(page.locator(".mixed-batch-result")).toContainText(key);
    await expect(page.locator("#change-mixed-results img")).toHaveCount(0);
    expect(await page.evaluate(() => (window as unknown as { mixedExecuted?: number }).mixedExecuted)).toBeUndefined();
    await page.locator("#change-mixed-query").fill(key); await page.locator("#change-mixed-query-key").click();
    await expect(page.locator(".mixed-batch-result")).toContainText(batch.batch_id);
    await page.locator("#change-mixed-query").fill("missing?中文/../#"); await page.locator("#change-mixed-query-key").click();
    await expect(page.locator("#change-mixed-status")).toContainText("未找到原批次");
    await page.locator("#change-mixed-query").fill("query draft");
    await page.locator("#locale-select").selectOption("en-US");
    await expect(page.locator("#change-mixed-query")).toHaveValue("query draft");
    await expect(page.getByText("Mixed batch history", { exact: true })).toBeVisible();
    const card = page.locator(`article[data-agent-id="${value.agent_id}"]`);
    await card.getByRole("button", { name: "Close session", exact: true }).click();
    await expect(card).toContainText("Session closed");
    await page.locator("#change-refresh").click();
    await expect(page.locator(".mixed-batch-result")).toContainText(batch.batch_id);
    await expect(page.locator("#change-mixed-cancel")).toBeDisabled();
  } finally { await destroy(request, value); }
});

test("混合等待只读、确认取消与未开始子项关闭", async ({ page, request }) => {
  const value = await agent(request, "mixed-history-cancel");
  try {
    const targets = await preview(request, value);
    await request.post("/_test/native-batch", { data: { agent_id: value.agent_id, action: "block" } });
    const base = `/v1/agents/${value.agent_id}/workspace/mixed-batches`;
    const started = await request.post(`${base}?confirmed=true`, { data: { action: "apply", request_key: "mixed-wait", targets } });
    const id = (await started.json()).batch_id;
    await expect.poll(async () => (await (await request.post("/_test/native-batch", { data: { agent_id: value.agent_id, action: "status" } })).json()).entered).toBe(true);
    await connect(page, value);
    await expect(page.locator(".mixed-batch-result")).toContainText("批次执行中");
    await page.locator("#change-mixed-wait").click();
    await expect(page.locator(".mixed-batch-result")).toContainText("批次执行中");
    page.once("dialog", dialog => dialog.accept()); await page.locator("#change-mixed-cancel").click();
    await expect(page.locator(".mixed-batch-result")).toContainText("批次已取消");
    const final = await (await request.get(`${base}/${id}`)).json();
    expect(final.items.every((item: { operation: unknown; status: string }) => item.operation === null && item.status === "skipped")).toBe(true);
    await expect(page.locator("#change-mixed-cancel")).toBeDisabled();
  } finally { await request.post("/_test/native-batch", { data: { agent_id: value.agent_id, action: "release" } }); await destroy(request, value); }
});

test("混合unknown导航原目录核实，返回同一原操作且零新attempt", async ({ page, request }) => {
  const value = await agent(request, "mixed-history-unknown");
  try {
    const targets = await preview(request, value);
    await request.post("/_test/directory", { data: { action: "fail_save" } });
    const base = `/v1/agents/${value.agent_id}/workspace/mixed-batches`;
    const started = await request.post(`${base}?confirmed=true`, { data: { action: "apply", request_key: "unknown-mixed", targets } });
    const id = (await started.json()).batch_id;
    const original = await (await request.post(`${base}/${id}/wait?timeout_seconds=30`)).json();
    expect(original.status).toBe("uncertain"); const operation = original.items[1].operation.operation_id;
    await connect(page, value); await expect(page.locator(".mixed-batch-result")).toContainText("批次结果待核实");
    await page.locator('#change-mixed-results [data-source="directory"] button').click();
    const panel = page.locator(`article[data-agent-id="${value.agent_id}"] .directory-changes`);
    await expect(panel.locator(".directory-operation > strong")).toContainText("目录结果待核实");
    page.once("dialog", dialog => dialog.accept()); await panel.getByRole("button", { name: "核实目录结果", exact: true }).click();
    await expect(panel.locator(".directory-operation > strong")).toContainText("目录操作已确认");
    await page.locator("#change-refresh").click();
    await expect(page.locator(".mixed-batch-result")).toContainText("批次已确认");
    await expect(page.locator('#change-mixed-results [data-source="directory"]')).toContainText(operation);
    const counts = await request.post("/_test/directory", { data: { action: "counts", agent_id: value.agent_id } });
    expect((await counts.json()).operations).toBe(1);
    expect((await (await request.get(`${base}/${id}`)).json()).items[1].operation.operation_id).toBe(operation);
  } finally { await destroy(request, value); }
});

test("混合33项真实拒绝历史分页，创建上界与原父ID保持", async ({ page, request }) => {
  test.setTimeout(150_000);
  const value = await agent(request, "mixed-history-pages");
  try {
    const targets = await preview(request, value), base = `/v1/agents/${value.agent_id}/workspace/mixed-batches`;
    const ids: string[] = [];
    for (let index = 0; index < 33; index++) {
      const started = await request.post(base, { data: { action: "apply", request_key: `denied-${index}`, targets } });
      expect(started.ok()).toBeTruthy(); const id = (await started.json()).batch_id; ids.unshift(id);
      const done = await request.post(`${base}/${id}/wait?timeout_seconds=30`);
      expect((await done.json()).status).toBe("failed");
    }
    await connect(page, value);
    await expect(page.locator("#change-mixed-batches button")).toHaveCount(32);
    await expect(page.locator("#change-mixed-info")).toContainText("共33项");
    await page.locator("#change-mixed-next").click();
    await expect(page.locator("#change-mixed-batches button")).toHaveCount(1);
    await page.locator("#change-mixed-batches button").click();
    await expect(page.locator(".mixed-batch-result")).toContainText(ids[32]!);
    await expect(page.locator("#change-mixed-next")).toBeDisabled();
    await page.locator("#change-mixed-prev").click();
    await expect(page.locator("#change-mixed-batches button")).toHaveCount(32);
    const history = await (await request.get(`${base}/history`)).json();
    expect(history.items[0].batch_id).toBe(ids[0]);
    expect(history.items.every((item: { done_count: number }) => item.done_count === 0)).toBe(true);
  } finally { await destroy(request, value); }
});

test("混合两来源切换拒绝迟到回程，查询草稿独立", async ({ page, request }) => {
  const first = await agent(request, "mixed-history-first"), second = await agent(request, "mixed-history-second");
  let release!: () => void;
  const gate = new Promise<void>(resolve => { release = resolve; });
  try {
    const firstBatch = await apply(request, first), secondBatch = await apply(request, second);
    await connect(page, first); await expect(page.locator(".mixed-batch-result")).toContainText(firstBatch.batch_id);
    await page.route(`**/v1/agents/${first.agent_id}/workspace/mixed-batches/${firstBatch.batch_id}`, async route => { const response = await route.fetch(); await gate; await route.fulfill({ response }); });
    await page.locator("#change-mixed-query").fill(firstBatch.batch_id); await page.locator("#change-mixed-query-id").click();
    await page.locator("#change-source").selectOption(second.agent_id);
    await expect(page.locator(".mixed-batch-result")).toContainText(secondBatch.batch_id);
    release();
    await expect(page.locator(".mixed-batch-result")).not.toContainText(firstBatch.batch_id);
    await page.locator("#change-mixed-query").fill("second-draft");
    await page.locator("#change-source").selectOption(first.agent_id);
    await expect(page.locator("#change-mixed-query")).toHaveValue(firstBatch.batch_id);
  } finally { release(); await destroy(request, first); await destroy(request, second); }
});

for (const mismatch of ["scope", "revision"] as const) test(`混合${mismatch}错误回程不能覆盖真实批次`, async ({ page, request }) => {
  const value = await agent(request, `mixed-history-guard-${mismatch}`);
  try {
    const batch = await apply(request, value);
    await connect(page, value); await expect(page.locator(".mixed-batch-result")).toContainText(batch.batch_id);
    await page.route(`**/v1/agents/${value.agent_id}/workspace/mixed-batches/${batch.batch_id}`, async route => {
      const response = await route.fetch(), result = await response.json();
      if (mismatch === "scope") result.scope_id = "session:foreign";
      else result.items[0].operation.expected_revision += 1;
      await route.fulfill({ response, json: result });
    });
    await page.locator("#change-mixed-query").fill(batch.batch_id); await page.locator("#change-mixed-query-id").click();
    await expect(page.locator("#change-mixed-status")).toContainText("回程与当前会话不一致");
    await expect(page.locator(".mixed-batch-result")).toContainText(batch.batch_id);
  } finally { await destroy(request, value); }
});
