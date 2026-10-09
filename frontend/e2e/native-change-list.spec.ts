import { expect, test as base, type APIRequestContext, type Page } from "@playwright/test";
import { writeFile } from "node:fs/promises";

const test = base.extend({
  request: async ({ playwright, baseURL }, use) => {
    const context = await playwright.request.newContext({ baseURL, extraHTTPHeaders: { Authorization: "Bearer browser-test-key" } });
    await use(context); await context.dispose();
  },
});

const headers = { Authorization: "Bearer browser-test-key" };
const tools = ["workspace_read", "workspace_list", "workspace_preview", "workspace_change", "workspace_reconcile", "workspace_finalize"];
type Agent = { agent_id: string; session_id: string; topic_id: string; name: string };
type Proposal = { proposal_id: string; revision: number };

async function createAgent(api: APIRequestContext, name: string, mode = "agent"): Promise<Agent> {
  const profile = await api.put(`/v1/agents/profiles/${name}`, { headers, data: { expected_revision: 0,
    config: { name, description: "原生清单浏览器验证", workspace_key: "edit", tool_names: tools, mode } } });
  expect(profile.ok()).toBeTruthy();
  const response = await api.post("/v1/agents", { headers, data: { name } });
  expect(response.ok()).toBeTruthy(); return await response.json();
}
async function proposal(api: APIRequestContext, agent: Agent, path: string, content: string, sha: string | null = null): Promise<Proposal> {
  const response = await api.post(`/v1/agents/${agent.agent_id}/workspace/changes/preview?confirmed=true`, {
    headers, data: { spec: { operation: "write", path, content, expected_sha256: sha }, request_key: crypto.randomUUID() },
  });
  expect(response.ok()).toBeTruthy(); return await response.json();
}
async function apply(api: APIRequestContext, agent: Agent, change: Proposal): Promise<void> {
  const result = await api.post(`/v1/agents/${agent.agent_id}/workspace/changes?confirmed=true`, { headers,
    data: { proposal_id: change.proposal_id, expected_revision: change.revision, action: "apply", request_key: crypto.randomUUID() } });
  expect(result.ok()).toBeTruthy(); expect((await result.json()).status).toBe("done");
}
async function setupFiles(api: APIRequestContext, agent: Agent, count: number): Promise<void> {
  for (let index = 0; index < count; index++) {
    const change = await proposal(api, agent, `${agent.name}-${index}.txt`, `content-${index}\n`); await apply(api, agent, change);
  }
}
async function connect(page: Page, agent: Agent): Promise<void> {
  await page.goto(`/ui/?topic=${agent.topic_id}`);
  await page.getByLabel("服务访问密钥").fill("browser-test-key");
  await page.getByRole("button", { name: "连接", exact: true }).click();
  await expect(page.locator("#connection")).toHaveClass(/ready/);
}
async function select(page: Page, agent: Agent): Promise<void> {
  if (!(await page.locator("#change-panel").evaluate(element => (element as HTMLDetailsElement).open))) await page.getByText("变更清单", { exact: true }).click();
  await page.locator("#change-source").selectOption(agent.agent_id);
  await expect(page.locator("#change-source")).toHaveValue(agent.agent_id);
  await expect(page.locator("#change-native-pages")).toBeVisible();
  await expect(page.locator("#change-items .change-card").first()).toBeVisible();
}
async function confirm(page: Page, id: string): Promise<void> {
  page.once("dialog", dialog => dialog.accept()); await page.locator(id).click();
}
async function destroy(api: APIRequestContext, agent: Agent): Promise<void> {
  await api.delete(`/v1/agents/${agent.agent_id}`, { headers });
}

test("原生清单33项分页、本页keep、真实Diff证明和已保留undo", async ({ page, request }) => {
  test.setTimeout(150_000);
  const agent = await createAgent(request, "native-page-review");
  try {
    await setupFiles(request, agent, 33); await connect(page, agent); await select(page, agent);
    await expect(page.locator("#change-items .change-card")).toHaveCount(32);
    await expect(page.locator("#change-page-info")).toContainText("共33项");
    await page.screenshot({ path: test.info().outputPath("native-change-list-page.png"), fullPage: true });
    await expect(page.locator("#change-clear")).toBeHidden();
    await page.locator("#change-next").click();
    await expect(page.locator("#change-items .change-card")).toHaveCount(1);
    await expect(page.locator("#change-next")).toBeDisabled();
    const changed = await page.locator("#change-items .change-card").getAttribute("data-change-id");
    let body: { targets: Array<{ proposal_id: string }>; host_session_id?: string } | undefined;
    page.on("request", request => { if (request.method() === "POST" && request.url().endsWith("/native-batches?confirmed=true")) body = request.postDataJSON(); });
    await confirm(page, "#change-keep-all");
    await expect(page.locator(".native-batch-result")).toContainText("文件操作已确认");
    expect(body?.targets).toEqual([{ proposal_id: changed, expected_revision: 2 }]);
    expect(body?.host_session_id).toBeUndefined();
    await page.locator("#change-results").getByRole("button", { name: "查看文件差异与证明", exact: true }).click();
    const detail = page.locator(`article[data-agent-id="${agent.agent_id}"] .workspace-change-detail`);
    await expect(detail.locator(".workspace-diff")).toContainText("+content-0");
    await expect(detail.locator(".workspace-operation-proof")).toContainText("原目标保留证明");
    await expect(detail.locator("strong")).toContainText("文件已保留");
    await page.screenshot({ path: test.info().outputPath("native-change-diff-proof.png"), fullPage: true });
    await page.locator("#change-next").click();
    const row = page.locator(`#change-items [data-change-id="${changed}"]`);
    await expect(row.getByRole("button", { name: "撤销变更", exact: true })).toBeEnabled();
    await expect(row.getByRole("button", { name: "保留变更", exact: true })).toBeDisabled();
    page.once("dialog", dialog => dialog.accept()); await row.getByRole("button", { name: "撤销变更", exact: true }).click();
    await expect(page.locator(".native-batch-result")).toContainText("文件操作已确认");
    await expect(page.locator("#change-page-info")).toContainText("共32项");
    const removed = await request.get(`/v1/agents/${agent.agent_id}/workspace/read?path=${agent.name}-0.txt`, { headers });
    expect(removed.status()).toBe(404);
    await page.locator("#change-filter").selectOption("history");
    await expect(page.locator("#change-items [data-change-status=undone]")).toHaveCount(1);
  } finally { await destroy(request, agent); }
});

test("丢失接受响应按原键查询一次批次，合法null与刷新历史恢复", async ({ page, request }) => {
  const agent = await createAgent(request, "native-response-review");
  try {
    await setupFiles(request, agent, 1); await connect(page, agent); await select(page, agent);
    let starts = 0; let key = "";
    await page.route(`**/v1/agents/${agent.agent_id}/workspace/native-batches?confirmed=true`, async route => {
      starts++; key = route.request().postDataJSON().request_key;
      await route.fetch(); await route.abort("failed");
    });
    await confirm(page, "#change-keep-all");
    await expect(page.locator(".native-batch-result")).toContainText("文件操作已确认");
    expect(starts).toBe(1);
    expect(await page.locator("#change-batch-query").inputValue()).toBe(key);
    await page.locator("#change-batch-query").fill("missing-key?中文/bytes");
    await page.locator("#change-query-key").click();
    await expect(page.locator("#change-status")).toContainText("未找到原批次");
    await expect(page.locator("#change-status")).not.toContainText("无效的数据");
    await connect(page, agent); await select(page, agent);
    await expect(page.locator("#change-batches button")).toHaveCount(1);
    await expect(page.locator(".native-batch-result")).toContainText(key);
    expect(starts).toBe(1);
  } finally { await destroy(request, agent); }
});

test("旧快照拒绝不重放，切换scope丢弃迟到页且保留各自查询草稿", async ({ page, request }) => {
  const first = await createAgent(request, "native-first-scope"), second = await createAgent(request, "native-second-scope");
  try {
    await setupFiles(request, first, 1); await setupFiles(request, second, 1);
    await connect(page, first); await select(page, first);
    const stale = await proposal(page.request, first, "native-unused.txt", "not applied"); expect(stale.proposal_id).toBeTruthy();
    let posts = 0;
    page.on("request", request => { if (request.method() === "POST" && request.url().includes("/native-batches?confirmed")) posts++; });
    await confirm(page, "#change-keep-all");
    await expect(page.locator("#change-status")).toContainText("快照已变化");
    await expect(page.locator("#change-keep-all")).toBeEnabled();
    expect(posts).toBe(1);
    await page.locator("#change-batch-query").fill("first-original-key");
    let release: (() => void) | undefined;
    const paused = new Promise<void>(resolve => { release = resolve; });
    let entered: (() => void) | undefined;
    const seen = new Promise<void>(resolve => { entered = resolve; });
    await page.route(`**/v1/agents/${first.agent_id}/workspace/native-changes?*`, async route => {
      const response = await route.fetch(); entered!(); await paused; await route.fulfill({ response });
    });
    await page.locator("#change-refresh").click(); await seen;
    await page.locator("#change-source").selectOption(second.agent_id);
    await expect(page.locator("#change-items .change-title")).toHaveText("native-second-scope-0.txt");
    await page.locator("#change-batch-query").fill("second-original-key");
    release!();
    await expect(page.locator("#change-items .change-title")).toHaveText("native-second-scope-0.txt");
    await page.unrouteAll({ behavior: "wait" });
    await page.locator("#change-source").selectOption(first.agent_id);
    await expect(page.locator("#change-batch-query")).toHaveValue("first-original-key");
    await page.locator("#locale-select").selectOption("en-US");
    await expect(page.locator("#change-batch-query")).toHaveValue("first-original-key");
    await expect(page.locator("#change-keep-all")).toHaveText("Keep this page");
    const card = page.locator(`article[data-agent-id="${first.agent_id}"]`);
    await card.getByRole("button", { name: "Close session", exact: true }).click();
    await expect(page.locator("#change-keep-all")).toBeDisabled();
    await page.locator("#change-refresh").click();
    await expect(page.locator("#change-items .change-title")).toHaveText("native-first-scope-0.txt");
  } finally { await destroy(request, first); await destroy(request, second); }
});

test("活动批次刷新发现、等待超时继续运行与原批次取消", async ({ page, request }) => {
  const agent = await createAgent(request, "native-cancel-review");
  try {
    await setupFiles(request, agent, 1); await connect(page, agent); await select(page, agent);
    expect((await request.post("/_test/native-batch", { data: { agent_id: agent.agent_id, action: "block" } })).ok()).toBeTruthy();
    await confirm(page, "#change-undo-all");
    await expect.poll(async () => (await (await request.post("/_test/native-batch", { data: { agent_id: agent.agent_id, action: "status" } })).json()).entered).toBeTruthy();
    await expect(page.locator(".native-batch-result")).toContainText("文件操作执行中");
    const id = await page.locator(".native-batch-result").getAttribute("data-batch-id");
    await connect(page, agent); await select(page, agent);
    await expect(page.locator(".native-batch-result")).toHaveAttribute("data-batch-id", id!);
    await expect(page.locator("#change-cancel")).toBeEnabled();
    const waiting = page.waitForResponse(response => response.url().includes(`/native-batches/${id}/wait?timeout_seconds=2`));
    await page.locator("#change-wait").click();
    expect((await (await waiting).json()).producer_status).toBe("running");
    await page.locator("#change-cancel").click();
    await expect(page.locator(".native-batch-result")).toContainText("批次已取消");
    await expect(page.locator("#change-cancel")).toBeDisabled();
    await expect(page.locator("#change-results")).toContainText("尚无关联文件操作");
    const content = await request.get(`/v1/agents/${agent.agent_id}/workspace/read?path=${agent.name}-0.txt`);
    expect((await content.json()).content).toBe("content-0\n");
  } finally {
    await request.post("/_test/native-batch", { data: { agent_id: agent.agent_id, action: "release" } });
    await destroy(request, agent);
  }
});

test("原批次不确定回执沿既有Diff入口核实，查询结果不重放文件", async ({ page, request }) => {
  const agent = await createAgent(request, "native-uncertain-review");
  try {
    await setupFiles(request, agent, 1); await connect(page, agent); await select(page, agent);
    expect((await request.post("/_test/workspace", { data: { action: "fail_save" } })).ok()).toBeTruthy();
    await confirm(page, "#change-undo-all");
    await expect(page.locator(".native-batch-result")).toContainText("文件结果待核实");
    await expect(page.locator("#change-cancel")).toBeDisabled();
    expect((await request.get(`/v1/agents/${agent.agent_id}/workspace/read?path=${agent.name}-0.txt`)).status()).toBe(404);
    await page.locator("#change-results").getByRole("button", { name: "查看文件差异与证明", exact: true }).click();
    const panel = page.locator(`article[data-agent-id="${agent.agent_id}"] .workspace-changes`);
    page.once("dialog", dialog => dialog.accept());
    await panel.getByRole("button", { name: "核实文件结果", exact: true }).click();
    await expect(panel.locator(".workspace-operation-result")).toContainText("文件操作已确认");
    await page.locator("#change-refresh").click();
    await expect(page.locator(".native-batch-result")).toContainText("文件操作已确认");
    expect((await (await request.get(`/v1/agents/${agent.agent_id}/workspace/native-batches/history`)).json()).total).toBe(1);
  } finally { await destroy(request, agent); }
});

test("批次历史32项分页、旧游标重读、HTML纯文本和JSON错误边界", async ({ page, request }) => {
  test.setTimeout(150_000);
  const agent = await createAgent(request, "native-history-review");
  try {
    await setupFiles(request, agent, 33);
    await connect(page, agent); await select(page, agent);
    await proposal(request, agent, "native-extra-preview.txt", "<img src=x onerror=window.nativeExecuted=1>");
    await page.locator("#change-next").click();
    await expect(page.locator("#change-status")).toContainText("已返回第一页");
    await expect(page.locator("#change-page-info")).toContainText("第1页");
    for (let index = 0; index < 33; index++) {
      const current = await (await request.get(`/v1/agents/${agent.agent_id}/workspace/native-changes?limit=32`)).json();
      let item = current.items.find((value: { status: string }) => value.status === "applied");
      if (!item) {
        const next = await (await request.get(`/v1/agents/${agent.agent_id}/workspace/native-changes?${new URLSearchParams({ limit: "32", cursor: current.next_cursor })}`)).json();
        item = next.items.find((value: { status: string }) => value.status === "applied");
      }
      const key = index === 0 ? '<img src=x onerror="window.nativeExecuted=1">' : `history-${index}`;
      const started = await request.post(`/v1/agents/${agent.agent_id}/workspace/native-batches?confirmed=true`, { data: {
        action: "keep", snapshot_revision: current.snapshot_revision, targets: [{ proposal_id: item.proposal_id, expected_revision: item.revision }], request_key: key,
      } });
      expect(started.ok()).toBeTruthy(); const batch = await started.json();
      // 原wait允许返回running；给异步单项批次独立的观察窗口，不重新发起批次。
      const done = await request.post(`/v1/agents/${agent.agent_id}/workspace/native-batches/${batch.batch_id}/wait?timeout_seconds=30`);
      let result = await done.json();
      if (result.status === "uncertain") {
        // 保留的资格检查可能未及时取得证明；保存原始未知回执，显式只读核实原operation。
        const operation = result.items[0].operation;
        expect(operation.action).toBe("keep");
        expect(operation.receipt.error_code).toBe("workspace_reconcile_inconclusive");
        const evidence = test.info().outputPath(`history-${index}-uncertain.json`);
        await writeFile(evidence, JSON.stringify(result, null, 2));
        await test.info().attach(`history-${index}-uncertain`, { path: evidence, contentType: "application/json" });
        const verified = await request.post(`/v1/agents/${agent.agent_id}/workspace/operations/${operation.operation_id}/reconcile?confirmed=true`);
        expect(verified.ok()).toBeTruthy();
        const receipt = await verified.json();
        expect(receipt.operation_id).toBe(operation.operation_id);
        expect(receipt.status).toBe("done"); expect(receipt.receipt.changed).toBe(false);
        result = await (await request.get(`/v1/agents/${agent.agent_id}/workspace/native-batches/${batch.batch_id}`)).json();
        expect(result.items[0].operation.operation_id).toBe(operation.operation_id);
      }
      expect(result.status).toBe("done");
    }
    await page.locator("#change-refresh").click();
    await expect(page.locator("#change-batches button")).toHaveCount(32);
    await expect(page.locator("#change-history-info")).toContainText("共33项");
    await page.locator("#change-history-next").click();
    await expect(page.locator("#change-batches button")).toHaveCount(1);
    await page.screenshot({ path: test.info().outputPath("native-batch-history.png"), fullPage: true });
    await page.locator("#change-batches button").click();
    await expect(page.locator(".native-batch-result")).toContainText('<img src=x onerror="window.nativeExecuted=1">');
    await expect(page.locator("#change-results img")).toHaveCount(0);
    expect(await page.evaluate(() => (window as unknown as { nativeExecuted?: number }).nativeExecuted)).toBeUndefined();
    await page.route("**/ui/test-json-null", route => route.fulfill({ status: 200, contentType: "application/json", body: "null" }));
    await page.route("**/ui/test-json-invalid", route => route.fulfill({ status: 200, contentType: "application/json", body: "{" }));
    const behavior = await page.evaluate(async () => {
      const api = await import("/ui/api.js") as { requestJson: (path: string) => Promise<unknown>; requestJsonNullable: (path: string) => Promise<unknown> };
      const nullable = await api.requestJsonNullable("/ui/test-json-null");
      let malformedRejected = false, requiredRejected = false;
      try { await api.requestJsonNullable("/ui/test-json-invalid"); } catch { malformedRejected = true; }
      try { await api.requestJson("/ui/test-json-null"); } catch { requiredRejected = true; }
      return { nullable, malformedRejected, requiredRejected };
    });
    expect(behavior).toEqual({ nullable: null, malformedRejected: true, requiredRejected: true });
    // 保持较早历史页与旧回执的审阅焦点；独立活动生产者结束后须自动解锁文件动作。
    const added = await proposal(request, agent, "native-history-live.txt", "live\n"); await apply(request, agent, added);
    await request.post("/_test/native-batch", { data: { agent_id: agent.agent_id, action: "block" } });
    const current = await (await request.get(`/v1/agents/${agent.agent_id}/workspace/native-changes`)).json();
    const target = current.items.find((item: { proposal_id: string }) => item.proposal_id === added.proposal_id);
    const running = await request.post(`/v1/agents/${agent.agent_id}/workspace/native-batches?confirmed=true`, { data: {
      action: "undo", snapshot_revision: current.snapshot_revision, request_key: "history-live",
      targets: [{ proposal_id: added.proposal_id, expected_revision: target.revision }],
    } });
    expect(running.ok()).toBeTruthy();
    await expect.poll(async () => (await (await request.post("/_test/native-batch", { data: { agent_id: agent.agent_id, action: "status" } })).json()).entered).toBeTruthy();
    await page.locator("#change-refresh").click();
    await expect(page.locator("#change-cancel")).toBeEnabled();
    await page.locator("#change-history-next").click();
    await expect(page.locator("#change-batches button")).toHaveCount(2);
    await page.locator("#change-batches button").last().click();
    await expect(page.locator(".native-batch-result")).toContainText("<img");
    const oldBatch = await page.locator(".native-batch-result").getAttribute("data-batch-id");
    await request.post("/_test/native-batch", { data: { agent_id: agent.agent_id, action: "release" } });
    await expect(page.locator("#change-undo-all")).toBeEnabled();
    await expect(page.locator("#change-cancel")).toBeDisabled();
    await expect(page.locator("#change-history-prev")).toBeEnabled();
    await expect(page.locator(".native-batch-result")).toHaveAttribute("data-batch-id", oldBatch!);
  } finally {
    await request.post("/_test/native-batch", { data: { agent_id: agent.agent_id, action: "release" } });
    await destroy(request, agent);
  }
});

test("关闭页迟到响应不能覆盖重开后的权限与草稿", async ({ page, request }) => {
  test.setTimeout(60_000);
  const agent = await createAgent(request, "native-reopen-race");
  let release: (() => void) | undefined;
  try {
    await setupFiles(request, agent, 1); await connect(page, agent); await select(page, agent);
    await page.locator("#change-batch-query").fill("authority-query-draft");
    let entered: (() => void) | undefined, delayed = false;
    const paused = new Promise<void>(resolve => { release = resolve; });
    const captured = new Promise<void>(resolve => { entered = resolve; });
    await page.route(`**/v1/agents/${agent.agent_id}/workspace/native-changes?*`, async route => {
      const response = await route.fetch();
      if (!delayed) { delayed = true; entered!(); await paused; }
      await route.fulfill({ response });
    });
    const card = page.locator(`article[data-agent-id="${agent.agent_id}"]`);
    await card.getByRole("button", { name: "关闭会话", exact: true }).click();
    await captured;
    await card.getByRole("button", { name: "重开会话", exact: true }).click();
    await expect(page.locator("#change-keep-all")).toBeEnabled();
    release!();
    await expect(page.locator("#change-keep-all")).toBeEnabled();
    await expect(page.locator("#change-batch-query")).toHaveValue("authority-query-draft");
    await expect(page.locator("#change-items .change-title")).toHaveText("native-reopen-race-0.txt");
    await page.unrouteAll({ behavior: "wait" });
  } finally { release?.(); await destroy(request, agent); }
});

test("POST响应挂起时切走再切回原source，返回后自动解除发送锁", async ({ page, request }) => {
  const first = await createAgent(request, "native-return-first"), second = await createAgent(request, "native-return-second");
  let release: (() => void) | undefined;
  try {
    await setupFiles(request, first, 1); await setupFiles(request, second, 1);
    await connect(page, first); await select(page, first);
    let entered: (() => void) | undefined, starts = 0;
    const paused = new Promise<void>(resolve => { release = resolve; });
    const captured = new Promise<void>(resolve => { entered = resolve; });
    await page.route(`**/v1/agents/${first.agent_id}/workspace/native-batches?confirmed=true`, async route => {
      starts++; const response = await route.fetch(); entered!(); await paused; await route.fulfill({ response });
    });
    await confirm(page, "#change-keep-all"); await captured;
    await select(page, second); await select(page, first);
    await expect(page.locator("#change-refresh")).toBeDisabled();
    release!();
    await expect(page.locator("#change-refresh")).toBeEnabled();
    await expect(page.locator("#change-undo-all")).toBeEnabled();
    await expect(page.locator(".native-batch-result")).toContainText("文件操作已确认");
    expect(starts).toBe(1);
  } finally { release?.(); await destroy(request, first); await destroy(request, second); }
});
