import { expect, test as base, type APIRequestContext, type Page } from "@playwright/test";

const headers = { Authorization: "Bearer browser-test-key" };
const tools = ["workspace_read", "workspace_list", "workspace_preview", "workspace_change", "workspace_reconcile", "workspace_finalize", "directory_inspect", "directory_preview", "directory_change", "directory_query", "directory_reconcile", "directory_finalize"];
const test = base.extend({ request: async ({ playwright, baseURL }, use) => {
  const context = await playwright.request.newContext({ baseURL, extraHTTPHeaders: headers }); await use(context); await context.dispose();
} });
type Agent = { agent_id: string; topic_id: string; session_id: string; name: string };
type Target = { source: "file" | "directory"; proposal_id: string; expected_revision: number };

async function createAgent(api: APIRequestContext, name: string, mode = "agent"): Promise<Agent> {
  const configured = await api.put(`/v1/agents/profiles/${name}`, { data: { expected_revision: 0, config: { name, description: "混合提案审阅验证", workspace_key: "edit", tool_names: tools, mode } } }); expect(configured.ok()).toBeTruthy();
  const response = await api.post("/v1/agents", { data: { name } }); expect(response.ok()).toBeTruthy(); return response.json();
}
async function setup(api: APIRequestContext, name: string, mode = "agent") {
  const agent = await createAgent(api, name, mode);
  const base = `/v1/agents/${agent.agent_id}/workspace`;
  const file = await api.post(`${base}/changes/preview`, { data: { request_key: "file", spec: { path: `${name}.txt`, operation: "write", expected_sha256: null, content: "mixed new\n" } } });
  const directory = await api.post(`${base}/directory-changes/preview`, { data: { request_key: "directory", spec: { path: `${name}-dir`, operation: "create", mode: 0o750 } } });
  expect(file.ok()).toBeTruthy(); expect(directory.ok()).toBeTruthy();
  return { agent, targets: [{ source: "file", proposal_id: (await file.json()).proposal_id, expected_revision: 0 }, { source: "directory", proposal_id: (await directory.json()).proposal_id, expected_revision: 0 }] as Target[] };
}
async function connect(page: Page, agent: Agent) {
  await page.goto(`/ui/?topic=${agent.topic_id}`); await page.getByLabel("服务访问密钥").fill("browser-test-key");
  await page.getByRole("button", { name: "连接", exact: true }).click(); await expect(page.locator("#connection")).toHaveClass(/ready/);
  await page.getByText("变更清单", { exact: true }).click(); await page.locator("#change-source").selectOption(agent.agent_id);
  await expect(page.locator("#change-mixed-review")).toBeVisible(); await page.getByText("混合提案审阅", { exact: true }).click();
}
async function read(page: Page) { await page.locator("#change-mixed-options-refresh").click(); await expect(page.locator("#change-mixed-options [data-mixed-candidate]")).toHaveCount(2); }
async function select(page: Page, targets: Target[]) { for (const target of targets) await page.locator(`[data-mixed-candidate="${target.source}:${target.proposal_id}"] input`).check(); }
async function confirm(page: Page, action: "apply" | "keep" | "undo", accept = true) {
  let settled!: () => void;
  const handled = new Promise<void>(resolve => { settled = resolve; });
  page.once("dialog", dialog => { void (accept ? dialog.accept() : dialog.dismiss()).finally(settled); });
  await page.locator(`#change-mixed-${action}`).click(); await handled;
}
async function destroy(api: APIRequestContext, agent: Agent) { await api.delete(`/v1/agents/${agent.agent_id}`); }
async function manyFiles(api: APIRequestContext, agent: Agent, count: number): Promise<Target[]> {
  const base = `/v1/agents/${agent.agent_id}/workspace`, targets: Target[] = [];
  for (let index = 0; index < count; index++) {
    const response = await api.post(`${base}/changes/preview`, { data: { request_key: `many-${index}`, spec: { path: `${agent.name}-${index}.txt`, operation: "write", expected_sha256: null, content: `many-${index}\n` } } });
    expect(response.ok()).toBeTruthy(); const value = await response.json(); targets.push({ source: "file", proposal_id: value.proposal_id, expected_revision: 0 });
  }
  const directory = await api.post(`${base}/directory-changes/preview`, { data: { request_key: "many-directory", spec: { path: `${agent.name}-directory`, operation: "create", mode: 0o750 } } });
  expect(directory.ok()).toBeTruthy(); targets.push({ source: "directory", proposal_id: (await directory.json()).proposal_id, expected_revision: 0 }); return targets;
}

test("浏览器显式file+directory确认取消、apply/keep/undo及原修订回程", async ({ page, request }) => {
  const { agent, targets } = await setup(request, "mixed-review-life"); const sent: Array<{ action: string; targets: Target[]; request_key: string }> = [];
  try {
    page.on("request", item => { if (item.method() === "POST" && item.url().endsWith("/mixed-batches?confirmed=true")) sent.push(item.postDataJSON()); });
    await connect(page, agent); await read(page); await select(page, targets);
    await expect(page.locator("#change-mixed-selected")).toContainText("已选择2项");
    await expect(page.locator("#change-mixed-keep")).toBeDisabled();
    await confirm(page, "apply", false); expect(sent).toHaveLength(0);
    for (const [index, action] of (["apply", "keep", "undo"] as const).entries()) {
      if (index) { await read(page); await select(page, targets); }
      await confirm(page, action);
      await expect.poll(async () => {
        const key = sent[index]?.request_key; if (!key) return null;
        const history = await (await request.get(`/v1/agents/${agent.agent_id}/workspace/mixed-batches/history`)).json();
        return history.items.find((item: { request_key: string }) => item.request_key === key)?.status;
      }).toBe("done");
      await page.locator("#change-refresh").click();
      await expect(page.locator(".mixed-batch-result")).toContainText("批次已确认");
      expect(sent[index]!.action).toBe(action);
      expect(sent[index]!.targets).toEqual(targets.map(target => ({ ...target, expected_revision: index * 2 })));
      expect(sent[index]!.request_key).toMatch(/^[a-f0-9-]{36}$/);
      await expect(page.locator("#change-mixed-pending")).toBeHidden();
    }
    const base = `/v1/agents/${agent.agent_id}/workspace`;
    const directory = await request.get(`${base}/directory-changes/${targets[1]!.proposal_id}`); expect((await directory.json()).status).toBe("undone");
    const file = await request.get(`${base}/changes/${targets[0]!.proposal_id}`); expect((await file.json()).status).toBe("undone");
    expect((await (await request.get(`${base}/mixed-batches/history`)).json()).total).toBe(3);
  } finally { await destroy(request, agent); }
});

test("混合批次丢接受响应、重载找回原父与零第二POST", async ({ page, request }) => {
  const { agent, targets } = await setup(request, "mixed-review-lost"); let posts = 0;
  try {
    await connect(page, agent); await read(page); await select(page, targets);
    await page.route(`**/v1/agents/${agent.agent_id}/workspace/mixed-batches?confirmed=true`, async route => { posts++; await route.fetch(); await route.abort("failed"); });
    await confirm(page, "apply"); await expect(page.locator(".mixed-batch-result")).toContainText("批次已确认");
    await connect(page, agent); await expect(page.locator(".mixed-batch-result")).toContainText("批次已确认");
    expect(posts).toBe(1);
    const history = await (await request.get(`/v1/agents/${agent.agent_id}/workspace/mixed-batches/history`)).json(); expect(history.total).toBe(1); expect(history.items[0].done_count).toBe(2);
  } finally { await destroy(request, agent); }
});

test("混合启动前断线与重载只读，明确重试保持同键同参数", async ({ page, request }) => {
  const { agent, targets } = await setup(request, "mixed-review-prestart"); const bodies: unknown[] = [];
  try {
    await connect(page, agent); await read(page); await select(page, targets);
    await page.route(`**/v1/agents/${agent.agent_id}/workspace/mixed-batches?confirmed=true`, async route => { bodies.push(route.request().postDataJSON()); if (bodies.length === 1) await route.abort("failed"); else await route.continue(); });
    await confirm(page, "apply"); await expect(page.locator("#change-mixed-pending")).toBeVisible();
    await expect(page.locator("#change-mixed-apply")).toBeDisabled();
    await connect(page, agent); await expect(page.locator("#change-mixed-pending")).toBeVisible();
    expect(bodies).toHaveLength(1);
    await page.locator("#change-mixed-recover").click(); await expect(page.locator("#change-mixed-retry")).toBeEnabled();
    page.once("dialog", dialog => dialog.accept()); await page.locator("#change-mixed-retry").click();
    await expect(page.locator(".mixed-batch-result")).toContainText("批次已确认"); expect(bodies).toHaveLength(2); expect(bodies[0]).toEqual(bodies[1]);
    expect((await (await request.get(`/v1/agents/${agent.agent_id}/workspace/mixed-batches/history`)).json()).total).toBe(1);
  } finally { await destroy(request, agent); }
});

test("混合原提案版本变化在确认前拒绝，不创建父批次", async ({ page, request }) => {
  const { agent, targets } = await setup(request, "mixed-review-version"); let posts = 0;
  try {
    await connect(page, agent); await read(page); await select(page, targets);
    page.on("request", item => { if (item.method() === "POST" && item.url().endsWith("/mixed-batches?confirmed=true")) posts++; });
    const applied = await request.post(`/v1/agents/${agent.agent_id}/workspace/changes?confirmed=true`, { data: { action: "apply", proposal_id: targets[0]!.proposal_id, expected_revision: 0, request_key: "other-apply" } }); expect((await applied.json()).status).toBe("done");
    await page.locator("#change-mixed-apply").click(); await expect(page.locator("#change-mixed-status")).toContainText("版本或状态已变化");
    expect(posts).toBe(0); expect((await (await request.get(`/v1/agents/${agent.agent_id}/workspace/mixed-batches/history`)).json()).total).toBe(0);
  } finally { await destroy(request, agent); }
});

test("跨文件页选择最多32项，目录与文件来源统一显示且超限不发起", async ({ page, request }) => {
  const value = await createAgent(request, "mixed-review-limit");
  try {
    const targets = await manyFiles(request, value, 32); await connect(page, value); await page.locator("#change-mixed-options-refresh").click();
    await expect(page.locator("#change-mixed-options [data-mixed-candidate]")).toHaveCount(21);
    for (let index = 0; index < 20; index++) await page.locator('[data-mixed-candidate^="file:"] input').nth(index).check();
    await page.locator("#change-mixed-files-next").click();
    await expect(page.locator("#change-mixed-options [data-mixed-candidate]")).toHaveCount(13);
    for (let index = 0; index < 12; index++) await page.locator('[data-mixed-candidate^="file:"] input').nth(index).check();
    expect(await page.locator("#change-mixed-selected").textContent()).toContain("32项");
    await expect(page.locator(`[data-mixed-candidate="directory:${targets[32]!.proposal_id}"] input`)).toBeEnabled();
    await page.locator(`[data-mixed-candidate="directory:${targets[32]!.proposal_id}"] input`).click();
    await expect(page.locator(`[data-mixed-candidate="directory:${targets[32]!.proposal_id}"] input`)).not.toBeChecked();
    await expect(page.locator("#change-mixed-selected")).toContainText("32项");
    await expect(page.locator("#change-mixed-status")).toContainText("最多选择32项");
    await expect(page.locator("#change-mixed-apply")).toBeEnabled();
    expect((await (await request.get(`/v1/agents/${value.agent_id}/workspace/mixed-batches/history`)).json()).total).toBe(0);
  } finally { await destroy(request, value); }
});

test("plan模式和恢复缓存篡改拒绝新的混合写入", async ({ page, request }) => {
  const planned = await setup(request, "mixed-review-plan", "plan");
  try {
    await connect(page, planned.agent); await read(page);
    await expect(page.locator("#change-mixed-options input").first()).toBeDisabled();
    await expect(page.locator("#change-mixed-apply")).toBeDisabled();
  } finally { await destroy(request, planned.agent); }
  const value = await setup(request, "mixed-review-cache");
  try {
    await page.evaluate(({ id }) => sessionStorage.setItem(`krim:mixed-pending:${id}`, JSON.stringify({ version: 9, request: { action: "apply", request_key: "bad", targets: [] }, fingerprint: "bad" })), { id: value.agent.session_id });
    await connect(page, value.agent); await expect(page.locator("#change-mixed-pending")).toBeVisible();
    await expect(page.locator("#change-mixed-clear-cache")).toBeVisible();
    await expect(page.locator("#change-mixed-apply")).toBeDisabled();
    const before = await (await request.get(`/v1/agents/${value.agent.agent_id}/workspace/mixed-batches/history`)).json();
    page.once("dialog", dialog => dialog.accept()); await page.locator("#change-mixed-clear-cache").click();
    await expect(page.locator("#change-mixed-pending")).toBeHidden();
    expect((await (await request.get(`/v1/agents/${value.agent.agent_id}/workspace/mixed-batches/history`)).json()).total).toBe(before.total);
  } finally { await destroy(request, value.agent); }
});

test("混合目录条件失败保留file成功，partial沿真实原attempt且不自报全成功", async ({ page, request }) => {
  const { agent, targets } = await setup(request, "mixed-review-partial");
  try {
    const seeded = await request.post("/_test/directory", { data: { action: "seed", path: `${agent.name}-dir` } }); expect(seeded.ok()).toBeTruthy();
    await connect(page, agent); await read(page); await select(page, targets); await confirm(page, "apply");
    await expect(page.locator(".mixed-batch-result")).toContainText("批次部分完成");
    const history = await (await request.get(`/v1/agents/${agent.agent_id}/workspace/mixed-batches/history`)).json();
    expect(history.items[0].status).toBe("partial"); expect(history.items[0].done_count).toBe(1);
    const batch = await (await request.get(`/v1/agents/${agent.agent_id}/workspace/mixed-batches/${history.items[0].batch_id}`)).json();
    expect(batch.items[0].status).toBe("done"); expect(batch.items[1].status).toBe("rejected");
    expect(batch.items.every((item: { operation: unknown }) => item.operation !== null)).toBe(true);
    await expect(page.locator("#change-mixed-pending")).toBeHidden();
  } finally { await destroy(request, agent); }
});

test("混合相同entry在接受前拒绝，确认无父记录后可修正选择", async ({ page, request }) => {
  const agent = await createAgent(request, "mixed-review-dependency");
  try {
    const base = `/v1/agents/${agent.agent_id}/workspace`, path = `${agent.name}-same`;
    const file = await request.post(`${base}/changes/preview`, { data: { request_key: "file", spec: { path, operation: "write", expected_sha256: null, content: "file\n" } } });
    const directory = await request.post(`${base}/directory-changes/preview`, { data: { request_key: "directory", spec: { path, operation: "create", mode: 0o750 } } });
    expect(file.ok()).toBeTruthy(); expect(directory.ok()).toBeTruthy();
    const targets: Target[] = [{ source: "file", proposal_id: (await file.json()).proposal_id, expected_revision: 0 }, { source: "directory", proposal_id: (await directory.json()).proposal_id, expected_revision: 0 }];
    await connect(page, agent); await read(page); await select(page, targets); await confirm(page, "apply");
    await expect(page.locator("#change-mixed-status")).toContainText("重叠");
    await expect(page.locator("#change-mixed-pending")).toBeHidden();
    expect((await (await request.get(`${base}/mixed-batches/history`)).json()).total).toBe(0);
    await page.locator(`[data-mixed-candidate="directory:${targets[1]!.proposal_id}"] input`).uncheck();
    await expect(page.locator("#change-mixed-apply")).toBeEnabled();
  } finally { await destroy(request, agent); }
});
