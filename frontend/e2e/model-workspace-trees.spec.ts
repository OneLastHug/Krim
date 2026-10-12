import { expect, test, type Page } from "@playwright/test";

const headers = { Authorization: "Bearer browser-test-key" };
const names = ["workspace_read", "workspace_preview", "workspace_change", "workspace_reconcile", "workspace_finalize",
  "command_execute", "command_get", "command_changes", "command_propose", "directory_source_preview", "directory_source_query",
  "directory_query", "directory_change", "directory_reconcile", "directory_finalize", "workspace_tree_prepare",
  "workspace_tree_model_prepare", "workspace_tree_start", "workspace_tree_query"];
type Agent = { agent_id: string; topic_id: string };
test.setTimeout(90_000);

async function create(page: Page, name: string): Promise<Agent> {
  const response = await page.request.put(`/v1/agents/profiles/${name}`, { headers, data: {
    expected_revision: 0, config: { name, description: "浏览器原模型目录树", workspace_key: "edit", execution_key: "sandbox",
      tool_names: names, tool_timeout_seconds: 30, timeout_seconds: 120,
      context_config: { tokenizer: "cl100k_base", reserved_output_tokens: 128 }, usage_budget: { max_total_tokens: 100000 } },
  } }); expect(response.ok()).toBeTruthy();
  const spawned = await page.request.post("/v1/agents", { headers, data: { name } }); expect(spawned.ok()).toBeTruthy(); return spawned.json();
}
async function source(page: Page, agent: Agent, root: string) {
  const response = await page.request.post(`/v1/agents/${agent.agent_id}/commands?confirmed=true`, { headers, data: {
    request_key: "source", command: { argv: ["/usr/bin/python3", "-I", "-c",
      `import os;os.makedirs('${root}/sub');os.chmod('${root}',0o750);os.chmod('${root}/sub',0o700);open('${root}/sub/data.bin','wb').write(bytes([0,255]))`] },
  } }); expect(response.ok()).toBeTruthy(); const command = await response.json();
  const done = await page.request.post(`/v1/agents/${agent.agent_id}/commands/${command.command_id}/wait?timeout_seconds=30`, { headers });
  expect(done.ok()).toBeTruthy(); expect((await done.json()).outcome).toBe("completed");
  const catalog = await page.request.get(`/v1/agents/${agent.agent_id}/commands/${command.command_id}/directory-changes`, { headers });
  expect(catalog.ok()).toBeTruthy(); return { command_id: command.command_id, source_hash: (await catalog.json()).source_hash,
    root_path: root, request_key: `model-${root}` };
}
async function connect(page: Page, agent: Agent) {
  await page.goto(`/ui/?topic=${agent.topic_id}`);
  await page.getByLabel("服务访问密钥", { exact: true }).fill("browser-test-key");
  await page.getByRole("button", { name: "连接", exact: true }).click();
  await expect(page.locator("#connection")).toHaveClass(/ready/);
  const card = page.locator(`article.agent-card[data-agent-id="${agent.agent_id}"]`);
  await card.locator(".agent-commands").getByText("命令执行与回执", { exact: true }).click();
  const tree = card.locator(".workspace-trees");
  await tree.getByText("目录树审阅与控制", { exact: true }).click(); return { card, tree };
}
async function read(page: Page, agent: Agent, key: string) {
  const view = await connect(page, agent);
  await view.tree.getByLabel("目录树标识或原请求键", { exact: true }).fill(key);
  await view.tree.getByRole("button", { name: "按原键找回目录树", exact: true }).click(); return view;
}

test("实际模型创建的目录树在浏览器冷读、原操作导航和重载保持零重放", async ({ page }) => {
  const agent = await create(page, "model-tree-browser-done"), root = "model-tree-browser-done";
  let writes = 0;
  page.on("request", request => { if (request.method() === "POST" && request.url().includes("/workspace/tree-plans")) writes++; });
  try {
    const request = await source(page, agent, root);
    const executed = await page.request.post("/_test/model-tree", { headers, data: { agent_id: agent.agent_id, request } });
    expect(executed.ok()).toBeTruthy(); const original = await executed.json();
    expect(original.provider_calls).toBe(4); expect(original.run.status).toBe("completed"); expect(original.active_model_tasks).toBe(0);
    const view = await read(page, agent, request.request_key);
    await expect(view.tree.locator(".tree-detail > strong")).toContainText("目录树已确认");
    const base = `/v1/agents/${agent.agent_id}/workspace/tree-plans/${original.plan.plan_id}`;
    const first = await (await page.request.get(`${base}/steps`, { headers })).json();
    expect(first.items).toHaveLength(3); expect(first.items.every((item: { status: string }) => item.status === "done")).toBe(true);
    await view.tree.locator('.tree-steps [data-position="0"]').getByRole("button", { name: "查看原目录提案", exact: true }).click();
    await expect(view.card.locator(".directory-operation")).toContainText(first.items[0].operation.operation_id);
    await view.tree.locator('.tree-steps [data-position="2"]').getByRole("button", { name: "查看原文件提案", exact: true }).click();
    await expect(view.card.locator(".workspace-operation-result")).toContainText(first.items[2].operation.operation_id);
    const restored = await connect(page, agent);
    await expect(restored.tree.locator(".tree-detail > strong")).toContainText("目录树已确认");
    await restored.tree.getByRole("button", { name: "刷新原目录树", exact: true }).click();
    expect(await (await page.request.get(`${base}/steps`, { headers })).json()).toEqual(first);
    expect(await (await page.request.get(base, { headers })).json()).toEqual(original.plan); expect(writes).toBe(0);
  } finally { await page.request.delete(`/v1/agents/${agent.agent_id}`, { headers }); }
});

test("浏览器取消收束同一实际模型目录树及等待child，后代零效果", async ({ page }) => {
  const agent = await create(page, "model-tree-browser-cancel"), root = "model-tree-browser-cancel";
  let execution: ReturnType<Page["request"]["post"]> | undefined;
  try {
    const request = await source(page, agent, root);
    expect((await page.request.post("/_test/native-batch", { headers, data: { agent_id: agent.agent_id, action: "block" } })).ok()).toBeTruthy();
    execution = page.request.post("/_test/model-tree", { headers, data: { agent_id: agent.agent_id, request } });
    await expect.poll(async () => (await (await page.request.post("/_test/native-batch", { headers, data: { agent_id: agent.agent_id, action: "status" } })).json()).entered).toBe(true);
    const view = await read(page, agent, request.request_key);
    await expect(view.tree.locator(".tree-detail > strong")).toContainText("目录树执行中");
    const handled = page.waitForEvent("dialog").then(dialog => dialog.accept());
    await view.tree.getByRole("button", { name: "取消原目录树", exact: true }).click(); await handled;
    await expect(view.tree.locator(".tree-detail > strong")).toContainText("目录树已取消");
    const result = await execution; expect(result.ok()).toBeTruthy(); const data = await result.json();
    expect(["failed", "aborted"]).toContain(data.run.status); expect(data.provider_calls).toBe(2); expect(data.active_model_tasks).toBe(0);
    const steps = await (await page.request.get(`/v1/agents/${agent.agent_id}/workspace/tree-plans/${data.plan.plan_id}/steps`, { headers })).json();
    expect(steps.items.every((item: { operation: unknown; status: string }) => item.operation === null && item.status === "skipped")).toBe(true);
    const state = await page.request.post("/_test/directory", { headers, data: { action: "stat", path: root } });
    expect((await state.json()).found).toBe(false);
  } finally {
    await page.request.post("/_test/native-batch", { headers, data: { agent_id: agent.agent_id, action: "release" } });
    if (execution) await execution.catch(() => {});
    await page.request.delete(`/v1/agents/${agent.agent_id}`, { headers });
  }
});
