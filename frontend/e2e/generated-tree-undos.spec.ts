import { destroyAgent } from "./session-fixtures.js";
import { expect, test, type Locator, type Page } from "@playwright/test";

const headers = { Authorization: "Bearer browser-test-key" };
const rootTools = ["generated_tree_undo_preview", "generated_tree_undo_change", "generated_tree_undo_query", "generated_tree_undo_reconcile", "generated_tree_undo_cancel"];
const sourceTools = ["tree_exchange_inspect", "tree_exchange_seal", "tree_exchange_seal_query", "workspace_tree_query"];
const generation = ["workspace_read", "workspace_preview", "workspace_change", "workspace_reconcile", "workspace_finalize", "command_execute", "command_get", "command_changes", "command_propose", "directory_source_preview", "directory_source_query", "directory_query", "directory_change", "directory_reconcile", "directory_finalize", "workspace_tree_prepare", "workspace_tree_start"];
const tools = [...rootTools, ...sourceTools, ...generation];
type Agent = { id: string; topic: string; panel: Locator };
test.setTimeout(90_000);

async function control(page: Page, id: string, action: string, path = "tree", plan?: string) {
  const response = await page.request.post("/_test/generated-root", { headers, data: { action, path, agent_id: id, plan_id: plan } });
  expect(response.ok(), await response.text()).toBeTruthy(); return response.json();
}
async function moveControl(page: Page, id: string, action: string) {
  const response = await page.request.post("/_test/tree-move", { headers, data: { action, agent_id: id } });
  expect(response.ok()).toBeTruthy(); return response.json();
}
async function connect(page: Page, topic: string): Promise<void> {
  await page.goto(`/ui/?topic=${encodeURIComponent(topic)}`);
  await page.getByLabel("服务访问密钥", { exact: true }).fill("browser-test-key");
  await page.getByRole("button", { name: "连接", exact: true }).click(); await expect(page.locator("#connection")).toHaveClass(/ready/);
}
async function open(page: Page, id: string) {
  const panel = page.locator(`article.agent-card[data-agent-id="${id}"] .generated-tree-undos`);
  await panel.getByText("原生成根撤销与历史", { exact: true }).click();
  await expect(panel).toHaveAttribute("aria-busy", "false"); return panel;
}
async function actor(page: Page, name: string, mode = "agent", selectedTools = tools, workspace = "edit"): Promise<Agent> {
  const response = await page.request.put(`/v1/agents/profiles/${name}`, { headers, data: { expected_revision: 0, config: {
    name, description: "实际原生成根审阅", workspace_key: workspace, tool_names: selectedTools, mode,
    max_steps: 16, tool_timeout_seconds: 30, timeout_seconds: 120,
    ...(selectedTools.includes("command_execute") ? { execution_key: "sandbox" } : {}),
    context_config: { tokenizer: "cl100k_base", reserved_output_tokens: 128 },
  } } }); expect(response.ok(), await response.text()).toBeTruthy();
  const spawned = await page.request.post("/v1/agents", { headers, data: { name } }); expect(spawned.ok()).toBeTruthy();
  const view = await spawned.json(); await connect(page, view.topic_id);
  return { id: view.agent_id, topic: view.topic_id, panel: await open(page, view.agent_id) };
}
async function destroy(page: Page, id: string) { await destroyAgent(page, id); }
async function generate(page: Page, id: string, root: string) {
  const base = `/v1/agents/${id}`;
  const command = await page.request.post(`${base}/commands?confirmed=true`, { headers, data: {
    request_key: `source-${root}`, command: { argv: ["/usr/bin/python3", "-I", "-c", `import os;root=${JSON.stringify(root)};os.makedirs(root+'/sub');os.chmod(root,0o750);os.chmod(root+'/sub',0o700);open(root+'/sub/data.bin','wb').write(bytes([0,255]))`] },
  } }); expect(command.ok(), await command.text()).toBeTruthy(); const original = await command.json();
  const waited = await page.request.post(`${base}/commands/${original.command_id}/wait?timeout_seconds=30`, { headers });
  expect(waited.ok(), await waited.text()).toBeTruthy(); expect((await waited.json()).outcome).toBe("completed");
  const catalog = await page.request.get(`${base}/commands/${original.command_id}/directory-changes`, { headers }); expect(catalog.ok()).toBeTruthy();
  const prepared = await page.request.post(`${base}/workspace/tree-plans?confirmed=true`, { headers, data: {
    command_id: original.command_id, source_hash: (await catalog.json()).source_hash, root_path: root, request_key: `plan-${root}`,
  } }); expect(prepared.ok(), await prepared.text()).toBeTruthy(); const plan = await prepared.json();
  const started = await page.request.post(`${base}/workspace/tree-plans/${plan.plan_id}/start?confirmed=true`, { headers }); expect(started.ok()).toBeTruthy();
  const finished = await page.request.post(`${base}/workspace/tree-plans/${plan.plan_id}/wait?timeout_seconds=30`, { headers });
  expect(finished.ok(), await finished.text()).toBeTruthy(); expect((await finished.json()).status).toBe("done");
  return plan.plan_id as string;
}
async function source(panel: Locator, plan: string, preserve: string, previous = "") {
  await panel.getByLabel("原完成生成计划标识", { exact: true }).fill(plan);
  await panel.getByLabel("生成根保留相对路径", { exact: true }).fill(preserve);
  await panel.getByLabel("上次实际还原操作标识（再次撤销时填写）", { exact: true }).fill(previous);
  await panel.getByRole("button", { name: "查询原完成生成树", exact: true }).click();
  await expect(panel.locator(".generated-root-summary")).toContainText(plan); await expect(panel).toHaveAttribute("aria-busy", "false");
}
async function confirm(page: Page, panel: Locator, label: string, accept = true) {
  let message = ""; let done!: () => void; const completed = new Promise<void>(resolve => { done = resolve; });
  page.once("dialog", dialog => { message = dialog.message(); void (accept ? dialog.accept() : dialog.dismiss()).finally(done); });
  await panel.getByRole("button", { name: label, exact: true }).click(); await completed; return message;
}
async function prepare(page: Page, panel: Locator, plan: string, preserve: string) {
  await source(panel, plan, preserve); await confirm(page, panel, "封存待撤销的完整树");
  await expect(panel.getByRole("button", { name: "预览生成根撤销", exact: true })).toBeEnabled();
  await panel.getByRole("button", { name: "预览生成根撤销", exact: true }).click();
  await expect(panel.locator(".generated-root-detail > strong")).toContainText("生成根撤销待确认");
}

test("真实原生成封存移除保留还原、二进制inode与Locale窄视口", async ({ page }) => {
  const root = "root-normal", retained = "root-normal-retained"; const { id, panel } = await actor(page, "root-normal");
  try {
    const plan = await generate(page, id, root), original = await control(page, id, "stat", root);
    await prepare(page, panel, plan, retained);
    const proposal = (await page.request.get(`/v1/agents/${id}/workspace/generated-tree-undos/query?kind=proposals`, { headers })).json();
    const fingerprint = (await proposal).proposals.items[0].move.before_fingerprint;
    await expect(panel.locator(".generated-root-condition")).toContainText(fingerprint);
    await expect(panel.locator(".generated-root-condition")).toContainText("原生成状态: 树已完成");
    await confirm(page, panel, "移除原生成根", false); expect(await control(page, id, "counts")).toMatchObject({ proposals: 1, operations: 0 });
    const message = await confirm(page, panel, "移除原生成根"); expect(message).toContain(plan); expect(message).toContain(fingerprint);
    await expect(panel.locator(".generated-root-detail > strong")).toContainText("生成根已移至保留位置");
    expect(await control(page, id, "stat", root)).toMatchObject({ found: false }); expect(await control(page, id, "stat", retained)).toMatchObject(original);
    await confirm(page, panel, "确认保留生成根"); await expect(panel.locator(".generated-root-detail > strong")).toContainText("生成根保留已确认");
    await expect(panel.locator(".generated-root-receipt")).toContainText("未产生工作区变化");
    await page.locator("#locale-select").selectOption("en-US");
    await expect(panel.getByLabel("Generated root preservation relative path", { exact: true })).toHaveValue(retained);
    await expect(panel.locator(".generated-root-summary")).toContainText("Current generated root not inspected");
    await expect(panel.getByRole("button", { name: "Restore original generated root", exact: true })).toBeEnabled();
    await page.locator("#locale-select").selectOption("zh-CN");
    await confirm(page, panel, "还原原生成根"); await expect(panel.locator(".generated-root-detail > strong")).toContainText("生成根已还原");
    await expect(panel.locator(".generated-root-summary")).toContainText("当前生成根尚未检查");
    expect(await control(page, id, "stat", root)).toMatchObject(original); expect(await control(page, id, "stat", retained)).toMatchObject({ found: false });
    expect(await control(page, id, "counts")).toMatchObject({ plans: ["done"], seals: 1, proposals: 1, operations: 3 });
    await expect(panel.locator(".generated-root-history li")).toHaveCount(3);
    await panel.screenshot({ path: test.info().outputPath("root-undo-complete.png") });
    await page.setViewportSize({ width: 420, height: 860 }); await panel.locator(".generated-root-detail > strong").scrollIntoViewIfNeeded();
    await page.screenshot({ path: test.info().outputPath("root-undo-narrow.png") }); expect(await panel.evaluate(node => node.scrollWidth <= node.clientWidth + 2)).toBe(true);
    const history = await page.request.get(`/v1/agents/${id}/workspace/generated-tree-undos/query?kind=history&identifier=${(await proposal).proposals.items[0].move.proposal_id}`, { headers });
    const restored = (await history.json()).history.items.find((item: { action: string }) => item.action === "restore_root");
    await source(panel, plan, retained, restored.operation.operation_id);
    await expect(panel.getByRole("button", { name: "预览生成根撤销", exact: true })).toBeEnabled();
    await panel.getByRole("button", { name: "预览生成根撤销", exact: true }).click();
    await expect(panel.locator(".generated-root-condition")).toContainText(restored.operation.operation_id);
    await confirm(page, panel, "取消生成根操作"); await expect(panel.locator(".generated-root-detail > strong")).toContainText("生成根操作已取消");
  } finally { await destroy(page, id); }
});

test("丢封存预览动作回程、重载仅GET原键且0第二操作", async ({ page }) => {
  const root = "root-lost", retained = "root-lost-retained"; const { id, topic, panel } = await actor(page, "root-lost");
  let seals = 0, previews = 0, changes = 0;
  try {
    const plan = await generate(page, id, root); await source(panel, plan, retained);
    await page.route("**/workspace/tree-exchanges/seal?**", async route => { seals++; await route.fetch(); await route.abort("failed"); });
    await confirm(page, panel, "封存待撤销的完整树"); await expect(panel.locator(".generated-root-pending")).toContainText("生成根响应尚未确认");
    await connect(page, topic); let recovered = await open(page, id);
    await expect(recovered.locator(".generated-root-summary")).toContainText(plan); expect(seals).toBe(1);
    await source(recovered, plan, retained); await page.unroute("**/workspace/tree-exchanges/seal?**");
    await page.route("**/workspace/generated-tree-undos/preview?**", async route => { previews++; await route.fetch(); await route.abort("failed"); });
    await recovered.getByRole("button", { name: "预览生成根撤销", exact: true }).click();
    await expect(recovered.locator(".generated-root-pending")).toContainText("生成根响应尚未确认");
    await recovered.getByRole("button", { name: "查询待确认生成根请求", exact: true }).click();
    await expect(recovered.locator(".generated-root-detail > strong")).toContainText("生成根撤销待确认");
    await page.unroute("**/workspace/generated-tree-undos/preview?**");
    await page.route("**/workspace/generated-tree-undos/change?**", async route => { changes++; await route.fetch(); await route.abort("failed"); });
    await confirm(page, recovered, "移除原生成根"); await expect(recovered.locator(".generated-root-pending")).toContainText("生成根响应尚未确认");
    await connect(page, topic); recovered = await open(page, id);
    await expect(recovered.locator(".generated-root-detail > strong")).toContainText("生成根已移至保留位置");
    expect([seals, previews, changes]).toEqual([1, 1, 1]); expect(await control(page, id, "counts")).toMatchObject({ seals: 1, proposals: 1, operations: 1, plans: ["done"] });
  } finally { await destroy(page, id); }
});

test("真实效果回执失败unknown、丢核实回复仅GET原operation", async ({ page }) => {
  const root = "root-unknown", retained = "root-unknown-retained"; const { id, topic, panel } = await actor(page, "root-unknown");
  let reconciles = 0;
  try {
    const plan = await generate(page, id, root), original = await control(page, id, "stat", root); await prepare(page, panel, plan, retained);
    await moveControl(page, id, "fail_save"); await confirm(page, panel, "移除原生成根");
    await expect(panel.locator(".generated-root-detail > strong")).toContainText("生成根结果待核实");
    expect(await control(page, id, "stat", retained)).toMatchObject(original);
    const operation = await panel.locator(".generated-root-history li").first().getAttribute("data-operation-id");
    await page.route("**/workspace/generated-tree-undo-operations/*/reconcile?**", async route => { reconciles++; await route.fetch(); await route.abort("failed"); });
    await confirm(page, panel, "核实原生成根结果"); await expect(panel.locator(".generated-root-pending")).toContainText(operation!);
    await connect(page, topic); const recovered = await open(page, id);
    await expect(recovered.locator(".generated-root-detail > strong")).toContainText("生成根已移至保留位置");
    await expect(recovered.locator(".generated-root-receipt")).toContainText(operation!);
    expect(reconciles).toBe(1); expect(await control(page, id, "counts")).toMatchObject({ proposals: 1, operations: 1 });
  } finally { await destroy(page, id); }
});

test("原动作等待中取消、同线程收束且原树未移动", async ({ page }) => {
  const root = "root-cancel", retained = "root-cancel-retained"; const { id, panel } = await actor(page, "root-cancel");
  try {
    const plan = await generate(page, id, root), original = await control(page, id, "stat", root); await prepare(page, panel, plan, retained);
    await moveControl(page, id, "pause"); await confirm(page, panel, "移除原生成根");
    await expect.poll(async () => (await moveControl(page, id, "gate")).entered).toBe(true);
    await expect(panel.getByRole("button", { name: "取消进行中的生成根操作", exact: true })).toBeEnabled();
    await confirm(page, panel, "取消进行中的生成根操作"); await expect.poll(async () => (await moveControl(page, id, "counts")).cancelled).toBe(1);
    await moveControl(page, id, "release"); await expect(panel.locator(".generated-root-detail > strong")).toContainText("生成根操作已取消");
    await expect(panel.locator(".generated-root-receipt")).toContainText("已请求取消生成根操作");
    expect(await control(page, id, "stat", root)).toMatchObject(original); expect(await control(page, id, "counts")).toMatchObject({ proposals: 1, operations: 1 });
  } finally { await moveControl(page, id, "release"); await destroy(page, id); }
});

test("规范保留位置与actual restore前驱、外部版本改变拒绝", async ({ page }) => {
  const root = "root-version", retained = "root-version-retained"; const { id, panel } = await actor(page, "root-version");
  try {
    const plan = await generate(page, id, root); await source(panel, plan, retained); await confirm(page, panel, "封存待撤销的完整树");
    for (const value of ["../escape", root, `${root}/child`, "x\\child", "/absolute", "a//b", "a/./b", "a:b", "é".repeat(257)]) {
      await panel.getByLabel("生成根保留相对路径", { exact: true }).fill(value);
      await expect(panel.getByRole("button", { name: "预览生成根撤销", exact: true, includeHidden: true })).toBeDisabled();
    }
    await panel.getByLabel("生成根保留相对路径", { exact: true }).fill(retained);
    await panel.getByLabel("上次实际还原操作标识（再次撤销时填写）", { exact: true }).fill("not-an-actual-restore");
    await expect(panel.getByRole("button", { name: "预览生成根撤销", exact: true })).toBeDisabled();
    await panel.getByLabel("上次实际还原操作标识（再次撤销时填写）", { exact: true }).fill("");
    await panel.getByRole("button", { name: "预览生成根撤销", exact: true }).click(); await expect(panel.locator(".generated-root-detail > strong")).toContainText("生成根撤销待确认");
    await control(page, id, "mutate", root); await confirm(page, panel, "移除原生成根");
    await expect(panel.locator(".generated-root-receipt")).toContainText("生成根操作已拒绝");
    expect(await control(page, id, "stat", root)).toMatchObject({ found: true, binary_hex: "6368616e676564" });
    expect(await control(page, id, "stat", retained)).toMatchObject({ found: false });
    expect(await control(page, id, "counts")).toMatchObject({ proposals: 1, operations: 1, plans: ["done"] });
  } finally { await destroy(page, id); }
});

test("只读plan ask缺工具与关闭边界不提升权限", async ({ page }) => {
  const refused = await page.request.put("/v1/agents/profiles/root-readonly-refused", { headers, data: { expected_revision: 0, config: { name: "root-readonly-refused", description: "只读资源明确拒绝写工具", workspace_key: "demo", tool_names: rootTools } } });
  expect(refused.ok()).toBe(false); expect(await refused.text()).toContain("workspace_read_only");
  for (const [name, mode, selected, workspace] of [
    ["root-readonly", "agent", ["generated_tree_undo_query", "tree_exchange_seal_query", "tree_exchange_inspect", "workspace_tree_query"], "demo"],
    ["root-plan", "plan", [...rootTools, ...sourceTools], "edit"],
    ["root-ask", "ask", [...rootTools, ...sourceTools], "edit"],
    ["root-restricted", "agent", ["tree_exchange_inspect"], "edit"],
  ] as const) {
    const { id, panel } = await actor(page, name, mode, [...selected], workspace);
    try {
      await expect(panel.getByRole("button", { name: "预览生成根撤销", exact: true, includeHidden: true })).toBeDisabled();
      if (name === "root-ask" || name === "root-restricted") await expect(panel.getByRole("button", { name: "刷新生成根历史", exact: true })).toBeDisabled();
      const response = await page.request.post(`/v1/agents/${id}/workspace/generated-tree-undos/change?confirmed=true`, { headers, data: { proposal_id: "unavailable", action: "remove_root", expected_revision: 0, request_key: name } });
      expect(response.ok()).toBe(false); expect(await control(page, id, "counts")).toMatchObject({ operations: 0 });
    } finally { await destroy(page, id); }
  }
  const { id, panel } = await actor(page, "root-closed");
  try {
    const plan = await generate(page, id, "root-closed"); await prepare(page, panel, plan, "root-closed-retained");
    await page.locator(`article.agent-card[data-agent-id="${id}"]`).getByRole("button", { name: "关闭会话", exact: true }).click();
    await expect(panel.getByRole("button", { name: "移除原生成根", exact: true })).toBeDisabled();
    await expect(panel.getByRole("button", { name: "刷新生成根历史", exact: true })).toBeEnabled();
    await panel.getByRole("button", { name: "刷新生成根历史", exact: true }).click(); await expect(panel.locator(".generated-root-detail > strong")).toContainText("生成根撤销待确认");
    expect(await control(page, id, "counts")).toMatchObject({ operations: 0 });
  } finally { await destroy(page, id); }
});

test("本地意图写入失败与损坏记录阻止POST", async ({ page }) => {
  const root = "root-storage"; const { id, topic, panel } = await actor(page, "root-storage");
  let previews = 0;
  try {
    const plan = await generate(page, id, root); await source(panel, plan, "root-storage-retained"); await confirm(page, panel, "封存待撤销的完整树");
    await expect(panel.getByRole("button", { name: "预览生成根撤销", exact: true })).toBeEnabled();
    await page.route("**/workspace/generated-tree-undos/preview?**", async route => { previews++; await route.continue(); });
    await page.evaluate(() => { const original = Storage.prototype.setItem; Storage.prototype.setItem = function(key, value) { if (key.startsWith("krim-generated-root-recovery:")) throw new DOMException("synthetic quota", "QuotaExceededError"); original.call(this, key, value); }; });
    await panel.getByRole("button", { name: "预览生成根撤销", exact: true }).click();
    await expect(panel.locator(".generated-root-pending")).toContainText("生成根恢复记录不可用，写操作已停用"); expect(previews).toBe(0);
    expect(await control(page, id, "counts")).toMatchObject({ proposals: 0, operations: 0 });
    // 损坏状态由可信测试设置，重载后产品仍须拒绝写；不假造服务端事实。
    await connect(page, topic); await page.evaluate(id => localStorage.setItem(`krim-generated-root-recovery:${id}`, "not-json"), id);
    await connect(page, topic); const recovered = await open(page, id);
    await expect(recovered.locator(".generated-root-pending")).toContainText("生成根恢复记录不可用，写操作已停用"); expect(previews).toBe(0);
  } finally { await destroy(page, id); }
});

test("POST未达服务端时保留原键、连续GET不换键重做", async ({ page }) => {
  const root = "root-not-sent"; const { id, topic, panel } = await actor(page, "root-not-sent"); let previews = 0;
  try {
    const plan = await generate(page, id, root); await source(panel, plan, "root-not-sent-retained"); await confirm(page, panel, "封存待撤销的完整树");
    await page.route("**/workspace/generated-tree-undos/preview?**", async route => { previews++; await route.abort("failed"); });
    await panel.getByRole("button", { name: "预览生成根撤销", exact: true }).click(); await expect(panel.locator(".generated-root-pending code")).toHaveCount(1);
    const key = await panel.locator(".generated-root-pending code").textContent();
    await panel.getByRole("button", { name: "查询待确认生成根请求", exact: true }).click();
    await expect(panel.locator(".generated-root-pending")).toContainText("未找到原树请求，请保留原键继续查询");
    await connect(page, topic); const recovered = await open(page, id);
    await expect(recovered.locator(".generated-root-pending code")).toHaveText(key!);
    await recovered.getByRole("button", { name: "查询待确认生成根请求", exact: true }).click();
    await expect(recovered.getByRole("button", { name: "预览生成根撤销", exact: true })).toBeDisabled();
    expect(previews).toBe(1); expect(await control(page, id, "counts")).toMatchObject({ proposals: 0, operations: 0, plans: ["done"] });
  } finally { await destroy(page, id); }
});

test("合法Schema错误来源Scope资源状态与分页回程拒绝", async ({ page }) => {
  const root = "root-response"; const { id, panel } = await actor(page, "root-response"); let changes = 0;
  try {
    const plan = await generate(page, id, root); await prepare(page, panel, plan, "root-response-retained");
    await page.route("**/workspace/generated-tree-undos/change?**", async route => { changes++; await route.continue(); });
    for (const mutate of [
      (value: any) => { value.proposal.move.scope_id = "session:another-agent"; },
      (value: any) => { value.proposal.move.resource_fingerprint = "0".repeat(64); },
      (value: any) => { value.proposal.source_plan_id = "different-original-plan"; },
      (value: any) => { value.proposal.source_hash = "0".repeat(64); },
      (value: any) => { value.proposal.root_state = "preserved"; },
      (value: any) => { value.proposal.sealed_fingerprint = "0".repeat(64); },
      (value: any) => { value.proposal.move.proposal_id = "different-original"; },
      (value: any) => { value.history = { items: [], next_after: null }; },
    ]) {
      await page.route("**/workspace/generated-tree-undos/query?kind=proposal&**", async route => { const response = await route.fetch(), value = await response.json(); mutate(value); await route.fulfill({ response, json: value }); });
      await panel.getByRole("button", { name: "刷新生成根历史", exact: true }).click(); await expect(panel).toHaveAttribute("aria-busy", "false");
      await expect(panel.locator(".generated-root-status")).toContainText("生成根回程与原事实不一致");
      await expect(panel.getByRole("button", { name: "移除原生成根", exact: true })).toBeDisabled();
      await page.unroute("**/workspace/generated-tree-undos/query?kind=proposal&**");
      await panel.getByRole("button", { name: "刷新生成根历史", exact: true }).click(); await expect(panel.getByRole("button", { name: "移除原生成根", exact: true })).toBeEnabled();
    }
    await page.route("**/workspace/generated-tree-undos/query?kind=proposals&**", async route => { const response = await route.fetch(), value = await response.json(); value.proposals.next_after = "invalid-cursor"; await route.fulfill({ response, json: value }); });
    await panel.getByRole("button", { name: "刷新生成根历史", exact: true }).click(); await expect(panel.locator(".generated-root-status")).toContainText("生成根回程与原事实不一致");
    expect(changes).toBe(0); expect(await control(page, id, "counts")).toMatchObject({ proposals: 1, operations: 0 });
  } finally { await destroy(page, id); }
});

test("稳定提案与操作分页、HTML保留路径纯文本无注入", async ({ page }) => {
  // 21个提案和21次独立原工具操作的浏览器总监督窗口；不改产品5秒物理预算。
  test.setTimeout(180_000);
  const root = "root-pages", html = "<img src=x onerror=window.rootInjected=1>"; const { id, panel } = await actor(page, "root-pages");
  try {
    const plan = await generate(page, id, root); await prepare(page, panel, plan, html);
    await expect(panel.locator(".generated-root-condition")).toContainText(html); await expect(panel.locator("img")).toHaveCount(0);
    expect(await page.evaluate(() => (window as unknown as { rootInjected?: number }).rootInjected)).toBeUndefined();
    await control(page, id, "catalog", root, plan); await panel.getByRole("button", { name: "刷新生成根历史", exact: true }).click();
    await expect(panel.locator(".generated-root-list li")).toHaveCount(20); await panel.getByRole("button", { name: "下一页生成根提案", exact: true }).click();
    await expect(panel.locator(".generated-root-list li")).toHaveCount(2); await panel.getByRole("button", { name: "上一页生成根提案", exact: true }).click();
    await expect(panel.locator(".generated-root-list li")).toHaveCount(20);
    await confirm(page, panel, "移除原生成根"); await expect(panel.locator(".generated-root-detail > strong")).toContainText("生成根已移至保留位置");
    expect((await control(page, id, "stat", html)).binary_hex).toBe("00ff");
    const identifier = await panel.locator(".generated-root-detail .generated-root-history li").first().getAttribute("data-operation-id");
    const query = await page.request.get(`/v1/agents/${id}/workspace/generated-tree-undos/query?kind=operation&identifier=${identifier}`, { headers });
    const proposal = (await query.json()).operation.operation.proposal_id;
    for (let position = 0; position < 20; position++) {
      const fact = await page.request.get(`/v1/agents/${id}/workspace/generated-tree-undos/query?kind=proposal&identifier=${proposal}`, { headers });
      const view = (await fact.json()).proposal;
      await control(page, id, "reject_next");
      const rejected = await page.request.post(`/v1/agents/${id}/workspace/generated-tree-undos/change?confirmed=true`, { headers, data: { proposal_id: proposal, action: "restore_root", expected_revision: view.move.revision, request_key: `rejected-page-${position}` } });
      expect(rejected.ok(), await rejected.text()).toBeTruthy(); const denied = (await rejected.json()).operation;
      expect(denied.status).toBe("rejected"); expect(denied.receipt.error_code).toBe("workspace_tree_move_authority");
    }
    await panel.getByRole("button", { name: "刷新生成根历史", exact: true }).click();
    // 完整历史读回沿现有30秒工具窗口等待；不延长服务端或物理执行预算。
    await expect(panel).toHaveAttribute("aria-busy", "false", { timeout: 30_000 });
    await expect(panel.locator(".generated-root-history li")).toHaveCount(20);
    await panel.getByRole("button", { name: "下一页生成根操作", exact: true }).click(); await expect(panel).toHaveAttribute("aria-busy", "false", { timeout: 30_000 }); await expect(panel.locator(".generated-root-history li")).toHaveCount(1);
    await panel.getByRole("button", { name: "上一页生成根操作", exact: true }).click(); await expect(panel).toHaveAttribute("aria-busy", "false", { timeout: 30_000 }); await expect(panel.locator(".generated-root-history li")).toHaveCount(20);
    expect((await control(page, id, "stat", html)).binary_hex).toBe("00ff");
  } finally { await destroy(page, id); }
});

test("离线Model原Run-call事实按键与重载冷读0新Provider-Writer", async ({ page }) => {
  const root = "root-model", retained = "root-model-retained"; const { id, topic, panel } = await actor(page, "root-model");
  try {
    const plan = await generate(page, id, root), original = await control(page, id, "stat", root);
    await source(panel, plan, retained); await confirm(page, panel, "封存待撤销的完整树");
    await expect(panel.getByRole("button", { name: "预览生成根撤销", exact: true })).toBeEnabled();
    const sealed = await page.request.get(`/v1/agents/${id}/workspace/tree-exchanges/seals/${plan}`, { headers }); const fingerprint = (await sealed.json()).origin.tree_fingerprint;
    const produced = await page.request.post("/_test/model-generated-root", { headers, data: { agent_id: id, request: { plan_id: plan, preserve_path: retained, expected_fingerprint: fingerprint, request_key: "root-model-original" } } });
    expect(produced.ok(), await produced.text()).toBeTruthy(); const fact = await produced.json(); expect(fact.provider_calls).toBe(4);
    await panel.getByLabel("生成根原请求键", { exact: true }).fill(fact.preview_key); await panel.getByRole("button", { name: "按键查看生成根提案", exact: true }).click();
    await expect(panel.locator(".generated-root-detail > strong")).toContainText("生成根已移至保留位置");
    await panel.getByLabel("生成根原请求键", { exact: true }).fill(fact.change_key); await panel.getByRole("button", { name: "按键查看生成根操作", exact: true }).click();
    await expect(panel.locator(".generated-root-receipt")).toContainText("树操作已确认");
    await connect(page, topic); const recovered = await open(page, id); await expect(recovered.locator(".generated-root-detail > strong")).toContainText("生成根已移至保留位置");
    expect(await control(page, id, "stat", retained)).toMatchObject(original);
    expect(await control(page, id, "counts")).toMatchObject({ provider_calls: 4, sources: 2, proposals: 1, operations: 1, plans: ["done"] });
  } finally { await destroy(page, id); }
});
