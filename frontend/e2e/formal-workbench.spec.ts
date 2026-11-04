import { randomUUID } from "node:crypto";
import { expect, test, type Page, type TestInfo } from "@playwright/test";

const baseURL = process.env.KRIM_FORMAL_BASE_URL ?? "http://127.0.0.1:8879";
const origin = new URL(baseURL).origin;
const ADMIN_KEY = "formal-workbench-admin-browser-key";
const OPERATOR_KEY = "formal-workbench-operator-browser-key";
const VIEWER_KEY = "formal-workbench-viewer-browser-key";
const MEMBER_KEY = "formal-workbench-member-browser-key";
const VIEWER_SESSION_TITLE = "formal-workbench viewer seed";

type BrowserObservations = {
  pageErrors: string[];
  apiRequests: string[];
  crossOriginRequests: string[];
  crossOriginWebSockets: string[];
};
type ApiResult = { status: number; body: unknown };
type SessionTurn = { sessionId: string; turnId: string; turnPath: string };

const observations = new WeakMap<Page, BrowserObservations>();

function observePage(page: Page) {
  const state: BrowserObservations = {
    pageErrors: [],
    apiRequests: [],
    crossOriginRequests: [],
    crossOriginWebSockets: [],
  };
  observations.set(page, state);
  page.on("pageerror", error => state.pageErrors.push(error.message));
  page.on("request", request => {
    const url = new URL(request.url());
    if (url.origin !== origin) state.crossOriginRequests.push(`${request.resourceType()} ${request.url()}`);
    if (request.resourceType() === "fetch" || request.resourceType() === "xhr") {
      state.apiRequests.push(`${request.method()} ${url.pathname}${url.search}`);
    }
  });
  page.on("websocket", socket => {
    if (new URL(socket.url()).origin !== origin) state.crossOriginWebSockets.push(socket.url());
  });
}

test.beforeEach(async ({ page }) => {
  observePage(page);
});

test.afterEach(async ({ page }, testInfo) => {
  const state = observations.get(page)!;
  await testInfo.attach("formal-browser-observations.json", {
    body: Buffer.from(JSON.stringify(state, null, 2)),
    contentType: "application/json",
  });
  expect(state.pageErrors, "uncaught page errors").toEqual([]);
  expect(state.crossOriginRequests, "cross-origin HTTP requests").toEqual([]);
  expect(state.crossOriginWebSockets, "cross-origin WebSocket connections").toEqual([]);
});

async function apiRequest(
  page: Page,
  key: string,
  path: string,
  method = "GET",
  body: unknown | null = null,
): Promise<ApiResult> {
  return page.evaluate(async ({ key, path, method, body }) => {
    const headers: Record<string, string> = {};
    if (key) headers.Authorization = `Bearer ${key}`;
    if (body !== null) headers["Content-Type"] = "application/json";
    const response = await fetch(path, {
      method,
      headers,
      ...(body !== null ? { body: JSON.stringify(body) } : {}),
    });
    return { status: response.status, body: await response.json() as unknown };
  }, { key, path, method, body });
}

async function connect(page: Page, key: string) {
  await page.goto("/ui/", { waitUntil: "domcontentloaded" });
  await expect(page.locator(".workbench-shell")).toBeVisible();
  await page.locator(".access-connect").click();
  await page.locator("#api-key").fill(key);
  await page.locator("#connect-save").click();
  await expect(page.locator(".topbar-status")).toContainText("已连接");
  await expect(page.locator(".new-conversation-button")).toBeEnabled();
}

async function openUnconnected(page: Page) {
  await page.goto("/ui/", { waitUntil: "domcontentloaded" });
  await expect(page.locator(".workbench-shell")).toBeVisible();
}

async function createConversation(page: Page, prefix: string) {
  const title = `${prefix}-${randomUUID().slice(0, 8)}`;
  await page.getByRole("button", { name: "新建对话" }).click();
  await page.locator("#session-title").fill(title);
  await expect(page.locator("#chat-target")).toHaveValue("demo");
  await page.getByRole("button", { name: "创建对话" }).click();
  await expect(page.locator("#chat-prompt")).toBeVisible();
  await expect(page.locator("#chat-prompt")).toBeEnabled();
  await expect(page.locator(".topbar-title h1")).toHaveText(title);
  return title;
}

async function send(page: Page, text: string) {
  const prompt = page.locator("#chat-prompt");
  await prompt.fill(text);
  await prompt.press("Enter");
  await expect(page.locator(".chat-user-text").last()).toHaveText(text);
}

async function currentSessionTurn(page: Page): Promise<SessionTurn> {
  const sessionId = new URL(page.url()).searchParams.get("session");
  if (!sessionId) throw new Error("当前地址没有正式会话 ID");
  const turn = page.locator(".chat-turn").last();
  await expect(turn).toBeVisible();
  const turnId = await turn.getAttribute("data-turn-id");
  if (!turnId) throw new Error("当前轮次没有正式 Turn ID");
  const turnPath = `/v2/workspaces/local/sessions/${encodeURIComponent(sessionId)}/turns/${encodeURIComponent(turnId)}`;
  return { sessionId, turnId, turnPath };
}

async function toolCounts(page: Page, key: string) {
  const response = await apiRequest(page, key, "/test/tool-counts");
  expect(response.status).toBe(200);
  return response.body as {
    principal_id: string;
    calls: number;
    active: number;
    executions: { name: string; arguments: Record<string, unknown> }[];
    by_name: Record<string, number>;
  };
}

async function waitForTurnStatus(page: Page, key: string, turnPath: string, status: string, timeout = 15_000) {
  await expect.poll(async () => {
    const response = await apiRequest(page, key, turnPath);
    return response.status === 200 ? (response.body as { status: string }).status : `HTTP ${response.status}`;
  }, { timeout, intervals: [100, 200, 300, 500] }).toBe(status);
}

async function getTurnFacts(page: Page, key: string, turnPath: string) {
  const response = await apiRequest(page, key, `${turnPath}/result`);
  expect(response.status).toBe(200);
  return response.body as {
    run: { id: string; status: string };
    text: string;
    tools: {
      call_id: string;
      provider_call_id: string | null;
      name: string;
      arguments: Record<string, unknown>;
      status: string;
      executor_started: boolean | null;
      result_available: boolean | null;
      value: unknown;
      code: string | null;
    }[];
  };
}

async function waitForApproval(page: Page) {
  await expect(page.locator(".chat-approval")).toBeVisible();
  await expect(page.locator(".approval-arguments")).toBeVisible();
}

async function apiApproval(page: Page, key: string, sessionId: string, turnId: string) {
  const response = await apiRequest(
    page,
    key,
    `/v2/workspaces/local/sessions/${encodeURIComponent(sessionId)}/turns/${encodeURIComponent(turnId)}/approvals`,
  );
  expect(response.status).toBe(200);
  const approvals = response.body as Record<string, unknown>[];
  expect(approvals).toHaveLength(1);
  return approvals[0]!;
}

function approvalReply(approval: Record<string, unknown>, requestKey: string, allow: boolean) {
  return {
    request_key: requestKey,
    call_id: approval.call_id,
    arguments_hash: approval.arguments_hash,
    tool_hash: approval.tool_hash,
    allow,
  };
}

async function expectFormalDesktopGeometry(page: Page) {
  const geometry = await page.evaluate(() => {
    const rect = (selector: string) => {
      const element = document.querySelector<HTMLElement>(selector);
      if (!element) throw new Error(`缺少几何验收元素：${selector}`);
      const { x, y, width, height } = element.getBoundingClientRect();
      return { x, y, width, height };
    };
    return {
      viewportWidth: window.innerWidth,
      globalbar: rect(".workbench-globalbar"),
      sidebar: rect(".desktop-sidebar"),
      main: rect(".workbench-main"),
    };
  });
  const withinOnePixel = (actual: number, expected: number, label: string) => {
    expect(Math.abs(actual - expected), label).toBeLessThanOrEqual(1);
  };
  withinOnePixel(geometry.globalbar.x, 0, "全局横栏 x");
  withinOnePixel(geometry.globalbar.y, 0, "全局横栏 y");
  withinOnePixel(geometry.globalbar.width, geometry.viewportWidth, "全局横栏宽度");
  withinOnePixel(geometry.globalbar.height, 36, "全局横栏高度");
  withinOnePixel(geometry.sidebar.x, 8, "桌面侧栏 x");
  withinOnePixel(geometry.sidebar.y, 48, "桌面侧栏 y");
  withinOnePixel(geometry.sidebar.width, 288, "桌面侧栏宽度");
  withinOnePixel(geometry.main.x, 304, "工作区主栏 x");
  withinOnePixel(geometry.main.y, 36, "工作区主栏 y");
}

async function attachScreenshot(page: Page, testInfo: TestInfo, name: string) {
  await testInfo.attach(name, {
    body: await page.screenshot({ type: "png", animations: "disabled" }),
    contentType: "image/png",
  });
}

test("正式对话使用共享字体与 Markdown，并把图片降为安全链接", async ({ page }, testInfo) => {
  await openUnconnected(page);
  const unauthenticated = await apiRequest(page, "", "/test/tool-counts");
  expect(unauthenticated.status).toBe(401);

  await page.locator(".access-connect").click();
  await page.locator("#api-key").fill(ADMIN_KEY);
  await page.locator("#connect-save").click();
  await expect(page.locator(".topbar-status")).toContainText("已连接");
  await expectFormalDesktopGeometry(page);
  await createConversation(page, "formal-markdown");
  await send(page, "/markdown");

  const answer = page.locator(".chat-answer").last();
  await expect(answer.locator("h1")).toHaveText("正式工作台 Markdown 验证");
  await expect(answer.locator(".message-table-scroll table th")).toHaveCount(2);
  await expect(answer.locator(".message-table-scroll table tbody tr")).toHaveCount(2);
  await expect(answer.locator(".message-code-block pre code")).toContainText("formal-workbench");
  await expect(answer.locator("img, script")).toHaveCount(0);
  await expect(answer.locator('a[href^="javascript:"]')).toHaveCount(0);
  const imageLink = answer.locator("a").filter({ hasText: "外部图片（只显示链接）" });
  await expect(imageLink).toHaveAttribute("href", "https://images.example.invalid/formal-workbench.png");

  const fontState = await page.evaluate(async () => {
    await Promise.all([
      document.fonts.load('14px "Geist Variable"', "Krim Agent"),
      document.fonts.load('14px "Noto Sans SC Variable"', "正式工作台字体"),
      document.fonts.load('14px "Geist Mono Variable"', "const value = 1;"),
    ]);
    await document.fonts.ready;
    const body = document.querySelector<HTMLElement>(".message-markdown p")!;
    const code = document.querySelector<HTMLElement>(".message-code-block pre code")!;
    return {
      sans: getComputedStyle(body).fontFamily,
      mono: getComputedStyle(code).fontFamily,
      geistLoaded: document.fonts.check('14px "Geist Variable"'),
      monoLoaded: document.fonts.check('14px "Geist Mono Variable"'),
      scriptRan: (window as Window & { __formalMarkdownScriptRan?: boolean }).__formalMarkdownScriptRan,
    };
  });
  const fontSession = await page.context().newCDPSession(page);
  let renderedBodyFonts: string[] = [];
  try {
    await fontSession.send("DOM.enable");
    await fontSession.send("CSS.enable");
    const { root } = await fontSession.send("DOM.getDocument");
    const { nodeId } = await fontSession.send("DOM.querySelector", {
      nodeId: root.nodeId,
      selector: ".message-markdown p",
    });
    const { fonts } = await fontSession.send("CSS.getPlatformFontsForNode", { nodeId });
    renderedBodyFonts = fonts.filter(font => font.glyphCount > 0).map(font => font.familyName);
  } finally {
    await fontSession.detach();
  }
  expect(fontState.sans).toContain("Geist Variable");
  expect(fontState.mono).toContain("Geist Mono Variable");
  expect(fontState.geistLoaded).toBe(true);
  expect(fontState.monoLoaded).toBe(true);
  expect(renderedBodyFonts.some(family => family.startsWith("Noto Sans SC"))).toBe(true);
  expect(fontState.scriptRan).toBeUndefined();

  await attachScreenshot(page, testInfo, "formal-desktop-open-1440.png");
  const collapse = page.getByRole("button", { name: "收起侧栏" });
  await collapse.click();
  await expect(page.locator(".workbench-shell")).toHaveClass(/sidebar-collapsed/);
  await expect(page.getByRole("button", { name: "展开侧栏" })).toBeVisible();
  await attachScreenshot(page, testInfo, "formal-desktop-collapsed-1440.png");

  await page.setViewportSize({ width: 390, height: 844 });
  const mobile = await page.evaluate(() => {
    const main = document.querySelector<HTMLElement>(".workbench-main")!;
    const bounds = main.getBoundingClientRect();
    return {
      viewportWidth: window.innerWidth,
      documentScrollWidth: document.documentElement.scrollWidth,
      bodyScrollWidth: document.body.scrollWidth,
      main: { x: bounds.x, width: bounds.width, right: bounds.right },
    };
  });
  expect(mobile.documentScrollWidth).toBeLessThanOrEqual(390);
  expect(mobile.bodyScrollWidth).toBeLessThanOrEqual(390);
  expect(mobile.main.x).toBeGreaterThanOrEqual(-1);
  expect(mobile.main.width).toBeGreaterThan(0);
  expect(mobile.main.right).toBeLessThanOrEqual(390 + 1);
  await expect(page.locator("#chat-prompt")).toBeVisible();
  await expect(page.locator("#chat-prompt")).toHaveValue("");
  await expect(page.locator(".workbench-shell")).toHaveClass(/sidebar-collapsed/);
  await attachScreenshot(page, testInfo, "formal-mobile-collapsed-390x844.png");

  await page.setViewportSize({ width: 1440, height: 900 });
  await expect(page.locator(".workbench-shell")).toHaveClass(/sidebar-collapsed/);
  const expand = page.getByRole("button", { name: "展开侧栏" });
  await expect(expand).toBeVisible();
  await attachScreenshot(page, testInfo, "formal-desktop-restored-collapsed-1440.png");
  await expand.click();
  await expect(page.getByRole("button", { name: "收起侧栏" })).toBeVisible();
  await expectFormalDesktopGeometry(page);
  await attachScreenshot(page, testInfo, "formal-desktop-expanded-1440.png");
});

test("operator 可允许真实工具并由 API facts 与执行计数确认", async ({ page }) => {
  await connect(page, OPERATOR_KEY);
  await createConversation(page, "formal-allow");
  const before = (await toolCounts(page, OPERATOR_KEY)).executions.length;
  await send(page, "/note welcome");
  await waitForApproval(page);
  await expect(page.locator(".chat-approval")).toContainText("等待审批");
  await page.locator(".chat-approval").getByRole("button", { name: "允许 lookup_note 本次调用" }).click();

  const { turnPath } = await currentSessionTurn(page);
  await waitForTurnStatus(page, OPERATOR_KEY, turnPath, "completed");
  const facts = await getTurnFacts(page, OPERATOR_KEY, turnPath);
  expect(facts.tools).toHaveLength(1);
  expect(facts.tools[0]).toMatchObject({
    name: "lookup_note",
    arguments: { key: "welcome" },
    status: "ok",
    executor_started: true,
    result_available: true,
    value: "这是需要本次批准的合成演示资料。",
  });
  const counts = await toolCounts(page, OPERATOR_KEY);
  expect(counts.executions).toHaveLength(before + 1);
  expect(counts.executions.at(-1)).toEqual({ name: "lookup_note", arguments: { key: "welcome" } });
});

test("拒绝审批在事实记录中标记为 denied 且不执行工具", async ({ page }) => {
  await connect(page, ADMIN_KEY);
  await createConversation(page, "formal-deny");
  const before = (await toolCounts(page, ADMIN_KEY)).executions.length;
  await send(page, "/note welcome");
  await waitForApproval(page);
  const { turnPath } = await currentSessionTurn(page);
  await page.locator(".chat-approval").getByRole("button", { name: "拒绝 lookup_note 本次调用" }).click();

  await waitForTurnStatus(page, ADMIN_KEY, turnPath, "failed");
  const facts = await getTurnFacts(page, ADMIN_KEY, turnPath);
  expect(facts.tools).toHaveLength(1);
  expect(facts.tools[0]).toMatchObject({ name: "lookup_note", status: "denied", executor_started: false, result_available: false });
  expect(facts.tools[0]!.code).toBe("approval_denied");
  expect((await toolCounts(page, ADMIN_KEY)).executions).toHaveLength(before);
});

test("待审批调用执行前可取消并保留后端取消事实", async ({ page }) => {
  await connect(page, ADMIN_KEY);
  await createConversation(page, "formal-cancel-approval");
  const before = (await toolCounts(page, ADMIN_KEY)).executions.length;
  await send(page, "/note welcome");
  await waitForApproval(page);
  const { turnPath } = await currentSessionTurn(page);
  await page.locator(".chat-turn").last().getByRole("button", { name: "停止此轮" }).click();

  await waitForTurnStatus(page, ADMIN_KEY, turnPath, "cancelled");
  const facts = await getTurnFacts(page, ADMIN_KEY, turnPath);
  expect(facts.tools).toHaveLength(1);
  expect(facts.tools[0]).toMatchObject({ name: "lookup_note", status: "cancelled", executor_started: false, result_available: false });
  expect((await toolCounts(page, ADMIN_KEY)).executions).toHaveLength(before);
});

test("响应丢失后刷新重连可按原 request_key 查找并幂等重放", async ({ page }, testInfo) => {
  await connect(page, ADMIN_KEY);
  await createConversation(page, "formal-recover-approval");
  const before = (await toolCounts(page, ADMIN_KEY)).executions.length;
  await send(page, "/note welcome");
  await waitForApproval(page);
  const current = await currentSessionTurn(page);
  const turnPath = current.turnPath;
  const routePattern = "**/turns/*/approvals/*";
  const lostResponse: {
    value: { path: string; body: Record<string, unknown>; originStatus: number } | null;
    handlerDone: boolean;
  } = { value: null, handlerDone: false };
  await page.route(routePattern, async route => {
    if (route.request().method() !== "POST") return route.continue();
    const body = route.request().postDataJSON() as Record<string, unknown>;
    const response = await route.fetch();
    lostResponse.value = {
      path: new URL(route.request().url()).pathname,
      body,
      originStatus: response.status(),
    };
    await route.abort("failed");
    lostResponse.handlerDone = true;
  });
  await page.locator(".chat-approval").getByRole("button", { name: "允许 lookup_note 本次调用" }).click();
  await expect.poll(() => lostResponse.handlerDone, { timeout: 15_000, intervals: [100, 200, 300, 500] }).toBe(true);
  const committedRequest = lostResponse.value;
  if (!committedRequest) throw new Error("审批请求未到达后端");
  expect(committedRequest.originStatus).toBe(200);
  expect(committedRequest.body.request_key).toEqual(expect.any(String));

  await page.reload({ waitUntil: "domcontentloaded" });
  await expect(page.locator(".workbench-shell")).toBeVisible();
  await page.locator(".access-connect").click();
  await page.locator("#api-key").fill(ADMIN_KEY);
  await page.locator("#connect-save").click();
  await expect(page.locator(".topbar-status")).toContainText("已连接");
  const recover = page.getByRole("button", { name: "查找操作结果" });
  await expect(recover).toBeVisible();
  const lookupPath = `/controls/by-key/${encodeURIComponent(String(committedRequest.body.request_key))}`;
  let lookupStatus: number | null = null;
  page.on("response", response => {
    if (new URL(response.url()).pathname.endsWith(lookupPath)) lookupStatus = response.status();
  });
  await recover.click();
  await expect.poll(() => lookupStatus, { timeout: 10_000 }).toBe(200);
  await expect(page.locator(".chat-pending")).toHaveCount(0);
  await waitForTurnStatus(page, ADMIN_KEY, turnPath, "completed");
  const facts = await getTurnFacts(page, ADMIN_KEY, turnPath);
  expect(facts.tools[0]).toMatchObject({ status: "ok", executor_started: true, arguments: { key: "welcome" } });

  const state = observations.get(page)!;
  expect(state.apiRequests.some(path => path.includes(lookupPath))).toBe(true);
  await page.unroute(routePattern);
  const replay = await apiRequest(page, ADMIN_KEY, committedRequest.path, "POST", committedRequest.body);
  const counts = await toolCounts(page, ADMIN_KEY);
  await testInfo.attach("approval-recovery-evidence.json", {
    body: Buffer.from(JSON.stringify({
      request_key: committedRequest.body.request_key,
      approval_post_status: committedRequest.originStatus,
      lookup_path: lookupPath,
      lookup_status: lookupStatus,
      replay_status: replay.status,
      executions_before: before,
      executions_after_replay: counts.executions.length,
    }, null, 2)),
    contentType: "application/json",
  });
  expect(replay.status).toBe(200);
  expect((replay.body as { request_key: string; status: string }).request_key).toBe(committedRequest.body.request_key);
  expect(counts.executions).toHaveLength(before + 1);
});

test("同名工具的调用 ID 与参数保持独立并分别审批", async ({ page }) => {
  await connect(page, ADMIN_KEY);
  await createConversation(page, "formal-two-notes");
  const before = (await toolCounts(page, ADMIN_KEY)).executions.length;
  await send(page, "/two-notes");
  await waitForApproval(page);
  const current = await currentSessionTurn(page);
  const eventsResponse = await apiRequest(page, ADMIN_KEY, `${current.turnPath}/events?from_seq=0`);
  expect(eventsResponse.status).toBe(200);
  const events = eventsResponse.body as { kind: string; data: Record<string, unknown> }[];
  const modelEnd = [...events].reverse().find(event => event.kind === "model_end");
  expect(modelEnd).toBeDefined();
  const calls = modelEnd!.data.tool_calls as { kind: string; id: string; name: string; arguments: Record<string, unknown> }[];
  expect(calls).toEqual([
    { kind: "tool_call", id: "formal-provider-note-welcome", name: "lookup_note", arguments: { key: "welcome" } },
    { kind: "tool_call", id: "formal-provider-note-agent", name: "lookup_note", arguments: { key: "agent" } },
  ]);
  expect(new Set(calls.map(call => call.id)).size).toBe(2);
  await expect(page.locator(".chat-approval")).toHaveCount(1);
  await expect(page.locator('.chat-tool[data-call-id="formal-provider-note-agent"]')).toHaveAttribute("data-state", "preparing");
  await expect(page.locator(".approval-arguments")).toContainText('"welcome"');

  await page.locator(".chat-approval").getByRole("button", { name: "允许 lookup_note 本次调用" }).click();
  await expect(page.locator(".chat-approval")).toBeVisible();
  await expect(page.locator(".approval-arguments")).toContainText('"agent"');
  const firstCompletedTool = page.locator('.chat-tool[data-state="ok"]').first();
  await expect(firstCompletedTool).toBeVisible();
  await expect(firstCompletedTool.locator(".chat-tool-body")).toContainText('"welcome"');
  await page.locator(".chat-approval").getByRole("button", { name: "允许 lookup_note 本次调用" }).click();

  await waitForTurnStatus(page, ADMIN_KEY, current.turnPath, "completed");
  const facts = await getTurnFacts(page, ADMIN_KEY, current.turnPath);
  expect(facts.tools).toHaveLength(2);
  expect(facts.tools.map(tool => tool.provider_call_id)).toEqual(calls.map(call => call.id));
  expect(facts.tools.map(tool => tool.arguments)).toEqual([{ key: "welcome" }, { key: "agent" }]);
  expect((await toolCounts(page, ADMIN_KEY)).executions).toHaveLength(before + 2);
});

test("运行中的会话可排队，停止当前轮后队列继续完成", async ({ page }) => {
  await connect(page, ADMIN_KEY);
  await createConversation(page, "formal-queue-stop");
  const before = (await toolCounts(page, ADMIN_KEY)).executions.length;
  await send(page, "/partial");
  await expect(page.locator(".chat-answer .message-code-block pre code")).toContainText("partial_value = 42");
  const first = await currentSessionTurn(page);

  await send(page, "queued after cancellation");
  await expect(page.locator(".chat-turn")).toHaveCount(2);
  const sessionId = first.sessionId;
  const turnsPath = `/v2/workspaces/local/sessions/${encodeURIComponent(sessionId)}/turns?offset=0&limit=20`;
  await expect.poll(async () => {
    const response = await apiRequest(page, ADMIN_KEY, turnsPath);
    const turns = response.body as { status: string }[];
    return turns.map(turn => turn.status);
  }).toEqual(["running", "queued"]);

  await page.locator(".chat-turn").first().getByRole("button", { name: "停止此轮" }).click();
  await expect.poll(async () => {
    const response = await apiRequest(page, ADMIN_KEY, turnsPath);
    const turns = response.body as { status: string }[];
    return turns.map(turn => turn.status);
  }, { timeout: 20_000, intervals: [100, 200, 300, 500] }).toEqual(["cancelled", "completed"]);
  await expect(page.locator(".chat-answer").last()).toContainText("离线脚本回显：queued after cancellation");
  const firstFacts = await getTurnFacts(page, ADMIN_KEY, first.turnPath);
  expect(firstFacts.run.status).toBe("aborted");
  const counts = await toolCounts(page, ADMIN_KEY);
  expect(counts.executions).toHaveLength(before);
  expect(counts.active).toBe(0);
});

test("viewer 与无审批权限 member 被 UI/API 门禁拦截", async ({ page }) => {
  await connect(page, ADMIN_KEY);
  await createConversation(page, "formal-approval-gates");
  const before = (await toolCounts(page, ADMIN_KEY)).executions.length;
  await send(page, "/note welcome");
  await waitForApproval(page);
  const current = await currentSessionTurn(page);
  const approval = await apiApproval(page, ADMIN_KEY, current.sessionId, current.turnId);
  const approvalPath = `${current.turnPath}/approvals/${encodeURIComponent(String(approval.request_id))}`;
  const viewerPost = await apiRequest(page, VIEWER_KEY, approvalPath, "POST", approvalReply(approval, "viewer-direct-denied", true));
  const memberPost = await apiRequest(page, MEMBER_KEY, approvalPath, "POST", approvalReply(approval, "member-direct-denied", true));
  expect(viewerPost.status).toBe(403);
  expect(memberPost.status).toBe(403);

  await page.goto("/ui/?view=chat&workspace=local", { waitUntil: "domcontentloaded" });
  await page.locator(".access-connect").click();
  await page.locator("#api-key").fill(VIEWER_KEY);
  await page.locator("#connect-save").click();
  await expect(page.locator(".topbar-status")).toContainText("已连接");
  const viewerMe = await apiRequest(page, VIEWER_KEY, "/v2/me");
  expect(viewerMe.status).toBe(200);
  expect((viewerMe.body as { workspaces: { role: string; approve: boolean }[] }).workspaces).toEqual([
    { workspace_id: "local", role: "viewer", approve: false },
  ]);
  await page.getByRole("button", { name: VIEWER_SESSION_TITLE }).click();
  await expect(page.locator("#chat-prompt")).toBeDisabled();
  await expect(page.getByRole("button", { name: "新建对话" })).toBeDisabled();
  await expect(page.locator(".chat-approval")).toHaveCount(0);

  await page.goto("/ui/?view=chat&workspace=local", { waitUntil: "domcontentloaded" });
  await page.locator(".access-connect").click();
  await page.locator("#api-key").fill(MEMBER_KEY);
  await page.locator("#connect-save").click();
  await expect(page.locator(".topbar-status")).toContainText("已连接");
  const memberMe = await apiRequest(page, MEMBER_KEY, "/v2/me");
  expect((memberMe.body as { workspaces: { role: string; approve: boolean }[] }).workspaces).toEqual([
    { workspace_id: "local", role: "member", approve: false },
  ]);
  await expect(page.getByRole("button", { name: "新建对话" })).toBeEnabled();
  await createConversation(page, "formal-member-write");

  const adminDecision = await apiRequest(
    page,
    ADMIN_KEY,
    approvalPath,
    "POST",
    approvalReply(approval, "admin-gate-test-cleanup", false),
  );
  expect(adminDecision.status).toBe(200);
  await waitForTurnStatus(page, ADMIN_KEY, current.turnPath, "failed");
  expect((await toolCounts(page, ADMIN_KEY)).executions).toHaveLength(before);
});

test("审批到期后运行事实记录过期且不执行", async ({ page }) => {
  test.setTimeout(45_000);
  await connect(page, ADMIN_KEY);
  await createConversation(page, "formal-expired-approval");
  const before = (await toolCounts(page, ADMIN_KEY)).executions.length;
  await send(page, "/note welcome");
  await waitForApproval(page);
  const { turnPath } = await currentSessionTurn(page);
  await waitForTurnStatus(page, ADMIN_KEY, turnPath, "failed", 35_000);
  const facts = await getTurnFacts(page, ADMIN_KEY, turnPath);
  expect(facts.tools).toHaveLength(1);
  expect(facts.tools[0]).toMatchObject({ name: "lookup_note", status: "denied", executor_started: false, result_available: false, code: "approval_expired" });
  expect((await toolCounts(page, ADMIN_KEY)).executions).toHaveLength(before);
});
