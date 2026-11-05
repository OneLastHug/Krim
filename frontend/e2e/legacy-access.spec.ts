import { expect, test, type Page, type Request } from "@playwright/test";
import { attachJsonEvidence, screenshotStep } from "./step-screenshot.js";

type Theme = "bright" | "dark";
type RequestEvidence = { method: string; path: string; body?: unknown };
type MeEnvelope = Record<string, unknown>;

function apiPath(value: string): string {
  return new URL(value).pathname;
}

function watchRequests(page: Page): RequestEvidence[] {
  const records: RequestEvidence[] = [];
  page.on("request", (request: Request) => {
    const bodyText = request.postData();
    let body: unknown = bodyText ?? undefined;
    if (bodyText) {
      try { body = JSON.parse(bodyText); } catch { /* 保留非 JSON 请求体原文。 */ }
    }
    records.push({ method: request.method(), path: apiPath(request.url()), ...(body === undefined ? {} : { body }) });
  });
  return records;
}

function v1Requests(records: RequestEvidence[]): RequestEvidence[] {
  return records.filter(record => record.path === "/v1" || record.path.startsWith("/v1/"));
}

function v1Posts(records: RequestEvidence[]): RequestEvidence[] {
  return v1Requests(records).filter(record => record.method === "POST");
}

function v1Gets(records: RequestEvidence[]): RequestEvidence[] {
  return v1Requests(records).filter(record => record.method === "GET");
}

async function openNavigationSurface(page: Page) {
  const drawer = page.getByRole("dialog", { name: "主导航与对话", exact: true });
  if (await drawer.isVisible()) return drawer;
  const mobileMenu = page.getByRole("button", { name: "打开导航", exact: true });
  if (await mobileMenu.isVisible()) { await mobileMenu.click(); await expect(drawer).toBeVisible(); }
  return drawer;
}

async function selectTheme(page: Page, theme: Theme) {
  const labels = { bright: "浅色", dark: "深色" };
  const drawer = await openNavigationSurface(page);
  const surface = await drawer.isVisible() ? drawer : page.locator(".desktop-sidebar");
  await surface.getByRole("button", { name: "设置", exact: true }).click();
  await page.locator(".settings-popover .theme-options").getByRole("button", { name: new RegExp("^" + labels[theme]) }).click();
  await expect(page.locator("html")).toHaveAttribute("data-theme", theme);
  // 主题选择后恢复抽屉开合状态，避免遮挡后续主区操作。
  if (await drawer.isVisible()) { await page.keyboard.press("Escape"); await expect(drawer).not.toBeVisible(); }
}

async function navigateWorkspace(page: Page, buttonName: string, view: string) {
  const drawer = await openNavigationSurface(page);
  const surface = await drawer.isVisible() ? drawer : page.locator(".desktop-sidebar");
  const button = surface.locator(".sidebar-buttons").getByRole("button", { name: buttonName, exact: true });
  await expect(button).toBeVisible();
  await button.focus();
  await expect(button).toBeFocused();
  await page.keyboard.press("Enter");
  await expect.poll(() => new URL(page.url()).searchParams.get("view")).toBe(view);
}

function stepFor(page: Page, info: Parameters<typeof screenshotStep>[1], caseName: string, theme: Theme) {
  return (stepName: string, action: () => Promise<void>) => screenshotStep(page, info, { caseName, stepName, theme }, action);
}

test("旧版门禁：身份等待与明确非 legacy 不降级", async ({ page }, info) => {
  await page.setViewportSize({ width: 1440, height: 900 });
  const step = stepFor(page, info, "旧版门禁-迟到非legacy", "bright");
  const requests = watchRequests(page);
  await page.addInitScript(() => localStorage.setItem("krim-agent-run", "unread-synthetic-run"));

  let releaseIdentity!: () => void;
  let identityArrived!: () => void;
  let identityFulfilled!: () => void;
  const release = new Promise<void>(resolve => { releaseIdentity = resolve; });
  const arrived = new Promise<void>(resolve => { identityArrived = resolve; });
  const fulfilled = new Promise<void>(resolve => { identityFulfilled = resolve; });
  page.once("close", releaseIdentity);
  let actualMe: MeEnvelope | undefined;
  let syntheticMe: MeEnvelope | undefined;

  await page.route(url => url.pathname === "/v2/me", async route => {
    const response = await route.fetch();
    actualMe = await response.json() as MeEnvelope;
    identityArrived();
    await release;
    syntheticMe = { ...actualMe, legacy: false };
    await route.fulfill({ status: response.status(), contentType: "application/json", body: JSON.stringify(syntheticMe) });
    identityFulfilled();
  });

  await step("身份响应延迟时不读取旧版 API", async () => {
    await page.goto("/ui/?view=run", { waitUntil: "domcontentloaded" });
    await arrived;
    await selectTheme(page, "bright");
    await expect(page.locator(".sidebar-account .account-sub")).toContainText("读取身份");
    await expect(page.locator("#prompt")).toHaveCount(0);
    expect(v1Requests(requests)).toHaveLength(0);
  });

  await step("实际 me 包络仅改为非 legacy", async () => {
    releaseIdentity();
    await fulfilled;
    await expect(page.getByRole("heading", { name: "运行记录暂不可用", exact: true })).toBeVisible();
    expect(actualMe).toBeTruthy();
    expect(syntheticMe).toBeTruthy();
    expect(actualMe!.legacy).toBe(true);
    expect(syntheticMe).toEqual({ ...actualMe!, legacy: false });
    const actualWithoutLegacy = { ...actualMe! };
    const syntheticWithoutLegacy = { ...syntheticMe! };
    delete actualWithoutLegacy.legacy;
    delete syntheticWithoutLegacy.legacy;
    expect(syntheticWithoutLegacy).toEqual(actualWithoutLegacy);
    await expect(page.locator("#prompt")).toHaveCount(0);
    expect(v1Requests(requests)).toHaveLength(0);
    await attachJsonEvidence(info, "me实际与合成非legacy包络.json", {
      source: "实际 GET /v2/me 由 route.fetch() 获取；只将 legacy 改为 false",
      actual: actualMe,
      synthetic: syntheticMe,
      clientRequests: requests,
    });
  });
});

test("旧版门禁：手机深色身份 503 保留意图且不降级", async ({ page }, info) => {
  await page.setViewportSize({ width: 375, height: 812 });
  await page.emulateMedia({ colorScheme: "dark" });
  const step = stepFor(page, info, "旧版门禁-手机身份503", "dark");
  const requests = watchRequests(page);
  const retained = { intent: JSON.stringify({ text: "保留原输入", request_key: "preserve-intent-sentinel", target: "demo" }), run: "unread-synthetic-run" };
  await page.addInitScript(values => {
    localStorage.setItem("krim-agent-intent", values.intent);
    localStorage.setItem("krim-agent-run", values.run);
  }, retained);

  await page.route(url => url.pathname === "/v2/me", route => route.fulfill({
    status: 503,
    contentType: "application/json",
    body: JSON.stringify({ error: { code: "execution_error" } }),
  }));
  const identityFailure = page.waitForResponse(response => apiPath(response.url()) === "/v2/me");

  await step("身份服务 503 时不触发旧版降级", async () => {
    await page.goto("/ui/?view=run");
    const response = await identityFailure;
    const failureBody = await response.json();
    expect(response.status()).toBe(503);
    expect(failureBody).toEqual({ error: { code: "execution_error" } });
    await expect(page.locator("#prompt")).toHaveCount(0);
    expect(v1Requests(requests)).toHaveLength(0);
    expect(v1Posts(requests)).toHaveLength(0);
    await attachJsonEvidence(info, "手机身份503未降级证据.json", {
      injectedIdentityFailure: { method: "GET", path: "/v2/me", status: response.status(), body: failureBody },
      clientRequests: requests,
    });
  });

  await step("深色主题下保留原 intent 与 Run 选择", async () => {
    await selectTheme(page, "dark");
    const storage = await page.evaluate(() => ({
      intent: localStorage.getItem("krim-agent-intent"),
      run: localStorage.getItem("krim-agent-run"),
    }));
    expect(storage).toEqual(retained);
    await expect(page.locator("#prompt")).toHaveCount(0);
    expect(v1Requests(requests)).toHaveLength(0);
    expect(v1Posts(requests)).toHaveLength(0);
    await attachJsonEvidence(info, "手机503后保留状态与请求.json", { storage, clientRequests: requests });
  });
});

test("旧版门禁：真实 legacy 运行、离页只读与返回重读", async ({ page }, info) => {
  await page.setViewportSize({ width: 1440, height: 900 });
  await page.emulateMedia({ colorScheme: "light" });
  const step = stepFor(page, info, "旧版门禁-真实legacy运行", "bright");
  const requests = watchRequests(page);
  const identityResponse = page.waitForResponse(response => apiPath(response.url()) === "/v2/me");
  let runId = "";
  let actualMe: MeEnvelope;

  await step("真实 legacy 身份进入浅色运行页", async () => {
    await page.goto("/ui/?view=run");
    const response = await identityResponse;
    expect(response.ok()).toBe(true);
    actualMe = await response.json() as MeEnvelope;
    expect(actualMe.legacy).toBe(true);
    await selectTheme(page, "bright");
    await expect(page.locator("#prompt")).toBeVisible();
    await expect(page.getByRole("heading", { name: "运行记录", level: 1 })).toBeVisible();
    await attachJsonEvidence(info, "运行页实际 legacy 身份.json", { identityStatus: response.status(), me: actualMe, clientRequests: requests });
  });

  await step("通过 UI 提交离线 echo 并保存真实结果", async () => {
    await page.locator("#prompt").fill("/echo gate-result-local");
    const submittedResponse = page.waitForResponse(response =>
      apiPath(response.url()) === "/v1/runs" && response.request().method() === "POST",
    );
    await page.locator("#send").click();
    const response = await submittedResponse;
    expect(response.ok()).toBe(true);
    await expect(page.locator("#status")).toHaveText("已完成");
    await expect(page.locator("#result")).toHaveText("gate-result-local");
    runId = (await page.locator("#run-id").textContent())!.trim();
    expect(runId).not.toBe("");

    const post = v1Posts(requests).find(record => record.path === "/v1/runs");
    expect(post).toBeTruthy();
    expect(post!.body).toMatchObject({ text: "/echo gate-result-local" });
    const requestKey = (post!.body as Record<string, unknown>).request_key;
    expect(typeof requestKey).toBe("string");
    expect(requestKey).not.toBe("");
    expect(v1Posts(requests)).toHaveLength(1);

    const accepted = await response.json();
    const resultResponse = await page.request.get(`/v1/runs/${encodeURIComponent(runId)}/result`);
    expect(resultResponse.ok()).toBe(true);
    const result = await resultResponse.json();
    expect(result.run.status).toBe("completed");
    expect(result.text).toBe("gate-result-local");
    await attachJsonEvidence(info, "实际 Run 请求与离线 echo 结果.json", {
      providerScope: "本机 ui_server 的离线 FakeProvider；未调用外部 Agent 或模型",
      request: post,
      acceptedResponse: accepted,
      resultResponse: result,
      ui: { runId, status: await page.locator("#status").textContent(), result: await page.locator("#result").textContent() },
    });
  });

  await step("进入设置期间没有新增 v1 请求", async () => {
    const before = v1Requests(requests).length;
    const postsBefore = v1Posts(requests).length;
    await page.getByRole("button", { name: "设置", exact: true }).click();
    await page.getByRole("menuitem", { name: "全部设置…", exact: true }).click();
    await expect.poll(() => new URL(page.url()).searchParams.get("view")).toBe("settings");
    await expect(page.getByRole("heading", { name: "设置", level: 1 })).toBeVisible();
    const httpTargetCapability = page.getByRole("status", { name: "HTTP 目标能力", exact: true });
    await expect(httpTargetCapability).toHaveText("未启用 HTTP 目标");
    await page.waitForTimeout(250);
    expect(v1Requests(requests)).toHaveLength(before);
    expect(v1Posts(requests)).toHaveLength(postsBefore);
    await attachJsonEvidence(info, "设置页 v1 请求增量.json", {
      httpTargetCapability: await httpTargetCapability.textContent(),
      before,
      after: v1Requests(requests).length,
      postsBefore,
      postsAfter: v1Posts(requests).length,
      clientRequests: requests,
    });
  });

  await step("正常返回运行页只重新 GET 不重发 POST", async () => {
    const getsBefore = v1Gets(requests).length;
    const postsBefore = v1Posts(requests).length;
    await navigateWorkspace(page, "导航到运行记录", "run");
    await expect(page.locator("#prompt")).toBeVisible();
    await expect(page.locator("#run-id")).toHaveText(runId);
    await expect(page.locator("#result")).toHaveText("gate-result-local");
    await expect.poll(() => v1Gets(requests).length).toBeGreaterThan(getsBefore);
    expect(v1Posts(requests)).toHaveLength(postsBefore);
    await attachJsonEvidence(info, "返回运行页重读且无重复提交.json", {
      getsBefore,
      getsAfter: v1Gets(requests).length,
      postsBefore,
      postsAfter: v1Posts(requests).length,
      clientRequests: requests,
    });
  });
});
