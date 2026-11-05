import { expect, test, type Page, type TestInfo } from "@playwright/test";
import { attachJsonEvidence, screenshotStep } from "./step-screenshot.js";

const workspace = "local";
const evalBase = `/v4/workspaces/${workspace}/eval`;
const automationBase = `${evalBase}/automation`;
const sessionBase = `/v2/workspaces/${workspace}/sessions`;
let persistedScheduleId = "";
let persistedTaskId = "";
let persistedDatasetName = "";
let readonlyTaskId = "";
let readonlyScheduleId = "";
let readonlyTaskRequestKey = "";
let readonlyDisableKey = "";

type Theme = "warm" | "bright" | "dark";
function step(page: Page, info: TestInfo, caseName: string, theme: Theme = "warm") {
  return (label: string) => (action: () => Promise<void>) => screenshotStep(page, info,
    { caseName, stepName: label, theme }, action);
}

async function selectTheme(page: Page, theme: Theme) {
  const labels: Record<Theme, string> = { warm: "暖纸", bright: "浅色", dark: "深色" };
  const drawer = page.getByRole("dialog", { name: "主导航与对话", exact: true });
  if (!await drawer.isVisible()) {
    const mobileMenu = page.getByRole("button", { name: "打开导航", exact: true });
    if (await mobileMenu.isVisible()) { await mobileMenu.click(); await expect(drawer).toBeVisible(); }
  }
  const surface = await drawer.isVisible() ? drawer : page.locator(".desktop-sidebar");
  await surface.getByRole("button", { name: "设置", exact: true }).click();
  await page.locator(".settings-popover .theme-options").getByRole("button", { name: new RegExp("^" + labels[theme]) }).click();
  await expect(page.locator("html")).toHaveAttribute("data-theme", theme);
  // 主题选择后恢复抽屉开合状态，避免遮挡后续主区操作。
  if (await drawer.isVisible()) { await page.keyboard.press("Escape"); await expect(drawer).not.toBeVisible(); }
}

async function readJson(page: Page, info: TestInfo, path: string, name: string): Promise<any> {
  const response = await page.request.get(path);
  expect(response.ok(), `${path} 应返回成功，实际 HTTP ${response.status()}`).toBe(true);
  const body = await response.json();
  await attachJsonEvidence(info, name, body);
  return body;
}

type BusinessMethod = { method: string; path: string };

function observeBusinessMethods(page: Page): BusinessMethod[] {
  const requests: BusinessMethod[] = [];
  page.on("request", request => {
    const path = new URL(request.url()).pathname;
    if (path === "/healthz" || /^\/v[1-9]\d*\//.test(path)) {
      requests.push({ method: request.method(), path });
    }
  });
  return requests;
}

async function readOnlyJson(page: Page, info: TestInfo, path: string, name: string, methods: BusinessMethod[]): Promise<any> {
  methods.push({ method: "GET", path });
  return readJson(page, info, path, name);
}

function expectOnlyReadRequests(methods: BusinessMethod[]) {
  expect(methods.length).toBeGreaterThan(0);
  expect(methods.filter(item => item.method !== "GET")).toEqual([]);
}

async function saveActionResponse(
  info: TestInfo,
  name: string,
  request: { postDataJSON: () => unknown },
  response: { ok: () => boolean; status: () => number; json: () => Promise<unknown> },
): Promise<any> {
  expect(response.ok(), `${name} 应返回成功，实际 HTTP ${response.status()}`).toBe(true);
  const body = await response.json();
  await attachJsonEvidence(info, name, {
    request: request.postDataJSON(),
    response: body,
  });
  return body;
}

async function openAutomation(page: Page) {
  const toggle = page.getByRole("button", { name: "持续回归", exact: true });
  await expect(toggle).toBeVisible();
  if (await toggle.getAttribute("aria-pressed") !== "true") await toggle.click();
  await expect(page.locator("#automation-feedback-run")).toBeVisible();
}

async function refreshScheduleListFromUi(page: Page, info: TestInfo, evidenceName: string): Promise<any[]> {
  const listPath = `${automationBase}/schedules?offset=0&limit=20`;
  const requestPromise = page.waitForRequest(request =>
    request.method() === "GET" && request.url().endsWith(listPath),
  );
  await page.getByRole("button", { name: "只读刷新", exact: true }).click();
  const request = await requestPromise;
  const response = await page.waitForResponse(candidate => candidate.request() === request);
  expect(response.ok(), `UI计划列表刷新应成功，实际 HTTP ${response.status()}`).toBe(true);
  const body = await response.json();
  await attachJsonEvidence(info, evidenceName, body);
  return body;
}

async function stopCaptureAfterFailure(page: Page, info: TestInfo, mayBeEnabled: boolean, disableAttempted: boolean) {
  if (!mayBeEnabled) return;
  try {
    await step(page, info, "P4-失败保护")("只读检查并停用本地采集")(async () => {
      await page.setViewportSize({ width: 1440, height: 1000 });
      await page.goto("/ui/?view=eval&workspace=local");
      await page.getByRole("button", { name: "导航到评测", exact: true }).waitFor({ state: "visible" });
      await openAutomation(page);
      const status = await readJson(page, info, `${automationBase}/status`, "失败保护-采集状态.json");
      expect(status.capture.text_capture).toBe(false);
      expect(status.capture.argument_capture).toBe(false);
      if (!status.capture.enabled) return;
      if (disableAttempted) {
        throw new Error("采集停用 POST 已尝试但状态仍启用；按不重发规则停止自动清理。");
      }
      const responsePromise = page.waitForResponse(response =>
        response.url().endsWith(`${automationBase}/capture`) && response.request().method() === "POST",
      );
      const requestPromise = page.waitForRequest(request =>
        request.url().endsWith(`${automationBase}/capture`) && request.method() === "POST",
      );
      await page.getByRole("button", { name: "停用本地采集", exact: true }).click();
      const [request, response] = await Promise.all([requestPromise, responsePromise]);
      await saveActionResponse(info, "失败保护-停用采集.json", request, response);
      const finalStatus = await readJson(page, info, `${automationBase}/status`, "失败保护-采集停用后状态.json");
      expect(finalStatus.capture.enabled).toBe(false);
    });
  } catch (error) {
    console.error("P4 失败保护无法确认停用本地采集", error);
  }
}

const strandedScheduleId = "aaa-p4-regression-1793279949684";

test("P4：UI清理上一轮留下的持久计划", async ({ page }, info) => {
  test.setTimeout(60000);
  await page.setViewportSize({ width: 1440, height: 1000 });
  await page.emulateMedia({ colorScheme: "light" });
  const cleanupStep = step(page, info, "P4-遗留计划UI清理");
  let schedule: any;

  await cleanupStep("读取遗留计划服务端状态与UI列表")(async () => {
    await page.goto("/ui/?view=eval&workspace=local");
    await openAutomation(page);
    const schedules = await readJson(page, info,
      `${automationBase}/schedules?offset=0&limit=50`,
      "遗留计划清理-服务端计划目录.json");
    schedule = schedules.find((item: any) => item.schedule_id === strandedScheduleId);
    expect(schedule).toBeTruthy();
    const detail = await readJson(page, info,
      `${automationBase}/schedules/${encodeURIComponent(strandedScheduleId)}`,
      "遗留计划清理-原始持久详情.json");
    expect(detail.schedule_id).toBe(strandedScheduleId);
    expect(detail.spec.template.dataset.name).toBe("a-p4-1793279949684");
    const row = page.locator(".automation-schedule-list li").filter({ hasText: strandedScheduleId });
    await expect(row).toContainText(detail.enabled ? "已启用" : "已停用");
  });

  if (schedule.enabled) {
    await cleanupStep("通过UI禁用遗留计划并核对控制回执")(async () => {
      const row = page.locator(".automation-schedule-list li").filter({ hasText: strandedScheduleId });
      const detailPath = `${automationBase}/schedules/${encodeURIComponent(strandedScheduleId)}`;
      const disablePath = `${detailPath}/disable`;
      const requestPromise = page.waitForRequest(request =>
        new URL(request.url()).pathname === disablePath && request.method() === "POST",
      );
      const responsePromise = page.waitForResponse(response =>
        new URL(response.url()).pathname === disablePath && response.request().method() === "POST",
      );
      await row.getByRole("button", { name: "禁用计划", exact: true }).click();
      const [request, response] = await Promise.all([requestPromise, responsePromise]);
      const requestBody = request.postDataJSON() as { revision: number; key: string };
      expect(requestBody.revision).toBe(schedule.revision);
      expect(requestBody.key).toBeTruthy();
      const disabled = await saveActionResponse(info, "遗留计划UI禁用-原控制键与回执.json", request, response);
      expect(disabled.schedule_id).toBe(strandedScheduleId);
      expect(disabled.enabled).toBe(false);
      expect(disabled.control_receipt).toEqual({
        key: requestBody.key,
        decision: "disabled",
        expected_revision: schedule.revision,
        revision: schedule.revision + 1,
      });
      const persisted = await readJson(page, info, detailPath, "遗留计划UI禁用-持久control-receipt.json");
      expect(persisted.enabled).toBe(false);
      expect(persisted.control_receipt.key).toBe(requestBody.key);
      expect(persisted.control_receipt.decision).toBe("disabled");
      expect(persisted.control_receipt.expected_revision).toBe(schedule.revision);
    });
  }

  await cleanupStep("只读刷新列表确认计划已停用")(async () => {
    const schedules = await refreshScheduleListFromUi(page, info, "遗留计划UI刷新后计划目录.json");
    const recovered = schedules.find((item: any) => item.schedule_id === strandedScheduleId);
    expect(recovered).toBeTruthy();
    expect(recovered.enabled).toBe(false);
    expect(recovered.control_receipt.decision).toBe("disabled");
    const detail = await readJson(page, info,
      `${automationBase}/schedules/${encodeURIComponent(strandedScheduleId)}`,
      "遗留计划UI刷新后control-receipt.json");
    expect(detail.enabled).toBe(false);
    expect(detail.control_receipt.key).toBe(recovered.control_receipt.key);
    await expect(page.locator(".automation-schedule-list li").filter({ hasText: strandedScheduleId })).toContainText("已停用");
  });
});

test("P4：本机来源反馈、人工确认与持久回归自动化", async ({ page }, info) => {
  test.setTimeout(240000);
  await page.setViewportSize({ width: 1440, height: 1000 });
  await page.emulateMedia({ colorScheme: "light" });
  const desktopStep = step(page, info, "桌面浅色完整旅程");
  const unique = Date.now();
  const sessionTitle = `p4-local-source-${unique}`;
  const datasetName = `a-p4-${unique}`;
  const scheduleId = `aaa-p4-regression-${unique}`;
  const scheduleVersion = "v1";
  let captureMayBeEnabled = false;
  let captureDisableAttempted = false;
  let sourceRunId = "";
  let feedbackKey = "";
  let feedbackCandidateId = "";
  let datasetEnvelopeFingerprint = "";
  let planRequestKey = "";
  let disableKey = "";
  let disabledControlReceipt: any;
  let windowIndex = -1;
  let taskRequestKey = "";
  const importPosts: string[] = [];
  page.on("request", request => {
    if (request.method() === "POST" && new URL(request.url()).pathname.endsWith("/sources/runs/import")) {
      importPosts.push(request.url());
    }
  });

  try {
    await desktopStep("打开评测并核验本机身份与能力")(async () => {
      await page.goto("/ui/?view=eval&workspace=local");
      await expect(page.getByRole("button", { name: "导航到评测", exact: true })).toBeVisible();
      await selectTheme(page, "warm");
      const html = page.locator("html");
      await expect(html).toHaveAttribute("data-theme", "warm");
      const health = await readJson(page, info, "/healthz", "宿主评测能力.json");
      expect(health.product_eval_enabled).toBe(true);
      expect(health.evaluation_automation_enabled).toBe(true);
      const actor = await readJson(page, info, "/v2/me", "本机工作区身份.json");
      const membership = actor.workspaces?.find((item: any) => item.workspace_id === workspace);
      expect(membership?.role).toBe("admin");
      expect(membership?.approve).toBe(true);
      await expect(page.getByRole("button", { name: "持续回归", exact: true })).toBeVisible();
    });

    await desktopStep("进入持续回归并明确启用本地采集")(async () => {
      await openAutomation(page);
      const initial = await readJson(page, info, `${automationBase}/status`, "采集启用前服务端状态.json");
      expect(initial.capture.text_capture).toBe(false);
      expect(initial.capture.argument_capture).toBe(false);
      captureMayBeEnabled = initial.capture.enabled === true;
      if (!initial.capture.enabled) {
        captureMayBeEnabled = true;
        const requestPromise = page.waitForRequest(request =>
          request.url().endsWith(`${automationBase}/capture`) && request.method() === "POST",
        );
        const responsePromise = page.waitForResponse(response =>
          response.url().endsWith(`${automationBase}/capture`) && response.request().method() === "POST",
        );
        await expect(page.getByRole("button", { name: "启用本地采集", exact: true })).toBeEnabled({ timeout: 10000 });
        await page.getByRole("button", { name: "启用本地采集", exact: true }).click();
        const [request, response] = await Promise.all([requestPromise, responsePromise]);
        const receipt = await saveActionResponse(info, "启用本地采集-原请求与回执.json", request, response);
        expect(receipt.capture.enabled).toBe(true);
      }
      const enabled = await readJson(page, info, `${automationBase}/status`, "采集启用后服务端状态.json");
      expect(enabled.capture.enabled).toBe(true);
      expect(enabled.capture.text_capture).toBe(false);
      expect(enabled.capture.argument_capture).toBe(false);
    });

    let session: any;
    await desktopStep("切换对话并新建本机来源会话")(async () => {
      await page.getByRole("button", { name: "导航到对话", exact: true }).click();
      await expect(page.locator(".sidebar-buttons").getByRole("button", { name: "新建对话", exact: true })).toBeEnabled();
      await page.locator(".sidebar-buttons").getByRole("button", { name: "新建对话", exact: true }).click();
      const dialog = page.getByRole("dialog");
      await dialog.locator("#session-title").fill(sessionTitle);
      const requestPromise = page.waitForRequest(request =>
        new URL(request.url()).pathname === sessionBase && request.method() === "POST",
      );
      const responsePromise = page.waitForResponse(response =>
        new URL(response.url()).pathname === sessionBase && response.request().method() === "POST",
      );
      await dialog.getByRole("button", { name: "创建对话", exact: true }).click();
      const [request, response] = await Promise.all([requestPromise, responsePromise]);
      expect(response.status()).toBe(201);
      session = await saveActionResponse(info, "本机来源会话-原请求与回执.json", request, response);
      expect(session.title).toBe(sessionTitle);
      expect(session.workspace_id).toBe(workspace);
      expect(session.target_key).toBe("demo");
      await expect(page.locator("#chat-prompt")).toBeEnabled();
    });

    let acceptedTurn: any;
    await desktopStep("运行合成本机加法并等待完成")(async () => {
      await page.locator("#chat-prompt").fill("/add 2.5 3.5");
      const turnPath = `${sessionBase}/${encodeURIComponent(session.id)}/turns`;
      const requestPromise = page.waitForRequest(request =>
        new URL(request.url()).pathname === turnPath && request.method() === "POST",
      );
      const responsePromise = page.waitForResponse(response =>
        new URL(response.url()).pathname === turnPath && response.request().method() === "POST",
      );
      await page.locator(".chat-compose-actions").getByRole("button", { name: "发送", exact: true }).click();
      const [request, response] = await Promise.all([requestPromise, responsePromise]);
      expect(response.status()).toBe(202);
      acceptedTurn = await saveActionResponse(info, "本机加法轮次-原请求与接收回执.json", request, response);
      expect(acceptedTurn.text).toBe("/add 2.5 3.5");
      await expect.poll(async () => {
        const result = await page.request.get(`${turnPath}/${encodeURIComponent(acceptedTurn.id)}`);
        expect(result.ok()).toBe(true);
        const turn = await result.json();
        expect(turn.id).toBe(acceptedTurn.id);
        expect(turn.session_id).toBe(session.id);
        return turn.status;
      }, { timeout: 45000 }).toBe("completed");
      await expect(page.locator(".chat-answer").last()).toHaveText("6");
      const turns = await readJson(page, info,
        `${sessionBase}/${encodeURIComponent(session.id)}/turns?offset=0&limit=50`,
        "本机加法轮次完成事实.json");
      const completed = turns.find((turn: any) => turn.id === acceptedTurn.id);
      expect(completed?.status).toBe("completed");
      expect(completed?.run_id).toBeTruthy();
      acceptedTurn = completed;
      await expect(page.locator(".chat-turn").last().getByRole("button", { name: "更多操作", exact: true })).toBeVisible();
      await expect(page.getByRole("button", { name: "评测此结果", exact: true })).toHaveCount(0);
    });

    await desktopStep("从对话结果直达来源运行")(async () => {
      const last = page.locator(".chat-turn").last();
      await last.getByRole("button", { name: "更多操作", exact: true }).click();
      const tool = last.locator(".chat-tool").first();
      await expect(tool.locator("summary")).toContainText("add");
      await tool.locator("summary").click();
      await expect(tool.locator(".chat-tool-section pre").first()).toContainText("left");
      await expect(last.locator(".chat-detail")).toBeVisible();
      await last.getByRole("button", { name: "查看 Trace", exact: true }).click();
      await expect(page.locator(".chat-toolbar .toolbar-title h1")).toHaveText("Trace");
      await expect(page.locator(".trace-span-node").first()).toBeVisible();
      await page.getByRole("button", { name: "加入数据集", exact: true }).click();
      await expect.poll(() => new URL(page.url()).searchParams.get("source_run")).toBe(acceptedTurn.run_id);
      sourceRunId = new URL(page.url()).searchParams.get("source_run") ?? "";
      expect(sourceRunId).toBeTruthy();
      const turns = await readJson(page, info,
        `${sessionBase}/${encodeURIComponent(session.id)}/turns?offset=0&limit=50`,
        "来源链接-服务器终态轮次JSON");
      expect(turns.find((turn: any) => turn.id === acceptedTurn.id)?.run_id).toBe(sourceRunId);
      await expect(page.locator("#product-source-run")).toHaveValue(sourceRunId);
    });

    await desktopStep("返回持续回归并确认来源运行自动预填")(async () => {
      await page.getByRole("button", { name: "导航到评测", exact: true }).click();
      await openAutomation(page);
      sourceRunId = new URL(page.url()).searchParams.get("source_run") ?? sourceRunId;
      await expect(page.locator("#automation-feedback-run")).toHaveValue(sourceRunId);
      expect(sourceRunId).toBe(acceptedTurn.run_id);
      const status = await readJson(page, info, `${automationBase}/status`, "来源返回持续回归-服务端采集状态.json");
      expect(status.capture.enabled).toBe(true);
      expect(status.capture.text_capture).toBe(false);
      expect(status.capture.argument_capture).toBe(false);
    });

    let feedbackReceipt: any;
    await desktopStep("明确选择理由并只提交一次来源反馈")(async () => {
      await page.locator("#automation-feedback-reason").selectOption("incorrect");
      const feedbackPath = `${automationBase}/feedback/${encodeURIComponent(sourceRunId)}`;
      const requestPromise = page.waitForRequest(request =>
        new URL(request.url()).pathname === feedbackPath && request.method() === "POST",
      );
      const responsePromise = page.waitForResponse(response =>
        new URL(response.url()).pathname === feedbackPath && response.request().method() === "POST",
      );
      await page.getByRole("button", { name: "提交明确反馈", exact: true }).click();
      const [request, response] = await Promise.all([requestPromise, responsePromise]);
      expect(response.status()).toBe(201);
      const requestBody = request.postDataJSON() as { key: string; reason: string };
      feedbackKey = requestBody.key;
      expect(feedbackKey).toBeTruthy();
      expect(requestBody.reason).toBe("incorrect");
      feedbackReceipt = await saveActionResponse(info, "来源反馈-唯一原键与服务端回执.json", request, response);
      expect(feedbackReceipt.key).toBe(feedbackKey);
      expect(feedbackReceipt.revision).toBeGreaterThan(0);
      const row = page.locator(".automation-recovery-list li").filter({ hasText: sourceRunId }).filter({ hasText: feedbackKey });
      await expect(row).toBeVisible();
    });

    let linkedReceipt: any;
    for (let attempt = 1; attempt <= 5; attempt += 1) {
      await desktopStep(`只读读取原反馈候选-${attempt}`)(async () => {
        const row = page.locator(".automation-recovery-list li").filter({ hasText: sourceRunId }).filter({ hasText: feedbackKey });
        const requestPromise = page.waitForResponse(response =>
          response.request().method() === "GET"
            && response.url().endsWith(`${automationBase}/feedback/${encodeURIComponent(sourceRunId)}/by-key/${encodeURIComponent(feedbackKey)}`),
        );
        await row.getByRole("button", { name: /读取反馈候选/ }).click();
        const response = await requestPromise;
        expect(response.ok(), `原反馈只读 GET 应成功，实际 HTTP ${response.status()}`).toBe(true);
        linkedReceipt = await response.json();
        await attachJsonEvidence(info, `原反馈候选只读回执-${attempt}.json`, linkedReceipt);
        expect(linkedReceipt.key).toBe(feedbackKey);
        expect(linkedReceipt.reason).toBe("incorrect");
        expect(["pending", "linked"]).toContain(linkedReceipt.delivery_status);
        if (linkedReceipt.delivery_status === "linked") {
          expect(linkedReceipt.candidate_ids).toHaveLength(1);
          feedbackCandidateId = linkedReceipt.candidate_ids[0];
          expect(feedbackCandidateId).toBeTruthy();
        }
      });
      if (linkedReceipt?.delivery_status === "linked") break;
      await page.waitForTimeout(1100);
    }
    expect(linkedReceipt?.delivery_status).toBe("linked");
    expect(feedbackCandidateId).toBeTruthy();

    let feedbackCandidate: any;
    await desktopStep("按原反馈候选ID进入人工确认")(async () => {
      const candidatePath = `${evalBase}/candidates/${encodeURIComponent(feedbackCandidateId)}`;
      const candidateResponsePromise = page.waitForResponse(response =>
        response.request().method() === "GET" && new URL(response.url()).pathname === candidatePath,
      );
      const recoveryRow = page.locator(".automation-recovery-list li").filter({ hasText: sourceRunId }).filter({ hasText: feedbackKey });
      const candidateRow = recoveryRow.locator(".automation-candidate-row").filter({ hasText: feedbackCandidateId });
      await candidateRow.getByRole("button", { name: "进入人工确认", exact: true }).click();
      const candidateResponse = await candidateResponsePromise;
      feedbackCandidate = await candidateResponse.json();
      await attachJsonEvidence(info, "反馈候选-原H1候选详情.json", feedbackCandidate);
      expect(feedbackCandidate.candidate_id).toBe(feedbackCandidateId);
      expect(feedbackCandidate.eligibility).not.toBe("ineligible");
      await expect.poll(() => new URL(page.url()).searchParams.get("product_candidate_id")).toBe(feedbackCandidateId);
      expect(new URL(page.url()).searchParams.get("source_run")).toBe(sourceRunId);
      await expect(page.locator("#product-candidate-instruction")).toBeVisible();
      expect(importPosts).toHaveLength(0);
    });

    let confirmation: any;
    let confirmedCandidate: any;
    await desktopStep("填写人工初态参考并确认原候选")(async () => {
      await page.locator("#product-candidate-instruction").fill("/add 2.5 3.5");
      await page.locator("#product-candidate-input").fill("/add 2.5 3.5");
      await page.locator("#product-candidate-expected").fill("6");
      await page.locator("#product-candidate-reason").fill("本机合成算式人工核对：2.5 + 3.5 = 6");
      const candidatePath = `${evalBase}/candidates/${encodeURIComponent(feedbackCandidateId)}`;
      const confirmationPath = `${candidatePath}/confirm`;
      const requestPromise = page.waitForRequest(request =>
        new URL(request.url()).pathname === confirmationPath && request.method() === "POST",
      );
      const responsePromise = page.waitForResponse(response =>
        new URL(response.url()).pathname === confirmationPath && response.request().method() === "POST",
      );
      const candidateRefreshPromise = page.waitForResponse(response =>
        response.request().method() === "GET" && new URL(response.url()).pathname === candidatePath,
      );
      await page.getByRole("button", { name: "确认人工参考", exact: true }).click();
      const [request, response] = await Promise.all([requestPromise, responsePromise]);
      confirmation = await saveActionResponse(info, "人工确认-原候选请求与确认回执.json", request, response);
      expect(confirmation.id).toBeTruthy();
      const confirmationBody = request.postDataJSON() as any;
      expect(confirmationBody.case.instruction).toBe("/add 2.5 3.5");
      expect(confirmationBody.case.files["INPUT.txt"]).toBe("/add 2.5 3.5");
      expect(confirmationBody.case.reference.expected).toBe("6");
      expect(confirmationBody.reason).toBe("本机合成算式人工核对：2.5 + 3.5 = 6");
      const candidateRefresh = await candidateRefreshPromise;
      confirmedCandidate = await candidateRefresh.json();
      await attachJsonEvidence(info, "人工确认后原候选只读事实.json", confirmedCandidate);
      expect(confirmedCandidate.candidate_id).toBe(feedbackCandidateId);
      expect(confirmedCandidate.confirmation_id).toBe(confirmation.id);
      await expect(page.getByRole("button", { name: "冻结连续评测版本", exact: true })).toBeVisible();
    });

    let frozenDataset: any;
    await desktopStep("冻结唯一连续数据集并读取服务端包络")(async () => {
      await page.locator("#product-confirmed-dataset-name").fill(datasetName);
      await page.locator("#product-confirmed-dataset-version").fill("v1");
      const freezePath = `${evalBase}/datasets/confirmed`;
      const requestPromise = page.waitForRequest(request =>
        new URL(request.url()).pathname === freezePath && request.method() === "POST",
      );
      const responsePromise = page.waitForResponse(response =>
        new URL(response.url()).pathname === freezePath && response.request().method() === "POST",
      );
      await page.getByRole("button", { name: "冻结连续评测版本", exact: true }).click();
      const [request, response] = await Promise.all([requestPromise, responsePromise]);
      expect(response.status()).toBe(201);
      frozenDataset = await saveActionResponse(info, "连续数据集冻结-原请求与服务端回执.json", request, response);
      expect(frozenDataset.dataset.name).toBe(datasetName);
      expect(frozenDataset.dataset.version).toBe("v1");
      const dataset = await readJson(page, info,
        `${evalBase}/datasets/${encodeURIComponent(datasetName)}/v1`,
        "连续数据集-服务端冻结包络.json");
      expect(dataset.name).toBe(datasetName);
      expect(dataset.version).toBe("v1");
      expect(dataset.cases).toHaveLength(1);
      expect(dataset.cases[0].instruction).toBe("/add 2.5 3.5");
      expect(dataset.cases[0].files["INPUT.txt"]).toBe("/add 2.5 3.5");
      expect(dataset.cases[0].reference.expected).toBe("6");
      expect(dataset.cases[0].verifier).toBe("krim-script/1");
      expect(dataset.fingerprint).toMatch(/^[a-f0-9]{64}$/);
      expect(dataset.dataset_envelope_fp).toMatch(/^[a-f0-9]{64}$/);
      datasetEnvelopeFingerprint = dataset.dataset_envelope_fp;
      expect(request.postDataJSON()).toMatchObject({ name: datasetName, version: "v1", confirmations: [confirmation.id] });
      persistedDatasetName = datasetName;
      await expect(page.locator(".product-eval-list-row").filter({ hasText: datasetName })).toBeVisible();
    });

    let schedule: any;
    await desktopStep("选择服务端冻结包络与真实必需指标")(async () => {
      await page.getByRole("button", { name: "导航到评测", exact: true }).click();
      await openAutomation(page);
      const datasetChoice = page.locator("#automation-dataset");
      const option = datasetChoice.locator(`option[value="${datasetName}/v1"]`);
      await expect(option).toBeAttached({ timeout: 15000 });
      const detailResponsePromise = page.waitForResponse(response =>
        response.request().method() === "GET"
          && new URL(response.url()).pathname === `${evalBase}/datasets/${datasetName}/v1`,
      );
      await datasetChoice.selectOption(`${datasetName}/v1`);
      const datasetResponse = await detailResponsePromise;
      const continuousDataset = await datasetResponse.json();
      await attachJsonEvidence(info, "自动化目录选择-服务端连续数据集.json", continuousDataset);
      expect(continuousDataset.dataset_envelope_fp).toBe(datasetEnvelopeFingerprint);
      await page.locator("#automation-target").selectOption("krim-script-v2");
      const metrics = await readJson(page, info,
        `${evalBase}/metrics?target_key=krim-script-v2&mode=continuous&include_process=false`,
        "自动化真实连续指标目录.json");
      const metricNames = metrics.map((metric: any) => metric.name);
      for (const required of ["fact_integrity", "context_coverage", "krim_result"]) {
        expect(metricNames).toContain(required);
        const row = page.locator(".automation-metric-choice").filter({ hasText: required === "fact_integrity" ? "事实完整性（必需）" : required === "context_coverage" ? "上下文覆盖（必需）" : "Krim 执行结果（必需）" });
        await expect(row.locator("input[type=checkbox]")).toBeChecked();
        await expect(row.locator("input[type=checkbox]")).toBeDisabled();
      }
      await expect(page.locator("#automation-dataset")).toHaveValue(`${datasetName}/v1`);
      await expect(page.locator("#automation-target")).toHaveValue("krim-script-v2");
    });

    await desktopStep("按现在时间创建唯一有界回归计划")(async () => {
      await page.locator("#automation-schedule-id").fill(scheduleId);
      await page.locator("#automation-schedule-version").fill(scheduleVersion);
      await page.locator("#automation-epoch").fill(String(Math.floor(Date.now() / 1000)));
      await page.locator("#automation-interval").fill("3600");
      await page.locator("#automation-max-windows").fill("1");
      const requestPromise = page.waitForRequest(request =>
        new URL(request.url()).pathname === `${automationBase}/schedules` && request.method() === "POST",
      );
      const responsePromise = page.waitForResponse(response =>
        new URL(response.url()).pathname === `${automationBase}/schedules` && response.request().method() === "POST",
      );
      await page.getByRole("button", { name: "创建回归计划", exact: true }).click();
      const [request, response] = await Promise.all([requestPromise, responsePromise]);
      expect(response.status()).toBe(201);
      const requestBody = request.postDataJSON() as any;
      planRequestKey = requestBody.template.request_key;
      schedule = await saveActionResponse(info, "回归计划创建-原请求与服务端回执.json", request, response);
      expect(schedule.schedule_id).toBe(scheduleId);
      expect(schedule.version).toBe(scheduleVersion);
      expect(schedule.enabled).toBe(true);
      expect(schedule.revision).toBe(0);
      expect(schedule.spec.template.dataset.name).toBe(datasetName);
      expect(schedule.spec.template.dataset.version).toBe("v1");
      expect(schedule.spec.template.dataset_envelope_fp).toBe(datasetEnvelopeFingerprint);
      expect(schedule.spec.template.target_key).toBe("krim-script-v2");
      expect(schedule.spec.interval_seconds).toBe(3600);
      expect(schedule.spec.max_windows).toBe(1);
      expect(schedule.spec.template.metrics.map((metric: any) => metric.name)).toEqual(["context_coverage", "fact_integrity", "krim_result"]);
      expect(planRequestKey).toBeTruthy();
      const detail = await readJson(page, info, `${automationBase}/schedules/${encodeURIComponent(scheduleId)}`, "新建计划-持久详情与完整规范.json");
      expect(detail.spec_fingerprint).toMatch(/^[a-f0-9]{64}$/);
      expect(detail.spec.template.request_key).toBe(planRequestKey);
      const refreshedSchedules = await refreshScheduleListFromUi(page, info, "新建计划-UI只读刷新目录.json");
      const refreshed = refreshedSchedules.find((item: any) => item.schedule_id === scheduleId);
      expect(refreshed).toBeTruthy();
      expect(refreshed.enabled).toBe(true);
      await expect(page.locator(".automation-schedule-list li").filter({ hasText: scheduleId })).toContainText("已启用");
      persistedScheduleId = scheduleId;
    });

    let currentWindow: any;
    let tickTask: any;
    await desktopStep("读取当前窗口并仅触发该原窗口一次")(async () => {
      const planRow = page.locator(".automation-schedule-list li").filter({ hasText: scheduleId });
      const currentPath = `${automationBase}/schedules/${encodeURIComponent(scheduleId)}/windows/current`;
      const currentPromise = page.waitForResponse(response => response.request().method() === "GET" && response.url().endsWith(currentPath));
      const tickRequestPromise = page.waitForRequest(request =>
        request.method() === "POST" && new URL(request.url()).pathname === `${automationBase}/schedules/${encodeURIComponent(scheduleId)}/tick`,
      );
      const tickResponsePromise = page.waitForResponse(response =>
        response.request().method() === "POST" && new URL(response.url()).pathname === `${automationBase}/schedules/${encodeURIComponent(scheduleId)}/tick`,
      );
      await planRow.getByRole("button", { name: "读取当前窗口任务", exact: true }).click();
      const currentResponse = await currentPromise;
      currentWindow = await currentResponse.json();
      await attachJsonEvidence(info, "当前窗口-派发前只读快照.json", currentWindow);
      expect(currentWindow.schedule_id).toBe(scheduleId);
      expect(currentWindow.enabled).toBe(true);
      expect(currentWindow.eligible).toBe(true);
      windowIndex = currentWindow.window_index;
      const [tickRequest, tickResponse] = await Promise.all([tickRequestPromise, tickResponsePromise]);
      expect(tickRequest.postDataJSON()).toEqual({ expected_window_index: windowIndex });
      expect(tickResponse.status()).toBe(202);
      tickTask = await saveActionResponse(info, "原窗口任务-guard请求与服务端回执.json", tickRequest, tickResponse);
      expect(tickTask.id).toBeTruthy();
      expect(tickTask.request_key).toBeTruthy();
      taskRequestKey = tickTask.request_key;
      expect(tickTask.status).toMatch(/queued|running|completed/);
      persistedTaskId = tickTask.id;
      const windowReceipt = await readJson(page, info,
        `${automationBase}/schedules/${encodeURIComponent(scheduleId)}/windows/${windowIndex}`,
        "原窗口持久任务回执.json");
      expect(windowReceipt.schedule_id).toBe(scheduleId);
      expect(windowReceipt.window_index).toBe(windowIndex);
      expect(windowReceipt.status).toBe("reserved");
      expect(windowReceipt.task.id).toBe(tickTask.id);
      expect(windowReceipt.task.request_key).toBe(taskRequestKey);
      await expect.poll(() => new URL(page.url()).searchParams.get("product_eval_id")).toBe(tickTask.id);
    });

    let completedTask: any;
    await desktopStep("打开原窗口任务并读取真实通过报告")(async () => {
      const taskPath = `${evalBase}/tasks/${encodeURIComponent(persistedTaskId)}`;
      await expect(page.getByRole("button", { name: "查看报告", exact: true })).toBeVisible({ timeout: 60000 });
      completedTask = await readJson(page, info, taskPath, "原窗口评测任务完成状态.json");
      expect(completedTask.id).toBe(persistedTaskId);
      expect(completedTask.status).toBe("completed");
      await page.getByRole("button", { name: "查看报告", exact: true }).click();
      await expect(page.locator(".product-eval-report-row")).toHaveCount(1);
      await expect(page.locator(".product-eval-report-row.is-failed")).toHaveCount(0);
      await expect(page.locator(".product-eval-report-summary")).toContainText("100%");
      await page.locator(".product-eval-report").scrollIntoViewIfNeeded();
      const report = await readJson(page, info,
        `${taskPath}/report?limit=16`, "原窗口任务-真实评测报告.json");
      expect(report.eval_id).toBe(persistedTaskId);
      expect(report.rows).toHaveLength(1);
      expect(report.rows[0].quality).toBe("passed");
      expect(report.rows[0].case_id).toBeTruthy();
      expect(report.quality).toBeTruthy();
    });

    let disabledSchedule: any;
    await desktopStep("返回持续回归并通过刷新恢复持久计划")(async () => {
      await page.getByRole("button", { name: "导航到评测", exact: true }).click();
      await openAutomation(page);
      await page.getByRole("button", { name: "只读刷新", exact: true }).click();
      const schedules = await readJson(page, info, `${automationBase}/schedules?offset=0&limit=20`, "计划目录-服务端持久恢复.json");
      const recovered = schedules.find((item: any) => item.schedule_id === scheduleId);
      expect(recovered).toBeTruthy();
      expect(recovered.enabled).toBe(true);
      expect(recovered.spec.template.request_key).toBe(planRequestKey);
      expect(recovered.spec.template.dataset.name).toBe(datasetName);
      const detail = await readJson(page, info, `${automationBase}/schedules/${encodeURIComponent(scheduleId)}`, "计划刷新后原规范与数据集.json");
      expect(detail.spec_fingerprint).toBe(schedule.spec_fingerprint);
      await expect(page.locator(".automation-schedule-list li").filter({ hasText: scheduleId })).toContainText("已启用");
      disabledSchedule = recovered;
    });

    await desktopStep("用原控制键明确停用计划并核对回执")(async () => {
      const planRow = page.locator(".automation-schedule-list li").filter({ hasText: scheduleId });
      const detailPath = `${automationBase}/schedules/${encodeURIComponent(scheduleId)}`;
      const disablePath = `${detailPath}/disable`;
      const requestPromise = page.waitForRequest(request => new URL(request.url()).pathname === disablePath && request.method() === "POST");
      const responsePromise = page.waitForResponse(response => new URL(response.url()).pathname === disablePath && response.request().method() === "POST");
      await planRow.getByRole("button", { name: "禁用计划", exact: true }).click();
      const [request, response] = await Promise.all([requestPromise, responsePromise]);
      const requestBody = request.postDataJSON() as { revision: number; key: string };
      disableKey = requestBody.key;
      expect(disableKey).toBeTruthy();
      expect(requestBody.revision).toBe(disabledSchedule.revision);
      const disabled = await saveActionResponse(info, "计划停用-原控制键与服务端回执.json", request, response);
      expect(disabled.schedule_id).toBe(scheduleId);
      expect(disabled.enabled).toBe(false);
      expect(disabled.revision).toBe(disabledSchedule.revision + 1);
      expect(disabled.control_receipt).toEqual({
        key: disableKey,
        decision: "disabled",
        expected_revision: disabledSchedule.revision,
        revision: disabledSchedule.revision + 1,
      });
      const persisted = await readJson(page, info, detailPath, "计划停用后持久控制回执.json");
      disabledControlReceipt = persisted.control_receipt;
      expect(persisted.enabled).toBe(false);
      expect(persisted.control_receipt.key).toBe(disableKey);
      expect(persisted.control_receipt.decision).toBe("disabled");
      expect(persisted.control_receipt.expected_revision).toBe(disabledSchedule.revision);
      const refreshedSchedules = await refreshScheduleListFromUi(page, info, "计划停用后UI刷新目录.json");
      const recovered = refreshedSchedules.find((item: any) => item.schedule_id === scheduleId);
      expect(recovered).toBeTruthy();
      expect(recovered.enabled).toBe(false);
      expect(recovered.control_receipt.key).toBe(disableKey);
      await expect(planRow).toContainText("已停用");
    });

    await desktopStep("最后停用本地采集避免影响后续样本")(async () => {
      const before = await readJson(page, info, `${automationBase}/status`, "采集停用前服务端状态.json");
      expect(before.capture.enabled).toBe(true);
      expect(before.capture.text_capture).toBe(false);
      expect(before.capture.argument_capture).toBe(false);
      const requestPromise = page.waitForRequest(request =>
        request.url().endsWith(`${automationBase}/capture`) && request.method() === "POST",
      );
      const responsePromise = page.waitForResponse(response =>
        response.url().endsWith(`${automationBase}/capture`) && response.request().method() === "POST",
      );
      await expect(page.getByRole("button", { name: "停用本地采集", exact: true })).toBeEnabled({ timeout: 10000 });
      await page.getByRole("button", { name: "停用本地采集", exact: true }).click();
      const request = await requestPromise;
      captureDisableAttempted = true;
      const response = await responsePromise;
      const captureRequest = request.postDataJSON() as { enabled: boolean };
      expect(captureRequest.enabled).toBe(false);
      const disabledCapture = await saveActionResponse(info, "停用本地采集-原请求与回执.json", request, response);
      expect(disabledCapture.capture.enabled).toBe(false);
      const finalStatus = await readJson(page, info, `${automationBase}/status`, "采集停用后最终状态.json");
      expect(finalStatus.capture.enabled).toBe(false);
      expect(finalStatus.capture.text_capture).toBe(false);
      expect(finalStatus.capture.argument_capture).toBe(false);
      captureMayBeEnabled = false;
      const associations = {
        workspace,
        source: { session_id: session.id, turn_id: acceptedTurn.id, run_id: sourceRunId },
        feedback: { key: feedbackKey, reason: "incorrect", receipt: linkedReceipt, candidate_id: feedbackCandidateId },
        confirmation: { candidate_id: feedbackCandidateId, confirmation_id: confirmation.id },
        dataset: { name: datasetName, version: "v1", fingerprint: frozenDataset.dataset.fingerprint, dataset_envelope_fp: datasetEnvelopeFingerprint },
        plan: { schedule_id: scheduleId, version: scheduleVersion, request_key: planRequestKey },
        window: { window_index: windowIndex, task_id: persistedTaskId, request_key: taskRequestKey },
        disable: { key: disableKey, control_receipt: disabledControlReceipt },
        capture: { enabled_after_test: finalStatus.capture.enabled },
      };
      await attachJsonEvidence(info, "P4-来源候选计划任务与原键关联.json", associations);
    });
  } finally {
    await stopCaptureAfterFailure(page, info, captureMayBeEnabled, captureDisableAttempted);
  }
});

test("P4：窄屏深色只读恢复本机持久计划", async ({ page }, info) => {
  test.setTimeout(60000);
  await page.setViewportSize({ width: 390, height: 844 });
  await page.emulateMedia({ colorScheme: "dark" });
  const stepNarrow = step(page, info, "P4-窄屏深色只读恢复", "dark");
  let planId = persistedScheduleId;
  test.skip(!planId, "完整旅程未创建本机持久计划；不制造fixture或替代数据。");
  await stepNarrow("以窄屏深色打开已存在的持久计划")(async () => {
    const schedules = await readJson(page, info, `${automationBase}/schedules?offset=0&limit=50`, "窄屏恢复-既有服务端计划目录.json");
    const row = schedules.find((item: any) => item.schedule_id === planId);
    expect(row).toBeTruthy();
    expect(row.enabled).toBe(false);
    await page.goto("/ui/?view=eval&workspace=local");
    await selectTheme(page, "dark");
    if (await page.getByRole("button", { name: "打开导航", exact: true }).isVisible().catch(() => false)) {
      await page.getByRole("button", { name: "打开导航", exact: true }).click();
      await page.locator(".mobile-drawer-content .sidebar-buttons").getByRole("button", { name: "导航到评测", exact: true }).click();
    }
    await expect(page.locator("html")).toHaveAttribute("data-theme", "dark");
    await openAutomation(page);
    const detail = await readJson(page, info, `${automationBase}/schedules/${encodeURIComponent(planId)}`, "窄屏恢复-既有计划只读详情.json");
    expect(detail.schedule_id).toBe(planId);
    expect(detail.enabled).toBe(false);
    expect(detail.control_receipt.decision).toBe("disabled");
    expect(detail.spec.template.dataset.name).toBe(persistedDatasetName);
    await expect(page.locator(".automation-schedule-list li").filter({ hasText: planId })).toContainText("已停用");
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
  });
  await stepNarrow("刷新后继续只读恢复原计划与控制回执")(async () => {
    await page.reload();
    await expect(page.locator("html")).toHaveAttribute("data-theme", "dark");
    await openAutomation(page);
    const detail = await readJson(page, info, `${automationBase}/schedules/${encodeURIComponent(planId)}`, "窄屏刷新-原计划持久回执.json");
    expect(detail.schedule_id).toBe(planId);
    expect(detail.enabled).toBe(false);
    expect(detail.control_receipt.key).toBeTruthy();
    expect(detail.control_receipt.decision).toBe("disabled");
    expect(detail.spec.template.request_key).toBeTruthy();
    expect(detail.spec.template.dataset_envelope_fp).toMatch(/^[a-f0-9]{64}$/);
    await expect(page.locator(".automation-schedule-list li").filter({ hasText: planId })).toContainText("已停用");
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
    const association = { schedule_id: planId, task_id: persistedTaskId, disabled_key: detail.control_receipt.key, control_receipt: detail.control_receipt };
    await attachJsonEvidence(info, "窄屏计划恢复-原任务与停用回执关联.json", association);
  });
});

test("P4：桌面只读核验已完成窗口任务与停用计划", async ({ page }, info) => {
  test.setTimeout(90000);
  await page.setViewportSize({ width: 1440, height: 1000 });
  await page.emulateMedia({ colorScheme: "light" });
  const methods = observeBusinessMethods(page);
  const desktopReadStep = step(page, info, "P4-桌面只读复核");
  let facts: any;

  await desktopReadStep("只读查找已完成任务和有控制回执的计划")(async () => {
    await page.goto("/ui/?view=eval&workspace=local");
    const health = await readOnlyJson(page, info, "/healthz", "只读复核-宿主能力.json", methods);
    expect(health.product_eval_enabled).toBe(true);
    expect(health.evaluation_automation_enabled).toBe(true);
    const actor = await readOnlyJson(page, info, "/v2/me", "只读复核-LOCAL身份.json", methods);
    expect(actor.workspaces?.find((item: any) => item.workspace_id === workspace)?.role).toBe("admin");
    const status = await readOnlyJson(page, info, `${automationBase}/status`, "只读复核-采集已停用状态.json", methods);
    expect(status.capture.enabled).toBe(false);
    expect(status.capture.text_capture).toBe(false);
    expect(status.capture.argument_capture).toBe(false);

    const scheduleRows = await readOnlyJson(page, info, `${automationBase}/schedules?offset=0&limit=50`, "只读复核-计划目录.json", methods);
    const candidates = scheduleRows
      .filter((item: any) => item.enabled === false && item.control_receipt?.decision === "disabled"
        && typeof item.dataset?.name === "string" && item.dataset.name.startsWith("a-p4-"))
      .sort((left: any, right: any) => right.epoch - left.epoch);
    for (const candidate of candidates) {
      const plan = await readOnlyJson(page, info,
        `${automationBase}/schedules/${encodeURIComponent(candidate.schedule_id)}`,
        `只读复核-计划详情-${candidate.schedule_id}.json`, methods);
      if (plan.enabled || plan.control_receipt?.decision !== "disabled") continue;
      const window = await readOnlyJson(page, info,
        `${automationBase}/schedules/${encodeURIComponent(candidate.schedule_id)}/windows/0`,
        `只读复核-原窗口-${candidate.schedule_id}.json`, methods);
      if (window.status !== "reserved" || !window.task?.id || !window.task?.request_key) continue;
      const task = await readOnlyJson(page, info,
        `${evalBase}/tasks/${encodeURIComponent(window.task.id)}`,
        `只读复核-窗口Task-${window.task.id}.json`, methods);
      if (task.status !== "completed" || !task.report_hash || task.request_key !== window.task.request_key) continue;
      const report = await readOnlyJson(page, info,
        `${evalBase}/tasks/${encodeURIComponent(task.id)}/report?limit=16`,
        `只读复核-原窗口通过报告-${task.id}.json`, methods);
      if (report.eval_id !== task.id || !Array.isArray(report.rows) || report.rows.length < 1
        || report.rows.some((row: any) => row.quality !== "passed")) continue;
      facts = { plan, window, task, report };
      break;
    }
    expect(facts, "隔离库应保留至少一个已完成的 P4 窗口任务与停用计划").toBeTruthy();
    readonlyTaskId = facts.task.id;
    readonlyScheduleId = facts.plan.schedule_id;
    readonlyTaskRequestKey = facts.task.request_key;
    readonlyDisableKey = facts.plan.control_receipt.key;
    expect(facts.window.task.request_key).toBe(readonlyTaskRequestKey);
    await page.goto(`/ui/?view=eval&workspace=local&product_eval_id=${encodeURIComponent(readonlyTaskId)}`);
    await expect(page.locator(".product-eval-task-facts")).toContainText(readonlyTaskId);
  });

  await desktopReadStep("桌面UI只读打开报告并核验计划回执")(async () => {
    await page.getByRole("button", { name: "查看报告", exact: true }).click();
    await expect(page.locator(".product-eval-report-row")).toHaveCount(facts.report.rows.length);
    await expect(page.locator(".product-eval-report-row.is-failed")).toHaveCount(0);
    await expect(page.locator(".product-eval-report-summary")).toContainText("100%");
    await page.locator(".product-eval-report").scrollIntoViewIfNeeded();
    const report = await readOnlyJson(page, info,
      `${evalBase}/tasks/${encodeURIComponent(readonlyTaskId)}/report?limit=16`,
      "只读复核-桌面原报告.json", methods);
    expect(report.eval_id).toBe(readonlyTaskId);
    expect(report.rows.every((row: any) => row.quality === "passed")).toBe(true);
    await openAutomation(page);
    const detail = await readOnlyJson(page, info,
      `${automationBase}/schedules/${encodeURIComponent(readonlyScheduleId)}`,
      "只读复核-桌面计划控制回执.json", methods);
    expect(detail.enabled).toBe(false);
    expect(detail.control_receipt.key).toBe(readonlyDisableKey);
    expect(detail.control_receipt.decision).toBe("disabled");
    await expect(page.locator(".automation-schedule-list li").filter({ hasText: readonlyScheduleId })).toContainText("已停用");
    expectOnlyReadRequests(methods);
    await attachJsonEvidence(info, "只读复核-Task计划与原键关联.json", {
      task_id: readonlyTaskId,
      task_request_key: readonlyTaskRequestKey,
      schedule_id: readonlyScheduleId,
      disable_key: readonlyDisableKey,
      control_receipt: detail.control_receipt,
    });
  });
});

test("P4：窄屏深色只读核验报告与控制回执", async ({ page }, info) => {
  test.setTimeout(60000);
  test.skip(!readonlyTaskId || !readonlyScheduleId, "桌面只读用例未找到已完成Task与停用计划。");
  await page.setViewportSize({ width: 390, height: 844 });
  await page.emulateMedia({ colorScheme: "dark" });
  const methods = observeBusinessMethods(page);
  const narrowReadStep = step(page, info, "P4-窄屏深色只读复核", "dark");

  await narrowReadStep("窄屏深色只读恢复原窗口报告")(async () => {
    await page.goto(`/ui/?view=eval&workspace=local&product_eval_id=${encodeURIComponent(readonlyTaskId)}`);
    await selectTheme(page, "dark");
    await expect(page.locator("html")).toHaveAttribute("data-theme", "dark");
    await expect(page.locator(".product-eval-task-facts")).toContainText(readonlyTaskId);
    await page.getByRole("button", { name: "查看报告", exact: true }).click();
    await expect(page.locator(".product-eval-report-row").first()).toBeVisible();
    await expect(page.locator(".product-eval-report-row.is-failed")).toHaveCount(0);
    await page.locator(".product-eval-report").scrollIntoViewIfNeeded();
    const report = await readOnlyJson(page, info,
      `${evalBase}/tasks/${encodeURIComponent(readonlyTaskId)}/report?limit=16`,
      "只读复核-窄屏深色原报告.json", methods);
    expect(report.eval_id).toBe(readonlyTaskId);
    expect(report.rows.every((row: any) => row.quality === "passed")).toBe(true);
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
  });

  await narrowReadStep("窄屏只读核验停用计划与原控制键")(async () => {
    await openAutomation(page);
    const status = await readOnlyJson(page, info, `${automationBase}/status`, "只读复核-窄屏采集已停用.json", methods);
    expect(status.capture.enabled).toBe(false);
    const detail = await readOnlyJson(page, info,
      `${automationBase}/schedules/${encodeURIComponent(readonlyScheduleId)}`,
      "只读复核-窄屏持久计划control-receipt.json", methods);
    const window = await readOnlyJson(page, info,
      `${automationBase}/schedules/${encodeURIComponent(readonlyScheduleId)}/windows/0`,
      "只读复核-窄屏原窗口task.json", methods);
    expect(detail.enabled).toBe(false);
    expect(detail.control_receipt.key).toBe(readonlyDisableKey);
    expect(detail.control_receipt.decision).toBe("disabled");
    expect(window.status).toBe("reserved");
    expect(window.task.id).toBe(readonlyTaskId);
    expect(window.task.request_key).toBe(readonlyTaskRequestKey);
    await expect(page.locator(".automation-schedule-list li").filter({ hasText: readonlyScheduleId })).toContainText("已停用");
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
    expectOnlyReadRequests(methods);
    await attachJsonEvidence(info, "只读复核-窄屏原窗口与控制回执关联.json", {
      task_id: readonlyTaskId,
      task_request_key: readonlyTaskRequestKey,
      schedule_id: readonlyScheduleId,
      disable_key: readonlyDisableKey,
      control_receipt: detail.control_receipt,
      window,
    });
  });
});
