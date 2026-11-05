import { createHash } from "node:crypto";
import { expect, test, type Page } from "@playwright/test";
import { attachJsonEvidence, screenshotStep } from "./step-screenshot.js";

const token = "p5-prepared-smoke-local-only";
const headers = { Authorization: "Bearer " + token };
test.skip(process.env.KRIM_PREPARED_WORKSPACE !== "1", "需要当前已资格的独立 Workspace 服务");

async function openNavigationSurface(page: Page) {
  const drawer = page.getByRole("dialog", { name: "主导航与对话", exact: true });
  if (!await drawer.isVisible()) {
    const mobileMenu = page.getByRole("button", { name: "打开导航", exact: true });
    if (await mobileMenu.isVisible()) { await mobileMenu.click(); await expect(drawer).toBeVisible(); }
  }
  return drawer;
}

async function closeMobileDrawer(page: Page) {
  const drawer = page.getByRole("dialog", { name: "主导航与对话", exact: true });
  if (await drawer.isVisible()) { await page.keyboard.press("Escape"); await expect(drawer).not.toBeVisible(); }
}

async function selectTheme(page: Page, theme: "warm" | "bright" | "dark") {
  const labels = { warm: "暖纸", bright: "浅色", dark: "深色" };
  const drawer = await openNavigationSurface(page);
  const surface = await drawer.isVisible() ? drawer : page.locator(".desktop-sidebar");
  await surface.getByRole("button", { name: "设置", exact: true }).click();
  await page.locator(".settings-popover .theme-options").getByRole("button", { name: new RegExp("^" + labels[theme]) }).click();
  await expect(page.locator("html")).toHaveAttribute("data-theme", theme);
  // 主题选择后恢复抽屉开合状态，避免遮挡后续主区操作。
  await closeMobileDrawer(page);
}

for (const viewport of [{ name: "桌面", width: 1440, height: 1000, theme: "warm" as const },
  { name: "窄屏", width: 390, height: 844, theme: "dark" as const }]) {
  test(`P2/P5：${viewport.name}文件、审批、实际命令与产物`, async ({ page }, info) => {
    test.setTimeout(90000);
    await page.setViewportSize(viewport);
    let sessionId = "", base = "";
    const step = (name: string, action: () => Promise<void>) => screenshotStep(page, info,
      { caseName: `P2-P5-${viewport.name}文件任务`, stepName: name, theme: viewport.theme }, action);
    await step("连接实际隔离工作区", async () => {
      await page.goto("/ui/");
      await openNavigationSurface(page);
      await page.getByRole("button", { name: "密钥身份" }).click();
      await page.getByRole("menuitem", { name: "连接 / 切换密钥" }).click();
      await page.locator("#api-key").fill(token);
      await page.locator("#connect-save").click();
      await expect(page.locator(".sidebar-account .account-sub")).toContainText("已连接");
      await expect(page.locator(".sidebar-buttons").getByRole("button", { name: "新建对话", exact: true })).toBeEnabled();
      await selectTheme(page, viewport.theme);
      // 主题选择会关闭移动端抽屉；重新打开，便于下一步从侧栏新建会话。
      await openNavigationSurface(page);
    });
    await step("新建文件任务会话", async () => {
      await page.locator(".sidebar-buttons").getByRole("button", { name: "新建对话", exact: true }).click();
      await page.locator("#session-title").fill(`${viewport.name}文件验收`);
      await page.locator("#chat-target").selectOption("target-workspace");
      await page.getByRole("dialog").getByRole("button", { name: "创建对话", exact: true }).click();
      await expect(page.locator(".session-create-dialog")).not.toBeVisible();
      await closeMobileDrawer(page);
      await page.locator(".workspace-task-area > summary").click();
      await expect(page.locator("#workspace-text")).toBeVisible();
      await expect(page.locator("#workspace-text")).toBeEnabled();
      sessionId = new URL(page.url()).searchParams.get("session") ?? "";
      expect(sessionId).toBeTruthy();
      base = `/v3/workspaces/local/sessions/${sessionId}`;
      await page.locator("#workspace-input-form").scrollIntoViewIfNeeded();
    });
    await step("读取真实文件版本并导入文本文件", async () => {
      await expect(page.locator("#workspace-input-revision")).toHaveValue("");
      await page.getByRole("button", { name: "读取当前输入版本", exact: true }).click();
      await expect(page.locator("#workspace-input-revision")).toHaveValue("0");
      await page.locator("#workspace-file-import").setInputFiles([
        { name: "seed.json", mimeType: "application/json", buffer: Buffer.from('{"answer":42}\n') },
        { name: "test_uploaded_input.py", mimeType: "text/x-python", buffer: Buffer.from(
          "import json, unittest\nfrom pathlib import Path\n\nclass UploadedInputTest(unittest.TestCase):\n" +
          "    def test_seed_and_write_result(self):\n        value = json.loads(Path('seed.json').read_text())\n" +
          "        self.assertEqual(value, {'answer': 42})\n        Path('answer.json').write_text(json.dumps(value))\n") },
      ]);
      await expect(page.locator(".workspace-file-list li")).toHaveCount(2);
      await expect(page.locator(".workspace-file-editor").getByLabel("相对路径", { exact: true })).toHaveValue("seed.json");
      expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
    });
    await step("保存持久输入快照", async () => {
      const saved = page.waitForResponse(response => response.url().endsWith(base + "/inputs") && response.request().method() === "POST");
      await page.getByRole("button", { name: "提交输入快照", exact: true }).click();
      const response = await saved;
      expect(response.ok()).toBe(true);
      const input = await response.json();
      expect(input.revision).toBe(1);
      expect(input.manifest).toHaveLength(2);
      await expect(page.locator(".workspace-artifact")).toContainText("产物版本 1");
      await page.locator(".workspace-artifact").scrollIntoViewIfNeeded();
      await attachJsonEvidence(info, "真实输入快照.json", input);
    });
    await step("填写文件完成条件", async () => {
      await page.locator("#workspace-text").fill("运行上传的 unittest，生成 answer.json 并核对答案为42");
      await page.locator("#workspace-required-path").fill("answer.json");
      await page.getByRole("button", { name: "添加路径", exact: true }).click();
      await page.getByRole("button", { name: "添加完成条件", exact: true }).click();
      const condition = page.locator(".workspace-condition");
      await condition.getByLabel("条件名称").fill("answer");
      await condition.getByLabel("检查文件").fill("answer.json");
      await condition.getByLabel("JSON Pointer").fill("/answer");
      await condition.getByLabel("预期 JSON 值").fill("42");
      await page.locator("#workspace-turn-form").scrollIntoViewIfNeeded();
    });
    await step("发起任务并检查命令审批", async () => {
      await page.getByRole("button", { name: "发送工作请求", exact: true }).click();
      const approval = page.locator(".chat-approval").filter({ hasText: "command_run" });
      await expect(approval).toBeVisible({ timeout: 20000 });
      await expect(approval.locator("pre")).toContainText("unittest");
      await approval.scrollIntoViewIfNeeded();
    });
    await step("批准一次调用并等待实际执行", async () => {
      await page.getByRole("button", { name: "允许 command_run 本次调用", exact: true }).click();
      await expect.poll(async () => {
        const turnId = await page.locator(".chat-turn").last().getAttribute("data-turn-id");
        expect(turnId).toBeTruthy();
        const response = await page.request.get(base + "/turns/" + encodeURIComponent(turnId!), { headers });
        expect(response.ok()).toBe(true);
        const turn = await response.json();
        expect(turn.id).toBe(turnId); expect(turn.session_id).toBe(sessionId);
        return turn.status;
      }, { timeout: 30000 }).toBe("completed");
      await page.getByRole("button", { name: "刷新工作结果", exact: true }).click();
      await expect(page.locator(".workspace-facts")).toContainText("exit=0");
      await expect(page.locator(".workspace-facts")).toContainText("passed");
      const response = await page.request.get(base + "/work", { headers });
      expect(response.ok()).toBe(true);
      const work = await response.json();
      expect(work.verification.state).toBe("passed");
      expect(work.effects.some((effect: { kind: string; exit_code: number; cleanup: string }) => effect.kind === "command" && effect.exit_code === 0 && effect.cleanup === "clean")).toBe(true);
      await attachJsonEvidence(info, "真实命令与核验.json", work);
      await page.locator(".workspace-facts").scrollIntoViewIfNeeded();
    });
    await step("读取真实产物内容与当前版本", async () => {
      await expect(page.locator(".workspace-artifact")).toContainText("answer.json");
      await page.locator(".workspace-artifact").getByRole("button", { name: "answer.json", exact: true }).click();
      await expect(page.locator(".workspace-artifact pre")).toContainText('"answer": 42');
      await page.locator(".workspace-artifact").scrollIntoViewIfNeeded();
    });
    await step("越界路径被拒绝且不改变工作结果", async () => {
      const before = await (await page.request.get(base + "/work", { headers })).json();
      const bytes = Buffer.from("x");
      const result = await page.request.post(base + "/inputs", { headers, data: {
        schema_version: 1, request_key: `reject-${Date.now()}`, expected_head_revision: 2,
        files: [{ path: "../escape.txt", encoding: "base64", data: bytes.toString("base64"), sha256: createHash("sha256").update(bytes).digest("hex") }],
      } });
      expect(result.status()).toBe(409);
      expect((await result.json()).error.code).toBe("artifact_invalid");
      expect(await (await page.request.get(base + "/work", { headers })).json()).toEqual(before);
      expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
      await attachJsonEvidence(info, "越界路径拒绝.json", { status: result.status(), body: await result.json() });
    });
  });
}

test("P2/P5：桌面拒绝command_run审批后不启动执行器且不生成answer.json", async ({ page }, info) => {
  test.setTimeout(90000);
  const viewport = { width: 1440, height: 1000 };
  await page.setViewportSize(viewport);
  let sessionId = "", base = "";
  const step = (name: string, action: () => Promise<void>) => screenshotStep(page, info,
    { caseName: "P2-P5-桌面拒绝审批", stepName: name, theme: "warm" }, action);
  const saveApi = async (name: string, response: { status(): number; json(): Promise<any> }) => {
    const body = await response.json();
    await attachJsonEvidence(info, name, { status: response.status(), body });
    return body;
  };

  await step("连接实际隔离工作区", async () => {
    await page.goto("/ui/");
    await page.getByRole("button", { name: "密钥身份" }).click();
    await page.getByRole("menuitem", { name: "连接 / 切换密钥" }).click();
    await page.locator("#api-key").fill(token);
    await page.locator("#connect-save").click();
    await expect(page.locator(".sidebar-account .account-sub")).toContainText("已连接");
    await expect(page.locator(".sidebar-buttons").getByRole("button", { name: "新建对话", exact: true })).toBeEnabled();
    await selectTheme(page, "warm");
  });
  await step("新建独立Workspace会话", async () => {
    await page.locator(".sidebar-buttons").getByRole("button", { name: "新建对话", exact: true }).click();
    await page.locator("#session-title").fill("桌面拒绝审批验收");
    await page.locator("#chat-target").selectOption("target-workspace");
    await page.getByRole("dialog").getByRole("button", { name: "创建对话", exact: true }).click();
    await page.locator(".workspace-task-area > summary").click();
    await expect(page.locator("#workspace-text")).toBeVisible();
    await expect(page.locator("#workspace-text")).toBeEnabled();
    sessionId = new URL(page.url()).searchParams.get("session") ?? "";
    expect(sessionId).toBeTruthy();
    base = `/v3/workspaces/local/sessions/${sessionId}`;
    const sessionResponse = await page.request.get(`/v2/workspaces/local/sessions/${sessionId}`, { headers });
    expect(sessionResponse.ok()).toBe(true);
    const session = await saveApi("拒绝流程-会话.json", sessionResponse);
    expect(session.target_key).toBe("target-workspace");
    await page.locator("#workspace-input-form").scrollIntoViewIfNeeded();
  });
  await step("读取head0并导入测试文件", async () => {
    const headRead = page.waitForResponse(response =>
      response.url().endsWith(base + "/artifacts/head") && response.request().method() === "GET");
    await page.getByRole("button", { name: "读取当前输入版本", exact: true }).click();
    const headResponse = await headRead;
    expect(headResponse.ok()).toBe(true);
    const initialHead = await saveApi("拒绝流程-初始head.json", headResponse);
    expect(initialHead.revision).toBe(0);
    await expect(page.locator("#workspace-input-revision")).toHaveValue("0");
    await page.locator("#workspace-file-import").setInputFiles([
      { name: "seed.json", mimeType: "application/json", buffer: Buffer.from('{"answer":42}\n') },
      { name: "test_uploaded_input.py", mimeType: "text/x-python", buffer: Buffer.from(
        "import json, unittest\nfrom pathlib import Path\n\nclass UploadedInputTest(unittest.TestCase):\n" +
        "    def test_seed_and_write_result(self):\n        value = json.loads(Path('seed.json').read_text())\n" +
        "        self.assertEqual(value, {'answer': 42})\n        Path('answer.json').write_text(json.dumps(value))\n") },
    ]);
    await expect(page.locator(".workspace-file-list li")).toHaveCount(2);
    await expect(page.locator(".workspace-file-editor").getByLabel("相对路径", { exact: true }))
      .toHaveValue("seed.json");
  });

  let input: any;
  await step("保存revision1输入快照", async () => {
    const saved = page.waitForResponse(response =>
      response.url().endsWith(base + "/inputs") && response.request().method() === "POST");
    await page.getByRole("button", { name: "提交输入快照", exact: true }).click();
    const response = await saved;
    expect(response.ok()).toBe(true);
    input = await saveApi("拒绝流程-revision1输入快照.json", response);
    expect(input.revision).toBe(1);
    expect(input.manifest.map((file: { path: string }) => file.path).sort())
      .toEqual(["seed.json", "test_uploaded_input.py"].sort());
    await expect(page.locator(".workspace-artifact")).toContainText("产物版本 1");
    await page.locator(".workspace-artifact").scrollIntoViewIfNeeded();
  });
  await step("填写要求answer.json的完成条件", async () => {
    await page.locator("#workspace-text").fill("运行上传的 unittest，生成 answer.json 并核对答案为42");
    await page.locator("#workspace-required-path").fill("answer.json");
    await page.getByRole("button", { name: "添加路径", exact: true }).click();
    await page.getByRole("button", { name: "添加完成条件", exact: true }).click();
    const condition = page.locator(".workspace-condition");
    await condition.getByLabel("条件名称").fill("answer");
    await condition.getByLabel("检查文件").fill("answer.json");
    await condition.getByLabel("JSON Pointer").fill("/answer");
    await condition.getByLabel("预期 JSON 值").fill("42");
    await page.locator("#workspace-turn-form").scrollIntoViewIfNeeded();
  });

  let turnId = "", commandApproval: any;
  await step("等待command_run审批", async () => {
    const acceptedPromise = page.waitForResponse(response =>
      response.url().endsWith(base + "/turns") && response.request().method() === "POST");
    await page.getByRole("button", { name: "发送工作请求", exact: true }).click();
    const acceptedResponse = await acceptedPromise;
    expect(acceptedResponse.ok()).toBe(true);
    const accepted = await saveApi("拒绝流程-工作请求.json", acceptedResponse);
    const turnRequest = acceptedResponse.request().postDataJSON();
    await attachJsonEvidence(info, "拒绝流程-工作请求请求.json", turnRequest);
    turnId = accepted.id;
    expect(turnId).toBeTruthy();
    expect(accepted.session_id).toBe(sessionId);
    expect(turnRequest.completion.required_paths).toContain("answer.json");
    const approval = page.locator(".chat-approval").filter({ hasText: "command_run" });
    await expect(approval).toBeVisible({ timeout: 20000 });
    await expect(approval.locator("pre")).toContainText("unittest");
    const approvalsResponse = await page.request.get(
      `/v2/workspaces/local/sessions/${sessionId}/turns/${turnId}/approvals`, { headers });
    expect(approvalsResponse.ok()).toBe(true);
    const approvals = await saveApi("拒绝流程-待审批command_run.json", approvalsResponse);
    expect(approvals).toHaveLength(1);
    commandApproval = approvals[0];
    expect(commandApproval.name).toBe("command_run");
    expect(commandApproval.arguments.expected_revision).toBe(1);
    expect(commandApproval.arguments.program).toBe("python");
    expect(commandApproval.arguments.argv).toEqual(["-m", "unittest", "discover", "-s", ".", "-p", "test_*.py"]);
    await approval.scrollIntoViewIfNeeded();
  });
  await step("在真实审批卡片中明确拒绝", async () => {
    const approval = page.locator(".chat-approval").filter({ hasText: "command_run" });
    const replyPromise = page.waitForResponse(response =>
      response.url().includes(`/approvals/${commandApproval.request_id}`) &&
      response.request().method() === "POST");
    await approval.getByRole("button", { name: "拒绝", exact: true }).click();
    const replyResponse = await replyPromise;
    expect(replyResponse.ok()).toBe(true);
    const reply = await saveApi("拒绝流程-审批拒绝回执.json", replyResponse);
    expect(reply.status).toBe("completed");
  });
  await step("核对真实Turn工具与Artifact终态", async () => {
    const turnPath = `/v2/workspaces/local/sessions/${sessionId}/turns/${turnId}`;
    await expect.poll(async () => {
      const response = await page.request.get(turnPath, { headers });
      expect(response.ok()).toBe(true);
      const turn = await response.json();
      return ["queued", "running"].includes(turn.status);
    }, { timeout: 30000 }).toBe(false);
    await expect(page.locator(".chat-turn").last().locator("[role=status]")).toHaveText(
      /^(已完成|失败|已停止|已中断)$/);
    await page.locator(".chat-turn").last().getByRole("button", { name: "更多操作", exact: true }).click();
    const visibleTool = page.locator(".chat-turn").last().locator(".chat-tool");
    await expect(visibleTool).toHaveCount(1);
    await expect(visibleTool.locator("summary")).toContainText("command_run");
    await expect(visibleTool.locator("summary")).toContainText("已拒绝");

    const finalTurnResponse = await page.request.get(turnPath, { headers });
    expect(finalTurnResponse.ok()).toBe(true);
    const finalTurn = await saveApi("拒绝流程-真实Turn终态.json", finalTurnResponse);
    expect(["queued", "running"]).not.toContain(finalTurn.status);
    expect(finalTurn.run_id).toBeTruthy();

    const resultResponse = await page.request.get(turnPath + "/result", { headers });
    expect(resultResponse.ok()).toBe(true);
    const result = await saveApi("拒绝流程-真实Turn工具结果.json", resultResponse);
    expect(result.run.id).toBe(finalTurn.run_id);
    const commandTools = result.tools.filter((tool: any) => tool.name === "command_run");
    expect(commandTools).toHaveLength(1);
    expect(commandTools[0]).toMatchObject({
      status: "denied", code: "approval_denied", decision: "ask",
      executor_started: false, result_available: false,
    });
    expect(commandTools[0].arguments.expected_revision).toBe(1);
    expect(result.events.some((event: any) => event.kind === "approval_reply" &&
      event.data.reply?.allow === false)).toBe(true);
    expect(result.events.some((event: any) => event.kind === "workspace_started" &&
      event.data.call_id === commandTools[0].call_id)).toBe(false);

    const workResponse = await page.request.get(base + "/turns/" + turnId + "/work", { headers });
    expect(workResponse.ok()).toBe(true);
    const work = await saveApi("拒绝流程-真实Work事实.json", workResponse);
    expect(work.turn_id).toBe(turnId);
    expect(work.run_id).toBe(finalTurn.run_id);
    expect(work.effects.every((effect: any) => effect.executor_started !== true)).toBe(true);

    const headResponse = await page.request.get(base + "/artifacts/head", { headers });
    expect(headResponse.ok()).toBe(true);
    const head = await saveApi("拒绝流程-拒绝后ArtifactHead.json", headResponse);
    expect(head.revision).toBe(1);
    expect(head.revision_id).toBe(input.revision_id);
    const artifactResponse = await page.request.get(base + "/artifacts/" + head.revision_id, { headers });
    expect(artifactResponse.ok()).toBe(true);
    const artifact = await saveApi("拒绝流程-拒绝后Artifact事实.json", artifactResponse);
    expect(artifact.source_kind).toBe("input");
    const paths = artifact.manifest.map((file: { path: string }) => file.path).sort();
    expect(paths).toEqual(["seed.json", "test_uploaded_input.py"].sort());
    expect(paths).not.toContain("answer.json");
  });
});
