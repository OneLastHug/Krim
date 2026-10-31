import { expect, test, type Page } from "@playwright/test";
import { attachJsonEvidence, screenshotStep } from "./step-screenshot.js";

type Theme = "warm" | "bright" | "dark";
const themes: Record<Theme, string> = { warm: "暖纸", bright: "亮彩", dark: "深色" };
async function geometry(page: Page) {
  // 视口与内容变化后等待布局帧，避免把上一个宽度的原生自适应高度记入证据。
  await page.evaluate(() => new Promise<void>(resolve => requestAnimationFrame(() => requestAnimationFrame(() => resolve()))));
  return page.evaluate(() => {
    const input = document.querySelector<HTMLTextAreaElement>("#chat-prompt")!;
    const composer = document.querySelector<HTMLElement>(".chat-composer")!;
    const send = document.querySelector<HTMLElement>(".chat-compose-actions button")!;
    const box = (element: HTMLElement) => {
      const value = element.getBoundingClientRect();
      return { x: value.x, y: value.y, width: value.width, height: value.height, bottom: value.bottom };
    };
    const padding = (element: HTMLElement) => {
      const style = getComputedStyle(element);
      return { top: style.paddingTop, right: style.paddingRight, bottom: style.paddingBottom, left: style.paddingLeft };
    };
    const lastFontSize = (selector: string) => {
      const elements = document.querySelectorAll<HTMLElement>(selector);
      const last = elements.item(elements.length - 1);
      return last ? getComputedStyle(last).fontSize : null;
    };
    const inputStyle = getComputedStyle(input);
    const sendStyle = getComputedStyle(send);
    return { composer: box(composer), input: box(input), send: box(send),
      rows: input.rows, value: input.value, scrollHeight: input.scrollHeight, clientHeight: input.clientHeight,
      resize: inputStyle.resize, fieldSizing: inputStyle.getPropertyValue("field-sizing"),
      typography: {
        body: getComputedStyle(document.body).fontSize,
        input: { fontSize: inputStyle.fontSize, lineHeight: inputStyle.lineHeight, minHeight: inputStyle.minHeight, padding: padding(input) },
        composerPadding: padding(composer),
        send: { fontSize: sendStyle.fontSize, minHeight: sendStyle.minHeight },
        buttons: Array.from(document.querySelectorAll<HTMLButtonElement>("button"))
          .filter(button => button.getClientRects().length > 0 && getComputedStyle(button).fontSize !== "0px")
          .map(button => ({ label: button.getAttribute("aria-label") ?? button.textContent?.trim() ?? "", fontSize: getComputedStyle(button).fontSize })),
        userMessage: lastFontSize(".chat-user-text"),
        answer: lastFontSize(".chat-answer"),
      },
      viewport: { width: innerWidth, height: innerHeight }, pageOverflow: document.documentElement.scrollWidth > innerWidth };
  });
}
for (const theme of ["warm", "bright", "dark"] as const) for (const mobile of [false, true]) {
  test(`紧凑输入框：${themes[theme]}${mobile ? "手机" : "桌面"}`, async ({ page }, info) => {
    const viewport = mobile ? { width: 375, height: 812 } : { width: 1440, height: 900 };
    await page.setViewportSize(viewport);
    const posts: Record<string, unknown>[] = [];
    page.on("request", request => {
      if (request.method() === "POST" && new URL(request.url()).pathname.endsWith("/turns")) posts.push(request.postDataJSON() as Record<string, unknown>);
    });
    const step = (name: string, action: () => Promise<void>) => screenshotStep(page, info,
      { caseName: `composer-${theme}-${mobile ? "mobile" : "desktop"}`, stepName: name, theme }, action);
    let empty = 0;
    await step("新建对话并选择主题", async () => {
      await page.goto("/ui/");
      await page.getByRole("button", { name: "选择显示主题", exact: true }).click();
      await page.locator(".theme-options").getByRole("button", { name: new RegExp("^" + themes[theme]) }).click();
      const navigation = page.getByRole("button", { name: "打开导航", exact: true });
      if (await navigation.isVisible()) await navigation.click();
      await page.getByRole("button", { name: "新建对话", exact: true }).click();
      await page.locator("#session-title").fill(mobile ? "消息草稿" : "任务说明");
      await page.getByRole("dialog", { name: "新建对话", exact: true }).getByRole("button", { name: "创建对话", exact: true }).click();
      await expect(page.locator("#chat-prompt")).toBeEnabled();
    });
    await step("空白保持单行与行内发送按钮", async () => {
      const value = await geometry(page);
      empty = value.composer.height;
      expect(value.rows).toBe(1);
      const expectedComposerHeight = mobile ? 52 : 40;
      expect(value.composer.height).toBeGreaterThanOrEqual(expectedComposerHeight - 1);
      expect(value.composer.height).toBeLessThanOrEqual(expectedComposerHeight + 1);
      expect(value.input.height).toBeLessThanOrEqual(40);
      const expectedSendSize = mobile ? 44 : 32;
      expect(value.send.width).toBeGreaterThanOrEqual(expectedSendSize - 1);
      expect(value.send.width).toBeLessThanOrEqual(expectedSendSize + 1);
      expect(value.send.height).toBeGreaterThanOrEqual(expectedSendSize - 1);
      expect(value.send.height).toBeLessThanOrEqual(expectedSendSize + 1);
      expect(value.typography.body).toBe("12px");
      expect(value.typography.input.fontSize).toBe("12px");
      expect(value.typography.input.lineHeight).toBe("18px");
      expect(value.typography.input.minHeight).toBe("26px");
      expect(value.typography.input.padding).toEqual({ top: "4px", right: "6px", bottom: "4px", left: "6px" });
      expect(value.typography.composerPadding).toEqual({ top: "3px", right: "6px", bottom: "3px", left: "6px" });
      if (!mobile) expect(value.typography.send.minHeight).toBe("32px");
      expect(value.typography.buttons.length).toBeGreaterThan(0);
      for (const button of value.typography.buttons) expect(button.fontSize).toBe("12px");
      expect(value.resize).toBe("none");
      expect(value.fieldSizing).toBe("content");
      expect(value.pageOverflow).toBe(false);
      await attachJsonEvidence(info, "空白输入框几何", value);
      if (!mobile && theme === "warm") await page.locator(".chat-composer").screenshot({ path: info.outputPath("composer-empty.png") });
    });
    await step("短文本不额外增高", async () => {
      await page.locator("#chat-prompt").fill("Try again");
      const value = await geometry(page);
      expect(value.composer.height).toBeLessThanOrEqual(empty + 1);
      expect(posts).toHaveLength(0);
      await attachJsonEvidence(info, "短文本几何", value);
      if (!mobile && theme === "warm") await page.locator(".chat-composer").screenshot({ path: info.outputPath("composer-short.png") });
    });
    await step("多行内容自动长高", async () => {
      await page.locator("#chat-prompt").fill("整理会议内容\n列出需要确认的事项\n保留原始日期和负责人\n输出简明的行动清单");
      const value = await geometry(page);
      expect(value.composer.height).toBeGreaterThanOrEqual(87);
      expect(value.composer.height).toBeLessThanOrEqual(89);
      expect(value.scrollHeight).toBeLessThanOrEqual(value.clientHeight + 1);
      expect(posts).toHaveLength(0);
      await attachJsonEvidence(info, "多行输入几何", value);
      if (!mobile && theme === "warm") await page.locator(".chat-composer").screenshot({ path: info.outputPath("composer-multiline.png") });
    });
    await step("超过上限仅内部滚动且输入区仍可见", async () => {
      await page.locator("#chat-prompt").fill(Array.from({ length: 40 }, (_, index) => `第${index + 1}行：保留这一条任务说明`).join("\n"));
      const value = await geometry(page);
      expect(value.input.height).toBeLessThanOrEqual(Math.min(240, viewport.height * 0.32) + 1);
      expect(value.scrollHeight).toBeGreaterThan(value.clientHeight);
      expect(value.composer.y).toBeGreaterThanOrEqual(0);
      expect(value.composer.bottom).toBeLessThanOrEqual(viewport.height);
      expect(value.pageOverflow).toBe(false);
      expect(posts).toHaveLength(0);
      await attachJsonEvidence(info, "长文本滚动与边界", value);
    });
    await step("删除后回到单行并按宽度重新排版", async () => {
      await page.locator("#chat-prompt").fill("");
      expect((await geometry(page)).composer.height).toBeLessThanOrEqual(empty + 1);
      const text = "请整理这些记录并保留每项任务的原始时间与状态。".repeat(8);
      await page.locator("#chat-prompt").fill(text);
      await page.setViewportSize({ width: 1440, height: viewport.height });
      const wide = await geometry(page);
      await page.setViewportSize({ width: 375, height: viewport.height });
      const narrow = await geometry(page);
      expect(narrow.input.height).toBeGreaterThan(wide.input.height);
      expect(narrow.value).toBe(text);
      expect(narrow.pageOverflow).toBe(false);
      await page.setViewportSize(viewport);
      await page.locator("#chat-prompt").fill("");
      await attachJsonEvidence(info, "清空恢复单行", await geometry(page));
    });
    await step("发送仍使用原有请求与工具链并清空缩回", async () => {
      await page.locator("#chat-prompt").fill("/add 2 3");
      await page.locator(".chat-compose-actions").getByRole("button", { name: "发送", exact: true }).click();
      await expect(page.locator(".chat-answer").last()).toHaveText("5");
      await expect(page.locator("#chat-prompt")).toHaveValue("");
      expect(posts).toHaveLength(1);
      expect(posts[0]!.text).toBe("/add 2 3");
      expect(typeof posts[0]!.request_key).toBe("string");
      const value = await geometry(page);
      expect(value.composer.height).toBeLessThanOrEqual(empty + 1);
      expect(value.composer.bottom).toBeLessThanOrEqual(viewport.height);
      expect(value.typography.userMessage).toBe("12px");
      expect(value.typography.answer).toBe("12px");
      await attachJsonEvidence(info, "发送后原请求与收缩", { posts, geometry: value, provider: "offline-script" });
    });
  });
}
