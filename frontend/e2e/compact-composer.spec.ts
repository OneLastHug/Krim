import { expect, test, type Page } from "@playwright/test";
import { attachJsonEvidence, screenshotStep } from "./step-screenshot.js";

type Theme = "warm" | "bright" | "dark";
const themes: Record<Theme, string> = { warm: "暖纸", bright: "浅色", dark: "深色" };
async function geometry(page: Page) {
  // 字体就绪后再取样，并等待连续布局帧，避免字体替换或视口变化污染几何证据。
  await page.evaluate(async () => {
    await document.fonts.ready;
    await new Promise<void>(resolve => requestAnimationFrame(() => requestAnimationFrame(() => resolve())));
  });
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
    const px = (value: string) => Number.parseFloat(value);
    const inputStyle = getComputedStyle(input);
    const composerStyle = getComputedStyle(composer);
    const sendStyle = getComputedStyle(send);
    const inputPaddingBorder = px(inputStyle.paddingTop) + px(inputStyle.paddingBottom)
      + px(inputStyle.borderTopWidth) + px(inputStyle.borderBottomWidth);
    const composerPaddingBorder = px(composerStyle.paddingTop) + px(composerStyle.paddingBottom)
      + px(composerStyle.borderTopWidth) + px(composerStyle.borderBottomWidth);
    const fourLineInputCssHeight = Math.max(
      4 * px(inputStyle.lineHeight) + inputPaddingBorder,
      px(inputStyle.minHeight) + (inputStyle.boxSizing === "content-box" ? inputPaddingBorder : 0),
    );
    const composerMinHeight = px(composerStyle.minHeight)
      + (composerStyle.boxSizing === "content-box" ? composerPaddingBorder : 0);
    const fourLineComposerCssHeight = Math.max(composerMinHeight, fourLineInputCssHeight + composerPaddingBorder);
    return { composer: box(composer), input: box(input), send: box(send),
      rows: input.rows, value: input.value, scrollHeight: input.scrollHeight, clientHeight: input.clientHeight,
      resize: inputStyle.resize, fieldSizing: inputStyle.getPropertyValue("field-sizing"),
      inputMaxHeight: inputStyle.maxHeight,
      fourLineCssDerived: { input: fourLineInputCssHeight, composer: fourLineComposerCssHeight },
      typography: {
        input: { fontSize: inputStyle.fontSize, lineHeight: inputStyle.lineHeight, minHeight: inputStyle.minHeight, padding: padding(input) },
        composer: { minHeight: composerStyle.minHeight, padding: padding(composer) },
        send: { minHeight: sendStyle.minHeight },
        messageMarkdown: lastFontSize(".message-markdown"),
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
      const drawer = page.getByRole("dialog", { name: "主导航与对话", exact: true });
      if (!await drawer.isVisible()) {
        const mobileMenu = page.getByRole("button", { name: "打开导航", exact: true });
        if (await mobileMenu.isVisible()) { await mobileMenu.click(); await expect(drawer).toBeVisible(); }
      }
      const surface = await drawer.isVisible() ? drawer : page.locator(".desktop-sidebar");
      await surface.getByRole("button", { name: "设置", exact: true }).click();
      await page.locator(".settings-popover .theme-options").getByRole("button", { name: new RegExp("^" + themes[theme]) }).click();
      await expect(page.locator("html")).toHaveAttribute("data-theme", theme);
      // 主题选择后恢复抽屉开合状态，避免遮挡后续主区操作。
      if (await drawer.isVisible()) { await page.keyboard.press("Escape"); await expect(drawer).not.toBeVisible(); }
      await page.locator(".toolbar-actions").getByRole("button", { name: "新建对话", exact: true }).click();
      await page.locator("#session-title").fill(mobile ? "消息草稿" : "任务说明");
      await page.getByRole("dialog", { name: "新建对话", exact: true }).getByRole("button", { name: "创建对话", exact: true }).click();
      await expect(page.locator("#chat-prompt")).toBeEnabled();
    });
    await step("空白保持单行与行内发送按钮", async () => {
      const value = await geometry(page);
      empty = value.composer.height;
      expect(value.rows).toBe(1);
      const expectedComposerHeight = mobile ? 52 : 46;
      expect(value.composer.height).toBeGreaterThanOrEqual(expectedComposerHeight - 1);
      expect(value.composer.height).toBeLessThanOrEqual(expectedComposerHeight + 1);
      expect(value.input.height).toBe(20);
      const expectedSendSize = mobile ? 40 : 28;
      expect(value.send.width).toBeGreaterThanOrEqual(expectedSendSize - 1);
      expect(value.send.width).toBeLessThanOrEqual(expectedSendSize + 1);
      expect(value.send.height).toBeGreaterThanOrEqual(expectedSendSize - 1);
      expect(value.send.height).toBeLessThanOrEqual(expectedSendSize + 1);
      expect(value.typography.input.fontSize).toBe("14px");
      expect(value.typography.input.lineHeight).toBe("18px");
      expect(value.typography.input.minHeight).toBe("20px");
      expect(value.typography.input.padding).toEqual({ top: "0px", right: "0px", bottom: "0px", left: "0px" });
      expect(value.typography.composer.minHeight).toBe(mobile ? "52px" : "46px");
      // 左内边距为行内加号按钮预留（绝对定位、28px），其余紧凑契约不变。
      expect(value.typography.composer.padding).toEqual(mobile
        ? { top: "14px", right: "56px", bottom: "14px", left: "46px" }
        : { top: "12px", right: "50px", bottom: "12px", left: "46px" });
      expect(value.typography.send.minHeight).toBe(mobile ? "40px" : "28px");
      const addBox = await page.locator(".chat-composer .composer-add").boundingBox();
      expect(addBox).toBeTruthy();
      expect(addBox!.width).toBeGreaterThanOrEqual(27);
      expect(addBox!.width).toBeLessThanOrEqual(29);
      expect(addBox!.height).toBeGreaterThanOrEqual(27);
      expect(addBox!.height).toBeLessThanOrEqual(29);
      expect(addBox!.x).toBeGreaterThanOrEqual(value.composer.x + 8);
      expect(addBox!.x + addBox!.width).toBeLessThanOrEqual(value.composer.x + 38);
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
      // CSS推导值与浏览器实际几何分开保存：四行内容高为4×line-height，再加输入框与composer盒模型尺寸。
      expect(value.input.height).toBeCloseTo(value.fourLineCssDerived.input, 1);
      expect(value.composer.height).toBeCloseTo(value.fourLineCssDerived.composer, 1);
      expect(value.scrollHeight).toBeLessThanOrEqual(value.clientHeight + 1);
      expect(posts).toHaveLength(0);
      await attachJsonEvidence(info, "多行输入几何", value);
      if (!mobile && theme === "warm") await page.locator(".chat-composer").screenshot({ path: info.outputPath("composer-multiline.png") });
    });
    await step("超过上限仅内部滚动且输入区仍可见", async () => {
      await page.locator("#chat-prompt").fill(Array.from({ length: 40 }, (_, index) => `第${index + 1}行：保留这一条任务说明`).join("\n"));
      const value = await geometry(page);
      expect(value.input.height).toBeCloseTo(Math.min(220, viewport.height * 0.34), 1);
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
      expect(value.typography.messageMarkdown).toBe("14px");
      await attachJsonEvidence(info, "发送后原请求与收缩", { posts, geometry: value, provider: "offline-script" });
    });
  });
}
