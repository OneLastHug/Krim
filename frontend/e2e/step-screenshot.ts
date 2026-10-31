import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { dirname } from "node:path";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { test, type Page, type TestInfo } from "@playwright/test";

const DEFAULT_MASK_SELECTORS = [
  "[data-sensitive]",
  "[data-screenshot-mask]",
  "input[type='password']",
  "#api-key",
];

type ScreenshotRecord = {
  sequence: number;
  case: string;
  step: string;
  viewport: { width: number; height: number };
  theme: "dark" | "light" | "warm" | "bright" | "unknown";
  runtimeRevision: string;
  workingTree: "clean" | "dirty" | "unknown";
  nodeVersion: string;
  screenshot: string;
  maskSelectors: string[];
  status: "passed" | "failed";
};

export type StepScreenshotOptions = {
  caseName?: string;
  stepName: string;
  theme?: "dark" | "light" | "warm" | "bright";
  fullPage?: boolean;
  maskSelectors?: readonly string[];
};

const stepSequences = new WeakMap<TestInfo, number>();

function nextStepSequence(testInfo: TestInfo): number {
  const sequence = (stepSequences.get(testInfo) ?? 0) + 1;
  stepSequences.set(testInfo, sequence);
  return sequence;
}

function filePart(value: string, maxLength = 40): string {
  const readable = value.normalize("NFKC")
    .replace(/[^\p{L}\p{N}._-]+/gu, "-")
    .replace(/^-+|-+$/g, "");
  const digest = createHash("sha256").update(value).digest("hex").slice(0, 8);
  return `${(readable || "step").slice(0, maxLength)}-${digest}`;
}

type RuntimeIdentity = {
  revision: string;
  workingTree: "clean" | "dirty" | "unknown";
};

function gitOutput(args: string[]): string | undefined {
  try {
    return execFileSync("git", args, {
      cwd: process.cwd(),
      encoding: "utf8",
      stdio: ["ignore", "pipe", "ignore"],
    }).trim();
  } catch {
    return undefined;
  }
}

function runtimeIdentity(): RuntimeIdentity {
  const revision = process.env.KRIM_RUNTIME_REVISION
    ?? process.env.GITHUB_SHA
    ?? process.env.BUILD_SOURCEVERSION
    ?? gitOutput(["rev-parse", "--short=12", "HEAD"])
    ?? "git-unavailable";
  const explicitDirty = process.env.KRIM_RUNTIME_DIRTY?.toLowerCase();
  const workingTree = explicitDirty === "true" || explicitDirty === "dirty" || explicitDirty === "1"
    ? "dirty"
    : explicitDirty === "false" || explicitDirty === "clean" || explicitDirty === "0"
      ? "clean"
      : (() => {
          const status = gitOutput(["status", "--porcelain", "--untracked-files=normal"]);
          return status === undefined ? "unknown" : status ? "dirty" : "clean";
        })();
  return { revision, workingTree };
}

async function readIndex(path: string): Promise<ScreenshotRecord[]> {
  try {
    return JSON.parse(await readFile(path, "utf8")) as ScreenshotRecord[];
  } catch (error) {
    if (error && typeof error === "object" && "code" in error && error.code === "ENOENT") {
      return [];
    }
    throw error;
  }
}

/** 将API结果写入本轮独立目录；不依赖报告器是否保留内存附件。 */
export async function attachJsonEvidence(testInfo: TestInfo, name: string, value: unknown): Promise<string> {
  const path = testInfo.outputPath("api-results", filePart(name, 60) + ".json");
  await mkdir(dirname(path), { recursive: true });
  await writeFile(path, JSON.stringify(value, null, 2) + "\n", { encoding: "utf8", flag: "wx" });
  await testInfo.attach(name, { path, contentType: "application/json" });
  return path;
}

/** 在一个可追踪的 Playwright step 中执行动作并保存脱敏截图与索引。 */
export async function screenshotStep<T>(
  page: Page,
  testInfo: TestInfo,
  options: StepScreenshotOptions,
  action: () => Promise<T>,
): Promise<T> {
  return test.step(options.stepName, async () => {
    const sequence = nextStepSequence(testInfo);
    const caseName = options.caseName ?? testInfo.titlePath.filter(Boolean).join("-");
    const identity = runtimeIdentity();
    const nodeVersion = process.version;
    const maskSelectors = [...new Set([
      ...DEFAULT_MASK_SELECTORS,
      ...(options.maskSelectors ?? []),
    ])];
    let actionFailed = false;
    let actionError: unknown;
    let result: T | undefined;

    try {
      result = await action();
    } catch (error) {
      actionFailed = true;
      actionError = error;
    }

    let captureError: unknown;
    let viewport = { width: 0, height: 0 };
    let theme: "dark" | "light" | "warm" | "bright" | "unknown" = options.theme ?? "unknown";
    try {
      viewport = page.viewportSize() ?? await page.evaluate(() => ({
        width: window.innerWidth,
        height: window.innerHeight,
      }));
      const actualTheme = await page.evaluate(() => document.documentElement.dataset.theme);
      if (actualTheme === "warm" || actualTheme === "bright" || actualTheme === "dark" || actualTheme === "light") {
        theme = actualTheme;
      } else if (!options.theme) {
        theme = await page.evaluate(() =>
          window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light",
        );
      }
    } catch (error) {
      captureError = error;
    }

    const filename = [
      `case-${filePart(caseName)}`,
      `step-${String(sequence).padStart(3, "0")}-${filePart(options.stepName)}`,
      `${viewport.width}x${viewport.height}`,
      `theme-${theme}`,
      `runtime-${filePart(identity.revision, 20)}-${identity.workingTree}`,
      `node-${filePart(nodeVersion, 16)}`,
    ].join("--") + ".png";
    const screenshotPath = testInfo.outputPath("step-screenshots", filename);
    const indexPath = testInfo.outputPath("step-screenshots", "index.json");

    try {
      await mkdir(dirname(screenshotPath), { recursive: true });
      await page.screenshot({
        path: screenshotPath,
        fullPage: options.fullPage ?? false,
        animations: "disabled",
        mask: maskSelectors.map(selector => page.locator(selector)),
        maskColor: "#333333",
      });
      await testInfo.attach(filename, { path: screenshotPath, contentType: "image/png" });
    } catch (error) {
      captureError ??= error;
    }

    const record: ScreenshotRecord = {
      sequence,
      case: caseName,
      step: options.stepName,
      viewport,
      theme,
      runtimeRevision: identity.revision,
      workingTree: identity.workingTree,
      nodeVersion,
      screenshot: filename,
      maskSelectors,
      status: actionFailed || captureError !== undefined ? "failed" : "passed",
    };
    try {
      const index = await readIndex(indexPath);
      await writeFile(indexPath, JSON.stringify([...index, record], null, 2) + "\n");
    } catch (error) {
      captureError ??= error;
    }

    if (actionFailed) throw actionError;
    if (captureError !== undefined) throw captureError;
    return result as T;
  });
}
