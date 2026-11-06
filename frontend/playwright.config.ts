import { randomUUID } from "node:crypto";
import { defineConfig, devices } from "@playwright/test";
import { resolve } from "node:path";
const quote = (v: string) => "'" + v.replaceAll("'", "'\"'\"'") + "'";
const root = resolve(import.meta.dirname, "..");
const requestedRunId = (process.env.KRIM_PLAYWRIGHT_RUN_ID ?? process.env.GITHUB_RUN_ID ?? "local")
  .replace(/[^A-Za-z0-9._-]/g, "-").slice(0, 80);
const executionId = process.env.KRIM_PLAYWRIGHT_EXECUTION_ID ?? randomUUID();
process.env.KRIM_PLAYWRIGHT_EXECUTION_ID = executionId;
const outputDir = process.env.KRIM_PLAYWRIGHT_OUTPUT_DIR
  ? resolve(process.env.KRIM_PLAYWRIGHT_OUTPUT_DIR)
  : resolve(root, ".cache", "playwright-results",
    `${requestedRunId}-${process.env.GITHUB_RUN_ATTEMPT ?? "1"}-${executionId}`);
const evidence = process.env.KRIM_AGENT_CORE_EVIDENCE ?? process.env.KRIM_GUIDE_EVIDENCE ?? resolve(root, ".cache/agent-core/lc-ui1/browser");
const port = Number(process.env.KRIM_GUIDE_PORT ?? 8876);
const workspace = process.env.KRIM_AGENT_WORKSPACE === "1";
const workspaceFixture = process.env.KRIM_AGENT_WORKSPACE_INSTALLED_FIXTURE;
if (workspace && !workspaceFixture) throw new Error("工作区必须显式提供同安装包fixture");
const command = [process.env.KRIM_GUIDE_PYTHON ?? "python3.12", "-I", "-B", workspace ? workspaceFixture! : resolve(root, "tests/core/fixtures/ui_server.py"), "--database", resolve(evidence, "browser.sqlite"), "--port", String(port), "--evidence", evidence].map(quote).join(" ");
export default defineConfig({ outputDir, testDir: "./e2e", testMatch: ["agent-core.spec.ts", "agent-core-ui.spec.ts", "evaluation.spec.ts", "http-provider.spec.ts", "chat.spec.ts", "agent-workspace.spec.ts", "step-screenshot.spec.ts", "workbench-phases.spec.ts", "product-evaluation.spec.ts", "prepared-workspace.spec.ts", "evaluation-automation.spec.ts", "installed-package-readonly.spec.ts", "workbench-redesign.spec.ts", "legacy-access.spec.ts", "compact-composer.spec.ts", "sidebar-footer.spec.ts"], fullyParallel: false, workers: 1, timeout: 30000, reporter: [["line"]], use: { baseURL: process.env.KRIM_AGENT_CORE_BASE_URL ?? process.env.KRIM_GUIDE_BASE_URL ?? `http://127.0.0.1:${port}`, trace: "retain-on-failure", launchOptions: { executablePath: process.env.KRIM_AGENT_CORE_BROWSER }, ...devices["Desktop Chrome"] }, webServer: (process.env.KRIM_AGENT_CORE_BASE_URL || process.env.KRIM_GUIDE_BASE_URL) ? undefined : { command, url: `http://127.0.0.1:${port}/healthz`, reuseExistingServer: false, timeout: 15000 } });
