import { defineConfig, devices } from "@playwright/test";
import { resolve } from "node:path";
const quote = (v: string) => "'" + v.replaceAll("'", "'\"'\"'") + "'";
const root = resolve(import.meta.dirname, "..");
const evidence = process.env.KRIM_GUIDE_EVIDENCE ?? resolve(root, ".cache/agent-core/lc-ui1/browser");
const port = Number(process.env.KRIM_GUIDE_PORT ?? 8876);
const command = [process.env.KRIM_GUIDE_PYTHON ?? "python3.12", "-I", "-B", resolve(root, "tests/core/fixtures/ui_server.py"), "--database", resolve(evidence, "browser.sqlite"), "--port", String(port), "--evidence", evidence].map(quote).join(" ");
export default defineConfig({ testDir: "./e2e", testMatch: ["agent-core.spec.ts", "agent-core-ui.spec.ts", "evaluation.spec.ts"], fullyParallel: false, workers: 1, timeout: 30000, reporter: [["line"]], use: { baseURL: process.env.KRIM_GUIDE_BASE_URL ?? `http://127.0.0.1:${port}`, trace: "retain-on-failure", ...devices["Desktop Chrome"] }, webServer: process.env.KRIM_GUIDE_BASE_URL ? undefined : { command, url: `http://127.0.0.1:${port}/healthz`, reuseExistingServer: false, timeout: 15000 } });
