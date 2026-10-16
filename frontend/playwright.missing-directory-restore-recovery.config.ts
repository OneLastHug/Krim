import { defineConfig, devices } from "@playwright/test";
import { pythonExecutable, pythonFixtureArgs } from "./e2e/python-client.js";
const quote = (value: string) => "'" + value.replaceAll("'", "'\\''") + "'";

const point = process.env.KRIM_TEST_MISSING_DIRECTORY_RESTORE_RECOVERY_POINT ?? "absence";
if (point !== "absence" && point !== "effect") throw new Error("目录恢复强杀点必须是 absence/effect");
export default defineConfig({
  testDir: "./e2e", testMatch: "missing-directory-restore-recovery.spec.ts", timeout: 30_000, expect: { timeout: 10_000 }, workers: 1, reporter: [["line"]],
  use: { baseURL: "http://127.0.0.1:8879", trace: "retain-on-failure", ...devices["Desktop Chrome"] },
  webServer: { command: [pythonExecutable, ...pythonFixtureArgs("missing_directory_restore_browser_recovery_server.py")].map(quote).join(" "), env: { KRIM_TEST_MISSING_DIRECTORY_RESTORE_RECOVERY_POINT: point }, url: "http://127.0.0.1:8879/healthz", reuseExistingServer: false, timeout: 30_000 },
});
