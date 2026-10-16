import { defineConfig, devices } from "@playwright/test";

const point = process.env.KRIM_TEST_DIRECTORY_RESTORE_RECOVERY_POINT ?? "backup";
if (point !== "backup" && point !== "effect") throw new Error("目录恢复强杀点必须是 backup/effect");
export default defineConfig({
  testDir: "./e2e", testMatch: "directory-restore-recovery.spec.ts", timeout: 30_000, expect: { timeout: 10_000 }, workers: 1, reporter: [["line"]],
  use: { baseURL: "http://127.0.0.1:8879", trace: "retain-on-failure", ...devices["Desktop Chrome"] },
  webServer: { command: "../.venv/bin/python ../tests/fixtures/directory_restore_browser_recovery_server.py", env: { KRIM_TEST_DIRECTORY_RESTORE_RECOVERY_POINT: point }, url: "http://127.0.0.1:8879/healthz", reuseExistingServer: false, timeout: 30_000 },
});
