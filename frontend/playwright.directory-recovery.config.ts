import { defineConfig, devices } from "@playwright/test";

const stage = process.env.KRIM_TEST_DIRECTORY_RECOVERY_STAGE ?? "record";
if (stage !== "record" && stage !== "finish") throw new Error("目录恢复测试只支持 record/finish 强杀点");

export default defineConfig({
  testDir: "./e2e",
  testMatch: "directory-checkpoint-recovery.spec.ts",
  timeout: 30_000,
  expect: { timeout: 10_000 },
  workers: 1,
  reporter: [["line"]],
  use: { baseURL: "http://127.0.0.1:8878", trace: "retain-on-failure", ...devices["Desktop Chrome"] },
  webServer: {
    command: "../.venv/bin/python ../tests/fixtures/directory_checkpoint_browser_recovery_server.py",
    env: { KRIM_TEST_DIRECTORY_RECOVERY_STAGE: stage },
    url: "http://127.0.0.1:8878/healthz",
    reuseExistingServer: false,
    timeout: 30_000,
  },
});
