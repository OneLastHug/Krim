import { defineConfig } from "@playwright/test";
import base from "./playwright.config.js";
import { pythonExecutable, pythonFixtureArgs } from "./e2e/python-client.js";
const quote = (value: string) => "'" + value.replaceAll("'", "'\\''") + "'";
export default defineConfig({
  ...base,
  testMatch: ["missing-directory-restores.spec.ts"],
  webServer: {
    ...base.webServer,
    command: [pythonExecutable, ...pythonFixtureArgs("browser_ui_server.py")].map(quote).join(" "),
    env: { ...(base.webServer && !Array.isArray(base.webServer) ? base.webServer.env : {}), KRIM_TEST_MISSING_DIRECTORY_RESTORES: "1" },
  },
});
