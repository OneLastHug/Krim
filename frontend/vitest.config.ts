import { defineConfig } from "vitest/config";
import react from "@vitejs/plugin-react";
import { fileURLToPath, URL } from "node:url";
export default defineConfig({ plugins: [react()], resolve: { alias: { "@": fileURLToPath(new URL("./src", import.meta.url)) } }, cacheDir: process.env.KRIM_GUIDE_CACHE ?? "../.cache/agent-core/lc-m2/vitest", test: { environment: "jsdom", include: ["contracts/run-ui.test.tsx", "contracts/eval-ui.test.tsx"], maxWorkers: 1 } });
