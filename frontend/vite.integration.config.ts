import { defineConfig, mergeConfig } from "vite";
import { realpathSync } from "node:fs";
import { fileURLToPath } from "node:url";
import base from "./vite.config";

// Development-only entry for the production React app and a local script-provider API.
// Keep Host and Origin unchanged so the API's normal same-origin boundary still applies.
export default mergeConfig(base, defineConfig({
  base: "/",
  cacheDir: "../.cache/production-ui/vite",
  plugins: [{
    name: "permission-ui-hidden-preview",
    transformIndexHtml(html, context) {
      // Explicit, local verification only. Production builds use vite.config.ts.
      const url = new URL(context.originalUrl ?? context.path, "http://local");
      return url.searchParams.get("permission_ui_test") === "1"
        ? html.replace("<head>", "<head><style data-permission-ui-test>*,*::before,*::after{animation:none!important;transition:none!important}</style><script>window.requestAnimationFrame=callback=>window.setTimeout(()=>callback(performance.now()),16);window.cancelAnimationFrame=id=>window.clearTimeout(id);</script>")
        : html;
    },
  }],
  server: {
    host: "127.0.0.1",
    port: 5200,
    strictPort: true,
    // Reused dependency junctions may resolve outside this worktree. Permit
    // exactly the frontend and that dependency directory, not another checkout.
    fs: { allow: [fileURLToPath(new URL(".", import.meta.url)), realpathSync(fileURLToPath(new URL("./node_modules", import.meta.url)))] },
    proxy: Object.fromEntries(["/v2", "/v1", "/api", "/healthz"].map(path => [path, { target: "http://127.0.0.1:5201", changeOrigin: false }])),
  },
}));
