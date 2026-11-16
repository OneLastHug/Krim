import { defineConfig, mergeConfig } from "vite";
import base from "./vite.config";

// Development-only entry for the production React app and a local script-provider API.
// Keep Host and Origin unchanged so the API's normal same-origin boundary still applies.
export default mergeConfig(base, defineConfig({
  base: "/",
  cacheDir: "../.cache/production-ui/vite",
  server: {
    host: "127.0.0.1",
    port: 5200,
    strictPort: true,
    proxy: Object.fromEntries(["/v2", "/v1", "/api", "/healthz"].map(path => [path, { target: "http://127.0.0.1:5201", changeOrigin: false }])),
  },
}));
