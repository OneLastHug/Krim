import { defineConfig, mergeConfig } from "vite";
import { realpathSync } from "node:fs";
import { fileURLToPath } from "node:url";
import base from "./vite.config.ts";

const upstream = "http://127.0.0.1:5188";
const proxy = () => ({ target: upstream, changeOrigin: true, headers: { Origin: upstream } });
export default mergeConfig(base, defineConfig({
  root: fileURLToPath(new URL(".", import.meta.url)),
  server: {
    host: "127.0.0.1", port: 5189, strictPort: true,
    fs: { allow: [
      fileURLToPath(new URL(".", import.meta.url)),
      realpathSync(fileURLToPath(new URL("./node_modules/@fontsource-variable", import.meta.url))),
    ] },
    proxy: { "/v2": proxy(), "/v4": proxy(), "/v1": proxy(), "/healthz": proxy() },
  },
}));
