import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";
import { fileURLToPath, URL } from "node:url";
export default defineConfig({ base: "/ui/", publicDir: false, plugins: [react(), tailwindcss()], resolve: { alias: { "@": fileURLToPath(new URL("./src", import.meta.url)) } }, cacheDir: process.env.KRIM_AGENT_CORE_VITE_CACHE ?? "../.cache/agent-core/ac-m2-final/vite", build: { outDir: process.env.KRIM_AGENT_CORE_BUILD ?? "../.cache/agent-core/ac-m2-final/build", cssCodeSplit: false, sourcemap: false, manifest: true, license: { fileName: ".vite/licenses.json" }, rolldownOptions: { output: { entryFileNames: "assets/app.js", chunkFileNames: "assets/[name].js", assetFileNames: asset => asset.names.some(name => name.endsWith(".css")) ? "assets/styles.css" : "assets/[name][extname]" } } } });
