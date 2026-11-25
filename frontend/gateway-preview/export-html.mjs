import { build } from "vite";
import { readFile, writeFile, mkdir } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const folder = dirname(fileURLToPath(import.meta.url));
const root = resolve(folder, "../..");
const output = resolve(root, ".cache/gateway-preview-export");
await build({
  configFile: resolve(folder, "../vite.gateway-preview.config.ts"),
  build: {
    outDir: output,
    emptyOutDir: true,
    cssCodeSplit: false,
    assetsInlineLimit: 20 * 1024 * 1024,
    rolldownOptions: { output: { codeSplitting: false } },
  },
});
let html = await readFile(resolve(output, "index.html"), "utf8");
const scripts = [...html.matchAll(/<script\b([^>]*?)src="([^"]+)"([^>]*)><\/script>/g)];
for (const match of scripts) {
  const code = await readFile(resolve(output, match[2].replace(/^\//, "")), "utf8");
  html = html.replace(match[0], () => '<script type="module">' + code.replace(/<\/script/gi, "<\\/script") + '</script>');
}
const styles = [...html.matchAll(/<link\b[^>]*rel="stylesheet"[^>]*href="([^"]+)"[^>]*>/g)];
for (const match of styles) {
  const css = await readFile(resolve(output, match[1].replace(/^\//, "")), "utf8");
  html = html.replace(match[0], () => '<style>' + css.replace(/<\/style/gi, "<\\/style") + '</style>');
}
html = html.replace(/<link\b[^>]*rel="modulepreload"[^>]*>/g, "");
const destination = resolve(root, "docs/prototypes/krim-gateway-full.html");
await mkdir(dirname(destination), { recursive: true });
await writeFile(destination, html, "utf8");
console.log("Exported complete preview HTML (" + (Buffer.byteLength(html) / 1024 / 1024).toFixed(1) + " MiB)");
