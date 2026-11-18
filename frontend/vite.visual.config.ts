import { defineConfig, normalizePath, type Plugin } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";
import { readFileSync, existsSync, realpathSync } from "node:fs";
import { resolve, dirname, extname, sep } from "node:path";
import { fileURLToPath } from "node:url";
const frontend = dirname(fileURLToPath(import.meta.url));
const root = resolve(frontend, "..");
const fixture = normalizePath(resolve(frontend, "visual/fixture-hooks.tsx"));
const aliases = new Set(["use-session", "use-development", "use-session-context", "use-session-sidebar", "use-session-features"]);
const dataOnly: Plugin = { name: "approved-visual-data-only", enforce: "pre", resolveId(source) {
  const file = source.split("/").at(-1)?.replace(/\.tsx?$/, "");
  if (source.startsWith(".") && file && aliases.has(file)) return fixture;
  return null;
} };
export default defineConfig(({ mode }) => ({
  root: resolve(frontend, "visual"), base: "/", publicDir: false,
  plugins: [dataOnly, react(), tailwindcss(), {
    name: "approved-reference-page", configureServer(server) {
      server.middlewares.use((req, res, next) => {
        const url = new URL(req.url ?? "/", "http://localhost");
        if (url.pathname.startsWith("/@fonts/")) {
          const fonts = resolve(frontend, "node_modules/@fontsource-variable");
          const file = resolve(fonts, decodeURIComponent(url.pathname.slice(8)));
          if (!file.startsWith(fonts + sep) || !existsSync(file) || extname(file) !== ".woff2") { res.statusCode = 404; res.end(); return; }
          res.setHeader("Content-Type", "font/woff2"); res.end(readFileSync(file)); return;
        }
        if (mode === "reference" && url.pathname === "/") {
          const reference = resolve(root, "docs/prototypes/qa-ui-improvements/index.html");
          let html = readFileSync(reference, "utf8");
          const end = html.lastIndexOf("})();");
          const theme = ["warm", "bright", "dark"].includes(url.searchParams.get("theme") ?? "") ? url.searchParams.get("theme") : "bright";
          const scene = url.searchParams.get("scene") === "empty" ? 'state.currentSessionId = null; state.newSessionProject = "krim-core";' : "";
          html = html.slice(0, end) + `state.theme = ${JSON.stringify(theme)}; document.documentElement.dataset.theme = state.theme; ${scene} renderAll();\n` + html.slice(end);
          res.setHeader("Content-Type", "text/html; charset=utf-8"); res.setHeader("Cache-Control", "no-store"); res.end(html); return;
        }
        if (url.pathname.startsWith("/v2/") || url.pathname.startsWith("/api/") || url.pathname === "/healthz") {
          res.statusCode = 503; res.setHeader("Content-Type", "application/json"); res.end(JSON.stringify({ error: { code: "visual_fixture_read_only" } })); return;
        }
        next();
      });
    },
  }],
  resolve: { alias: { "@": resolve(frontend, "src") } },
  cacheDir: resolve(root, ".cache/visual-contract-vite", mode),
  server: { host: "localhost", strictPort: true, fs: { allow: [frontend, realpathSync(resolve(frontend, "node_modules/@fontsource-variable"))] } },
}));
