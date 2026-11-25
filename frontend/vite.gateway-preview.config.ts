import { defineConfig, normalizePath, type Plugin } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";
import { readFileSync, realpathSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const frontend = dirname(fileURLToPath(import.meta.url));
const fixtures = normalizePath(resolve(frontend, "gateway-preview/fixture-hooks.tsx"));
const hookNames = new Set(["use-session", "use-development", "use-session-context", "use-session-sidebar", "use-session-features", "use-subagents"]);
const preview: Plugin = {
  name: "gateway-preview-only", enforce: "pre",
  resolveId(source) {
    if (!source.startsWith(".")) return null;
    const name = source.split("/").at(-1)?.replace(/\.tsx?$/, "");
    return name && hookNames.has(name) ? fixtures : null;
  },
  transform(code, rawId) {
    const id = normalizePath(rawId).split("?")[0];
    if (id === normalizePath(resolve(frontend, "src/components/ChatWorkbench.tsx"))) {
      const integrated = code.indexOf('          {gateways ? <ModelGatewayControls');
      const start = integrated >= 0 ? integrated : code.indexOf('          <button type="button" ref={modelRef}');
      const end = code.indexOf("          {!chat.session", start);
      const infoStart = code.indexOf('      <PopoverMenu open={info !== null}');
      const infoEnd = code.indexOf("      </PopoverMenu>", infoStart);
      if (start < 0 || end < 0 || infoStart < 0 || infoEnd < 0) this.error("Chat model control preview anchor changed");
      code = code.slice(0, infoStart) + code.slice(infoEnd + "      </PopoverMenu>".length);
      code = code.slice(0, start) + '          <GatewayControls sessionId={chat.id || "new"} />\n' + code.slice(end);
      return { code: 'import { GatewayControls } from "/@fs/' + normalizePath(resolve(frontend, "gateway-preview/GatewayControls.tsx")) + '";\n' + code, map: null };
    }
    if (id === normalizePath(resolve(frontend, "src/components/AppSidebar.tsx"))) {
      if (code.includes('onAccountTab?.("gateways")')) return { code: code.replace('onAccountTab?.("gateways")', 'window.dispatchEvent(new Event("krim:gateway-settings"))'), map: null };
      const anchor = '      {accountAdmin && <button type="button" role="menuitem"';
      const at = code.indexOf(anchor);
      if (at < 0) this.error("Account menu preview anchor changed");
      return { code: code.slice(0, at) + '      <button type="button" role="menuitem" className="menu-item" onClick={() => { setAccountOpen(false); window.dispatchEvent(new Event("krim:gateway-settings")); onClose?.(); }}><Settings2 aria-hidden="true" />接入与模型</button>\n' + code.slice(at), map: null };
    }
    return null;
  },
  configureServer(server) {
    server.middlewares.use((req, res, next) => {
      const url = new URL(req.url ?? "/", "http://localhost");
      if (url.pathname.startsWith("/v2/") || url.pathname.startsWith("/api/") || url.pathname === "/healthz") {
        if (url.pathname.endsWith("/session-sidebar")) {
          const cases = JSON.parse(readFileSync(resolve(frontend, "visual/approved-cases.json"), "utf8"));
          res.setHeader("Content-Type", "application/json");
          res.end(JSON.stringify({ revision: 0, groups: cases.groups, sessions: cases.sessions.map((s: {id:string;projectId:string;parentId:string|null;pinned:boolean}) => ({session_id:s.id,group_id:s.projectId,parent_id:s.parentId,pinned:s.pinned})) }));
          return;
        }
        res.statusCode = 404; res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ error: { code: "preview_local_only" } })); return;
      }
      next();
    });
  },
};

export default defineConfig({
  root: resolve(frontend, "gateway-preview"), base: "/", publicDir: false,
  plugins: [preview, react(), tailwindcss()],
  resolve: { alias: { "@": resolve(frontend, "src") } },
  cacheDir: resolve(frontend, "../.cache/gateway-preview-vite"),
  server: { host: "127.0.0.1", port: 5188, strictPort: true, fs: { allow: [resolve(frontend, ".."), realpathSync(resolve(frontend, "node_modules"))] } },
  build: { outDir: resolve(frontend, "../.cache/gateway-preview-build"), emptyOutDir: true },
});
