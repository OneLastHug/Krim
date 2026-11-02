import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";
import { fileURLToPath, URL } from "node:url";

export default defineConfig({
  base: "/",
  publicDir: false,
  plugins: [react(), tailwindcss(), {
    name: "prototype-entry",
    configureServer(server) {
      server.middlewares.use((request, response, next) => {
        if (request.url !== "/") return next();
        response.writeHead(302, { Location: "/prototype.html" });
        response.end();
      });
    },
  }],
  resolve: { alias: { "@": fileURLToPath(new URL("./src", import.meta.url)) } },
  cacheDir: "../.cache/ui-prototype/vite",
  server: { host: "127.0.0.1", port: 5182, strictPort: true },
  build: {
    outDir: "../.cache/ui-prototype/build",
    emptyOutDir: true,
    sourcemap: false,
    rolldownOptions: {
      input: fileURLToPath(new URL("./prototype.html", import.meta.url)),
    },
  },
});
