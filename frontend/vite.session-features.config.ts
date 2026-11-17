import { defineConfig, mergeConfig } from "vite";
import base from "./vite.config.ts";

// Keep linked dependencies inside the current checkout's Vite module paths.
// This fixture does not widen server.fs.allow or change production font styles.
export default mergeConfig(base, defineConfig({
  resolve: { preserveSymlinks: true },
}));
