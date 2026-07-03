import { defineConfig } from "vite";
import solid from "vite-plugin-solid";
import { fileURLToPath, URL } from "node:url";

export default defineConfig({
  plugins: [solid()],
  resolve: {
    alias: { "@": fileURLToPath(new URL("./src", import.meta.url)) },
  },
  // Single static bundle the Rust binary can embed and serve.
  build: { target: "esnext", outDir: "dist", sourcemap: false },
  server: { port: 5173 },
});
