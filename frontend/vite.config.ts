import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import { fileURLToPath } from "node:url";

export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      "@maintai/validators": fileURLToPath(
        new URL(
          "./node_modules/.cache/maintai/validators.cjs",
          import.meta.url,
        ),
      ),
    },
  },
  server: {
    proxy: {
      ...(process.env.MAINTAI_M8_INTEGRATION === "1"
        ? {
            "/replica-2/api": {
              target: "http://127.0.0.1:18032",
              changeOrigin: false,
              rewrite: (path: string) => path.replace(/^\/replica-2/, ""),
            },
          }
        : {}),
      "/api": {
        target:
          process.env.MAINTAI_BUSINESS_PROXY_ORIGIN ?? "http://127.0.0.1:8003",
        changeOrigin: false,
      },
    },
  },
});
