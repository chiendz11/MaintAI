import { defineConfig, loadEnv } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig(({ mode }) => {
  const { SENSOR_CONTROL_PROXY_TARGET } = loadEnv(mode, process.cwd(), "");
  return {
    plugins: [react()],
    server: {
      proxy: SENSOR_CONTROL_PROXY_TARGET
        ? { "/api/v1/simulator": { target: SENSOR_CONTROL_PROXY_TARGET } }
        : undefined,
    },
    build: { sourcemap: false },
  };
});
