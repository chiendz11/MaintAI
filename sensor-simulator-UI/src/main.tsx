import { createRoot } from "react-dom/client";
import { App } from "./App";
import { loadConfig } from "./config";
import { createSession } from "./auth";
import { HttpControlApi } from "./api";
import { DemoControlApi } from "./demo";
import { SimulatorController } from "./controller";
import "./styles.css";

const root = createRoot(document.getElementById("root")!);
root.render(
  <div className="boot">
    <div className="brand-mark">S</div>
    <p>Đang mở Sensor Simulator…</p>
  </div>,
);

async function boot() {
  const config = await loadConfig();
  const session = await createSession(config);
  const api =
    config.mode === "demo"
      ? new DemoControlApi(sessionStorage)
      : new HttpControlApi(
          config.apiBaseUrl,
          config.requestTimeoutMs,
          session.token,
        );
  const storageKey = `maintai.simulator.commands.v1:${config.mode}:${config.apiBaseUrl}:${config.auth.mode === "oidc" ? config.auth.authority + config.auth.clientId : "local"}:${session.subject}`;
  const controller = new SimulatorController(
    api,
    sessionStorage,
    storageKey,
    config.staleAfterMs,
  );
  root.render(
    <App config={config} session={session} controller={controller} />,
  );
}
void boot().catch(() => {
  // Configuration/IdP responses may contain secrets. Keep raw exceptions out of the UI/logs.
  root.render(
    <div className="boot">
      <div className="brand-mark">S</div>
      <h1>Chưa thể mở Simulator UI</h1>
      <p>
        Kiểm tra runtime-config.json hoặc cấu hình đăng nhập OIDC. Không tự
        chuyển sang demo khi kết nối thật gặp lỗi.
      </p>
      <button className="button primary" onClick={() => location.assign("/")}>
        Mở lại ứng dụng
      </button>
    </div>,
  );
});
