import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { App } from "./app/App";
import { loadConfig } from "./app/config";
import { createSession } from "./app/session";
import { sourceContextSupported } from "./api/codec";
import "./styles.css";

const root = createRoot(document.getElementById("root")!);
async function start() {
  if (!sourceContextSupported())
    throw new Error(
      "Cần cập nhật trình duyệt để đọc chính xác số thứ tự dữ liệu.",
    );
  const config = await loadConfig(),
    session = await createSession(config);
  root.render(
    <StrictMode>
      <App config={config} session={session} />
    </StrictMode>,
  );
}
root.render(
  <main className="login">
    <h1>MaintAI</h1>
    <p>Đang chuẩn bị không gian vận hành…</p>
  </main>,
);
void start().catch((error) =>
  root.render(
    <main className="login">
      <h1>Chưa thể mở giao diện</h1>
      <p role="alert">
        {error instanceof Error ? error.message : "Không đọc được cấu hình."}
      </p>
    </main>,
  ),
);
