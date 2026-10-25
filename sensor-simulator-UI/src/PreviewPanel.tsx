import { Activity, ArrowUpRight, Radio, Waves } from "lucide-react";
import type { Preview } from "./demo";

export function PreviewPanel({
  preview,
  demo,
  machineId,
  paused,
}: {
  preview?: Preview;
  demo: boolean;
  machineId: string;
  paused: boolean;
}) {
  const features = [
    ["Nhiệt độ không khí", preview?.air.toFixed(1), "K"],
    ["Nhiệt độ quá trình", preview?.process.toFixed(1), "K"],
    ["Tốc độ quay", preview?.rpm.toLocaleString("vi-VN"), "RPM"],
    ["Mô-men xoắn", preview?.torque.toFixed(1), "Nm"],
    ["Độ mòn dụng cụ", preview?.wear.toFixed(1), "min"],
    ["Loại sản phẩm", preview?.type, "TYPE"],
  ];
  const history = preview?.history ?? [];
  const lower = Math.min(1400, ...history) - 40;
  const upper = Math.max(1650, ...history) + 40;
  const points = history
    .map(
      (value, i) =>
        `${8 + (i / Math.max(29, history.length - 1)) * 690},${128 - ((value - lower) / (upper - lower)) * 103}`,
    )
    .join(" ");
  return (
    <section className="card preview-panel" aria-labelledby="preview-title">
      <div className="card-heading">
        <div>
          <div className="eyebrow">SENSOR TELEMETRY</div>
          <h2 id="preview-title">Sensor Preview</h2>
        </div>
        <span className={`badge ${demo ? "teal" : "neutral"}`}>
          <Radio size={12} />
          {demo ? "Dữ liệu demo" : "Chưa có nguồn đọc"}
        </span>
      </div>
      <div className="telemetry-grid">
        {features.map(([label, value, unit], i) => (
          <div
            className={`telemetry-cell ${i === 2 ? "featured" : ""}`}
            key={label}
          >
            <span>{label}</span>
            <div>
              <strong>{demo && value ? value : "—"}</strong>
              <small>{unit}</small>
            </div>
            <div className="cell-rule">
              <i style={{ width: `${demo && value ? 45 + i * 7 : 0}%` }} />
            </div>
          </div>
        ))}
      </div>
      <div className="chart-box">
        <div className="chart-heading">
          <span>
            <Waves size={14} /> Tốc độ quay
          </span>
          <span>
            {demo
              ? "30 readings gần nhất · demo"
              : "Telemetry chưa được cung cấp"}
          </span>
        </div>
        {demo ? (
          <>
            <svg
              className="sparkline"
              viewBox="0 0 710 150"
              role="img"
              aria-label={`Biểu đồ RPM demo của ${machineId}`}
            >
              <defs>
                <linearGradient id="chart-fill" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor="#09a78a" stopOpacity=".18" />
                  <stop offset="100%" stopColor="#09a78a" stopOpacity="0" />
                </linearGradient>
              </defs>
              {[28, 63, 98, 133].map((y) => (
                <line
                  key={y}
                  x1="8"
                  x2="702"
                  y1={y}
                  y2={y}
                  stroke="#e5ecef"
                  strokeDasharray="4 5"
                />
              ))}
              {points && (
                <>
                  <polygon
                    points={`8,140 ${points} ${8 + ((history.length - 1) / 29) * 690},140`}
                    fill="url(#chart-fill)"
                  />
                  <polyline
                    points={points}
                    fill="none"
                    stroke="#08a88a"
                    strokeWidth="2.8"
                    strokeLinejoin="round"
                    strokeLinecap="round"
                  />
                </>
              )}
            </svg>
            <div className="chart-caption">
              <span>
                <i className="status-dot" /> RPM ·{" "}
                {paused
                  ? "Đang tạm dừng preview"
                  : "Mô phỏng trong trình duyệt"}
              </span>
              <span>Hiện tại</span>
            </div>
          </>
        ) : (
          <div className="preview-empty">
            <Activity size={26} />
            <strong>Chưa có sensor preview từ API</strong>
            <p>
              Bạn vẫn có thể điều khiển máy. Các giá trị cảm biến sẽ hiển thị
              khi có API đọc telemetry được thống nhất.
            </p>
          </div>
        )}
      </div>
      <div className="preview-foot">
        <span className="preview-icon">
          <ArrowUpRight size={16} />
        </span>
        <div>
          <strong>
            {demo
              ? `DEMO-${machineId || "—"}-${preview?.seq ?? 0}`
              : "Control và telemetry là hai nguồn riêng"}
          </strong>
          <p>
            {demo
              ? "Reading mẫu chỉ ở trình duyệt, không được gửi tới Ingestion hoặc Kafka."
              : "RUNNING là control state, không xác nhận Ingestion, Kafka hay ML đang hoạt động."}
          </p>
        </div>
        {demo && <span className="sequence">SEQ {preview?.seq ?? 0}</span>}
      </div>
    </section>
  );
}
